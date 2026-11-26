# Copyright 2026 Benoit Chesneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Zygote of a session template: prepare an interpreter once, then fork one
fresh child per session.

Started by src/py_session_template.erl as

    python3 py_zygote.py CONTROL_SOCKET_PATH

The zygote stays single threaded (a fork only copies the calling thread) and
never serves Python requests itself. Frames on the control socket use the
format of the isolated child (_erlang_impl/_isolated.py):

    Erlang -> zygote  {init, Paths, Imports, Preload}
                      {fork, SocketPath, ContextPid, #{rlimits, cgroup, cd}}
    zygote -> Erlang  replies to both, and the events
                      {ready, Info}, {exited, OsPid, ExitCode}

The child runtime (_isolated.Runtime and the `erlang` module) is built here,
before the first fork, without a socket: user code can `import erlang` at
top level, and preload globals are the `__main__` namespace of every
session. After the fork the child connects the runtime to its own socket and
continues exactly like a spawned isolated child.
"""

import os
import sys


def main(argv):
    if len(argv) != 2:
        sys.stderr.write('usage: py_zygote.py CONTROL_SOCKET_PATH\n')
        os._exit(3)
    priv = os.path.dirname(os.path.abspath(__file__))
    if priv not in sys.path:
        sys.path.insert(0, priv)

    # Everything the session start-up path uses is imported now, once:
    # imported after the fork it would be paid by every session.
    import ctypes
    import io
    import resource  # noqa: F401
    import selectors
    import signal
    import socket
    import struct
    import threading
    import traceback
    import py_isolated_child as child
    from _erlang_impl import _etf, _isolated
    from _erlang_impl._etf import Atom
    ctypes.CDLL(None)

    child._arm_parent_death()
    ctrl = child._connect(argv[1])
    runtime = _isolated.Runtime(None)
    _isolated.install_erlang_module(runtime)

    header = struct.Struct('=QI')

    def send(frame_id, status, term):
        body = bytes([status]) + _etf.encode(term)
        ctrl.sendall(header.pack(frame_id, len(body)) + body)

    def event(term):
        send(0, _isolated.STATUS_EVENT, term)

    def thread_names():
        return [t.name for t in threading.enumerate() if t is not threading.main_thread()]

    def init(term):
        _, paths, imports, preload = term
        for path in reversed(_isolated._as_list(paths)):
            path = _isolated._as_text(path)
            if path not in sys.path:
                sys.path.insert(0, path)
        import importlib
        for name in _isolated._as_list(imports):
            importlib.import_module(_isolated._as_text(name))
        code = _isolated._as_text(preload)
        if code:
            exec(compile(code, '<preload>', 'exec'), runtime.globals)
        names = thread_names()
        if names:
            # A fork copies only this thread; the others' locks would be
            # held forever in every session
            return _isolated.STATUS_ERROR, (Atom('threads'), names)
        return _isolated.STATUS_OK, Atom('ok')

    # SIGCHLD wakes the select loop through this pipe
    rpipe, wpipe = os.pipe()
    os.set_blocking(wpipe, False)
    os.set_blocking(rpipe, False)
    signal.set_wakeup_fd(wpipe)
    signal.signal(signal.SIGCHLD, lambda *_: None)

    class LogStream(io.TextIOBase):
        """sys.stdout / sys.stderr of a session: each line is logged by
        the session's context in Erlang."""

        def __init__(self, level):
            self._level = Atom(level)
            self._buf = ''

        def writable(self):
            return True

        def write(self, text):
            self._buf += text
            while '\n' in self._buf:
                line, self._buf = self._buf.split('\n', 1)
                self._emit(line)
            return len(text)

        def flush(self):
            if self._buf:
                line, self._buf = self._buf, ''
                self._emit(line)

        def _emit(self, line):
            try:
                runtime.event((Atom('log'), self._level, line))
            except Exception:
                pass

    def detach_stdio(runtime):
        """fds 0-2 are the template's port pipes: a session holding them
        would keep the port open after the zygote exits. Point them at
        /dev/null and send Python-level output to Erlang instead."""
        null = os.open(os.devnull, os.O_RDWR)
        for fd in (0, 1, 2):
            os.dup2(null, fd)
        os.close(null)
        sys.stdin = open(os.devnull)
        sys.stdout = LogStream('info')
        sys.stderr = LogStream('warning')

    def session(path, context_pid, opts, frame_fds):
        """Runs in the forked child; never returns."""
        try:
            signal.set_wakeup_fd(-1)
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            for fd in frame_fds:
                try:
                    os.close(fd)
                except OSError:
                    pass
            os.setsid()
            limits = {_isolated._as_text(k): v
                      for k, v in _isolated._as_dict(opts.get(Atom('rlimits'))).items()}
            rlimit_errors = child._apply_rlimits(limits)
            cgroup = opts.get(Atom('cgroup'))
            cgroup_error = child._join_cgroup(_isolated._as_text(cgroup) if cgroup else None)
            cd = opts.get(Atom('cd'))
            if cd:
                os.chdir(_isolated._as_text(cd))
            try:
                sock = child._connect(_isolated._as_text(path))
            except OSError as exc:
                child._die('cannot connect to %s: %s' % (path, exc))
            runtime.sock = sock
            runtime.context_pid = context_pid
            detach_stdio(runtime)
            child.serve(runtime, limits, rlimit_errors, cgroup_error)
        except BaseException:
            traceback.print_exc()
        finally:
            os._exit(0)

    def fork(term):
        _, path, context_pid, opts = term
        names = thread_names()
        if names:
            return _isolated.STATUS_ERROR, (Atom('threads'), names)
        sys.stdout.flush()
        sys.stderr.flush()
        pid = os.fork()
        if pid == 0:
            session(path, context_pid, opts if isinstance(opts, dict) else {},
                    [ctrl.fileno(), rpipe, wpipe, sel_fd])
        return _isolated.STATUS_OK, pid

    def reap():
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return
            event((Atom('exited'), pid, os.waitstatus_to_exitcode(status)))

    def handle(frame_id, status, term):
        tag = term[0] if isinstance(term, tuple) else term
        try:
            if tag == 'init':
                reply = init(term)
            elif tag == 'fork':
                reply = fork(term)
            else:
                reply = _isolated.STATUS_ERROR, (Atom('unknown_request'), tag)
        except BaseException as exc:
            reply = _isolated.STATUS_ERROR, _isolated._exc_term(exc)
        send(frame_id, reply[0], reply[1])

    info = {
        Atom('os_pid'): os.getpid(),
        Atom('python_version'): '%d.%d.%d' % sys.version_info[:3],
        Atom('executable'): sys.executable,
        Atom('platform'): sys.platform,
        Atom('hash_seed'): os.environ.get('PYTHONHASHSEED', 'random'),
    }
    event((Atom('ready'), info))

    sel = selectors.DefaultSelector()
    sel_fd = sel.fileno() if hasattr(sel, 'fileno') else -1
    sel.register(ctrl, selectors.EVENT_READ, 'ctrl')
    sel.register(rpipe, selectors.EVENT_READ, 'chld')
    buf = bytearray()
    while True:
        for key, _ in sel.select():
            if key.data == 'chld':
                try:
                    os.read(rpipe, 4096)
                except BlockingIOError:
                    pass
                reap()
                continue
            data = ctrl.recv(1024 * 1024)
            if not data:
                # Erlang closed the template: sessions keep their own
                # sockets and outlive us
                os._exit(0)
            buf += data
            while len(buf) >= header.size:
                frame_id, length = header.unpack_from(buf)
                if len(buf) < header.size + length:
                    break
                body = bytes(buf[header.size:header.size + length])
                del buf[:header.size + length]
                handle(frame_id, body[0], _etf.decode(body[1:]) if len(body) > 1 else None)


if __name__ == '__main__':
    main(sys.argv)
