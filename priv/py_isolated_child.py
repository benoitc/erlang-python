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
Entry point of an `isolated` context child process.

Started by src/py_isolated.erl as

    python3 py_isolated_child.py SOCKET_PATH [--rlimit-as BYTES]
        [--rlimit-cpu SECONDS] [--rlimit-nofile N] [--cgroup DIR]

Order of operations matters: limits are applied before anything else is
imported, the parent-death signal is armed before the socket connects, and
the ready/startup_error event is the first frame Erlang sees.
"""

import os
import sys


def _die(reason):
    sys.stderr.write('py_isolated_child: %s\n' % reason)
    sys.stderr.flush()
    os._exit(3)


def _parse_args(argv):
    if len(argv) < 2:
        _die('usage: py_isolated_child.py SOCKET_PATH [options]')
    opts = {'socket': argv[1], 'rlimits': {}, 'cgroup': None, 'caps': None}
    i = 2
    while i < len(argv):
        flag = argv[i]
        if flag in ('--rlimit-as', '--rlimit-cpu', '--rlimit-nofile'):
            opts['rlimits'][flag[len('--rlimit-'):]] = int(argv[i + 1])
            i += 2
        elif flag == '--cgroup':
            opts['cgroup'] = argv[i + 1]
            i += 2
        elif flag == '--caps-json':
            import json
            try:
                opts['caps'] = json.loads(argv[i + 1])
            except ValueError as exc:
                _die('bad --caps-json: %s' % exc)
            i += 2
        else:
            _die('unknown option %s' % flag)
    return opts


def _arm_parent_death():
    """Get SIGKILL when the parent (the BEAM) dies: prctl on Linux, procctl
    on FreeBSD. Elsewhere the reader thread's EOF handling covers it."""
    SIGKILL = 9
    try:
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        if sys.platform.startswith('linux'):
            PR_SET_PDEATHSIG = 1
            libc.prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0)
        elif sys.platform.startswith('freebsd'):
            # procctl(P_PID, 0, PROC_PDEATHSIG_CTL, &sig) (FreeBSD 11.2+)
            P_PID = 0
            PROC_PDEATHSIG_CTL = 11
            sig = ctypes.c_int(SIGKILL)
            libc.procctl.argtypes = [ctypes.c_int, ctypes.c_int64, ctypes.c_int, ctypes.c_void_p]
            libc.procctl(P_PID, 0, PROC_PDEATHSIG_CTL, ctypes.byref(sig))
        else:
            return
        # The parent may already be gone between fork and the call
        if os.getppid() == 1:
            os._exit(0)
    except Exception:
        pass


# macOS does not enforce RLIMIT_AS: `as` is enforced by a watchdog thread
# there (see _start_memory_watchdog) instead of setrlimit.
_AS_VIA_WATCHDOG = sys.platform == 'darwin'


def _apply_rlimits(limits):
    if not limits:
        return []
    import resource
    if _AS_VIA_WATCHDOG:
        limits = {k: v for k, v in limits.items() if k != 'as'}
    names = {
        'as': getattr(resource, 'RLIMIT_AS', None),
        'cpu': getattr(resource, 'RLIMIT_CPU', None),
        'nofile': getattr(resource, 'RLIMIT_NOFILE', None),
    }
    errors = []
    for key, value in limits.items():
        res = names.get(key)
        if res is None:
            errors.append((key, 'not supported on this platform'))
            continue
        try:
            _soft, hard = resource.getrlimit(res)
            if hard != resource.RLIM_INFINITY and hard < value:
                value = hard   # cannot raise a hard limit, clamp to it
            resource.setrlimit(res, (value, hard))
        except (ValueError, OSError) as exc:
            errors.append((key, str(exc)))
    return errors


def _start_memory_watchdog(limit, runtime):
    """Portable memory bound: poll this process's resident set every 50 ms
    and exit when it passes `limit`. Erlang reports the in-flight call as
    {error, {child_exited, {memory_limit, Bytes}}}."""
    import resource
    import threading
    from _erlang_impl._etf import Atom

    def rss():
        r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return r if sys.platform == 'darwin' else r * 1024

    def watch():
        while True:
            used = rss()
            if used > limit:
                try:
                    runtime.event((Atom('memory_limit'), used))
                except Exception:
                    pass
                os._exit(3)
            threading.Event().wait(0.05)

    t = threading.Thread(target=watch, name='erlang-memory-watchdog', daemon=True)
    t.start()


def _join_cgroup(path):
    """cgroup v2, best effort: the directory is created by the operator (or
    by Erlang) with the limits already written; we only join it."""
    if not path:
        return None
    if not sys.platform.startswith('linux'):
        return 'cgroups are Linux only (platform %s); use rlimits' % sys.platform
    try:
        with open(os.path.join(path, 'cgroup.procs'), 'w') as f:
            f.write(str(os.getpid()))
        return None
    except OSError as exc:
        return str(exc)


def _connect(path):
    import socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    # Default Unix socket buffers are small (8 KB on macOS); the kernel
    # clamps to its maximum, so this is best effort.
    for opt in (socket.SO_SNDBUF, socket.SO_RCVBUF):
        try:
            sock.setsockopt(socket.SOL_SOCKET, opt, 1024 * 1024)
        except OSError:
            pass
    return sock


def _caps_summary():
    """What was granted, so `py_context:child_info/1` can report it."""
    try:
        from _erlang_impl import _caps
        return _caps.grants()
    except Exception:
        return None


def main(argv):
    opts = _parse_args(argv)
    _arm_parent_death()
    rlimit_errors = _apply_rlimits(opts['rlimits'])
    cgroup_error = _join_cgroup(opts['cgroup'])

    priv = os.path.dirname(os.path.abspath(__file__))
    if priv not in sys.path:
        sys.path.insert(0, priv)

    try:
        sock = _connect(opts['socket'])
    except OSError as exc:
        _die('cannot connect to %s: %s' % (opts['socket'], exc))

    from _erlang_impl import _isolated
    from _erlang_impl._etf import Atom

    runtime = _isolated.Runtime(sock)
    _isolated.install_erlang_module(runtime)
    runtime.start()
    if _AS_VIA_WATCHDOG and 'as' in opts['rlimits']:
        _start_memory_watchdog(opts['rlimits']['as'], runtime)

    # Last thing before the parent is told this child is ready, so the
    # runtime's own imports are not subject to the grants and everything
    # that runs afterwards is: the registered imports, the preload, and
    # every request.
    caps_errors = []
    if opts['caps'] is not None:
        from _erlang_impl import _caps
        caps_errors = _caps.install(opts['caps'])

    if rlimit_errors or cgroup_error or caps_errors:
        problems = [(Atom('rlimit'), Atom(k), msg) for k, msg in rlimit_errors]
        if cgroup_error:
            problems.append((Atom('cgroup'), cgroup_error))
        problems += [(Atom('caps'), msg) for msg in caps_errors]
        try:
            runtime.event((Atom('startup_error'), problems))
        finally:
            os._exit(2)

    info = {
        Atom('os_pid'): os.getpid(),
        Atom('python_version'): '%d.%d.%d' % sys.version_info[:3],
        Atom('executable'): sys.executable,
        Atom('platform'): sys.platform,
        Atom('caps'): _caps_summary(),
    }
    runtime.event((Atom('ready'), info))

    try:
        runtime.serve_forever()
    finally:
        try:
            sock.close()
        except OSError:
            pass
    os._exit(0)


if __name__ == '__main__':
    main(sys.argv)
