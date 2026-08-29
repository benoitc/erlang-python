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
Runtime of an `isolated` context: the far end of the socket that
src/py_isolated.erl talks to.

Wire format, shared with the blocking callback pipe of the embedded modes:

    <<Id:64/native, Len:32/native, Body:Len/binary>>
    Body = <<Status:8, Payload/binary>>   Payload is ETF unless noted

    Status 0  request, Erlang -> child      {call,M,F,A,K} | {eval,Code,Locals}
                                           | {exec,Code} | ping | shutdown
                                           | start_loop | stop_loop
                                           | {submit,Ref,M,F,A,K} | pass_fd
    Status 1  error reply (either way)      reason term
    Status 2  ok reply (either way)         value term
    Status 3  request, child -> Erlang      {call,Name,Args} | {send,Pid,Msg}
                                           | {whereis,Name}
    Status 4  event, child -> Erlang        {ready,Info} | {startup_error,R}
                                           | {async_result,Ref,R}
                                           | {loop_exit,R} | {log,Level,Msg}
    Status 5  control, Erlang -> child      interrupt (handled on the reader
                                           thread, never queued)

Threads:
  * the main thread executes requests, one at a time, and owns the asyncio
    loop; while it waits for the reply to an erlang.call it keeps serving
    requests, so a callback may call back into this context (nesting);
  * the reader thread owns socket reads. It routes replies to whoever is
    waiting (any Python thread may use erlang.call/send/whereis), queues
    requests for the main thread, handles `interrupt` by signalling the main
    thread, and exits the process on EOF so a BEAM death never leaves an
    orphan, even if the main thread is stuck in a C call.
"""

import asyncio
import inspect
import os
import queue
import signal
import socket
import struct
import sys
import threading
import traceback

from . import _etf
from ._etf import Atom, Pid, Ref, Port, DecodeError

__all__ = ['Runtime', 'install_erlang_module']

STATUS_REQUEST = 0
STATUS_ERROR = 1
STATUS_OK = 2
STATUS_CALLBACK = 3
STATUS_EVENT = 4
STATUS_CONTROL = 5

_HEADER = struct.Struct('=QI')   # native byte order, no padding
_HEADER_LEN = _HEADER.size

_INTERRUPT_SIGNAL = signal.SIGUSR1


class ProcessError(Exception):
    """Raised by erlang.send when the target process does not exist."""


class SuspensionRequired(BaseException):
    """Kept for source compatibility with the embedded erlang module. It is
    never raised in isolated mode: callbacks are real socket round-trips."""


class PipeBroken(RuntimeError):
    """The control socket to Erlang is gone."""


class _Interrupted(KeyboardInterrupt):
    """KeyboardInterrupt raised by the interrupt signal handler. A subclass so
    user code catching KeyboardInterrupt keeps working, while the dispatcher
    can tell an Erlang interrupt from a stray Ctrl-C."""


class _Waiter:
    """Reply slot for a request this process sent to Erlang."""

    __slots__ = ('event', 'result', 'inbox', 'future', 'loop')

    def __init__(self, inbox=None, future=None, loop=None):
        self.event = None if (inbox is not None or future is not None) else threading.Event()
        self.result = None
        self.inbox = inbox
        self.future = future
        self.loop = loop

    def deliver(self, result):
        self.result = result
        if self.inbox is not None:
            self.inbox.put(('reply', self))
        elif self.future is not None:
            try:
                self.loop.call_soon_threadsafe(_resolve_future, self.future, result)
            except RuntimeError:
                pass   # the user's loop is closed: nobody is waiting
        else:
            self.event.set()


def _resolve_future(future, result):
    if future.cancelled():
        return
    status, value = result
    if status == STATUS_OK:
        future.set_result(value)
    else:
        future.set_exception(_callback_error(value))


def _callback_error(reason):
    if isinstance(reason, str):
        return RuntimeError(reason)
    if isinstance(reason, tuple) and len(reason) == 2 and reason[0] == 'noproc':
        return ProcessError('process %r does not exist' % (reason[1],))
    return RuntimeError('erlang call failed: %r' % (reason,))


class Runtime:
    """One per child process."""

    def __init__(self, sock, context_pid=None):
        self.sock = sock
        self.context_pid = context_pid
        self._wlock = threading.Lock()
        self._idlock = threading.Lock()
        self._next_id = 1
        self._pending = {}
        self._plock = threading.Lock()
        self.inbox = queue.SimpleQueue()
        self.broken = False
        self.broken_reason = None
        self.globals = {'__name__': '__main__', '__builtins__': __builtins__}
        self.running = False           # a request is executing on main thread
        self.loop = None
        self.loop_running = False
        self._received_fds = []
        self._fds_lock = threading.Lock()
        self._cancelled = set()          # request ids Erlang gave up on
        self._cancel_lock = threading.Lock()
        self._exec_stack = []            # main thread: ids being executed (nesting)
        self.main_thread = threading.main_thread()
        self._reader = None

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        signal.signal(_INTERRUPT_SIGNAL, self._on_interrupt_signal)
        self._reader = threading.Thread(target=self._reader_main,
                                        name='erlang-reader', daemon=True)
        self._reader.start()

    def get_loop(self):
        if self.loop is None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        return self.loop

    # -- signals -----------------------------------------------------------

    def _on_interrupt_signal(self, signum, frame):
        # Only a request in flight can be interrupted: an interrupt that
        # lands between two requests is dropped, so it can never leak into
        # the next one.
        if self.running:
            raise _Interrupted()

    def _signal_main(self):
        try:
            signal.pthread_kill(self.main_thread.ident, _INTERRUPT_SIGNAL)
        except Exception:
            pass

    # -- writing -----------------------------------------------------------

    def _write_frame(self, frame_id, status, payload):
        body = bytes([status]) + payload
        data = _HEADER.pack(frame_id, len(body)) + body
        # A signal landing inside sendall would tear the frame and
        # desynchronise the stream: hold it until the write is complete.
        on_main = threading.current_thread() is self.main_thread
        if on_main:
            signal.pthread_sigmask(signal.SIG_BLOCK, {_INTERRUPT_SIGNAL})
        try:
            with self._wlock:
                if self.broken:
                    raise PipeBroken(self.broken_reason or 'socket to Erlang is closed')
                try:
                    self.sock.sendall(data)
                except OSError as exc:
                    self._mark_broken('write failed: %s' % exc)
                    raise PipeBroken(self.broken_reason) from None
                except BaseException:
                    # Anything else escaping mid-write leaves a torn frame
                    self._mark_broken('write interrupted')
                    raise
        finally:
            if on_main:
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {_INTERRUPT_SIGNAL})

    def reply(self, frame_id, status, value):
        self._write_frame(frame_id, status, _etf.encode(value))

    def event(self, term):
        self._write_frame(0, STATUS_EVENT, _etf.encode(term))

    def _alloc_id(self):
        with self._idlock:
            n = self._next_id
            self._next_id = n + 1
            return n

    # -- child -> Erlang requests -------------------------------------------

    def request(self, term, timeout=None):
        """Send a status-3 request and wait for its reply.

        On the main thread the wait also serves requests coming from Erlang,
        so nested calls work. Returns (status, value)."""
        if self.broken:
            raise PipeBroken(self.broken_reason)
        on_main = threading.current_thread() is self.main_thread
        waiter = _Waiter(inbox=self.inbox if on_main else None)
        frame_id = self._alloc_id()
        with self._plock:
            self._pending[frame_id] = waiter
        try:
            self._write_frame(frame_id, STATUS_CALLBACK, _etf.encode(term))
        except PipeBroken:
            with self._plock:
                self._pending.pop(frame_id, None)
            raise
        if on_main:
            return self._wait_on_main(waiter)
        if not waiter.event.wait(timeout):
            with self._plock:
                self._pending.pop(frame_id, None)
            raise TimeoutError('no reply from Erlang')
        return waiter.result

    def request_async(self, term):
        """Send a status-3 request; returns an asyncio Future for the reply."""
        if self.broken:
            raise PipeBroken(self.broken_reason)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        waiter = _Waiter(future=future, loop=loop)
        frame_id = self._alloc_id()
        with self._plock:
            self._pending[frame_id] = waiter
        try:
            self._write_frame(frame_id, STATUS_CALLBACK, _etf.encode(term))
        except PipeBroken:
            with self._plock:
                self._pending.pop(frame_id, None)
            raise
        return future

    def _wait_on_main(self, waiter):
        while True:
            kind, item = self.inbox.get()
            if kind == 'reply':
                if item is waiter:
                    return waiter.result
                # stale reply for an interrupted wait: drop
                continue
            if kind == 'request':
                self._serve(*item)
            elif kind == 'broken':
                raise PipeBroken(item)
            elif kind == 'interrupt':
                if self._exec_stack and self._exec_stack[-1] == item:
                    raise _Interrupted()
                # stale: for a request that already finished

    # -- reader thread -----------------------------------------------------

    def _reader_main(self):
        try:
            self._read_loop()
        except Exception as exc:   # never leave silently
            self._mark_broken('reader failed: %r' % (exc,))
        # EOF or error: Erlang is gone (or closed us on purpose). Nothing
        # useful can happen in this process any more. _exit so a main thread
        # stuck in a C call cannot keep the process alive.
        os._exit(0)

    def _read_loop(self):
        sock = self.sock
        buf = bytearray()
        need_hdr = _HEADER_LEN
        while True:
            try:
                data, fds, _flags, _addr = socket.recv_fds(sock, 1024 * 1024, 16)
            except InterruptedError:
                continue
            except OSError as exc:
                self._mark_broken('read failed: %s' % exc)
                return
            if fds:
                with self._fds_lock:
                    self._received_fds.extend(fds)
            if not data:
                self._mark_broken('Erlang closed the socket')
                return
            buf += data
            # Resumable parse: header, then body, buffering partial frames
            while True:
                if len(buf) < need_hdr:
                    break
                frame_id, body_len = _HEADER.unpack_from(buf, 0)
                total = _HEADER_LEN + body_len
                if len(buf) < total:
                    break
                body = bytes(buf[_HEADER_LEN:total])
                del buf[:total]
                self._on_frame(frame_id, body)

    def _on_frame(self, frame_id, body):
        if not body:
            self._mark_broken('empty frame')
            return
        status = body[0]
        try:
            term = _etf.decode(body[1:]) if len(body) > 1 else None
        except (DecodeError, struct.error, IndexError) as exc:
            if status == STATUS_REQUEST:
                self.reply(frame_id, STATUS_ERROR,
                           (Atom('bad_request'), 'malformed frame: %s' % exc))
                return
            self._mark_broken('malformed frame from Erlang: %s' % exc)
            return
        if status in (STATUS_OK, STATUS_ERROR):
            with self._plock:
                waiter = self._pending.pop(frame_id, None)
            if waiter is not None:
                waiter.deliver((status, term))
        elif status == STATUS_CONTROL:
            self._on_control(term)
        elif status == STATUS_REQUEST:
            self._on_request(frame_id, term)
        else:
            self._mark_broken('unexpected status %d from Erlang' % status)

    def _on_control(self, term):
        if term == 'interrupt':
            self._signal_main()
        elif isinstance(term, tuple) and len(term) == 2 and term[0] == 'interrupt':
            target = term[1]
            stack = list(self._exec_stack)
            if target == 'loop':
                if self.loop_running:
                    self._signal_main()
            elif stack and stack[-1] == target:
                self._signal_main()
            elif target in stack:
                # An outer request blocked in a callback wait: its wait
                # raises when the nested request finishes
                self.inbox.put(('interrupt', target))
            # else: already finished (or still queued: cancel handles that)
        elif isinstance(term, tuple) and len(term) == 2 and term[0] == 'cancel':
            with self._cancel_lock:
                self._cancelled.add(term[1])
                # A cancel that arrives after its request ran stays behind;
                # keep the set bounded (ids only grow, oldest are stalest)
                if len(self._cancelled) > 1024:
                    for old in sorted(self._cancelled)[:512]:
                        self._cancelled.discard(old)
        elif term == 'stop_loop':
            loop = self.loop
            if loop is not None and self.loop_running:
                loop.call_soon_threadsafe(loop.stop)

    def _on_request(self, frame_id, term):
        """Requests that must not wait for the main thread are handled here;
        everything else is queued for it."""
        tag = term[0] if isinstance(term, tuple) else term
        if tag == 'ping':
            self.reply(frame_id, STATUS_OK, Atom('pong'))
        elif tag == 'submit':
            self._on_submit(frame_id, term)
        elif tag == 'stop_loop':
            loop = self.loop
            if loop is not None and self.loop_running:
                loop.call_soon_threadsafe(loop.stop)
                self.reply(frame_id, STATUS_OK, Atom('ok'))
            else:
                self.reply(frame_id, STATUS_ERROR, Atom('no_loop'))
        elif self.loop_running and tag != 'shutdown':
            # The main thread is inside run_forever: run the request as a
            # loop callback so it does not wait for the loop to end.
            self.loop.call_soon_threadsafe(self._serve, frame_id, term)
        else:
            self.inbox.put(('request', (frame_id, term)))

    def _on_submit(self, frame_id, term):
        _, task_ref, module, func, args, kwargs = term
        loop = self.loop
        if loop is None or not self.loop_running:
            self.reply(frame_id, STATUS_ERROR, Atom('no_loop'))
            return
        self.reply(frame_id, STATUS_OK, Atom('ok'))

        def schedule():
            try:
                fn = _resolve(module, func)
                result = fn(*_as_list(args), **_as_dict(kwargs))
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    task.add_done_callback(
                        lambda t: self._report_task(task_ref, t))
                    return
                self.event((Atom('async_result'), task_ref, (Atom('ok'), result)))
            except BaseException as exc:
                self.event((Atom('async_result'), task_ref,
                            (Atom('error'), _exc_term(exc))))
        loop.call_soon_threadsafe(schedule)

    def _report_task(self, task_ref, task):
        try:
            if task.cancelled():
                result = (Atom('error'), Atom('cancelled'))
            elif task.exception() is not None:
                result = (Atom('error'), _exc_term(task.exception()))
            else:
                result = (Atom('ok'), task.result())
            self.event((Atom('async_result'), task_ref, result))
        except PipeBroken:
            pass

    def _mark_broken(self, reason):
        if self.broken:
            return
        self.broken = True
        self.broken_reason = reason
        with self._plock:
            pending = list(self._pending.values())
            self._pending.clear()
        for waiter in pending:
            waiter.deliver((STATUS_ERROR, reason))
        self.inbox.put(('broken', reason))

    # -- main thread -------------------------------------------------------

    def serve_forever(self):
        """Main-thread request loop. Returns when Erlang asks for shutdown."""
        while True:
            try:
                kind, item = self.inbox.get()
            except _Interrupted:
                continue   # interrupt raced with the end of a request
            if kind == 'request':
                if self._serve(*item) == 'shutdown':
                    return
            elif kind == 'broken':
                return
            # stale replies are dropped

    def _serve(self, frame_id, term):
        """Execute one Erlang request and reply. Returns 'shutdown' when the
        child should exit."""
        with self._cancel_lock:
            if frame_id in self._cancelled:
                self._cancelled.discard(frame_id)
                return None    # the caller timed out while this was queued
        tag = term[0] if isinstance(term, tuple) else term
        if tag == 'shutdown':
            try:
                self.reply(frame_id, STATUS_OK, Atom('ok'))
            except PipeBroken:
                pass
            return 'shutdown'
        if tag == 'start_loop':
            return self._run_loop(frame_id)
        if tag == 'pass_fd':
            with self._fds_lock:
                fd = self._received_fds.pop(0) if self._received_fds else None
            if fd is None:
                self.reply(frame_id, STATUS_ERROR, Atom('no_fd_received'))
            else:
                self.reply(frame_id, STATUS_OK, fd)
            return None

        self._exec_stack.append(frame_id)
        try:
            status, value = self._execute(tag, term)
        finally:
            self._exec_stack.pop()
        try:
            self.reply(frame_id, status, value)
        except PipeBroken:
            pass
        except KeyboardInterrupt:
            # running is False here so the handler does not raise; a
            # stray Ctrl-C style interrupt must still not lose the reply
            try:
                self.reply(frame_id, status, value)
            except (PipeBroken, KeyboardInterrupt):
                pass
        return None

    def _execute(self, tag, term):
        """Run call/eval/exec with interrupt handling. The handler raises only
        while `running` is set, and a late signal is absorbed by the retry so
        the reply is always sent."""
        prev = self.running
        result = None
        while result is None:
            self.running = True
            try:
                result = STATUS_OK, self._dispatch(tag, term)
            except KeyboardInterrupt:          # includes _Interrupted
                result = STATUS_ERROR, Atom('interrupted')
            except PipeBroken as exc:
                result = STATUS_ERROR, (Atom('pipe_broken'), str(exc))
            except StopIteration:
                result = STATUS_ERROR, (Atom('StopIteration'), None)
            except (SystemExit, GeneratorExit):
                self.running = prev
                raise
            except BaseException as exc:
                result = STATUS_ERROR, _exc_term(exc)
            finally:
                # Cleared first thing so a second signal landing in the
                # bookkeeping above is dropped by the handler, not raised
                self.running = False
        self.running = prev
        return result

    def _dispatch(self, tag, term):
        if tag == 'init':
            return self._init(term)
        if tag == 'call':
            _, module, func, args, kwargs = term
            fn = _resolve(module, func, self.globals)
            result = fn(*_as_list(args), **_as_dict(kwargs))
        elif tag == 'eval':
            _, code, locals_ = term
            loc = dict(self.globals)
            loc.update(_as_dict(locals_))
            result = eval(compile(_as_text(code), '<erlang>', 'eval'), self.globals, loc)
        elif tag == 'exec':
            _, code = term
            exec(compile(_as_text(code), '<erlang>', 'exec'), self.globals)
            return Atom('ok')
        else:
            raise RuntimeError('unknown request %r' % (tag,))
        if inspect.isawaitable(result):
            result = self.get_loop().run_until_complete(result)
        return result

    def _init(self, term):
        _, context_pid, paths, imports = term
        self.context_pid = context_pid
        for path in reversed(_as_list(paths)):
            path = _as_text(path)
            if path not in sys.path:
                sys.path.insert(0, path)
        import importlib
        for name in _as_list(imports):
            importlib.import_module(_as_text(name))
        return Atom('ok')

    def _run_loop(self, frame_id):
        loop = self.get_loop()
        self.loop_running = True
        self.reply(frame_id, STATUS_OK, Atom('ok'))
        self.running = True
        result = Atom('ok')
        self._exec_stack.append('loop')
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            self.running = False
            result = (Atom('error'), Atom('interrupted'))
        except BaseException as exc:
            self.running = False
            result = (Atom('error'), _exc_term(exc))
        finally:
            self._exec_stack.pop()
            self.running = False
            self.loop_running = False
            # 10: submits scheduled between loop.stop() and here sit in the
            # ready queue; run them into tasks, then cancel everything so
            # each reports {error, cancelled} instead of vanishing
            _drain_and_cancel(loop)
        try:
            self.event((Atom('loop_exit'), result))
        except PipeBroken:
            pass
        return None


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _as_list(v):
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    return [v]


def _as_dict(v):
    if v is None:
        return {}
    if isinstance(v, dict):
        return {(k if isinstance(k, str) else str(k)): val for k, val in v.items()}
    return {}


def _as_text(code):
    if isinstance(code, bytes):
        return code.decode('utf-8')
    if isinstance(code, list):   # Erlang charlist
        return ''.join(chr(c) for c in code)
    return code


def _resolve(module, func, globals_=None):
    module = _as_text(module)
    func = _as_text(func)
    if module in ('__main__', '') and globals_ is not None and func in globals_:
        return globals_[func]
    import importlib
    if module == '__main__' and globals_ is not None:
        raise AttributeError("name '%s' is not defined in the context" % func)
    mod = importlib.import_module(module)
    try:
        return getattr(mod, func)
    except AttributeError:
        raise AttributeError("module '%s' has no attribute '%s'" % (module, func)) from None


def _exc_term(exc):
    if isinstance(exc, KeyboardInterrupt):
        return Atom('interrupted')
    try:
        msg = str(exc)
    except Exception:
        msg = 'unknown'
    return (Atom(type(exc).__name__), msg)


def _drain_and_cancel(loop):
    for _ in range(3):
        try:
            loop.run_until_complete(asyncio.sleep(0))
        except BaseException:
            break
        _cancel_all_tasks(loop)


def _cancel_all_tasks(loop):
    try:
        tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
    except RuntimeError:
        return
    for t in tasks:
        t.cancel()
    if tasks:
        try:
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        except BaseException:
            pass


# ---------------------------------------------------------------------------
# The `erlang` module seen by user code
# ---------------------------------------------------------------------------

def install_erlang_module(runtime):
    """Build the `erlang` module for this child and register it in
    sys.modules. Mirrors the embedded module's public surface; anything that
    cannot cross a process boundary raises a clear RuntimeError."""
    import types
    mod = types.ModuleType('erlang', __doc__)
    rt = runtime

    def _not_supported(name):
        def fn(*args, **kwargs):
            raise RuntimeError(
                '%s is not available in isolated mode (it needs the embedded '
                'interpreter); use erlang.call/erlang.send instead' % name)
        fn.__name__ = name
        return fn

    def call(name, *args, **kwargs):
        if kwargs:
            raise TypeError('erlang.call takes positional arguments only')
        status, value = rt.request((Atom('call'), _as_text(name), list(args)))
        if status == STATUS_OK:
            return value
        raise _callback_error(value)

    async def async_call(name, *args):
        return await rt.request_async((Atom('call'), _as_text(name), list(args)))

    def send(pid, message):
        if not isinstance(pid, Pid):
            raise TypeError('erlang.send: pid must be an erlang.Pid, got %s'
                            % type(pid).__name__)
        status, value = rt.request((Atom('send'), pid, message))
        if status != STATUS_OK:
            raise _callback_error(value)
        return None

    def whereis(name):
        status, value = rt.request((Atom('whereis'), Atom(_as_text(name))))
        if status != STATUS_OK:
            raise _callback_error(value)
        return value

    def self_():
        return rt.context_pid

    def atom(name):
        return Atom(name)

    def is_isolated():
        return True

    def run(main, *, debug=None):
        loop = rt.get_loop()
        if debug is not None:
            loop.set_debug(debug)
        return loop.run_until_complete(main)

    def new_event_loop():
        return asyncio.new_event_loop()

    def get_event_loop_policy():
        return asyncio.get_event_loop_policy()

    def install(*, silent=False):
        return None

    def spawn_task(coro, *, name=None):
        loop = rt.get_loop()
        return loop.create_task(coro, name=name)

    def sleep(seconds):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            import time
            time.sleep(seconds)
            return None
        return asyncio.sleep(seconds)

    def log(level, message):
        rt.event((Atom('log'), Atom(_as_text(level)), str(message)))

    class Function:
        __slots__ = ('name',)

        def __init__(self, name):
            self.name = name

        def __call__(self, *args):
            return call(self.name, *args)

        def __repr__(self):
            return '<erlang.Function %s>' % self.name

    def __getattr__(name):
        if name.startswith('_'):
            raise AttributeError(name)
        return Function(name)

    from . import _server as server

    ns = dict(
        call=call, async_call=async_call, send=send, whereis=whereis,
        self=self_, atom=atom, Atom=Atom, Pid=Pid, Ref=Ref, Port=Port,
        ProcessError=ProcessError, SuspensionRequired=SuspensionRequired,
        Function=Function, is_isolated=is_isolated, run=run,
        new_event_loop=new_event_loop, get_event_loop_policy=get_event_loop_policy,
        install=install, spawn_task=spawn_task, sleep=sleep, log=log,
        server=server, __getattr__=__getattr__,
        schedule=_not_supported('erlang.schedule'),
        schedule_py=_not_supported('erlang.schedule_py'),
        schedule_inline=_not_supported('erlang.schedule_inline'),
        consume_time_slice=lambda *_a, **_k: False,
        channel=_not_supported('erlang.channel'),
        byte_channel=_not_supported('erlang.byte_channel'),
        reactor=_not_supported('erlang.reactor'),
        shared_dict=_not_supported('erlang.shared_dict'),
        Channel=_not_supported('erlang.Channel'),
        ByteChannel=_not_supported('erlang.ByteChannel'),
        __all__=['call', 'async_call', 'send', 'whereis', 'self', 'atom',
                 'Atom', 'Pid', 'Ref', 'ProcessError', 'SuspensionRequired',
                 'run', 'sleep', 'spawn_task', 'server', 'is_isolated'],
    )
    mod.__dict__.update(ns)
    sys.modules['erlang'] = mod
    return mod


def format_exception(exc):
    return ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
