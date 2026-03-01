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
Erlang-native asyncio event loop implementation.

This module provides an asyncio event loop backed by Erlang's scheduler
using enif_select for I/O multiplexing.

For the new uvloop-compatible API, use the 'erlang' package:

    import erlang
    erlang.run(main())

This module provides backward compatibility with the original API.
"""

import asyncio
import errno
import heapq
import os
import socket
import ssl
import sys
import threading
import time
import warnings
from asyncio import events, futures, tasks, transports
from collections import deque

__all__ = [
    'ErlangEventLoop',
    'get_event_loop_policy',
    '_ErlangSocketTransport',
    '_ErlangDatagramTransport',
    '_ErlangServer',
    '_run_and_send',
]

# Event type constants (match C enum values for fast integer comparison)
EVENT_TYPE_READ = 1
EVENT_TYPE_WRITE = 2
EVENT_TYPE_TIMER = 3


class ErlangEventLoop(asyncio.AbstractEventLoop):
    """asyncio event loop backed by Erlang's scheduler.

    This event loop implementation delegates I/O multiplexing to Erlang
    via enif_select, providing:

    - Sub-millisecond latency (vs 10ms polling)
    - Zero CPU usage when idle
    - Full GIL release during waits
    - Native Erlang scheduler integration

    The loop works by:
    1. add_reader/add_writer register fds with enif_select
    2. call_later creates timers via erlang:send_after
    3. _run_once waits for events (GIL released in C)
    4. Callbacks are dispatched when events occur
    """

    # Use __slots__ for faster attribute access and reduced memory
    __slots__ = (
        '_pel',
        '_readers', '_writers', '_readers_by_cid', '_writers_by_cid',
        '_timers', '_timer_refs', '_timer_heap', '_handle_to_callback_id',
        '_ready', '_callback_id',
        '_handle_pool', '_handle_pool_max', '_running', '_stopping', '_closed',
        '_thread_id', '_clock_resolution', '_exception_handler', '_current_handle',
        '_debug', '_task_factory', '_default_executor',
        # Cached method references for hot paths
        '_ready_append', '_ready_popleft',
    )

    def __init__(self):
        """Initialize the Erlang event loop.

        The event loop is backed by Erlang's scheduler via the py_event_loop
        C module. This module provides direct access to the event loop
        without going through Erlang callbacks.
        """
        try:
            import py_event_loop as pel
            self._pel = pel

            # Check it's initialized
            if not pel._is_initialized():
                raise RuntimeError("Erlang event loop not initialized. "
                                 "Make sure erlang_python application is started.")
        except ImportError:
            # Fallback for testing without actual NIF
            self._pel = _MockNifModule()

        # Callback management
        self._readers = {}  # fd -> (callback, args, callback_id, fd_key)
        self._writers = {}  # fd -> (callback, args, callback_id, fd_key)
        self._readers_by_cid = {}  # callback_id -> fd (reverse map for O(1) lookup)
        self._writers_by_cid = {}  # callback_id -> fd (reverse map for O(1) lookup)
        self._timers = {}   # callback_id -> handle
        self._timer_refs = {}  # callback_id -> timer_ref (for cancellation)
        self._timer_heap = []  # min-heap of (when, callback_id) for O(1) minimum lookup
        self._handle_to_callback_id = {}  # handle -> callback_id (reverse map for O(1) cancellation)
        self._ready = deque()  # Callbacks ready to run
        self._callback_id = 0

        # Cache deque methods for hot path (avoids attribute lookup)
        self._ready_append = self._ready.append
        self._ready_popleft = self._ready.popleft

        # Handle object pool for reduced allocations
        self._handle_pool = []
        self._handle_pool_max = 150

        # State
        self._running = False
        self._stopping = False
        self._closed = False
        self._thread_id = None
        self._clock_resolution = 1e-9  # nanoseconds

        # Exception handling
        self._exception_handler = None
        self._current_handle = None

        # Debug mode
        self._debug = False

        # Task factory
        self._task_factory = None

        # SSL context
        self._default_executor = None

    def _next_id(self):
        """Generate a unique callback ID."""
        self._callback_id += 1
        return self._callback_id

    # ========================================================================
    # Running and stopping the event loop
    # ========================================================================

    def run_forever(self):
        """Run the event loop until stop() is called."""
        self._check_closed()
        self._check_running()
        self._set_coroutine_origin_tracking(self._debug)

        self._thread_id = threading.get_ident()
        self._running = True
        self._stopping = False

        # Register as the running loop so asyncio.get_running_loop() works
        old_running_loop = events._get_running_loop()
        events._set_running_loop(self)
        try:
            while not self._stopping:
                self._run_once()
        finally:
            events._set_running_loop(old_running_loop)
            self._stopping = False
            self._running = False
            self._thread_id = None
            self._set_coroutine_origin_tracking(False)

    def run_until_complete(self, future):
        """Run the event loop until a future is done."""
        self._check_closed()
        self._check_running()

        new_task = not futures.isfuture(future)
        future = tasks.ensure_future(future, loop=self)

        if new_task:
            future._log_destroy_pending = False

        # Use a single callback reference to ensure proper removal
        def _done_callback(f):
            self.stop()

        future.add_done_callback(_done_callback)

        try:
            self.run_forever()
        except Exception:
            if new_task and future.done() and not future.cancelled():
                future.exception()
            raise
        finally:
            future.remove_done_callback(_done_callback)

        if not future.done():
            raise RuntimeError('Event loop stopped before Future completed.')

        return future.result()

    def stop(self):
        """Stop the event loop."""
        self._stopping = True
        # Wake up the event loop if it's waiting
        try:
            self._pel._wakeup()
        except Exception:
            pass

    def is_running(self):
        """Return True if the event loop is running."""
        return self._running

    def is_closed(self):
        """Return True if the event loop is closed."""
        return self._closed

    def close(self):
        """Close the event loop."""
        if self._running:
            raise RuntimeError("Cannot close a running event loop")
        if self._closed:
            return

        self._closed = True

        # Cancel all timers
        for callback_id, handle in list(self._timers.items()):
            handle.cancel()
            timer_ref = self._timer_refs.get(callback_id)
            if timer_ref is not None:
                try:
                    self._pel._cancel_timer(timer_ref)
                except (AttributeError, RuntimeError):
                    pass
        self._timers.clear()
        self._timer_refs.clear()
        self._timer_heap.clear()
        self._handle_to_callback_id.clear()

        # Remove all readers/writers
        for fd in list(self._readers.keys()):
            self.remove_reader(fd)
        for fd in list(self._writers.keys()):
            self.remove_writer(fd)

        # Shutdown default executor
        if self._default_executor is not None:
            self._default_executor.shutdown(wait=False)
            self._default_executor = None

    async def shutdown_asyncgens(self):
        """Shutdown all active asynchronous generators.

        Note: This is a no-op in ErlangEventLoop. Async generators are
        managed by Python's garbage collector. For proper cleanup, ensure
        async generators are explicitly closed or exhausted before loop shutdown.
        """
        # No-op: we don't track async generators to avoid global hook issues
        pass

    async def shutdown_default_executor(self, timeout=None):
        """Shutdown the default executor."""
        if self._default_executor is not None:
            self._default_executor.shutdown(wait=True)
            self._default_executor = None

    # ========================================================================
    # Scheduling callbacks
    # ========================================================================

    def call_soon(self, callback, *args, context=None):
        """Schedule a callback to be called soon."""
        self._check_closed()
        handle = events.Handle(callback, args, self, context)
        self._ready_append(handle)  # Use cached method
        return handle

    def call_soon_threadsafe(self, callback, *args, context=None):
        """Thread-safe version of call_soon."""
        handle = self.call_soon(callback, *args, context=context)
        # Wake up the event loop
        try:
            self._pel._wakeup()
        except Exception:
            pass
        return handle

    def call_later(self, delay, callback, *args, context=None):
        """Schedule a callback to be called after delay seconds."""
        self._check_closed()
        timer = self.call_at(self.time() + delay, callback, *args, context=context)
        return timer

    def call_at(self, when, callback, *args, context=None):
        """Schedule a callback to be called at a specific time."""
        self._check_closed()
        callback_id = self._next_id()

        handle = events.TimerHandle(when, callback, args, self, context)
        self._timers[callback_id] = handle
        self._handle_to_callback_id[id(handle)] = callback_id

        # Push to timer heap for O(1) minimum lookup
        heapq.heappush(self._timer_heap, (when, callback_id))

        # Schedule with Erlang's native timer system
        delay_ms = max(0, int((when - self.time()) * 1000))
        try:
            timer_ref = self._pel._schedule_timer(delay_ms, callback_id)
            self._timer_refs[callback_id] = timer_ref
        except AttributeError:
            pass
        except RuntimeError as e:
            raise RuntimeError(f"Timer scheduling failed: {e}") from e

        return handle

    def time(self):
        """Return the current time according to the event loop's clock."""
        return time.monotonic()

    # ========================================================================
    # Creating Futures and Tasks
    # ========================================================================

    def create_future(self):
        """Create a Future object attached to this loop."""
        return futures.Future(loop=self)

    def create_task(self, coro, *, name=None, context=None):
        """Schedule a coroutine to be executed."""
        self._check_closed()
        if self._task_factory is None:
            if sys.version_info >= (3, 11):
                task = tasks.Task(coro, loop=self, name=name, context=context)
            elif sys.version_info >= (3, 8):
                task = tasks.Task(coro, loop=self, name=name)
            else:
                task = tasks.Task(coro, loop=self)
                if name is not None:
                    task.set_name(name)
        else:
            if sys.version_info >= (3, 11) and context is not None:
                task = self._task_factory(self, coro, context=context)
            else:
                task = self._task_factory(self, coro)
            if name is not None:
                task.set_name(name)
        return task

    def set_task_factory(self, factory):
        """Set a task factory."""
        self._task_factory = factory

    def get_task_factory(self):
        """Return the task factory."""
        return self._task_factory

    # ========================================================================
    # File descriptor callbacks
    # ========================================================================

    def add_reader(self, fd, callback, *args):
        """Register a reader callback for a file descriptor."""
        self._check_closed()
        self.remove_reader(fd)

        callback_id = self._next_id()

        try:
            fd_key = self._pel._add_reader(fd, callback_id)
            self._readers[fd] = (callback, args, callback_id, fd_key)
            self._readers_by_cid[callback_id] = fd
        except Exception as e:
            raise RuntimeError(f"Failed to add reader: {e}")

    def remove_reader(self, fd):
        """Unregister a reader callback for a file descriptor."""
        if fd in self._readers:
            entry = self._readers[fd]
            callback_id = entry[2]
            fd_key = entry[3] if len(entry) > 3 else None
            del self._readers[fd]
            self._readers_by_cid.pop(callback_id, None)
            try:
                if fd_key is not None:
                    self._pel._remove_reader(fd_key)
            except Exception:
                pass
            return True
        return False

    def add_writer(self, fd, callback, *args):
        """Register a writer callback for a file descriptor."""
        self._check_closed()
        self.remove_writer(fd)

        callback_id = self._next_id()

        try:
            fd_key = self._pel._add_writer(fd, callback_id)
            self._writers[fd] = (callback, args, callback_id, fd_key)
            self._writers_by_cid[callback_id] = fd
        except Exception as e:
            raise RuntimeError(f"Failed to add writer: {e}")

    def remove_writer(self, fd):
        """Unregister a writer callback for a file descriptor."""
        if fd in self._writers:
            entry = self._writers[fd]
            callback_id = entry[2]
            fd_key = entry[3] if len(entry) > 3 else None
            del self._writers[fd]
            self._writers_by_cid.pop(callback_id, None)
            try:
                if fd_key is not None:
                    self._pel._remove_writer(fd_key)
            except Exception:
                pass
            return True
        return False

    # ========================================================================
    # Socket operations
    # ========================================================================

    async def sock_recv(self, sock, nbytes):
        """Receive data from a socket."""
        fut = self.create_future()

        def _recv():
            try:
                data = sock.recv(nbytes)
                self.call_soon(fut.set_result, data)
            except (BlockingIOError, InterruptedError):
                return
            except Exception as e:
                self.call_soon(fut.set_exception, e)
            self.remove_reader(sock.fileno())

        self.add_reader(sock.fileno(), _recv)
        return await fut

    async def sock_recv_into(self, sock, buf):
        """Receive data from a socket into a buffer."""
        fut = self.create_future()

        def _recv_into():
            try:
                nbytes = sock.recv_into(buf)
                self.call_soon(fut.set_result, nbytes)
            except (BlockingIOError, InterruptedError):
                return
            except Exception as e:
                self.call_soon(fut.set_exception, e)
            self.remove_reader(sock.fileno())

        self.add_reader(sock.fileno(), _recv_into)
        return await fut

    async def sock_sendall(self, sock, data):
        """Send data to a socket."""
        fut = self.create_future()
        data = memoryview(data)
        offset = [0]

        def _send():
            try:
                n = sock.send(data[offset[0]:])
                offset[0] += n
                if offset[0] >= len(data):
                    self.remove_writer(sock.fileno())
                    self.call_soon(fut.set_result, None)
            except (BlockingIOError, InterruptedError):
                return
            except Exception as e:
                self.remove_writer(sock.fileno())
                self.call_soon(fut.set_exception, e)

        self.add_writer(sock.fileno(), _send)
        return await fut

    async def sock_connect(self, sock, address):
        """Connect a socket to a remote address."""
        fut = self.create_future()

        try:
            sock.connect(address)
            fut.set_result(None)
            return await fut
        except (BlockingIOError, InterruptedError):
            pass

        def _connect():
            try:
                err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if err != 0:
                    raise OSError(err, f'Connect call failed {address}')
                self.call_soon(fut.set_result, None)
            except Exception as e:
                self.call_soon(fut.set_exception, e)
            self.remove_writer(sock.fileno())

        self.add_writer(sock.fileno(), _connect)
        return await fut

    async def sock_accept(self, sock):
        """Accept a connection on a socket."""
        fut = self.create_future()

        def _accept():
            try:
                conn, address = sock.accept()
                conn.setblocking(False)
                self.call_soon(fut.set_result, (conn, address))
            except (BlockingIOError, InterruptedError):
                return
            except Exception as e:
                self.call_soon(fut.set_exception, e)
            self.remove_reader(sock.fileno())

        self.add_reader(sock.fileno(), _accept)
        return await fut

    async def sock_sendfile(self, sock, file, offset=0, count=None, *, fallback=True):
        """Send a file through a socket."""
        raise NotImplementedError("sock_sendfile not implemented")

    # ========================================================================
    # High-level connection methods
    # ========================================================================

    async def create_connection(
            self, protocol_factory, host=None, port=None,
            *, ssl=None, family=0, proto=0, flags=0, sock=None,
            local_addr=None, server_hostname=None,
            ssl_handshake_timeout=None,
            ssl_shutdown_timeout=None,
            happy_eyeballs_delay=None, interleave=None):
        """Create a streaming transport connection."""
        if sock is not None:
            sock.setblocking(False)
        else:
            infos = await self.getaddrinfo(
                host, port, family=family, type=socket.SOCK_STREAM,
                proto=proto, flags=flags)
            if not infos:
                raise OSError(f'getaddrinfo({host!r}) returned empty list')

            exceptions = []
            for family, type_, proto, cname, address in infos:
                sock = socket.socket(family, type_, proto)
                sock.setblocking(False)
                try:
                    await self.sock_connect(sock, address)
                    break
                except OSError as exc:
                    exceptions.append(exc)
                    sock.close()
                    sock = None

            if sock is None:
                if len(exceptions) == 1:
                    raise exceptions[0]
                raise OSError(f'Multiple exceptions: {exceptions}')

        protocol = protocol_factory()
        transport = _ErlangSocketTransport(self, sock, protocol)

        try:
            await transport._start()
        except Exception:
            transport.close()
            raise

        return transport, protocol

    async def create_server(
            self, protocol_factory, host=None, port=None,
            *, family=socket.AF_UNSPEC, flags=socket.AI_PASSIVE,
            sock=None, backlog=100, ssl=None,
            reuse_address=None, reuse_port=None,
            ssl_handshake_timeout=None,
            ssl_shutdown_timeout=None,
            start_serving=True):
        """Create a TCP server."""
        if sock is not None:
            sockets = [sock]
        else:
            if host == '':
                hosts = [None]
            elif isinstance(host, str):
                hosts = [host]
            else:
                hosts = host if host else [None]

            sockets = []
            infos = []
            for h in hosts:
                info = await self.getaddrinfo(
                    h, port, family=family, type=socket.SOCK_STREAM,
                    flags=flags)
                infos.extend(info)

            completed = set()
            for family, type_, proto, cname, address in infos:
                key = (family, address)
                if key in completed:
                    continue
                completed.add(key)

                sock = socket.socket(family, type_, proto)
                sock.setblocking(False)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if reuse_port:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

                try:
                    sock.bind(address)
                except OSError:
                    sock.close()
                    raise

                sock.listen(backlog)
                sockets.append(sock)

        server = _ErlangServer(self, sockets, protocol_factory, ssl, backlog)
        if start_serving:
            server._start_serving()

        return server

    async def create_datagram_endpoint(self, protocol_factory,
                                       local_addr=None, remote_addr=None, *,
                                       family=0, proto=0, flags=0,
                                       reuse_address=None, reuse_port=None,
                                       allow_broadcast=None, sock=None):
        """Create datagram (UDP) connection."""
        if sock is not None:
            sock.setblocking(False)
        else:
            if family == 0:
                if local_addr:
                    family = socket.AF_INET
                elif remote_addr:
                    family = socket.AF_INET
                else:
                    family = socket.AF_INET

            sock = socket.socket(family, socket.SOCK_DGRAM, proto)
            sock.setblocking(False)

            if reuse_address:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if reuse_port:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except (AttributeError, OSError):
                    pass

            if allow_broadcast:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            if local_addr:
                sock.bind(local_addr)

            if remote_addr:
                sock.connect(remote_addr)

        protocol = protocol_factory()
        transport = _ErlangDatagramTransport(
            self, sock, protocol, address=remote_addr
        )

        transport._start()

        return transport, protocol

    # ========================================================================
    # Signal handling
    # ========================================================================

    def add_signal_handler(self, sig, callback, *args):
        """Add a signal handler."""
        raise NotImplementedError("Signal handlers not supported in ErlangEventLoop")

    def remove_signal_handler(self, sig):
        """Remove a signal handler."""
        raise NotImplementedError("Signal handlers not supported in ErlangEventLoop")

    # ========================================================================
    # Error handling
    # ========================================================================

    def set_exception_handler(self, handler):
        """Set the exception handler."""
        self._exception_handler = handler

    def get_exception_handler(self):
        """Get the exception handler."""
        return self._exception_handler

    def default_exception_handler(self, context):
        """Default exception handler."""
        message = context.get('message', 'Unhandled exception')
        exception = context.get('exception')

        if exception is not None:
            import traceback
            exc_info = (type(exception), exception, exception.__traceback__)
            tb = ''.join(traceback.format_exception(*exc_info))
            print(f'{message}\n{tb}', file=sys.stderr)
        else:
            print(f'{message}', file=sys.stderr)

    def call_exception_handler(self, context):
        """Call the exception handler."""
        if self._exception_handler is not None:
            try:
                self._exception_handler(self, context)
            except Exception:
                self.default_exception_handler(context)
        else:
            self.default_exception_handler(context)

    # ========================================================================
    # Debug mode
    # ========================================================================

    def get_debug(self):
        """Return the debug mode setting."""
        return self._debug

    def set_debug(self, enabled):
        """Set the debug mode."""
        self._debug = enabled

    # ========================================================================
    # Internal methods
    # ========================================================================

    def _run_once(self):
        """Run one iteration of the event loop."""
        ready = self._ready
        popleft = self._ready_popleft
        return_handle = self._return_handle

        # Run all ready callbacks
        ntodo = len(ready)
        for _ in range(ntodo):
            if not ready:
                break
            handle = popleft()
            if handle._cancelled:
                return_handle(handle)
                continue
            self._current_handle = handle
            try:
                handle._run()
            except Exception as e:
                self.call_exception_handler({
                    'message': 'Exception in callback',
                    'exception': e,
                    'handle': handle,
                })
            finally:
                self._current_handle = None
                return_handle(handle)

        # Calculate timeout based on next timer using heap with lazy deletion
        if ready or self._stopping:
            timeout = 0
        elif self._timer_heap:
            timer_heap = self._timer_heap
            timers = self._timers
            while timer_heap:
                when, cid = timer_heap[0]
                handle = timers.get(cid)
                if handle is None or handle._cancelled:
                    heapq.heappop(timer_heap)
                    continue
                break

            if timer_heap:
                when, _ = timer_heap[0]
                timeout = max(0, int((when - self.time()) * 1000))
                timeout = max(1, min(timeout, 1000))
            else:
                timers.clear()
                self._timer_refs.clear()
                timeout = 1000
        else:
            timeout = 1000

        # Poll for events
        try:
            pending = self._pel._run_once_native(timeout)
            dispatch = self._dispatch
            for callback_id, event_type in pending:
                dispatch(callback_id, event_type)
        except AttributeError:
            try:
                num_events = self._pel._poll_events(timeout)
                if num_events > 0:
                    pending = self._pel._get_pending()
                    dispatch = self._dispatch
                    for callback_id, event_type in pending:
                        dispatch(callback_id, event_type)
            except AttributeError:
                pass
            except RuntimeError as e:
                raise RuntimeError(f"Event loop poll failed: {e}") from e
        except RuntimeError as e:
            raise RuntimeError(f"Event loop poll failed: {e}") from e

    def _dispatch(self, callback_id, event_type):
        """Dispatch a callback based on event type."""
        if event_type == EVENT_TYPE_READ:
            entry = self._readers.get(self._readers_by_cid.get(callback_id))
            if entry is not None:
                self._ready_append(self._get_handle(entry[0], entry[1]))
        elif event_type == EVENT_TYPE_WRITE:
            entry = self._writers.get(self._writers_by_cid.get(callback_id))
            if entry is not None:
                self._ready_append(self._get_handle(entry[0], entry[1]))
        elif event_type == EVENT_TYPE_TIMER:
            handle = self._timers.pop(callback_id, None)
            if handle is not None:
                self._timer_refs.pop(callback_id, None)
                self._handle_to_callback_id.pop(id(handle), None)
                if not handle._cancelled:
                    self._ready_append(handle)

    def _check_closed(self):
        """Raise an error if the loop is closed."""
        if self._closed:
            raise RuntimeError('Event loop is closed')

    def _check_running(self):
        """Raise an error if the loop is already running."""
        if self._running:
            raise RuntimeError('This event loop is already running')

    def _timer_handle_cancelled(self, handle):
        """Called when a TimerHandle is cancelled."""
        callback_id = self._handle_to_callback_id.pop(id(handle), None)
        if callback_id is not None:
            self._timers.pop(callback_id, None)
            timer_ref = self._timer_refs.pop(callback_id, None)
            if timer_ref is not None:
                try:
                    self._pel._cancel_timer(timer_ref)
                except (AttributeError, RuntimeError):
                    pass

    def _set_coroutine_origin_tracking(self, enabled):
        """Enable/disable coroutine origin tracking."""
        if enabled:
            sys.set_coroutine_origin_tracking_depth(1)
        else:
            sys.set_coroutine_origin_tracking_depth(0)

    # ========================================================================
    # Handle pool for reduced allocations
    # ========================================================================

    def _get_handle(self, callback, args):
        """Get a Handle from the pool or create a new one."""
        if self._handle_pool:
            handle = self._handle_pool.pop()
            handle._callback = callback
            handle._args = args
            handle._cancelled = False
            return handle
        return events.Handle(callback, args, self, None)

    def _return_handle(self, handle):
        """Return a Handle to the pool for reuse."""
        if len(self._handle_pool) < self._handle_pool_max:
            handle._callback = None
            handle._args = None
            self._handle_pool.append(handle)

    # ========================================================================
    # Executor methods
    # ========================================================================

    def run_in_executor(self, executor, func, *args):
        """Run a function in an executor."""
        self._check_closed()
        if executor is None:
            executor = self._get_default_executor()
        return asyncio.wrap_future(
            executor.submit(func, *args),
            loop=self
        )

    def _get_default_executor(self):
        """Get or create the default executor."""
        if self._default_executor is None:
            from concurrent.futures import ThreadPoolExecutor
            self._default_executor = ThreadPoolExecutor()
        return self._default_executor

    def set_default_executor(self, executor):
        """Set the default executor."""
        self._default_executor = executor

    # ========================================================================
    # DNS resolution
    # ========================================================================

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        """Resolve host/port to address info."""
        return await self.run_in_executor(
            None, socket.getaddrinfo, host, port, family, type, proto, flags
        )

    async def getnameinfo(self, sockaddr, flags=0):
        """Resolve socket address to host/port."""
        return await self.run_in_executor(None, socket.getnameinfo, sockaddr, flags)


class _ErlangSocketTransport(transports.Transport):
    """Socket transport for ErlangEventLoop."""

    __slots__ = (
        '_loop', '_sock', '_protocol', '_buffer', '_closing', '_conn_lost',
        '_write_ready', '_paused', '_extra', '_fileno',
    )

    _buffer_factory = bytearray
    max_size = 256 * 1024

    def __init__(self, loop, sock, protocol, extra=None):
        super().__init__(extra)
        self._loop = loop
        self._sock = sock
        self._protocol = protocol
        self._buffer = self._buffer_factory()
        self._closing = False
        self._conn_lost = 0
        self._write_ready = True
        self._paused = False
        self._fileno = sock.fileno()
        self._extra = extra or {}
        self._extra['socket'] = sock
        try:
            self._extra['sockname'] = sock.getsockname()
        except OSError:
            pass
        try:
            self._extra['peername'] = sock.getpeername()
        except OSError:
            pass

    async def _start(self):
        """Start the transport."""
        self._loop.call_soon(self._protocol.connection_made, self)
        self._loop.add_reader(self._fileno, self._read_ready)

    def _read_ready(self):
        """Called when data is available to read."""
        if self._conn_lost:
            return
        try:
            data = self._sock.recv(self.max_size)
        except (BlockingIOError, InterruptedError):
            return
        except Exception as exc:
            self._fatal_error(exc, 'Fatal read error')
            return

        if data:
            self._protocol.data_received(data)
        else:
            self._loop.remove_reader(self._fileno)
            self._protocol.eof_received()

    def write(self, data):
        """Write data to the transport."""
        if self._conn_lost or self._closing:
            return
        if not data:
            return

        if not self._buffer:
            try:
                n = self._sock.send(data)
            except (BlockingIOError, InterruptedError):
                n = 0
            except Exception as exc:
                self._fatal_error(exc, 'Fatal write error')
                return

            if n == len(data):
                return
            elif n > 0:
                data = data[n:]
            self._loop.add_writer(self._fileno, self._write_ready_cb)

        self._buffer.extend(data)

    def _write_ready_cb(self):
        """Called when socket is ready for writing."""
        if not self._buffer:
            self._loop.remove_writer(self._fileno)
            if self._closing:
                self._call_connection_lost(None)
            return

        try:
            n = self._sock.send(self._buffer)
        except (BlockingIOError, InterruptedError):
            return
        except Exception as exc:
            self._loop.remove_writer(self._fileno)
            self._fatal_error(exc, 'Fatal write error')
            return

        if n:
            del self._buffer[:n]

        if not self._buffer:
            self._loop.remove_writer(self._fileno)
            if self._closing:
                self._call_connection_lost(None)

    def write_eof(self):
        """Close the write end."""
        if self._closing:
            return
        self._closing = True
        if not self._buffer:
            self._loop.remove_reader(self._fileno)
            self._call_connection_lost(None)

    def can_write_eof(self):
        return True

    def close(self):
        """Close the transport."""
        if self._closing:
            return
        self._closing = True
        self._loop.remove_reader(self._fileno)
        if not self._buffer:
            self._conn_lost += 1
            self._call_connection_lost(None)

    def _call_connection_lost(self, exc):
        """Call protocol.connection_lost()."""
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._sock.close()

    def _fatal_error(self, exc, message='Fatal error'):
        """Handle fatal errors."""
        self._loop.call_exception_handler({
            'message': message,
            'exception': exc,
            'transport': self,
            'protocol': self._protocol,
        })
        self.close()

    def get_extra_info(self, name, default=None):
        return self._extra.get(name, default)

    def is_closing(self):
        return self._closing

    def get_write_buffer_size(self):
        return len(self._buffer)

    def abort(self):
        """Close immediately."""
        self._closing = True
        self._conn_lost += 1
        self._loop.remove_reader(self._fileno)
        self._loop.remove_writer(self._fileno)
        self._call_connection_lost(None)


class _ErlangDatagramTransport(transports.DatagramTransport):
    """Datagram (UDP) transport for ErlangEventLoop."""

    __slots__ = (
        '_loop', '_sock', '_protocol', '_address', '_buffer',
        '_closing', '_conn_lost', '_extra', '_fileno',
    )

    max_size = 256 * 1024

    def __init__(self, loop, sock, protocol, address=None, extra=None):
        super().__init__(extra)
        self._loop = loop
        self._sock = sock
        self._protocol = protocol
        self._address = address
        self._buffer = deque()
        self._closing = False
        self._conn_lost = 0
        self._fileno = sock.fileno()
        self._extra = extra or {}
        self._extra['socket'] = sock
        try:
            self._extra['sockname'] = sock.getsockname()
        except OSError:
            pass
        if address:
            self._extra['peername'] = address

    def _start(self):
        """Start the transport."""
        self._loop.call_soon(self._protocol.connection_made, self)
        self._loop.add_reader(self._fileno, self._read_ready)

    def _read_ready(self):
        """Called when data is available to read."""
        if self._conn_lost:
            return
        try:
            data, addr = self._sock.recvfrom(self.max_size)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            self._protocol.error_received(exc)
            return
        except Exception as exc:
            self._fatal_error(exc, 'Fatal read error on datagram transport')
            return

        self._protocol.datagram_received(data, addr)

    def sendto(self, data, addr=None):
        """Send data to the transport."""
        if self._conn_lost or self._closing:
            return
        if not data:
            return

        if addr is None:
            addr = self._address

        if not self._buffer:
            try:
                if addr:
                    self._sock.sendto(data, addr)
                else:
                    self._sock.send(data)
                return
            except (BlockingIOError, InterruptedError):
                self._loop.add_writer(self._fileno, self._write_ready)
            except OSError as exc:
                self._protocol.error_received(exc)
                return
            except Exception as exc:
                self._fatal_error(exc, 'Fatal write error on datagram transport')
                return

        self._buffer.append((data, addr))

    def _write_ready(self):
        """Called when socket is ready for writing."""
        while self._buffer:
            data, addr = self._buffer[0]
            try:
                if addr:
                    self._sock.sendto(data, addr)
                else:
                    self._sock.send(data)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:
                self._buffer.popleft()
                self._protocol.error_received(exc)
                return
            except Exception as exc:
                self._fatal_error(exc, 'Fatal write error on datagram transport')
                return

            self._buffer.popleft()

        self._loop.remove_writer(self._fileno)
        if self._closing:
            self._call_connection_lost(None)

    def close(self):
        """Close the transport."""
        if self._closing:
            return
        self._closing = True
        self._loop.remove_reader(self._fileno)
        if not self._buffer:
            self._conn_lost += 1
            self._call_connection_lost(None)

    def _call_connection_lost(self, exc):
        """Call protocol.connection_lost()."""
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._sock.close()

    def _fatal_error(self, exc, message='Fatal error on datagram transport'):
        """Handle fatal errors."""
        self._loop.call_exception_handler({
            'message': message,
            'exception': exc,
            'transport': self,
            'protocol': self._protocol,
        })
        self.close()

    def get_extra_info(self, name, default=None):
        return self._extra.get(name, default)

    def is_closing(self):
        return self._closing

    def abort(self):
        """Close immediately."""
        self._closing = True
        self._conn_lost += 1
        self._loop.remove_reader(self._fileno)
        self._loop.remove_writer(self._fileno)
        self._buffer.clear()
        self._call_connection_lost(None)

    def get_write_buffer_size(self):
        """Return the current size of the write buffer."""
        return sum(len(data) for data, _ in self._buffer)


class _ErlangServer:
    """TCP server for ErlangEventLoop."""

    def __init__(self, loop, sockets, protocol_factory, ssl_context, backlog):
        self._loop = loop
        self._sockets = sockets
        self._protocol_factory = protocol_factory
        self._ssl_context = ssl_context
        self._backlog = backlog
        self._serving = False
        self._waiters = []

    def _start_serving(self):
        """Start accepting connections."""
        if self._serving:
            return
        self._serving = True
        for sock in self._sockets:
            self._loop.add_reader(sock.fileno(), self._accept_connection, sock)

    def _accept_connection(self, server_sock):
        """Accept a new connection."""
        try:
            conn, addr = server_sock.accept()
            conn.setblocking(False)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            if exc.errno not in (errno.EMFILE, errno.ENFILE,
                                 errno.ENOBUFS, errno.ENOMEM):
                raise
            return

        protocol = self._protocol_factory()
        transport = _ErlangSocketTransport(self._loop, conn, protocol)
        self._loop.create_task(transport._start())

    def close(self):
        """Stop the server."""
        if not self._serving:
            return
        self._serving = False
        for sock in self._sockets:
            self._loop.remove_reader(sock.fileno())
            sock.close()
        self._sockets.clear()

    async def start_serving(self):
        """Start serving."""
        self._start_serving()

    async def serve_forever(self):
        """Serve forever."""
        if not self._serving:
            self._start_serving()
        waiter = self._loop.create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        finally:
            self._waiters.remove(waiter)

    def is_serving(self):
        return self._serving

    def get_loop(self):
        return self._loop

    @property
    def sockets(self):
        return tuple(self._sockets)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.close()
        await self.wait_closed()

    async def wait_closed(self):
        """Wait until server is closed."""
        if self._sockets:
            await asyncio.sleep(0)


class _MockNifModule:
    """Mock NIF module for testing without actual Erlang integration."""

    def __init__(self):
        self.readers = {}
        self.writers = {}
        self.pending = []
        self._counter = 0

    def _is_initialized(self):
        return True

    def _poll_events(self, timeout_ms):
        time.sleep(min(timeout_ms, 10) / 1000.0)
        return len(self.pending)

    def _get_pending(self):
        result = list(self.pending)
        self.pending.clear()
        return result

    def _run_once_native(self, timeout_ms):
        """Combined poll + get_pending returning integer event types."""
        time.sleep(min(timeout_ms, 10) / 1000.0)
        result = []
        for callback_id, event_type in self.pending:
            if isinstance(event_type, str):
                if event_type == 'read':
                    event_type = EVENT_TYPE_READ
                elif event_type == 'write':
                    event_type = EVENT_TYPE_WRITE
                else:
                    event_type = EVENT_TYPE_TIMER
            result.append((callback_id, event_type))
        self.pending.clear()
        return result

    def _wakeup(self):
        pass

    def _add_pending(self, callback_id, type_str):
        self.pending.append((callback_id, type_str))

    def _add_reader(self, fd, callback_id):
        self._counter += 1
        self.readers[fd] = (callback_id, self._counter)
        return self._counter

    def _remove_reader(self, fd_key):
        for fd, (cid, key) in list(self.readers.items()):
            if key == fd_key:
                del self.readers[fd]
                break

    def _add_writer(self, fd, callback_id):
        self._counter += 1
        self.writers[fd] = (callback_id, self._counter)
        return self._counter

    def _remove_writer(self, fd_key):
        for fd, (cid, key) in list(self.writers.items()):
            if key == fd_key:
                del self.writers[fd]
                break

    def _schedule_timer(self, delay_ms, callback_id):
        """Mock timer scheduling."""
        return callback_id

    def _cancel_timer(self, timer_ref):
        """Mock timer cancellation."""
        pass


def get_event_loop_policy():
    """Get an event loop policy that uses ErlangEventLoop for the main thread.

    Non-main threads get the default SelectorEventLoop to avoid conflicts
    with the Erlang-native event loop which is designed for the main thread.
    """
    main_thread_id = threading.main_thread().ident

    class ErlangEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
        def __init__(self):
            self._local = threading.local()

        def get_event_loop(self):
            if not hasattr(self._local, 'loop') or self._local.loop is None:
                self._local.loop = self.new_event_loop()
            return self._local.loop

        def set_event_loop(self, loop):
            self._local.loop = loop

        def new_event_loop(self):
            if threading.current_thread().ident == main_thread_id:
                return ErlangEventLoop()
            else:
                return asyncio.SelectorEventLoop()

    return ErlangEventLoopPolicy()


# =============================================================================
# Async coroutine wrapper for result delivery
# =============================================================================

async def _run_and_send(coro, caller_pid, ref):
    """Run a coroutine and send the result to an Erlang caller via erlang.send().

    This function wraps a coroutine and sends its result (or error) to the
    specified Erlang process using erlang.send(). Used by the async worker
    backend to deliver results without pthread polling.

    Args:
        coro: The coroutine to run
        caller_pid: An erlang.Pid object for the caller process
        ref: A reference to include in the result message

    The result message format is:
        ('async_result', ref, ('ok', result)) - on success
        ('async_result', ref, ('error', error_str)) - on failure
    """
    import erlang
    try:
        result = await coro
        erlang.send(caller_pid, ('async_result', ref, ('ok', result)))
    except asyncio.CancelledError:
        erlang.send(caller_pid, ('async_result', ref, ('error', 'cancelled')))
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        erlang.send(caller_pid, ('async_result', ref, ('error', f'{type(e).__name__}: {e}\n{tb}')))
