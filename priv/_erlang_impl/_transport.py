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
Transport classes for the Erlang event loop.

This module provides asyncio-compatible transport implementations
for TCP, UDP, and Unix sockets backed by the Erlang event loop.
"""

import asyncio
import errno
import socket
from asyncio import transports
from collections import deque
from typing import Any, Optional, Tuple

__all__ = [
    'ErlangSocketTransport',
    'ErlangDatagramTransport',
    'ErlangServer',
]


class ErlangSocketTransport(transports.Transport):
    """Socket transport for ErlangEventLoop.

    Implements asyncio.Transport for TCP and Unix stream sockets.
    """

    __slots__ = (
        '_loop', '_sock', '_protocol', '_buffer', '_closing', '_conn_lost',
        '_write_ready', '_paused', '_extra', '_fileno',
    )

    _buffer_factory = bytearray
    max_size = 256 * 1024  # 256 KB

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
            # Connection closed (EOF received)
            self._loop.remove_reader(self._fileno)
            keep_open = self._protocol.eof_received()
            # If eof_received returns False/None, close the transport
            if not keep_open:
                self._closing = True
                self._conn_lost += 1
                self._call_connection_lost(None)

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

    def get_write_buffer_limits(self):
        return (0, 0)

    def set_write_buffer_limits(self, high=None, low=None):
        pass

    def abort(self):
        """Close immediately."""
        self._closing = True
        self._conn_lost += 1
        self._loop.remove_reader(self._fileno)
        self._loop.remove_writer(self._fileno)
        self._call_connection_lost(None)

    def pause_reading(self):
        """Pause reading from the transport."""
        if self._closing or self._paused:
            return
        self._paused = True
        self._loop.remove_reader(self._fileno)

    def resume_reading(self):
        """Resume reading from the transport."""
        if self._closing or not self._paused:
            return
        self._paused = False
        self._loop.add_reader(self._fileno, self._read_ready)

    def is_reading(self):
        """Return True if the transport is receiving."""
        return not self._paused and not self._closing


class ErlangDatagramTransport(transports.DatagramTransport):
    """Datagram (UDP) transport for ErlangEventLoop."""

    __slots__ = (
        '_loop', '_sock', '_protocol', '_address', '_buffer',
        '_closing', '_conn_lost', '_extra', '_fileno',
    )

    max_size = 256 * 1024  # 256 KB

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

    async def _start(self):
        """Start the transport."""
        # Call connection_made directly to ensure it runs before returning
        self._protocol.connection_made(self)
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
                # For connected sockets (self._address is set), use send() not sendto()
                # because sendto() with an address fails with "Socket is already connected"
                if self._address is not None:
                    # Connected socket - use send()
                    self._sock.send(data)
                elif addr:
                    # Not connected, addr provided - use sendto()
                    self._sock.sendto(data, addr)
                else:
                    # Not connected, no addr - use send() (will fail if not connected)
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
                # For connected sockets (self._address is set), use send() not sendto()
                if self._address is not None:
                    self._sock.send(data)
                elif addr:
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


class ErlangServer:
    """TCP/Unix server for ErlangEventLoop."""

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
        transport = ErlangSocketTransport(self._loop, conn, protocol)
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

        # Wake up waiters
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)

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
