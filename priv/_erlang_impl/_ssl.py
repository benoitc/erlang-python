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
SSL/TLS transport using ssl.MemoryBIO.

This module provides SSL/TLS support that works with the Erlang event
loop by using Python's ssl.MemoryBIO for encryption while letting
Erlang handle the socket I/O.

Architecture:
- Raw socket data flows through Erlang (enif_select)
- Encryption/decryption happens in Python via MemoryBIO
- Application sees decrypted data
"""

import ssl
from asyncio import transports
from typing import Any, Optional, Callable

__all__ = ['SSLTransport', 'create_ssl_transport']


class SSLTransport(transports.Transport):
    """SSL transport using ssl.MemoryBIO for encryption.

    This transport wraps a raw transport and provides transparent
    SSL/TLS encryption using Python's ssl module with MemoryBIO.

    The key insight is that MemoryBIO allows us to do SSL encryption
    without requiring a real socket file descriptor, which works
    perfectly with Erlang's enif_select model.
    """

    max_size = 256 * 1024  # 256 KB

    def __init__(self, loop, raw_transport, protocol, ssl_context,
                 server_hostname=None, server_side=False,
                 ssl_handshake_timeout=None, call_connection_made=True):
        """Initialize the SSL transport.

        Args:
            loop: The event loop.
            raw_transport: The underlying raw transport.
            protocol: The application protocol.
            ssl_context: SSL context for encryption.
            server_hostname: Hostname for SNI (client side).
            server_side: True if this is a server connection.
            ssl_handshake_timeout: Timeout for the SSL handshake.
            call_connection_made: Whether to call connection_made.
        """
        self._loop = loop
        self._raw_transport = raw_transport
        self._protocol = protocol
        self._ssl_context = ssl_context
        self._server_hostname = server_hostname
        self._server_side = server_side
        self._handshake_timeout = ssl_handshake_timeout
        self._call_connection_made = call_connection_made

        # SSL state
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl_object = ssl_context.wrap_bio(
            self._incoming, self._outgoing,
            server_side=server_side,
            server_hostname=server_hostname
        )

        # State flags
        self._handshake_complete = False
        self._closing = False
        self._closed = False
        self._write_buffer = []

        # Extra info
        self._extra = {
            'ssl_context': ssl_context,
        }

        # Create a protocol that receives raw data
        self._raw_protocol = _SSLRawProtocol(self)

    async def _start(self):
        """Start the SSL transport and perform handshake."""
        # Replace the raw transport's protocol with ours
        self._raw_transport._protocol = self._raw_protocol

        # Perform SSL handshake
        await self._do_handshake()

        # Update extra info
        self._extra['peercert'] = self._ssl_object.getpeercert()
        self._extra['cipher'] = self._ssl_object.cipher()
        self._extra['compression'] = self._ssl_object.compression()
        self._extra['ssl_object'] = self._ssl_object

        # Notify application protocol
        if self._call_connection_made:
            self._loop.call_soon(self._protocol.connection_made, self)

    async def _do_handshake(self):
        """Perform SSL handshake."""
        while not self._handshake_complete:
            try:
                self._ssl_object.do_handshake()
                self._handshake_complete = True
            except ssl.SSLWantReadError:
                # Need to send data and receive more
                self._flush_outgoing()
                await self._wait_for_data()
            except ssl.SSLWantWriteError:
                # Need to send buffered data
                self._flush_outgoing()

    def _flush_outgoing(self):
        """Flush outgoing encrypted data to raw transport."""
        data = self._outgoing.read()
        if data:
            self._raw_transport.write(data)

    async def _wait_for_data(self):
        """Wait for data from the raw transport."""
        fut = self._loop.create_future()

        def on_data():
            if not fut.done():
                fut.set_result(None)

        self._raw_protocol._read_waiter = on_data
        try:
            await fut
        finally:
            self._raw_protocol._read_waiter = None

    def _on_raw_data(self, data: bytes):
        """Called when raw encrypted data is received.

        Args:
            data: Encrypted data from the network.
        """
        self._incoming.write(data)

        if not self._handshake_complete:
            # Still handshaking, notify waiter
            return

        # Decrypt and deliver to application
        try:
            while True:
                chunk = self._ssl_object.read(self.max_size)
                if chunk:
                    self._protocol.data_received(chunk)
                else:
                    break
        except ssl.SSLWantReadError:
            pass
        except ssl.SSLError as e:
            self._fatal_error(e, 'SSL read error')

    def _on_raw_eof(self):
        """Called when the raw transport receives EOF."""
        try:
            self._ssl_object.unwrap()
        except ssl.SSLError:
            pass

        if hasattr(self._protocol, 'eof_received'):
            self._protocol.eof_received()

    def write(self, data: bytes):
        """Write data to the transport.

        Args:
            data: Plaintext data to send.
        """
        if self._closing or self._closed:
            return
        if not data:
            return

        if not self._handshake_complete:
            self._write_buffer.append(data)
            return

        try:
            self._ssl_object.write(data)
            self._flush_outgoing()
        except ssl.SSLError as e:
            self._fatal_error(e, 'SSL write error')

    def writelines(self, list_of_data):
        """Write a list of data items."""
        for data in list_of_data:
            self.write(data)

    def write_eof(self):
        """Close the write end of the transport."""
        if self._closing:
            return
        self._closing = True

        try:
            self._ssl_object.unwrap()
            self._flush_outgoing()
        except ssl.SSLError:
            pass

        self._raw_transport.write_eof()

    def can_write_eof(self):
        return False

    def close(self):
        """Close the transport."""
        if self._closed:
            return
        self._closing = True

        try:
            self._ssl_object.unwrap()
            self._flush_outgoing()
        except ssl.SSLError:
            pass

        self._raw_transport.close()
        self._closed = True

    def is_closing(self):
        return self._closing

    def abort(self):
        """Close immediately without flushing."""
        self._closed = True
        self._raw_transport.abort()

    def get_extra_info(self, name, default=None):
        if name in self._extra:
            return self._extra[name]
        return self._raw_transport.get_extra_info(name, default)

    def get_write_buffer_size(self):
        return self._raw_transport.get_write_buffer_size()

    def get_write_buffer_limits(self):
        return self._raw_transport.get_write_buffer_limits()

    def set_write_buffer_limits(self, high=None, low=None):
        self._raw_transport.set_write_buffer_limits(high, low)

    def pause_reading(self):
        self._raw_transport.pause_reading()

    def resume_reading(self):
        self._raw_transport.resume_reading()

    def is_reading(self):
        return self._raw_transport.is_reading()

    def _fatal_error(self, exc, message='Fatal SSL error'):
        """Handle fatal SSL errors."""
        self._loop.call_exception_handler({
            'message': message,
            'exception': exc,
            'transport': self,
            'protocol': self._protocol,
        })
        self.abort()


class _SSLRawProtocol:
    """Protocol that receives raw encrypted data for SSLTransport."""

    def __init__(self, ssl_transport):
        self._ssl_transport = ssl_transport
        self._read_waiter = None

    def connection_made(self, transport):
        pass

    def data_received(self, data):
        self._ssl_transport._on_raw_data(data)
        if self._read_waiter is not None:
            self._read_waiter()

    def eof_received(self):
        self._ssl_transport._on_raw_eof()

    def connection_lost(self, exc):
        self._ssl_transport._protocol.connection_lost(exc)


async def create_ssl_transport(
        loop, raw_transport, protocol, ssl_context,
        server_hostname=None, server_side=False,
        ssl_handshake_timeout=None):
    """Create an SSL transport wrapping a raw transport.

    Args:
        loop: The event loop.
        raw_transport: The underlying raw transport.
        protocol: The application protocol.
        ssl_context: SSL context for encryption.
        server_hostname: Hostname for SNI (client side).
        server_side: True if this is a server connection.
        ssl_handshake_timeout: Timeout for the SSL handshake.

    Returns:
        The SSL transport.
    """
    transport = SSLTransport(
        loop, raw_transport, protocol, ssl_context,
        server_hostname=server_hostname,
        server_side=server_side,
        ssl_handshake_timeout=ssl_handshake_timeout
    )
    await transport._start()
    return transport
