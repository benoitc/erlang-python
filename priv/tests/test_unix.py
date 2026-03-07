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
Unix socket tests adapted from uvloop's test_unix.py.

These tests verify Unix domain socket operations:
- create_unix_server
- create_unix_connection
- Unix socket data transfer
"""

import asyncio
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

from . import _testbase as tb


def _is_unix_socket_supported():
    """Check if Unix sockets are supported on this platform."""
    return sys.platform != 'win32'


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class _TestUnixServer:
    """Tests for create_unix_server functionality."""

    def test_create_unix_server_basic(self):
        """Test basic Unix socket server creation."""
        connections = []

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')

            class ServerProtocol(asyncio.Protocol):
                def connection_made(self, transport):
                    connections.append(transport)
                    transport.write(b'welcome')

                def data_received(self, data):
                    pass

            async def main():
                server = await self.loop.create_unix_server(
                    ServerProtocol, path
                )

                # Connect a client
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.setblocking(False)
                await self.loop.sock_connect(sock, path)
                data = await self.loop.sock_recv(sock, 1024)
                sock.close()

                server.close()
                await server.wait_closed()

                return data

            result = self.loop.run_until_complete(main())

            self.assertEqual(result, b'welcome')
            self.assertEqual(len(connections), 1)

    def test_create_unix_server_existing_path(self):
        """Test that server removes existing socket file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')

            # Create file at path
            with open(path, 'w') as f:
                f.write('test')

            async def main():
                # Should replace the file
                server = await self.loop.create_unix_server(
                    asyncio.Protocol, path
                )
                server.close()
                await server.wait_closed()

            self.loop.run_until_complete(main())

    def test_unix_server_client_echo(self):
        """Test Unix socket server with echo pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'echo.sock')
            received = []

            class EchoProtocol(asyncio.Protocol):
                def connection_made(self, transport):
                    self.transport = transport

                def data_received(self, data):
                    received.append(data)
                    self.transport.write(b'echo:' + data)
                    self.transport.close()

            async def main():
                server = await self.loop.create_unix_server(
                    EchoProtocol, path
                )

                class ClientProtocol(asyncio.Protocol):
                    def __init__(self):
                        self.received = []
                        self.done = asyncio.get_event_loop().create_future()

                    def connection_made(self, transport):
                        transport.write(b'hello')

                    def data_received(self, data):
                        self.received.append(data)

                    def connection_lost(self, exc):
                        if not self.done.done():
                            self.done.set_result(self.received)

                transport, protocol = await self.loop.create_unix_connection(
                    ClientProtocol, path
                )

                result = await protocol.done

                server.close()
                await server.wait_closed()

                return result

            result = self.loop.run_until_complete(main())

            self.assertEqual(result, [b'echo:hello'])
            self.assertEqual(received, [b'hello'])


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class _TestUnixConnection:
    """Tests for create_unix_connection functionality."""

    def test_create_unix_connection_basic(self):
        """Test basic Unix socket connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')

            class ServerProtocol(asyncio.Protocol):
                def connection_made(self, transport):
                    transport.write(b'hello')
                    transport.close()

            async def main():
                server = await self.loop.create_unix_server(
                    ServerProtocol, path
                )

                class ClientProtocol(asyncio.Protocol):
                    def __init__(self):
                        self.data = bytearray()
                        self.done = asyncio.get_event_loop().create_future()

                    def data_received(self, data):
                        self.data.extend(data)

                    def connection_lost(self, exc):
                        if not self.done.done():
                            self.done.set_result(bytes(self.data))

                transport, protocol = await self.loop.create_unix_connection(
                    ClientProtocol, path
                )

                result = await protocol.done

                server.close()
                await server.wait_closed()

                return result

            result = self.loop.run_until_complete(main())
            self.assertEqual(result, b'hello')

    def test_create_unix_connection_with_sock(self):
        """Test create_unix_connection with existing socket."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')

            class ServerProtocol(asyncio.Protocol):
                def connection_made(self, transport):
                    transport.write(b'hi')
                    transport.close()

            async def main():
                server = await self.loop.create_unix_server(
                    ServerProtocol, path
                )

                # Create socket manually
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.setblocking(False)
                await self.loop.sock_connect(sock, path)

                class ClientProtocol(asyncio.Protocol):
                    def __init__(self):
                        self.data = bytearray()
                        self.done = asyncio.get_event_loop().create_future()

                    def data_received(self, data):
                        self.data.extend(data)

                    def connection_lost(self, exc):
                        if not self.done.done():
                            self.done.set_result(bytes(self.data))

                transport, protocol = await self.loop.create_unix_connection(
                    ClientProtocol, sock=sock
                )

                result = await protocol.done

                server.close()
                await server.wait_closed()

                return result

            result = self.loop.run_until_complete(main())
            self.assertEqual(result, b'hi')


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class _TestUnixSocketOps:
    """Tests for low-level Unix socket operations."""

    def test_unix_sock_connect_sendall_recv(self):
        """Test Unix socket connect, sendall, recv."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')
            received = []
            server_ready = threading.Event()

            def server_thread():
                server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                server.bind(path)
                server.listen(1)
                server_ready.set()
                conn, _ = server.accept()
                data = conn.recv(1024)
                received.append(data)
                conn.sendall(b'pong')
                conn.close()
                server.close()

            thread = threading.Thread(target=server_thread)
            thread.start()
            server_ready.wait(timeout=5)

            async def client():
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.setblocking(False)
                await self.loop.sock_connect(sock, path)
                await self.loop.sock_sendall(sock, b'ping')
                data = await self.loop.sock_recv(sock, 1024)
                sock.close()
                return data

            result = self.loop.run_until_complete(client())
            thread.join(timeout=5)

            self.assertEqual(result, b'pong')
            self.assertEqual(received, [b'ping'])

    def test_unix_sock_accept(self):
        """Test Unix socket accept."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')

            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            server.setblocking(False)

            def client_thread():
                time.sleep(0.05)
                client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                client.connect(path)
                client.sendall(b'hello')
                client.close()

            thread = threading.Thread(target=client_thread)
            thread.start()

            async def accept():
                conn, _ = await self.loop.sock_accept(server)
                data = await self.loop.sock_recv(conn, 1024)
                conn.close()
                server.close()
                return data

            result = self.loop.run_until_complete(accept())
            thread.join(timeout=5)

            self.assertEqual(result, b'hello')


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class _TestUnixLargeData:
    """Tests for large data transfer over Unix sockets."""

    def test_unix_large_data(self):
        """Test transferring large data over Unix sockets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')
            data_size = 2 * 1024 * 1024  # 2 MB
            received = bytearray()

            class ServerProtocol(asyncio.Protocol):
                def connection_made(self, transport):
                    self.transport = transport

                def data_received(self, data):
                    received.extend(data)

            async def main():
                server = await self.loop.create_unix_server(
                    ServerProtocol, path
                )

                class ClientProtocol(asyncio.Protocol):
                    def connection_made(self, transport):
                        transport.write(b'x' * data_size)
                        transport.close()

                await self.loop.create_unix_connection(
                    ClientProtocol, path
                )

                # Wait for data
                for _ in range(100):
                    if len(received) >= data_size:
                        break
                    await asyncio.sleep(0.1)

                server.close()
                await server.wait_closed()

                return len(received)

            result = self.loop.run_until_complete(main())
            self.assertEqual(result, data_size)


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestErlangUnixServer(_TestUnixServer, tb.ErlangTestCase):
    pass


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestAIOUnixServer(_TestUnixServer, tb.AIOTestCase):
    pass


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestErlangUnixConnection(_TestUnixConnection, tb.ErlangTestCase):
    pass


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestAIOUnixConnection(_TestUnixConnection, tb.AIOTestCase):
    pass


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestErlangUnixSocketOps(_TestUnixSocketOps, tb.ErlangTestCase):
    pass


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestAIOUnixSocketOps(_TestUnixSocketOps, tb.AIOTestCase):
    pass


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestErlangUnixLargeData(_TestUnixLargeData, tb.ErlangTestCase):
    pass


@unittest.skipUnless(_is_unix_socket_supported(), "Unix sockets not available")
class TestAIOUnixLargeData(_TestUnixLargeData, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
