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
TCP protocol tests adapted from uvloop's test_tcp.py.

These tests verify high-level TCP operations:
- create_server
- create_connection
- Transport and Protocol interactions
- Data transmission and flow control
"""

import asyncio
import socket
import threading
import time
import unittest

from . import _testbase as tb


class _TestCreateServer:
    """Tests for create_server functionality."""

    def test_create_server_basic(self):
        """Test basic TCP server creation."""
        connections = []

        class ServerProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                connections.append(transport)
                transport.write(b'welcome')

            def data_received(self, data):
                pass

            def connection_lost(self, exc):
                pass

        async def main():
            server = await self.loop.create_server(
                ServerProtocol, '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]

            # Connect a client
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))
            data = await self.loop.sock_recv(sock, 1024)
            sock.close()

            server.close()
            await server.wait_closed()

            return data

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, b'welcome')
        self.assertEqual(len(connections), 1)

    def test_create_server_multiple_clients(self):
        """Test server handling multiple clients."""
        connections = []
        received_data = []

        class ServerProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                connections.append(transport)

            def data_received(self, data):
                received_data.append(data)
                self.transport.write(b'echo:' + data)

            def connection_lost(self, exc):
                pass

        async def main():
            server = await self.loop.create_server(
                ServerProtocol, '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]

            async def client(msg):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setblocking(False)
                await self.loop.sock_connect(sock, ('127.0.0.1', port))
                await self.loop.sock_sendall(sock, msg)
                data = await self.loop.sock_recv(sock, 1024)
                sock.close()
                return data

            results = await asyncio.gather(
                client(b'msg1'),
                client(b'msg2'),
                client(b'msg3'),
            )

            server.close()
            await server.wait_closed()

            return results

        results = self.loop.run_until_complete(main())

        self.assertEqual(len(results), 3)
        self.assertEqual(len(connections), 3)
        self.assertEqual(sorted(received_data), [b'msg1', b'msg2', b'msg3'])

    def test_create_server_close_during_accept(self):
        """Test closing server during accept."""
        async def main():
            server = await self.loop.create_server(
                asyncio.Protocol, '127.0.0.1', 0
            )
            self.assertTrue(server.is_serving())
            server.close()
            await server.wait_closed()
            self.assertFalse(server.is_serving())

        self.loop.run_until_complete(main())

    def test_create_server_reuse_address(self):
        """Test server with reuse_address option."""
        async def main():
            server1 = await self.loop.create_server(
                asyncio.Protocol, '127.0.0.1', 0,
                reuse_address=True
            )
            port = server1.sockets[0].getsockname()[1]
            server1.close()
            await server1.wait_closed()

            # Should be able to bind to same port quickly
            server2 = await self.loop.create_server(
                asyncio.Protocol, '127.0.0.1', port,
                reuse_address=True
            )
            server2.close()
            await server2.wait_closed()

        self.loop.run_until_complete(main())

    def test_create_server_start_serving_false(self):
        """Test server with start_serving=False."""
        class ServerProtocol(asyncio.Protocol):
            pass

        async def main():
            server = await self.loop.create_server(
                ServerProtocol, '127.0.0.1', 0,
                start_serving=False
            )
            self.assertFalse(server.is_serving())

            await server.start_serving()
            self.assertTrue(server.is_serving())

            server.close()
            await server.wait_closed()

        self.loop.run_until_complete(main())


class _TestCreateConnection:
    """Tests for create_connection functionality."""

    def test_create_connection_basic(self):
        """Test basic TCP connection."""
        port = tb.find_free_port()
        received = []

        class ServerProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                received.append(data)
                self.transport.write(b'echo:' + data)
                self.transport.close()

        async def main():
            server = await self.loop.create_server(
                ServerProtocol, '127.0.0.1', port
            )

            class ClientProtocol(asyncio.Protocol):
                def __init__(self):
                    self.received = []
                    self.done = asyncio.get_event_loop().create_future()

                def connection_made(self, transport):
                    self.transport = transport
                    transport.write(b'hello')

                def data_received(self, data):
                    self.received.append(data)

                def connection_lost(self, exc):
                    if not self.done.done():
                        self.done.set_result(self.received)

            transport, protocol = await self.loop.create_connection(
                ClientProtocol, '127.0.0.1', port
            )

            result = await protocol.done
            server.close()
            await server.wait_closed()
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, [b'echo:hello'])
        self.assertEqual(received, [b'hello'])

    def test_create_connection_with_existing_socket(self):
        """Test create_connection with existing socket."""
        port = tb.find_free_port()

        class ServerProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b'hello')
                transport.close()

        async def main():
            server = await self.loop.create_server(
                ServerProtocol, '127.0.0.1', port
            )

            # Create and connect socket manually
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))

            class ClientProtocol(asyncio.Protocol):
                def __init__(self):
                    self.data = bytearray()
                    self.done = asyncio.get_event_loop().create_future()

                def data_received(self, data):
                    self.data.extend(data)

                def connection_lost(self, exc):
                    if not self.done.done():
                        self.done.set_result(bytes(self.data))

            # Pass existing socket
            transport, protocol = await self.loop.create_connection(
                ClientProtocol, sock=sock
            )

            result = await protocol.done
            server.close()
            await server.wait_closed()
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, b'hello')


class _TestTransportProtocol:
    """Tests for Transport and Protocol interactions."""

    def test_protocol_callbacks(self):
        """Test protocol callback sequence."""
        callbacks = []

        class TestProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                callbacks.append('connection_made')
                self.transport = transport

            def data_received(self, data):
                callbacks.append(f'data_received:{data}')

            def eof_received(self):
                callbacks.append('eof_received')

            def connection_lost(self, exc):
                callbacks.append(f'connection_lost:{exc}')

        async def main():
            server = await self.loop.create_server(
                TestProtocol, '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]

            # Connect and send data
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('127.0.0.1', port))
            sock.sendall(b'hello')
            time.sleep(0.05)  # Give server time to process
            sock.close()

            await asyncio.sleep(0.1)
            server.close()
            await server.wait_closed()

        self.loop.run_until_complete(main())

        self.assertIn('connection_made', callbacks)
        self.assertTrue(any('data_received' in c for c in callbacks))

    def test_transport_write(self):
        """Test transport write operation."""
        received = bytearray()
        done = threading.Event()

        def server_thread(port):
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
            server.listen(1)
            server.settimeout(5)
            conn, _ = server.accept()
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                received.extend(data)
            conn.close()
            server.close()
            done.set()

        port = tb.find_free_port()
        thread = threading.Thread(target=server_thread, args=(port,))
        thread.start()
        time.sleep(0.05)

        class ClientProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b'hello ')
                transport.write(b'world')
                transport.close()

        async def main():
            await self.loop.create_connection(
                ClientProtocol, '127.0.0.1', port
            )
            await asyncio.sleep(0.1)

        self.loop.run_until_complete(main())
        done.wait(timeout=5)
        thread.join(timeout=5)

        self.assertEqual(bytes(received), b'hello world')

    def test_transport_close(self):
        """Test transport close operation."""
        closed = []

        class TestProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def connection_lost(self, exc):
                closed.append(exc)

        async def main():
            server = await self.loop.create_server(
                TestProtocol, '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]

            # Connect
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('127.0.0.1', port))
            sock.close()

            await asyncio.sleep(0.1)
            server.close()
            await server.wait_closed()

        self.loop.run_until_complete(main())

        # connection_lost should be called
        self.assertEqual(len(closed), 1)

    def test_transport_get_extra_info(self):
        """Test transport get_extra_info method."""
        extra_info = {}

        class TestProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                extra_info['socket'] = transport.get_extra_info('socket')
                extra_info['sockname'] = transport.get_extra_info('sockname')
                extra_info['peername'] = transport.get_extra_info('peername')
                extra_info['unknown'] = transport.get_extra_info('unknown', 'default')
                transport.close()

        async def main():
            server = await self.loop.create_server(
                TestProtocol, '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('127.0.0.1', port))
            await asyncio.sleep(0.1)
            sock.close()

            server.close()
            await server.wait_closed()

        self.loop.run_until_complete(main())

        self.assertIsNotNone(extra_info.get('socket'))
        self.assertIsNotNone(extra_info.get('sockname'))
        self.assertEqual(extra_info.get('unknown'), 'default')

    def test_transport_pause_resume_reading(self):
        """Test transport pause_reading and resume_reading."""
        data_chunks = []
        paused = []

        class TestProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                data_chunks.append(data)
                if len(data_chunks) == 1:
                    self.transport.pause_reading()
                    paused.append(True)
                    # Resume after a delay
                    asyncio.get_event_loop().call_later(
                        0.05, self.transport.resume_reading
                    )

        async def main():
            server = await self.loop.create_server(
                TestProtocol, '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]

            # Send data
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('127.0.0.1', port))
            sock.sendall(b'chunk1')
            await asyncio.sleep(0.01)
            sock.sendall(b'chunk2')
            await asyncio.sleep(0.1)
            sock.close()

            server.close()
            await server.wait_closed()

        self.loop.run_until_complete(main())

        # Should have paused and received data
        self.assertTrue(paused)
        self.assertTrue(len(data_chunks) >= 1)


class _TestLargeData:
    """Tests for large data transmission."""

    def test_large_data_transfer(self):
        """Test transferring large amounts of data."""
        data_size = 5 * 1024 * 1024  # 5 MB
        received = bytearray()

        class ServerProtocol(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                received.extend(data)

            def connection_lost(self, exc):
                pass

        async def main():
            server = await self.loop.create_server(
                ServerProtocol, '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]

            # Connect and send large data
            class ClientProtocol(asyncio.Protocol):
                def connection_made(self, transport):
                    self.transport = transport
                    # Send large data
                    transport.write(b'x' * data_size)
                    transport.close()

            await self.loop.create_connection(
                ClientProtocol, '127.0.0.1', port
            )

            # Wait for data to be received
            for _ in range(100):
                if len(received) >= data_size:
                    break
                await asyncio.sleep(0.1)

            server.close()
            await server.wait_closed()

            return len(received)

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, data_size)


class _TestServerSockets:
    """Tests for server socket management."""

    def test_server_sockets_property(self):
        """Test server.sockets property."""
        async def main():
            server = await self.loop.create_server(
                asyncio.Protocol, '127.0.0.1', 0
            )
            sockets = server.sockets
            self.assertIsInstance(sockets, tuple)
            self.assertEqual(len(sockets), 1)
            self.assertIsInstance(sockets[0], socket.socket)

            server.close()
            await server.wait_closed()

        self.loop.run_until_complete(main())

    def test_server_get_loop(self):
        """Test server.get_loop() method."""
        async def main():
            server = await self.loop.create_server(
                asyncio.Protocol, '127.0.0.1', 0
            )
            self.assertIs(server.get_loop(), self.loop)

            server.close()
            await server.wait_closed()

        self.loop.run_until_complete(main())

    def test_server_context_manager(self):
        """Test server as async context manager."""
        async def main():
            async with await self.loop.create_server(
                asyncio.Protocol, '127.0.0.1', 0
            ) as server:
                self.assertTrue(server.is_serving())
            # Should be closed after context exits

        self.loop.run_until_complete(main())


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangCreateServer(_TestCreateServer, tb.ErlangTestCase):
    pass


class TestAIOCreateServer(_TestCreateServer, tb.AIOTestCase):
    pass


class TestErlangCreateConnection(_TestCreateConnection, tb.ErlangTestCase):
    pass


class TestAIOCreateConnection(_TestCreateConnection, tb.AIOTestCase):
    pass


class TestErlangTransportProtocol(_TestTransportProtocol, tb.ErlangTestCase):
    pass


class TestAIOTransportProtocol(_TestTransportProtocol, tb.AIOTestCase):
    pass


class TestErlangLargeData(_TestLargeData, tb.ErlangTestCase):
    pass


class TestAIOLargeData(_TestLargeData, tb.AIOTestCase):
    pass


class TestErlangServerSockets(_TestServerSockets, tb.ErlangTestCase):
    pass


class TestAIOServerSockets(_TestServerSockets, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
