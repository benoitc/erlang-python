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
UDP protocol tests adapted from uvloop's test_udp.py.

These tests verify UDP/datagram operations:
- create_datagram_endpoint
- DatagramTransport and DatagramProtocol
- sendto and datagram_received
"""

import asyncio
import socket
import unittest

from . import _testbase as tb


class _TestCreateDatagramEndpoint:
    """Tests for create_datagram_endpoint functionality."""

    def test_create_datagram_endpoint_local(self):
        """Test creating a local UDP server."""
        received = []

        class ServerProtocol(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                received.append((data, addr))

            def error_received(self, exc):
                pass

        async def main():
            transport, protocol = await self.loop.create_datagram_endpoint(
                ServerProtocol,
                local_addr=('127.0.0.1', 0)
            )
            server_addr = transport.get_extra_info('sockname')
            self.assertIsNotNone(server_addr)

            transport.close()
            return server_addr

        addr = self.loop.run_until_complete(main())
        self.assertIsInstance(addr, tuple)

    def test_create_datagram_endpoint_remote(self):
        """Test creating a UDP client with remote address."""
        class ClientProtocol(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                pass

            def error_received(self, exc):
                pass

        async def main():
            # First create a server
            class ServerProtocol(asyncio.DatagramProtocol):
                def datagram_received(self, data, addr):
                    pass

            server_transport, _ = await self.loop.create_datagram_endpoint(
                ServerProtocol,
                local_addr=('127.0.0.1', 0)
            )
            server_addr = server_transport.get_extra_info('sockname')

            # Create client connected to server
            client_transport, _ = await self.loop.create_datagram_endpoint(
                ClientProtocol,
                remote_addr=server_addr
            )

            client_transport.close()
            server_transport.close()

        self.loop.run_until_complete(main())

    def test_udp_echo(self):
        """Test UDP echo server pattern."""
        received = []

        class EchoServerProtocol(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                received.append(data)
                self.transport.sendto(b'echo:' + data, addr)

            def connection_made(self, transport):
                self.transport = transport

        class ClientProtocol(asyncio.DatagramProtocol):
            def __init__(self):
                self.received = []
                self.done = None

            def connection_made(self, transport):
                self.done = asyncio.get_running_loop().create_future()

            def datagram_received(self, data, addr):
                self.received.append(data)
                self.done.set_result(data)

        async def main():
            # Create server
            server_transport, _ = await self.loop.create_datagram_endpoint(
                EchoServerProtocol,
                local_addr=('127.0.0.1', 0)
            )
            server_addr = server_transport.get_extra_info('sockname')

            # Create client
            client_transport, client_protocol = await self.loop.create_datagram_endpoint(
                ClientProtocol,
                remote_addr=server_addr
            )

            client_transport.sendto(b'hello')
            result = await asyncio.wait_for(client_protocol.done, timeout=5.0)

            client_transport.close()
            server_transport.close()

            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, b'echo:hello')
        self.assertEqual(received, [b'hello'])

    def test_udp_sendto_without_connect(self):
        """Test sendto without pre-connected remote address."""
        received = []

        class ServerProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, addr):
                received.append((data, addr))
                self.transport.sendto(b'ack', addr)

        class ClientProtocol(asyncio.DatagramProtocol):
            def __init__(self):
                self.received = []
                self.done = None

            def connection_made(self, transport):
                self.transport = transport
                self.done = asyncio.get_running_loop().create_future()

            def datagram_received(self, data, addr):
                self.received.append((data, addr))
                if not self.done.done():
                    self.done.set_result(data)

        async def main():
            # Create server
            server_transport, _ = await self.loop.create_datagram_endpoint(
                ServerProtocol,
                local_addr=('127.0.0.1', 0)
            )
            server_addr = server_transport.get_extra_info('sockname')

            # Create client without remote_addr
            client_transport, client_protocol = await self.loop.create_datagram_endpoint(
                ClientProtocol,
                local_addr=('127.0.0.1', 0)
            )

            # Send to specific address
            client_transport.sendto(b'test message', server_addr)
            result = await asyncio.wait_for(client_protocol.done, timeout=5.0)

            client_transport.close()
            server_transport.close()

            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, b'ack')

    def test_udp_multiple_messages(self):
        """Test multiple UDP messages."""
        messages = []

        class ServerProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, addr):
                messages.append(data)
                self.transport.sendto(data, addr)  # Echo back

        class ClientProtocol(asyncio.DatagramProtocol):
            def __init__(self):
                self.received = []
                self.expected = 0
                self.done = None

            def connection_made(self, transport):
                self.done = asyncio.get_running_loop().create_future()

            def datagram_received(self, data, addr):
                self.received.append(data)
                if len(self.received) >= self.expected:
                    if not self.done.done():
                        self.done.set_result(self.received)

        async def main():
            server_transport, _ = await self.loop.create_datagram_endpoint(
                ServerProtocol,
                local_addr=('127.0.0.1', 0)
            )
            server_addr = server_transport.get_extra_info('sockname')

            client_transport, client_protocol = await self.loop.create_datagram_endpoint(
                ClientProtocol,
                remote_addr=server_addr
            )

            client_protocol.expected = 3
            for i in range(3):
                client_transport.sendto(f'msg{i}'.encode())

            results = await asyncio.wait_for(client_protocol.done, timeout=5.0)

            client_transport.close()
            server_transport.close()

            return results

        results = self.loop.run_until_complete(main())
        self.assertEqual(len(results), 3)
        self.assertEqual(sorted(results), [b'msg0', b'msg1', b'msg2'])

    def test_udp_broadcast(self):
        """Test UDP with broadcast (requires allow_broadcast)."""
        class BroadcastProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

        async def main():
            transport, _ = await self.loop.create_datagram_endpoint(
                BroadcastProtocol,
                local_addr=('127.0.0.1', 0),
                allow_broadcast=True
            )

            # Just verify we can create with allow_broadcast
            sockname = transport.get_extra_info('sockname')
            self.assertIsNotNone(sockname)

            transport.close()

        self.loop.run_until_complete(main())


class _TestDatagramTransport:
    """Tests for DatagramTransport functionality."""

    def test_datagram_transport_close(self):
        """Test closing datagram transport."""
        close_called = []

        class TestProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def connection_lost(self, exc):
                close_called.append(exc)

        async def main():
            transport, protocol = await self.loop.create_datagram_endpoint(
                TestProtocol,
                local_addr=('127.0.0.1', 0)
            )

            self.assertFalse(transport.is_closing())
            transport.close()
            await asyncio.sleep(0.1)

        self.loop.run_until_complete(main())
        self.assertEqual(len(close_called), 1)

    def test_datagram_transport_abort(self):
        """Test aborting datagram transport."""
        class TestProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def connection_lost(self, exc):
                pass

        async def main():
            transport, _ = await self.loop.create_datagram_endpoint(
                TestProtocol,
                local_addr=('127.0.0.1', 0)
            )

            transport.abort()
            await asyncio.sleep(0.1)

        self.loop.run_until_complete(main())

    def test_datagram_transport_get_extra_info(self):
        """Test datagram transport get_extra_info."""
        extra_info = {}

        class TestProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                extra_info['socket'] = transport.get_extra_info('socket')
                extra_info['sockname'] = transport.get_extra_info('sockname')
                extra_info['unknown'] = transport.get_extra_info('unknown', 'default')

        async def main():
            transport, _ = await self.loop.create_datagram_endpoint(
                TestProtocol,
                local_addr=('127.0.0.1', 0)
            )
            await asyncio.sleep(0.01)
            transport.close()

        self.loop.run_until_complete(main())

        self.assertIsNotNone(extra_info.get('socket'))
        self.assertIsNotNone(extra_info.get('sockname'))
        self.assertEqual(extra_info.get('unknown'), 'default')

    def test_datagram_transport_write_buffer_size(self):
        """Test datagram transport get_write_buffer_size."""
        class TestProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

        async def main():
            transport, _ = await self.loop.create_datagram_endpoint(
                TestProtocol,
                local_addr=('127.0.0.1', 0)
            )

            size = transport.get_write_buffer_size()
            self.assertIsInstance(size, int)
            self.assertGreaterEqual(size, 0)

            transport.close()

        self.loop.run_until_complete(main())


class _TestDatagramProtocol:
    """Tests for DatagramProtocol callback behavior."""

    def test_error_received(self):
        """Test error_received callback."""
        errors = []

        class TestProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def error_received(self, exc):
                errors.append(exc)

        async def main():
            transport, _ = await self.loop.create_datagram_endpoint(
                TestProtocol,
                remote_addr=('127.0.0.1', 1)  # Connection refused
            )

            # Try to send to closed port
            transport.sendto(b'test')
            await asyncio.sleep(0.2)

            transport.close()

        self.loop.run_until_complete(main())
        # May or may not receive error depending on platform


class _TestUDPReuse:
    """Tests for UDP socket reuse options."""

    def test_udp_reuse_port(self):
        """Test UDP with reuse_port."""
        class TestProtocol(asyncio.DatagramProtocol):
            pass

        async def main():
            transport1, _ = await self.loop.create_datagram_endpoint(
                TestProtocol,
                local_addr=('127.0.0.1', 0),
                reuse_port=True
            )
            addr = transport1.get_extra_info('sockname')
            transport1.close()

            # Should be able to bind same address quickly
            transport2, _ = await self.loop.create_datagram_endpoint(
                TestProtocol,
                local_addr=addr,
                reuse_port=True
            )
            transport2.close()

        self.loop.run_until_complete(main())


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangCreateDatagramEndpoint(_TestCreateDatagramEndpoint, tb.ErlangTestCase):
    pass


class TestAIOCreateDatagramEndpoint(_TestCreateDatagramEndpoint, tb.AIOTestCase):
    pass


class TestErlangDatagramTransport(_TestDatagramTransport, tb.ErlangTestCase):
    pass


class TestAIODatagramTransport(_TestDatagramTransport, tb.AIOTestCase):
    pass


class TestErlangDatagramProtocol(_TestDatagramProtocol, tb.ErlangTestCase):
    pass


class TestAIODatagramProtocol(_TestDatagramProtocol, tb.AIOTestCase):
    pass


class TestErlangUDPReuse(_TestUDPReuse, tb.ErlangTestCase):
    pass


class TestAIOUDPReuse(_TestUDPReuse, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
