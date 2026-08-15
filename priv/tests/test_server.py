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

"""Unit tests for erlang.server (serve / adopt / stop_serving on handed-over fds).

The fds come from sockets created here and detached, which is what
py:dup_fd/1 produces on the Erlang side: a descriptor Python may own.
"""

import asyncio
import os
import socket
import unittest

from . import _testbase as tb


def _server_module():
    try:
        import erlang
        if hasattr(erlang, 'server'):
            return erlang.server
    except ImportError:
        pass
    from _erlang_impl import _server
    return _server


class Echo(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(b'echo:' + data)
        self.transport.close()


class _TestServe:

    def test_serve_tcp_on_listen_fd(self):
        server_mod = _server_module()
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(('127.0.0.1', 0))
        lsock.listen(16)
        port = lsock.getsockname()[1]
        fd = lsock.detach()  # what py:dup_fd hands over

        async def main():
            server = await server_mod.serve(fd, Echo)
            self.assertTrue(server.is_serving())
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setblocking(False)
            await self.loop.sock_connect(client, ('127.0.0.1', port))
            await self.loop.sock_sendall(client, b'hi')
            data = await self.loop.sock_recv(client, 1024)
            client.close()
            await server_mod.stop_serving(server)
            self.assertFalse(server.is_serving())
            return data

        self.assertEqual(self.loop.run_until_complete(main()), b'echo:hi')

    def test_serve_udp_on_bound_fd(self):
        server_mod = _server_module()
        usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        usock.bind(('127.0.0.1', 0))
        port = usock.getsockname()[1]
        fd = usock.detach()
        got = []

        class UDP(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, addr):
                got.append(data)
                self.transport.sendto(b'udp:' + data, addr)

        async def main():
            transport = await server_mod.serve(fd, UDP, udp=True)
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client.setblocking(False)
            client.sendto(b'ping', ('127.0.0.1', port))
            fut = self.loop.create_future()

            def on_ready():
                try:
                    fut.set_result(client.recv(1024))
                except BlockingIOError:
                    return
                self.loop.remove_reader(client.fileno())

            self.loop.add_reader(client.fileno(), on_ready)
            data = await asyncio.wait_for(fut, 5)
            client.close()
            await server_mod.stop_serving(transport, wait_closed=False)
            return data

        self.assertEqual(self.loop.run_until_complete(main()), b'udp:ping')
        self.assertEqual(got, [b'ping'])

    def test_adopt_connected_fd(self):
        server_mod = _server_module()
        a, b = socket.socketpair()
        fd = a.detach()
        b.setblocking(False)

        async def main():
            transport, protocol = await server_mod.adopt(fd, Echo)
            self.assertIsInstance(protocol, Echo)
            await self.loop.sock_sendall(b, b'x')
            data = await self.loop.sock_recv(b, 1024)
            b.close()
            return data

        self.assertEqual(self.loop.run_until_complete(main()), b'echo:x')

    def test_bad_fd_rejected(self):
        server_mod = _server_module()

        async def main():
            with self.assertRaises(ValueError):
                await server_mod.serve(-1, Echo)
            with self.assertRaises(ValueError):
                await server_mod.serve('nope', Echo)
            with self.assertRaises(OSError):
                await server_mod.serve(99999, Echo)
            with self.assertRaises(ValueError):
                await server_mod.adopt(-5, Echo)
            return 'ok'

        self.assertEqual(self.loop.run_until_complete(main()), 'ok')

    def test_wrong_socket_type_rejected(self):
        server_mod = _server_module()
        usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        usock.bind(('127.0.0.1', 0))
        fd = usock.detach()

        async def main():
            with self.assertRaises(ValueError):
                await server_mod.serve(fd, Echo)  # datagram fd, stream expected
            return 'ok'

        try:
            self.assertEqual(self.loop.run_until_complete(main()), 'ok')
        finally:
            os.close(fd)


class TestErlangServe(_TestServe, tb.ErlangTestCase):
    """erlang.server on ErlangEventLoop."""


class TestAIOServe(_TestServe, tb.AIOTestCase):
    """erlang.server on the stdlib loop (portable helpers)."""
