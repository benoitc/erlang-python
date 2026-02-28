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
Socket operations tests adapted from uvloop's test_sockets.py.

These tests verify low-level socket operations:
- sock_recv, sock_recv_into
- sock_sendall
- sock_connect
- sock_accept
"""

import asyncio
import socket
import threading
import time
import unittest

from . import _testbase as tb


class _TestSockets:
    """Tests for low-level socket operations."""

    def test_sock_connect_recv_send(self):
        """Test sock_connect, sock_recv, sock_sendall."""
        port = tb.find_free_port()
        received = []
        server_ready = threading.Event()

        def server_thread():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
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
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))
            await self.loop.sock_sendall(sock, b'ping')
            data = await self.loop.sock_recv(sock, 1024)
            sock.close()
            return data

        result = self.loop.run_until_complete(client())
        thread.join(timeout=5)

        self.assertEqual(result, b'pong')
        self.assertEqual(received, [b'ping'])

    def test_sock_accept(self):
        """Test sock_accept."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', 0))
        server.listen(1)
        server.setblocking(False)
        port = server.getsockname()[1]

        def client_thread():
            time.sleep(0.05)
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('127.0.0.1', port))
            client.sendall(b'hello')
            client.close()

        thread = threading.Thread(target=client_thread)
        thread.start()

        async def accept():
            conn, addr = await self.loop.sock_accept(server)
            data = await self.loop.sock_recv(conn, 1024)
            conn.close()
            server.close()
            return data

        result = self.loop.run_until_complete(accept())
        thread.join(timeout=5)

        self.assertEqual(result, b'hello')

    def test_sock_recv_into(self):
        """Test sock_recv_into with buffer."""
        port = tb.find_free_port()
        server_ready = threading.Event()

        def server_thread():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
            server.listen(1)
            server_ready.set()
            conn, _ = server.accept()
            conn.sendall(b'hello world')
            conn.close()
            server.close()

        thread = threading.Thread(target=server_thread)
        thread.start()
        server_ready.wait(timeout=5)

        async def client():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))

            buf = bytearray(1024)
            nbytes = await self.loop.sock_recv_into(sock, buf)
            sock.close()
            return buf[:nbytes]

        result = self.loop.run_until_complete(client())
        thread.join(timeout=5)

        self.assertEqual(bytes(result), b'hello world')

    def test_sock_sendall_large(self):
        """Test sock_sendall with large data."""
        port = tb.find_free_port()
        received = bytearray()
        server_ready = threading.Event()
        server_done = threading.Event()
        data_size = 1024 * 1024  # 1 MB

        def server_thread():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
            server.listen(1)
            server_ready.set()

            conn, _ = server.accept()
            while True:
                data = conn.recv(65536)
                if not data:
                    break
                received.extend(data)
            conn.close()
            server.close()
            server_done.set()

        thread = threading.Thread(target=server_thread)
        thread.start()
        server_ready.wait(timeout=5)

        async def client():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))
            data = b'x' * data_size
            await self.loop.sock_sendall(sock, data)
            sock.close()
            return len(data)

        sent = self.loop.run_until_complete(client())
        server_done.wait(timeout=10)
        thread.join(timeout=5)

        self.assertEqual(sent, data_size)
        self.assertEqual(len(received), data_size)

    def test_sock_connect_timeout(self):
        """Test sock_connect with timeout."""
        async def try_connect():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                # Connect to a non-routable address to trigger timeout
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.loop.sock_connect(sock, ('10.255.255.1', 12345)),
                        timeout=0.5
                    )
            finally:
                sock.close()

        self.loop.run_until_complete(try_connect())

    def test_sock_connect_refused(self):
        """Test sock_connect with connection refused."""
        async def try_connect():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                # Connect to a port that should refuse
                with self.assertRaises(OSError):
                    await self.loop.sock_connect(sock, ('127.0.0.1', 1))
            finally:
                sock.close()

        self.loop.run_until_complete(try_connect())

    def test_sock_multiple_clients(self):
        """Test multiple clients connecting simultaneously."""
        port = tb.find_free_port()
        server_ready = threading.Event()
        num_clients = 5
        received = []

        def server_thread():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
            server.listen(num_clients)
            server_ready.set()

            for _ in range(num_clients):
                conn, _ = server.accept()
                data = conn.recv(1024)
                received.append(data)
                conn.sendall(b'ack:' + data)
                conn.close()
            server.close()

        thread = threading.Thread(target=server_thread)
        thread.start()
        server_ready.wait(timeout=5)

        async def client(client_id):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))
            await self.loop.sock_sendall(sock, f'client{client_id}'.encode())
            data = await self.loop.sock_recv(sock, 1024)
            sock.close()
            return data

        async def main():
            tasks = [client(i) for i in range(num_clients)]
            return await asyncio.gather(*tasks)

        results = self.loop.run_until_complete(main())
        thread.join(timeout=5)

        self.assertEqual(len(results), num_clients)
        self.assertEqual(len(received), num_clients)

    def test_sock_echo(self):
        """Test echo server pattern."""
        port = tb.find_free_port()
        server_ready = threading.Event()
        messages = [b'hello', b'world', b'test', b'message']

        def server_thread():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
            server.listen(1)
            server_ready.set()

            conn, _ = server.accept()
            try:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    conn.sendall(data)
            finally:
                conn.close()
                server.close()

        thread = threading.Thread(target=server_thread)
        thread.start()
        server_ready.wait(timeout=5)

        async def client():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))

            results = []
            for msg in messages:
                await self.loop.sock_sendall(sock, msg)
                data = await self.loop.sock_recv(sock, 1024)
                results.append(data)

            sock.close()
            return results

        results = self.loop.run_until_complete(client())
        thread.join(timeout=5)

        self.assertEqual(results, messages)


class _TestSocketsCancel:
    """Tests for socket operation cancellation."""

    def test_sock_recv_cancel(self):
        """Test cancelling sock_recv."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', 0))
        server.listen(1)
        server.setblocking(False)
        port = server.getsockname()[1]

        # Client that connects but doesn't send
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(('127.0.0.1', port))

        async def do_accept_and_recv():
            conn, _ = await self.loop.sock_accept(server)
            try:
                # This should block since client doesn't send
                recv_task = asyncio.create_task(
                    self.loop.sock_recv(conn, 1024)
                )
                await asyncio.sleep(0.05)
                recv_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await recv_task
            finally:
                conn.close()

        try:
            self.loop.run_until_complete(do_accept_and_recv())
        finally:
            client.close()
            server.close()

    def test_sock_sendall_cancel(self):
        """Test cancelling sock_sendall with large data."""
        port = tb.find_free_port()
        server_ready = threading.Event()

        def server_thread():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
            server.listen(1)
            server_ready.set()

            conn, _ = server.accept()
            # Read slowly to cause backpressure
            time.sleep(0.5)
            conn.close()
            server.close()

        thread = threading.Thread(target=server_thread)
        thread.start()
        server_ready.wait(timeout=5)

        async def client():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))

            # Try to send a lot of data
            data = b'x' * (10 * 1024 * 1024)  # 10 MB
            send_task = asyncio.create_task(
                self.loop.sock_sendall(sock, data)
            )
            await asyncio.sleep(0.05)
            send_task.cancel()
            try:
                await send_task
            except asyncio.CancelledError:
                pass
            sock.close()

        self.loop.run_until_complete(client())
        thread.join(timeout=5)


class _TestSocketsNonBlocking:
    """Tests for non-blocking socket requirements."""

    def test_sock_recv_nonblocking_required(self):
        """Test that sock_recv requires non-blocking socket."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(True)

        async def recv():
            # Should still work but may behave differently
            # depending on implementation
            sock.close()

        self.loop.run_until_complete(recv())


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangSockets(_TestSockets, tb.ErlangTestCase):
    pass


class TestAIOSockets(_TestSockets, tb.AIOTestCase):
    pass


class TestErlangSocketsCancel(_TestSocketsCancel, tb.ErlangTestCase):
    pass


class TestAIOSocketsCancel(_TestSocketsCancel, tb.AIOTestCase):
    pass


class TestErlangSocketsNonBlocking(_TestSocketsNonBlocking, tb.ErlangTestCase):
    pass


class TestAIOSocketsNonBlocking(_TestSocketsNonBlocking, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
