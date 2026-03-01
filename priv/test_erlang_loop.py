#!/usr/bin/env python3
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
Test suite for ErlangEventLoop asyncio compatibility.

This test suite verifies that ErlangEventLoop can fully replace asyncio's
default event loop, similar to uvloop. Tests are adapted from uvloop's
test suite.

Run with:
    python test_erlang_loop.py

Or via Erlang:
    py:exec(<<"exec(open('priv/test_erlang_loop.py').read())">>).
"""

import asyncio
import gc
import os
import signal
import socket
import sys
import tempfile
import threading
import time
import unittest
import weakref

# Import the event loop
try:
    from _erlang_impl import ErlangEventLoop, get_event_loop_policy
except ImportError:
    # Try the erlang package
    from erlang import ErlangEventLoop
    from erlang._policy import ErlangEventLoopPolicy
    def get_event_loop_policy():
        return ErlangEventLoopPolicy()


def find_free_port():
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class ErlangLoopTestCase(unittest.TestCase):
    """Base test case for ErlangEventLoop tests."""

    def setUp(self):
        self.loop = ErlangEventLoop()
        self.exceptions = []

    def tearDown(self):
        if not self.loop.is_closed():
            self.loop.close()
        # Force garbage collection
        gc.collect()

    def loop_exception_handler(self, loop, context):
        """Custom exception handler that records exceptions."""
        self.exceptions.append(context)

    def run_briefly(self):
        """Run the loop briefly to process pending callbacks."""
        async def noop():
            pass
        self.loop.run_until_complete(noop())


class TestCallSoon(ErlangLoopTestCase):
    """Test call_soon functionality."""

    def test_call_soon_basic(self):
        """Test basic call_soon scheduling."""
        results = []

        def callback(x):
            results.append(x)

        self.loop.call_soon(callback, 1)
        self.loop.call_soon(callback, 2)
        self.loop.call_soon(callback, 3)

        self.run_briefly()

        self.assertEqual(results, [1, 2, 3])

    def test_call_soon_order(self):
        """Test that call_soon preserves order."""
        results = []

        for i in range(10):
            self.loop.call_soon(results.append, i)

        self.run_briefly()

        self.assertEqual(results, list(range(10)))

    def test_call_soon_cancel(self):
        """Test cancelling a call_soon handle."""
        results = []

        def callback(x):
            results.append(x)

        self.loop.call_soon(callback, 1)
        handle = self.loop.call_soon(callback, 2)
        self.loop.call_soon(callback, 3)

        handle.cancel()
        self.run_briefly()

        self.assertEqual(results, [1, 3])

    def test_call_soon_threadsafe(self):
        """Test call_soon_threadsafe from another thread."""
        results = []
        event = threading.Event()

        def callback(x):
            results.append(x)
            if x == 3:
                self.loop.stop()

        def thread_func():
            event.wait()
            self.loop.call_soon_threadsafe(callback, 2)
            self.loop.call_soon_threadsafe(callback, 3)

        self.loop.call_soon(callback, 1)
        thread = threading.Thread(target=thread_func)
        thread.start()

        self.loop.call_soon(event.set)
        self.loop.run_forever()

        thread.join()
        self.assertEqual(results, [1, 2, 3])


class TestCallLater(ErlangLoopTestCase):
    """Test call_later and call_at functionality."""

    def test_call_later_basic(self):
        """Test basic call_later scheduling."""
        results = []
        start = time.monotonic()

        def callback():
            results.append(time.monotonic() - start)
            self.loop.stop()

        self.loop.call_later(0.05, callback)
        self.loop.run_forever()

        self.assertEqual(len(results), 1)
        self.assertGreaterEqual(results[0], 0.04)

    def test_call_later_ordering(self):
        """Test that call_later respects timing order."""
        results = []

        def callback(x):
            results.append(x)
            if x == 3:
                self.loop.stop()

        # Schedule out of order
        self.loop.call_later(0.03, callback, 3)
        self.loop.call_later(0.01, callback, 1)
        self.loop.call_later(0.02, callback, 2)

        self.loop.run_forever()

        self.assertEqual(results, [1, 2, 3])

    def test_call_later_cancel(self):
        """Test cancelling a call_later handle."""
        results = []

        def callback(x):
            results.append(x)
            if len(results) == 2:
                self.loop.stop()

        self.loop.call_later(0.01, callback, 1)
        handle = self.loop.call_later(0.02, callback, 2)
        self.loop.call_later(0.03, callback, 3)

        handle.cancel()
        self.loop.run_forever()

        self.assertEqual(results, [1, 3])

    def test_call_later_zero_delay(self):
        """Test call_later with zero delay."""
        results = []

        def callback(x):
            results.append(x)

        self.loop.call_later(0, callback, 1)
        self.loop.call_later(0, callback, 2)

        self.run_briefly()

        self.assertEqual(results, [1, 2])

    def test_call_at(self):
        """Test call_at scheduling."""
        results = []
        now = self.loop.time()

        def callback():
            results.append(True)
            self.loop.stop()

        self.loop.call_at(now + 0.05, callback)
        self.loop.run_forever()

        self.assertEqual(results, [True])


class TestRunMethods(ErlangLoopTestCase):
    """Test run_forever, run_until_complete, stop, close."""

    def test_run_until_complete_basic(self):
        """Test run_until_complete with a coroutine."""
        async def coro():
            return 42

        result = self.loop.run_until_complete(coro())
        self.assertEqual(result, 42)

    def test_run_until_complete_future(self):
        """Test run_until_complete with a future."""
        future = self.loop.create_future()
        self.loop.call_soon(future.set_result, 'hello')
        result = self.loop.run_until_complete(future)
        self.assertEqual(result, 'hello')

    def test_run_forever_stop(self):
        """Test run_forever and stop."""
        results = []

        def callback():
            results.append(1)
            self.loop.stop()

        self.loop.call_soon(callback)
        self.loop.run_forever()

        self.assertEqual(results, [1])
        self.assertFalse(self.loop.is_running())

    def test_close(self):
        """Test closing the loop."""
        self.assertFalse(self.loop.is_closed())
        self.loop.close()
        self.assertTrue(self.loop.is_closed())

        # Should be idempotent
        self.loop.close()
        self.assertTrue(self.loop.is_closed())

    def test_close_running_raises(self):
        """Test that closing a running loop raises."""
        async def try_close():
            with self.assertRaises(RuntimeError):
                self.loop.close()

        self.loop.run_until_complete(try_close())

    def test_run_until_complete_nested_raises(self):
        """Test that nested run_until_complete raises."""
        async def outer():
            with self.assertRaises(RuntimeError):
                self.loop.run_until_complete(asyncio.sleep(0))

        self.loop.run_until_complete(outer())


class TestTasks(ErlangLoopTestCase):
    """Test task creation and management."""

    def test_create_task(self):
        """Test create_task."""
        async def coro():
            await asyncio.sleep(0.01)
            return 42

        async def main():
            task = self.loop.create_task(coro())
            result = await task
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, 42)

    def test_create_future(self):
        """Test create_future."""
        future = self.loop.create_future()
        self.assertIsInstance(future, asyncio.Future)
        self.assertFalse(future.done())

        self.loop.call_soon(future.set_result, 123)
        result = self.loop.run_until_complete(future)
        self.assertEqual(result, 123)

    def test_task_cancel(self):
        """Test task cancellation."""
        async def long_running():
            await asyncio.sleep(10)

        async def main():
            task = self.loop.create_task(long_running())
            await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.loop.run_until_complete(main())

    def test_gather(self):
        """Test asyncio.gather with our loop."""
        async def task(n):
            await asyncio.sleep(0.01)
            return n * 2

        async def main():
            results = await asyncio.gather(
                task(1), task(2), task(3)
            )
            return results

        results = self.loop.run_until_complete(main())
        self.assertEqual(results, [2, 4, 6])


class TestSockets(ErlangLoopTestCase):
    """Test socket operations."""

    def test_sock_connect_recv_send(self):
        """Test sock_connect, sock_recv, sock_sendall."""
        port = find_free_port()
        received = []

        def server_thread():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', port))
            server.listen(1)
            conn, _ = server.accept()
            data = conn.recv(1024)
            received.append(data)
            conn.sendall(b'pong')
            conn.close()
            server.close()

        thread = threading.Thread(target=server_thread)
        thread.start()
        time.sleep(0.05)  # Let server start

        async def client():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            await self.loop.sock_connect(sock, ('127.0.0.1', port))
            await self.loop.sock_sendall(sock, b'ping')
            data = await self.loop.sock_recv(sock, 1024)
            sock.close()
            return data

        result = self.loop.run_until_complete(client())
        thread.join()

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
        thread.join()

        self.assertEqual(result, b'hello')


class TestCreateConnection(ErlangLoopTestCase):
    """Test create_connection."""

    def test_create_connection_basic(self):
        """Test basic TCP connection."""
        port = find_free_port()
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
                    self.done = asyncio.Future()

                def connection_made(self, transport):
                    self.transport = transport
                    transport.write(b'hello')

                def data_received(self, data):
                    self.received.append(data)

                def connection_lost(self, exc):
                    self.done.set_result(self.received)

            transport, protocol = await self.loop.create_connection(
                ClientProtocol, '127.0.0.1', port
            )

            result = await protocol.done
            server.close()
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, [b'echo:hello'])
        self.assertEqual(received, [b'hello'])


class TestCreateServer(ErlangLoopTestCase):
    """Test create_server."""

    def test_create_server_basic(self):
        """Test basic TCP server."""
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


class TestDatagramEndpoint(ErlangLoopTestCase):
    """Test create_datagram_endpoint."""

    def test_udp_echo(self):
        """Test UDP echo server."""
        received = []

        class EchoServerProtocol(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                received.append(data)
                self.transport.sendto(b'echo:' + data, addr)

        class ClientProtocol(asyncio.DatagramProtocol):
            def __init__(self):
                self.received = []
                self.done = asyncio.Future()

            def datagram_received(self, data, addr):
                self.received.append(data)
                self.done.set_result(data)

        async def main():
            # Create server
            transport, _ = await self.loop.create_datagram_endpoint(
                EchoServerProtocol,
                local_addr=('127.0.0.1', 0)
            )
            server_addr = transport.get_extra_info('sockname')

            # Create client
            client_transport, client_protocol = await self.loop.create_datagram_endpoint(
                ClientProtocol,
                remote_addr=server_addr
            )

            client_transport.sendto(b'hello')
            result = await asyncio.wait_for(client_protocol.done, timeout=5.0)

            client_transport.close()
            transport.close()

            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, b'echo:hello')
        self.assertEqual(received, [b'hello'])


class TestExecutor(ErlangLoopTestCase):
    """Test run_in_executor."""

    def test_run_in_executor_basic(self):
        """Test basic executor usage."""
        def blocking_func(x):
            time.sleep(0.01)
            return x * 2

        async def main():
            result = await self.loop.run_in_executor(None, blocking_func, 21)
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, 42)

    def test_run_in_executor_multiple(self):
        """Test multiple executor tasks."""
        def blocking_func(x):
            time.sleep(0.01)
            return x

        async def main():
            tasks = [
                self.loop.run_in_executor(None, blocking_func, i)
                for i in range(5)
            ]
            results = await asyncio.gather(*tasks)
            return results

        results = self.loop.run_until_complete(main())
        self.assertEqual(sorted(results), [0, 1, 2, 3, 4])


class TestDNS(ErlangLoopTestCase):
    """Test DNS resolution."""

    def test_getaddrinfo(self):
        """Test getaddrinfo."""
        async def main():
            result = await self.loop.getaddrinfo(
                'localhost', 80,
                family=socket.AF_INET,
                type=socket.SOCK_STREAM
            )
            return result

        result = self.loop.run_until_complete(main())
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        # Check structure
        family, type_, proto, canonname, sockaddr = result[0]
        self.assertEqual(family, socket.AF_INET)
        self.assertEqual(type_, socket.SOCK_STREAM)


class TestExceptionHandling(ErlangLoopTestCase):
    """Test exception handling."""

    def test_default_exception_handler(self):
        """Test default exception handler."""
        self.loop.set_exception_handler(self.loop_exception_handler)

        def callback():
            raise ValueError("test error")

        self.loop.call_soon(callback)
        self.run_briefly()

        self.assertEqual(len(self.exceptions), 1)
        self.assertIn('exception', self.exceptions[0])
        self.assertIsInstance(self.exceptions[0]['exception'], ValueError)

    def test_custom_exception_handler(self):
        """Test custom exception handler."""
        errors = []

        def handler(loop, context):
            errors.append(context)

        self.loop.set_exception_handler(handler)

        def callback():
            raise RuntimeError("custom test")

        self.loop.call_soon(callback)
        self.run_briefly()

        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0]['exception'], RuntimeError)


class TestDebugMode(ErlangLoopTestCase):
    """Test debug mode."""

    def test_debug_mode(self):
        """Test debug mode toggle."""
        self.assertFalse(self.loop.get_debug())
        self.loop.set_debug(True)
        self.assertTrue(self.loop.get_debug())
        self.loop.set_debug(False)
        self.assertFalse(self.loop.get_debug())


class TestUnixSockets(ErlangLoopTestCase):
    """Test Unix socket operations."""

    def test_unix_server_client(self):
        """Test Unix socket server and client."""
        if sys.platform == 'win32':
            self.skipTest("Unix sockets not available on Windows")

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'test.sock')
            received = []

            class ServerProtocol(asyncio.Protocol):
                def connection_made(self, transport):
                    self.transport = transport

                def data_received(self, data):
                    received.append(data)
                    self.transport.write(b'ack')
                    self.transport.close()

            async def main():
                # Create Unix server
                server = await self.loop.create_unix_server(
                    ServerProtocol, path
                )

                # Connect client
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.setblocking(False)
                await self.loop.sock_connect(sock, path)
                await self.loop.sock_sendall(sock, b'unix test')
                data = await self.loop.sock_recv(sock, 1024)
                sock.close()

                server.close()
                await server.wait_closed()

                return data

            result = self.loop.run_until_complete(main())
            self.assertEqual(result, b'ack')
            self.assertEqual(received, [b'unix test'])


class TestPolicy(unittest.TestCase):
    """Test event loop policy."""

    def test_policy_creates_erlang_loop(self):
        """Test that the policy creates ErlangEventLoop."""
        policy = get_event_loop_policy()
        loop = policy.new_event_loop()
        self.assertIsInstance(loop, ErlangEventLoop)
        loop.close()


class TestAsyncioIntegration(ErlangLoopTestCase):
    """Test integration with asyncio APIs."""

    def test_asyncio_sleep(self):
        """Test asyncio.sleep works correctly."""
        async def main():
            start = time.monotonic()
            await asyncio.sleep(0.05)
            elapsed = time.monotonic() - start
            return elapsed

        elapsed = self.loop.run_until_complete(main())
        self.assertGreaterEqual(elapsed, 0.04)

    def test_asyncio_wait_for(self):
        """Test asyncio.wait_for."""
        async def slow():
            await asyncio.sleep(10)

        async def main():
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(slow(), timeout=0.05)

        self.loop.run_until_complete(main())

    def test_asyncio_shield(self):
        """Test asyncio.shield."""
        async def important():
            await asyncio.sleep(0.01)
            return "done"

        async def main():
            task = asyncio.shield(important())
            result = await task
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, "done")

    def test_asyncio_all_tasks(self):
        """Test asyncio.all_tasks."""
        async def bg_task():
            await asyncio.sleep(1)

        async def main():
            task = self.loop.create_task(bg_task())
            await asyncio.sleep(0)
            all_tasks = asyncio.all_tasks(self.loop)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return len(all_tasks)

        count = self.loop.run_until_complete(main())
        self.assertGreaterEqual(count, 1)

    def test_asyncio_current_task(self):
        """Test asyncio.current_task."""
        async def main():
            current = asyncio.current_task(self.loop)
            self.assertIsNotNone(current)
            return current

        task = self.loop.run_until_complete(main())
        self.assertIsInstance(task, asyncio.Task)


def run_tests():
    """Run all tests."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestCallSoon))
    suite.addTests(loader.loadTestsFromTestCase(TestCallLater))
    suite.addTests(loader.loadTestsFromTestCase(TestRunMethods))
    suite.addTests(loader.loadTestsFromTestCase(TestTasks))
    suite.addTests(loader.loadTestsFromTestCase(TestSockets))
    suite.addTests(loader.loadTestsFromTestCase(TestCreateConnection))
    suite.addTests(loader.loadTestsFromTestCase(TestCreateServer))
    suite.addTests(loader.loadTestsFromTestCase(TestDatagramEndpoint))
    suite.addTests(loader.loadTestsFromTestCase(TestExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestDNS))
    suite.addTests(loader.loadTestsFromTestCase(TestExceptionHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestDebugMode))
    suite.addTests(loader.loadTestsFromTestCase(TestUnixSockets))
    suite.addTests(loader.loadTestsFromTestCase(TestPolicy))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncioIntegration))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
