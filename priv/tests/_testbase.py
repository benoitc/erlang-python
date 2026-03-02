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
Test infrastructure for ErlangEventLoop tests.

This module provides the base test classes and utilities for testing
asyncio compatibility. The design follows uvloop's mixin pattern for
maximum test reuse across different event loop implementations.

Usage pattern (uvloop-style mixin):

    class _TestSockets:
        # All test methods - generic to any event loop
        def test_socket_accept_recv_send(self):
            ...

    class TestErlangSockets(_TestSockets, tb.ErlangTestCase):
        pass  # Runs against ErlangEventLoop

    class TestAIOSockets(_TestSockets, tb.AIOTestCase):
        pass  # Runs against asyncio
"""

import asyncio
import gc
import os
import socket
import ssl
import sys
import tempfile
import threading
import time
import unittest
from typing import Optional, Callable, Any

# Check for SSL support
try:
    import ssl as ssl_module
    HAVE_SSL = True
except ImportError:
    HAVE_SSL = False


def _is_inside_erlang_nif():
    """Check if we're running inside the Erlang NIF environment.

    Returns True if py_event_loop module is available, which indicates
    Python is embedded inside the Erlang NIF. In this environment,
    fork() operations will corrupt the Erlang VM.
    """
    try:
        import py_event_loop
        return True
    except ImportError:
        return False


INSIDE_ERLANG_NIF = _is_inside_erlang_nif()


# Subprocess is not supported in ErlangEventLoop.
# Python subprocess uses fork() which corrupts the Erlang VM.
# Use Erlang ports directly via erlang.call() instead.
HAS_SUBPROCESS_SUPPORT = False

# Markers for test filtering
ONLYUV = unittest.skipUnless(False, "uvloop-only test")
ONLYERL = object()  # Marker for Erlang-only tests


def find_free_port(host: str = '127.0.0.1') -> int:
    """Find a free TCP port on the given host.

    Args:
        host: Host address to bind to.

    Returns:
        An available port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return s.getsockname()[1]


def find_free_udp_port(host: str = '127.0.0.1') -> int:
    """Find a free UDP port on the given host.

    Args:
        host: Host address to bind to.

    Returns:
        An available port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind((host, 0))
        return s.getsockname()[1]


def make_unix_socket_path() -> str:
    """Create a temporary path for Unix socket.

    Returns:
        A path string suitable for Unix socket binding.
    """
    fd, path = tempfile.mkstemp(prefix='erlang_test_sock_')
    os.close(fd)
    os.unlink(path)
    return path


class BaseTestCase(unittest.TestCase):
    """Base test case for event loop tests.

    Subclasses must implement new_loop() to provide the event loop
    implementation to test.
    """

    def new_loop(self) -> asyncio.AbstractEventLoop:
        """Create a new event loop instance.

        Must be implemented by subclasses.

        Returns:
            A new event loop instance.
        """
        raise NotImplementedError

    def setUp(self):
        """Set up the test case."""
        self.loop = self.new_loop()
        self.exceptions = []

    def tearDown(self):
        """Tear down the test case."""
        if self.loop is not None and not self.loop.is_closed():
            # Cancel all pending tasks before closing
            try:
                pending = asyncio.all_tasks(self.loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self.loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception:
                pass

            # Shutdown async generators
            try:
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            except Exception:
                pass

            self.loop.close()

        # Force garbage collection to catch resource leaks
        gc.collect()
        gc.collect()
        gc.collect()

    def loop_exception_handler(
        self, loop: asyncio.AbstractEventLoop, context: dict
    ):
        """Custom exception handler that records exceptions.

        Args:
            loop: The event loop.
            context: Exception context dictionary.
        """
        self.exceptions.append(context)

    def run_briefly(self, *, timeout: float = 1.0):
        """Run the loop briefly to process pending callbacks.

        Args:
            timeout: Maximum time to run in seconds.
        """
        async def noop():
            pass
        self.loop.run_until_complete(
            asyncio.wait_for(noop(), timeout=timeout)
        )

    def run_until(
        self,
        predicate: Callable[[], bool],
        timeout: float = 5.0
    ):
        """Run the loop until predicate returns True or timeout.

        Args:
            predicate: Function that returns True when done.
            timeout: Maximum time to wait in seconds.

        Raises:
            TimeoutError: If predicate doesn't become True within timeout.
        """
        async def wait_for_predicate():
            deadline = time.monotonic() + timeout
            while not predicate():
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Condition not met within {timeout}s"
                    )
                await asyncio.sleep(0.01)

        self.loop.run_until_complete(wait_for_predicate())

    def suppress_log_errors(self):
        """Context manager to suppress error logging during test."""
        return _SuppressLogErrors(self.loop)

    # Assertion helpers

    def assertIsSubclass(self, cls, parent_cls, msg=None):
        """Assert that cls is a subclass of parent_cls."""
        if not issubclass(cls, parent_cls):
            self.fail(msg or f"{cls} is not a subclass of {parent_cls}")

    def assertRunsWithin(
        self, coro, timeout: float, msg: Optional[str] = None
    ):
        """Assert that a coroutine completes within timeout.

        Args:
            coro: Coroutine to run.
            timeout: Maximum time in seconds.
            msg: Optional failure message.
        """
        try:
            return self.loop.run_until_complete(
                asyncio.wait_for(coro, timeout=timeout)
            )
        except asyncio.TimeoutError:
            self.fail(msg or f"Coroutine did not complete within {timeout}s")


class _SuppressLogErrors:
    """Context manager that suppresses error logging."""

    def __init__(self, loop):
        self._loop = loop
        self._handler = None

    def __enter__(self):
        self._handler = self._loop.get_exception_handler()
        self._loop.set_exception_handler(lambda loop, ctx: None)
        return self

    def __exit__(self, *args):
        self._loop.set_exception_handler(self._handler)


class ErlangTestCase(BaseTestCase):
    """Test case for ErlangEventLoop.

    This class creates ErlangEventLoop instances for testing.
    """

    def new_loop(self) -> asyncio.AbstractEventLoop:
        """Create a new ErlangEventLoop instance.

        Returns:
            A new ErlangEventLoop instance.
        """
        # Try to use the unified erlang module (C module with Python extensions)
        try:
            import erlang
            if hasattr(erlang, 'new_event_loop'):
                return erlang.new_event_loop()
        except ImportError:
            pass

        # Try to import from _erlang_impl package
        try:
            from _erlang_impl import ErlangEventLoop
            return ErlangEventLoop()
        except ImportError:
            pass

        # Add parent directory to path and try again
        import sys
        import os
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)

        from _erlang_impl import ErlangEventLoop
        return ErlangEventLoop()


class AIOTestCase(BaseTestCase):
    """Test case for asyncio's default event loop.

    This class creates asyncio's SelectorEventLoop for comparison testing.
    """

    def new_loop(self) -> asyncio.AbstractEventLoop:
        """Create a new asyncio event loop instance.

        Returns:
            A new asyncio event loop instance.
        """
        return asyncio.new_event_loop()


# Convenience mixins for common test patterns

class ServerMixin:
    """Mixin providing server testing utilities."""

    def start_server(
        self,
        protocol_factory,
        host: str = '127.0.0.1',
        port: int = 0,
        **kwargs
    ):
        """Start a TCP server for testing.

        Args:
            protocol_factory: Factory for creating protocols.
            host: Host to bind to.
            port: Port to bind to (0 for auto).
            **kwargs: Additional server arguments.

        Returns:
            A tuple of (server, address) where address is (host, port).
        """
        async def create():
            server = await self.loop.create_server(
                protocol_factory, host, port, **kwargs
            )
            addr = server.sockets[0].getsockname()
            return server, addr

        return self.loop.run_until_complete(create())

    def connect_to_server(self, protocol_factory, host: str, port: int):
        """Connect to a server.

        Args:
            protocol_factory: Factory for creating protocols.
            host: Host to connect to.
            port: Port to connect to.

        Returns:
            A tuple of (transport, protocol).
        """
        async def connect():
            return await self.loop.create_connection(
                protocol_factory, host, port
            )

        return self.loop.run_until_complete(connect())


class SocketMixin:
    """Mixin providing socket testing utilities."""

    def create_tcp_socket_pair(self):
        """Create a connected pair of TCP sockets.

        Returns:
            A tuple of (server_sock, client_sock) both in non-blocking mode.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', 0))
        server.listen(1)
        server.setblocking(False)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setblocking(False)

        try:
            client.connect(server.getsockname())
        except BlockingIOError:
            pass

        conn, _ = server.accept()
        conn.setblocking(False)

        server.close()
        return conn, client

    def create_unix_socket_pair(self):
        """Create a connected pair of Unix sockets.

        Returns:
            A tuple of (server_sock, client_sock) both in non-blocking mode.
        """
        if sys.platform == 'win32':
            self.skipTest("Unix sockets not available on Windows")

        path = make_unix_socket_path()
        try:
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            server.setblocking(False)

            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.setblocking(False)

            try:
                client.connect(path)
            except BlockingIOError:
                pass

            conn, _ = server.accept()
            conn.setblocking(False)

            server.close()
            return conn, client
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass


class SSLMixin:
    """Mixin providing SSL testing utilities."""

    @staticmethod
    def create_ssl_context(*, server: bool = False) -> ssl.SSLContext:
        """Create an SSL context for testing.

        Args:
            server: If True, create a server context.

        Returns:
            An SSL context configured for testing.
        """
        if not HAVE_SSL:
            raise unittest.SkipTest("SSL not available")

        # Use a self-signed certificate for testing
        # In a real implementation, you would load actual certificates
        if server:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        else:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        return ctx


# Test protocol classes for common patterns

class EchoProtocol(asyncio.Protocol):
    """Protocol that echoes received data back to the sender."""

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)

    def connection_lost(self, exc):
        pass


class AccumulatingProtocol(asyncio.Protocol):
    """Protocol that accumulates received data."""

    def __init__(self):
        self.data = bytearray()
        self.done = None
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.done = asyncio.get_event_loop().create_future()

    def data_received(self, data):
        self.data.extend(data)

    def connection_lost(self, exc):
        if self.done and not self.done.done():
            if exc:
                self.done.set_exception(exc)
            else:
                self.done.set_result(bytes(self.data))

    def eof_received(self):
        pass


class EchoDatagramProtocol(asyncio.DatagramProtocol):
    """Datagram protocol that echoes received data back."""

    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.transport.sendto(data, addr)

    def error_received(self, exc):
        pass


class AccumulatingDatagramProtocol(asyncio.DatagramProtocol):
    """Datagram protocol that accumulates received data."""

    def __init__(self):
        self.data = []
        self.done = None
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.done = asyncio.get_event_loop().create_future()

    def datagram_received(self, data, addr):
        self.data.append((data, addr))

    def error_received(self, exc):
        if self.done and not self.done.done():
            self.done.set_exception(exc)


# Utility functions

def run_test_server(
    host: str,
    port: int,
    handler: Callable[[socket.socket, tuple], None],
    ready_event: Optional[threading.Event] = None
):
    """Run a simple test server in a thread.

    Args:
        host: Host to bind to.
        port: Port to bind to.
        handler: Function to handle connections.
        ready_event: Event to set when server is ready.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(1)

    if ready_event:
        ready_event.set()

    conn, addr = server.accept()
    try:
        handler(conn, addr)
    finally:
        conn.close()
        server.close()
