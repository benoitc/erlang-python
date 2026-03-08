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

"""Erlang Reactor - fd-based protocol layer.

This module provides a low-level fd-based API where Erlang handles I/O
scheduling via enif_select and Python handles protocol logic.

Works with any fd - TCP, UDP, Unix sockets, pipes, etc.

Example usage:

    import erlang.reactor as reactor

    class EchoProtocol(reactor.Protocol):
        def data_received(self, data):
            self.write_buffer.extend(data)
            return "write_pending"

        def write_ready(self):
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            return "continue" if self.write_buffer else "read_pending"

    reactor.set_protocol_factory(EchoProtocol)
"""

import os
from typing import Dict, Optional, Callable

__all__ = [
    'Protocol',
    'set_protocol_factory',
    'get_protocol',
    'init_connection',
    'on_read_ready',
    'on_write_ready',
    'close_connection',
    'signal_write_ready',
]


class Protocol:
    """Base protocol for Erlang reactor.

    Subclasses implement data_received() and write_ready() to handle
    I/O events. The base class provides buffer management and fd I/O.

    Attributes:
        fd: The file descriptor for this connection
        client_info: Dict with connection metadata (addr, port, type, etc.)
        write_buffer: Bytearray for buffering writes
        closed: Whether the connection is closed
    """

    def __init__(self):
        """Initialize protocol with empty state.

        Note: fd and client_info are set later via connection_made().
        """
        self.fd = -1
        self.client_info = {}
        self.write_buffer = bytearray()
        self.closed = False

    def connection_made(self, fd: int, client_info: dict):
        """Called when fd is handed off from Erlang.

        Args:
            fd: File descriptor for the connection
            client_info: Dict with connection metadata (e.g., addr, port, type)
        """
        self.fd = fd
        self.client_info = client_info

    def data_received(self, data: bytes) -> str:
        """Handle received data.

        Called when data has been read from the fd.

        Args:
            data: The bytes that were read. This is a bytes-like object
                  that supports the buffer protocol, indexing, slicing,
                  and common bytes methods (startswith, find, decode, etc.).
                  Can be used directly with memoryview() for zero-copy access.

        Returns:
            Action string:
            - "continue": More data expected, re-register for read
            - "write_pending": Response ready, switch to write mode
            - "close": Close the connection
        """
        raise NotImplementedError

    def write_ready(self) -> str:
        """Handle write readiness.

        Called when the fd is ready for writing.

        Returns:
            Action string:
            - "continue": More data to write, stay in write mode
            - "read_pending": Done writing, switch back to read mode
            - "close": Close the connection
        """
        raise NotImplementedError

    def connection_lost(self):
        """Called when connection closes.

        Override to perform cleanup when the connection ends.
        """
        pass

    def read(self, size: int = 65536) -> bytes:
        """Read from fd.

        Args:
            size: Maximum bytes to read

        Returns:
            Bytes read, or empty bytes on EOF/error
        """
        try:
            return os.read(self.fd, size)
        except (BlockingIOError, OSError):
            return b''

    def write(self, data: bytes) -> int:
        """Write to fd.

        Args:
            data: Bytes to write

        Returns:
            Number of bytes written, or 0 on error
        """
        try:
            return os.write(self.fd, data)
        except (BlockingIOError, OSError):
            return 0


# =============================================================================
# Registry
# =============================================================================

_protocols: Dict[int, Protocol] = {}
_reactor_pids: Dict[int, object] = {}  # fd -> reactor PID
_protocol_factory: Optional[Callable[[], Protocol]] = None


def set_protocol_factory(factory: Callable[[], Protocol]):
    """Set factory for creating protocols.

    The factory is called for each new connection to create a Protocol instance.

    Args:
        factory: Callable that returns a Protocol instance
    """
    global _protocol_factory
    _protocol_factory = factory


def get_protocol(fd: int) -> Optional[Protocol]:
    """Get the protocol instance for an fd.

    Args:
        fd: File descriptor

    Returns:
        Protocol instance or None if not found
    """
    return _protocols.get(fd)


# =============================================================================
# NIF callbacks (called by py_reactor_context)
# =============================================================================

def init_connection(fd: int, client_info: dict):
    """Called by NIF on fd_handoff.

    Creates a protocol instance using the factory and registers it.

    Args:
        fd: File descriptor
        client_info: Connection metadata from Erlang
    """
    global _protocols, _protocol_factory, _reactor_pids
    if _protocol_factory is not None:
        proto = _protocol_factory()
        proto.connection_made(fd, client_info)
        _protocols[fd] = proto
        # Store reactor PID for signal_write_ready
        reactor_pid = client_info.get('reactor_pid')
        if reactor_pid is not None:
            _reactor_pids[fd] = reactor_pid


def on_read_ready(fd: int, data: bytes = None) -> str:
    """Called by NIF when fd readable.

    Receives data from the NIF and passes it to the protocol.
    Data is provided as a zero-copy ReactorBuffer when available.

    Args:
        fd: File descriptor
        data: Read data from NIF (ReactorBuffer or bytes-like object).
              If None, falls back to calling proto.read() for compatibility.

    Returns:
        Action string from protocol.data_received()
    """
    proto = _protocols.get(fd)
    if proto is None:
        return "close"

    # Use data from NIF if provided, otherwise fall back to proto.read()
    if data is None:
        data = proto.read()

    if not data:
        return "close"
    return proto.data_received(data)


def on_write_ready(fd: int) -> str:
    """Called by NIF when fd writable.

    Calls the protocol's write_ready method.

    Args:
        fd: File descriptor

    Returns:
        Action string from protocol.write_ready()
    """
    proto = _protocols.get(fd)
    if proto is None:
        return "close"
    return proto.write_ready()


def close_connection(fd: int):
    """Called by NIF on close.

    Removes the protocol from the registry and calls connection_lost.

    Args:
        fd: File descriptor
    """
    proto = _protocols.pop(fd, None)
    _reactor_pids.pop(fd, None)
    if proto is not None:
        proto.closed = True
        proto.connection_lost()


def signal_write_ready(fd: int) -> bool:
    """Signal the reactor that a response is ready for the given fd.

    Call this after an async task completes and the response buffer is ready.
    The reactor will then trigger write selection for the fd.

    Args:
        fd: File descriptor with pending response

    Returns:
        True if signal was sent, False if no reactor PID registered
    """
    import erlang
    reactor_pid = _reactor_pids.get(fd)
    if reactor_pid is not None:
        erlang.send(reactor_pid, ('write_ready', fd))
        return True
    return False
