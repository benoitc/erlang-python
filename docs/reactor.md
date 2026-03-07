# Reactor Module

The `erlang.reactor` module provides low-level FD-based protocol handling for building custom servers. It enables Python to implement protocol logic while Erlang handles I/O scheduling via `enif_select`.

## Overview

The reactor pattern separates I/O multiplexing (handled by Erlang) from protocol logic (handled by Python). This provides:

- **Efficient I/O** - Erlang's `enif_select` for event notification
- **Protocol flexibility** - Python implements the protocol state machine
- **Zero-copy potential** - Direct fd access for high-throughput scenarios
- **Works with any fd** - TCP, UDP, Unix sockets, pipes, etc.

### Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                       Reactor Architecture                            │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Erlang (BEAM)                        Python                          │
│  ─────────────                        ──────                          │
│                                                                       │
│  ┌─────────────────────┐              ┌─────────────────────────────┐ │
│  │  py_reactor_context │              │      erlang.reactor         │ │
│  │                     │              │                             │ │
│  │  accept() ──────────┼──fd_handoff─▶│  init_connection(fd, info)  │ │
│  │                     │              │       │                     │ │
│  │  enif_select(READ)  │              │       ▼                     │ │
│  │       │             │              │  Protocol.connection_made() │ │
│  │       ▼             │              │                             │ │
│  │  {select, fd, READ} │              │                             │ │
│  │       │             │              │                             │ │
│  │       └─────────────┼─on_read_ready│  Protocol.data_received()   │ │
│  │                     │              │       │                     │ │
│  │  action = "write_   │◀─────────────┼───────┘                     │ │
│  │           pending"  │              │                             │ │
│  │       │             │              │                             │ │
│  │  enif_select(WRITE) │              │                             │ │
│  │       │             │              │                             │ │
│  │       ▼             │              │                             │ │
│  │  {select, fd, WRITE}│              │                             │ │
│  │       │             │              │                             │ │
│  │       └─────────────┼on_write_ready│  Protocol.write_ready()     │ │
│  │                     │              │                             │ │
│  └─────────────────────┘              └─────────────────────────────┘ │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

## Protocol Base Class

The `Protocol` class is the base for implementing custom protocols:

```python
import erlang.reactor as reactor

class Protocol(reactor.Protocol):
    """Base protocol attributes and methods."""

    # Set by reactor on connection
    fd: int           # File descriptor
    client_info: dict # Connection metadata from Erlang
    write_buffer: bytearray  # Buffer for outgoing data
    closed: bool      # Whether connection is closed
```

### Lifecycle Methods

#### `connection_made(fd, client_info)`

Called when a file descriptor is handed off from Erlang.

```python
def connection_made(self, fd: int, client_info: dict):
    """Called when fd is handed off from Erlang.

    Args:
        fd: File descriptor for the connection
        client_info: Dict with connection metadata
            - 'addr': Client IP address
            - 'port': Client port
            - 'type': Connection type (tcp, udp, unix, etc.)
    """
    # Initialize per-connection state
    self.request_buffer = bytearray()
```

#### `data_received(data) -> action`

Called when data has been read from the fd.

```python
def data_received(self, data: bytes) -> str:
    """Handle received data.

    Args:
        data: The bytes that were read

    Returns:
        Action string indicating what to do next
    """
    self.request_buffer.extend(data)

    if self.request_complete():
        self.prepare_response()
        return "write_pending"

    return "continue"  # Need more data
```

#### `write_ready() -> action`

Called when the fd is ready for writing.

```python
def write_ready(self) -> str:
    """Handle write readiness.

    Returns:
        Action string indicating what to do next
    """
    if not self.write_buffer:
        return "read_pending"

    written = self.write(bytes(self.write_buffer))
    del self.write_buffer[:written]

    if self.write_buffer:
        return "continue"  # More to write
    return "read_pending"  # Done writing
```

#### `connection_lost()`

Called when the connection is closed.

```python
def connection_lost(self):
    """Called when connection closes.

    Override to perform cleanup.
    """
    # Clean up resources
    self.cleanup()
```

### I/O Helper Methods

#### `read(size) -> bytes`

Read from the file descriptor:

```python
data = self.read(65536)  # Read up to 64KB
if not data:
    return "close"  # EOF or error
```

#### `write(data) -> int`

Write to the file descriptor:

```python
written = self.write(response_bytes)
del self.write_buffer[:written]  # Remove written bytes
```

## Action Return Values

Protocol methods return action strings that tell Erlang what to do next:

| Action | Description | Erlang Behavior |
|--------|-------------|-----------------|
| `"continue"` | Keep current mode | Re-register same event |
| `"write_pending"` | Ready to write | Switch to write mode (`enif_select` WRITE) |
| `"read_pending"` | Ready to read | Switch to read mode (`enif_select` READ) |
| `"close"` | Close connection | Close fd and call `connection_lost()` |

## Factory Pattern

Register a protocol factory to create protocol instances for each connection:

```python
import erlang.reactor as reactor

class MyProtocol(reactor.Protocol):
    # ... implementation

# Set the factory - called for each new connection
reactor.set_protocol_factory(MyProtocol)

# Get the protocol instance for an fd
proto = reactor.get_protocol(fd)
```

## Complete Example: Echo Protocol

Here's a complete echo server protocol:

```python
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    """Simple echo protocol - sends back whatever it receives."""

    def connection_made(self, fd, client_info):
        super().connection_made(fd, client_info)
        print(f"Connection from {client_info.get('addr')}:{client_info.get('port')}")

    def data_received(self, data):
        """Echo received data back to client."""
        if not data:
            return "close"

        # Buffer the data for writing
        self.write_buffer.extend(data)
        return "write_pending"

    def write_ready(self):
        """Write buffered data."""
        if not self.write_buffer:
            return "read_pending"

        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]

        if self.write_buffer:
            return "continue"  # More data to write
        return "read_pending"  # Done, wait for more input

    def connection_lost(self):
        print(f"Connection closed: fd={self.fd}")

# Register the factory
reactor.set_protocol_factory(EchoProtocol)
```

## Example: HTTP Protocol (Simplified)

```python
import erlang.reactor as reactor

class SimpleHTTPProtocol(reactor.Protocol):
    """Minimal HTTP/1.0 protocol."""

    def __init__(self):
        super().__init__()
        self.request_buffer = bytearray()

    def data_received(self, data):
        self.request_buffer.extend(data)

        # Check for end of headers
        if b'\r\n\r\n' in self.request_buffer:
            self.handle_request()
            return "write_pending"

        return "continue"

    def handle_request(self):
        """Parse request and prepare response."""
        request = self.request_buffer.decode('utf-8', errors='replace')
        first_line = request.split('\r\n')[0]
        method, path, _ = first_line.split(' ', 2)

        # Simple response
        body = f"Hello! You requested {path}"
        response = (
            f"HTTP/1.0 200 OK\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Content-Type: text/plain\r\n"
            f"\r\n"
            f"{body}"
        )
        self.write_buffer.extend(response.encode())

    def write_ready(self):
        if not self.write_buffer:
            return "close"  # HTTP/1.0: close after response

        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]

        if self.write_buffer:
            return "continue"
        return "close"

reactor.set_protocol_factory(SimpleHTTPProtocol)
```

## Integration with Erlang

### From Erlang: Starting a Reactor Server

```erlang
%% In your Erlang code
-module(my_server).
-export([start/1]).

start(Port) ->
    %% Set up the Python protocol factory first
    ok = py:exec(<<"
import erlang.reactor as reactor
from my_protocols import MyProtocol
reactor.set_protocol_factory(MyProtocol)
">>),

    %% Start accepting connections
    {ok, ListenSock} = gen_tcp:listen(Port, [binary, {active, false}, {reuseaddr, true}]),
    accept_loop(ListenSock).

accept_loop(ListenSock) ->
    {ok, ClientSock} = gen_tcp:accept(ListenSock),
    {ok, {Addr, Port}} = inet:peername(ClientSock),

    %% Hand off to Python reactor
    {ok, Fd} = inet:getfd(ClientSock),
    py_reactor_context:handoff(Fd, #{
        addr => inet:ntoa(Addr),
        port => Port,
        type => tcp
    }),

    accept_loop(ListenSock).
```

### How FDs Are Passed from Erlang to Python

1. Erlang accepts a connection and gets the socket fd
2. Erlang calls `py_reactor_context:handoff(Fd, ClientInfo)`
3. The NIF calls Python's `reactor.init_connection(fd, client_info)`
4. Protocol factory creates a new Protocol instance
5. `enif_select` is registered for read events on the fd
6. When events occur, Python callbacks handle the protocol logic

## Module API Reference

### `set_protocol_factory(factory)`

Set the factory function for creating protocols.

```python
reactor.set_protocol_factory(MyProtocol)
# or with a custom factory
reactor.set_protocol_factory(lambda: MyProtocol(custom_arg))
```

### `get_protocol(fd)`

Get the protocol instance for a file descriptor.

```python
proto = reactor.get_protocol(fd)
if proto:
    print(f"Protocol state: {proto.client_info}")
```

### `init_connection(fd, client_info)`

Internal - called by NIF on fd handoff.

### `on_read_ready(fd)`

Internal - called by NIF when fd is readable.

### `on_write_ready(fd)`

Internal - called by NIF when fd is writable.

### `close_connection(fd)`

Internal - called by NIF to close connection.

## See Also

- [Asyncio](asyncio.md) - Higher-level asyncio event loop for Python
- [Security](security.md) - Security sandbox documentation
- [Getting Started](getting-started.md) - Basic usage guide
