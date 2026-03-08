# Reactor Module

The `erlang.reactor` module provides low-level FD-based protocol handling for building custom servers. It enables Python to implement protocol logic while Erlang handles I/O scheduling via `enif_select`.

## Overview

The reactor pattern separates I/O multiplexing (handled by Erlang) from protocol logic (handled by Python). This provides:

- **Efficient I/O** - Erlang's `enif_select` for event notification
- **Protocol flexibility** - Python implements the protocol state machine
- **Zero-copy buffers** - ReactorBuffer provides zero-copy data access via buffer protocol
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

Called when data has been read from the fd. The `data` argument is a `ReactorBuffer` - a bytes-like object that supports zero-copy access via the buffer protocol.

```python
def data_received(self, data: bytes) -> str:
    """Handle received data.

    Args:
        data: A bytes-like object (ReactorBuffer) supporting:
            - Buffer protocol: memoryview(data) for zero-copy access
            - Indexing/slicing: data[0], data[0:10]
            - Bytes methods: data.startswith(), data.find(), data.decode()
            - Comparison: data == b'...'
            - Conversion: bytes(data) creates a copy

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

## Zero-Copy ReactorBuffer

The `data` argument passed to `data_received()` is a `ReactorBuffer` - a special bytes-like type that provides zero-copy access to read data. The data is read by the NIF before acquiring the GIL, and wrapped in a ReactorBuffer that exposes the memory via Python's buffer protocol.

### Benefits

- **No data copying** - Data goes directly from kernel to Python without intermediate copies
- **Transparent compatibility** - ReactorBuffer acts like `bytes` in all common operations
- **Memory efficiency** - Large payloads don't require extra allocations

### Supported Operations

ReactorBuffer supports all common bytes operations:

```python
def data_received(self, data):
    # Buffer protocol - zero-copy access
    mv = memoryview(data)
    first_byte = mv[0]

    # Indexing and slicing
    header = data[0:4]
    last_byte = data[-1]

    # Bytes methods
    if data.startswith(b'GET'):
        method = 'GET'
    pos = data.find(b'\r\n')
    count = data.count(b'/')

    # String conversion
    text = data.decode('utf-8')

    # Comparison
    if data == b'PING':
        self.write_buffer.extend(b'PONG')

    # Convert to bytes (creates a copy)
    data_copy = bytes(data)

    # 'in' operator
    if b'HTTP' in data:
        self.handle_http()

    # Length
    size = len(data)

    # Extend bytearray (uses buffer protocol)
    self.request_buffer.extend(data)

    return "continue"
```

### Performance Considerations

- For small reads (<1KB), the overhead of buffer management may exceed benefits
- For large reads (>=1KB), zero-copy provides 15-25% throughput improvement
- Use `memoryview()` for parsing without copying
- Call `bytes(data)` only when you need a persistent copy

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

## Passing Sockets from Erlang to Python

### Method 1: Socket FD Handoff to Reactor

The most efficient way is to hand off the socket's file descriptor directly:

```erlang
%% Erlang: Accept and hand off to Python reactor
{ok, ClientSock} = gen_tcp:accept(ListenSock),
{ok, {Addr, Port}} = inet:peername(ClientSock),

%% Get the raw file descriptor
{ok, Fd} = inet:getfd(ClientSock),

%% Hand off to Python - Erlang no longer owns this socket
py_reactor_context:handoff(Fd, #{
    addr => inet:ntoa(Addr),
    port => Port,
    type => tcp
}).
```

```python
# Python: Protocol handles the fd
import erlang.reactor as reactor

class MyProtocol(reactor.Protocol):
    def data_received(self, data):
        # self.fd is the socket fd from Erlang
        self.write_buffer.extend(b"Got: " + data)
        return "write_pending"

reactor.set_protocol_factory(MyProtocol)
```

### Method 2: Pass Socket FD to asyncio

For asyncio-based code, pass the fd and wrap it in Python:

```erlang
%% Erlang: Get fd and pass to Python
{ok, ClientSock} = gen_tcp:accept(ListenSock),
{ok, Fd} = inet:getfd(ClientSock),

%% Call Python with the fd
Ctx = py:context(1),
py:call(Ctx, my_handler, handle_connection, [Fd]).
```

```python
# Python: Wrap fd in asyncio
import asyncio
import socket

async def handle_connection(fd: int):
    # Create socket from fd (Python takes ownership)
    sock = socket.socket(fileno=fd)
    sock.setblocking(False)

    # Use asyncio streams
    reader, writer = await asyncio.open_connection(sock=sock)

    data = await reader.read(1024)
    writer.write(b"Echo: " + data)
    await writer.drain()
    writer.close()
    await writer.wait_closed()

def handle_connection_sync(fd: int):
    """Sync wrapper for Erlang call."""
    asyncio.run(handle_connection(fd))
```

### Method 3: Pass Socket Object via Pickle (Not Recommended)

For simple cases, you can pickle socket info, but this is less efficient:

```erlang
%% Erlang: Pass connection info
{ok, {Addr, Port}} = inet:peername(ClientSock),
py:call(Ctx, my_handler, connect_to, [Addr, Port]).
```

```python
# Python: Create new connection (less efficient - new socket)
import socket

def connect_to(addr: str, port: int):
    sock = socket.create_connection((addr, port))
    # ... use socket
```

### Socket Ownership

When passing an fd from Erlang to Python, you must decide who owns it:

**Option 1: Transfer ownership to Python**

Erlang gives up the fd entirely. Don't close the Erlang socket.

```erlang
{ok, ClientSock} = gen_tcp:accept(ListenSock),
{ok, Fd} = inet:getfd(ClientSock),
py_reactor_context:handoff(Fd, #{type => tcp}).
%% Don't close ClientSock - Python owns the fd now
```

**Option 2: Duplicate the fd (recommended)**

Use `py:dup_fd/1` to create an independent copy. Both sides can close their own fd.

```erlang
{ok, ClientSock} = gen_tcp:accept(ListenSock),
{ok, Fd} = inet:getfd(ClientSock),
{ok, DupFd} = py:dup_fd(Fd),
py_reactor_context:handoff(DupFd, #{type => tcp}),
gen_tcp:close(ClientSock).  %% Safe - Python has its own fd copy
```

This is safer because:
1. **Erlang controls its socket lifecycle** - GC won't affect Python
2. **Python has its own fd** - Independent of Erlang's socket
3. **No double-close issues** - Each side manages its own fd

## Integration with Erlang

### From Erlang: Starting a Reactor Server

```erlang
%% In your Erlang code
-module(my_server).
-export([start/1]).

start(Port) ->
    %% Set up the Python protocol factory first
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
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

### `on_read_ready(fd, data)`

Internal - called by NIF when fd is readable. The `data` argument is a `ReactorBuffer` containing the bytes read from the fd.

### `on_write_ready(fd)`

Internal - called by NIF when fd is writable.

### `close_connection(fd)`

Internal - called by NIF to close connection.

## See Also

- [Asyncio](asyncio.md) - Higher-level asyncio event loop for Python
- [Security](security.md) - Security sandbox documentation
- [Getting Started](getting-started.md) - Basic usage guide
