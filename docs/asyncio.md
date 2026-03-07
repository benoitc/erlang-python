# Erlang-native asyncio Event Loop

This guide covers the Erlang-native asyncio event loop implementation that provides high-performance async I/O for Python applications running within erlang_python.

## Overview

The `ErlangEventLoop` is a custom asyncio event loop backed by Erlang's scheduler using `enif_select` for I/O multiplexing. This replaces Python's polling-based event loop with true event-driven callbacks integrated into the BEAM VM.

All asyncio functionality is available through the unified `erlang` module:

```python
import erlang

# Preferred way to run async code
erlang.run(main())
```

### Key Benefits

- **Sub-millisecond latency** - Events are delivered immediately via Erlang messages instead of polling every 10ms
- **Zero CPU usage when idle** - No busy-waiting or polling overhead
- **Full GIL release during waits** - Python's Global Interpreter Lock is released while waiting for events
- **Native Erlang scheduler integration** - I/O events are handled by BEAM's scheduler

### Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          ErlangEventLoop Architecture                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Python (asyncio)                    Erlang (BEAM)                          │
│   ────────────────                    ─────────────                          │
│                                                                              │
│   ┌──────────────────┐                ┌────────────────────────────────────┐ │
│   │  ErlangEventLoop │                │           py_event_worker          │ │
│   │                  │                │                                    │ │
│   │  call_later()  ──┼─{timer,ms,id}─▶│  erlang:send_after(ms, self, {})   │ │
│   │  call_at()       │                │         │                          │ │
│   │                  │                │         ▼                          │ │
│   │  add_reader()  ──┼──{add_fd,fd}──▶│  enif_select(fd, READ)             │ │
│   │  add_writer()    │                │         │                          │ │
│   │                  │                │         ▼                          │ │
│   │                  │◀──{fd_ready}───│  handle_info({select, ...})        │ │
│   │                  │◀──{timeout}────│  handle_info({timeout, ...})       │ │
│   │                  │                │                                    │ │
│   │  _run_once()     │                └────────────────────────────────────┘ │
│   │      │           │                                                       │
│   │      ▼           │                ┌────────────────────────────────────┐ │
│   │  process pending │                │           py_event_router          │ │
│   │  callbacks       │                │                                    │ │
│   └──────────────────┘                │  Routes events to correct loop     │ │
│                                       │  based on resource backref         │ │
│   ┌──────────────────┐                └────────────────────────────────────┘ │
│   │  erlang_asyncio  │                                                       │
│   │                  │                ┌────────────────────────────────────┐ │
│   │  sleep()       ──┼─{sleep_wait}──▶│  erlang:send_after() + cond_wait   │ │
│   │  gather()        │                │                                    │ │
│   │  wait_for()      │◀──{complete}───│  pthread_cond_broadcast()          │ │
│   │  create_task()   │                └────────────────────────────────────┘ │
│   └──────────────────┘                                                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Components:**

| Component | Role |
|-----------|------|
| `ErlangEventLoop` | Python asyncio event loop using Erlang for I/O and timers |
| `py_event_worker` | Erlang gen_server managing FDs and timers for a Python context |
| `py_event_router` | Routes timer/FD events to the correct event loop instance |
| `erlang_asyncio` | High-level asyncio-compatible API with direct Erlang integration |

## Usage Patterns

### Pattern 1: `erlang.run()` (Recommended)

The preferred way to run async code, matching uvloop's API:

```python
import erlang

async def main():
    await asyncio.sleep(1.0)  # Uses erlang:send_after internally
    print("Done!")

# Simple and clean
erlang.run(main())
```

### Pattern 2: With `asyncio.Runner` (Python 3.11+)

```python
import asyncio
import erlang

with asyncio.Runner(loop_factory=erlang.new_event_loop) as runner:
    runner.run(main())
```

### Pattern 3: `erlang.install()` (Deprecated in Python 3.12+)

This pattern installs the ErlangEventLoopPolicy globally. It's deprecated in Python 3.12+ because `asyncio.run()` no longer respects global policies:

```python
import asyncio
import erlang

erlang.install()  # Deprecated in 3.12+, use erlang.run() instead
asyncio.run(main())
```

### Pattern 4: Manual Loop Management

For cases where you need direct control:

```python
import asyncio
import erlang

loop = erlang.new_event_loop()
asyncio.set_event_loop(loop)
try:
    loop.run_until_complete(main())
finally:
    loop.close()
```

## Execution Mode Detection

The `erlang` module can detect the Python execution mode:

```python
from erlang import detect_mode, ExecutionMode

mode = detect_mode()
if mode == ExecutionMode.FREE_THREADED:
    print("Running in free-threaded mode (no GIL)")
elif mode == ExecutionMode.SUBINTERP:
    print("Running in subinterpreter with per-interpreter GIL")
else:
    print("Running with shared GIL")
```

**ExecutionMode values:**
- `FREE_THREADED` - Python 3.13+ with `Py_GIL_DISABLED` (no GIL)
- `SUBINTERP` - Python 3.12+ running in a subinterpreter
- `SHARED_GIL` - Traditional Python with shared GIL

## TCP Support

### Client Connections

Use `create_connection()` to establish TCP client connections:

```python
import asyncio

class EchoClientProtocol(asyncio.Protocol):
    def __init__(self, message, on_con_lost):
        self.message = message
        self.on_con_lost = on_con_lost

    def connection_made(self, transport):
        transport.write(self.message.encode())

    def data_received(self, data):
        print(f'Received: {data.decode()}')

    def connection_lost(self, exc):
        self.on_con_lost.set_result(True)

async def main():
    loop = asyncio.get_running_loop()
    on_con_lost = loop.create_future()

    transport, protocol = await loop.create_connection(
        lambda: EchoClientProtocol('Hello, World!', on_con_lost),
        host='127.0.0.1',
        port=8888
    )

    try:
        await on_con_lost
    finally:
        transport.close()
```

### TCP Servers

Use `create_server()` to create TCP servers:

```python
import asyncio

class EchoServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print(f'Connection from {peername}')
        self.transport = transport

    def data_received(self, data):
        message = data.decode()
        print(f'Received: {message}')
        # Echo back
        self.transport.write(data)

    def connection_lost(self, exc):
        print('Connection closed')

async def main():
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        EchoServerProtocol,
        host='127.0.0.1',
        port=8888,
        reuse_address=True
    )

    async with server:
        await server.serve_forever()
```

### Transport Class

The `_ErlangSocketTransport` class implements the asyncio Transport interface with these features:

- Non-blocking I/O using Erlang's `enif_select`
- Write buffering with automatic drain
- Connection lifecycle management (`connection_made`, `connection_lost`, `eof_received`)
- Extra info access via `get_extra_info()` (socket, sockname, peername)

## UDP/Datagram Support

The event loop provides full UDP/datagram support through `create_datagram_endpoint()`.

### Creating UDP Endpoints

```python
import asyncio

class EchoUDPProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        print(f'Listening on {transport.get_extra_info("sockname")}')

    def datagram_received(self, data, addr):
        message = data.decode()
        print(f'Received {message!r} from {addr}')
        # Echo back to sender
        self.transport.sendto(data, addr)

    def error_received(self, exc):
        print(f'Error received: {exc}')

    def connection_lost(self, exc):
        print('Connection closed')

async def main():
    loop = asyncio.get_running_loop()

    # Create UDP server
    transport, protocol = await loop.create_datagram_endpoint(
        EchoUDPProtocol,
        local_addr=('127.0.0.1', 9999)
    )

    try:
        await asyncio.sleep(3600)  # Run for an hour
    finally:
        transport.close()
```

### Parameters

The `create_datagram_endpoint()` method accepts these parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `protocol_factory` | callable | Factory function returning a `DatagramProtocol` |
| `local_addr` | tuple | Local `(host, port)` to bind to |
| `remote_addr` | tuple | Remote `(host, port)` to connect to (optional) |
| `family` | int | Socket family (`AF_INET` or `AF_INET6`) |
| `proto` | int | Socket protocol number |
| `flags` | int | `getaddrinfo` flags |
| `reuse_address` | bool | Allow reuse of local address (`SO_REUSEADDR`) |
| `reuse_port` | bool | Allow reuse of local port (`SO_REUSEPORT`) |
| `allow_broadcast` | bool | Allow sending to broadcast addresses (`SO_BROADCAST`) |
| `sock` | socket | Pre-existing socket to use (overrides other options) |

### DatagramProtocol Callbacks

Implement these callbacks in your `DatagramProtocol`:

```python
class MyDatagramProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        """Called when the endpoint is ready."""
        self.transport = transport

    def datagram_received(self, data, addr):
        """Called when a datagram is received.

        Args:
            data: The received bytes
            addr: The sender's (host, port) tuple
        """
        pass

    def error_received(self, exc):
        """Called when a send or receive operation fails.

        Args:
            exc: The exception that occurred
        """
        pass

    def connection_lost(self, exc):
        """Called when the transport is closed.

        Args:
            exc: Exception if abnormal close, None otherwise
        """
        pass
```

### Connected vs Unconnected UDP

**Unconnected UDP** (default): Each datagram can be sent to any destination:

```python
# Server can send to any client
transport, protocol = await loop.create_datagram_endpoint(
    MyProtocol,
    local_addr=('0.0.0.0', 9999)
)
# Send to different destinations
transport.sendto(b'Hello', ('192.168.1.100', 5000))
transport.sendto(b'World', ('192.168.1.101', 5000))
```

**Connected UDP**: The endpoint is bound to a specific remote address:

```python
# Client connected to specific server
transport, protocol = await loop.create_datagram_endpoint(
    MyProtocol,
    remote_addr=('127.0.0.1', 9999)
)
# Can use sendto without address
transport.sendto(b'Hello')  # Goes to connected address
```

### Example: UDP Echo Server and Client

**Server:**

```python
import asyncio
from erlang import ErlangEventLoop

class EchoServerProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        sockname = transport.get_extra_info('sockname')
        print(f'UDP Echo Server listening on {sockname}')

    def datagram_received(self, data, addr):
        print(f'Received {len(data)} bytes from {addr}')
        # Echo back
        self.transport.sendto(data, addr)

async def run_server():
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        EchoServerProtocol,
        local_addr=('127.0.0.1', 9999)
    )
    try:
        await asyncio.sleep(3600)
    finally:
        transport.close()

asyncio.run(run_server())
```

**Client:**

```python
import asyncio

class EchoClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, message, on_response):
        self.message = message
        self.on_response = on_response
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        print(f'Sending: {self.message}')
        transport.sendto(self.message.encode())

    def datagram_received(self, data, addr):
        print(f'Received: {data.decode()} from {addr}')
        self.on_response.set_result(data)

    def error_received(self, exc):
        print(f'Error: {exc}')

async def run_client():
    loop = asyncio.get_running_loop()
    on_response = loop.create_future()

    transport, _ = await loop.create_datagram_endpoint(
        lambda: EchoClientProtocol('Hello UDP!', on_response),
        remote_addr=('127.0.0.1', 9999)
    )

    try:
        response = await asyncio.wait_for(on_response, timeout=5.0)
        print(f'Echo response: {response.decode()}')
    finally:
        transport.close()

asyncio.run(run_client())
```

### Broadcast UDP

Enable broadcast for sending to broadcast addresses:

```python
transport, protocol = await loop.create_datagram_endpoint(
    MyProtocol,
    local_addr=('0.0.0.0', 0),
    allow_broadcast=True
)
# Send to broadcast address
transport.sendto(b'Broadcast message', ('255.255.255.255', 9999))
```

## Performance

The event loop includes several optimizations for high-throughput applications.

### Built-in Optimizations

| Optimization | Description | Impact |
|-------------|-------------|--------|
| **Cached function lookups** | `ast.literal_eval` cached at module init | Avoids import per callback |
| **O(1) timer cancellation** | Handle-to-callback reverse map | Was O(n) iteration |
| **O(1) duplicate detection** | Hash set for pending events | Was O(n) linear scan |
| **Lock-free event consumption** | Detach queue under lock, process outside | Reduced contention |
| **Object pooling** | Reuse event structures via freelist | Fewer allocations |
| **Deque method caching** | Pre-bound `append`/`popleft` methods | Avoids attribute lookup |

### Performance Build

For maximum performance, rebuild with the `PERF_BUILD` cmake option:

```bash
# Clean build with performance optimizations
rm -rf _build/cmake
mkdir -p _build/cmake && cd _build/cmake
cmake ../../c_src -DPERF_BUILD=ON
cmake --build .
```

This enables:
- `-O3` optimization level
- Link-Time Optimization (LTO)
- `-march=native` (CPU-specific optimizations)
- `-ffast-math` and `-funroll-loops`

**Note:** Performance builds are not portable across different CPU architectures due to `-march=native`.

### Benchmarking

Run the event loop benchmarks to measure performance:

```bash
python3 examples/benchmark_event_loop.py
```

Example output:
```
Timer throughput: 150,000 timers/sec
Callback dispatch: 200,000 callbacks/sec
I/O ready detection: <1ms latency
```

## Low-level APIs

The event loop is backed by NIFs (Native Implemented Functions) that provide direct access to Erlang's event system. These are primarily for internal use and testing.

### Event Loop NIFs

From Erlang, you can access the low-level event loop operations:

```erlang
%% Create a new event loop instance
{ok, LoopRef} = py_nif:event_loop_new().

%% Add a reader callback for a file descriptor
{ok, FdRef} = py_nif:add_reader(LoopRef, Fd, CallbackId).

%% Remove a reader
ok = py_nif:remove_reader(LoopRef, FdRef).

%% Poll for events (returns number of events ready)
NumEvents = py_nif:poll_events(LoopRef, TimeoutMs).

%% Get pending callback events
Pending = py_nif:get_pending(LoopRef).
%% Returns: [{CallbackId, read|write}, ...]

%% Destroy the event loop
py_nif:event_loop_destroy(LoopRef).
```

### UDP Socket NIFs (for testing)

```erlang
%% Create a UDP socket bound to a port
{ok, {Fd, Port}} = py_nif:create_test_udp_socket(0).  % 0 = ephemeral port

%% Send data via UDP
ok = py_nif:sendto_test_udp(Fd, <<"hello">>, <<"127.0.0.1">>, 9999).

%% Receive data
{ok, {Data, {Host, Port}}} = py_nif:recvfrom_test_udp(Fd, MaxBytes).

%% Set broadcast option
ok = py_nif:set_udp_broadcast(Fd, true).

%% Close the socket
py_nif:close_test_fd(Fd).
```

## Integration with Erlang

The event loop integrates with Erlang's message passing system through a router process:

```erlang
%% Start the event router
{ok, LoopRef} = py_nif:event_loop_new(),
{ok, RouterPid} = py_event_router:start_link(LoopRef),
ok = py_nif:event_loop_set_router(LoopRef, RouterPid).
```

Events are delivered as Erlang messages, enabling the event loop to participate in BEAM's supervision trees and distributed computing capabilities.

## Event Loop Architecture

Each `ErlangEventLoop` instance has its own isolated capsule with a dedicated pending queue. This ensures that timers and FD events are properly routed to the correct loop instance.

### Multi-threaded Example

```python
from erlang import ErlangEventLoop
import threading

def run_tasks(loop_id):
    """Each thread gets its own event loop."""
    loop = ErlangEventLoop()

    results = []

    def callback():
        results.append(loop_id)

    # Schedule callbacks - isolated to this loop
    loop.call_soon(callback)
    loop.call_later(0.01, callback)

    # Process events
    import time
    deadline = time.time() + 0.5
    while time.time() < deadline and len(results) < 2:
        loop._run_once()
        time.sleep(0.01)

    loop.close()
    return results

# Run in separate threads
t1 = threading.Thread(target=run_tasks, args=('loop_a',))
t2 = threading.Thread(target=run_tasks, args=('loop_b',))

t1.start()
t2.start()
t1.join()
t2.join()
# Each thread only sees its own callbacks
```

### Internal Architecture

A shared router process handles timer and FD events for all loops:

```
┌─────────────────────────────────────────────────────────────────┐
│                     py_event_router (shared)                    │
│                                                                 │
│  Receives:                                                      │
│  - Timer expirations from erlang:send_after                    │
│  - FD ready events from enif_select                            │
│                                                                 │
│  Dispatches to correct loop via resource backref                │
└─────────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │                    │                    │
    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
    │ Loop A  │          │ Loop B  │          │ Loop C  │
    │ pending │          │ pending │          │ pending │
    └─────────┘          └─────────┘          └─────────┘
```

Each loop has its own pending queue, ensuring callbacks are processed only by the loop that scheduled them. The shared router dispatches timer and FD events to the correct loop based on the capsule backref.

## Erlang Asyncio Primitives

> **Note:** The `erlang_asyncio` module has been unified into the main `erlang` module. Use `import erlang` and `erlang.run()` instead.

The `erlang` module provides asyncio-compatible primitives that use Erlang's native scheduler for maximum performance. This is the recommended way to use async/await patterns when you need explicit Erlang timer integration.

### Overview

Unlike the standard `asyncio` module which uses Python's polling-based event loop, the `erlang` module uses Erlang's `erlang:send_after/3` for timers and integrates directly with the BEAM scheduler. This eliminates Python event loop overhead (~0.5-1ms per operation) and provides more precise timing.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         erlang_asyncio.sleep()                          │
│                                                                         │
│   Python                           Erlang                               │
│   ──────                           ──────                               │
│                                                                         │
│   ┌─────────────────┐              ┌─────────────────────────────────┐  │
│   │ erlang_asyncio  │              │         py_event_worker         │  │
│   │    .sleep(0.1)  │              │                                 │  │
│   └────────┬────────┘              │  handle_info({sleep_wait,...})  │  │
│            │                       │         │                       │  │
│            ▼                       │         ▼                       │  │
│   ┌─────────────────┐              │  erlang:send_after(100ms)       │  │
│   │ py_event_loop.  │──{sleep_wait,│         │                       │  │
│   │ _erlang_sleep() │   100, Id}──▶│         ▼                       │  │
│   └────────┬────────┘              │  handle_info({sleep_complete})  │  │
│            │                       │         │                       │  │
│   ┌────────▼────────┐              │         ▼                       │  │
│   │  Release GIL    │              │  py_nif:dispatch_sleep_complete │  │
│   │  pthread_cond_  │◀─────────────│         │                       │  │
│   │     wait()      │   signal     └─────────┼───────────────────────┘  │
│   └────────┬────────┘                        │                          │
│            │                                 │                          │
│            ▼                                 ▼                          │
│   ┌─────────────────┐              ┌─────────────────────────────────┐  │
│   │  Reacquire GIL  │              │  pthread_cond_broadcast()       │  │
│   │  Return result  │              │  (wakes Python thread)          │  │
│   └─────────────────┘              └─────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key features:**
- **GIL released during sleep** - Python thread doesn't hold the GIL while waiting
- **BEAM scheduler integration** - Uses Erlang's native timer system
- **Zero CPU usage** - Condition variable wait, no polling
- **Sub-millisecond precision** - Timers managed by BEAM scheduler

### Basic Usage

```python
import erlang
import asyncio

async def my_handler():
    # Sleep using Erlang's timer system
    await asyncio.sleep(0.1)  # 100ms - uses erlang:send_after internally
    return "done"

# Run a coroutine with Erlang event loop
result = erlang.run(my_handler())
```

### API Reference

When using `erlang.run()` or the Erlang event loop, all standard asyncio functions work seamlessly with Erlang's backend.

#### asyncio.sleep(delay)

Sleep for the specified delay. Uses Erlang's `erlang:send_after/3` internally.

```python
import erlang
import asyncio

async def example():
    # Simple sleep - uses Erlang timer system
    await asyncio.sleep(0.05)  # 50ms

erlang.run(example())
```

#### erlang.run(coro)

Run a coroutine to completion using an ErlangEventLoop.

```python
import erlang
import asyncio

async def main():
    await asyncio.sleep(0.01)
    return 42

result = erlang.run(main())
assert result == 42
```

#### asyncio.gather(*coros, return_exceptions=False)

Run coroutines concurrently and gather results.

```python
import erlang
import asyncio

async def task(n):
    await asyncio.sleep(0.01)
    return n * 2

async def main():
    results = await asyncio.gather(task(1), task(2), task(3))
    assert results == [2, 4, 6]

erlang.run(main())
```

#### asyncio.wait_for(coro, timeout)

Wait for a coroutine with a timeout.

```python
import erlang
import asyncio

async def fast_task():
    await asyncio.sleep(0.01)
    return 'done'

async def main():
    try:
        result = await asyncio.wait_for(fast_task(), timeout=1.0)
    except asyncio.TimeoutError:
        print("Task timed out")

erlang.run(main())
```

#### asyncio.create_task(coro, *, name=None)

Create a task to run a coroutine in the background.

```python
import erlang
import asyncio

async def background_work():
    await asyncio.sleep(0.1)
    return 'background_done'

async def main():
    task = asyncio.create_task(background_work())

    # Do other work while task runs
    await asyncio.sleep(0.05)

    # Wait for task to complete
    result = await task
    assert result == 'background_done'

erlang.run(main())
```

#### asyncio.wait(fs, *, timeout=None, return_when=ALL_COMPLETED)

Wait for multiple futures/tasks.

```python
import erlang
import asyncio

async def main():
    tasks = [
        asyncio.create_task(asyncio.sleep(0.01))
        for i in range(3)
    ]

    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.ALL_COMPLETED
    )

    assert len(done) == 3
    assert len(pending) == 0

erlang.run(main())
```

#### Event Loop Functions

```python
import erlang
import asyncio

# Create a new Erlang-backed event loop
loop = erlang.new_event_loop()

# Set the current event loop
asyncio.set_event_loop(loop)

# Get the running loop (raises RuntimeError if none)
loop = asyncio.get_running_loop()
```

#### Context Manager for Timeouts

```python
import erlang
import asyncio

async def main():
    async with asyncio.timeout(1.0):
        await slow_operation()  # Raises TimeoutError if > 1s

erlang.run(main())
```

### Performance Comparison

| Operation | Standard asyncio | Erlang Event Loop | Improvement |
|-----------|------------------|-------------------|-------------|
| sleep(1ms) | ~1.5ms | ~1.1ms | ~27% faster |
| Event loop overhead | ~0.5-1ms | ~0 | Erlang scheduler |
| Timer precision | 10ms polling | Sub-ms | BEAM scheduler |
| Idle CPU | Polling | Zero | Event-driven |

### When to Use Erlang Event Loop

**Use `erlang.run()` when:**
- You need precise sub-millisecond timing
- Your app makes many small sleep calls
- You want to eliminate Python event loop overhead
- Building ASGI handlers that need efficient sleep
- Your app is running inside erlang_python

**Use standard `asyncio.run()` when:**
- You're running outside the Erlang VM
- Testing Python code in isolation

### Integration with ASGI Frameworks

For ASGI applications (FastAPI, Starlette, etc.), you can use the Erlang event loop for better performance:

```python
from fastapi import FastAPI
import asyncio

app = FastAPI()

@app.get("/delay")
async def delay_endpoint(ms: int = 100):
    # When running via py_asgi, uses Erlang timer
    await asyncio.sleep(ms / 1000.0)
    return {"slept_ms": ms}
```

## Async Worker Backend (Internal)

The `py:async_call/3,4` and `py:await/1,2` APIs use an event-driven backend based on `py_event_loop`.

### Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│ Erlang      │     │ C NIF           │     │ py_event_loop        │
│ py:async_   │     │ (no thread)     │     │ (Erlang process)     │
│ call()      │     │                 │     │                      │
└──────┬──────┘     └────────┬────────┘     └──────────┬───────────┘
       │                     │                         │
       │ 1. Message to       │                         │
       │    event_loop       │                         │
       │─────────────────────┼────────────────────────>│
       │                     │                         │
       │ 2. Return Ref       │                         │
       │<────────────────────┼─────────────────────────│
       │                     │                         │
       │                     │   enif_select (wait)    │
       │                     │   ┌───────────────────┐ │
       │                     │   │ Run Python        │ │
       │                     │   │ erlang.send(pid,  │ │
       │                     │   │   result)         │ │
       │                     │   └───────────────────┘ │
       │                     │                         │
       │ 3. {async_result}   │                         │
       │<──────────────────────────────────────────────│
       │     (direct erlang.send from Python)          │
       │                     │                         │
```

### Key Components

| Component | Role |
|-----------|------|
| `py_event_loop_pool` | Pool manager for event loop-based async execution |
| `py_event_loop:run_async/2` | Submit coroutine to event loop |
| `_run_and_send` | Python wrapper that sends result via `erlang.send()` |
| `nif_event_loop_run_async` | NIF for direct coroutine submission |

### Performance Benefits

| Aspect | Previous (pthread) | Current (event_loop) |
|--------|-------------------|---------------------|
| Latency | ~10-20ms polling | <1ms (enif_select) |
| CPU idle | 100 wakeups/sec | Zero |
| Threads | N pthreads | 0 extra threads |
| GIL | Acquire/release in thread | Already held in callback |
| Shutdown | pthread_join (blocking) | Clean Erlang messages |

The event-driven model eliminates the polling overhead of the previous pthread+usleep
implementation, resulting in significantly lower latency for async operations.

## Limitations

### Subprocess Operations Not Supported

The `ErlangEventLoop` does not support subprocess operations:

```python
# These will raise NotImplementedError:
loop.subprocess_shell(...)
loop.subprocess_exec(...)

# asyncio.create_subprocess_* will also fail
await asyncio.create_subprocess_shell(...)
await asyncio.create_subprocess_exec(...)
```

**Why?** Subprocess operations use `fork()` which would corrupt the Erlang VM. See [Security](security.md) for details.

**Alternative:** Use Erlang ports (`open_port/2`) for subprocess management. You can register an Erlang function that runs shell commands and call it from Python via `erlang.call()`.

### Signal Handling Not Supported

The `ErlangEventLoop` does not support signal handlers:

```python
# These will raise NotImplementedError:
loop.add_signal_handler(signal.SIGTERM, handler)
loop.remove_signal_handler(signal.SIGTERM)
```

**Why?** Signal handling should be done at the Erlang VM level. The BEAM has its own signal handling infrastructure that's integrated with supervisors and the OTP design patterns.

**Alternative:** Handle signals in Erlang using the `kernel` application's signal handling or write a port program that forwards signals to Erlang processes.

## Protocol-Based I/O

For building custom servers with low-level protocol handling, see the [Reactor](reactor.md) module. The reactor provides FD-based protocol handling where Erlang manages I/O scheduling via `enif_select` and Python implements protocol logic.

## See Also

- [Reactor](reactor.md) - Low-level FD-based protocol handling
- [Security](security.md) - Sandbox and blocked operations
- [Threading](threading.md) - For `erlang.async_call()` in asyncio contexts
- [Streaming](streaming.md) - For working with Python generators
- [Getting Started](getting-started.md) - Basic usage guide
