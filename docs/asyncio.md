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
│   │  process pending │                │                                    │ │
│   │  callbacks       │                │                                    │ │
│   └──────────────────┘                │                                    │ │
│                                       │                                    │ │
│   ┌──────────────────┐                └────────────────────────────────────┘ │
│   │  asyncio (via    │                                                       │
│   │  erlang.run())   │                ┌────────────────────────────────────┐ │
│   │  sleep()         │                │  asyncio.sleep() uses call_later() │ │
│   │  gather()        │─call_later()──▶│  which triggers erlang:send_after  │ │
│   │  wait_for()      │                │                                    │ │
│   │  create_task()   │                └────────────────────────────────────┘ │
│   └──────────────────┘                                                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Components:**

| Component | Role |
|-----------|------|
| `ErlangEventLoop` | Python asyncio event loop using Erlang for I/O and timers |
| `py_event_worker` | Erlang gen_server handling FDs, timers, and task processing |
| `erlang.run()` | Entry point to run asyncio code with the Erlang event loop |

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

The event loop integrates with Erlang's message passing system through a worker process:

```erlang
%% Start the event worker
{ok, LoopRef} = py_nif:event_loop_new(),
{ok, WorkerPid} = py_event_worker:start_link(<<"worker">>, LoopRef),
ok = py_nif:event_loop_set_worker(LoopRef, WorkerPid).
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

Each event loop has an associated worker process that handles timer and FD events:

```
┌─────────────────────────────────────────────────────────────────┐
│                     py_event_worker                              │
│                                                                 │
│  Receives:                                                      │
│  - Timer expirations from erlang:send_after                    │
│  - FD ready events from enif_select                            │
│  - task_ready messages for processing tasks                    │
│                                                                 │
│  Dispatches events to the loop's pending queue                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                        ┌───────────┐
                        │   Loop    │
                        │  pending  │
                        └───────────┘
```

Each loop has its own pending queue, ensuring callbacks are processed only by the loop that scheduled them. The worker dispatches timer, FD events, and tasks to the correct loop.

## Erlang Timer Integration

When using `erlang.run()` to execute asyncio code, standard asyncio functions like `asyncio.sleep()` are automatically backed by Erlang's native timer system for maximum performance.

### Overview

Unlike Python's standard polling-based event loop, the Erlang event loop uses `erlang:send_after/3` for timers and integrates directly with the BEAM scheduler. This eliminates Python event loop overhead (~0.5-1ms per operation) and provides more precise timing.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    asyncio.sleep() via ErlangEventLoop                  │
│                                                                         │
│   Python                           Erlang                               │
│   ──────                           ──────                               │
│                                                                         │
│   ┌─────────────────┐              ┌─────────────────────────────────┐  │
│   │  asyncio.sleep  │              │         py_event_worker         │  │
│   │    (0.1)        │              │                                 │  │
│   └────────┬────────┘              │                                 │  │
│            │                       │                                 │  │
│            ▼                       │                                 │  │
│   ┌─────────────────┐              │                                 │  │
│   │ ErlangEventLoop │──{timer,100, │  erlang:send_after(100ms)       │  │
│   │   call_later()  │     Id}─────▶│         │                       │  │
│   └────────┬────────┘              │         ▼                       │  │
│            │                       │  handle_info({timeout, ...})    │  │
│   ┌────────▼────────┐              │         │                       │  │
│   │  Yield to event │              │         ▼                       │  │
│   │  loop (dirty    │              │  py_nif:dispatch_timer()        │  │
│   │  scheduler      │◀─────────────│         │                       │  │
│   │  released)      │   callback   └─────────┼───────────────────────┘  │
│   └────────┬────────┘                        │                          │
│            │                                 │                          │
│            ▼                                 ▼                          │
│   ┌─────────────────┐              ┌─────────────────────────────────┐  │
│   │  Resume after   │              │  Timer callback dispatched to   │  │
│   │  timer fires    │              │  Python pending queue           │  │
│   └─────────────────┘              └─────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key features:**
- **Dirty scheduler released during sleep** - Python yields to event loop, freeing the dirty NIF thread
- **BEAM scheduler integration** - Uses Erlang's native timer system
- **Zero CPU usage** - No polling, event-driven callback
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

#### erlang.sleep(seconds)

Sleep for the specified duration. Works in both async and sync contexts.

```python
import erlang

# Async context - yields to event loop
async def async_handler():
    await erlang.sleep(0.1)  # Uses asyncio.sleep() internally
    return "done"

# Sync context - blocks Python, releases dirty scheduler
def sync_handler():
    erlang.sleep(0.1)  # Suspends Erlang process via receive/after
    return "done"
```

**Behavior by Context:**

| Context | Mechanism | Effect |
|---------|-----------|--------|
| Async (`await erlang.sleep()`) | `asyncio.sleep()` via `call_later()` | Yields to event loop, dirty scheduler released |
| Sync (`erlang.sleep()`) | `erlang.call('_py_sleep')` with `receive/after` | Blocks Python, Erlang process suspends, dirty scheduler released |

Both modes allow other Erlang processes and Python contexts to run during the sleep.

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

#### erlang.spawn_task(coro, *, name=None)

Spawn an async task from both sync and async contexts. This is useful for fire-and-forget background work from synchronous Python code called by Erlang.

```python
import erlang

# From sync code called by Erlang
def handle_request(data):
    # This works even though there's no running event loop
    erlang.spawn_task(process_async(data))
    return 'ok'

# From async code
async def handler():
    # Also works in async context
    erlang.spawn_task(background_work())
    await other_work()

async def process_async(data):
    await asyncio.sleep(0.1)
    # Do async processing...

async def background_work():
    await asyncio.sleep(0.1)
    # Do background work...
```

**Key features:**
- Works in sync context where `asyncio.get_running_loop()` would fail
- Returns `asyncio.Task` for optional await/cancel
- Automatically wakes up the event loop to ensure the task runs promptly
- Works with both ErlangEventLoop and standard asyncio loops

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
- Building async handlers that need efficient sleep
- Your app is running inside erlang_python

**Use standard `asyncio.run()` when:**
- You're running outside the Erlang VM
- Testing Python code in isolation

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
| `run_async/2` (internal) | Submit coroutine to event loop |
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

## Erlang Callbacks from Python

Python code can call registered Erlang functions using `erlang.call()`. This enables Python handlers to leverage Erlang's concurrency and I/O capabilities.

### erlang.call() - Blocking Callbacks

`erlang.call(name, *args)` calls a registered Erlang function and blocks until it returns.

```python
import erlang

def handler():
    # Call Erlang function - blocks until complete
    result = erlang.call('my_callback', arg1, arg2)
    return process(result)
```

**Behavior:**
- Blocks the current Python execution until the Erlang callback completes
- Code executes exactly once (no replay)
- The callback can release the dirty scheduler by using Erlang's `receive` (e.g., `erlang.sleep()`, `channel.receive()`)
- Quick callbacks hold the dirty scheduler; callbacks that wait via `receive` release it

### Explicit Scheduling API

For long-running operations or when you need to release the dirty scheduler, use the explicit scheduling functions. These return `ScheduleMarker` objects that **must be returned from your handler** to take effect.

#### erlang.schedule(callback_name, *args)

Release the dirty scheduler and continue via an Erlang callback.

```python
import erlang

# Register callback in Erlang:
# py_callback:register(<<"compute">>, fun([X]) -> X * 2 end).

def handler(x):
    # Returns ScheduleMarker - MUST be returned from handler
    return erlang.schedule('compute', x)
    # Nothing after this executes - Erlang callback continues
```

The result is transparent to the caller:
```erlang
%% Caller just gets the callback result
{ok, 10} = py:call('__main__', 'handler', [5]).
```

#### erlang.schedule_py(module, func, args=None, kwargs=None)

Release the dirty scheduler and continue by calling a Python function.

```python
import erlang

def compute(x, multiplier=2):
    return x * multiplier

def handler(x):
    # Schedule Python function - releases dirty scheduler
    return erlang.schedule_py('__main__', 'compute', [x], {'multiplier': 3})
```

This is useful for:
- Breaking up long computations
- Allowing other Erlang processes to run
- Cooperative multitasking

#### erlang.schedule_inline(module, func, args=None, kwargs=None)

Release the dirty scheduler and continue by calling a Python function via `enif_schedule_nif()` - bypassing Erlang messaging entirely.

```python
import erlang

def process_chunk(data, offset=0, results=None):
    """Process data in chunks with inline continuations."""
    if results is None:
        results = []

    chunk_end = min(offset + 100, len(data))
    for i in range(offset, chunk_end):
        results.append(transform(data[i]))

    if chunk_end < len(data):
        # Continue inline - no Erlang messaging overhead
        return erlang.schedule_inline(
            '__main__', 'process_chunk',
            args=[data, chunk_end, results]
        )

    return results
```

**When to use `schedule_inline` vs `schedule_py`:**

| Aspect | `schedule_inline` | `schedule_py` |
|--------|-------------------|---------------|
| Flow | Python -> NIF -> enif_schedule_nif -> Python | Python -> NIF -> Erlang message -> Python |
| Speed | ~3x faster for tight loops | Slower due to messaging |
| Use case | Pure Python chains, no Erlang interaction | When you need Erlang messaging between steps |
| Overhead | Minimal (direct NIF continuation) | Higher (gen_server call) |

**Important:** `schedule_inline` captures the caller's globals/locals, ensuring correct namespace resolution even with subinterpreters.

#### erlang.consume_time_slice(percent)

Check if the NIF time slice is exhausted. Returns `True` if you should yield, `False` if more time remains.

```python
import erlang

def long_computation(items, start_idx=0):
    results = []
    for i in range(start_idx, len(items)):
        results.append(process(items[i]))

        # Check if we should yield (1% of time slice per iteration)
        if erlang.consume_time_slice(1):
            # Time slice exhausted - save progress and reschedule
            return erlang.schedule_py(
                '__main__', 'long_computation',
                [items], {'start_idx': i + 1}
            )

    return results
```

**Parameters:**
- `percent` (1-100): How much of the time slice was consumed by recent work

**Returns:**
- `True`: Time slice exhausted, you should yield
- `False`: More time remains, continue processing

### When to Use Each Pattern

| Pattern | Use When | Dirty Scheduler |
|---------|----------|-----------------|
| `erlang.call()` | Quick operations or callbacks that use `receive` | Held (unless callback suspends via `receive`) |
| `erlang.schedule()` | Need to call Erlang callback and always release scheduler | Released |
| `erlang.schedule_py()` | Long Python computation, need Erlang interaction between steps | Released |
| `erlang.schedule_inline()` | Tight Python loops, no Erlang interaction needed (~3x faster) | Released |
| `consume_time_slice()` | Fine-grained control over yielding | N/A (checks time slice) |

### Example: Cooperative Long-Running Task

```python
import erlang

def process_batch(items, batch_size=100, offset=0):
    """Process items in batches, yielding between batches."""
    end = min(offset + batch_size, len(items))

    # Process this batch
    for i in range(offset, end):
        expensive_operation(items[i])

    if end < len(items):
        # More work to do - yield and continue
        return erlang.schedule_py(
            '__main__', 'process_batch',
            [items], {'batch_size': batch_size, 'offset': end}
        )

    return 'done'
```

### Important Notes

1. **Must return the marker**: `schedule()` and `schedule_py()` return `ScheduleMarker` objects that must be returned from your handler function. Calling them without returning has no effect:

```python
def wrong():
    erlang.schedule('callback', arg)  # No effect!
    return "oops"  # This is returned instead

def correct():
    return erlang.schedule('callback', arg)  # Works
```

2. **Cannot be nested**: The schedule marker must be the direct return value. You cannot return it from a nested function:

```python
def outer():
    def inner():
        return erlang.schedule('callback', arg)
    return inner()  # Works - marker propagates up

def broken():
    def inner():
        erlang.schedule('callback', arg)  # Wrong - not returned
    inner()
    return "oops"
```

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

## Async Task API (Erlang)

The `py_event_loop` module provides a high-level API for submitting async Python tasks from Erlang. This API is inspired by uvloop and uses a thread-safe task queue, allowing task submission from any dirty scheduler without blocking.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Async Task Submission                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Erlang Process           C NIF Layer              py_event_worker         │
│   ───────────────          ─────────────            ─────────────────        │
│                                                                              │
│   py_event_loop:           nif_submit_task          handle_info(task_ready) │
│   create_task(M,F,A)       │                        │                       │
│         │                  │ Thread-safe enqueue    │                       │
│         │──────────────────▶ (pthread_mutex)        │                       │
│         │                  │                        │                       │
│         │                  │ enif_send(task_ready)──▶                       │
│         │                  │                        │                       │
│         │                  │                        │ py_nif:process_ready  │
│         │                  │                        │       │               │
│         │                  │                        │       ▼               │
│         │                  │                        │ Run Python coro       │
│         │                  │                        │       │               │
│         │◀─────────────────────────────────────────────────┘               │
│         │    {async_result, Ref, {ok, Result}}      │                       │
│         │                                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- Thread-safe submission from any dirty scheduler via `enif_send`
- Non-blocking task creation
- Message-based result delivery
- Fire-and-forget support

### API Reference

#### py_event_loop:run/3,4

Blocking execution of an async Python function. Submits the task and waits for the result.

```erlang
%% Basic usage
{ok, Result} = py_event_loop:run(my_module, my_async_func, [arg1, arg2]).

%% With options (timeout, kwargs)
{ok, Result} = py_event_loop:run(aiohttp, get, [Url], #{
    timeout => 10000,
    kwargs => #{headers => #{}}
}).
```

**Parameters:**
- `Module` - Python module name (atom or binary)
- `Func` - Python function name (atom or binary)
- `Args` - List of positional arguments
- `Opts` - Options map (optional):
  - `timeout` - Timeout in milliseconds (default: 5000)
  - `kwargs` - Keyword arguments map (default: #{})

**Returns:**
- `{ok, Result}` - Task completed successfully
- `{error, Reason}` - Task failed or timed out

#### py_event_loop:create_task/3,4

Non-blocking task submission. Returns immediately with a reference for awaiting the result later.

```erlang
%% Submit task
Ref = py_event_loop:create_task(my_module, my_async_func, [arg1]).

%% Do other work while task runs...
do_other_work(),

%% Await result when needed
{ok, Result} = py_event_loop:await(Ref).
```

**Parameters:**
- `Module` - Python module name (atom or binary)
- `Func` - Python function name (atom or binary)
- `Args` - List of positional arguments
- `Kwargs` - Keyword arguments map (optional, default: #{})

**Returns:**
- `reference()` - Task reference for awaiting

#### py_event_loop:await/1,2

Wait for an async task result.

```erlang
%% Default timeout (5 seconds)
{ok, Result} = py_event_loop:await(Ref).

%% Custom timeout
{ok, Result} = py_event_loop:await(Ref, 10000).

%% Infinite timeout
{ok, Result} = py_event_loop:await(Ref, infinity).
```

**Parameters:**
- `Ref` - Task reference from `create_task`
- `Timeout` - Timeout in milliseconds or `infinity` (optional, default: 5000)

**Returns:**
- `{ok, Result}` - Task completed successfully
- `{error, Reason}` - Task failed with error
- `{error, timeout}` - Timeout waiting for result

#### py_event_loop:spawn_task/3,4

Fire-and-forget task execution. Submits the task but does not wait for or return the result.

```erlang
%% Background logging
ok = py_event_loop:spawn_task(logger, log_event, [EventData]).

%% With kwargs
ok = py_event_loop:spawn_task(metrics, record, [Name, Value], #{tags => Tags}).
```

**Parameters:**
- `Module` - Python module name (atom or binary)
- `Func` - Python function name (atom or binary)
- `Args` - List of positional arguments
- `Kwargs` - Keyword arguments map (optional, default: #{})

**Returns:**
- `ok` - Task submitted (result is discarded)

### Example: Concurrent HTTP Requests

```erlang
%% Submit multiple requests concurrently
Refs = [
    py_event_loop:create_task(aiohttp, get, [<<"https://api.example.com/users">>]),
    py_event_loop:create_task(aiohttp, get, [<<"https://api.example.com/posts">>]),
    py_event_loop:create_task(aiohttp, get, [<<"https://api.example.com/comments">>])
],

%% Await all results
Results = [py_event_loop:await(Ref, 10000) || Ref <- Refs].
```

### Example: Background Processing

```erlang
%% Fire-and-forget analytics
handle_request(Request) ->
    %% Process request...
    Response = process(Request),

    %% Log analytics in background (don't wait)
    ok = py_event_loop:spawn_task(analytics, track_event, [
        <<"page_view">>,
        #{path => Request#request.path, user_id => Request#request.user_id}
    ]),

    Response.
```

### Thread Safety

The async task API is fully thread-safe:

- `create_task` and `spawn_task` can be called from any Erlang process, including processes running on dirty schedulers
- Task submission uses `enif_send` which is safe to call from any thread
- The task queue uses mutex protection for thread-safe enqueueing
- Results are delivered via standard Erlang message passing

This means you can safely call `py_event_loop:create_task` from within a callback that's already running on a dirty NIF scheduler.

## Event Loop Pool

The `py_event_loop_pool` module provides a pool of event loops for parallel Python coroutine execution. Inspired by libuv's "one loop per thread" model, each loop has its own worker and maintains event ordering.

### Process Affinity

All tasks from the same Erlang process are routed to the same event loop (via PID hash). This guarantees that timers and related async operations from a single process execute in order.

```
                    ┌─► [loop_1] ──► [worker_1] ──► ordered execution
[process] ──► [hash(PID)] ─┼─► [loop_2] ──► [worker_2] ──► ordered execution
                    └─► [loop_N] ──► [worker_N] ──► ordered execution
```

### API

The pool provides the same API as `py_event_loop`, but with automatic load distribution:

```erlang
%% Get event loop for current process (always the same loop for same PID)
{ok, LoopRef} = py_event_loop_pool:get_loop().

%% Submit task and await result
Ref = py_event_loop_pool:create_task(math, sqrt, [16.0]),
{ok, 4.0} = py_event_loop_pool:await(Ref).

%% Blocking call
{ok, 4.0} = py_event_loop_pool:run(math, sqrt, [16.0]).

%% Fire-and-forget
ok = py_event_loop_pool:spawn_task(logger, info, [<<"message">>]).

%% Pool statistics
#{num_loops := N, supported := true} = py_event_loop_pool:get_stats().
```

### Configuration

Configure the pool size via application environment:

```erlang
%% sys.config
[
    {erlang_python, [
        %% Number of event loops (default: erlang:system_info(schedulers))
        {event_loop_pool_size, 8}
    ]}
].
```

### When to Use

| Use Case | Module |
|----------|--------|
| Single caller, ordered tasks | `py_event_loop` |
| Multiple callers, parallel execution | `py_event_loop_pool` |
| High throughput, many concurrent processes | `py_event_loop_pool` |

### Performance

Benchmarks on 14-core system with Python 3.14:

| Pattern | Throughput |
|---------|------------|
| Sequential (single loop) | 83k tasks/sec |
| Sequential (pool) | 150k tasks/sec |
| Concurrent (50 processes) | 164k tasks/sec |
| Fire-and-collect (10k tasks) | 417k tasks/sec |

### Example: Parallel Processing

```erlang
%% Process items in parallel using multiple Erlang processes
%% Each process gets its own event loop for ordered execution
process_batch(Items) ->
    Parent = self(),
    Pids = [spawn_link(fun() ->
        Results = [begin
            Ref = py_event_loop_pool:create_task(processor, handle, [Item]),
            py_event_loop_pool:await(Ref)
        end || Item <- Chunk],
        Parent ! {done, self(), Results}
    end) || Chunk <- partition(Items, 100)],

    [receive {done, Pid, R} -> R end || Pid <- Pids].
```

## See Also

- [Reactor](reactor.md) - Low-level FD-based protocol handling
- [Security](security.md) - Sandbox and blocked operations
- [Threading](threading.md) - For `erlang.async_call()` in asyncio contexts
- [Streaming](streaming.md) - For working with Python generators
- [Getting Started](getting-started.md) - Basic usage guide
