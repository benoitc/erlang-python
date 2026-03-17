# Channel API

The Channel API provides efficient bidirectional message passing between Erlang and Python. Channels use `enif_ioq` for zero-copy buffering and integrate with Python's asyncio for non-blocking operations.

## Overview

Channels are faster than the Reactor pattern for message passing scenarios:

| Message Size | Channel | Reactor | Speedup |
|-------------|---------|---------|---------|
| 64 bytes    | 6.2M ops/s | 772K ops/s | **8x** |
| 1KB         | 3.8M ops/s | 734K ops/s | **5x** |
| 16KB        | 1.1M ops/s | 576K ops/s | **2x** |

Use channels when you need:
- High-throughput message streaming
- Bidirectional Erlang-Python communication
- Asyncio integration
- Backpressure support

## Quick Start

### Erlang Side

```erlang
%% Create a channel
{ok, Ch} = py_channel:new(),

%% Send messages with sender PID for replies
ok = py_channel:send(Ch, {request, self(), <<"data">>}),

%% Wait for response
receive
    {response, Result} ->
        io:format("Got result: ~p~n", [Result])
end,

%% Close when done
py_channel:close(Ch).
```

### Python Side (Sync)

```python
from erlang.channel import Channel, reply

def process_messages(channel_ref):
    ch = Channel(channel_ref)

    for msg in ch:
        # Extract sender PID from message
        _, sender_pid, data = msg

        # Process and reply
        result = process(data)
        reply(sender_pid, ('response', result))
```

### Python Side (Async)

```python
from erlang.channel import Channel, reply

async def process_messages(channel_ref):
    ch = Channel(channel_ref)

    async for msg in ch:
        # Extract sender PID from message
        _, sender_pid, data = msg

        # Process and reply
        result = await process(data)
        reply(sender_pid, ('response', result))
```

## Erlang API

### `py_channel:new/0,1`

Create a new channel.

```erlang
%% Unbounded channel
{ok, Ch} = py_channel:new().

%% Channel with backpressure (max 10KB queued)
{ok, Ch} = py_channel:new(#{max_size => 10000}).
```

**Options:**
- `max_size` - Maximum queue size in bytes. When exceeded, `send/2` returns `busy`.

### `py_channel:send/2`

Send an Erlang term to Python.

```erlang
ok = py_channel:send(Ch, Term).
```

**Returns:**
- `ok` - Message queued successfully
- `busy` - Queue full (backpressure)
- `{error, closed}` - Channel was closed

### `py_channel:close/1`

Close the channel. Python receivers will get `StopIteration`.

```erlang
ok = py_channel:close(Ch).
```

### `py_channel:info/1`

Get channel status.

```erlang
Info = py_channel:info(Ch).
%% #{size => 1024, max_size => 10000, closed => false}
```

## Python API

### `Channel` class

Wrapper for receiving messages from Erlang.

```python
from erlang.channel import Channel

ch = Channel(channel_ref)
```

#### `receive()`

Blocking receive. Blocks Python execution until a message is available.

```python
msg = ch.receive()  # Blocks until message available
```

**Behavior:**
- If the channel has data, returns immediately
- If empty, suspends the Erlang process via `receive`, releasing the dirty scheduler
- Other Erlang processes can run while waiting for data

**Raises:** `ChannelClosed` when the channel is closed.

#### `try_receive()`

Non-blocking receive. Returns immediately.

```python
msg = ch.try_receive()  # Returns None if empty
```

**Returns:** Message or `None` if empty.
**Raises:** `ChannelClosed` when the channel is closed.

#### `async_receive()`

Asyncio-compatible receive. Yields to other coroutines while waiting.

```python
msg = await ch.async_receive()
```

**Raises:** `ChannelClosed` when the channel is closed.

#### Iteration

```python
# Sync iteration
for msg in channel:
    process(msg)

# Async iteration
async for msg in channel:
    process(msg)
```

### `reply(pid, term)`

Send a message to an Erlang process.

```python
from erlang.channel import reply

# Reply to the sender
reply(sender_pid, {"status": "ok", "result": data})
```

### `ChannelClosed` exception

Raised when receiving from a closed channel.

```python
from erlang.channel import Channel, ChannelClosed

try:
    msg = ch.receive()
except ChannelClosed:
    print("Channel closed")
```

## Backpressure

Channels support backpressure to prevent unbounded memory growth.

### Erlang Side

```erlang
{ok, Ch} = py_channel:new(#{max_size => 10000}),

case py_channel:send(Ch, LargeData) of
    ok ->
        continue;
    busy ->
        %% Queue is full, wait before retrying
        timer:sleep(10),
        retry
end.
```

### Monitoring Queue Size

```erlang
#{size := Size, max_size := MaxSize} = py_channel:info(Ch),
Utilization = Size / MaxSize.
```

## Examples

### Request-Response Pattern

```erlang
%% Erlang: Send request, receive response
{ok, Ch} = py_channel:new(),
ok = py_channel:send(Ch, {request, self(), <<"compute">>}),
receive
    {response, Result} -> Result
end.
```

```python
from erlang.channel import Channel, reply

def handle_requests(channel_ref):
    ch = Channel(channel_ref)

    for msg in ch:
        if msg[0] == 'request':
            _, sender_pid, data = msg
            result = compute(data)
            reply(sender_pid, ('response', result))
```

### Streaming Data

```erlang
%% Erlang: Stream data to Python
{ok, Ch} = py_channel:new(),
lists:foreach(fun(Item) ->
    ok = py_channel:send(Ch, Item)
end, large_list()),
py_channel:close(Ch).
```

```python
async def process_stream(channel_ref):
    ch = Channel(channel_ref)
    results = []

    async for item in ch:
        results.append(process(item))

    return results
```

### Worker Pool Pattern

```erlang
%% Erlang: Distribute work across Python workers
{ok, Ch} = py_channel:new(#{max_size => 100000}),

%% Start multiple Python workers on the channel
[spawn_python_worker(Ch) || _ <- lists:seq(1, 4)],

%% Send work items
[py_channel:send(Ch, {work, Item}) || Item <- WorkItems],

%% Signal completion
py_channel:close(Ch).
```

```python
import asyncio
from erlang.channel import Channel

async def worker(channel_ref, worker_id):
    ch = Channel(channel_ref)

    async for msg in ch:
        if msg[0] == 'work':
            _, item = msg
            await process_item(item)
            print(f"Worker {worker_id} processed {item}")
```

## Performance Tips

1. **Use async iteration** for high-throughput scenarios - it allows other coroutines to run while waiting.

2. **Set appropriate `max_size`** to prevent memory issues while maintaining throughput.

3. **Batch messages** when possible - sending fewer larger messages is more efficient than many small ones.

4. **Avoid `try_receive` polling** - use blocking `receive()` or async `async_receive()` instead.

## Architecture

```
Erlang                              Python
──────                              ──────

py_channel:new() ─────────────────▶ Channel created

py_channel:send(Ch, Term)
       │
       ▼
  enif_term_to_binary()
       │
       ▼
  enif_ioq_enq_binary() ──────────▶ channel.receive()
                                          │
                                          ▼
                                    enif_ioq_peek()
                                          │
                                          ▼
                                    enif_binary_to_term()
                                          │
                                          ▼
                                    Python term

py_channel:close() ───────────────▶ StopIteration
```

## ByteChannel - Raw Byte Streaming

For binary protocols and raw byte streaming (e.g., HTTP bodies, file transfers), use `ByteChannel` instead of `Channel`. ByteChannel passes bytes directly without term serialization, avoiding encoding/decoding overhead.

### When to Use ByteChannel

| Use Case | Channel | ByteChannel |
|----------|---------|-------------|
| Structured messages | Yes | No |
| RPC-style communication | Yes | No |
| HTTP bodies | No | Yes |
| File streaming | No | Yes |
| Binary protocols | No | Yes |
| Raw byte streams | No | Yes |

### Erlang API

```erlang
%% Create a byte channel
{ok, Ch} = py_byte_channel:new(),

%% Send raw bytes
ok = py_byte_channel:send(Ch, <<"HTTP/1.1 200 OK\r\n">>),
ok = py_byte_channel:send(Ch, BodyBytes),

%% Receive raw bytes
{ok, Data} = py_byte_channel:recv(Ch),

%% Non-blocking receive
{ok, Data} = py_byte_channel:try_receive(Ch),
{error, empty} = py_byte_channel:try_receive(Ch),  %% If no data

%% Close when done
py_byte_channel:close(Ch).
```

### Python API

```python
from erlang import ByteChannel, ByteChannelClosed

def process_bytes(channel_ref):
    ch = ByteChannel(channel_ref)

    # Blocking receive (releases GIL while waiting)
    data = ch.receive_bytes()

    # Non-blocking receive
    data = ch.try_receive_bytes()  # Returns None if empty

    # Iterate over bytes
    for chunk in ch:
        process(chunk)

    # Send bytes back
    ch.send_bytes(b"response data")
```

### Async Python API

```python
from erlang import ByteChannel, ByteChannelClosed

async def process_bytes_async(channel_ref):
    ch = ByteChannel(channel_ref)

    # Async receive (yields to other coroutines)
    data = await ch.async_receive_bytes()

    # Async iteration
    async for chunk in ch:
        process(chunk)
```

### ByteChannel vs Channel Architecture

```
Channel (term-based):
  Erlang:  term_to_binary() ──▶ enif_ioq ──▶ binary_to_term() :Python

ByteChannel (raw bytes):
  Erlang:  raw bytes ─────────▶ enif_ioq ─────────▶ raw bytes :Python
```

ByteChannel reuses the same underlying `py_channel_t` structure but skips the term serialization/deserialization steps.

## See Also

- [Reactor](reactor.md) - FD-based protocol handling for sockets
- [Asyncio](asyncio.md) - Erlang-native asyncio event loop
- [Getting Started](getting-started.md) - Basic usage guide
