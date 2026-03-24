# Buffer API

The Buffer API provides a zero-copy WSGI input buffer for streaming HTTP request bodies from Erlang to Python. Buffers use shared memory with GIL-released blocking reads for efficient data transfer.

## Overview

Buffers are designed for WSGI/ASGI `wsgi.input` scenarios where Erlang receives HTTP body chunks and Python needs to consume them:

- Zero-copy access via Python's buffer protocol (`memoryview`)
- File-like interface (`read`, `readline`, `readlines`)
- Blocking reads that release the GIL while waiting
- Fast substring search using `memchr`/`memmem`

Use buffers when you need:
- WSGI input for HTTP request bodies
- Streaming data from Erlang to Python
- Zero-copy access to binary data

## Quick Start

### Erlang Side

```erlang
%% Create a buffer (chunked - unknown size)
{ok, Buf} = py_buffer:new(),

%% Or with known content length (pre-allocates memory)
{ok, Buf} = py_buffer:new(4096),

%% Write HTTP body chunks
ok = py_buffer:write(Buf, <<"chunk1">>),
ok = py_buffer:write(Buf, <<"chunk2">>),

%% Signal end of data
ok = py_buffer:close(Buf),

%% Pass to WSGI app
py:call(Ctx, myapp, handle_request, [#{<<"wsgi.input">> => Buf}]).
```

### Python Side

```python
def handle_request(environ):
    wsgi_input = environ['wsgi.input']

    # Read all data
    body = wsgi_input.read()

    # Or read line by line
    for line in wsgi_input:
        process(line)

    # Or read specific amount
    chunk = wsgi_input.read(1024)
```

## Erlang API

### `py_buffer:new/0`

Create a buffer for chunked/streaming data (unknown content length).

```erlang
{ok, Buf} = py_buffer:new().
```

The buffer starts with a default capacity (64KB) and grows as needed.

### `py_buffer:new/1`

Create a buffer with known content length.

```erlang
{ok, Buf} = py_buffer:new(ContentLength).
```

**Arguments:**
- `ContentLength` - Expected total size in bytes, or `undefined` for chunked

Pre-allocating avoids buffer growth overhead when content length is known.

### `py_buffer:write/2`

Write binary data to the buffer.

```erlang
ok = py_buffer:write(Buf, Data).
```

**Arguments:**
- `Buf` - Buffer reference from `new/0,1`
- `Data` - Binary data to append

**Returns:**
- `ok` - Data written successfully
- `{error, closed}` - Buffer was closed

Writing signals any waiting Python readers via `pthread_cond_broadcast`.

### `py_buffer:close/1`

Signal end of data (EOF).

```erlang
ok = py_buffer:close(Buf).
```

After closing:
- No more data can be written
- Python's `read()` returns remaining data then empty bytes
- Waiting Python threads are woken up

## Python API

### `PyBuffer` class

The buffer appears in Python as `erlang.PyBuffer` when passed from Erlang.

```python
from erlang import PyBuffer
```

#### `read(size=-1)`

Read up to `size` bytes, blocking if needed.

```python
data = buf.read()      # Read all (blocks until EOF)
chunk = buf.read(1024) # Read up to 1024 bytes
```

**Behavior:**
- If `size=-1`, reads all data (waits for EOF if content length known)
- If data available, returns immediately
- If empty, blocks until data arrives (GIL released during wait)
- Returns empty bytes at EOF

#### `read_nonblock(size=-1)`

Read available bytes without blocking. For async I/O.

```python
chunk = buf.read_nonblock(1024)  # Read up to 1024 available bytes
data = buf.read_nonblock()       # Read all available bytes
```

**Behavior:**
- Returns immediately with whatever data is available
- Never blocks, even if no data available
- Returns empty bytes if nothing available (check `readable_amount()` first)
- Use with `readable_amount()` and `at_eof()` for async I/O loops

#### `readable_amount()`

Return number of bytes available without blocking.

```python
available = buf.readable_amount()
if available > 0:
    data = buf.read_nonblock(available)
```

**Returns:** Number of bytes that can be read immediately.

#### `at_eof()`

Check if buffer is at EOF with no more data.

```python
while not buf.at_eof():
    if buf.readable_amount() > 0:
        chunk = buf.read_nonblock(4096)
        process(chunk)
    else:
        await asyncio.sleep(0.001)  # Yield to event loop
```

**Returns:** `True` if EOF signaled AND all data has been read.

#### `readline(size=-1)`

Read one line, blocking if needed.

```python
line = buf.readline()  # Read until newline or EOF
```

**Returns:** Bytes including the trailing newline, or empty at EOF.

Uses `memchr` for fast newline scanning.

#### `readlines(hint=-1)`

Read all lines as a list.

```python
lines = buf.readlines()  # ['line1\n', 'line2\n', ...]
```

**Arguments:**
- `hint` - Optional size hint; stops after approximately this many bytes

#### `seek(offset, whence=0)`

Seek to position within already-written data.

```python
buf.seek(0)      # Seek to beginning (SEEK_SET)
buf.seek(10, 1)  # Seek forward 10 bytes (SEEK_CUR)
buf.seek(-5, 2)  # Seek 5 bytes before end (SEEK_END, requires EOF)
```

**Limitations:**
- Cannot seek past written data
- `SEEK_END` requires EOF flag set

#### `tell()`

Return current read position.

```python
pos = buf.tell()  # Current byte offset
```

#### `find(sub, start=0, end=None)`

Fast substring search using `memchr`/`memmem`.

```python
idx = buf.find(b'\n')       # Find first newline
idx = buf.find(b'boundary') # Find multipart boundary
```

**Returns:** Lowest index where substring found, or -1 if not found.

Single-byte search uses `memchr` (very fast). Multi-byte uses `memmem`.

#### Buffer Protocol

Buffers support Python's buffer protocol for zero-copy access:

```python
# Create memoryview for zero-copy access
mv = memoryview(buf)

# Access without copying
first_byte = mv[0]
slice_data = bytes(mv[10:20])

# Release when done
mv.release()
```

**Properties:**
- `readonly=True` - Buffer is read-only from Python
- `ndim=1` - One-dimensional byte array

#### Iteration

Line-by-line iteration:

```python
for line in buf:
    process(line)
```

Equivalent to calling `readline()` until EOF.

#### Properties and Methods

```python
buf.readable()   # True - always readable
buf.writable()   # False - not writable from Python
buf.seekable()   # True - limited seeking supported
buf.closed       # True if buffer is closed
len(buf)         # Available bytes (write_pos - read_pos)
buf.close()      # Mark buffer as closed
```

## Architecture

```
Erlang                              Python
------                              ------

py_buffer:new() -----------------> Buffer created
                                   (pthread mutex+cond initialized)

py_buffer:write(Buf, Data)
       |
       v
  memcpy to buffer
  pthread_cond_broadcast() ------> read()/readline() wakes up
                                   (GIL was released during wait)
                                          |
                                          v
                                   Return data to Python

py_buffer:close() ---------------> EOF flag set
                                   Waiting readers return
```

**Memory Layout:**

```
py_buffer_resource_t
+------------------+
| data*            | --> [chunk1][chunk2][chunk3]...
| capacity         |     ^       ^
| write_pos        | ----+       |
| read_pos         | ------------+
| content_length   |
| mutex            |
| data_ready (cond)|
| eof              |
| closed           |
| view_count       |
+------------------+
```

## Performance Tips

1. **Use known content length** when available - avoids buffer reallocation:
   ```erlang
   ContentLength = byte_size(Body),
   {ok, Buf} = py_buffer:new(ContentLength).
   ```

2. **Write in reasonable chunks** - very small writes have overhead:
   ```erlang
   %% Good: write accumulated chunks
   ok = py_buffer:write(Buf, AccumulatedData).

   %% Less efficient: many tiny writes
   %% [py_buffer:write(Buf, <<B>>) || B <- binary_to_list(Data)].
   ```

3. **Use memoryview for zero-copy** when processing large bodies:
   ```python
   mv = memoryview(buf)
   # Process without copying
   boundary_pos = buf.find(b'--boundary')
   part = bytes(mv[:boundary_pos])
   ```

4. **Use find() for parsing** - `memchr`/`memmem` are faster than Python string methods.

## Examples

### WSGI Input Simulation

```erlang
%% Simulate receiving HTTP body
{ok, Buf} = py_buffer:new(byte_size(Body)),
ok = py_buffer:write(Buf, Body),
ok = py_buffer:close(Buf),

%% Build WSGI environ
Environ = #{
    <<"REQUEST_METHOD">> => <<"POST">>,
    <<"PATH_INFO">> => <<"/api/data">>,
    <<"CONTENT_TYPE">> => <<"application/json">>,
    <<"CONTENT_LENGTH">> => integer_to_binary(byte_size(Body)),
    <<"wsgi.input">> => Buf
},

%% Call WSGI app
{ok, Response} = py:call(myapp, handle, [Environ]).
```

### Chunked Transfer

```erlang
%% Create buffer for chunked encoding
{ok, Buf} = py_buffer:new(),

%% Spawn writer process
spawn(fun() ->
    %% Simulate receiving chunks
    lists:foreach(fun(Chunk) ->
        ok = py_buffer:write(Buf, Chunk),
        timer:sleep(10)  % Simulate network delay
    end, get_chunks()),
    ok = py_buffer:close(Buf)
end),

%% Python can start reading immediately
%% read() will block until data available
py:call(myapp, stream_handler, [Buf]).
```

### Multipart Form Parsing

```python
def parse_multipart(buf, boundary):
    """Parse multipart form data from buffer."""
    parts = []

    while True:
        # Find next boundary using fast memmem
        idx = buf.find(boundary.encode())
        if idx == -1:
            break

        # Read headers until blank line
        headers = {}
        while True:
            line = buf.readline()
            if line == b'\r\n':
                break
            name, value = line.split(b':', 1)
            headers[name.strip()] = value.strip()

        # Read content until next boundary
        # ... process part
        parts.append({'headers': headers, 'data': data})

    return parts
```

### Async I/O Integration

For asyncio applications, use the non-blocking methods to avoid blocking the event loop:

```python
import asyncio
from erlang import PyBuffer

async def read_buffer_async(buf):
    """Read from buffer without blocking the event loop."""
    chunks = []

    while not buf.at_eof():
        available = buf.readable_amount()
        if available > 0:
            # Read available data
            chunk = buf.read_nonblock(4096)
            chunks.append(chunk)
        else:
            # Yield to event loop, check again soon
            await asyncio.sleep(0.001)

    return b''.join(chunks)

async def process_wsgi_body_async(environ):
    """Process WSGI body in async context."""
    buf = environ['wsgi.input']

    # Read body without blocking
    body = await read_buffer_async(buf)
    return json.loads(body)
```

For production use, consider integrating with Erlang's event notification:

```python
async def read_with_notification(buf, notify_channel):
    """Read using Erlang channel for data-ready notifications."""
    chunks = []

    while not buf.at_eof():
        available = buf.readable_amount()
        if available > 0:
            chunk = buf.read_nonblock(available)
            chunks.append(chunk)
        else:
            # Wait for Erlang to signal data is ready
            await notify_channel.async_receive()

    return b''.join(chunks)
```

## See Also

- [Channel](channel.md) - Bidirectional message passing
- [Reactor](reactor.md) - FD-based protocol handling
- [Getting Started](getting-started.md) - Basic usage guide
