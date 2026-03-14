%%% @doc Common Test suite for py_buffer API.
%%%
%%% Tests the zero-copy WSGI input buffer for streaming HTTP bodies.
-module(py_buffer_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    create_buffer_test/1,
    create_buffer_with_size_test/1,
    write_read_test/1,
    readline_test/1,
    readlines_test/1,
    seek_tell_test/1,
    find_test/1,
    memoryview_test/1,
    iterator_test/1,
    closed_buffer_test/1,
    empty_buffer_test/1,
    pass_to_python_test/1,
    gc_refcount_test/1,
    nonblock_read_test/1,
    asyncio_read_test/1
]).

all() -> [
    create_buffer_test,
    create_buffer_with_size_test,
    write_read_test,
    readline_test,
    readlines_test,
    seek_tell_test,
    find_test,
    memoryview_test,
    iterator_test,
    closed_buffer_test,
    empty_buffer_test,
    pass_to_python_test,
    gc_refcount_test,
    nonblock_read_test,
    asyncio_read_test
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Test creating a buffer with default settings (chunked encoding)
create_buffer_test(_Config) ->
    {ok, Buf} = py_buffer:new(),
    true = is_reference(Buf),
    ok = py_buffer:close(Buf).

%% @doc Test creating a buffer with known content length
create_buffer_with_size_test(_Config) ->
    {ok, Buf} = py_buffer:new(1024),
    true = is_reference(Buf),
    ok = py_buffer:close(Buf).

%% @doc Test basic write and read cycle
write_read_test(_Config) ->
    {ok, Buf} = py_buffer:new(),

    %% Write some data
    ok = py_buffer:write(Buf, <<"Hello, ">>),
    ok = py_buffer:write(Buf, <<"World!">>),
    ok = py_buffer:close(Buf),

    %% Read from Python
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    {ok, <<"Hello, World!">>} = py:eval(Ctx, <<"PyBuffer._test_create(b'Hello, World!').read()">>),

    ok.

%% @doc Test readline method
readline_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Create buffer with multiple lines and close it
    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'line1\\nline2\\nline3\\n')
buf.close()
">>),

    {ok, Line1} = py:eval(Ctx, <<"buf.readline()">>),
    <<"line1\n">> = Line1,

    {ok, Line2} = py:eval(Ctx, <<"buf.readline()">>),
    <<"line2\n">> = Line2,

    {ok, Line3} = py:eval(Ctx, <<"buf.readline()">>),
    <<"line3\n">> = Line3,

    %% Empty at EOF
    {ok, <<>>} = py:eval(Ctx, <<"buf.readline()">>),

    ok.

%% @doc Test readlines method
readlines_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'a\\nb\\nc\\n')
buf.close()
">>),
    {ok, Lines} = py:eval(Ctx, <<"buf.readlines()">>),
    [<<"a\n">>, <<"b\n">>, <<"c\n">>] = Lines,

    ok.

%% @doc Test seek and tell methods
seek_tell_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Create buffer, close it, and check initial position
    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'0123456789')
buf.close()
">>),
    {ok, 0} = py:eval(Ctx, <<"buf.tell()">>),

    %% Read 5 bytes, position should advance
    {ok, <<"01234">>} = py:eval(Ctx, <<"buf.read(5)">>),
    {ok, 5} = py:eval(Ctx, <<"buf.tell()">>),

    %% Seek back to beginning (SEEK_SET)
    {ok, 0} = py:eval(Ctx, <<"buf.seek(0)">>),
    {ok, 0} = py:eval(Ctx, <<"buf.tell()">>),

    %% Seek relative (SEEK_CUR)
    {ok, 3} = py:eval(Ctx, <<"buf.seek(3, 1)">>),
    {ok, 3} = py:eval(Ctx, <<"buf.tell()">>),

    %% Seek to position 7
    {ok, 7} = py:eval(Ctx, <<"buf.seek(7)">>),
    {ok, <<"789">>} = py:eval(Ctx, <<"buf.read()">>),

    ok.

%% @doc Test find method with memchr/memmem
find_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Create buffer with test data
    ok = py:exec(Ctx, <<"buf = PyBuffer._test_create(b'hello world hello')">>),

    %% Find single byte (memchr path)
    {ok, 0} = py:eval(Ctx, <<"buf.find(b'h')">>),
    {ok, 4} = py:eval(Ctx, <<"buf.find(b'o')">>),

    %% Find substring (memmem path)
    {ok, 0} = py:eval(Ctx, <<"buf.find(b'hello')">>),
    {ok, 6} = py:eval(Ctx, <<"buf.find(b'world')">>),

    %% Find with start position
    {ok, 12} = py:eval(Ctx, <<"buf.find(b'hello', 1)">>),

    %% Not found
    {ok, -1} = py:eval(Ctx, <<"buf.find(b'xyz')">>),

    %% Empty substring returns start
    {ok, 0} = py:eval(Ctx, <<"buf.find(b'')">>),
    {ok, 5} = py:eval(Ctx, <<"buf.find(b'', 5)">>),

    ok.

%% @doc Test buffer protocol (memoryview)
memoryview_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Create buffer and get memoryview
    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'test data')
mv = memoryview(buf)
">>),

    %% Check memoryview properties
    {ok, 9} = py:eval(Ctx, <<"len(mv)">>),
    {ok, true} = py:eval(Ctx, <<"mv.readonly">>),
    {ok, 1} = py:eval(Ctx, <<"mv.ndim">>),

    %% Access bytes via memoryview (zero-copy)
    {ok, <<"test data">>} = py:eval(Ctx, <<"bytes(mv)">>),

    %% Slice access
    {ok, <<"test">>} = py:eval(Ctx, <<"bytes(mv[:4])">>),
    {ok, <<"data">>} = py:eval(Ctx, <<"bytes(mv[5:])">>),

    %% Release memoryview
    ok = py:exec(Ctx, <<"mv.release()">>),

    ok.

%% @doc Test iterator protocol for line-by-line reading
iterator_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Create buffer and iterate
    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'line1\\nline2\\nline3\\n')
buf.close()
">>),
    {ok, Lines} = py:eval(Ctx, <<"list(buf)">>),
    [<<"line1\n">>, <<"line2\n">>, <<"line3\n">>] = Lines,

    ok.

%% @doc Test operations on closed buffer
closed_buffer_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Create buffer and close it
    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'data')
buf.close()
">>),

    %% Check closed property
    {ok, true} = py:eval(Ctx, <<"buf.closed">>),

    %% Reading should return remaining data then empty
    {ok, <<"data">>} = py:eval(Ctx, <<"buf.read()">>),
    {ok, <<>>} = py:eval(Ctx, <<"buf.read()">>),

    ok.

%% @doc Test empty buffer behavior
empty_buffer_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Create empty buffer and close immediately
    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'')
buf.close()
">>),

    %% Read should return empty
    {ok, <<>>} = py:eval(Ctx, <<"buf.read()">>),
    {ok, <<>>} = py:eval(Ctx, <<"buf.readline()">>),
    {ok, []} = py:eval(Ctx, <<"buf.readlines()">>),

    %% Length should be 0
    {ok, 0} = py:eval(Ctx, <<"len(buf)">>),

    %% File-like properties
    {ok, true} = py:eval(Ctx, <<"buf.readable()">>),
    {ok, false} = py:eval(Ctx, <<"buf.writable()">>),
    {ok, true} = py:eval(Ctx, <<"buf.seekable()">>),

    ok.

%% @doc Test passing buffer ref to Python - auto-conversion via py_convert.c
pass_to_python_test(_Config) ->
    {ok, Buf} = py_buffer:new(),

    %% Write data from Erlang
    ok = py_buffer:write(Buf, <<"chunk1">>),
    ok = py_buffer:write(Buf, <<"chunk2">>),
    ok = py_buffer:close(Buf),

    %% Pass buffer to Python via py:eval - should auto-convert to PyBuffer
    Ctx = py:context(1),

    %% Define a function that reads from a buffer
    ok = py:exec(Ctx, <<"
def read_buffer(buf):
    return buf.read()
">>),

    %% Call with buffer ref - py_convert.c should wrap it as PyBuffer
    {ok, <<"chunk1chunk2">>} = py:eval(Ctx, <<"read_buffer(buf)">>, #{<<"buf">> => Buf}),

    ok.

%% @doc Test that buffer resources are properly garbage collected
%% Verifies reference counting between Erlang and Python
gc_refcount_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import gc
from erlang import PyBuffer

# Track if we can create many buffers without memory issues
def create_and_release_buffers(count):
    '''Create many buffers and let them be GC'd.'''
    for i in range(count):
        buf = PyBuffer._test_create(b'x' * 1000)
        buf.close()
        # buf goes out of scope, should be released
    gc.collect()
    return True

def test_memoryview_refcount():
    '''Test that memoryview keeps buffer alive.'''
    buf = PyBuffer._test_create(b'test data')
    mv = memoryview(buf)

    # Buffer should stay alive while memoryview exists
    data = bytes(mv[:4])

    # Release memoryview
    mv.release()

    # Buffer should still be usable after memoryview release
    buf.close()
    remaining = buf.read()

    return data == b'test' and remaining == b'test data'

def test_multiple_views():
    '''Test multiple memoryviews on same buffer.'''
    buf = PyBuffer._test_create(b'hello world')
    mv1 = memoryview(buf)
    mv2 = memoryview(buf)

    result1 = bytes(mv1[:5])
    result2 = bytes(mv2[6:])

    mv1.release()
    mv2.release()
    buf.close()

    return result1 == b'hello' and result2 == b'world'
">>),

    %% Test 1: Create and release many buffers
    {ok, true} = py:eval(Ctx, <<"create_and_release_buffers(100)">>),

    %% Test 2: Memoryview reference counting
    {ok, true} = py:eval(Ctx, <<"test_memoryview_refcount()">>),

    %% Test 3: Multiple memoryviews
    {ok, true} = py:eval(Ctx, <<"test_multiple_views()">>),

    %% Test 4: Erlang-side reference counting
    %% Create buffer, pass to Python, let Erlang ref go out of scope
    {ok, Data} = begin
        {ok, TempBuf} = py_buffer:new(),
        ok = py_buffer:write(TempBuf, <<"erlang data">>),
        ok = py_buffer:close(TempBuf),
        %% Pass to Python - Python now holds a reference
        py:eval(Ctx, <<"buf.read()">>, #{<<"buf">> => TempBuf})
        %% TempBuf goes out of scope here but Python read it first
    end,
    <<"erlang data">> = Data,

    %% Force Erlang GC
    erlang:garbage_collect(),

    %% Test 5: Verify no crashes after GC
    {ok, true} = py:eval(Ctx, <<"True">>),

    ok.

%% @doc Test non-blocking read methods for async I/O
nonblock_read_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"from erlang import PyBuffer">>),

    %% Test read_nonblock returns available data immediately
    ok = py:exec(Ctx, <<"
buf = PyBuffer._test_create(b'hello world')
">>),

    %% readable_amount should return 11 (length of 'hello world')
    {ok, 11} = py:eval(Ctx, <<"buf.readable_amount()">>),

    %% at_eof should be False (buffer not closed yet)
    {ok, false} = py:eval(Ctx, <<"buf.at_eof()">>),

    %% read_nonblock should return available data
    {ok, <<"hello">>} = py:eval(Ctx, <<"buf.read_nonblock(5)">>),

    %% readable_amount should now be 6
    {ok, 6} = py:eval(Ctx, <<"buf.readable_amount()">>),

    %% read_nonblock with no size returns all remaining
    {ok, <<" world">>} = py:eval(Ctx, <<"buf.read_nonblock()">>),

    %% readable_amount should be 0 now
    {ok, 0} = py:eval(Ctx, <<"buf.readable_amount()">>),

    %% read_nonblock on empty buffer returns empty bytes (not blocking)
    {ok, <<>>} = py:eval(Ctx, <<"buf.read_nonblock()">>),

    %% Close buffer and check at_eof
    ok = py:exec(Ctx, <<"buf.close()">>),
    {ok, true} = py:eval(Ctx, <<"buf.at_eof()">>),

    %% Test async I/O pattern simulation
    ok = py:exec(Ctx, <<"
def async_read_simulation():
    '''Simulate async I/O read pattern.'''
    buf = PyBuffer._test_create(b'chunk1chunk2chunk3')
    buf.close()  # EOF

    chunks = []
    while not buf.at_eof():
        available = buf.readable_amount()
        if available > 0:
            # Read in chunks of 6
            chunk = buf.read_nonblock(6)
            chunks.append(chunk)
        else:
            # Would yield to event loop here in real async code
            break

    return chunks
">>),

    {ok, [<<"chunk1">>, <<"chunk2">>, <<"chunk3">>]} =
        py:eval(Ctx, <<"async_read_simulation()">>),

    ok.

%% @doc Test PyBuffer with asyncio - Erlang fills buffer while Python reads
asyncio_read_test(_Config) ->
    %% Create a buffer that Erlang will fill
    {ok, Buf} = py_buffer:new(),

    Ctx = py:context(1),
    Self = self(),

    %% Define async reader in Python
    ok = py:exec(Ctx, <<"
import asyncio

async def async_buffer_reader(buf):
    '''Read from buffer asynchronously as Erlang fills it.'''
    chunks = []

    while not buf.at_eof():
        available = buf.readable_amount()
        if available > 0:
            chunk = buf.read_nonblock(available)
            chunks.append(chunk)
        else:
            # Yield to event loop - Erlang is still writing
            await asyncio.sleep(0.005)

    return b''.join(chunks)

def run_async_reader(buf):
    '''Run async reader.'''
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(async_buffer_reader(buf))
    finally:
        loop.close()
">>),

    %% Spawn a process to write data with delays (simulating streaming)
    spawn_link(fun() ->
        timer:sleep(10),
        ok = py_buffer:write(Buf, <<"chunk1:">>),
        timer:sleep(20),
        ok = py_buffer:write(Buf, <<"chunk2:">>),
        timer:sleep(20),
        ok = py_buffer:write(Buf, <<"chunk3">>),
        timer:sleep(10),
        ok = py_buffer:close(Buf),
        Self ! writer_done
    end),

    %% Read asynchronously while Erlang writes
    {ok, Result} = py:eval(Ctx, <<"run_async_reader(buf)">>, #{<<"buf">> => Buf}),

    %% Wait for writer to finish
    receive writer_done -> ok after 1000 -> ok end,

    %% Verify we got all the data
    <<"chunk1:chunk2:chunk3">> = Result,

    ok.
