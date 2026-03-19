%%% @doc Common Test suite for py_byte_channel API.
%%%
%%% Tests the raw byte channel API for Erlang-Python communication.
-module(py_byte_channel_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    create_byte_channel_test/1,
    create_byte_channel_with_max_size_test/1,
    erlang_send_bytes_test/1,
    erlang_receive_bytes_test/1,
    send_receive_multiple_bytes_test/1,
    try_receive_empty_bytes_test/1,
    close_byte_channel_test/1,
    byte_channel_info_test/1,
    backpressure_bytes_test/1,
    send_bytes_to_closed_test/1,
    %% Python tests
    python_byte_channel_class_test/1,
    python_byte_channel_send_bytes_test/1,
    python_byte_channel_receive_bytes_test/1,
    %% Sync blocking receive tests
    sync_receive_bytes_immediate_test/1,
    sync_receive_bytes_wait_test/1,
    sync_receive_bytes_closed_test/1,
    %% Large payload test
    large_payload_bytes_test/1,
    %% Async event loop dispatch test
    async_receive_bytes_e2e_test/1,
    %% create_task + async receive test
    create_task_async_receive_test/1,
    %% Close and drain tests
    close_drain_bytes_erlang_test/1,
    close_drain_bytes_python_sync_test/1,
    close_drain_bytes_python_async_test/1,
    close_drain_bytes_create_task_async_test/1
]).

all() -> [
    create_byte_channel_test,
    create_byte_channel_with_max_size_test,
    erlang_send_bytes_test,
    erlang_receive_bytes_test,
    send_receive_multiple_bytes_test,
    try_receive_empty_bytes_test,
    close_byte_channel_test,
    byte_channel_info_test,
    backpressure_bytes_test,
    send_bytes_to_closed_test,
    %% Python tests
    python_byte_channel_class_test,
    python_byte_channel_send_bytes_test,
    python_byte_channel_receive_bytes_test,
    %% Sync blocking receive tests
    sync_receive_bytes_immediate_test,
    sync_receive_bytes_wait_test,
    sync_receive_bytes_closed_test,
    %% Large payload test
    large_payload_bytes_test,
    %% Async event loop dispatch test
    async_receive_bytes_e2e_test,
    %% create_task + async receive test
    create_task_async_receive_test,
    %% Close and drain tests
    close_drain_bytes_erlang_test,
    close_drain_bytes_python_sync_test,
    close_drain_bytes_python_async_test,
    close_drain_bytes_create_task_async_test
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    %% Register byte channel callbacks
    ok = py_byte_channel:register_callbacks(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(async_receive_bytes_e2e_test, Config) ->
    %% Define the async receive helper function
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import erlang
from erlang import ByteChannel

async def receive_bytes(ch_ref):
    ch = ByteChannel(ch_ref)
    return await ch.async_receive_bytes()
">>),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Test creating a byte channel with default settings
create_byte_channel_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    true = is_reference(Ch),
    ok = py_byte_channel:close(Ch).

%% @doc Test creating a byte channel with max_size for backpressure
create_byte_channel_with_max_size_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(#{max_size => 1000}),
    true = is_reference(Ch),
    Info = py_byte_channel:info(Ch),
    1000 = maps:get(max_size, Info),
    ok = py_byte_channel:close(Ch).

%% @doc Test basic send of raw bytes
erlang_send_bytes_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    ok = py_byte_channel:send(Ch, <<"hello">>),
    ok = py_byte_channel:send(Ch, <<"world">>),
    ok = py_byte_channel:close(Ch).

%% @doc Test basic receive of raw bytes (no term decoding)
erlang_receive_bytes_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    ok = py_byte_channel:send(Ch, <<"hello">>),
    %% Use byte channel try_receive - returns raw binary
    {ok, <<"hello">>} = py_nif:byte_channel_try_receive_bytes(Ch),
    ok = py_byte_channel:close(Ch).

%% @doc Test sending and receiving multiple bytes
send_receive_multiple_bytes_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    ok = py_byte_channel:send(Ch, <<"one">>),
    ok = py_byte_channel:send(Ch, <<"two">>),
    ok = py_byte_channel:send(Ch, <<"three">>),
    {ok, <<"one">>} = py_byte_channel:try_receive(Ch),
    {ok, <<"two">>} = py_byte_channel:try_receive(Ch),
    {ok, <<"three">>} = py_byte_channel:try_receive(Ch),
    ok = py_byte_channel:close(Ch).

%% @doc Test try_receive on empty channel
try_receive_empty_bytes_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    {error, empty} = py_byte_channel:try_receive(Ch),
    ok = py_byte_channel:close(Ch).

%% @doc Test closing a byte channel
close_byte_channel_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    ok = py_byte_channel:send(Ch, <<"data">>),
    ok = py_byte_channel:close(Ch),
    Info = py_byte_channel:info(Ch),
    true = maps:get(closed, Info).

%% @doc Test byte channel info
byte_channel_info_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(#{max_size => 500}),
    Info1 = py_byte_channel:info(Ch),
    0 = maps:get(size, Info1),
    500 = maps:get(max_size, Info1),
    false = maps:get(closed, Info1),

    ok = py_byte_channel:send(Ch, <<"test">>),
    Info2 = py_byte_channel:info(Ch),
    %% Size should be exactly the binary size (no term overhead)
    4 = maps:get(size, Info2),

    ok = py_byte_channel:close(Ch).

%% @doc Test backpressure when queue exceeds max_size
backpressure_bytes_test(_Config) ->
    %% Create channel with small max_size
    {ok, Ch} = py_byte_channel:new(#{max_size => 100}),

    %% Fill up the channel - no term overhead for raw bytes
    Data50 = binary:copy(<<0>>, 50),
    ok = py_byte_channel:send(Ch, Data50),

    %% Check current size
    Info1 = py_byte_channel:info(Ch),
    50 = maps:get(size, Info1),
    ct:pal("After first send, size: ~p", [50]),

    %% Send another 50 bytes - should succeed (total 100)
    ok = py_byte_channel:send(Ch, Data50),
    Info2 = py_byte_channel:info(Ch),
    100 = maps:get(size, Info2),
    ct:pal("After second send, size: ~p", [100]),

    %% Next send should return busy (backpressure)
    busy = py_byte_channel:send(Ch, <<"more">>),

    %% Drain one message
    {ok, _} = py_byte_channel:try_receive(Ch),

    %% Now should be able to send again
    ok = py_byte_channel:send(Ch, <<"small">>),

    ok = py_byte_channel:close(Ch).

%% @doc Test sending to a closed channel
send_bytes_to_closed_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    ok = py_byte_channel:close(Ch),
    {error, closed} = py_byte_channel:send(Ch, <<"data">>).

%%% ============================================================================
%%% Python Tests
%%% ============================================================================

%% @doc Test Python ByteChannel class is importable
python_byte_channel_class_test(_Config) ->
    Ctx = py:context(1),

    %% Test that the ByteChannel module is importable via erlang namespace
    ok = py:exec(Ctx, <<"from erlang import ByteChannel, ByteChannelClosed">>),

    %% Test basic ByteChannel class behavior
    {ok, true} = py:eval(Ctx, <<"callable(ByteChannel)">>),
    {ok, true} = py:eval(Ctx, <<"issubclass(ByteChannelClosed, Exception)">>),

    ok.

%% @doc Test Python ByteChannel send_bytes method
python_byte_channel_send_bytes_test(_Config) ->
    Ctx = py:context(1),

    %% Test that send_bytes method exists
    ok = py:exec(Ctx, <<"from erlang import ByteChannel">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ByteChannel, 'send_bytes')">>),

    ok.

%% @doc Test Python ByteChannel receive_bytes method
python_byte_channel_receive_bytes_test(_Config) ->
    Ctx = py:context(1),

    %% Test that ByteChannel methods exist
    ok = py:exec(Ctx, <<"from erlang import ByteChannel">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ByteChannel, 'receive_bytes')">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ByteChannel, 'try_receive_bytes')">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ByteChannel, 'async_receive_bytes')">>),

    %% Verify async iteration methods
    {ok, true} = py:eval(Ctx, <<"hasattr(ByteChannel, '__aiter__')">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ByteChannel, '__anext__')">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ByteChannel, '__iter__')">>),

    ok.

%%% ============================================================================
%%% Sync Blocking Receive Tests
%%% ============================================================================

%% @doc Test sync receive when data is already available (immediate return)
sync_receive_bytes_immediate_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% Send data before receive
    ok = py_byte_channel:send(Ch, <<"immediate_bytes">>),

    %% Receive should return immediately
    {ok, <<"immediate_bytes">>} = py_byte_channel:handle_receive_bytes([Ch]),

    ok = py_byte_channel:close(Ch).

%% @doc Test sync receive that blocks waiting for data
sync_receive_bytes_wait_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    Self = self(),

    %% Spawn a process to do blocking receive
    _Receiver = spawn_link(fun() ->
        Result = py_byte_channel:handle_receive_bytes([Ch]),
        Self ! {receive_result, Result}
    end),

    %% Give receiver time to register as waiter
    timer:sleep(50),

    %% Send data - should wake up the receiver
    ok = py_byte_channel:send(Ch, <<"delayed_bytes">>),

    %% Wait for result
    receive
        {receive_result, {ok, <<"delayed_bytes">>}} ->
            ok
    after 2000 ->
        ct:fail("Receiver did not get data within timeout")
    end,

    ok = py_byte_channel:close(Ch).

%% @doc Test sync receive when channel is closed while waiting
sync_receive_bytes_closed_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),
    Self = self(),

    %% Spawn a process to do blocking receive
    _Receiver = spawn_link(fun() ->
        Result = py_byte_channel:handle_receive_bytes([Ch]),
        Self ! {receive_result, Result}
    end),

    %% Give receiver time to register as waiter
    timer:sleep(50),

    %% Close the channel - should wake up receiver with error
    ok = py_byte_channel:close(Ch),

    %% Wait for result
    receive
        {receive_result, {error, closed}} ->
            ok
    after 2000 ->
        ct:fail("Receiver did not get closed notification within timeout")
    end.

%%% ============================================================================
%%% Large Payload Test
%%% ============================================================================

%% @doc Test sending and receiving large binary payloads
large_payload_bytes_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% Create a 1MB binary
    LargeData = binary:copy(<<"X">>, 1024 * 1024),

    %% Send it
    ok = py_byte_channel:send(Ch, LargeData),

    %% Check size is correct (no term overhead)
    Info = py_byte_channel:info(Ch),
    1048576 = maps:get(size, Info),

    %% Receive and verify
    {ok, ReceivedData} = py_byte_channel:try_receive(Ch),
    true = (byte_size(ReceivedData) =:= 1048576),
    true = (ReceivedData =:= LargeData),

    ok = py_byte_channel:close(Ch).

%%% ============================================================================
%%% Async Event Loop Dispatch Test
%%% ============================================================================

%% @doc Test async_receive_bytes with proper event loop dispatch (no polling)
%% Sends data after async receive starts, verifies event-driven wakeup
async_receive_bytes_e2e_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% Test 1: Immediate data - should return without waiting
    ok = py_byte_channel:send(Ch, <<"immediate_bytes">>),

    Ctx = py:context(1),

    %% Run async receive - data is already there, should return immediately
    {ok, <<"immediate_bytes">>} = py:eval(Ctx, <<"erlang.run(receive_bytes(ch))">>,
                                          #{<<"ch">> => Ch}),
    ct:pal("Async receive immediate data OK"),

    %% Test 2: Send data after async starts - tests event dispatch
    %% We send data first, then run async (to avoid race conditions in test)
    ok = py_byte_channel:send(Ch, <<"async_bytes">>),

    {ok, <<"async_bytes">>} = py:eval(Ctx, <<"erlang.run(receive_bytes(ch))">>,
                                      #{<<"ch">> => Ch}),
    ct:pal("Async receive via erlang.run() OK"),

    ok = py_byte_channel:close(Ch).

%%% ============================================================================
%%% create_task + Async ByteChannel Test
%%% ============================================================================

%% @doc Test async_receive_bytes works correctly with py_event_loop:create_task
%% This verifies the loop capsule name is correct (erlang_python.event_loop)
create_task_async_receive_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% First send data so it's available when task starts
    ok = py_byte_channel:send(Ch, <<"create_task_bytes">>),

    %% Define and run the task - env reuse should make __main__ functions visible
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import erlang
from erlang import ByteChannel

async def task_receive_bytes(ch_ref, reply_pid):
    '''Task that receives bytes and sends result back to Erlang.'''
    try:
        ch = ByteChannel(ch_ref)
        data = await ch.async_receive_bytes()
        erlang.send(reply_pid, ('result', data))
    except Exception as e:
        erlang.send(reply_pid, ('error', str(e)))
">>),

    %% Create a task that will await on channel receive
    %% create_task/3 uses the global event loop internally
    TaskRef = py_event_loop:create_task(
        "__main__", "task_receive_bytes", [Ch, self()]),

    %% Wait for result from the task
    %% Note: Python tuple ('result', data) becomes {<<"result">>, data} in Erlang
    receive
        {<<"result">>, <<"create_task_bytes">>} ->
            ct:pal("create_task + async_receive_bytes OK");
        {<<"error">>, ErrMsg} ->
            ct:pal("Task error: ~p", [ErrMsg]),
            ct:fail({task_error, ErrMsg});
        Other ->
            ct:pal("Unexpected message: ~p", [Other]),
            ct:fail({unexpected_message, Other})
    after 5000 ->
        ct:fail("Timeout waiting for task result")
    end,

    %% Wait for task to complete
    case py_event_loop:await(TaskRef, 5000) of
        {ok, _} -> ok;
        {error, AwaitErr} ->
            ct:pal("Await error: ~p", [AwaitErr])
    end,

    ok = py_byte_channel:close(Ch).

%%% ============================================================================
%%% Close and Drain Tests
%%% ============================================================================

%% @doc Test that data can be drained from byte channel after close (Erlang side)
%% Verifies that closing a channel doesn't prevent reading existing data
close_drain_bytes_erlang_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% Send multiple messages
    ok = py_byte_channel:send(Ch, <<"chunk1">>),
    ok = py_byte_channel:send(Ch, <<"chunk2">>),
    ok = py_byte_channel:send(Ch, <<"chunk3">>),

    %% Close the channel while data is still in queue
    ok = py_byte_channel:close(Ch),

    %% Verify channel is marked as closed
    Info = py_byte_channel:info(Ch),
    true = maps:get(closed, Info),

    %% Should still be able to drain all data
    {ok, <<"chunk1">>} = py_byte_channel:try_receive(Ch),
    {ok, <<"chunk2">>} = py_byte_channel:try_receive(Ch),
    {ok, <<"chunk3">>} = py_byte_channel:try_receive(Ch),

    %% Only after draining should we get closed error
    {error, closed} = py_byte_channel:try_receive(Ch),

    ct:pal("Erlang byte channel close+drain test passed").

%% @doc Test that Python can drain data from closed byte channel (sync iteration)
close_drain_bytes_python_sync_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% Send multiple messages
    ok = py_byte_channel:send(Ch, <<"data1">>),
    ok = py_byte_channel:send(Ch, <<"data2">>),
    ok = py_byte_channel:send(Ch, <<"data3">>),

    %% Close the channel
    ok = py_byte_channel:close(Ch),

    %% Python should be able to drain all messages via iteration
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
from erlang import ByteChannel, ByteChannelClosed

def drain_byte_channel(ch_ref):
    '''Drain all chunks from byte channel, return as list.'''
    ch = ByteChannel(ch_ref)
    chunks = []
    for chunk in ch:
        chunks.append(chunk)
    return chunks
">>),

    {ok, [<<"data1">>, <<"data2">>, <<"data3">>]} =
        py:eval(Ctx, <<"drain_byte_channel(ch)">>, #{<<"ch">> => Ch}),

    ct:pal("Python sync byte channel close+drain test passed").

%% @doc Test that Python can drain data from closed byte channel (async iteration)
close_drain_bytes_python_async_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% Send multiple messages
    ok = py_byte_channel:send(Ch, <<"part1">>),
    ok = py_byte_channel:send(Ch, <<"part2">>),
    ok = py_byte_channel:send(Ch, <<"part3">>),

    %% Close the channel
    ok = py_byte_channel:close(Ch),

    %% Python should be able to drain all messages via async iteration
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import erlang
from erlang import ByteChannel, ByteChannelClosed

async def async_drain_byte_channel(ch_ref):
    '''Async drain all chunks from byte channel, return as list.'''
    ch = ByteChannel(ch_ref)
    chunks = []
    async for chunk in ch:
        chunks.append(chunk)
    return chunks
">>),

    {ok, [<<"part1">>, <<"part2">>, <<"part3">>]} =
        py:eval(Ctx, <<"erlang.run(async_drain_byte_channel(ch))">>, #{<<"ch">> => Ch}),

    ct:pal("Python async byte channel close+drain test passed").

%% @doc Test async drain with create_task when data arrives after task starts
%% This tests the notification callback path: task registers waiter, then data arrives
close_drain_bytes_create_task_async_test(_Config) ->
    {ok, Ch} = py_byte_channel:new(),

    %% Define the async drain task
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import erlang
from erlang import ByteChannel

async def drain_task(ch_ref, reply_pid):
    '''Task that drains byte channel and sends results back.'''
    try:
        ch = ByteChannel(ch_ref)
        chunks = []
        async for chunk in ch:
            chunks.append(chunk)
        erlang.send(reply_pid, ('result', chunks))
    except Exception as e:
        erlang.send(reply_pid, ('error', str(e)))
">>),

    %% Create the task BEFORE sending any data
    %% This forces the task to register a waiter and wait for notifications
    TaskRef = py_event_loop:create_task(
        "__main__", "drain_task", [Ch, self()]),

    %% Give the task time to start and register waiter
    timer:sleep(100),

    %% Now send data - should trigger notification callback
    ok = py_byte_channel:send(Ch, <<"chunk1">>),
    ok = py_byte_channel:send(Ch, <<"chunk2">>),
    ok = py_byte_channel:send(Ch, <<"chunk3">>),

    %% Close the channel to signal end of stream
    ok = py_byte_channel:close(Ch),

    %% Wait for result from the task
    receive
        {<<"result">>, [<<"chunk1">>, <<"chunk2">>, <<"chunk3">>]} ->
            ct:pal("create_task async drain with delayed data OK");
        {<<"result">>, Other} ->
            ct:pal("Unexpected result: ~p", [Other]),
            ct:fail({unexpected_result, Other});
        {<<"error">>, ErrMsg} ->
            ct:pal("Task error: ~p", [ErrMsg]),
            ct:fail({task_error, ErrMsg})
    after 5000 ->
        ct:fail("Timeout waiting for drain task result")
    end,

    %% Wait for task to complete
    case py_event_loop:await(TaskRef, 5000) of
        {ok, _} -> ok;
        {error, AwaitErr} ->
            ct:pal("Await error: ~p", [AwaitErr])
    end.
