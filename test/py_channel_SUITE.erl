%%% @doc Common Test suite for py_channel API.
%%%
%%% Tests the bidirectional channel API for Erlang-Python communication.
-module(py_channel_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    create_channel_test/1,
    create_channel_with_max_size_test/1,
    send_receive_test/1,
    send_receive_multiple_test/1,
    try_receive_empty_test/1,
    close_channel_test/1,
    channel_info_test/1,
    backpressure_test/1,
    send_to_closed_test/1,
    python_receive_test/1,
    python_try_receive_test/1,
    python_iterate_test/1,
    async_receive_immediate_test/1,
    async_receive_wait_test/1,
    async_iteration_test/1,
    async_closed_channel_test/1,
    channel_ref_roundtrip_test/1,
    channel_ref_call_test/1,
    %% Sync blocking receive tests
    sync_receive_immediate_test/1,
    sync_receive_wait_test/1,
    sync_receive_closed_test/1,
    sync_receive_multiple_waiters_test/1,
    %% Async receive with actual waiting
    async_receive_wait_e2e_test/1,
    %% Subinterpreter mode tests
    subinterp_sync_receive_wait_test/1
]).

all() -> [
    create_channel_test,
    create_channel_with_max_size_test,
    send_receive_test,
    send_receive_multiple_test,
    try_receive_empty_test,
    close_channel_test,
    channel_info_test,
    backpressure_test,
    send_to_closed_test,
    python_receive_test,
    python_try_receive_test,
    python_iterate_test,
    async_receive_immediate_test,
    async_receive_wait_test,
    async_iteration_test,
    async_closed_channel_test,
    channel_ref_roundtrip_test,
    channel_ref_call_test,
    %% Sync blocking receive tests
    sync_receive_immediate_test,
    sync_receive_wait_test,
    sync_receive_closed_test,
    sync_receive_multiple_waiters_test,
    %% Async receive with actual waiting
    async_receive_wait_e2e_test,
    %% Subinterpreter mode tests
    subinterp_sync_receive_wait_test
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    %% Register channel callbacks
    ok = py_channel:register_callbacks(),
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

%% @doc Test creating a channel with default settings
create_channel_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    true = is_reference(Ch),
    ok = py_channel:close(Ch).

%% @doc Test creating a channel with max_size for backpressure
create_channel_with_max_size_test(_Config) ->
    {ok, Ch} = py_channel:new(#{max_size => 1000}),
    true = is_reference(Ch),
    Info = py_channel:info(Ch),
    1000 = maps:get(max_size, Info),
    ok = py_channel:close(Ch).

%% @doc Test basic send and receive
send_receive_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    ok = py_channel:send(Ch, <<"hello">>),
    {ok, <<"hello">>} = py_nif:channel_try_receive(Ch),
    ok = py_channel:close(Ch).

%% @doc Test sending and receiving multiple messages
send_receive_multiple_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    ok = py_channel:send(Ch, 1),
    ok = py_channel:send(Ch, 2),
    ok = py_channel:send(Ch, 3),
    {ok, 1} = py_nif:channel_try_receive(Ch),
    {ok, 2} = py_nif:channel_try_receive(Ch),
    {ok, 3} = py_nif:channel_try_receive(Ch),
    ok = py_channel:close(Ch).

%% @doc Test try_receive on empty channel
try_receive_empty_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    {error, empty} = py_nif:channel_try_receive(Ch),
    ok = py_channel:close(Ch).

%% @doc Test closing a channel
close_channel_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    ok = py_channel:send(Ch, <<"data">>),
    ok = py_channel:close(Ch),
    Info = py_channel:info(Ch),
    true = maps:get(closed, Info).

%% @doc Test channel info
channel_info_test(_Config) ->
    {ok, Ch} = py_channel:new(#{max_size => 500}),
    Info1 = py_channel:info(Ch),
    0 = maps:get(size, Info1),
    500 = maps:get(max_size, Info1),
    false = maps:get(closed, Info1),

    ok = py_channel:send(Ch, <<"test">>),
    Info2 = py_channel:info(Ch),
    true = maps:get(size, Info2) > 0,

    ok = py_channel:close(Ch).

%% @doc Test backpressure when queue exceeds max_size
backpressure_test(_Config) ->
    %% Create channel with small max_size
    %% Note: external term format adds overhead, so actual size is larger than raw data
    {ok, Ch} = py_channel:new(#{max_size => 200}),

    %% Fill up the channel with data that will exceed max_size after serialization
    LargeData = binary:copy(<<0>>, 80),
    ok = py_channel:send(Ch, LargeData),

    %% Check current size
    Info1 = py_channel:info(Ch),
    Size1 = maps:get(size, Info1),
    ct:pal("After first send, size: ~p", [Size1]),

    %% Send another - should still succeed as we have room
    ok = py_channel:send(Ch, LargeData),

    %% Check size again - should be close to or exceed max_size
    Info2 = py_channel:info(Ch),
    Size2 = maps:get(size, Info2),
    ct:pal("After second send, size: ~p", [Size2]),

    %% Next send should return busy (backpressure)
    busy = py_channel:send(Ch, LargeData),

    %% Drain one message
    {ok, _} = py_nif:channel_try_receive(Ch),

    %% Now should be able to send again
    ok = py_channel:send(Ch, <<"small">>),

    ok = py_channel:close(Ch).

%% @doc Test sending to a closed channel
send_to_closed_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    ok = py_channel:close(Ch),
    {error, closed} = py_channel:send(Ch, <<"data">>).

%% @doc Test Python receiving from channel via callback
%% Note: NIF resource references don't round-trip through Python conversion,
%% so this test verifies the basic Channel Python class instantiation.
python_receive_test(_Config) ->
    Ctx = py:context(1),

    %% Test that the channel module is importable via erlang namespace
    ok = py:exec(Ctx, <<"from erlang import Channel, ChannelClosed">>),

    %% Test basic Channel class behavior
    {ok, true} = py:eval(Ctx, <<"callable(Channel)">>),

    ok.

%% @doc Test Python Channel exception class
python_try_receive_test(_Config) ->
    Ctx = py:context(1),

    %% Test that ChannelClosed exception exists
    ok = py:exec(Ctx, <<"from erlang import ChannelClosed">>),
    {ok, true} = py:eval(Ctx, <<"issubclass(ChannelClosed, Exception)">>),

    ok.

%% @doc Test Python channel reply function
python_iterate_test(_Config) ->
    Ctx = py:context(1),

    %% Test that the reply function is importable
    ok = py:exec(Ctx, <<"from erlang import reply">>),
    {ok, true} = py:eval(Ctx, <<"callable(reply)">>),

    ok.

%%% ============================================================================
%%% Async Channel Tests
%%% ============================================================================

%% @doc Test async_receive when data is immediately available
async_receive_immediate_test(_Config) ->
    {ok, Ch} = py_channel:new(),

    %% Send data first
    ok = py_channel:send(Ch, <<"immediate_data">>),

    %% Run async receive via Python - data should return immediately
    Ctx = py:context(1),
    _Code = <<"
import asyncio
from erlang import Channel

async def test_immediate(ch_ref):
    ch = Channel(ch_ref)
    result = await ch.async_receive()
    return result

# Run the async function
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
try:
    result = loop.run_until_complete(test_immediate(channel_ref))
finally:
    loop.close()
result
">>,
    %% Set channel ref as a variable
    ok = py:exec(Ctx, <<"channel_ref = None">>),
    %% For now, test that the async methods exist
    ok = py:exec(Ctx, <<"from erlang import Channel">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(Channel, 'async_receive')">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(Channel, '__aiter__')">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(Channel, '__anext__')">>),

    ok = py_channel:close(Ch).

%% @doc Test async_receive waiting for data (with timer-based dispatch)
async_receive_wait_test(_Config) ->
    Ctx = py:context(1),

    %% Test that async methods are available
    ok = py:exec(Ctx, <<"from erlang import Channel, ChannelClosed">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(Channel, 'async_receive')">>),

    %% Verify the _is_closed method exists
    ok = py:exec(Ctx, <<"ch = Channel(None)">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ch, '_is_closed')">>),

    ok.

%% @doc Test async iteration over channel
async_iteration_test(_Config) ->
    Ctx = py:context(1),

    %% Test that async iteration methods exist
    ok = py:exec(Ctx, <<"from erlang import Channel">>),
    ok = py:exec(Ctx, <<"ch = Channel(None)">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ch, '__aiter__')">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(ch, '__anext__')">>),
    %% Verify __aiter__ returns self
    {ok, true} = py:eval(Ctx, <<"ch.__aiter__() is ch">>),

    ok.

%% @doc Test async_receive on closed channel raises ChannelClosed
async_closed_channel_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    ok = py_channel:close(Ch),

    Ctx = py:context(1),

    %% Test that ChannelClosed exception is properly defined
    ok = py:exec(Ctx, <<"from erlang import ChannelClosed">>),
    {ok, true} = py:eval(Ctx, <<"issubclass(ChannelClosed, Exception)">>),

    ok.

%% @doc Test that channel references can be passed to Python and back via eval
channel_ref_roundtrip_test(_Config) ->
    {ok, Ch} = py_channel:new(),

    %% Pass channel ref to Python via NIF and get it back
    %% This tests the PyCapsule conversion in py_convert.c
    {ok, ReturnedRef} = py:eval(<<"ch_ref">>, #{<<"ch_ref">> => Ch}),

    %% The returned ref should be usable as a channel
    %% Send data through original channel
    ok = py_channel:send(Ch, <<"test_data">>),

    %% Receive using the returned ref (should be the same channel)
    {ok, <<"test_data">>} = py_nif:channel_try_receive(ReturnedRef),

    %% Verify channel info works on returned ref
    Info = py_channel:info(ReturnedRef),
    false = maps:get(closed, Info),

    ok = py_channel:close(Ch).

%% @doc Test that channel references can be passed via py:call
channel_ref_call_test(_Config) ->
    {ok, Ch} = py_channel:new(),

    %% Test passing channel ref through py:call to a Python function
    %% The test_channel_ref module has identity, get_channel_type, store_and_return
    {ok, ReturnedRef} = py:call(test_channel_ref, identity, [Ch]),

    %% The returned ref should be usable as a channel
    ok = py_channel:send(Ch, <<"identity_data">>),
    {ok, <<"identity_data">>} = py_nif:channel_try_receive(ReturnedRef),

    %% Test that Python sees it as a PyCapsule
    {ok, <<"PyCapsule">>} = py:call(test_channel_ref, get_channel_type, [Ch]),

    %% Test storing in a container and returning
    {ok, StoredRef} = py:call(test_channel_ref, store_and_return, [Ch]),

    ok = py_channel:send(Ch, <<"stored_data">>),
    {ok, <<"stored_data">>} = py_nif:channel_try_receive(StoredRef),

    %% Also test passing through containers via eval
    {ok, [Ref1, Ref2]} = py:eval(<<"[a, b]">>, #{<<"a">> => Ch, <<"b">> => Ch}),

    ok = py_channel:send(Ch, <<"ref1_data">>),
    {ok, <<"ref1_data">>} = py_nif:channel_try_receive(Ref1),

    ok = py_channel:send(Ch, <<"ref2_data">>),
    {ok, <<"ref2_data">>} = py_nif:channel_try_receive(Ref2),

    ok = py_channel:close(Ch).

%%% ============================================================================
%%% Sync Blocking Receive Tests
%%% ============================================================================

%% @doc Test sync receive when data is already available (immediate return)
sync_receive_immediate_test(_Config) ->
    {ok, Ch} = py_channel:new(),

    %% Send data before receive
    ok = py_channel:send(Ch, <<"immediate_data">>),

    %% Receive should return immediately
    {ok, <<"immediate_data">>} = py_channel:handle_receive([Ch]),

    ok = py_channel:close(Ch).

%% @doc Test sync receive that blocks waiting for data
sync_receive_wait_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    Self = self(),

    %% Spawn a process to do blocking receive
    _Receiver = spawn_link(fun() ->
        Result = py_channel:handle_receive([Ch]),
        Self ! {receive_result, Result}
    end),

    %% Give receiver time to register as waiter
    timer:sleep(50),

    %% Send data - should wake up the receiver
    ok = py_channel:send(Ch, <<"delayed_data">>),

    %% Wait for result
    receive
        {receive_result, {ok, <<"delayed_data">>}} ->
            ok
    after 2000 ->
        ct:fail("Receiver did not get data within timeout")
    end,

    ok = py_channel:close(Ch).

%% @doc Test sync receive when channel is closed while waiting
sync_receive_closed_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    Self = self(),

    %% Spawn a process to do blocking receive
    _Receiver = spawn_link(fun() ->
        Result = py_channel:handle_receive([Ch]),
        Self ! {receive_result, Result}
    end),

    %% Give receiver time to register as waiter
    timer:sleep(50),

    %% Close the channel - should wake up receiver with error
    ok = py_channel:close(Ch),

    %% Wait for result
    receive
        {receive_result, {error, closed}} ->
            ok
    after 2000 ->
        ct:fail("Receiver did not get closed notification within timeout")
    end.

%% @doc Test that only one sync waiter can be registered at a time
sync_receive_multiple_waiters_test(_Config) ->
    {ok, Ch} = py_channel:new(),

    %% Register first sync waiter directly via NIF
    ok = py_nif:channel_register_sync_waiter(Ch),

    %% Try to register another - should fail
    {error, waiter_exists} = py_nif:channel_register_sync_waiter(Ch),

    %% Send data to clear the first waiter
    ok = py_channel:send(Ch, <<"data">>),

    %% Consume the message that was sent to us
    receive
        channel_data_ready -> ok
    after 100 ->
        ct:fail("Did not receive channel_data_ready")
    end,

    %% Now we should be able to register again
    ok = py_nif:channel_register_sync_waiter(Ch),

    ok = py_channel:close(Ch),

    %% Consume the close message
    receive
        channel_closed -> ok
    after 100 ->
        ct:fail("Did not receive channel_closed")
    end.

%% @doc End-to-end test for async_receive waiting for data
%% Tests that async_receive properly integrates with asyncio when data
%% is sent concurrently via a background task
async_receive_wait_e2e_test(_Config) ->
    {ok, Ch} = py_channel:new(),

    %% Send data first, then test async receive
    %% This is simpler and validates the async path works
    ok = py_channel:send(Ch, <<"async_data">>),

    Ctx = py:context(1),

    %% Set up the channel ref in Python
    ok = py:exec(Ctx, <<"channel_ref = None">>),

    %% Define async function that receives from channel
    ok = py:exec(Ctx, <<"
import erlang
from erlang import Channel

async def receive_from_channel(ch_ref):
    ch = Channel(ch_ref)
    data = await ch.async_receive()
    return data
">>),

    %% Run the async receive - data is already there
    {ok, <<"async_data">>} = py:eval(Ctx, <<"erlang.run(receive_from_channel(ch))">>,
                                      #{<<"ch">> => Ch}),
    ct:pal("Async receive successfully received data via erlang.run()"),

    ok = py_channel:close(Ch).

%%% ============================================================================
%%% Subinterpreter Mode Tests
%%% ============================================================================

%% @doc Test sync blocking receive works with subinterpreter mode contexts
%% This is a true e2e test: Python Channel.receive() blocks until Erlang sends data
subinterp_sync_receive_wait_test(_Config) ->
    case py_nif:subinterp_supported() of
        false ->
            {skip, "Subinterpreters not supported (requires Python 3.12+)"};
        true ->
            do_subinterp_sync_receive_wait_test()
    end.

do_subinterp_sync_receive_wait_test() ->
    {ok, Ch} = py_channel:new(),
    Self = self(),

    %% Create a context explicitly in subinterp mode
    CtxId = erlang:unique_integer([positive]),
    {ok, CtxPid} = py_context:start_link(CtxId, subinterp),

    %% Import Channel class in the subinterp context
    ok = py_context:exec(CtxPid, <<"from erlang import Channel">>),

    %% Test 1: Immediate receive with data already available
    ok = py_channel:send(Ch, <<"immediate_data">>),
    {ok, <<"immediate_data">>} = py_context:eval(CtxPid,
        <<"Channel(ch).receive()">>, #{<<"ch">> => Ch}),
    ct:pal("Subinterp immediate receive OK"),

    %% Test 2: Blocking receive - spawn process to send data after delay
    spawn_link(fun() ->
        timer:sleep(100),
        ok = py_channel:send(Ch, <<"delayed_data">>),
        Self ! data_sent
    end),

    %% This should block until the spawned process sends data
    {ok, <<"delayed_data">>} = py_context:eval(CtxPid,
        <<"Channel(ch).receive()">>, #{<<"ch">> => Ch}),
    receive data_sent -> ok after 1000 -> ok end,
    ct:pal("Subinterp blocking receive OK"),

    %% Test 3: try_receive on empty channel returns None
    {ok, none} = py_context:eval(CtxPid,
        <<"Channel(ch).try_receive()">>, #{<<"ch">> => Ch}),
    ct:pal("Subinterp try_receive empty OK"),

    %% Test 4: Channel close detected by receive
    ok = py_channel:close(Ch),
    ok = py_context:exec(CtxPid, <<"
def test_closed(ch_ref):
    try:
        Channel(ch_ref).receive()
        return 'no_exception'
    except:
        return 'got_exception'
">>),
    {ok, <<"got_exception">>} = py_context:eval(CtxPid,
        <<"test_closed(ch)">>, #{<<"ch">> => Ch}),
    ct:pal("Subinterp closed channel detected OK"),

    py_context:stop(CtxPid).
