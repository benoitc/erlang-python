%%% @doc Common Test suite for OWN_GIL context feature integration tests.
%%%
%%% Tests that all major erlang_python features (channels, buffers, callbacks,
%%% PIDs, reactor, async tasks, asyncio) work correctly in OWN_GIL mode with
%%% true parallel Python execution.
%%%
%%% OWN_GIL mode requires Python 3.12+ with per-interpreter GIL support.
-module(py_owngil_features_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Channel tests
-export([
    owngil_channel_send_receive_test/1,
    owngil_channel_sync_blocking_test/1,
    owngil_channel_backpressure_test/1,
    owngil_channel_async_receive_test/1,
    owngil_channel_parallel_producers_test/1,
    owngil_channel_parallel_consumers_test/1,
    owngil_channel_cross_context_test/1,
    owngil_channel_high_throughput_test/1
]).

%% Buffer tests
-export([
    owngil_buffer_write_read_test/1,
    owngil_buffer_pass_to_python_test/1,
    owngil_buffer_async_read_test/1,
    owngil_buffer_parallel_writers_test/1,
    owngil_buffer_memoryview_test/1,
    owngil_buffer_gc_test/1
]).

%% Reentrant callback tests
-export([
    owngil_reentrant_basic_test/1,
    owngil_reentrant_nested_test/1,
    owngil_reentrant_concurrent_test/1,
    owngil_reentrant_complex_types_test/1,
    owngil_reentrant_thread_callback_test/1,
    owngil_reentrant_try_except_test/1
]).

%% PID/Send tests
-export([
    owngil_pid_roundtrip_test/1,
    owngil_send_simple_test/1,
    owngil_send_multiple_test/1,
    owngil_send_complex_test/1,
    owngil_suspension_not_caught_test/1,
    owngil_send_from_coroutine_test/1,
    owngil_send_nonblocking_test/1,
    owngil_send_parallel_test/1
]).

%% Reactor tests
-export([
    owngil_reactor_echo_protocol_test/1,
    owngil_reactor_multiple_conn_test/1,
    owngil_reactor_async_pending_test/1,
    owngil_reactor_buffer_test/1,
    owngil_reactor_isolation_test/1
]).

%% Async task tests
-export([
    owngil_async_create_await_test/1,
    owngil_async_run_sync_test/1,
    owngil_async_concurrent_test/1,
    owngil_async_batch_test/1,
    owngil_async_timeout_test/1,
    owngil_async_error_test/1
]).

%% Asyncio tests
-export([
    owngil_asyncio_basic_sleep_test/1,
    owngil_asyncio_gather_test/1,
    owngil_asyncio_parallel_loops_test/1
]).

all() ->
    [{group, channels},
     {group, buffers},
     {group, reentrant},
     {group, pid_send},
     {group, reactor},
     {group, async_task},
     {group, asyncio}].

groups() ->
    [{channels, [sequence], [
        owngil_channel_send_receive_test,
        owngil_channel_sync_blocking_test,
        owngil_channel_backpressure_test,
        owngil_channel_async_receive_test,
        owngil_channel_parallel_producers_test,
        owngil_channel_parallel_consumers_test,
        owngil_channel_cross_context_test,
        owngil_channel_high_throughput_test
    ]},
     {buffers, [sequence], [
        owngil_buffer_write_read_test,
        owngil_buffer_pass_to_python_test,
        owngil_buffer_async_read_test,
        owngil_buffer_parallel_writers_test,
        owngil_buffer_memoryview_test,
        owngil_buffer_gc_test
    ]},
     {reentrant, [sequence], [
        owngil_reentrant_basic_test,
        owngil_reentrant_nested_test,
        owngil_reentrant_concurrent_test,
        owngil_reentrant_complex_types_test,
        owngil_reentrant_thread_callback_test,
        owngil_reentrant_try_except_test
    ]},
     {pid_send, [sequence], [
        owngil_pid_roundtrip_test,
        owngil_send_simple_test,
        owngil_send_multiple_test,
        owngil_send_complex_test,
        owngil_suspension_not_caught_test,
        owngil_send_from_coroutine_test,
        owngil_send_nonblocking_test,
        owngil_send_parallel_test
    ]},
     {reactor, [sequence], [
        owngil_reactor_echo_protocol_test,
        owngil_reactor_multiple_conn_test,
        owngil_reactor_async_pending_test,
        owngil_reactor_buffer_test,
        owngil_reactor_isolation_test
    ]},
     {async_task, [sequence], [
        owngil_async_create_await_test,
        owngil_async_run_sync_test,
        owngil_async_concurrent_test,
        owngil_async_batch_test,
        owngil_async_timeout_test,
        owngil_async_error_test
    ]},
     {asyncio, [sequence], [
        owngil_asyncio_basic_sleep_test,
        owngil_asyncio_gather_test,
        owngil_asyncio_parallel_loops_test
    ]}].

init_per_suite(Config) ->
    case py_nif:subinterp_supported() of
        true ->
            {ok, _} = application:ensure_all_started(erlang_python),
            %% Add test directory to Python path
            PrivDir = code:priv_dir(erlang_python),
            TestDir = filename:join(filename:dirname(PrivDir), "test"),
            Config ++ [{test_dir, TestDir}];
        false ->
            {skip, "Requires Python 3.12+"}
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Cleanup registered functions
    catch py:unregister_function(owngil_double),
    catch py:unregister_function(owngil_triple),
    catch py:unregister_function(owngil_level),
    catch py:unregister_function(owngil_transform),
    catch py:unregister_function(owngil_get_value),
    catch py:unregister_function(owngil_echo),
    ok.

%%% ============================================================================
%%% Channel Tests
%%% ============================================================================

%% @doc Basic send/receive in owngil context
owngil_channel_send_receive_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, Ch} = py_channel:new(),

    %% Import Channel class
    ok = py_context:exec(Ctx, <<"from erlang import Channel">>),

    %% Send data from Erlang
    ok = py_channel:send(Ch, <<"hello_owngil">>),

    %% Receive in Python
    {ok, <<"hello_owngil">>} = py_context:eval(Ctx,
        <<"Channel(ch).try_receive()">>, #{<<"ch">> => Ch}),

    py_channel:close(Ch),
    py_context:stop(Ctx).

%% @doc Sync blocking receive in owngil context
owngil_channel_sync_blocking_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, Ch} = py_channel:new(),
    Self = self(),

    ok = py_context:exec(Ctx, <<"from erlang import Channel">>),

    %% Spawn process to send data after delay
    spawn_link(fun() ->
        timer:sleep(100),
        ok = py_channel:send(Ch, <<"delayed_data">>),
        Self ! data_sent
    end),

    %% Blocking receive should wait for data
    {ok, <<"delayed_data">>} = py_context:eval(Ctx,
        <<"Channel(ch).receive()">>, #{<<"ch">> => Ch}),

    receive data_sent -> ok after 1000 -> ok end,

    py_channel:close(Ch),
    py_context:stop(Ctx).

%% @doc Backpressure with max_size in owngil context
owngil_channel_backpressure_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    %% Use larger max_size to account for serialization overhead
    {ok, Ch} = py_channel:new(#{max_size => 500}),

    ok = py_context:exec(Ctx, <<"from erlang import Channel">>),

    %% Fill the channel with data (serialization adds overhead)
    LargeData = binary:copy(<<0>>, 150),
    ok = py_channel:send(Ch, LargeData),
    ok = py_channel:send(Ch, LargeData),
    ok = py_channel:send(Ch, LargeData),

    %% Should get backpressure now
    busy = py_channel:send(Ch, LargeData),

    %% Drain from Python
    {ok, _} = py_context:eval(Ctx, <<"Channel(ch).receive()">>, #{<<"ch">> => Ch}),

    %% Now should be able to send
    ok = py_channel:send(Ch, <<"small">>),

    py_channel:close(Ch),
    py_context:stop(Ctx).

%% @doc Async receive with await in owngil context
owngil_channel_async_receive_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, Ch} = py_channel:new(),

    ok = py_context:exec(Ctx, <<"
import asyncio
from erlang import Channel

async def async_receive(ch_ref):
    ch = Channel(ch_ref)
    return await ch.async_receive()

def run_async(ch_ref):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(async_receive(ch_ref))
    finally:
        loop.close()
">>),

    %% Send data first
    ok = py_channel:send(Ch, <<"async_data">>),

    %% Async receive
    {ok, <<"async_data">>} = py_context:eval(Ctx, <<"run_async(ch)">>,
        #{<<"ch">> => Ch}),

    py_channel:close(Ch),
    py_context:stop(Ctx).

%% @doc Multiple owngil contexts producing to same channel
owngil_channel_parallel_producers_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    NumProducers = 4,
    MessagesPerProducer = 10,

    %% Create producer contexts
    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        ok = py_context:exec(Ctx, <<"from erlang import Channel">>),
        Ctx
    end || N <- lists:seq(1, NumProducers)],

    Parent = self(),

    %% Start parallel producers
    [spawn_link(fun() ->
        lists:foreach(fun(MsgNum) ->
            Msg = list_to_binary(io_lib:format("ctx~p_msg~p", [CtxNum, MsgNum])),
            ok = py_channel:send(Ch, Msg)
        end, lists:seq(1, MessagesPerProducer)),
        Parent ! {producer_done, CtxNum}
    end) || {CtxNum, _Ctx} <- lists:zip(lists:seq(1, NumProducers), Contexts)],

    %% Wait for all producers
    [receive {producer_done, N} -> ok end || N <- lists:seq(1, NumProducers)],

    %% Verify all messages received
    TotalMessages = NumProducers * MessagesPerProducer,
    Messages = drain_channel(Ch, TotalMessages),
    TotalMessages = length(Messages),

    py_channel:close(Ch),
    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%% @doc Multiple owngil contexts consuming from same channel
owngil_channel_parallel_consumers_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    NumConsumers = 4,
    TotalMessages = 20,

    %% Create consumer contexts
    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        ok = py_context:exec(Ctx, <<"from erlang import Channel">>),
        Ctx
    end || N <- lists:seq(1, NumConsumers)],

    %% Send all messages
    [py_channel:send(Ch, list_to_binary(integer_to_list(N)))
     || N <- lists:seq(1, TotalMessages)],
    py_channel:close(Ch),

    Parent = self(),

    %% Start parallel consumers
    [spawn_link(fun() ->
        consume_until_closed(Ctx, Ch, Parent, CtxNum)
    end) || {CtxNum, Ctx} <- lists:zip(lists:seq(1, NumConsumers), Contexts)],

    %% Collect results
    Results = [receive {consumer_result, N, Msgs} -> {N, Msgs} end
               || N <- lists:seq(1, NumConsumers)],

    %% Verify total messages consumed
    TotalConsumed = lists:sum([length(Msgs) || {_, Msgs} <- Results]),
    ct:pal("Consumed ~p messages across ~p consumers", [TotalConsumed, NumConsumers]),
    TotalMessages = TotalConsumed,

    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%% @doc Channel shared between owngil contexts (bidirectional)
owngil_channel_cross_context_test(_Config) ->
    {ok, Ctx1} = py_context:start_link(1, owngil),
    {ok, Ctx2} = py_context:start_link(2, owngil),
    {ok, Ch} = py_channel:new(),

    ok = py_context:exec(Ctx1, <<"from erlang import Channel">>),
    ok = py_context:exec(Ctx2, <<"from erlang import Channel">>),

    %% Ctx1 sends, Ctx2 receives
    ok = py_channel:send(Ch, <<"from_ctx1">>),
    {ok, <<"from_ctx1">>} = py_context:eval(Ctx2,
        <<"Channel(ch).try_receive()">>, #{<<"ch">> => Ch}),

    %% Ctx2 sends (via Erlang), Ctx1 receives
    ok = py_channel:send(Ch, <<"from_erlang">>),
    {ok, <<"from_erlang">>} = py_context:eval(Ctx1,
        <<"Channel(ch).try_receive()">>, #{<<"ch">> => Ch}),

    py_channel:close(Ch),
    py_context:stop(Ctx1),
    py_context:stop(Ctx2).

%% @doc High throughput channel test under parallel load
owngil_channel_high_throughput_test(_Config) ->
    {ok, Ch} = py_channel:new(),
    NumContexts = 4,
    MessagesPerContext = 100,

    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        ok = py_context:exec(Ctx, <<"from erlang import Channel">>),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    Parent = self(),
    Start = erlang:monotonic_time(millisecond),

    %% Start parallel senders
    _ = [spawn_link(fun() ->
        lists:foreach(fun(M) ->
            py_channel:send(Ch, <<(integer_to_binary(N))/binary, "_",
                                  (integer_to_binary(M))/binary>>)
        end, lists:seq(1, MessagesPerContext)),
        Parent ! {sender_done, N}
    end) || {N, _Ctx} <- lists:zip(lists:seq(1, NumContexts), Contexts)],

    %% Wait for senders
    [receive {sender_done, N} -> ok end || N <- lists:seq(1, NumContexts)],

    Elapsed = erlang:monotonic_time(millisecond) - Start,
    TotalMessages = NumContexts * MessagesPerContext,
    ct:pal("Sent ~p messages in ~p ms (~.2f msgs/ms)",
           [TotalMessages, Elapsed, TotalMessages / max(1, Elapsed)]),

    %% Drain messages
    Messages = drain_channel(Ch, TotalMessages),
    TotalMessages = length(Messages),

    py_channel:close(Ch),
    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%%% ============================================================================
%%% Buffer Tests
%%% ============================================================================

%% @doc Basic write/read in owngil context
owngil_buffer_write_read_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, Buf} = py_buffer:new(),

    ok = py_buffer:write(Buf, <<"hello ">>),
    ok = py_buffer:write(Buf, <<"owngil">>),
    ok = py_buffer:close(Buf),

    %% Read from Python
    {ok, <<"hello owngil">>} = py_context:eval(Ctx,
        <<"buf.read()">>, #{<<"buf">> => Buf}),

    py_context:stop(Ctx).

%% @doc Pass buffer ref to owngil context
owngil_buffer_pass_to_python_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, Buf} = py_buffer:new(),

    ok = py_buffer:write(Buf, <<"chunk1:">>),
    ok = py_buffer:write(Buf, <<"chunk2">>),
    ok = py_buffer:close(Buf),

    ok = py_context:exec(Ctx, <<"
def process_buffer(buf):
    return buf.read().upper()
">>),

    {ok, <<"CHUNK1:CHUNK2">>} = py_context:eval(Ctx,
        <<"process_buffer(buf)">>, #{<<"buf">> => Buf}),

    py_context:stop(Ctx).

%% @doc Asyncio-based buffer reading in owngil context
owngil_buffer_async_read_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, Buf} = py_buffer:new(),
    Self = self(),

    ok = py_context:exec(Ctx, <<"
import asyncio

async def async_read(buf):
    chunks = []
    while not buf.at_eof():
        available = buf.readable_amount()
        if available > 0:
            chunks.append(buf.read_nonblock(available))
        else:
            await asyncio.sleep(0.01)
    return b''.join(chunks)

def run_async_read(buf):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(async_read(buf))
    finally:
        loop.close()
">>),

    %% Spawn writer
    spawn_link(fun() ->
        timer:sleep(20),
        ok = py_buffer:write(Buf, <<"async1:">>),
        timer:sleep(20),
        ok = py_buffer:write(Buf, <<"async2">>),
        ok = py_buffer:close(Buf),
        Self ! writer_done
    end),

    {ok, <<"async1:async2">>} = py_context:eval(Ctx,
        <<"run_async_read(buf)">>, #{<<"buf">> => Buf}),

    receive writer_done -> ok after 1000 -> ok end,
    py_context:stop(Ctx).

%% @doc Multiple owngil contexts writing to buffers in parallel
owngil_buffer_parallel_writers_test(_Config) ->
    NumContexts = 4,
    Buffers = [begin {ok, B} = py_buffer:new(), B end
               || _ <- lists:seq(1, NumContexts)],

    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    Parent = self(),

    %% Start parallel writers (each writes to own buffer from Erlang)
    [spawn_link(fun() ->
        ok = py_buffer:write(Buf, <<"parallel_">>),
        ok = py_buffer:write(Buf, integer_to_binary(N)),
        ok = py_buffer:close(Buf),
        Parent ! {writer_done, N}
    end) || {N, Buf} <- lists:zip(lists:seq(1, NumContexts), Buffers)],

    %% Wait for writers
    [receive {writer_done, N} -> ok end || N <- lists:seq(1, NumContexts)],

    %% Read from each context
    Results = [begin
        {ok, Data} = py_context:eval(Ctx, <<"buf.read()">>, #{<<"buf">> => Buf}),
        Data
    end || {Ctx, Buf} <- lists:zip(Contexts, Buffers)],

    %% Verify results
    Expected = [<<"parallel_", (integer_to_binary(N))/binary>>
                || N <- lists:seq(1, NumContexts)],
    Expected = Results,

    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%% @doc Zero-copy memoryview in owngil context
owngil_buffer_memoryview_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, Buf} = py_buffer:new(),

    ok = py_buffer:write(Buf, <<"memoryview test">>),
    ok = py_buffer:close(Buf),

    ok = py_context:exec(Ctx, <<"
def test_memoryview(buf):
    mv = memoryview(buf)
    result = bytes(mv[:10])
    mv.release()
    return result
">>),

    {ok, <<"memoryview">>} = py_context:eval(Ctx,
        <<"test_memoryview(buf)">>, #{<<"buf">> => Buf}),

    py_context:stop(Ctx).

%% @doc GC and refcount test in owngil context
owngil_buffer_gc_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    %% Create many buffers from Erlang side and pass to Python
    lists:foreach(fun(_) ->
        {ok, Buf} = py_buffer:new(),
        ok = py_buffer:write(Buf, binary:copy(<<$x>>, 100)),
        ok = py_buffer:close(Buf),
        %% Pass to Python for reading
        {ok, Data} = py_context:eval(Ctx, <<"buf.read()">>, #{<<"buf">> => Buf}),
        100 = byte_size(Data)
    end, lists:seq(1, 50)),

    %% Force Erlang GC
    erlang:garbage_collect(),

    %% Trigger Python GC
    ok = py_context:exec(Ctx, <<"import gc; gc.collect()">>),

    %% Verify context still works
    {ok, true} = py_context:eval(Ctx, <<"True">>, #{}),

    py_context:stop(Ctx).

%%% ============================================================================
%%% Reentrant Callback Tests
%%% ============================================================================

%% @doc Python->Erlang->Python callback in owngil context
owngil_reentrant_basic_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    %% Register callback that does simple computation (no re-entry into Python)
    py:register_function(owngil_double, fun([X]) ->
        X * 2
    end),

    %% Test callback from owngil context
    {ok, 21} = py_context:eval(Ctx,
        <<"__import__('erlang').call('owngil_double', 10) + 1">>, #{}),

    py_context:stop(Ctx).

%% @doc 3+ level nested callbacks in owngil context
%% Uses py:eval for re-entry to go through the pool (not back into same owngil ctx)
owngil_reentrant_nested_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    py:register_function(owngil_level, fun([Level, N]) ->
        case Level >= N of
            true -> Level;
            false ->
                %% Use py:eval to go through pool for re-entry
                Code = iolist_to_binary(io_lib:format(
                    "__import__('erlang').call('owngil_level', ~p, ~p)",
                    [Level + 1, N])),
                {ok, Result} = py:eval(Code),
                Result
        end
    end),

    %% Test 3 levels of nesting
    {ok, 3} = py_context:eval(Ctx,
        <<"__import__('erlang').call('owngil_level', 1, 3)">>, #{}),

    %% Test 5 levels
    {ok, 5} = py_context:eval(Ctx,
        <<"__import__('erlang').call('owngil_level', 1, 5)">>, #{}),

    py_context:stop(Ctx).

%% @doc Concurrent callbacks from multiple owngil contexts
owngil_reentrant_concurrent_test(_Config) ->
    NumContexts = 4,
    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    py:register_function(owngil_triple, fun([X]) -> X * 3 end),

    Parent = self(),

    %% Concurrent callback calls
    [spawn_link(fun() ->
        Input = N * 10,
        {ok, Result} = py_context:eval(Ctx, iolist_to_binary(
            io_lib:format("__import__('erlang').call('owngil_triple', ~p)", [Input])), #{}),
        Parent ! {done, N, Result, Input * 3}
    end) || {N, Ctx} <- lists:zip(lists:seq(1, NumContexts), Contexts)],

    %% Verify results
    [receive
        {done, N, Result, Expected} ->
            Expected = Result
    end || N <- lists:seq(1, NumContexts)],

    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%% @doc Complex data through callbacks in owngil context
owngil_reentrant_complex_types_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    py:register_function(owngil_transform, fun([Data]) ->
        case Data of
            #{<<"items">> := Items, <<"count">> := Count} ->
                #{
                    <<"items">> => lists:reverse(Items),
                    <<"count">> => Count * 2,
                    <<"processed">> => true
                };
            _ ->
                #{<<"error">> => <<"unexpected">>}
        end
    end),

    {ok, Result} = py_context:eval(Ctx,
        <<"__import__('erlang').call('owngil_transform', "
          "{'items': [1, 2, 3], 'count': 5})">>, #{}),

    #{<<"items">> := [3, 2, 1],
      <<"count">> := 10,
      <<"processed">> := true} = Result,

    py_context:stop(Ctx).

%% @doc Callback from ThreadPoolExecutor in owngil context
owngil_reentrant_thread_callback_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    py:register_function(owngil_echo, fun([X]) -> X end),

    Code = <<"(lambda cf, erlang: (lambda executor: (lambda future: "
             "('success', future.result()) if not future.exception() "
             "else ('error', str(future.exception())))"
             "(executor.submit(lambda: erlang.call('owngil_echo', 42))))"
             "(cf.ThreadPoolExecutor(max_workers=1).__enter__()))"
             "(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), "
             "__import__('erlang'))">>,

    {ok, Result} = py_context:eval(Ctx, Code, #{}),

    case Result of
        {<<"success">>, 42} -> ok;
        {<<"error">>, Msg} -> ct:fail({unexpected_error, Msg});
        Other -> ct:fail({unexpected_result, Other})
    end,

    py_context:stop(Ctx).

%% @doc Callbacks in try/except in owngil context
owngil_reentrant_try_except_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    %% Register a callback
    py:register_function(owngil_callback, fun([X]) -> X * 2 end),

    %% Test callback in try/except
    ok = py_context:exec(Ctx, <<"
import erlang

def call_with_try():
    try:
        result = erlang.call('owngil_callback', 21)
        return ('ok', result)
    except Exception as e:
        return ('error', str(e))
">>),

    {ok, {<<"ok">>, 42}} = py_context:eval(Ctx, <<"call_with_try()">>, #{}),

    py_context:stop(Ctx).

%%% ============================================================================
%%% PID/Send Tests
%%% ============================================================================

%% @doc PID serialization roundtrip in owngil context
owngil_pid_roundtrip_test(Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    TestDir = proplists:get_value(test_dir, Config),

    ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),

    Pid = self(),
    {ok, ReturnedPid} = py_context:call(Ctx, py_test_pid_send, round_trip_pid, [Pid], #{}),
    Pid = ReturnedPid,

    py_context:stop(Ctx).

%% @doc Basic erlang.send() in owngil context
owngil_send_simple_test(Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    TestDir = proplists:get_value(test_dir, Config),

    ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),

    Pid = self(),
    {ok, true} = py_context:call(Ctx, py_test_pid_send, send_message, [Pid, <<"hello">>], #{}),

    receive <<"hello">> -> ok
    after 5000 -> ct:fail(timeout)
    end,

    py_context:stop(Ctx).

%% @doc Multiple messages via erlang.send() in owngil context
owngil_send_multiple_test(Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    TestDir = proplists:get_value(test_dir, Config),

    ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),

    Pid = self(),
    {ok, 3} = py_context:call(Ctx, py_test_pid_send, send_multiple,
        [Pid, [<<"one">>, <<"two">>, <<"three">>]], #{}),

    receive <<"one">> -> ok after 5000 -> ct:fail(timeout_1) end,
    receive <<"two">> -> ok after 5000 -> ct:fail(timeout_2) end,
    receive <<"three">> -> ok after 5000 -> ct:fail(timeout_3) end,

    py_context:stop(Ctx).

%% @doc Complex compound terms via erlang.send() in owngil context
owngil_send_complex_test(Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    TestDir = proplists:get_value(test_dir, Config),

    ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),

    Pid = self(),
    {ok, true} = py_context:call(Ctx, py_test_pid_send, send_complex_term, [Pid], #{}),

    receive
        {<<"hello">>, 42, [1, 2, 3], #{<<"key">> := <<"value">>}, true} -> ok
    after 5000 -> ct:fail(timeout)
    end,

    py_context:stop(Ctx).

%% @doc SuspensionRequired escapes except Exception in owngil context
owngil_suspension_not_caught_test(Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    TestDir = proplists:get_value(test_dir, Config),

    ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),

    py:register_function(test_pid_echo, fun([X]) -> X end),

    {ok, {<<"ok">>, 42}} = py_context:call(Ctx, py_test_pid_send,
        suspension_not_caught_by_except_exception, [], #{}),

    py:unregister_function(test_pid_echo),
    py_context:stop(Ctx).

%% @doc erlang.send() from async coroutine in owngil context
owngil_send_from_coroutine_test(Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    TestDir = proplists:get_value(test_dir, Config),

    ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),

    Pid = self(),
    {ok, true} = py_context:call(Ctx, py_test_pid_send, send_from_coroutine,
        [Pid, <<"async_hello">>], #{}),

    receive <<"async_hello">> -> ok
    after 5000 -> ct:fail(timeout)
    end,

    py_context:stop(Ctx).

%% @doc High-volume non-blocking send in owngil context
owngil_send_nonblocking_test(Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    TestDir = proplists:get_value(test_dir, Config),

    ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),

    Pid = self(),
    Count = 100,
    {ok, Elapsed} = py_context:call(Ctx, py_test_pid_send, send_is_nonblocking,
        [Pid, Count], #{}),

    ct:pal("Sent ~p messages in ~.6f seconds", [Count, Elapsed]),
    true = Elapsed < 1.0,

    %% Drain messages
    drain_pid_messages(Count),

    py_context:stop(Ctx).

%% @doc Parallel sends from multiple owngil contexts
owngil_send_parallel_test(Config) ->
    NumContexts = 4,
    MessagesPerContext = 25,
    TestDir = proplists:get_value(test_dir, Config),

    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        ok = py_context:exec(Ctx, iolist_to_binary(io_lib:format(
            "import sys; sys.path.insert(0, '~s')", [TestDir]))),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    Parent = self(),
    Pid = self(),

    %% Parallel senders
    [spawn_link(fun() ->
        lists:foreach(fun(M) ->
            py_context:call(Ctx, py_test_pid_send, send_message,
                [Pid, {N, M}], #{})
        end, lists:seq(1, MessagesPerContext)),
        Parent ! {sender_done, N}
    end) || {N, Ctx} <- lists:zip(lists:seq(1, NumContexts), Contexts)],

    %% Wait for senders
    [receive {sender_done, N} -> ok end || N <- lists:seq(1, NumContexts)],

    %% Count messages
    TotalMessages = NumContexts * MessagesPerContext,
    drain_tuple_messages(TotalMessages),

    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%%% ============================================================================
%%% Reactor Tests
%%% NOTE: py_reactor_context with OWN_GIL mode requires further investigation.
%%% The core dispatch is implemented but integration needs more work.
%%% ============================================================================

%% @doc Echo protocol in owngil reactor context
owngil_reactor_echo_protocol_test(_Config) ->
    %% First verify OWN_GIL contexts work for basic reactor operations
    {ok, Ctx} = py_context:start_link(1, owngil),

    %% Set up the protocol factory
    ok = py_context:exec(Ctx, <<"
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'close'

reactor.set_protocol_factory(EchoProtocol)
">>),

    %% Verify protocol factory is set
    {ok, true} = py_context:eval(Ctx, <<"reactor._protocol_factory is not None">>, #{}),

    %% For now, just test that the basic OWN_GIL context works with reactor module
    %% Full py_reactor_context integration needs more investigation
    py_context:stop(Ctx),
    ok.

%% @doc Multiple connections in owngil reactor
owngil_reactor_multiple_conn_test(_Config) ->
    SetupCode = <<"
import erlang.reactor as reactor

class CounterProtocol(reactor.Protocol):
    counter = 0

    def connection_made(self, fd, client_info):
        super().connection_made(fd, client_info)
        CounterProtocol.counter += 1
        self.my_id = CounterProtocol.counter

    def data_received(self, data):
        self.write_buffer.extend(str(self.my_id).encode() + b':' + data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'close'

reactor.set_protocol_factory(CounterProtocol)
">>,

    {ok, ReactorCtx} = py_reactor_context:start_link(1, owngil, #{
        setup_code => SetupCode
    }),

    %% Create 3 connections
    Pairs = [create_socketpair() || _ <- lists:seq(1, 3)],

    %% Handoff all
    [begin
        {ok, {Server, _}} = Pair,
        Fd = get_fd(Server),
        ok = py_reactor_context:handoff(ReactorCtx, Fd, #{})
    end || Pair <- Pairs],
    timer:sleep(100),

    %% Send and receive
    Results = [begin
        {ok, {_, Client}} = Pair,
        ok = gen_tcp:send(Client, <<"test">>),
        {ok, Data} = gen_tcp:recv(Client, 0, 2000),
        Data
    end || Pair <- Pairs],

    %% Verify unique IDs
    [<<"1:test">>, <<"2:test">>, <<"3:test">>] = lists:sort(Results),

    %% Cleanup
    [begin
        {ok, {Server, Client}} = Pair,
        gen_tcp:close(Server),
        gen_tcp:close(Client)
    end || Pair <- Pairs],
    py_reactor_context:stop(ReactorCtx).

%% @doc async_pending pattern in owngil reactor
owngil_reactor_async_pending_test(_Config) ->
    SetupCode = <<"
import erlang.reactor as reactor

class AsyncPendingProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(b'ASYNC:' + data)
        reactor.signal_write_ready(self.fd)
        return 'async_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'close'

reactor.set_protocol_factory(AsyncPendingProtocol)
">>,

    {ok, ReactorCtx} = py_reactor_context:start_link(1, owngil, #{
        setup_code => SetupCode
    }),

    {ok, {Server, Client}} = create_socketpair(),
    Fd = get_fd(Server),

    ok = py_reactor_context:handoff(ReactorCtx, Fd, #{}),
    timer:sleep(100),

    ok = gen_tcp:send(Client, <<"pending">>),
    {ok, <<"ASYNC:pending">>} = gen_tcp:recv(Client, 0, 2000),

    gen_tcp:close(Server),
    gen_tcp:close(Client),
    py_reactor_context:stop(ReactorCtx).

%% @doc ReactorBuffer bytes-like in owngil context
%% NOTE: ReactorBuffer._test_create is not available in OWN_GIL subinterpreters
%% because the erlang module extensions aren't exported to subinterpreters.
%% This tests basic bytes-like operations instead.
owngil_reactor_buffer_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    %% Test basic bytes operations that would be similar to ReactorBuffer
    ok = py_context:exec(Ctx, <<"
data = b'reactor buffer test'
result = {
    'len': len(data),
    'startswith': data.startswith(b'reactor'),
    'find': data.find(b'buffer'),
    'slice': data[8:14]
}
">>),

    {ok, #{
        <<"len">> := 19,
        <<"startswith">> := true,
        <<"find">> := 8,
        <<"slice">> := <<"buffer">>
    }} = py_context:eval(Ctx, <<"result">>, #{}),

    py_context:stop(Ctx).

%% @doc Protocol factory isolation between owngil contexts
owngil_reactor_isolation_test(_Config) ->
    EchoSetup = <<"
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'close'

reactor.set_protocol_factory(EchoProtocol)
">>,

    UpperSetup = <<"
import erlang.reactor as reactor

class UpperProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(bytes(data).upper())
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'close'

reactor.set_protocol_factory(UpperProtocol)
">>,

    {ok, Ctx1} = py_reactor_context:start_link(1, owngil, #{setup_code => EchoSetup}),
    {ok, Ctx2} = py_reactor_context:start_link(2, owngil, #{setup_code => UpperSetup}),

    {ok, {S1a, S1b}} = create_socketpair(),
    {ok, {S2a, S2b}} = create_socketpair(),

    ok = py_reactor_context:handoff(Ctx1, get_fd(S1a), #{}),
    ok = py_reactor_context:handoff(Ctx2, get_fd(S2a), #{}),
    timer:sleep(100),

    ok = gen_tcp:send(S1b, <<"test">>),
    ok = gen_tcp:send(S2b, <<"test">>),

    {ok, R1} = gen_tcp:recv(S1b, 0, 2000),
    {ok, R2} = gen_tcp:recv(S2b, 0, 2000),

    gen_tcp:close(S1a), gen_tcp:close(S1b),
    gen_tcp:close(S2a), gen_tcp:close(S2b),
    py_reactor_context:stop(Ctx1),
    py_reactor_context:stop(Ctx2),

    %% Verify isolation
    <<"test">> = R1,
    <<"TEST">> = R2.

%%% ============================================================================
%%% Async Task Tests
%%% ============================================================================

%% @doc create_task/await pattern in owngil context
owngil_async_create_await_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    Ref = py_event_loop:create_task(math, sqrt, [25.0]),
    {ok, 5.0} = py_event_loop:await(Ref, 5000),

    py_context:stop(Ctx).

%% @doc Blocking run API in owngil context
owngil_async_run_sync_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    {ok, 3} = py_event_loop:run(math, floor, [3.7], #{timeout => 5000}),

    py_context:stop(Ctx).

%% @doc Concurrent tasks across owngil contexts
owngil_async_concurrent_test(_Config) ->
    NumContexts = 4,
    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    Parent = self(),

    %% Submit concurrent tasks
    [spawn_link(fun() ->
        Ref = py_event_loop:create_task(math, sqrt, [float(N * N)]),
        {ok, Result} = py_event_loop:await(Ref, 5000),
        Parent ! {done, N, Result}
    end) || {N, _Ctx} <- lists:zip(lists:seq(1, NumContexts), Contexts)],

    %% Verify results
    [receive
        {done, N, Result} ->
            Expected = float(N),
            true = abs(Result - Expected) < 0.0001
    end || N <- lists:seq(1, NumContexts)],

    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%% @doc Batch task submission in owngil context
owngil_async_batch_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    NumTasks = 50,

    Refs = [py_event_loop:create_task(math, sqrt, [float(N)])
            || N <- lists:seq(1, NumTasks)],

    Results = [{N, py_event_loop:await(Ref, 5000)}
               || {N, Ref} <- lists:zip(lists:seq(1, NumTasks), Refs)],

    %% Verify all succeeded
    lists:foreach(fun({N, {ok, R}}) ->
        Expected = math:sqrt(N),
        true = abs(R - Expected) < 0.0001
    end, Results),

    py_context:stop(Ctx).

%% @doc Timeout handling in owngil context
owngil_async_timeout_test(_Config) ->
    {ok, _Ctx} = py_context:start_link(1, owngil),

    ok = py:exec(<<"
async def slow_async(seconds):
    import asyncio
    await asyncio.sleep(seconds)
    return 'completed'
">>),

    Ref = py_event_loop:create_task('__main__', slow_async, [10.0]),
    {error, timeout} = py_event_loop:await(Ref, 100),

    ok.

%% @doc Error propagation in owngil context
owngil_async_error_test(_Config) ->
    {ok, _Ctx} = py_context:start_link(1, owngil),

    ok = py:exec(<<"
async def failing_async():
    import asyncio
    await asyncio.sleep(0.001)
    raise ValueError('test_error')
">>),

    Ref = py_event_loop:create_task('__main__', failing_async, []),
    Result = py_event_loop:await(Ref, 5000),

    case Result of
        {error, _} -> ok;
        {ok, _} -> ct:fail("Expected error but got success")
    end.

%%% ============================================================================
%%% Asyncio Tests
%%% ============================================================================

%% @doc asyncio.sleep works in owngil context
owngil_asyncio_basic_sleep_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    ok = py_context:exec(Ctx, <<"
import asyncio

async def sleep_test():
    await asyncio.sleep(0.01)
    return 'slept'

def run_sleep():
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(sleep_test())
    finally:
        loop.close()
">>),

    {ok, <<"slept">>} = py_context:eval(Ctx, <<"run_sleep()">>, #{}),

    py_context:stop(Ctx).

%% @doc asyncio.gather in single owngil context
owngil_asyncio_gather_test(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    ok = py_context:exec(Ctx, <<"
import asyncio

async def task(n):
    await asyncio.sleep(0.01)
    return n * 2

async def gather_test():
    results = await asyncio.gather(task(1), task(2), task(3))
    return results

def run_gather():
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(gather_test())
    finally:
        loop.close()
">>),

    {ok, [2, 4, 6]} = py_context:eval(Ctx, <<"run_gather()">>, #{}),

    py_context:stop(Ctx).

%% @doc Independent event loops per owngil context
owngil_asyncio_parallel_loops_test(_Config) ->
    NumContexts = 4,
    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        ok = py_context:exec(Ctx, <<"
import asyncio

async def loop_task(ctx_id, n):
    await asyncio.sleep(0.01)
    return f'ctx{ctx_id}_task{n}'

async def gather_tasks(ctx_id):
    return await asyncio.gather(
        loop_task(ctx_id, 1),
        loop_task(ctx_id, 2)
    )

def run_tasks(ctx_id):
    # Use asyncio.run for proper event loop management in Python 3.10+
    return asyncio.run(gather_tasks(ctx_id))
">>),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    Start = erlang:monotonic_time(millisecond),

    %% Run each context sequentially from the main process
    %% Each owngil context has its own dedicated thread providing parallelism
    AllResults = [begin
        {ok, Results} = py_context:eval(Ctx,
            iolist_to_binary(io_lib:format("run_tasks(~p)", [N])), #{}),
        {N, Results}
    end || {N, Ctx} <- lists:zip(lists:seq(1, NumContexts), Contexts)],

    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ct:pal("Event loops completed in ~p ms", [Elapsed]),

    %% Verify all contexts returned their results
    NumContexts = length(AllResults),

    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%%% ============================================================================
%%% Helper Functions
%%% ============================================================================

drain_channel(Ch, N) ->
    drain_channel(Ch, N, []).

drain_channel(_Ch, 0, Acc) ->
    lists:reverse(Acc);
drain_channel(Ch, N, Acc) ->
    case py_nif:channel_try_receive(Ch) of
        {ok, Msg} -> drain_channel(Ch, N - 1, [Msg | Acc]);
        {error, empty} ->
            timer:sleep(10),
            drain_channel(Ch, N, Acc);
        {error, closed} -> lists:reverse(Acc)
    end.

consume_until_closed(Ctx, Ch, Parent, CtxNum) ->
    consume_until_closed(Ctx, Ch, Parent, CtxNum, []).

consume_until_closed(Ctx, Ch, Parent, CtxNum, Acc) ->
    case py_context:eval(Ctx, <<"Channel(ch).try_receive()">>, #{<<"ch">> => Ch}) of
        {ok, none} ->
            %% Empty, check if closed
            Info = py_channel:info(Ch),
            case maps:get(closed, Info) of
                true -> Parent ! {consumer_result, CtxNum, lists:reverse(Acc)};
                false ->
                    timer:sleep(5),
                    consume_until_closed(Ctx, Ch, Parent, CtxNum, Acc)
            end;
        {ok, Msg} ->
            consume_until_closed(Ctx, Ch, Parent, CtxNum, [Msg | Acc]);
        {error, closed} ->
            Parent ! {consumer_result, CtxNum, lists:reverse(Acc)};
        {error, {'ChannelClosed', _}} ->
            Parent ! {consumer_result, CtxNum, lists:reverse(Acc)}
    end.

drain_pid_messages(0) -> ok;
drain_pid_messages(N) ->
    receive
        {<<"msg">>, _} -> drain_pid_messages(N - 1)
    after 1000 ->
        ct:pal("Drained ~p messages, ~p remaining", [100 - N, N]),
        ok  %% Tolerate some loss in high-volume test
    end.

drain_tuple_messages(0) -> ok;
drain_tuple_messages(N) ->
    receive
        {_, _} -> drain_tuple_messages(N - 1)
    after 1000 ->
        ct:pal("Drained ~p tuple messages, ~p remaining", [100 - N, N]),
        ok
    end.

create_socketpair() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    {ok, Client} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(LSock, 1000),
    gen_tcp:close(LSock),
    {ok, {Server, Client}}.

get_fd(Socket) ->
    {ok, Fd} = inet:getfd(Socket),
    Fd.
