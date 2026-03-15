%%% @doc Common Test suite for OWN_GIL context support.
%%%
%%% Tests the OWN_GIL mode for py_context which creates dedicated pthreads
%%% with independent Python GILs for true parallel execution.
%%%
%%% OWN_GIL mode requires Python 3.12+.
-module(py_context_owngil_SUITE).

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

%% Lifecycle tests
-export([
    test_owngil_context_create/1,
    test_owngil_thread_init/1,
    test_owngil_context_destroy/1
]).

%% Basic operations tests
-export([
    test_owngil_basic_call/1,
    test_owngil_eval/1,
    test_owngil_exec/1
]).

%% IPC tests
-export([
    test_owngil_type_conversions/1,
    test_owngil_large_data/1,
    test_owngil_binary_data/1
]).

%% Isolation tests
-export([
    test_owngil_isolation/1,
    test_owngil_interp_id/1
]).

%% Parallelism tests
-export([
    test_owngil_parallel_execution/1,
    test_owngil_concurrent_sleep/1
]).

%% Feature tests
-export([
    test_owngil_state_persistence/1,
    test_owngil_module_import/1
]).

all() ->
    [{group, lifecycle},
     {group, basic_ops},
     {group, ipc},
     {group, isolation},
     {group, parallelism},
     {group, features}].

groups() ->
    [{lifecycle, [sequence], [
        test_owngil_context_create,
        test_owngil_thread_init,
        test_owngil_context_destroy
    ]},
     {basic_ops, [sequence], [
        test_owngil_basic_call,
        test_owngil_eval,
        test_owngil_exec
    ]},
     {ipc, [sequence], [
        test_owngil_type_conversions,
        test_owngil_large_data,
        test_owngil_binary_data
    ]},
     {isolation, [sequence], [
        test_owngil_isolation,
        test_owngil_interp_id
    ]},
     {parallelism, [parallel], [
        test_owngil_parallel_execution,
        test_owngil_concurrent_sleep
    ]},
     {features, [sequence], [
        test_owngil_state_persistence,
        test_owngil_module_import
    ]}].

init_per_suite(Config) ->
    case py_nif:subinterp_supported() of
        true ->
            {ok, _} = application:ensure_all_started(erlang_python),
            Config;
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
    ok.

%%% ============================================================================
%%% Lifecycle Tests
%%% ============================================================================

%% @doc Test OWN_GIL context creation
test_owngil_context_create(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    true = is_pid(Ctx),
    true = is_process_alive(Ctx),
    py_context:stop(Ctx).

%% @doc Test that thread is running after initialization
test_owngil_thread_init(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    %% If we get here, the thread initialized successfully
    %% (owngil_context_init waits for thread_running flag)
    true = is_process_alive(Ctx),
    py_context:stop(Ctx).

%% @doc Test OWN_GIL context destruction
test_owngil_context_destroy(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    MRef = erlang:monitor(process, Ctx),
    py_context:stop(Ctx),
    receive
        {'DOWN', MRef, process, Ctx, _Reason} ->
            ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        ct:fail(timeout_waiting_for_context_stop)
    end.

%%% ============================================================================
%%% Basic Operations Tests
%%% ============================================================================

%% @doc Test basic Python function call
test_owngil_basic_call(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{}),
    {ok, 3.0} = py_context:call(Ctx, math, sqrt, [9], #{}),
    py_context:stop(Ctx).

%% @doc Test Python expression evaluation
test_owngil_eval(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    {ok, 6} = py_context:eval(Ctx, <<"2 + 4">>, #{}),
    {ok, 15} = py_context:eval(Ctx, <<"3 * 5">>, #{}),
    py_context:stop(Ctx).

%% @doc Test Python statement execution
test_owngil_exec(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    ok = py_context:exec(Ctx, <<"x = 42">>),
    {ok, 42} = py_context:eval(Ctx, <<"x">>, #{}),
    py_context:stop(Ctx).

%%% ============================================================================
%%% IPC Tests
%%% ============================================================================

%% @doc Test type conversions through OWN_GIL dispatch
test_owngil_type_conversions(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    %% Lists
    {ok, [1, 2, 3]} = py_context:eval(Ctx, <<"[1, 2, 3]">>, #{}),
    %% Dicts -> Maps
    {ok, #{<<"a">> := 1}} = py_context:eval(Ctx, <<"{'a': 1}">>, #{}),
    %% Booleans
    {ok, true} = py_context:eval(Ctx, <<"True">>, #{}),
    {ok, false} = py_context:eval(Ctx, <<"False">>, #{}),
    %% None
    {ok, none} = py_context:eval(Ctx, <<"None">>, #{}),
    %% Strings
    {ok, <<"hello">>} = py_context:eval(Ctx, <<"'hello'">>, #{}),
    py_context:stop(Ctx).

%% @doc Test large data transfer through OWN_GIL dispatch
test_owngil_large_data(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    LargeList = lists:seq(1, 10000),
    {ok, 10000} = py_context:call(Ctx, builtins, len, [LargeList], #{}),
    py_context:stop(Ctx).

%% @doc Test binary data transfer
test_owngil_binary_data(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),
    Bin = crypto:strong_rand_bytes(65536),
    {ok, 65536} = py_context:call(Ctx, builtins, len, [Bin], #{}),
    py_context:stop(Ctx).

%%% ============================================================================
%%% Isolation Tests
%%% ============================================================================

%% @doc Test that OWN_GIL contexts are isolated from each other
test_owngil_isolation(_Config) ->
    {ok, Ctx1} = py_context:start_link(1, owngil),
    {ok, Ctx2} = py_context:start_link(2, owngil),

    ok = py_context:exec(Ctx1, <<"x = 'ctx1'">>),
    ok = py_context:exec(Ctx2, <<"x = 'ctx2'">>),

    {ok, <<"ctx1">>} = py_context:eval(Ctx1, <<"x">>, #{}),
    {ok, <<"ctx2">>} = py_context:eval(Ctx2, <<"x">>, #{}),

    py_context:stop(Ctx1),
    py_context:stop(Ctx2).

%% @doc Test that OWN_GIL contexts have different interpreter IDs
test_owngil_interp_id(_Config) ->
    {ok, Ctx1} = py_context:start_link(1, owngil),
    {ok, Ctx2} = py_context:start_link(2, owngil),

    {ok, Id1} = py_context:get_interp_id(Ctx1),
    {ok, Id2} = py_context:get_interp_id(Ctx2),

    %% Different contexts should have different interp IDs
    true = Id1 =/= Id2,

    py_context:stop(Ctx1),
    py_context:stop(Ctx2).

%%% ============================================================================
%%% Parallelism Tests (Critical - proves OWN_GIL works)
%%% ============================================================================

%% @doc Test that OWN_GIL contexts execute truly in parallel
test_owngil_parallel_execution(_Config) ->
    NumContexts = 4,
    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, owngil),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    %% CPU-bound code
    Code = <<"sum(range(500000))">>,
    Parent = self(),

    %% Sequential execution timing
    SeqStart = erlang:monotonic_time(millisecond),
    [py_context:eval(Ctx, Code, #{}) || Ctx <- Contexts],
    SeqTime = erlang:monotonic_time(millisecond) - SeqStart,

    %% Parallel execution timing
    ParStart = erlang:monotonic_time(millisecond),
    Pids = [spawn(fun() ->
        Result = py_context:eval(Ctx, Code, #{}),
        Parent ! {done, self(), Result}
    end) || Ctx <- Contexts],
    [receive {done, Pid, _Result} -> ok end || Pid <- Pids],
    ParTime = erlang:monotonic_time(millisecond) - ParStart,

    ct:pal("Sequential: ~p ms, Parallel: ~p ms, Speedup: ~.2fx",
           [SeqTime, ParTime, SeqTime / max(1, ParTime)]),

    %% With OWN_GIL, parallel should be significantly faster
    %% Use a conservative check - parallel should be at least 1.3x faster
    true = ParTime * 1.3 < SeqTime orelse SeqTime < 100,

    [py_context:stop(Ctx) || Ctx <- Contexts],
    ok.

%% @doc Test concurrent sleep operations
test_owngil_concurrent_sleep(_Config) ->
    {ok, Ctx1} = py_context:start_link(1, owngil),
    {ok, Ctx2} = py_context:start_link(2, owngil),

    Parent = self(),
    Start = erlang:monotonic_time(millisecond),

    spawn(fun() ->
        py_context:eval(Ctx1, <<"import time; time.sleep(0.1)">>, #{}),
        Parent ! {done, 1}
    end),
    spawn(fun() ->
        py_context:eval(Ctx2, <<"import time; time.sleep(0.1)">>, #{}),
        Parent ! {done, 2}
    end),

    receive {done, _} -> ok end,
    receive {done, _} -> ok end,

    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ct:pal("Two 100ms sleeps completed in ~p ms", [Elapsed]),

    %% Should be ~100ms (parallel), not ~200ms (serial)
    %% Allow some overhead, but should be less than 180ms
    true = Elapsed < 180,

    py_context:stop(Ctx1),
    py_context:stop(Ctx2).

%%% ============================================================================
%%% Feature Tests
%%% ============================================================================

%% @doc Test that state persists across calls in OWN_GIL context
test_owngil_state_persistence(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    ok = py_context:exec(Ctx, <<"counter = 0">>),
    ok = py_context:exec(Ctx, <<"counter += 1">>),
    ok = py_context:exec(Ctx, <<"counter += 1">>),
    {ok, 2} = py_context:eval(Ctx, <<"counter">>, #{}),

    py_context:stop(Ctx).

%% @doc Test module import in OWN_GIL context
test_owngil_module_import(_Config) ->
    {ok, Ctx} = py_context:start_link(1, owngil),

    ok = py_context:exec(Ctx, <<"import json">>),
    {ok, <<"{\"a\": 1}">>} = py_context:eval(Ctx, <<"json.dumps({'a': 1})">>, #{}),

    py_context:stop(Ctx).
