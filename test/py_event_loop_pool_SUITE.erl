%%% @doc Common Test suite for Event Loop Worker Pool.
%%%
%%% Tests the pool of event loops with process affinity for ordered execution.
-module(py_event_loop_pool_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    %% Pool tests
    test_pool_starts/1,
    test_pool_stats/1,
    test_get_loop_returns_reference/1,
    test_same_process_same_loop/1,

    %% Task API tests
    test_create_task_and_await/1,
    test_run_blocking/1,
    test_spawn_task/1,
    test_concurrent_tasks/1,

    %% Ordering test
    test_tasks_execute_in_order/1
]).

all() ->
    [
        test_pool_starts,
        test_pool_stats,
        test_get_loop_returns_reference,
        test_same_process_same_loop,
        test_create_task_and_await,
        test_run_blocking,
        test_spawn_task,
        test_concurrent_tasks,
        test_tasks_execute_in_order
    ].

init_per_suite(Config) ->
    case application:ensure_all_started(erlang_python) of
        {ok, _} ->
            timer:sleep(500),
            case wait_for_pool(5000) of
                ok -> Config;
                {error, Reason} -> ct:fail({pool_not_ready, Reason})
            end;
        {error, {App, Reason}} ->
            ct:fail({failed_to_start, App, Reason})
    end.

wait_for_pool(Timeout) when Timeout =< 0 ->
    {error, timeout};
wait_for_pool(Timeout) ->
    case py_event_loop_pool:get_loop() of
        {ok, LoopRef} when is_reference(LoopRef) -> ok;
        _ ->
            timer:sleep(100),
            wait_for_pool(Timeout - 100)
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Pool Tests
%% ============================================================================

test_pool_starts(_Config) ->
    Stats = py_event_loop_pool:get_stats(),
    NumLoops = maps:get(num_loops, Stats),
    ct:log("Pool size: ~p", [NumLoops]),
    true = NumLoops > 0,
    ok.

test_pool_stats(_Config) ->
    Stats = py_event_loop_pool:get_stats(),
    ct:log("Pool stats: ~p", [Stats]),
    true = is_map(Stats),
    true = maps:is_key(num_loops, Stats),
    true = maps:is_key(supported, Stats),
    true = maps:get(supported, Stats),
    ok.

test_get_loop_returns_reference(_Config) ->
    {ok, LoopRef} = py_event_loop_pool:get_loop(),
    ct:log("Got loop ref: ~p", [LoopRef]),
    true = is_reference(LoopRef),
    ok.

test_same_process_same_loop(_Config) ->
    %% Same process always gets the same loop (process affinity)
    {ok, Loop1} = py_event_loop_pool:get_loop(),
    {ok, Loop2} = py_event_loop_pool:get_loop(),
    {ok, Loop3} = py_event_loop_pool:get_loop(),
    ct:log("Loops: ~p, ~p, ~p", [Loop1, Loop2, Loop3]),
    Loop1 = Loop2,
    Loop2 = Loop3,
    ok.

%% ============================================================================
%% Task API Tests
%% ============================================================================

test_create_task_and_await(_Config) ->
    Ref = py_event_loop_pool:create_task(math, sqrt, [25.0]),
    ct:log("Created task ref: ~p", [Ref]),
    true = is_reference(Ref),
    {ok, 5.0} = py_event_loop_pool:await(Ref, 5000),
    ok.

test_run_blocking(_Config) ->
    {ok, 3} = py_event_loop_pool:run(math, floor, [3.7]),
    ok.

test_spawn_task(_Config) ->
    ok = py_event_loop_pool:spawn_task(math, ceil, [2.3]),
    timer:sleep(100),
    ok.

test_concurrent_tasks(_Config) ->
    NumTasks = 50,
    Refs = [py_event_loop_pool:create_task(math, sqrt, [float(I * I)])
            || I <- lists:seq(1, NumTasks)],
    ct:log("Created ~p tasks", [length(Refs)]),

    Results = [py_event_loop_pool:await(Ref, 5000) || Ref <- Refs],
    OkResults = [{ok, V} || {ok, V} <- Results],
    NumTasks = length(OkResults),

    Values = lists:sort([round(V) || {ok, V} <- Results]),
    Expected = lists:seq(1, NumTasks),
    Values = Expected,
    ok.

%% ============================================================================
%% Ordering Test
%% ============================================================================

test_tasks_execute_in_order(_Config) ->
    %% All tasks from this process go to the same loop, so they execute in order
    Refs = [py_event_loop_pool:create_task(math, sqrt, [float(I)])
            || I <- [1, 4, 9, 16, 25]],

    Results = [py_event_loop_pool:await(Ref, 5000) || Ref <- Refs],
    ct:log("Results: ~p", [Results]),

    %% Results should be in submission order
    [{ok, 1.0}, {ok, 2.0}, {ok, 3.0}, {ok, 4.0}, {ok, 5.0}] = Results,
    ok.
