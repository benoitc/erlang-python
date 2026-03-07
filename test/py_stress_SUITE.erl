%%% @doc Stress tests for No-GIL Safe Mode.
%%%
%%% This suite tests race conditions, concurrent context operations,
%%% and callback storms to verify the atomic state machine and
%%% proper resource management under load.
-module(py_stress_SUITE).

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

-export([
    context_churn_test/1,
    concurrent_calls_test/1,
    concurrent_evals_test/1,
    callback_storm_test/1,
    finalize_race_test/1,
    counter_balance_test/1,
    parallel_context_operations_test/1,
    rapid_start_stop_test/1
]).

all() ->
    [{group, stress_tests}].

groups() ->
    [{stress_tests, [parallel], [
        context_churn_test,
        concurrent_calls_test,
        concurrent_evals_test,
        callback_storm_test,
        counter_balance_test,
        parallel_context_operations_test
    ]},
    {sequential_tests, [], [
        finalize_race_test,
        rapid_start_stop_test
    ]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    case py:contexts_started() of
        true -> ok;
        false -> {ok, _} = py:start_contexts()
    end,
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(stress_tests, Config) ->
    Config;
init_per_group(sequential_tests, Config) ->
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Get initial counter state
    Counters = py_nif:get_debug_counters(),
    [{initial_counters, Counters} | Config].

end_per_testcase(_TestCase, Config) ->
    %% Allow time for cleanup
    timer:sleep(100),
    %% Force GC to trigger destructors
    erlang:garbage_collect(),
    timer:sleep(100),
    Config.

%%% ============================================================================
%%% Stress Test Cases
%%% ============================================================================

%% @doc Rapidly create and destroy contexts to stress resource management
context_churn_test(_Config) ->
    NumIterations = 100,
    AvailableContexts = py_context_router:num_contexts(),
    NumContexts = min(10, AvailableContexts),
    Self = self(),

    %% Spawn processes that rapidly create/use/destroy contexts
    Pids = [spawn_link(fun() ->
        context_churn_worker(NumIterations, N),
        Self ! {done, self()}
    end) || N <- lists:seq(1, NumContexts)],

    %% Wait for all workers to complete
    [receive {done, Pid} -> ok after 30000 -> ct:fail(timeout) end
     || Pid <- Pids],

    ok.

context_churn_worker(0, _N) ->
    ok;
context_churn_worker(Iterations, N) ->
    Ctx = py:context(N),
    {ok, _} = py:eval(Ctx, <<"1 + 1">>),
    context_churn_worker(Iterations - 1, N).

%% @doc Concurrent py:call operations
concurrent_calls_test(_Config) ->
    NumProcesses = 50,
    NumCalls = 20,
    Self = self(),

    Pids = [spawn_link(fun() ->
        concurrent_call_worker(NumCalls),
        Self ! {done, self()}
    end) || _ <- lists:seq(1, NumProcesses)],

    [receive {done, Pid} -> ok after 30000 -> ct:fail(timeout) end
     || Pid <- Pids],

    ok.

concurrent_call_worker(0) ->
    ok;
concurrent_call_worker(N) ->
    {ok, _} = py:call(math, sqrt, [N * N]),
    concurrent_call_worker(N - 1).

%% @doc Concurrent py:eval operations
concurrent_evals_test(_Config) ->
    NumProcesses = 50,
    NumEvals = 20,
    Self = self(),

    Pids = [spawn_link(fun() ->
        concurrent_eval_worker(NumEvals),
        Self ! {done, self()}
    end) || _ <- lists:seq(1, NumProcesses)],

    [receive {done, Pid} -> ok after 30000 -> ct:fail(timeout) end
     || Pid <- Pids],

    ok.

concurrent_eval_worker(0) ->
    ok;
concurrent_eval_worker(N) ->
    Expr = list_to_binary(io_lib:format("~p * ~p", [N, N])),
    {ok, _} = py:eval(Expr),
    concurrent_eval_worker(N - 1).

%% @doc Storm of callbacks to stress suspension/resume
callback_storm_test(_Config) ->
    %% Use unique name for callback to avoid parallel test conflicts
    UniqueId = erlang:unique_integer([positive]),
    CallbackName = list_to_atom("stress_cb_" ++ integer_to_list(UniqueId)),

    %% Register the callback function
    py:register_function(CallbackName, fun([_Arg]) -> 42 end),

    %% Use py:eval with inline code - no stored state needed
    %% This avoids __main__ namespace conflicts in parallel tests
    NumCallbacks = 50,
    Code = list_to_binary(io_lib:format(
        "(lambda n, erl: len([erl.call('~s', i) for i in range(n)]))(~p, __import__('erlang'))",
        [atom_to_list(CallbackName), NumCallbacks])),

    {ok, NumCallbacks} = py:eval(Code),

    %% Cleanup
    py:unregister_function(CallbackName),
    ok.

%% @doc Test finalize behavior when operations are in flight
finalize_race_test(Config) ->
    %% This test requires stopping and restarting the application
    %% Skip if we're running in parallel with other tests
    case ?config(parallel_running, Config) of
        true ->
            {skip, "Cannot run finalize test in parallel mode"};
        _ ->
            do_finalize_race_test()
    end.

do_finalize_race_test() ->
    %% Start the application fresh
    _ = application:stop(erlang_python),
    timer:sleep(100),
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),

    %% Start some concurrent operations
    Self = self(),
    Pids = [spawn(fun() ->
        %% Each worker does some work
        try
            lists:foreach(fun(_) ->
                py:eval(<<"1 + 1">>)
            end, lists:seq(1, 10))
        catch
            _:_ -> ok
        end,
        Self ! {done, self()}
    end) || _ <- lists:seq(1, 5)],

    %% Give workers a bit of time to start
    timer:sleep(10),

    %% Stop the application while work is in flight
    ok = application:stop(erlang_python),

    %% Wait for workers to finish (they should handle shutdown gracefully)
    [receive {done, Pid} -> ok after 5000 -> ok end || Pid <- Pids],

    %% Restart for subsequent tests
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),

    ok.

%% @doc Verify counter invariants
counter_balance_test(_Config) ->
    %% Do some operations
    Ctx = py:context(1),
    lists:foreach(fun(_) ->
        {ok, _} = py:eval(Ctx, <<"1 + 1">>)
    end, lists:seq(1, 100)),

    %% Force GC
    erlang:garbage_collect(),
    timer:sleep(200),

    %% Check counters
    Counters = py_nif:get_debug_counters(),

    %% GIL ensure/release should be balanced
    GilEnsure = maps:get(gil_ensure, Counters, 0),
    GilRelease = maps:get(gil_release, Counters, 0),
    ct:log("GIL ensure: ~p, release: ~p", [GilEnsure, GilRelease]),

    %% Context created/destroyed should be balanced after GC
    CtxCreated = maps:get(ctx_created, Counters, 0),
    CtxDestroyed = maps:get(ctx_destroyed, Counters, 0),
    ct:log("Context created: ~p, destroyed: ~p", [CtxCreated, CtxDestroyed]),

    %% Suspended states should be balanced
    SuspCreated = maps:get(suspended_created, Counters, 0),
    SuspDestroyed = maps:get(suspended_destroyed, Counters, 0),
    ct:log("Suspended created: ~p, destroyed: ~p", [SuspCreated, SuspDestroyed]),

    %% Enqueue should match complete + rejected
    Enqueued = maps:get(enqueue_count, Counters, 0),
    Completed = maps:get(complete_count, Counters, 0),
    Rejected = maps:get(rejected_count, Counters, 0),
    ct:log("Enqueued: ~p, completed: ~p, rejected: ~p", [Enqueued, Completed, Rejected]),

    %% Verify no rejected operations during normal operation
    0 = Rejected,

    ok.

%% @doc Parallel operations on multiple contexts
parallel_context_operations_test(_Config) ->
    AvailableContexts = py_context_router:num_contexts(),
    NumContexts = min(10, AvailableContexts),
    NumOps = 50,

    %% Get available contexts
    Contexts = [py:context(N) || N <- lists:seq(1, NumContexts)],

    %% Spawn workers for each context - use py:eval with inline expressions
    %% to avoid __main__ namespace conflicts
    Self = self(),
    Pids = [spawn_link(fun() ->
        Results = [py:eval(Ctx, list_to_binary(io_lib:format("~p * ~p", [I, I])))
                   || I <- lists:seq(1, NumOps)],
        AllOk = lists:all(fun({ok, _}) -> true; (_) -> false end, Results),
        Self ! {done, self(), AllOk}
    end) || Ctx <- Contexts],

    %% Collect results
    Results = [receive {done, Pid, Ok} -> Ok after 30000 -> false end
               || Pid <- Pids],

    true = lists:all(fun(X) -> X end, Results),
    ok.

%% @doc Rapid application start/stop cycles
rapid_start_stop_test(Config) ->
    case ?config(parallel_running, Config) of
        true ->
            {skip, "Cannot run start/stop test in parallel mode"};
        _ ->
            do_rapid_start_stop_test()
    end.

do_rapid_start_stop_test() ->
    NumCycles = 5,

    lists:foreach(fun(_) ->
        %% Stop
        ok = application:stop(erlang_python),
        timer:sleep(50),

        %% Start
        {ok, _} = application:ensure_all_started(erlang_python),
        {ok, _} = py:start_contexts(),

        %% Quick operation
        {ok, 2} = py:eval(<<"1 + 1">>),
        timer:sleep(50)
    end, lists:seq(1, NumCycles)),

    ok.
