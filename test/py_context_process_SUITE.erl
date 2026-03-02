%%% @doc Common Test suite for py_context process-per-context module.
%%%
%%% Tests the process-per-context architecture where each Erlang process
%%% owns a Python context (subinterpreter or worker).
-module(py_context_process_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    test_context_start_stop/1,
    test_context_call/1,
    test_context_eval/1,
    test_context_exec/1,
    test_context_isolation/1,
    test_context_module_caching/1,
    test_context_under_supervisor/1,
    test_multiple_contexts/1,
    test_context_parallel_calls/1,
    test_context_timeout/1,
    test_context_error_handling/1,
    test_context_type_conversions/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        {group, worker_mode},
        {group, subinterp_mode}
    ].

groups() ->
    Tests = [
        test_context_start_stop,
        test_context_call,
        test_context_eval,
        test_context_exec,
        test_context_isolation,
        test_context_module_caching,
        test_context_under_supervisor,
        test_multiple_contexts,
        test_context_parallel_calls,
        test_context_timeout,
        test_context_error_handling,
        test_context_type_conversions
    ],
    [
        {worker_mode, [sequence], Tests},
        {subinterp_mode, [sequence], Tests}
    ].

init_per_suite(Config) ->
    %% Ensure the application is started
    application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(worker_mode, Config) ->
    [{context_mode, worker} | Config];
init_per_group(subinterp_mode, Config) ->
    case py_nif:subinterp_supported() of
        true ->
            [{context_mode, subinterp} | Config];
        false ->
            {skip, "Subinterpreters not supported (requires Python 3.12+)"}
    end.

end_per_group(_Group, _Config) ->
    ok.

%% ============================================================================
%% Test cases
%% ============================================================================

%% @doc Test that a context can be started and stopped.
test_context_start_stop(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),
    true = is_process_alive(Ctx),
    ok = py_context:stop(Ctx),
    timer:sleep(50),
    false = is_process_alive(Ctx).

%% @doc Test basic Python function calls.
test_context_call(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),
    try
        %% Test math.sqrt
        {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{}),

        %% Test with kwargs
        {ok, _} = py_context:call(Ctx, json, dumps, [[{<<"a">>, 1}]], #{indent => 2}),

        %% Test len function
        {ok, 3} = py_context:call(Ctx, builtins, len, [[1, 2, 3]], #{})
    after
        py_context:stop(Ctx)
    end.

%% @doc Test Python expression evaluation.
test_context_eval(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),
    try
        %% Simple arithmetic
        {ok, 6} = py_context:eval(Ctx, <<"2 + 4">>, #{}),

        %% With locals
        {ok, 15} = py_context:eval(Ctx, <<"x * 3">>, #{x => 5}),

        %% List comprehension
        {ok, [1, 4, 9, 16]} = py_context:eval(Ctx, <<"[i*i for i in range(1, 5)]">>, #{})
    after
        py_context:stop(Ctx)
    end.

%% @doc Test Python statement execution.
test_context_exec(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),
    try
        %% Execute statements
        ok = py_context:exec(Ctx, <<"x = 42">>),
        {ok, 42} = py_context:eval(Ctx, <<"x">>, #{}),

        %% Define a function
        ok = py_context:exec(Ctx, <<"def add(a, b): return a + b">>),
        {ok, 7} = py_context:eval(Ctx, <<"add(3, 4)">>, #{})
    after
        py_context:stop(Ctx)
    end.

%% @doc Test that contexts are isolated from each other.
test_context_isolation(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx1} = py_context:start_link(1, Mode),
    {ok, Ctx2} = py_context:start_link(2, Mode),
    try
        %% Set different values in each context
        ok = py_context:exec(Ctx1, <<"isolation_var = 'ctx1'">>),
        ok = py_context:exec(Ctx2, <<"isolation_var = 'ctx2'">>),

        %% Verify isolation
        {ok, <<"ctx1">>} = py_context:eval(Ctx1, <<"isolation_var">>, #{}),
        {ok, <<"ctx2">>} = py_context:eval(Ctx2, <<"isolation_var">>, #{}),

        %% Verify interpreter IDs are different
        {ok, Id1} = py_context:get_interp_id(Ctx1),
        {ok, Id2} = py_context:get_interp_id(Ctx2),
        true = Id1 =/= Id2
    after
        py_context:stop(Ctx1),
        py_context:stop(Ctx2)
    end.

%% @doc Test that modules are cached within a context.
test_context_module_caching(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),
    try
        %% First call imports the module
        {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{}),

        %% Second call should use cached module (faster)
        {ok, 5.0} = py_context:call(Ctx, math, sqrt, [25], #{}),

        %% Multiple different modules
        {ok, _} = py_context:call(Ctx, json, dumps, [[1, 2, 3]], #{}),
        {ok, _} = py_context:call(Ctx, os, getcwd, [], #{})
    after
        py_context:stop(Ctx)
    end.

%% @doc Test contexts under the supervisor.
test_context_under_supervisor(Config) ->
    Mode = ?config(context_mode, Config),

    %% Start supervisor if not already running
    case whereis(py_context_sup) of
        undefined ->
            {ok, _SupPid} = py_context_sup:start_link();
        _ ->
            ok
    end,
    try
        %% Start contexts via supervisor
        {ok, Ctx1} = py_context_sup:start_context(1, Mode),
        {ok, Ctx2} = py_context_sup:start_context(2, Mode),

        %% Verify they work
        {ok, 4.0} = py_context:call(Ctx1, math, sqrt, [16], #{}),
        {ok, 9.0} = py_context:call(Ctx2, math, sqrt, [81], #{}),

        %% Check which_contexts
        Contexts = py_context_sup:which_contexts(),
        true = lists:member(Ctx1, Contexts),
        true = lists:member(Ctx2, Contexts),

        %% Stop one via supervisor
        ok = py_context_sup:stop_context(Ctx1),
        timer:sleep(50),
        false = is_process_alive(Ctx1),
        true = is_process_alive(Ctx2),

        %% Clean up
        py_context_sup:stop_context(Ctx2)
    after
        ok
    end.

%% @doc Test multiple contexts working in parallel.
test_multiple_contexts(Config) ->
    Mode = ?config(context_mode, Config),
    NumContexts = 4,

    %% Start multiple contexts
    Contexts = [begin
        {ok, Ctx} = py_context:start_link(N, Mode),
        Ctx
    end || N <- lists:seq(1, NumContexts)],

    try
        %% Run calls in parallel
        Parent = self(),
        Pids = [spawn_link(fun() ->
            Results = [py_context:call(Ctx, math, sqrt, [N*N], #{})
                       || N <- lists:seq(1, 10)],
            Parent ! {self(), Results}
        end) || Ctx <- Contexts],

        %% Collect results
        [receive
            {Pid, Results} ->
                %% Verify all calls succeeded
                lists:foreach(fun({ok, _}) -> ok end, Results)
        after 5000 ->
            ct:fail("Timeout waiting for results")
        end || Pid <- Pids]
    after
        [py_context:stop(Ctx) || Ctx <- Contexts]
    end.

%% @doc Test parallel calls to the same context.
test_context_parallel_calls(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),

    try
        %% Make multiple calls from different processes
        %% The context should serialize them (process-owned)
        Parent = self(),
        NumCalls = 20,

        Pids = [spawn_link(fun() ->
            Result = py_context:call(Ctx, math, sqrt, [N*N], #{}),
            Parent ! {self(), N, Result}
        end) || N <- lists:seq(1, NumCalls)],

        Results = [receive
            {Pid, N, Result} -> {N, Result}
        after 5000 ->
            ct:fail("Timeout")
        end || Pid <- Pids],

        %% Verify all returned correct values
        lists:foreach(fun({N, {ok, Val}}) ->
            true = abs(Val - float(N)) < 0.0001
        end, Results)
    after
        py_context:stop(Ctx)
    end.

%% @doc Test call timeout handling.
test_context_timeout(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),

    try
        %% Quick call should succeed
        {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{}, 1000),

        %% Slow call with short timeout should fail
        %% (Can't easily test this without a slow Python function)
        ok
    after
        py_context:stop(Ctx)
    end.

%% @doc Test error handling in context calls.
test_context_error_handling(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),

    try
        %% Invalid module
        {error, _} = py_context:call(Ctx, nonexistent_module, func, [], #{}),

        %% Invalid function
        {error, _} = py_context:call(Ctx, math, nonexistent_func, [], #{}),

        %% Python exception
        {error, _} = py_context:eval(Ctx, <<"1/0">>, #{}),

        %% Syntax error in exec
        {error, _} = py_context:exec(Ctx, <<"if true">>),

        %% Context should still work after errors
        {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{})
    after
        py_context:stop(Ctx)
    end.

%% @doc Test type conversions between Erlang and Python.
test_context_type_conversions(Config) ->
    Mode = ?config(context_mode, Config),
    {ok, Ctx} = py_context:start_link(1, Mode),

    try
        %% Integers
        {ok, 42} = py_context:eval(Ctx, <<"x">>, #{x => 42}),

        %% Floats
        {ok, 3.14159} = py_context:eval(Ctx, <<"x">>, #{x => 3.14159}),

        %% Strings (binaries)
        {ok, <<"hello">>} = py_context:eval(Ctx, <<"x">>, #{x => <<"hello">>}),

        %% Lists
        {ok, [1, 2, 3]} = py_context:eval(Ctx, <<"x">>, #{x => [1, 2, 3]}),

        %% Maps/Dicts
        {ok, Map} = py_context:eval(Ctx, <<"x">>, #{x => #{a => 1, b => 2}}),
        true = is_map(Map),

        %% Booleans
        {ok, true} = py_context:eval(Ctx, <<"x">>, #{x => true}),
        {ok, false} = py_context:eval(Ctx, <<"x">>, #{x => false}),

        %% None
        {ok, none} = py_context:eval(Ctx, <<"None">>, #{})
    after
        py_context:stop(Ctx)
    end.
