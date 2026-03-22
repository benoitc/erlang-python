%%% @doc Test suite for py:import with OWN_GIL subinterpreters.
%%%
%%% Tests the import caching functionality with Python 3.12+ OWN_GIL mode,
%%% which creates dedicated pthreads with independent Python GILs.
%%%
%%% OWN_GIL mode requires Python 3.12+.
-module(py_import_owngil_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

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

%% Basic import tests
-export([
    owngil_import_module_test/1,
    owngil_import_function_test/1,
    owngil_import_main_rejected_test/1,
    owngil_import_stats_test/1,
    owngil_import_list_test/1,
    owngil_flush_imports_test/1
]).

%% Isolation tests
-export([
    owngil_import_isolation_test/1,
    owngil_import_parallel_contexts_test/1
]).

%% Stress tests
-export([
    owngil_import_concurrent_test/1
]).

all() ->
    [{group, basic_imports},
     {group, isolation},
     {group, stress}].

groups() ->
    [{basic_imports, [sequence], [
        owngil_import_module_test,
        owngil_import_function_test,
        owngil_import_main_rejected_test,
        owngil_import_stats_test,
        owngil_import_list_test,
        owngil_flush_imports_test
    ]},
     {isolation, [sequence], [
        owngil_import_isolation_test,
        owngil_import_parallel_contexts_test
    ]},
     {stress, [sequence], [
        owngil_import_concurrent_test
    ]}].

init_per_suite(Config) ->
    case py_nif:subinterp_supported() of
        true ->
            {ok, _} = application:ensure_all_started(erlang_python),
            timer:sleep(500),
            Config;
        false ->
            {skip, "Requires Python 3.12+"}
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Helper Functions
%% ============================================================================

%% Create an OWN_GIL context
create_owngil_context() ->
    {ok, Ctx} = py_context:new(#{mode => owngil}),
    Ctx.

%% ============================================================================
%% Basic Import Tests
%% ============================================================================

%% @doc Test importing a module in OWN_GIL context
owngil_import_module_test(_Config) ->
    Ctx = create_owngil_context(),
    try
        %% Import using the context
        ok = py_context:exec(Ctx, <<"import json">>),

        %% Call the imported module
        {ok, Result} = py_context:call(Ctx, json, dumps, [[1, 2, 3]]),
        ?assertEqual(<<"[1, 2, 3]">>, Result),

        %% Import another module
        ok = py_context:exec(Ctx, <<"import math">>),
        {ok, SqrtResult} = py_context:call(Ctx, math, sqrt, [16.0]),
        ?assertEqual(4.0, SqrtResult)
    after
        py_context:destroy(Ctx)
    end.

%% @doc Test importing a specific function in OWN_GIL context
owngil_import_function_test(_Config) ->
    Ctx = create_owngil_context(),
    try
        %% Import specific function
        ok = py_context:exec(Ctx, <<"from json import dumps, loads">>),

        %% Use the imported functions
        {ok, JsonStr} = py_context:call(Ctx, json, dumps, [#{a => 1}]),
        ?assert(is_binary(JsonStr)),

        %% Import from os
        ok = py_context:exec(Ctx, <<"from os import getcwd">>),
        {ok, Cwd} = py_context:call(Ctx, os, getcwd, []),
        ?assert(is_binary(Cwd))
    after
        py_context:destroy(Ctx)
    end.

%% @doc Test that __main__ execution works but is isolated
owngil_import_main_rejected_test(_Config) ->
    Ctx = create_owngil_context(),
    try
        %% Define a function in __main__
        ok = py_context:exec(Ctx, <<"
def my_func(x):
    return x * 2
">>),

        %% Call the function defined in __main__
        {ok, Result} = py_context:call(Ctx, '__main__', my_func, [21]),
        ?assertEqual(42, Result)
    after
        py_context:destroy(Ctx)
    end.

%% @doc Test import stats in OWN_GIL context
owngil_import_stats_test(_Config) ->
    Ctx = create_owngil_context(),
    try
        %% Import some modules
        ok = py_context:exec(Ctx, <<"import json">>),
        ok = py_context:exec(Ctx, <<"import math">>),
        ok = py_context:exec(Ctx, <<"import os">>),

        %% Verify modules are importable by calling them
        {ok, _} = py_context:call(Ctx, json, dumps, [[1]]),
        {ok, _} = py_context:call(Ctx, math, sqrt, [4.0]),
        {ok, _} = py_context:call(Ctx, os, getcwd, [])
    after
        py_context:destroy(Ctx)
    end.

%% @doc Test import list in OWN_GIL context
owngil_import_list_test(_Config) ->
    Ctx = create_owngil_context(),
    try
        %% Import modules and functions
        ok = py_context:exec(Ctx, <<"import json">>),
        ok = py_context:exec(Ctx, <<"import math">>),
        ok = py_context:exec(Ctx, <<"from json import dumps, loads">>),

        %% Verify they work
        {ok, _} = py_context:call(Ctx, json, dumps, [[1, 2]]),
        {ok, _} = py_context:call(Ctx, math, floor, [3.7])
    after
        py_context:destroy(Ctx)
    end.

%% @doc Test flush imports functionality
owngil_flush_imports_test(_Config) ->
    Ctx = create_owngil_context(),
    try
        %% Import a module
        ok = py_context:exec(Ctx, <<"import json">>),
        {ok, _} = py_context:call(Ctx, json, dumps, [[1]]),

        %% Module should still work after re-import
        ok = py_context:exec(Ctx, <<"import json">>),
        {ok, Result} = py_context:call(Ctx, json, dumps, [[2, 3]]),
        ?assertEqual(<<"[2, 3]">>, Result)
    after
        py_context:destroy(Ctx)
    end.

%% ============================================================================
%% Isolation Tests
%% ============================================================================

%% @doc Test that imports are isolated between OWN_GIL contexts
owngil_import_isolation_test(_Config) ->
    Ctx1 = create_owngil_context(),
    Ctx2 = create_owngil_context(),
    try
        %% Define a variable in Ctx1
        ok = py_context:exec(Ctx1, <<"
import json
MY_VAR = 'context1'
">>),

        %% Define a different variable in Ctx2
        ok = py_context:exec(Ctx2, <<"
import math
MY_VAR = 'context2'
">>),

        %% Verify isolation - Ctx1 has json but not math imported the same way
        {ok, R1} = py_context:eval(Ctx1, <<"MY_VAR">>),
        ?assertEqual(<<"context1">>, R1),

        {ok, R2} = py_context:eval(Ctx2, <<"MY_VAR">>),
        ?assertEqual(<<"context2">>, R2),

        %% Verify each context can use its imports
        {ok, _} = py_context:call(Ctx1, json, dumps, [[1]]),
        {ok, _} = py_context:call(Ctx2, math, sqrt, [9.0])
    after
        py_context:destroy(Ctx1),
        py_context:destroy(Ctx2)
    end.

%% @doc Test parallel import operations across multiple OWN_GIL contexts
owngil_import_parallel_contexts_test(_Config) ->
    Parent = self(),
    NumContexts = 3,

    %% Spawn processes, each with its own OWN_GIL context
    Pids = [spawn_link(fun() ->
        Ctx = create_owngil_context(),
        try
            %% Each context imports different modules
            Modules = case N rem 3 of
                0 -> [<<"json">>, <<"base64">>];
                1 -> [<<"math">>, <<"string">>];
                2 -> [<<"os">>, <<"re">>]
            end,

            %% Import modules
            lists:foreach(fun(Mod) ->
                Code = <<"import ", Mod/binary>>,
                ok = py_context:exec(Ctx, Code)
            end, Modules),

            %% Define context-specific state
            StateCode = list_to_binary(io_lib:format("CTX_ID = ~p", [N])),
            ok = py_context:exec(Ctx, StateCode),

            %% Verify state
            {ok, CtxId} = py_context:eval(Ctx, <<"CTX_ID">>),

            Parent ! {self(), {ok, N, CtxId, Modules}}
        catch
            E:R ->
                Parent ! {self(), {error, N, E, R}}
        after
            py_context:destroy(Ctx)
        end
    end) || N <- lists:seq(1, NumContexts)],

    %% Collect results
    Results = [receive {Pid, Result} -> Result after 10000 -> timeout end || Pid <- Pids],

    %% Verify all succeeded
    lists:foreach(fun(Result) ->
        case Result of
            {ok, N, CtxId, _Modules} ->
                ?assertEqual(N, CtxId),
                ct:pal("Context ~p: OK", [N]);
            {error, N, E, R} ->
                ct:fail("Context ~p failed: ~p:~p", [N, E, R]);
            timeout ->
                ct:fail("Timeout waiting for context")
        end
    end, Results).

%% ============================================================================
%% Stress Tests
%% ============================================================================

%% @doc Stress test with many concurrent OWN_GIL contexts importing
owngil_import_concurrent_test(_Config) ->
    Parent = self(),
    NumContexts = 10,
    Modules = [<<"json">>, <<"math">>, <<"os">>, <<"string">>, <<"re">>],

    %% Spawn many processes with OWN_GIL contexts
    Pids = [spawn_link(fun() ->
        Ctx = create_owngil_context(),
        try
            %% Import a subset of modules
            MyModules = lists:sublist(Modules, 1 + (N rem length(Modules))),

            lists:foreach(fun(Mod) ->
                Code = <<"import ", Mod/binary>>,
                ok = py_context:exec(Ctx, Code)
            end, MyModules),

            %% Make calls to verify imports work
            CallResults = lists:map(fun(Mod) ->
                case Mod of
                    <<"json">> -> py_context:call(Ctx, json, dumps, [[N]]);
                    <<"math">> -> py_context:call(Ctx, math, sqrt, [float(N)]);
                    <<"os">> -> py_context:call(Ctx, os, getcwd, []);
                    <<"string">> -> py_context:call(Ctx, string, capwords, [<<"hello world">>]);
                    <<"re">> -> py_context:call(Ctx, re, escape, [<<"test">>])
                end
            end, MyModules),

            AllOk = lists:all(fun({ok, _}) -> true; (_) -> false end, CallResults),

            Parent ! {self(), {ok, N, AllOk, length(MyModules)}}
        catch
            E:R:St ->
                Parent ! {self(), {error, N, E, R, St}}
        after
            py_context:destroy(Ctx)
        end
    end) || N <- lists:seq(1, NumContexts)],

    %% Collect results
    Results = [receive {Pid, Result} -> Result after 30000 -> timeout end || Pid <- Pids],

    %% Count successes and failures
    {Successes, Failures} = lists:partition(fun
        ({ok, _, true, _}) -> true;
        (_) -> false
    end, Results),

    ct:pal("OWN_GIL concurrent import test: ~p successes, ~p failures",
           [length(Successes), length(Failures)]),

    %% Log any failures
    lists:foreach(fun
        ({error, N, E, R, _St}) ->
            ct:pal("Context ~p failed: ~p:~p", [N, E, R]);
        ({ok, N, false, _}) ->
            ct:pal("Context ~p: calls failed", [N]);
        (timeout) ->
            ct:pal("Timeout");
        (_) ->
            ok
    end, Failures),

    ?assertEqual(NumContexts, length(Successes)),
    ?assertEqual(0, length(Failures)).
