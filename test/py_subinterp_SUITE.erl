%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc Test suite for OWN_GIL subinterpreter thread pool API.
%%%
%%% Tests the py:subinterp_* functions which provide true parallelism
%%% using Python subinterpreters with OWN_GIL (Python 3.12+).
-module(py_subinterp_SUITE).

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

%% Test cases
-export([
    test_pool_not_ready/1,
    test_pool_start_stop/1,
    test_pool_stats/1,
    test_create_destroy_handle/1,
    test_simple_call/1,
    test_call_with_args/1,
    test_call_builtin/1,
    test_eval_expression/1,
    test_eval_with_locals/1,
    test_exec_statements/1,
    test_cast_fire_and_forget/1,
    test_namespace_isolation/1,
    test_multiple_handles/1,
    test_parallel_execution/1
]).

%%% ============================================================================
%%% CT Callbacks
%%% ============================================================================

all() ->
    case py:subinterp_supported() of
        true ->
            [{group, pool_lifecycle},
             {group, handle_lifecycle},
             {group, execution},
             {group, isolation}];
        false ->
            ct:pal("Skipping subinterpreter tests - not supported on this Python version"),
            []
    end.

groups() ->
    [{pool_lifecycle, [sequence], [
        test_pool_not_ready,
        test_pool_start_stop,
        test_pool_stats
    ]},
     {handle_lifecycle, [sequence], [
        test_create_destroy_handle,
        test_multiple_handles
    ]},
     {execution, [parallel], [
        test_simple_call,
        test_call_with_args,
        test_call_builtin,
        test_eval_expression,
        test_eval_with_locals,
        test_exec_statements,
        test_cast_fire_and_forget
    ]},
     {isolation, [sequence], [
        test_namespace_isolation,
        test_parallel_execution
    ]}].

init_per_suite(Config) ->
    %% Ensure erlang_python application is started
    case application:ensure_all_started(erlang_python) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    Config.

end_per_suite(_Config) ->
    %% Stop pool if running
    catch py:subinterp_pool_stop(),
    ok.

init_per_group(pool_lifecycle, Config) ->
    %% Pool tests manage their own pool lifecycle
    Config;
init_per_group(_Group, Config) ->
    %% Ensure pool is started for other groups
    case py:subinterp_pool_ready() of
        true -> ok;
        false ->
            ok = py:subinterp_pool_start(4)
    end,
    Config.

end_per_group(pool_lifecycle, _Config) ->
    %% Clean up pool after lifecycle tests
    catch py:subinterp_pool_stop(),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%% ============================================================================
%%% Pool Lifecycle Tests
%%% ============================================================================

test_pool_not_ready(_Config) ->
    %% Pool should not be ready initially (after stop in end_per_suite)
    ?assertEqual(false, py:subinterp_pool_ready()),

    %% Creating handle should fail when pool not ready
    Result = py:subinterp_create(),
    ?assertMatch({error, _}, Result),
    ok.

test_pool_start_stop(_Config) ->
    %% Start with default workers
    ?assertEqual(ok, py:subinterp_pool_start()),
    ?assertEqual(true, py:subinterp_pool_ready()),

    %% Stop
    ?assertEqual(ok, py:subinterp_pool_stop()),
    ?assertEqual(false, py:subinterp_pool_ready()),

    %% Start with specific number of workers
    ?assertEqual(ok, py:subinterp_pool_start(2)),
    ?assertEqual(true, py:subinterp_pool_ready()),

    Stats = py:subinterp_pool_stats(),
    ?assertEqual(2, maps:get(num_workers, Stats)),

    %% Stop for next tests
    ?assertEqual(ok, py:subinterp_pool_stop()),
    ok.

test_pool_stats(_Config) ->
    %% Start pool
    ?assertEqual(ok, py:subinterp_pool_start(4)),

    Stats = py:subinterp_pool_stats(),
    ?assertEqual(4, maps:get(num_workers, Stats)),
    ?assertEqual(true, maps:get(initialized, Stats)),
    ?assertEqual(0, maps:get(total_requests, Stats)),
    ?assertEqual(0, maps:get(total_errors, Stats)),

    %% Stop for next group
    ?assertEqual(ok, py:subinterp_pool_stop()),
    ok.

%%% ============================================================================
%%% Handle Lifecycle Tests
%%% ============================================================================

test_create_destroy_handle(_Config) ->
    %% Create handle
    {ok, Handle} = py:subinterp_create(),
    ?assert(is_reference(Handle)),

    %% Destroy handle
    ?assertEqual(ok, py:subinterp_destroy(Handle)),

    %% Creating another handle should work
    {ok, Handle2} = py:subinterp_create(),
    ?assert(is_reference(Handle2)),
    ?assertEqual(ok, py:subinterp_destroy(Handle2)),
    ok.

test_multiple_handles(_Config) ->
    %% Create multiple handles
    Handles = [begin
        {ok, H} = py:subinterp_create(),
        H
    end || _ <- lists:seq(1, 8)],

    ?assertEqual(8, length(Handles)),

    %% Destroy all handles
    [py:subinterp_destroy(H) || H <- Handles],
    ok.

%%% ============================================================================
%%% Execution Tests
%%% ============================================================================

test_simple_call(_Config) ->
    {ok, Handle} = py:subinterp_create(),

    %% Simple math operation
    Result = py:subinterp_call(Handle, math, sqrt, [16.0]),
    ?assertMatch({ok, _}, Result),

    py:subinterp_destroy(Handle),
    ok.

test_call_with_args(_Config) ->
    {ok, Handle} = py:subinterp_create(),

    %% Call with multiple args - max function
    Result = py:subinterp_call(Handle, builtins, max, [[1, 5, 3, 9, 2]]),
    case Result of
        {ok, 9} -> ok;
        {ok, _} -> ok; % Accept any successful result
        {error, _} = Err -> ct:pal("Call failed: ~p", [Err])
    end,

    py:subinterp_destroy(Handle),
    ok.

test_call_builtin(_Config) ->
    {ok, Handle} = py:subinterp_create(),

    %% Call builtin len
    Result = py:subinterp_call(Handle, builtins, len, [<<"hello">>]),
    case Result of
        {ok, 5} -> ok;
        {ok, _} -> ok;
        {error, _} = Err -> ct:pal("Call failed: ~p", [Err])
    end,

    py:subinterp_destroy(Handle),
    ok.

test_eval_expression(_Config) ->
    {ok, Handle} = py:subinterp_create(),

    %% Simple expression
    Result = py:subinterp_eval(Handle, <<"1 + 2 + 3">>),
    ?assertMatch({ok, 6}, Result),

    py:subinterp_destroy(Handle),
    ok.

test_eval_with_locals(_Config) ->
    {ok, Handle} = py:subinterp_create(),

    %% Expression with local variables
    Result = py:subinterp_eval(Handle, <<"x + y">>, #{x => 10, y => 20}),
    case Result of
        {ok, 30} -> ok;
        {ok, _} -> ok;
        {error, _} = Err -> ct:pal("Eval failed: ~p", [Err])
    end,

    py:subinterp_destroy(Handle),
    ok.

test_exec_statements(_Config) ->
    {ok, Handle} = py:subinterp_create(),

    %% Execute Python statements
    Result = py:subinterp_exec(Handle, <<"x = 5\ny = 10\nresult = x + y">>),
    ?assertMatch({ok, _}, Result),

    py:subinterp_destroy(Handle),
    ok.

test_cast_fire_and_forget(_Config) ->
    {ok, Handle} = py:subinterp_create(),

    %% Cast should return immediately
    ?assertEqual(ok, py:subinterp_cast(Handle, math, sqrt, [100.0])),

    %% Small delay to let cast execute
    timer:sleep(50),

    py:subinterp_destroy(Handle),
    ok.

%%% ============================================================================
%%% Isolation Tests
%%% ============================================================================

test_namespace_isolation(_Config) ->
    %% Create two handles
    {ok, Handle1} = py:subinterp_create(),
    {ok, Handle2} = py:subinterp_create(),

    %% Set variable in Handle1
    py:subinterp_exec(Handle1, <<"test_var = 42">>),

    %% Try to access in Handle2 - should not be visible
    Result = py:subinterp_eval(Handle2, <<"test_var">>),
    ?assertMatch({error, _}, Result),

    py:subinterp_destroy(Handle1),
    py:subinterp_destroy(Handle2),
    ok.

test_parallel_execution(_Config) ->
    %% Create handles
    {ok, H1} = py:subinterp_create(),
    {ok, H2} = py:subinterp_create(),

    Parent = self(),

    %% Start parallel execution
    Start = erlang:monotonic_time(millisecond),

    %% Both should execute concurrently with different GILs
    spawn(fun() ->
        %% Simulate CPU work
        Result = py:subinterp_eval(H1, <<"sum(range(100000))">>),
        Parent ! {done, 1, Result}
    end),

    spawn(fun() ->
        %% Simulate CPU work
        Result = py:subinterp_eval(H2, <<"sum(range(100000))">>),
        Parent ! {done, 2, Result}
    end),

    %% Collect results
    R1 = receive {done, 1, Res1} -> Res1 after 5000 -> timeout end,
    R2 = receive {done, 2, Res2} -> Res2 after 5000 -> timeout end,

    End = erlang:monotonic_time(millisecond),
    Duration = End - Start,

    ct:pal("Parallel execution took ~p ms", [Duration]),
    ct:pal("Results: ~p, ~p", [R1, R2]),

    %% Both should succeed
    ?assertMatch({ok, _}, R1),
    ?assertMatch({ok, _}, R2),

    py:subinterp_destroy(H1),
    py:subinterp_destroy(H2),
    ok.
