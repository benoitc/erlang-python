%%% @doc Common Test suite for py context API.
%%%
%%% Tests the explicit context API where py:call/eval/exec can take
%%% a context pid as the first argument.
-module(py_context_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    get_context_test/1,
    get_specific_context_test/1,
    state_persists_in_context_test/1,
    contexts_are_isolated_test/1,
    explicit_context_call_test/1,
    explicit_context_eval_test/1,
    explicit_context_exec_test/1,
    implicit_routing_test/1,
    scheduler_affinity_test/1
]).

all() -> [
    get_context_test,
    get_specific_context_test,
    state_persists_in_context_test,
    contexts_are_isolated_test,
    explicit_context_call_test,
    explicit_context_eval_test,
    explicit_context_exec_test,
    implicit_routing_test,
    scheduler_affinity_test
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

%% @doc Test py:context/0 returns a valid context pid
get_context_test(_Config) ->
    Ctx = py:context(),
    true = is_pid(Ctx),
    true = is_process_alive(Ctx).

%% @doc Test py:context/1 returns specific contexts
get_specific_context_test(_Config) ->
    Ctx1 = py:context(1),
    Ctx2 = py:context(2),
    true = is_pid(Ctx1),
    true = is_pid(Ctx2),
    %% Different indices should give different contexts
    true = Ctx1 =/= Ctx2.

%% @doc Test that state persists within a context
state_persists_in_context_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"test_var = 42">>),
    {ok, 42} = py:eval(Ctx, <<"test_var">>),
    ok = py:exec(Ctx, <<"test_var += 1">>),
    {ok, 43} = py:eval(Ctx, <<"test_var">>).

%% @doc Test that different contexts are isolated
contexts_are_isolated_test(_Config) ->
    Ctx1 = py:context(1),
    Ctx2 = py:context(2),

    ok = py:exec(Ctx1, <<"isolation_var = 'context1'">>),
    ok = py:exec(Ctx2, <<"isolation_var = 'context2'">>),

    %% Each context should have its own value
    {ok, <<"context1">>} = py:eval(Ctx1, <<"isolation_var">>),
    {ok, <<"context2">>} = py:eval(Ctx2, <<"isolation_var">>).

%% @doc Test py:call with explicit context
explicit_context_call_test(_Config) ->
    Ctx = py:context(1),
    {ok, 4.0} = py:call(Ctx, math, sqrt, [16]),
    {ok, 5.0} = py:call(Ctx, math, sqrt, [25]).

%% @doc Test py:eval with explicit context
explicit_context_eval_test(_Config) ->
    Ctx = py:context(1),
    {ok, 6} = py:eval(Ctx, <<"2 + 4">>),
    {ok, 15} = py:eval(Ctx, <<"x * 3">>, #{x => 5}).

%% @doc Test py:exec with explicit context
explicit_context_exec_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"exec_test = 123">>),
    {ok, 123} = py:eval(Ctx, <<"exec_test">>).

%% @doc Test implicit routing (without explicit context)
implicit_routing_test(_Config) ->
    %% These should work via scheduler affinity
    {ok, 4.0} = py:call(math, sqrt, [16]),
    {ok, 6} = py:eval(<<"2 + 4">>),
    ok = py:exec(<<"implicit_var = 1">>).

%% @doc Test that same process gets same context (scheduler affinity)
scheduler_affinity_test(_Config) ->
    Ctx1 = py:context(),
    Ctx2 = py:context(),
    %% Same process should get same context
    Ctx1 = Ctx2.
