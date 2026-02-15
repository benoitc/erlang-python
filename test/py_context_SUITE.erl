%%% @doc Common Test suite for py context affinity.
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
    bind_unbind_test/1,
    bind_persists_state_test/1,
    explicit_context_test/1,
    with_context_implicit_test/1,
    with_context_explicit_test/1,
    automatic_cleanup_test/1,
    multiple_contexts_isolated_test/1,
    double_bind_idempotent_test/1,
    unbind_without_bind_test/1,
    context_call_test/1
]).

all() -> [
    bind_unbind_test,
    bind_persists_state_test,
    explicit_context_test,
    with_context_implicit_test,
    with_context_explicit_test,
    automatic_cleanup_test,
    multiple_contexts_isolated_test,
    double_bind_idempotent_test,
    unbind_without_bind_test,
    context_call_test
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Ensure the test process is not bound at the start of each test
    py:unbind(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Clean up any bindings after each test
    py:unbind(),
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Test basic bind/unbind functionality
bind_unbind_test(_Config) ->
    false = py:is_bound(),
    ok = py:bind(),
    true = py:is_bound(),
    ok = py:unbind(),
    false = py:is_bound().

%% @doc Test that state persists across calls when bound
bind_persists_state_test(_Config) ->
    ok = py:bind(),
    ok = py:exec(<<"test_var = 42">>),
    {ok, 42} = py:eval(<<"test_var">>),
    ok = py:exec(<<"test_var += 1">>),
    {ok, 43} = py:eval(<<"test_var">>),
    ok = py:unbind().

%% @doc Test explicit context creation and usage
explicit_context_test(_Config) ->
    {ok, Ctx} = py:bind(new),
    ok = py:ctx_exec(Ctx, <<"ctx_var = 'hello'">>),
    {ok, <<"hello">>} = py:ctx_eval(Ctx, <<"ctx_var">>),
    ok = py:unbind(Ctx).

%% @doc Test with_context with implicit (arity-0) function
with_context_implicit_test(_Config) ->
    Result = py:with_context(fun() ->
        ok = py:exec(<<"x = 10">>),
        ok = py:exec(<<"x *= 2">>),
        py:eval(<<"x">>)
    end),
    {ok, 20} = Result,
    false = py:is_bound().

%% @doc Test with_context with explicit (arity-1) function
with_context_explicit_test(_Config) ->
    Result = py:with_context(fun(Ctx) ->
        ok = py:ctx_exec(Ctx, <<"y = 5">>),
        py:ctx_eval(Ctx, <<"y * 3">>)
    end),
    {ok, 15} = Result.

%% @doc Test automatic cleanup when bound process dies
automatic_cleanup_test(_Config) ->
    Parent = self(),
    Stats1 = py_pool:get_stats(),
    Avail1 = maps:get(available_workers, Stats1),

    Pid = spawn(fun() ->
        ok = py:bind(),
        Parent ! bound,
        receive stop -> ok end
    end),
    receive bound -> ok end,

    %% Worker should be checked out
    Stats2 = py_pool:get_stats(),
    Avail2 = maps:get(available_workers, Stats2),
    true = Avail2 < Avail1,

    %% Kill the process
    exit(Pid, kill),
    timer:sleep(50),

    %% Worker should be returned to pool
    Stats3 = py_pool:get_stats(),
    Avail3 = maps:get(available_workers, Stats3),
    Avail1 = Avail3.

%% @doc Test that multiple explicit contexts are isolated
multiple_contexts_isolated_test(_Config) ->
    {ok, Ctx1} = py:bind(new),
    {ok, Ctx2} = py:bind(new),

    ok = py:ctx_exec(Ctx1, <<"shared_name = 1">>),
    ok = py:ctx_exec(Ctx2, <<"shared_name = 2">>),

    %% Each context should have its own value
    {ok, 1} = py:ctx_eval(Ctx1, <<"shared_name">>),
    {ok, 2} = py:ctx_eval(Ctx2, <<"shared_name">>),

    ok = py:unbind(Ctx1),
    ok = py:unbind(Ctx2).

%% @doc Test that double bind is idempotent
double_bind_idempotent_test(_Config) ->
    ok = py:bind(),
    ok = py:bind(),  % Should be idempotent
    true = py:is_bound(),
    ok = py:unbind(),
    false = py:is_bound().

%% @doc Test that unbind without bind is safe (idempotent)
unbind_without_bind_test(_Config) ->
    false = py:is_bound(),
    ok = py:unbind(),  % Should be safe even without prior bind
    false = py:is_bound().

%% @doc Test py:ctx_call with explicit context
context_call_test(_Config) ->
    {ok, Ctx} = py:bind(new),

    %% Import a module in this context
    ok = py:ctx_exec(Ctx, <<"import json">>),

    %% Use the imported module via ctx_call
    {ok, <<"{\"a\": 1}">>} = py:ctx_call(Ctx, json, dumps, [#{a => 1}]),

    ok = py:unbind(Ctx).
