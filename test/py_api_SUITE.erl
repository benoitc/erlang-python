%%% @doc Common Test suite for py module's new process-per-context API.
%%%
%%% Tests the new explicit context API where py:call/eval/exec can take
%%% a context pid as the first argument.
-module(py_api_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    %% Existing API compatibility
    test_call_basic/1,
    test_call_with_kwargs/1,
    test_eval_basic/1,
    test_exec_basic/1,
    %% New explicit context API
    test_explicit_context_call/1,
    test_explicit_context_eval/1,
    test_explicit_context_exec/1,
    test_explicit_context_isolation/1,
    %% Context management API
    test_context_management/1,
    test_start_stop_contexts/1,
    %% Mixed usage
    test_mixed_api_usage/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        %% Existing API compatibility
        test_call_basic,
        test_call_with_kwargs,
        test_eval_basic,
        test_exec_basic,
        %% New explicit context API
        test_explicit_context_call,
        test_explicit_context_eval,
        test_explicit_context_exec,
        test_explicit_context_isolation,
        %% Context management API
        test_context_management,
        test_start_stop_contexts,
        %% Mixed usage
        test_mixed_api_usage
    ].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Ensure fresh contexts are available for each test
    catch py:stop_contexts(),
    {ok, _} = py:start_contexts(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Clean up after each test
    catch py:stop_contexts(),
    ok.

%% ============================================================================
%% Existing API Compatibility Tests
%% ============================================================================

%% @doc Test that basic py:call still works (backwards compatibility).
test_call_basic(_Config) ->
    {ok, 4.0} = py:call(math, sqrt, [16]),
    {ok, 5.0} = py:call(math, sqrt, [25]),
    {ok, 3} = py:call(builtins, len, [[1, 2, 3]]).

%% @doc Test that py:call with kwargs still works.
test_call_with_kwargs(_Config) ->
    {ok, _} = py:call(json, dumps, [[{a, 1}]], #{indent => 2}),
    {ok, _} = py:call(json, dumps, [#{a => 1, b => 2}], #{sort_keys => true}).

%% @doc Test that py:eval still works.
test_eval_basic(_Config) ->
    {ok, 6} = py:eval(<<"2 + 4">>),
    {ok, 15} = py:eval(<<"x * 3">>, #{x => 5}),
    {ok, [1, 4, 9]} = py:eval(<<"[i*i for i in range(1, 4)]">>).

%% @doc Test that py:exec still works.
test_exec_basic(_Config) ->
    ok = py:exec(<<"import math">>).

%% ============================================================================
%% New Explicit Context API Tests
%% ============================================================================

%% @doc Test py:call with explicit context pid.
test_explicit_context_call(_Config) ->
    {ok, _} = py:start_contexts(#{contexts => 2}),

    Ctx = py:context(),
    true = is_pid(Ctx),

    %% Call with context as first argument
    {ok, 4.0} = py:call(Ctx, math, sqrt, [16]),
    {ok, 5.0} = py:call(Ctx, math, sqrt, [25]),

    %% Call with options
    {ok, _} = py:call(Ctx, json, dumps, [[{a, 1}]], #{kwargs => #{indent => 2}}).

%% @doc Test py:eval with explicit context pid.
test_explicit_context_eval(_Config) ->
    {ok, _} = py:start_contexts(#{contexts => 2}),

    Ctx = py:context(),

    %% Eval with context as first argument
    {ok, 6} = py:eval(Ctx, <<"2 + 4">>),

    %% Eval with context and locals
    {ok, 15} = py:eval(Ctx, <<"x * 3">>, #{x => 5}).

%% @doc Test py:exec with explicit context pid.
test_explicit_context_exec(_Config) ->
    {ok, _} = py:start_contexts(#{contexts => 2}),

    Ctx = py:context(),

    %% Exec with context as first argument
    ok = py:exec(Ctx, <<"test_var = 42">>),

    %% Verify the variable persists in that context
    {ok, 42} = py:eval(Ctx, <<"test_var">>, #{}).

%% @doc Test that different contexts are isolated.
test_explicit_context_isolation(_Config) ->
    {ok, _} = py:start_contexts(#{contexts => 2}),

    Ctx1 = py:context(1),
    Ctx2 = py:context(2),

    %% Set different values in each context
    ok = py:exec(Ctx1, <<"isolation_test = 'context1'">>),
    ok = py:exec(Ctx2, <<"isolation_test = 'context2'">>),

    %% Verify isolation
    {ok, <<"context1">>} = py:eval(Ctx1, <<"isolation_test">>, #{}),
    {ok, <<"context2">>} = py:eval(Ctx2, <<"isolation_test">>, #{}).

%% ============================================================================
%% Context Management API Tests
%% ============================================================================

%% @doc Test py:context/0 and py:context/1.
test_context_management(_Config) ->
    %% Stop any existing contexts first, then start with specific count
    py:stop_contexts(),
    {ok, Contexts} = py:start_contexts(#{contexts => 4}),
    4 = length(Contexts),

    %% Get context for current scheduler
    Ctx = py:context(),
    true = is_pid(Ctx),
    true = lists:member(Ctx, Contexts),

    %% Get specific contexts by index
    Ctx1 = py:context(1),
    Ctx2 = py:context(2),
    Ctx3 = py:context(3),
    Ctx4 = py:context(4),

    [Ctx1, Ctx2, Ctx3, Ctx4] = Contexts.

%% @doc Test py:start_contexts/0,1 and py:stop_contexts/0.
test_start_stop_contexts(_Config) ->
    %% Start with default settings
    {ok, Contexts1} = py:start_contexts(),
    true = length(Contexts1) > 0,

    %% All contexts should be alive
    lists:foreach(fun(Ctx) ->
        true = is_process_alive(Ctx)
    end, Contexts1),

    %% Stop
    ok = py:stop_contexts(),

    %% Wait for contexts to die
    timer:sleep(100),
    lists:foreach(fun(Ctx) ->
        false = is_process_alive(Ctx)
    end, Contexts1),

    %% Start with custom settings
    {ok, Contexts2} = py:start_contexts(#{contexts => 2}),
    2 = length(Contexts2),

    ok = py:stop_contexts().

%% ============================================================================
%% Mixed Usage Tests
%% ============================================================================

%% @doc Test implicit and explicit context API usage.
test_mixed_api_usage(_Config) ->
    {ok, _} = py:start_contexts(#{contexts => 2}),

    %% Use implicit API (auto-routes through py_context_router)
    {ok, 4.0} = py:call(math, sqrt, [16]),

    %% Use explicit API (direct context pid)
    Ctx = py:context(),
    {ok, 5.0} = py:call(Ctx, math, sqrt, [25]),

    %% Both should work correctly
    {ok, 6} = py:eval(<<"2 + 4">>),
    {ok, 7} = py:eval(Ctx, <<"3 + 4">>, #{}).
