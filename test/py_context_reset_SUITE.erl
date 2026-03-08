%%% @doc Common Test suite for context reset and reload functionality.
-module(py_context_reset_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_reset_clears_variables/1,
    test_reset_keeps_builtins/1,
    test_reset_keeps_erlang_module/1,
    test_reset_allows_new_definitions/1,
    test_reset_multiple_times/1,
    test_reload_module/1,
    test_reload_nonexistent_module/1,
    test_reload_multiple_modules/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        test_reset_clears_variables,
        test_reset_keeps_builtins,
        test_reset_keeps_erlang_module,
        test_reset_allows_new_definitions,
        test_reset_multiple_times,
        test_reload_module,
        test_reload_nonexistent_module,
        test_reload_multiple_modules
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Get a fresh context for each test
    Ctx = py:context(1),
    %% Reset before each test to ensure clean state
    ok = py:reset_context(Ctx),
    [{ctx, Ctx} | Config].

end_per_testcase(_TestCase, Config) ->
    %% Reset after each test for clean slate
    Ctx = proplists:get_value(ctx, Config),
    catch py:reset_context(Ctx),
    ok.

%% ============================================================================
%% Test cases
%% ============================================================================

%% @doc Test that reset clears user-defined variables.
test_reset_clears_variables(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    %% Define some variables
    ok = py:exec(Ctx, <<"x = 42">>),
    ok = py:exec(Ctx, <<"y = 'hello'">>),
    ok = py:exec(Ctx, <<"z = [1, 2, 3]">>),

    %% Verify they exist
    {ok, 42} = py:eval(Ctx, <<"x">>),
    {ok, <<"hello">>} = py:eval(Ctx, <<"y">>),
    {ok, [1, 2, 3]} = py:eval(Ctx, <<"z">>),

    %% Reset the context
    ok = py:reset_context(Ctx),

    %% Variables should no longer exist
    {error, _} = py:eval(Ctx, <<"x">>),
    {error, _} = py:eval(Ctx, <<"y">>),
    {error, _} = py:eval(Ctx, <<"z">>).

%% @doc Test that reset keeps __builtins__.
test_reset_keeps_builtins(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    %% Reset
    ok = py:reset_context(Ctx),

    %% Builtins should still work
    {ok, 5} = py:eval(Ctx, <<"len([1,2,3,4,5])">>),
    {ok, 6} = py:eval(Ctx, <<"sum([1,2,3])">>),
    {ok, <<"hello">>} = py:eval(Ctx, <<"str('hello')">>),
    {ok, 10} = py:eval(Ctx, <<"int('10')">>).

%% @doc Test that reset keeps erlang module importable.
test_reset_keeps_erlang_module(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    %% Import erlang and use it
    ok = py:exec(Ctx, <<"import erlang">>),

    %% Reset (this clears the import from namespace)
    ok = py:reset_context(Ctx),

    %% erlang module should still be importable and usable
    ok = py:exec(Ctx, <<"import erlang">>),
    {ok, true} = py:eval(Ctx, <<"hasattr(erlang, 'call')">>).

%% @doc Test that we can define new variables after reset.
test_reset_allows_new_definitions(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    %% Define and reset
    ok = py:exec(Ctx, <<"old_var = 100">>),
    ok = py:reset_context(Ctx),

    %% Define new variables
    ok = py:exec(Ctx, <<"new_var = 200">>),
    ok = py:exec(Ctx, <<"def my_func(): return 42">>),

    %% New definitions should work
    {ok, 200} = py:eval(Ctx, <<"new_var">>),
    {ok, 42} = py:eval(Ctx, <<"my_func()">>),

    %% Old variable still gone
    {error, _} = py:eval(Ctx, <<"old_var">>).

%% @doc Test that reset can be called multiple times.
test_reset_multiple_times(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    lists:foreach(fun(I) ->
        %% Define a variable
        Code = iolist_to_binary(io_lib:format("var_~p = ~p", [I, I])),
        ok = py:exec(Ctx, Code),

        %% Verify it exists
        CheckCode = iolist_to_binary(io_lib:format("var_~p", [I])),
        {ok, I} = py:eval(Ctx, CheckCode),

        %% Reset
        ok = py:reset_context(Ctx),

        %% Variable should be gone
        {error, _} = py:eval(Ctx, CheckCode)
    end, lists:seq(1, 5)).

%% @doc Test reloading a module.
test_reload_module(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    %% Import a standard library module
    ok = py:exec(Ctx, <<"import json">>),

    %% Reload should succeed (even if no changes)
    ok = py:reload_context(Ctx, [<<"json">>]).

%% @doc Test reloading a non-existent module (should not error).
test_reload_nonexistent_module(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    %% Reloading non-existent module should be ok (just ignored)
    ok = py:reload_context(Ctx, [<<"nonexistent_module_xyz">>]).

%% @doc Test reloading multiple modules.
test_reload_multiple_modules(Config) ->
    Ctx = proplists:get_value(ctx, Config),

    %% Import multiple modules
    ok = py:exec(Ctx, <<"import json, os, sys">>),

    %% Reload all of them
    ok = py:reload_context(Ctx, [<<"json">>, <<"os">>, <<"sys">>]).
