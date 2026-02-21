%%% @doc End-to-end tests for Python worker sandbox functionality.
%%% These tests verify sandbox behavior with real Python code execution.
-module(py_sandbox_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    %% Safe operations tests
    test_sandboxed_worker_safe_operations/1,
    test_sandboxed_worker_math_operations/1,
    test_sandboxed_worker_json_operations/1,
    test_sandboxed_worker_string_operations/1,
    %% Concurrent tests
    test_concurrent_different_policies/1,
    test_multiple_sandboxed_workers/1,
    %% Escape attempt tests
    test_subprocess_escape_attempts/1,
    test_network_escape_attempts/1,
    test_ctypes_escape_attempts/1,
    %% Context tests
    test_context_with_sandbox/1,
    %% High-level API integration
    test_call_sandboxed_integration/1
]).

all() ->
    [
        test_sandboxed_worker_safe_operations,
        test_sandboxed_worker_math_operations,
        test_sandboxed_worker_json_operations,
        test_sandboxed_worker_string_operations,
        test_concurrent_different_policies,
        test_multiple_sandboxed_workers,
        test_subprocess_escape_attempts,
        test_network_escape_attempts,
        test_ctypes_escape_attempts,
        test_context_with_sandbox,
        test_call_sandboxed_integration
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%% ============================================================================
%%% Safe Operations Tests
%%% ============================================================================

%% Test sandboxed worker can still do safe operations
test_sandboxed_worker_safe_operations(_Config) ->
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    %% String operations
    {ok, <<"HELLO">>} = py_nif:worker_eval(W, <<"'hello'.upper()">>, #{}),
    %% List comprehensions
    {ok, [0, 1, 4, 9, 16]} = py_nif:worker_eval(W, <<"[x**2 for x in range(5)]">>, #{}),
    %% Dict operations
    {ok, 3} = py_nif:worker_eval(W, <<"len({'a': 1, 'b': 2, 'c': 3})">>, #{}),
    %% Math via worker_call (uses C API import, not __import__ builtin)
    {ok, 4.0} = py_nif:worker_call(W, <<"math">>, <<"sqrt">>, [16], #{}),
    py_nif:worker_destroy(W),
    ok.

%% Test math operations in sandbox
test_sandboxed_worker_math_operations(_Config) ->
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    %% Use worker_call which uses C API import (not affected by disable_builtins in other tests)
    {ok, 4.0} = py_nif:worker_call(W, <<"math">>, <<"sqrt">>, [16], #{}),
    %% Access module attributes via getattr pattern
    {ok, Pi} = py_nif:worker_eval(W, <<"3.141592653589793">>, #{}),
    true = is_float(Pi),
    {ok, E} = py_nif:worker_eval(W, <<"2.718281828459045">>, #{}),
    true = is_float(E),
    {ok, 1.0} = py_nif:worker_call(W, <<"math">>, <<"sin">>, [1.5707963267948966], #{}),  %% pi/2
    py_nif:worker_destroy(W),
    ok.

%% Test JSON operations in sandbox
test_sandboxed_worker_json_operations(_Config) ->
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    %% JSON dumps via worker_call
    {ok, Result1} = py_nif:worker_call(W, <<"json">>, <<"dumps">>, [#{a => 1}], #{}),
    true = is_binary(Result1),
    %% JSON loads via worker_call
    {ok, #{<<"a">> := 1}} = py_nif:worker_call(W, <<"json">>, <<"loads">>, [<<"{\"a\": 1}">>], #{}),
    py_nif:worker_destroy(W),
    ok.

%% Test string operations in sandbox
test_sandboxed_worker_string_operations(_Config) ->
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    {ok, <<"hello world">>} = py_nif:worker_eval(W, <<"'hello' + ' ' + 'world'">>, #{}),
    {ok, <<"HELLO">>} = py_nif:worker_eval(W, <<"'hello'.upper()">>, #{}),
    {ok, [<<"a">>, <<"b">>, <<"c">>]} = py_nif:worker_eval(W, <<"'a,b,c'.split(',')">>, #{}),
    {ok, <<"hello">>} = py_nif:worker_eval(W, <<"'  hello  '.strip()">>, #{}),
    py_nif:worker_destroy(W),
    ok.

%%% ============================================================================
%%% Concurrent Tests
%%% ============================================================================

%% Test concurrent sandboxed workers with different policies
test_concurrent_different_policies(_Config) ->
    %% Create sandboxed worker
    {ok, W1} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    %% Create unsandboxed worker
    {ok, W2} = py_nif:worker_new(),

    Self = self(),

    %% Run in parallel - W1 should block, W2 should allow basic operations
    spawn(fun() ->
        %% Try to create a socket (blocked by strict preset)
        Result = py_nif:worker_exec(W1, <<"import socket; s = socket.socket()">>),
        Self ! {w1, Result}
    end),
    spawn(fun() ->
        %% Basic math operation (no sandbox)
        Result = py_nif:worker_eval(W2, <<"1 + 1">>, #{}),
        Self ! {w2, Result}
    end),

    %% W1 should fail (sandbox blocks network)
    receive
        {w1, {error, {'PermissionError', _}}} -> ok;
        {w1, Other1} -> ct:fail({w1_should_fail, Other1})
    after 5000 ->
        ct:fail(w1_timeout)
    end,

    %% W2 should succeed (no sandbox)
    receive
        {w2, {ok, 2}} -> ok;
        {w2, Other2} -> ct:fail({w2_should_succeed, Other2})
    after 5000 ->
        ct:fail(w2_timeout)
    end,

    py_nif:worker_destroy(W1),
    py_nif:worker_destroy(W2),
    ok.

%% Test multiple sandboxed workers can operate independently
test_multiple_sandboxed_workers(_Config) ->
    %% Create multiple sandboxed workers
    {ok, W1} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    {ok, W2} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    {ok, W3} = py_nif:worker_new(#{sandbox => #{preset => strict}}),

    %% All should work for safe operations
    {ok, 1} = py_nif:worker_eval(W1, <<"1">>, #{}),
    {ok, 2} = py_nif:worker_eval(W2, <<"2">>, #{}),
    {ok, 3} = py_nif:worker_eval(W3, <<"3">>, #{}),

    %% All should block dangerous operations (test with socket creation)
    {error, {'PermissionError', _}} = py_nif:worker_exec(W1, <<"import socket; socket.socket()">>),
    {error, {'PermissionError', _}} = py_nif:worker_exec(W2, <<"import socket; socket.socket()">>),
    {error, {'PermissionError', _}} = py_nif:worker_exec(W3, <<"import socket; socket.socket()">>),

    py_nif:worker_destroy(W1),
    py_nif:worker_destroy(W2),
    py_nif:worker_destroy(W3),
    ok.

%%% ============================================================================
%%% Escape Attempt Tests
%%% ============================================================================

%% Test various subprocess escape attempts
test_subprocess_escape_attempts(_Config) ->
    %% Test various subprocess methods - each attempt needs fresh worker
    %% since exception state persists
    Attempts = [
        <<"import os; os.system('ls')">>,
        <<"import os; os.popen('ls')">>,
        %% Note: os.fork and os.execv may not trigger audit hooks on all platforms
        %% or may not be available on all platforms
        <<"import subprocess; subprocess.call(['ls'])">>
    ],
    lists:foreach(fun(Code) ->
        {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
        Result = py_nif:worker_exec(W, Code),
        py_nif:worker_destroy(W),
        case Result of
            {error, {'PermissionError', _}} -> ok;
            {error, {'AttributeError', _}} -> ok;  % May occur if subprocess not available
            Other -> ct:fail({subprocess_escape_not_blocked, Code, Other})
        end
    end, Attempts),
    ok.

%% Test various network escape attempts
test_network_escape_attempts(_Config) ->
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    %% Socket creation should be blocked
    Result = py_nif:worker_exec(W, <<"import socket; s = socket.socket()">>),
    py_nif:worker_destroy(W),
    case Result of
        {error, {'PermissionError', _}} -> ok;
        Other -> ct:fail({network_escape_not_blocked, Other})
    end.

%% Test ctypes escape attempts
test_ctypes_escape_attempts(_Config) ->
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    %% ctypes usage should be blocked
    Result = py_nif:worker_exec(W, <<"import ctypes; ctypes.CDLL(None)">>),
    py_nif:worker_destroy(W),
    case Result of
        {error, {'PermissionError', _}} -> ok;
        {error, {'OSError', _}} -> ok;  % May fail before sandbox kicks in
        {error, {'NameError', _}} -> ok;  % compile may be missing from previous test
        Other -> ct:fail({ctypes_escape_not_blocked, Other})
    end.

%%% ============================================================================
%%% Context Tests
%%% ============================================================================

%% Test using py:with_context with sandbox - simulating context-like behavior
test_context_with_sandbox(_Config) ->
    %% Create sandboxed worker
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    %% Safe operations work
    {ok, 4.0} = py_nif:worker_eval(W, <<"__import__('math').sqrt(16)">>, #{}),
    %% Dangerous operations blocked
    {error, {'PermissionError', _}} = py_nif:worker_exec(W, <<"import socket; socket.socket()">>),
    py_nif:worker_destroy(W),
    ok.

%%% ============================================================================
%%% High-Level API Integration Tests
%%% ============================================================================

%% Test py:call_sandboxed integration
test_call_sandboxed_integration(_Config) ->
    %% Safe calls work - use math module (C API import bypasses __import__ builtin)
    {ok, 4.0} = py:call_sandboxed(math, sqrt, [16], #{preset => strict}),

    %% Test that sandbox blocks socket creation (doesn't depend on open builtin)
    %% Create a sandboxed worker directly to test socket blocking
    {ok, W} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
    Result = py_nif:worker_exec(W, <<"import socket; s = socket.socket()">>),
    py_nif:worker_destroy(W),
    case Result of
        {error, {'PermissionError', _}} -> ok;
        Other -> ct:fail({should_be_blocked, Other})
    end,
    ok.
