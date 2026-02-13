%%% @doc Common Test suite for py module.
-module(py_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_call_math/1,
    test_call_json/1,
    test_eval/1,
    test_exec/1,
    test_async_call/1,
    test_type_conversions/1,
    test_timeout/1,
    test_special_floats/1,
    test_streaming/1,
    test_error_handling/1,
    test_version/1
]).

all() ->
    [
        test_call_math,
        test_call_json,
        test_eval,
        test_exec,
        test_async_call,
        test_type_conversions,
        test_timeout,
        test_special_floats,
        test_streaming,
        test_error_handling,
        test_version
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

test_call_math(_Config) ->
    {ok, 4.0} = py:call(math, sqrt, [16]),
    {ok, 120} = py:call(math, factorial, [5]),
    %% Test accessing module attribute via eval - use __import__
    {ok, Pi} = py:eval(<<"__import__('math').pi">>),
    true = Pi > 3.14 andalso Pi < 3.15,
    ok.

test_call_json(_Config) ->
    %% Encode
    {ok, Json} = py:call(json, dumps, [#{foo => bar}]),
    true = is_binary(Json),

    %% Decode
    {ok, Decoded} = py:call(json, loads, [<<"{\"a\": 1, \"b\": [1,2,3]}">>]),
    #{<<"a">> := 1, <<"b">> := [1, 2, 3]} = Decoded,
    ok.

test_eval(_Config) ->
    {ok, 45} = py:eval(<<"sum(range(10))">>),
    {ok, 25} = py:eval(<<"x * y">>, #{x => 5, y => 5}),
    {ok, [0, 1, 4, 9, 16]} = py:eval(<<"[x**2 for x in range(5)]">>),
    ok.

test_exec(_Config) ->
    %% Test exec - just ensure it doesn't fail
    %% Note: exec runs on any worker, subsequent calls may go to different workers
    ok = py:exec(<<"x = 42">>),
    ok = py:exec(<<"
def my_func():
    pass
">>),
    ok.

test_async_call(_Config) ->
    Ref1 = py:call_async(math, sqrt, [100]),
    Ref2 = py:call_async(math, sqrt, [144]),

    {ok, 10.0} = py:await(Ref1),
    {ok, 12.0} = py:await(Ref2),
    ok.

test_type_conversions(_Config) ->
    %% Integer
    {ok, 42} = py:eval(<<"42">>),

    %% Float
    {ok, 3.14} = py:eval(<<"3.14">>),

    %% String -> binary
    {ok, <<"hello">>} = py:eval(<<"'hello'">>),

    %% Boolean
    {ok, true} = py:eval(<<"True">>),
    {ok, false} = py:eval(<<"False">>),

    %% None
    {ok, none} = py:eval(<<"None">>),

    %% List
    {ok, [1, 2, 3]} = py:eval(<<"[1, 2, 3]">>),

    %% Tuple
    {ok, {1, 2, 3}} = py:eval(<<"(1, 2, 3)">>),

    %% Dict -> map
    {ok, #{<<"a">> := 1}} = py:eval(<<"{'a': 1}">>),

    ok.

test_timeout(_Config) ->
    %% Test that timeout works - use a heavy computation
    %% sum(range(10**8)) will trigger timeout
    {error, timeout} = py:eval(<<"sum(range(10**8))">>, #{}, 100),

    %% Test that normal operations complete within timeout
    {ok, 45} = py:eval(<<"sum(range(10))">>, #{}, 5000),

    %% Test call with timeout
    {ok, 4.0} = py:call(math, sqrt, [16], #{}, 5000),

    ok.

test_special_floats(_Config) ->
    %% Test NaN
    {ok, nan} = py:eval(<<"float('nan')">>),

    %% Test Infinity
    {ok, infinity} = py:eval(<<"float('inf')">>),

    %% Test negative infinity
    {ok, neg_infinity} = py:eval(<<"float('-inf')">>),

    ok.

test_streaming(_Config) ->
    %% Test generator expression streaming
    {ok, [0, 1, 4, 9, 16]} = py:stream_eval(<<"(x**2 for x in range(5))">>),

    %% Test range iterator
    {ok, [0, 1, 2, 3, 4]} = py:stream_eval(<<"iter(range(5))">>),

    %% Test map streaming
    {ok, [<<"A">>, <<"B">>, <<"C">>]} = py:stream_eval(<<"(c.upper() for c in 'abc')">>),

    ok.

test_error_handling(_Config) ->
    %% Test Python exception
    {error, {'NameError', _}} = py:eval(<<"undefined_variable">>),

    %% Test syntax error
    {error, {'SyntaxError', _}} = py:eval(<<"if True">>),

    %% Test division by zero
    {error, {'ZeroDivisionError', _}} = py:eval(<<"1/0">>),

    %% Test import error
    {error, {'ModuleNotFoundError', _}} = py:call(nonexistent_module, func, []),

    ok.

test_version(_Config) ->
    {ok, Version} = py:version(),
    true = is_binary(Version),
    true = byte_size(Version) > 0,
    ok.
