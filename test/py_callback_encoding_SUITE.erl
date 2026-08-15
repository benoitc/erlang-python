%%% @doc Common Test suite for the callback result encoding.
%%%
%%% Callback results cross from Erlang to Python as external term format and
%%% are decoded by term_to_py() in c_src/py_convert.c, the same converter that
%%% handles call arguments. These cases pin the resulting Python types and the
%%% round-trip fidelity, including payloads that the previous repr-string
%%% encoder corrupted.
-module(py_callback_encoding_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    end_per_testcase/2
]).

-export([
    test_binary_with_escapes/1,
    test_binary_non_utf8/1,
    test_large_binary/1,
    test_atom_becomes_str/1,
    test_empty_list/1,
    test_erlang_string_is_int_list/1,
    test_nested_containers/1,
    test_pid_round_trip/1,
    test_ref_round_trip/1,
    test_float_round_trip/1,
    test_booleans_and_none/1,
    test_python_types/1
]).

all() -> [
    test_binary_with_escapes,
    test_binary_non_utf8,
    test_large_binary,
    test_atom_becomes_str,
    test_empty_list,
    test_erlang_string_is_int_list,
    test_nested_containers,
    test_pid_round_trip,
    test_ref_round_trip,
    test_float_round_trip,
    test_booleans_and_none,
    test_python_types
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

end_per_testcase(_TestCase, _Config) ->
    catch py:unregister_function(cbenc_probe),
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Backslashes, both quote styles, newlines and tabs survive intact. The
%% repr encoder escaped only single quotes, so these payloads failed to parse
%% and were silently handed to Python as the raw repr text.
test_binary_with_escapes(_Config) ->
    Value = <<"back\\slash \"dq\" 'sq'\nnewline\ttab\r">>,
    Value = probe(Value),
    <<"str">> = probe_type(Value),
    ok.

%% @doc A binary that is not valid UTF-8 arrives as Python bytes and returns
%% byte-identical.
test_binary_non_utf8(_Config) ->
    Value = <<0, 1, 255, 254, 128>>,
    Value = probe(Value),
    <<"bytes">> = probe_type(Value),
    ok.

test_large_binary(_Config) ->
    Value = binary:copy(<<"abcdefghij">>, 20000),
    Value = probe(Value),
    ok.

%% @doc Atoms have no Python counterpart and arrive as str.
test_atom_becomes_str(_Config) ->
    <<"some_atom">> = probe(some_atom),
    <<"str">> = probe_type(some_atom),
    ok.

%% @doc The empty list is a list. The repr encoder classified it as a printable
%% string and produced '' instead.
test_empty_list(_Config) ->
    [] = probe([]),
    <<"list">> = probe_type([]),
    ok.

%% @doc An Erlang string is a list of integers, matching how call arguments
%% have always been converted. Return a binary for a Python str.
test_erlang_string_is_int_list(_Config) ->
    "abc" = probe("abc"),
    <<"list">> = probe_type("abc"),
    <<"abc">> = probe(<<"abc">>),
    <<"str">> = probe_type(<<"abc">>),
    ok.

test_nested_containers(_Config) ->
    Value = #{<<"k">> => [1, 2.5, {a, b}, #{<<"inner">> => [[], {}]}]},
    Expected = #{<<"k">> => [1, 2.5, {<<"a">>, <<"b">>}, #{<<"inner">> => [[], {}]}]},
    Expected = probe(Value),
    <<"dict">> = probe_type(Value),
    ok.

%% @doc Pids cross as native Pid objects, with no base64 marker round-trip.
test_pid_round_trip(_Config) ->
    Pid = self(),
    Pid = probe(Pid),
    <<"Pid">> = probe_type(Pid),
    ok.

test_ref_round_trip(_Config) ->
    Ref = make_ref(),
    Ref = probe(Ref),
    <<"Ref">> = probe_type(Ref),
    ok.

%% @doc Floats are exact, not routed through a decimal-formatted string.
test_float_round_trip(_Config) ->
    lists:foreach(fun(F) -> F = probe(F) end,
                  [3.14159265358979, 1.0e-300, 1.7976931348623157e308, -0.0]),
    ok.

test_booleans_and_none(_Config) ->
    true = probe(true),
    false = probe(false),
    <<"bool">> = probe_type(true),
    lists:foreach(fun(A) ->
        none = probe(A),
        <<"NoneType">> = probe_type(A)
    end, [undefined, nil, none]),
    ok.

%% @doc Tuples stay tuples and integers stay ints on the Python side.
test_python_types(_Config) ->
    <<"tuple">> = probe_type({1, 2}),
    <<"int">> = probe_type(42),
    <<"float">> = probe_type(1.5),
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

%% Return the value through a callback and back to Erlang.
probe(Value) ->
    py:register_function(cbenc_probe, fun(_) -> Value end),
    {ok, Got} = py:eval(<<"__import__('erlang').call('cbenc_probe', [])">>),
    py:unregister_function(cbenc_probe),
    Got.

%% Return the Python type name the callback result lands as.
probe_type(Value) ->
    py:register_function(cbenc_probe, fun(_) -> Value end),
    {ok, Type} = py:eval(
        <<"type(__import__('erlang').call('cbenc_probe', [])).__name__">>),
    py:unregister_function(cbenc_probe),
    Type.
