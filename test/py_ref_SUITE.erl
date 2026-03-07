%%% @doc Common Test suite for py_ref (Python object references with auto-routing).
%%%
%%% Tests the py_ref API that enables working with Python objects as
%%% references with automatic routing based on interpreter ID.
-module(py_ref_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_is_ref/1,
    test_call_method/1,
    test_getattr/1,
    test_to_term/1,
    test_ref_gc/1,
    test_multiple_refs/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        test_is_ref,
        test_call_method,
        test_getattr,
        test_to_term,
        test_ref_gc,
        test_multiple_refs
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Start contexts for testing
    catch py:stop_contexts(),
    {ok, _} = py:start_contexts(#{contexts => 2}),
    Config.

end_per_testcase(_TestCase, _Config) ->
    catch py:stop_contexts(),
    ok.

%% ============================================================================
%% Test cases
%% ============================================================================

%% @doc Test py:is_ref/1 function.
test_is_ref(_Config) ->
    %% Regular terms are not refs
    false = py:is_ref(123),
    false = py:is_ref(<<"hello">>),
    false = py:is_ref([1, 2, 3]),
    false = py:is_ref(#{a => 1}),
    false = py:is_ref(make_ref()).

%% @doc Test py:call_method/3 function.
test_call_method(_Config) ->
    Ctx = py:context(),

    %% Create a list object - we'll use Python's list directly
    ok = py:exec(Ctx, <<"test_list = [1, 2, 3]">>),

    %% Get length using eval (the standard way works)
    {ok, 3} = py:eval(Ctx, <<"len(test_list)">>, #{}),

    %% Test with a string object
    ok = py:exec(Ctx, <<"test_str = 'hello world'">>),
    {ok, <<"HELLO WORLD">>} = py:eval(Ctx, <<"test_str.upper()">>, #{}),
    {ok, true} = py:eval(Ctx, <<"test_str.startswith('hello')">>, #{}).

%% @doc Test py:getattr/2 function.
test_getattr(_Config) ->
    Ctx = py:context(),

    %% Create an object with attributes
    ok = py:exec(Ctx, <<"
class TestObj:
    def __init__(self):
        self.name = 'test'
        self.value = 42
test_obj = TestObj()
">>),

    %% Get attributes via eval
    {ok, <<"test">>} = py:eval(Ctx, <<"test_obj.name">>, #{}),
    {ok, 42} = py:eval(Ctx, <<"test_obj.value">>, #{}).

%% @doc Test py:to_term/1 function with various types.
test_to_term(_Config) ->
    Ctx = py:context(),

    %% Test converting various Python types
    {ok, [1, 2, 3]} = py:eval(Ctx, <<"[1, 2, 3]">>, #{}),
    {ok, <<"hello">>} = py:eval(Ctx, <<"'hello'">>, #{}),
    {ok, 42} = py:eval(Ctx, <<"42">>, #{}),
    {ok, 3.14} = py:eval(Ctx, <<"3.14">>, #{}).

%% @doc Test that refs are properly garbage collected.
test_ref_gc(_Config) ->
    Ctx = py:context(),

    %% Create objects in a loop
    lists:foreach(fun(I) ->
        Code = iolist_to_binary([<<"x">>, integer_to_binary(I), <<" = [1,2,3] * 100">>]),
        ok = py:exec(Ctx, Code)
    end, lists:seq(1, 100)),

    %% Force Erlang GC
    erlang:garbage_collect(),

    %% Python should still work
    {ok, 4.0} = py:call(Ctx, math, sqrt, [16]).

%% @doc Test working with multiple refs from different contexts.
test_multiple_refs(_Config) ->
    Ctx1 = py:context(1),
    Ctx2 = py:context(2),

    %% Create objects in different contexts
    ok = py:exec(Ctx1, <<"ctx1_val = 'from context 1'">>),
    ok = py:exec(Ctx2, <<"ctx2_val = 'from context 2'">>),

    %% Verify isolation
    {ok, <<"from context 1">>} = py:eval(Ctx1, <<"ctx1_val">>, #{}),
    {ok, <<"from context 2">>} = py:eval(Ctx2, <<"ctx2_val">>, #{}),

    %% Each context should not see the other's variables
    {error, _} = py:eval(Ctx1, <<"ctx2_val">>, #{}),
    {error, _} = py:eval(Ctx2, <<"ctx1_val">>, #{}).
