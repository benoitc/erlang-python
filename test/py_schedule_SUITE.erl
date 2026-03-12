%% @doc Tests for erlang.schedule(), schedule_py(), and consume_time_slice().
%%
%% Tests explicit scheduling API for cooperative dirty scheduler release.
-module(py_schedule_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    test_schedule_available/1,
    test_schedule_py_available/1,
    test_consume_time_slice_available/1,
    test_schedule_returns_marker/1,
    test_schedule_py_returns_marker/1,
    test_consume_time_slice_returns_bool/1,
    test_schedule_with_callback/1,
    test_schedule_py_basic/1,
    test_schedule_py_with_args/1,
    test_schedule_py_with_kwargs/1,
    test_call_is_blocking/1
]).

all() ->
    [
        test_schedule_available,
        test_schedule_py_available,
        test_consume_time_slice_available,
        test_schedule_returns_marker,
        test_schedule_py_returns_marker,
        test_consume_time_slice_returns_bool,
        test_schedule_with_callback,
        test_schedule_py_basic,
        test_schedule_py_with_args,
        test_schedule_py_with_kwargs,
        test_call_is_blocking
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    %% Bind to a specific context to ensure exec/eval use the same namespace
    %% (scheduler-based routing may use different contexts on some platforms)
    Ctx = py_context_router:get_context(1),
    py_context_router:bind_context(Ctx),
    %% Register a test callback for schedule() tests
    py_callback:register(<<"_test_add">>, fun([A, B]) -> A + B end),
    py_callback:register(<<"_test_mul">>, fun([A, B]) -> A * B end),
    py_callback:register(<<"_test_echo">>, fun(Args) -> Args end),
    timer:sleep(500),
    Config.

end_per_suite(_Config) ->
    py_context_router:unbind_context(),
    py_callback:unregister(<<"_test_add">>),
    py_callback:unregister(<<"_test_mul">>),
    py_callback:unregister(<<"_test_echo">>),
    ok.

%% Test that erlang.schedule is available
test_schedule_available(_Config) ->
    ok = py:exec(<<"
import erlang
assert hasattr(erlang, 'schedule'), 'erlang.schedule not found'
">>),
    ct:pal("erlang.schedule is available"),
    ok.

%% Test that erlang.schedule_py is available
test_schedule_py_available(_Config) ->
    ok = py:exec(<<"
import erlang
assert hasattr(erlang, 'schedule_py'), 'erlang.schedule_py not found'
">>),
    ct:pal("erlang.schedule_py is available"),
    ok.

%% Test that erlang.consume_time_slice is available
test_consume_time_slice_available(_Config) ->
    ok = py:exec(<<"
import erlang
assert hasattr(erlang, 'consume_time_slice'), 'erlang.consume_time_slice not found'
">>),
    ct:pal("erlang.consume_time_slice is available"),
    ok.

%% Test that schedule() returns a ScheduleMarker
test_schedule_returns_marker(_Config) ->
    ok = py:exec(<<"
import erlang
marker = erlang.schedule('_test_add', 1, 2)
assert isinstance(marker, erlang.ScheduleMarker), f'Expected ScheduleMarker, got {type(marker)}'
">>),
    ct:pal("schedule() returns ScheduleMarker"),
    ok.

%% Test that schedule_py() returns a ScheduleMarker
test_schedule_py_returns_marker(_Config) ->
    ok = py:exec(<<"
import erlang
marker = erlang.schedule_py('math', 'sqrt', [16.0])
assert isinstance(marker, erlang.ScheduleMarker), f'Expected ScheduleMarker, got {type(marker)}'
">>),
    ct:pal("schedule_py() returns ScheduleMarker"),
    ok.

%% Test that consume_time_slice() returns bool
test_consume_time_slice_returns_bool(_Config) ->
    ok = py:exec(<<"
import erlang
result = erlang.consume_time_slice(1)
assert isinstance(result, bool), f'Expected bool, got {type(result)}'
">>),
    ct:pal("consume_time_slice() returns bool"),
    ok.

%% Test schedule() with a registered Erlang callback
test_schedule_with_callback(_Config) ->
    %% Define the function
    ok = py:exec(<<"
def schedule_add(a, b):
    import erlang
    return erlang.schedule('_test_add', a, b)
">>),
    %% Call it - the schedule marker should be detected and callback executed
    {ok, Result} = py:eval(<<"schedule_add(5, 7)">>),
    ct:pal("schedule() result: ~p", [Result]),
    12 = Result,
    ok.

%% Test schedule_py() basic functionality
test_schedule_py_basic(_Config) ->
    %% Define the target function in __main__ so it's accessible via py:call
    ok = py:exec(<<"
import __main__

def double(x):
    return x * 2

# Add to __main__ so it's accessible from schedule_py callback
__main__.double = double

def schedule_double(x):
    import erlang
    return erlang.schedule_py('__main__', 'double', [x])
">>),
    %% Call the scheduling function
    {ok, Result} = py:eval(<<"schedule_double(5)">>),
    ct:pal("schedule_py() result: ~p", [Result]),
    10 = Result,
    ok.

%% Test schedule_py() with multiple args
test_schedule_py_with_args(_Config) ->
    ok = py:exec(<<"
import __main__

def add_three(a, b, c):
    return a + b + c

__main__.add_three = add_three

def schedule_add_three(a, b, c):
    import erlang
    return erlang.schedule_py('__main__', 'add_three', [a, b, c])
">>),
    {ok, Result} = py:eval(<<"schedule_add_three(1, 2, 3)">>),
    ct:pal("schedule_py() with args result: ~p", [Result]),
    6 = Result,
    ok.

%% Test schedule_py() with kwargs
test_schedule_py_with_kwargs(_Config) ->
    ok = py:exec(<<"
import __main__

def greet(name, prefix='Hello'):
    return f'{prefix}, {name}!'

__main__.greet = greet

def schedule_greet(name, prefix):
    import erlang
    return erlang.schedule_py('__main__', 'greet', [name], {'prefix': prefix})
">>),
    {ok, Result} = py:eval(<<"schedule_greet('World', 'Hi')">>),
    ct:pal("schedule_py() with kwargs result: ~p", [Result]),
    <<"Hi, World!">> = Result,
    ok.

%% Test that erlang.call() is now blocking (doesn't replay)
test_call_is_blocking(_Config) ->
    %% The original bug was that erlang.call() used replay mechanism which
    %% caused double-execution of code. With blocking mode, the call should
    %% only execute once even with timing-sensitive code.
    ok = py:exec(<<"
import erlang
import time

counter = [0]  # Use list to avoid closure issues

def test_call_once():
    counter[0] += 1
    erlang.call('_py_sleep', 0.05)  # 50ms sleep
    return counter[0]

result = test_call_once()
assert result == 1, f'Expected 1, got {result} - call may have replayed'
">>),
    ct:pal("erlang.call() is blocking (no replay)"),
    ok.
