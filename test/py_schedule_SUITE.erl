%% @doc Tests for erlang.schedule(), schedule_py(), schedule_inline() and consume_time_slice().
%%
%% Tests explicit scheduling API for cooperative dirty scheduler release.
-module(py_schedule_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    test_schedule_available/1,
    test_schedule_py_available/1,
    test_schedule_inline_available/1,
    test_consume_time_slice_available/1,
    test_schedule_returns_marker/1,
    test_schedule_py_returns_marker/1,
    test_schedule_inline_returns_marker/1,
    test_consume_time_slice_returns_bool/1,
    test_schedule_with_callback/1,
    test_schedule_py_basic/1,
    test_schedule_py_with_args/1,
    test_schedule_py_with_kwargs/1,
    test_schedule_inline_basic/1,
    test_schedule_inline_chain/1,
    test_schedule_inline_with_args/1,
    test_schedule_inline_with_kwargs/1,
    test_schedule_inline_to_schedule_py/1,
    test_schedule_inline_error/1,
    test_schedule_inline_captures_globals/1,
    test_call_is_blocking/1
]).

all() ->
    [
        test_schedule_available,
        test_schedule_py_available,
        test_schedule_inline_available,
        test_consume_time_slice_available,
        test_schedule_returns_marker,
        test_schedule_py_returns_marker,
        test_schedule_inline_returns_marker,
        test_consume_time_slice_returns_bool,
        test_schedule_with_callback,
        test_schedule_py_basic,
        test_schedule_py_with_args,
        test_schedule_py_with_kwargs,
        test_schedule_inline_basic,
        test_schedule_inline_chain,
        test_schedule_inline_with_args,
        test_schedule_inline_with_kwargs,
        test_schedule_inline_to_schedule_py,
        test_schedule_inline_error,
        test_schedule_inline_captures_globals,
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
    Ctx = py:context(),
    %% Define the function
    ok = py:exec(Ctx, <<"
def schedule_add(a, b):
    import erlang
    return erlang.schedule('_test_add', a, b)
">>),
    %% Call it - the schedule marker should be detected and callback executed
    {ok, Result} = py:eval(Ctx, <<"schedule_add(5, 7)">>),
    ct:pal("schedule() result: ~p", [Result]),
    12 = Result,
    ok.

%% Test schedule_py() basic functionality
test_schedule_py_basic(_Config) ->
    Ctx = py:context(),
    %% Define the target function in __main__ so it's accessible via py:call
    ok = py:exec(Ctx, <<"
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
    {ok, Result} = py:eval(Ctx, <<"schedule_double(5)">>),
    ct:pal("schedule_py() result: ~p", [Result]),
    10 = Result,
    ok.

%% Test schedule_py() with multiple args
test_schedule_py_with_args(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

def add_three(a, b, c):
    return a + b + c

__main__.add_three = add_three

def schedule_add_three(a, b, c):
    import erlang
    return erlang.schedule_py('__main__', 'add_three', [a, b, c])
">>),
    {ok, Result} = py:eval(Ctx, <<"schedule_add_three(1, 2, 3)">>),
    ct:pal("schedule_py() with args result: ~p", [Result]),
    6 = Result,
    ok.

%% Test schedule_py() with kwargs
test_schedule_py_with_kwargs(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

def greet(name, prefix='Hello'):
    return f'{prefix}, {name}!'

__main__.greet = greet

def schedule_greet(name, prefix):
    import erlang
    return erlang.schedule_py('__main__', 'greet', [name], {'prefix': prefix})
">>),
    {ok, Result} = py:eval(Ctx, <<"schedule_greet('World', 'Hi')">>),
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

%% ============================================================================
%% schedule_inline tests
%% ============================================================================

%% Test that erlang.schedule_inline is available
test_schedule_inline_available(_Config) ->
    ok = py:exec(<<"
import erlang
assert hasattr(erlang, 'schedule_inline'), 'erlang.schedule_inline not found'
">>),
    ct:pal("erlang.schedule_inline is available"),
    ok.

%% Test that schedule_inline() returns an InlineScheduleMarker
test_schedule_inline_returns_marker(_Config) ->
    ok = py:exec(<<"
import erlang
marker = erlang.schedule_inline('math', 'sqrt', args=[16.0])
assert isinstance(marker, erlang.InlineScheduleMarker), f'Expected InlineScheduleMarker, got {type(marker)}'
">>),
    ct:pal("schedule_inline() returns InlineScheduleMarker"),
    ok.

%% Test schedule_inline() basic functionality - single continuation
test_schedule_inline_basic(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

def inline_double(x):
    return x * 2

__main__.inline_double = inline_double

def call_inline_double(x):
    import erlang
    return erlang.schedule_inline('__main__', 'inline_double', args=[x])
">>),
    {ok, Result} = py:eval(Ctx, <<"call_inline_double(21)">>),
    ct:pal("schedule_inline() basic result: ~p", [Result]),
    42 = Result,
    ok.

%% Test schedule_inline() with chained continuations
test_schedule_inline_chain(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

def chain_step(n, acc):
    import erlang
    if n <= 0:
        return acc
    # Chain to next step via inline continuation
    return erlang.schedule_inline('__main__', 'chain_step', args=[n - 1, acc + n])

__main__.chain_step = chain_step
">>),
    %% Sum of 1 to 10 = 55
    {ok, Result} = py:eval(Ctx, <<"chain_step(10, 0)">>),
    ct:pal("schedule_inline() chain result: ~p", [Result]),
    55 = Result,
    ok.

%% Test schedule_inline() with multiple args
test_schedule_inline_with_args(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

def inline_add(a, b, c):
    return a + b + c

__main__.inline_add = inline_add

def call_inline_add(a, b, c):
    import erlang
    return erlang.schedule_inline('__main__', 'inline_add', args=[a, b, c])
">>),
    {ok, Result} = py:eval(Ctx, <<"call_inline_add(10, 20, 30)">>),
    ct:pal("schedule_inline() with args result: ~p", [Result]),
    60 = Result,
    ok.

%% Test schedule_inline() with kwargs
test_schedule_inline_with_kwargs(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

def inline_greet(name, prefix='Hello'):
    return f'{prefix}, {name}!'

__main__.inline_greet = inline_greet

def call_inline_greet(name, prefix):
    import erlang
    return erlang.schedule_inline('__main__', 'inline_greet', args=[name], kwargs={'prefix': prefix})
">>),
    {ok, Result} = py:eval(Ctx, <<"call_inline_greet('Erlang', 'Greetings')">>),
    ct:pal("schedule_inline() with kwargs result: ~p", [Result]),
    <<"Greetings, Erlang!">> = Result,
    ok.

%% Test schedule_inline transitioning to schedule_py (mixed schedule types)
test_schedule_inline_to_schedule_py(_Config) ->
    %% Start with schedule_inline, then transition to schedule_py
    %% Use explicit context to ensure consistent namespace
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

def step1(x):
    import erlang
    # First step uses inline continuation
    return erlang.schedule_inline('__main__', 'step2', args=[x * 2])

def step2(x):
    import erlang
    # Second step switches to schedule_py (goes through Erlang messaging)
    return erlang.schedule_py('__main__', 'step3', [x + 10])

def step3(x):
    # Final step returns result
    return x * 3

__main__.step1 = step1
__main__.step2 = step2
__main__.step3 = step3
">>),
    %% (5 * 2 + 10) * 3 = 60
    {ok, Result} = py:eval(Ctx, <<"step1(5)">>),
    ct:pal("schedule_inline to schedule_py result: ~p", [Result]),
    60 = Result,
    ok.

%% Test schedule_inline error handling (function not found)
test_schedule_inline_error(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
def call_nonexistent():
    import erlang
    return erlang.schedule_inline('__main__', 'nonexistent_function_xyz', args=[])
">>),
    {error, Reason} = py:eval(Ctx, <<"call_nonexistent()">>),
    ct:pal("schedule_inline error: ~p", [Reason]),
    %% Should get a NameError - error comes as {ErrorType, Message} tuple or binary
    case Reason of
        {'NameError', _Msg} -> ok;
        _ when is_binary(Reason) -> ok;
        _ when is_list(Reason) -> ok;
        _ -> ct:fail("Unexpected error format: ~p", [Reason])
    end,
    ok.

%% Test that schedule_inline captures globals from caller's frame
test_schedule_inline_captures_globals(_Config) ->
    %% This test verifies that schedule_inline captures the caller's frame
    %% globals and uses them for function lookup in the continuation.
    %% This is important for subinterpreter support.
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__

# Define a helper function at module scope
def helper_multiply(x):
    return x * 3

# Define an outer function that creates a local function reference
# and uses schedule_inline to call it
def test_captured_globals():
    import erlang
    # This should capture the current globals which includes helper_multiply
    return erlang.schedule_inline('__main__', 'helper_multiply', args=[7])

__main__.helper_multiply = helper_multiply
__main__.test_captured_globals = test_captured_globals
">>),
    %% Call the function - should work because globals are captured
    {ok, Result} = py:eval(Ctx, <<"test_captured_globals()">>),
    ct:pal("schedule_inline captures globals result: ~p", [Result]),
    21 = Result,  %% 7 * 3 = 21
    ok.
