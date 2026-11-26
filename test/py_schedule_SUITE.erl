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
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "assert hasattr(erlang, 'schedule'), 'erlang.schedule not found'\n"
    >>),
    ct:pal("erlang.schedule is available"),
    ok.

%% Test that erlang.schedule_py is available
test_schedule_py_available(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "assert hasattr(erlang, 'schedule_py'), 'erlang.schedule_py not found'\n"
    >>),
    ct:pal("erlang.schedule_py is available"),
    ok.

%% Test that erlang.consume_time_slice is available
test_consume_time_slice_available(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "assert hasattr(erlang, 'consume_time_slice'), 'erlang.consume_time_slice not found'\n"
    >>),
    ct:pal("erlang.consume_time_slice is available"),
    ok.

%% Test that schedule() returns a ScheduleMarker
test_schedule_returns_marker(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "marker = erlang.schedule('_test_add', 1, 2)\n"
        "assert isinstance(marker, erlang.ScheduleMarker), f'Expected ScheduleMarker, got {type(marker)}'\n"
    >>),
    ct:pal("schedule() returns ScheduleMarker"),
    ok.

%% Test that schedule_py() returns a ScheduleMarker
test_schedule_py_returns_marker(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "marker = erlang.schedule_py('math', 'sqrt', [16.0])\n"
        "assert isinstance(marker, erlang.ScheduleMarker), f'Expected ScheduleMarker, got {type(marker)}'\n"
    >>),
    ct:pal("schedule_py() returns ScheduleMarker"),
    ok.

%% Test that consume_time_slice() returns bool
test_consume_time_slice_returns_bool(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "result = erlang.consume_time_slice(1)\n"
        "assert isinstance(result, bool), f'Expected bool, got {type(result)}'\n"
    >>),
    ct:pal("consume_time_slice() returns bool"),
    ok.

%% Test schedule() with a registered Erlang callback
test_schedule_with_callback(_Config) ->
    Ctx = py:context(),
    %% Define the function
    ok = py:exec(Ctx, <<
        "\n"
        "def schedule_add(a, b):\n"
        "    import erlang\n"
        "    return erlang.schedule('_test_add', a, b)\n"
    >>),
    %% Call it - the schedule marker should be detected and callback executed
    {ok, Result} = py:eval(Ctx, <<"schedule_add(5, 7)">>),
    ct:pal("schedule() result: ~p", [Result]),
    12 = Result,
    ok.

%% Test schedule_py() basic functionality
test_schedule_py_basic(_Config) ->
    Ctx = py:context(),
    %% Define the target function in __main__ so it's accessible via py:call
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def double(x):\n"
        "    return x * 2\n"
        "\n"
        "# Add to __main__ so it's accessible from schedule_py callback\n"
        "__main__.double = double\n"
        "\n"
        "def schedule_double(x):\n"
        "    import erlang\n"
        "    return erlang.schedule_py('__main__', 'double', [x])\n"
    >>),
    %% Call the scheduling function
    {ok, Result} = py:eval(Ctx, <<"schedule_double(5)">>),
    ct:pal("schedule_py() result: ~p", [Result]),
    10 = Result,
    ok.

%% Test schedule_py() with multiple args
test_schedule_py_with_args(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def add_three(a, b, c):\n"
        "    return a + b + c\n"
        "\n"
        "__main__.add_three = add_three\n"
        "\n"
        "def schedule_add_three(a, b, c):\n"
        "    import erlang\n"
        "    return erlang.schedule_py('__main__', 'add_three', [a, b, c])\n"
    >>),
    {ok, Result} = py:eval(Ctx, <<"schedule_add_three(1, 2, 3)">>),
    ct:pal("schedule_py() with args result: ~p", [Result]),
    6 = Result,
    ok.

%% Test schedule_py() with kwargs
test_schedule_py_with_kwargs(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def greet(name, prefix='Hello'):\n"
        "    return f'{prefix}, {name}!'\n"
        "\n"
        "__main__.greet = greet\n"
        "\n"
        "def schedule_greet(name, prefix):\n"
        "    import erlang\n"
        "    return erlang.schedule_py('__main__', 'greet', [name], {'prefix': prefix})\n"
    >>),
    {ok, Result} = py:eval(Ctx, <<"schedule_greet('World', 'Hi')">>),
    ct:pal("schedule_py() with kwargs result: ~p", [Result]),
    <<"Hi, World!">> = Result,
    ok.

%% Test that erlang.call() is now blocking (doesn't replay)
test_call_is_blocking(_Config) ->
    %% The original bug was that erlang.call() used replay mechanism which
    %% caused double-execution of code. With blocking mode, the call should
    %% only execute once even with timing-sensitive code.
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "import time\n"
        "\n"
        "counter = [0]  # Use list to avoid closure issues\n"
        "\n"
        "def test_call_once():\n"
        "    counter[0] += 1\n"
        "    erlang.call('_py_sleep', 0.05)  # 50ms sleep\n"
        "    return counter[0]\n"
        "\n"
        "result = test_call_once()\n"
        "assert result == 1, f'Expected 1, got {result} - call may have replayed'\n"
    >>),
    ct:pal("erlang.call() is blocking (no replay)"),
    ok.

%% ============================================================================
%% schedule_inline tests
%% ============================================================================

%% Test that erlang.schedule_inline is available
test_schedule_inline_available(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "assert hasattr(erlang, 'schedule_inline'), 'erlang.schedule_inline not found'\n"
    >>),
    ct:pal("erlang.schedule_inline is available"),
    ok.

%% Test that schedule_inline() returns an InlineScheduleMarker
test_schedule_inline_returns_marker(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "marker = erlang.schedule_inline('math', 'sqrt', args=[16.0])\n"
        "assert isinstance(marker, erlang.InlineScheduleMarker), f'Expected InlineScheduleMarker, got {type(marker)}'\n"
    >>),
    ct:pal("schedule_inline() returns InlineScheduleMarker"),
    ok.

%% Test schedule_inline() basic functionality - single continuation
test_schedule_inline_basic(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def inline_double(x):\n"
        "    return x * 2\n"
        "\n"
        "__main__.inline_double = inline_double\n"
        "\n"
        "def call_inline_double(x):\n"
        "    import erlang\n"
        "    return erlang.schedule_inline('__main__', 'inline_double', args=[x])\n"
    >>),
    {ok, Result} = py:eval(Ctx, <<"call_inline_double(21)">>),
    ct:pal("schedule_inline() basic result: ~p", [Result]),
    42 = Result,
    ok.

%% Test schedule_inline() with chained continuations
test_schedule_inline_chain(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def chain_step(n, acc):\n"
        "    import erlang\n"
        "    if n <= 0:\n"
        "        return acc\n"
        "    # Chain to next step via inline continuation\n"
        "    return erlang.schedule_inline('__main__', 'chain_step', args=[n - 1, acc + n])\n"
        "\n"
        "__main__.chain_step = chain_step\n"
    >>),
    %% Sum of 1 to 10 = 55
    {ok, Result} = py:eval(Ctx, <<"chain_step(10, 0)">>),
    ct:pal("schedule_inline() chain result: ~p", [Result]),
    55 = Result,
    ok.

%% Test schedule_inline() with multiple args
test_schedule_inline_with_args(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def inline_add(a, b, c):\n"
        "    return a + b + c\n"
        "\n"
        "__main__.inline_add = inline_add\n"
        "\n"
        "def call_inline_add(a, b, c):\n"
        "    import erlang\n"
        "    return erlang.schedule_inline('__main__', 'inline_add', args=[a, b, c])\n"
    >>),
    {ok, Result} = py:eval(Ctx, <<"call_inline_add(10, 20, 30)">>),
    ct:pal("schedule_inline() with args result: ~p", [Result]),
    60 = Result,
    ok.

%% Test schedule_inline() with kwargs
test_schedule_inline_with_kwargs(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def inline_greet(name, prefix='Hello'):\n"
        "    return f'{prefix}, {name}!'\n"
        "\n"
        "__main__.inline_greet = inline_greet\n"
        "\n"
        "def call_inline_greet(name, prefix):\n"
        "    import erlang\n"
        "    return erlang.schedule_inline('__main__', 'inline_greet', args=[name], kwargs={'prefix': prefix})\n"
    >>),
    {ok, Result} = py:eval(Ctx, <<"call_inline_greet('Erlang', 'Greetings')">>),
    ct:pal("schedule_inline() with kwargs result: ~p", [Result]),
    <<"Greetings, Erlang!">> = Result,
    ok.

%% Test schedule_inline transitioning to schedule_py (mixed schedule types)
test_schedule_inline_to_schedule_py(_Config) ->
    %% Start with schedule_inline, then transition to schedule_py
    %% Use explicit context to ensure consistent namespace
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "def step1(x):\n"
        "    import erlang\n"
        "    # First step uses inline continuation\n"
        "    return erlang.schedule_inline('__main__', 'step2', args=[x * 2])\n"
        "\n"
        "def step2(x):\n"
        "    import erlang\n"
        "    # Second step switches to schedule_py (goes through Erlang messaging)\n"
        "    return erlang.schedule_py('__main__', 'step3', [x + 10])\n"
        "\n"
        "def step3(x):\n"
        "    # Final step returns result\n"
        "    return x * 3\n"
        "\n"
        "__main__.step1 = step1\n"
        "__main__.step2 = step2\n"
        "__main__.step3 = step3\n"
    >>),
    %% (5 * 2 + 10) * 3 = 60
    {ok, Result} = py:eval(Ctx, <<"step1(5)">>),
    ct:pal("schedule_inline to schedule_py result: ~p", [Result]),
    60 = Result,
    ok.

%% Test schedule_inline error handling (function not found)
test_schedule_inline_error(_Config) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<
        "\n"
        "def call_nonexistent():\n"
        "    import erlang\n"
        "    return erlang.schedule_inline('__main__', 'nonexistent_function_xyz', args=[])\n"
    >>),
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
    ok = py:exec(Ctx, <<
        "\n"
        "import __main__\n"
        "\n"
        "# Define a helper function at module scope\n"
        "def helper_multiply(x):\n"
        "    return x * 3\n"
        "\n"
        "# Define an outer function that creates a local function reference\n"
        "# and uses schedule_inline to call it\n"
        "def test_captured_globals():\n"
        "    import erlang\n"
        "    # This should capture the current globals which includes helper_multiply\n"
        "    return erlang.schedule_inline('__main__', 'helper_multiply', args=[7])\n"
        "\n"
        "__main__.helper_multiply = helper_multiply\n"
        "__main__.test_captured_globals = test_captured_globals\n"
    >>),
    %% Call the function - should work because globals are captured
    {ok, Result} = py:eval(Ctx, <<"test_captured_globals()">>),
    ct:pal("schedule_inline captures globals result: ~p", [Result]),
    %% 7 * 3 = 21
    21 = Result,
    ok.
