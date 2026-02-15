%%% @doc Reentrant callback demonstration.
%%%
%%% This module demonstrates transparent Python ↔ Erlang reentrant callbacks.
%%% The suspension/resume mechanism is invisible to both Python and Erlang code.
%%%
%%% Usage:
%%%   1> reentrant_demo:start().
%%%   2> reentrant_demo:demo_basic().
%%%   3> reentrant_demo:demo_fibonacci().
%%%   4> reentrant_demo:demo_nested().
%%%   5> reentrant_demo:demo_processor().
-module(reentrant_demo).

-export([
    start/0,
    stop/0,
    demo_basic/0,
    demo_fibonacci/0,
    demo_nested/0,
    demo_processor/0,
    demo_all/0
]).

%% @doc Start the application and register callback functions.
start() ->
    {ok, _} = application:ensure_all_started(erlang_python),

    %% Load our Python demo module
    {ok, _} = py:exec(<<"
import sys
sys.path.insert(0, 'examples')
import reentrant_demo
">>),

    %% Register Erlang functions that Python can call
    register_callbacks(),

    io:format("Reentrant demo started. Try:~n"),
    io:format("  reentrant_demo:demo_basic().~n"),
    io:format("  reentrant_demo:demo_fibonacci().~n"),
    io:format("  reentrant_demo:demo_nested().~n"),
    io:format("  reentrant_demo:demo_processor().~n"),
    io:format("  reentrant_demo:demo_all().~n"),
    ok.

%% @doc Stop the application.
stop() ->
    application:stop(erlang_python).

%% @doc Register all callback functions.
register_callbacks() ->
    %% Basic: Erlang function that calls Python's double()
    py:register_function(double_via_python, fun([X]) ->
        {ok, Result} = py:call(reentrant_demo, double, [X]),
        Result
    end),

    %% Fibonacci with memoization
    py:register_function(fib_cache, fun([N]) ->
        %% Check cache (using process dictionary for simplicity)
        case get({fib, N}) of
            undefined ->
                %% Not cached - compute by calling Python's fibonacci
                %% This creates: Python -> Erlang -> Python -> Erlang -> ...
                {ok, A} = py:call(reentrant_demo, fibonacci, [N - 1]),
                {ok, B} = py:call(reentrant_demo, fibonacci, [N - 2]),
                Result = A + B,
                put({fib, N}, Result),
                Result;
            Cached ->
                Cached
        end
    end),

    %% Nested computation step
    py:register_function(nested_step, fun([N, Depth]) ->
        %% Call back into Python with reduced depth
        {ok, Result} = py:call(reentrant_demo, nested_compute, [N + 1, Depth - 1]),
        Result
    end),

    %% Item validation that calls back to Python
    py:register_function(validate_and_transform, fun([Item]) ->
        {ok, Validated} = py:call(reentrant_demo, validate_item, [Item]),
        case Validated of
            #{<<"valid">> := true, <<"data">> := Data} ->
                %% Transform valid data
                {ok, Transformed} = py:call(reentrant_demo, transform_data, [Data]),
                Transformed;
            #{<<"valid">> := false} = Error ->
                Error
        end
    end),

    %% Rules checking (simple implementation)
    py:register_function(check_rules, fun([Type]) ->
        %% Simulate rules database lookup
        ValidTypes = [<<"user">>, <<"product">>, <<"order">>],
        lists:member(Type, ValidTypes)
    end),

    %% Multiplier lookup
    py:register_function(get_multiplier, fun([Type]) ->
        case Type of
            <<"premium">> -> 3;
            <<"standard">> -> 2;
            _ -> 1
        end
    end),

    %% Processor handler
    py:register_function(processor_handle, fun([Name, Value]) ->
        %% Call back to the processor's compute method
        {ok, Result} = py:eval(iolist_to_binary(io_lib:format(
            "reentrant_demo.DataProcessor('~s').compute(~p, 100)", [Name, Value]))),
        Result
    end),

    %% Factor lookup for processor
    py:register_function(get_factor, fun([_Name]) ->
        2  % Simple factor
    end),

    ok.

%% @doc Demonstrate basic reentrant callback.
demo_basic() ->
    io:format("~n=== Basic Reentrant Demo ===~n"),
    io:format("Calling Python's add_one(10) which calls Erlang's double_via_python~n"),
    io:format("which calls Python's double(), then adds 1.~n~n"),

    %% Python: add_one(10) -> erlang.call('double_via_python', 10) + 1
    %% Erlang: double_via_python -> py:call(double, [10])
    %% Python: double(10) -> 20
    %% Back to Python: 20 + 1 = 21
    {ok, Result} = py:call(reentrant_demo, add_one, [10]),

    io:format("Flow: Python -> Erlang -> Python -> Erlang (result)~n"),
    io:format("Result: ~p (expected: 21)~n", [Result]),
    io:format("Test ~s~n", [if Result == 21 -> "PASSED"; true -> "FAILED" end]),
    Result.

%% @doc Demonstrate Fibonacci with reentrant memoization.
demo_fibonacci() ->
    io:format("~n=== Fibonacci Reentrant Demo ===~n"),
    io:format("Computing Fibonacci using Python with Erlang memoization.~n"),
    io:format("Each recursive call goes: Python -> Erlang -> Python~n~n"),

    %% Clear the cache
    erase(),

    {ok, Fib10} = py:call(reentrant_demo, fibonacci, [10]),
    io:format("fibonacci(10) = ~p (expected: 55)~n", [Fib10]),

    {ok, Fib15} = py:call(reentrant_demo, fibonacci, [15]),
    io:format("fibonacci(15) = ~p (expected: 610)~n", [Fib15]),

    io:format("Test ~s~n", [if Fib10 == 55, Fib15 == 610 -> "PASSED"; true -> "FAILED" end]),
    {Fib10, Fib15}.

%% @doc Demonstrate deeply nested callbacks.
demo_nested() ->
    io:format("~n=== Nested Callbacks Demo ===~n"),
    io:format("Testing deep nesting: Python -> Erlang -> Python -> ...~n~n"),

    %% nested_compute(0, 5) will create 5 levels of nesting
    %% Each level: Python calls Erlang, Erlang calls Python
    {ok, Result5} = py:call(reentrant_demo, nested_compute, [0, 5]),
    io:format("nested_compute(0, 5) = ~p (expected: 5, depth levels traversed)~n", [Result5]),

    {ok, Result10} = py:call(reentrant_demo, nested_compute, [0, 10]),
    io:format("nested_compute(0, 10) = ~p (expected: 10)~n", [Result10]),

    io:format("Test ~s~n", [if Result5 == 5, Result10 == 10 -> "PASSED"; true -> "FAILED" end]),
    {Result5, Result10}.

%% @doc Demonstrate OOP-style reentrant callbacks.
demo_processor() ->
    io:format("~n=== DataProcessor Demo ===~n"),
    io:format("Using a Python class with methods called from Erlang.~n~n"),

    %% Create a processor and call its process method
    %% This triggers: Python.process -> Erlang.processor_handle -> Python.compute -> Erlang.get_factor
    {ok, Result} = py:eval(<<"reentrant_demo.DataProcessor('test').process(42)">>),

    io:format("DataProcessor('test').process(42) = ~p~n", [Result]),
    io:format("Expected: processor='test', result=(42+100)*2=284~n"),

    #{<<"result">> := ComputeResult} = Result,
    io:format("Test ~s~n", [if ComputeResult == 284 -> "PASSED"; true -> "FAILED" end]),
    Result.

%% @doc Run all demos.
demo_all() ->
    io:format("~n========================================~n"),
    io:format("   REENTRANT CALLBACKS DEMONSTRATION~n"),
    io:format("========================================~n"),

    demo_basic(),
    demo_fibonacci(),
    demo_nested(),
    demo_processor(),

    io:format("~n========================================~n"),
    io:format("   ALL DEMOS COMPLETE~n"),
    io:format("========================================~n"),
    ok.
