%% @doc Example demonstrating reentrant Python→Erlang→Python callbacks.
%%
%% This example shows how Python code can call Erlang functions that
%% themselves call back into Python, without deadlocking.
%%
%% Run with:
%%   rebar3 shell
%%   c(examples/reentrant_callback).
%%   reentrant_callback:run().

-module(reentrant_callback).

-export([run/0]).

run() ->
    io:format("~n=== Reentrant Callback Example ===~n~n"),

    %% Ensure application is started
    {ok, _} = application:ensure_all_started(erlang_python),

    %% Example 1: Simple reentrant callback
    simple_example(),

    %% Example 2: Nested callbacks (multiple levels)
    nested_example(),

    %% Example 3: Data transformation pipeline
    pipeline_example(),

    io:format("~n=== All examples completed successfully! ===~n~n"),
    ok.

%% Simple example: Python calls Erlang which calls Python
simple_example() ->
    io:format("--- Simple Reentrant Callback ---~n"),

    %% Define a Python function
    ok = py:exec(<<"
def square(x):
    '''Simple Python function'''
    return x * x
">>),

    %% Register an Erlang function that uses Python
    py:register_function(square_via_python, fun([X]) ->
        {ok, Result} = py:call('__main__', square, [X]),
        Result
    end),

    %% Define a Python function that calls back to Erlang
    ok = py:exec(<<"
def process_number(x):
    '''Calls Erlang which calls Python'''
    from erlang import call
    squared = call('square_via_python', x)
    return squared + 1
">>),

    %% Test the full round-trip
    {ok, Result1} = py:call('__main__', process_number, [5]),
    io:format("process_number(5) = ~p (expected 26)~n", [Result1]),
    26 = Result1,

    {ok, Result2} = py:call('__main__', process_number, [10]),
    io:format("process_number(10) = ~p (expected 101)~n", [Result2]),
    101 = Result2,

    py:unregister_function(square_via_python),
    io:format("Simple example passed!~n~n").

%% Nested example: Multiple levels of Python<->Erlang calls
nested_example() ->
    io:format("--- Nested Callbacks (3+ levels) ---~n"),

    %% Register an Erlang function that recursively calls Python
    py:register_function(recurse, fun([Level, Max]) ->
        io:format("  [Erlang] recurse(~p, ~p)~n", [Level, Max]),
        case Level >= Max of
            true ->
                Level;  %% Base case
            false ->
                %% Call back into Python to continue
                {ok, Result} = py:call('__main__', python_recurse, [Level + 1, Max]),
                Result
        end
    end),

    %% Define Python functions for recursion
    ok = py:exec(<<"
def start_recursion(max_level):
    '''Entry point - starts the recursive chain'''
    from erlang import call
    print(f'  [Python] start_recursion({max_level})')
    return call('recurse', 1, max_level)

def python_recurse(level, max_level):
    '''Called from Erlang during recursion'''
    from erlang import call
    print(f'  [Python] python_recurse({level}, {max_level})')
    return call('recurse', level, max_level)
">>),

    %% Test with 4 levels of nesting
    io:format("Testing 4 levels of nesting:~n"),
    {ok, Result} = py:call('__main__', start_recursion, [4]),
    io:format("Result = ~p (expected 4)~n", [Result]),
    4 = Result,

    py:unregister_function(recurse),
    io:format("Nested example passed!~n~n").

%% Pipeline example: Data flows through Erlang and Python transformations
pipeline_example() ->
    io:format("--- Data Transformation Pipeline ---~n"),

    %% Register Erlang transformations
    py:register_function(erlang_uppercase, fun([S]) when is_binary(S) ->
        string:uppercase(S)
    end),

    py:register_function(erlang_wrap, fun([S]) when is_binary(S) ->
        <<"[", S/binary, "]">>
    end),

    %% Define Python pipeline
    ok = py:exec(<<"
def transform_pipeline(text):
    '''Transform text through Python and Erlang functions'''
    from erlang import call

    # Step 1: Python processing
    step1 = text.strip()

    # Step 2: Erlang uppercase
    step2 = call('erlang_uppercase', step1)

    # Step 3: Python add prefix
    step3 = 'Result: ' + step2

    # Step 4: Erlang wrap in brackets
    step4 = call('erlang_wrap', step3)

    return step4
">>),

    %% Test the pipeline
    {ok, Result} = py:call('__main__', transform_pipeline, [<<"  hello world  ">>]),
    io:format("transform_pipeline('  hello world  ') = ~p~n", [Result]),
    Expected = <<"[Result: HELLO WORLD]">>,
    Expected = Result,

    py:unregister_function(erlang_uppercase),
    py:unregister_function(erlang_wrap),
    io:format("Pipeline example passed!~n~n").
