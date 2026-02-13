#!/usr/bin/env escript
%%% @doc Streaming example - demonstrates Python generators.
%%%
%%% Run with: escript examples/streaming_example.erl

-mode(compile).

main(_) ->
    ok = application:start(erlang_python),

    io:format("~n=== Streaming from Python Generators ===~n~n"),

    %% First, define a generator function in Python
    ok = py:exec(<<"
def token_generator(text, delay=0):
    '''Simulate token-by-token streaming like an LLM'''
    import time
    for word in text.split():
        if delay > 0:
            time.sleep(delay)
        yield word + ' '

def fibonacci(n):
    '''Generate first n Fibonacci numbers'''
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b

def range_gen(start, stop, step=1):
    '''Custom range generator'''
    current = start
    while current < stop:
        yield current
        current += step
">>),

    %% Stream tokens
    io:format("Streaming tokens: "),
    {ok, Tokens} = py:stream(<<"__main__">>, <<"token_generator">>,
                              [<<"Hello world from Python generator">>]),
    lists:foreach(fun(T) -> io:format("~s", [T]) end, Tokens),
    io:format("~n~n"),

    %% Fibonacci sequence
    io:format("Fibonacci(10): "),
    {ok, Fibs} = py:stream(<<"__main__">>, <<"fibonacci">>, [10]),
    io:format("~p~n~n", [Fibs]),

    %% Custom range
    io:format("Range(0, 20, 3): "),
    {ok, Range} = py:stream(<<"__main__">>, <<"range_gen">>, [0, 20, 3]),
    io:format("~p~n~n", [Range]),

    io:format("=== Done ===~n~n"),

    ok = application:stop(erlang_python).
