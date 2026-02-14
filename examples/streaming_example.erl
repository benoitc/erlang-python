#!/usr/bin/env escript
%%% @doc Streaming example - demonstrates Python generators.
%%%
%%% Prerequisites: rebar3 compile
%%% Run from project root: escript examples/streaming_example.erl

-mode(compile).

main(_) ->
    %% Add the compiled beam files to the code path
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir),

    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Streaming from Python Generators ===~n~n"),

    %% Note: Since workers don't share state, we use stream_eval for inline generators
    %% or stream with builtin/importable modules. This is the recommended pattern.

    %% Stream tokens using inline generator
    io:format("Streaming tokens: "),
    {ok, Tokens} = py:stream_eval(<<"(word + ' ' for word in 'Hello world from Python generator'.split())">>),
    lists:foreach(fun(T) -> io:format("~s", [T]) end, Tokens),
    io:format("~n~n"),

    %% Fibonacci sequence using inline generator with walrus operator
    io:format("Fibonacci(10): "),
    {ok, Fibs} = py:stream_eval(<<"(lambda: ((fib := [0, 1]), [fib.append(fib[-1] + fib[-2]) for _ in range(8)], iter(fib))[-1])()">>),
    io:format("~p~n~n", [Fibs]),

    %% Custom range using builtin iter
    io:format("Range(0, 20, 3): "),
    {ok, Range} = py:stream(builtins, iter, [[0, 3, 6, 9, 12, 15, 18]]),
    io:format("~p~n~n", [Range]),

    %% Stream with enumerate (builtin)
    io:format("Enumerate(['a', 'b', 'c']): "),
    {ok, Enum} = py:stream(builtins, enumerate, [[<<"a">>, <<"b">>, <<"c">>]]),
    io:format("~p~n~n", [Enum]),

    %% Filter example (lazy evaluation)
    io:format("Filter(x > 2, [1,2,3,4,5]): "),
    {ok, Filtered} = py:stream_eval(<<"filter(lambda x: x > 2, [1, 2, 3, 4, 5])">>),
    io:format("~p~n~n", [Filtered]),

    io:format("=== Done ===~n~n"),

    ok = application:stop(erlang_python).
