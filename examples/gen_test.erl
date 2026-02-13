#!/usr/bin/env escript
%%% Generator iteration test
-mode(compile).

main(_) ->
    code:add_patha("_build/default/lib/erlang_python/ebin"),

    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("=== Generator Iteration Test ===~n~n"),

    %% Get a worker directly
    ok = py_nif:init(),
    {ok, Worker} = py_nif:worker_new(),

    %% Create a generator via eval
    io:format("Creating generator (x**2 for x in range(5))...~n"),
    {ok, {generator, Gen}} = py_nif:worker_eval(Worker, <<"(x**2 for x in range(5))">>, #{}),
    io:format("Got generator ref~n~n"),

    %% Iterate manually
    io:format("Iterating: "),
    iterate(Worker, Gen),
    io:format("~n~n"),

    %% Test with range
    io:format("Range(10): "),
    {ok, {generator, Gen2}} = py_nif:worker_eval(Worker, <<"iter(range(10))">>, #{}),
    iterate(Worker, Gen2),
    io:format("~n~n"),

    %% Test Fibonacci generator defined inline
    io:format("Fibonacci via exec + call:~n"),
    ok = py_nif:worker_exec(Worker, <<"
def fib(n):
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b
">>),
    {ok, {generator, Gen3}} = py_nif:worker_call(Worker, <<"__main__">>, <<"fib">>, [10], #{}),
    io:format("  fib(10) = "),
    iterate(Worker, Gen3),
    io:format("~n~n"),

    io:format("=== Done ===~n"),
    ok = application:stop(erlang_python).

iterate(Worker, Gen) ->
    case py_nif:worker_next(Worker, Gen) of
        {ok, Value} ->
            io:format("~p ", [Value]),
            iterate(Worker, Gen);
        {error, stop_iteration} ->
            ok;
        {error, Error} ->
            io:format("Error: ~p", [Error])
    end.
