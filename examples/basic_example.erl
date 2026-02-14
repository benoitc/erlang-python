#!/usr/bin/env escript
%%% @doc Basic example of using erlang_python.
%%%
%%% Prerequisites: rebar3 compile
%%% Run from project root: escript examples/basic_example.erl

-mode(compile).

main(_) ->
    %% Add the compiled beam files to the code path
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Basic Python Calls ===~n~n"),

    %% Simple math
    {ok, Result1} = py:call(math, sqrt, [16]),
    io:format("math.sqrt(16) = ~p~n", [Result1]),

    %% String operations (using eval since str methods are on objects, not a module)
    {ok, Result2} = py:eval(<<"'hello world'.upper()">>),
    io:format("'hello world'.upper() = ~s~n", [Result2]),

    %% JSON encoding
    {ok, Json} = py:call(json, dumps, [#{name => <<"Alice">>, age => 30}]),
    io:format("json.dumps({name: 'Alice', age: 30}) = ~s~n", [Json]),

    %% JSON decoding
    {ok, Decoded} = py:call(json, loads, [<<"{\"foo\": [1, 2, 3]}">>]),
    io:format("json.loads(...) = ~p~n", [Decoded]),

    io:format("~n=== Python Eval ===~n~n"),

    %% Evaluate expressions
    {ok, Sum} = py:eval(<<"sum(range(10))">>),
    io:format("sum(range(10)) = ~p~n", [Sum]),

    %% Eval with locals
    {ok, Computed} = py:eval(<<"x * 2 + y">>, #{x => 10, y => 5}),
    io:format("x * 2 + y (x=10, y=5) = ~p~n", [Computed]),

    io:format("~n=== List Operations ===~n~n"),

    %% List comprehension
    {ok, Squares} = py:eval(<<"[x**2 for x in range(5)]">>),
    io:format("[x**2 for x in range(5)] = ~p~n", [Squares]),

    io:format("~n=== Async Calls ===~n~n"),

    %% Async call
    Ref1 = py:call_async(math, factorial, [10]),
    Ref2 = py:call_async(math, factorial, [20]),

    {ok, Fact10} = py:await(Ref1),
    {ok, Fact20} = py:await(Ref2),

    io:format("math.factorial(10) = ~p~n", [Fact10]),
    io:format("math.factorial(20) = ~p~n", [Fact20]),

    io:format("~n=== Done ===~n~n"),

    ok = application:stop(erlang_python).
