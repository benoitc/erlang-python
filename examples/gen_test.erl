%% Iterating Python generators from Erlang.
%%
%% A generator object cannot cross into Erlang, so the values are streamed:
%% py:stream/3 collects everything a generator yields, py:stream_start/3
%% delivers them one message at a time. Run with:
%%
%%   rebar3 shell
%%   > c("examples/gen_test.erl"), gen_test:run().
-module(gen_test).
-export([run/0]).

run() ->
    {ok, _} = application:ensure_all_started(erlang_python),
    %% A generator expression, collected in one go
    {ok, Squares} = py:stream_eval(<<"(x**2 for x in range(5))">>),
    io:format("squares: ~p~n", [Squares]),

    %% Any iterable a module function returns
    {ok, Range} = py:stream(builtins, range, [5]),
    io:format("range: ~p~n", [Range]),

    %% One value per message, as the generator yields them
    {ok, Ref} = py:stream_start(builtins, iter, [[1, 2, 3]]),
    receive_all(Ref).

receive_all(Ref) ->
    receive
        {py_stream, Ref, {data, V}} ->
            io:format("got ~p~n", [V]),
            receive_all(Ref);
        {py_stream, Ref, done} ->
            io:format("done~n");
        {py_stream, Ref, {error, Reason}} ->
            io:format("error: ~p~n", [Reason])
    after 5000 ->
        timeout
    end.
