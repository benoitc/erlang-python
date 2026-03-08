#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script for event loop performance.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_event_loop.erl

-mode(compile).

main(_Args) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),

    Code = <<"exec(open('examples/benchmark_event_loop.py').read())">>,
    case py:exec(Code) of
        ok -> ok;
        {error, E} -> io:format("Error: ~p~n", [E])
    end,

    halt(0).
