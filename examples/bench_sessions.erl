#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark for isolated sessions (py_session).
%%%
%%% Measures, with the same workflow module for every row:
%%%   1. a fresh session (new + first call + close): fork template, spawn
%%%      template, spawn template with a warm pool, plain isolated context
%%%   2. sessions per second with 1, 8 and 64 callers
%%%   3. a call and a re-entrant call (Python -> Erlang -> same session)
%%%   4. a re-import run (start => reimport) on worker and owngil contexts
%%%
%%% examples/bench_sessions_sdks.py measures what Temporal's workflow
%%% sandbox and Restate's per-invocation state cost on the same module, to
%%% compare the isolation step of each.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_sessions.erl

-mode(compile).

-define(PY, <<"
import sys, types, erlang
m = types.ModuleType('bench_sessions_wf'); sys.modules['bench_sessions_wf'] = m
exec('''
from dataclasses import dataclass
import json, decimal, datetime

@dataclass
class Step:
    n: int

STATE = {}

def handle(event, state):
    n = state.get('n', 0) + 1
    return {'commands': [['activity', 'charge', {'n': n}]], 'state': {'n': Step(n).n}}

def reenter(n):
    import erlang
    return 0 if n == 0 else erlang.call('bench_reenter', erlang.self(), n) + 1
''', m.__dict__)
">>).

main(_) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    py:register_function(bench_reenter, fun([Ctx, N]) ->
        {ok, R} = py_context:call(Ctx, bench_sessions_wf, reenter, [N - 1]), R
    end),
    Fork = template(#{start => fork}),
    Spawn = template(#{start => spawn}),
    Warm = template(#{start => spawn, warm => 4}),
    io:format("~n== fresh session: new + first call + close ==~n"),
    row("fork", session_loop(Fork, 200)),
    row("spawn", session_loop(Spawn, 20)),
    row("spawn, warm pool", [begin timer:sleep(80), hd(session_loop(Warm, 1)) end
                             || _ <- lists:seq(1, 20)]),
    row("plain isolated", [plain() || _ <- lists:seq(1, 20)]),
    io:format("~n== sessions per second, fork template ==~n"),
    [begin
         L = throughput(Fork, C, 3),
         io:format("  ~2w callers  ~7.1f sessions/s  p50 ~6.2f ms~n", [C, length(L) / 3, p50(L)])
     end || C <- [1, 8, 64]],
    io:format("~n== calls inside a session ==~n"),
    {ok, S} = py_session:new(Fork),
    row("call", [us(fun() -> {ok, _} = py_context:call(S, bench_sessions_wf, handle, [[], #{}]) end)
                 || _ <- lists:seq(1, 2000)]),
    row("re-entrant call", [us(fun() -> {ok, 1} = py_context:call(S, bench_sessions_wf, reenter, [1]) end)
                            || _ <- lists:seq(1, 1000)]),
    py_session:close(S),
    io:format("~n== re-import runs (no process per run) ==~n"),
    %% the same workflow as a module file: a re-import run imports it anew
    Dir = filename:join(py_child:sock_dir(), "bench_reimport"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = file:write_file(filename:join(Dir, "bench_reimport_wf.py"), reimport_module()),
    [begin
         {ok, R} = py_session:template(#{start => reimport, mode => M, contexts => 4,
                                         paths => [Dir]}),
         row(atom_to_list(M) ++ ", one run",
             [us(fun() -> {ok, _} = py_session:run(R, bench_reimport_wf, handle, [[], #{}]) end)
              || _ <- lists:seq(1, 500)]),
         L = reimport_throughput(R, 8, 3),
         io:format("  ~-18s ~7.1f runs/s with 8 callers~n", [M, length(L) / 3])
     end || M <- [worker] ++ [owngil || py_nif:owngil_supported()]],
    ok.

reimport_module() ->
    <<"from dataclasses import dataclass\nimport json, decimal, datetime\n\n"
      "@dataclass\nclass Step:\n    n: int\n\n"
      "def handle(event, state):\n"
      "    n = state.get('n', 0) + 1\n"
      "    return {'commands': [['activity', 'charge', {'n': n}]], 'state': {'n': Step(n).n}}\n">>.

reimport_throughput(T, Callers, Secs) ->
    Self = self(),
    Deadline = erlang:monotonic_time(millisecond) + Secs * 1000,
    Loop = fun L(Acc) ->
        case erlang:monotonic_time(millisecond) < Deadline of
            true -> L([us(fun() -> {ok, _} = py_session:run(T, bench_reimport_wf, handle, [[], #{}]) end) | Acc]);
            false -> Acc
        end
    end,
    Pids = [spawn_link(fun() -> Self ! {done, self(), Loop([])} end) || _ <- lists:seq(1, Callers)],
    lists:append([receive {done, P, L} -> L end || P <- Pids]).

template(Opts) ->
    {ok, T} = py_session:template(Opts#{preload => ?PY}),
    T.

session_loop(T, N) ->
    [us(fun() ->
            {ok, S} = py_session:new(T),
            {ok, _} = py_context:call(S, bench_sessions_wf, handle, [[], #{}]),
            py_session:close(S)
        end) || _ <- lists:seq(1, N)].

plain() ->
    us(fun() ->
        {ok, C} = py_context:new(#{mode => isolated, preload => ?PY}),
        {ok, _} = py_context:call(C, bench_sessions_wf, handle, [[], #{}]),
        py_context:stop(C)
    end).

throughput(T, Callers, Secs) ->
    Self = self(),
    Deadline = erlang:monotonic_time(millisecond) + Secs * 1000,
    Loop = fun L(Acc) ->
        case erlang:monotonic_time(millisecond) < Deadline of
            true -> L([hd(session_loop(T, 1)) | Acc]);
            false -> Acc
        end
    end,
    Pids = [spawn_link(fun() -> Self ! {done, self(), Loop([])} end) || _ <- lists:seq(1, Callers)],
    lists:append([receive {done, P, L} -> L end || P <- Pids]).

us(F) ->
    T0 = erlang:monotonic_time(microsecond),
    _ = F(),
    erlang:monotonic_time(microsecond) - T0.

row(Name, L) ->
    S = lists:sort(L),
    P = fun(Q) -> lists:nth(max(1, round(Q * length(S))), S) / 1000 end,
    io:format("  ~-18s p50 ~8.3f ms  p99 ~8.3f ms~n", [Name, P(0.5), P(0.99)]).

p50(L) ->
    lists:nth(length(L) div 2 + 1, lists:sort(L)) / 1000.
