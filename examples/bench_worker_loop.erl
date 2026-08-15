#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark for worker loops (py_context:start_loop/submit, erlang.server).
%%%
%%% Measures, on OWN_GIL contexts:
%%%   1. connections/s and requests/s of an echo server on one worker loop
%%%   2. scaling with 1, 2, 4, 8 workers accepting on one listen fd
%%%   3. submit round trip latency into a running loop, 1/10/100 callers,
%%%      against py_context:call on an idle context
%%%   4. adopt (Erlang accepts, hands the fd over) vs Python side accept
%%%
%%% Clients are plain gen_tcp sockets in this VM, so the numbers include the
%%% client cost; use them to compare shapes, not as absolute server figures.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_worker_loop.erl

-mode(compile).

-define(HOST, {127, 0, 0, 1}).

-define(PY, <<"
import asyncio, erlang, sys, types
m = types.ModuleType('bench_wl'); sys.modules['bench_wl'] = m

class Echo(asyncio.Protocol):
    def connection_made(self, t): self.t = t
    def data_received(self, d): self.t.write(d)

class EchoClose(asyncio.Protocol):
    def connection_made(self, t): self.t = t
    def data_received(self, d): self.t.write(d); self.t.close()

_servers = {}
async def serve(fd, keepalive):
    srv = await erlang.server.serve(fd, Echo if keepalive else EchoClose)
    _servers[fd] = srv
    return 'ok'
async def adopt(fd):
    await erlang.server.adopt(fd, EchoClose)
    return 'ok'
async def noop():
    return 1
def sync_noop():
    return 1
m.serve = serve; m.adopt = adopt; m.noop = noop; m.sync_noop = sync_noop
">>).

main(_Args) ->
    io:format("~n"),
    io:format("========================================================~n"),
    io:format("  Worker loop benchmark~n"),
    io:format("========================================================~n~n"),
    {ok, _} = application:ensure_all_started(erlang_python),
    print_system_info(),
    case py_nif:owngil_supported() of
        true ->
            bench_single_worker(),
            bench_scaling(),
            bench_submit_latency(),
            bench_adopt_vs_accept();
        false ->
            io:format("~n[ERROR] worker loops need OWN_GIL (Python 3.14+)~n~n")
    end,
    halt(0).

print_system_info() ->
    io:format("System Information~n"),
    io:format("------------------~n"),
    io:format("  Erlang/OTP:       ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers:       ~p~n", [erlang:system_info(schedulers)]),
    {ok, PyVer} = py:version(),
    io:format("  Python:           ~s~n", [PyVer]),
    io:format("~n").

%% ============================================================================
%% 1. Single worker: connections/s (connect, echo, close) and requests/s on
%%    keep-alive connections
%% ============================================================================

bench_single_worker() ->
    io:format("1. Single worker echo server~n"),
    W = worker(),
    {LSock, Port, LFd} = listen(),
    {ok, Dup} = py:dup_fd(LFd),
    {ok, <<"ok">>} = py_context:submit_await(W, bench_wl, serve, [Dup, false]),
    N = 5000,
    {Ms, Fails} = timed(fun() -> parallel_conns(Port, N, 50) end),
    io:format("   connect+echo+close: ~p conns in ~p ms = ~p conn/s (failed ~p)~n",
              [N, Ms, N * 1000 div max(1, Ms), Fails]),
    ok = py_context:stop_loop(W),
    py_context:stop(W),
    gen_tcp:close(LSock),

    W2 = worker(),
    {LSock2, Port2, LFd2} = listen(),
    {ok, Dup2} = py:dup_fd(LFd2),
    {ok, <<"ok">>} = py_context:submit_await(W2, bench_wl, serve, [Dup2, true]),
    Conns = 50,
    Reqs = 20000,
    {Ms2, _} = timed(fun() -> keepalive_requests(Port2, Conns, Reqs div Conns) end),
    io:format("   keep-alive echo:    ~p reqs on ~p conns in ~p ms = ~p req/s~n",
              [Reqs, Conns, Ms2, Reqs * 1000 div max(1, Ms2)]),
    ok = py_context:stop_loop(W2),
    py_context:stop(W2),
    gen_tcp:close(LSock2),
    io:format("~n").

%% ============================================================================
%% 2. Scaling: N workers accepting on one listen fd
%% ============================================================================

bench_scaling() ->
    io:format("2. Scaling: connect+echo+close, workers accepting on one listen fd~n"),
    io:format("   ~-8s ~12s ~12s~n", ["workers", "conn/s", "ms"]),
    N = 8000,
    lists:foreach(fun(Workers) ->
        Ws = [worker() || _ <- lists:seq(1, Workers)],
        {LSock, Port, LFd} = listen(),
        [begin
            {ok, Dup} = py:dup_fd(LFd),
            {ok, <<"ok">>} = py_context:submit_await(W, bench_wl, serve, [Dup, false])
         end || W <- Ws],
        {Ms, _} = timed(fun() -> parallel_conns(Port, N, 100) end),
        io:format("   ~-8w ~12w ~12w~n", [Workers, N * 1000 div max(1, Ms), Ms]),
        [ok = py_context:stop_loop(W) || W <- Ws],
        [py_context:stop(W) || W <- Ws],
        gen_tcp:close(LSock)
    end, [1, 2, 4, 8]),
    io:format("~n").

%% ============================================================================
%% 3. submit latency vs py_context:call
%% ============================================================================

bench_submit_latency() ->
    io:format("3. Round trip latency~n"),
    io:format("   ~-52s ~10s ~12s~n", ["path", "us/op", "ops/s"]),
    {ok, Idle} = py_context:new(#{mode => owngil, preload => ?PY}),
    N = 5000,
    {MsCall, _} = timed(fun() ->
        [{ok, 1} = py_context:call(Idle, bench_wl, sync_noop, []) || _ <- lists:seq(1, N)]
    end),
    row("py_context:call, idle owngil ctx", N, MsCall),
    {MsSubIdle, _} = timed(fun() ->
        [{ok, 1} = py_context:submit_await(Idle, bench_wl, sync_noop, []) || _ <- lists:seq(1, N)]
    end),
    row("submit_await sync fn, idle ctx", N, MsSubIdle),
    py_context:stop(Idle),

    W = worker(),
    lists:foreach(fun(Callers) ->
        Per = N div Callers,
        {Ms, _} = timed(fun() ->
            Self = self(),
            [spawn_link(fun() ->
                [{ok, 1} = py_context:submit_await(W, bench_wl, noop, []) || _ <- lists:seq(1, Per)],
                Self ! done
             end) || _ <- lists:seq(1, Callers)],
            [receive done -> ok end || _ <- lists:seq(1, Callers)]
        end),
        row(io_lib:format("submit_await coroutine, running loop, ~p callers", [Callers]), N, Ms)
    end, [1, 10, 100]),
    ok = py_context:stop_loop(W),
    py_context:stop(W),
    io:format("~n").

row(Label, N, Ms) ->
    Us = Ms * 1000 / max(1, N),
    io:format("   ~-52s ~10.1f ~12w~n", [Label, Us, N * 1000 div max(1, Ms)]).

%% ============================================================================
%% 4. adopt (Erlang accepts) vs Python accept
%% ============================================================================

bench_adopt_vs_accept() ->
    io:format("4. Per connection: Erlang accept + adopt vs Python accept~n"),
    N = 3000,
    W = worker(),
    {LSock, Port, LFd} = listen(),
    {ok, Dup} = py:dup_fd(LFd),
    {ok, <<"ok">>} = py_context:submit_await(W, bench_wl, serve, [Dup, false]),
    {MsPy, _} = timed(fun() -> parallel_conns(Port, N, 50) end),
    io:format("   Python accept:        ~p conn/s~n", [N * 1000 div max(1, MsPy)]),
    ok = py_context:stop_loop(W),
    py_context:stop(W),
    gen_tcp:close(LSock),

    W2 = worker(),
    {LSock2, Port2, _} = listen(),
    Acceptor = spawn_link(fun() -> acceptor(LSock2, W2) end),
    {MsAd, _} = timed(fun() -> parallel_conns(Port2, N, 50) end),
    io:format("   Erlang accept+adopt:  ~p conn/s~n", [N * 1000 div max(1, MsAd)]),
    unlink(Acceptor), exit(Acceptor, kill),
    ok = py_context:stop_loop(W2),
    py_context:stop(W2),
    gen_tcp:close(LSock2),
    io:format("~n").

acceptor(LSock, W) ->
    case gen_tcp:accept(LSock) of
        {ok, Conn} ->
            {ok, Fd} = inet:getfd(Conn),
            {ok, Dup} = py:dup_fd(Fd),
            _ = py_context:submit(W, bench_wl, adopt, [Dup]),
            gen_tcp:close(Conn),
            acceptor(LSock, W);
        _ ->
            ok
    end.

%% ============================================================================
%% Helpers
%% ============================================================================

worker() ->
    {ok, W} = py_context:new(#{mode => owngil, preload => ?PY}),
    ok = py_context:start_loop(W),
    W.

listen() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, ?HOST}, {active, false}, {backlog, 1024}]),
    {ok, Port} = inet:port(LSock),
    {ok, LFd} = inet:getfd(LSock),
    {LSock, Port, LFd}.

timed(Fun) ->
    T0 = erlang:monotonic_time(millisecond),
    R = Fun(),
    {erlang:monotonic_time(millisecond) - T0, R}.

parallel_conns(Port, N, Clients) ->
    Self = self(),
    Per = N div Clients,
    [spawn_link(fun() ->
        Fails = length([bad || _ <- lists:seq(1, Per), roundtrip(Port) =/= ok]),
        Self ! {done, Fails}
     end) || _ <- lists:seq(1, Clients)],
    lists:sum([receive {done, F} -> F after 120000 -> Per end || _ <- lists:seq(1, Clients)]).

roundtrip(Port) ->
    case gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000) of
        {ok, S} ->
            ok = gen_tcp:send(S, <<"x">>),
            R = gen_tcp:recv(S, 0, 5000),
            gen_tcp:close(S),
            case R of {ok, <<"x">>} -> ok; _ -> bad end;
        _ ->
            bad
    end.

keepalive_requests(Port, Conns, PerConn) ->
    Self = self(),
    [spawn_link(fun() ->
        {ok, S} = gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000),
        [begin ok = gen_tcp:send(S, <<"ping">>), {ok, <<"ping">>} = gen_tcp:recv(S, 4, 5000) end
         || _ <- lists:seq(1, PerConn)],
        gen_tcp:close(S),
        Self ! done
     end) || _ <- lists:seq(1, Conns)],
    [receive done -> ok after 120000 -> ok end || _ <- lists:seq(1, Conns)],
    ok.
