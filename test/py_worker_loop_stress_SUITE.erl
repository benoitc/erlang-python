%%% @doc Stress tests for worker loops.
%%%
%%% Long running, high volume checks on py_context:start_loop/submit and
%%% erlang.server: connection churn on several workers with no fd left in the
%%% BEAM poll set, held keep-alive connections, start/stop cycling without
%%% leaks, submit storms during traffic, and the kill paths (memory cap,
%%% interrupt of a wedged loop).
%%%
%%% Skipped unless the environment variable STRESS is set (`STRESS=1 rebar3
%%% ct --suite py_worker_loop_stress_SUITE`), and needs owngil (Python 3.14+).
-module(py_worker_loop_stress_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_churn_four_workers/1,
    test_keepalive_connections_held/1,
    test_start_stop_cycles/1,
    test_submit_storm_under_traffic/1,
    test_memory_cap_in_handler/1,
    test_interrupt_wedged_loop/1
]).

%% logger handler callback
-export([log/2]).

-define(HOST, {127, 0, 0, 1}).

all() -> [
    test_churn_four_workers,
    test_keepalive_connections_held,
    test_start_stop_cycles,
    test_submit_storm_under_traffic,
    test_memory_cap_in_handler,
    test_interrupt_wedged_loop
].

init_per_suite(Config) ->
    case os:getenv("STRESS") of
        false ->
            {skip, "set STRESS=1 to run the worker loop stress suite"};
        _ ->
            {ok, _} = application:ensure_all_started(erlang_python),
            case py_nif:owngil_supported() of
                false -> {skip, "worker loops need OWN_GIL (Python 3.14+)"};
                true ->
                    TestDir = filename:dirname(code:which(?MODULE)),
                    [{test_dir, TestDir} | Config]
            end
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self()}),
    Config.

end_per_testcase(_TestCase, _Config) ->
    _ = logger:remove_handler(?MODULE),
    flush(),
    ok.

%%% ============================================================================
%%% Cases
%%% ============================================================================

%% @doc 10k short connections against four workers on one listen fd: no
%% failed connects, no erts_poll reports, fd count back to baseline.
test_churn_four_workers(Config) ->
    Baseline = fd_count(),
    Ctxs = [new_ctx(Config) || _ <- lists:seq(1, 4)],
    {LSock, Port, LFd} = listen(),
    lists:foreach(fun({I, C}) ->
        ok = py_context:start_loop(C),
        {ok, Dup} = py:dup_fd(LFd),
        Tag = list_to_binary("w" ++ integer_to_list(I) ++ ":"),
        {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve, [Dup, Tag])
    end, lists:zip(lists:seq(1, 4), Ctxs)),
    N = 10000,
    Clients = 50,
    T0 = erlang:monotonic_time(millisecond),
    Replies = parallel_roundtrips(Port, N, Clients),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    Failed = length([R || R <- Replies, not is_binary(R)]),
    Tags = lists:usort([binary:part(R, 0, 3) || R <- Replies, is_binary(R)]),
    ct:log("~p connections in ~p ms (~p conn/s), failed ~p, workers ~p",
           [N, Elapsed, N * 1000 div max(1, Elapsed), Failed, Tags]),
    0 = Failed,
    4 = length(Tags),
    [ok = py_context:stop_loop(C) || C <- Ctxs],
    [stop_ctx(C) || C <- Ctxs],
    gen_tcp:close(LSock),
    timer:sleep(500),
    [] = collect_reports(),
    After = fd_count(),
    ct:log("fds before ~p after ~p", [Baseline, After]),
    true = After =< Baseline + 8,
    ok.

%% @doc 1000 keep-alive connections held open with periodic writes for 20 s;
%% the worker's memory stays bounded.
test_keepalive_connections_held(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {LSock, Port, LFd} = listen(),
    {ok, Dup} = py:dup_fd(LFd),
    {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve_keepalive, [Dup]),
    Ref = py_context:get_nif_ref(C),
    {ok, Mem0, _} = py_nif:context_memory_usage(Ref),
    Socks = [begin
        {ok, S} = gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000),
        S
    end || _ <- lists:seq(1, 1000)],
    Rounds = 10,
    lists:foreach(fun(_) ->
        [ok = gen_tcp:send(S, <<"ping">>) || S <- Socks],
        [{ok, <<"pong">>} = gen_tcp:recv(S, 4, 10000) || S <- Socks],
        timer:sleep(2000)
    end, lists:seq(1, Rounds)),
    {ok, Mem1, _} = py_nif:context_memory_usage(Ref),
    ct:log("memory ~p -> ~p bytes with 1000 held connections", [Mem0, Mem1]),
    true = Mem1 < Mem0 + 256 * 1024 * 1024,
    [gen_tcp:close(S) || S <- Socks],
    timer:sleep(500),
    ok = py_context:stop_loop(C),
    gen_tcp:close(LSock),
    stop_ctx(C),
    [] = collect_reports(),
    ok.

%% @doc start_loop/stop_loop cycled 200 times: no leaked event workers, no
%% memory growth, context still fine.
test_start_stop_cycles(Config) ->
    C = new_ctx(Config),
    Ref = py_context:get_nif_ref(C),
    Workers0 = length(supervisor:which_children(py_event_worker_sup)),
    Procs0 = erlang:system_info(process_count),
    {ok, Mem0, _} = py_nif:context_memory_usage(Ref),
    lists:foreach(fun(I) ->
        ok = py_context:start_loop(C),
        {ok, I} = py_context:submit_await(C, py_test_workerloop, add, [I, 0]),
        ok = py_context:stop_loop(C)
    end, lists:seq(1, 200)),
    {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 5000),
    {ok, Mem1, _} = py_nif:context_memory_usage(Ref),
    Workers1 = length(supervisor:which_children(py_event_worker_sup)),
    Procs1 = erlang:system_info(process_count),
    ct:log("workers ~p -> ~p, processes ~p -> ~p, memory ~p -> ~p",
           [Workers0, Workers1, Procs0, Procs1, Mem0, Mem1]),
    Workers0 = Workers1,
    true = Procs1 =< Procs0 + 5,
    true = Mem1 < Mem0 + 64 * 1024 * 1024,
    stop_ctx(C),
    ok.

%% @doc 10k coroutine submits while 100 connections exchange data: every
%% result arrives, per caller ordering holds.
test_submit_storm_under_traffic(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {LSock, Port, LFd} = listen(),
    {ok, Dup} = py:dup_fd(LFd),
    {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve_keepalive, [Dup]),
    Self = self(),
    Traffic = spawn_link(fun() ->
        Socks = [element(2, gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000))
                 || _ <- lists:seq(1, 100)],
        traffic_loop(Socks, Self)
    end),
    Callers = 10,
    PerCaller = 1000,
    [spawn_link(fun() ->
        Refs = [element(2, py_context:submit(C, py_test_workerloop, add, [I, 0]))
                || I <- lists:seq(1, PerCaller)],
        Results = [py_event_loop:await(R, 30000) || R <- Refs],
        Self ! {storm, Results}
     end) || _ <- lists:seq(1, Callers)],
    AllOk = lists:all(fun(Results) ->
        Results =:= [{ok, I} || I <- lists:seq(1, PerCaller)]
    end, [receive {storm, Rs} -> Rs after 120000 -> [] end || _ <- lists:seq(1, Callers)]),
    Traffic ! stop,
    receive {traffic_done, Exchanged} -> ct:log("traffic exchanged ~p messages", [Exchanged])
    after 30000 -> ct:fail(traffic_did_not_stop)
    end,
    true = AllOk,
    ok = py_context:stop_loop(C),
    gen_tcp:close(LSock),
    stop_ctx(C),
    ok.

%% @doc A handler exceeding the context memory cap gets MemoryError there;
%% the loop keeps serving other connections.
test_memory_cap_in_handler(Config) ->
    case probe_memory_limits(Config) of
        false ->
            {skip, "runtime started without enable_memory_limits"};
        true ->
            {ok, C} = py_context:new(#{mode => owngil, memory_limit => 64 * 1024 * 1024}),
            setup_ctx(C, Config),
            ok = py_context:start_loop(C),
            {LSock, Port, LFd} = listen(),
            {ok, Dup} = py:dup_fd(LFd),
            {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve_greedy, [Dup]),
            %% greedy request: the handler tries to allocate past the cap
            {ok, S1} = gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000),
            ok = gen_tcp:send(S1, <<"hog">>),
            R1 = gen_tcp:recv(S1, 0, 30000),
            gen_tcp:close(S1),
            ct:log("greedy handler replied ~p", [R1]),
            {ok, <<"memoryerror">>} = R1,
            %% normal request still served
            {ok, S2} = gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000),
            ok = gen_tcp:send(S2, <<"x">>),
            {ok, <<"ok:x">>} = gen_tcp:recv(S2, 0, 5000),
            gen_tcp:close(S2),
            ok = py_context:stop_loop(C),
            gen_tcp:close(LSock),
            stop_ctx(C),
            ok
    end.

%% @doc A loop wedged in a blocking C call is interrupted once the call
%% returns; the loop exits and the context recovers.
test_interrupt_wedged_loop(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, _} = py_context:submit(C, py_test_workerloop, block_loop, [3]),
    timer:sleep(200),
    T0 = erlang:monotonic_time(millisecond),
    ok = py_context:interrupt(C),
    receive {py_loop_exit, C, {error, interrupted}} ->
        Elapsed = erlang:monotonic_time(millisecond) - T0,
        ct:log("wedged loop interrupted after ~p ms", [Elapsed]),
        true = Elapsed < 6000
    after 10000 -> ct:fail(loop_not_interrupted)
    end,
    {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 5000),
    stop_ctx(C),
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

new_ctx(Config) ->
    {ok, C} = py_context:new(#{mode => owngil}),
    setup_ctx(C, Config),
    C.

setup_ctx(C, Config) ->
    TestDir = ?config(test_dir, Config),
    Code = iolist_to_binary(io_lib:format(
        "import sys\nif '~s' not in sys.path:\n    sys.path.insert(0, '~s')\n"
        "import py_test_workerloop\n", [TestDir, TestDir])),
    ok = py_context:exec(C, Code).

stop_ctx(C) ->
    try py_context:stop(C) catch _:_ -> ok end,
    ok.

listen() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, ?HOST}, {active, false}, {backlog, 1024}]),
    {ok, Port} = inet:port(LSock),
    {ok, LFd} = inet:getfd(LSock),
    {LSock, Port, LFd}.

roundtrip(Port, Data) ->
    case gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000) of
        {ok, S} ->
            ok = gen_tcp:send(S, Data),
            R = case gen_tcp:recv(S, 0, 5000) of
                {ok, Bin} -> Bin;
                Other -> Other
            end,
            gen_tcp:close(S),
            R;
        Error ->
            Error
    end.

parallel_roundtrips(Port, N, Clients) ->
    Self = self(),
    Per = N div Clients,
    [spawn_link(fun() ->
        Self ! {rt, [roundtrip(Port, <<"x">>) || _ <- lists:seq(1, Per)]}
     end) || _ <- lists:seq(1, Clients)],
    lists:append([receive {rt, L} -> L after 300000 -> [] end || _ <- lists:seq(1, Clients)]).

traffic_loop(Socks, Parent) ->
    traffic_loop(Socks, Parent, 0).

traffic_loop(Socks, Parent, Count) ->
    receive
        stop ->
            [gen_tcp:close(S) || S <- Socks],
            Parent ! {traffic_done, Count}
    after 0 ->
        [ok = gen_tcp:send(S, <<"ping">>) || S <- Socks],
        [{ok, <<"pong">>} = gen_tcp:recv(S, 4, 10000) || S <- Socks],
        traffic_loop(Socks, Parent, Count + length(Socks))
    end.

probe_memory_limits(Config) ->
    case py_context:new(#{mode => owngil}) of
        {ok, Ctx} ->
            setup_ctx(Ctx, Config),
            Ref = py_context:get_nif_ref(Ctx),
            Result = py_nif:context_set_memory_limit(Ref, 0),
            py_context:stop(Ctx),
            Result =:= ok;
        _ ->
            false
    end.

fd_count() ->
    case os:type() of
        {unix, linux} -> length(element(2, file:list_dir("/proc/self/fd")));
        {unix, _} -> length(element(2, file:list_dir("/dev/fd")));
        _ -> 0
    end.

log(#{msg := Msg}, #{config := Pid}) ->
    Text = case Msg of
        {string, S} -> unicode:characters_to_binary(S);
        {report, R} -> unicode:characters_to_binary(io_lib:format("~p", [R]));
        {Fmt, Args} -> unicode:characters_to_binary(io_lib:format(Fmt, Args))
    end,
    case binary:match(Text, [<<"erts_poll">>, <<"stealing control">>]) of
        nomatch -> ok;
        _ -> Pid ! {poll_report, Text}
    end,
    ok.

collect_reports() ->
    receive {poll_report, T} -> [T | collect_reports()]
    after 200 -> []
    end.

flush() ->
    receive _ -> flush() after 0 -> ok end.
