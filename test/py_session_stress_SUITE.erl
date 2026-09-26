%%% @doc Stress and profiling for sessions.
%%%
%%% Numbers are logged (ct:print), not asserted tightly: they show what a
%%% fresh session costs next to a plain isolated context on the same
%%% machine. The asserts only catch regressions of an order of magnitude.
-module(py_session_stress_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, end_per_testcase/2]).

-export([
    test_new_latency/1,
    test_new_breakdown/1,
    test_throughput/1,
    test_call_overhead/1,
    test_memory_per_session/1
]).

-define(MOD, py_test_session).

all() -> [
    test_new_latency,
    test_new_breakdown,
    test_throughput,
    test_call_overhead,
    test_memory_per_session
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config].

end_per_suite(_Config) ->
    ok.

end_per_testcase(_Case, _Config) ->
    [py_session:stop_template(T) || {_, T, _, _} <- supervisor:which_children(py_session_sup)],
    ok.

%% @doc Time to a usable session (new + first call), 100 in a row, for each
%% way of starting one, next to a plain isolated context.
test_new_latency(Config) ->
    Fork = template(Config, #{start => fork}),
    Spawn = template(Config, #{start => spawn}),
    Warm = template(Config, #{start => spawn, warm => 4}),
    N = 100,
    Session = fun(T) ->
        fun() ->
            {Us, S} = us(fun() ->
                {ok, S0} = py_session:new(T),
                {ok, _} = py_context:call(S0, ?MOD, peek, []),
                S0
            end),
            py_session:close(S),
            Us
        end
    end,
    Plain = fun() ->
        {Us, C} = us(fun() ->
            {ok, C0} = py_context:new(#{mode => isolated, paths => [?config(test_dir, Config)]}),
            {ok, _} = py_context:call(C0, ?MOD, peek, []),
            C0
        end),
        py_context:stop(C),
        Us
    end,
    Rows = [{fork, run(Session(Fork), N)},
            {spawn, run(Session(Spawn), 20)},
            %% warm: paced so the pool refills between sessions
            {warm_spawn, [begin timer:sleep(80), (Session(Warm))() end || _ <- lists:seq(1, 20)]},
            {plain_isolated, run(Plain, 20)}],
    [ct:print("new+call ~-15s p50 ~7.2f ms  p99 ~7.2f ms", [K, p(L, 50), p(L, 99)]) || {K, L} <- Rows],
    ForkP50 = p(proplists:get_value(fork, Rows), 50),
    SpawnP50 = p(proplists:get_value(spawn, Rows), 50),
    true = ForkP50 < SpawnP50,
    ok.

%% @doc Where a forked session's time goes: the fork request, the child
%% connecting, and its `ready' frame. Same split as the prototype.
test_new_breakdown(Config) ->
    T = template(Config, #{start => fork}),
    Samples = [breakdown(T) || _ <- lists:seq(1, 100)],
    [ct:print("fork breakdown ~-8s p50 ~6.3f ms  p99 ~6.3f ms",
              [K, p([maps:get(K, S) || S <- Samples], 50), p([maps:get(K, S) || S <- Samples], 99)])
     || K <- [fork, accept, ready, total]],
    ok.

breakdown(T) ->
    Path = py_child:new_sock_path("bench_"),
    {ok, L} = py_child:listen(Path),
    T0 = erlang:monotonic_time(microsecond),
    {ok, OsPid} = py_session_template:fork(T, Path, #{}, 5000),
    T1 = erlang:monotonic_time(microsecond),
    {ok, S} = py_child:accept(L, {os_pid, OsPid}, 5000),
    T2 = erlang:monotonic_time(microsecond),
    {ok, <<_:64/native, Len:32/native>>} = socket:recv(S, 12, 5000),
    {ok, _Ready} = socket:recv(S, Len, 5000),
    T3 = erlang:monotonic_time(microsecond),
    socket:close(S), socket:close(L), file:delete(Path),
    py_child:kill_os_pid(OsPid),
    #{fork => T1 - T0, accept => T2 - T1, ready => T3 - T2, total => T3 - T0}.

%% @doc Sessions per second (new + call + close) with 1, 8 and 64 callers,
%% for one and two zygotes.
test_throughput(Config) ->
    Secs = 3,
    Rows = [begin
                T = template(Config, #{start => fork, zygotes => Z}),
                R = [{C, throughput(T, C, Secs)} || C <- [1, 8, 64]],
                py_session:stop_template(T),
                {Z, R}
            end || Z <- [1, 2]],
    [[ct:print("zygotes ~w callers ~2w: ~7.1f sessions/s  p50 ~6.2f ms  p99 ~7.2f ms",
               [Z, C, length(L) / Secs, p(L, 50), p(L, 99)]) || {C, L} <- R] || {Z, R} <- Rows],
    ok.

throughput(T, Callers, Secs) ->
    Self = self(),
    Deadline = erlang:monotonic_time(millisecond) + Secs * 1000,
    Loop = fun L(Acc) ->
        case erlang:monotonic_time(millisecond) < Deadline of
            true ->
                {Us, ok} = us(fun() ->
                    {ok, S} = py_session:new(T),
                    {ok, _} = py_context:call(S, ?MOD, peek, []),
                    py_session:close(S)
                end),
                L([Us | Acc]);
            false ->
                Acc
        end
    end,
    Pids = [spawn_link(fun() -> Self ! {done, self(), Loop([])} end) || _ <- lists:seq(1, Callers)],
    lists:append([receive {done, P, L} -> L after 60000 -> ct:fail(caller_hung) end || P <- Pids]).

%% @doc A call in a session costs what it costs in an isolated context.
test_call_overhead(Config) ->
    T = template(Config, #{start => fork}),
    {ok, S} = py_session:new(T),
    {ok, C} = py_context:new(#{mode => isolated, paths => [?config(test_dir, Config)]}),
    Call = fun(Ctx) -> fun() -> element(1, us(fun() -> {ok, _} = py_context:call(Ctx, ?MOD, peek, []) end)) end end,
    Reenter = fun(Ctx) -> fun() -> element(1, us(fun() -> {ok, 1} = py_context:call(Ctx, ?MOD, reenter, [1]) end)) end end,
    py:register_function(sess_reenter, fun([Ctx, N]) ->
        {ok, R} = py_context:call(Ctx, ?MOD, reenter, [N - 1]), R
    end),
    try
        Rows = [{session_call, run(Call(S), 5000)},
                {isolated_call, run(Call(C), 5000)},
                {session_reenter, run(Reenter(S), 2000)},
                {isolated_reenter, run(Reenter(C), 2000)}],
        [ct:print("~-17s p50 ~6.1f us  p99 ~6.1f us", [K, p(L, 50) * 1000, p(L, 99) * 1000])
         || {K, L} <- Rows],
        SessionP50 = p(proplists:get_value(session_call, Rows), 50),
        PlainP50 = p(proplists:get_value(isolated_call, Rows), 50),
        true = SessionP50 < PlainP50 * 2
    after
        py:unregister_function(sess_reenter),
        py_session:close(S),
        py_context:stop(C)
    end.

%% @doc Resident memory of 20 live sessions, forked vs spawned. A forked
%% session shares the zygote's pages until it writes them.
test_memory_per_session(Config) ->
    Rows = [begin
                T = template(Config, #{start => Start}),
                Ss = [begin {ok, S} = py_session:new(T), S end || _ <- lists:seq(1, 20)],
                Pids = [begin {ok, P} = py_context:call(S, ?MOD, pid, []), P end || S <- Ss],
                Rss = [rss_kb(P) || P <- Pids],
                [py_session:close(S) || S <- Ss],
                {Start, Rss}
            end || Start <- [fork, spawn]],
    [ct:print("~-5s rss per session p50 ~7.1f MB (ps rss counts shared pages in full)",
              [K, lists:nth(length(L) div 2 + 1, lists:sort(L)) / 1024]) || {K, L} <- Rows],
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

template(Config, Opts) ->
    {ok, T} = py_session:template(Opts#{paths => [?config(test_dir, Config)],
                                        imports => [?MOD]}),
    T.

run(F, N) ->
    _ = [F() || _ <- lists:seq(1, 3)],
    [F() || _ <- lists:seq(1, N)].

us(F) ->
    T0 = erlang:monotonic_time(microsecond),
    R = F(),
    {erlang:monotonic_time(microsecond) - T0, R}.

%% Percentile in milliseconds
p(L, P) ->
    S = lists:sort(L),
    lists:nth(max(1, round(P / 100 * length(S))), S) / 1000.

rss_kb(OsPid) ->
    list_to_integer(string:trim(os:cmd("ps -o rss= -p " ++ integer_to_list(OsPid)))).
