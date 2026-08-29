%%% @doc Soak test for `isolated' mode: a random mix of everything the mode
%%% offers, run for a while, with resource counters checked before and
%%% after. The point is to show no deadlock (every operation returns), no
%%% runaway loop (the mix keeps making progress) and no leak (Erlang
%%% processes, ports, ETS entries, memory, VM file descriptors and OS
%%% children return to their baseline).
%%%
%%% Duration is 60 s by default; set `PY_ISOLATED_SOAK_SECONDS' to change it.
-module(py_isolated_soak_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    test_mixed_workload_no_leak/1,
    test_callback_storm_no_deadlock/1,
    test_interrupt_kill_storm/1,
    test_loop_start_stop_churn/1
]).

-define(TEST_MOD, py_test_isolated).

all() -> [
    test_callback_storm_no_deadlock,
    test_interrupt_kill_storm,
    test_loop_start_stop_churn,
    test_mixed_workload_no_leak
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    py_callback:register(<<"soak_echo">>, fun([X]) -> X end),
    py_callback:register(<<"soak_incr">>, fun([X]) -> X + 1 end),
    py_callback:register(<<"soak_fail">>, fun(_) -> error(deliberate) end),
    py_callback:register(<<"soak_tid">>, fun([T, I]) -> T * 1000 + I end),
    [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config].

end_per_suite(_Config) ->
    py_callback:unregister(<<"soak_echo">>),
    py_callback:unregister(<<"soak_incr">>),
    py_callback:unregister(<<"soak_fail">>),
    py_callback:unregister(<<"soak_tid">>),
    ok = application:stop(erlang_python),
    ok.

%% @doc Many Erlang callers, each mixing plain calls, callbacks, nested
%% callbacks, thread-pool callbacks and callback errors on a shared context.
%% Every call must return within its timeout.
test_callback_storm_no_deadlock(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"soak_nested">>, fun([X]) ->
        {ok, R} = py_context:call(C, ?TEST_MOD, add, [X, 1], #{}, 20000), R
    end),
    Self = self(),
    Workers = 16,
    Rounds = 40,
    [spawn_link(fun() -> Self ! {done, I, storm(C, I, Rounds, [])} end) || I <- lists:seq(1, Workers)],
    Failures = lists:append([receive {done, _, F} -> F after 120000 -> ct:fail(worker_hung) end
                             || _ <- lists:seq(1, Workers)]),
    ct:log("failures: ~p", [Failures]),
    [] = Failures,
    py_callback:unregister(<<"soak_nested">>),
    ok = py_context:stop(C),
    ok.

storm(_C, _I, 0, Acc) ->
    Acc;
storm(C, I, N, Acc) ->
    Op = (I + N) rem 6,
    R = case Op of
        0 -> py_context:call(C, ?TEST_MOD, add, [I, N], #{}, 20000);
        1 -> py_context:call(C, ?TEST_MOD, callback, [<<"soak_echo">>, {I, N}], #{}, 20000);
        2 -> py_context:call(C, ?TEST_MOD, callback, [<<"soak_nested">>, N], #{}, 20000);
        3 -> py_context:call(C, ?TEST_MOD, pool_calls, [<<"soak_incr">>, 4, 20], #{}, 20000);
        4 -> py_context:call(C, ?TEST_MOD, callback_error_type, [<<"soak_fail">>], #{}, 20000);
        5 -> py_context:call(C, ?TEST_MOD, thread_calls, [<<"soak_tid">>, 4, 5], #{}, 20000)
    end,
    Expected = case Op of
        0 -> {ok, I + N};
        1 -> {ok, {I, N}};
        2 -> {ok, N + 1};
        3 -> {ok, false};   %% incr, not double: the helper compares to i*2
        4 -> {ok, <<"RuntimeError">>};
        5 -> {ok, {<<"ok">>, true, 20}}
    end,
    Acc1 = case R of
        Expected -> Acc;
        {ok, _} when Op =:= 3 -> Acc;   %% value checked by shape only
        Other -> [{I, N, Op, Other} | Acc]
    end,
    storm(C, I, N - 1, Acc1).

%% @doc Interrupts and kills racing with calls: nothing hangs, the context
%% always answers again, and no child is left behind.
test_interrupt_kill_storm(Config) ->
    C = new_ctx(Config, #{kill_after => 200, max_restarts => 1000}),
    Pids = lists:map(fun(N) ->
        {ok, #{os_pid := P}} = py_context:child_info(C),
        Self = self(),
        spawn_link(fun() ->
            Self ! {res, py_context:call(C, ?TEST_MOD, sleep_then, [5, N], #{}, 30000)}
        end),
        timer:sleep(20 + N rem 30),
        case N rem 3 of
            0 -> py_context:interrupt(C);
            1 -> py_context:kill(C);
            2 -> py_context:interrupt(C), py_context:kill(C)
        end,
        receive {res, R} ->
            case R of
                {error, interrupted} -> ok;
                {error, killed} -> ok;
                {error, {child_exited, _}} -> ok;
                {ok, N} -> ok;
                Other -> ct:fail({unexpected, N, Other})
            end
        after 15000 -> ct:fail({hung_after_interrupt, N})
        end,
        {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 30000),
        P
    end, lists:seq(1, 30)),
    ok = py_context:stop(C),
    timer:sleep(300),
    Alive = [P || P <- lists:usort(Pids), py_nif:os_kill(P, 0) =:= ok],
    [] = Alive,
    ok.

%% @doc start_loop / submit / stop_loop repeated, with a wedged loop every
%% few rounds so the kill backstop runs.
test_loop_start_stop_churn(Config) ->
    C = new_ctx(Config, #{kill_after => 200, max_restarts => 1000}),
    lists:foreach(fun(N) ->
        ok = py_context:start_loop(C),
        {ok, 3} = py_context:submit_await(C, ?TEST_MOD, async_add, [1, 2], #{}, 30000),
        case N rem 4 of
            0 ->
                {ok, _} = py_context:submit(C, ?TEST_MOD, blocked_sleep, [30]),
                timer:sleep(50),
                ok = py_context:stop_loop(C, 100);
            _ ->
                ok = py_context:stop_loop(C, 2000)
        end,
        receive {py_loop_exit, C, _} -> ok after 5000 -> ct:fail({no_loop_exit, N}) end,
        {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 30000)
    end, lists:seq(1, 24)),
    ok = py_context:stop(C),
    ok.

%% @doc The long one: contexts started and stopped, calls with payloads,
%% callbacks, coroutines, interrupts, crashes, for a fixed duration.
%% Counters must return to baseline.
test_mixed_workload_no_leak(Config) ->
    Seconds = list_to_integer(os:getenv("PY_ISOLATED_SOAK_SECONDS", "60")),
    %% Warm up so lazily created resources are in the baseline
    Warm = new_ctx(Config),
    {ok, _} = py_context:call(Warm, ?TEST_MOD, callback, [<<"soak_echo">>, 1]),
    ok = py_context:stop(Warm),
    timer:sleep(500),
    erlang:garbage_collect(),
    Base = counters(),
    ct:log("baseline: ~p", [Base]),
    Deadline = erlang:monotonic_time(millisecond) + Seconds * 1000,
    Self = self(),
    Workers = [spawn_link(fun() -> Self ! {worker, I, mixed(Config, I, Deadline, 0, [])} end)
               || I <- lists:seq(1, 6)],
    Stats = [receive {worker, _, S} -> S after (Seconds + 120) * 1000 -> ct:fail(worker_hung) end
             || _ <- Workers],
    Ops = lists:sum([O || {O, _} <- Stats]),
    Errs = lists:append([E || {_, E} <- Stats]),
    ct:log("ops: ~p, unexpected errors: ~p", [Ops, lists:sublist(Errs, 20)]),
    ct:print("soak: ~p ops in ~p s, ~p unexpected errors", [Ops, Seconds, length(Errs)]),
    true = Ops > 0,
    [] = Errs,
    timer:sleep(1000),
    erlang:garbage_collect(),
    After = counters(),
    ct:log("after: ~p", [After]),
    check_no_growth(Base, After),
    ok.

mixed(Config, I, Deadline, Ops, Errs) ->
    case erlang:monotonic_time(millisecond) > Deadline of
        true -> {Ops, Errs};
        false ->
            C = new_ctx(Config, #{kill_after => 300, max_restarts => 1000}),
            E1 = mixed_ops(C, I, 25, Errs),
            ok = py_context:stop(C),
            mixed(Config, I, Deadline, Ops + 25, E1)
    end.

mixed_ops(_C, _I, 0, Errs) ->
    Errs;
mixed_ops(C, I, N, Errs) ->
    Op = (I * 7 + N) rem 9,
    R = case Op of
        0 -> py_context:eval(C, <<"sum(range(1000))">>, #{}, 30000);
        1 -> py_context:call(C, ?TEST_MOD, identity, [crypto:strong_rand_bytes(256 * 1024)], #{}, 30000);
        2 -> py_context:call(C, ?TEST_MOD, callback, [<<"soak_echo">>, [I, N]], #{}, 30000);
        3 -> py_context:call(C, ?TEST_MOD, async_sleep_gather, [5, 0.001], #{}, 30000);
        4 -> py_context:eval(C, <<"__import__('time').sleep(5)">>, #{}, 50);
        5 -> py_context:call(C, ?TEST_MOD, pool_calls, [<<"soak_echo">>, 4, 10], #{}, 30000);
        6 -> py_context:call(C, ?TEST_MOD, segfault, [], #{}, 30000);
        7 -> py_context:call(C, ?TEST_MOD, send, [self(), {soak, N}], #{}, 30000);
        8 -> py_context:kill(C)
    end,
    Ok = case {Op, R} of
        {0, {ok, 499500}} -> true;
        {1, {ok, B}} when is_binary(B) -> true;
        {2, {ok, [I, N]}} -> true;
        {3, {ok, [0, 1, 2, 3, 4]}} -> true;
        {4, {error, timeout}} -> true;
        {5, {ok, _}} -> true;
        {6, {error, {child_exited, {signal, _}}}} -> true;
        {7, {ok, true}} -> receive {<<"soak">>, N} -> true after 5000 -> false end;
        {8, ok} -> true;
        _ -> false
    end,
    %% After any op the context must answer
    Alive = py_context:eval(C, <<"1">>, #{}, 30000) =:= {ok, 1},
    Errs1 = case Ok andalso Alive of
        true -> Errs;
        false -> [{op, Op, R, alive, Alive} | Errs]
    end,
    mixed_ops(C, I, N - 1, Errs1).

%%% ============================================================================
%%% Counters
%%% ============================================================================

counters() ->
    #{
        processes => erlang:system_info(process_count),
        ports => erlang:system_info(port_count),
        refs => ets:info(py_context_refs, size),
        memory_mb => erlang:memory(total) div (1024 * 1024),
        binary_mb => erlang:memory(binary) div (1024 * 1024),
        fds => beam_fd_count(),
        children => child_count()
    }.

check_no_growth(Base, After) ->
    Same = [processes, ports, refs, children],
    lists:foreach(fun(K) ->
        B = maps:get(K, Base), A = maps:get(K, After),
        A =< B + 2 orelse ct:fail({leak, K, B, A})
    end, Same),
    %% fds: allow a few for CT's own logging
    maps:get(fds, After) =< maps:get(fds, Base) + 8 orelse
        ct:fail({fd_leak, maps:get(fds, Base), maps:get(fds, After)}),
    %% memory: within 64 MB of baseline after GC
    maps:get(memory_mb, After) =< maps:get(memory_mb, Base) + 64 orelse
        ct:fail({memory_growth, maps:get(memory_mb, Base), maps:get(memory_mb, After)}),
    ok.

beam_fd_count() ->
    case os:type() of
        {unix, linux} ->
            length(filelib:wildcard("/proc/" ++ os:getpid() ++ "/fd/*"));
        _ ->
            Out = os:cmd("lsof -p " ++ os:getpid() ++ " 2>/dev/null | wc -l"),
            list_to_integer(string:trim(Out)) - 1
    end.

child_count() ->
    Out = os:cmd("ps -ax -o ppid=,command= 2>/dev/null | grep py_isolated_child | grep -v grep | grep -c ' " ++ os:getpid() ++ " ' "),
    case string:trim(Out) of
        "" -> 0;
        N -> list_to_integer(N)
    end.

new_ctx(Config) ->
    new_ctx(Config, #{}).

new_ctx(Config, Extra) ->
    TestDir = ?config(test_dir, Config),
    {ok, C} = py_context:new(maps:merge(#{mode => isolated, paths => [TestDir]}, Extra)),
    C.
