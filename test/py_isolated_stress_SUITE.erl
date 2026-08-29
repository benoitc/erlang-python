%%% @doc Stress and profiling for `isolated' mode.
%%%
%%% Numbers are logged, not asserted tightly: the point is to show the cost
%%% of the process boundary next to worker mode on the same machine, and
%%% that churn does not leak OS processes or memory.
-module(py_isolated_stress_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).

-export([
    test_call_latency_vs_worker/1,
    test_callback_round_trips/1,
    test_context_churn_no_leak/1,
    test_startup_time/1,
    test_payload_throughput/1,
    test_parallel_contexts_cpu_bound/1,
    test_shared_memory_vs_copy/1
]).

all() -> [
    test_call_latency_vs_worker,
    test_callback_round_trips,
    test_context_churn_no_leak,
    test_startup_time,
    test_payload_throughput,
    test_parallel_contexts_cpu_bound,
    test_shared_memory_vs_copy
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

%% @doc 10k sequential evals per mode; p50/p99 per call.
test_call_latency_vs_worker(_Config) ->
    N = 10000,
    {ok, I} = py_context:new(#{mode => isolated}),
    {ok, W} = py_context:new(#{mode => worker}),
    {ok, 2} = py_context:eval(I, <<"1+1">>),
    {ok, 2} = py_context:eval(W, <<"1+1">>),
    IsoLat = latencies(fun() -> {ok, 2} = py_context:eval(I, <<"1+1">>) end, N),
    WrkLat = latencies(fun() -> {ok, 2} = py_context:eval(W, <<"1+1">>) end, N),
    IsoCall = latencies(fun() -> {ok, 4.0} = py_context:call(I, math, sqrt, [16]) end, N),
    WrkCall = latencies(fun() -> {ok, 4.0} = py_context:call(W, math, sqrt, [16]) end, N),
    ct:log("eval  isolated: ~s~n      worker:   ~s", [stats(IsoLat), stats(WrkLat)]),
    ct:log("call  isolated: ~s~n      worker:   ~s", [stats(IsoCall), stats(WrkCall)]),
    ct:print("eval p50 isolated ~p us vs worker ~p us", [pct(IsoLat, 50), pct(WrkLat, 50)]),
    py_context:stop(I),
    py_context:stop(W),
    ok.

test_callback_round_trips(_Config) ->
    {ok, C} = py_context:new(#{mode => isolated}),
    py_callback:register(<<"stress_echo">>, fun([X]) -> X end),
    Code = <<"__import__('erlang').call('stress_echo', 1)">>,
    Lat = latencies(fun() -> {ok, 1} = py_context:eval(C, Code) end, 1000),
    ct:log("eval+callback isolated: ~s", [stats(Lat)]),
    %% 1000 callbacks inside one request
    ok = py_context:exec(C, <<"import erlang\ndef burst(n):\n    return sum(erlang.call('stress_echo', i) for i in range(n))\n">>),
    T0 = erlang:monotonic_time(microsecond),
    {ok, 499500} = py_context:call(C, '__main__', burst, [1000]),
    Per = (erlang:monotonic_time(microsecond) - T0) / 1000,
    ct:log("1000 callbacks in one request: ~.1f us each", [Per]),
    py_callback:unregister(<<"stress_echo">>),
    py_context:stop(C),
    ok.

%% @doc 100 contexts started and stopped: no child left, RSS reported.
test_context_churn_no_leak(_Config) ->
    Pids = lists:map(fun(_) ->
        {ok, C} = py_context:new(#{mode => isolated}),
        {ok, #{os_pid := P}} = py_context:child_info(C),
        {ok, 2} = py_context:eval(C, <<"1+1">>),
        ok = py_context:stop(C),
        P
    end, lists:seq(1, 100)),
    timer:sleep(500),
    Alive = [P || P <- Pids, py_nif:os_kill(P, 0) =:= ok],
    ct:log("children still alive after churn: ~p", [Alive]),
    [] = Alive,
    %% Memory per child
    {ok, C} = py_context:new(#{mode => isolated}),
    {ok, #{os_pid := P}} = py_context:child_info(C),
    Rss = string:trim(os:cmd("ps -o rss= -p " ++ integer_to_list(P))),
    ct:log("child RSS after start: ~s KB", [Rss]),
    ct:print("child RSS: ~s KB", [Rss]),
    {ok, _} = py_context:eval(C, <<"__import__('json').dumps([1]*1000)">>),
    Rss2 = string:trim(os:cmd("ps -o rss= -p " ++ integer_to_list(P))),
    ct:log("child RSS after json import: ~s KB", [Rss2]),
    py_context:stop(C),
    ok.

test_startup_time(_Config) ->
    Times = lists:map(fun(_) ->
        T0 = erlang:monotonic_time(microsecond),
        {ok, C} = py_context:new(#{mode => isolated}),
        T = erlang:monotonic_time(microsecond) - T0,
        ok = py_context:stop(C),
        T
    end, lists:seq(1, 20)),
    ct:log("isolated context start (spawn -> ready -> init): ~s", [stats(Times)]),
    ct:print("startup p50 ~p ms", [pct(Times, 50) div 1000]),
    ok.

test_payload_throughput(_Config) ->
    {ok, I} = py_context:new(#{mode => isolated}),
    {ok, W} = py_context:new(#{mode => worker}),
    ok = py_context:exec(I, <<"def ident(x): return x">>),
    ok = py_context:exec(W, <<"def ident(x): return x">>),
    lists:foreach(fun(Size) ->
        Bin = crypto:strong_rand_bytes(Size),
        TI = timed(fun() -> {ok, Bin} = py_context:call(I, '__main__', ident, [Bin]) end),
        TW = timed(fun() -> {ok, Bin} = py_context:call(W, '__main__', ident, [Bin]) end),
        ct:log("~p MB round trip: isolated ~.1f ms (~.1f MB/s), worker ~.1f ms",
               [Size div (1024 * 1024), TI / 1000, 2 * Size / 1048576 / (TI / 1.0e6), TW / 1000])
    end, [1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024]),
    py_context:stop(I),
    py_context:stop(W),
    ok.

%% @doc Four isolated children run CPU-bound work in parallel: the GIL is
%% per process, so wall time is close to one child's time.
test_parallel_contexts_cpu_bound(_Config) ->
    Ctxs = [begin {ok, C} = py_context:new(#{mode => isolated}), C end || _ <- lists:seq(1, 4)],
    Code = <<"sum(i*i for i in range(2000000))">>,
    T1 = timed(fun() -> {ok, _} = py_context:eval(hd(Ctxs), Code) end),
    Self = self(),
    T4 = timed(fun() ->
        [spawn_link(fun() -> Self ! {done, py_context:eval(C, Code)} end) || C <- Ctxs],
        [receive {done, {ok, _}} -> ok after 60000 -> ct:fail(timeout) end || _ <- Ctxs]
    end),
    ct:log("cpu-bound: 1 child ~.1f ms, 4 children in parallel ~.1f ms", [T1 / 1000, T4 / 1000]),
    %% On a dedicated machine T4 is close to T1 (see the log). CI VMs are
    %% overcommitted, so only assert the children did not serialise.
    true = T4 < 4 * T1,
    [py_context:stop(C) || C <- Ctxs],
    ok.

%% @doc Bulk data both ways: a py_shm region against the socket copy, in
%% isolated and worker mode, for 1, 16 and 64 MB.
test_shared_memory_vs_copy(_Config) ->
    case py_shm:available() of
        false -> {skip, "iommap not available"};
        true -> shared_memory_vs_copy()
    end.

shared_memory_vs_copy() ->
    TestDir = filename:join(code:lib_dir(erlang_python), "test"),
    {ok, I} = py_context:new(#{mode => isolated, paths => [TestDir]}),
    {ok, W} = py_context:new(#{mode => worker}),
    ok = py_context:exec(W, iolist_to_binary(io_lib:format(
        "import sys\nif '~s' not in sys.path: sys.path.insert(0, '~s')", [TestDir, TestDir]))),
    ok = py_context:exec(I, <<"def ident(x): return x">>),
    ok = py_context:exec(W, <<"def ident(x): return x">>),
    lists:foreach(fun(Mb) ->
        Size = Mb * 1024 * 1024,
        Bin = crypto:strong_rand_bytes(Size),
        {ok, Shm} = py_shm:new(Size),
        %% Erlang -> Python: copy through the socket vs write into the region
        %% and sum the first 4 KB through a memoryview
        CopyI = timed(fun() -> {ok, _} = py_context:call(I, '__main__', ident, [Bin]) end),
        ShmI = timed(fun() ->
            ok = py_shm:write(Shm, 0, Bin),
            {ok, _} = py_context:call(I, py_test_isolated_shm, shm_sum, [Shm, 4096])
        end),
        ShmW = timed(fun() ->
            ok = py_shm:write(Shm, 0, Bin),
            {ok, _} = py_context:call(W, py_test_isolated_shm, shm_sum, [Shm, 4096])
        end),
        %% Python -> Erlang: result through the socket vs fill the region and
        %% read it with a region binary
        OutI = timed(fun() -> {ok, _} = py_context:call(I, py_test_isolated, big_payload, [Size]) end),
        FillI = timed(fun() ->
            {ok, _} = py_context:call(I, py_test_isolated_shm, shm_fill, [Shm, 1, Size]),
            _ = py_shm:binary(Shm, 0, Size)
        end),
        ct:log("~p MB  Erlang->Python: socket ~.1f ms, shm isolated ~.1f ms, shm worker ~.1f ms~n"
               "       Python->Erlang: socket ~.1f ms, shm isolated ~.1f ms",
               [Mb, CopyI / 1000, ShmI / 1000, ShmW / 1000, OutI / 1000, FillI / 1000]),
        ct:print("~p MB: to Python socket ~.1f ms vs shm ~.1f ms; from Python socket ~.1f ms vs shm ~.1f ms",
                 [Mb, CopyI / 1000, ShmI / 1000, OutI / 1000, FillI / 1000]),
        ok = py_shm:close(Shm)
    end, [1, 16, 64]),
    py_context:stop(I),
    py_context:stop(W),
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

latencies(Fun, N) ->
    lists:sort([timed(Fun) || _ <- lists:seq(1, N)]).

timed(Fun) ->
    T0 = erlang:monotonic_time(microsecond),
    Fun(),
    erlang:monotonic_time(microsecond) - T0.

pct(Sorted, P) ->
    Idx = max(1, min(length(Sorted), round(length(Sorted) * P / 100))),
    lists:nth(Idx, Sorted).

stats(Sorted) ->
    Mean = lists:sum(Sorted) / length(Sorted),
    io_lib:format("p50 ~p us, p99 ~p us, max ~p us, mean ~.1f us",
                  [pct(Sorted, 50), pct(Sorted, 99), lists:last(Sorted), Mean]).
