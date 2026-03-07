#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script for py_resource_pool performance testing.
%%%
%%% Compares the new resource pool with the existing worker pool.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_resource_pool.erl

-mode(compile).

main(_Args) ->
    io:format("~n=== py_resource_pool Benchmark ===~n~n"),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),

    %% Print system info
    print_system_info(),

    %% Benchmark the resource pool
    io:format("~n--- Resource Pool Benchmarks ---~n~n"),
    ok = py_resource_pool:start(),
    Stats = py_resource_pool:stats(),
    io:format("Pool stats: ~p~n~n", [Stats]),

    %% Sequential calls
    bench_resource_pool_sequential(1000),

    %% Concurrent calls
    bench_resource_pool_concurrent(10, 100),
    bench_resource_pool_concurrent(50, 100),
    bench_resource_pool_concurrent(100, 100),

    %% Stop resource pool
    ok = py_resource_pool:stop(),

    %% Now benchmark the old worker pool for comparison
    io:format("~n--- Old Worker Pool (py:call) Benchmarks ---~n~n"),

    %% Sequential calls
    bench_old_pool_sequential(1000),

    %% Concurrent calls
    bench_old_pool_concurrent(10, 100),
    bench_old_pool_concurrent(50, 100),
    bench_old_pool_concurrent(100, 100),

    io:format("~n=== Benchmark Complete ===~n"),
    halt(0).

print_system_info() ->
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers: ~p~n", [erlang:system_info(schedulers)]),
    io:format("  Python: "),
    {ok, PyVer} = py:version(),
    io:format("~s~n", [PyVer]),
    io:format("  Execution Mode: ~p~n", [py:execution_mode()]),
    io:format("~n").

%% Resource pool benchmarks
bench_resource_pool_sequential(N) ->
    io:format("Resource Pool: Sequential calls (math.sqrt)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            {ok, _} = py_resource_pool:call(math, sqrt, [I])
        end, lists:seq(1, N))
    end),

    print_results(Time, N).

bench_resource_pool_concurrent(NumProcs, CallsPerProc) ->
    TotalCalls = NumProcs * CallsPerProc,
    io:format("Resource Pool: Concurrent calls~n"),
    io:format("  Processes: ~p, Calls/process: ~p, Total: ~p~n",
              [NumProcs, CallsPerProc, TotalCalls]),

    Parent = self(),

    {Time, _} = timer:tc(fun() ->
        Pids = [spawn_link(fun() ->
            lists:foreach(fun(I) ->
                {ok, _} = py_resource_pool:call(math, sqrt, [I])
            end, lists:seq(1, CallsPerProc)),
            Parent ! {done, self()}
        end) || _ <- lists:seq(1, NumProcs)],

        [receive {done, Pid} -> ok end || Pid <- Pids]
    end),

    print_results(Time, TotalCalls).

%% Old pool benchmarks (py:call)
bench_old_pool_sequential(N) ->
    io:format("Old Pool (py:call): Sequential calls (math.sqrt)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            {ok, _} = py:call(math, sqrt, [I])
        end, lists:seq(1, N))
    end),

    print_results(Time, N).

bench_old_pool_concurrent(NumProcs, CallsPerProc) ->
    TotalCalls = NumProcs * CallsPerProc,
    io:format("Old Pool (py:call): Concurrent calls~n"),
    io:format("  Processes: ~p, Calls/process: ~p, Total: ~p~n",
              [NumProcs, CallsPerProc, TotalCalls]),

    Parent = self(),

    {Time, _} = timer:tc(fun() ->
        Pids = [spawn_link(fun() ->
            lists:foreach(fun(I) ->
                {ok, _} = py:call(math, sqrt, [I])
            end, lists:seq(1, CallsPerProc)),
            Parent ! {done, self()}
        end) || _ <- lists:seq(1, NumProcs)],

        [receive {done, Pid} -> ok end || Pid <- Pids]
    end),

    print_results(Time, TotalCalls).

print_results(TimeUs, N) ->
    TimeMs = TimeUs / 1000,
    CallsPerSec = N / (TimeMs / 1000),
    PerCall = TimeMs / N,
    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Per call: ~.3f ms~n", [PerCall]),
    io:format("  Throughput: ~p calls/sec~n~n", [round(CallsPerSec)]).
