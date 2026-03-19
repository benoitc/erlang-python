#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark for event loop pool parallel processing.
%%%
%%% Compares single event loop vs pool performance.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_event_loop_pool.erl

-mode(compile).

main(_Args) ->
    io:format("~n=== Event Loop Pool Benchmark ===~n~n"),

    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    timer:sleep(500),

    print_system_info(),

    %% Verify pool is ready
    case py_event_loop_pool:get_loop() of
        {ok, _} -> ok;
        {error, R} ->
            io:format("Pool not available: ~p~n", [R]),
            halt(1)
    end,

    Stats = py_event_loop_pool:get_stats(),
    io:format("Pool Stats: ~p~n~n", [Stats]),

    %% Run benchmarks
    bench_single_vs_pool_sequential(1000),
    bench_pool_concurrent(20, 100),
    bench_pool_concurrent(50, 100),
    bench_pool_throughput(10000),

    io:format("=== Benchmark Complete ===~n"),
    halt(0).

print_system_info() ->
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers: ~p~n", [erlang:system_info(schedulers)]),
    {ok, PyVer} = py:version(),
    io:format("  Python: ~s~n~n", [PyVer]).

%% Compare single loop vs pool for sequential tasks
bench_single_vs_pool_sequential(N) ->
    io:format("Benchmark: Sequential tasks (single caller)~n"),
    io:format("  Iterations: ~p~n", [N]),

    %% Single event loop
    {T1, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Ref = py_event_loop:create_task(math, sqrt, [float(I)]),
            {ok, _} = py_event_loop:await(Ref)
        end, lists:seq(1, N))
    end),

    %% Pool (should be similar since same caller = same loop)
    {T2, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Ref = py_event_loop_pool:create_task(math, sqrt, [float(I)]),
            {ok, _} = py_event_loop_pool:await(Ref)
        end, lists:seq(1, N))
    end),

    io:format("  py_event_loop:     ~.2f ms (~p tasks/sec)~n",
              [T1/1000, round(N / (T1/1000000))]),
    io:format("  py_event_loop_pool: ~.2f ms (~p tasks/sec)~n~n",
              [T2/1000, round(N / (T2/1000000))]).

%% Pool with concurrent callers (each gets own loop = parallel)
bench_pool_concurrent(NumProcs, TasksPerProc) ->
    TotalTasks = NumProcs * TasksPerProc,
    io:format("Benchmark: Concurrent callers via pool~n"),
    io:format("  Processes: ~p, Tasks/process: ~p, Total: ~p~n",
              [NumProcs, TasksPerProc, TotalTasks]),

    Parent = self(),

    {Time, _} = timer:tc(fun() ->
        Pids = [spawn_link(fun() ->
            lists:foreach(fun(I) ->
                Ref = py_event_loop_pool:create_task(math, sqrt, [float(I)]),
                {ok, _} = py_event_loop_pool:await(Ref)
            end, lists:seq(1, TasksPerProc)),
            Parent ! {done, self()}
        end) || _ <- lists:seq(1, NumProcs)],

        [receive {done, Pid} -> ok end || Pid <- Pids]
    end),

    io:format("  Total time: ~.2f ms~n", [Time/1000]),
    io:format("  Throughput: ~p tasks/sec~n~n", [round(TotalTasks / (Time/1000000))]).

%% High throughput test
bench_pool_throughput(N) ->
    io:format("Benchmark: Pool throughput (fire-and-collect)~n"),
    io:format("  Tasks: ~p~n", [N]),

    %% Submit all tasks first, then await all
    {Time, _} = timer:tc(fun() ->
        Refs = [py_event_loop_pool:create_task(math, sqrt, [float(I)])
                || I <- lists:seq(1, N)],
        [py_event_loop_pool:await(Ref) || Ref <- Refs]
    end),

    io:format("  Total time: ~.2f ms~n", [Time/1000]),
    io:format("  Throughput: ~p tasks/sec~n~n", [round(N / (Time/1000000))]).
