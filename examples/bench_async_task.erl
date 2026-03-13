#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script for async task API performance.
%%%
%%% Tests the new py_event_loop async task API:
%%%   - py_event_loop:run/3,4 (blocking)
%%%   - py_event_loop:create_task/3,4 + await (non-blocking)
%%%   - py_event_loop:spawn/3,4 (fire-and-forget)
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_async_task.erl

-mode(compile).

main(Args) ->
    Opts = parse_args(Args),

    case maps:get(help, Opts, false) of
        true ->
            print_help(),
            halt(0);
        false ->
            ok
    end,

    io:format("~n=== Async Task API Benchmark ===~n~n"),

    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),

    %% Give event loop time to initialize
    timer:sleep(500),

    %% Verify event loop is ready
    case py_event_loop:get_loop() of
        {ok, LoopRef} ->
            io:format("Event loop initialized: ~p~n", [LoopRef]);
        {error, Reason} ->
            io:format("Event loop failed: ~p~n", [Reason]),
            halt(1)
    end,

    print_system_info(),
    setup_python(),

    Mode = maps:get(mode, Opts, standard),
    run_benchmarks(Mode),

    io:format("~n=== Benchmark Complete ===~n"),
    halt(0).

parse_args(Args) ->
    parse_args(Args, #{mode => standard}).

parse_args([], Acc) -> Acc;
parse_args(["--quick" | Rest], Acc) ->
    parse_args(Rest, Acc#{mode => quick});
parse_args(["--full" | Rest], Acc) ->
    parse_args(Rest, Acc#{mode => full});
parse_args(["--help" | _], Acc) ->
    Acc#{help => true};
parse_args([_ | Rest], Acc) ->
    parse_args(Rest, Acc).

print_help() ->
    io:format("Usage: escript examples/bench_async_task.erl [OPTIONS]~n~n"),
    io:format("Options:~n"),
    io:format("  --quick   Run quick benchmark (fewer iterations)~n"),
    io:format("  --full    Run full benchmark (more iterations)~n"),
    io:format("  --help    Show this help~n").

print_system_info() ->
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers: ~p~n", [erlang:system_info(schedulers)]),
    io:format("  Dirty Schedulers: ~p~n", [erlang:system_info(dirty_cpu_schedulers)]),
    {ok, PyVer} = py:version(),
    io:format("  Python: ~s~n", [PyVer]),
    io:format("~n").

setup_python() ->
    io:format("Python stdlib ready.~n"),

    %% Smoke test with math.sqrt (stdlib function)
    %% Note: Functions defined via py:exec run in a different context
    %% than process_ready_tasks, so we use stdlib functions instead.
    io:format("Smoke test (math.sqrt via create_task/await): "),
    Ref = py_event_loop:create_task(math, sqrt, [16.0]),
    case py_event_loop:await(Ref, 2000) of
        {ok, 4.0} ->
            io:format("PASS~n~n");
        Other ->
            io:format("FAIL: ~p~n", [Other]),
            halt(1)
    end.

run_benchmarks(quick) ->
    io:format("Running quick benchmarks...~n~n"),
    bench_baseline(100),
    bench_create_task_await(100),
    bench_concurrent_tasks(10, 10),
    bench_spawn_fire_forget(100);

run_benchmarks(full) ->
    io:format("Running full benchmarks...~n~n"),
    bench_baseline(5000),
    bench_create_task_await(5000),
    bench_concurrent_tasks(50, 100),
    bench_concurrent_tasks(100, 50),
    bench_spawn_fire_forget(1000);

run_benchmarks(standard) ->
    io:format("Running standard benchmarks...~n~n"),
    bench_baseline(1000),
    bench_create_task_await(1000),
    bench_concurrent_tasks(20, 50),
    bench_spawn_fire_forget(500).

%% Baseline: sync py:call for comparison
bench_baseline(N) ->
    io:format("Benchmark: Baseline py:call (sync)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            {ok, _} = py:call(math, sqrt, [I])
        end, lists:seq(1, N))
    end),

    print_results(Time, N, "calls").

%% Create task + await (non-blocking submission)
bench_create_task_await(N) ->
    io:format("Benchmark: create_task + await (math.sqrt)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Ref = py_event_loop:create_task(math, sqrt, [float(I)]),
            {ok, _} = py_event_loop:await(Ref)
        end, lists:seq(1, N))
    end),

    print_results(Time, N, "tasks").

%% Concurrent tasks from multiple processes
bench_concurrent_tasks(NumProcs, TasksPerProc) ->
    TotalTasks = NumProcs * TasksPerProc,
    io:format("Benchmark: Concurrent tasks (math.sqrt)~n"),
    io:format("  Processes: ~p, Tasks/process: ~p, Total: ~p~n",
              [NumProcs, TasksPerProc, TotalTasks]),

    Parent = self(),

    {Time, _} = timer:tc(fun() ->
        Pids = [spawn_link(fun() ->
            lists:foreach(fun(I) ->
                Ref = py_event_loop:create_task(math, sqrt, [float(I)]),
                {ok, _} = py_event_loop:await(Ref)
            end, lists:seq(1, TasksPerProc)),
            Parent ! {done, self()}
        end) || _ <- lists:seq(1, NumProcs)],

        [receive {done, Pid} -> ok end || Pid <- Pids]
    end),

    TimeMs = Time / 1000,
    TasksPerSec = TotalTasks / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Throughput: ~p tasks/sec~n~n", [round(TasksPerSec)]).

%% Spawn fire-and-forget
bench_spawn_fire_forget(N) ->
    io:format("Benchmark: spawn_task fire-and-forget (math.sqrt)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            ok = py_event_loop:spawn_task(math, sqrt, [float(I)])
        end, lists:seq(1, N))
    end),

    print_results(Time, N, "spawns"),
    %% Give time for spawned tasks to complete
    timer:sleep(100).

print_results(TimeUs, N, Unit) ->
    TimeMs = TimeUs / 1000,
    PerOp = TimeMs / N,
    OpsPerSec = N / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Per ~s: ~.3f ms~n", [Unit, PerOp]),
    io:format("  Throughput: ~p ~s/sec~n~n", [round(OpsPerSec), Unit]).
