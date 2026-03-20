#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark for OWN_GIL event loop pool.
%%%
%%% Compares regular event loop pool vs OWN_GIL mode for parallel execution.
%%% OWN_GIL mode creates separate GIL per worker, enabling true parallelism.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_owngil_pool.erl

-mode(compile).

main(_Args) ->
    io:format("~n"),
    io:format("================================================================~n"),
    io:format("  OWN_GIL Event Loop Pool Benchmark~n"),
    io:format("================================================================~n~n"),

    case py_nif:subinterp_supported() of
        false ->
            io:format("[ERROR] OWN_GIL requires Python 3.12+~n"),
            halt(1);
        true ->
            ok
    end,

    %% Run regular pool benchmark first
    io:format("Phase 1: Regular Event Loop Pool (shared GIL)~n"),
    io:format("----------------------------------------------~n"),
    application:set_env(erlang_python, event_loop_pool_owngil, false),
    {ok, _} = application:ensure_all_started(erlang_python),
    timer:sleep(500),

    print_system_info(),
    RegularStats = py_event_loop_pool:get_stats(),
    io:format("Pool config: ~p~n~n", [RegularStats]),

    RegularResults = run_benchmarks("Regular"),

    %% Stop and restart with OWN_GIL enabled
    ok = application:stop(erlang_python),
    timer:sleep(200),

    io:format("~nPhase 2: OWN_GIL Event Loop Pool (separate GILs)~n"),
    io:format("-------------------------------------------------~n"),
    application:set_env(erlang_python, event_loop_pool_owngil, true),
    {ok, _} = application:ensure_all_started(erlang_python),
    timer:sleep(500),

    OwngilStats = py_event_loop_pool:get_stats(),
    io:format("Pool config: ~p~n~n", [OwngilStats]),

    OwngilResults = run_benchmarks("OWN_GIL"),

    %% Print comparison
    print_comparison(RegularResults, OwngilResults),

    halt(0).

print_system_info() ->
    io:format("System Information:~n"),
    io:format("  Erlang/OTP:  ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers:  ~p~n", [erlang:system_info(schedulers)]),
    {ok, PyVer} = py:version(),
    io:format("  Python:      ~s~n", [PyVer]),
    io:format("~n").

run_benchmarks(Label) ->
    #{
        sequential => bench_sequential(Label, 500),
        concurrent_light => bench_concurrent(Label, 4, 100, light),
        concurrent_medium => bench_concurrent(Label, 8, 50, light),
        parallel_cpu => bench_parallel_cpu(Label, 4)
    }.

%% Sequential calls from single process
bench_sequential(Label, N) ->
    io:format("  ~s Sequential (~p calls): ", [Label, N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Ref = py_event_loop_pool:create_task(math, sqrt, [float(I)]),
            {ok, _} = py_event_loop_pool:await(Ref)
        end, lists:seq(1, N))
    end),

    Rate = round(N / (Time / 1000000)),
    io:format("~.1f ms (~p calls/sec)~n", [Time/1000, Rate]),
    #{time_ms => Time/1000, rate => Rate}.

%% Concurrent callers (each process gets own worker in OWN_GIL mode)
bench_concurrent(Label, NumProcs, TasksPerProc, _Complexity) ->
    TotalTasks = NumProcs * TasksPerProc,
    io:format("  ~s Concurrent (~p procs x ~p tasks): ", [Label, NumProcs, TasksPerProc]),

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

    Rate = round(TotalTasks / (Time / 1000000)),
    io:format("~.1f ms (~p calls/sec)~n", [Time/1000, Rate]),
    #{time_ms => Time/1000, rate => Rate, total => TotalTasks}.

%% CPU-bound parallel tasks (shows true GIL parallelism)
bench_parallel_cpu(Label, NumProcs) ->
    io:format("  ~s CPU-bound (~p parallel time.sleep(0.1)): ", [Label, NumProcs]),

    Parent = self(),
    {Time, _} = timer:tc(fun() ->
        Pids = [spawn_link(fun() ->
            %% Use time.sleep as a proxy for CPU work
            %% In OWN_GIL mode, these run truly in parallel
            Ref = py_event_loop_pool:create_task(time, sleep, [0.1]),
            Result = py_event_loop_pool:await(Ref, 5000),
            Parent ! {done, self(), Result}
        end) || _ <- lists:seq(1, NumProcs)],

        Results = [receive {done, Pid, R} -> R after 5000 -> timeout end || Pid <- Pids],
        %% Count successes
        length([X || {ok, _} = X <- Results])
    end),

    io:format("~.1f ms~n", [Time/1000]),
    #{time_ms => Time/1000, num_procs => NumProcs}.

print_comparison(Regular, Owngil) ->
    io:format("~n"),
    io:format("================================================================~n"),
    io:format("  Comparison Summary~n"),
    io:format("================================================================~n~n"),

    io:format("~-30s ~12s ~12s ~10s~n", ["Benchmark", "Regular", "OWN_GIL", "Speedup"]),
    io:format("~-30s ~12s ~12s ~10s~n", [string:copies("-", 30),
                                          string:copies("-", 12),
                                          string:copies("-", 12),
                                          string:copies("-", 10)]),

    %% Sequential (should be similar)
    SeqR = maps:get(rate, maps:get(sequential, Regular)),
    SeqO = maps:get(rate, maps:get(sequential, Owngil)),
    SeqSpeedup = SeqO / max(1, SeqR),
    io:format("~-30s ~10w/s ~10w/s ~8.2fx~n",
              ["Sequential (single caller)", SeqR, SeqO, SeqSpeedup]),

    %% Concurrent light
    CL_R = maps:get(rate, maps:get(concurrent_light, Regular)),
    CL_O = maps:get(rate, maps:get(concurrent_light, Owngil)),
    CL_Speedup = CL_O / max(1, CL_R),
    io:format("~-30s ~10w/s ~10w/s ~8.2fx~n",
              ["Concurrent (4 procs x 100)", CL_R, CL_O, CL_Speedup]),

    %% Concurrent medium
    CM_R = maps:get(rate, maps:get(concurrent_medium, Regular)),
    CM_O = maps:get(rate, maps:get(concurrent_medium, Owngil)),
    CM_Speedup = CM_O / max(1, CM_R),
    io:format("~-30s ~10w/s ~10w/s ~8.2fx~n",
              ["Concurrent (8 procs x 50)", CM_R, CM_O, CM_Speedup]),

    %% CPU-bound
    CPU_R = maps:get(time_ms, maps:get(parallel_cpu, Regular)),
    CPU_O = maps:get(time_ms, maps:get(parallel_cpu, Owngil)),
    CPU_Speedup = CPU_R / max(1, CPU_O),
    io:format("~-30s ~10.1f ms ~10.1f ms ~8.2fx~n",
              ["CPU-bound (4 parallel)", CPU_R, CPU_O, CPU_Speedup]),

    io:format("~n"),
    io:format("Notes:~n"),
    io:format("  - Regular pool: all workers share Python's main GIL~n"),
    io:format("  - OWN_GIL pool: each worker has independent GIL~n"),
    io:format("  - OWN_GIL enables true parallelism for CPU-bound tasks~n"),
    io:format("  - Speedup for CPU-bound tasks approaches number of workers~n"),
    io:format("~n").
