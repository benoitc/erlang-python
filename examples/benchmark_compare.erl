#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark for comparing performance between erlang_python versions.

-mode(compile).

main(_Args) ->
    io:format("~n"),
    io:format("╔══════════════════════════════════════════════════════════════╗~n"),
    io:format("║          erlang_python Version Comparison Benchmark          ║~n"),
    io:format("╚══════════════════════════════════════════════════════════════╝~n~n"),

    {ok, _} = application:ensure_all_started(erlang_python),

    print_system_info(),

    io:format("Running benchmarks...~n"),
    io:format("════════════════════════════════════════════════════════════════~n~n"),

    %% Run all benchmarks and collect results
    Results = [
        bench_sync_call(),
        bench_sync_eval(),
        bench_cast_single(),
        bench_cast_multiple(),
        bench_cast_parallel(),
        bench_concurrent_sync(),
        bench_concurrent_cast()
    ],

    %% Print summary
    print_summary(Results),

    halt(0).

print_system_info() ->
    io:format("System Information~n"),
    io:format("──────────────────~n"),
    io:format("  Erlang/OTP:     ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers:     ~p~n", [erlang:system_info(schedulers)]),
    {ok, PyVer} = py:version(),
    io:format("  Python:         ~s~n", [PyVer]),
    io:format("  Execution Mode: ~p~n", [py:execution_mode()]),
    io:format("  Max Concurrent: ~p~n", [py_semaphore:max_concurrent()]),
    io:format("~n").

%% ============================================================================
%% Benchmark: Synchronous Calls
%% ============================================================================

bench_sync_call() ->
    Name = "Sync py:call (math.sqrt)",
    N = 1000,

    io:format("▶ ~s~n", [Name]),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            {ok, _} = py:call(math, sqrt, [I])
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerCall = TimeMs / N,
    Throughput = round(N / (TimeMs / 1000)),

    io:format("  Total:      ~.2f ms~n", [TimeMs]),
    io:format("  Per call:   ~.3f ms~n", [PerCall]),
    io:format("  Throughput: ~p calls/sec~n~n", [Throughput]),

    {Name, PerCall, Throughput}.

bench_sync_eval() ->
    Name = "Sync py:eval (arithmetic)",
    N = 1000,

    io:format("▶ ~s~n", [Name]),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            {ok, _} = py:eval(<<"x * x + y">>, #{x => I, y => I})
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerCall = TimeMs / N,
    Throughput = round(N / (TimeMs / 1000)),

    io:format("  Total:      ~.2f ms~n", [TimeMs]),
    io:format("  Per eval:   ~.3f ms~n", [PerCall]),
    io:format("  Throughput: ~p evals/sec~n~n", [Throughput]),

    {Name, PerCall, Throughput}.

%% ============================================================================
%% Benchmark: Cast (non-blocking) Calls
%% ============================================================================

bench_cast_single() ->
    Name = "Cast py:cast single",
    N = 1000,

    io:format("▶ ~s~n", [Name]),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Ref = py:cast(math, sqrt, [I]),
            {ok, _} = py:await(Ref, 5000)
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerCall = TimeMs / N,
    Throughput = round(N / (TimeMs / 1000)),

    io:format("  Total:      ~.2f ms~n", [TimeMs]),
    io:format("  Per call:   ~.3f ms~n", [PerCall]),
    io:format("  Throughput: ~p calls/sec~n~n", [Throughput]),

    {Name, PerCall, Throughput}.

bench_cast_multiple() ->
    Name = "Cast py:cast batch (10 calls)",
    N = 100,

    io:format("▶ ~s~n", [Name]),
    io:format("  Batches: ~p (10 cast calls each)~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(Batch) ->
            %% Start 10 cast calls
            Refs = [py:cast(math, sqrt, [Batch * 10 + I])
                    || I <- lists:seq(1, 10)],
            %% Await all
            [{ok, _} = py:await(Ref, 5000) || Ref <- Refs]
        end, lists:seq(1, N))
    end),

    TotalCalls = N * 10,
    TimeMs = Time / 1000,
    PerBatch = TimeMs / N,
    Throughput = round(TotalCalls / (TimeMs / 1000)),

    io:format("  Total:      ~.2f ms~n", [TimeMs]),
    io:format("  Per batch:  ~.3f ms~n", [PerBatch]),
    io:format("  Throughput: ~p calls/sec~n~n", [Throughput]),

    {Name, PerBatch, Throughput}.

bench_cast_parallel() ->
    Name = "Cast py:cast parallel (10 concurrent)",
    N = 100,

    io:format("▶ ~s~n", [Name]),
    io:format("  Batches: ~p (10 concurrent calls each)~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(Batch) ->
            %% Start 10 cast calls in parallel
            Refs = [py:cast(math, factorial, [20 + (Batch rem 10)])
                    || _ <- lists:seq(1, 10)],
            %% Await all results
            [py:await(Ref, 5000) || Ref <- Refs]
        end, lists:seq(1, N))
    end),

    TotalCalls = N * 10,
    TimeMs = Time / 1000,
    PerBatch = TimeMs / N,
    Throughput = round(TotalCalls / (TimeMs / 1000)),

    io:format("  Total:      ~.2f ms~n", [TimeMs]),
    io:format("  Per batch:  ~.3f ms~n", [PerBatch]),
    io:format("  Throughput: ~p calls/sec~n~n", [Throughput]),

    {Name, PerBatch, Throughput}.

%% ============================================================================
%% Benchmark: Concurrent Operations
%% ============================================================================

bench_concurrent_sync() ->
    Name = "Concurrent sync (50 procs x 20 calls)",
    NumProcs = 50,
    CallsPerProc = 20,
    TotalCalls = NumProcs * CallsPerProc,

    io:format("▶ ~s~n", [Name]),
    io:format("  Processes: ~p, Calls/proc: ~p, Total: ~p~n",
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

    TimeMs = Time / 1000,
    Throughput = round(TotalCalls / (TimeMs / 1000)),

    io:format("  Total:      ~.2f ms~n", [TimeMs]),
    io:format("  Throughput: ~p calls/sec~n~n", [Throughput]),

    {Name, TimeMs, Throughput}.

bench_concurrent_cast() ->
    Name = "Concurrent cast (50 procs x 5 casts)",
    NumProcs = 50,
    CallsPerProc = 5,
    TotalCalls = NumProcs * CallsPerProc,

    io:format("▶ ~s~n", [Name]),
    io:format("  Processes: ~p, Casts/proc: ~p, Total: ~p~n",
              [NumProcs, CallsPerProc, TotalCalls]),

    Parent = self(),

    {Time, _} = timer:tc(fun() ->
        Pids = [spawn_link(fun() ->
            lists:foreach(fun(I) ->
                Ref = py:cast(math, factorial, [20 + I]),
                {ok, _} = py:await(Ref, 5000)
            end, lists:seq(1, CallsPerProc)),
            Parent ! {done, self()}
        end) || _ <- lists:seq(1, NumProcs)],

        [receive {done, Pid} -> ok end || Pid <- Pids]
    end),

    TimeMs = Time / 1000,
    Throughput = round(TotalCalls / (TimeMs / 1000)),

    io:format("  Total:      ~.2f ms~n", [TimeMs]),
    io:format("  Throughput: ~p calls/sec~n~n", [Throughput]),

    {Name, TimeMs, Throughput}.

%% ============================================================================
%% Summary
%% ============================================================================

print_summary(Results) ->
    io:format("════════════════════════════════════════════════════════════════~n"),
    io:format("SUMMARY~n"),
    io:format("════════════════════════════════════════════════════════════════~n~n"),

    io:format("┌────────────────────────────────────────┬───────────┬───────────┐~n"),
    io:format("│ Benchmark                              │ Latency   │ Thru/sec  │~n"),
    io:format("├────────────────────────────────────────┼───────────┼───────────┤~n"),

    lists:foreach(fun({Name, Latency, Throughput}) ->
        LatStr = if
            Latency < 0 -> "N/A";
            Latency < 1000 -> io_lib:format("~.3f ms", [Latency]);
            true -> io_lib:format("~.1f ms", [Latency])
        end,
        ThrStr = if
            Throughput =:= 0 -> "N/A";
            true -> integer_to_list(Throughput)
        end,
        io:format("│ ~-38s │ ~-9s │ ~-9s │~n",
                  [string:slice(Name, 0, 38), LatStr, ThrStr])
    end, Results),

    io:format("└────────────────────────────────────────┴───────────┴───────────┘~n~n"),

    io:format("Key metrics to compare between versions:~n"),
    io:format("  * Sync call performance should be similar~n"),
    io:format("  * Cast (non-blocking) call overhead~n"),
    io:format("  * Concurrent throughput with multiple processes~n~n").
