#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark comparing SHARED_GIL vs OWN_GIL context modes.
%%%
%%% OWN_GIL mode creates a dedicated pthread with its own Python GIL,
%%% enabling true parallel execution for CPU-bound workloads.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_owngil.erl

-mode(compile).

main(_Args) ->
    io:format("~n"),
    io:format("========================================================~n"),
    io:format("  OWN_GIL vs SHARED_GIL Benchmark~n"),
    io:format("========================================================~n~n"),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),

    %% Print system info
    print_system_info(),

    case py_nif:subinterp_supported() of
        true ->
            bench_single_latency(),
            bench_parallel_throughput(),
            bench_cpu_speedup();
        false ->
            io:format("~n[ERROR] OWN_GIL requires Python 3.12+~n"),
            io:format("        Current Python version does not support subinterpreters.~n~n")
    end,

    halt(0).

print_system_info() ->
    io:format("System Information~n"),
    io:format("------------------~n"),
    io:format("  Erlang/OTP:       ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers:       ~p~n", [erlang:system_info(schedulers)]),
    {ok, PyVer} = py:version(),
    io:format("  Python:           ~s~n", [PyVer]),
    io:format("  Subinterp:        ~p~n", [py_nif:subinterp_supported()]),
    io:format("~n").

%% ============================================================================
%% Benchmark: Single Context Latency
%% ============================================================================

bench_single_latency() ->
    io:format("1. Single Context Latency (1000 calls to math.sqrt)~n"),
    io:format("   ~-15s ~10s ~12s~n", ["Mode", "us/call", "calls/sec"]),
    io:format("   ~-15s ~10s ~12s~n", ["----", "-------", "---------"]),

    lists:foreach(fun({Label, Mode}) ->
        {ok, Ctx} = py_context:start_link(1, Mode),

        %% Warmup
        [py_context:call(Ctx, math, sqrt, [N], #{}) || N <- lists:seq(1, 100)],

        %% Benchmark
        Iterations = 1000,
        Start = erlang:monotonic_time(microsecond),
        [py_context:call(Ctx, math, sqrt, [N], #{}) || N <- lists:seq(1, Iterations)],
        Elapsed = erlang:monotonic_time(microsecond) - Start,

        UsPerCall = Elapsed / Iterations,
        CallsPerSec = round(Iterations * 1000000 / Elapsed),
        io:format("   ~-15s ~10.1f ~12w~n", [Label, UsPerCall, CallsPerSec]),

        py_context:stop(Ctx)
    end, [{subinterp, subinterp}, {owngil, owngil}]),
    io:format("~n").

%% ============================================================================
%% Benchmark: Parallel Throughput
%% ============================================================================

bench_parallel_throughput() ->
    io:format("2. Parallel Throughput (4 contexts x 250 calls)~n"),
    io:format("   ~-15s ~10s ~12s~n", ["Mode", "Total ms", "calls/sec"]),
    io:format("   ~-15s ~10s ~12s~n", ["----", "--------", "---------"]),

    NumContexts = 4,
    CallsPerContext = 250,
    TotalCalls = NumContexts * CallsPerContext,

    lists:foreach(fun({Label, Mode}) ->
        Contexts = [begin
            {ok, Ctx} = py_context:start_link(N, Mode),
            Ctx
        end || N <- lists:seq(1, NumContexts)],

        %% Warmup
        [py_context:call(Ctx, math, sqrt, [16], #{}) || Ctx <- Contexts],

        %% Parallel benchmark
        Parent = self(),
        Start = erlang:monotonic_time(millisecond),

        Pids = [spawn(fun() ->
            [py_context:call(Ctx, math, sqrt, [N], #{})
             || N <- lists:seq(1, CallsPerContext)],
            Parent ! {done, self()}
        end) || Ctx <- Contexts],

        [receive {done, Pid} -> ok end || Pid <- Pids],

        Elapsed = erlang:monotonic_time(millisecond) - Start,
        CallsPerSec = round(TotalCalls * 1000 / max(1, Elapsed)),
        io:format("   ~-15s ~10w ~12w~n", [Label, Elapsed, CallsPerSec]),

        [py_context:stop(Ctx) || Ctx <- Contexts]
    end, [{subinterp, subinterp}, {owngil, owngil}]),
    io:format("~n").

%% ============================================================================
%% Benchmark: CPU-Bound Speedup
%% ============================================================================

bench_cpu_speedup() ->
    io:format("3. CPU-Bound Speedup (sum(range(500000)) x 4 contexts)~n"),
    io:format("   ~-15s ~10s ~10s ~10s~n", ["Mode", "Seq ms", "Par ms", "Speedup"]),
    io:format("   ~-15s ~10s ~10s ~10s~n", ["----", "------", "------", "-------"]),

    NumContexts = 4,
    Code = <<"sum(range(500000))">>,

    lists:foreach(fun({Label, Mode}) ->
        Contexts = [begin
            {ok, Ctx} = py_context:start_link(N, Mode),
            Ctx
        end || N <- lists:seq(1, NumContexts)],

        %% Sequential execution
        SeqStart = erlang:monotonic_time(millisecond),
        [py_context:eval(Ctx, Code, #{}) || Ctx <- Contexts],
        SeqTime = erlang:monotonic_time(millisecond) - SeqStart,

        %% Parallel execution
        Parent = self(),
        ParStart = erlang:monotonic_time(millisecond),
        Pids = [spawn(fun() ->
            py_context:eval(Ctx, Code, #{}),
            Parent ! {done, self()}
        end) || Ctx <- Contexts],
        [receive {done, Pid} -> ok end || Pid <- Pids],
        ParTime = erlang:monotonic_time(millisecond) - ParStart,

        Speedup = SeqTime / max(1, ParTime),
        io:format("   ~-15s ~10w ~10w ~10.2fx~n", [Label, SeqTime, ParTime, Speedup]),

        [py_context:stop(Ctx) || Ctx <- Contexts]
    end, [{subinterp, subinterp}, {owngil, owngil}]),

    io:format("~n"),
    io:format("Notes:~n"),
    io:format("  - SHARED_GIL (subinterp) contexts share Python's GIL~n"),
    io:format("  - OWN_GIL contexts have independent GILs for true parallelism~n"),
    io:format("  - OWN_GIL speedup should approach number of CPU cores~n"),
    io:format("~n").
