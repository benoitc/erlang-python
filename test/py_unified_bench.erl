%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc Benchmarks for the unified event-driven architecture.
%%%
%%% Run with: rebar3 as test shell
%%% Then: py_unified_bench:run_all().
%%%
%%% Individual benchmarks:
%%% - py_unified_bench:bench_sync_call(N)
%%% - py_unified_bench:bench_async_call(N)
%%% - py_unified_bench:bench_concurrent(N, Concurrency)
%%% - py_unified_bench:bench_async_gather(N, BatchSize)
-module(py_unified_bench).

-export([
    run_all/0,
    bench_sync_call/1,
    bench_async_call/1,
    bench_concurrent/2,
    bench_async_gather/2,
    latency_stats/1
]).

%% @doc Run all benchmarks with default parameters
run_all() ->
    io:format("~n=== Unified Event-Driven Architecture Benchmarks ===~n~n"),

    %% Ensure application is started
    {ok, _} = application:ensure_all_started(erlang_python),
    timer:sleep(100),

    %% Sync call benchmark
    io:format("--- Synchronous py:call ---~n"),
    bench_sync_call(1000),

    %% Async call benchmark
    io:format("~n--- Async py:async_call ---~n"),
    bench_async_call(1000),

    %% Concurrent benchmark
    io:format("~n--- Concurrent Requests ---~n"),
    bench_concurrent(1000, 10),
    bench_concurrent(1000, 50),
    bench_concurrent(1000, 100),

    %% Async gather benchmark
    io:format("~n--- Async Gather ---~n"),
    bench_async_gather(100, 10),
    bench_async_gather(100, 50),

    io:format("~n=== Benchmarks Complete ===~n"),
    ok.

%% @doc Benchmark synchronous py:call
bench_sync_call(N) ->
    %% Warmup
    _ = [py:call(math, sqrt, [I]) || I <- lists:seq(1, 100)],

    %% Measure
    {Time, Results} = timer:tc(fun() ->
        [py:call(math, sqrt, [I]) || I <- lists:seq(1, N)]
    end),

    TimeMs = Time / 1000,
    OpsPerSec = trunc(N / (Time / 1_000_000)),
    AvgUs = trunc(Time / N),

    io:format("  N=~p: ~.1f ms total, ~p ops/sec, ~p us/op avg~n",
              [N, TimeMs, OpsPerSec, AvgUs]),

    %% Verify all succeeded
    Successes = length([R || {ok, _} = R <- Results]),
    io:format("  Success rate: ~p/~p~n", [Successes, N]),
    ok.

%% @doc Benchmark async py:async_call with latency stats
bench_async_call(N) ->
    %% Use asyncio.sleep(0) as a minimal async operation
    %% Warmup
    WarmupRefs = [py:async_call(asyncio, sleep, [0]) || _ <- lists:seq(1, 100)],
    _ = [py:async_await(Ref, 5000) || Ref <- WarmupRefs],

    %% Measure individual latencies
    Latencies = lists:map(fun(_I) ->
        Start = erlang:monotonic_time(microsecond),
        Ref = py:async_call(asyncio, sleep, [0]),
        {ok, _} = py:async_await(Ref, 5000),
        erlang:monotonic_time(microsecond) - Start
    end, lists:seq(1, N)),

    TotalTime = lists:sum(Latencies),
    TimeMs = TotalTime / 1000,
    OpsPerSec = trunc(N / (TotalTime / 1_000_000)),

    io:format("  N=~p: ~.1f ms total, ~p ops/sec~n", [N, TimeMs, OpsPerSec]),
    latency_stats(Latencies),
    ok.

%% @doc Benchmark concurrent requests
bench_concurrent(N, Concurrency) ->
    Parent = self(),

    %% Warmup
    _ = [py:call(math, sqrt, [I]) || I <- lists:seq(1, 100)],

    Start = erlang:monotonic_time(microsecond),

    %% Spawn workers
    Workers = [spawn_link(fun() ->
        Results = [begin
            T0 = erlang:monotonic_time(microsecond),
            {ok, _} = py:call(math, sqrt, [I]),
            erlang:monotonic_time(microsecond) - T0
        end || I <- lists:seq(WorkerId, N, Concurrency)],
        Parent ! {done, self(), Results}
    end) || WorkerId <- lists:seq(1, Concurrency)],

    %% Collect results
    AllLatencies = lists:flatten([receive
        {done, W, Lats} -> Lats
    after 30000 ->
        io:format("  Timeout waiting for worker ~p~n", [W]),
        []
    end || W <- Workers]),

    TotalTime = erlang:monotonic_time(microsecond) - Start,
    TimeMs = TotalTime / 1000,
    OpsPerSec = trunc(N / (TotalTime / 1_000_000)),

    io:format("  N=~p, Concurrency=~p: ~.1f ms total, ~p ops/sec~n",
              [N, Concurrency, TimeMs, OpsPerSec]),
    latency_stats(AllLatencies),
    ok.

%% @doc Benchmark async_gather with different batch sizes
bench_async_gather(Batches, BatchSize) ->
    %% Use asyncio.sleep(0) for minimal async operation
    %% Warmup
    _ = py:async_gather([{asyncio, sleep, [0]} || _ <- lists:seq(1, 10)]),

    %% Measure
    Latencies = lists:map(fun(_) ->
        Calls = [{asyncio, sleep, [0]} || _ <- lists:seq(1, BatchSize)],
        Start = erlang:monotonic_time(microsecond),
        {ok, _Results} = py:async_gather(Calls),
        erlang:monotonic_time(microsecond) - Start
    end, lists:seq(1, Batches)),

    TotalTime = lists:sum(Latencies),
    TotalOps = Batches * BatchSize,
    TimeMs = TotalTime / 1000,
    OpsPerSec = trunc(TotalOps / (TotalTime / 1_000_000)),
    AvgBatchUs = trunc(TotalTime / Batches),

    io:format("  Batches=~p, BatchSize=~p: ~.1f ms total, ~p ops/sec, ~p us/batch~n",
              [Batches, BatchSize, TimeMs, OpsPerSec, AvgBatchUs]),
    ok.

%% @doc Calculate and print latency statistics (p50, p90, p99, p999)
latency_stats(Latencies) when length(Latencies) > 0 ->
    Sorted = lists:sort(Latencies),
    Len = length(Sorted),

    P50 = lists:nth(max(1, trunc(Len * 0.50)), Sorted),
    P90 = lists:nth(max(1, trunc(Len * 0.90)), Sorted),
    P99 = lists:nth(max(1, trunc(Len * 0.99)), Sorted),
    P999 = lists:nth(max(1, min(Len, trunc(Len * 0.999))), Sorted),
    Min = hd(Sorted),
    Max = lists:last(Sorted),
    Avg = trunc(lists:sum(Latencies) / Len),

    io:format("  Latency (us): min=~p, avg=~p, p50=~p, p90=~p, p99=~p, p999=~p, max=~p~n",
              [Min, Avg, P50, P90, P99, P999, Max]),
    ok;
latency_stats([]) ->
    io:format("  No latency data~n"),
    ok.
