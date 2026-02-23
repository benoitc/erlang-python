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

%% @doc Benchmark module for event loop optimizations.
%%
%% This module provides benchmarks to measure:
%% - FD event throughput (events/second)
%% - Router message handling latency
%% - Combined NIF performance vs separate calls
%%
%% Usage:
%%   py_event_loop_bench:run().
%%   py_event_loop_bench:run(#{iterations => 10000, fds => 10}).
-module(py_event_loop_bench).

-export([
    run/0,
    run/1,
    bench_fd_events/1,
    bench_pending_queue/1,
    bench_high_concurrency/1
]).

%% Default benchmark parameters
-define(DEFAULT_ITERATIONS, 5000).
-define(DEFAULT_FDS, 10).
-define(DEFAULT_WARMUP, 500).

%% @doc Run all benchmarks with default parameters.
run() ->
    run(#{}).

%% @doc Run all benchmarks with custom parameters.
%% Options:
%%   iterations - Number of events to process (default: 5000)
%%   fds - Number of file descriptors to use (default: 10)
%%   warmup - Warmup iterations (default: 500)
run(Opts) ->
    Iterations = maps:get(iterations, Opts, ?DEFAULT_ITERATIONS),
    Fds = maps:get(fds, Opts, ?DEFAULT_FDS),
    Warmup = maps:get(warmup, Opts, ?DEFAULT_WARMUP),

    io:format("~n=== Event Loop Benchmark ===~n"),
    io:format("Iterations: ~p, FDs: ~p, Warmup: ~p~n~n", [Iterations, Fds, Warmup]),

    %% Ensure Python is initialized
    ok = py_nif:init(),

    Results = [
        {fd_events, bench_fd_events(#{iterations => Iterations, fds => Fds, warmup => Warmup})},
        {pending_queue, bench_pending_queue(#{iterations => Iterations * 10, warmup => Warmup})},
        {high_concurrency, bench_high_concurrency(#{iterations => Iterations, fds => Fds * 5})}
    ],

    io:format("~n=== Summary ===~n"),
    lists:foreach(fun({Name, {Rate, Unit}}) ->
        io:format("  ~-20s ~.2f ~s~n", [Name, Rate, Unit])
    end, Results),

    Results.

%% @doc Benchmark FD event throughput.
%% Measures how many FD read events can be processed per second.
bench_fd_events(Opts) ->
    Iterations = maps:get(iterations, Opts, ?DEFAULT_ITERATIONS),
    Fds = maps:get(fds, Opts, ?DEFAULT_FDS),
    Warmup = maps:get(warmup, Opts, ?DEFAULT_WARMUP),

    io:format("Benchmarking FD events...~n"),

    %% Create event loop and router
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create pipes and register readers
    Pipes = [begin
        {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),
        {ok, FdRes} = py_nif:add_reader(LoopRef, ReadFd, N),
        {ReadFd, WriteFd, FdRes}
    end || N <- lists:seq(1, Fds)],

    %% Warmup
    warmup_fd_events(Pipes, Warmup div Fds),

    %% Timed run
    Start = erlang:monotonic_time(microsecond),
    TotalEvents = run_fd_events(Pipes, Iterations div Fds),
    End = erlang:monotonic_time(microsecond),

    %% Cleanup
    lists:foreach(fun({ReadFd, WriteFd, _FdRes}) ->
        py_nif:close_test_fd(ReadFd),
        py_nif:close_test_fd(WriteFd)
    end, Pipes),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),

    ElapsedMs = (End - Start) / 1000,
    EventsPerSec = TotalEvents / (ElapsedMs / 1000),

    io:format("  Events: ~p, Time: ~.2f ms, Rate: ~.2f events/sec~n",
              [TotalEvents, ElapsedMs, EventsPerSec]),

    {EventsPerSec, "events/sec"}.

%% @doc Benchmark pending queue operations.
%% Measures dispatch_callback throughput without actual FD I/O.
bench_pending_queue(Opts) ->
    Iterations = maps:get(iterations, Opts, ?DEFAULT_ITERATIONS * 10),
    Warmup = maps:get(warmup, Opts, ?DEFAULT_WARMUP),

    io:format("Benchmarking pending queue dispatch...~n"),

    %% Create event loop
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Warmup
    warmup_pending_queue(LoopRef, Warmup),

    %% Timed run - dispatch many events and consume them
    Start = erlang:monotonic_time(microsecond),
    run_pending_queue(LoopRef, Iterations),
    End = erlang:monotonic_time(microsecond),

    %% Cleanup
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),

    ElapsedMs = (End - Start) / 1000,
    OpsPerSec = Iterations / (ElapsedMs / 1000),

    io:format("  Operations: ~p, Time: ~.2f ms, Rate: ~.2f ops/sec~n",
              [Iterations, ElapsedMs, OpsPerSec]),

    {OpsPerSec, "ops/sec"}.

%% @doc Benchmark high concurrency scenario.
%% Simulates many FDs being ready simultaneously.
bench_high_concurrency(Opts) ->
    Iterations = maps:get(iterations, Opts, ?DEFAULT_ITERATIONS),
    Fds = maps:get(fds, Opts, ?DEFAULT_FDS * 5),

    io:format("Benchmarking high concurrency (~p FDs)...~n", [Fds]),

    %% Create event loop and router
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create many pipes
    Pipes = [begin
        {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),
        {ok, FdRes} = py_nif:add_reader(LoopRef, ReadFd, N),
        {ReadFd, WriteFd, FdRes}
    end || N <- lists:seq(1, Fds)],

    %% Write to ALL pipes at once, then handle events
    Start = erlang:monotonic_time(microsecond),
    TotalEvents = run_burst_events(Pipes, Iterations div Fds),
    End = erlang:monotonic_time(microsecond),

    %% Cleanup
    lists:foreach(fun({ReadFd, WriteFd, _FdRes}) ->
        py_nif:close_test_fd(ReadFd),
        py_nif:close_test_fd(WriteFd)
    end, Pipes),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),

    ElapsedMs = (End - Start) / 1000,
    EventsPerSec = TotalEvents / (ElapsedMs / 1000),

    io:format("  Events: ~p, Time: ~.2f ms, Rate: ~.2f events/sec~n",
              [TotalEvents, ElapsedMs, EventsPerSec]),

    {EventsPerSec, "events/sec"}.

%% Internal functions

warmup_fd_events(Pipes, IterPerFd) ->
    run_fd_events(Pipes, IterPerFd),
    ok.

run_fd_events(Pipes, IterPerFd) ->
    lists:foldl(fun(_, Acc) ->
        lists:foreach(fun({_ReadFd, WriteFd, _FdRes}) ->
            py_nif:write_test_fd(WriteFd, <<"x">>)
        end, Pipes),
        %% Small delay to let events propagate
        timer:sleep(1),
        Acc + length(Pipes)
    end, 0, lists:seq(1, IterPerFd)).

warmup_pending_queue(LoopRef, Iterations) ->
    run_pending_queue(LoopRef, Iterations),
    ok.

run_pending_queue(LoopRef, Iterations) ->
    %% Dispatch events in batches and consume them
    BatchSize = 100,
    NumBatches = Iterations div BatchSize,
    lists:foreach(fun(BatchNum) ->
        %% Add a batch of events
        lists:foreach(fun(N) ->
            CallbackId = BatchNum * BatchSize + N,
            py_nif:dispatch_callback(LoopRef, CallbackId, read)
        end, lists:seq(1, BatchSize)),
        %% Consume them
        _ = py_nif:get_pending(LoopRef)
    end, lists:seq(1, NumBatches)).

run_burst_events(Pipes, IterPerFd) ->
    lists:foldl(fun(_, Acc) ->
        %% Write to ALL pipes simultaneously
        lists:foreach(fun({_ReadFd, WriteFd, _FdRes}) ->
            py_nif:write_test_fd(WriteFd, <<"burst">>)
        end, Pipes),
        %% Let events accumulate
        timer:sleep(5),
        Acc + length(Pipes)
    end, 0, lists:seq(1, IterPerFd)).
