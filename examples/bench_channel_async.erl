#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script for Channel API: Sync vs Async comparison.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_channel_async.erl

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("Channel Benchmark: Sync vs Async~n"),
    io:format("========================================~n~n"),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    ok = py_channel:register_callbacks(),

    %% Initialize event loop for async operations (gen_server)
    %% Already started by application, just ensure it's running
    case py_event_loop:start_link() of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    %% Print system info
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    {ok, PyVer} = py:version(),
    io:format("  Python: ~s~n", [PyVer]),
    io:format("~n"),

    %% Setup Python async channel receiver
    setup_python_async_receiver(),

    %% Run benchmarks
    run_sync_channel_bench(),
    run_async_channel_bench(),
    run_comparison_bench(),

    io:format("~n========================================~n"),
    io:format("Benchmark Complete~n"),
    io:format("========================================~n"),

    halt(0).

setup_python_async_receiver() ->
    io:format("Python channel helpers ready.~n~n").

run_sync_channel_bench() ->
    io:format("--- Sync Channel Benchmark ---~n"),
    io:format("(Erlang send + NIF try_receive - pure Erlang)~n~n"),

    Sizes = [64, 1024, 16384],
    Iterations = 5000,

    io:format("~8s | ~12s | ~12s~n",
              ["Size", "Throughput", "Avg (us)"]),
    io:format("~s~n", [string:copies("-", 38)]),

    lists:foreach(fun(Size) ->
        {ok, Ch} = py_channel:new(),
        Data = binary:copy(<<0>>, Size),

        %% Fill channel
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data)
        end, lists:seq(1, Iterations)),

        %% Time receiving all messages via NIF
        Start = erlang:monotonic_time(microsecond),
        receive_all_sync(Ch, Iterations),
        End = erlang:monotonic_time(microsecond),

        TotalTime = (End - Start) / 1000000,
        AvgUs = (TotalTime / Iterations) * 1000000,
        Throughput = round(Iterations / TotalTime),

        io:format("~8B | ~12w | ~12.2f~n", [Size, Throughput, AvgUs]),

        py_channel:close(Ch)
    end, Sizes),
    ok.

receive_all_sync(_Ch, 0) -> ok;
receive_all_sync(Ch, N) ->
    {ok, _} = py_nif:channel_try_receive(Ch),
    receive_all_sync(Ch, N - 1).

run_async_channel_bench() ->
    io:format("~n--- Async Task API Benchmark ---~n"),
    io:format("(py_event_loop:create_task + await using stdlib)~n~n"),

    Iterations = 1000,

    io:format("~15s | ~12s | ~12s~n",
              ["Operation", "Throughput", "Avg (us)"]),
    io:format("~s~n", [string:copies("-", 44)]),

    %% Test math.sqrt via async task API
    Start1 = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_) ->
        Ref = py_event_loop:create_task(math, sqrt, [2.0]),
        {ok, _} = py_event_loop:await(Ref, 5000)
    end, lists:seq(1, Iterations)),
    End1 = erlang:monotonic_time(microsecond),

    TotalTime1 = (End1 - Start1) / 1000000,
    AvgUs1 = (TotalTime1 / Iterations) * 1000000,
    Throughput1 = round(Iterations / TotalTime1),

    io:format("~15s | ~12w | ~12.2f~n", ["math.sqrt", Throughput1, AvgUs1]),

    %% Test concurrent tasks (20 processes, 50 each)
    NumProcs = 20,
    TasksPerProc = 50,
    TotalTasks = NumProcs * TasksPerProc,

    Start2 = erlang:monotonic_time(microsecond),
    Parent = self(),
    lists:foreach(fun(_) ->
        spawn(fun() ->
            lists:foreach(fun(_) ->
                Ref = py_event_loop:create_task(math, sqrt, [2.0]),
                {ok, _} = py_event_loop:await(Ref, 5000)
            end, lists:seq(1, TasksPerProc)),
            Parent ! done
        end)
    end, lists:seq(1, NumProcs)),
    wait_all(NumProcs),
    End2 = erlang:monotonic_time(microsecond),

    TotalTime2 = (End2 - Start2) / 1000000,
    AvgUs2 = (TotalTime2 / TotalTasks) * 1000000,
    Throughput2 = round(TotalTasks / TotalTime2),

    io:format("~15s | ~12w | ~12.2f~n", ["concurrent", Throughput2, AvgUs2]),

    ok.

wait_all(0) -> ok;
wait_all(N) ->
    receive done -> wait_all(N - 1) end.

run_comparison_bench() ->
    io:format("~n--- Sync vs Async Comparison ---~n"),
    io:format("(Channel operations: NIF sync vs py:call)~n~n"),

    Size = 1024,
    Iterations = 1000,

    io:format("Message size: ~B bytes, Iterations: ~B~n~n", [Size, Iterations]),
    io:format("~15s | ~12s | ~12s~n",
              ["Method", "Time (ms)", "Throughput"]),
    io:format("~s~n", [string:copies("-", 45)]),

    Data = binary:copy(<<0>>, Size),

    %% NIF-level sync (fastest - no Python)
    {ok, NifCh} = py_channel:new(),
    lists:foreach(fun(_) -> ok = py_channel:send(NifCh, Data) end, lists:seq(1, Iterations)),
    NifStart = erlang:monotonic_time(microsecond),
    receive_all_sync(NifCh, Iterations),
    NifEnd = erlang:monotonic_time(microsecond),
    NifTime = (NifEnd - NifStart) / 1000,
    NifThroughput = round(Iterations / (NifTime / 1000)),
    io:format("~15s | ~12.2f | ~12w~n", ["NIF sync", NifTime, NifThroughput]),
    py_channel:close(NifCh),

    %% py:call sync (Python stdlib function)
    PyStart = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_) ->
        {ok, _} = py:call(math, sqrt, [2.0])
    end, lists:seq(1, Iterations)),
    PyEnd = erlang:monotonic_time(microsecond),
    PyTime = (PyEnd - PyStart) / 1000,
    PyThroughput = round(Iterations / (PyTime / 1000)),
    io:format("~15s | ~12.2f | ~12w~n", ["py:call sync", PyTime, PyThroughput]),

    %% Async task API (sequential)
    AsyncStart = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_) ->
        Ref = py_event_loop:create_task(math, sqrt, [2.0]),
        {ok, _} = py_event_loop:await(Ref, 5000)
    end, lists:seq(1, Iterations)),
    AsyncEnd = erlang:monotonic_time(microsecond),
    AsyncTime = (AsyncEnd - AsyncStart) / 1000,
    AsyncThroughput = round(Iterations / (AsyncTime / 1000)),
    io:format("~15s | ~12.2f | ~12w~n", ["async task", AsyncTime, AsyncThroughput]),

    %% Spawn task (fire-and-forget, then collect)
    SpawnStart = erlang:monotonic_time(microsecond),
    Refs = lists:map(fun(_) ->
        py_event_loop:create_task(math, sqrt, [2.0])
    end, lists:seq(1, Iterations)),
    %% Await all
    lists:foreach(fun(R) ->
        {ok, _} = py_event_loop:await(R, 5000)
    end, Refs),
    SpawnEnd = erlang:monotonic_time(microsecond),
    SpawnTime = (SpawnEnd - SpawnStart) / 1000,
    SpawnThroughput = round(Iterations / (SpawnTime / 1000)),
    io:format("~15s | ~12.2f | ~12w~n", ["spawn batch", SpawnTime, SpawnThroughput]),

    %% Print summary
    io:format("~n"),
    io:format("NIF sync is ~.1fx faster than py:call~n", [PyTime / NifTime]),
    io:format("NIF sync is ~.1fx faster than async task~n", [AsyncTime / NifTime]),
    io:format("Spawn batch is ~.1fx faster than sequential async~n", [AsyncTime / SpawnTime]),
    ok.
