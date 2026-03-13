#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark: Channel.receive() (sync) vs Channel.async_receive() (asyncio)
%%%
%%% Compares Python-side synchronous blocking receive against asyncio async_receive.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_channel_receive.erl

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("Channel Receive Benchmark~n"),
    io:format("sync receive() vs async_receive()~n"),
    io:format("========================================~n~n"),

    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    ok = py_channel:register_callbacks(),

    %% Print system info
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    {ok, PyVer} = py:version(),
    io:format("  Python: ~s~n~n", [PyVer]),

    %% Run benchmarks
    run_sync_receive_bench(),
    run_async_receive_bench(),
    run_comparison(),

    io:format("~n========================================~n"),
    io:format("Benchmark Complete~n"),
    io:format("========================================~n"),

    halt(0).

run_sync_receive_bench() ->
    io:format("--- Sync Channel.receive() Benchmark ---~n"),
    io:format("(Python blocking receive via erlang.call)~n~n"),

    Sizes = [64, 1024, 16384],
    Iterations = 2000,

    io:format("~8s | ~12s | ~12s~n", ["Size", "Throughput", "Avg (us)"]),
    io:format("~s~n", [string:copies("-", 38)]),

    Ctx = py:context(1),

    %% Define sync receiver function
    ok = py:exec(Ctx, <<"
from erlang import Channel

def sync_receive_n(ch_ref, n):
    ch = Channel(ch_ref)
    for _ in range(n):
        ch.receive()
    return n
">>),

    lists:foreach(fun(Size) ->
        {ok, Ch} = py_channel:new(),
        Data = binary:copy(<<0>>, Size),

        %% Fill channel
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data)
        end, lists:seq(1, Iterations)),

        %% Time Python sync receive
        Start = erlang:monotonic_time(microsecond),
        {ok, Iterations} = py:eval(Ctx, <<"sync_receive_n(ch, n)">>,
                                    #{<<"ch">> => Ch, <<"n">> => Iterations}),
        End = erlang:monotonic_time(microsecond),

        TotalTime = (End - Start) / 1000000,
        AvgUs = (TotalTime / Iterations) * 1000000,
        Throughput = round(Iterations / TotalTime),

        io:format("~8B | ~12w | ~12.2f~n", [Size, Throughput, AvgUs]),

        py_channel:close(Ch)
    end, Sizes),
    ok.

run_async_receive_bench() ->
    io:format("~n--- Async Channel.async_receive() Benchmark ---~n"),
    io:format("(Python asyncio via erlang.run)~n~n"),

    Sizes = [64, 1024, 16384],
    Iterations = 2000,

    io:format("~8s | ~12s | ~12s~n", ["Size", "Throughput", "Avg (us)"]),
    io:format("~s~n", [string:copies("-", 38)]),

    Ctx = py:context(1),

    %% Define async receiver function
    ok = py:exec(Ctx, <<"
import erlang
from erlang import Channel

async def async_receive_n(ch_ref, n):
    ch = Channel(ch_ref)
    for _ in range(n):
        await ch.async_receive()
    return n

def run_async_receive(ch_ref, n):
    return erlang.run(async_receive_n(ch_ref, n))
">>),

    lists:foreach(fun(Size) ->
        {ok, Ch} = py_channel:new(),
        Data = binary:copy(<<0>>, Size),

        %% Fill channel
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data)
        end, lists:seq(1, Iterations)),

        %% Time Python async receive
        Start = erlang:monotonic_time(microsecond),
        {ok, Iterations} = py:eval(Ctx, <<"run_async_receive(ch, n)">>,
                                    #{<<"ch">> => Ch, <<"n">> => Iterations}),
        End = erlang:monotonic_time(microsecond),

        TotalTime = (End - Start) / 1000000,
        AvgUs = (TotalTime / Iterations) * 1000000,
        Throughput = round(Iterations / TotalTime),

        io:format("~8B | ~12w | ~12.2f~n", [Size, Throughput, AvgUs]),

        py_channel:close(Ch)
    end, Sizes),
    ok.

run_comparison() ->
    io:format("~n--- Direct Comparison ---~n"),
    io:format("(1KB messages, 2000 iterations)~n~n"),

    Size = 1024,
    Iterations = 2000,

    Ctx = py:context(1),

    %% Define both functions
    ok = py:exec(Ctx, <<"
import erlang
from erlang import Channel

def sync_receive_n(ch_ref, n):
    ch = Channel(ch_ref)
    for _ in range(n):
        ch.receive()
    return n

async def async_receive_n(ch_ref, n):
    ch = Channel(ch_ref)
    for _ in range(n):
        await ch.async_receive()
    return n

def run_async_receive(ch_ref, n):
    return erlang.run(async_receive_n(ch_ref, n))
">>),

    io:format("~15s | ~12s | ~12s~n", ["Method", "Time (ms)", "Throughput"]),
    io:format("~s~n", [string:copies("-", 45)]),

    Data = binary:copy(<<0>>, Size),

    %% Sync receive benchmark
    {ok, SyncCh} = py_channel:new(),
    lists:foreach(fun(_) -> ok = py_channel:send(SyncCh, Data) end, lists:seq(1, Iterations)),
    SyncStart = erlang:monotonic_time(microsecond),
    {ok, Iterations} = py:eval(Ctx, <<"sync_receive_n(ch, n)">>,
                                #{<<"ch">> => SyncCh, <<"n">> => Iterations}),
    SyncEnd = erlang:monotonic_time(microsecond),
    SyncTime = (SyncEnd - SyncStart) / 1000,
    SyncThroughput = round(Iterations / (SyncTime / 1000)),
    io:format("~15s | ~12.2f | ~12w~n", ["sync receive", SyncTime, SyncThroughput]),
    py_channel:close(SyncCh),

    %% Async receive benchmark
    {ok, AsyncCh} = py_channel:new(),
    lists:foreach(fun(_) -> ok = py_channel:send(AsyncCh, Data) end, lists:seq(1, Iterations)),
    AsyncStart = erlang:monotonic_time(microsecond),
    {ok, Iterations} = py:eval(Ctx, <<"run_async_receive(ch, n)">>,
                                #{<<"ch">> => AsyncCh, <<"n">> => Iterations}),
    AsyncEnd = erlang:monotonic_time(microsecond),
    AsyncTime = (AsyncEnd - AsyncStart) / 1000,
    AsyncThroughput = round(Iterations / (AsyncTime / 1000)),
    io:format("~15s | ~12.2f | ~12w~n", ["async_receive", AsyncTime, AsyncThroughput]),
    py_channel:close(AsyncCh),

    %% NIF baseline (no Python)
    {ok, NifCh} = py_channel:new(),
    lists:foreach(fun(_) -> ok = py_channel:send(NifCh, Data) end, lists:seq(1, Iterations)),
    NifStart = erlang:monotonic_time(microsecond),
    receive_all_nif(NifCh, Iterations),
    NifEnd = erlang:monotonic_time(microsecond),
    NifTime = (NifEnd - NifStart) / 1000,
    NifThroughput = round(Iterations / (NifTime / 1000)),
    io:format("~15s | ~12.2f | ~12w~n", ["NIF baseline", NifTime, NifThroughput]),
    py_channel:close(NifCh),

    %% Summary
    io:format("~n"),
    io:format("sync receive is ~.1fx faster than async_receive~n", [AsyncTime / SyncTime]),
    io:format("NIF baseline is ~.1fx faster than sync receive~n", [SyncTime / NifTime]),
    ok.

receive_all_nif(_Ch, 0) -> ok;
receive_all_nif(Ch, N) ->
    {ok, _} = py_nif:channel_try_receive(Ch),
    receive_all_nif(Ch, N - 1).
