#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script comparing PyBuffer with Channel API.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_py_buffer.erl

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("PyBuffer vs Channel Benchmark~n"),
    io:format("========================================~n~n"),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    ok = py_channel:register_callbacks(),

    %% Print system info
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    {ok, PyVer} = py:version(),
    io:format("  Python: ~s~n", [PyVer]),
    io:format("~n"),

    %% Run benchmarks
    run_write_throughput_bench(),
    run_read_throughput_bench(),
    run_buffer_vs_channel_bench(),
    run_streaming_bench(),

    io:format("~n========================================~n"),
    io:format("Benchmark Complete~n"),
    io:format("========================================~n"),

    halt(0).

run_write_throughput_bench() ->
    io:format("~n--- PyBuffer Write Throughput ---~n"),
    io:format("Writes per batch: 1000~n~n"),

    Sizes = [64, 256, 1024, 4096, 16384],

    io:format("~8s | ~12s | ~12s~n",
              ["Size", "Writes/sec", "MB/sec"]),
    io:format("~s~n", [string:copies("-", 38)]),

    lists:foreach(fun(Size) ->
        {ok, Buf} = py_buffer:new(),
        Data = binary:copy(<<0>>, Size),
        Iterations = 1000,

        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_buffer:write(Buf, Data)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        TotalTime = (End - Start) / 1000000,
        WriteRate = Iterations / TotalTime,
        MBPerSec = (Iterations * Size) / TotalTime / 1048576,

        io:format("~8B | ~12w | ~12.2f~n",
                  [Size, round(WriteRate), MBPerSec]),

        py_buffer:close(Buf)
    end, Sizes),
    ok.

run_read_throughput_bench() ->
    io:format("~n--- PyBuffer Read Throughput (Python) ---~n"),
    io:format("Reads per batch: 1000~n~n"),

    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import time

def bench_buffer_reads(buf, size, iterations):
    '''Read from buffer and measure throughput.'''
    # Read all data
    start = time.perf_counter()
    total_read = 0
    while True:
        data = buf.read(size)
        if not data:
            break
        total_read += len(data)
    elapsed = time.perf_counter() - start

    if elapsed > 0:
        reads_per_sec = (total_read / size) / elapsed
        mb_per_sec = total_read / elapsed / 1048576
    else:
        reads_per_sec = 0
        mb_per_sec = 0

    return {
        'total_bytes': total_read,
        'reads_per_sec': reads_per_sec,
        'mb_per_sec': mb_per_sec,
    }
">>),

    Sizes = [64, 256, 1024, 4096, 16384],

    io:format("~8s | ~12s | ~12s~n",
              ["Size", "Reads/sec", "MB/sec"]),
    io:format("~s~n", [string:copies("-", 38)]),

    lists:foreach(fun(Size) ->
        Iterations = 1000,
        {ok, Buf} = py_buffer:new(Size * Iterations),
        Data = binary:copy(<<0>>, Size),

        %% Fill the buffer
        lists:foreach(fun(_) ->
            ok = py_buffer:write(Buf, Data)
        end, lists:seq(1, Iterations)),
        ok = py_buffer:close(Buf),

        %% Measure Python read performance
        {ok, Result} = py:eval(Ctx, <<"bench_buffer_reads(buf, size, iterations)">>,
                               #{<<"buf">> => Buf, <<"size">> => Size, <<"iterations">> => Iterations}),

        ReadsPerSec = maps:get(<<"reads_per_sec">>, Result),
        MBPerSec = maps:get(<<"mb_per_sec">>, Result),

        io:format("~8B | ~12w | ~12.2f~n",
                  [Size, round(ReadsPerSec), MBPerSec])
    end, Sizes),
    ok.

run_buffer_vs_channel_bench() ->
    io:format("~n--- PyBuffer vs Channel Comparison ---~n"),
    io:format("Pattern: Erlang write/send -> Python read/receive~n"),
    io:format("Iterations: 1000~n~n"),

    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
from erlang.channel import Channel

def read_buffer_all(buf):
    '''Read entire buffer.'''
    return buf.read()

def recv_channel(ch_ref):
    '''Receive from channel.'''
    ch = Channel(ch_ref)
    return ch.try_receive()
">>),

    Sizes = [64, 256, 1024, 4096, 16384],

    io:format("~8s | ~14s | ~14s | ~8s~n",
              ["Size", "Buffer (ops/s)", "Channel (ops/s)", "Ratio"]),
    io:format("~s~n", [string:copies("-", 52)]),

    lists:foreach(fun(Size) ->
        Data = binary:copy(<<0>>, Size),
        Iterations = 1000,

        %% Benchmark PyBuffer: Erlang write -> Python read
        BufStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            {ok, Buf} = py_buffer:new(Size),
            ok = py_buffer:write(Buf, Data),
            ok = py_buffer:close(Buf),
            {ok, _} = py:eval(Ctx, <<"read_buffer_all(buf)">>, #{<<"buf">> => Buf})
        end, lists:seq(1, Iterations)),
        BufEnd = erlang:monotonic_time(microsecond),
        BufTime = (BufEnd - BufStart) / 1000000,
        BufOpsPerSec = Iterations / BufTime,

        %% Benchmark Channel: Erlang send -> Python receive
        {ok, Ch} = py_channel:new(),
        ChanStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data),
            {ok, _} = py:eval(Ctx, <<"recv_channel(ch)">>, #{<<"ch">> => Ch})
        end, lists:seq(1, Iterations)),
        ChanEnd = erlang:monotonic_time(microsecond),
        ChanTime = (ChanEnd - ChanStart) / 1000000,
        ChanOpsPerSec = Iterations / ChanTime,
        py_channel:close(Ch),

        Ratio = BufOpsPerSec / ChanOpsPerSec,
        io:format("~8B | ~14w | ~14w | ~.2fx~n",
                  [Size, round(BufOpsPerSec), round(ChanOpsPerSec), Ratio])
    end, Sizes),
    ok.

run_streaming_bench() ->
    io:format("~n--- Streaming Comparison (chunked transfer) ---~n"),
    io:format("Total data: 1MB, varying chunk sizes~n~n"),

    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
from erlang.channel import Channel

def stream_read_buffer(buf):
    '''Stream read entire buffer.'''
    total = 0
    while True:
        chunk = buf.read(8192)  # 8KB reads
        if not chunk:
            break
        total += len(chunk)
    return total

def stream_recv_channel(ch_ref, num_chunks):
    '''Stream receive from channel.'''
    ch = Channel(ch_ref)
    total = 0
    for _ in range(num_chunks):
        msg = ch.try_receive()
        if msg is None:
            break
        total += len(msg)
    return total
">>),

    ChunkSizes = [256, 1024, 4096, 16384, 65536],
    TotalBytes = 1048576,  % 1MB

    io:format("~10s | ~14s | ~14s | ~8s~n",
              ["Chunk", "Buffer (MB/s)", "Channel (MB/s)", "Ratio"]),
    io:format("~s~n", [string:copies("-", 54)]),

    lists:foreach(fun(ChunkSize) ->
        NumChunks = TotalBytes div ChunkSize,
        Chunk = binary:copy(<<0>>, ChunkSize),

        %% Benchmark PyBuffer streaming (Erlang write -> Python read)
        {ok, Buf} = py_buffer:new(TotalBytes),
        BufStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_buffer:write(Buf, Chunk)
        end, lists:seq(1, NumChunks)),
        ok = py_buffer:close(Buf),
        {ok, _} = py:eval(Ctx, <<"stream_read_buffer(buf)">>, #{<<"buf">> => Buf}),
        BufEnd = erlang:monotonic_time(microsecond),
        BufTime = (BufEnd - BufStart) / 1000000,
        BufMBPerSec = (TotalBytes / 1048576) / BufTime,

        %% Benchmark Channel streaming (Erlang send -> Python receive)
        {ok, Ch} = py_channel:new(),
        ChanStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Chunk)
        end, lists:seq(1, NumChunks)),
        {ok, _} = py:eval(Ctx, <<"stream_recv_channel(ch, num_chunks)">>,
                          #{<<"ch">> => Ch, <<"num_chunks">> => NumChunks}),
        ChanEnd = erlang:monotonic_time(microsecond),
        ChanTime = (ChanEnd - ChanStart) / 1000000,
        ChanMBPerSec = (TotalBytes / 1048576) / ChanTime,
        py_channel:close(Ch),

        Ratio = BufMBPerSec / ChanMBPerSec,
        io:format("~10B | ~14.2f | ~14.2f | ~.2fx~n",
                  [ChunkSize, BufMBPerSec, ChanMBPerSec, Ratio])
    end, ChunkSizes),
    ok.
