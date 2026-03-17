#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark comparing Channel, ByteChannel, and PyBuffer.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_byte_channel.erl

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("Channel vs ByteChannel vs PyBuffer~n"),
    io:format("========================================~n~n"),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    ok = py_channel:register_callbacks(),
    ok = py_byte_channel:register_callbacks(),

    %% Print system info
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    {ok, PyVer} = py:version(),
    io:format("  Python: ~s~n", [PyVer]),
    io:format("~n"),

    %% Run benchmarks
    run_erlang_send_bench(),
    run_erlang_roundtrip_bench(),
    run_python_receive_bench(),
    run_streaming_bench(),

    io:format("~n========================================~n"),
    io:format("Benchmark Complete~n"),
    io:format("========================================~n"),

    halt(0).

%% Benchmark Erlang-side send performance (no Python involved)
run_erlang_send_bench() ->
    io:format("~n--- Erlang Send Throughput (no Python) ---~n"),
    io:format("Iterations: 10000~n~n"),

    Sizes = [64, 256, 1024, 4096, 16384],
    Iterations = 10000,

    io:format("~8s | ~14s | ~14s | ~14s~n",
              ["Size", "Channel", "ByteChannel", "PyBuffer"]),
    io:format("~8s | ~14s | ~14s | ~14s~n",
              ["(bytes)", "(msg/sec)", "(msg/sec)", "(writes/sec)"]),
    io:format("~s~n", [string:copies("-", 58)]),

    lists:foreach(fun(Size) ->
        Data = binary:copy(<<0>>, Size),

        %% Channel (term-based)
        {ok, Ch} = py_channel:new(),
        ChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data)
        end, lists:seq(1, Iterations)),
        ChEnd = erlang:monotonic_time(microsecond),
        ChRate = Iterations / ((ChEnd - ChStart) / 1000000),
        py_channel:close(Ch),

        %% ByteChannel (raw bytes)
        {ok, BCh} = py_byte_channel:new(),
        BChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_byte_channel:send(BCh, Data)
        end, lists:seq(1, Iterations)),
        BChEnd = erlang:monotonic_time(microsecond),
        BChRate = Iterations / ((BChEnd - BChStart) / 1000000),
        py_byte_channel:close(BCh),

        %% PyBuffer
        {ok, Buf} = py_buffer:new(),
        BufStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_buffer:write(Buf, Data)
        end, lists:seq(1, Iterations)),
        BufEnd = erlang:monotonic_time(microsecond),
        BufRate = Iterations / ((BufEnd - BufStart) / 1000000),
        py_buffer:close(Buf),

        io:format("~8B | ~14w | ~14w | ~14w~n",
                  [Size, round(ChRate), round(BChRate), round(BufRate)])
    end, Sizes),
    ok.

%% Benchmark Erlang-side roundtrip (send + receive, no Python)
run_erlang_roundtrip_bench() ->
    io:format("~n--- Erlang Roundtrip (send + receive, no Python) ---~n"),
    io:format("Iterations: 10000~n~n"),

    Sizes = [64, 256, 1024, 4096, 16384],
    Iterations = 10000,

    io:format("~8s | ~14s | ~14s~n",
              ["Size", "Channel", "ByteChannel"]),
    io:format("~8s | ~14s | ~14s~n",
              ["(bytes)", "(roundtrip/s)", "(roundtrip/s)"]),
    io:format("~s~n", [string:copies("-", 42)]),

    lists:foreach(fun(Size) ->
        Data = binary:copy(<<0>>, Size),

        %% Channel roundtrip
        {ok, Ch} = py_channel:new(),
        ChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data),
            {ok, _} = py_nif:channel_try_receive(Ch)
        end, lists:seq(1, Iterations)),
        ChEnd = erlang:monotonic_time(microsecond),
        ChRate = Iterations / ((ChEnd - ChStart) / 1000000),
        py_channel:close(Ch),

        %% ByteChannel roundtrip
        {ok, BCh} = py_byte_channel:new(),
        BChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_byte_channel:send(BCh, Data),
            {ok, _} = py_byte_channel:try_receive(BCh)
        end, lists:seq(1, Iterations)),
        BChEnd = erlang:monotonic_time(microsecond),
        BChRate = Iterations / ((BChEnd - BChStart) / 1000000),
        py_byte_channel:close(BCh),

        io:format("~8B | ~14w | ~14w~n",
                  [Size, round(ChRate), round(BChRate)])
    end, Sizes),
    ok.

%% Benchmark Python receive performance
run_python_receive_bench() ->
    io:format("~n--- Python Receive Performance ---~n"),
    io:format("Pattern: Erlang send -> Python receive~n"),
    io:format("Iterations: 1000~n~n"),

    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
from erlang import Channel, ByteChannel

def recv_channel(ch_ref):
    ch = Channel(ch_ref)
    return ch.try_receive()

def recv_byte_channel(ch_ref):
    ch = ByteChannel(ch_ref)
    return ch.try_receive_bytes()

def read_buffer(buf):
    return buf.read()
">>),

    Sizes = [64, 256, 1024, 4096, 16384],
    Iterations = 1000,

    io:format("~8s | ~12s | ~12s | ~12s~n",
              ["Size", "Channel", "ByteChannel", "PyBuffer"]),
    io:format("~8s | ~12s | ~12s | ~12s~n",
              ["(bytes)", "(ops/sec)", "(ops/sec)", "(ops/sec)"]),
    io:format("~s~n", [string:copies("-", 54)]),

    lists:foreach(fun(Size) ->
        Data = binary:copy(<<0>>, Size),

        %% Channel: Erlang send -> Python receive
        {ok, Ch} = py_channel:new(),
        ChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data),
            {ok, _} = py:eval(Ctx, <<"recv_channel(ch)">>, #{<<"ch">> => Ch})
        end, lists:seq(1, Iterations)),
        ChEnd = erlang:monotonic_time(microsecond),
        ChRate = Iterations / ((ChEnd - ChStart) / 1000000),
        py_channel:close(Ch),

        %% ByteChannel: Erlang send -> Python receive
        {ok, BCh} = py_byte_channel:new(),
        BChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_byte_channel:send(BCh, Data),
            {ok, _} = py:eval(Ctx, <<"recv_byte_channel(ch)">>, #{<<"ch">> => BCh})
        end, lists:seq(1, Iterations)),
        BChEnd = erlang:monotonic_time(microsecond),
        BChRate = Iterations / ((BChEnd - BChStart) / 1000000),
        py_byte_channel:close(BCh),

        %% PyBuffer: Erlang write -> Python read
        BufStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            {ok, Buf} = py_buffer:new(Size),
            ok = py_buffer:write(Buf, Data),
            ok = py_buffer:close(Buf),
            {ok, _} = py:eval(Ctx, <<"read_buffer(buf)">>, #{<<"buf">> => Buf})
        end, lists:seq(1, Iterations)),
        BufEnd = erlang:monotonic_time(microsecond),
        BufRate = Iterations / ((BufEnd - BufStart) / 1000000),

        io:format("~8B | ~12w | ~12w | ~12w~n",
                  [Size, round(ChRate), round(BChRate), round(BufRate)])
    end, Sizes),
    ok.

%% Benchmark streaming (1MB transfer with varying chunk sizes)
run_streaming_bench() ->
    io:format("~n--- Streaming Benchmark (1MB transfer) ---~n"),
    io:format("Pattern: Erlang sends chunks -> Python receives all~n~n"),

    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
from erlang import Channel, ByteChannel

def stream_channel(ch_ref, num_chunks):
    ch = Channel(ch_ref)
    total = 0
    for _ in range(num_chunks):
        msg = ch.try_receive()
        if msg is None:
            break
        total += len(msg)
    return total

def stream_byte_channel(ch_ref, num_chunks):
    ch = ByteChannel(ch_ref)
    total = 0
    for _ in range(num_chunks):
        data = ch.try_receive_bytes()
        if data is None:
            break
        total += len(data)
    return total

def stream_buffer(buf):
    total = 0
    while True:
        chunk = buf.read(8192)
        if not chunk:
            break
        total += len(chunk)
    return total
">>),

    ChunkSizes = [1024, 4096, 16384, 65536],
    TotalBytes = 1048576,  % 1MB

    io:format("~10s | ~12s | ~12s | ~12s~n",
              ["Chunk", "Channel", "ByteChannel", "PyBuffer"]),
    io:format("~10s | ~12s | ~12s | ~12s~n",
              ["(bytes)", "(MB/sec)", "(MB/sec)", "(MB/sec)"]),
    io:format("~s~n", [string:copies("-", 54)]),

    lists:foreach(fun(ChunkSize) ->
        NumChunks = TotalBytes div ChunkSize,
        Chunk = binary:copy(<<0>>, ChunkSize),

        %% Channel streaming
        {ok, Ch} = py_channel:new(),
        ChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Chunk)
        end, lists:seq(1, NumChunks)),
        {ok, _} = py:eval(Ctx, <<"stream_channel(ch, n)">>,
                          #{<<"ch">> => Ch, <<"n">> => NumChunks}),
        ChEnd = erlang:monotonic_time(microsecond),
        ChMBps = (TotalBytes / 1048576) / ((ChEnd - ChStart) / 1000000),
        py_channel:close(Ch),

        %% ByteChannel streaming
        {ok, BCh} = py_byte_channel:new(),
        BChStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_byte_channel:send(BCh, Chunk)
        end, lists:seq(1, NumChunks)),
        {ok, _} = py:eval(Ctx, <<"stream_byte_channel(ch, n)">>,
                          #{<<"ch">> => BCh, <<"n">> => NumChunks}),
        BChEnd = erlang:monotonic_time(microsecond),
        BChMBps = (TotalBytes / 1048576) / ((BChEnd - BChStart) / 1000000),
        py_byte_channel:close(BCh),

        %% PyBuffer streaming
        {ok, Buf} = py_buffer:new(TotalBytes),
        BufStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_buffer:write(Buf, Chunk)
        end, lists:seq(1, NumChunks)),
        ok = py_buffer:close(Buf),
        {ok, _} = py:eval(Ctx, <<"stream_buffer(buf)">>, #{<<"buf">> => Buf}),
        BufEnd = erlang:monotonic_time(microsecond),
        BufMBps = (TotalBytes / 1048576) / ((BufEnd - BufStart) / 1000000),

        io:format("~10B | ~12.2f | ~12.2f | ~12.2f~n",
                  [ChunkSize, ChMBps, BChMBps, BufMBps])
    end, ChunkSizes),
    ok.
