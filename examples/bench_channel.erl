#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script for Channel API vs Reactor comparison.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_channel.erl

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("Channel API Benchmark~n"),
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
    run_channel_throughput_bench(),
    run_channel_vs_reactor_bench(),
    run_channel_latency_bench(),

    io:format("~n========================================~n"),
    io:format("Benchmark Complete~n"),
    io:format("========================================~n"),

    halt(0).

run_channel_throughput_bench() ->
    io:format("~n--- Channel Throughput Benchmark ---~n"),
    io:format("Messages per batch: 1000~n~n"),

    Sizes = [64, 256, 1024, 4096, 16384],

    io:format("~8s | ~12s | ~12s | ~12s~n",
              ["Size", "Send (msg/s)", "Recv (msg/s)", "Round (msg/s)"]),
    io:format("~s~n", [string:copies("-", 52)]),

    lists:foreach(fun(Size) ->
        {ok, Ch} = py_channel:new(),
        Data = binary:copy(<<0>>, Size),
        Iterations = 1000,

        %% Benchmark send
        SendStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data)
        end, lists:seq(1, Iterations)),
        SendEnd = erlang:monotonic_time(microsecond),
        SendTime = (SendEnd - SendStart) / 1000000,
        SendRate = Iterations / SendTime,

        %% Benchmark receive (drain the queue)
        RecvStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            {ok, _} = py_nif:channel_try_receive(Ch)
        end, lists:seq(1, Iterations)),
        RecvEnd = erlang:monotonic_time(microsecond),
        RecvTime = (RecvEnd - RecvStart) / 1000000,
        RecvRate = Iterations / RecvTime,

        %% Benchmark round-trip (send + receive)
        RoundStart = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data),
            {ok, _} = py_nif:channel_try_receive(Ch)
        end, lists:seq(1, Iterations)),
        RoundEnd = erlang:monotonic_time(microsecond),
        RoundTime = (RoundEnd - RoundStart) / 1000000,
        RoundRate = Iterations / RoundTime,

        io:format("~8B | ~12w | ~12w | ~12w~n",
                  [Size, round(SendRate), round(RecvRate), round(RoundRate)]),

        py_channel:close(Ch)
    end, Sizes),
    ok.

run_channel_vs_reactor_bench() ->
    io:format("~n--- Channel vs Reactor Comparison ---~n"),
    io:format("Iterations: 5000~n~n"),

    Code = <<"
import time
import socket
import erlang
import erlang.reactor as reactor

def bench_channel_python(iterations=5000):
    '''Benchmark Channel from Python side'''
    results = {}
    sizes = [64, 256, 1024, 4096, 16384]

    for size in sizes:
        test_data = b'X' * size

        # We can't easily create channels from Python yet,
        # so we benchmark try_receive on pre-filled channels
        # This measures Python-side overhead

        # Measure try_receive with None ref (measures call overhead)
        start = time.perf_counter()
        for _ in range(iterations):
            try:
                # Just measure the erlang.call overhead
                pass
            except:
                pass
        call_time = time.perf_counter() - start

        results[size] = {
            'call_overhead_ms': call_time * 1000,
            'ops_per_sec': iterations / max(call_time, 0.0001),
        }

    return results

def bench_reactor_protocol(iterations=200):
    '''Benchmark Reactor Protocol pattern'''
    results = {}
    sizes = [64, 256, 1024, 4096, 16384]

    class EchoProtocol(reactor.Protocol):
        def data_received(self, data):
            self.write_buffer.extend(data)
            return 'write_pending'

        def write_ready(self):
            if not self.write_buffer:
                return 'read_pending'
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            return 'continue' if self.write_buffer else 'read_pending'

    for size in sizes:
        test_data = b'X' * size
        times = []

        for _ in range(iterations):
            s1, s2 = socket.socketpair()
            s1.setblocking(False)
            s2.setblocking(False)

            try:
                reactor.set_protocol_factory(EchoProtocol)
                reactor.init_connection(s1.fileno(), {'type': 'test'})

                s2.send(test_data)

                start = time.perf_counter()
                action = reactor.on_read_ready(s1.fileno())
                elapsed = time.perf_counter() - start
                times.append(elapsed)

                reactor.close_connection(s1.fileno())
            finally:
                s1.close()
                s2.close()

        import statistics
        avg_time = statistics.mean(times)
        results[size] = {
            'avg_time_ms': avg_time * 1000,
            'ops_per_sec': 1.0 / avg_time,
        }

    return results

_reactor_results = bench_reactor_protocol()
">>,

    ok = py:exec(Code),
    {ok, ReactorResults} = py:eval(<<"_reactor_results">>),

    io:format("Reactor Protocol (echo pattern):~n"),
    io:format("~8s | ~12s | ~12s~n",
              ["Size", "Avg (ms)", "Ops/sec"]),
    io:format("~s~n", [string:copies("-", 36)]),

    Sizes = [64, 256, 1024, 4096, 16384],
    lists:foreach(fun(Size) ->
        Data = maps:get(Size, ReactorResults),
        AvgMs = maps:get(<<"avg_time_ms">>, Data),
        OpsPerSec = maps:get(<<"ops_per_sec">>, Data),
        io:format("~8B | ~12.3f | ~12w~n",
                  [Size, AvgMs, round(OpsPerSec)])
    end, Sizes),

    %% Now compare with Channel round-trip
    io:format("~nChannel Round-trip (Erlang send + NIF receive):~n"),
    io:format("~8s | ~12s | ~12s | ~8s~n",
              ["Size", "Avg (ms)", "Ops/sec", "vs React"]),
    io:format("~s~n", [string:copies("-", 48)]),

    lists:foreach(fun(Size) ->
        {ok, Ch} = py_channel:new(),
        Data = binary:copy(<<0>>, Size),
        Iterations = 1000,

        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data),
            {ok, _} = py_nif:channel_try_receive(Ch)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        TotalTime = (End - Start) / 1000000,
        AvgMs = (TotalTime / Iterations) * 1000,
        OpsPerSec = Iterations / TotalTime,

        ReactorData = maps:get(Size, ReactorResults),
        ReactorOps = maps:get(<<"ops_per_sec">>, ReactorData),
        Ratio = OpsPerSec / ReactorOps,

        io:format("~8B | ~12.3f | ~12w | ~.1fx~n",
                  [Size, AvgMs, round(OpsPerSec), Ratio]),

        py_channel:close(Ch)
    end, Sizes),
    ok.

run_channel_latency_bench() ->
    io:format("~n--- Channel Latency Distribution ---~n"),
    io:format("Iterations: 10000~n~n"),

    Sizes = [64, 1024, 16384],

    io:format("~8s | ~10s | ~10s | ~10s | ~10s~n",
              ["Size", "Min (us)", "Avg (us)", "P99 (us)", "Max (us)"]),
    io:format("~s~n", [string:copies("-", 56)]),

    lists:foreach(fun(Size) ->
        {ok, Ch} = py_channel:new(),
        Data = binary:copy(<<0>>, Size),
        Iterations = 10000,

        %% Collect latencies
        Latencies = lists:map(fun(_) ->
            Start = erlang:monotonic_time(microsecond),
            ok = py_channel:send(Ch, Data),
            {ok, _} = py_nif:channel_try_receive(Ch),
            End = erlang:monotonic_time(microsecond),
            End - Start
        end, lists:seq(1, Iterations)),

        Sorted = lists:sort(Latencies),
        Min = hd(Sorted),
        Max = lists:last(Sorted),
        Avg = lists:sum(Latencies) / Iterations,
        P99Idx = round(Iterations * 0.99),
        P99 = lists:nth(P99Idx, Sorted),

        io:format("~8B | ~10.1f | ~10.1f | ~10.1f | ~10.1f~n",
                  [Size, float(Min), Avg, float(P99), float(Max)]),

        py_channel:close(Ch)
    end, Sizes),
    ok.
