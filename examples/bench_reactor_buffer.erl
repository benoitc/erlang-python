#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script for ReactorBuffer zero-copy performance.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_reactor_buffer.erl

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("ReactorBuffer Zero-Copy Benchmark~n"),
    io:format("========================================~n~n"),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),

    %% Print system info
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    {ok, PyVer} = py:version(),
    io:format("  Python: ~s~n", [PyVer]),
    io:format("~n"),

    %% Run benchmarks
    run_buffer_operations_bench(),
    run_protocol_simulation_bench(),
    run_echo_protocol_bench(),

    io:format("~n========================================~n"),
    io:format("Benchmark Complete~n"),
    io:format("========================================~n"),

    halt(0).

run_buffer_operations_bench() ->
    io:format("~n--- Buffer Operations Benchmark ---~n"),
    io:format("Iterations: 10000~n~n"),

    Code = <<"
import time
import erlang

def run_buffer_ops_bench(iterations=10000):
    results = {}
    sizes = [64, 256, 1024, 4096, 16384, 65536]

    for size in sizes:
        test_data = b'X' * size
        buf = erlang.ReactorBuffer._test_create(test_data)
        regular_bytes = bytes(test_data)

        # Benchmark: extend bytearray (uses buffer protocol)
        start = time.perf_counter()
        for _ in range(iterations):
            ba = bytearray()
            ba.extend(buf)
        extend_buf_time = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(iterations):
            ba = bytearray()
            ba.extend(regular_bytes)
        extend_bytes_time = time.perf_counter() - start

        # Benchmark: startswith
        prefix = test_data[:10]
        start = time.perf_counter()
        for _ in range(iterations):
            _ = buf.startswith(prefix)
        startswith_buf_time = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(iterations):
            _ = regular_bytes.startswith(prefix)
        startswith_bytes_time = time.perf_counter() - start

        results[size] = {
            'extend_buf': extend_buf_time * 1000,
            'extend_bytes': extend_bytes_time * 1000,
            'startswith_buf': startswith_buf_time * 1000,
            'startswith_bytes': startswith_bytes_time * 1000,
        }

    return results

_buffer_ops_results = run_buffer_ops_bench()
">>,

    ok = py:exec(Code),
    {ok, Results} = py:eval(<<"_buffer_ops_results">>),

    io:format("~8s | ~12s | ~12s | ~12s | ~8s~n",
              ["Size", "Operation", "Buffer (ms)", "Bytes (ms)", "Ratio"]),
    io:format("~s~n", [string:copies("-", 60)]),

    Sizes = [64, 256, 1024, 4096, 16384, 65536],
    lists:foreach(fun(Size) ->
        Data = maps:get(Size, Results),

        ExtBuf = maps:get(<<"extend_buf">>, Data),
        ExtBytes = maps:get(<<"extend_bytes">>, Data),
        ExtRatio = ExtBytes / max(ExtBuf, 0.001),

        SwBuf = maps:get(<<"startswith_buf">>, Data),
        SwBytes = maps:get(<<"startswith_bytes">>, Data),
        SwRatio = SwBytes / max(SwBuf, 0.001),

        io:format("~8B | ~12s | ~12.3f | ~12.3f | ~.2f x~n",
                  [Size, "extend", ExtBuf, ExtBytes, ExtRatio]),
        io:format("~8s | ~12s | ~12.3f | ~12.3f | ~.2f x~n",
                  ["", "startswith", SwBuf, SwBytes, SwRatio])
    end, Sizes),
    ok.

run_protocol_simulation_bench() ->
    io:format("~n--- Protocol Simulation Benchmark ---~n"),
    io:format("Iterations: 5000~n~n"),

    Code = <<"
import time
import erlang

def run_protocol_sim_bench(iterations=5000):
    results = {}
    sizes = [64, 256, 1024, 4096, 16384, 65536]

    for size in sizes:
        test_data = b'GET / HTTP/1.1\\r\\nHost: example.com\\r\\n\\r\\n' + b'X' * (size - 40)
        test_data = test_data[:size]

        buf = erlang.ReactorBuffer._test_create(test_data)
        regular_bytes = bytes(test_data)

        def parse_request(data):
            if data.startswith(b'GET'):
                method = 'GET'
            elif data.startswith(b'POST'):
                method = 'POST'
            else:
                method = 'OTHER'
            pos = data.find(b'\\r\\n\\r\\n')
            write_buf = bytearray()
            write_buf.extend(data)
            return len(write_buf)

        # Benchmark with ReactorBuffer
        start = time.perf_counter()
        for _ in range(iterations):
            _ = parse_request(buf)
        buf_time = time.perf_counter() - start

        # Benchmark with regular bytes
        start = time.perf_counter()
        for _ in range(iterations):
            _ = parse_request(regular_bytes)
        bytes_time = time.perf_counter() - start

        results[size] = {
            'buffer_ops_per_sec': iterations / buf_time,
            'bytes_ops_per_sec': iterations / bytes_time,
        }

    return results

_protocol_sim_results = run_protocol_sim_bench()
">>,

    ok = py:exec(Code),
    {ok, Results} = py:eval(<<"_protocol_sim_results">>),

    io:format("~8s | ~14s | ~14s | ~8s~n",
              ["Size", "Buffer (ops/s)", "Bytes (ops/s)", "Speedup"]),
    io:format("~s~n", [string:copies("-", 52)]),

    Sizes = [64, 256, 1024, 4096, 16384, 65536],
    SpeedupsList = lists:map(fun(Size) ->
        Data = maps:get(Size, Results),

        BufOps = maps:get(<<"buffer_ops_per_sec">>, Data),
        BytesOps = maps:get(<<"bytes_ops_per_sec">>, Data),
        Speedup = BufOps / max(BytesOps, 1),

        io:format("~8B | ~14w | ~14w | ~.2f~n",
                  [Size, round(BufOps), round(BytesOps), Speedup]),
        {Size, Speedup}
    end, Sizes),

    %% Calculate average speedup for >= 1KB
    LargeSpeedups = [S || {Size, S} <- SpeedupsList, Size >= 1024],
    case LargeSpeedups of
        [] -> ok;
        _ ->
            AvgSpeedup = lists:sum(LargeSpeedups) / length(LargeSpeedups),
            Improvement = (AvgSpeedup - 1.0) * 100,
            io:format("~nAverage speedup for payloads >= 1KB: ~.2f x~n", [AvgSpeedup]),
            io:format("Performance improvement: ~.1f%~n", [Improvement])
    end,
    ok.

run_echo_protocol_bench() ->
    io:format("~n--- Echo Protocol Benchmark ---~n"),
    io:format("Iterations: 200~n~n"),

    Code = <<"
import time
import socket
import statistics
import erlang.reactor as reactor

def run_echo_bench(iterations=200):
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

    results = {}
    sizes = [64, 256, 1024, 4096, 16384]

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

        avg_time = statistics.mean(times)
        results[size] = {
            'avg_time_ms': avg_time * 1000,
            'ops_per_sec': 1.0 / avg_time,
            'p50_ms': statistics.median(times) * 1000,
            'p95_ms': sorted(times)[int(len(times) * 0.95)] * 1000,
        }

    return results

_echo_bench_results = run_echo_bench()
">>,

    ok = py:exec(Code),
    {ok, Results} = py:eval(<<"_echo_bench_results">>),

    io:format("~8s | ~10s | ~10s | ~10s | ~10s~n",
              ["Size", "Avg (ms)", "P50 (ms)", "P95 (ms)", "Ops/sec"]),
    io:format("~s~n", [string:copies("-", 56)]),

    Sizes = [64, 256, 1024, 4096, 16384],
    lists:foreach(fun(Size) ->
        Data = maps:get(Size, Results),

        AvgMs = maps:get(<<"avg_time_ms">>, Data),
        P50Ms = maps:get(<<"p50_ms">>, Data),
        P95Ms = maps:get(<<"p95_ms">>, Data),
        OpsPerSec = maps:get(<<"ops_per_sec">>, Data),

        io:format("~8B | ~10.3f | ~10.3f | ~10.3f | ~10w~n",
                  [Size, AvgMs, P50Ms, P95Ms, round(OpsPerSec)])
    end, Sizes),
    ok.
