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

    Code = <<
        "\n"
        "import time\n"
        "import erlang\n"
        "\n"
        "def run_buffer_ops_bench(iterations=10000):\n"
        "    results = {}\n"
        "    sizes = [64, 256, 1024, 4096, 16384, 65536]\n"
        "\n"
        "    for size in sizes:\n"
        "        test_data = b'X' * size\n"
        "        buf = erlang.ReactorBuffer._test_create(test_data)\n"
        "        regular_bytes = bytes(test_data)\n"
        "\n"
        "        # Benchmark: extend bytearray (uses buffer protocol)\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(iterations):\n"
        "            ba = bytearray()\n"
        "            ba.extend(buf)\n"
        "        extend_buf_time = time.perf_counter() - start\n"
        "\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(iterations):\n"
        "            ba = bytearray()\n"
        "            ba.extend(regular_bytes)\n"
        "        extend_bytes_time = time.perf_counter() - start\n"
        "\n"
        "        # Benchmark: startswith\n"
        "        prefix = test_data[:10]\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(iterations):\n"
        "            _ = buf.startswith(prefix)\n"
        "        startswith_buf_time = time.perf_counter() - start\n"
        "\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(iterations):\n"
        "            _ = regular_bytes.startswith(prefix)\n"
        "        startswith_bytes_time = time.perf_counter() - start\n"
        "\n"
        "        results[size] = {\n"
        "            'extend_buf': extend_buf_time * 1000,\n"
        "            'extend_bytes': extend_bytes_time * 1000,\n"
        "            'startswith_buf': startswith_buf_time * 1000,\n"
        "            'startswith_bytes': startswith_bytes_time * 1000,\n"
        "        }\n"
        "\n"
        "    return results\n"
        "\n"
        "_buffer_ops_results = run_buffer_ops_bench()\n"
    >>,

    ok = py:exec(Code),
    {ok, Results} = py:eval(<<"_buffer_ops_results">>),

    io:format(
        "~8s | ~12s | ~12s | ~12s | ~8s~n",
        ["Size", "Operation", "Buffer (ms)", "Bytes (ms)", "Ratio"]
    ),
    io:format("~s~n", [string:copies("-", 60)]),

    Sizes = [64, 256, 1024, 4096, 16384, 65536],
    lists:foreach(
        fun(Size) ->
            Data = maps:get(Size, Results),

            ExtBuf = maps:get(<<"extend_buf">>, Data),
            ExtBytes = maps:get(<<"extend_bytes">>, Data),
            ExtRatio = ExtBytes / max(ExtBuf, 0.001),

            SwBuf = maps:get(<<"startswith_buf">>, Data),
            SwBytes = maps:get(<<"startswith_bytes">>, Data),
            SwRatio = SwBytes / max(SwBuf, 0.001),

            io:format(
                "~8B | ~12s | ~12.3f | ~12.3f | ~.2f x~n",
                [Size, "extend", ExtBuf, ExtBytes, ExtRatio]
            ),
            io:format(
                "~8s | ~12s | ~12.3f | ~12.3f | ~.2f x~n",
                ["", "startswith", SwBuf, SwBytes, SwRatio]
            )
        end,
        Sizes
    ),
    ok.

run_protocol_simulation_bench() ->
    io:format("~n--- Protocol Simulation Benchmark ---~n"),
    io:format("Iterations: 5000~n~n"),

    Code = <<
        "\n"
        "import time\n"
        "import erlang\n"
        "\n"
        "def run_protocol_sim_bench(iterations=5000):\n"
        "    results = {}\n"
        "    sizes = [64, 256, 1024, 4096, 16384, 65536]\n"
        "\n"
        "    for size in sizes:\n"
        "        test_data = b'GET / HTTP/1.1\\r\\nHost: example.com\\r\\n\\r\\n' + b'X' * (size - 40)\n"
        "        test_data = test_data[:size]\n"
        "\n"
        "        buf = erlang.ReactorBuffer._test_create(test_data)\n"
        "        regular_bytes = bytes(test_data)\n"
        "\n"
        "        def parse_request(data):\n"
        "            if data.startswith(b'GET'):\n"
        "                method = 'GET'\n"
        "            elif data.startswith(b'POST'):\n"
        "                method = 'POST'\n"
        "            else:\n"
        "                method = 'OTHER'\n"
        "            pos = data.find(b'\\r\\n\\r\\n')\n"
        "            write_buf = bytearray()\n"
        "            write_buf.extend(data)\n"
        "            return len(write_buf)\n"
        "\n"
        "        # Benchmark with ReactorBuffer\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(iterations):\n"
        "            _ = parse_request(buf)\n"
        "        buf_time = time.perf_counter() - start\n"
        "\n"
        "        # Benchmark with regular bytes\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(iterations):\n"
        "            _ = parse_request(regular_bytes)\n"
        "        bytes_time = time.perf_counter() - start\n"
        "\n"
        "        results[size] = {\n"
        "            'buffer_ops_per_sec': iterations / buf_time,\n"
        "            'bytes_ops_per_sec': iterations / bytes_time,\n"
        "        }\n"
        "\n"
        "    return results\n"
        "\n"
        "_protocol_sim_results = run_protocol_sim_bench()\n"
    >>,

    ok = py:exec(Code),
    {ok, Results} = py:eval(<<"_protocol_sim_results">>),

    io:format(
        "~8s | ~14s | ~14s | ~8s~n",
        ["Size", "Buffer (ops/s)", "Bytes (ops/s)", "Speedup"]
    ),
    io:format("~s~n", [string:copies("-", 52)]),

    Sizes = [64, 256, 1024, 4096, 16384, 65536],
    SpeedupsList = lists:map(
        fun(Size) ->
            Data = maps:get(Size, Results),

            BufOps = maps:get(<<"buffer_ops_per_sec">>, Data),
            BytesOps = maps:get(<<"bytes_ops_per_sec">>, Data),
            Speedup = BufOps / max(BytesOps, 1),

            io:format(
                "~8B | ~14w | ~14w | ~.2f~n",
                [Size, round(BufOps), round(BytesOps), Speedup]
            ),
            {Size, Speedup}
        end,
        Sizes
    ),

    %% Calculate average speedup for >= 1KB
    LargeSpeedups = [S || {Size, S} <- SpeedupsList, Size >= 1024],
    case LargeSpeedups of
        [] ->
            ok;
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

    Code = <<
        "\n"
        "import time\n"
        "import socket\n"
        "import statistics\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "def run_echo_bench(iterations=200):\n"
        "    class EchoProtocol(reactor.Protocol):\n"
        "        def data_received(self, data):\n"
        "            self.write_buffer.extend(data)\n"
        "            return 'write_pending'\n"
        "\n"
        "        def write_ready(self):\n"
        "            if not self.write_buffer:\n"
        "                return 'read_pending'\n"
        "            written = self.write(bytes(self.write_buffer))\n"
        "            del self.write_buffer[:written]\n"
        "            return 'continue' if self.write_buffer else 'read_pending'\n"
        "\n"
        "    results = {}\n"
        "    sizes = [64, 256, 1024, 4096, 16384]\n"
        "\n"
        "    for size in sizes:\n"
        "        test_data = b'X' * size\n"
        "        times = []\n"
        "\n"
        "        for _ in range(iterations):\n"
        "            s1, s2 = socket.socketpair()\n"
        "            s1.setblocking(False)\n"
        "            s2.setblocking(False)\n"
        "\n"
        "            try:\n"
        "                reactor.set_protocol_factory(EchoProtocol)\n"
        "                reactor.init_connection(s1.fileno(), {'type': 'test'})\n"
        "\n"
        "                s2.send(test_data)\n"
        "\n"
        "                start = time.perf_counter()\n"
        "                action = reactor.on_read_ready(s1.fileno())\n"
        "                elapsed = time.perf_counter() - start\n"
        "                times.append(elapsed)\n"
        "\n"
        "                reactor.close_connection(s1.fileno())\n"
        "            finally:\n"
        "                s1.close()\n"
        "                s2.close()\n"
        "\n"
        "        avg_time = statistics.mean(times)\n"
        "        results[size] = {\n"
        "            'avg_time_ms': avg_time * 1000,\n"
        "            'ops_per_sec': 1.0 / avg_time,\n"
        "            'p50_ms': statistics.median(times) * 1000,\n"
        "            'p95_ms': sorted(times)[int(len(times) * 0.95)] * 1000,\n"
        "        }\n"
        "\n"
        "    return results\n"
        "\n"
        "_echo_bench_results = run_echo_bench()\n"
    >>,

    ok = py:exec(Code),
    {ok, Results} = py:eval(<<"_echo_bench_results">>),

    io:format(
        "~8s | ~10s | ~10s | ~10s | ~10s~n",
        ["Size", "Avg (ms)", "P50 (ms)", "P95 (ms)", "Ops/sec"]
    ),
    io:format("~s~n", [string:copies("-", 56)]),

    Sizes = [64, 256, 1024, 4096, 16384],
    lists:foreach(
        fun(Size) ->
            Data = maps:get(Size, Results),

            AvgMs = maps:get(<<"avg_time_ms">>, Data),
            P50Ms = maps:get(<<"p50_ms">>, Data),
            P95Ms = maps:get(<<"p95_ms">>, Data),
            OpsPerSec = maps:get(<<"ops_per_sec">>, Data),

            io:format(
                "~8B | ~10.3f | ~10.3f | ~10.3f | ~10w~n",
                [Size, AvgMs, P50Ms, P95Ms, round(OpsPerSec)]
            )
        end,
        Sizes
    ),
    ok.
