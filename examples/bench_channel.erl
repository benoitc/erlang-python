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

    io:format(
        "~8s | ~12s | ~12s | ~12s~n",
        ["Size", "Send (msg/s)", "Recv (msg/s)", "Round (msg/s)"]
    ),
    io:format("~s~n", [string:copies("-", 52)]),

    lists:foreach(
        fun(Size) ->
            {ok, Ch} = py_channel:new(),
            Data = binary:copy(<<0>>, Size),
            Iterations = 1000,

            %% Benchmark send
            SendStart = erlang:monotonic_time(microsecond),
            lists:foreach(
                fun(_) ->
                    ok = py_channel:send(Ch, Data)
                end,
                lists:seq(1, Iterations)
            ),
            SendEnd = erlang:monotonic_time(microsecond),
            SendTime = (SendEnd - SendStart) / 1000000,
            SendRate = Iterations / SendTime,

            %% Benchmark receive (drain the queue)
            RecvStart = erlang:monotonic_time(microsecond),
            lists:foreach(
                fun(_) ->
                    {ok, _} = py_nif:channel_try_receive(Ch)
                end,
                lists:seq(1, Iterations)
            ),
            RecvEnd = erlang:monotonic_time(microsecond),
            RecvTime = (RecvEnd - RecvStart) / 1000000,
            RecvRate = Iterations / RecvTime,

            %% Benchmark round-trip (send + receive)
            RoundStart = erlang:monotonic_time(microsecond),
            lists:foreach(
                fun(_) ->
                    ok = py_channel:send(Ch, Data),
                    {ok, _} = py_nif:channel_try_receive(Ch)
                end,
                lists:seq(1, Iterations)
            ),
            RoundEnd = erlang:monotonic_time(microsecond),
            RoundTime = (RoundEnd - RoundStart) / 1000000,
            RoundRate = Iterations / RoundTime,

            io:format(
                "~8B | ~12w | ~12w | ~12w~n",
                [Size, round(SendRate), round(RecvRate), round(RoundRate)]
            ),

            py_channel:close(Ch)
        end,
        Sizes
    ),
    ok.

run_channel_vs_reactor_bench() ->
    io:format("~n--- Channel vs Reactor Comparison ---~n"),
    io:format("Iterations: 5000~n~n"),

    Code = <<
        "\n"
        "import time\n"
        "import socket\n"
        "import erlang\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "def bench_channel_python(iterations=5000):\n"
        "    '''Benchmark Channel from Python side'''\n"
        "    results = {}\n"
        "    sizes = [64, 256, 1024, 4096, 16384]\n"
        "\n"
        "    for size in sizes:\n"
        "        test_data = b'X' * size\n"
        "\n"
        "        # We can't easily create channels from Python yet,\n"
        "        # so we benchmark try_receive on pre-filled channels\n"
        "        # This measures Python-side overhead\n"
        "\n"
        "        # Measure try_receive with None ref (measures call overhead)\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(iterations):\n"
        "            try:\n"
        "                # Just measure the erlang.call overhead\n"
        "                pass\n"
        "            except:\n"
        "                pass\n"
        "        call_time = time.perf_counter() - start\n"
        "\n"
        "        results[size] = {\n"
        "            'call_overhead_ms': call_time * 1000,\n"
        "            'ops_per_sec': iterations / max(call_time, 0.0001),\n"
        "        }\n"
        "\n"
        "    return results\n"
        "\n"
        "def bench_reactor_protocol(iterations=200):\n"
        "    '''Benchmark Reactor Protocol pattern'''\n"
        "    results = {}\n"
        "    sizes = [64, 256, 1024, 4096, 16384]\n"
        "\n"
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
        "        import statistics\n"
        "        avg_time = statistics.mean(times)\n"
        "        results[size] = {\n"
        "            'avg_time_ms': avg_time * 1000,\n"
        "            'ops_per_sec': 1.0 / avg_time,\n"
        "        }\n"
        "\n"
        "    return results\n"
        "\n"
        "_reactor_results = bench_reactor_protocol()\n"
    >>,

    ok = py:exec(Code),
    {ok, ReactorResults} = py:eval(<<"_reactor_results">>),

    io:format("Reactor Protocol (echo pattern):~n"),
    io:format(
        "~8s | ~12s | ~12s~n",
        ["Size", "Avg (ms)", "Ops/sec"]
    ),
    io:format("~s~n", [string:copies("-", 36)]),

    Sizes = [64, 256, 1024, 4096, 16384],
    lists:foreach(
        fun(Size) ->
            Data = maps:get(Size, ReactorResults),
            AvgMs = maps:get(<<"avg_time_ms">>, Data),
            OpsPerSec = maps:get(<<"ops_per_sec">>, Data),
            io:format(
                "~8B | ~12.3f | ~12w~n",
                [Size, AvgMs, round(OpsPerSec)]
            )
        end,
        Sizes
    ),

    %% Now compare with Channel round-trip
    io:format("~nChannel Round-trip (Erlang send + NIF receive):~n"),
    io:format(
        "~8s | ~12s | ~12s | ~8s~n",
        ["Size", "Avg (ms)", "Ops/sec", "vs React"]
    ),
    io:format("~s~n", [string:copies("-", 48)]),

    lists:foreach(
        fun(Size) ->
            {ok, Ch} = py_channel:new(),
            Data = binary:copy(<<0>>, Size),
            Iterations = 1000,

            Start = erlang:monotonic_time(microsecond),
            lists:foreach(
                fun(_) ->
                    ok = py_channel:send(Ch, Data),
                    {ok, _} = py_nif:channel_try_receive(Ch)
                end,
                lists:seq(1, Iterations)
            ),
            End = erlang:monotonic_time(microsecond),

            TotalTime = (End - Start) / 1000000,
            AvgMs = (TotalTime / Iterations) * 1000,
            OpsPerSec = Iterations / TotalTime,

            ReactorData = maps:get(Size, ReactorResults),
            ReactorOps = maps:get(<<"ops_per_sec">>, ReactorData),
            Ratio = OpsPerSec / ReactorOps,

            io:format(
                "~8B | ~12.3f | ~12w | ~.1fx~n",
                [Size, AvgMs, round(OpsPerSec), Ratio]
            ),

            py_channel:close(Ch)
        end,
        Sizes
    ),
    ok.

run_channel_latency_bench() ->
    io:format("~n--- Channel Latency Distribution ---~n"),
    io:format("Iterations: 10000~n~n"),

    Sizes = [64, 1024, 16384],

    io:format(
        "~8s | ~10s | ~10s | ~10s | ~10s~n",
        ["Size", "Min (us)", "Avg (us)", "P99 (us)", "Max (us)"]
    ),
    io:format("~s~n", [string:copies("-", 56)]),

    lists:foreach(
        fun(Size) ->
            {ok, Ch} = py_channel:new(),
            Data = binary:copy(<<0>>, Size),
            Iterations = 10000,

            %% Collect latencies
            Latencies = lists:map(
                fun(_) ->
                    Start = erlang:monotonic_time(microsecond),
                    ok = py_channel:send(Ch, Data),
                    {ok, _} = py_nif:channel_try_receive(Ch),
                    End = erlang:monotonic_time(microsecond),
                    End - Start
                end,
                lists:seq(1, Iterations)
            ),

            Sorted = lists:sort(Latencies),
            Min = hd(Sorted),
            Max = lists:last(Sorted),
            Avg = lists:sum(Latencies) / Iterations,
            P99Idx = round(Iterations * 0.99),
            P99 = lists:nth(P99Idx, Sorted),

            io:format(
                "~8B | ~10.1f | ~10.1f | ~10.1f | ~10.1f~n",
                [Size, float(Min), Avg, float(P99), float(Max)]
            ),

            py_channel:close(Ch)
        end,
        Sizes
    ),
    ok.
