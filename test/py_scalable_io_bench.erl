%% @doc Scalable I/O model benchmark suite.
-module(py_scalable_io_bench).

-export([
    run_all/0,
    run_all/1,
    timer_throughput_single/1,
    timer_throughput_concurrent/1,
    timer_latency/1,
    tcp_echo_single/1,
    tcp_echo_concurrent/1,
    tcp_connections_scaling/1,
    format_results/1,
    save_results/2
]).

-export([all/0, init_per_suite/1, end_per_suite/1]).

-define(DEFAULT_OPTS, #{
    timer_iterations => 10000,
    timer_delay_ms => 1,
    concurrent_workers => 4,
    tcp_messages => 2000,
    tcp_message_size => 64,
    warmup_iterations => 50,
    call_timeout => 120000
}).

all() -> [].
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.
end_per_suite(_Config) -> ok.

run_all() -> run_all(#{}).
run_all(UserOpts) ->
    Opts = maps:merge(?DEFAULT_OPTS, UserOpts),
    io:format("~n========================================~n"),
    io:format("Scalable I/O Model Benchmark~n"),
    io:format("========================================~n"),
    io:format("Commit: ~s~n", [get_git_commit()]),
    io:format("Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    io:format("Schedulers: ~p~n", [erlang:system_info(schedulers)]),
    {ok, _} = application:ensure_all_started(erlang_python),
    Results = #{
        commit => list_to_binary(get_git_commit()),
        timestamp => erlang:system_time(millisecond),
        timer_throughput_single => safe_bench(fun() -> timer_throughput_single(Opts) end),
        timer_latency => safe_bench(fun() -> timer_latency(Opts) end),
        tcp_echo_single => safe_bench(fun() -> tcp_echo_single(Opts) end),
        timer_throughput_concurrent => safe_bench(fun() -> timer_throughput_concurrent(Opts) end),
        tcp_echo_concurrent => safe_bench(fun() -> tcp_echo_concurrent(Opts) end),
        tcp_connections_scaling => safe_bench(fun() -> tcp_connections_scaling(Opts) end)
    },
    io:format("~n========================================~n"),
    io:format("Summary~n"),
    io:format("========================================~n"),
    format_results(Results),
    Results.

safe_bench(Fun) ->
    try
        Fun()
    catch
        Class:Reason:Stack ->
            io:format("ERROR: ~p:~p~n~p~n", [Class, Reason, Stack]),
            #{error => {Class, Reason}}
    end.

timer_throughput_single(Opts) ->
    N = maps:get(timer_iterations, Opts),
    WarmupN = maps:get(warmup_iterations, Opts),
    io:format("~n--- Timer Throughput (single worker) ---~n"),
    io:format("Iterations: ~p~n", [N]),
    Code = <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "def run_timer_throughput_single(n):\n"
        "    async def _run(n):\n"
        "        for _ in range(n):\n"
        "            await asyncio.sleep(0)\n"
        "        return n\n"
        "    start = time.perf_counter()\n"
        "    count = asyncio.run(_run(n))\n"
        "    elapsed = time.perf_counter() - start\n"
        "    return {'count': count, 'elapsed': elapsed}\n"
    >>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_timer_throughput_single, [WarmupN]),
    {_, {ok, Result}} = timer:tc(fun() ->
        py:call('__main__', run_timer_throughput_single, [N])
    end),
    Count = maps:get(<<"count">>, Result),
    PythonElapsed = maps:get(<<"elapsed">>, Result),
    TimersPerSec = Count / PythonElapsed,
    io:format("Time: ~.3f sec | Timers/sec: ~w~n", [PythonElapsed, round(TimersPerSec)]),
    #{iterations => N, python_time_sec => PythonElapsed, timers_per_sec => TimersPerSec}.

timer_throughput_concurrent(Opts) ->
    N = maps:get(timer_iterations, Opts) div 4,
    Workers = maps:get(concurrent_workers, Opts),
    io:format("~n--- Timer Throughput (concurrent workers: ~p) ---~n", [Workers]),
    io:format("Iterations per worker: ~p~n", [N]),
    Code = <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "import threading\n"
        "import sys\n"
        "sys.path.insert(0, 'priv')\n"
        "from _erlang_impl import ErlangEventLoop\n"
        "\n"
        "def run_timer_throughput_concurrent(n_timers, n_workers):\n"
        "    results = []\n"
        "    errors = []\n"
        "    def run_in_thread(worker_id, num_timers):\n"
        "        try:\n"
        "            async def _run(n):\n"
        "                for _ in range(n):\n"
        "                    await asyncio.sleep(0)\n"
        "                return n\n"
        "            loop = ErlangEventLoop()\n"
        "            asyncio.set_event_loop(loop)\n"
        "            try:\n"
        "                count = loop.run_until_complete(_run(num_timers))\n"
        "                results.append(count)\n"
        "            finally:\n"
        "                loop.close()\n"
        "        except Exception as e:\n"
        "            errors.append(str(e))\n"
        "    start = time.perf_counter()\n"
        "    threads = []\n"
        "    for i in range(n_workers):\n"
        "        t = threading.Thread(target=run_in_thread, args=(i, n_timers))\n"
        "        threads.append(t)\n"
        "        t.start()\n"
        "    for t in threads:\n"
        "        t.join()\n"
        "    elapsed = time.perf_counter() - start\n"
        "    total = sum(results)\n"
        "    return {'total': total, 'elapsed': elapsed, 'errors': len(errors), 'workers': n_workers}\n"
    >>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_timer_throughput_concurrent, [100, 2]),
    {_, {ok, Result}} = timer:tc(fun() ->
        py:call('__main__', run_timer_throughput_concurrent, [N, Workers])
    end),
    Total = maps:get(<<"total">>, Result),
    PythonElapsed = maps:get(<<"elapsed">>, Result),
    Errors = maps:get(<<"errors">>, Result),
    TimersPerSec =
        case Total of
            0 -> 0.0;
            _ -> Total / PythonElapsed
        end,
    io:format(
        "Time: ~.3f sec | Total timers: ~w | Errors: ~p | Timers/sec: ~w~n",
        [PythonElapsed, Total, Errors, round(TimersPerSec)]
    ),
    #{
        workers => Workers,
        total_timers => Total,
        python_time_sec => PythonElapsed,
        errors => Errors,
        timers_per_sec => TimersPerSec
    }.

timer_latency(Opts) ->
    N = min(1000, maps:get(timer_iterations, Opts)),
    DelayMs = maps:get(timer_delay_ms, Opts),
    WarmupN = min(50, maps:get(warmup_iterations, Opts)),
    io:format("~n--- Timer Latency (target: ~pms) ---~n", [DelayMs]),
    io:format("Iterations: ~p~n", [N]),
    Code = <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "import statistics\n"
        "def run_timer_latency(n, delay_ms):\n"
        "    async def _run(n, delay_sec):\n"
        "        latencies = []\n"
        "        for _ in range(n):\n"
        "            start = time.perf_counter()\n"
        "            await asyncio.sleep(delay_sec)\n"
        "            elapsed = time.perf_counter() - start\n"
        "            latencies.append((elapsed - delay_sec) * 1000)\n"
        "        latencies.sort()\n"
        "        return {\n"
        "            'mean_ms': statistics.mean(latencies),\n"
        "            'p50_ms': latencies[int(len(latencies) * 0.50)],\n"
        "            'p95_ms': latencies[int(len(latencies) * 0.95)],\n"
        "            'p99_ms': latencies[int(len(latencies) * 0.99)] if len(latencies) >= 100 else latencies[-1],\n"
        "            'min_ms': min(latencies),\n"
        "            'max_ms': max(latencies),\n"
        "        }\n"
        "    return asyncio.run(_run(n, delay_ms / 1000.0))\n"
    >>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_timer_latency, [WarmupN, DelayMs]),
    {_, {ok, Stats}} = timer:tc(fun() ->
        py:call('__main__', run_timer_latency, [N, DelayMs])
    end),
    P95Ms = maps:get(<<"p95_ms">>, Stats),
    P99Ms = maps:get(<<"p99_ms">>, Stats),
    io:format(
        "Latency overhead (ms): mean=~.3f | p50=~.3f | p95=~.3f | p99=~.3f~n",
        [maps:get(<<"mean_ms">>, Stats), maps:get(<<"p50_ms">>, Stats), P95Ms, P99Ms]
    ),
    #{
        iterations => N,
        target_delay_ms => DelayMs,
        p95_latency_ms => P95Ms,
        p99_latency_ms => P99Ms
    }.

tcp_echo_single(Opts) ->
    N = maps:get(tcp_messages, Opts),
    MsgSize = maps:get(tcp_message_size, Opts),
    WarmupN = min(100, maps:get(warmup_iterations, Opts)),
    Timeout = maps:get(call_timeout, Opts),
    io:format("~n--- TCP Echo (single connection) ---~n"),
    io:format("Messages: ~p x ~p bytes~n", [N, MsgSize]),
    Code = <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "def run_tcp_echo_single(n_messages, msg_size):\n"
        "    async def _run(n_messages, msg_size):\n"
        "        async def handle_client(reader, writer):\n"
        "            try:\n"
        "                while True:\n"
        "                    data = await reader.read(msg_size)\n"
        "                    if not data:\n"
        "                        break\n"
        "                    writer.write(data)\n"
        "                    await writer.drain()\n"
        "            finally:\n"
        "                writer.close()\n"
        "                try: await writer.wait_closed()\n"
        "                except: pass\n"
        "        server = await asyncio.start_server(handle_client, '127.0.0.1', 0)\n"
        "        port = server.sockets[0].getsockname()[1]\n"
        "        reader, writer = await asyncio.open_connection('127.0.0.1', port)\n"
        "        msg = b'x' * msg_size\n"
        "        start = time.perf_counter()\n"
        "        for _ in range(n_messages):\n"
        "            writer.write(msg)\n"
        "            await writer.drain()\n"
        "            await reader.readexactly(msg_size)\n"
        "        elapsed = time.perf_counter() - start\n"
        "        writer.close()\n"
        "        try: await writer.wait_closed()\n"
        "        except: pass\n"
        "        server.close()\n"
        "        await server.wait_closed()\n"
        "        return {'count': n_messages, 'elapsed': elapsed}\n"
        "    return asyncio.run(_run(n_messages, msg_size))\n"
    >>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_tcp_echo_single, [WarmupN, MsgSize], #{}, Timeout),
    {_, {ok, Result}} = timer:tc(fun() ->
        py:call('__main__', run_tcp_echo_single, [N, MsgSize], #{}, Timeout)
    end),
    Count = maps:get(<<"count">>, Result),
    PythonElapsed = maps:get(<<"elapsed">>, Result),
    MsgsPerSec = Count / PythonElapsed,
    ThroughputMB = (Count * MsgSize) / PythonElapsed / 1024 / 1024,
    io:format(
        "Time: ~.3f sec | Messages/sec: ~w | Throughput: ~.2f MB/sec~n",
        [PythonElapsed, round(MsgsPerSec), ThroughputMB]
    ),
    #{
        messages => N,
        msg_size => MsgSize,
        python_time_sec => PythonElapsed,
        messages_per_sec => MsgsPerSec,
        throughput_mb_sec => ThroughputMB
    }.

tcp_echo_concurrent(Opts) ->
    N = maps:get(tcp_messages, Opts) div 4,
    MsgSize = maps:get(tcp_message_size, Opts),
    Connections = maps:get(concurrent_workers, Opts),
    Timeout = maps:get(call_timeout, Opts),
    io:format("~n--- TCP Echo (concurrent connections: ~p) ---~n", [Connections]),
    io:format("Messages per connection: ~p x ~p bytes~n", [N, MsgSize]),
    Code = <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "def run_tcp_echo_concurrent(n_messages, msg_size, n_connections):\n"
        "    async def _run(n_messages, msg_size, n_connections):\n"
        "        async def handle_client(reader, writer):\n"
        "            try:\n"
        "                while True:\n"
        "                    data = await reader.read(msg_size)\n"
        "                    if not data:\n"
        "                        break\n"
        "                    writer.write(data)\n"
        "                    await writer.drain()\n"
        "            finally:\n"
        "                writer.close()\n"
        "                try: await writer.wait_closed()\n"
        "                except: pass\n"
        "        async def run_client(port, num_msgs):\n"
        "            reader, writer = await asyncio.open_connection('127.0.0.1', port)\n"
        "            msg = b'x' * msg_size\n"
        "            for _ in range(num_msgs):\n"
        "                writer.write(msg)\n"
        "                await writer.drain()\n"
        "                await reader.readexactly(msg_size)\n"
        "            writer.close()\n"
        "            try: await writer.wait_closed()\n"
        "            except: pass\n"
        "            return num_msgs\n"
        "        server = await asyncio.start_server(handle_client, '127.0.0.1', 0)\n"
        "        port = server.sockets[0].getsockname()[1]\n"
        "        start = time.perf_counter()\n"
        "        tasks = [run_client(port, n_messages) for _ in range(n_connections)]\n"
        "        counts = await asyncio.gather(*tasks)\n"
        "        elapsed = time.perf_counter() - start\n"
        "        server.close()\n"
        "        await server.wait_closed()\n"
        "        return {'total': sum(counts), 'elapsed': elapsed, 'connections': n_connections}\n"
        "    return asyncio.run(_run(n_messages, msg_size, n_connections))\n"
    >>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_tcp_echo_concurrent, [50, MsgSize, 2], #{}, Timeout),
    {_, {ok, Result}} = timer:tc(fun() ->
        py:call('__main__', run_tcp_echo_concurrent, [N, MsgSize, Connections], #{}, Timeout)
    end),
    Total = maps:get(<<"total">>, Result),
    PythonElapsed = maps:get(<<"elapsed">>, Result),
    MsgsPerSec = Total / PythonElapsed,
    ThroughputMB = (Total * MsgSize) / PythonElapsed / 1024 / 1024,
    io:format(
        "Time: ~.3f sec | Total msgs: ~w | Messages/sec: ~w | Throughput: ~.2f MB/sec~n",
        [PythonElapsed, Total, round(MsgsPerSec), ThroughputMB]
    ),
    #{
        connections => Connections,
        total_messages => Total,
        msg_size => MsgSize,
        python_time_sec => PythonElapsed,
        messages_per_sec => MsgsPerSec,
        throughput_mb_sec => ThroughputMB
    }.

tcp_connections_scaling(Opts) ->
    N = maps:get(tcp_messages, Opts) div 8,
    MsgSize = maps:get(tcp_message_size, Opts),
    WorkerCounts = [1, 2, 4],
    Timeout = maps:get(call_timeout, Opts),
    io:format("~n--- TCP Connections Scaling ---~n"),
    io:format("Messages per connection: ~p x ~p bytes~n", [N, MsgSize]),
    io:format("Worker counts: ~p~n", [WorkerCounts]),
    Code = <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "def run_tcp_scaling(n_messages, msg_size, n_workers):\n"
        "    async def _run():\n"
        "        async def run_echo_pair(pair_id, n_msgs):\n"
        "            async def handle_client(reader, writer):\n"
        "                try:\n"
        "                    while True:\n"
        "                        data = await reader.read(msg_size)\n"
        "                        if not data:\n"
        "                            break\n"
        "                        writer.write(data)\n"
        "                        await writer.drain()\n"
        "                finally:\n"
        "                    writer.close()\n"
        "                    try: await writer.wait_closed()\n"
        "                    except: pass\n"
        "            server = await asyncio.start_server(handle_client, '127.0.0.1', 0)\n"
        "            port = server.sockets[0].getsockname()[1]\n"
        "            reader, writer = await asyncio.open_connection('127.0.0.1', port)\n"
        "            msg = b'x' * msg_size\n"
        "            for _ in range(n_msgs):\n"
        "                writer.write(msg)\n"
        "                await writer.drain()\n"
        "                await reader.readexactly(msg_size)\n"
        "            writer.close()\n"
        "            try: await writer.wait_closed()\n"
        "            except: pass\n"
        "            server.close()\n"
        "            await server.wait_closed()\n"
        "            return n_msgs\n"
        "        start = time.perf_counter()\n"
        "        tasks = [run_echo_pair(i, n_messages) for i in range(n_workers)]\n"
        "        counts = await asyncio.gather(*tasks)\n"
        "        elapsed = time.perf_counter() - start\n"
        "        return {'total': sum(counts), 'elapsed': elapsed, 'workers': n_workers, 'errors': 0}\n"
        "    return asyncio.run(_run())\n"
    >>,
    ok = py:exec(Code),
    ScalingResults = lists:map(
        fun(Workers) ->
            io:format("  Testing ~p worker(s)...~n", [Workers]),
            {ok, Result} = py:call(
                '__main__', run_tcp_scaling, [N, MsgSize, Workers], #{}, Timeout
            ),
            Total = maps:get(<<"total">>, Result),
            Elapsed = maps:get(<<"elapsed">>, Result),
            Errors = maps:get(<<"errors">>, Result),
            MsgsPerSec = Total / Elapsed,
            io:format(
                "    -> ~w msgs in ~.3f sec (~w/sec, ~p errors)~n",
                [Total, Elapsed, round(MsgsPerSec), Errors]
            ),
            #{
                workers => Workers,
                total => Total,
                elapsed => Elapsed,
                msgs_per_sec => MsgsPerSec,
                errors => Errors
            }
        end,
        WorkerCounts
    ),
    [Single | _] = ScalingResults,
    SingleRate = maps:get(msgs_per_sec, Single),
    Efficiency = lists:map(
        fun(R) ->
            W = maps:get(workers, R),
            Rate = maps:get(msgs_per_sec, R),
            Eff = (Rate / (SingleRate * W)) * 100,
            {W, Eff}
        end,
        ScalingResults
    ),
    io:format("Scaling efficiency: ~p~n", [Efficiency]),
    #{results => ScalingResults, efficiency => maps:from_list(Efficiency)}.

get_git_commit() ->
    case os:cmd("git rev-parse --short HEAD 2>/dev/null") of
        [] -> "unknown";
        Commit -> string:trim(Commit)
    end.

format_results(Results) ->
    case maps:get(timer_throughput_single, Results, undefined) of
        #{error := _} -> io:format("Timer throughput (single): ERROR~n");
        #{timers_per_sec := T} -> io:format("Timer throughput (single): ~w/sec~n", [round(T)]);
        _ -> ok
    end,
    case maps:get(timer_latency, Results, undefined) of
        #{error := _} ->
            io:format("Timer latency: ERROR~n");
        #{p95_latency_ms := P95, p99_latency_ms := P99} ->
            io:format("Timer latency: p95=~.3fms p99=~.3fms~n", [P95, P99]);
        _ ->
            ok
    end,
    case maps:get(tcp_echo_single, Results, undefined) of
        #{error := _} ->
            io:format("TCP echo (single): ERROR~n");
        #{messages_per_sec := M, throughput_mb_sec := MB} ->
            io:format("TCP echo (single): ~w msg/sec (~.2f MB/sec)~n", [round(M), MB]);
        _ ->
            ok
    end,
    case maps:get(timer_throughput_concurrent, Results, undefined) of
        #{error := _} ->
            io:format("Timer throughput (concurrent): ERROR~n");
        #{timers_per_sec := CT, workers := W} ->
            io:format("Timer throughput (~p workers): ~w/sec~n", [W, round(CT)]);
        _ ->
            ok
    end,
    case maps:get(tcp_echo_concurrent, Results, undefined) of
        #{error := _} ->
            io:format("TCP echo (concurrent): ERROR~n");
        #{messages_per_sec := CM, connections := C, throughput_mb_sec := CMB} ->
            io:format("TCP echo (~p connections): ~w msg/sec (~.2f MB/sec)~n", [C, round(CM), CMB]);
        _ ->
            ok
    end,
    case maps:get(tcp_connections_scaling, Results, undefined) of
        #{error := _} ->
            io:format("TCP scaling: ERROR~n");
        #{efficiency := Eff} ->
            io:format("TCP scaling efficiency:~n"),
            maps:foreach(fun(W, E) -> io:format("  ~p workers: ~.1f%~n", [W, E]) end, Eff);
        _ ->
            ok
    end,
    io:format("~n").

save_results(Results, Filename) ->
    Content = io_lib:format("~p.~n", [Results]),
    file:write_file(Filename, Content).
