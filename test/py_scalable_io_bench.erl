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
    py:bind(),
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
    py:unbind(),
    io:format("~n========================================~n"),
    io:format("Summary~n"),
    io:format("========================================~n"),
    format_results(Results),
    Results.

safe_bench(Fun) ->
    try Fun()
    catch Class:Reason:Stack ->
        io:format("ERROR: ~p:~p~n~p~n", [Class, Reason, Stack]),
        #{error => {Class, Reason}}
    end.

timer_throughput_single(Opts) ->
    N = maps:get(timer_iterations, Opts),
    WarmupN = maps:get(warmup_iterations, Opts),
    io:format("~n--- Timer Throughput (single worker) ---~n"),
    io:format("Iterations: ~p~n", [N]),
    Code = <<"
import asyncio
import time
def run_timer_throughput_single(n):
    async def _run(n):
        for _ in range(n):
            await asyncio.sleep(0)
        return n
    start = time.perf_counter()
    count = asyncio.run(_run(n))
    elapsed = time.perf_counter() - start
    return {'count': count, 'elapsed': elapsed}
">>,
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
    Code = <<"
import asyncio
import time
import threading
import sys
sys.path.insert(0, 'priv')
from erlang_loop import ErlangEventLoop

def run_timer_throughput_concurrent(n_timers, n_workers):
    results = []
    errors = []
    def run_in_thread(worker_id, num_timers):
        try:
            async def _run(n):
                for _ in range(n):
                    await asyncio.sleep(0)
                return n
            loop = ErlangEventLoop()
            asyncio.set_event_loop(loop)
            try:
                count = loop.run_until_complete(_run(num_timers))
                results.append(count)
            finally:
                loop.close()
        except Exception as e:
            errors.append(str(e))
    start = time.perf_counter()
    threads = []
    for i in range(n_workers):
        t = threading.Thread(target=run_in_thread, args=(i, n_timers))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    total = sum(results)
    return {'total': total, 'elapsed': elapsed, 'errors': len(errors), 'workers': n_workers}
">>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_timer_throughput_concurrent, [100, 2]),
    {_, {ok, Result}} = timer:tc(fun() ->
        py:call('__main__', run_timer_throughput_concurrent, [N, Workers])
    end),
    Total = maps:get(<<"total">>, Result),
    PythonElapsed = maps:get(<<"elapsed">>, Result),
    Errors = maps:get(<<"errors">>, Result),
    TimersPerSec = case Total of 0 -> 0.0; _ -> Total / PythonElapsed end,
    io:format("Time: ~.3f sec | Total timers: ~w | Errors: ~p | Timers/sec: ~w~n",
              [PythonElapsed, Total, Errors, round(TimersPerSec)]),
    #{workers => Workers, total_timers => Total, python_time_sec => PythonElapsed,
      errors => Errors, timers_per_sec => TimersPerSec}.

timer_latency(Opts) ->
    N = min(1000, maps:get(timer_iterations, Opts)),
    DelayMs = maps:get(timer_delay_ms, Opts),
    WarmupN = min(50, maps:get(warmup_iterations, Opts)),
    io:format("~n--- Timer Latency (target: ~pms) ---~n", [DelayMs]),
    io:format("Iterations: ~p~n", [N]),
    Code = <<"
import asyncio
import time
import statistics
def run_timer_latency(n, delay_ms):
    async def _run(n, delay_sec):
        latencies = []
        for _ in range(n):
            start = time.perf_counter()
            await asyncio.sleep(delay_sec)
            elapsed = time.perf_counter() - start
            latencies.append((elapsed - delay_sec) * 1000)
        latencies.sort()
        return {
            'mean_ms': statistics.mean(latencies),
            'p50_ms': latencies[int(len(latencies) * 0.50)],
            'p95_ms': latencies[int(len(latencies) * 0.95)],
            'p99_ms': latencies[int(len(latencies) * 0.99)] if len(latencies) >= 100 else latencies[-1],
            'min_ms': min(latencies),
            'max_ms': max(latencies),
        }
    return asyncio.run(_run(n, delay_ms / 1000.0))
">>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_timer_latency, [WarmupN, DelayMs]),
    {_, {ok, Stats}} = timer:tc(fun() ->
        py:call('__main__', run_timer_latency, [N, DelayMs])
    end),
    P95Ms = maps:get(<<"p95_ms">>, Stats),
    P99Ms = maps:get(<<"p99_ms">>, Stats),
    io:format("Latency overhead (ms): mean=~.3f | p50=~.3f | p95=~.3f | p99=~.3f~n",
              [maps:get(<<"mean_ms">>, Stats), maps:get(<<"p50_ms">>, Stats), P95Ms, P99Ms]),
    #{iterations => N, target_delay_ms => DelayMs, p95_latency_ms => P95Ms, p99_latency_ms => P99Ms}.

tcp_echo_single(Opts) ->
    N = maps:get(tcp_messages, Opts),
    MsgSize = maps:get(tcp_message_size, Opts),
    WarmupN = min(100, maps:get(warmup_iterations, Opts)),
    Timeout = maps:get(call_timeout, Opts),
    io:format("~n--- TCP Echo (single connection) ---~n"),
    io:format("Messages: ~p x ~p bytes~n", [N, MsgSize]),
    Code = <<"
import asyncio
import time
def run_tcp_echo_single(n_messages, msg_size):
    async def _run(n_messages, msg_size):
        async def handle_client(reader, writer):
            try:
                while True:
                    data = await reader.read(msg_size)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
            finally:
                writer.close()
                try: await writer.wait_closed()
                except: pass
        server = await asyncio.start_server(handle_client, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        msg = b'x' * msg_size
        start = time.perf_counter()
        for _ in range(n_messages):
            writer.write(msg)
            await writer.drain()
            await reader.readexactly(msg_size)
        elapsed = time.perf_counter() - start
        writer.close()
        try: await writer.wait_closed()
        except: pass
        server.close()
        await server.wait_closed()
        return {'count': n_messages, 'elapsed': elapsed}
    return asyncio.run(_run(n_messages, msg_size))
">>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_tcp_echo_single, [WarmupN, MsgSize], #{}, Timeout),
    {_, {ok, Result}} = timer:tc(fun() ->
        py:call('__main__', run_tcp_echo_single, [N, MsgSize], #{}, Timeout)
    end),
    Count = maps:get(<<"count">>, Result),
    PythonElapsed = maps:get(<<"elapsed">>, Result),
    MsgsPerSec = Count / PythonElapsed,
    ThroughputMB = (Count * MsgSize) / PythonElapsed / 1024 / 1024,
    io:format("Time: ~.3f sec | Messages/sec: ~w | Throughput: ~.2f MB/sec~n",
              [PythonElapsed, round(MsgsPerSec), ThroughputMB]),
    #{messages => N, msg_size => MsgSize, python_time_sec => PythonElapsed,
      messages_per_sec => MsgsPerSec, throughput_mb_sec => ThroughputMB}.

tcp_echo_concurrent(Opts) ->
    N = maps:get(tcp_messages, Opts) div 4,
    MsgSize = maps:get(tcp_message_size, Opts),
    Connections = maps:get(concurrent_workers, Opts),
    Timeout = maps:get(call_timeout, Opts),
    io:format("~n--- TCP Echo (concurrent connections: ~p) ---~n", [Connections]),
    io:format("Messages per connection: ~p x ~p bytes~n", [N, MsgSize]),
    Code = <<"
import asyncio
import time
def run_tcp_echo_concurrent(n_messages, msg_size, n_connections):
    async def _run(n_messages, msg_size, n_connections):
        async def handle_client(reader, writer):
            try:
                while True:
                    data = await reader.read(msg_size)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
            finally:
                writer.close()
                try: await writer.wait_closed()
                except: pass
        async def run_client(port, num_msgs):
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            msg = b'x' * msg_size
            for _ in range(num_msgs):
                writer.write(msg)
                await writer.drain()
                await reader.readexactly(msg_size)
            writer.close()
            try: await writer.wait_closed()
            except: pass
            return num_msgs
        server = await asyncio.start_server(handle_client, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        start = time.perf_counter()
        tasks = [run_client(port, n_messages) for _ in range(n_connections)]
        counts = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start
        server.close()
        await server.wait_closed()
        return {'total': sum(counts), 'elapsed': elapsed, 'connections': n_connections}
    return asyncio.run(_run(n_messages, msg_size, n_connections))
">>,
    ok = py:exec(Code),
    {ok, _} = py:call('__main__', run_tcp_echo_concurrent, [50, MsgSize, 2], #{}, Timeout),
    {_, {ok, Result}} = timer:tc(fun() ->
        py:call('__main__', run_tcp_echo_concurrent, [N, MsgSize, Connections], #{}, Timeout)
    end),
    Total = maps:get(<<"total">>, Result),
    PythonElapsed = maps:get(<<"elapsed">>, Result),
    MsgsPerSec = Total / PythonElapsed,
    ThroughputMB = (Total * MsgSize) / PythonElapsed / 1024 / 1024,
    io:format("Time: ~.3f sec | Total msgs: ~w | Messages/sec: ~w | Throughput: ~.2f MB/sec~n",
              [PythonElapsed, Total, round(MsgsPerSec), ThroughputMB]),
    #{connections => Connections, total_messages => Total, msg_size => MsgSize,
      python_time_sec => PythonElapsed, messages_per_sec => MsgsPerSec,
      throughput_mb_sec => ThroughputMB}.

tcp_connections_scaling(Opts) ->
    N = maps:get(tcp_messages, Opts) div 8,
    MsgSize = maps:get(tcp_message_size, Opts),
    WorkerCounts = [1, 2, 4],
    Timeout = maps:get(call_timeout, Opts),
    io:format("~n--- TCP Connections Scaling ---~n"),
    io:format("Messages per connection: ~p x ~p bytes~n", [N, MsgSize]),
    io:format("Worker counts: ~p~n", [WorkerCounts]),
    Code = <<"
import asyncio
import time
def run_tcp_scaling(n_messages, msg_size, n_workers):
    async def _run():
        async def run_echo_pair(pair_id, n_msgs):
            async def handle_client(reader, writer):
                try:
                    while True:
                        data = await reader.read(msg_size)
                        if not data:
                            break
                        writer.write(data)
                        await writer.drain()
                finally:
                    writer.close()
                    try: await writer.wait_closed()
                    except: pass
            server = await asyncio.start_server(handle_client, '127.0.0.1', 0)
            port = server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            msg = b'x' * msg_size
            for _ in range(n_msgs):
                writer.write(msg)
                await writer.drain()
                await reader.readexactly(msg_size)
            writer.close()
            try: await writer.wait_closed()
            except: pass
            server.close()
            await server.wait_closed()
            return n_msgs
        start = time.perf_counter()
        tasks = [run_echo_pair(i, n_messages) for i in range(n_workers)]
        counts = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start
        return {'total': sum(counts), 'elapsed': elapsed, 'workers': n_workers, 'errors': 0}
    return asyncio.run(_run())
">>,
    ok = py:exec(Code),
    ScalingResults = lists:map(fun(Workers) ->
        io:format("  Testing ~p worker(s)...~n", [Workers]),
        {ok, Result} = py:call('__main__', run_tcp_scaling, [N, MsgSize, Workers], #{}, Timeout),
        Total = maps:get(<<"total">>, Result),
        Elapsed = maps:get(<<"elapsed">>, Result),
        Errors = maps:get(<<"errors">>, Result),
        MsgsPerSec = Total / Elapsed,
        io:format("    -> ~w msgs in ~.3f sec (~w/sec, ~p errors)~n",
                  [Total, Elapsed, round(MsgsPerSec), Errors]),
        #{workers => Workers, total => Total, elapsed => Elapsed,
          msgs_per_sec => MsgsPerSec, errors => Errors}
    end, WorkerCounts),
    [Single | _] = ScalingResults,
    SingleRate = maps:get(msgs_per_sec, Single),
    Efficiency = lists:map(fun(R) ->
        W = maps:get(workers, R),
        Rate = maps:get(msgs_per_sec, R),
        Eff = (Rate / (SingleRate * W)) * 100,
        {W, Eff}
    end, ScalingResults),
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
        #{error := _} -> io:format("Timer latency: ERROR~n");
        #{p95_latency_ms := P95, p99_latency_ms := P99} ->
            io:format("Timer latency: p95=~.3fms p99=~.3fms~n", [P95, P99]);
        _ -> ok
    end,
    case maps:get(tcp_echo_single, Results, undefined) of
        #{error := _} -> io:format("TCP echo (single): ERROR~n");
        #{messages_per_sec := M, throughput_mb_sec := MB} ->
            io:format("TCP echo (single): ~w msg/sec (~.2f MB/sec)~n", [round(M), MB]);
        _ -> ok
    end,
    case maps:get(timer_throughput_concurrent, Results, undefined) of
        #{error := _} -> io:format("Timer throughput (concurrent): ERROR~n");
        #{timers_per_sec := CT, workers := W} ->
            io:format("Timer throughput (~p workers): ~w/sec~n", [W, round(CT)]);
        _ -> ok
    end,
    case maps:get(tcp_echo_concurrent, Results, undefined) of
        #{error := _} -> io:format("TCP echo (concurrent): ERROR~n");
        #{messages_per_sec := CM, connections := C, throughput_mb_sec := CMB} ->
            io:format("TCP echo (~p connections): ~w msg/sec (~.2f MB/sec)~n", [C, round(CM), CMB]);
        _ -> ok
    end,
    case maps:get(tcp_connections_scaling, Results, undefined) of
        #{error := _} -> io:format("TCP scaling: ERROR~n");
        #{efficiency := Eff} ->
            io:format("TCP scaling efficiency:~n"),
            maps:foreach(fun(W, E) -> io:format("  ~p workers: ~.1f%~n", [W, E]) end, Eff);
        _ -> ok
    end,
    io:format("~n").

save_results(Results, Filename) ->
    Content = io_lib:format("~p.~n", [Results]),
    file:write_file(Filename, Content).
