#!/usr/bin/env python3
"""Simple event loop benchmark."""

import asyncio
import time
import statistics

def run_benchmark():
    results = {}

    # Timer benchmark
    async def bench_timers(n=100):
        latencies = []
        for _ in range(n):
            start = time.perf_counter()
            await asyncio.sleep(0.001)  # 1ms
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed - 1.0)  # overhead only
        return statistics.mean(latencies), sorted(latencies)[int(n*0.95)]

    # Callback benchmark
    async def bench_callbacks(n=10000):
        count = [0]
        done = asyncio.Event()
        loop = asyncio.get_running_loop()

        def callback():
            count[0] += 1
            if count[0] < n:
                loop.call_soon(callback)
            else:
                loop.call_soon(done.set)

        start = time.perf_counter()
        loop.call_soon(callback)
        await done.wait()
        elapsed = time.perf_counter() - start
        return count[0] / elapsed

    # Test Erlang loop
    try:
        from _erlang_impl import ErlangEventLoop
        erl_loop = ErlangEventLoop()
        asyncio.set_event_loop(erl_loop)

        erl_timer_mean, erl_timer_p95 = erl_loop.run_until_complete(bench_timers())
        erl_cb_rate = erl_loop.run_until_complete(bench_callbacks())
        erl_loop.close()

        results['erlang'] = {
            'timer_mean': erl_timer_mean,
            'timer_p95': erl_timer_p95,
            'callback_rate': erl_cb_rate
        }
    except Exception as e:
        results['erlang_error'] = str(e)

    # Test standard loop
    std_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(std_loop)

    std_timer_mean, std_timer_p95 = std_loop.run_until_complete(bench_timers())
    std_cb_rate = std_loop.run_until_complete(bench_callbacks())
    std_loop.close()

    results['standard'] = {
        'timer_mean': std_timer_mean,
        'timer_p95': std_timer_p95,
        'callback_rate': std_cb_rate
    }

    return results

if __name__ == '__main__':
    results = run_benchmark()
    print(results)
else:
    # When exec'd from Erlang
    benchmark_results = run_benchmark()
