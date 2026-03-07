#!/usr/bin/env python3
"""Test to verify timer dispatch path."""

import sys
sys.path.insert(0, 'priv')

import asyncio
import time
from _erlang_impl import ErlangEventLoop

def run_test():
    results = {}

    # Check policy
    policy = asyncio.get_event_loop_policy()
    results['policy'] = type(policy).__name__

    # Check what asyncio.run creates
    async def check_loop():
        loop = asyncio.get_running_loop()
        return {
            'type': type(loop).__name__,
            'handle': str(getattr(loop, '_loop_handle', 'NO ATTR'))
        }

    results['loop_info'] = asyncio.run(check_loop())

    # Timer performance test
    n = 5000

    async def timer_test(n):
        for _ in range(n):
            await asyncio.sleep(0)

    # Default loop test
    start = time.perf_counter()
    asyncio.run(timer_test(n))
    default_time = time.perf_counter() - start
    results['default_time'] = default_time
    results['default_rate'] = int(n/default_time)

    # Direct loop test
    loop = ErlangEventLoop()
    asyncio.set_event_loop(loop)
    start = time.perf_counter()
    try:
        loop.run_until_complete(timer_test(n))
    finally:
        loop.close()
    direct_time = time.perf_counter() - start
    results['direct_time'] = direct_time
    results['direct_rate'] = int(n/direct_time)

    results['ratio'] = default_time/direct_time

    return results
