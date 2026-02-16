"""Test module for erlang.async_call() functionality.

This module tests the async API that integrates with asyncio without
raising exceptions for control flow.
"""

import asyncio
import erlang


async def single_async_call():
    """Test a single async_call."""
    result = await erlang.async_call('async_multiply', 6, 7)
    return result


async def concurrent_async_calls():
    """Test multiple concurrent async_calls using asyncio.gather."""
    results = await asyncio.gather(
        erlang.async_call('async_multiply', 2, 3),
        erlang.async_call('async_multiply', 4, 5),
        erlang.async_call('async_multiply', 6, 7)
    )
    return results


def run_single_test():
    """Run single async_call test."""
    return asyncio.run(single_async_call())


def run_concurrent_test():
    """Run concurrent async_calls test."""
    return asyncio.run(concurrent_async_calls())
