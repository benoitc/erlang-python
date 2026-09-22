"""Helper for py_event_loop_pool_SUITE: a coroutine that sleeps on the loop.

Sleeps of a few milliseconds or more go through the Erlang timer path
(erlang:send_after via the loop's worker); the suite submits many of them
across the pool at once.
"""

import asyncio
import threading


async def nap(ms):
    """Sleep ``ms`` milliseconds, then return the thread that resumed us."""
    await asyncio.sleep(ms / 1000)
    return threading.get_ident()
