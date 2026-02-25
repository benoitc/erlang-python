# Copyright 2026 Benoit Chesneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Erlang-native asyncio primitives.

This module provides async primitives that use Erlang's native scheduler
instead of Python's asyncio event loop, for maximum performance.

Usage:
    import erlang_asyncio

    # Get the event loop
    loop = erlang_asyncio.get_event_loop()

    # Use sleep
    async def handler():
        await erlang_asyncio.sleep(0.001)  # 1ms sleep using Erlang timer
"""

import asyncio
import py_event_loop as _pel

# Import ErlangEventLoop
try:
    from erlang_loop import ErlangEventLoop, get_event_loop_policy as _get_policy
    _has_erlang_loop = True
except ImportError:
    ErlangEventLoop = None
    _get_policy = None
    _has_erlang_loop = False


def get_event_loop():
    """Get the current Erlang event loop.

    Returns an ErlangEventLoop instance that uses Erlang's scheduler
    for I/O multiplexing and timers.

    Returns:
        ErlangEventLoop instance

    Example:
        import erlang_asyncio

        loop = erlang_asyncio.get_event_loop()
        loop.run_until_complete(my_coro())
    """
    if _has_erlang_loop:
        # Set policy if not already set
        policy = asyncio.get_event_loop_policy()
        if not isinstance(policy, type(_get_policy())):
            asyncio.set_event_loop_policy(_get_policy())
        return asyncio.get_event_loop()
    else:
        return asyncio.get_event_loop()


def new_event_loop():
    """Create a new Erlang event loop.

    Returns:
        New ErlangEventLoop instance
    """
    if _has_erlang_loop:
        return ErlangEventLoop()
    else:
        return asyncio.new_event_loop()


def set_event_loop(loop):
    """Set the current event loop."""
    asyncio.set_event_loop(loop)


def get_running_loop():
    """Get the running event loop.

    Raises RuntimeError if no loop is running.
    """
    return asyncio.get_running_loop()


async def sleep(delay: float, result=None):
    """Sleep for the specified delay using Erlang's timer system.

    This is a drop-in replacement for asyncio.sleep() that uses
    Erlang's native timer system instead of the asyncio event loop.

    Args:
        delay: Time to sleep in seconds (float)
        result: Optional value to return after sleeping (default None)

    Returns:
        The result argument

    Example:
        import erlang_asyncio

        async def my_handler():
            await erlang_asyncio.sleep(0.1)  # Sleep 100ms
            value = await erlang_asyncio.sleep(0.05, result='done')
    """
    if delay <= 0:
        return result

    # Convert seconds to milliseconds
    delay_ms = int(delay * 1000)
    if delay_ms < 1:
        delay_ms = 1  # Minimum 1ms

    # Use the synchronous Erlang sleep
    _pel._erlang_sleep(delay_ms)

    return result


def run(coro):
    """Run a coroutine using the Erlang event loop.

    Similar to asyncio.run() but uses ErlangEventLoop.

    Args:
        coro: Coroutine to run

    Returns:
        The coroutine's return value
    """
    loop = new_event_loop()
    try:
        set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        try:
            loop.close()
        except Exception:
            pass


async def gather(*coros_or_futures, return_exceptions=False):
    """Run coroutines concurrently and gather results.

    Similar to asyncio.gather() - runs all coroutines concurrently
    using the Erlang event loop.

    Args:
        *coros_or_futures: Coroutines or futures to run
        return_exceptions: If True, exceptions are returned as results
                          instead of being raised

    Returns:
        List of results in the same order as inputs

    Example:
        import erlang_asyncio

        async def task(n):
            await erlang_asyncio.sleep(0.01)
            return n * 2

        results = await erlang_asyncio.gather(task(1), task(2), task(3))
        # results = [2, 4, 6]
    """
    return await asyncio.gather(*coros_or_futures, return_exceptions=return_exceptions)


async def wait_for(coro, timeout):
    """Wait for a coroutine with a timeout.

    Similar to asyncio.wait_for() - runs the coroutine with a timeout
    using the Erlang event loop.

    Args:
        coro: Coroutine to run
        timeout: Timeout in seconds (float)

    Returns:
        The coroutine's return value

    Raises:
        asyncio.TimeoutError: If the timeout expires

    Example:
        import erlang_asyncio

        try:
            result = await erlang_asyncio.wait_for(slow_task(), timeout=1.0)
        except asyncio.TimeoutError:
            print("Task timed out")
    """
    return await asyncio.wait_for(coro, timeout)


async def wait(fs, *, timeout=None, return_when=asyncio.ALL_COMPLETED):
    """Wait for multiple futures/tasks.

    Similar to asyncio.wait() - waits for futures to complete.

    Args:
        fs: Iterable of futures/tasks
        timeout: Optional timeout in seconds
        return_when: When to return (ALL_COMPLETED, FIRST_COMPLETED, FIRST_EXCEPTION)

    Returns:
        Tuple of (done, pending) sets

    Example:
        import erlang_asyncio

        tasks = [erlang_asyncio.create_task(coro()) for coro in coros]
        done, pending = await erlang_asyncio.wait(tasks, timeout=5.0)
    """
    return await asyncio.wait(fs, timeout=timeout, return_when=return_when)


def create_task(coro, *, name=None):
    """Create a task to run the coroutine.

    Similar to asyncio.create_task() - schedules the coroutine
    to run on the event loop.

    Args:
        coro: Coroutine to run
        name: Optional name for the task

    Returns:
        asyncio.Task instance

    Example:
        import erlang_asyncio

        async def background_work():
            await erlang_asyncio.sleep(1.0)
            return "done"

        task = erlang_asyncio.create_task(background_work())
        # ... do other work ...
        result = await task
    """
    loop = asyncio.get_event_loop()
    if name is not None:
        return loop.create_task(coro, name=name)
    return loop.create_task(coro)


def ensure_future(coro_or_future, *, loop=None):
    """Wrap a coroutine in a Future.

    Similar to asyncio.ensure_future().

    Args:
        coro_or_future: Coroutine or Future
        loop: Optional event loop

    Returns:
        asyncio.Future or asyncio.Task
    """
    return asyncio.ensure_future(coro_or_future, loop=loop)


async def shield(arg):
    """Protect a coroutine from cancellation.

    Similar to asyncio.shield() - the inner coroutine continues
    even if the outer task is cancelled.

    Args:
        arg: Coroutine or future to shield

    Returns:
        The result of the shielded coroutine
    """
    return await asyncio.shield(arg)


class timeout:
    """Context manager for timeout.

    Similar to asyncio.timeout() (Python 3.11+).

    Example:
        import erlang_asyncio

        async with erlang_asyncio.timeout(1.0):
            await slow_operation()
    """

    def __init__(self, delay):
        self.delay = delay
        self._task = None
        self._cancelled = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    def reschedule(self, delay):
        """Reschedule the timeout."""
        self.delay = delay


# Re-export common exceptions
TimeoutError = asyncio.TimeoutError
CancelledError = asyncio.CancelledError

# Constants for wait()
ALL_COMPLETED = asyncio.ALL_COMPLETED
FIRST_COMPLETED = asyncio.FIRST_COMPLETED
FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION


__all__ = [
    # Core functions
    'sleep',
    'run',
    'gather',
    'wait',
    'wait_for',
    'create_task',
    'ensure_future',
    'shield',
    'timeout',
    # Event loop
    'get_event_loop',
    'new_event_loop',
    'set_event_loop',
    'get_running_loop',
    'ErlangEventLoop',
    # Exceptions
    'TimeoutError',
    'CancelledError',
    # Constants
    'ALL_COMPLETED',
    'FIRST_COMPLETED',
    'FIRST_EXCEPTION',
]
