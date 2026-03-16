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

"""
Erlang-backed asyncio event loop - uvloop-compatible API.

This module provides a drop-in replacement for uvloop, using Erlang's
BEAM VM scheduler for I/O multiplexing via enif_select.

Usage patterns (matching uvloop exactly):

    # Pattern 1: Recommended (Python 3.11+)
    import erlang
    erlang.run(main())

    # Pattern 2: With asyncio.Runner (Python 3.11+)
    import asyncio
    import erlang
    with asyncio.Runner(loop_factory=erlang.new_event_loop) as runner:
        runner.run(main())

    # Pattern 3: Legacy (deprecated in 3.12+)
    import asyncio
    import erlang
    erlang.install()
    asyncio.run(main())

    # Pattern 4: Manual
    import asyncio
    import erlang
    loop = erlang.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
"""

import os
import sys
import asyncio
import time
import warnings

# Install sandbox when running inside Erlang VM
# This must happen before any other imports to block subprocess/fork
try:
    import py_event_loop  # Only available when running in Erlang NIF
    from ._sandbox import install_sandbox
    install_sandbox()
except ImportError:
    pass  # Not running inside Erlang VM

from ._loop import ErlangEventLoop
from ._policy import ErlangEventLoopPolicy
from ._mode import detect_mode, ExecutionMode
from . import _reactor as reactor
from . import _channel as channel
from ._channel import Channel, reply, ChannelClosed

__all__ = [
    'run',
    'sleep',
    'spawn_task',
    'new_event_loop',
    'get_event_loop_policy',
    'install',
    'EventLoopPolicy',
    'ErlangEventLoopPolicy',
    'ErlangEventLoop',
    'detect_mode',
    'ExecutionMode',
    'reactor',
    'channel',
    'Channel',
    'reply',
    'ChannelClosed',
    'atom',
]

# Atom caching with configurable limit to prevent BEAM atom table exhaustion.
# The BEAM VM has a hard limit (~1M atoms) and crashes when exceeded.
# This provides a Python-level safety valve well under that limit.
_MAX_USER_ATOMS = int(os.environ.get('ERLANG_PYTHON_MAX_ATOMS', '10000'))
_atom_cache = {}

# Re-export for uvloop API compatibility
EventLoopPolicy = ErlangEventLoopPolicy


def atom(name):
    """Create an Erlang atom with safety limit.

    Atoms in Erlang are permanent and the BEAM VM has a hard limit
    (~1M atoms). This function provides a Python-level cache with
    a configurable limit to prevent atom table exhaustion from
    untrusted Python code.

    Args:
        name: The atom name as a string.

    Returns:
        An ErlangAtom object that converts to an Erlang atom.

    Raises:
        RuntimeError: If the atom limit is reached.

    The limit can be configured via the ERLANG_PYTHON_MAX_ATOMS
    environment variable (default: 10000).

    Example:
        >>> import erlang
        >>> ok = erlang.atom('ok')
        >>> error = erlang.atom('error')
    """
    if name in _atom_cache:
        return _atom_cache[name]

    if len(_atom_cache) >= _MAX_USER_ATOMS:
        raise RuntimeError(
            f"Atom limit ({_MAX_USER_ATOMS}) reached. "
            "Set ERLANG_PYTHON_MAX_ATOMS env var to increase."
        )

    # Import erlang module to access internal _atom function
    import erlang as _erlang
    result = _erlang._atom(name)
    _atom_cache[name] = result
    return result


def get_event_loop_policy() -> ErlangEventLoopPolicy:
    """Get an Erlang event loop policy instance.

    Returns a policy that uses ErlangEventLoop for event loops.
    This is used by Erlang code to set the default asyncio policy.

    Returns:
        ErlangEventLoopPolicy: A new policy instance.
    """
    return ErlangEventLoopPolicy()


def new_event_loop() -> ErlangEventLoop:
    """Create a new Erlang-backed event loop.

    Returns:
        ErlangEventLoop: A new event loop instance backed by Erlang's
            scheduler via enif_select. Each loop has its own isolated
            capsule for proper timer and FD event routing.
    """
    return ErlangEventLoop()


def run(main, *, debug=None, **run_kwargs):
    """Run a coroutine using Erlang event loop.

    The preferred way to run async code with Erlang backend.
    Equivalent to uvloop.run().

    Args:
        main: The coroutine to run.
        debug: Enable debug mode if True.
        **run_kwargs: Additional arguments passed to asyncio.run() or Runner.

    Returns:
        The return value of the coroutine.

    Example:
        import erlang

        async def main():
            await asyncio.sleep(1)
            return "done"

        result = erlang.run(main())
    """
    if sys.version_info >= (3, 12):
        # Python 3.12+ supports loop_factory in asyncio.run()
        return asyncio.run(
            main,
            loop_factory=new_event_loop,
            debug=debug,
            **run_kwargs
        )
    elif sys.version_info >= (3, 11):
        # Python 3.11 has asyncio.Runner with loop_factory
        with asyncio.Runner(loop_factory=new_event_loop, debug=debug) as runner:
            return runner.run(main)
    else:
        # Python 3.10 and earlier: manual loop management
        loop = new_event_loop()
        if debug is not None:
            loop.set_debug(debug)
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(main)
        finally:
            try:
                _cancel_all_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
                if hasattr(loop, 'shutdown_default_executor'):
                    loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                asyncio.set_event_loop(None)
                loop.close()


def sleep(seconds):
    """Sleep for the given duration.

    Works in both async and sync contexts:
    - Async context: Returns an awaitable (use with await)
    - Sync context: Blocks synchronously

    **Dirty Scheduler Release:**

    In async context, uses asyncio.sleep() which routes through the Erlang
    timer system via erlang:send_after. The dirty scheduler is released
    because the Python code yields back to the event loop.

    In sync context (when called from py:exec or py:eval), the sleep uses
    Erlang's receive/after via erlang.call('_py_sleep', seconds), which
    releases the dirty NIF scheduler thread. When called from py:call
    contexts, falls back to Python's time.sleep() which blocks the dirty
    scheduler but ensures correct time measurement behavior.

    Args:
        seconds: Duration to sleep in seconds (float or int).

    Returns:
        In async context: A coroutine that should be awaited.
        In sync context: None (blocks until sleep completes).

    Example:
        # Async context - releases dirty scheduler via event loop yield
        async def main():
            await erlang.sleep(0.5)  # Uses Erlang timer system

        # Sync context
        def handler():
            erlang.sleep(0.5)  # Blocks for 0.5 seconds
    """
    try:
        asyncio.get_running_loop()
        # Async context - return awaitable that uses Erlang timers
        return asyncio.sleep(seconds)
    except RuntimeError:
        # Sync context - use erlang.call to truly suspend and free dirty scheduler
        try:
            import erlang
            erlang.call('_py_sleep', seconds)
        except BaseException as e:
            # SuspensionRequiredException inherits from BaseException (not Exception).
            # When suspension is triggered, the NIF would replay the entire Python
            # function from the beginning after the callback completes. This causes
            # issues with time measurement since time.time() is called again during
            # replay. For sync sleep, we fall back to time.sleep() which blocks
            # correctly from the caller's perspective.
            # Note: This means the dirty scheduler is NOT freed during sync sleep
            # when running in context_call mode. For proper dirty scheduler release
            # in sync contexts, use py:exec/py:eval instead of py:call.
            time.sleep(seconds)


def spawn_task(coro, *, name=None):
    """Spawn an async task, working in both async and sync contexts.

    This function creates and schedules a task on the event loop, with
    automatic wakeup for Erlang-driven loops where the loop may not be
    actively polling.

    Args:
        coro: The coroutine to run as a task.
        name: Optional name for the task (Python 3.8+).

    Returns:
        asyncio.Task: The created task. Can be ignored (fire-and-forget)
        or awaited/cancelled if needed.

    Raises:
        RuntimeError: If no event loop is available or the loop is closed.

    Example:
        # From sync code called by Erlang
        def handle_request(data):
            erlang.spawn_task(process_async(data))
            return 'ok'

        # From async code
        async def handler():
            erlang.spawn_task(background_work())
            await other_work()
    """
    # Try to get the running loop first (works in async context)
    try:
        loop = asyncio.get_running_loop()
        # In async context, just create_task directly
        if name is not None:
            return loop.create_task(coro, name=name)
        else:
            return loop.create_task(coro)
    except RuntimeError:
        pass

    # Sync context: get the event loop
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        coro.close()  # Prevent "coroutine was never awaited" warning
        raise RuntimeError(
            "No event loop available. Ensure erlang is initialized or "
            "call from within an async context."
        )

    if loop.is_closed():
        coro.close()
        raise RuntimeError("Event loop is closed")

    # Create the task
    try:
        if name is not None:
            task = loop.create_task(coro, name=name)
        else:
            task = loop.create_task(coro)
    except Exception:
        coro.close()
        raise

    # Wake up the event loop to process the task
    # This is critical for sync context - without wakeup, the task
    # waits until the next event/timeout
    if hasattr(loop, '_pel') and hasattr(loop, '_loop_capsule'):
        # ErlangEventLoop - use native wakeup
        try:
            loop._pel._wakeup_for(loop._loop_capsule)
        except Exception:
            pass
    elif hasattr(loop, '_write_to_self'):
        # Standard asyncio loop - use self-pipe trick
        try:
            loop._write_to_self()
        except Exception:
            pass

    return task


def _run_async_from_erlang(module, func, args, kwargs):
    """Helper function called from Erlang to run async code.

    This is used by py_event_loop:run/3,4 to execute async Python
    functions from Erlang in a blocking manner.

    Args:
        module: Module name (string or bytes)
        func: Function name (string or bytes)
        args: Positional arguments (list)
        kwargs: Keyword arguments (dict)

    Returns:
        The result of the async function.
    """
    import importlib

    # Convert module/func to strings if needed
    if isinstance(module, bytes):
        module = module.decode('utf-8')
    if isinstance(func, bytes):
        func = func.decode('utf-8')

    # Import module and get function
    mod = importlib.import_module(module)
    fn = getattr(mod, func)

    # Call function to get coroutine
    if kwargs:
        coro = fn(*args, **kwargs)
    else:
        coro = fn(*args)

    # Run the coroutine using erlang.run()
    return run(coro)


def install():
    """Install ErlangEventLoopPolicy as the default event loop policy.

    This function is deprecated in Python 3.12+. Use run() instead.

    Example (legacy pattern):
        import asyncio
        import erlang

        erlang.install()
        asyncio.run(main())  # Uses Erlang event loop
    """
    if sys.version_info >= (3, 12):
        warnings.warn(
            "erlang.install() is deprecated in Python 3.12+. "
            "Use erlang.run(main()) instead.",
            DeprecationWarning,
            stacklevel=2
        )
    asyncio.set_event_loop_policy(ErlangEventLoopPolicy())


def _cancel_all_tasks(loop):
    """Cancel all tasks in the loop (helper for run())."""
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(
        asyncio.gather(*to_cancel, return_exceptions=True)
    )

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler({
                'message': 'unhandled exception during erlang.run() shutdown',
                'exception': task.exception(),
                'task': task,
            })
