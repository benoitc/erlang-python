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

import sys
import asyncio
import warnings

from ._loop import ErlangEventLoop
from ._policy import ErlangEventLoopPolicy
from ._mode import detect_mode, ExecutionMode

__all__ = [
    'run',
    'new_event_loop',
    'install',
    'EventLoopPolicy',
    'ErlangEventLoopPolicy',
    'ErlangEventLoop',
    'detect_mode',
    'ExecutionMode',
]

# Re-export for uvloop API compatibility
EventLoopPolicy = ErlangEventLoopPolicy


def new_event_loop() -> ErlangEventLoop:
    """Create a new Erlang-backed event loop.

    Returns:
        ErlangEventLoop: A new event loop instance backed by Erlang's
            scheduler via enif_select.
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
