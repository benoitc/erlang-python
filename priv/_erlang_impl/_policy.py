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
Event loop policy for Erlang-backed asyncio integration.

This module provides an asyncio event loop policy that creates
Erlang-backed event loops, enabling transparent integration with
asyncio.run() and other asyncio APIs.
"""

import asyncio
import threading
from typing import Optional

__all__ = ['ErlangEventLoopPolicy']


class ErlangEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    """Event loop policy that uses Erlang-backed event loops.

    This policy creates ErlangEventLoop instances for the main thread
    and optionally for child threads depending on configuration.

    Usage:
        import asyncio
        import erlang

        # Install the policy
        asyncio.set_event_loop_policy(erlang.EventLoopPolicy())

        # Now asyncio.run() uses Erlang event loop
        asyncio.run(main())

    Note:
        This approach is deprecated in Python 3.12+.
        Use erlang.run() instead.
    """

    def __init__(self):
        """Initialize the policy with thread-local storage."""
        self._local = threading.local()
        self._main_thread_id = threading.main_thread().ident
        self._watcher = None

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get the event loop for the current context.

        Creates a new event loop if one doesn't exist for the current thread.

        Returns:
            asyncio.AbstractEventLoop: The event loop for this thread.

        Raises:
            RuntimeError: If there is no current event loop and the current
                thread is not the main thread (and no loop was set explicitly).
        """
        loop = getattr(self._local, 'loop', None)
        if loop is not None and not loop.is_closed():
            return loop

        # Check if we're in the main thread
        if threading.current_thread().ident == self._main_thread_id:
            loop = self.new_event_loop()
            self.set_event_loop(loop)
            return loop

        # For non-main threads, raise error (matches asyncio behavior)
        raise RuntimeError(
            "There is no current event loop in thread %r. "
            "Use asyncio.set_event_loop() or set an explicit loop."
            % threading.current_thread().name
        )

    def set_event_loop(self, loop: Optional[asyncio.AbstractEventLoop]) -> None:
        """Set the event loop for the current context.

        Args:
            loop: The event loop to set, or None to clear.
        """
        self._local.loop = loop

    def new_event_loop(self) -> asyncio.AbstractEventLoop:
        """Create a new Erlang-backed event loop.

        Returns:
            ErlangEventLoop: A new event loop instance.

        Note:
            Always returns ErlangEventLoop when using this policy.
            The Erlang event loop handles thread safety internally.
        """
        # Import here to avoid circular imports
        from ._loop import ErlangEventLoop
        return ErlangEventLoop()

    # Child watcher methods (for subprocess support)

    def get_child_watcher(self):
        """Get the child watcher.

        Deprecated in Python 3.12.
        """
        if self._watcher is None:
            self._init_watcher()
        return self._watcher

    def set_child_watcher(self, watcher):
        """Set the child watcher.

        Deprecated in Python 3.12.
        """
        self._watcher = watcher

    def _init_watcher(self):
        """Initialize the child watcher.

        Uses ThreadedChildWatcher which works well with Erlang integration.
        """
        import sys
        if sys.version_info >= (3, 12):
            # Child watchers are deprecated in 3.12
            return

        if hasattr(asyncio, 'ThreadedChildWatcher'):
            self._watcher = asyncio.ThreadedChildWatcher()
        elif hasattr(asyncio, 'SafeChildWatcher'):
            self._watcher = asyncio.SafeChildWatcher()


class _ErlangChildWatcher:
    """Child watcher that delegates to Erlang for process monitoring.

    This watcher uses Erlang ports and monitors instead of SIGCHLD,
    making it compatible with subinterpreters and free-threaded Python.
    """

    def __init__(self):
        self._callbacks = {}
        self._loop = None

    def attach_loop(self, loop):
        """Attach to an event loop."""
        self._loop = loop

    def close(self):
        """Close the watcher."""
        self._callbacks.clear()
        self._loop = None

    def is_active(self):
        """Return True if the watcher is active."""
        return self._loop is not None and not self._loop.is_closed()

    def add_child_handler(self, pid, callback, *args):
        """Register a callback for when a child process exits.

        Args:
            pid: Process ID to watch.
            callback: Callback function(pid, returncode, *args).
            *args: Additional arguments for the callback.
        """
        self._callbacks[pid] = (callback, args)
        # TODO: Use Erlang port monitoring

    def remove_child_handler(self, pid):
        """Remove the handler for a child process.

        Returns:
            bool: True if handler was removed, False if not found.
        """
        return self._callbacks.pop(pid, None) is not None

    def _do_waitpid(self, pid, returncode):
        """Called when a child process exits.

        Args:
            pid: Process ID that exited.
            returncode: Exit code of the process.
        """
        entry = self._callbacks.pop(pid, None)
        if entry is not None:
            callback, args = entry
            if self._loop is not None and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(callback, pid, returncode, *args)
