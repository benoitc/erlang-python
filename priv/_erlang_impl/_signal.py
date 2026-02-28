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
Signal handling via Erlang.

This module provides signal handling that integrates with Erlang's
signal trapping via os:set_signal/2. This allows signals to be
handled correctly even in subinterpreters and free-threaded Python.

Architecture:
- Erlang traps signals via os:set_signal/2
- py_signal_handler gen_server maintains signal->callback mappings
- When a signal arrives, Erlang dispatches to Python via NIF
- Python callback is executed in the event loop
"""

import signal as signal_mod
from typing import Callable, Dict, Optional, Tuple, Any

__all__ = ['SignalHandler', 'get_signal_name']


# Map signal numbers to names for better error messages
SIGNAL_NAMES = {
    signal_mod.SIGINT: 'SIGINT',
    signal_mod.SIGTERM: 'SIGTERM',
}

# Add Unix-specific signals if available
if hasattr(signal_mod, 'SIGHUP'):
    SIGNAL_NAMES[signal_mod.SIGHUP] = 'SIGHUP'
if hasattr(signal_mod, 'SIGUSR1'):
    SIGNAL_NAMES[signal_mod.SIGUSR1] = 'SIGUSR1'
if hasattr(signal_mod, 'SIGUSR2'):
    SIGNAL_NAMES[signal_mod.SIGUSR2] = 'SIGUSR2'
if hasattr(signal_mod, 'SIGCHLD'):
    SIGNAL_NAMES[signal_mod.SIGCHLD] = 'SIGCHLD'


def get_signal_name(sig: int) -> str:
    """Get the name of a signal.

    Args:
        sig: Signal number.

    Returns:
        Signal name (e.g., 'SIGINT') or 'signal N' if unknown.
    """
    return SIGNAL_NAMES.get(sig, f'signal {sig}')


class SignalHandler:
    """Signal handler that integrates with Erlang.

    This handler registers signals with Erlang's os:set_signal/2
    and receives callbacks when signals are delivered.

    Usage:
        handler = SignalHandler(loop)
        handler.add_signal_handler(signal.SIGINT, my_callback)
        # ... later ...
        handler.remove_signal_handler(signal.SIGINT)
    """

    # Signals that can be handled via Erlang
    SUPPORTED_SIGNALS = {
        signal_mod.SIGINT,
        signal_mod.SIGTERM,
    }

    # Add Unix-specific supported signals
    if hasattr(signal_mod, 'SIGHUP'):
        SUPPORTED_SIGNALS.add(signal_mod.SIGHUP)
    if hasattr(signal_mod, 'SIGUSR1'):
        SUPPORTED_SIGNALS.add(signal_mod.SIGUSR1)
    if hasattr(signal_mod, 'SIGUSR2'):
        SUPPORTED_SIGNALS.add(signal_mod.SIGUSR2)

    def __init__(self, loop):
        """Initialize the signal handler.

        Args:
            loop: The event loop to use for callbacks.
        """
        self._loop = loop
        self._handlers: Dict[int, Tuple[Callable, Tuple[Any, ...]]] = {}
        self._callback_ids: Dict[int, int] = {}  # sig -> callback_id
        self._pel = None

        try:
            import py_event_loop as pel
            self._pel = pel
        except ImportError:
            pass

    def add_signal_handler(self, sig: int, callback: Callable, *args: Any) -> None:
        """Add a signal handler.

        Args:
            sig: Signal number to handle.
            callback: Callback function to invoke.
            *args: Additional arguments for the callback.

        Raises:
            ValueError: If the signal is not supported.
            RuntimeError: If called from a non-main thread.
        """
        if sig not in self.SUPPORTED_SIGNALS:
            raise ValueError(
                f"{get_signal_name(sig)} is not supported. "
                f"Supported signals: {', '.join(get_signal_name(s) for s in sorted(self.SUPPORTED_SIGNALS))}"
            )

        # Check we're in the main thread
        import threading
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError(
                "Signal handlers can only be added from the main thread"
            )

        # Store the handler
        self._handlers[sig] = (callback, args)

        # Register with Erlang
        callback_id = self._loop._next_id()
        self._callback_ids[sig] = callback_id

        if self._pel is not None:
            try:
                self._pel._signal_add_handler(sig, callback_id)
            except AttributeError:
                # Fallback to Python's signal module
                self._use_python_signal(sig, callback, args)
        else:
            # Use Python's signal module directly
            self._use_python_signal(sig, callback, args)

    def _use_python_signal(self, sig: int, callback: Callable, args: Tuple) -> None:
        """Fall back to Python's signal module.

        Args:
            sig: Signal number.
            callback: Callback function.
            args: Callback arguments.
        """
        def handler(signum, frame):
            self._loop.call_soon_threadsafe(callback, *args)

        signal_mod.signal(sig, handler)

    def remove_signal_handler(self, sig: int) -> bool:
        """Remove a signal handler.

        Args:
            sig: Signal number to stop handling.

        Returns:
            True if a handler was removed, False if no handler was registered.
        """
        if sig not in self._handlers:
            return False

        del self._handlers[sig]
        callback_id = self._callback_ids.pop(sig, None)

        if self._pel is not None:
            try:
                self._pel._signal_remove_handler(sig)
            except AttributeError:
                signal_mod.signal(sig, signal_mod.SIG_DFL)
        else:
            signal_mod.signal(sig, signal_mod.SIG_DFL)

        return True

    def dispatch_signal(self, sig: int) -> None:
        """Dispatch a signal to its handler.

        Called from Erlang when a signal is received.

        Args:
            sig: Signal number that was received.
        """
        entry = self._handlers.get(sig)
        if entry is not None:
            callback, args = entry
            self._loop.call_soon(callback, *args)

    def close(self) -> None:
        """Remove all signal handlers."""
        for sig in list(self._handlers.keys()):
            self.remove_signal_handler(sig)
