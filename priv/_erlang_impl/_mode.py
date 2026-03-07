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
Python execution mode detection.

This module detects the Python execution mode to enable proper
event loop behavior for different Python configurations:

- free_threaded: Python 3.13+ with Py_GIL_DISABLED (no GIL)
- subinterp: Python 3.12+ with per-interpreter GIL
- shared_gil: Traditional Python with shared GIL
"""

import sys
import sysconfig
from enum import Enum
from typing import Optional

__all__ = ['ExecutionMode', 'detect_mode', 'is_free_threaded', 'is_subinterpreter']


class ExecutionMode(Enum):
    """Python execution mode."""
    FREE_THREADED = 'free_threaded'
    SUBINTERP = 'subinterp'
    SHARED_GIL = 'shared_gil'


# Cache the detected mode
_cached_mode: Optional[ExecutionMode] = None


def detect_mode() -> ExecutionMode:
    """Detect the current Python execution mode.

    Returns:
        ExecutionMode: One of FREE_THREADED, SUBINTERP, or SHARED_GIL.

    The detection logic:
    1. Check for free-threaded Python (3.13+ with Py_GIL_DISABLED)
    2. Check for subinterpreter mode (3.12+ per-interpreter GIL)
    3. Default to shared GIL mode
    """
    global _cached_mode
    if _cached_mode is not None:
        return _cached_mode

    mode = _detect_mode_impl()
    _cached_mode = mode
    return mode


def _detect_mode_impl() -> ExecutionMode:
    """Implementation of mode detection."""
    # Check for free-threaded Python (3.13+ with Py_GIL_DISABLED)
    if sys.version_info >= (3, 13):
        # Python 3.13+ has sys._is_gil_enabled()
        if hasattr(sys, '_is_gil_enabled'):
            if not sys._is_gil_enabled():
                return ExecutionMode.FREE_THREADED

    # Check sysconfig for Py_GIL_DISABLED build flag
    gil_disabled = sysconfig.get_config_var("Py_GIL_DISABLED")
    if gil_disabled == 1:
        return ExecutionMode.FREE_THREADED

    # Check for per-interpreter GIL (Python 3.12+)
    if sys.version_info >= (3, 12):
        # In Python 3.12+, subinterpreters can have their own GIL
        # We check if we're in a subinterpreter by comparing interpreter IDs
        if _is_in_subinterpreter():
            return ExecutionMode.SUBINTERP

    # Default to shared GIL mode
    return ExecutionMode.SHARED_GIL


def _is_in_subinterpreter() -> bool:
    """Check if we're running in a subinterpreter.

    Returns:
        bool: True if running in a subinterpreter, False if in main interpreter.
    """
    if sys.version_info < (3, 12):
        return False

    try:
        # Python 3.12+ has interpreter ID support
        import _interpreters
        current_id = _interpreters.get_current()
        main_id = _interpreters.get_main()
        return current_id != main_id
    except (ImportError, AttributeError):
        pass

    try:
        # Alternative: check via sys
        if hasattr(sys, 'get_interpreter_id'):
            # Main interpreter typically has ID 0
            return sys.get_interpreter_id() != 0
    except (AttributeError, TypeError):
        pass

    return False


def is_free_threaded() -> bool:
    """Check if Python is running in free-threaded mode (no GIL).

    Returns:
        bool: True if running without GIL, False otherwise.
    """
    return detect_mode() == ExecutionMode.FREE_THREADED


def is_subinterpreter() -> bool:
    """Check if we're running in a subinterpreter.

    Returns:
        bool: True if in a subinterpreter, False if in main interpreter.
    """
    return detect_mode() == ExecutionMode.SUBINTERP


def get_interpreter_id() -> int:
    """Get the current interpreter ID.

    Returns:
        int: The interpreter ID (0 for main interpreter).
    """
    if sys.version_info < (3, 12):
        return 0

    try:
        import _interpreters
        return _interpreters.get_current()
    except (ImportError, AttributeError):
        pass

    try:
        if hasattr(sys, 'get_interpreter_id'):
            return sys.get_interpreter_id()
    except (AttributeError, TypeError):
        pass

    return 0
