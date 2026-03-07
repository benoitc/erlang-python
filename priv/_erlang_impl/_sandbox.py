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
Sandbox module using Python's audit hook mechanism (PEP 578).

When Python runs embedded in Erlang, certain operations are unsafe and must
be blocked at a low level. This module uses audit hooks to intercept these
operations before they execute.

Blocked operations:
- subprocess.Popen - would use fork() which corrupts Erlang VM
- os.system, os.popen - shell execution
- os.fork, os.forkpty - process forking
- os.exec*, os.spawn* - process execution
- os.posix_spawn* - POSIX process spawning
"""

import sys

__all__ = ['install_sandbox', 'is_sandboxed']

_sandboxed = False

# Subprocess/process audit events to block
_SUBPROCESS_EVENTS = frozenset({
    'subprocess.Popen',
    'os.system',
    'os.popen',
    'os.fork',
    'os.forkpty',
    'os.posix_spawn',
    'os.posix_spawnp',
})

# os.exec* and os.spawn* prefixes
_EXEC_PREFIXES = ('os.exec', 'os.spawn')

_ERROR_MSG = (
    "blocked in Erlang VM context. "
    "fork()/exec() would corrupt the Erlang runtime. "
    "Use Erlang ports (open_port/2) for subprocess management."
)


def _sandbox_hook(event, args):
    """Audit hook that blocks dangerous subprocess operations."""
    # Fast path: check direct matches
    if event in _SUBPROCESS_EVENTS:
        raise RuntimeError(f"{event} is {_ERROR_MSG}")

    # Check exec/spawn prefixes
    for prefix in _EXEC_PREFIXES:
        if event.startswith(prefix):
            raise RuntimeError(f"{event} is {_ERROR_MSG}")


def install_sandbox():
    """Install the sandbox audit hook.

    Once installed, audit hooks cannot be removed. This blocks all
    subprocess/fork/exec operations for the lifetime of the Python
    interpreter.
    """
    global _sandboxed
    if _sandboxed:
        return

    sys.addaudithook(_sandbox_hook)
    _sandboxed = True


def is_sandboxed():
    """Check if sandbox is active."""
    return _sandboxed
