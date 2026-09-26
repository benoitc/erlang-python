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
Re-import runs for sessions started with `start => reimport`.

Each run gets its own module dictionary: the modules the template passes
through (the standard library, `erlang`, and the names it lists) are shared
with the interpreter, every other module is imported again inside the run,
so module globals start fresh and nothing a run changes in them is seen by
the next one. This is the isolation of Temporal's Python workflow sandbox,
without its restrictions on time, randomness and I/O.

The swap is per thread. `sys.modules` and `builtins.__import__` are replaced
once, in each interpreter, by stand-ins that use the running thread's module
dictionary when it has one and the interpreter's otherwise. A worker context
is one thread of the main interpreter, so several worker contexts run their
sandboxes at once without seeing each other's.

Passthrough modules are imported by the interpreter itself, not inside the
run: C code looks modules up in the interpreter's own table and must find
them there.
"""

import builtins
import importlib
import sys
import threading
from collections.abc import MutableMapping

_tls = threading.local()
_install_lock = threading.Lock()
_real_import = builtins.__import__
_real_modules = sys.modules

_ALWAYS_SHARED = frozenset(sys.stdlib_module_names) | {'erlang', '_erlang_impl', 'py_event_loop'}


class _ThreadModules(MutableMapping):
    """sys.modules as seen by each thread: its run's dictionary while a run
    is active, the interpreter's otherwise. A mapping rather than a dict
    subclass, so C code that reads sys.modules goes through these methods
    instead of an empty dict storage."""

    def _d(self):
        mods = getattr(_tls, 'modules', None)
        return _real_modules if mods is None else mods

    def __getitem__(self, key):
        return self._d()[key]

    def __setitem__(self, key, value):
        self._d()[key] = value

    def __delitem__(self, key):
        del self._d()[key]

    def __contains__(self, key):
        return key in self._d()

    def __iter__(self):
        return iter(list(self._d()))

    def __len__(self):
        return len(self._d())

    def get(self, key, default=None):
        return self._d().get(key, default)

    def copy(self):
        return dict(self._d())

    def __repr__(self):
        return repr(self._d())


def _install():
    global _real_modules
    with _install_lock:
        if not isinstance(sys.modules, _ThreadModules):
            _real_modules = sys.modules
            sys.modules = _ThreadModules()
            builtins.__import__ = _import


def _import(name, globals=None, locals=None, fromlist=(), level=0):
    mods = getattr(_tls, 'modules', None)
    if mods is None:
        return _real_import(name, globals, locals, fromlist, level)
    sandbox = _tls.sandbox
    if level == 0 and sandbox.shared(name) and name not in mods:
        sandbox.pass_through(name, fromlist, mods)
    return importlib.__import__(name, globals, locals, fromlist, level)


class Sandbox:
    """Runs functions in fresh module dictionaries. `passthrough` names
    (top-level packages) are shared with the interpreter."""

    def __init__(self, passthrough=()):
        _install()
        self.passthrough = frozenset(passthrough)

    def shared(self, name):
        root = name.split('.', 1)[0]
        return root in _ALWAYS_SHARED or root in self.passthrough

    def pass_through(self, name, fromlist, mods):
        """Import `name` in the interpreter, then share its module objects
        (with its parents and loaded submodules) with the run."""
        _tls.modules = None
        try:
            _real_import(name, None, None, fromlist or (), 0)
            for sub in fromlist or ():
                full = name + '.' + sub
                if full not in _real_modules:
                    try:
                        _real_import(full)
                    except ImportError:
                        pass   # an attribute, not a submodule
        finally:
            _tls.modules = mods
        parts = name.split('.')
        for i in range(1, len(parts) + 1):
            parent = '.'.join(parts[:i])
            if parent in _real_modules:
                mods[parent] = _real_modules[parent]
        prefix = name + '.'
        for key, mod in list(_real_modules.items()):
            if key.startswith(prefix) and key not in mods:
                mods[key] = mod

    def run(self, module, func, args=(), kwargs=None):
        prev = (getattr(_tls, 'modules', None), getattr(_tls, 'sandbox', None))
        _tls.sandbox = self
        _tls.modules = {k: m for k, m in list(_real_modules.items()) if self.shared(k)}
        try:
            fn = getattr(importlib.import_module(module), func)
            return fn(*args, **(kwargs or {}))
        finally:
            _tls.modules, _tls.sandbox = prev


_sandboxes = {}


def run(passthrough, module, func, args, kwargs):
    """Entry point called by py_session:run/5 on a reimport template."""
    key = tuple(sorted(_as_text(p) for p in passthrough))
    sandbox = _sandboxes.get(key)
    if sandbox is None:
        sandbox = _sandboxes[key] = Sandbox(key)
    return sandbox.run(_as_text(module), _as_text(func), list(args), dict(kwargs or {}))


def _as_text(v):
    return v.decode('utf-8') if isinstance(v, (bytes, bytearray)) else str(v)
