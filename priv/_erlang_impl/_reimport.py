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
import contextlib
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

    def new_modules(self):
        """A module dictionary holding only what this sandbox shares."""
        return {k: m for k, m in list(_real_modules.items()) if self.shared(k)}

    def run(self, module, func, args=(), kwargs=None):
        with _active(self, self.new_modules()):
            fn = getattr(importlib.import_module(module), func)
            return fn(*args, **(kwargs or {}))


@contextlib.contextmanager
def _active(sandbox, modules):
    """Make `modules` this thread's sys.modules for the duration; nested
    uses (a callback calling back in) restore the outer one."""
    prev = (getattr(_tls, 'modules', None), getattr(_tls, 'sandbox', None))
    _tls.sandbox = sandbox
    _tls.modules = modules
    try:
        yield
    finally:
        _tls.modules, _tls.sandbox = prev


_sandboxes = {}


def _sandbox(passthrough):
    key = tuple(sorted(_as_text(p) for p in passthrough))
    sandbox = _sandboxes.get(key)
    if sandbox is None:
        sandbox = _sandboxes[key] = Sandbox(key)
    return sandbox


def run(passthrough, module, func, args, kwargs):
    """Entry point called by py_session:run/5 on a reimport template."""
    return _sandbox(passthrough).run(_as_text(module), _as_text(func),
                                     list(args), dict(kwargs or {}))


# ---------------------------------------------------------------------------
# Sessions spanning several calls (py_session:new/1 on a reimport template).
# Each keeps its module dictionary and its __main__ namespace between calls,
# by session id, until session_close.
# ---------------------------------------------------------------------------

class _Session:
    __slots__ = ('sandbox', 'modules', 'globals')

    def __init__(self, sandbox):
        self.sandbox = sandbox
        self.modules = sandbox.new_modules()
        self.globals = {'__name__': '__main__', '__builtins__': builtins}


_sessions = {}


def _session(sid):
    try:
        return _sessions[sid]
    except KeyError:
        raise RuntimeError('reimport session %r is closed' % (sid,)) from None


def session_open(sid, passthrough):
    _sessions[sid] = _Session(_sandbox(passthrough))
    return True


def session_call(sid, module, func, args, kwargs):
    s = _session(sid)
    module, func = _as_text(module), _as_text(func)
    with _active(s.sandbox, s.modules):
        if module in ('__main__', ''):
            try:
                fn = s.globals[func]
            except KeyError:
                raise AttributeError("name '%s' is not defined in the session" % func) from None
        else:
            fn = getattr(importlib.import_module(module), func)
        return fn(*list(args), **dict(kwargs or {}))


def session_eval(sid, code, local_vars):
    """Evaluated in the session's namespace; like a context's eval,
    assignments made by the expression are not kept."""
    s = _session(sid)
    scope = dict(s.globals)
    scope.update({_as_text(k): v for k, v in dict(local_vars or {}).items()})
    with _active(s.sandbox, s.modules):
        return eval(compile(_as_text(code), '<session>', 'eval'), s.globals, scope)


def session_exec(sid, code):
    s = _session(sid)
    with _active(s.sandbox, s.modules):
        exec(compile(_as_text(code), '<session>', 'exec'), s.globals)
    return True


def session_close(sid):
    """Idempotent: the session process and the template may both close."""
    _sessions.pop(sid, None)
    return True


def session_count():
    return len(_sessions)


def _as_text(v):
    return v.decode('utf-8') if isinstance(v, (bytes, bytearray)) else str(v)
