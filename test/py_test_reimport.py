"""Imported again in every re-import run (py_session_SUITE reimport groups)."""

import json
import pickle
import typing
from dataclasses import dataclass

import erlang
import py_test_reimport_shared as shared

COUNT = {'n': 0}


@dataclass
class Point:
    x: int
    y: int


def bump():
    COUNT['n'] += 1
    return COUNT['n']


def bump_shared():
    return shared.bump()


def mark_stdlib(value):
    """The standard library is shared: this is seen by later runs."""
    json._reimport_mark = value
    return value


def read_stdlib_mark():
    return getattr(json, '_reimport_mark', None)


def call_back(n):
    return erlang.call('reimport_nested', n)


def typed():
    """typing reads sys.modules from Python: it finds the run's module."""
    return sorted(typing.get_type_hints(Point))


def pickle_shared():
    """A class from a shared module pickles."""
    p = pickle.loads(pickle.dumps(shared.Pair(1, 2)))
    return p.a + p.b


def pickle_reimported():
    """The C pickle looks the class's module up in the interpreter's own
    table, where a re-imported module is not."""
    try:
        pickle.dumps(Point(1, 2))
    except Exception as exc:
        return type(exc).__name__
    return 'pickled'


def fail(msg):
    raise ValueError(msg)


def whoami():
    return __name__
