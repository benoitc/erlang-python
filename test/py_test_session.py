"""Helpers for py_session_SUITE. Imported by the template, so everything at
module level here runs once, in the zygote (start => fork) or in each child
(start => spawn)."""

import os
import random
import sys
import threading
import time

import erlang
from erlang import call   # resolved before any session exists

STATE = {}
IMPORTED_IN = os.getpid()


def mark(value):
    STATE['mark'] = value
    return value


def peek():
    return STATE.get('mark')


def pid():
    return os.getpid()


def cwd():
    return os.getcwd()


def env_names():
    return sorted(os.environ)


def env_get(name):
    return os.environ.get(name)


def env_set(name, value):
    os.environ[name] = value
    return value


def put_module(name):
    import types
    sys.modules[name] = types.ModuleType(name)
    return name in sys.modules


def has_module(name):
    return name in sys.modules


def start_thread():
    threading.Thread(target=time.sleep, args=(60,), daemon=True).start()
    return threading.active_count()


def thread_count():
    return threading.active_count()


def write_file(name, data):
    with open(name, 'w') as f:
        f.write(data)
    return sorted(os.listdir('.'))


def list_cwd():
    return sorted(os.listdir('.'))


def random_value():
    return random.random()


def hash_probe():
    return (hash('erlang-python'), repr(list({'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'})))


def reenter(n):
    """Python -> Erlang -> this same session -> ... n levels deep."""
    return 0 if n == 0 else call('sess_reenter', erlang.self(), n) + 1


def cross(n):
    """Calls into another session through Erlang."""
    return call('sess_cross', n)


def nested_sleep_then(seconds):
    """Erlang runs sleep() in this session with a short timeout: the nested
    request is interrupted, this outer one carries on."""
    inner = call('sess_nested_sleep', erlang.self(), seconds)
    return ('outer-done', inner)


def sleep(seconds):
    time.sleep(seconds)
    return 'slept'


async def async_add(a, b):
    return a + b


def fd_is_open(fd):
    os.fstat(fd)
    return True


def imported_in():
    return IMPORTED_IN


def call_during_preload():
    try:
        call('anything')
    except RuntimeError as exc:
        return str(exc)
    return 'no error'


def abort():
    os.abort()


def open_files():
    return len(os.listdir('/dev/fd'))
