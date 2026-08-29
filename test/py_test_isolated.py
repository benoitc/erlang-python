"""Helpers for the isolated-mode suites (py_isolated_SUITE,
py_isolated_vm_SUITE, py_isolated_async_SUITE).

Every function here runs unchanged in worker and isolated mode; the suites
run both so a divergence between modes shows up as a failing pair.
"""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import erlang

# ---------------------------------------------------------------------------
# basics
# ---------------------------------------------------------------------------

def add(a, b):
    return a + b


def kwargs_probe(*args, **kwargs):
    return (list(args), sorted(kwargs.items()))


def identity(x):
    return x


def type_name(x):
    return type(x).__name__


def raise_value_error(msg):
    raise ValueError(msg)


def big_payload(n):
    return b'x' * n


def sleep_then(seconds, value):
    time.sleep(seconds)
    return value


def blocked_sleep(seconds):
    """Sleep with every signal blocked: a soft interrupt cannot land, only
    SIGKILL can end this. Exercises the kill backstop."""
    import signal
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1, signal.SIGINT})
    time.sleep(seconds)
    return 'slept'


def segfault():
    import ctypes
    ctypes.memset(0, 0, 1)


def close_control_socket():
    """Break the control socket from inside the child (mid-conversation),
    to exercise the fail-loud discipline."""
    import os
    import sys
    rt = sys.modules['_erlang_impl._isolated']
    # The runtime installed in this process
    import gc
    for obj in gc.get_objects():
        if isinstance(obj, rt.Runtime):
            os.close(obj.sock.fileno())
            break
    time.sleep(5)
    return 'unreachable'


def allocate(n_bytes):
    data = bytearray(n_bytes)
    return len(data)


def allocate_and_touch(n_bytes):
    """Allocate and write every page, so the memory is resident (a bare
    bytearray may stay untouched virtual memory on some platforms)."""
    data = bytearray(n_bytes)
    for i in range(0, n_bytes, 4096):
        data[i] = 1
    return len(data)


def spin(seconds):
    end = time.monotonic() + seconds
    n = 0
    while time.monotonic() < end:
        n += 1
    return n


def numpy_sum(n):
    import numpy
    return int(numpy.arange(n).sum())


# ---------------------------------------------------------------------------
# VM interaction: pids, send, whereis, callbacks
# ---------------------------------------------------------------------------

def is_pid(x):
    return isinstance(x, erlang.Pid)


def pid_equal(a, b):
    return a == b


def pid_hash_equal(a, b):
    return hash(a) == hash(b)


def pid_in_structure(pid):
    return {'owner': pid, 'list': [pid, (pid, 1)]}


def send(pid, msg):
    erlang.send(pid, msg)
    return True


def send_many(pid, n):
    for i in range(n):
        erlang.send(pid, ('item', i))
    erlang.send(pid, 'done')
    return n


def send_timing(pid, n):
    t0 = time.perf_counter()
    for i in range(n):
        erlang.send(pid, i)
    return (time.perf_counter() - t0) * 1000.0


def send_to_dead(pid):
    try:
        erlang.send(pid, 'msg')
        return 'sent'
    except erlang.ProcessError:
        return 'process_error'


def send_bad_pid():
    try:
        erlang.send('not_a_pid', 'msg')
        return 'sent'
    except TypeError:
        return 'type_error'


def send_from_coroutine(pid, msg):
    async def go():
        erlang.send(pid, msg)
        return 'sent'
    return asyncio.run(go())


def whereis(name):
    return erlang.whereis(name)


def suspension_is_base_exception():
    return (issubclass(erlang.SuspensionRequired, BaseException)
            and not issubclass(erlang.SuspensionRequired, Exception))


def call_inside_except_exception(name, arg):
    """A callback inside `except Exception` must work in every mode."""
    try:
        return ('ok', erlang.call(name, arg))
    except Exception as exc:
        return ('caught', type(exc).__name__)


def callback(name, *args):
    return erlang.call(name, *args)


def callback_error_type(name):
    try:
        erlang.call(name)
        return 'no_error'
    except Exception as exc:
        return type(exc).__name__


def callback_type(name):
    return type(erlang.call(name)).__name__


def ping_pong(name, rounds):
    """Each round calls Erlang with the round number and checks the answer."""
    for i in range(rounds):
        got = erlang.call(name, i)
        if got != i + 1:
            return ('mismatch', i, got)
    return rounds


def poll_feed(name, expect):
    """Pull terms from Erlang through a callback until `expect` items."""
    items = []
    while len(items) < expect:
        item = erlang.call(name)
        if item is None:
            time.sleep(0.001)
            continue
        items.append(item)
    return items


# ---------------------------------------------------------------------------
# Threads calling Erlang
# ---------------------------------------------------------------------------

def thread_calls(name, n_threads, n_calls):
    results = {}
    errors = []

    def worker(tid):
        try:
            results[tid] = [erlang.call(name, tid, i) for i in range(n_calls)]
        except Exception as exc:
            errors.append(repr(exc))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        return ('errors', errors)
    ok = all(results[t] == [t * 1000 + i for i in range(n_calls)]
             for t in range(n_threads))
    return ('ok', ok, n_threads * n_calls)


def pool_calls(name, n_workers, n_calls):
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(erlang.call, name, i) for i in range(n_calls)]
        got = [f.result() for f in futures]
    return got == [i * 2 for i in range(n_calls)]


def pool_error(name):
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut = pool.submit(erlang.call, name)
        try:
            fut.result()
            return 'no_error'
        except Exception as exc:
            return type(exc).__name__


def pool_nested(name):
    """A pool thread makes an erlang.call whose argument is itself the
    result of an erlang.call: two round trips nested in one thread."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: erlang.call(name, erlang.call(name, 20))).result()


# ---------------------------------------------------------------------------
# Actor-style state
# ---------------------------------------------------------------------------

class Counter:
    def __init__(self):
        self.value = 0

    def increment(self, by=1):
        self.value += by
        return self.value


_counter = Counter()


def counter_increment(by=1):
    return _counter.increment(by)


def counter_value():
    return _counter.value


# ---------------------------------------------------------------------------
# asyncio
# ---------------------------------------------------------------------------

async def async_add(a, b):
    await asyncio.sleep(0)
    return a + b


async def async_sleep_gather(n, seconds):
    async def one(i):
        await asyncio.sleep(seconds)
        return i
    return await asyncio.gather(*[one(i) for i in range(n)])


async def async_raise(msg):
    await asyncio.sleep(0)
    raise KeyError(msg)


async def async_big(n):
    await asyncio.sleep(0)
    return b'y' * n


async def async_erlang_call(name, x):
    return await erlang.async_call(name, x)


async def async_erlang_calls(name, n):
    return await asyncio.gather(*[erlang.async_call(name, i) for i in range(n)])


async def async_erlang_call_error(name):
    try:
        await erlang.async_call(name)
        return 'no_error'
    except Exception as exc:
        return type(exc).__name__


async def async_send(pid, msg):
    erlang.send(pid, msg)
    return 'sent'


async def stream_to(pid, n):
    async def agen():
        for i in range(n):
            await asyncio.sleep(0)
            yield i
    async for item in agen():
        erlang.send(pid, ('item', item))
    erlang.send(pid, 'done')
    return n


async def task_value(i):
    await asyncio.sleep(0.001 * (i % 5))
    return i * i


async def slow_task(seconds):
    await asyncio.sleep(seconds)
    return 'slow_done'


async def block_loop(seconds):
    time.sleep(seconds)
    return 'unblocked'


def run_helper_compat():
    """erlang.run / erlang.sleep / erlang.spawn_task behave like stdlib."""
    async def main():
        await erlang.sleep(0.001)
        t = erlang.spawn_task(async_add(1, 2))
        return await t
    return erlang.run(main())
