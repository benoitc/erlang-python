"""Helpers for py_reentrant_SUITE: functions called with py:call that call
back into Erlang with erlang.call, where the Erlang side calls py:call again.
"""

import erlang


def down(n, depth):
    """Recurse through Erlang: nest_step does py:call(down, [n + 1, depth - 1])."""
    if depth <= 0:
        return n
    return erlang.call('nest_step', n, depth)


def chain(x):
    """Three sequential erlang.call in one function."""
    step1 = erlang.call('add_ten', x)
    step2 = erlang.call('multiply_by_two', step1)
    return erlang.call('subtract_five', step2)


def minus(x, y):
    """Plain helper the subtract_five callback calls with py:call."""
    return x - y
