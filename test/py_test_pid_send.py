"""Test module for PID serialization and erlang.send().

Tests that:
- Erlang PIDs arrive in Python as erlang.Pid objects
- erlang.Pid objects round-trip back to Erlang as real PIDs
- erlang.send(pid, term) delivers messages to Erlang processes
- SuspensionRequired is not caught by `except Exception`
"""

import erlang


def get_pid_type(pid):
    """Return the type name of the received argument."""
    return type(pid).__qualname__


def is_erlang_pid(pid):
    """Return True if the argument is an erlang.Pid."""
    return isinstance(pid, erlang.Pid)


def round_trip_pid(pid):
    """Return the PID unchanged - tests that it converts back to Erlang PID."""
    return pid


def pid_equality(a, b):
    """Return True if two erlang.Pid objects are equal."""
    return a == b


def pid_inequality(a, b):
    """Return True if two different erlang.Pid objects are not equal."""
    return a != b


def pid_hash_equal(a, b):
    """Return True if two equal erlang.Pid objects produce the same hash."""
    return hash(a) == hash(b)


def send_message(pid, msg):
    """Send a message to an Erlang process using erlang.send()."""
    erlang.send(pid, msg)
    return True


def send_multiple(pid, messages):
    """Send multiple messages to an Erlang process."""
    for msg in messages:
        erlang.send(pid, msg)
    return len(messages)


def send_complex_term(pid):
    """Send a complex term (tuple of various types) to an Erlang process."""
    erlang.send(pid, ('hello', 42, [1, 2, 3], {'key': 'value'}, True))
    return True


def send_bad_pid():
    """Try to call erlang.send with a non-PID - should raise TypeError."""
    try:
        erlang.send("not_a_pid", "msg")
        return False  # Should not reach here
    except TypeError:
        return True


def send_dead_process_raises_process_error(pid):
    """Verify sending to a dead process raises erlang.ProcessError."""
    try:
        erlang.send(pid, "msg")
        return False
    except erlang.ProcessError:
        return True


def process_error_is_exception_subclass():
    """Verify erlang.ProcessError is a subclass of Exception (catchable)."""
    return issubclass(erlang.ProcessError, Exception)


def suspension_not_caught_by_except_exception():
    """Verify SuspensionRequired is NOT caught by `except Exception`.

    Calls a registered Erlang function inside a try/except Exception block.
    With BaseException inheritance, SuspensionRequired passes through.
    """
    try:
        result = erlang.call('test_pid_echo', 42)
        return ('ok', result)
    except Exception as e:
        # SuspensionRequired should NOT land here anymore
        return ('caught', str(type(e).__name__))


def suspension_caught_by_except_base():
    """Verify SuspensionRequired IS caught by `except BaseException`.

    This confirms the exception still exists and propagates, just not
    as a subclass of Exception.
    """
    return issubclass(erlang.SuspensionRequired, BaseException)


def suspension_not_subclass_of_exception():
    """Verify SuspensionRequired is NOT a subclass of Exception."""
    return not issubclass(erlang.SuspensionRequired, Exception)
