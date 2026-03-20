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


def send_from_coroutine(pid, msg):
    """Run erlang.send from within a coroutine.

    This verifies erlang.send() works from async context.
    """
    import asyncio

    async def async_sender():
        erlang.send(pid, msg)
        return True

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(async_sender())
    finally:
        loop.close()


def send_multiple_from_coroutine(pid, messages):
    """Send multiple messages from within a coroutine.

    Tests that erlang.send() can be called multiple times in async context.
    """
    import asyncio

    async def async_sender():
        for msg in messages:
            erlang.send(pid, msg)
        return len(messages)

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(async_sender())
    finally:
        loop.close()


def send_is_nonblocking(pid, count):
    """Send many messages and verify it returns quickly.

    Returns elapsed time in seconds. Should be very small since
    erlang.send() is non-blocking fire-and-forget.
    """
    import time

    start = time.monotonic()
    for i in range(count):
        erlang.send(pid, ('msg', i))
    elapsed = time.monotonic() - start

    return elapsed


def send_interleaved_with_async(pid, messages):
    """Send messages interleaved with async operations.

    Verifies erlang.send() can be freely mixed with async/await.
    """
    import asyncio

    async def async_interleaved():
        results = []
        for i, msg in enumerate(messages):
            erlang.send(pid, msg)
            await asyncio.sleep(0)  # yield to event loop
            results.append(i)
        return results

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(async_interleaved())
    finally:
        loop.close()


def whereis_and_send(name, msg):
    """Look up a registered process by name and send it a message.

    Returns True if the process was found and message sent, False otherwise.
    """
    pid = erlang.whereis(name)
    if pid is not None:
        erlang.send(pid, msg)
        return True
    return False


# OWN_GIL test helpers for erlang.* functions

def whereis_basic(name):
    """Look up a registered process by name."""
    return erlang.whereis(name)


def atom_equality_test(atom1, atom2):
    """Test that two atoms are equal."""
    return atom1 == atom2


def atom_inequality_test(atom1, atom2):
    """Test that two different atoms are not equal."""
    return atom1 != atom2


def atom_roundtrip(atom):
    """Receive an atom and return it unchanged."""
    return atom


def atom_type_check(atom):
    """Verify a value is an erlang.Atom type."""
    return type(atom).__name__


def atom_has_value_attr(atom):
    """Check if atom has a value attribute."""
    return hasattr(atom, 'value')


def ref_type_check(ref):
    """Verify a value is an erlang.Ref type."""
    return isinstance(ref, erlang.Ref)


def ref_inequality_test(ref1, ref2):
    """Test that two different refs are not equal."""
    return ref1 != ref2


def pid_as_dict_key(pid):
    """Test using PID as dict key."""
    d = {pid: 'value'}
    return d.get(pid) == 'value'


def pid_in_set(pid):
    """Test using PID in a set."""
    s = {pid}
    return pid in s


# OWN_GIL test helpers for erlang.atom() creation

def atom_create_test():
    """Create atom using erlang.atom() in OWN_GIL and test equality."""
    import erlang
    a1 = erlang.atom('test_owngil_atom')
    a2 = erlang.atom('test_owngil_atom')
    return a1 == a2


def atom_create_different_test():
    """Test different atoms created with erlang.atom() are not equal."""
    import erlang
    a1 = erlang.atom('atom_x')
    a2 = erlang.atom('atom_y')
    return a1 != a2


def atom_cache_test():
    """Test atom caching - same name returns same object."""
    import erlang
    a1 = erlang.atom('cached_atom_owngil')
    a2 = erlang.atom('cached_atom_owngil')
    return a1 is a2  # Should be same object due to caching


def atom_type_name():
    """Get type name of atom created with erlang.atom()."""
    import erlang
    a = erlang.atom('type_check_atom')
    return type(a).__name__
