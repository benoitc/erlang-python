"""
Test helpers for multi-loop isolation testing.

These helpers are used to verify that multiple ErlangEventLoop instances
can operate concurrently without cross-talk between their pending queues.

Usage from Erlang:
    py:call(test_multi_loop, run_concurrent_timers, [100])
"""

import asyncio
import time
from typing import List, Tuple, Dict, Set

# Try to import the actual event loop module
try:
    import py_event_loop as pel
    from erlang_loop import ErlangEventLoop
    HAS_EVENT_LOOP = True
except ImportError:
    HAS_EVENT_LOOP = False


def check_available() -> bool:
    """Check if multi-loop testing is available."""
    return HAS_EVENT_LOOP


def create_isolated_loops(count: int = 2) -> List:
    """
    Create multiple isolated ErlangEventLoop instances.

    This tests that _loop_new() creates truly independent loops.
    With proper isolation, each loop has its own:
    - pending queue
    - callback ID space (though we use Python-side IDs)
    - condition variable for wakeup

    Returns:
        List of ErlangEventLoop instances
    """
    if not HAS_EVENT_LOOP:
        raise RuntimeError("Event loop module not available")

    loops = []
    for _ in range(count):
        loop = ErlangEventLoop()
        loops.append(loop)
    return loops


def run_concurrent_timers_on_loop(loop, timer_count: int, base_id: int) -> List[int]:
    """
    Schedule timers on a loop and collect the callback IDs that fire.

    Args:
        loop: ErlangEventLoop instance
        timer_count: Number of timers to schedule
        base_id: Starting callback ID (for identification)

    Returns:
        List of callback IDs that were dispatched
    """
    received_ids = []

    def make_callback(cid):
        def callback():
            received_ids.append(cid)
        return callback

    # Schedule all timers with small delays
    handles = []
    for i in range(timer_count):
        callback_id = base_id + i
        handle = loop.call_later(0.001, make_callback(callback_id))  # 1ms delay
        handles.append(handle)

    # Run the loop until all timers fire or timeout
    start = time.monotonic()
    timeout = 1.0  # 1 second timeout

    while len(received_ids) < timer_count:
        if time.monotonic() - start > timeout:
            break
        loop._run_once()

    return received_ids


def test_two_loops_concurrent_timers(timer_count: int = 100) -> Dict:
    """
    Test that two loops can schedule concurrent timers without cross-talk.

    Returns:
        Dict with test results:
        - loop_a_count: Number of events received by loop A
        - loop_b_count: Number of events received by loop B
        - loop_a_ids: Set of callback IDs received by loop A
        - loop_b_ids: Set of callback IDs received by loop B
        - overlap: Any IDs that appear in both (should be empty)
        - passed: True if test passed
    """
    if not HAS_EVENT_LOOP:
        return {"error": "Event loop not available", "passed": False}

    loop_a = ErlangEventLoop()
    loop_b = ErlangEventLoop()

    loop_a_base = 1       # IDs 1-100
    loop_b_base = 1001    # IDs 1001-1100

    # Run timers on both loops
    # Note: In real implementation with _for methods, these would be isolated
    ids_a = run_concurrent_timers_on_loop(loop_a, timer_count, loop_a_base)
    ids_b = run_concurrent_timers_on_loop(loop_b, timer_count, loop_b_base)

    # Check for overlap
    set_a = set(ids_a)
    set_b = set(ids_b)
    overlap = set_a & set_b

    # Expected IDs
    expected_a = set(range(loop_a_base, loop_a_base + timer_count))
    expected_b = set(range(loop_b_base, loop_b_base + timer_count))

    passed = (
        len(ids_a) == timer_count and
        len(ids_b) == timer_count and
        set_a == expected_a and
        set_b == expected_b and
        len(overlap) == 0
    )

    loop_a.close()
    loop_b.close()

    return {
        "loop_a_count": len(ids_a),
        "loop_b_count": len(ids_b),
        "loop_a_ids": sorted(ids_a),
        "loop_b_ids": sorted(ids_b),
        "overlap": sorted(overlap),
        "passed": passed
    }


def test_cross_isolation() -> Dict:
    """
    Test that events on loop A are not visible to loop B.

    Returns:
        Dict with test results
    """
    if not HAS_EVENT_LOOP:
        return {"error": "Event loop not available", "passed": False}

    loop_a = ErlangEventLoop()
    loop_b = ErlangEventLoop()

    a_received = []
    b_received = []

    def callback_a():
        a_received.append("event_a")

    def callback_b():
        b_received.append("event_b")

    # Schedule only on loop A
    loop_a.call_soon(callback_a)

    # Run loop A - should receive the event
    loop_a._run_once()

    # Run loop B - should NOT receive loop A's event
    loop_b._run_once()

    passed = (
        len(a_received) == 1 and
        len(b_received) == 0
    )

    loop_a.close()
    loop_b.close()

    return {
        "loop_a_events": len(a_received),
        "loop_b_events": len(b_received),
        "passed": passed
    }


def test_cleanup_no_leak() -> Dict:
    """
    Test that destroying one loop doesn't affect another.

    Returns:
        Dict with test results
    """
    if not HAS_EVENT_LOOP:
        return {"error": "Event loop not available", "passed": False}

    loop_a = ErlangEventLoop()
    loop_b = ErlangEventLoop()

    b_received = []

    def callback_b():
        b_received.append("event_b")

    # Schedule timer on loop B for after loop A is destroyed
    loop_b.call_later(0.05, callback_b)  # 50ms delay

    # Close loop A
    loop_a.close()

    # Wait and run loop B - should still work
    time.sleep(0.1)  # 100ms
    loop_b._run_once()

    passed = len(b_received) == 1

    loop_b.close()

    return {
        "loop_b_events": len(b_received),
        "passed": passed
    }
