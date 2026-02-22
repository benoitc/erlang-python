%%% @doc Integration test suite for isolated event loops with real asyncio workloads.
%%%
%%% Tests that multiple ErlangEventLoop instances created with `isolated=True`
%%% can run real asyncio operations concurrently without interference.
-module(py_multi_loop_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_isolated_loop_creation/1,
    test_two_loops_concurrent_callbacks/1,
    test_two_loops_isolation/1,
    test_multiple_loops_lifecycle/1,
    test_isolated_loops_with_timers/1
]).

all() ->
    [
        test_isolated_loop_creation,
        test_two_loops_concurrent_callbacks,
        test_two_loops_isolation,
        test_multiple_loops_lifecycle,
        test_isolated_loops_with_timers
    ].

init_per_suite(Config) ->
    %% Stop application if already running (from other test suites)
    _ = application:stop(erlang_python),
    timer:sleep(200),

    {ok, _} = application:ensure_all_started(erlang_python),
    timer:sleep(200),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    py:unbind(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    py:unbind(),
    ok.

%% ============================================================================
%% Test: Verify isolated loop can be created with isolated=True
%% ============================================================================

test_isolated_loop_creation(_Config) ->
    ok = py:exec(<<"
from erlang_loop import ErlangEventLoop

# Default loop uses shared global
default_loop = ErlangEventLoop()
assert default_loop._loop_handle is None, 'Default loop should use global (None handle)'
default_loop.close()

# Isolated loop gets its own handle
isolated_loop = ErlangEventLoop(isolated=True)
assert isolated_loop._loop_handle is not None, 'Isolated loop should have its own handle'
isolated_loop.close()
">>),
    ok.

%% ============================================================================
%% Test: Two isolated loops running concurrent call_soon callbacks
%% ============================================================================

test_two_loops_concurrent_callbacks(_Config) ->
    ok = py:exec(<<"
import threading
from erlang_loop import ErlangEventLoop

results = {}

def run_loop_callbacks(loop_id, num_callbacks):
    '''Run callbacks in a separate thread with its own isolated loop.'''
    loop = ErlangEventLoop(isolated=True)

    callback_results = []

    def make_callback(val):
        def cb():
            callback_results.append(val)
        return cb

    try:
        # Schedule callbacks
        for i in range(num_callbacks):
            loop.call_soon(make_callback(i))

        # Run loop to process callbacks
        loop._run_once()

        results[loop_id] = {
            'count': len(callback_results),
            'values': callback_results[:5],
            'success': True
        }
    except Exception as e:
        results[loop_id] = {'error': str(e), 'success': False}
    finally:
        loop.close()

# Run two isolated loops concurrently
t1 = threading.Thread(target=run_loop_callbacks, args=('loop_a', 10))
t2 = threading.Thread(target=run_loop_callbacks, args=('loop_b', 10))

t1.start()
t2.start()
t1.join()
t2.join()

# Both should complete
assert results.get('loop_a', {}).get('success'), f'Loop A failed: {results.get(\"loop_a\")}'
assert results.get('loop_b', {}).get('success'), f'Loop B failed: {results.get(\"loop_b\")}'

# Each should process 10 callbacks
assert results['loop_a']['count'] == 10, f'Loop A wrong count: {results[\"loop_a\"][\"count\"]}'
assert results['loop_b']['count'] == 10, f'Loop B wrong count: {results[\"loop_b\"][\"count\"]}'
">>),
    ok.

%% ============================================================================
%% Test: Callbacks are isolated between loops
%% ============================================================================

test_two_loops_isolation(_Config) ->
    ok = py:exec(<<"
import threading
from erlang_loop import ErlangEventLoop

results = {}

def run_isolated_loop(loop_id, marker_value):
    '''Each loop schedules callbacks with unique markers.'''
    loop = ErlangEventLoop(isolated=True)

    collected = []

    def cb():
        collected.append(marker_value)

    try:
        # Schedule 5 callbacks with this loop's marker
        for _ in range(5):
            loop.call_soon(cb)

        # Process
        loop._run_once()

        # All collected should be our marker
        all_match = all(v == marker_value for v in collected)
        results[loop_id] = {
            'collected': collected,
            'all_match': all_match,
            'count': len(collected),
            'success': True
        }
    except Exception as e:
        results[loop_id] = {'error': str(e), 'success': False}
    finally:
        loop.close()

# Run with different markers
t1 = threading.Thread(target=run_isolated_loop, args=('loop_a', 'A'))
t2 = threading.Thread(target=run_isolated_loop, args=('loop_b', 'B'))

t1.start()
t2.start()
t1.join()
t2.join()

# Both should succeed
assert results.get('loop_a', {}).get('success'), f'Loop A failed: {results.get(\"loop_a\")}'
assert results.get('loop_b', {}).get('success'), f'Loop B failed: {results.get(\"loop_b\")}'

# Each should only see its own marker (isolation test)
assert results['loop_a']['all_match'], f'Loop A saw wrong markers: {results[\"loop_a\"][\"collected\"]}'
assert results['loop_b']['all_match'], f'Loop B saw wrong markers: {results[\"loop_b\"][\"collected\"]}'

# Each should have 5 callbacks
assert results['loop_a']['count'] == 5, f'Loop A wrong count'
assert results['loop_b']['count'] == 5, f'Loop B wrong count'
">>),
    ok.

%% ============================================================================
%% Test: Multiple isolated loops lifecycle
%% ============================================================================

test_multiple_loops_lifecycle(_Config) ->
    ok = py:exec(<<"
from erlang_loop import ErlangEventLoop

# Create multiple isolated loops
loops = []
for i in range(3):
    loop = ErlangEventLoop(isolated=True)
    loops.append(loop)

# Each loop should be independent
collected = {i: [] for i in range(3)}

for i, loop in enumerate(loops):
    def make_cb(idx):
        def cb():
            collected[idx].append(idx)
        return cb
    loop.call_soon(make_cb(i))

# Process each loop
for loop in loops:
    loop._run_once()

# Verify isolation
for i in range(3):
    assert collected[i] == [i], f'Loop {i} wrong: {collected[i]}'

# Close loops in reverse order
for loop in reversed(loops):
    loop.close()

# Verify all closed
for loop in loops:
    assert loop.is_closed(), 'Loop not closed'
">>),
    ok.

%% ============================================================================
%% Test: Isolated loops with timer callbacks (call_later)
%% ============================================================================

test_isolated_loops_with_timers(_Config) ->
    ok = py:exec(<<"
import time
from erlang_loop import ErlangEventLoop

# Test timer in an isolated loop
loop = ErlangEventLoop(isolated=True)
timer_fired = []

def timer_callback():
    timer_fired.append(time.time())

# Schedule a 50ms timer
handle = loop.call_later(0.05, timer_callback)

# Run the loop to process the timer
start = time.time()
deadline = start + 0.5  # 500ms timeout

while time.time() < deadline and not timer_fired:
    loop._run_once()
    time.sleep(0.01)

assert len(timer_fired) > 0, f'Timer did not fire within timeout, elapsed={time.time()-start:.3f}s'

loop.close()

# Now test two isolated loops sequentially
for loop_id in ['loop_a', 'loop_b']:
    loop = ErlangEventLoop(isolated=True)
    timer_result = []

    def make_cb(lid):
        def cb():
            timer_result.append(lid)
        return cb

    loop.call_later(0.03, make_cb(loop_id))

    start = time.time()
    while time.time() < start + 0.3 and not timer_result:
        loop._run_once()
        time.sleep(0.01)

    assert timer_result == [loop_id], f'Loop {loop_id} timer failed: {timer_result}'
    loop.close()
">>),
    ok.
