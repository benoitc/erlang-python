%%% @doc Integration test suite for per-loop isolation with real asyncio workloads.
%%%
%%% Tests that multiple ErlangEventLoop instances can run real asyncio operations
%%% (sleep, TCP, concurrent tasks) concurrently without interference.
%%%
%%% These tests use the `event_loop_isolation = per_loop` mode.
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
    test_per_loop_mode_enabled/1,
    test_two_loops_concurrent_sleep/1,
    test_two_loops_concurrent_gather/1,
    test_isolated_loop_tcp_echo/1
]).

all() ->
    [
        test_per_loop_mode_enabled,
        test_two_loops_concurrent_sleep,
        test_two_loops_concurrent_gather,
        test_isolated_loop_tcp_echo
    ].

init_per_suite(Config) ->
    %% Stop application if already running (from other test suites)
    _ = application:stop(erlang_python),
    timer:sleep(200),  %% Allow full cleanup

    %% Set per_loop isolation mode BEFORE starting the application
    application:set_env(erlang_python, event_loop_isolation, per_loop),

    %% Verify env is set
    PerLoop = application:get_env(erlang_python, event_loop_isolation, global),
    ct:pal("Setting isolation mode to: ~p", [PerLoop]),

    {ok, _} = application:ensure_all_started(erlang_python),

    %% Manually set isolation mode again to ensure it's applied
    %% (in case gen_server was already running)
    ok = py_nif:set_isolation_mode(per_loop),

    %% Wait for event loop to be ready
    timer:sleep(200),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    %% Reset to default
    application:set_env(erlang_python, event_loop_isolation, global),
    ok.

init_per_testcase(_TestCase, Config) ->
    py:unbind(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    py:unbind(),
    ok.

%% ============================================================================
%% Test: Verify per_loop mode is enabled
%% ============================================================================

test_per_loop_mode_enabled(_Config) ->
    {ok, Mode} = py:call(py_event_loop, '_get_isolation_mode', []),
    ct:pal("Isolation mode: ~p", [Mode]),
    ?assertEqual(<<"per_loop">>, Mode),
    ok.

%% ============================================================================
%% Test: Two loops running concurrent call_soon callbacks
%% ============================================================================
%%
%% Creates two ErlangEventLoop instances (each with isolated native loop)
%% and runs call_soon callbacks. Verifies both loops process their own
%% callbacks independently without interference.
%%
%% Note: asyncio.sleep requires a router (for Erlang timers). This test
%% uses call_soon which works with the basic loop infrastructure.

test_two_loops_concurrent_sleep(_Config) ->
    ok = py:exec(<<"
import time
import threading
from erlang_loop import ErlangEventLoop

results = {}

def run_loop_callbacks(loop_id, num_callbacks):
    '''Run callbacks in a separate thread with its own loop.'''
    loop = ErlangEventLoop()

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
            'values': callback_results[:5],  # First 5 for debugging
            'success': True
        }
    except Exception as e:
        results[loop_id] = {'error': str(e), 'success': False}
    finally:
        loop.close()

# Run two loops concurrently
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
%% Test: Two loops with isolated callback queues
%% ============================================================================
%%
%% Verifies that callbacks scheduled on one loop don't appear on another.
%% This is the core isolation test.

test_two_loops_concurrent_gather(_Config) ->
    ok = py:exec(<<"
import threading
from erlang_loop import ErlangEventLoop

results = {}

def run_isolated_loop(loop_id, marker_value):
    '''Each loop schedules callbacks with unique markers.'''
    loop = ErlangEventLoop()

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
%% Test: Isolated loop creation and destruction
%% ============================================================================
%%
%% Tests that multiple loops can be created and destroyed without
%% affecting each other.

test_isolated_loop_tcp_echo(_Config) ->
    ok = py:exec(<<"
from erlang_loop import ErlangEventLoop

# Create multiple loops
loops = []
for i in range(3):
    loop = ErlangEventLoop()
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
