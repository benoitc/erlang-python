%% @doc Tests for Erlang sleep fast path (erlang_asyncio module).
%%
%% Tests the _erlang_sleep NIF and erlang_asyncio Python module.
-module(py_erlang_sleep_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    test_erlang_sleep_available/1,
    test_erlang_sleep_basic/1,
    test_erlang_sleep_zero/1,
    test_erlang_sleep_accuracy/1,
    test_erlang_asyncio_module/1,
    test_erlang_asyncio_gather/1,
    test_erlang_asyncio_wait_for/1,
    test_erlang_asyncio_create_task/1
]).

all() ->
    [
        test_erlang_sleep_available,
        test_erlang_sleep_basic,
        test_erlang_sleep_zero,
        test_erlang_sleep_accuracy,
        test_erlang_asyncio_module,
        test_erlang_asyncio_gather,
        test_erlang_asyncio_wait_for,
        test_erlang_asyncio_create_task
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    timer:sleep(500),
    Config.

end_per_suite(_Config) ->
    ok.

%% Test that _erlang_sleep is available in py_event_loop
test_erlang_sleep_available(_Config) ->
    ok = py:exec(<<"
import py_event_loop as pel
result = hasattr(pel, '_erlang_sleep')
assert result, '_erlang_sleep not found in py_event_loop'
">>),
    ct:pal("_erlang_sleep is available"),
    ok.

%% Test basic sleep functionality
test_erlang_sleep_basic(_Config) ->
    ok = py:exec(<<"
import py_event_loop as pel
# Test basic sleep - should not raise
pel._erlang_sleep(10)  # 10ms
">>),
    ct:pal("Basic sleep completed"),
    ok.

%% Test zero/negative delay returns immediately
test_erlang_sleep_zero(_Config) ->
    ok = py:exec(<<"
import py_event_loop as pel
import time

start = time.time()
pel._erlang_sleep(0)
elapsed = (time.time() - start) * 1000
# Should return immediately (< 5ms accounting for Python overhead)
assert elapsed < 5, f'Zero sleep was slow: {elapsed}ms'
">>),
    ct:pal("Zero sleep returned fast"),
    ok.

%% Test sleep accuracy
test_erlang_sleep_accuracy(_Config) ->
    ok = py:exec(<<"
import py_event_loop as pel
import time

delays = [10, 50, 100]  # ms
for delay in delays:
    start = time.time()
    pel._erlang_sleep(delay)
    elapsed = (time.time() - start) * 1000
    # Allow wide tolerance for CI runners (can be slow/unpredictable)
    assert delay * 0.5 <= elapsed <= delay * 10.0, \\
        f'{delay}ms sleep took {elapsed:.1f}ms'
">>),
    ct:pal("Sleep accuracy within tolerance"),
    ok.

%% Test erlang_asyncio module
test_erlang_asyncio_module(_Config) ->
    ok = py:exec(<<"
import erlang_asyncio

# Test module has expected functions
funcs = ['sleep', 'get_event_loop', 'new_event_loop', 'run', 'gather', 'wait_for', 'create_task']
for f in funcs:
    assert hasattr(erlang_asyncio, f), f'erlang_asyncio missing {f}'

# Test run() with sleep
async def test_sleep():
    await erlang_asyncio.sleep(0.01)  # 10ms
    return 'done'

result = erlang_asyncio.run(test_sleep())
assert result == 'done', f'Expected done, got {result}'
">>),
    ct:pal("erlang_asyncio module works"),
    ok.

%% Test erlang_asyncio.gather
test_erlang_asyncio_gather(_Config) ->
    ok = py:exec(<<"
import erlang_asyncio

async def task(n):
    await erlang_asyncio.sleep(0.01)
    return n * 2

async def main():
    results = await erlang_asyncio.gather(task(1), task(2), task(3))
    assert results == [2, 4, 6], f'Expected [2, 4, 6], got {results}'

erlang_asyncio.run(main())
">>),
    ct:pal("erlang_asyncio.gather works"),
    ok.

%% Test erlang_asyncio.wait_for with timeout
test_erlang_asyncio_wait_for(_Config) ->
    ok = py:exec(<<"
import erlang_asyncio

async def fast_task():
    await erlang_asyncio.sleep(0.01)
    return 'fast'

async def main():
    # Should complete before timeout
    result = await erlang_asyncio.wait_for(fast_task(), timeout=1.0)
    assert result == 'fast', f'Expected fast, got {result}'

erlang_asyncio.run(main())
">>),
    ct:pal("erlang_asyncio.wait_for works"),
    ok.

%% Test erlang_asyncio.create_task
test_erlang_asyncio_create_task(_Config) ->
    ok = py:exec(<<"
import erlang_asyncio

async def background():
    await erlang_asyncio.sleep(0.01)
    return 'background_done'

async def main():
    task = erlang_asyncio.create_task(background())
    # Do some other work
    await erlang_asyncio.sleep(0.005)
    # Wait for task
    result = await task
    assert result == 'background_done', f'Expected background_done, got {result}'

erlang_asyncio.run(main())
">>),
    ct:pal("erlang_asyncio.create_task works"),
    ok.
