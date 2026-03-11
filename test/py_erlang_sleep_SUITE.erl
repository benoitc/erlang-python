%% @doc Tests for erlang.sleep() and asyncio integration.
%%
%% Tests the erlang.sleep() function and erlang module asyncio integration.
-module(py_erlang_sleep_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    test_erlang_sleep_available/1,
    test_erlang_sleep_basic/1,
    test_erlang_sleep_zero/1,
    test_erlang_sleep_accuracy/1,
    test_erlang_run_module/1,
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
        test_erlang_run_module,
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

%% Test that erlang.sleep is available
test_erlang_sleep_available(_Config) ->
    ok = py:exec(<<"
import erlang
result = hasattr(erlang, 'sleep')
assert result, 'erlang.sleep not found'
">>),
    ct:pal("erlang.sleep is available"),
    ok.

%% Test basic sleep functionality (sync context via callback)
test_erlang_sleep_basic(_Config) ->
    ok = py:exec(<<"
import erlang
# Test basic sleep in sync context - should not raise
erlang.sleep(0.01)  # 10ms
">>),
    ct:pal("Basic sleep completed"),
    ok.

%% Test zero/negative delay returns immediately
test_erlang_sleep_zero(_Config) ->
    ok = py:exec(<<"
import erlang
import time

start = time.time()
erlang.sleep(0)
elapsed = (time.time() - start) * 1000
# Should return immediately (< 10ms accounting for Python overhead)
assert elapsed < 10, f'Zero sleep was slow: {elapsed}ms'
">>),
    ct:pal("Zero sleep returned fast"),
    ok.

%% Test sleep accuracy
test_erlang_sleep_accuracy(_Config) ->
    ok = py:exec(<<"
import erlang
import time

delays = [0.01, 0.05, 0.1]  # seconds
for delay in delays:
    start = time.time()
    erlang.sleep(delay)
    elapsed = time.time() - start
    # Allow wide tolerance for CI runners (can be slow/unpredictable)
    assert delay * 0.5 <= elapsed <= delay * 10.0, \\
        f'{delay}s sleep took {elapsed:.3f}s'
">>),
    ct:pal("Sleep accuracy within tolerance"),
    ok.

%% Test erlang.run() with asyncio
test_erlang_run_module(_Config) ->
    ok = py:exec(<<"
import erlang
import asyncio

# Test erlang module has expected functions for event loop integration
funcs = ['run', 'new_event_loop', 'EventLoopPolicy', 'sleep']
for f in funcs:
    assert hasattr(erlang, f), f'erlang missing {f}'

# Test run() with asyncio.sleep
async def test_sleep():
    await asyncio.sleep(0.01)  # 10ms
    return 'done'

result = erlang.run(test_sleep())
assert result == 'done', f'Expected done, got {result}'
">>),
    ct:pal("erlang.run() with asyncio works"),
    ok.

%% Test asyncio.gather with erlang.run()
test_erlang_asyncio_gather(_Config) ->
    ok = py:exec(<<"
import erlang
import asyncio

async def task(n):
    await asyncio.sleep(0.01)
    return n * 2

async def main():
    results = await asyncio.gather(task(1), task(2), task(3))
    assert results == [2, 4, 6], f'Expected [2, 4, 6], got {results}'

erlang.run(main())
">>),
    ct:pal("asyncio.gather with erlang.run() works"),
    ok.

%% Test asyncio.wait_for with timeout
test_erlang_asyncio_wait_for(_Config) ->
    ok = py:exec(<<"
import erlang
import asyncio

async def fast_task():
    await asyncio.sleep(0.01)
    return 'fast'

async def main():
    # Should complete before timeout
    result = await asyncio.wait_for(fast_task(), timeout=1.0)
    assert result == 'fast', f'Expected fast, got {result}'

erlang.run(main())
">>),
    ct:pal("asyncio.wait_for with erlang.run() works"),
    ok.

%% Test asyncio.create_task with erlang.run()
test_erlang_asyncio_create_task(_Config) ->
    ok = py:exec(<<"
import erlang
import asyncio

async def background():
    await asyncio.sleep(0.01)
    return 'background_done'

async def main():
    task = asyncio.create_task(background())
    # Do some other work
    await asyncio.sleep(0.005)
    # Wait for task
    result = await task
    assert result == 'background_done', f'Expected background_done, got {result}'

erlang.run(main())
">>),
    ct:pal("asyncio.create_task with erlang.run() works"),
    ok.
