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
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "result = hasattr(erlang, 'sleep')\n"
        "assert result, 'erlang.sleep not found'\n"
    >>),
    ct:pal("erlang.sleep is available"),
    ok.

%% Test basic sleep functionality (sync context via callback)
test_erlang_sleep_basic(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "# Test basic sleep in sync context - should not raise\n"
        "erlang.sleep(0.01)  # 10ms\n"
    >>),
    ct:pal("Basic sleep completed"),
    ok.

%% Test zero/negative delay returns immediately
test_erlang_sleep_zero(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "import time\n"
        "\n"
        "start = time.time()\n"
        "erlang.sleep(0)\n"
        "elapsed = (time.time() - start) * 1000\n"
        "# Should return immediately (< 10ms accounting for Python overhead)\n"
        "assert elapsed < 10, f'Zero sleep was slow: {elapsed}ms'\n"
    >>),
    ct:pal("Zero sleep returned fast"),
    ok.

%% Test sleep accuracy
test_erlang_sleep_accuracy(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "import time\n"
        "\n"
        "delays = [0.01, 0.05, 0.1]  # seconds\n"
        "for delay in delays:\n"
        "    start = time.time()\n"
        "    erlang.sleep(delay)\n"
        "    elapsed = time.time() - start\n"
        "    # Allow wide tolerance for CI runners (can be slow/unpredictable)\n"
        "    assert delay * 0.5 <= elapsed <= delay * 10.0, \\\n"
        "        f'{delay}s sleep took {elapsed:.3f}s'\n"
    >>),
    ct:pal("Sleep accuracy within tolerance"),
    ok.

%% Test erlang.run() with asyncio
test_erlang_run_module(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "import asyncio\n"
        "\n"
        "# Test erlang module has expected functions for event loop integration\n"
        "funcs = ['run', 'new_event_loop', 'EventLoopPolicy', 'sleep']\n"
        "for f in funcs:\n"
        "    assert hasattr(erlang, f), f'erlang missing {f}'\n"
        "\n"
        "# Test run() with asyncio.sleep\n"
        "async def test_sleep():\n"
        "    await asyncio.sleep(0.01)  # 10ms\n"
        "    return 'done'\n"
        "\n"
        "result = erlang.run(test_sleep())\n"
        "assert result == 'done', f'Expected done, got {result}'\n"
    >>),
    ct:pal("erlang.run() with asyncio works"),
    ok.

%% Test asyncio.gather with erlang.run()
test_erlang_asyncio_gather(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "import asyncio\n"
        "\n"
        "async def task(n):\n"
        "    await asyncio.sleep(0.01)\n"
        "    return n * 2\n"
        "\n"
        "async def main():\n"
        "    results = await asyncio.gather(task(1), task(2), task(3))\n"
        "    assert results == [2, 4, 6], f'Expected [2, 4, 6], got {results}'\n"
        "\n"
        "erlang.run(main())\n"
    >>),
    ct:pal("asyncio.gather with erlang.run() works"),
    ok.

%% Test asyncio.wait_for with timeout
test_erlang_asyncio_wait_for(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "import asyncio\n"
        "\n"
        "async def fast_task():\n"
        "    await asyncio.sleep(0.01)\n"
        "    return 'fast'\n"
        "\n"
        "async def main():\n"
        "    # Should complete before timeout\n"
        "    result = await asyncio.wait_for(fast_task(), timeout=1.0)\n"
        "    assert result == 'fast', f'Expected fast, got {result}'\n"
        "\n"
        "erlang.run(main())\n"
    >>),
    ct:pal("asyncio.wait_for with erlang.run() works"),
    ok.

%% Test asyncio.create_task with erlang.run()
test_erlang_asyncio_create_task(_Config) ->
    ok = py:exec(<<
        "\n"
        "import erlang\n"
        "import asyncio\n"
        "\n"
        "async def background():\n"
        "    await asyncio.sleep(0.01)\n"
        "    return 'background_done'\n"
        "\n"
        "async def main():\n"
        "    task = asyncio.create_task(background())\n"
        "    # Do some other work\n"
        "    await asyncio.sleep(0.005)\n"
        "    # Wait for task\n"
        "    result = await task\n"
        "    assert result == 'background_done', f'Expected background_done, got {result}'\n"
        "\n"
        "erlang.run(main())\n"
    >>),
    ct:pal("asyncio.create_task with erlang.run() works"),
    ok.
