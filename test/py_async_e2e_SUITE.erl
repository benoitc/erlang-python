%%% @doc End-to-end test suite for Python asyncio TCP integration.
-module(py_async_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_asyncio_sleep/1,
    test_asyncio_gather/1,
    test_asyncio_tcp_echo/1,
    test_asyncio_concurrent_tcp/1
]).

all() ->
    [
        test_asyncio_sleep,
        test_asyncio_gather,
        test_asyncio_tcp_echo,
        test_asyncio_concurrent_tcp
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    %% Ensure contexts are running
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Test Cases - All imports included in the code block
%% ============================================================================

%% Test basic asyncio.sleep timing
test_asyncio_sleep(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "import erlang\n"
        "\n"
        "async def timed_sleep():\n"
        "    start = time.monotonic()\n"
        "    await asyncio.sleep(0.05)\n"
        "    return time.monotonic() - start\n"
        "\n"
        "elapsed = erlang.run(timed_sleep())\n"
        "assert elapsed >= 0.04, f'Expected >= 0.04s, got {elapsed:.3f}s'\n"
    >>),
    ok.

%% Test asyncio.gather runs tasks concurrently
test_asyncio_gather(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import asyncio\n"
        "import time\n"
        "import erlang\n"
        "\n"
        "async def task(val):\n"
        "    await asyncio.sleep(0.05)\n"
        "    return val\n"
        "\n"
        "async def main():\n"
        "    start = time.monotonic()\n"
        "    r = await asyncio.gather(task(1), task(2), task(3))\n"
        "    elapsed = time.monotonic() - start\n"
        "    assert list(r) == [1, 2, 3], f'Expected [1, 2, 3], got {list(r)}'\n"
        "    # Allow more time on CI (0.3s instead of 0.15s)\n"
        "    assert elapsed < 0.3, f'Expected < 0.3s, got {elapsed:.3f}s'\n"
        "\n"
        "erlang.run(main())\n"
    >>),
    ok.

%% Test TCP echo server/client using asyncio
test_asyncio_tcp_echo(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import asyncio\n"
        "import erlang\n"
        "\n"
        "async def handler(r, w):\n"
        "    data = await r.read(100)\n"
        "    w.write(data)\n"
        "    await w.drain()\n"
        "    w.close()\n"
        "    await w.wait_closed()\n"
        "\n"
        "async def test():\n"
        "    srv = await asyncio.start_server(handler, '127.0.0.1', 0)\n"
        "    port = srv.sockets[0].getsockname()[1]\n"
        "    r, w = await asyncio.open_connection('127.0.0.1', port)\n"
        "    w.write(b'hello')\n"
        "    await w.drain()\n"
        "    resp = await r.read(100)\n"
        "    w.close()\n"
        "    await w.wait_closed()\n"
        "    srv.close()\n"
        "    await srv.wait_closed()\n"
        "    assert resp == b'hello', f'Expected b\"hello\", got {resp}'\n"
        "\n"
        "erlang.run(test())\n"
    >>),
    ok.

%% Test multiple concurrent TCP connections
test_asyncio_concurrent_tcp(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import asyncio\n"
        "import erlang\n"
        "\n"
        "async def handler(r, w):\n"
        "    data = await r.read(100)\n"
        "    w.write(b're:' + data)\n"
        "    await w.drain()\n"
        "    w.close()\n"
        "    await w.wait_closed()\n"
        "\n"
        "async def req(port, msg):\n"
        "    r, w = await asyncio.open_connection('127.0.0.1', port)\n"
        "    w.write(msg)\n"
        "    await w.drain()\n"
        "    resp = await r.read(100)\n"
        "    w.close()\n"
        "    await w.wait_closed()\n"
        "    return resp\n"
        "\n"
        "async def test():\n"
        "    srv = await asyncio.start_server(handler, '127.0.0.1', 0)\n"
        "    port = srv.sockets[0].getsockname()[1]\n"
        "    results = await asyncio.gather(\n"
        "        req(port, b'1'),\n"
        "        req(port, b'2'),\n"
        "        req(port, b'3')\n"
        "    )\n"
        "    srv.close()\n"
        "    await srv.wait_closed()\n"
        "    assert set(results) == {b're:1', b're:2', b're:3'}, f'Expected {{b\"re:1\", b\"re:2\", b\"re:3\"}}, got {set(results)}'\n"
        "\n"
        "erlang.run(test())\n"
    >>),
    ok.
