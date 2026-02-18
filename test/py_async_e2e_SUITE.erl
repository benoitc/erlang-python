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
%% Test Cases - All imports included in the code block
%% ============================================================================

%% Test basic asyncio.sleep timing
test_asyncio_sleep(_Config) ->
    ok = py:exec(<<"
import asyncio
import time

async def timed_sleep():
    start = time.monotonic()
    await asyncio.sleep(0.05)
    return time.monotonic() - start

elapsed = asyncio.run(timed_sleep())
assert elapsed >= 0.04
">>),
    ok.

%% Test asyncio.gather runs tasks concurrently
test_asyncio_gather(_Config) ->
    ok = py:exec(<<"
import asyncio
import time

async def task(val):
    await asyncio.sleep(0.05)
    return val

async def main():
    start = time.monotonic()
    r = await asyncio.gather(task(1), task(2), task(3))
    elapsed = time.monotonic() - start
    assert list(r) == [1, 2, 3]
    assert elapsed < 0.15

asyncio.run(main())
">>),
    ok.

%% Test TCP echo server/client using asyncio
test_asyncio_tcp_echo(_Config) ->
    ok = py:exec(<<"
import asyncio

async def handler(r, w):
    data = await r.read(100)
    w.write(data)
    await w.drain()
    w.close()
    await w.wait_closed()

async def test():
    srv = await asyncio.start_server(handler, '127.0.0.1', 0)
    port = srv.sockets[0].getsockname()[1]
    r, w = await asyncio.open_connection('127.0.0.1', port)
    w.write(b'hello')
    await w.drain()
    resp = await r.read(100)
    w.close()
    await w.wait_closed()
    srv.close()
    await srv.wait_closed()
    assert resp == b'hello'

asyncio.run(test())
">>),
    ok.

%% Test multiple concurrent TCP connections
test_asyncio_concurrent_tcp(_Config) ->
    ok = py:exec(<<"
import asyncio

async def handler(r, w):
    data = await r.read(100)
    w.write(b're:' + data)
    await w.drain()
    w.close()
    await w.wait_closed()

async def req(port, msg):
    r, w = await asyncio.open_connection('127.0.0.1', port)
    w.write(msg)
    await w.drain()
    resp = await r.read(100)
    w.close()
    await w.wait_closed()
    return resp

async def test():
    srv = await asyncio.start_server(handler, '127.0.0.1', 0)
    port = srv.sockets[0].getsockname()[1]
    results = await asyncio.gather(
        req(port, b'1'),
        req(port, b'2'),
        req(port, b'3')
    )
    srv.close()
    await srv.wait_closed()
    assert set(results) == {b're:1', b're:2', b're:3'}

asyncio.run(test())
">>),
    ok.
