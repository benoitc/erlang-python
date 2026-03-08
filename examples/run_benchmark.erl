-module(run_benchmark).
-export([run/0]).

run() ->
    application:ensure_all_started(erlang_python),
    timer:sleep(1000),

    io:format("~n~n========================================~n"),
    io:format("EVENT LOOP BENCHMARK~n"),
    io:format("========================================~n~n"),

    %% Erlang loop callback benchmark
    ErlCode = "
import asyncio
import time
from _erlang_impl import ErlangEventLoop

async def bench(n=50000):
    count = [0]
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    def cb():
        count[0] += 1
        if count[0] < n: loop.call_soon(cb)
        else: loop.call_soon(done.set)
    start = time.perf_counter()
    loop.call_soon(cb)
    await done.wait()
    return count[0] / (time.perf_counter() - start)

erl = ErlangEventLoop()
asyncio.set_event_loop(erl)
rate = erl.run_until_complete(bench())
erl.close()
rate
",
    {ok, ErlRate} = py:eval(list_to_binary(ErlCode)),

    %% Standard loop callback benchmark
    StdCode = "
import asyncio
import time

async def bench(n=50000):
    count = [0]
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    def cb():
        count[0] += 1
        if count[0] < n: loop.call_soon(cb)
        else: loop.call_soon(done.set)
    start = time.perf_counter()
    loop.call_soon(cb)
    await done.wait()
    return count[0] / (time.perf_counter() - start)

std = asyncio.new_event_loop()
asyncio.set_event_loop(std)
rate = std.run_until_complete(bench())
std.close()
rate
",
    {ok, StdRate} = py:eval(list_to_binary(StdCode)),

    %% Print callback results
    io:format("CALLBACK DISPATCH~n"),
    io:format("  Erlang:   ~.0f /sec~n", [ErlRate]),
    io:format("  Standard: ~.0f /sec~n", [StdRate]),
    CbDiff = (ErlRate - StdRate) / StdRate * 100,
    io:format("  Diff:     ~.1f%~n~n", [CbDiff]),

    %% TCP echo benchmark - Erlang
    ErlTcpCode = "
import asyncio
import time
from _erlang_impl import ErlangEventLoop

class Server(asyncio.Protocol):
    def connection_made(self, t): self.t = t
    def data_received(self, d): self.t.write(d)

class Client(asyncio.Protocol):
    def __init__(self, msgs, sz, fut):
        self.msgs, self.sz, self.fut = msgs, sz, fut
        self.sent = self.recv = 0
        self.buf = b''
    def connection_made(self, t):
        self.t = t
        self.start = time.perf_counter()
        self._send()
    def _send(self):
        if self.sent < self.msgs:
            self.t.write(b'x' * self.sz)
            self.sent += 1
    def data_received(self, d):
        self.buf += d
        while len(self.buf) >= self.sz:
            self.buf = self.buf[self.sz:]
            self.recv += 1
            if self.recv < self.msgs: self._send()
            elif self.recv == self.msgs:
                self.fut.set_result(time.perf_counter() - self.start)
                self.t.close()

async def bench_tcp(messages=5000, size=64):
    loop = asyncio.get_running_loop()
    srv = await loop.create_server(Server, '127.0.0.1', 0, reuse_address=True)
    port = srv.sockets[0].getsockname()[1]
    fut = loop.create_future()
    await loop.create_connection(lambda: Client(messages, size, fut), '127.0.0.1', port)
    elapsed = await fut
    srv.close()
    await srv.wait_closed()
    return messages / elapsed

erl = ErlangEventLoop()
asyncio.set_event_loop(erl)
rate = erl.run_until_complete(bench_tcp())
erl.close()
rate
",
    {ok, ErlTcpRate} = py:eval(list_to_binary(ErlTcpCode)),

    %% TCP echo benchmark - Standard
    StdTcpCode = "
import asyncio
import time

class Server(asyncio.Protocol):
    def connection_made(self, t): self.t = t
    def data_received(self, d): self.t.write(d)

class Client(asyncio.Protocol):
    def __init__(self, msgs, sz, fut):
        self.msgs, self.sz, self.fut = msgs, sz, fut
        self.sent = self.recv = 0
        self.buf = b''
    def connection_made(self, t):
        self.t = t
        self.start = time.perf_counter()
        self._send()
    def _send(self):
        if self.sent < self.msgs:
            self.t.write(b'x' * self.sz)
            self.sent += 1
    def data_received(self, d):
        self.buf += d
        while len(self.buf) >= self.sz:
            self.buf = self.buf[self.sz:]
            self.recv += 1
            if self.recv < self.msgs: self._send()
            elif self.recv == self.msgs:
                self.fut.set_result(time.perf_counter() - self.start)
                self.t.close()

async def bench_tcp(messages=5000, size=64):
    loop = asyncio.get_running_loop()
    srv = await loop.create_server(Server, '127.0.0.1', 0, reuse_address=True)
    port = srv.sockets[0].getsockname()[1]
    fut = loop.create_future()
    await loop.create_connection(lambda: Client(messages, size, fut), '127.0.0.1', port)
    elapsed = await fut
    srv.close()
    await srv.wait_closed()
    return messages / elapsed

std = asyncio.new_event_loop()
asyncio.set_event_loop(std)
rate = std.run_until_complete(bench_tcp())
std.close()
rate
",
    {ok, StdTcpRate} = py:eval(list_to_binary(StdTcpCode)),

    %% Print TCP results
    io:format("TCP ECHO (5000 msgs, 64 bytes)~n"),
    io:format("  Erlang:   ~.0f msgs/sec~n", [ErlTcpRate]),
    io:format("  Standard: ~.0f msgs/sec~n", [StdTcpRate]),
    TcpDiff = (ErlTcpRate - StdTcpRate) / StdTcpRate * 100,
    io:format("  Diff:     ~.1f%~n~n", [TcpDiff]),

    io:format("========================================~n~n"),
    ok.
