-module(run_benchmark).
-export([run/0]).

run() ->
    application:ensure_all_started(erlang_python),
    timer:sleep(1000),

    io:format("~n~n========================================~n"),
    io:format("EVENT LOOP BENCHMARK~n"),
    io:format("========================================~n~n"),

    %% Erlang loop callback benchmark
    ErlCode =
        "\n"
        "import asyncio\n"
        "import time\n"
        "from _erlang_impl import ErlangEventLoop\n"
        "\n"
        "async def bench(n=50000):\n"
        "    count = [0]\n"
        "    done = asyncio.Event()\n"
        "    loop = asyncio.get_running_loop()\n"
        "    def cb():\n"
        "        count[0] += 1\n"
        "        if count[0] < n: loop.call_soon(cb)\n"
        "        else: loop.call_soon(done.set)\n"
        "    start = time.perf_counter()\n"
        "    loop.call_soon(cb)\n"
        "    await done.wait()\n"
        "    return count[0] / (time.perf_counter() - start)\n"
        "\n"
        "erl = ErlangEventLoop()\n"
        "asyncio.set_event_loop(erl)\n"
        "rate = erl.run_until_complete(bench())\n"
        "erl.close()\n"
        "rate\n",
    {ok, ErlRate} = py:eval(list_to_binary(ErlCode)),

    %% Standard loop callback benchmark
    StdCode =
        "\n"
        "import asyncio\n"
        "import time\n"
        "\n"
        "async def bench(n=50000):\n"
        "    count = [0]\n"
        "    done = asyncio.Event()\n"
        "    loop = asyncio.get_running_loop()\n"
        "    def cb():\n"
        "        count[0] += 1\n"
        "        if count[0] < n: loop.call_soon(cb)\n"
        "        else: loop.call_soon(done.set)\n"
        "    start = time.perf_counter()\n"
        "    loop.call_soon(cb)\n"
        "    await done.wait()\n"
        "    return count[0] / (time.perf_counter() - start)\n"
        "\n"
        "std = asyncio.new_event_loop()\n"
        "asyncio.set_event_loop(std)\n"
        "rate = std.run_until_complete(bench())\n"
        "std.close()\n"
        "rate\n",
    {ok, StdRate} = py:eval(list_to_binary(StdCode)),

    %% Print callback results
    io:format("CALLBACK DISPATCH~n"),
    io:format("  Erlang:   ~.0f /sec~n", [ErlRate]),
    io:format("  Standard: ~.0f /sec~n", [StdRate]),
    CbDiff = (ErlRate - StdRate) / StdRate * 100,
    io:format("  Diff:     ~.1f%~n~n", [CbDiff]),

    %% TCP echo benchmark - Erlang
    ErlTcpCode =
        "\n"
        "import asyncio\n"
        "import time\n"
        "from _erlang_impl import ErlangEventLoop\n"
        "\n"
        "class Server(asyncio.Protocol):\n"
        "    def connection_made(self, t): self.t = t\n"
        "    def data_received(self, d): self.t.write(d)\n"
        "\n"
        "class Client(asyncio.Protocol):\n"
        "    def __init__(self, msgs, sz, fut):\n"
        "        self.msgs, self.sz, self.fut = msgs, sz, fut\n"
        "        self.sent = self.recv = 0\n"
        "        self.buf = b''\n"
        "    def connection_made(self, t):\n"
        "        self.t = t\n"
        "        self.start = time.perf_counter()\n"
        "        self._send()\n"
        "    def _send(self):\n"
        "        if self.sent < self.msgs:\n"
        "            self.t.write(b'x' * self.sz)\n"
        "            self.sent += 1\n"
        "    def data_received(self, d):\n"
        "        self.buf += d\n"
        "        while len(self.buf) >= self.sz:\n"
        "            self.buf = self.buf[self.sz:]\n"
        "            self.recv += 1\n"
        "            if self.recv < self.msgs: self._send()\n"
        "            elif self.recv == self.msgs:\n"
        "                self.fut.set_result(time.perf_counter() - self.start)\n"
        "                self.t.close()\n"
        "\n"
        "async def bench_tcp(messages=5000, size=64):\n"
        "    loop = asyncio.get_running_loop()\n"
        "    srv = await loop.create_server(Server, '127.0.0.1', 0, reuse_address=True)\n"
        "    port = srv.sockets[0].getsockname()[1]\n"
        "    fut = loop.create_future()\n"
        "    await loop.create_connection(lambda: Client(messages, size, fut), '127.0.0.1', port)\n"
        "    elapsed = await fut\n"
        "    srv.close()\n"
        "    await srv.wait_closed()\n"
        "    return messages / elapsed\n"
        "\n"
        "erl = ErlangEventLoop()\n"
        "asyncio.set_event_loop(erl)\n"
        "rate = erl.run_until_complete(bench_tcp())\n"
        "erl.close()\n"
        "rate\n",
    {ok, ErlTcpRate} = py:eval(list_to_binary(ErlTcpCode)),

    %% TCP echo benchmark - Standard
    StdTcpCode =
        "\n"
        "import asyncio\n"
        "import time\n"
        "\n"
        "class Server(asyncio.Protocol):\n"
        "    def connection_made(self, t): self.t = t\n"
        "    def data_received(self, d): self.t.write(d)\n"
        "\n"
        "class Client(asyncio.Protocol):\n"
        "    def __init__(self, msgs, sz, fut):\n"
        "        self.msgs, self.sz, self.fut = msgs, sz, fut\n"
        "        self.sent = self.recv = 0\n"
        "        self.buf = b''\n"
        "    def connection_made(self, t):\n"
        "        self.t = t\n"
        "        self.start = time.perf_counter()\n"
        "        self._send()\n"
        "    def _send(self):\n"
        "        if self.sent < self.msgs:\n"
        "            self.t.write(b'x' * self.sz)\n"
        "            self.sent += 1\n"
        "    def data_received(self, d):\n"
        "        self.buf += d\n"
        "        while len(self.buf) >= self.sz:\n"
        "            self.buf = self.buf[self.sz:]\n"
        "            self.recv += 1\n"
        "            if self.recv < self.msgs: self._send()\n"
        "            elif self.recv == self.msgs:\n"
        "                self.fut.set_result(time.perf_counter() - self.start)\n"
        "                self.t.close()\n"
        "\n"
        "async def bench_tcp(messages=5000, size=64):\n"
        "    loop = asyncio.get_running_loop()\n"
        "    srv = await loop.create_server(Server, '127.0.0.1', 0, reuse_address=True)\n"
        "    port = srv.sockets[0].getsockname()[1]\n"
        "    fut = loop.create_future()\n"
        "    await loop.create_connection(lambda: Client(messages, size, fut), '127.0.0.1', port)\n"
        "    elapsed = await fut\n"
        "    srv.close()\n"
        "    await srv.wait_closed()\n"
        "    return messages / elapsed\n"
        "\n"
        "std = asyncio.new_event_loop()\n"
        "asyncio.set_event_loop(std)\n"
        "rate = std.run_until_complete(bench_tcp())\n"
        "std.close()\n"
        "rate\n",
    {ok, StdTcpRate} = py:eval(list_to_binary(StdTcpCode)),

    %% Print TCP results
    io:format("TCP ECHO (5000 msgs, 64 bytes)~n"),
    io:format("  Erlang:   ~.0f msgs/sec~n", [ErlTcpRate]),
    io:format("  Standard: ~.0f msgs/sec~n", [StdTcpRate]),
    TcpDiff = (ErlTcpRate - StdTcpRate) / StdTcpRate * 100,
    io:format("  Diff:     ~.1f%~n~n", [TcpDiff]),

    io:format("========================================~n~n"),
    ok.
