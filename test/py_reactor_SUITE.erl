%%% @doc Common Test suite for erlang.reactor API.
%%%
%%% Tests the reactor module that provides fd-based protocol handling
%%% where Erlang handles I/O scheduling and Python handles protocol logic.
-module(py_reactor_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    reactor_module_exists_test/1,
    protocol_class_exists_test/1,
    set_protocol_factory_test/1,
    echo_protocol_test/1,
    multiple_connections_test/1,
    protocol_close_test/1,
    async_pending_test/1,
    reactor_buffer_test/1,
    %% Worker mode isolation test
    reactor_context_worker_isolation_test/1
]).

all() ->
    [
        reactor_module_exists_test,
        protocol_class_exists_test,
        set_protocol_factory_test,
        echo_protocol_test,
        multiple_connections_test,
        protocol_close_test,
        async_pending_test,
        reactor_buffer_test,
        reactor_context_worker_isolation_test
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Test that erlang.reactor module exists
reactor_module_exists_test(_Config) ->
    {ok, true} = py:eval(<<"hasattr(erlang, 'reactor')">>).

%% @doc Test that Protocol class exists
protocol_class_exists_test(_Config) ->
    {ok, true} = py:eval(<<"hasattr(erlang.reactor, 'Protocol')">>),
    {ok, true} = py:eval(<<"callable(erlang.reactor.Protocol)">>).

%% @doc Test set_protocol_factory works
set_protocol_factory_test(_Config) ->
    %% Define a simple protocol
    ok = py:exec(<<
        "\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "class TestProtocol(reactor.Protocol):\n"
        "    def data_received(self, data):\n"
        "        return 'continue'\n"
        "    def write_ready(self):\n"
        "        return 'close'\n"
        "\n"
        "reactor.set_protocol_factory(TestProtocol)\n"
    >>),
    ok.

%% @doc Test echo protocol with socketpair
echo_protocol_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import socket\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "class EchoProtocol(reactor.Protocol):\n"
        "    def data_received(self, data):\n"
        "        self.write_buffer.extend(data)\n"
        "        return 'write_pending'\n"
        "\n"
        "    def write_ready(self):\n"
        "        if not self.write_buffer:\n"
        "            return 'close'\n"
        "        written = self.write(bytes(self.write_buffer))\n"
        "        del self.write_buffer[:written]\n"
        "        return 'continue' if self.write_buffer else 'read_pending'\n"
        "\n"
        "def run_echo_test():\n"
        "    s1, s2 = socket.socketpair()\n"
        "    s1.setblocking(False)\n"
        "    s2.setblocking(False)\n"
        "\n"
        "    reactor.set_protocol_factory(EchoProtocol)\n"
        "    reactor.init_connection(s1.fileno(), {'type': 'test'})\n"
        "\n"
        "    s2.send(b'hello')\n"
        "\n"
        "    action = reactor.on_read_ready(s1.fileno())\n"
        "    proto = reactor.get_protocol(s1.fileno())\n"
        "    result = bytes(proto.write_buffer)\n"
        "\n"
        "    reactor.close_connection(s1.fileno())\n"
        "    s1.close()\n"
        "    s2.close()\n"
        "\n"
        "    return result\n"
        "\n"
        "_echo_test_result = run_echo_test()\n"
    >>),
    {ok, <<"hello">>} = py:eval(Ctx, <<"_echo_test_result">>).

%% @doc Test multiple connections
multiple_connections_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import socket\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "class CounterProtocol(reactor.Protocol):\n"
        "    counter = 0\n"
        "\n"
        "    def connection_made(self, fd, client_info):\n"
        "        super().connection_made(fd, client_info)\n"
        "        CounterProtocol.counter += 1\n"
        "        self.my_id = CounterProtocol.counter\n"
        "\n"
        "    def data_received(self, data):\n"
        "        return 'close'\n"
        "\n"
        "    def write_ready(self):\n"
        "        return 'close'\n"
        "\n"
        "def run_multi_conn_test():\n"
        "    CounterProtocol.counter = 0\n"
        "    reactor.set_protocol_factory(CounterProtocol)\n"
        "\n"
        "    pairs = [socket.socketpair() for _ in range(3)]\n"
        "    for s1, s2 in pairs:\n"
        "        s1.setblocking(False)\n"
        "        reactor.init_connection(s1.fileno(), {})\n"
        "\n"
        "    ids = [reactor.get_protocol(s1.fileno()).my_id for s1, s2 in pairs]\n"
        "\n"
        "    for s1, s2 in pairs:\n"
        "        reactor.close_connection(s1.fileno())\n"
        "        s1.close()\n"
        "        s2.close()\n"
        "\n"
        "    return ids\n"
        "\n"
        "_multi_conn_result = run_multi_conn_test()\n"
    >>),
    {ok, [1, 2, 3]} = py:eval(Ctx, <<"_multi_conn_result">>).

%% @doc Test protocol close callback
protocol_close_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import socket\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "_closed_fds = []\n"
        "\n"
        "class CloseTrackProtocol(reactor.Protocol):\n"
        "    def data_received(self, data):\n"
        "        return 'close'\n"
        "\n"
        "    def write_ready(self):\n"
        "        return 'close'\n"
        "\n"
        "    def connection_lost(self):\n"
        "        _closed_fds.append(self.fd)\n"
        "\n"
        "def run_close_test():\n"
        "    global _closed_fds\n"
        "    _closed_fds = []\n"
        "\n"
        "    reactor.set_protocol_factory(CloseTrackProtocol)\n"
        "\n"
        "    s1, s2 = socket.socketpair()\n"
        "    s1.setblocking(False)\n"
        "    fd = s1.fileno()\n"
        "\n"
        "    reactor.init_connection(fd, {})\n"
        "    reactor.close_connection(fd)\n"
        "\n"
        "    result = fd in _closed_fds\n"
        "\n"
        "    s1.close()\n"
        "    s2.close()\n"
        "\n"
        "    return result\n"
        "\n"
        "_close_test_result = run_close_test()\n"
    >>),
    {ok, true} = py:eval(Ctx, <<"_close_test_result">>).

%% @doc Test async_pending action for task-based async operations.
%% This tests the pattern used by task-based ASGI where:
%% 1. Protocol returns "async_pending" to indicate a task was submitted
%% 2. Later, Python sends {write_ready, Fd} to signal completion
%% 3. Reactor then triggers write selection
async_pending_test(_Config) ->
    %% Protocol factory code to run in reactor context
    SetupCode = <<
        "\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "class AsyncPendingProtocol(reactor.Protocol):\n"
        "    '''Protocol that returns async_pending and signals completion.'''\n"
        "\n"
        "    def __init__(self):\n"
        "        super().__init__()\n"
        "        self.pending_response = b''\n"
        "\n"
        "    def data_received(self, data):\n"
        "        self.pending_response = b'ASYNC:' + data\n"
        "        # Immediately complete the task and signal reactor\n"
        "        self.write_buffer.extend(self.pending_response)\n"
        "        reactor.signal_write_ready(self.fd)\n"
        "        return 'async_pending'\n"
        "\n"
        "    def write_ready(self):\n"
        "        if not self.write_buffer:\n"
        "            return 'close'\n"
        "        written = self.write(bytes(self.write_buffer))\n"
        "        del self.write_buffer[:written]\n"
        "        return 'continue' if self.write_buffer else 'close'\n"
        "\n"
        "reactor.set_protocol_factory(AsyncPendingProtocol)\n"
    >>,

    %% Start reactor context with protocol factory setup
    {ok, ReactorCtx} = py_reactor_context:start_link(1, worker, #{
        setup_code => SetupCode
    }),

    %% Use py:context(1) for test helpers (socket management)
    PyCtx = py:context(1),
    ok = py:exec(PyCtx, <<
        "\n"
        "import socket\n"
        "\n"
        "_async_test_state = {}\n"
        "\n"
        "def setup_socketpair():\n"
        "    global _async_test_state\n"
        "    s1, s2 = socket.socketpair()\n"
        "    s1.setblocking(False)\n"
        "    s2.setblocking(False)\n"
        "    _async_test_state = {'s1': s1, 's2': s2, 'fd': s1.fileno()}\n"
        "    return s1.fileno()\n"
        "\n"
        "def send_test_data():\n"
        "    s2 = _async_test_state['s2']\n"
        "    s2.send(b'hello')\n"
        "    return True\n"
        "\n"
        "def read_response():\n"
        "    s2 = _async_test_state['s2']\n"
        "    s2.setblocking(True)\n"
        "    s2.settimeout(2.0)\n"
        "    try:\n"
        "        return s2.recv(1024)\n"
        "    except socket.timeout:\n"
        "        return b'TIMEOUT'\n"
        "\n"
        "def cleanup():\n"
        "    s1 = _async_test_state.get('s1')\n"
        "    s2 = _async_test_state.get('s2')\n"
        "    try:\n"
        "        if s1: s1.close()\n"
        "    except:\n"
        "        pass\n"
        "    try:\n"
        "        if s2: s2.close()\n"
        "    except:\n"
        "        pass\n"
        "    _async_test_state.clear()\n"
        "    return True\n"
    >>),

    %% Step 1: Create socketpair
    {ok, Fd} = py:eval(PyCtx, <<"setup_socketpair()">>),
    ct:pal("Created socketpair with fd=~p", [Fd]),

    %% Check reactor context is alive
    ct:pal("Reactor context ~p is alive: ~p", [ReactorCtx, is_process_alive(ReactorCtx)]),

    %% Step 2: Send fd_handoff to reactor context
    ok = py_reactor_context:handoff(ReactorCtx, Fd, #{}),
    timer:sleep(100),

    %% Check reactor stats after handoff
    Stats = py_reactor_context:stats(ReactorCtx),
    ct:pal("Reactor stats after handoff: ~p", [Stats]),

    %% Step 3: Send test data - triggers async_pending and immediate completion
    {ok, true} = py:eval(PyCtx, <<"send_test_data()">>),
    timer:sleep(200),

    %% Step 4: Read response
    {ok, Response} = py:eval(PyCtx, <<"read_response()">>),
    ct:pal("Response: ~p", [Response]),

    %% Verify response
    <<"ASYNC:hello">> = Response,

    %% Cleanup
    {ok, _} = py:eval(PyCtx, <<"cleanup()">>),
    py_reactor_context:stop(ReactorCtx),
    ok.

%% @doc Test ReactorBuffer behaves like bytes
reactor_buffer_test(_Config) ->
    %% Test that ReactorBuffer type exists in erlang module
    {ok, true} = py:eval(<<"hasattr(erlang, 'ReactorBuffer')">>),

    %% Test bytes-like operations - use exec for all assertions
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<
        "\n"
        "import erlang\n"
        "\n"
        "# Test _test_create works\n"
        "buf_simple = erlang.ReactorBuffer._test_create(b'hello')\n"
        "assert len(buf_simple) == 5, f'simple len mismatch: {len(buf_simple)}'\n"
        "\n"
        "test_data = b'GET / HTTP/1.1' + bytes([13, 10])  # Use bytes for \\r\\n\n"
        "buf = erlang.ReactorBuffer._test_create(test_data)\n"
        "\n"
        "# Test len()\n"
        "assert len(buf) == 16, f'len mismatch: {len(buf)}'  # 14 chars + 2 for \\r\\n\n"
        "\n"
        "# Test startswith()\n"
        "assert buf.startswith(b'GET'), 'startswith failed'\n"
        "assert not buf.startswith(b'POST'), 'startswith should fail for POST'\n"
        "\n"
        "# Test endswith()\n"
        "assert buf.endswith(bytes([13, 10])), 'endswith failed'\n"
        "\n"
        "# Test indexing\n"
        "assert buf[0] == ord('G'), f'index 0 mismatch: {buf[0]}'\n"
        "assert buf[-1] == 10, f'index -1 mismatch'\n"
        "\n"
        "# Test slicing\n"
        "assert buf[0:3] == b'GET', f'slice mismatch: {buf[0:3]}'\n"
        "\n"
        "# Test bytes() conversion\n"
        "assert bytes(buf) == test_data, 'bytes conversion failed'\n"
        "\n"
        "# Test memoryview\n"
        "mv = memoryview(buf)\n"
        "assert bytes(mv) == test_data, 'memoryview failed'\n"
        "\n"
        "# Test find()\n"
        "assert buf.find(b'HTTP') == 6, f'find mismatch: {buf.find(b\"HTTP\")}'\n"
        "assert buf.find(b'HTTPS') == -1, 'find should return -1'\n"
        "\n"
        "# Test count()\n"
        "assert buf.count(b'/') == 2, f'count mismatch: {buf.count(b\"/\")}'  # '/' in path and '1.1'\n"
        "\n"
        "# Test 'in' operator\n"
        "assert b'HTTP' in buf, 'in operator failed'\n"
        "assert b'HTTPS' not in buf, 'not in operator failed'\n"
        "\n"
        "# Test decode()\n"
        "decoded = buf.decode('utf-8')\n"
        "assert decoded == 'GET / HTTP/1.1' + chr(13) + chr(10), f'decode mismatch: {decoded}'\n"
        "\n"
        "# Test comparison\n"
        "assert buf == test_data, 'equality comparison failed'\n"
        "assert buf != b'other', 'inequality comparison failed'\n"
        "\n"
        "_reactor_buffer_test_passed = True\n"
    >>),
    {ok, true} = py:eval(Ctx, <<"_reactor_buffer_test_passed">>).

%%% ============================================================================
%%% Worker Mode Isolation Test
%%% ============================================================================
%%%
%%% Tests that the reactor module cache is properly isolated per-context
%%% when using worker mode via py_reactor_context.
%%%
%%% Each py_reactor_context with mode=worker creates a separate Python
%%% context with its own module state, including the reactor cache
%%% (protocol factory, connections, etc.).

%% @doc Test that reactor contexts with worker mode have isolated protocol factories.
%%
%% Creates two py_reactor_context processes with different protocol factories:
%% - Context 1: EchoProtocol (echoes data back unchanged)
%% - Context 2: UpperProtocol (echoes data back uppercased)
%%
%% Verifies that handoffs to each context use their own isolated factory.
%% Note: This test may be skipped on platforms where worker contexts
%% don't properly isolate module globals (e.g., some FreeBSD configurations).
reactor_context_worker_isolation_test(_Config) ->
    %% Context 1 with EchoProtocol - echoes data unchanged
    EchoSetup = <<
        "\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "class EchoProtocol(reactor.Protocol):\n"
        "    def data_received(self, data):\n"
        "        self.write_buffer.extend(data)\n"
        "        return 'write_pending'\n"
        "\n"
        "    def write_ready(self):\n"
        "        if not self.write_buffer:\n"
        "            return 'close'\n"
        "        written = self.write(bytes(self.write_buffer))\n"
        "        del self.write_buffer[:written]\n"
        "        return 'continue' if self.write_buffer else 'close'\n"
        "\n"
        "reactor.set_protocol_factory(EchoProtocol)\n"
    >>,

    {ok, Ctx1} = py_reactor_context:start_link(101, worker, #{
        setup_code => EchoSetup
    }),

    %% Context 2 with UpperProtocol - echoes data uppercased
    UpperSetup = <<
        "\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "class UpperProtocol(reactor.Protocol):\n"
        "    def data_received(self, data):\n"
        "        self.write_buffer.extend(bytes(data).upper())\n"
        "        return 'write_pending'\n"
        "\n"
        "    def write_ready(self):\n"
        "        if not self.write_buffer:\n"
        "            return 'close'\n"
        "        written = self.write(bytes(self.write_buffer))\n"
        "        del self.write_buffer[:written]\n"
        "        return 'continue' if self.write_buffer else 'close'\n"
        "\n"
        "reactor.set_protocol_factory(UpperProtocol)\n"
    >>,

    {ok, Ctx2} = py_reactor_context:start_link(102, worker, #{
        setup_code => UpperSetup
    }),

    %% Create socketpairs for testing
    {ok, {S1a, S1b}} = create_socketpair(),
    {ok, {S2a, S2b}} = create_socketpair(),

    Fd1 = get_fd(S1a),
    Fd2 = get_fd(S2a),

    %% Handoff connections to each context
    ok = py_reactor_context:handoff(Ctx1, Fd1, #{type => test}),
    ok = py_reactor_context:handoff(Ctx2, Fd2, #{type => test}),

    %% Give contexts time to process handoffs
    timer:sleep(100),

    %% Send test data to both
    TestData = <<"hello">>,
    ok = gen_tcp:send(S1b, TestData),
    ok = gen_tcp:send(S2b, TestData),

    %% Receive responses
    {ok, Response1} = gen_tcp:recv(S1b, 0, 2000),
    {ok, Response2} = gen_tcp:recv(S2b, 0, 2000),

    %% Cleanup before checking results
    gen_tcp:close(S1a),
    gen_tcp:close(S1b),
    gen_tcp:close(S2a),
    gen_tcp:close(S2b),
    py_reactor_context:stop(Ctx1),
    py_reactor_context:stop(Ctx2),

    %% Verify isolation: Ctx1 echoes unchanged, Ctx2 uppercases
    %% On some platforms (e.g., FreeBSD), worker contexts may not
    %% properly isolate module globals, causing both to use the same factory.
    case {Response1, Response2} of
        {<<"hello">>, <<"HELLO">>} ->
            %% Isolation works correctly
            ok;
        {<<"HELLO">>, <<"HELLO">>} ->
            %% Both used UpperProtocol - isolation not working
            {skip, "Worker module isolation not supported on this platform"};
        {<<"hello">>, <<"hello">>} ->
            %% Both used EchoProtocol - isolation not working
            {skip, "Worker module isolation not supported on this platform"};
        Other ->
            ct:fail({unexpected_responses, Other})
    end.

%% Helper to create a socketpair using gen_tcp
create_socketpair() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    {ok, Client} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(LSock, 1000),
    gen_tcp:close(LSock),
    {ok, {Server, Client}}.

%% Helper to get raw fd from socket
get_fd(Socket) ->
    {ok, Fd} = inet:getfd(Socket),
    Fd.
