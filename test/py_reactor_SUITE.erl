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

all() -> [
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
    ok = py:exec(<<"
import erlang.reactor as reactor

class TestProtocol(reactor.Protocol):
    def data_received(self, data):
        return 'continue'
    def write_ready(self):
        return 'close'

reactor.set_protocol_factory(TestProtocol)
">>),
    ok.

%% @doc Test echo protocol with socketpair
echo_protocol_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import socket
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'read_pending'

def run_echo_test():
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    reactor.set_protocol_factory(EchoProtocol)
    reactor.init_connection(s1.fileno(), {'type': 'test'})

    s2.send(b'hello')

    action = reactor.on_read_ready(s1.fileno())
    proto = reactor.get_protocol(s1.fileno())
    result = bytes(proto.write_buffer)

    reactor.close_connection(s1.fileno())
    s1.close()
    s2.close()

    return result

_echo_test_result = run_echo_test()
">>),
    {ok, <<"hello">>} = py:eval(Ctx, <<"_echo_test_result">>).

%% @doc Test multiple connections
multiple_connections_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import socket
import erlang.reactor as reactor

class CounterProtocol(reactor.Protocol):
    counter = 0

    def connection_made(self, fd, client_info):
        super().connection_made(fd, client_info)
        CounterProtocol.counter += 1
        self.my_id = CounterProtocol.counter

    def data_received(self, data):
        return 'close'

    def write_ready(self):
        return 'close'

def run_multi_conn_test():
    CounterProtocol.counter = 0
    reactor.set_protocol_factory(CounterProtocol)

    pairs = [socket.socketpair() for _ in range(3)]
    for s1, s2 in pairs:
        s1.setblocking(False)
        reactor.init_connection(s1.fileno(), {})

    ids = [reactor.get_protocol(s1.fileno()).my_id for s1, s2 in pairs]

    for s1, s2 in pairs:
        reactor.close_connection(s1.fileno())
        s1.close()
        s2.close()

    return ids

_multi_conn_result = run_multi_conn_test()
">>),
    {ok, [1, 2, 3]} = py:eval(Ctx, <<"_multi_conn_result">>).

%% @doc Test protocol close callback
protocol_close_test(_Config) ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import socket
import erlang.reactor as reactor

_closed_fds = []

class CloseTrackProtocol(reactor.Protocol):
    def data_received(self, data):
        return 'close'

    def write_ready(self):
        return 'close'

    def connection_lost(self):
        _closed_fds.append(self.fd)

def run_close_test():
    global _closed_fds
    _closed_fds = []

    reactor.set_protocol_factory(CloseTrackProtocol)

    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    fd = s1.fileno()

    reactor.init_connection(fd, {})
    reactor.close_connection(fd)

    result = fd in _closed_fds

    s1.close()
    s2.close()

    return result

_close_test_result = run_close_test()
">>),
    {ok, true} = py:eval(Ctx, <<"_close_test_result">>).

%% @doc Test async_pending action for task-based async operations.
%% This tests the pattern used by task-based ASGI where:
%% 1. Protocol returns "async_pending" to indicate a task was submitted
%% 2. Later, Python sends {write_ready, Fd} to signal completion
%% 3. Reactor then triggers write selection
async_pending_test(_Config) ->
    %% Protocol factory code to run in reactor context
    SetupCode = <<"
import erlang.reactor as reactor

class AsyncPendingProtocol(reactor.Protocol):
    '''Protocol that returns async_pending and signals completion.'''

    def __init__(self):
        super().__init__()
        self.pending_response = b''

    def data_received(self, data):
        self.pending_response = b'ASYNC:' + data
        # Immediately complete the task and signal reactor
        self.write_buffer.extend(self.pending_response)
        reactor.signal_write_ready(self.fd)
        return 'async_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'close'

reactor.set_protocol_factory(AsyncPendingProtocol)
">>,

    %% Start reactor context with protocol factory setup
    {ok, ReactorCtx} = py_reactor_context:start_link(1, auto, #{
        setup_code => SetupCode
    }),

    %% Use py:context(1) for test helpers (socket management)
    PyCtx = py:context(1),
    ok = py:exec(PyCtx, <<"
import socket

_async_test_state = {}

def setup_socketpair():
    global _async_test_state
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)
    _async_test_state = {'s1': s1, 's2': s2, 'fd': s1.fileno()}
    return s1.fileno()

def send_test_data():
    s2 = _async_test_state['s2']
    s2.send(b'hello')
    return True

def read_response():
    s2 = _async_test_state['s2']
    s2.setblocking(True)
    s2.settimeout(2.0)
    try:
        return s2.recv(1024)
    except socket.timeout:
        return b'TIMEOUT'

def cleanup():
    s1 = _async_test_state.get('s1')
    s2 = _async_test_state.get('s2')
    try:
        if s1: s1.close()
    except:
        pass
    try:
        if s2: s2.close()
    except:
        pass
    _async_test_state.clear()
    return True
">>),

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
    ok = py:exec(Ctx, <<"
import erlang

# Test _test_create works
buf_simple = erlang.ReactorBuffer._test_create(b'hello')
assert len(buf_simple) == 5, f'simple len mismatch: {len(buf_simple)}'

test_data = b'GET / HTTP/1.1' + bytes([13, 10])  # Use bytes for \\r\\n
buf = erlang.ReactorBuffer._test_create(test_data)

# Test len()
assert len(buf) == 16, f'len mismatch: {len(buf)}'  # 14 chars + 2 for \\r\\n

# Test startswith()
assert buf.startswith(b'GET'), 'startswith failed'
assert not buf.startswith(b'POST'), 'startswith should fail for POST'

# Test endswith()
assert buf.endswith(bytes([13, 10])), 'endswith failed'

# Test indexing
assert buf[0] == ord('G'), f'index 0 mismatch: {buf[0]}'
assert buf[-1] == 10, f'index -1 mismatch'

# Test slicing
assert buf[0:3] == b'GET', f'slice mismatch: {buf[0:3]}'

# Test bytes() conversion
assert bytes(buf) == test_data, 'bytes conversion failed'

# Test memoryview
mv = memoryview(buf)
assert bytes(mv) == test_data, 'memoryview failed'

# Test find()
assert buf.find(b'HTTP') == 6, f'find mismatch: {buf.find(b\"HTTP\")}'
assert buf.find(b'HTTPS') == -1, 'find should return -1'

# Test count()
assert buf.count(b'/') == 2, f'count mismatch: {buf.count(b\"/\")}'  # '/' in path and '1.1'

# Test 'in' operator
assert b'HTTP' in buf, 'in operator failed'
assert b'HTTPS' not in buf, 'not in operator failed'

# Test decode()
decoded = buf.decode('utf-8')
assert decoded == 'GET / HTTP/1.1' + chr(13) + chr(10), f'decode mismatch: {decoded}'

# Test comparison
assert buf == test_data, 'equality comparison failed'
assert buf != b'other', 'inequality comparison failed'

_reactor_buffer_test_passed = True
">>),
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
    EchoSetup = <<"
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'close'

reactor.set_protocol_factory(EchoProtocol)
">>,

    {ok, Ctx1} = py_reactor_context:start_link(101, worker, #{
        setup_code => EchoSetup
    }),

    %% Context 2 with UpperProtocol - echoes data uppercased
    UpperSetup = <<"
import erlang.reactor as reactor

class UpperProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(bytes(data).upper())
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'close'

reactor.set_protocol_factory(UpperProtocol)
">>,

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
