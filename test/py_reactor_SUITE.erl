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
    async_pending_test/1
]).

all() -> [
    reactor_module_exists_test,
    protocol_class_exists_test,
    set_protocol_factory_test,
    echo_protocol_test,
    multiple_connections_test,
    protocol_close_test,
    async_pending_test
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
import erlang
import erlang.reactor as reactor

class AsyncPendingProtocol(reactor.Protocol):
    '''Protocol that returns async_pending and signals completion.'''

    def __init__(self):
        super().__init__()
        self.reactor_pid = None
        self.pending_response = b''

    def connection_made(self, fd, client_info):
        super().connection_made(fd, client_info)
        self.reactor_pid = client_info.get('reactor_pid')

    def data_received(self, data):
        import sys
        self.pending_response = b'ASYNC:' + data
        # Immediately complete the task and signal reactor
        self.write_buffer.extend(self.pending_response)
        if self.reactor_pid:
            print(f'Sending write_ready to {self.reactor_pid} for fd={self.fd}', file=sys.stderr)
            try:
                erlang.send(self.reactor_pid, ('write_ready', self.fd))
                print('write_ready sent successfully', file=sys.stderr)
            except Exception as e:
                print(f'erlang.send failed: {e}', file=sys.stderr)
        else:
            print('No reactor_pid available!', file=sys.stderr)
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
