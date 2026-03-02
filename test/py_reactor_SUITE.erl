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
    protocol_close_test/1
]).

all() -> [
    reactor_module_exists_test,
    protocol_class_exists_test,
    set_protocol_factory_test,
    echo_protocol_test,
    multiple_connections_test,
    protocol_close_test
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Bind context to ensure all py:exec/eval calls use same context
    Ctx = py:context(1),
    ok = py_context_router:bind_context(Ctx),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = py_context_router:unbind_context(),
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
    %% Define and run the test
    ok = py:exec(<<"
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
    {ok, <<"hello">>} = py:eval(<<"_echo_test_result">>).

%% @doc Test multiple connections
multiple_connections_test(_Config) ->
    ok = py:exec(<<"
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
    {ok, [1, 2, 3]} = py:eval(<<"_multi_conn_result">>).

%% @doc Test protocol close callback
protocol_close_test(_Config) ->
    ok = py:exec(<<"
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
    {ok, true} = py:eval(<<"_close_test_result">>).
