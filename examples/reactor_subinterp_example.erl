%% @doc Example: SHARED_GIL reactor with subinterpreters.
%%
%% Each py_reactor_context runs in an isolated subinterpreter with its own
%% protocol factory. Multiple contexts can process connections in parallel
%% while sharing Python's GIL.
%%
%% Best for: High-concurrency I/O-bound workloads (HTTP servers, WebSockets).

-module(reactor_subinterp_example).
-export([start/0, start/1, stop/1]).

-define(ECHO_PROTOCOL, <<"
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    '''Echo back all received data.'''

    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if self.write_buffer:
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            if self.write_buffer:
                return 'continue'
        return 'read_pending'

reactor.set_protocol_factory(EchoProtocol)
">>).

-define(HTTP_PROTOCOL, <<"
import erlang.reactor as reactor

class SimpleHTTPProtocol(reactor.Protocol):
    '''Simple HTTP/1.1 response protocol.'''

    def __init__(self):
        super().__init__()
        self.request_data = bytearray()

    def data_received(self, data):
        self.request_data.extend(data)
        # Check for end of HTTP headers
        if b'\\r\\n\\r\\n' in self.request_data:
            # Build simple response
            body = b'Hello from subinterpreter!'
            response = (
                b'HTTP/1.1 200 OK\\r\\n'
                b'Content-Type: text/plain\\r\\n'
                b'Content-Length: ' + str(len(body)).encode() + b'\\r\\n'
                b'Connection: close\\r\\n'
                b'\\r\\n' + body
            )
            self.write_buffer.extend(response)
            return 'write_pending'
        return 'continue'

    def write_ready(self):
        if self.write_buffer:
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            if self.write_buffer:
                return 'continue'
        return 'close'

reactor.set_protocol_factory(SimpleHTTPProtocol)
">>).

%% @doc Start with default settings (4 contexts, 2 echo + 2 http).
start() ->
    start(#{contexts => 4, port => 8080}).

%% @doc Start reactor contexts.
%%
%% Options:
%%   contexts - Number of contexts to create (default: 4)
%%   port - Port to listen on (default: 8080)
%%
%% Returns: {ok, #{echo => [Pid], http => [Pid], acceptor => Pid, socket => Socket}}
start(Opts) ->
    NumContexts = maps:get(contexts, Opts, 4),
    Port = maps:get(port, Opts, 8080),
    HalfContexts = NumContexts div 2,

    %% Start echo protocol contexts
    EchoContexts = [begin
        {ok, Pid} = py_reactor_context:start_link(N, subinterp, #{
            max_connections => 100,
            setup_code => ?ECHO_PROTOCOL
        }),
        Pid
    end || N <- lists:seq(1, HalfContexts)],

    %% Start HTTP protocol contexts
    HttpContexts = [begin
        {ok, Pid} = py_reactor_context:start_link(N, subinterp, #{
            max_connections => 100,
            setup_code => ?HTTP_PROTOCOL
        }),
        Pid
    end || N <- lists:seq(HalfContexts + 1, NumContexts)],

    AllContexts = EchoContexts ++ HttpContexts,

    %% Start acceptor that routes to contexts
    {ok, ListenSock} = gen_tcp:listen(Port, [
        binary,
        {active, false},
        {reuseaddr, true},
        {backlog, 128}
    ]),
    Acceptor = spawn_link(fun() -> accept_loop(ListenSock, AllContexts, 1) end),

    io:format("Reactor started on port ~p with ~p contexts~n", [Port, NumContexts]),
    io:format("  Echo contexts: ~p~n", [EchoContexts]),
    io:format("  HTTP contexts: ~p~n", [HttpContexts]),

    {ok, #{
        echo => EchoContexts,
        http => HttpContexts,
        acceptor => Acceptor,
        socket => ListenSock
    }}.

%% @doc Stop the reactor server.
stop(#{acceptor := Acceptor, socket := Socket, echo := Echo, http := Http}) ->
    exit(Acceptor, shutdown),
    gen_tcp:close(Socket),
    [py_reactor_context:stop(Pid) || Pid <- Echo ++ Http],
    ok.

%% @private Simple round-robin acceptor
accept_loop(ListenSock, Contexts, Idx) ->
    case gen_tcp:accept(ListenSock) of
        {ok, Socket} ->
            %% Get FD and hand off to reactor context
            {ok, Fd} = prim_inet:getfd(Socket),
            Ctx = lists:nth(Idx, Contexts),
            ClientInfo = get_client_info(Socket),
            py_reactor_context:handoff(Ctx, Fd, ClientInfo),

            %% Round-robin to next context
            NextIdx = (Idx rem length(Contexts)) + 1,
            accept_loop(ListenSock, Contexts, NextIdx);

        {error, closed} ->
            ok
    end.

get_client_info(Socket) ->
    case inet:peername(Socket) of
        {ok, {Addr, Port}} ->
            #{addr => inet:ntoa(Addr), port => Port, type => tcp};
        _ ->
            #{type => tcp}
    end.
