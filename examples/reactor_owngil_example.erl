%% @doc Example: OWN_GIL reactor with dedicated threads.
%%
%% Each subinterpreter handle runs in a dedicated pthread with its own GIL.
%% This provides true parallelism for CPU-bound protocol processing.
%%
%% Best for: ML inference, heavy parsing, CPU-bound protocol logic.
%%
%% Note: Requires Python 3.12+ with subinterpreter support.

-module(reactor_owngil_example).
-export([start/0, start/1, stop/1]).

%% Protocol that simulates CPU-intensive work
-define(CPU_PROTOCOL, <<"
import erlang.reactor as reactor
import hashlib

class CPUProtocol(reactor.Protocol):
    '''Protocol with CPU-intensive hashing.'''

    def __init__(self):
        super().__init__()
        self.iterations = 10000

    def connection_made(self, fd, client_info):
        super().connection_made(fd, client_info)

    def data_received(self, data):
        # CPU-intensive hashing (runs in parallel due to OWN_GIL)
        result = bytes(data)
        for _ in range(self.iterations):
            result = hashlib.sha256(result).digest()

        self.write_buffer.extend(result)
        return 'write_pending'

    def write_ready(self):
        if self.write_buffer:
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            if self.write_buffer:
                return 'continue'
        return 'read_pending'

reactor.set_protocol_factory(CPUProtocol)
">>).

%% @doc Start with default settings (4 handles).
start() ->
    start(#{handles => 4, port => 8081}).

%% @doc Start OWN_GIL reactor.
%%
%% Options:
%%   handles - Number of subinterpreter handles (default: 4)
%%   port - Port to listen on (default: 8081)
%%
%% Returns: {ok, State} where State can be passed to stop/1
start(Opts) ->
    NumHandles = maps:get(handles, Opts, 4),
    Port = maps:get(port, Opts, 8081),

    %% Start OWN_GIL thread pool
    ok = py:subinterp_pool_start(NumHandles),

    %% Create subinterpreter handles - each with its own pthread + GIL
    Handles = [begin
        {ok, Handle} = py:subinterp_create(),
        %% Initialize reactor protocol in this subinterpreter
        ok = py:subinterp_exec(Handle, ?CPU_PROTOCOL),
        Handle
    end || _ <- lists:seq(1, NumHandles)],

    %% Start acceptor
    {ok, ListenSock} = gen_tcp:listen(Port, [
        binary,
        {active, false},
        {reuseaddr, true},
        {backlog, 64}
    ]),

    Acceptor = spawn_link(fun() ->
        accept_loop(ListenSock, Handles, 1)
    end),

    io:format("OWN_GIL reactor started on port ~p with ~p handles~n", [Port, NumHandles]),
    io:format("Each handle runs in its own pthread with dedicated GIL~n"),

    {ok, #{handles => Handles, acceptor => Acceptor, socket => ListenSock}}.

%% @doc Stop the OWN_GIL reactor.
stop(#{handles := Handles, acceptor := Acceptor, socket := Socket}) ->
    exit(Acceptor, shutdown),
    gen_tcp:close(Socket),
    [py:subinterp_destroy(H) || H <- Handles],
    py:subinterp_pool_stop(),
    ok.

accept_loop(ListenSock, Handles, Idx) ->
    case gen_tcp:accept(ListenSock) of
        {ok, Socket} ->
            {ok, Fd} = prim_inet:getfd(Socket),
            Handle = lists:nth(Idx, Handles),
            ClientInfo = get_client_info(Socket),

            %% Initialize connection via OWN_GIL reactor API
            ok = py:subinterp_reactor_init(Handle, Fd, ClientInfo),

            %% Spawn handler for this connection
            spawn_link(fun() -> handle_connection(Handle, Fd, Socket) end),

            NextIdx = (Idx rem length(Handles)) + 1,
            accept_loop(ListenSock, Handles, NextIdx);

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

handle_connection(Handle, Fd, Socket) ->
    %% Simple blocking receive for example purposes
    case gen_tcp:recv(Socket, 0, 30000) of
        {ok, Data} ->
            %% Dispatch to OWN_GIL subinterpreter
            case py:subinterp_reactor_read(Handle, Fd, Data) of
                {ok, <<"write_pending">>} ->
                    handle_write(Handle, Fd, Socket);
                {ok, <<"continue">>} ->
                    handle_connection(Handle, Fd, Socket);
                {ok, <<"close">>} ->
                    py:subinterp_reactor_close(Handle, Fd),
                    gen_tcp:close(Socket);
                {error, _Reason} ->
                    py:subinterp_reactor_close(Handle, Fd),
                    gen_tcp:close(Socket)
            end;
        {error, closed} ->
            py:subinterp_reactor_close(Handle, Fd);
        {error, _} ->
            py:subinterp_reactor_close(Handle, Fd),
            gen_tcp:close(Socket)
    end.

handle_write(Handle, Fd, Socket) ->
    case py:subinterp_reactor_write(Handle, Fd) of
        {ok, <<"read_pending">>} ->
            handle_connection(Handle, Fd, Socket);
        {ok, <<"continue">>} ->
            handle_write(Handle, Fd, Socket);
        {ok, <<"close">>} ->
            py:subinterp_reactor_close(Handle, Fd),
            gen_tcp:close(Socket);
        _ ->
            py:subinterp_reactor_close(Handle, Fd),
            gen_tcp:close(Socket)
    end.
