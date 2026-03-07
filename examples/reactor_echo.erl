#!/usr/bin/env escript
%%% @doc Simple TCP echo server using erlang.reactor.
%%%
%%% This example demonstrates the Erlang-as-Reactor architecture where:
%%% - Erlang handles TCP accept and I/O scheduling via enif_select
%%% - Python handles protocol logic (echo in this case)
%%%
%%% Prerequisites: rebar3 compile
%%% Run from project root: escript examples/reactor_echo.erl
%%%
%%% Test with: echo "hello" | nc localhost 9999

-mode(compile).

main(_) ->
    %% Add the compiled beam files to the code path
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Erlang Reactor Echo Server ===~n~n"),

    %% Start a reactor context
    {ok, Ctx} = py_reactor_context:start_link(1, auto),

    %% Set up Python echo protocol
    ok = py:exec(Ctx, <<"
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    '''Simple echo protocol - sends back whatever it receives.'''

    def data_received(self, data):
        # Echo data back
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'read_pending'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        if self.write_buffer:
            return 'continue'
        return 'read_pending'

    def connection_lost(self):
        print(f'Connection closed: fd={self.fd}')

reactor.set_protocol_factory(EchoProtocol)
print('Echo protocol registered')
">>),

    %% Listen on port 9999
    Port = 9999,
    {ok, LSock} = gen_tcp:listen(Port, [
        binary,
        {active, false},
        {reuseaddr, true},
        {nodelay, true}
    ]),

    io:format("Listening on port ~p~n", [Port]),
    io:format("Test with: echo 'hello' | nc localhost ~p~n~n", [Port]),

    %% Accept loop (in main process for simplicity)
    accept_loop(LSock, Ctx).

accept_loop(LSock, Ctx) ->
    case gen_tcp:accept(LSock, 5000) of
        {ok, Sock} ->
            %% Get the fd
            {ok, Fd} = prim_inet:getfd(Sock),

            %% Get client info
            ClientInfo = case inet:peername(Sock) of
                {ok, {Addr, Port}} ->
                    #{addr => format_addr(Addr), port => Port};
                _ ->
                    #{addr => <<"unknown">>, port => 0}
            end,

            io:format("Accepted connection from ~s:~p (fd=~p)~n",
                      [maps:get(addr, ClientInfo), maps:get(port, ClientInfo), Fd]),

            %% Hand off to reactor context
            Ctx ! {fd_handoff, Fd, ClientInfo},

            accept_loop(LSock, Ctx);

        {error, timeout} ->
            %% No connection, keep waiting
            accept_loop(LSock, Ctx);

        {error, closed} ->
            io:format("Listen socket closed~n"),
            ok;

        {error, Reason} ->
            io:format("Accept error: ~p~n", [Reason]),
            accept_loop(LSock, Ctx)
    end.

format_addr({A, B, C, D}) ->
    iolist_to_binary(io_lib:format("~B.~B.~B.~B", [A, B, C, D]));
format_addr(_) ->
    <<"unknown">>.
