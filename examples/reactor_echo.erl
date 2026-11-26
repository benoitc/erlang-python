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
    {ok, Ctx} = py_reactor_context:start_link(1, worker),

    %% Set up Python echo protocol
    ok = py:exec(Ctx, <<
        "\n"
        "import erlang.reactor as reactor\n"
        "\n"
        "class EchoProtocol(reactor.Protocol):\n"
        "    '''Simple echo protocol - sends back whatever it receives.'''\n"
        "\n"
        "    def data_received(self, data):\n"
        "        # Echo data back\n"
        "        self.write_buffer.extend(data)\n"
        "        return 'write_pending'\n"
        "\n"
        "    def write_ready(self):\n"
        "        if not self.write_buffer:\n"
        "            return 'read_pending'\n"
        "        written = self.write(bytes(self.write_buffer))\n"
        "        del self.write_buffer[:written]\n"
        "        if self.write_buffer:\n"
        "            return 'continue'\n"
        "        return 'read_pending'\n"
        "\n"
        "    def connection_lost(self):\n"
        "        print(f'Connection closed: fd={self.fd}')\n"
        "\n"
        "reactor.set_protocol_factory(EchoProtocol)\n"
        "print('Echo protocol registered')\n"
    >>),

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
            ClientInfo =
                case inet:peername(Sock) of
                    {ok, {Addr, Port}} ->
                        #{addr => format_addr(Addr), port => Port};
                    _ ->
                        #{addr => <<"unknown">>, port => 0}
                end,

            io:format(
                "Accepted connection from ~s:~p (fd=~p)~n",
                [maps:get(addr, ClientInfo), maps:get(port, ClientInfo), Fd]
            ),

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
