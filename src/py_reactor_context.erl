%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc Reactor context process with FD ownership.
%%%
%%% This module extends py_context with FD ownership and {select, ...} handling
%%% for the Erlang-as-Reactor architecture.
%%%
%%% Each py_reactor_context process:
%%% - Owns a Python context (subinterpreter or worker)
%%% - Handles FD handoffs from py_reactor_acceptor
%%% - Receives {select, FdRes, Ref, ready_input/ready_output} messages from BEAM
%%% - Calls Python protocol handlers via reactor NIFs
%%%
%%% == Connection Lifecycle ==
%%%
%%% 1. Acceptor sends {fd_handoff, Fd, ClientInfo} to this process
%%% 2. Process registers FD via py_nif:reactor_register_fd/3
%%% 3. Process calls py_nif:reactor_init_connection/3 to create Python protocol
%%% 4. BEAM sends {select, FdRes, Ref, ready_input} when data is available
%%% 5. Process calls py_nif:reactor_on_read_ready/2 to process data
%%% 6. Python returns action (continue, write_pending, close)
%%% 7. Process acts on action (reselect read, select write, close)
%%%
%%% @end
-module(py_reactor_context).

-export([
    start_link/2,
    start_link/3,
    stop/1,
    stats/1
]).

%% Internal exports
-export([init/4]).

-record(state, {
    %% Context
    id :: pos_integer(),
    ref :: reference(),

    %% Active connections
    %% Map: Fd -> #{fd_ref => FdRef, client_info => ClientInfo}
    connections :: map(),

    %% Stats
    total_requests :: non_neg_integer(),
    total_connections :: non_neg_integer(),
    active_connections :: non_neg_integer(),

    %% Config
    max_connections :: non_neg_integer(),

    %% App config (for Python protocol)
    app_module :: binary() | undefined,
    app_callable :: binary() | undefined
}).

-define(DEFAULT_MAX_CONNECTIONS, 100).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start a new py_reactor_context process.
%%
%% @param Id Unique identifier for this context
%% @param Mode Context mode (auto, subinterp, worker)
%% @returns {ok, Pid} | {error, Reason}
-spec start_link(pos_integer(), atom()) -> {ok, pid()} | {error, term()}.
start_link(Id, Mode) ->
    start_link(Id, Mode, #{}).

%% @doc Start a new py_reactor_context process with options.
%%
%% Options:
%% - max_connections: Maximum connections per context (default: 100)
%% - app_module: Python module containing ASGI/WSGI app
%% - app_callable: Python callable name (e.g., "app", "application")
%%
%% @param Id Unique identifier for this context
%% @param Mode Context mode (auto, subinterp, worker)
%% @param Opts Options map
%% @returns {ok, Pid} | {error, Reason}
-spec start_link(pos_integer(), atom(), map()) -> {ok, pid()} | {error, term()}.
start_link(Id, Mode, Opts) ->
    Parent = self(),
    Pid = spawn_link(fun() -> init(Parent, Id, Mode, Opts) end),
    receive
        {Pid, started} ->
            {ok, Pid};
        {Pid, {error, Reason}} ->
            {error, Reason}
    after 5000 ->
        exit(Pid, kill),
        {error, timeout}
    end.

%% @doc Stop a py_reactor_context process.
-spec stop(pid()) -> ok.
stop(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {stop, self(), MRef},
    receive
        {MRef, ok} ->
            erlang:demonitor(MRef, [flush]),
            ok;
        {'DOWN', MRef, process, Ctx, _Reason} ->
            ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        exit(Ctx, kill),
        ok
    end.

%% @doc Get context statistics.
-spec stats(pid()) -> map().
stats(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {stats, self(), MRef},
    receive
        {MRef, Stats} ->
            erlang:demonitor(MRef, [flush]),
            Stats;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% ============================================================================
%% Process loop
%% ============================================================================

%% @private
init(Parent, Id, Mode, Opts) ->
    process_flag(trap_exit, true),

    %% Determine mode
    ActualMode = case Mode of
        auto ->
            case py_nif:subinterp_supported() of
                true -> subinterp;
                false -> worker
            end;
        _ -> Mode
    end,

    %% Create Python context
    case py_nif:context_create(ActualMode) of
        {ok, Ref, _InterpId} ->
            %% Set up callback handler
            py_nif:context_set_callback_handler(Ref, self()),

            MaxConns = maps:get(max_connections, Opts, ?DEFAULT_MAX_CONNECTIONS),
            AppModule = maps:get(app_module, Opts, undefined),
            AppCallable = maps:get(app_callable, Opts, undefined),

            %% Initialize app in Python context if specified
            case AppModule of
                undefined -> ok;
                _ ->
                    Code = io_lib:format(
                        "import sys; sys.path.insert(0, '.'); "
                        "from ~s import ~s as _reactor_app",
                        [binary_to_list(AppModule),
                         binary_to_list(AppCallable)]),
                    py_nif:context_exec(Ref, iolist_to_binary(Code))
            end,

            State = #state{
                id = Id,
                ref = Ref,
                connections = #{},
                total_requests = 0,
                total_connections = 0,
                active_connections = 0,
                max_connections = MaxConns,
                app_module = AppModule,
                app_callable = AppCallable
            },

            Parent ! {self(), started},
            loop(State);

        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

%% @private
loop(State) ->
    receive
        %% FD handoff from acceptor
        {fd_handoff, Fd, ClientInfo} ->
            handle_fd_handoff(Fd, ClientInfo, State);

        %% Select events from BEAM scheduler
        {select, FdRes, _Ref, ready_input} ->
            handle_read_ready(FdRes, State);

        {select, FdRes, _Ref, ready_output} ->
            handle_write_ready(FdRes, State);

        %% Control messages
        {stop, From, MRef} ->
            cleanup(State),
            From ! {MRef, ok};

        {stats, From, MRef} ->
            Stats = #{
                id => State#state.id,
                active_connections => State#state.active_connections,
                total_connections => State#state.total_connections,
                total_requests => State#state.total_requests,
                max_connections => State#state.max_connections
            },
            From ! {MRef, Stats},
            loop(State);

        %% Handle EXIT signals
        {'EXIT', _Pid, Reason} ->
            cleanup(State),
            exit(Reason);

        _Other ->
            loop(State)
    end.

%% ============================================================================
%% FD Handoff
%% ============================================================================

%% @private
handle_fd_handoff(Fd, ClientInfo, State) ->
    #state{
        ref = Ref,
        connections = Conns,
        active_connections = Active,
        max_connections = MaxConns,
        total_connections = TotalConns
    } = State,

    %% Check connection limit
    case Active >= MaxConns of
        true ->
            %% At limit, reject connection
            %% Close the FD directly (it's just an integer here)
            %% The acceptor will close the socket
            loop(State);

        false ->
            %% Register FD for monitoring
            case py_nif:reactor_register_fd(Ref, Fd, self()) of
                {ok, FdRef} ->
                    %% Initialize Python protocol handler
                    case py_nif:reactor_init_connection(Ref, Fd, ClientInfo) of
                        ok ->
                            %% Store connection info
                            ConnInfo = #{
                                fd_ref => FdRef,
                                client_info => ClientInfo
                            },
                            NewConns = maps:put(Fd, ConnInfo, Conns),
                            NewState = State#state{
                                connections = NewConns,
                                active_connections = Active + 1,
                                total_connections = TotalConns + 1
                            },
                            loop(NewState);

                        {error, _Reason} ->
                            %% Failed to init connection, close
                            py_nif:reactor_close_fd(FdRef),
                            loop(State)
                    end;

                {error, _Reason} ->
                    %% Failed to register FD
                    loop(State)
            end
    end.

%% ============================================================================
%% Read Ready Handler
%% ============================================================================

%% @private
handle_read_ready(FdRes, State) ->
    #state{ref = Ref} = State,

    %% Get FD from resource
    case py_nif:get_fd_from_resource(FdRes) of
        Fd when is_integer(Fd) ->
            %% Call Python on_read_ready
            case py_nif:reactor_on_read_ready(Ref, Fd) of
                {ok, <<"continue">>} ->
                    %% More data expected, re-register for read
                    py_nif:reactor_reselect_read(FdRes),
                    loop(State);

                {ok, <<"write_pending">>} ->
                    %% Response ready, switch to write mode
                    py_nif:reactor_select_write(FdRes),
                    NewState = State#state{
                        total_requests = State#state.total_requests + 1
                    },
                    loop(NewState);

                {ok, <<"close">>} ->
                    %% Close connection
                    close_connection(Fd, FdRes, State);

                {error, _Reason} ->
                    %% Error, close connection
                    close_connection(Fd, FdRes, State)
            end;

        {error, _} ->
            %% FD resource invalid
            loop(State)
    end.

%% ============================================================================
%% Write Ready Handler
%% ============================================================================

%% @private
handle_write_ready(FdRes, State) ->
    #state{ref = Ref} = State,

    %% Get FD from resource
    case py_nif:get_fd_from_resource(FdRes) of
        Fd when is_integer(Fd) ->
            %% Call Python on_write_ready
            case py_nif:reactor_on_write_ready(Ref, Fd) of
                {ok, <<"continue">>} ->
                    %% More data to write, re-register for write
                    py_nif:reactor_select_write(FdRes),
                    loop(State);

                {ok, <<"read_pending">>} ->
                    %% Keep-alive, switch back to read mode
                    py_nif:reactor_reselect_read(FdRes),
                    loop(State);

                {ok, <<"close">>} ->
                    %% Close connection
                    close_connection(Fd, FdRes, State);

                {error, _Reason} ->
                    %% Error, close connection
                    close_connection(Fd, FdRes, State)
            end;

        {error, _} ->
            %% FD resource invalid
            loop(State)
    end.

%% ============================================================================
%% Connection Management
%% ============================================================================

%% @private
close_connection(Fd, FdRes, State) ->
    #state{
        connections = Conns,
        active_connections = Active
    } = State,

    %% Close via NIF (cleans up Python protocol handler)
    py_nif:reactor_close_fd(FdRes),

    %% Remove from connections map
    NewConns = maps:remove(Fd, Conns),
    NewState = State#state{
        connections = NewConns,
        active_connections = max(0, Active - 1)
    },
    loop(NewState).

%% @private
cleanup(State) ->
    #state{ref = Ref, connections = Conns} = State,

    %% Close all connections
    maps:foreach(fun(_Fd, #{fd_ref := FdRef}) ->
        py_nif:reactor_close_fd(FdRef)
    end, Conns),

    %% Destroy Python context
    py_nif:context_destroy(Ref),
    ok.
