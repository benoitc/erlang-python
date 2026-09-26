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

%%% @doc Context process for `isolated' mode: CPython in a child OS process.
%%%
%%% A py_context started with `mode => isolated' runs this state machine
%%% instead of the embedded loop. The child is spawned through a port (so
%%% the VM reaps it and reports its exit status) and talks over a Unix
%%% socket using the frame format of the blocking callback pipe:
%%%
%%% ```
%%% <<Id:64/native, Len:32/native, Body:Len/binary>>
%%% Body = <<Status:8, ETF/binary>>
%%% '''
%%%
%%% Status 0 request to the child, 1/2 error/ok reply (either direction),
%%% 3 request from the child (`erlang.call', `erlang.send', `erlang.whereis'),
%%% 4 event from the child, 5 control to the child (`interrupt', `cancel',
%%% `stop_loop').
%%%
%%% == States ==
%%%
%%% <ul>
%%%   <li>`idle' - child up, nothing on its main thread.</li>
%%%   <li>`{busy, Id}' - top-level request `Id' executing. Requests from
%%%       other callers are postponed (served in order once the child is
%%%       free); requests from a process running a callback for this
%%%       context are nested and dispatched at once.</li>
%%%   <li>`looping' - `start_loop' accepted, `run_forever' on the main
%%%       thread; `call/eval/exec' answer `{error, loop_running}'.</li>
%%%   <li>`stopping_loop' - `stop_loop' sent, waiting for the loop to exit;
%%%       interrupt after the grace period, SIGKILL after `kill_after'.</li>
%%%   <li>`{restarting, Reason}' - the child is gone or being killed;
%%%       requests are postponed until the new child is up. In-flight
%%%       requests fail with `{error, Reason}'.</li>
%%%   <li>`{exited, Reason}' - a session's child is gone. Sessions are never
%%%       restarted: every request answers `{error, Reason}' until the
%%%       context is stopped.</li>
%%% </ul>
%%%
%%% == Sessions ==
%%%
%%% `py_session' starts contexts with `session => true' and an `origin'.
%%% `{fork, Template}' asks a `py_session_template' to fork the child from
%%% its zygote: there is no port, the template reports the exit with
%%% `{py_session_exited, OsPid, Code}', and the child already holds the
%%% template's imports and preload. `spawn' starts one as usual. Either way
%%% the child runs in a fresh scratch directory removed when it stops.
%%%
%%% The message protocol with py_context is unchanged: requests are plain
%%% messages `{call, From, MRef, ...}' answered with `From ! {MRef, Reply}'.
%%% Use `sys:get_state/1' to see the state and `sys:trace/2' for events.
%%%
%%% @private
%%%
%%% Owns: the child OS process, its Unix socket and the request in flight.
%%% Talks to: `py_context' (public API, the same messages as the embedded
%%%   loop), `py_shm' (region handles crossing the socket), `py_callback'
%%%   (registered functions the child calls).
%%% Never: runs Python in the VM, touches NIF resources other than `os_kill'.
-module(py_isolated).

-behaviour(gen_statem).

-export([init/4, python_executable/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3, code_change/4,
         format_status/1]).

-define(REF_TAB, py_context_refs).
-define(STATUS_REQUEST, 0).
-define(STATUS_ERROR, 1).
-define(STATUS_OK, 2).
-define(STATUS_CALLBACK, 3).
-define(STATUS_EVENT, 4).
-define(STATUS_CONTROL, 5).

-define(DEFAULT_KILL_AFTER_MS, 1000).
-define(DEFAULT_START_TIMEOUT_MS, 10000).
-define(DEFAULT_MAX_RESTARTS, 5).
-define(DEFAULT_RESTART_PERIOD_MS, 10000).
-define(SHUTDOWN_GRACE_MS, 1000).
-define(EXIT_STATUS_WAIT_MS, 5000).
-define(EXIT_PROBE_MS, 100).

-record(child, {
    %% undefined for a child forked by a session template
    port :: port() | undefined,
    os_pid :: pos_integer(),
    listener :: socket:socket() | undefined,
    sock :: socket:socket(),
    sock_path :: string(),
    buf = <<>> :: binary(),
    info = #{} :: map()
}).

%% A running worker loop (state `looping' / `stopping_loop')
-record(loop, {
    owner :: pid() | undefined,
    owner_mon :: reference() | undefined,
    stop_waiters = [] :: [{pid(), reference()}]
}).

-record(data, {
    id :: term(),
    parent :: pid() | undefined,
    opts :: map(),
    child :: #child{} | undefined,
    next_id = 1 :: pos_integer(),
    %% Id => {From, MRef, Kind} for requests the child has
    pending = #{} :: map(),
    %% TaskRef => SubmitterPid for submit/5 results
    tasks = #{} :: map(),
    %% MonitorRef => FrameId of callbacks running in their own process,
    %% and Pid => MonitorRef of those processes (their requests are nested)
    callbacks = #{} :: map(),
    cb_pids = #{} :: map(),
    restarts = [] :: [integer()],
    loop :: #loop{} | undefined,
    %% Request id (or `loop') the armed kill backstop is bound to
    kill_target :: pos_integer() | loop | undefined,
    %% Callers of kill/1 answered once the new child is up
    kill_waiters = [] :: [{pid(), reference()}],
    %% Session scratch directory (the child's cwd), removed on stop
    scratch :: file:filename() | undefined
}).

-define(IS_MAIN(K), (K =:= call orelse K =:= eval orelse K =:= exec orelse
                     (is_tuple(K) andalso element(1, K) =:= start_loop))).

-type state() :: idle | {busy, pos_integer()} | looping | stopping_loop
               | {restarting, term()} | {exited, term()}.
-export_type([state/0]).

%% ============================================================================
%% Entry point (called from py_context:init/4 in the context process)
%% ============================================================================

%% @private Runs in the process py_context:start_link/3 spawned with
%% proc_lib. The child start is bounded by `start_timeout'; the parent
%% is answered before the state machine takes over.
init(Parent, Id, _Mode, Opts) ->
    process_flag(trap_exit, true),
    %% gen_statem stops when its OTP parent (head of '$ancestors') exits,
    %% for any reason. A context outlives the process that created it, as
    %% the embedded contexts do; only a crash of that process (non-normal
    %% EXIT, handled below) or a shutdown stops it. Point the parent slot at
    %% ourselves so gen_statem leaves the creator's exit to us.
    put('$ancestors', [self() | case get('$ancestors') of
                                    L when is_list(L) -> L;
                                    _ -> []
                                end]),
    ets:insert(?REF_TAB, {self(), isolated}),
    Data0 = #data{id = Id, parent = Parent, opts = Opts},
    case start_child(Data0) of
        {ok, Data} ->
            Parent ! {self(), started},
            %% Arm the socket select before handling events
            {State, Data1, Actions} = drain_socket(idle, Data, []),
            gen_statem:enter_loop(?MODULE, [], State, Data1, Actions);
        {error, Reason} ->
            ets:delete(?REF_TAB, self()),
            Parent ! {self(), {error, Reason}}
    end.

%% ============================================================================
%% gen_statem callbacks
%% ============================================================================

callback_mode() ->
    [handle_event_function, state_enter].

%% @private Not used: the process is started through init/4 and
%% gen_statem:enter_loop/5.
init(_Args) ->
    {stop, use_init_4}.

%% ---- state enter -----------------------------------------------------------

handle_event(enter, _Old, idle, #data{kill_waiters = Waiters} = Data) ->
    [W ! {M, ok} || {W, M} <- Waiters],
    {keep_state, Data#data{kill_waiters = []}};
handle_event(enter, _Old, {restarting, _}, #data{child = #child{port = undefined}}) ->
    %% A forked child's exit comes from its template, which may be gone:
    %% check the pid too
    {keep_state_and_data, [{state_timeout, ?EXIT_PROBE_MS, {probe_exit, 0}}]};
handle_event(enter, _Old, {restarting, _}, _Data) ->
    %% SIGKILL was sent (or the child is exiting): the port reports it
    %% within milliseconds; this is the safety net
    {keep_state_and_data, [{state_timeout, ?EXIT_STATUS_WAIT_MS, exit_status}]};
handle_event(enter, _Old, stopping_loop, _Data) ->
    keep_state_and_data;
handle_event(enter, _Old, _State, _Data) ->
    keep_state_and_data;

%% ---- socket and port -------------------------------------------------------

handle_event(info, {'$socket', S, select, _}, State, #data{child = #child{sock = S}} = Data) ->
    result(drain_socket(State, Data, []));
handle_event(info, {'$socket', S, abort, {_, Reason}}, State, #data{child = #child{sock = S}} = Data) ->
    result(socket_broken(Reason, State, Data, []));
handle_event(info, {'$socket', _, _, _}, _State, _Data) ->
    keep_state_and_data;
handle_event(info, {Port, {data, Out}}, _State, #data{child = #child{port = Port}} = Data) ->
    log_output(Data, Out),
    keep_state_and_data;
handle_event(info, {Port, {exit_status, Status}}, State, #data{child = #child{port = Port}} = Data) ->
    child_exited(py_child:exit_reason(Status), State, Data);
handle_event(info, {Port, _}, _State, _Data) when is_port(Port) ->
    keep_state_and_data;
handle_event(info, {py_session_exited, OsPid, Code}, State,
             #data{child = #child{port = undefined, os_pid = OsPid}} = Data) ->
    child_exited(py_child:code_reason(Code), State, Data);
handle_event(info, {py_session_exited, _, _}, _State, _Data) ->
    keep_state_and_data;
handle_event(state_timeout, {probe_exit, Waited}, {restarting, _} = State,
             #data{child = #child{os_pid = OsPid}} = Data) ->
    case py_child:os_pid_alive(OsPid) of
        false ->
            child_exited({signal, 9}, State, Data);
        true when Waited >= ?EXIT_STATUS_WAIT_MS ->
            handle_event(state_timeout, exit_status, State, Data);
        true ->
            {keep_state_and_data,
             [{state_timeout, ?EXIT_PROBE_MS, {probe_exit, Waited + ?EXIT_PROBE_MS}}]}
    end;
handle_event(state_timeout, exit_status, {restarting, _} = State, #data{child = Child} = Data) ->
    logger:error("py_context ~p (isolated): child ~p did not exit after SIGKILL",
                 [Data#data.id, Child#child.os_pid]),
    child_exited({signal, 9}, State, Data);

%% ---- requests --------------------------------------------------------------

handle_event(info, {call, From, MRef, Module, Func, Args, Kwargs}, State, Data) ->
    request(From, MRef, call, {call, Module, Func, Args, Kwargs}, State, Data);
handle_event(info, {call, From, MRef, Module, Func, Args, Kwargs, _EnvRef}, State, Data) ->
    request(From, MRef, call, {call, Module, Func, Args, Kwargs}, State, Data);
handle_event(info, {eval, From, MRef, Code, Locals}, State, Data) ->
    request(From, MRef, eval, {eval, iolist_to_binary(Code), Locals}, State, Data);
handle_event(info, {eval, From, MRef, Code, Locals, _EnvRef}, State, Data) ->
    request(From, MRef, eval, {eval, iolist_to_binary(Code), Locals}, State, Data);
handle_event(info, {exec, From, MRef, Code}, State, Data) ->
    request(From, MRef, exec, {exec, iolist_to_binary(Code)}, State, Data);
handle_event(info, {exec, From, MRef, Code, _EnvRef}, State, Data) ->
    request(From, MRef, exec, {exec, iolist_to_binary(Code)}, State, Data);
handle_event(info, {start_loop, From, MRef, Owner}, State, Data) ->
    request(From, MRef, {start_loop, Owner}, start_loop, State, Data);
handle_event(info, {submit, From, MRef, TaskRef, Module, Func, Args, Kwargs}, State, Data) ->
    request(From, MRef, {submit, TaskRef}, {submit, TaskRef, Module, Func, Args, Kwargs},
            State, Data);
handle_event(info, {pass_fd, From, MRef, Fd}, State, Data) ->
    pass_fd(From, MRef, Fd, State, Data);
handle_event(info, {call_method, From, MRef, _ObjRef, _Method, _Args}, _State, _Data) ->
    From ! {MRef, {error, not_supported_in_isolated}},
    keep_state_and_data;

%% ---- introspection ---------------------------------------------------------

handle_event(info, {get_interp_id, From, MRef}, _State, #data{child = Child}) ->
    Id = case Child of
        #child{os_pid = P} -> P;
        _ -> 0
    end,
    From ! {MRef, {ok, Id}},
    keep_state_and_data;
handle_event(info, {is_subinterp, From, MRef}, _State, _Data) ->
    From ! {MRef, true},
    keep_state_and_data;
handle_event(info, {create_local_env, From, MRef}, _State, _Data) ->
    %% Process-local environments are a NIF feature; the child has one
    %% namespace per context. A fresh ref keeps py:call(Ctx, ...) working.
    From ! {MRef, {ok, make_ref()}},
    keep_state_and_data;
handle_event(info, {get_nif_ref, From, MRef}, _State, _Data) ->
    From ! {MRef, {error, not_supported_in_isolated}},
    keep_state_and_data;
handle_event(info, {loop_ref, From, MRef}, _State, _Data) ->
    From ! {MRef, {error, not_supported_in_isolated}},
    keep_state_and_data;
handle_event(info, {child_info, From, MRef}, _State, #data{child = Child}) ->
    Info = case Child of
        #child{os_pid = P, info = I} -> I#{os_pid => P};
        undefined -> #{}
    end,
    From ! {MRef, {ok, Info}},
    keep_state_and_data;
handle_event(info, {cancel_ctrl, MRef}, _State, #data{pending = Pending} = Data) ->
    %% The caller of a control request stopped waiting: drop the entry so
    %% the late reply is not delivered
    Drop = [Id || {Id, {_, M, _}} <- maps:to_list(Pending), M =:= MRef],
    {keep_state, Data#data{pending = maps:without(Drop, Pending)}};

%% ---- interrupt, kill, stop -------------------------------------------------

handle_event(info, {interrupt, From, MRef}, State, Data) ->
    case executing_request(State, Data) of
        undefined ->
            From ! {MRef, not_running},
            keep_state_and_data;
        Target ->
            From ! {MRef, ok},
            result(send_interrupt(Target, State, Data, []))
    end;
handle_event(info, {interrupt_request, ReqMRef}, State, #data{pending = Pending} = Data) ->
    %% A timed-out request: if the child has it, interrupt that request
    %% only. If it is still postponed here nothing happens: py_context has
    %% already stopped waiting, and the reply goes nowhere.
    case [Id || {Id, {_, M, _}} <- maps:to_list(Pending), M =:= ReqMRef] of
        [Id] -> result(send_interrupt(Id, State, Data, []));
        [] -> keep_state_and_data
    end;
handle_event({timeout, kill}, Target, State, #data{pending = Pending} = Data) ->
    Still = case Target of
        loop -> State =:= looping orelse State =:= stopping_loop;
        Id -> maps:is_key(Id, Pending)
    end,
    case Still andalso Data#data.child =/= undefined of
        true ->
            logger:warning("py_context ~p (isolated): interrupt not honoured, killing child",
                           [Data#data.id]),
            kill(killed, State, Data#data{kill_target = undefined});
        false ->
            {keep_state, Data#data{kill_target = undefined}}
    end;
handle_event(info, {kill, From, MRef}, {exited, _}, _Data) ->
    From ! {MRef, ok},
    keep_state_and_data;
handle_event(info, {kill, From, MRef}, State, Data) ->
    case State of
        {restarting, _} ->
            %% Already on its way: answer with the others when it is back
            {keep_state, Data#data{kill_waiters = [{From, MRef} | Data#data.kill_waiters]}};
        _ ->
            kill(killed, State, Data#data{kill_waiters = [{From, MRef} | Data#data.kill_waiters]})
    end;
handle_event(info, {stop, From, MRef}, _State, #data{opts = Opts} = Data) ->
    How = case maps:get(session, Opts, false) of
        %% The zygote (or the VM, for a spawned one) reaps the child:
        %% nothing to wait for once it has SIGKILL
        true -> kill_nowait;
        false -> graceful
    end,
    Data1 = stop_child(Data, How),
    %% The scratch directory is removed by terminate/3, right after the
    %% reply: the caller does not wait for the file system
    From ! {MRef, ok},
    {stop, normal, Data1};

%% A session started ahead of time by its template is handed to the process
%% that asked for it: that process becomes the parent (linked, its crash
%% stops the session) and the template lets go.
handle_event(info, {set_parent, From, MRef, NewParent}, _State, #data{parent = Old} = Data) ->
    link(NewParent),
    _ = Old =/= NewParent andalso unlink(Old),
    %% An EXIT from the old parent may already be queued: forget it
    receive {'EXIT', Old, _} -> ok after 0 -> ok end,
    From ! {MRef, ok},
    {keep_state, Data#data{parent = NewParent}};

%% ---- worker loop -----------------------------------------------------------

handle_event(info, {stop_loop, From, MRef, GraceMs}, looping, #data{loop = Loop} = Data) ->
    _ = send_frame(Data#data.child, 0, ?STATUS_CONTROL, stop_loop),
    Loop1 = Loop#loop{stop_waiters = [{From, MRef} | Loop#loop.stop_waiters]},
    {next_state, stopping_loop, Data#data{loop = Loop1},
     [{state_timeout, GraceMs, interrupt}]};
handle_event(info, {stop_loop, From, MRef, _GraceMs}, stopping_loop, #data{loop = Loop} = Data) ->
    Loop1 = Loop#loop{stop_waiters = [{From, MRef} | Loop#loop.stop_waiters]},
    {keep_state, Data#data{loop = Loop1}};
handle_event(info, {stop_loop, From, MRef, _GraceMs}, _State, _Data) ->
    From ! {MRef, {error, no_loop}},
    keep_state_and_data;
handle_event(state_timeout, interrupt, stopping_loop, Data) ->
    %% Cooperative stop did not land: interrupt, then the kill backstop
    result(send_interrupt(loop, stopping_loop, Data, []));
handle_event(info, {'DOWN', Mon, process, _Owner, _Reason}, looping,
             #data{loop = #loop{owner_mon = Mon} = Loop} = Data) ->
    %% Owner is gone: nobody will hear the exit, stop the loop
    _ = send_frame(Data#data.child, 0, ?STATUS_CONTROL, stop_loop),
    Loop1 = Loop#loop{owner = undefined, owner_mon = undefined},
    {next_state, stopping_loop, Data#data{loop = Loop1},
     [{state_timeout, 5000, interrupt}]};

%% ---- callbacks (child -> Erlang) -------------------------------------------

handle_event(info, {callback_reply, FrameId, Status, Term}, State,
             #data{child = Child} = Data) when Child =/= undefined ->
    case State of
        {restarting, _} ->
            keep_state_and_data;
        _ ->
            case send_frame(Child, FrameId, Status, Term) of
                ok -> keep_state_and_data;
                {error, Reason} -> result(socket_broken(Reason, State, Data, []))
            end
    end;
handle_event(info, {callback_reply, _, _, _}, _State, _Data) ->
    keep_state_and_data;
handle_event(info, {'DOWN', Mon, process, Pid, Reason}, State, #data{callbacks = Cbs} = Data) ->
    case maps:take(Mon, Cbs) of
        {Id, Rest} ->
            Data1 = Data#data{callbacks = Rest, cb_pids = maps:remove(Pid, Data#data.cb_pids)},
            case Reason of
                normal ->
                    {keep_state, Data1};
                _ ->
                    %% The callback process died without replying: the
                    %% child must not wait for it
                    Msg = iolist_to_binary(io_lib:format("callback crashed: ~p", [Reason])),
                    handle_event(info, {callback_reply, Id, ?STATUS_ERROR, Msg}, State, Data1)
            end;
        error ->
            keep_state_and_data
    end;

%% ---- exits -----------------------------------------------------------------

handle_event(info, {'EXIT', Parent, Reason}, _State, #data{parent = Parent}) when Reason =/= normal ->
    %% Whoever started us is gone: do not leave a child without an owner
    {stop, Reason};
handle_event(info, {'EXIT', _Pid, Reason}, _State, _Data)
        when Reason =:= shutdown; Reason =:= kill ->
    {stop, Reason};
handle_event(info, {'EXIT', _Pid, {shutdown, _} = Reason}, _State, _Data) ->
    {stop, Reason};
handle_event(info, {'EXIT', _Pid, _Reason}, _State, _Data) ->
    keep_state_and_data;
handle_event(info, _Other, _State, _Data) ->
    keep_state_and_data.

terminate(Reason, _State, #data{child = Child} = Data) ->
    _ = case Child of
        undefined -> Data;
        _ when Reason =:= normal; Reason =:= shutdown ->
            case maps:get(session, Data#data.opts, false) of
                true -> stop_child(Data, kill);
                false -> stop_child(Data, graceful)
            end;
        _ -> stop_child(Data, kill)
    end,
    remove_scratch(Data),
    ets:delete(?REF_TAB, self()),
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%% @private Keep sys:get_status readable: the socket buffer is noise
format_status(#{data := #data{child = #child{} = Child} = Data} = Status) ->
    Status#{data => Data#data{child = Child#child{buf = <<>>}}};
format_status(Status) ->
    Status.

%% @doc Python executable used for isolated children; see
%% py_child:python_executable/1.
-spec python_executable(map()) -> string() | {error, term()}.
python_executable(Opts) ->
    py_child:python_executable(Opts).

%% ============================================================================
%% Child startup
%% ============================================================================

start_child(#data{opts = Opts} = St) ->
    case check_platform_opts(Opts) of
        ok ->
            case py_child:check_env_opts(Opts) of
                ok -> start_child_1(St);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% cgroups exist only on Linux; rlimits are POSIX and apply everywhere.
%% RLIMIT_AS is enforced by the kernel on Linux and FreeBSD; on macOS the
%% child enforces `as' with a watchdog thread on its resident set.
check_platform_opts(Opts) ->
    case {maps:get(cgroup, Opts, undefined), os:type()} of
        {undefined, _} -> ok;
        {_, {unix, linux}} -> ok;
        {_, {unix, Os}} -> {error, {cgroup_unsupported, Os}}
    end.

start_child_1(#data{opts = Opts} = St0) ->
    St = with_scratch(St0),
    ChildOpts = case St#data.scratch of
        undefined -> Opts;
        Dir -> Opts#{cd => Dir}
    end,
    Started = case maps:get(origin, Opts, spawn) of
        {fork, Template} ->
            fork_child(Template, ChildOpts);
        spawn ->
            case python_executable(Opts) of
                {error, _} = NoPython -> NoPython;
                Python -> spawn_child(Python, ChildOpts)
            end
    end,
    case Started of
        {ok, Child} ->
            handshake(St#data{child = Child});
        {error, _} = Failed ->
            remove_scratch(St),
            Failed
    end.

%% A session runs in its own empty directory, created here and removed
%% when the context stops.
with_scratch(#data{scratch = undefined, opts = #{session := true}} = St) ->
    {ok, Dir} = py_child:make_scratch_dir("sess_"),
    St#data{scratch = Dir};
with_scratch(St) ->
    St.

remove_scratch(#data{scratch = undefined}) ->
    ok;
remove_scratch(#data{scratch = Dir}) ->
    py_child:remove_tree(Dir),
    ok.

%% Ask the session template to fork a child from its zygote; the child
%% connects to our socket like a spawned one.
fork_child(Template, Opts) ->
    Path = py_child:new_sock_path("sess_"),
    case py_child:listen(Path) of
        {ok, L} ->
            Timeout = maps:get(start_timeout, Opts, ?DEFAULT_START_TIMEOUT_MS),
            ForkOpts = maps:with([rlimits, cgroup, cd], Opts),
            case py_session_template:fork(Template, Path, ForkOpts, Timeout) of
                {ok, OsPid} ->
                    case py_child:accept(L, {os_pid, OsPid}, Timeout) of
                        {ok, S} ->
                            py_child:delete_file(Path),
                            py_child:tune_socket(S),
                            {ok, #child{port = undefined, os_pid = OsPid, listener = L,
                                        sock = S, sock_path = Path}};
                        {error, Reason} ->
                            py_child:delete_file(Path),
                            socket:close(L),
                            py_child:kill_os_pid(OsPid),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    py_child:delete_file(Path),
                    socket:close(L),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, {spawn_failed, Reason}}
    end.

spawn_child(Python, Opts) ->
    Path = py_child:new_sock_path("ctx_"),
    case py_child:listen(Path) of
        {ok, L} ->
            try
                Script = filename:join(py_child:priv_dir(), "py_isolated_child.py"),
                Args = [Script, Path | py_child:rlimit_args(Opts) ++ py_child:cgroup_args(Opts)],
                PortOpts = [exit_status, stderr_to_stdout, binary, use_stdio,
                            {args, Args}, {env, py_child:port_env(Opts)}]
                           ++ [{cd, Dir} || Dir <- [maps:get(cd, Opts, undefined)],
                                            Dir =/= undefined],
                Port = open_port({spawn_executable, Python}, PortOpts),
                OsPid = case erlang:port_info(Port, os_pid) of
                    {os_pid, Pid} -> Pid;
                    _ -> 0
                end,
                Timeout = maps:get(start_timeout, Opts, ?DEFAULT_START_TIMEOUT_MS),
                case py_child:accept(L, {port, Port}, Timeout) of
                    {ok, S} ->
                        py_child:delete_file(Path),
                        py_child:tune_socket(S),
                        {ok, #child{port = Port, os_pid = OsPid, listener = L,
                                    sock = S, sock_path = Path}};
                    {error, Reason} ->
                        py_child:delete_file(Path),
                        socket:close(L),
                        kill_port(Port, OsPid),
                        {error, Reason}
                end
            catch
                Class:Err:Stack ->
                    py_child:delete_file(Path),
                    socket:close(L),
                    {error, {spawn_failed, {Class, Err, Stack}}}
            end;
        {error, {socket_open_failed, _}} = Err ->
            Err;
        {error, Reason} ->
            {error, {spawn_failed, Reason}}
    end.

%% Blocking handshake: ready event, init request, then the preload exec.
handshake(#data{child = Child, opts = Opts} = St0) ->
    St = St0#data{},
    Timeout = maps:get(start_timeout, Opts, ?DEFAULT_START_TIMEOUT_MS),
    case recv_frame_sync(Child, Timeout) of
        {ok, {0, ?STATUS_EVENT, {ready, Info}}, Child1} when Child1#child.port =:= undefined ->
            %% Forked: imports and preload ran in the zygote, and the child
            %% learnt our pid from the fork request
            {ok, St#data{child = Child1#child{info = Info}}};
        {ok, {0, ?STATUS_EVENT, {ready, Info}}, Child1} ->
            St1 = St#data{child = Child1#child{info = Info}},
            Paths = [py_child:to_bin(P) || P <- py_import:all_paths()] ++ extra_paths(Opts),
            %% Registered imports are pre-cached in sys.modules, as
            %% interp_apply_imports does for the embedded modes
            Imports = lists:usort([py_child:to_bin(M) || {M, _} <- py_import:all_imports()]),
            case sync_request(St1, {init, self(), Paths, Imports}, Timeout) of
                {{ok, _}, St2} ->
                    run_preload(St2, Timeout);
                {{error, Reason}, St2} ->
                    stop_child(St2, kill),
                    {error, {init_failed, Reason}}
            end;
        {ok, {0, ?STATUS_EVENT, {startup_error, Problems}}, Child1} ->
            stop_child(St#data{child = Child1}, kill),
            {error, {startup_error, Problems}};
        {ok, {0, ?STATUS_EVENT, {memory_limit, Rss}}, Child1} ->
            %% The memory watchdog fired before the child was ready
            stop_child(St#data{child = Child1}, kill),
            {error, {startup_error, [{memory_limit, Rss}]}};
        {ok, Other, Child1} ->
            stop_child(St#data{child = Child1}, kill),
            {error, {unexpected_handshake, Other}};
        {error, Reason} ->
            stop_child(St, kill),
            {error, {handshake_failed, Reason}}
    end.

run_preload(#data{opts = Opts} = St, Timeout) ->
    Code = case maps:get(preload, Opts, undefined) of
        undefined -> py_preload_code();
        C -> [py_preload_code(), <<"\n">>, iolist_to_binary(C)]
    end,
    case iolist_to_binary(Code) of
        <<>> ->
            {ok, St};
        Bin ->
            case sync_request(St, {exec, Bin}, Timeout) of
                {{ok, _}, St1} ->
                    {ok, St1};
                {{error, Reason}, St1} ->
                    logger:warning("py_context ~p (isolated): preload failed: ~p",
                                   [St#data.id, Reason]),
                    {ok, St1}
            end
    end.

%% Global preload registered with py_preload (applied to every context)
py_preload_code() ->
    try py_preload:get_code() of
        Code when is_binary(Code) -> Code;
        _ -> <<>>
    catch
        _:_ -> <<>>
    end.

extra_paths(Opts) ->
    [py_child:to_bin(P) || P <- maps:get(paths, Opts, [])].

%% Send a request and wait for its reply, ignoring nothing: callbacks made
%% by the child during startup are served too.
sync_request(#data{child = Child, next_id = Id} = St, Term, Timeout) ->
    case send_frame(Child, Id, ?STATUS_REQUEST, Term) of
        ok -> sync_wait(St#data{next_id = Id + 1}, Id, Timeout);
        {error, Reason} -> {{error, Reason}, St}
    end.

run_callback_bounded(Term, Timeout) ->
    {Pid, Mon} = spawn_monitor(fun() -> exit({callback_done, run_callback(Term)}) end),
    receive
        {'DOWN', Mon, process, Pid, {callback_done, Result}} ->
            Result;
        {'DOWN', Mon, process, Pid, Reason} ->
            {?STATUS_ERROR, iolist_to_binary(io_lib:format("callback crashed: ~p", [Reason]))}
    after Timeout ->
        erlang:demonitor(Mon, [flush]),
        exit(Pid, kill),
        {?STATUS_ERROR, <<"callback timed out during context start">>}
    end.

sync_wait(#data{child = Child} = St, Id, Timeout) ->
    case recv_frame_sync(Child, Timeout) of
        {ok, {FrameId, ?STATUS_CALLBACK, Term}, Child1} ->
            %% Callbacks made while the child starts (preload, imports) run
            %% in a separate process so a crash cannot take this one down.
            %% They cannot re-enter this context yet: the loop is not
            %% running, so such a call would wait for the handshake timeout.
            St1 = St#data{child = Child1},
            {Status, Reply} = run_callback_bounded(Term, Timeout),
            case send_frame(Child1, FrameId, Status, Reply) of
                ok -> sync_wait(St1, Id, Timeout);
                {error, Reason} -> {{error, Reason}, St1}
            end;
        {ok, {Id, Status, Term}, Child1}
                when Status =:= ?STATUS_OK; Status =:= ?STATUS_ERROR ->
            {reply_term(Status, Term), St#data{child = Child1}};
        {ok, {0, ?STATUS_EVENT, {log, Level, Msg}}, Child1} ->
            log_event(St, Level, Msg),
            sync_wait(St#data{child = Child1}, Id, Timeout);
        {ok, {_, _, _}, Child1} ->
            sync_wait(St#data{child = Child1}, Id, Timeout);
        {error, Reason} ->
            {{error, Reason}, St}
    end.

recv_frame_sync(#child{buf = Buf} = Child, Timeout) ->
    case py_child:parse_frame(Buf) of
        {ok, Frame, Rest} ->
            {ok, Frame, Child#child{buf = Rest}};
        more ->
            case socket:recv(Child#child.sock, 0, Timeout) of
                {ok, <<>>} ->
                    {error, closed};
                {ok, Data} ->
                    recv_frame_sync(Child#child{buf = <<Buf/binary, Data/binary>>}, Timeout);
                {error, {Reason, _Data}} ->
                    {error, Reason};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% ============================================================================
%% Requests to the child
%% ============================================================================

%% Every path returns a gen_statem result. Main-thread requests (call, eval,
%% exec, start_loop) are served one caller at a time; the rest is handled by
%% the child's reader thread and can go in any state that has a child.
request(From, MRef, {start_loop, _}, _Term, State, _Data)
        when State =:= looping; State =:= stopping_loop ->
    From ! {MRef, {error, already_running}},
    keep_state_and_data;
request(From, MRef, Kind, _Term, looping, _Data) when ?IS_MAIN(Kind) ->
    From ! {MRef, {error, loop_running}},
    keep_state_and_data;
request(From, MRef, _Kind, _Term, {exited, Reason}, _Data) ->
    From ! {MRef, {error, Reason}},
    keep_state_and_data;
request(_From, _MRef, _Kind, _Term, {restarting, _}, _Data) ->
    {keep_state_and_data, [postpone]};
request(_From, _MRef, Kind, _Term, stopping_loop, _Data) when ?IS_MAIN(Kind) ->
    {keep_state_and_data, [postpone]};
request(From, MRef, Kind, Term, {busy, _} = State, #data{cb_pids = CbPids} = Data)
        when ?IS_MAIN(Kind) ->
    case maps:is_key(From, CbPids) of
        true ->
            %% Nested: a callback of the executing request calls back in
            result(dispatch(From, MRef, Kind, Term, State, Data));
        false ->
            {keep_state_and_data, [postpone]}
    end;
request(From, MRef, Kind, Term, idle, Data) when ?IS_MAIN(Kind) ->
    case dispatch(From, MRef, Kind, Term, idle, Data) of
        {idle, Data1, Actions} ->
            {next_state, {busy, Data1#data.next_id - 1}, Data1, Actions};
        Other ->
            result(Other)
    end;
request(From, MRef, Kind, Term, State, Data) ->
    result(dispatch(From, MRef, Kind, Term, State, Data)).

dispatch(From, MRef, Kind, Term, State, #data{child = Child, next_id = Id, pending = Pending} = Data) ->
    case send_frame(Child, Id, ?STATUS_REQUEST, Term) of
        ok ->
            {State, Data#data{next_id = Id + 1, pending = Pending#{Id => {From, MRef, Kind}}}, []};
        {error, Reason} ->
            From ! {MRef, {error, {child_exited, Reason}}},
            socket_broken(Reason, State, Data, [])
    end.

pass_fd(From, MRef, _Fd, {exited, Reason}, _Data) ->
    From ! {MRef, {error, Reason}},
    keep_state_and_data;
pass_fd(_From, _MRef, _Fd, {restarting, _}, _Data) ->
    {keep_state_and_data, [postpone]};
pass_fd(From, MRef, Fd, _State, #data{child = Child, next_id = Id, pending = Pending} = Data)
        when is_integer(Fd), Fd >= 0 ->
    Frame = py_child:frame(Id, ?STATUS_REQUEST, term_to_binary(pass_fd)),
    Msg = #{iov => [Frame],
            ctrl => [#{level => socket, type => rights, data => <<Fd:32/native>>}]},
    case socket:sendmsg(Child#child.sock, Msg) of
        ok ->
            {keep_state, Data#data{next_id = Id + 1, pending = Pending#{Id => {From, MRef, pass_fd}}}};
        {error, Reason} ->
            From ! {MRef, {error, {pass_fd_failed, Reason}}},
            keep_state_and_data
    end;
pass_fd(From, MRef, Fd, _State, _Data) ->
    From ! {MRef, {error, {invalid_fd, Fd}}},
    keep_state_and_data.

%% Turn a {State, Data, Actions} triple into a gen_statem result
result({stop, Reason, Data}) ->
    {stop, Reason, Data};
result({State, Data, Actions}) ->
    {next_state, State, Data, Actions}.

%% ============================================================================
%% Frames from the child
%% ============================================================================

%% Read until the socket would block, processing complete frames.
drain_socket({restarting, _} = State, Data, Actions) ->
    {State, Data, Actions};
drain_socket(State, #data{child = #child{sock = S, buf = Buf} = Child} = Data, Actions) ->
    case socket:recv(S, 0, nowait) of
        {ok, <<>>} ->
            socket_broken(closed, State, Data, Actions);
        {ok, Bytes} ->
            Data1 = Data#data{child = Child#child{buf = <<Buf/binary, Bytes/binary>>}},
            case process_frames(State, Data1, Actions) of
                {{restarting, _}, _, _} = Broken -> Broken;
                {State1, Data2, Actions1} -> drain_socket(State1, Data2, Actions1)
            end;
        {select, _SelectInfo} ->
            process_frames(State, Data, Actions);
        {error, {Reason, Bytes}} when is_binary(Bytes) ->
            Data1 = Data#data{child = Child#child{buf = <<Buf/binary, Bytes/binary>>}},
            {State1, Data2, Actions1} = process_frames(State, Data1, Actions),
            socket_broken(Reason, State1, Data2, Actions1);
        {error, Reason} ->
            socket_broken(Reason, State, Data, Actions)
    end.

process_frames({restarting, _} = State, Data, Actions) ->
    {State, Data, Actions};
process_frames(State, #data{child = #child{buf = Buf} = Child} = Data, Actions) ->
    case py_child:parse_frame(Buf) of
        {ok, Frame, Rest} ->
            Data1 = Data#data{child = Child#child{buf = Rest}},
            {State1, Data2, Actions1} = handle_frame(Frame, State, Data1, Actions),
            process_frames(State1, Data2, Actions1);
        more ->
            {State, Data, Actions};
        {error, Reason} ->
            socket_broken({malformed_frame, Reason}, State, Data, Actions)
    end.


handle_frame({Id, Status, Term}, State, #data{pending = Pending} = Data, Actions)
        when Status =:= ?STATUS_OK; Status =:= ?STATUS_ERROR ->
    case maps:take(Id, Pending) of
        {{From, MRef, Kind}, Rest} ->
            {Data1, Actions1} = cancel_kill_timer(Id, Data#data{pending = Rest}, Actions),
            {Next, Data2} = deliver(Kind, From, MRef, reply_term(Status, Term), Data1),
            %% Only the reply of the request holding the main thread frees
            %% it; nested replies (callbacks calling back in) do not
            State1 = case {Next, State} of
                {looping, _} -> looping;
                {done, {busy, Id}} -> idle;
                _ -> State
            end,
            {State1, Data2, Actions1};
        error ->
            {State, Data, Actions}
    end;
handle_frame({Id, ?STATUS_CALLBACK, Term}, State, #data{callbacks = Cbs, cb_pids = CbPids} = Data, Actions) ->
    Owner = self(),
    {Pid, Mon} = spawn_monitor(fun() ->
        {Status, Reply} = run_callback(Term),
        Owner ! {callback_reply, Id, Status, Reply}
    end),
    {State, Data#data{callbacks = Cbs#{Mon => Id}, cb_pids = CbPids#{Pid => Mon}}, Actions};
handle_frame({_, ?STATUS_EVENT, Event}, State, Data, Actions) ->
    on_child_event(Event, State, Data, Actions);
handle_frame({_, _, _}, State, Data, Actions) ->
    {State, Data, Actions}.

%% Deliver a reply. Returns `done' for a main-thread request, `looping'
%% when a loop just started, `keep' otherwise.
deliver(exec, From, MRef, {ok, _}, Data) ->
    From ! {MRef, ok},
    {done, Data};
deliver({start_loop, Owner}, From, MRef, {ok, _}, Data) ->
    Mon = case is_pid(Owner) of
        true -> erlang:monitor(process, Owner);
        false -> undefined
    end,
    From ! {MRef, ok},
    {looping, Data#data{loop = #loop{owner = Owner, owner_mon = Mon}}};
deliver({submit, TaskRef}, From, MRef, {ok, _}, #data{tasks = Tasks} = Data) ->
    From ! {MRef, {ok, TaskRef}},
    {keep, Data#data{tasks = Tasks#{TaskRef => From}}};
deliver(Kind, From, MRef, Reply, Data) when ?IS_MAIN(Kind) ->
    From ! {MRef, Reply},
    {done, Data};
deliver(_Kind, From, MRef, Reply, Data) ->
    From ! {MRef, Reply},
    {keep, Data}.

on_child_event({async_result, TaskRef, Result}, State, #data{tasks = Tasks} = Data, Actions) ->
    case maps:take(TaskRef, Tasks) of
        {Pid, Rest} ->
            Pid ! {async_result, TaskRef, Result},
            {State, Data#data{tasks = Rest}, Actions};
        error ->
            {State, Data, Actions}
    end;
on_child_event({loop_exit, Result}, State, Data, Actions)
        when State =:= looping; State =:= stopping_loop ->
    {Data1, Actions1} = cancel_kill_timer(loop, Data, Actions),
    {idle, loop_exited(Result, Data1), Actions1};
on_child_event({memory_limit, Rss}, State, Data, Actions) ->
    %% The child's memory watchdog is exiting; the exit_status follows
    socket_broken({memory_limit, Rss}, State, Data, Actions);
on_child_event({log, Level, Msg}, State, Data, Actions) ->
    log_event(Data, Level, Msg),
    {State, Data, Actions};
on_child_event(_, State, Data, Actions) ->
    {State, Data, Actions}.

reply_term(?STATUS_OK, Term) -> {ok, Term};
reply_term(?STATUS_ERROR, Term) -> {error, Term}.

%% ---------------------------------------------------------------------------
%% Callbacks (child -> Erlang)
%% ---------------------------------------------------------------------------

run_callback({call, Name, Args}) ->
    ArgsList = case Args of
        L when is_list(L) -> L;
        T when is_tuple(T) -> tuple_to_list(T);
        _ -> [Args]
    end,
    try py_callback:execute(py_child:to_bin(Name), ArgsList) of
        {ok, Result} ->
            {?STATUS_OK, Result};
        {error, {not_found, N}} ->
            {?STATUS_ERROR, iolist_to_binary(io_lib:format("Function '~s' not registered", [N]))};
        {error, {Class, Reason, _Stack}} ->
            {?STATUS_ERROR, iolist_to_binary(io_lib:format("~p: ~p", [Class, Reason]))}
    catch
        Class:Reason ->
            {?STATUS_ERROR, iolist_to_binary(io_lib:format("~p:~p", [Class, Reason]))}
    end;
run_callback({send, Pid, Msg}) when is_pid(Pid) ->
    case node(Pid) =:= node() andalso not is_process_alive(Pid) of
        true -> {?STATUS_ERROR, {noproc, Pid}};
        false -> Pid ! Msg, {?STATUS_OK, ok}
    end;
run_callback({send, Other, _}) ->
    {?STATUS_ERROR, {badarg, Other}};
run_callback({whereis, Name}) ->
    try
        Atom = if is_atom(Name) -> Name;
                  is_binary(Name) -> binary_to_existing_atom(Name, utf8);
                  is_list(Name) -> list_to_existing_atom(Name)
               end,
        case erlang:whereis(Atom) of
            undefined -> {?STATUS_OK, none};
            Pid -> {?STATUS_OK, Pid}
        end
    catch
        _:_ -> {?STATUS_OK, none}
    end;
run_callback(Other) ->
    {?STATUS_ERROR, {unknown_request, Other}}.


%% ============================================================================
%% Interrupt / kill
%% ============================================================================

%% What an interrupt targets: the innermost main-thread request the child
%% is executing (nested requests are dispatched while the outer waits), or
%% the loop.
executing_request(looping, _Data) -> loop;
executing_request(stopping_loop, _Data) -> loop;
executing_request({busy, _}, #data{pending = Pending}) ->
    lists:max([Id || {Id, {_, _, Kind}} <- maps:to_list(Pending), ?IS_MAIN(Kind)]);
executing_request(_State, _Data) -> undefined.

%% The child signals only if Target is what it is executing, so an
%% interrupt for a request that just completed cannot hit its successor.
%% The kill backstop is bound to Target.
send_interrupt(Target, State, #data{opts = Opts} = Data, Actions) ->
    case send_frame(Data#data.child, 0, ?STATUS_CONTROL, {interrupt, Target}) of
        ok ->
            After = maps:get(kill_after, Opts, ?DEFAULT_KILL_AFTER_MS),
            {State, Data#data{kill_target = Target},
             [{{timeout, kill}, After, Target} | Actions]};
        {error, Reason} ->
            socket_broken(Reason, State, Data, Actions)
    end.

%% A reply for the interrupted request means the interrupt landed
cancel_kill_timer(Target, #data{kill_target = Target} = Data, Actions) ->
    {Data#data{kill_target = undefined}, [{{timeout, kill}, cancel} | Actions]};
cancel_kill_timer(_Target, Data, Actions) ->
    {Data, Actions}.

%% SIGKILL the child; the port's exit_status drives the restart. Callers
%% of kill/1 are answered when the new child is idle.
kill(Reason, State, #data{child = #child{port = Port, os_pid = OsPid}} = Data) ->
    kill_port(Port, OsPid),
    result(enter_restarting(Reason, State, Data, []));
kill(_Reason, _State, _Data) ->
    keep_state_and_data.

kill_port(undefined, OsPid) ->
    py_child:kill_os_pid(OsPid);
kill_port(Port, OsPid) ->
    case erlang:port_info(Port) =/= undefined of
        true -> py_child:kill_os_pid(OsPid);
        false -> ok
    end.


%% ============================================================================
%% Failure handling and restart
%% ============================================================================

%% Nothing can reach the child any more. Make sure it exits; the port's
%% exit_status (which follows within milliseconds) fails the pending
%% requests with the real cause and runs the restart policy.
socket_broken(_Reason, {restarting, _} = State, Data, Actions) ->
    {State, Data, Actions};
socket_broken(Reason, State, #data{child = #child{port = Port, os_pid = OsPid}} = Data, Actions) ->
    logger:debug("py_context ~p (isolated): socket to child ~p broken: ~p",
                 [Data#data.id, OsPid, Reason]),
    kill_port(Port, OsPid),
    enter_restarting({child_exited, {socket, Reason}}, State, Data, Actions).

enter_restarting(_Reason, {restarting, _} = State, Data, Actions) ->
    {State, Data, Actions};
enter_restarting(Reason, _State, Data, Actions) ->
    %% Timers of the old child are meaningless now
    {{restarting, Reason}, Data#data{kill_target = undefined},
     [{{timeout, kill}, cancel} | Actions]}.

%% The port reported the child's exit: fail what was in flight, then
%% restart within the budget or stop.
child_exited(Reason, State, #data{child = Child, opts = Opts} = Data0) ->
    close_child(Child),
    FailReason = case {State, Reason} of
        {{restarting, killed}, _} -> killed;
        %% The memory watchdog announced the exit; our SIGKILL may win the race
        {{restarting, {child_exited, {socket, {memory_limit, _} = Mem}}}, _} -> {child_exited, Mem};
        %% We killed it because the socket broke: report the socket, unless
        %% the child was already dying of something more telling
        {{restarting, {child_exited, {socket, _}} = SockReason}, {signal, 9}} -> SockReason;
        {{restarting, {child_exited, {socket, _}}}, _} -> {child_exited, Reason};
        {{restarting, Other}, _} -> Other;
        {_, _} -> {child_exited, Reason}
    end,
    Data1 = fail_pending(FailReason, Data0#data{child = undefined, kill_target = undefined}),
    case FailReason of
        killed ->
            logger:info("py_context ~p (isolated): child killed", [Data0#data.id]);
        _ ->
            logger:warning("py_context ~p (isolated): child exited: ~p",
                           [Data0#data.id, FailReason])
    end,
    case maps:get(session, Opts, false) of
        true ->
            %% No new child will come: kill/1 callers are answered now
            [W ! {M, ok} || {W, M} <- Data1#data.kill_waiters],
            {next_state, {exited, FailReason}, Data1#data{kill_waiters = []}};
        false ->
            restart_or_stop(Reason, Data0, Data1)
    end.

restart_or_stop(Reason, Data0, #data{opts = Opts} = Data1) ->
    case maps:get(restart, Opts, true) andalso restart_allowed(Data1) of
        true ->
            Now = erlang:monotonic_time(millisecond),
            Data2 = Data1#data{restarts = [Now | Data1#data.restarts]},
            case start_child(Data2) of
                {ok, Data3} ->
                    logger:info("py_context ~p (isolated): child restarted (pid ~p)",
                                [Data0#data.id, (Data3#data.child)#child.os_pid]),
                    result(drain_socket(idle, Data3, [{{timeout, kill}, cancel}]));
                {error, RestartError} ->
                    logger:error("py_context ~p (isolated): restart failed: ~p",
                                 [Data0#data.id, RestartError]),
                    {stop, {child_restart_failed, RestartError}, Data1}
            end;
        false ->
            {stop, {child_exited, Reason}, Data1}
    end.

%% In-flight requests, submitted tasks and a running loop fail with Reason.
%% Postponed requests are untouched: they are served by the next child, or
%% their callers get a DOWN if the process stops.
fail_pending(Reason, #data{pending = Pending, tasks = Tasks} = Data) ->
    maps:foreach(fun(_, {From, MRef, _Kind}) ->
        From ! {MRef, {error, Reason}}
    end, Pending),
    maps:foreach(fun(TaskRef, Pid) ->
        Pid ! {async_result, TaskRef, {error, Reason}}
    end, Tasks),
    Data1 = case Data#data.loop of
        undefined -> Data;
        _ -> loop_exited({error, Reason}, Data)
    end,
    Data1#data{pending = #{}, tasks = #{}}.

restart_allowed(#data{restarts = Restarts, opts = Opts}) ->
    Max = maps:get(max_restarts, Opts, ?DEFAULT_MAX_RESTARTS),
    Period = maps:get(restart_period, Opts, ?DEFAULT_RESTART_PERIOD_MS),
    Now = erlang:monotonic_time(millisecond),
    Recent = [T || T <- Restarts, Now - T =< Period],
    length(Recent) < Max.


%% Graceful: ask the child to exit, wait briefly, then SIGKILL.
stop_child(#data{child = undefined} = Data, _How) ->
    Data;
stop_child(#data{child = #child{port = Port, os_pid = OsPid} = Child} = Data, How) ->
    case How of
        graceful ->
            _ = send_frame(Child, 0, ?STATUS_REQUEST, shutdown),
            case wait_exit(Child, ?SHUTDOWN_GRACE_MS) of
                ok -> ok;
                timeout -> kill_port(Port, OsPid), wait_exit(Child, 2000)
            end;
        kill_nowait ->
            kill_port(Port, OsPid);
        _ ->
            kill_port(Port, OsPid),
            wait_exit(Child, 2000)
    end,
    close_child(Child),
    Data1 = fail_pending({child_exited, stopped}, Data),
    Data1#data{child = undefined}.

wait_exit(#child{port = undefined, os_pid = OsPid}, Timeout) ->
    wait_forked_exit(OsPid, Timeout);
wait_exit(#child{port = Port}, Timeout) ->
    receive
        {Port, {exit_status, _}} -> ok
    after Timeout ->
        timeout
    end.

%% The template reports a forked child's exit; if it is gone, the pid is
%% enough
wait_forked_exit(OsPid, Timeout) ->
    receive
        {py_session_exited, OsPid, _} -> ok
    after min(Timeout, ?EXIT_PROBE_MS) ->
        case py_child:os_pid_alive(OsPid) of
            false -> ok;
            true when Timeout =< ?EXIT_PROBE_MS -> timeout;
            true -> wait_forked_exit(OsPid, Timeout - ?EXIT_PROBE_MS)
        end
    end.

close_child(#child{port = Port, sock = S, listener = L}) ->
    _ = socket:close(S),
    _ = socket:close(L),
    close_port(Port).

close_port(undefined) ->
    ok;
close_port(Port) ->
    try port_close(Port) catch error:badarg -> ok end,
    ok.


%% ---------------------------------------------------------------------------
%% Worker loop helpers
%% ---------------------------------------------------------------------------

loop_exited(Result, #data{loop = #loop{owner = Owner, owner_mon = Mon,
                                        stop_waiters = Waiters}} = Data) ->
    case Mon of
        undefined -> ok;
        _ -> erlang:demonitor(Mon, [flush])
    end,
    case is_pid(Owner) of
        true -> Owner ! {py_loop_exit, self(), Result};
        false -> ok
    end,
    [W ! {M, ok} || {W, M} <- Waiters],
    Data#data{loop = undefined};
loop_exited(_Result, Data) ->
    Data.

%% ---------------------------------------------------------------------------
%% Wire helpers
%% ---------------------------------------------------------------------------

send_frame(#child{sock = S}, Id, Status, Term) ->
    py_child:send_frame(S, Id, Status, Term).

log_output(#data{id = Id, child = #child{os_pid = OsPid}}, Data) ->
    Lines = binary:split(Data, <<"\n">>, [global, trim_all]),
    [logger:info("py_context ~p (isolated pid ~p): ~s", [Id, OsPid, L]) || L <- Lines],
    ok.

log_event(#data{id = Id}, Level, Msg) ->
    Lvl = case Level of
        error -> error; warning -> warning; debug -> debug; _ -> info
    end,
    logger:log(Lvl, "py_context ~p (isolated): ~s", [Id, Msg]).
