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

%%% @doc Process body of a context in `worker' or `owngil' mode.
%%%
%%% `py_context:init/4' hands the process here once the mode is known. The
%%% NIF context is created, the registry imports, paths and preload are
%%% applied, and the process enters `loop/1': one request at a time,
%%% forwarded to the context thread with the `*_async' NIFs and answered by
%%% `{py_result, Ref, Result}'. While the thread waits for an Erlang
%%% callback the process runs it here, serving nested requests, so
%%% callbacks can call Python again to any depth.
%%%
%%% Messages and replies are those documented in `py_context'; callers never
%%% address this module.
%%%
%%% Owns: the NIF context resource, the request in flight, the worker loop
%%%   state and the callback handler process.
%%% Talks to: `py_nif' (context NIFs), `py_callback' (registered funs),
%%%   `py_event_worker' (owngil loops), `py_import', `py_preload'.
%%% Never: answers a caller directly with anything but `{MRef, Reply}'.
%%%
%%% @private
-module(py_context_embedded).

-export([init/4]).
%% Used by py_reactor_context
-export([extend_erlang_module_in_context/1]).

-record(state, {
    ref :: reference(),
    id :: pos_integer(),
    interp_id :: non_neg_integer(),
    event_state = #{} :: map(),  %% #{loop_ref => ref(), worker_pid => pid()}
    callback_handler :: pid() | undefined,  %% For thread-model callback handling
    %% Worker loop (start_loop/1): request id of the run_forever exec, the
    %% owner that gets {py_loop_exit, Ctx, Result}, its monitor, and the
    %% callers waiting in stop_loop/2
    loop_req :: reference() | undefined,
    loop_owner :: pid() | undefined,
    loop_owner_mon :: reference() | undefined,
    loop_stop_waiters = [] :: [{pid(), reference()}]
}).

%% Time given to a running loop to exit after py_context:interrupt/1
-define(LOOP_INTERRUPT_GRACE_MS, 3000).

%% @private
init(Parent, Id, Mode, Opts) ->
    process_flag(trap_exit, true),
    case create_context(Mode) of
        {ok, Ref, InterpId} ->
            %% Publish the NIF reference so interrupt/1 can reach it while
            %% this process is blocked in a NIF
            py_context:register_nif_ref(Ref),
            case apply_memory_limit(Ref, Opts) of
                ok ->
                    init_started(Parent, Id, Ref, InterpId, Opts);
                {error, LimitError} ->
                    py_context:unregister_nif_ref(),
                    try py_nif:context_destroy(Ref) catch _:_ -> ok end,
                    Parent ! {self(), {error, LimitError}}
            end;
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

%% @private
apply_memory_limit(Ref, Opts) ->
    case maps:get(memory_limit, Opts, undefined) of
        undefined ->
            ok;
        Bytes when is_integer(Bytes), Bytes >= 0 ->
            py_nif:context_set_memory_limit(Ref, Bytes);
        Other ->
            {error, {invalid_memory_limit, Other}}
    end.

%% @private
init_started(Parent, Id, Ref, InterpId, Opts) ->
    %% Apply all registered imports and paths to this interpreter
    apply_registered_imports(Ref),
    apply_registered_paths(Ref),
    %% Apply preload code (populates globals for process-local envs)
    apply_preload(Ref),
    %% Per-context preload from new/1 (imports the app once per worker)
    case maps:get(preload, Opts, undefined) of
        undefined -> ok;
        PreCode when is_binary(PreCode); is_list(PreCode) ->
            case handle_exec_with_async(Ref, iolist_to_binary(PreCode)) of
                ok -> ok;
                {error, PreErr} ->
                    error_logger:warning_msg(
                        "py_context ~p: preload failed: ~p~n", [InterpId, PreErr])
            end
    end,
    %% For subinterpreters, create a dedicated event worker
    EventState = setup_event_worker(Ref, InterpId),
    %% For thread-model subinterpreters, spawn a dedicated callback handler
    %% because the main context process will be blocked in the NIF
    CallbackHandler = case maps:get(mode, EventState, normal) of
        thread_model ->
            Handler = spawn_callback_handler(Ref),
            ok = py_nif:context_set_callback_handler(Ref, Handler),
            Handler;
        _ ->
            undefined
    end,
    Parent ! {self(), started},
    State = #state{
        ref = Ref,
        id = Id,
        interp_id = InterpId,
        event_state = EventState,
        callback_handler = CallbackHandler
    },
    loop(State).

%% @private Create event worker for subinterpreter contexts
setup_event_worker(Ref, InterpId) ->
    case py_nif:context_get_event_loop(Ref) of
        {ok, LoopRef} ->
            %% This is a subinterpreter - create dedicated event worker
            WorkerId = iolist_to_binary(["ctx_", integer_to_list(InterpId)]),
            case py_event_worker:start_link(WorkerId, LoopRef) of
                {ok, WorkerPid} ->
                    ok = py_nif:event_loop_set_worker(LoopRef, WorkerPid),
                    %% Extend erlang module with event loop functions
                    extend_erlang_module_in_context(Ref),
                    #{loop_ref => LoopRef, worker_pid => WorkerPid};
                {error, WorkerError} ->
                    error_logger:warning_msg(
                        "py_context ~p: Failed to start event worker: ~p~n",
                        [InterpId, WorkerError]),
                    #{}
            end;
        {error, not_subinterp} ->
            %% Worker mode - uses shared router (lazy initialization)
            #{};
        {error, event_loop_owned_by_thread} ->
            %% Thread-model subinterpreter: event loop is managed by dedicated thread.
            %% This is expected behavior, not a failure.
            #{mode => thread_model};
        {error, Reason} ->
            error_logger:warning_msg(
                "py_context ~p: Failed to get event loop: ~p~n",
                [InterpId, Reason]),
            #{}
    end.

%% @private Extend the erlang module with event loop functions in a subinterpreter
extend_erlang_module_in_context(Ref) ->
    PrivDir = code:priv_dir(erlang_python),
    Code = iolist_to_binary([
        "import sys\n",
        "priv_dir = '", PrivDir, "'\n",
        "if priv_dir not in sys.path:\n",
        "    sys.path.insert(0, priv_dir)\n",
        "import erlang\n",
        "if hasattr(erlang, '_extend_erlang_module'):\n",
        "    erlang._extend_erlang_module(priv_dir)\n"
    ]),
    case py_nif:context_exec(Ref, Code) of
        ok -> ok;
        {error, Reason} ->
            error_logger:warning_msg(
                "py_context: Failed to extend erlang module: ~p~n", [Reason]),
            ok
    end.

%% @private Apply all imports from the global registry to this interpreter.
%%
%% Called when a new interpreter is created to pre-warm the module cache
%% with all modules registered via py_import:ensure_imported/1,2.
apply_registered_imports(Ref) ->
    case py_import:all_imports() of
        [] -> ok;
        Imports -> py_nif:interp_apply_imports(Ref, Imports)
    end.

%% @private Apply all paths from the global registry to this interpreter.
%%
%% Called when a new interpreter is created to add all registered paths
%% to sys.path.
apply_registered_paths(Ref) ->
    case py_import:all_paths() of
        [] -> ok;
        Paths -> py_nif:interp_apply_paths(Ref, Paths)
    end.

%% @private Apply preload code to the interpreter's globals.
%%
%% Called when a new interpreter is created. The preload code populates
%% the context's globals dict, which process-local environments inherit.
apply_preload(Ref) ->
    py_preload:apply_preload(Ref).

%% @private
create_context(worker) ->
    py_nif:context_create(worker);
create_context(owngil) ->
    %% OWN_GIL mode requires Python 3.14+ due to C extension bugs in earlier versions
    case py_nif:owngil_supported() of
        true -> py_nif:context_create(owngil);
        false -> {error, owngil_requires_python314}
    end.

%% @private
%% Main context loop. Handles requests and uses suspension-based callback support.
loop(#state{ref = Ref, interp_id = InterpId, loop_req = LoopReq} = State) ->
    receive
        %% ---- worker loop management (start_loop/stop_loop/loop_ref) ----
        {start_loop, From, MRef, _Owner} when LoopReq =/= undefined ->
            From ! {MRef, {error, already_running}},
            loop(State);

        {start_loop, From, MRef, Owner} ->
            {Reply, NewState} = do_start_loop(Owner, State),
            From ! {MRef, Reply},
            loop(NewState);

        {stop_loop, From, MRef, _GraceMs} when LoopReq =:= undefined ->
            From ! {MRef, {error, no_loop}},
            loop(State);

        {stop_loop, From, MRef, GraceMs} ->
            loop(begin_stop_loop(From, MRef, GraceMs, State));

        {loop_ref, From, MRef} ->
            From ! {MRef, context_loop_ref(State)},
            loop(State);

        {py_result, LoopReq, Result} when LoopReq =/= undefined ->
            loop(loop_exited(Result, State));

        {loop_stop_deadline, LoopReq} when LoopReq =/= undefined ->
            %% Cooperative stop did not land: interrupt the thread
            _ = py_nif:context_interrupt(Ref),
            erlang:send_after(?LOOP_INTERRUPT_GRACE_MS, self(),
                              {loop_interrupt_deadline, LoopReq}),
            loop(State);

        {loop_interrupt_deadline, LoopReq} when LoopReq =/= undefined ->
            [W ! {M, {error, timeout}} || {W, M} <- State#state.loop_stop_waiters],
            loop(State#state{loop_stop_waiters = []});

        {loop_stop_deadline, _} ->
            loop(State);
        {loop_interrupt_deadline, _} ->
            loop(State);

        {'DOWN', Mon, process, _Owner, _Reason}
                when Mon =:= State#state.loop_owner_mon, LoopReq =/= undefined ->
            %% Owner is gone: nobody will hear the exit, stop the loop
            loop(begin_stop_loop(undefined, undefined, 5000,
                                 State#state{loop_owner_mon = undefined}));

        {async_result, _TaskRef, _} ->
            %% Result of a coroutine this process submitted (loop stop) - drop
            loop(State);

        %% ---- while a worker loop runs, the thread is not available ----
        {call, From, MRef, _, _, _, _} when LoopReq =/= undefined ->
            From ! {MRef, {error, loop_running}}, loop(State);
        {call, From, MRef, _, _, _, _, _} when LoopReq =/= undefined ->
            From ! {MRef, {error, loop_running}}, loop(State);
        {eval, From, MRef, _, _} when LoopReq =/= undefined ->
            From ! {MRef, {error, loop_running}}, loop(State);
        {eval, From, MRef, _, _, _} when LoopReq =/= undefined ->
            From ! {MRef, {error, loop_running}}, loop(State);
        {exec, From, MRef, _} when LoopReq =/= undefined ->
            From ! {MRef, {error, loop_running}}, loop(State);
        {exec, From, MRef, _, _} when LoopReq =/= undefined ->
            From ! {MRef, {error, loop_running}}, loop(State);
        {call_method, From, MRef, _, _, _} when LoopReq =/= undefined ->
            From ! {MRef, {error, loop_running}}, loop(State);

        {stop, From, MRef} when LoopReq =/= undefined ->
            %% Get the thread out of the loop before destroying the context,
            %% otherwise context_destroy waits for a thread that never returns
            terminate(normal, stop_running_loop(State)),
            From ! {MRef, ok};

        {'EXIT', _Pid, Reason} = Exit when LoopReq =/= undefined,
                                            (Reason =:= shutdown orelse Reason =:= kill orelse
                                             (is_tuple(Reason) andalso element(1, Reason) =:= shutdown)) ->
            self() ! Exit,
            loop(stop_running_loop(State));

        {call, From, MRef, Module, Func, Args, Kwargs} ->
            Result = handle_call_with_suspension(Ref, Module, Func, Args, Kwargs),
            From ! {MRef, Result},
            loop(State);

        %% Call with process-local environment (worker mode)
        {call, From, MRef, Module, Func, Args, Kwargs, EnvRef} ->
            Result = handle_call_with_suspension_and_env(Ref, Module, Func, Args, Kwargs, EnvRef),
            From ! {MRef, Result},
            loop(State);

        {eval, From, MRef, Code, Locals} ->
            Result = handle_eval_with_suspension(Ref, Code, Locals),
            From ! {MRef, Result},
            loop(State);

        %% Eval with process-local environment (worker mode)
        {eval, From, MRef, Code, Locals, EnvRef} ->
            Result = handle_eval_with_suspension_and_env(Ref, Code, Locals, EnvRef),
            From ! {MRef, Result},
            loop(State);

        {exec, From, MRef, Code} ->
            Result = handle_exec_with_async(Ref, Code),
            From ! {MRef, Result},
            loop(State);

        %% Exec with process-local environment (worker mode).
        %% Async dispatch with sync fallback (mirrors call/eval).
        {exec, From, MRef, Code, EnvRef} ->
            Result = handle_exec_with_async_and_env(Ref, Code, EnvRef),
            From ! {MRef, Result},
            loop(State);

        {call_method, From, MRef, ObjRef, Method, Args} ->
            Result = py_nif:context_call_method(Ref, ObjRef, Method, Args),
            From ! {MRef, Result},
            loop(State);

        {get_interp_id, From, MRef} ->
            From ! {MRef, {ok, InterpId}},
            loop(State);

        {is_subinterp, From, MRef} ->
            %% Check the interp_id to determine if this is a subinterpreter
            %% Subinterpreters have interp_id > 0 (main interpreter is 0)
            %% But actually we need to check the mode, not just interp_id
            IsSubinterp = is_context_subinterp(Ref),
            From ! {MRef, IsSubinterp},
            loop(State);

        {create_local_env, From, MRef} ->
            %% Create env inside this context's interpreter
            Result = py_nif:create_local_env(Ref),
            From ! {MRef, Result},
            loop(State);

        {get_nif_ref, From, MRef} ->
            From ! {MRef, Ref},
            loop(State);

        {stop, From, MRef} ->
            terminate(normal, State),
            From ! {MRef, ok};

        {'EXIT', Pid, Reason} ->
            %% Handle EXIT from linked processes
            case State#state.callback_handler of
                Pid ->
                    %% Callback handler died - restart it for thread-model contexts
                    error_logger:warning_msg(
                        "py_context ~p: Callback handler died: ~p, restarting~n",
                        [InterpId, Reason]),
                    NewHandler = spawn_callback_handler(Ref),
                    ok = py_nif:context_set_callback_handler(Ref, NewHandler),
                    NewState = State#state{callback_handler = NewHandler},
                    loop(NewState);
                _ ->
                    case State#state.event_state of
                        #{worker_pid := Pid} ->
                            %% Event worker died - log and continue (degraded asyncio support)
                            error_logger:warning_msg(
                                "py_context ~p: Event worker died: ~p~n",
                                [InterpId, Reason]),
                            NewState = State#state{event_state = #{}},
                            loop(NewState);
                        _ when Reason =:= shutdown; Reason =:= kill ->
                            %% Supervisor shutdown or kill signal - clean exit
                            terminate(Reason, State);
                        _ when is_tuple(Reason), element(1, Reason) =:= shutdown ->
                            %% Supervisor shutdown with extra info: {shutdown, _}
                            terminate(Reason, State);
                        _ ->
                            %% Ignore EXIT from other processes
                            loop(State)
                    end
            end
    end.

%% ============================================================================
%% Worker loop helpers
%% ============================================================================

%% @private Loop reference: the context's own loop (owngil) or the shared
%% main-interpreter loop (worker mode)
context_loop_ref(#state{event_state = #{loop_ref := LoopRef}}) ->
    {ok, LoopRef};
context_loop_ref(_State) ->
    py_event_loop:get_loop().

%% @private Start run_forever on the context thread through the async exec
%% path, so this process stays free to serve loop_ref/stop_loop and the
%% dirty schedulers are not held.
do_start_loop(Owner, #state{ref = Ref} = State) ->
    case context_loop_ref(State) of
        {ok, _} ->
            LoopReq = make_ref(),
            case py_nif:context_call_async(Ref, self(), LoopReq, <<"erlang">>,
                                           <<"_run_loop_forever">>, [self()], #{}) of
                {enqueued, LoopReq} ->
                    %% Wait for the loop to actually run before answering, so
                    %% a submit right after start_loop finds it
                    receive
                        {py_loop_started} ->
                            Mon = case is_pid(Owner) of
                                true -> erlang:monitor(process, Owner);
                                false -> undefined
                            end,
                            {ok, State#state{loop_req = LoopReq, loop_owner = Owner,
                                             loop_owner_mon = Mon, loop_stop_waiters = []}};
                        {py_result, LoopReq, {error, Reason}} ->
                            {{error, Reason}, State};
                        {py_result, LoopReq, Other} ->
                            {{error, {loop_exited, Other}}, State}
                    after 10000 ->
                        {{error, loop_start_timeout}, State}
                    end;
                {error, Reason} ->
                    {{error, Reason}, State}
            end;
        {error, Reason} ->
            {{error, Reason}, State}
    end.

%% @private Ask the running loop to stop from inside, arm the interrupt
%% deadline, and remember who to answer once it has exited.
begin_stop_loop(From, MRef, GraceMs, #state{loop_req = LoopReq} = State) ->
    Waiters = case From of
        undefined -> State#state.loop_stop_waiters;
        _ -> [{From, MRef} | State#state.loop_stop_waiters]
    end,
    case context_loop_ref(State) of
        {ok, LoopRef} ->
            _ = py_nif:submit_task(LoopRef, self(), make_ref(),
                                   <<"erlang">>, <<"_stop_loop">>, [], #{});
        _ ->
            ok
    end,
    erlang:send_after(GraceMs, self(), {loop_stop_deadline, LoopReq}),
    State#state{loop_stop_waiters = Waiters}.

%% @private The exec running the loop returned: tell the owner and the
%% stop_loop callers, clear the loop state.
loop_exited(Result, #state{loop_owner = Owner, loop_owner_mon = Mon,
                           loop_stop_waiters = Waiters} = State) ->
    case Mon of
        undefined -> ok;
        _ -> erlang:demonitor(Mon, [flush])
    end,
    case is_pid(Owner) of
        true -> Owner ! {py_loop_exit, self(), Result};
        false -> ok
    end,
    [W ! {M, ok} || {W, M} <- Waiters],
    State#state{loop_req = undefined, loop_owner = undefined,
                loop_owner_mon = undefined, loop_stop_waiters = []}.

%% @private Synchronous stop used before terminate: interrupt and wait a
%% bounded time for the exec to return.
stop_running_loop(#state{ref = Ref, loop_req = LoopReq} = State) ->
    _ = py_nif:context_interrupt(Ref),
    receive
        {py_result, LoopReq, Result} ->
            loop_exited(Result, State)
    after ?LOOP_INTERRUPT_GRACE_MS ->
        loop_exited({error, timeout}, State)
    end.

%% @private Clean up resources on termination
terminate(_Reason, #state{ref = Ref, event_state = EventState, callback_handler = CallbackHandler}) ->
    py_context:unregister_nif_ref(),
    %% Stop the callback handler if it exists
    case CallbackHandler of
        Pid when is_pid(Pid) ->
            Pid ! stop;
        _ ->
            ok
    end,
    %% Stop the event worker first (if it exists and is still alive)
    case EventState of
        #{worker_pid := WorkerPid} ->
            try gen_server:stop(WorkerPid, normal, 5000) catch _:_ -> ok end;
        _ ->
            ok
    end,
    %% Destroy the Python context
    try py_nif:context_destroy(Ref) catch _:_ -> ok end,
    ok.

%% ============================================================================
%% Blocking callback handling (for thread-model subinterpreters)
%% ============================================================================
%%
%% Thread-model subinterpreters use blocking pipe-based callbacks because
%% the suspension mechanism doesn't work when Python runs in a dedicated thread.
%% The Python thread blocks waiting for a response on the callback pipe.
%%
%% A separate callback handler process is spawned because the main context
%% process is blocked in the NIF (dispatch_to_thread) and cannot receive messages.

%% @private
%% Spawn a dedicated callback handler process for thread-model subinterpreters.
spawn_callback_handler(Ref) ->
    spawn_link(fun() -> callback_handler_loop(Ref) end).

%% @private
%% Callback handler loop - receives erlang_callback messages and responds.
callback_handler_loop(Ref) ->
    receive
        {erlang_callback, _CallbackId, FuncName, Args} ->
            handle_blocking_callback(Ref, FuncName, Args),
            callback_handler_loop(Ref);
        stop ->
            ok
    end.

%% @private
%% Handle a blocking callback from a thread-model subinterpreter.
%% Executes the callback and writes the response to the callback pipe.
handle_blocking_callback(Ref, FuncName, Args) ->
    %% Convert Args from tuple to list if needed
    ArgsList = case Args of
        T when is_tuple(T) -> tuple_to_list(T);
        L when is_list(L) -> L;
        _ -> [Args]
    end,
    %% Execute the registered function
    Response = case py_callback:execute(FuncName, ArgsList) of
        {ok, Result} ->
            %% Format: status_byte (2=ok, ETF) + external term format
            <<2, (term_to_binary(Result))/binary>>;
        {error, {not_found, Name}} ->
            ErrMsg = iolist_to_binary(
                io_lib:format("Function '~s' not registered", [Name])),
            <<1, ErrMsg/binary>>;
        {error, {Class, Reason, _Stack}} ->
            ErrMsg = iolist_to_binary(
                io_lib:format("~p: ~p", [Class, Reason])),
            <<1, ErrMsg/binary>>
    end,
    %% Write response to context's callback pipe
    py_nif:context_write_callback_response(Ref, Response).

%% ============================================================================
%% Suspension-based callback handling
%% ============================================================================
%%
%% When Python calls erlang.call(), the NIF returns {suspended, ...} instead of
%% blocking. We handle the callback inline and then resume Python execution.
%% This enables unlimited nesting depth without deadlock.

%% @private
%% Handle call with potential suspension for callbacks
handle_call_with_suspension(Ref, Module, Func, Args, Kwargs) ->
    RequestId = make_ref(),
    case py_nif:context_call_async(Ref, self(), RequestId, Module, Func, Args, Kwargs) of
        {enqueued, RequestId} ->
            %% Async dispatch succeeded - wait for result message
            wait_for_async_result(Ref, RequestId);
        {error, Reason} ->
            {error, Reason}
    end.


%% @private
%% Handle eval with potential suspension for callbacks
handle_eval_with_suspension(Ref, Code, Locals) ->
    RequestId = make_ref(),
    case py_nif:context_eval_async(Ref, self(), RequestId, Code, Locals) of
        {enqueued, RequestId} ->
            %% Async dispatch succeeded - wait for result message
            wait_for_async_result(Ref, RequestId);
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Handle exec with async dispatch
handle_exec_with_async(Ref, Code) ->
    RequestId = make_ref(),
    case py_nif:context_exec_async(Ref, self(), RequestId, Code) of
        {enqueued, RequestId} ->
            wait_for_async_result(Ref, RequestId);
        {error, Reason} ->
            {error, Reason}
    end.


%% @private
%% Wait for async result from worker thread
%% The worker thread sends {py_result, RequestId, Result} when done.
%%
%% Drains stale {py_result, _, _} messages from prior timed-out
%% requests before the matching receive so a context that experiences
%% repeat timeouts doesn't grow an unbounded mailbox: when
%% wait_for_async_result/2 returns {error, async_timeout}, the C
%% worker can still finish later and deliver the result; without the
%% drain those messages would accumulate forever.
%%
%% Safe because the context process is the sole receiver for its own
%% async results and only one wait_for_async_result/2 is in flight at
%% a time, so the drain cannot consume the result of a concurrent live
%% request.
wait_for_async_result(Ref, RequestId) ->
    drain_stale_async_results(RequestId),
    receive
        {py_result, RequestId, Result} ->
            process_async_result(Ref, Result)
    after 300000 ->  %% 5 minute timeout
        {error, async_timeout}
    end.

%% @private
drain_stale_async_results(CurrentId) ->
    receive
        {py_result, OldId, _} when OldId =/= CurrentId ->
            drain_stale_async_results(CurrentId)
    after 0 ->
        ok
    end.

%% @private
%% Process the result from async dispatch
%% Handles suspension, schedule markers, and normal results.
process_async_result(Ref, {suspended, _CallbackId, StateRef, {FuncName, CallbackArgs}}) ->
    CallbackResult = handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs),
    resume_and_continue(Ref, StateRef, CallbackResult);
process_async_result(Ref, {schedule, CallbackName, CallbackArgs}) ->
    handle_schedule(Ref, CallbackName, CallbackArgs);
process_async_result(_Ref, Result) ->
    Result.

%% @private
%% Handle call with process-local environment.
handle_call_with_suspension_and_env(Ref, Module, Func, Args, Kwargs, EnvRef) ->
    RequestId = make_ref(),
    case py_nif:context_call_with_env_async(Ref, self(), RequestId,
                                              Module, Func, Args, Kwargs,
                                              EnvRef) of
        {enqueued, RequestId} ->
            wait_for_async_result(Ref, RequestId);
        {error, Reason} ->
            {error, Reason}
    end.


%% @private
%% Handle eval with process-local environment.
handle_eval_with_suspension_and_env(Ref, Code, Locals, EnvRef) ->
    RequestId = make_ref(),
    case py_nif:context_eval_with_env_async(Ref, self(), RequestId,
                                              Code, Locals, EnvRef) of
        {enqueued, RequestId} ->
            wait_for_async_result(Ref, RequestId);
        {error, Reason} ->
            {error, Reason}
    end.


%% @private
%% Handle exec with process-local environment via the same async-first
%% path used for call/eval.
handle_exec_with_async_and_env(Ref, Code, EnvRef) ->
    RequestId = make_ref(),
    case py_nif:context_exec_with_env_async(Ref, self(), RequestId,
                                              Code, EnvRef) of
        {enqueued, RequestId} ->
            wait_for_async_result(Ref, RequestId);
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Check if a context is a subinterpreter (has interp_id > 0)
is_context_subinterp(Ref) ->
    py_nif:context_interp_id(Ref) > 0.

%% @private
%% Handle schedule marker - Python returned erlang.schedule() or schedule_py()
%% Execute the callback and return its result transparently to the caller.
%%
%% Special case for _execute_py: this callback is used by schedule_py() to
%% call back into Python with a different function. We handle it directly
%% using context_call to avoid recursion through py:call.
handle_schedule(Ref, <<"_execute_py">>, {Module, Func, Args, Kwargs}) ->
    %% schedule_py callback: call Python function via context
    CallArgs = case Args of
        none -> [];
        undefined -> [];
        List when is_list(List) -> List;
        Tuple when is_tuple(Tuple) -> tuple_to_list(Tuple);
        _ -> [Args]
    end,
    CallKwargs = case Kwargs of
        none -> #{};
        undefined -> #{};
        Map when is_map(Map) -> Map;
        _ -> #{}
    end,
    handle_call_with_suspension(Ref, Module, Func, CallArgs, CallKwargs);
handle_schedule(_Ref, CallbackName, CallbackArgs) when is_binary(CallbackName) ->
    %% Regular callback: execute via py_callback:execute
    ArgsList = tuple_to_list(CallbackArgs),
    case py_callback:execute(CallbackName, ArgsList) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Handle callback, allowing nested py:eval/call to be processed.
%% We spawn a process to execute the callback so we can stay in a receive loop
%% for nested calls while the callback runs.
handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs) ->
    Parent = self(),
    CallbackPid = spawn_link(fun() ->
        Result = try
            ArgsList = tuple_to_list(CallbackArgs),
            case py_callback:execute(FuncName, ArgsList) of
                {ok, Value} ->
                    {ok, <<2, (term_to_binary(Value))/binary>>};
                {error, Reason} ->
                    ErrMsg = iolist_to_binary(io_lib:format("~p", [Reason])),
                    {ok, <<1, ErrMsg/binary>>}
            end
        catch
            Class:ExcReason:Stacktrace ->
                ErrorMsg = iolist_to_binary(io_lib:format("~p:~p~n~p",
                    [Class, ExcReason, Stacktrace])),
                {ok, <<1, ErrorMsg/binary>>}
        end,
        Parent ! {callback_result, self(), Result}
    end),
    %% Wait for callback, processing nested requests
    wait_for_callback(Ref, CallbackPid).

%% @private
%% Wait for callback result while processing nested py:call/eval requests.
%% This enables arbitrarily deep callback nesting.
wait_for_callback(Ref, CallbackPid) ->
    receive
        {callback_result, CallbackPid, Result} ->
            Result;

        %% Handle nested py:call while waiting for callback
        {call, From, MRef, Module, Func, Args, Kwargs} ->
            NestedResult = handle_call_with_suspension(Ref, Module, Func, Args, Kwargs),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested py:call while waiting for callback (with EnvRef)
        {call, From, MRef, Module, Func, Args, Kwargs, EnvRef} ->
            NestedResult = handle_call_with_suspension_and_env(Ref, Module, Func, Args, Kwargs, EnvRef),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested py:eval while waiting for callback (without EnvRef)
        {eval, From, MRef, Code, Locals} ->
            NestedResult = handle_eval_with_suspension(Ref, Code, Locals),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested py:eval while waiting for callback (with EnvRef)
        {eval, From, MRef, Code, Locals, EnvRef} ->
            NestedResult = handle_eval_with_suspension_and_env(Ref, Code, Locals, EnvRef),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested py:exec while waiting for callback
        {exec, From, MRef, Code} ->
            NestedResult = py_nif:context_exec(Ref, Code),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested py:exec while waiting for callback (with EnvRef)
        {exec, From, MRef, Code, EnvRef} ->
            NestedResult = py_nif:context_exec(Ref, Code, EnvRef),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested call_method while waiting for callback
        {call_method, From, MRef, ObjRef, Method, Args} ->
            NestedResult = py_nif:context_call_method(Ref, ObjRef, Method, Args),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle get_interp_id while waiting
        {get_interp_id, From, MRef} ->
            InterpId = py_nif:context_interp_id(Ref),
            From ! {MRef, {ok, InterpId}},
            wait_for_callback(Ref, CallbackPid);

        %% Handle create_local_env while waiting
        {create_local_env, From, MRef} ->
            Result = py_nif:create_local_env(Ref),
            From ! {MRef, Result},
            wait_for_callback(Ref, CallbackPid);

        {get_nif_ref, From, MRef} ->
            From ! {MRef, Ref},
            wait_for_callback(Ref, CallbackPid)
    end.

%% @private
%% Resume suspended state, handle additional suspensions (nested callbacks)
resume_and_continue(Ref, StateRef, {ok, ResultBin}) ->
    case py_nif:context_resume(Ref, StateRef, ResultBin) of
        {suspended, _CallbackId2, StateRef2, {FuncName2, Args2}} ->
            %% Another callback during resume - recursive handling
            CallbackResult2 = handle_callback_with_nested_receive(Ref, FuncName2, Args2),
            resume_and_continue(Ref, StateRef2, CallbackResult2);
        FinalResult ->
            FinalResult
    end;
resume_and_continue(Ref, StateRef, {error, _} = Err) ->
    _ = py_nif:context_cancel_resume(Ref, StateRef),
    Err.

%% ============================================================================
%% Utility functions
%% ============================================================================

%% Callback results cross to Python as external term format (status byte 2)
%% and are decoded by term_to_py() in c_src/py_convert.c, the same
%% converter used for call arguments. The former Python-repr encoder was
%% removed in favour of it.
