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

%%% @doc Python context process.
%%%
%%% A py_context process owns a Python context (subinterpreter or worker).
%%% Each process has exclusive access to its context, eliminating mutex
%%% contention and enabling true N-way parallelism.
%%%
%%% The context is created when the process starts and destroyed when it
%%% stops. All Python operations are serialized through message passing.
%%%
%%% == Callback Handling ==
%%%
%%% When Python code calls `erlang.call()`, the NIF returns a `{suspended, ...}`
%%% tuple instead of blocking. The context process handles the callback inline
%%% using a recursive receive pattern, enabling arbitrarily deep callback nesting.
%%%
%%% This approach is inspired by PyO3's suspension mechanism and avoids the
%%% deadlock issues that occur with separate callback handler processes.
%%%
%%% @end
-module(py_context).

-export([
    start_link/2,
    new/1,
    stop/1,
    destroy/1,
    call/4,
    call/5,
    call/6,
    call/7,
    eval/2,
    eval/3,
    eval/4,
    eval/5,
    exec/2,
    exec/3,
    call_method/4,
    to_term/1,
    get_interp_id/1,
    is_subinterp/1,
    create_local_env/1,
    get_nif_ref/1
]).

%% Internal exports
-export([init/3]).

%% Exported for py_reactor_context
-export([extend_erlang_module_in_context/1]).

-type context_mode() :: auto | subinterp | worker | owngil.
-type context() :: pid().

-export_type([context_mode/0, context/0]).

-record(state, {
    ref :: reference(),
    id :: pos_integer(),
    interp_id :: non_neg_integer(),
    event_state = #{} :: map(),  %% #{loop_ref => ref(), worker_pid => pid()}
    callback_handler :: pid() | undefined  %% For thread-model callback handling
}).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start a new py_context process.
%%
%% The process creates a Python context based on the mode:
%% - `auto' - Detect best mode (subinterp on Python 3.12+, worker otherwise)
%% - `subinterp' - Create a sub-interpreter with shared GIL (uses pool)
%% - `worker' - Create a thread-state worker (main interpreter namespace)
%% - `owngil' - Create a sub-interpreter with its own GIL (true parallelism)
%%
%% The `owngil' mode creates a dedicated pthread for each context, allowing
%% true parallel Python execution. This is useful for CPU-bound workloads.
%% Requires Python 3.12+.
%%
%% @param Id Unique identifier for this context
%% @param Mode Context mode
%% @returns {ok, Pid} | {error, Reason}
-spec start_link(pos_integer(), context_mode()) -> {ok, pid()} | {error, term()}.
start_link(Id, Mode) ->
    Parent = self(),
    Pid = spawn_link(fun() -> init(Parent, Id, Mode) end),
    receive
        {Pid, started} ->
            {ok, Pid};
        {Pid, {error, Reason}} ->
            {error, Reason}
    after 5000 ->
        exit(Pid, kill),
        {error, timeout}
    end.

%% @doc Stop a py_context process.
-spec stop(context()) -> ok.
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

%% @doc Create a new context with options map.
%%
%% Options:
%% - `mode' - Context mode (auto | subinterp | worker | owngil), default: auto
%%
%% @param Opts Options map
%% @returns {ok, Pid} | {error, Reason}
-spec new(map()) -> {ok, context()} | {error, term()}.
new(Opts) when is_map(Opts) ->
    Mode = maps:get(mode, Opts, auto),
    Id = erlang:unique_integer([positive]),
    start_link(Id, Mode).

%% @doc Alias for stop/1 for API consistency.
-spec destroy(context()) -> ok.
destroy(Ctx) ->
    stop(Ctx).

%% @doc Call a Python function with empty kwargs.
%%
%% This is a convenience wrapper for call/5 that defaults Kwargs to #{}.
%%
%% @param Ctx Context process
%% @param Module Python module name
%% @param Func Function name
%% @param Args List of arguments
%% @returns {ok, Result} | {error, Reason}
-spec call(context(), atom() | binary(), atom() | binary(), list()) ->
    {ok, term()} | {error, term()}.
call(Ctx, Module, Func, Args) ->
    call(Ctx, Module, Func, Args, #{}).

%% @doc Call a Python function.
%%
%% @param Ctx Context process
%% @param Module Python module name
%% @param Func Function name
%% @param Args List of arguments
%% @param Kwargs Map of keyword arguments
%% @returns {ok, Result} | {error, Reason}
-spec call(context(), atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
call(Ctx, Module, Func, Args, Kwargs) ->
    call(Ctx, Module, Func, Args, Kwargs, infinity).

%% @doc Call a Python function with timeout.
-spec call(context(), atom() | binary(), atom() | binary(), list(), map(),
           timeout()) -> {ok, term()} | {error, term()}.
call(Ctx, Module, Func, Args, Kwargs, Timeout) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    Ctx ! {call, self(), MRef, ModuleBin, FuncBin, Args, Kwargs},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Call a Python function with a process-local environment.
%%
%% @param Ctx Context process
%% @param Module Python module name
%% @param Func Function name
%% @param Args List of arguments
%% @param Kwargs Map of keyword arguments
%% @param Timeout Timeout in milliseconds
%% @param EnvRef Process-local environment reference
%% @returns {ok, Result} | {error, Reason}
-spec call(context(), atom() | binary(), atom() | binary(), list(), map(),
           timeout(), reference()) -> {ok, term()} | {error, term()}.
call(Ctx, Module, Func, Args, Kwargs, Timeout, EnvRef) when is_pid(Ctx), is_reference(EnvRef) ->
    MRef = erlang:monitor(process, Ctx),
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    Ctx ! {call, self(), MRef, ModuleBin, FuncBin, Args, Kwargs, EnvRef},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Evaluate a Python expression with empty locals.
%%
%% This is a convenience wrapper for eval/3 that defaults Locals to #{}.
%%
%% @param Ctx Context process
%% @param Code Python code to evaluate
%% @returns {ok, Result} | {error, Reason}
-spec eval(context(), binary() | string()) ->
    {ok, term()} | {error, term()}.
eval(Ctx, Code) ->
    eval(Ctx, Code, #{}).

%% @doc Evaluate a Python expression.
%%
%% @param Ctx Context process
%% @param Code Python code to evaluate
%% @param Locals Map of local variables
%% @returns {ok, Result} | {error, Reason}
-spec eval(context(), binary() | string(), map()) ->
    {ok, term()} | {error, term()}.
eval(Ctx, Code, Locals) ->
    eval(Ctx, Code, Locals, infinity).

%% @doc Evaluate a Python expression with timeout.
-spec eval(context(), binary() | string(), map(), timeout()) ->
    {ok, term()} | {error, term()}.
eval(Ctx, Code, Locals, Timeout) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    CodeBin = to_binary(Code),
    Ctx ! {eval, self(), MRef, CodeBin, Locals},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Evaluate a Python expression with a process-local environment.
%%
%% @param Ctx Context process
%% @param Code Python code to evaluate
%% @param Locals Map of local variables
%% @param Timeout Timeout in milliseconds
%% @param EnvRef Process-local environment reference
%% @returns {ok, Result} | {error, Reason}
-spec eval(context(), binary() | string(), map(), timeout(), reference()) ->
    {ok, term()} | {error, term()}.
eval(Ctx, Code, Locals, Timeout, EnvRef) when is_pid(Ctx), is_reference(EnvRef) ->
    MRef = erlang:monitor(process, Ctx),
    CodeBin = to_binary(Code),
    Ctx ! {eval, self(), MRef, CodeBin, Locals, EnvRef},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Execute Python statements.
%%
%% @param Ctx Context process
%% @param Code Python code to execute
%% @returns ok | {error, Reason}
-spec exec(context(), binary() | string()) -> ok | {error, term()}.
exec(Ctx, Code) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    CodeBin = to_binary(Code),
    Ctx ! {exec, self(), MRef, CodeBin},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after infinity ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Execute Python statements with a process-local environment.
%%
%% @param Ctx Context process
%% @param Code Python code to execute
%% @param EnvRef Process-local environment reference
%% @returns ok | {error, Reason}
-spec exec(context(), binary() | string(), reference()) -> ok | {error, term()}.
exec(Ctx, Code, EnvRef) when is_pid(Ctx), is_reference(EnvRef) ->
    MRef = erlang:monitor(process, Ctx),
    CodeBin = to_binary(Code),
    Ctx ! {exec, self(), MRef, CodeBin, EnvRef},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after infinity ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Call a method on a Python object reference.
-spec call_method(context(), reference(), atom() | binary(), list()) ->
    {ok, term()} | {error, term()}.
call_method(Ctx, Ref, Method, Args) when is_pid(Ctx), is_reference(Ref) ->
    MRef = erlang:monitor(process, Ctx),
    MethodBin = to_binary(Method),
    Ctx ! {call_method, self(), MRef, Ref, MethodBin, Args},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    end.

%% @doc Convert a Python object reference to an Erlang term.
-spec to_term(reference()) -> {ok, term()} | {error, term()}.
to_term(Ref) when is_reference(Ref) ->
    %% This uses the ref's embedded interp_id to route automatically
    py_nif:context_to_term(Ref).

%% @doc Get the interpreter ID for this context.
-spec get_interp_id(context()) -> {ok, non_neg_integer()} | {error, term()}.
get_interp_id(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {get_interp_id, self(), MRef},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    end.

%% @doc Check if this context is a subinterpreter.
%%
%% Returns true for subinterpreter mode, false for worker mode.
%% In worker mode, process-local environments are used.
%% In subinterpreter mode, each context has its own isolated namespace.
-spec is_subinterp(context()) -> boolean().
is_subinterp(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {is_subinterp, self(), MRef},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, _Reason} ->
            false
    end.

%% @doc Create a process-local Python environment for this context.
%%
%% The environment is created inside the context's interpreter to ensure
%% the correct memory allocator is used. This is critical for subinterpreters
%% where each interpreter has its own memory allocator.
%%
%% The returned EnvRef should be stored in the calling process's dictionary,
%% keyed by interpreter ID.
-spec create_local_env(context()) -> {ok, reference()} | {error, term()}.
create_local_env(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {create_local_env, self(), MRef},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    end.

%% @doc Get the NIF context reference from a context process.
%% This is useful for calling low-level py_nif functions directly.
-spec get_nif_ref(context()) -> reference().
get_nif_ref(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {get_nif_ref, self(), MRef},
    receive
        {MRef, Ref} ->
            erlang:demonitor(MRef, [flush]),
            Ref;
        {'DOWN', MRef, process, Ctx, Reason} ->
            error({context_died, Reason})
    end.

%% ============================================================================
%% Internal functions
%% ============================================================================

%% @private
init(Parent, Id, Mode) ->
    process_flag(trap_exit, true),
    case create_context(Mode) of
        {ok, Ref, InterpId} ->
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
            loop(State);
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

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

%% @private
create_context(auto) ->
    case py_nif:subinterp_supported() of
        true -> create_context(subinterp);
        false -> create_context(worker)
    end;
create_context(subinterp) ->
    py_nif:context_create(subinterp);
create_context(worker) ->
    py_nif:context_create(worker);
create_context(owngil) ->
    %% OWN_GIL mode requires Python 3.12+
    case py_nif:subinterp_supported() of
        true -> py_nif:context_create(owngil);
        false -> {error, owngil_requires_python312}
    end.

%% @private
%% Main context loop. Handles requests and uses suspension-based callback support.
loop(#state{ref = Ref, interp_id = InterpId} = State) ->
    receive
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
            Result = py_nif:context_exec(Ref, Code),
            From ! {MRef, Result},
            loop(State);

        %% Exec with process-local environment (worker mode)
        {exec, From, MRef, Code, EnvRef} ->
            Result = py_nif:context_exec(Ref, Code, EnvRef),
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

%% @private Clean up resources on termination
terminate(_Reason, #state{ref = Ref, event_state = EventState, callback_handler = CallbackHandler}) ->
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
            catch gen_server:stop(WorkerPid, normal, 5000);
        _ ->
            ok
    end,
    %% Destroy the Python context
    catch py_nif:context_destroy(Ref),
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
            %% Format: status_byte (0=ok) + python_repr
            ResultStr = term_to_python_repr(Result),
            <<0, ResultStr/binary>>;
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
    case py_nif:context_call(Ref, Module, Func, Args, Kwargs) of
        {suspended, _CallbackId, StateRef, {FuncName, CallbackArgs}} ->
            %% Callback needed - handle it with recursive receive
            CallbackResult = handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs),
            %% Resume and potentially get more suspensions
            resume_and_continue(Ref, StateRef, CallbackResult);
        {schedule, CallbackName, CallbackArgs} ->
            %% Schedule marker: Python returned erlang.schedule()
            %% Execute the callback and return its result
            handle_schedule(Ref, CallbackName, CallbackArgs);
        Result ->
            Result
    end.

%% @private
%% Handle eval with potential suspension for callbacks
handle_eval_with_suspension(Ref, Code, Locals) ->
    case py_nif:context_eval(Ref, Code, Locals) of
        {suspended, _CallbackId, StateRef, {FuncName, CallbackArgs}} ->
            %% Callback needed - handle it with recursive receive
            CallbackResult = handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs),
            %% Resume and potentially get more suspensions
            resume_and_continue(Ref, StateRef, CallbackResult);
        {schedule, CallbackName, CallbackArgs} ->
            %% Schedule marker: Python returned erlang.schedule()
            %% Execute the callback and return its result
            handle_schedule(Ref, CallbackName, CallbackArgs);
        Result ->
            Result
    end.

%% @private
%% Handle call with process-local environment
handle_call_with_suspension_and_env(Ref, Module, Func, Args, Kwargs, EnvRef) ->
    case py_nif:context_call(Ref, Module, Func, Args, Kwargs, EnvRef) of
        {suspended, _CallbackId, StateRef, {FuncName, CallbackArgs}} ->
            CallbackResult = handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs),
            resume_and_continue(Ref, StateRef, CallbackResult);
        {schedule, CallbackName, CallbackArgs} ->
            handle_schedule(Ref, CallbackName, CallbackArgs);
        Result ->
            Result
    end.

%% @private
%% Handle eval with process-local environment
handle_eval_with_suspension_and_env(Ref, Code, Locals, EnvRef) ->
    case py_nif:context_eval(Ref, Code, Locals, EnvRef) of
        {suspended, _CallbackId, StateRef, {FuncName, CallbackArgs}} ->
            CallbackResult = handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs),
            resume_and_continue(Ref, StateRef, CallbackResult);
        {schedule, CallbackName, CallbackArgs} ->
            handle_schedule(Ref, CallbackName, CallbackArgs);
        Result ->
            Result
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
                    ReprStr = term_to_python_repr(Value),
                    {ok, <<0, ReprStr/binary>>};
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

%% @private
%% Convert Erlang term to Python repr string
term_to_python_repr(Term) when is_integer(Term) ->
    integer_to_binary(Term);
term_to_python_repr(Term) when is_float(Term) ->
    float_to_binary(Term, [{decimals, 15}, compact]);
term_to_python_repr(true) ->
    <<"True">>;
term_to_python_repr(false) ->
    <<"False">>;
term_to_python_repr(none) ->
    <<"None">>;
term_to_python_repr(nil) ->
    <<"None">>;
term_to_python_repr(undefined) ->
    <<"None">>;
term_to_python_repr(Term) when is_atom(Term) ->
    %% Convert atom to Python string
    BinStr = atom_to_binary(Term, utf8),
    <<"'", BinStr/binary, "'">>;
term_to_python_repr(Term) when is_binary(Term) ->
    %% Escape the binary for Python
    Escaped = binary:replace(Term, <<"'">>, <<"\\'">>, [global]),
    <<"'", Escaped/binary, "'">>;
term_to_python_repr(Term) when is_list(Term) ->
    case io_lib:printable_unicode_list(Term) of
        true ->
            %% It's a string
            Bin = unicode:characters_to_binary(Term),
            Escaped = binary:replace(Bin, <<"'">>, <<"\\'">>, [global]),
            <<"'", Escaped/binary, "'">>;
        false ->
            %% It's a list
            Items = [term_to_python_repr(E) || E <- Term],
            ItemsBin = join_binaries(Items, <<", ">>),
            <<"[", ItemsBin/binary, "]">>
    end;
term_to_python_repr(Term) when is_tuple(Term) ->
    Items = [term_to_python_repr(E) || E <- tuple_to_list(Term)],
    ItemsBin = join_binaries(Items, <<", ">>),
    case tuple_size(Term) of
        1 -> <<"(", ItemsBin/binary, ",)">>;
        _ -> <<"(", ItemsBin/binary, ")">>
    end;
term_to_python_repr(Term) when is_map(Term) ->
    Items = maps:fold(fun(K, V, Acc) ->
        KeyRepr = term_to_python_repr(K),
        ValRepr = term_to_python_repr(V),
        [<<KeyRepr/binary, ": ", ValRepr/binary>> | Acc]
    end, [], Term),
    ItemsBin = join_binaries(lists:reverse(Items), <<", ">>),
    <<"{", ItemsBin/binary, "}">>;
term_to_python_repr(Term) when is_pid(Term) ->
    %% Encode PID using ETF (Erlang Term Format) for exact reconstruction.
    %% Format: "__etf__:<base64_encoded_binary>"
    %% The C side will detect this, base64 decode, and use enif_binary_to_term
    %% to reconstruct the pid, then convert to ErlangPidObject.
    Etf = term_to_binary(Term),
    B64 = base64:encode(Etf),
    <<"\"__etf__:", B64/binary, "\"">>;
term_to_python_repr(Term) when is_reference(Term) ->
    %% References also need ETF encoding for round-trip
    Etf = term_to_binary(Term),
    B64 = base64:encode(Etf),
    <<"\"__etf__:", B64/binary, "\"">>;
term_to_python_repr(_Term) ->
    <<"None">>.

%% @private
join_binaries([], _Sep) -> <<>>;
join_binaries([H], _Sep) -> H;
join_binaries([H|T], Sep) ->
    lists:foldl(fun(B, Acc) -> <<Acc/binary, Sep/binary, B/binary>> end, H, T).

%% @private
to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.
