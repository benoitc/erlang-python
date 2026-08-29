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

%%% @doc The context process: one Python execution environment, one request
%%% at a time.
%%%
%%% Every mode goes through this module. In `worker' and `owngil' mode the
%%% process holds a NIF context resource, forwards each request to the
%%% context thread in C (`nif_context_call_async') and waits for its
%%% `{py_result, Ref, Result}'. In `isolated' mode `init/4' hands the
%%% process to `py_isolated', which speaks the same messages to a child OS
%%% process. Callers do not see the difference.
%%%
%%% == Callbacks ==
%%%
%%% When Python calls `erlang.call', the context thread blocks on the
%%% callback pipe and sends `{erlang_callback, Id, Fun, Args}' to this
%%% process, which runs the registered function and writes the reply frame
%%% back. Nested requests from the callback are served inline, so callbacks
%%% can call Python again to any depth.
%%%
%%% Owns: the public API, the reply protocol (`{MRef, Reply}', timeouts,
%%% interrupt on timeout) and the pid to NIF reference table.
%%% Talks to: `py_context_embedded' (the process body for worker and owngil
%%% mode), `py_isolated' (isolated mode), `py_nif' (interrupt).
%%% Never: runs Python on a scheduler thread; the context thread does.
%%%
%%% @end
-module(py_context).

-export([
    start_link/2,
    start_link/3,
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
    get_nif_ref/1,
    interrupt/1,
    start_loop/1,
    start_loop/2,
    stop_loop/1,
    stop_loop/2,
    loop_ref/1,
    submit/4,
    submit/5,
    submit_await/4,
    submit_await/5,
    submit_await/6
]).

%% Internal exports
-export([kill/1, pass_fd/2, child_info/1]).

-export([init/3, init/4, init_ref_tab/0]).
%% Used by py_context_embedded
-export([register_nif_ref/1, unregister_nif_ref/0]).


%% Maps context pid -> NIF context reference. Read by interrupt/1, which must
%% reach the NIF reference while the context process is blocked in a NIF and
%% therefore cannot answer get_nif_ref/1.
-define(REF_TAB, py_context_refs).

%% How long to wait for an interrupted call to unwind and reply, so the late
%% reply is drained instead of being left in the caller's mailbox.
-define(INTERRUPT_GRACE_MS, 1000).
%% Time given to a running loop to exit after interrupt/1 (also in
%% py_context_embedded, which drives the stop)
-define(LOOP_INTERRUPT_GRACE_MS, 3000).

-type context_mode() :: worker | owngil | isolated.
-type context() :: pid().

-export_type([context_mode/0, context/0]).




%% ============================================================================
%% API
%% ============================================================================

%% @doc Start a new py_context process.
%%
%% The process creates a Python context based on the mode:
%% - `worker' - Create a thread-state worker (main interpreter namespace)
%% - `owngil' - Create a sub-interpreter with its own GIL (Python 3.14+)
%% - `isolated' - Run CPython in a child OS process (see py_isolated)
%%
%% The `owngil' mode creates a dedicated pthread for each context, allowing
%% true parallel Python execution. Requires Python 3.14+. The `isolated'
%% mode gives failure isolation: the child can be killed, capped with
%% rlimits, and a crash in a C extension only takes the child down.
%%
%% @param Id Unique identifier for this context
%% @param Mode Context mode
%% @returns {ok, Pid} | {error, Reason}
-spec start_link(pos_integer(), context_mode()) -> {ok, pid()} | {error, term()}.
start_link(Id, Mode) ->
    start_link(Id, Mode, #{}).

%% @doc Start a new py_context process with options.
%%
%% See new/1 for the recognised options.
%%
%% @param Id Unique identifier for this context
%% @param Mode Context mode
%% @param Opts Options map
%% @returns {ok, Pid} | {error, Reason}
-spec start_link(pos_integer(), context_mode(), map()) ->
    {ok, pid()} | {error, term()}.
start_link(Id, Mode, Opts) when is_map(Opts) ->
    Parent = self(),
    Pid = proc_lib:spawn_link(fun() -> init(Parent, Id, Mode, Opts) end),
    receive
        {Pid, started} ->
            {ok, Pid};
        {Pid, {error, Reason}} ->
            {error, Reason}
    after start_timeout(Mode, Opts) ->
        exit(Pid, kill),
        _ = ets:member(?REF_TAB, Pid) andalso ets:delete(?REF_TAB, Pid),
        {error, timeout}
    end.

%% @private An isolated child has to spawn and connect; give it its
%% start_timeout plus a margin. Embedded contexts start in well under 5 s.
start_timeout(isolated, Opts) ->
    maps:get(start_timeout, Opts, 10000) + 2000;
start_timeout(_Mode, _Opts) ->
    5000.

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
%% - `mode' - Context mode (worker | owngil | isolated), default: worker
%% - `memory_limit' - Cap in bytes on memory allocated by this context.
%%   Requires `mode => owngil' and the runtime started with
%%   `enable_memory_limits'; see py_nif:context_set_memory_limit/2 for what
%%   is counted.
%%
%% @param Opts Options map
%% @returns {ok, Pid} | {error, Reason}
-spec new(map()) -> {ok, context()} | {error, term()}.
new(Opts) when is_map(Opts) ->
    Mode = maps:get(mode, Opts, worker),
    Id = erlang:unique_integer([positive]),
    start_link(Id, Mode, Opts).

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
    await_reply(Ctx, MRef, Timeout).

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
    await_reply(Ctx, MRef, Timeout).

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
    await_reply(Ctx, MRef, Timeout).

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
    await_reply(Ctx, MRef, Timeout).

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
    await_reply(Ctx, MRef, infinity).

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
    await_reply(Ctx, MRef, infinity).

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

%% @doc Interrupt Python code currently running in this context.
%%
%% Raises KeyboardInterrupt in the thread executing the context; the in-flight
%% call returns `{error, interrupted}'. Callable from any process, including
%% while the context process is blocked in a NIF.
%%
%% Returns `not_running' if the context is idle, unknown, or the exception
%% could not be delivered. Code blocked in a C call (`time.sleep', a numpy
%% kernel, a socket read) is only interrupted once that call returns.
%%
%% @param Ctx Context process
%% @returns ok | not_running
-spec interrupt(context()) -> ok | not_running.
interrupt(Ctx) when is_pid(Ctx) ->
    case lookup_nif_ref(Ctx) of
        {ok, isolated} ->
            %% The context process is never blocked in a NIF: ask it. It
            %% signals the child and arms the SIGKILL backstop.
            MRef = erlang:monitor(process, Ctx),
            Ctx ! {interrupt, self(), MRef},
            case await_ctrl_reply(Ctx, MRef, 5000) of
                ok -> ok;
                _ -> not_running
            end;
        {ok, Ref} ->
            try py_nif:context_interrupt(Ref) of
                ok -> ok;
                _ -> not_running
            catch
                _:_ -> not_running
            end;
        error ->
            not_running
    end.

%% @doc Kill the child process of an isolated context with SIGKILL.
%%
%% Total and immediate, whatever the child is doing (a C call, a numpy
%% kernel, a blocked read). In-flight calls return `{error, killed}'. With
%% `restart => true' (the default) a fresh child is started and the context
%% stays usable, with its Python state gone. Embedded contexts (`worker',
%% `owngil') cannot be killed: they return `{error, not_isolated}'.
%%
%% @param Ctx Context process
%% @returns ok | {error, not_isolated}
-spec kill(context()) -> ok | {error, not_isolated}.
kill(Ctx) when is_pid(Ctx) ->
    case lookup_nif_ref(Ctx) of
        {ok, isolated} ->
            MRef = erlang:monitor(process, Ctx),
            Ctx ! {kill, self(), MRef},
            await_ctrl_reply(Ctx, MRef, 5000);
        _ ->
            {error, not_isolated}
    end.

%% @doc Hand a file descriptor to the child of an isolated context.
%%
%% The fd is sent over the control socket (`SCM_RIGHTS') and the number it
%% got in the child is returned; use it with `erlang.server.serve' from a
%% submitted coroutine. Get the fd with `py:dup_fd/1' on a listening socket.
%%
%% @param Ctx Context process
%% @param Fd File descriptor in this VM
%% @returns {ok, ChildFd} | {error, Reason}
-spec pass_fd(context(), non_neg_integer()) -> {ok, non_neg_integer()} | {error, term()}.
pass_fd(Ctx, Fd) when is_pid(Ctx), is_integer(Fd) ->
    case lookup_nif_ref(Ctx) of
        {ok, isolated} ->
            MRef = erlang:monitor(process, Ctx),
            Ctx ! {pass_fd, self(), MRef, Fd},
            await_ctrl_reply(Ctx, MRef, 5000);
        _ ->
            {error, not_isolated}
    end.

%% @doc Information about the child of an isolated context: `os_pid',
%% `python_version', `executable', `platform'.
-spec child_info(context()) -> {ok, map()} | {error, term()}.
child_info(Ctx) when is_pid(Ctx) ->
    case lookup_nif_ref(Ctx) of
        {ok, isolated} ->
            MRef = erlang:monitor(process, Ctx),
            Ctx ! {child_info, self(), MRef},
            await_ctrl_reply(Ctx, MRef, 5000);
        _ ->
            {error, not_isolated}
    end.

%% @private Interrupt on behalf of a timed-out request. An isolated context
%% cancels that request only (interrupting the child if it is the one
%% executing, dropping it from the queue otherwise); embedded contexts can
%% only interrupt whatever runs.
interrupt_request(Ctx, ReqMRef) ->
    case lookup_nif_ref(Ctx) of
        {ok, isolated} ->
            Ctx ! {interrupt_request, ReqMRef},
            ok;
        _ ->
            interrupt(Ctx)
    end.

%% @private Create the pid -> NIF reference table. Called by the supervisor
%% before any context starts.
-spec init_ref_tab() -> ok.
init_ref_tab() ->
    case ets:whereis(?REF_TAB) of
        undefined ->
            ?REF_TAB = ets:new(?REF_TAB, [
                named_table, public, set, {read_concurrency, true}
            ]),
            ok;
        _ ->
            ok
    end.

%% @doc Run an ErlangEventLoop forever on the context's thread.
%%
%% Returns as soon as the loop is started. The loop keeps running until
%% stop_loop/1,2 or interrupt/1; the owner (the caller by default) receives
%% `{py_loop_exit, Ctx, Result}' when it ends, where Result is the return of
%% the exec that ran it (`ok', `{error, interrupted}', or a Python error).
%%
%% While the loop runs, call/eval/exec/call_method on this context return
%% `{error, loop_running}': the thread is busy in the loop, and a timed-out
%% call would interrupt it. Use submit/4,5 and submit_await/4,5,6 instead;
%% they inject coroutines into the running loop.
%%
%% Options:
%% - `owner' - pid that receives `{py_loop_exit, Ctx, Result}' (default: caller).
%%   If the owner dies the loop is stopped.
-spec start_loop(context()) -> ok | {error, term()}.
start_loop(Ctx) ->
    start_loop(Ctx, #{}).

-spec start_loop(context(), map()) -> ok | {error, term()}.
start_loop(Ctx, Opts) when is_pid(Ctx), is_map(Opts) ->
    Owner = maps:get(owner, Opts, self()),
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {start_loop, self(), MRef, Owner},
    await_ctrl_reply(Ctx, MRef, 15000).

%% @doc Stop a loop started with start_loop/1,2.
%%
%% Asks the loop to stop from inside (a coroutine calling `loop.stop()'),
%% then interrupts the thread if it has not exited after Grace ms (default
%% 5000). Returns `ok' once the loop has exited, `{error, no_loop}' when none
%% is running, `{error, timeout}' if it survived the interrupt too.
-spec stop_loop(context()) -> ok | {error, term()}.
stop_loop(Ctx) ->
    stop_loop(Ctx, 5000).

-spec stop_loop(context(), non_neg_integer()) -> ok | {error, term()}.
stop_loop(Ctx, GraceMs) when is_pid(Ctx), is_integer(GraceMs), GraceMs >= 0 ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {stop_loop, self(), MRef, GraceMs},
    await_ctrl_reply(Ctx, MRef, GraceMs + ?LOOP_INTERRUPT_GRACE_MS + 2000).

%% @doc Event loop reference of this context, usable with py_nif:submit_task/7
%% and py_event_loop:create_task/4.
%%
%% owngil contexts have their own loop; worker contexts share the main
%% interpreter's loop (py_event_loop:get_loop/0).
-spec loop_ref(context()) -> {ok, reference()} | {error, term()}.
loop_ref(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {loop_ref, self(), MRef},
    await_ctrl_reply(Ctx, MRef, 5000).

%% @doc Schedule `Module:Func(Args...)' on the context's event loop and
%% return at once with `{ok, TaskRef}'.
%%
%% Works whether or not start_loop/1 is active: with a running loop the
%% coroutine is injected into it, otherwise the event worker steps the loop.
%% The result arrives as `{async_result, TaskRef, {ok, Value} | {error, R}}';
%% use py_event_loop:await/1,2 or submit_await/4,5,6. Coroutine functions
%% are awaited, plain functions are called and their value returned.
%% Module must be importable in the context (sys.modules), so put entry
%% points in a module rather than in the exec namespace.
-spec submit(context(), atom() | binary(), atom() | binary(), list()) ->
    {ok, reference()} | {error, term()}.
submit(Ctx, Module, Func, Args) ->
    submit(Ctx, Module, Func, Args, #{}).

-spec submit(context(), atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, reference()} | {error, term()}.
submit(Ctx, Module, Func, Args, Kwargs) when is_pid(Ctx), is_list(Args), is_map(Kwargs) ->
    case lookup_nif_ref(Ctx) of
        {ok, isolated} ->
            TaskRef = make_ref(),
            MRef = erlang:monitor(process, Ctx),
            Ctx ! {submit, self(), MRef, TaskRef, to_binary(Module), to_binary(Func), Args, Kwargs},
            await_ctrl_reply(Ctx, MRef, 5000);
        _ ->
            submit_embedded(Ctx, Module, Func, Args, Kwargs)
    end.

%% @private
submit_embedded(Ctx, Module, Func, Args, Kwargs) ->
    case loop_ref(Ctx) of
        {ok, LoopRef} ->
            TaskRef = make_ref(),
            case py_nif:submit_task(LoopRef, self(), TaskRef,
                                    to_binary(Module), to_binary(Func), Args, Kwargs) of
                ok -> {ok, TaskRef};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc submit/4 followed by py_event_loop:await/2 (default timeout 5000 ms).
-spec submit_await(context(), atom() | binary(), atom() | binary(), list()) ->
    {ok, term()} | {error, term()}.
submit_await(Ctx, Module, Func, Args) ->
    submit_await(Ctx, Module, Func, Args, #{}, 5000).

-spec submit_await(context(), atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
submit_await(Ctx, Module, Func, Args, Kwargs) ->
    submit_await(Ctx, Module, Func, Args, Kwargs, 5000).

-spec submit_await(context(), atom() | binary(), atom() | binary(), list(), map(),
                   timeout()) -> {ok, term()} | {error, term()}.
submit_await(Ctx, Module, Func, Args, Kwargs, Timeout) ->
    case submit(Ctx, Module, Func, Args, Kwargs) of
        {ok, TaskRef} -> py_event_loop:await(TaskRef, Timeout);
        {error, _} = Error -> Error
    end.

%% ============================================================================
%% Internal functions
%% ============================================================================

%% @private Wait for a context reply, interrupting the running Python code if
%% the timeout expires.
%%
%% On timeout the Python side is interrupted and we wait a bounded grace period
%% for the unwinding call to reply, so the late reply is consumed here rather
%% than left behind in the caller's mailbox. The result is still
%% `{error, timeout}': the caller asked to stop waiting.
await_reply(Ctx, MRef, Timeout) ->
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        _ = interrupt_request(Ctx, MRef),
        receive
            {MRef, _Late} ->
                erlang:demonitor(MRef, [flush]);
            {'DOWN', MRef, process, Ctx, _} ->
                ok
        after ?INTERRUPT_GRACE_MS ->
            erlang:demonitor(MRef, [flush])
        end,
        {error, timeout}
    end.

%% @private
%% Reply wait for loop control messages: unlike await_reply/3 a timeout here
%% must not interrupt the context (it would kill the loop we are managing).
await_ctrl_reply(Ctx, MRef, Timeout) ->
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        %% An isolated context drops the pending entry so a late reply is
        %% not delivered to a caller that stopped waiting
        Ctx ! {cancel_ctrl, MRef},
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @private
register_nif_ref(Ref) ->
    try
        true = ets:insert(?REF_TAB, {self(), Ref}),
        ok
    catch
        error:badarg -> ok  %% table not created (library used without the app)
    end.

%% @private
unregister_nif_ref() ->
    try
        true = ets:delete(?REF_TAB, self()),
        ok
    catch
        error:badarg -> ok
    end.

%% @private
lookup_nif_ref(Ctx) ->
    try ets:lookup(?REF_TAB, Ctx) of
        [{Ctx, Ref}] -> {ok, Ref};
        [] -> error
    catch
        error:badarg -> error
    end.

%% @private
init(Parent, Id, Mode) ->
    init(Parent, Id, Mode, #{}).

%% @private
init(Parent, Id, isolated, Opts) ->
    py_isolated:init(Parent, Id, isolated, Opts);
init(Parent, Id, Mode, Opts) ->
    py_context_embedded:init(Parent, Id, Mode, Opts).

%% @private
to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.
