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

%% @doc Erlang-native asyncio event loop manager.
%%
%% This module provides the high-level interface for using the Erlang-backed
%% asyncio event loop. It manages the lifecycle of event loops and routers,
%% and registers callback functions for Python to call.
%%
%% @private
-module(py_event_loop).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    stop/0,
    get_loop/0,
    register_callbacks/0,
    run_async/2,
    %% High-level async task API (uvloop-inspired)
    run/3, run/4,
    create_task/3, create_task/4,
    await/1, await/2,
    spawn_task/3, spawn_task/4,
    %% Per-process namespace API
    exec/1, exec/2,
    eval/1, eval/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    loop_ref :: reference() | undefined,
    worker_pid :: pid() | undefined,
    worker_id :: binary(),
    router_pid :: pid() | undefined
}).

%% ============================================================================
%% API
%% ============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

-spec get_loop() -> {ok, reference()} | {error, not_started}.
get_loop() ->
    gen_server:call(?MODULE, get_loop).

%% @doc Register event loop callbacks for Python access.
-spec register_callbacks() -> ok.
register_callbacks() ->
    %% Register all event loop functions as callbacks
    py_callback:register(py_event_loop_new, fun cb_event_loop_new/1),
    py_callback:register(py_event_loop_destroy, fun cb_event_loop_destroy/1),
    py_callback:register(py_event_loop_set_router, fun cb_event_loop_set_router/1),
    py_callback:register(py_event_loop_wakeup, fun cb_event_loop_wakeup/1),
    py_callback:register(py_event_loop_add_reader, fun cb_add_reader/1),
    py_callback:register(py_event_loop_remove_reader, fun cb_remove_reader/1),
    py_callback:register(py_event_loop_add_writer, fun cb_add_writer/1),
    py_callback:register(py_event_loop_remove_writer, fun cb_remove_writer/1),
    py_callback:register(py_event_loop_call_later, fun cb_call_later/1),
    py_callback:register(py_event_loop_cancel_timer, fun cb_cancel_timer/1),
    py_callback:register(py_event_loop_poll_events, fun cb_poll_events/1),
    py_callback:register(py_event_loop_get_pending, fun cb_get_pending/1),
    py_callback:register(py_event_loop_dispatch_callback, fun cb_dispatch_callback/1),
    py_callback:register(py_event_loop_dispatch_timer, fun cb_dispatch_timer/1),
    %% Sleep callback - suspends Erlang process, fully releasing dirty scheduler
    py_callback:register(<<"_py_sleep">>, fun cb_sleep/1),
    %% Execute Python callback - used by erlang.schedule_py() to call Python functions
    %% Args: [Module, Func, Args, Kwargs]
    py_callback:register(<<"_execute_py">>, fun cb_execute_py/1),
    ok.

%% @doc Run an async coroutine on the event loop.
%% The result will be sent to the caller via erlang.send().
%%
%% Request should be a map with the following keys:
%%   ref => reference() - A reference to identify the result
%%   caller => pid() - The pid to send the result to
%%   module => atom() | binary() - Python module name
%%   func => atom() | binary() - Python function name
%%   args => list() - Arguments to pass to the function
%%   kwargs => map() - Keyword arguments (optional)
%%
%% Returns ok immediately. The result will be sent as:
%%   {async_result, Ref, {ok, Result}} - on success
%%   {async_result, Ref, {error, Reason}} - on failure
-spec run_async(reference(), map()) -> ok | {error, term()}.
run_async(LoopRef, #{ref := Ref, caller := Caller, module := Module,
                     func := Func, args := Args} = Request) ->
    Kwargs = maps:get(kwargs, Request, #{}),
    ModuleBin = py_util:to_binary(Module),
    FuncBin = py_util:to_binary(Func),
    py_nif:event_loop_run_async(LoopRef, Caller, Ref, ModuleBin, FuncBin, Args, Kwargs).

%% ============================================================================
%% High-level Async Task API (uvloop-inspired)
%% ============================================================================

%% @doc Blocking run of an async Python function.
%%
%% Submits the task and waits for the result. Returns when the task completes
%% or when the timeout is reached.
%%
%% Example:
%%   {ok, Result} = py_event_loop:run(my_module, my_async_func, [arg1, arg2])
-spec run(Module :: atom() | binary(), Func :: atom() | binary(), Args :: list()) ->
    {ok, term()} | {error, term()}.
run(Module, Func, Args) ->
    run(Module, Func, Args, #{}).

-spec run(Module :: atom() | binary(), Func :: atom() | binary(),
          Args :: list(), Opts :: map()) -> {ok, term()} | {error, term()}.
run(Module, Func, Args, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Kwargs = maps:get(kwargs, Opts, #{}),
    Ref = create_task(Module, Func, Args, Kwargs),
    await(Ref, Timeout).

%% @doc Submit an async task and return a reference to await the result.
%%
%% Non-blocking: returns immediately with a reference that can be used
%% to await the result later. Uses the uvloop-inspired task queue for
%% thread-safe submission from any dirty scheduler.
%%
%% Example:
%%   Ref = py_event_loop:create_task(my_module, my_async_func, [arg1]),
%%   %% ... do other work ...
%%   {ok, Result} = py_event_loop:await(Ref)
-spec create_task(Module :: atom() | binary(), Func :: atom() | binary(),
                  Args :: list()) -> reference().
create_task(Module, Func, Args) ->
    create_task(Module, Func, Args, #{}).

-spec create_task(Module :: atom() | binary(), Func :: atom() | binary(),
                  Args :: list(), Kwargs :: map()) -> reference().
create_task(Module, Func, Args, Kwargs) ->
    {ok, LoopRef} = get_loop(),
    Ref = make_ref(),
    Caller = self(),
    ModuleBin = py_util:to_binary(Module),
    FuncBin = py_util:to_binary(Func),
    ok = py_nif:submit_task(LoopRef, Caller, Ref, ModuleBin, FuncBin, Args, Kwargs),
    Ref.

%% @doc Wait for an async task result.
%%
%% Blocks until the result is received or timeout is reached.
%%
%% Returns:
%%   {ok, Result} - Task completed successfully
%%   {error, Reason} - Task failed with error
%%   {error, timeout} - Timeout waiting for result
-spec await(Ref :: reference()) -> {ok, term()} | {error, term()}.
await(Ref) ->
    await(Ref, 5000).

-spec await(Ref :: reference(), Timeout :: non_neg_integer() | infinity) ->
    {ok, term()} | {error, term()}.
await(Ref, Timeout) ->
    receive
        {async_result, Ref, Result} -> Result
    after Timeout ->
        {error, timeout}
    end.

%% @doc Fire-and-forget task execution.
%%
%% Submits the task but does not wait for or return the result.
%% Useful for background tasks where you don't care about the outcome.
%%
%% Example:
%%   ok = py_event_loop:spawn_task(logger, log_event, [event_data])
-spec spawn_task(Module :: atom() | binary(), Func :: atom() | binary(),
                 Args :: list()) -> ok.
spawn_task(Module, Func, Args) ->
    spawn_task(Module, Func, Args, #{}).

-spec spawn_task(Module :: atom() | binary(), Func :: atom() | binary(),
                 Args :: list(), Kwargs :: map()) -> ok.
spawn_task(Module, Func, Args, Kwargs) ->
    {ok, LoopRef} = get_loop(),
    Ref = make_ref(),
    %% Spawn a process that will receive and discard the result
    Receiver = erlang:spawn(fun() ->
        receive
            {async_result, _, _} -> ok
        after 30000 ->
            %% Cleanup after 30 seconds if no response
            ok
        end
    end),
    ModuleBin = py_util:to_binary(Module),
    FuncBin = py_util:to_binary(Func),
    ok = py_nif:submit_task(LoopRef, Receiver, Ref, ModuleBin, FuncBin, Args, Kwargs),
    ok.

%% ============================================================================
%% Per-Process Namespace API
%% ============================================================================

%% @doc Execute Python code in the calling process's event loop namespace.
%%
%% Each Erlang process gets an isolated Python namespace (globals/locals)
%% for the event loop. Functions defined via exec/1 can be called via
%% create_task/3 with the `__main__' module.
%%
%% The namespace is automatically cleaned up when the process exits.
%%
%% Example:
%% <pre>
%% ok = py_event_loop:exec(&lt;&lt;"
%%     async def my_async_func(x):
%%         return x * 2
%% "&gt;&gt;),
%% Ref = py_event_loop:create_task('__main__', my_async_func, [21]),
%% {ok, 42} = py_event_loop:await(Ref)
%% </pre>
-spec exec(Code :: binary() | iolist()) -> ok | {error, term()}.
exec(Code) ->
    {ok, LoopRef} = get_loop(),
    exec(LoopRef, Code).

-spec exec(LoopRef :: reference(), Code :: binary() | iolist()) -> ok | {error, term()}.
exec(LoopRef, Code) ->
    py_nif:event_loop_exec(LoopRef, Code).

%% @doc Evaluate a Python expression in the calling process's namespace.
%%
%% Returns the result of evaluating the expression.
%%
%% Example:
%% <pre>
%% ok = py_event_loop:exec(&lt;&lt;"x = 42"&gt;&gt;),
%% {ok, 42} = py_event_loop:eval(&lt;&lt;"x"&gt;&gt;),
%% {ok, 84} = py_event_loop:eval(&lt;&lt;"x * 2"&gt;&gt;)
%% </pre>
-spec eval(Expr :: binary() | iolist()) -> {ok, term()} | {error, term()}.
eval(Expr) ->
    {ok, LoopRef} = get_loop(),
    eval(LoopRef, Expr).

-spec eval(LoopRef :: reference(), Expr :: binary() | iolist()) -> {ok, term()} | {error, term()}.
eval(LoopRef, Expr) ->
    py_nif:event_loop_eval(LoopRef, Expr).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init([]) ->
    %% Register callbacks on startup
    register_callbacks(),

    %% Set priv_dir for module imports in subinterpreters
    PrivDir = code:priv_dir(erlang_python),
    ok = py_nif:set_event_loop_priv_dir(PrivDir),

    %% Create and initialize the event loop immediately
    case py_nif:event_loop_new() of
        {ok, LoopRef} ->
            %% Scalable I/O model: use dedicated worker process
            WorkerId = <<"default">>,
            {ok, WorkerPid} = py_event_worker:start_link(WorkerId, LoopRef),
            ok = py_nif:event_loop_set_worker(LoopRef, WorkerPid),
            ok = py_nif:event_loop_set_id(LoopRef, WorkerId),

            %% Also start legacy router for backward compatibility
            {ok, RouterPid} = py_event_router:start_link(LoopRef),
            ok = py_nif:set_shared_router(RouterPid),

            %% Make the event loop available to Python
            ok = py_nif:set_python_event_loop(LoopRef),
            %% Set ErlangEventLoop as the default asyncio policy
            ok = set_default_policy(),
            {ok, #state{
                loop_ref = LoopRef,
                worker_pid = WorkerPid,
                worker_id = WorkerId,
                router_pid = RouterPid
            }};
        {error, Reason} ->
            {stop, {event_loop_init_failed, Reason}}
    end.

%% @doc Set ErlangEventLoop as the default asyncio event loop policy.
%% Also extends the C 'erlang' module with Python event loop exports.
set_default_policy() ->
    PrivDir = code:priv_dir(erlang_python),
    %% First, extend the erlang module with Python event loop exports
    extend_erlang_module(PrivDir),
    %% Then set the event loop policy
    Code = iolist_to_binary([
        "import sys\n",
        "priv_dir = '", PrivDir, "'\n",
        "if priv_dir not in sys.path:\n",
        "    sys.path.insert(0, priv_dir)\n",
        "from _erlang_impl import get_event_loop_policy\n",
        "import asyncio\n",
        "asyncio.set_event_loop_policy(get_event_loop_policy())\n"
    ]),
    case py:exec(Code) of
        ok -> ok;
        {error, Reason} ->
            error_logger:warning_msg("Failed to set ErlangEventLoop policy: ~p~n", [Reason]),
            ok  %% Non-fatal
    end.

%% @doc Extend the C 'erlang' module with Python event loop exports.
%% This makes erlang.run(), erlang.new_event_loop(), etc. available.
extend_erlang_module(PrivDir) ->
    Code = iolist_to_binary([
        "import erlang\n",
        "priv_dir = '", PrivDir, "'\n",
        "if hasattr(erlang, '_extend_erlang_module'):\n",
        "    erlang._extend_erlang_module(priv_dir)\n"
    ]),
    case py:exec(Code) of
        ok -> ok;
        {error, Reason} ->
            error_logger:warning_msg("Failed to extend erlang module: ~p~n", [Reason]),
            ok  %% Non-fatal
    end.

handle_call(get_loop, _From, #state{loop_ref = undefined} = State) ->
    %% Create event loop and worker on demand
    case py_nif:event_loop_new() of
        {ok, LoopRef} ->
            WorkerId = <<"default">>,
            {ok, WorkerPid} = py_event_worker:start_link(WorkerId, LoopRef),
            ok = py_nif:event_loop_set_worker(LoopRef, WorkerPid),
            ok = py_nif:event_loop_set_id(LoopRef, WorkerId),
            {ok, RouterPid} = py_event_router:start_link(LoopRef),
            ok = py_nif:set_python_event_loop(LoopRef),
            NewState = State#state{
                loop_ref = LoopRef,
                worker_pid = WorkerPid,
                worker_id = WorkerId,
                router_pid = RouterPid
            },
            {reply, {ok, LoopRef}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call(get_loop, _From, #state{loop_ref = LoopRef} = State) ->
    {reply, {ok, LoopRef}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{loop_ref = LoopRef, worker_pid = WorkerPid, router_pid = RouterPid}) ->
    %% Reset asyncio policy back to default before destroying the loop
    reset_default_policy(),
    %% Clean up worker (scalable I/O model)
    case WorkerPid of
        undefined -> ok;
        WPid -> py_event_worker:stop(WPid)
    end,
    %% Clean up legacy router
    case RouterPid of
        undefined -> ok;
        RPid -> py_event_router:stop(RPid)
    end,
    %% Clean up event loop
    case LoopRef of
        undefined -> ok;
        Ref -> py_nif:event_loop_destroy(Ref)
    end,
    ok.

%% @doc Reset asyncio back to the default event loop policy.
reset_default_policy() ->
    Code = <<"
import asyncio
asyncio.set_event_loop_policy(None)
">>,
    catch py:exec(Code),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ============================================================================
%% Callback implementations for Python
%% ============================================================================

cb_event_loop_new([]) ->
    py_nif:event_loop_new().

cb_event_loop_destroy([LoopRef]) ->
    py_nif:event_loop_destroy(LoopRef).

cb_event_loop_set_router([LoopRef, RouterPid]) ->
    py_nif:event_loop_set_router(LoopRef, RouterPid).

cb_event_loop_wakeup([LoopRef]) ->
    py_nif:event_loop_wakeup(LoopRef).

cb_add_reader([LoopRef, Fd, CallbackId]) ->
    py_nif:add_reader(LoopRef, Fd, CallbackId).

cb_remove_reader([LoopRef, FdRef]) ->
    py_nif:remove_reader(LoopRef, FdRef).

cb_add_writer([LoopRef, Fd, CallbackId]) ->
    py_nif:add_writer(LoopRef, Fd, CallbackId).

cb_remove_writer([LoopRef, FdRef]) ->
    py_nif:remove_writer(LoopRef, FdRef).

cb_call_later([LoopRef, DelayMs, CallbackId]) ->
    py_nif:call_later(LoopRef, DelayMs, CallbackId).

cb_cancel_timer([LoopRef, TimerRef]) ->
    py_nif:cancel_timer(LoopRef, TimerRef).

cb_poll_events([LoopRef, TimeoutMs]) ->
    py_nif:poll_events(LoopRef, TimeoutMs).

cb_get_pending([LoopRef]) ->
    py_nif:get_pending(LoopRef).

cb_dispatch_callback([LoopRef, CallbackId, Type]) ->
    py_nif:dispatch_callback(LoopRef, CallbackId, Type).

cb_dispatch_timer([LoopRef, CallbackId]) ->
    py_nif:dispatch_timer(LoopRef, CallbackId).

%% @doc Sleep callback for Python erlang.sleep().
%% Suspends the current Erlang process for the specified duration,
%% fully releasing the dirty NIF scheduler to handle other work.
%% This is true cooperative yielding - the dirty scheduler thread is freed.
%% Args: [Seconds] - number of seconds (converted to non-negative ms internally)
cb_sleep([Seconds]) when is_number(Seconds) ->
    Ms = max(0, round(Seconds * 1000)),
    receive after Ms -> ok end;
cb_sleep(_Args) ->
    ok.

%% @doc Execute Python callback for erlang.schedule_py().
%% Calls a Python function via the worker pool.
%% Args: [Module, Func, Args, Kwargs]
%% - Module: binary - Python module name
%% - Func: binary - Python function name
%% - Args: list | none - Positional arguments
%% - Kwargs: map | none - Keyword arguments
cb_execute_py([Module, Func, Args, Kwargs]) ->
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
    %% Use default pool via py:call
    case py:call(Module, Func, CallArgs, CallKwargs) of
        {ok, Result} -> Result;
        {error, Reason} -> error(Reason)
    end;
cb_execute_py(_Args) ->
    error({badarg, invalid_execute_py_args}).
