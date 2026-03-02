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

%%% @doc Low-level NIF wrapper for Python integration.
%%%
%%% This module provides the direct NIF interface. Most users should use
%%% the higher-level `py' module instead.
%%%
%%% @private
-module(py_nif).

-export([
    init/0,
    init/1,
    finalize/0,
    worker_new/0,
    worker_new/1,
    worker_destroy/1,
    worker_call/5,
    worker_call/6,
    worker_eval/3,
    worker_eval/4,
    worker_exec/2,
    worker_next/2,
    import_module/2,
    get_attr/3,
    version/0,
    memory_stats/0,
    gc/0,
    gc/1,
    tracemalloc_start/0,
    tracemalloc_start/1,
    tracemalloc_stop/0,
    set_callback_handler/2,
    send_callback_response/2,
    resume_callback/2,
    %% Async workers
    async_worker_new/0,
    async_worker_destroy/1,
    async_call/6,
    async_gather/3,
    async_stream/6,
    %% Sub-interpreters (Python 3.12+)
    subinterp_supported/0,
    subinterp_worker_new/0,
    subinterp_worker_destroy/1,
    subinterp_call/5,
    subinterp_asgi_run/6,
    parallel_execute/2,
    %% Execution mode info
    execution_mode/0,
    num_executors/0,
    %% Thread worker support (ThreadPoolExecutor)
    thread_worker_set_coordinator/1,
    thread_worker_write/2,
    thread_worker_signal_ready/1,
    %% Async callback support (for erlang.async_call)
    async_callback_response/3,
    %% Callback name registry (prevents torch introspection issues)
    register_callback_name/1,
    unregister_callback_name/1,
    %% Logging and tracing
    set_log_receiver/2,
    clear_log_receiver/0,
    set_trace_receiver/1,
    clear_trace_receiver/0,
    %% Erlang-native event loop (for asyncio integration)
    event_loop_new/0,
    event_loop_destroy/1,
    event_loop_set_router/2,
    event_loop_set_worker/2,
    event_loop_set_id/2,
    event_loop_wakeup/1,
    event_loop_run_async/7,
    add_reader/3,
    remove_reader/2,
    add_writer/3,
    remove_writer/2,
    call_later/3,
    cancel_timer/2,
    poll_events/2,
    get_pending/1,
    dispatch_callback/3,
    dispatch_timer/2,
    dispatch_sleep_complete/2,
    get_fd_callback_id/2,
    reselect_reader/2,
    reselect_writer/2,
    reselect_reader_fd/1,
    reselect_writer_fd/1,
    %% FD lifecycle management (uvloop-like API)
    handle_fd_event/2,
    handle_fd_event_and_reselect/2,
    stop_reader/1,
    start_reader/1,
    stop_writer/1,
    start_writer/1,
    cancel_reader/2,  %% Legacy alias for stop_reader
    cancel_writer/2,  %% Legacy alias for stop_writer
    close_fd/1,
    %% Test helpers for fd monitoring (using pipes)
    create_test_pipe/0,
    close_test_fd/1,
    write_test_fd/2,
    read_test_fd/2,
    %% TCP test helpers
    create_test_tcp_listener/1,
    accept_test_tcp/1,
    connect_test_tcp/2,
    %% UDP test helpers
    create_test_udp_socket/1,
    recvfrom_test_udp/2,
    sendto_test_udp/4,
    set_udp_broadcast/2,
    %% Python event loop integration
    set_python_event_loop/1,
    set_isolation_mode/1,
    set_shared_router/1,
    %% ASGI optimizations
    asgi_build_scope/1,
    asgi_run/5,
    %% ASGI profiling (only available when compiled with -DASGI_PROFILING)
    asgi_profile_stats/0,
    asgi_profile_reset/0,
    %% WSGI optimizations
    wsgi_run/4,
    %% Worker pool
    pool_start/1,
    pool_stop/0,
    pool_submit/5,
    pool_stats/0,
    %% Process-per-context API (no mutex)
    context_create/1,
    context_destroy/1,
    context_call/5,
    context_eval/3,
    context_exec/2,
    context_call_method/4,
    context_to_term/1,
    context_interp_id/1,
    context_set_callback_handler/2,
    context_get_callback_pipe/1,
    context_write_callback_response/2,
    context_resume/3,
    context_cancel_resume/2,
    %% py_ref API (Python object references with interp_id)
    ref_wrap/2,
    is_ref/1,
    ref_interp_id/1,
    ref_to_term/1,
    ref_getattr/2,
    ref_call_method/3,
    %% Reactor NIFs - Erlang-as-Reactor architecture
    reactor_register_fd/3,
    reactor_reselect_read/1,
    reactor_select_write/1,
    get_fd_from_resource/1,
    reactor_on_read_ready/2,
    reactor_on_write_ready/2,
    reactor_init_connection/3,
    reactor_close_fd/1
]).

-on_load(load_nif/0).

-define(NIF_STUB, erlang:nif_error(nif_not_loaded)).

%%% ============================================================================
%%% NIF Loading
%%% ============================================================================

load_nif() ->
    PrivDir = case code:priv_dir(erlang_python) of
        {error, bad_name} ->
            %% Fallback for development
            case code:which(?MODULE) of
                Filename when is_list(Filename) ->
                    filename:join([filename:dirname(Filename), "..", "priv"]);
                _ ->
                    "priv"
            end;
        Dir ->
            Dir
    end,
    NifPath = filename:join(PrivDir, "py_nif"),
    erlang:load_nif(NifPath, 0).

%%% ============================================================================
%%% Initialization
%%% ============================================================================

%% @doc Initialize the Python interpreter.
%% Must be called before any other functions.
%% Usually called automatically by the application.
-spec init() -> ok | {error, term()}.
init() ->
    init(#{}).

%% @doc Initialize with options.
%% Options:
%%   python_home => string() - Python installation directory
%%   python_path => [string()] - Additional module search paths
%%   isolated => boolean() - Run in isolated mode (default: false)
-spec init(map()) -> ok | {error, term()}.
init(_Opts) ->
    ?NIF_STUB.

%% @doc Finalize the Python interpreter.
%% Call this when shutting down. After this, no Python calls can be made.
-spec finalize() -> ok.
finalize() ->
    ?NIF_STUB.

%%% ============================================================================
%%% Worker Management
%%% ============================================================================

%% @doc Create a new Python worker context.
%% Returns an opaque reference to be used with other worker functions.
-spec worker_new() -> {ok, reference()} | {error, term()}.
worker_new() ->
    worker_new(#{}).

%% @doc Create a worker with options.
%% Options:
%%   use_subinterpreter => boolean() - Use a separate sub-interpreter (Python 3.12+)
-spec worker_new(map()) -> {ok, reference()} | {error, term()}.
worker_new(_Opts) ->
    ?NIF_STUB.

%% @doc Destroy a worker context.
-spec worker_destroy(reference()) -> ok.
worker_destroy(_WorkerRef) ->
    ?NIF_STUB.

%% @doc Call a Python function from a worker.
%% This is a dirty NIF that acquires the GIL.
%% May return {suspended, ...} if Python calls erlang.call() (reentrant callback).
-spec worker_call(reference(), binary(), binary(), list(), map()) ->
    {ok, term()} | {error, term()} | {suspended, term(), reference(), {binary(), term()}}.
worker_call(_WorkerRef, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Call a Python function from a worker with timeout.
%% May return {suspended, ...} if Python calls erlang.call() (reentrant callback).
-spec worker_call(reference(), binary(), binary(), list(), map(), non_neg_integer()) ->
    {ok, term()} | {error, term()} | {suspended, term(), reference(), {binary(), term()}}.
worker_call(_WorkerRef, _Module, _Func, _Args, _Kwargs, _TimeoutMs) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in a worker.
%% May return {suspended, ...} if Python calls erlang.call() (reentrant callback).
-spec worker_eval(reference(), binary(), map()) ->
    {ok, term()} | {error, term()} | {suspended, term(), reference(), {binary(), term()}}.
worker_eval(_WorkerRef, _Code, _Locals) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in a worker with timeout.
%% May return {suspended, ...} if Python calls erlang.call() (reentrant callback).
-spec worker_eval(reference(), binary(), map(), non_neg_integer()) ->
    {ok, term()} | {error, term()} | {suspended, term(), reference(), {binary(), term()}}.
worker_eval(_WorkerRef, _Code, _Locals, _TimeoutMs) ->
    ?NIF_STUB.

%% @doc Execute Python statements in a worker.
-spec worker_exec(reference(), binary()) -> ok | {error, term()}.
worker_exec(_WorkerRef, _Code) ->
    ?NIF_STUB.

%% @doc Get next item from a generator/iterator.
%% Returns {ok, Value} | {error, stop_iteration} | {error, Error}
-spec worker_next(reference(), reference()) -> {ok, term()} | {error, term()}.
worker_next(_WorkerRef, _GeneratorRef) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Module Operations
%%% ============================================================================

%% @doc Import a Python module in a worker context.
-spec import_module(reference(), binary()) -> {ok, reference()} | {error, term()}.
import_module(_WorkerRef, _ModuleName) ->
    ?NIF_STUB.

%% @doc Get an attribute from a Python object.
-spec get_attr(reference(), reference(), binary()) -> {ok, term()} | {error, term()}.
get_attr(_WorkerRef, _ObjRef, _AttrName) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Info
%%% ============================================================================

%% @doc Get Python version info.
-spec version() -> {ok, binary()} | {error, term()}.
version() ->
    ?NIF_STUB.

%%% ============================================================================
%%% Memory and GC
%%% ============================================================================

%% @doc Get Python memory statistics.
%% Returns a map with gc_stats, gc_count, gc_threshold, and optionally
%% traced_memory_current and traced_memory_peak if tracemalloc is enabled.
-spec memory_stats() -> {ok, map()} | {error, term()}.
memory_stats() ->
    ?NIF_STUB.

%% @doc Force Python garbage collection.
%% Returns the number of unreachable objects collected.
-spec gc() -> {ok, integer()} | {error, term()}.
gc() ->
    ?NIF_STUB.

%% @doc Force garbage collection of a specific generation.
%% Generation: 0, 1, or 2 (default 2 = full collection).
-spec gc(0..2) -> {ok, integer()} | {error, term()}.
gc(_Generation) ->
    ?NIF_STUB.

%% @doc Start memory tracing with tracemalloc.
%% This allows tracking memory allocations.
-spec tracemalloc_start() -> ok | {error, term()}.
tracemalloc_start() ->
    ?NIF_STUB.

%% @doc Start memory tracing with specified number of frames.
-spec tracemalloc_start(pos_integer()) -> ok | {error, term()}.
tracemalloc_start(_NFrame) ->
    ?NIF_STUB.

%% @doc Stop memory tracing.
-spec tracemalloc_stop() -> ok | {error, term()}.
tracemalloc_stop() ->
    ?NIF_STUB.

%%% ============================================================================
%%% Callback Support
%%% ============================================================================

%% @doc Set callback handler process for a worker.
%% Returns {ok, Fd} where Fd is the file descriptor for sending responses.
-spec set_callback_handler(reference(), pid()) -> {ok, integer()} | {error, term()}.
set_callback_handler(_WorkerRef, _HandlerPid) ->
    ?NIF_STUB.

%% @doc Send a callback response to a worker via file descriptor.
-spec send_callback_response(integer(), binary()) -> ok | {error, term()}.
send_callback_response(_Fd, _Response) ->
    ?NIF_STUB.

%% @doc Resume a suspended Python callback with the result.
%% StateRef is the reference returned in the {suspended, ...} tuple.
%% Result is the callback result as a binary (status byte + data).
%% Returns {ok, FinalResult}, {error, Reason}, or another {suspended, ...} for nested callbacks.
-spec resume_callback(reference(), binary()) ->
    {ok, term()} | {error, term()} | {suspended, term(), reference(), {binary(), term()}}.
resume_callback(_StateRef, _Result) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Async Worker Support
%%% ============================================================================

%% @doc Create a new async worker with background event loop.
%% Returns an opaque reference to be used with async functions.
-spec async_worker_new() -> {ok, reference()} | {error, term()}.
async_worker_new() ->
    ?NIF_STUB.

%% @doc Destroy an async worker.
-spec async_worker_destroy(reference()) -> ok.
async_worker_destroy(_WorkerRef) ->
    ?NIF_STUB.

%% @doc Submit an async call to the event loop.
%% Args: AsyncWorkerRef, Module, Func, Args, Kwargs, CallerPid
%% Returns: {ok, AsyncId} | {ok, {immediate, Result}} | {error, term()}
-spec async_call(reference(), binary(), binary(), list(), map(), pid()) ->
    {ok, non_neg_integer() | {immediate, term()}} | {error, term()}.
async_call(_WorkerRef, _Module, _Func, _Args, _Kwargs, _CallerPid) ->
    ?NIF_STUB.

%% @doc Execute multiple async calls concurrently using asyncio.gather.
%% Args: AsyncWorkerRef, CallsList (list of {Module, Func, Args}), CallerPid
%% Returns: {ok, AsyncId} | {ok, {immediate, Results}} | {error, term()}
-spec async_gather(reference(), [{binary(), binary(), list()}], pid()) ->
    {ok, non_neg_integer() | {immediate, list()}} | {error, term()}.
async_gather(_WorkerRef, _Calls, _CallerPid) ->
    ?NIF_STUB.

%% @doc Stream from an async generator.
%% Args: AsyncWorkerRef, Module, Func, Args, Kwargs, CallerPid
%% Returns: {ok, AsyncId} | {error, term()}
-spec async_stream(reference(), binary(), binary(), list(), map(), pid()) ->
    {ok, non_neg_integer()} | {error, term()}.
async_stream(_WorkerRef, _Module, _Func, _Args, _Kwargs, _CallerPid) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Sub-interpreter Support (Python 3.12+)
%%% ============================================================================

%% @doc Check if sub-interpreters with per-interpreter GIL are supported.
%% Returns true on Python 3.12+, false otherwise.
-spec subinterp_supported() -> boolean().
subinterp_supported() ->
    ?NIF_STUB.

%% @doc Create a new sub-interpreter worker with its own GIL.
%% Returns an opaque reference to be used with subinterp functions.
-spec subinterp_worker_new() -> {ok, reference()} | {error, term()}.
subinterp_worker_new() ->
    ?NIF_STUB.

%% @doc Destroy a sub-interpreter worker.
-spec subinterp_worker_destroy(reference()) -> ok | {error, term()}.
subinterp_worker_destroy(_WorkerRef) ->
    ?NIF_STUB.

%% @doc Call a Python function in a sub-interpreter.
%% Args: WorkerRef, Module (binary), Func (binary), Args (list), Kwargs (map)
-spec subinterp_call(reference(), binary(), binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
subinterp_call(_WorkerRef, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Run an ASGI application in a sub-interpreter.
%% This runs ASGI in a subinterpreter with its own GIL for true parallelism.
%% Args: WorkerRef, Runner (binary), Module (binary), Callable (binary), Scope (map), Body (binary)
-spec subinterp_asgi_run(reference(), binary(), binary(), binary(), map(), binary()) ->
    {ok, {integer(), [{binary(), binary()}], binary()}} | {error, term()}.
subinterp_asgi_run(_WorkerRef, _Runner, _Module, _Callable, _Scope, _Body) ->
    ?NIF_STUB.

%% @doc Execute multiple calls in parallel across sub-interpreters.
%% Args: WorkerRefs (list of refs), Calls (list of {Module, Func, Args})
%% Returns: List of results (one per call)
-spec parallel_execute([reference()], [{binary(), binary(), list()}]) ->
    {ok, list()} | {error, term()}.
parallel_execute(_WorkerRefs, _Calls) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Execution Mode Info
%%% ============================================================================

%% @doc Get the current execution mode.
%% Returns one of: free_threaded | subinterp | multi_executor
%% - free_threaded: Python 3.13+ with no GIL (Py_GIL_DISABLED)
%% - subinterp: Python 3.12+ with per-interpreter GIL
%% - multi_executor: Traditional Python with N executor threads
-spec execution_mode() -> free_threaded | subinterp | multi_executor.
execution_mode() ->
    ?NIF_STUB.

%% @doc Get the number of executor threads.
%% For multi_executor mode, this is the number of executor threads.
%% For other modes, returns 1.
-spec num_executors() -> pos_integer().
num_executors() ->
    ?NIF_STUB.

%%% ============================================================================
%%% Thread Worker Support (ThreadPoolExecutor)
%%% ============================================================================

%% @doc Set the thread worker coordinator process.
%% This process receives spawn and callback messages from Python threads
%% spawned via ThreadPoolExecutor.
-spec thread_worker_set_coordinator(pid()) -> ok | {error, term()}.
thread_worker_set_coordinator(_Pid) ->
    ?NIF_STUB.

%% @doc Write a callback response to a thread worker's pipe.
%% Fd is the write end of the response pipe.
%% Response is the result binary (status byte + python repr).
-spec thread_worker_write(integer(), binary()) -> ok | {error, term()}.
thread_worker_write(_Fd, _Response) ->
    ?NIF_STUB.

%% @doc Signal that a thread worker handler is ready.
%% Writes a zero-length response to the pipe to indicate readiness.
-spec thread_worker_signal_ready(integer()) -> ok | {error, term()}.
thread_worker_signal_ready(_Fd) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Async Callback Support (for erlang.async_call)
%%% ============================================================================

%% @doc Write an async callback response to the async callback pipe.
%% Fd is the write end of the async callback pipe.
%% CallbackId is the unique identifier for this callback.
%% Response is the result binary (status byte + encoded result).
%%
%% This is called when an async_callback message is processed.
%% The response is written in the format:
%%   callback_id (8 bytes) + response_len (4 bytes) + response_data
-spec async_callback_response(integer(), non_neg_integer(), binary()) ->
    ok | {error, term()}.
async_callback_response(_Fd, _CallbackId, _Response) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Callback Name Registry
%%% ============================================================================

%% @doc Register a callback name in the C-side registry.
%% This allows the Python erlang module's __getattr__ to return
%% ErlangFunction wrappers only for registered callbacks, preventing
%% introspection issues with libraries like torch.
-spec register_callback_name(atom() | binary()) -> ok | {error, term()}.
register_callback_name(_Name) ->
    ?NIF_STUB.

%% @doc Unregister a callback name from the C-side registry.
-spec unregister_callback_name(atom() | binary()) -> ok.
unregister_callback_name(_Name) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Logging and Tracing
%%% ============================================================================

%% @doc Set the log receiver process and minimum level threshold.
%% Level is the Python levelno (0=DEBUG to 50=CRITICAL).
-spec set_log_receiver(pid(), integer()) -> ok | {error, term()}.
set_log_receiver(_Pid, _Level) ->
    ?NIF_STUB.

%% @doc Clear the log receiver - log messages will be dropped.
-spec clear_log_receiver() -> ok.
clear_log_receiver() ->
    ?NIF_STUB.

%% @doc Set the trace receiver process for span events.
-spec set_trace_receiver(pid()) -> ok | {error, term()}.
set_trace_receiver(_Pid) ->
    ?NIF_STUB.

%% @doc Clear the trace receiver - trace events will be dropped.
-spec clear_trace_receiver() -> ok.
clear_trace_receiver() ->
    ?NIF_STUB.

%%% ============================================================================
%%% Erlang-native Event Loop (asyncio integration)
%%% ============================================================================

%% @doc Create a new Erlang-backed asyncio event loop.
%% Returns an opaque reference to be used with event loop functions.
-spec event_loop_new() -> {ok, reference()} | {error, term()}.
event_loop_new() ->
    ?NIF_STUB.

%% @doc Destroy an event loop.
-spec event_loop_destroy(reference()) -> ok | {error, term()}.
event_loop_destroy(_LoopRef) ->
    ?NIF_STUB.

%% @doc Set the router process for an event loop (legacy).
%% The router receives enif_select messages and timer events.
-spec event_loop_set_router(reference(), pid()) -> ok | {error, term()}.
event_loop_set_router(_LoopRef, _RouterPid) ->
    ?NIF_STUB.

%% @doc Set the worker process for an event loop (scalable I/O model).
%% The worker receives FD events and timers directly.
-spec event_loop_set_worker(reference(), pid()) -> ok | {error, term()}.
event_loop_set_worker(_LoopRef, _WorkerPid) ->
    ?NIF_STUB.

%% @doc Set the loop identifier for multi-loop routing.
-spec event_loop_set_id(reference(), binary() | atom()) -> ok | {error, term()}.
event_loop_set_id(_LoopRef, _LoopId) ->
    ?NIF_STUB.

%% @doc Wake up an event loop from a wait.
-spec event_loop_wakeup(reference()) -> ok | {error, term()}.
event_loop_wakeup(_LoopRef) ->
    ?NIF_STUB.

%% @doc Submit an async coroutine to run on the event loop.
%% When the coroutine completes, the result is sent to CallerPid via erlang.send().
%% This replaces the pthread+usleep polling model with direct message passing.
-spec event_loop_run_async(reference(), pid(), reference(), binary(), binary(), list(), map()) ->
    ok | {error, term()}.
event_loop_run_async(_LoopRef, _CallerPid, _Ref, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Register a file descriptor for read monitoring.
%% Uses enif_select to register with the Erlang scheduler.
-spec add_reader(reference(), integer(), non_neg_integer()) ->
    {ok, reference()} | {error, term()}.
add_reader(_LoopRef, _Fd, _CallbackId) ->
    ?NIF_STUB.

%% @doc Stop monitoring a file descriptor for reads.
%% FdRef must be the same resource returned by add_reader.
-spec remove_reader(reference(), reference()) -> ok | {error, term()}.
remove_reader(_LoopRef, _FdRef) ->
    ?NIF_STUB.

%% @doc Register a file descriptor for write monitoring.
-spec add_writer(reference(), integer(), non_neg_integer()) ->
    {ok, reference()} | {error, term()}.
add_writer(_LoopRef, _Fd, _CallbackId) ->
    ?NIF_STUB.

%% @doc Stop monitoring a file descriptor for writes.
%% FdRef must be the same resource returned by add_writer.
-spec remove_writer(reference(), reference()) -> ok | {error, term()}.
remove_writer(_LoopRef, _FdRef) ->
    ?NIF_STUB.

%% @doc Schedule a timer callback.
%% DelayMs is the delay in milliseconds.
-spec call_later(reference(), integer(), non_neg_integer()) ->
    {ok, reference()} | {error, term()}.
call_later(_LoopRef, _DelayMs, _CallbackId) ->
    ?NIF_STUB.

%% @doc Cancel a pending timer.
-spec cancel_timer(reference(), reference()) -> ok | {error, term()}.
cancel_timer(_LoopRef, _TimerRef) ->
    ?NIF_STUB.

%% @doc Wait for events with timeout.
%% Returns the number of pending events.
%% This is a dirty NIF that releases the GIL while waiting.
-spec poll_events(reference(), integer()) -> {ok, integer()} | {error, term()}.
poll_events(_LoopRef, _TimeoutMs) ->
    ?NIF_STUB.

%% @doc Get list of pending events.
%% Returns [{CallbackId, Type}] where Type is read | write | timer.
-spec get_pending(reference()) -> [{non_neg_integer(), read | write | timer}].
get_pending(_LoopRef) ->
    ?NIF_STUB.

%% @doc Dispatch a callback from the router.
%% Called when an fd becomes ready.
-spec dispatch_callback(reference(), non_neg_integer(), read | write | timer) -> ok.
dispatch_callback(_LoopRef, _CallbackId, _Type) ->
    ?NIF_STUB.

%% @doc Dispatch a timer callback.
%% Called when a timer expires.
-spec dispatch_timer(reference(), non_neg_integer()) -> ok.
dispatch_timer(_LoopRef, _CallbackId) ->
    ?NIF_STUB.

%% @doc Signal that a synchronous sleep has completed.
%% Called from Erlang when a sleep timer expires.
-spec dispatch_sleep_complete(reference(), non_neg_integer()) -> ok.
dispatch_sleep_complete(_LoopRef, _SleepId) ->
    ?NIF_STUB.

%% @doc Get callback ID from an fd resource.
%% Type is read or write.
-spec get_fd_callback_id(reference(), read | write) -> non_neg_integer() | undefined.
get_fd_callback_id(_FdRes, _Type) ->
    ?NIF_STUB.

%% @doc Re-register an fd resource for read monitoring.
%% Called after an event is delivered since enif_select is one-shot.
-spec reselect_reader(reference(), reference()) -> ok | {error, term()}.
reselect_reader(_LoopRef, _FdRes) ->
    ?NIF_STUB.

%% @doc Re-register an fd resource for write monitoring.
%% Called after an event is delivered since enif_select is one-shot.
-spec reselect_writer(reference(), reference()) -> ok | {error, term()}.
reselect_writer(_LoopRef, _FdRes) ->
    ?NIF_STUB.

%% @doc Re-register an fd resource for read monitoring using fd_res->loop.
%% This variant doesn't require LoopRef since the fd resource already
%% has a back-reference to its parent loop.
-spec reselect_reader_fd(reference()) -> ok | {error, term()}.
reselect_reader_fd(_FdRes) ->
    ?NIF_STUB.

%% @doc Re-register an fd resource for write monitoring using fd_res->loop.
%% This variant doesn't require LoopRef since the fd resource already
%% has a back-reference to its parent loop.
-spec reselect_writer_fd(reference()) -> ok | {error, term()}.
reselect_writer_fd(_FdRes) ->
    ?NIF_STUB.

%%% ============================================================================
%%% FD Lifecycle Management (uvloop-like API)
%%% ============================================================================

%% @doc Handle a select event (dispatch + auto-reselect).
%% Called by py_event_router when receiving {select, FdRes, Ref, ready_input/output}.
%% This combines get_fd_callback_id + dispatch_callback + reselect into one NIF call.
%% Type: read | write
-spec handle_fd_event(reference(), read | write) -> ok | {error, term()}.
handle_fd_event(_FdRef, _Type) ->
    ?NIF_STUB.

%% @doc Handle FD event and immediately reselect for next event.
%% Combined operation that eliminates one roundtrip - dispatch and reselect in one NIF call.
%% Type: read | write
-spec handle_fd_event_and_reselect(reference(), read | write) -> ok | {error, term()}.
handle_fd_event_and_reselect(_FdRef, _Type) ->
    ?NIF_STUB.

%% @doc Stop/pause read monitoring without closing the FD.
%% The watcher still exists and can be restarted with start_reader.
-spec stop_reader(reference()) -> ok | {error, term()}.
stop_reader(_FdRef) ->
    ?NIF_STUB.

%% @doc Start/resume read monitoring on an existing watcher.
%% Must have been created with add_reader first.
-spec start_reader(reference()) -> ok | {error, term()}.
start_reader(_FdRef) ->
    ?NIF_STUB.

%% @doc Stop/pause write monitoring without closing the FD.
%% The watcher still exists and can be restarted with start_writer.
-spec stop_writer(reference()) -> ok | {error, term()}.
stop_writer(_FdRef) ->
    ?NIF_STUB.

%% @doc Start/resume write monitoring on an existing watcher.
%% Must have been created with add_writer first.
-spec start_writer(reference()) -> ok | {error, term()}.
start_writer(_FdRef) ->
    ?NIF_STUB.

%% @doc Cancel read monitoring (legacy alias for stop_reader).
%% Kept for backward compatibility.
-spec cancel_reader(reference(), reference()) -> ok | {error, term()}.
cancel_reader(_LoopRef, _FdRef) ->
    ?NIF_STUB.

%% @doc Cancel write monitoring (legacy alias for stop_writer).
%% Kept for backward compatibility.
-spec cancel_writer(reference(), reference()) -> ok | {error, term()}.
cancel_writer(_LoopRef, _FdRef) ->
    ?NIF_STUB.

%% @doc Explicitly close an FD with proper lifecycle cleanup.
%% Transfers ownership and triggers proper cleanup via ERL_NIF_SELECT_STOP.
%% Safe to call multiple times (idempotent).
-spec close_fd(reference()) -> ok | {error, term()}.
close_fd(_FdRef) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Test Helper Functions (for fd monitoring tests using pipes)
%%% ============================================================================

%% @doc Create a pipe for testing fd monitoring.
%% Returns {ReadFd, WriteFd} where both are non-blocking.
-spec create_test_pipe() -> {ok, {integer(), integer()}} | {error, term()}.
create_test_pipe() ->
    ?NIF_STUB.

%% @doc Close a test file descriptor.
-spec close_test_fd(integer()) -> ok | {error, term()}.
close_test_fd(_Fd) ->
    ?NIF_STUB.

%% @doc Write binary data to a test file descriptor.
-spec write_test_fd(integer(), binary()) -> ok | {error, term()}.
write_test_fd(_Fd, _Data) ->
    ?NIF_STUB.

%% @doc Read data from a test file descriptor.
-spec read_test_fd(integer(), integer()) -> {ok, binary()} | {error, term()}.
read_test_fd(_Fd, _MaxSize) ->
    ?NIF_STUB.

%%% ============================================================================
%%% TCP Test Helper Functions
%%% ============================================================================

%% @doc Create a TCP listener socket for testing.
%% Port 0 will cause the OS to assign an ephemeral port.
%% Returns {ListenFd, ActualPort}.
-spec create_test_tcp_listener(integer()) -> {ok, {integer(), integer()}} | {error, term()}.
create_test_tcp_listener(_Port) ->
    ?NIF_STUB.

%% @doc Accept a connection on a TCP listener socket.
%% Returns the client socket fd.
-spec accept_test_tcp(integer()) -> {ok, integer()} | {error, term()}.
accept_test_tcp(_ListenFd) ->
    ?NIF_STUB.

%% @doc Connect to a TCP server.
%% Host should be a binary like `&lt;&lt;"127.0.0.1"&gt;&gt;'.
-spec connect_test_tcp(binary(), integer()) -> {ok, integer()} | {error, term()}.
connect_test_tcp(_Host, _Port) ->
    ?NIF_STUB.

%%% ============================================================================
%%% UDP Test Helper Functions
%%% ============================================================================

%% @doc Create a UDP socket for testing.
%% Port 0 will cause the OS to assign an ephemeral port.
%% Returns {Fd, ActualPort}.
-spec create_test_udp_socket(integer()) -> {ok, {integer(), integer()}} | {error, term()}.
create_test_udp_socket(_Port) ->
    ?NIF_STUB.

%% @doc Receive data from a UDP socket.
%% Returns the data and source address: {Data, {Host, Port}}.
-spec recvfrom_test_udp(integer(), integer()) -> {ok, {binary(), {binary(), integer()}}} | {error, term()}.
recvfrom_test_udp(_Fd, _MaxSize) ->
    ?NIF_STUB.

%% @doc Send data to a UDP destination address.
%% Host should be a binary like `&lt;&lt;"127.0.0.1"&gt;&gt;'.
-spec sendto_test_udp(integer(), binary(), binary(), integer()) -> ok | {error, term()}.
sendto_test_udp(_Fd, _Data, _Host, _Port) ->
    ?NIF_STUB.

%% @doc Enable or disable SO_BROADCAST on a UDP socket.
-spec set_udp_broadcast(integer(), boolean()) -> ok | {error, term()}.
set_udp_broadcast(_Fd, _Enable) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Python Event Loop Integration
%%% ============================================================================

%% @doc Set the global Python event loop.
%% This makes the event loop available to Python asyncio code.
-spec set_python_event_loop(reference()) -> ok | {error, term()}.
set_python_event_loop(_LoopRef) ->
    ?NIF_STUB.

%% @doc Set the event loop isolation mode.
%% - global: all ErlangEventLoop instances share the global loop (default)
%% - per_loop: each ErlangEventLoop creates its own isolated native loop
-spec set_isolation_mode(global | per_loop) -> ok | {error, term()}.
set_isolation_mode(_Mode) ->
    ?NIF_STUB.

%% @doc Set the shared router PID for per-loop created loops.
%% All loops created via _loop_new() in Python will use this router
%% for FD monitoring and timer operations.
-spec set_shared_router(pid()) -> ok | {error, term()}.
set_shared_router(_RouterPid) ->
    ?NIF_STUB.

%%% ============================================================================
%%% ASGI Optimizations
%%% ============================================================================

%% @doc Build an optimized ASGI scope dict from an Erlang map.
%%
%% This function converts an Erlang map to a Python dict using
%% interned keys and cached constant values for maximum performance.
%% The returned reference wraps a Python dict object.
%%
%% Example scope map:
%% ```
%% #{
%%   type => <<"http">>,
%%   asgi => #{version => <<"3.0">>, spec_version => <<"2.3">>},
%%   http_version => <<"1.1">>,
%%   method => <<"GET">>,
%%   scheme => <<"http">>,
%%   path => <<"/">>,
%%   raw_path => <<"/">>,
%%   query_string => <<>>,
%%   root_path => <<>>,
%%   headers => [[<<"host">>, <<"localhost">>]],
%%   server => {<<"localhost">>, 8080},
%%   client => {<<"127.0.0.1">>, 54321},
%%   state => #{}
%% }
%% '''
%%
%% @param ScopeMap Erlang map containing ASGI scope keys
%% @returns {ok, ScopeRef} where ScopeRef is a wrapped Python dict,
%%          or {error, Reason}
-spec asgi_build_scope(map()) -> {ok, reference()} | {error, term()}.
asgi_build_scope(_ScopeMap) ->
    ?NIF_STUB.

%% @doc Execute an ASGI application with optimized marshalling.
%%
%% This is a direct NIF that bypasses the generic py:call() path:
%% <ul>
%% <li>Builds scope dict using interned keys</li>
%% <li>Uses response pooling</li>
%% <li>Runs the ASGI app coroutine synchronously</li>
%% </ul>
%%
%% Requires the hornbeam_asgi_runner Python module to be available,
%% which provides the _run_asgi_sync function that handles the
%% ASGI receive/send protocol.
%%
%% @param Runner Python runner module name
%% @param Module Application module name
%% @param Callable ASGI callable name
%% @param ScopeMap Erlang map containing ASGI scope (see asgi_build_scope/1)
%% @param Body Request body as binary
%% @returns {ok, {Status, Headers, Body}} on success, or {error, Reason}
-spec asgi_run(binary(), binary(), binary(), map(), binary()) ->
    {ok, {integer(), [{binary(), binary()}], binary()}} | {error, term()}.
asgi_run(_Runner, _Module, _Callable, _ScopeMap, _Body) ->
    ?NIF_STUB.

%% @doc Get ASGI profiling statistics.
%%
%% Only available when NIF is compiled with -DASGI_PROFILING.
%% Returns timing breakdown for each phase of ASGI request handling:
%% - gil_acquire_us: Time to acquire Python GIL
%% - string_conv_us: Time to convert binary strings
%% - module_import_us: Time to import Python module
%% - get_callable_us: Time to get ASGI callable
%% - scope_build_us: Time to build scope dict
%% - body_conv_us: Time to convert body binary
%% - runner_import_us: Time to import runner module
%% - runner_call_us: Time to call the runner (includes Python ASGI execution)
%% - response_extract_us: Time to extract response
%% - gil_release_us: Time to release GIL
%% - total_us: Total time
%%
%% @returns {ok, StatsMap} or {error, not_available} if profiling not enabled
-spec asgi_profile_stats() -> {ok, map()} | {error, term()}.
asgi_profile_stats() ->
    {error, profiling_not_enabled}.

%% @doc Reset ASGI profiling statistics.
%%
%% Only available when NIF is compiled with -DASGI_PROFILING.
-spec asgi_profile_reset() -> ok | {error, term()}.
asgi_profile_reset() ->
    {error, profiling_not_enabled}.

%%% ============================================================================
%%% WSGI Optimizations
%%% ============================================================================

%% @doc Execute a WSGI application with optimized marshalling.
%%
%% This is a direct NIF that bypasses the generic py:call() path:
%% <ul>
%% <li>Builds environ dict using interned keys</li>
%% <li>Uses cached constant values</li>
%% <li>Runs the WSGI app synchronously</li>
%% </ul>
%%
%% Requires the hornbeam_wsgi_runner Python module to be available,
%% which provides the _run_wsgi_sync function that handles the
%% WSGI start_response protocol.
%%
%% @param Runner Python runner module name
%% @param Module Application module name
%% @param Callable WSGI callable name
%% @param EnvironMap Erlang map containing WSGI environ
%% @returns {ok, {Status, Headers, Body}} on success, or {error, Reason}
-spec wsgi_run(binary(), binary(), binary(), map()) ->
    {ok, {binary(), [{binary(), binary()}], binary()}} | {error, term()}.
wsgi_run(_Runner, _Module, _Callable, _EnvironMap) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Worker Pool
%%% ============================================================================

%% @doc Start the worker pool with the specified number of workers.
%%
%% Creates a pool of worker threads that process Python operations.
%% Each worker may have its own subinterpreter (Python 3.12+) for true
%% parallelism, or share the GIL with optimized batching.
%%
%% If NumWorkers is 0, the pool will use the number of CPU cores.
%%
%% @param NumWorkers Number of worker threads (0 = auto-detect)
%% @returns ok on success, or {error, Reason}
-spec pool_start(non_neg_integer()) -> ok | {error, term()}.
pool_start(_NumWorkers) ->
    ?NIF_STUB.

%% @doc Stop the worker pool.
%%
%% Signals all workers to shut down and waits for them to terminate.
%% Any pending requests will receive {error, pool_shutdown}.
%%
%% @returns ok
-spec pool_stop() -> ok.
pool_stop() ->
    ?NIF_STUB.

%% @doc Submit a request to the worker pool.
%%
%% Submits an asynchronous request to the pool. The caller will receive
%% a {py_response, RequestId, Result} message when the request completes.
%%
%% Request types and arguments:
%% <ul>
%% <li>`call' - Module, Func, Args, undefined (or Timeout)</li>
%% <li>`apply' - Module, Func, Args, Kwargs</li>
%% <li>`eval' - Code, Locals, undefined, undefined</li>
%% <li>`exec' - Code, undefined, undefined, undefined</li>
%% <li>`asgi' - Runner, Module, Callable, {Scope, Body}</li>
%% <li>`wsgi' - Module, Callable, Environ, undefined</li>
%% </ul>
%%
%% @param Type Request type atom
%% @param Arg1 First argument (varies by type)
%% @param Arg2 Second argument (varies by type)
%% @param Arg3 Third argument (varies by type)
%% @param Arg4 Fourth argument (varies by type)
%% @returns {ok, RequestId} on success, or {error, Reason}
-spec pool_submit(atom(), term(), term(), term(), term()) ->
    {ok, non_neg_integer()} | {error, term()}.
pool_submit(_Type, _Arg1, _Arg2, _Arg3, _Arg4) ->
    ?NIF_STUB.

%% @doc Get worker pool statistics.
%%
%% Returns a map with the following keys:
%% <ul>
%% <li>`num_workers' - Number of worker threads</li>
%% <li>`initialized' - Whether the pool is started</li>
%% <li>`use_subinterpreters' - Whether using subinterpreters (Python 3.12+)</li>
%% <li>`free_threaded' - Whether using free-threaded Python (3.13+)</li>
%% <li>`pending_count' - Number of pending requests in queue</li>
%% <li>`total_enqueued' - Total requests submitted</li>
%% </ul>
%%
%% @returns Stats map
-spec pool_stats() -> map().
pool_stats() ->
    ?NIF_STUB.

%%% ============================================================================
%%% Process-per-context API (no mutex)
%%%
%%% These NIFs are designed for the process-per-context architecture where
%%% each Erlang process owns one Python context. Since access is serialized
%%% by the owning process, no mutex locking is needed.
%%% ============================================================================

%% @doc Create a new Python context.
%%
%% Creates a subinterpreter (Python 3.12+) or worker thread-state based
%% on the mode parameter. Returns a reference to the context and its
%% interpreter ID for routing.
%%
%% @param Mode `subinterp' or `worker'
%% @returns {ok, ContextRef, InterpId} | {error, Reason}
-spec context_create(subinterp | worker) ->
    {ok, reference(), non_neg_integer()} | {error, term()}.
context_create(_Mode) ->
    ?NIF_STUB.

%% @doc Destroy a Python context.
%%
%% Cleans up the Python interpreter or thread-state. Should only be
%% called by the owning process.
%%
%% @param ContextRef Reference returned by context_create/1
%% @returns ok
-spec context_destroy(reference()) -> ok.
context_destroy(_ContextRef) ->
    ?NIF_STUB.

%% @doc Call a Python function in a context.
%%
%% NO MUTEX - caller must ensure exclusive access (process ownership).
%%
%% @param ContextRef Context reference
%% @param Module Python module name
%% @param Func Function name
%% @param Args List of arguments
%% @param Kwargs Map of keyword arguments
%% @returns {ok, Result} | {error, Reason}
-spec context_call(reference(), binary(), binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
context_call(_ContextRef, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in a context.
%%
%% NO MUTEX - caller must ensure exclusive access (process ownership).
%%
%% @param ContextRef Context reference
%% @param Code Python code to evaluate
%% @param Locals Map of local variables
%% @returns {ok, Result} | {error, Reason}
-spec context_eval(reference(), binary(), map()) ->
    {ok, term()} | {error, term()}.
context_eval(_ContextRef, _Code, _Locals) ->
    ?NIF_STUB.

%% @doc Execute Python statements in a context.
%%
%% NO MUTEX - caller must ensure exclusive access (process ownership).
%%
%% @param ContextRef Context reference
%% @param Code Python code to execute
%% @returns ok | {error, Reason}
-spec context_exec(reference(), binary()) -> ok | {error, term()}.
context_exec(_ContextRef, _Code) ->
    ?NIF_STUB.

%% @doc Call a method on a Python object in a context.
%%
%% NO MUTEX - caller must ensure exclusive access (process ownership).
%%
%% @param ContextRef Context reference
%% @param ObjRef Python object reference
%% @param Method Method name
%% @param Args List of arguments
%% @returns {ok, Result} | {error, Reason}
-spec context_call_method(reference(), reference(), binary(), list()) ->
    {ok, term()} | {error, term()}.
context_call_method(_ContextRef, _ObjRef, _Method, _Args) ->
    ?NIF_STUB.

%% @doc Convert a Python object reference to an Erlang term.
%%
%% The reference carries the interpreter ID, allowing automatic routing
%% to the correct context.
%%
%% @param ObjRef Python object reference
%% @returns {ok, Term} | {error, Reason}
-spec context_to_term(reference()) -> {ok, term()} | {error, term()}.
context_to_term(_ObjRef) ->
    ?NIF_STUB.

%% @doc Get the interpreter ID from a context reference.
%%
%% @param ContextRef Context reference
%% @returns InterpId
-spec context_interp_id(reference()) -> non_neg_integer().
context_interp_id(_ContextRef) ->
    ?NIF_STUB.

%% @doc Set the callback handler pid for a context.
%%
%% This must be called before the context can handle erlang.call() callbacks.
%%
%% @param ContextRef Context reference
%% @param Pid Erlang pid to handle callbacks
%% @returns ok | {error, Reason}
-spec context_set_callback_handler(reference(), pid()) -> ok | {error, term()}.
context_set_callback_handler(_ContextRef, _Pid) ->
    ?NIF_STUB.

%% @doc Get the callback pipe write FD for a context.
%%
%% Returns the write end of the callback pipe for sending responses.
%%
%% @param ContextRef Context reference
%% @returns {ok, WriteFd} | {error, Reason}
-spec context_get_callback_pipe(reference()) -> {ok, integer()} | {error, term()}.
context_get_callback_pipe(_ContextRef) ->
    ?NIF_STUB.

%% @doc Write a callback response to the context's pipe.
%%
%% Writes a length-prefixed binary response that Python will read.
%%
%% @param ContextRef Context reference
%% @param Data Binary data to write
%% @returns ok | {error, Reason}
-spec context_write_callback_response(reference(), binary()) -> ok | {error, term()}.
context_write_callback_response(_ContextRef, _Data) ->
    ?NIF_STUB.

%% @doc Resume a suspended context with callback result.
%%
%% After handling a callback, call this to resume Python execution with
%% the callback result. May return {suspended, ...} if Python makes another
%% erlang.call() during resume (nested callback).
%%
%% @param ContextRef Context reference
%% @param StateRef Suspended state reference from {suspended, _, StateRef, _}
%% @param Result Binary result to return to Python (format: status_byte + repr)
%% @returns {ok, Result} | {error, Reason} | {suspended, CallbackId, StateRef, {FuncName, Args}}
-spec context_resume(reference(), reference(), binary()) ->
    {ok, term()} | {error, term()} | {suspended, non_neg_integer(), reference(), {binary(), tuple()}}.
context_resume(_ContextRef, _StateRef, _Result) ->
    ?NIF_STUB.

%% @doc Cancel a suspended context resume (cleanup on error).
%%
%% Called when callback execution fails and resume won't be called.
%% Allows proper cleanup of the suspended state.
%%
%% @param ContextRef Context reference
%% @param StateRef Suspended state reference
%% @returns ok
-spec context_cancel_resume(reference(), reference()) -> ok.
context_cancel_resume(_ContextRef, _StateRef) ->
    ?NIF_STUB.

%%% ============================================================================
%%% py_ref API (Python object references with interp_id)
%%%
%%% These functions work with py_ref resources that carry both a Python
%%% object reference and the interpreter ID that created it. This enables
%%% automatic routing of method calls and attribute access.
%%% ============================================================================

%% @doc Wrap a Python object as a py_ref with interp_id.
%%
%% @param ContextRef Context that owns the object
%% @param PyObj Python object reference
%% @returns {ok, RefTerm} | {error, Reason}
-spec ref_wrap(reference(), reference()) -> {ok, reference()} | {error, term()}.
ref_wrap(_ContextRef, _PyObj) ->
    ?NIF_STUB.

%% @doc Check if a term is a py_ref.
%%
%% @param Term Term to check
%% @returns true | false
-spec is_ref(term()) -> boolean().
is_ref(_Term) ->
    ?NIF_STUB.

%% @doc Get the interpreter ID from a py_ref.
%%
%% This is fast - no GIL needed, just reads the stored interp_id.
%%
%% @param Ref py_ref reference
%% @returns InterpId
-spec ref_interp_id(reference()) -> non_neg_integer().
ref_interp_id(_Ref) ->
    ?NIF_STUB.

%% @doc Convert a py_ref to an Erlang term.
%%
%% @param Ref py_ref reference
%% @returns {ok, Term} | {error, Reason}
-spec ref_to_term(reference()) -> {ok, term()} | {error, term()}.
ref_to_term(_Ref) ->
    ?NIF_STUB.

%% @doc Get an attribute from a py_ref object.
%%
%% @param Ref py_ref reference
%% @param AttrName Attribute name (binary)
%% @returns {ok, Value} | {error, Reason}
-spec ref_getattr(reference(), binary()) -> {ok, term()} | {error, term()}.
ref_getattr(_Ref, _AttrName) ->
    ?NIF_STUB.

%% @doc Call a method on a py_ref object.
%%
%% @param Ref py_ref reference
%% @param Method Method name (binary)
%% @param Args List of arguments
%% @returns {ok, Result} | {error, Reason}
-spec ref_call_method(reference(), binary(), list()) -> {ok, term()} | {error, term()}.
ref_call_method(_Ref, _Method, _Args) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Reactor NIFs - Erlang-as-Reactor Architecture
%%%
%%% These NIFs support the Erlang-as-Reactor pattern where Erlang handles
%%% TCP accept/routing and Python handles HTTP parsing and ASGI/WSGI execution.
%%% ============================================================================

%% @doc Register an FD for reactor monitoring.
%%
%% The FD is owned by the context and receives {select, FdRes, Ref, ready_input/ready_output}
%% messages. Initial registration is for read events.
%%
%% @param ContextRef Context reference from context_create/1
%% @param Fd File descriptor to monitor
%% @param OwnerPid Process to receive select messages
%% @returns {ok, FdRef} | {error, Reason}
-spec reactor_register_fd(reference(), integer(), pid()) ->
    {ok, reference()} | {error, term()}.
reactor_register_fd(_ContextRef, _Fd, _OwnerPid) ->
    ?NIF_STUB.

%% @doc Re-register for read events after a one-shot event was delivered.
%%
%% Since enif_select is one-shot, this must be called after processing
%% each read event to continue monitoring.
%%
%% @param FdRef FD resource reference from reactor_register_fd/3
%% @returns ok | {error, Reason}
-spec reactor_reselect_read(reference()) -> ok | {error, term()}.
reactor_reselect_read(_FdRef) ->
    ?NIF_STUB.

%% @doc Switch to write monitoring for response sending.
%%
%% After HTTP request parsing is complete and a response is ready,
%% switch to write monitoring to send the response when the socket is ready.
%%
%% @param FdRef FD resource reference
%% @returns ok | {error, Reason}
-spec reactor_select_write(reference()) -> ok | {error, term()}.
reactor_select_write(_FdRef) ->
    ?NIF_STUB.

%% @doc Extract the file descriptor integer from an FD resource.
%%
%% Useful for passing the FD to Python for os.read/os.write operations.
%%
%% @param FdRef FD resource reference
%% @returns Fd integer | {error, Reason}
-spec get_fd_from_resource(reference()) -> integer() | {error, term()}.
get_fd_from_resource(_FdRef) ->
    ?NIF_STUB.

%% @doc Call Python's erlang_reactor.on_read_ready(fd).
%%
%% This is called when the FD is ready for reading. Python reads data,
%% parses HTTP, and returns an action indicating what to do next.
%%
%% Actions:
%% <ul>
%% <li>`"continue"' - Continue reading (call reactor_reselect_read)</li>
%% <li>`"write_pending"' - Response ready, switch to write mode</li>
%% <li>`"close"' - Close the connection</li>
%% </ul>
%%
%% @param ContextRef Context reference
%% @param Fd File descriptor
%% @returns {ok, Action} | {error, Reason}
-spec reactor_on_read_ready(reference(), integer()) ->
    {ok, binary()} | {error, term()}.
reactor_on_read_ready(_ContextRef, _Fd) ->
    ?NIF_STUB.

%% @doc Call Python's erlang_reactor.on_write_ready(fd).
%%
%% This is called when the FD is ready for writing. Python writes
%% buffered response data and returns an action.
%%
%% Actions:
%% <ul>
%% <li>`"continue"' - More data to write</li>
%% <li>`"read_pending"' - Keep-alive, switch back to read mode</li>
%% <li>`"close"' - Close the connection</li>
%% </ul>
%%
%% @param ContextRef Context reference
%% @param Fd File descriptor
%% @returns {ok, Action} | {error, Reason}
-spec reactor_on_write_ready(reference(), integer()) ->
    {ok, binary()} | {error, term()}.
reactor_on_write_ready(_ContextRef, _Fd) ->
    ?NIF_STUB.

%% @doc Initialize a Python protocol handler for a new connection.
%%
%% Called when a new connection is accepted. Creates an HTTPProtocol
%% instance in Python and registers it in the protocol registry.
%%
%% @param ContextRef Context reference
%% @param Fd File descriptor
%% @param ClientInfo Map with client info (addr, port)
%% @returns ok | {error, Reason}
-spec reactor_init_connection(reference(), integer(), map()) ->
    ok | {error, term()}.
reactor_init_connection(_ContextRef, _Fd, _ClientInfo) ->
    ?NIF_STUB.

%% @doc Close an FD and clean up the protocol handler.
%%
%% Calls Python's erlang_reactor.close_connection(fd) to clean up
%% the protocol handler, then closes the FD.
%%
%% @param FdRef FD resource reference
%% @returns ok | {error, Reason}
-spec reactor_close_fd(reference()) -> ok | {error, term()}.
reactor_close_fd(_FdRef) ->
    ?NIF_STUB.
