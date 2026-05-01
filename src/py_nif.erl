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
    get_debug_counters/0,
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
    %% Subinterpreter capability probes (Python 3.12+ / 3.14+)
    subinterp_supported/0,
    owngil_supported/0,
    %% OWN_GIL thread pool (internal, used by py_event_loop_pool)
    subinterp_thread_pool_start/0,
    subinterp_thread_pool_start/1,
    subinterp_thread_pool_stop/0,
    subinterp_thread_pool_ready/0,
    subinterp_thread_pool_stats/0,
    %% OWN_GIL session management for event loop pool
    owngil_create_session/1,
    owngil_submit_task/7,
    owngil_destroy_session/2,
    owngil_apply_imports/3,
    owngil_apply_paths/3,
    %% Execution mode info
    execution_mode/0,
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
    set_event_loop_priv_dir/1,
    event_loop_new/0,
    event_loop_destroy/1,
    event_loop_set_router/2,
    event_loop_set_worker/2,
    event_loop_set_id/2,
    event_loop_wakeup/1,
    event_loop_run_async/7,
    %% Async task queue NIFs (uvloop-inspired)
    submit_task/7,
    submit_task_with_env/8,
    process_ready_tasks/1,
    event_loop_set_py_loop/2,
    %% Per-process namespace NIFs
    event_loop_exec/2,
    event_loop_eval/2,
    %% Per-interpreter import caching NIFs
    interp_apply_imports/2,
    interp_apply_paths/2,
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
    %% File descriptor utilities
    dup_fd/1,
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
    set_shared_worker/1,
    %% Worker pool
    pool_start/1,
    pool_stop/0,
    pool_submit/5,
    pool_stats/0,
    %% Process-per-context API (no mutex)
    context_create/1,
    context_destroy/1,
    context_call/5,
    context_call/6,
    context_eval/3,
    context_eval/4,
    context_exec/2,
    context_exec/3,
    %% Async dispatch (non-blocking)
    context_call_async/7,
    context_eval_async/5,
    context_exec_async/4,
    context_call_with_env_async/8,
    context_eval_with_env_async/6,
    context_exec_with_env_async/5,
    context_call_method/4,
    create_local_env/1,
    context_to_term/1,
    context_interp_id/1,
    context_set_callback_handler/2,
    context_get_callback_pipe/1,
    context_write_callback_response/2,
    context_resume/3,
    context_cancel_resume/2,
    context_get_event_loop/1,
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
    reactor_close_fd/2,
    %% Direct FD operations
    fd_read/2,
    fd_write/2,
    fd_select_read/1,
    fd_select_write/1,
    fd_close/1,
    socketpair/0,
    %% Channel API - bidirectional message passing
    channel_create/0,
    channel_create/1,
    channel_send/2,
    channel_receive/2,
    channel_try_receive/1,
    channel_reply/3,
    channel_close/1,
    channel_info/1,
    channel_wait/3,
    channel_cancel_wait/2,
    channel_register_sync_waiter/1,
    %% ByteChannel API - raw bytes, no term conversion
    byte_channel_send_bytes/2,
    byte_channel_try_receive_bytes/1,
    byte_channel_wait_bytes/3,
    %% PyBuffer API - zero-copy WSGI input
    py_buffer_create/1,
    py_buffer_write/2,
    py_buffer_close/1,
    %% SharedDict API - process-scoped shared dictionary
    shared_dict_new/0,
    shared_dict_get/3,
    shared_dict_set/3,
    shared_dict_del/2,
    shared_dict_keys/1,
    shared_dict_destroy/1
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

%% @doc Get debug counters for tracking resource lifecycle.
%% Returns a map with counter names and their values. Used for detecting leaks.
-spec get_debug_counters() -> map().
get_debug_counters() ->
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

%% @doc Check if OWN_GIL mode is supported (Python 3.14+).
%% OWN_GIL requires Python 3.14+ due to C extension global state bugs
%% in earlier versions (e.g., _decimal). See gh-106078.
-spec owngil_supported() -> boolean().
owngil_supported() ->
    ?NIF_STUB.

%%% ============================================================================
%%% OWN_GIL Thread Pool (internal, used by py_event_loop_pool)
%%% ============================================================================

%% @doc Start the OWN_GIL subinterpreter thread pool with default workers.
%% @private
-spec subinterp_thread_pool_start() -> ok | {error, term()}.
subinterp_thread_pool_start() ->
    ?NIF_STUB.

%% @doc Start the OWN_GIL subinterpreter thread pool with N workers.
%% @private
-spec subinterp_thread_pool_start(non_neg_integer()) -> ok | {error, term()}.
subinterp_thread_pool_start(_NumWorkers) ->
    ?NIF_STUB.

%% @doc Stop the OWN_GIL subinterpreter thread pool.
%% @private
-spec subinterp_thread_pool_stop() -> ok.
subinterp_thread_pool_stop() ->
    ?NIF_STUB.

%% @doc Check if the OWN_GIL thread pool is ready.
%% @private
-spec subinterp_thread_pool_ready() -> boolean().
subinterp_thread_pool_ready() ->
    ?NIF_STUB.

%% @doc Get OWN_GIL thread pool statistics.
%% @private
-spec subinterp_thread_pool_stats() -> map().
subinterp_thread_pool_stats() ->
    ?NIF_STUB.

%%% ============================================================================
%%% OWN_GIL Session Management (for event loop pool)
%%% ============================================================================

%% @doc Create a new OWN_GIL session for event loop pool.
%% The WorkerHint is used for worker assignment (typically loop index).
%% Returns {ok, WorkerId, HandleId} where:
%%   - WorkerId is the assigned worker thread index
%%   - HandleId is the unique namespace handle within that worker
-spec owngil_create_session(non_neg_integer()) ->
    {ok, non_neg_integer(), non_neg_integer()} | {error, term()}.
owngil_create_session(_WorkerHint) ->
    ?NIF_STUB.

%% @doc Submit an async task to an OWN_GIL worker.
%% Args: WorkerId, HandleId, CallerPid, Ref, Module, Func, Args
%% The task runs in the worker's asyncio event loop.
%% Result is sent to CallerPid as {async_result, Ref, Result}.
-spec owngil_submit_task(non_neg_integer(), non_neg_integer(), pid(), reference(),
                          binary(), binary(), list()) -> ok | {error, term()}.
owngil_submit_task(_WorkerId, _HandleId, _CallerPid, _Ref, _Module, _Func, _Args) ->
    ?NIF_STUB.

%% @doc Destroy an OWN_GIL session.
%% Cleans up the namespace within the worker.
-spec owngil_destroy_session(non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
owngil_destroy_session(_WorkerId, _HandleId) ->
    ?NIF_STUB.

%% @doc Apply imports to an OWN_GIL session.
%% Imports modules into the worker's sys.modules.
-spec owngil_apply_imports(non_neg_integer(), non_neg_integer(), [{binary(), binary() | all}]) -> ok | {error, term()}.
owngil_apply_imports(_WorkerId, _HandleId, _Imports) ->
    ?NIF_STUB.

%% @doc Apply paths to an OWN_GIL session.
%% Adds paths to the worker's sys.path.
-spec owngil_apply_paths(non_neg_integer(), non_neg_integer(), [binary()]) -> ok | {error, term()}.
owngil_apply_paths(_WorkerId, _HandleId, _Paths) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Execution Mode Info
%%% ============================================================================

%% @doc Get Python capability (internal use).
%% Returns the detected Python runtime capability:
%% - free_threaded: Python 3.13+ with no GIL (Py_GIL_DISABLED)
%% - gil: Conventional GIL build (any other supported version)
%%
%% For public execution mode, use py:execution_mode/0 which returns
%% `worker | owngil' based on the application configuration.
%% @private
-spec execution_mode() -> free_threaded | gil.
execution_mode() ->
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

%% @doc Set the priv_dir path for module imports in subinterpreters.
%% Must be called during application startup before creating event loops.
-spec set_event_loop_priv_dir(binary() | string()) -> ok | {error, term()}.
set_event_loop_priv_dir(_Path) ->
    ?NIF_STUB.

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

%%% ============================================================================
%%% Async Task Queue NIFs (uvloop-inspired)
%%% ============================================================================

%% @doc Submit an async task to the event loop (thread-safe).
%%
%% This NIF can be called from any thread including dirty schedulers.
%% It serializes the task info, enqueues to the task queue, and sends
%% a 'task_ready' wakeup to the worker via enif_send.
%%
%% The result will be sent to CallerPid as:
%%   {async_result, Ref, {ok, Result}} - on success
%%   {async_result, Ref, {error, Reason}} - on failure
-spec submit_task(reference(), pid(), reference(), binary(), binary(), list(), map()) ->
    ok | {error, term()}.
submit_task(_LoopRef, _CallerPid, _Ref, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Submit an async task with process-local env.
%%
%% Like submit_task but includes an env resource reference. The env's globals
%% dict is used for function lookup, allowing functions defined via py:exec()
%% to be called from the event loop.
-spec submit_task_with_env(reference(), pid(), reference(), binary(), binary(), list(), map(), reference()) ->
    ok | {error, term()}.
submit_task_with_env(_LoopRef, _CallerPid, _Ref, _Module, _Func, _Args, _Kwargs, _EnvRef) ->
    ?NIF_STUB.

%% @doc Process all pending tasks from the task queue.
%%
%% Called by the event worker when it receives 'task_ready' message.
%% Dequeues all tasks, creates coroutines, and schedules them on the loop.
%% Returns 'more' if batch limit was hit and more tasks remain.
-spec process_ready_tasks(reference()) -> ok | more | {error, term()}.
process_ready_tasks(_LoopRef) ->
    ?NIF_STUB.

%% @doc Store a Python event loop reference in the C struct.
%%
%% This avoids thread-local lookup issues when processing tasks.
%% Called from Python after creating the ErlangEventLoop.
-spec event_loop_set_py_loop(reference(), reference()) -> ok | {error, term()}.
event_loop_set_py_loop(_LoopRef, _PyLoopRef) ->
    ?NIF_STUB.

%% @doc Execute Python code in the calling process's namespace.
%% Each Erlang process gets an isolated namespace for the event loop.
%% Functions defined via exec can be called via create_task with __main__ module.
-spec event_loop_exec(reference(), binary() | iolist()) -> ok | {error, term()}.
event_loop_exec(_LoopRef, _Code) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in the calling process's namespace.
%% Returns the result of the expression.
-spec event_loop_eval(reference(), binary() | iolist()) -> {ok, term()} | {error, term()}.
event_loop_eval(_LoopRef, _Expr) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Per-Interpreter Import Caching
%%% ============================================================================

%% @doc Apply a list of imports to an interpreter's module cache.
%%
%% Called when a new interpreter is created to pre-warm the cache with
%% all modules registered via py:import/1,2.
%%
%% @param Ref Context reference (from context_create/1)
%% @param Imports List of {ModuleBin, FuncBin | all} tuples
%% @returns ok | {error, Reason}
-spec interp_apply_imports(reference(), [{binary(), binary() | all}]) -> ok | {error, term()}.
interp_apply_imports(_Ref, _Imports) ->
    ?NIF_STUB.

%% @doc Apply a list of paths to an interpreter's sys.path.
%%
%% Paths are inserted at the beginning of sys.path to take precedence
%% over system paths. Called when a new context is created.
%%
%% @param Ref Context reference (from context_create/1)
%% @param Paths List of path binaries
%% @returns ok | {error, Reason}
-spec interp_apply_paths(reference(), [binary()]) -> ok | {error, term()}.
interp_apply_paths(_Ref, _Paths) ->
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
%% Called by py_event_worker when receiving {select, FdRes, Ref, ready_input/output}.
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

%% @doc Duplicate a file descriptor.
%% Creates an independent copy of the fd.
-spec dup_fd(integer()) -> {ok, integer()} | {error, term()}.
dup_fd(_Fd) ->
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

%% @doc Set the shared worker PID for task_ready notifications.
%% The worker receives task_ready messages from dispatch_timer and other
%% event sources to trigger process_ready_tasks.
-spec set_shared_worker(pid()) -> ok | {error, term()}.
set_shared_worker(_WorkerPid) ->
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
%% Creates a subinterpreter (Python 3.14+ OWN_GIL) or worker thread-state
%% based on the mode parameter. Returns a reference to the context and its
%% interpreter ID for routing.
%%
%% @param Mode `worker' or `owngil'
%% @returns {ok, ContextRef, InterpId} | {error, Reason}
-spec context_create(worker | owngil) ->
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
%% @returns {ok, Result} | {error, Reason} | {suspended, ...} | {schedule, ...}
-spec context_call(reference(), binary(), binary(), list(), map()) ->
    {ok, term()} | {error, term()} |
    {suspended, non_neg_integer(), reference(), {atom(), list()}} |
    {schedule, binary(), tuple()}.
context_call(_ContextRef, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Call a Python function in a context with a process-local environment.
%%
%% @param ContextRef Context reference
%% @param Module Python module name
%% @param Func Function name
%% @param Args List of arguments
%% @param Kwargs Map of keyword arguments
%% @param EnvRef Process-local environment reference
%% @returns {ok, Result} | {error, Reason} | {suspended, ...} | {schedule, ...}
-spec context_call(reference(), binary(), binary(), list(), map(), reference()) ->
    {ok, term()} | {error, term()} |
    {suspended, non_neg_integer(), reference(), {atom(), list()}} |
    {schedule, binary(), tuple()}.
context_call(_ContextRef, _Module, _Func, _Args, _Kwargs, _EnvRef) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in a context.
%%
%% NO MUTEX - caller must ensure exclusive access (process ownership).
%%
%% @param ContextRef Context reference
%% @param Code Python code to evaluate
%% @param Locals Map of local variables
%% @returns {ok, Result} | {error, Reason} | {suspended, ...} | {schedule, ...}
-spec context_eval(reference(), binary(), map()) ->
    {ok, term()} | {error, term()} |
    {suspended, non_neg_integer(), reference(), {atom(), list()}} |
    {schedule, binary(), tuple()}.
context_eval(_ContextRef, _Code, _Locals) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in a context with a process-local environment.
%%
%% @param ContextRef Context reference
%% @param Code Python code to evaluate
%% @param Locals Map of local variables
%% @param EnvRef Process-local environment reference
%% @returns {ok, Result} | {error, Reason} | {suspended, ...} | {schedule, ...}
-spec context_eval(reference(), binary(), map(), reference()) ->
    {ok, term()} | {error, term()} |
    {suspended, non_neg_integer(), reference(), {atom(), list()}} |
    {schedule, binary(), tuple()}.
context_eval(_ContextRef, _Code, _Locals, _EnvRef) ->
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

%% @doc Execute Python code in a context with a process-local environment.
%%
%% @param ContextRef Context reference
%% @param Code Python code to execute
%% @param EnvRef Process-local environment reference
%% @returns ok | {error, Reason}
-spec context_exec(reference(), binary(), reference()) -> ok | {error, term()}.
context_exec(_ContextRef, _Code, _EnvRef) ->
    ?NIF_STUB.

%% @doc Async call - enqueue and return immediately.
%%
%% Dispatches a Python function call to the worker thread and returns
%% immediately with {enqueued, RequestId}. The worker thread will send
%% {py_result, RequestId, Result} to CallerPid when done.
%%
%% @param ContextRef Context reference
%% @param CallerPid PID to send result to
%% @param RequestId Request ID for correlation
%% @param Module Python module name
%% @param Func Function name
%% @param Args List of arguments
%% @param Kwargs Keyword arguments map
%% @returns {enqueued, RequestId} | {error, Reason}
-spec context_call_async(reference(), pid(), term(), binary(), binary(), list(), map()) ->
    {enqueued, term()} | {error, term()}.
context_call_async(_ContextRef, _CallerPid, _RequestId, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Async eval - enqueue and return immediately.
%%
%% Dispatches a Python eval to the worker thread and returns immediately
%% with {enqueued, RequestId}. The worker thread will send
%% {py_result, RequestId, Result} to CallerPid when done.
%%
%% @param ContextRef Context reference
%% @param CallerPid PID to send result to
%% @param RequestId Request ID for correlation
%% @param Code Python expression to evaluate
%% @param Locals Local variables map
%% @returns {enqueued, RequestId} | {error, Reason}
-spec context_eval_async(reference(), pid(), term(), binary(), map()) ->
    {enqueued, term()} | {error, term()}.
context_eval_async(_ContextRef, _CallerPid, _RequestId, _Code, _Locals) ->
    ?NIF_STUB.

%% @doc Async exec - enqueue and return immediately.
%%
%% Dispatches Python code execution to the worker thread and returns
%% immediately with {enqueued, RequestId}. The worker thread will send
%% {py_result, RequestId, Result} to CallerPid when done.
%%
%% @param ContextRef Context reference
%% @param CallerPid PID to send result to
%% @param RequestId Request ID for correlation
%% @param Code Python code to execute
%% @returns {enqueued, RequestId} | {error, Reason}
-spec context_exec_async(reference(), pid(), term(), binary()) ->
    {enqueued, term()} | {error, term()}.
context_exec_async(_ContextRef, _CallerPid, _RequestId, _Code) ->
    ?NIF_STUB.

%% @doc Async call with process-local environment.
%% @private
-spec context_call_with_env_async(reference(), pid(), term(),
                                   binary(), binary(), list(), map(),
                                   reference()) ->
    {enqueued, term()} | {error, term()}.
context_call_with_env_async(_CtxRef, _CallerPid, _RequestId,
                             _Module, _Func, _Args, _Kwargs, _EnvRef) ->
    ?NIF_STUB.

%% @doc Async eval with process-local environment.
%% @private
-spec context_eval_with_env_async(reference(), pid(), term(),
                                   binary(), map(), reference()) ->
    {enqueued, term()} | {error, term()}.
context_eval_with_env_async(_CtxRef, _CallerPid, _RequestId,
                             _Code, _Locals, _EnvRef) ->
    ?NIF_STUB.

%% @doc Async exec with process-local environment.
%% @private
-spec context_exec_with_env_async(reference(), pid(), term(),
                                   binary(), reference()) ->
    {enqueued, term()} | {error, term()}.
context_exec_with_env_async(_CtxRef, _CallerPid, _RequestId,
                             _Code, _EnvRef) ->
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
%%
%% Note: In thread-model subinterpreters (Python 3.13+ OWN_GIL), this function
%% returns {error, not_supported_in_thread_model}. Use context_call with the
%% object reference instead.
-spec context_call_method(reference(), reference(), binary(), list()) ->
    {ok, term()} | {error, term()} | {error, not_supported_in_thread_model}.
context_call_method(_ContextRef, _ObjRef, _Method, _Args) ->
    ?NIF_STUB.

%% @doc Create a new process-local Python environment.
%%
%% Creates a new Python globals/locals dict pair for use as a process-local
%% environment. The dicts are created inside the context's interpreter to
%% ensure the correct memory allocator is used.
%%
%% The returned resource should be stored in the process dictionary, keyed
%% by the interpreter ID.
%% When the process exits, the resource destructor frees the Python dicts.
%%
%% @param CtxRef Context reference (from context_create/1)
%% @returns {ok, EnvRef} | {error, Reason}
-spec create_local_env(reference()) -> {ok, reference()} | {error, term()}.
create_local_env(_CtxRef) ->
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
%%
%% Note: In thread-model subinterpreters (Python 3.13+ OWN_GIL), this function
%% returns {error, not_supported_in_thread_model}. Thread-model contexts use
%% blocking pipe-based callbacks instead of suspension/resume.
-spec context_resume(reference(), reference(), binary()) ->
    {ok, term()} | {error, term()} | {error, not_supported_in_thread_model} |
    {suspended, non_neg_integer(), reference(), {binary(), tuple()}}.
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

%% @doc Get the event loop for a subinterpreter context.
%%
%% For subinterpreter contexts (Python 3.12 and later), this returns the event loop
%% reference that can be used to create a dedicated event worker. Worker mode
%% contexts (Python before 3.12) use the shared router instead and return an error.
%%
%% @param ContextRef Context reference
%% @returns {ok, LoopRef} for subinterpreter contexts, or {error, not_subinterp} for worker mode
-spec context_get_event_loop(reference()) -> {ok, reference()} | {error, term()}.
context_get_event_loop(_ContextRef) ->
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
%% @param ContextRef Context resource reference
%% @param FdRef FD resource reference
%% @returns ok | {error, Reason}
-spec reactor_close_fd(reference(), reference()) -> ok | {error, term()}.
reactor_close_fd(_ContextRef, _FdRef) ->
    ?NIF_STUB.

%%% ============================================================================
%%% Direct FD Operations
%%% ============================================================================

%% @doc Read up to Size bytes from a file descriptor.
%%
%% @param Fd File descriptor
%% @param Size Maximum bytes to read
%% @returns {ok, Data} | {error, Reason}
-spec fd_read(integer(), non_neg_integer()) -> {ok, binary()} | {error, term()}.
fd_read(_Fd, _Size) ->
    ?NIF_STUB.

%% @doc Write data to a file descriptor.
%%
%% @param Fd File descriptor
%% @param Data Binary data to write
%% @returns {ok, Written} | {error, Reason}
-spec fd_write(integer(), binary()) -> {ok, non_neg_integer()} | {error, term()}.
fd_write(_Fd, _Data) ->
    ?NIF_STUB.

%% @doc Register FD for read selection.
%%
%% Caller will receive {select, FdRef, Ref, ready_input} when readable.
%% The returned FdRef must be kept alive while monitoring.
%%
%% @param Fd File descriptor
%% @returns {ok, FdRef} | {error, Reason}
-spec fd_select_read(integer()) -> {ok, reference()} | {error, term()}.
fd_select_read(_Fd) ->
    ?NIF_STUB.

%% @doc Register FD for write selection.
%%
%% Caller will receive {select, FdRef, Ref, ready_output} when writable.
%% The returned FdRef must be kept alive while monitoring.
%%
%% @param Fd File descriptor
%% @returns {ok, FdRef} | {error, Reason}
-spec fd_select_write(integer()) -> {ok, reference()} | {error, term()}.
fd_select_write(_Fd) ->
    ?NIF_STUB.

%% @doc Close a raw file descriptor.
%%
%% @param Fd File descriptor to close
%% @returns ok | {error, Reason}
-spec fd_close(integer()) -> ok | {error, term()}.
fd_close(_Fd) ->
    ?NIF_STUB.

%% @doc Create a Unix socketpair for bidirectional communication.
%%
%% Both FDs are set to non-blocking mode.
%%
%% @returns {ok, {Fd1, Fd2}} | {error, Reason}
-spec socketpair() -> {ok, {integer(), integer()}} | {error, term()}.
socketpair() ->
    ?NIF_STUB.

%%% ============================================================================
%%% Channel API - Bidirectional Message Passing
%%%
%%% Channels provide efficient bidirectional communication between Erlang
%%% processes and Python code using enif_ioq for zero-copy buffering.
%%% ============================================================================

%% @doc Create a new channel with default settings.
%%
%% @returns {ok, ChannelRef} | {error, Reason}
-spec channel_create() -> {ok, reference()} | {error, term()}.
channel_create() ->
    ?NIF_STUB.

%% @doc Create a new channel with specified max size for backpressure.
%%
%% @param MaxSize Maximum queue size in bytes (0 = unlimited)
%% @returns {ok, ChannelRef} | {error, Reason}
-spec channel_create(non_neg_integer()) -> {ok, reference()} | {error, term()}.
channel_create(_MaxSize) ->
    ?NIF_STUB.

%% @doc Send a message to a channel.
%%
%% @param ChannelRef Channel reference
%% @param Term Erlang term to send
%% @returns ok | busy | {error, closed}
-spec channel_send(reference(), term()) -> ok | busy | {error, term()}.
channel_send(_ChannelRef, _Term) ->
    ?NIF_STUB.

%% @doc Receive a message from a channel (may trigger Python suspension).
%%
%% @param ContextRef Context reference for suspension support
%% @param ChannelRef Channel reference
%% @returns {ok, Term} | {error, empty} | {error, closed}
-spec channel_receive(reference(), reference()) ->
    {ok, term()} | {error, empty | closed | term()}.
channel_receive(_ContextRef, _ChannelRef) ->
    ?NIF_STUB.

%% @doc Try to receive a message from a channel (non-blocking).
%%
%% @param ChannelRef Channel reference
%% @returns {ok, Term} | {error, empty} | {error, closed}
-spec channel_try_receive(reference()) ->
    {ok, term()} | {error, empty | closed | term()}.
channel_try_receive(_ChannelRef) ->
    ?NIF_STUB.

%% @doc Send a reply to an Erlang process from Python context.
%%
%% @param ContextRef Context reference
%% @param Pid Target process PID
%% @param Term Erlang term to send
%% @returns ok | {error, Reason}
-spec channel_reply(reference(), pid(), term()) -> ok | {error, term()}.
channel_reply(_ContextRef, _Pid, _Term) ->
    ?NIF_STUB.

%% @doc Close a channel.
%%
%% Closes the channel and signals any waiting receivers.
%%
%% @param ChannelRef Channel reference
%% @returns ok
-spec channel_close(reference()) -> ok.
channel_close(_ChannelRef) ->
    ?NIF_STUB.

%% @doc Get channel information.
%%
%% @param ChannelRef Channel reference
%% @returns #{size => N, max_size => M, closed => Bool}
-spec channel_info(reference()) -> map().
channel_info(_ChannelRef) ->
    ?NIF_STUB.

%% @doc Register an async waiter for a channel.
%%
%% If data is available, returns immediately with {ok, Data}.
%% If empty, registers the callback_id for dispatch when data arrives.
%%
%% @param ChannelRef Channel reference
%% @param CallbackId Callback ID for timer dispatch
%% @param LoopRef Event loop reference for dispatching
%% @returns ok | {ok, Data} | {error, closed}
-spec channel_wait(reference(), non_neg_integer(), reference()) ->
    ok | {ok, term()} | {error, term()}.
channel_wait(_ChannelRef, _CallbackId, _LoopRef) ->
    ?NIF_STUB.

%% @doc Cancel an async waiter for a channel.
%%
%% Called when the Python Future is cancelled or times out.
%%
%% @param ChannelRef Channel reference
%% @param CallbackId Callback ID to cancel
%% @returns ok
-spec channel_cancel_wait(reference(), non_neg_integer()) -> ok.
channel_cancel_wait(_ChannelRef, _CallbackId) ->
    ?NIF_STUB.

%% @doc Register a sync waiter for blocking receive.
%%
%% Registers the calling process as a sync waiter. When data arrives,
%% the waiter receives a 'channel_data_ready' message. When the channel
%% closes, receives 'channel_closed'.
%%
%% @param ChannelRef Channel reference
%% @returns ok | has_data | {error, closed} | {error, waiter_exists}
-spec channel_register_sync_waiter(reference()) -> ok | has_data | {error, term()}.
channel_register_sync_waiter(_ChannelRef) ->
    ?NIF_STUB.

%%% ============================================================================
%%% ByteChannel API - Raw bytes, no term conversion
%%%
%%% ByteChannel provides raw byte streaming without term serialization,
%%% suitable for HTTP bodies, file transfers, and binary protocols.
%%% ============================================================================

%% @doc Send raw bytes to a channel (no term serialization).
%%
%% Sends binary data directly to the channel without converting to
%% Erlang external term format.
%%
%% @param ChannelRef Channel reference
%% @param Bytes Binary data to send
%% @returns ok | busy | {error, closed}
-spec byte_channel_send_bytes(reference(), binary()) -> ok | busy | {error, term()}.
byte_channel_send_bytes(_ChannelRef, _Bytes) ->
    ?NIF_STUB.

%% @doc Try to receive raw bytes from a channel (non-blocking).
%%
%% Returns immediately with binary data or empty/closed status.
%% No term deserialization is performed.
%%
%% @param ChannelRef Channel reference
%% @returns {ok, Binary} | {error, empty} | {error, closed}
-spec byte_channel_try_receive_bytes(reference()) ->
    {ok, binary()} | {error, empty | closed | term()}.
byte_channel_try_receive_bytes(_ChannelRef) ->
    ?NIF_STUB.

%% @doc Register an async waiter for raw bytes from a channel.
%%
%% If data is available, returns immediately with {ok, Binary}.
%% If empty, registers the callback_id for dispatch when data arrives.
%% Same semantics as channel_wait but returns raw binary data.
%%
%% @param ChannelRef Channel reference
%% @param CallbackId Callback ID for timer dispatch
%% @param LoopRef Event loop reference for dispatching
%% @returns ok | {ok, Binary} | {error, closed}
-spec byte_channel_wait_bytes(reference(), non_neg_integer(), reference()) ->
    ok | {ok, binary()} | {error, term()}.
byte_channel_wait_bytes(_ChannelRef, _CallbackId, _LoopRef) ->
    ?NIF_STUB.

%%% ============================================================================
%%% PyBuffer API - Zero-copy WSGI Input
%%% ============================================================================

%% @doc Create a new PyBuffer resource.
%%
%% Creates a buffer that can be written by Erlang and read by Python
%% with zero-copy semantics. The buffer is suitable for use as wsgi.input.
%%
%% @param ContentLength Expected size in bytes, or `undefined' for chunked
%% @returns {ok, BufferRef} | {error, Reason}
-spec py_buffer_create(non_neg_integer() | undefined) -> {ok, reference()} | {error, term()}.
py_buffer_create(_ContentLength) ->
    ?NIF_STUB.

%% @doc Write binary data to the buffer.
%%
%% Appends data to the buffer and signals any waiting Python readers.
%%
%% @param BufferRef Buffer reference from py_buffer_create/1
%% @param Data Binary data to append
%% @returns ok | {error, Reason}
-spec py_buffer_write(reference(), binary()) -> ok | {error, term()}.
py_buffer_write(_BufferRef, _Data) ->
    ?NIF_STUB.

%% @doc Close the buffer (signal EOF).
%%
%% Sets the EOF flag and wakes up any Python threads waiting for data.
%%
%% @param BufferRef Buffer reference
%% @returns ok
-spec py_buffer_close(reference()) -> ok.
py_buffer_close(_BufferRef) ->
    ?NIF_STUB.

%%% ============================================================================
%%% SharedDict API - Process-scoped Shared Dictionary
%%% ============================================================================

%% @doc Create a new process-scoped SharedDict.
%%
%% Creates a SharedDict owned by the calling process. The dict is automatically
%% destroyed when the owning process terminates.
%%
%% @returns {ok, Reference} on success, {error, Reason} on failure
-spec shared_dict_new() -> {ok, reference()} | {error, term()}.
shared_dict_new() ->
    ?NIF_STUB.

%% @doc Get a value from SharedDict.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @param Default Default value if key not found
%% @returns Value or Default, badarg if destroyed
-spec shared_dict_get(reference(), binary(), term()) -> term().
shared_dict_get(_Handle, _Key, _Default) ->
    ?NIF_STUB.

%% @doc Set a value in SharedDict.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @param Value Erlang term value (will be pickled)
%% @returns ok on success, badarg if destroyed
-spec shared_dict_set(reference(), binary(), term()) -> ok | {error, term()}.
shared_dict_set(_Handle, _Key, _Value) ->
    ?NIF_STUB.

%% @doc Delete a key from SharedDict.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @returns ok on success (even if key didn't exist), badarg if destroyed
-spec shared_dict_del(reference(), binary()) -> ok.
shared_dict_del(_Handle, _Key) ->
    ?NIF_STUB.

%% @doc Get all keys from SharedDict.
%%
%% @param Handle SharedDict reference
%% @returns List of binary keys, badarg if destroyed
-spec shared_dict_keys(reference()) -> [binary()].
shared_dict_keys(_Handle) ->
    ?NIF_STUB.

%% @doc Explicitly destroy a SharedDict.
%%
%% Marks the SharedDict as destroyed and clears its Python dict.
%% This is idempotent - calling on an already-destroyed dict returns ok.
%%
%% @param Handle SharedDict reference
%% @returns ok
-spec shared_dict_destroy(reference()) -> ok.
shared_dict_destroy(_Handle) ->
    ?NIF_STUB.
