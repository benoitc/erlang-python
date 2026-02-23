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
    %% Sub-interpreters (Python 3.12+)
    subinterp_supported/0,
    subinterp_worker_new/0,
    subinterp_worker_destroy/1,
    subinterp_call/5,
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
    event_loop_set_event_proc/2,
    poll_via_proc/2,
    event_loop_wakeup/1,
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
    %% WSGI optimizations
    wsgi_run/4,
    %% Non-blocking submit (Phase 3 unified event-driven architecture)
    submit_call/6,
    submit_coroutine/6
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

%% @doc Set the router process for an event loop (legacy architecture).
%% The router receives enif_select messages and timer events.
-spec event_loop_set_router(reference(), pid()) -> ok | {error, term()}.
event_loop_set_router(_LoopRef, _RouterPid) ->
    ?NIF_STUB.

%% @doc Set the event process for an event loop (new architecture).
%% The event process uses Erlang mailbox as the event queue - no pthread_cond.
%% FD events and timers are delivered directly to this process.
-spec event_loop_set_event_proc(reference(), pid()) -> ok | {error, term()}.
event_loop_set_event_proc(_LoopRef, _EventProcPid) ->
    ?NIF_STUB.

%% @doc Poll for events via the event process.
%% Sends {poll, self(), Ref, TimeoutMs} to event process.
%% Returns {ok, Ref} - caller should receive {events, Ref, Events}.
-spec poll_via_proc(reference(), non_neg_integer()) -> {ok, reference()} | {error, term()}.
poll_via_proc(_LoopRef, _TimeoutMs) ->
    ?NIF_STUB.

%% @doc Wake up an event loop from a wait.
-spec event_loop_wakeup(reference()) -> ok | {error, term()}.
event_loop_wakeup(_LoopRef) ->
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

%% @doc Handle a select event (dispatch only, no auto-reselect).
%% Called by py_event_router when receiving {select, FdRes, Ref, ready_input/output}.
%% This combines get_fd_callback_id + dispatch_callback into one NIF call.
%% Does NOT auto-reselect - caller must explicitly call reselect_*_fd.
%% Type: read | write
-spec handle_fd_event(reference(), read | write) -> ok | {error, term()}.
handle_fd_event(_FdRef, _Type) ->
    ?NIF_STUB.

%% @doc Handle a select event and reselect in one NIF call.
%% Combines: get callback ID, dispatch to pending queue, re-register with enif_select.
%% This reduces NIF overhead by combining two operations.
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
%%% Non-blocking Submit NIFs (Phase 3 unified event-driven architecture)
%%% ============================================================================

%% @doc Submit a Python function call for non-blocking execution.
%%
%% The call is queued to a background worker thread. When complete,
%% the result is sent to EventProcPid as:
%%   {call_result, CallbackId, Result} or
%%   {call_error, CallbackId, Error}
%%
%% @param EventProcPid PID of event loop process to receive result
%% @param CallbackId Unique callback ID for correlation
%% @param Module Python module name (binary)
%% @param Func Python function name (binary)
%% @param Args Arguments list
%% @param Kwargs Keyword arguments map
%% @returns ok | {error, Reason}
-spec submit_call(pid(), non_neg_integer(), binary(), binary(), list(), map()) ->
    ok | {error, term()}.
submit_call(_EventProcPid, _CallbackId, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Submit an asyncio coroutine for non-blocking execution.
%%
%% The coroutine is queued to a background worker thread and run
%% in an asyncio event loop. Result delivery is the same as submit_call.
%%
%% @param EventProcPid PID of event loop process to receive result
%% @param CallbackId Unique callback ID for correlation
%% @param Module Python module name (binary)
%% @param Func Python async function name (binary)
%% @param Args Arguments list
%% @param Kwargs Keyword arguments map
%% @returns ok | {error, Reason}
-spec submit_coroutine(pid(), non_neg_integer(), binary(), binary(), list(), map()) ->
    ok | {error, term()}.
submit_coroutine(_EventProcPid, _CallbackId, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.
