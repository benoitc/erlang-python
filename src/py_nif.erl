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
    worker_recv/2,
    import_module/2,
    get_attr/3,
    version/0,
    memory_stats/0,
    gc/0,
    gc/1,
    tracemalloc_start/0,
    tracemalloc_start/1,
    tracemalloc_stop/0
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
-spec worker_call(reference(), binary(), binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
worker_call(_WorkerRef, _Module, _Func, _Args, _Kwargs) ->
    ?NIF_STUB.

%% @doc Call a Python function from a worker with timeout.
-spec worker_call(reference(), binary(), binary(), list(), map(), non_neg_integer()) ->
    {ok, term()} | {error, term()}.
worker_call(_WorkerRef, _Module, _Func, _Args, _Kwargs, _TimeoutMs) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in a worker.
-spec worker_eval(reference(), binary(), map()) -> {ok, term()} | {error, term()}.
worker_eval(_WorkerRef, _Code, _Locals) ->
    ?NIF_STUB.

%% @doc Evaluate a Python expression in a worker with timeout.
-spec worker_eval(reference(), binary(), map(), non_neg_integer()) -> {ok, term()} | {error, term()}.
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

%% @doc Wait for a message in the worker loop.
%% Releases the GIL while waiting.
-spec worker_recv(reference(), timeout()) -> {ok, term()} | timeout | {error, term()}.
worker_recv(_WorkerRef, _TimeoutMs) ->
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
