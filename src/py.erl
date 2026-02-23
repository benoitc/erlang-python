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

%%% @doc High-level API for executing Python code from Erlang.
%%%
%%% This module provides a simple interface to call Python functions,
%%% execute Python code, and stream results from Python generators.
%%%
%%% == Examples ==
%%%
%%% ```
%%% %% Call a Python function
%%% {ok, Result} = py:call(json, dumps, [#{foo => bar}]).
%%%
%%% %% Call with keyword arguments
%%% {ok, Result} = py:call(json, dumps, [Data], #{indent => 2}).
%%%
%%% %% Execute raw Python code
%%% {ok, Result} = py:eval("1 + 2").
%%%
%%% %% Stream from a generator
%%% {ok, Stream} = py:stream(mymodule, generate_tokens, [Prompt]),
%%% lists:foreach(fun(Token) -> io:format("~s", [Token]) end, Stream).
%%% '''
-module(py).

-export([
    call/3,
    call/4,
    call/5,
    call_async/3,
    call_async/4,
    await/1,
    await/2,
    eval/1,
    eval/2,
    eval/3,
    exec/1,
    stream/3,
    stream/4,
    stream_eval/1,
    stream_eval/2,
    version/0,
    memory_stats/0,
    gc/0,
    gc/1,
    tracemalloc_start/0,
    tracemalloc_start/1,
    tracemalloc_stop/0,
    register_function/2,
    register_function/3,
    unregister_function/1,
    %% Asyncio integration
    async_call/3,
    async_call/4,
    async_await/1,
    async_await/2,
    async_gather/1,
    async_stream/3,
    async_stream/4,
    %% Parallel execution (Python 3.12+ sub-interpreters)
    parallel/1,
    subinterp_supported/0,
    %% Virtual environment
    activate_venv/1,
    deactivate_venv/0,
    venv_info/0,
    %% Execution info
    execution_mode/0,
    num_executors/0,
    %% Shared state (accessible from Python workers)
    state_fetch/1,
    state_store/2,
    state_remove/1,
    state_keys/0,
    state_clear/0,
    state_incr/1,
    state_incr/2,
    state_decr/1,
    state_decr/2,
    %% Module reload
    reload/1,
    %% Context affinity
    bind/0, bind/1,
    unbind/0, unbind/1,
    is_bound/0,
    with_context/1,
    ctx_call/4, ctx_call/5, ctx_call/6,
    ctx_eval/2, ctx_eval/3, ctx_eval/4,
    ctx_exec/2,
    %% Logging and tracing
    configure_logging/0,
    configure_logging/1,
    enable_tracing/0,
    disable_tracing/0,
    get_traces/0,
    clear_traces/0
]).

-type py_result() :: {ok, term()} | {error, term()}.
-type py_ref() :: reference().
-type py_module() :: atom() | binary() | string().
-type py_func() :: atom() | binary() | string().
-type py_args() :: [term()].
-type py_kwargs() :: #{atom() | binary() => term()}.

%% Context affinity handle
-record(py_ctx, {ref :: reference()}).
-opaque py_ctx() :: #py_ctx{}.

-export_type([py_result/0, py_ref/0, py_ctx/0]).

%% Default timeout for synchronous calls (30 seconds)
-define(DEFAULT_TIMEOUT, 30000).

%%% ============================================================================
%%% Synchronous API
%%% ============================================================================

%% @doc Call a Python function synchronously.
-spec call(py_module(), py_func(), py_args()) -> py_result().
call(Module, Func, Args) ->
    call(Module, Func, Args, #{}).

%% @doc Call a Python function with keyword arguments.
-spec call(py_module(), py_func(), py_args(), py_kwargs()) -> py_result().
call(Module, Func, Args, Kwargs) ->
    call(Module, Func, Args, Kwargs, ?DEFAULT_TIMEOUT).

%% @doc Call a Python function with keyword arguments and custom timeout.
%% Timeout is in milliseconds. Use `infinity' for no timeout.
%% Rate limited via ETS-based semaphore to prevent overload.
-spec call(py_module(), py_func(), py_args(), py_kwargs(), timeout()) -> py_result().
call(Module, Func, Args, Kwargs, Timeout) ->
    %% Acquire semaphore slot before making the call
    case py_semaphore:acquire(Timeout) of
        ok ->
            try
                do_call(Module, Func, Args, Kwargs, Timeout)
            after
                py_semaphore:release()
            end;
        {error, max_concurrent} ->
            {error, {overloaded, py_semaphore:current(), py_semaphore:max_concurrent()}}
    end.

%% @private
do_call(Module, Func, Args, Kwargs, Timeout) ->
    Ref = make_ref(),
    TimeoutMs = py_util:normalize_timeout(Timeout, ?DEFAULT_TIMEOUT),
    Request = {call, Ref, self(), Module, Func, Args, Kwargs, TimeoutMs},
    case get_binding() of
        {bound, Worker} -> py_pool:direct_request(Worker, Request);
        unbound -> py_pool:request(Request)
    end,
    await(Ref, Timeout).

%% @private Get binding if process is bound
get_binding() ->
    Key = {process, self()},
    case py_pool:lookup_binding(Key) of
        {ok, Worker} -> {bound, Worker};
        not_found -> unbound
    end.

%% @doc Evaluate a Python expression and return the result.
-spec eval(string() | binary()) -> py_result().
eval(Code) ->
    eval(Code, #{}).

%% @doc Evaluate a Python expression with local variables.
-spec eval(string() | binary(), map()) -> py_result().
eval(Code, Locals) ->
    eval(Code, Locals, ?DEFAULT_TIMEOUT).

%% @doc Evaluate a Python expression with local variables and timeout.
%% Timeout is in milliseconds. Use `infinity' for no timeout.
-spec eval(string() | binary(), map(), timeout()) -> py_result().
eval(Code, Locals, Timeout) ->
    Ref = make_ref(),
    TimeoutMs = py_util:normalize_timeout(Timeout, ?DEFAULT_TIMEOUT),
    Request = {eval, Ref, self(), Code, Locals, TimeoutMs},
    case get_binding() of
        {bound, Worker} -> py_pool:direct_request(Worker, Request);
        unbound -> py_pool:request(Request)
    end,
    await(Ref, Timeout).

%% @doc Execute Python statements (no return value expected).
-spec exec(string() | binary()) -> ok | {error, term()}.
exec(Code) ->
    Ref = make_ref(),
    Request = {exec, Ref, self(), Code},
    case get_binding() of
        {bound, Worker} -> py_pool:direct_request(Worker, Request);
        unbound -> py_pool:request(Request)
    end,
    case await(Ref, ?DEFAULT_TIMEOUT) of
        {ok, _} -> ok;
        Error -> Error
    end.

%%% ============================================================================
%%% Asynchronous API
%%% ============================================================================

%% @doc Call a Python function asynchronously, returns immediately with a ref.
-spec call_async(py_module(), py_func(), py_args()) -> py_ref().
call_async(Module, Func, Args) ->
    call_async(Module, Func, Args, #{}).

%% @doc Call a Python function asynchronously with kwargs.
-spec call_async(py_module(), py_func(), py_args(), py_kwargs()) -> py_ref().
call_async(Module, Func, Args, Kwargs) ->
    Ref = make_ref(),
    py_pool:request({call, Ref, self(), Module, Func, Args, Kwargs}),
    Ref.

%% @doc Wait for an async call to complete.
-spec await(py_ref()) -> py_result().
await(Ref) ->
    await(Ref, ?DEFAULT_TIMEOUT).

%% @doc Wait for an async call with timeout.
-spec await(py_ref(), timeout()) -> py_result().
await(Ref, Timeout) ->
    receive
        {py_response, Ref, Result} -> Result;
        {py_error, Ref, Error} -> {error, Error}
    after Timeout ->
        {error, timeout}
    end.

%%% ============================================================================
%%% Streaming API
%%% ============================================================================

%% @doc Stream results from a Python generator.
%% Returns a list of all yielded values.
-spec stream(py_module(), py_func(), py_args()) -> py_result().
stream(Module, Func, Args) ->
    stream(Module, Func, Args, #{}).

%% @doc Stream results from a Python generator with kwargs.
-spec stream(py_module(), py_func(), py_args(), py_kwargs()) -> py_result().
stream(Module, Func, Args, Kwargs) ->
    Ref = make_ref(),
    py_pool:request({stream, Ref, self(), Module, Func, Args, Kwargs}),
    stream_collect(Ref, []).

%% @private
stream_collect(Ref, Acc) ->
    receive
        {py_chunk, Ref, Chunk} ->
            stream_collect(Ref, [Chunk | Acc]);
        {py_end, Ref} ->
            {ok, lists:reverse(Acc)};
        {py_error, Ref, Error} ->
            {error, Error}
    after ?DEFAULT_TIMEOUT ->
        {error, timeout}
    end.

%% @doc Stream results from a Python generator expression.
%% Evaluates the expression and if it returns a generator, streams all values.
-spec stream_eval(string() | binary()) -> py_result().
stream_eval(Code) ->
    stream_eval(Code, #{}).

%% @doc Stream results from a Python generator expression with local variables.
-spec stream_eval(string() | binary(), map()) -> py_result().
stream_eval(Code, Locals) ->
    Ref = make_ref(),
    py_pool:request({stream_eval, Ref, self(), Code, Locals}),
    stream_collect(Ref, []).

%%% ============================================================================
%%% Info
%%% ============================================================================

%% @doc Get Python version string.
-spec version() -> {ok, binary()} | {error, term()}.
version() ->
    py_nif:version().

%%% ============================================================================
%%% Memory and GC
%%% ============================================================================

%% @doc Get Python memory statistics.
%% Returns a map containing:
%% - gc_stats: List of per-generation GC statistics
%% - gc_count: Tuple of object counts per generation
%% - gc_threshold: Collection thresholds per generation
%% - traced_memory_current: Current traced memory (if tracemalloc enabled)
%% - traced_memory_peak: Peak traced memory (if tracemalloc enabled)
-spec memory_stats() -> {ok, map()} | {error, term()}.
memory_stats() ->
    py_nif:memory_stats().

%% @doc Force Python garbage collection.
%% Performs a full collection (all generations).
%% Returns the number of unreachable objects collected.
-spec gc() -> {ok, integer()} | {error, term()}.
gc() ->
    py_nif:gc().

%% @doc Force garbage collection of a specific generation.
%% Generation 0 collects only the youngest objects.
%% Generation 1 collects generations 0 and 1.
%% Generation 2 (default) performs a full collection.
-spec gc(0..2) -> {ok, integer()} | {error, term()}.
gc(Generation) when Generation >= 0, Generation =< 2 ->
    py_nif:gc(Generation).

%% @doc Start memory allocation tracing.
%% After starting, memory_stats() will include traced_memory_current
%% and traced_memory_peak values.
-spec tracemalloc_start() -> ok | {error, term()}.
tracemalloc_start() ->
    py_nif:tracemalloc_start().

%% @doc Start memory tracing with specified frame depth.
%% Higher frame counts provide more detailed tracebacks but use more memory.
-spec tracemalloc_start(pos_integer()) -> ok | {error, term()}.
tracemalloc_start(NFrame) when is_integer(NFrame), NFrame > 0 ->
    py_nif:tracemalloc_start(NFrame).

%% @doc Stop memory allocation tracing.
-spec tracemalloc_stop() -> ok | {error, term()}.
tracemalloc_stop() ->
    py_nif:tracemalloc_stop().

%%% ============================================================================
%%% Erlang Function Registration
%%% ============================================================================

%% @doc Register an Erlang function to be callable from Python.
%% Python code can then call: erlang.call('name', arg1, arg2, ...)
%% The function should accept a list of arguments and return a term.
-spec register_function(Name :: atom() | binary(), Fun :: fun((list()) -> term())) -> ok.
register_function(Name, Fun) when is_function(Fun, 1) ->
    py_callback:register(Name, Fun).

%% @doc Register an Erlang module:function to be callable from Python.
%% The function will be called as Module:Function(Args).
-spec register_function(Name :: atom() | binary(), Module :: atom(), Function :: atom()) -> ok.
register_function(Name, Module, Function) when is_atom(Module), is_atom(Function) ->
    py_callback:register(Name, {Module, Function}).

%% @doc Unregister a previously registered function.
-spec unregister_function(Name :: atom() | binary()) -> ok.
unregister_function(Name) ->
    py_callback:unregister(Name).

%%% ============================================================================
%%% Asyncio Integration
%%% ============================================================================

%% @doc Call a Python async function (coroutine).
%% Returns immediately with a reference. Use async_await/1,2 to get the result.
%% This is for calling functions defined with `async def' in Python.
%%
%% Example:
%% ```
%% Ref = py:async_call(aiohttp, get, [<<"https://example.com">>]),
%% {ok, Response} = py:async_await(Ref).
%% '''
-spec async_call(py_module(), py_func(), py_args()) -> py_ref().
async_call(Module, Func, Args) ->
    async_call(Module, Func, Args, #{}).

%% @doc Call a Python async function with keyword arguments.
-spec async_call(py_module(), py_func(), py_args(), py_kwargs()) -> py_ref().
async_call(Module, Func, Args, Kwargs) ->
    case py_async_driver:submit(
            py_util:to_binary(Module),
            py_util:to_binary(Func),
            Args,
            Kwargs) of
        {ok, Ref} -> Ref;
        {error, Reason} -> error({async_call_failed, Reason})
    end.

%% @doc Wait for an async call to complete.
-spec async_await(py_ref()) -> py_result().
async_await(Ref) ->
    async_await(Ref, ?DEFAULT_TIMEOUT).

%% @doc Wait for an async call with timeout.
-spec async_await(py_ref(), timeout()) -> py_result().
async_await(Ref, Timeout) ->
    receive
        {py_result, Ref, Result} -> {ok, Result};
        {py_error, Ref, Error} -> {error, Error}
    after Timeout ->
        {error, timeout}
    end.

%% @doc Execute multiple async calls concurrently using asyncio.gather.
%% Takes a list of {Module, Func, Args} tuples and executes them all
%% concurrently, returning when all are complete.
%%
%% Example:
%% ```
%% {ok, Results} = py:async_gather([
%%     {aiohttp, get, [Url1]},
%%     {aiohttp, get, [Url2]},
%%     {aiohttp, get, [Url3]}
%% ]).
%% '''
-spec async_gather([{py_module(), py_func(), py_args()}]) -> py_result().
async_gather(Calls) ->
    Ref = make_ref(),
    py_async_pool:request({async_gather, Ref, self(), Calls}),
    async_await(Ref, ?DEFAULT_TIMEOUT).

%% @doc Stream results from a Python async generator.
%% Returns a list of all yielded values.
-spec async_stream(py_module(), py_func(), py_args()) -> py_result().
async_stream(Module, Func, Args) ->
    async_stream(Module, Func, Args, #{}).

%% @doc Stream results from a Python async generator with kwargs.
-spec async_stream(py_module(), py_func(), py_args(), py_kwargs()) -> py_result().
async_stream(Module, Func, Args, Kwargs) ->
    Ref = make_ref(),
    py_async_pool:request({async_stream, Ref, self(), Module, Func, Args, Kwargs}),
    async_stream_collect(Ref, []).

%% @private
async_stream_collect(Ref, Acc) ->
    receive
        {py_response, Ref, {ok, Result}} ->
            %% Got final result (async generator collected)
            {ok, Result};
        {py_chunk, Ref, Chunk} ->
            async_stream_collect(Ref, [Chunk | Acc]);
        {py_end, Ref} ->
            {ok, lists:reverse(Acc)};
        {py_error, Ref, Error} ->
            {error, Error}
    after ?DEFAULT_TIMEOUT ->
        {error, timeout}
    end.

%%% ============================================================================
%%% Parallel Execution (Python 3.12+ Sub-interpreters)
%%% ============================================================================

%% @doc Check if true parallel execution is supported.
%% Returns true on Python 3.12+ which supports per-interpreter GIL.
-spec subinterp_supported() -> boolean().
subinterp_supported() ->
    py_nif:subinterp_supported().

%% @doc Execute multiple Python calls in true parallel using sub-interpreters.
%% Each call runs in its own sub-interpreter with its own GIL, allowing
%% CPU-bound Python code to run in parallel.
%%
%% Requires Python 3.12+. Use subinterp_supported/0 to check availability.
%%
%% Example:
%% ```
%% %% Run numpy matrix operations in parallel
%% {ok, Results} = py:parallel([
%%     {numpy, dot, [MatrixA, MatrixB]},
%%     {numpy, dot, [MatrixC, MatrixD]},
%%     {numpy, dot, [MatrixE, MatrixF]}
%% ]).
%% '''
%%
%% On older Python versions, returns {error, subinterpreters_not_supported}.
-spec parallel([{py_module(), py_func(), py_args()}]) -> py_result().
parallel(Calls) when is_list(Calls) ->
    case py_nif:subinterp_supported() of
        true ->
            py_subinterp_pool:parallel(Calls);
        false ->
            {error, subinterpreters_not_supported}
    end.

%%% ============================================================================
%%% Virtual Environment Support
%%% ============================================================================

%% @doc Activate a Python virtual environment.
%% This modifies sys.path to use packages from the specified venv.
%% The venv path should be the root directory (containing bin/lib folders).
%%
%% Example:
%% ```
%% ok = py:activate_venv(<<"/path/to/myenv">>).
%% {ok, _} = py:call(sentence_transformers, 'SentenceTransformer', [<<"all-MiniLM-L6-v2">>]).
%% '''
-spec activate_venv(string() | binary()) -> ok | {error, term()}.
activate_venv(VenvPath) ->
    VenvBin = ensure_binary(VenvPath),
    %% Build site-packages path based on platform
    {ok, SitePackages} = eval(<<"__import__('os').path.join(vp, 'Lib' if __import__('sys').platform == 'win32' else 'lib', '' if __import__('sys').platform == 'win32' else f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}', 'site-packages')">>, #{vp => VenvBin}),
    %% Verify site-packages exists
    case eval(<<"__import__('os').path.isdir(sp)">>, #{sp => SitePackages}) of
        {ok, true} ->
            %% Save original path if not already saved
            _ = eval(<<"setattr(__import__('sys'), '_original_path', __import__('sys').path.copy()) if not hasattr(__import__('sys'), '_original_path') else None">>),
            %% Set venv info
            _ = eval(<<"setattr(__import__('sys'), '_active_venv', vp)">>, #{vp => VenvBin}),
            _ = eval(<<"setattr(__import__('sys'), '_venv_site_packages', sp)">>, #{sp => SitePackages}),
            %% Add to sys.path
            _ = eval(<<"__import__('sys').path.insert(0, sp) if sp not in __import__('sys').path else None">>, #{sp => SitePackages}),
            ok;
        {ok, false} ->
            {error, {invalid_venv, SitePackages}};
        Error ->
            Error
    end.

%% @doc Deactivate the current virtual environment.
%% Restores sys.path to its original state.
-spec deactivate_venv() -> ok | {error, term()}.
deactivate_venv() ->
    case eval(<<"hasattr(__import__('sys'), '_original_path')">>) of
        {ok, true} ->
            _ = eval(<<"__import__('sys').path.clear(); __import__('sys').path.extend(__import__('sys')._original_path)">>),
            _ = eval(<<"delattr(__import__('sys'), '_original_path')">>),
            _ = eval(<<"delattr(__import__('sys'), '_active_venv') if hasattr(__import__('sys'), '_active_venv') else None">>),
            _ = eval(<<"delattr(__import__('sys'), '_venv_site_packages') if hasattr(__import__('sys'), '_venv_site_packages') else None">>),
            ok;
        {ok, false} ->
            ok;
        Error ->
            Error
    end.

%% @doc Get information about the currently active virtual environment.
%% Returns a map with venv_path and site_packages, or none if no venv is active.
-spec venv_info() -> {ok, map() | none} | {error, term()}.
venv_info() ->
    Code = <<"({'active': True, 'venv_path': __import__('sys')._active_venv, 'site_packages': __import__('sys')._venv_site_packages, 'sys_path': __import__('sys').path} if hasattr(__import__('sys'), '_active_venv') else {'active': False})">>,
    eval(Code).

%% @private
ensure_binary(S) ->
    py_util:to_binary(S).

%%% ============================================================================
%%% Execution Info
%%% ============================================================================

%% @doc Get the current execution mode.
%% Returns one of:
%% - `free_threaded': Python 3.13+ with no GIL (Py_GIL_DISABLED)
%% - `subinterp': Python 3.12+ with per-interpreter GIL
%% - `multi_executor': Traditional Python with N executor threads
-spec execution_mode() -> free_threaded | subinterp | multi_executor.
execution_mode() ->
    py_nif:execution_mode().

%% @doc Get the number of executor threads.
%% For `multi_executor' mode, this is the number of executor threads.
%% For other modes, returns 1.
-spec num_executors() -> pos_integer().
num_executors() ->
    py_nif:num_executors().

%%% ============================================================================
%%% Shared State
%%% ============================================================================

%% @doc Fetch a value from shared state.
%% This state is accessible from Python workers via state_get('key').
-spec state_fetch(term()) -> {ok, term()} | {error, not_found}.
state_fetch(Key) ->
    py_state:fetch(Key).

%% @doc Store a value in shared state.
%% This state is accessible from Python workers via state_set('key', value).
-spec state_store(term(), term()) -> ok.
state_store(Key, Value) ->
    py_state:store(Key, Value).

%% @doc Remove a key from shared state.
-spec state_remove(term()) -> ok.
state_remove(Key) ->
    py_state:remove(Key).

%% @doc Get all keys in shared state.
-spec state_keys() -> [term()].
state_keys() ->
    py_state:keys().

%% @doc Clear all shared state.
-spec state_clear() -> ok.
state_clear() ->
    py_state:clear().

%% @doc Atomically increment a counter by 1.
-spec state_incr(term()) -> integer().
state_incr(Key) ->
    py_state:incr(Key).

%% @doc Atomically increment a counter by Amount.
-spec state_incr(term(), integer()) -> integer().
state_incr(Key, Amount) ->
    py_state:incr(Key, Amount).

%% @doc Atomically decrement a counter by 1.
-spec state_decr(term()) -> integer().
state_decr(Key) ->
    py_state:decr(Key).

%% @doc Atomically decrement a counter by Amount.
-spec state_decr(term(), integer()) -> integer().
state_decr(Key, Amount) ->
    py_state:decr(Key, Amount).

%%% ============================================================================
%%% Module Reload
%%% ============================================================================

%% @doc Reload a Python module across all workers.
%% This uses importlib.reload() to refresh the module from disk.
%% Useful during development when Python code changes.
%%
%% Note: This only affects already-imported modules. If the module
%% hasn't been imported in a worker yet, the reload is a no-op for that worker.
%%
%% Example:
%% ```
%% %% After modifying mymodule.py on disk:
%% ok = py:reload(mymodule).
%% '''
%%
%% Returns ok if reload succeeded in all workers, or {error, Reasons}
%% if any workers failed.
-spec reload(py_module()) -> ok | {error, [{worker, term()}]}.
reload(Module) ->
    ModuleBin = ensure_binary(Module),
    %% Build Python code that:
    %% 1. Checks if module is loaded in sys.modules
    %% 2. If yes, reloads it with importlib.reload()
    %% 3. Returns the module name or None if not loaded
    Code = <<"__import__('importlib').reload(__import__('sys').modules['",
             ModuleBin/binary,
             "']) if '", ModuleBin/binary, "' in __import__('sys').modules else None">>,
    %% Broadcast to all workers
    Request = {eval, undefined, undefined, Code, #{}},
    Results = py_pool:broadcast(Request),
    %% Check if any failed
    Errors = lists:filtermap(fun
        ({ok, _}) -> false;
        ({error, Reason}) -> {true, Reason}
    end, Results),
    case Errors of
        [] -> ok;
        _ -> {error, [{worker, E} || E <- Errors]}
    end.

%%% ============================================================================
%%% Context Affinity API
%%% ============================================================================

%% @doc Bind current process to a dedicated Python worker.
%% All subsequent py:call/eval/exec operations from this process will use
%% the same worker, preserving Python state (variables, imports) across calls.
%%
%% Example:
%% ```
%% ok = py:bind(),
%% ok = py:exec(<<"x = 42">>),
%% {ok, 42} = py:eval(<<"x">>),  % Same worker, x persists
%% ok = py:unbind().
%% '''
-spec bind() -> ok | {error, term()}.
bind() ->
    Key = {process, self()},
    case py_pool:lookup_binding(Key) of
        {ok, _} -> ok;  % Already bound
        not_found ->
            case py_pool:checkout(Key) of
                {ok, _} -> ok;
                Error -> Error
            end
    end.

%% @doc Create an explicit context with a dedicated worker.
%% Returns a context handle that can be passed to call/eval/exec variants.
%% Multiple contexts can exist per process.
%%
%% Example:
%% ```
%% {ok, Ctx1} = py:bind(new),
%% {ok, Ctx2} = py:bind(new),
%% ok = py:exec(Ctx1, <<"x = 1">>),
%% ok = py:exec(Ctx2, <<"x = 2">>),
%% {ok, 1} = py:eval(Ctx1, <<"x">>),  % Isolated
%% {ok, 2} = py:eval(Ctx2, <<"x">>),  % Isolated
%% ok = py:unbind(Ctx1),
%% ok = py:unbind(Ctx2).
%% '''
-spec bind(new) -> {ok, py_ctx()} | {error, term()}.
bind(new) ->
    Ref = make_ref(),
    Key = {context, Ref},
    case py_pool:checkout(Key) of
        {ok, _} -> {ok, #py_ctx{ref = Ref}};
        Error -> Error
    end.

%% @doc Release bound worker for current process.
-spec unbind() -> ok.
unbind() ->
    py_pool:checkin({process, self()}).

%% @doc Release explicit context's worker.
-spec unbind(py_ctx()) -> ok.
unbind(#py_ctx{ref = Ref}) ->
    py_pool:checkin({context, Ref}).

%% @doc Check if current process is bound.
-spec is_bound() -> boolean().
is_bound() ->
    case py_pool:lookup_binding({process, self()}) of
        {ok, _} -> true;
        not_found -> false
    end.

%% @doc Execute function with temporary bound context.
%% Automatically binds before and unbinds after (even on exception).
%%
%% With arity-0 function (uses implicit process binding):
%% ```
%% Result = py:with_context(fun() ->
%%     ok = py:exec(<<"total = 0">>),
%%     ok = py:exec(<<"total += 1">>),
%%     py:eval(<<"total">>)
%% end).
%% %% {ok, 1}
%% '''
%%
%% With arity-1 function (receives explicit context):
%% ```
%% Result = py:with_context(fun(Ctx) ->
%%     ok = py:exec(Ctx, <<"x = 10">>),
%%     py:eval(Ctx, <<"x * 2">>)
%% end).
%% %% {ok, 20}
%% '''
-spec with_context(fun(() -> Result) | fun((py_ctx()) -> Result)) -> Result.
with_context(Fun) when is_function(Fun, 0) ->
    ok = bind(),
    try Fun()
    after unbind()
    end;
with_context(Fun) when is_function(Fun, 1) ->
    {ok, Ctx} = bind(new),
    try Fun(Ctx)
    after unbind(Ctx)
    end.

%% @doc Call with explicit context.
-spec ctx_call(py_ctx(), py_module(), py_func(), py_args()) -> py_result().
ctx_call(Ctx, Module, Func, Args) ->
    ctx_call(Ctx, Module, Func, Args, #{}).

%% @doc Call with explicit context and kwargs.
-spec ctx_call(py_ctx(), py_module(), py_func(), py_args(), py_kwargs()) -> py_result().
ctx_call(Ctx, Module, Func, Args, Kwargs) ->
    ctx_call(Ctx, Module, Func, Args, Kwargs, ?DEFAULT_TIMEOUT).

%% @doc Call with explicit context, kwargs, and timeout.
-spec ctx_call(py_ctx(), py_module(), py_func(), py_args(), py_kwargs(), timeout()) -> py_result().
ctx_call(#py_ctx{ref = CtxRef}, Module, Func, Args, Kwargs, Timeout) ->
    case py_semaphore:acquire(Timeout) of
        ok ->
            try
                Ref = make_ref(),
                TimeoutMs = py_util:normalize_timeout(Timeout, ?DEFAULT_TIMEOUT),
                Request = {call, Ref, self(), Module, Func, Args, Kwargs, TimeoutMs},
                case py_pool:lookup_binding({context, CtxRef}) of
                    {ok, Worker} -> py_pool:direct_request(Worker, Request);
                    not_found -> error(context_not_bound)
                end,
                await(Ref, Timeout)
            after
                py_semaphore:release()
            end;
        {error, max_concurrent} ->
            {error, {overloaded, py_semaphore:current(), py_semaphore:max_concurrent()}}
    end.

%% @doc Eval with explicit context.
-spec ctx_eval(py_ctx(), string() | binary()) -> py_result().
ctx_eval(Ctx, Code) ->
    ctx_eval(Ctx, Code, #{}).

%% @doc Eval with explicit context and locals.
-spec ctx_eval(py_ctx(), string() | binary(), map()) -> py_result().
ctx_eval(Ctx, Code, Locals) ->
    ctx_eval(Ctx, Code, Locals, ?DEFAULT_TIMEOUT).

%% @doc Eval with explicit context, locals, and timeout.
-spec ctx_eval(py_ctx(), string() | binary(), map(), timeout()) -> py_result().
ctx_eval(#py_ctx{ref = CtxRef}, Code, Locals, Timeout) ->
    Ref = make_ref(),
    TimeoutMs = py_util:normalize_timeout(Timeout, ?DEFAULT_TIMEOUT),
    Request = {eval, Ref, self(), Code, Locals, TimeoutMs},
    case py_pool:lookup_binding({context, CtxRef}) of
        {ok, Worker} -> py_pool:direct_request(Worker, Request);
        not_found -> error(context_not_bound)
    end,
    await(Ref, Timeout).

%% @doc Exec with explicit context.
-spec ctx_exec(py_ctx(), string() | binary()) -> ok | {error, term()}.
ctx_exec(#py_ctx{ref = CtxRef}, Code) ->
    Ref = make_ref(),
    Request = {exec, Ref, self(), Code},
    case py_pool:lookup_binding({context, CtxRef}) of
        {ok, Worker} -> py_pool:direct_request(Worker, Request);
        not_found -> error(context_not_bound)
    end,
    case await(Ref, ?DEFAULT_TIMEOUT) of
        {ok, _} -> ok;
        Error -> Error
    end.

%%% ============================================================================
%%% Logging and Tracing API
%%% ============================================================================

%% @doc Configure Python logging to forward to Erlang logger.
%% Uses default settings (debug level, default format).
-spec configure_logging() -> ok | {error, term()}.
configure_logging() ->
    configure_logging(#{}).

%% @doc Configure Python logging with options.
%% Options:
%%   level => debug | info | warning | error (default: debug)
%%   format => string() - Python format string (optional)
%%
%% Example:
%% ```
%% ok = py:configure_logging(#{level => info}).
%% '''
-spec configure_logging(map()) -> ok | {error, term()}.
configure_logging(Opts) ->
    Level = maps:get(level, Opts, debug),
    LevelInt = case Level of
        debug -> 10;
        info -> 20;
        warning -> 30;
        error -> 40;
        critical -> 50;
        _ -> 10
    end,
    Format = maps:get(format, Opts, undefined),
    %% Use __import__ for single-expression evaluation
    Code = case Format of
        undefined ->
            iolist_to_binary([
                "__import__('erlang').setup_logging(",
                integer_to_binary(LevelInt),
                ")"
            ]);
        F when is_binary(F) ->
            iolist_to_binary([
                "__import__('erlang').setup_logging(",
                integer_to_binary(LevelInt),
                ", '", F, "')"
            ]);
        F when is_list(F) ->
            iolist_to_binary([
                "__import__('erlang').setup_logging(",
                integer_to_binary(LevelInt),
                ", '", F, "')"
            ])
    end,
    case eval(Code) of
        {ok, _} -> ok;
        Error -> Error
    end.

%% @doc Enable distributed tracing from Python.
%% After enabling, Python code can create spans with erlang.Span().
-spec enable_tracing() -> ok.
enable_tracing() ->
    py_tracer:enable().

%% @doc Disable distributed tracing.
-spec disable_tracing() -> ok.
disable_tracing() ->
    py_tracer:disable().

%% @doc Get all collected trace spans.
%% Returns a list of span maps with keys:
%%   name, span_id, parent_id, start_time, end_time, duration_us,
%%   status, attributes, events
-spec get_traces() -> {ok, [map()]}.
get_traces() ->
    py_tracer:get_spans().

%% @doc Clear all collected trace spans.
-spec clear_traces() -> ok.
clear_traces() ->
    py_tracer:clear().
