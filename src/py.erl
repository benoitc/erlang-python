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
    exec/2,
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
    %% Logging and tracing
    configure_logging/0,
    configure_logging/1,
    enable_tracing/0,
    disable_tracing/0,
    get_traces/0,
    clear_traces/0,
    %% Process-per-context API (new architecture)
    context/0,
    context/1,
    start_contexts/0,
    start_contexts/1,
    stop_contexts/0,
    %% py_ref API (Python object references with auto-routing)
    call_method/3,
    getattr/2,
    to_term/1,
    is_ref/1
]).

-type py_result() :: {ok, term()} | {error, term()}.
-type py_ref() :: reference().
-type py_module() :: atom() | binary() | string().
-type py_func() :: atom() | binary() | string().
-type py_args() :: [term()].
-type py_kwargs() :: #{atom() | binary() => term()}.

-export_type([py_result/0, py_ref/0]).

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
%%
%% When the first argument is a pid (context), calls using the new
%% process-per-context architecture.
%%
%% @param CtxOrModule Context pid or Python module
%% @param ModuleOrFunc Python module or function name
%% @param FuncOrArgs Function name or arguments list
%% @param ArgsOrKwargs Arguments list or keyword arguments
-spec call(pid(), py_module(), py_func(), py_args()) -> py_result()
    ; (py_module(), py_func(), py_args(), py_kwargs()) -> py_result().
call(Ctx, Module, Func, Args) when is_pid(Ctx) ->
    py_context:call(Ctx, Module, Func, Args, #{});
call(Module, Func, Args, Kwargs) ->
    call(Module, Func, Args, Kwargs, ?DEFAULT_TIMEOUT).

%% @doc Call a Python function with keyword arguments and custom timeout.
%%
%% When the first argument is a pid (context), calls using the new
%% process-per-context architecture with options map.
%%
%% Timeout is in milliseconds. Use `infinity' for no timeout.
%% Rate limited via ETS-based semaphore to prevent overload.
-spec call(pid(), py_module(), py_func(), py_args(), map()) -> py_result()
    ; (py_module(), py_func(), py_args(), py_kwargs(), timeout()) -> py_result().
call(Ctx, Module, Func, Args, Opts) when is_pid(Ctx), is_map(Opts) ->
    Kwargs = maps:get(kwargs, Opts, #{}),
    Timeout = maps:get(timeout, Opts, infinity),
    py_context:call(Ctx, Module, Func, Args, Kwargs, Timeout);
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
%% Always route through context process - it handles callbacks inline using
%% suspension-based approach (no separate callback handler, no blocking)
do_call(Module, Func, Args, Kwargs, _Timeout) ->
    Ctx = py_context_router:get_context(),
    py_context:call(Ctx, Module, Func, Args, Kwargs).

%% @doc Evaluate a Python expression and return the result.
-spec eval(string() | binary()) -> py_result().
eval(Code) ->
    eval(Code, #{}).

%% @doc Evaluate a Python expression with local variables.
%%
%% When the first argument is a pid (context), evaluates using the new
%% process-per-context architecture.
-spec eval(pid(), string() | binary()) -> py_result()
    ; (string() | binary(), map()) -> py_result().
eval(Ctx, Code) when is_pid(Ctx) ->
    py_context:eval(Ctx, Code, #{});
eval(Code, Locals) ->
    eval(Code, Locals, ?DEFAULT_TIMEOUT).

%% @doc Evaluate a Python expression with local variables and timeout.
%%
%% When the first argument is a pid (context), evaluates using the new
%% process-per-context architecture with locals.
%%
%% Timeout is in milliseconds. Use `infinity' for no timeout.
-spec eval(pid(), string() | binary(), map()) -> py_result()
    ; (string() | binary(), map(), timeout()) -> py_result().
eval(Ctx, Code, Locals) when is_pid(Ctx), is_map(Locals) ->
    py_context:eval(Ctx, Code, Locals);
eval(Code, Locals, _Timeout) ->
    %% Always route through context process - it handles callbacks inline using
    %% suspension-based approach (no separate callback handler, no blocking)
    Ctx = py_context_router:get_context(),
    py_context:eval(Ctx, Code, Locals).

%% @doc Execute Python statements (no return value expected).
-spec exec(string() | binary()) -> ok | {error, term()}.
exec(Code) ->
    %% Always route through context process - it handles callbacks inline using
    %% suspension-based approach (no separate callback handler, no blocking)
    Ctx = py_context_router:get_context(),
    py_context:exec(Ctx, Code).

%% @doc Execute Python statements using a specific context.
%%
%% This is the explicit context variant of exec/1.
-spec exec(pid(), string() | binary()) -> ok | {error, term()}.
exec(Ctx, Code) when is_pid(Ctx) ->
    py_context:exec(Ctx, Code).

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
    %% Spawn a process to execute the call and return a ref
    Ref = make_ref(),
    Parent = self(),
    spawn(fun() ->
        Ctx = py_context_router:get_context(),
        Result = py_context:call(Ctx, Module, Func, Args, Kwargs),
        Parent ! {py_response, Ref, Result}
    end),
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
    %% Route through the new process-per-context system
    %% Create the generator and collect all values using list()
    Ctx = py_context_router:get_context(),
    ModuleBin = ensure_binary(Module),
    FuncBin = ensure_binary(Func),
    %% Build code that calls the function and collects all yielded values
    KwargsCode = format_kwargs(Kwargs),
    ArgsCode = format_args(Args),
    Code = iolist_to_binary([
        <<"list(__import__('">>, ModuleBin, <<"').">>, FuncBin,
        <<"(">>, ArgsCode, KwargsCode, <<"))">>
    ]),
    py_context:eval(Ctx, Code, #{}).

%% @private Format arguments for Python code
format_args([]) -> <<>>;
format_args(Args) ->
    ArgStrs = [format_arg(A) || A <- Args],
    iolist_to_binary(lists:join(<<", ">>, ArgStrs)).

%% @private Format a single argument
format_arg(A) when is_integer(A) -> integer_to_binary(A);
format_arg(A) when is_float(A) -> float_to_binary(A);
format_arg(A) when is_binary(A) -> <<"'", A/binary, "'">>;
format_arg(A) when is_atom(A) -> <<"'", (atom_to_binary(A))/binary, "'">>;
format_arg(A) when is_list(A) -> iolist_to_binary([<<"[">>, format_args(A), <<"]">>]);
format_arg(_) -> <<"None">>.

%% @private Format kwargs for Python code
format_kwargs(Kwargs) when map_size(Kwargs) == 0 -> <<>>;
format_kwargs(Kwargs) ->
    KwList = maps:fold(fun(K, V, Acc) ->
        KB = if is_atom(K) -> atom_to_binary(K); is_binary(K) -> K end,
        [<<KB/binary, "=", (format_arg(V))/binary>> | Acc]
    end, [], Kwargs),
    iolist_to_binary([<<", ">>, lists:join(<<", ">>, KwList)]).

%% @doc Stream results from a Python generator expression.
%% Evaluates the expression and if it returns a generator, streams all values.
-spec stream_eval(string() | binary()) -> py_result().
stream_eval(Code) ->
    stream_eval(Code, #{}).

%% @doc Stream results from a Python generator expression with local variables.
-spec stream_eval(string() | binary(), map()) -> py_result().
stream_eval(Code, Locals) ->
    %% Route through the new process-per-context system
    %% Wrap the code in list() to collect generator values
    Ctx = py_context_router:get_context(),
    CodeBin = ensure_binary(Code),
    WrappedCode = <<"list(", CodeBin/binary, ")">>,
    py_context:eval(Ctx, WrappedCode, Locals).

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
    Ref = make_ref(),
    py_async_pool:request({async_call, Ref, self(), Module, Func, Args, Kwargs}),
    Ref.

%% @doc Wait for an async call to complete.
-spec async_await(py_ref()) -> py_result().
async_await(Ref) ->
    await(Ref, ?DEFAULT_TIMEOUT).

%% @doc Wait for an async call with timeout.
%% Note: Identical to await/2 - provided for API symmetry with async_call.
-spec async_await(py_ref(), timeout()) -> py_result().
async_await(Ref, Timeout) ->
    await(Ref, Timeout).

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
    %% Distribute calls across available contexts for true parallel execution
    NumContexts = py_context_router:num_contexts(),
    Parent = self(),
    Ref = make_ref(),

    %% Spawn processes to execute calls in parallel
    CallsWithIdx = lists:zip(lists:seq(1, length(Calls)), Calls),
    _ = [spawn(fun() ->
        %% Distribute calls round-robin across contexts
        CtxIdx = ((Idx - 1) rem NumContexts) + 1,
        Ctx = py_context_router:get_context(CtxIdx),
        Result = py_context:call(Ctx, M, F, A, #{}),
        Parent ! {Ref, Idx, Result}
    end) || {Idx, {M, F, A}} <- CallsWithIdx],

    %% Collect results in order
    Results = [receive
        {Ref, Idx, Result} -> {Idx, Result}
    after ?DEFAULT_TIMEOUT ->
        {Idx, {error, timeout}}
    end || {Idx, _} <- CallsWithIdx],

    %% Sort by index and extract results
    SortedResults = [R || {_, R} <- lists:keysort(1, Results)],

    %% Check if all succeeded
    case lists:all(fun({ok, _}) -> true; (_) -> false end, SortedResults) of
        true ->
            {ok, [V || {ok, V} <- SortedResults]};
        false ->
            %% Return first error or all results
            case lists:keyfind(error, 1, SortedResults) of
                {error, _} = Err -> Err;
                false -> {ok, SortedResults}
            end
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

%% @doc Reload a Python module across all contexts.
%% This uses importlib.reload() to refresh the module from disk.
%% Useful during development when Python code changes.
%%
%% Note: This only affects already-imported modules. If the module
%% hasn't been imported in a context yet, the reload is a no-op for that context.
%%
%% Example:
%% ```
%% %% After modifying mymodule.py on disk:
%% ok = py:reload(mymodule).
%% '''
%%
%% Returns ok if reload succeeded in all contexts, or {error, Reasons}
%% if any contexts failed.
-spec reload(py_module()) -> ok | {error, [{context, term()}]}.
reload(Module) ->
    ModuleBin = ensure_binary(Module),
    %% Build Python code that:
    %% 1. Checks if module is loaded in sys.modules
    %% 2. If yes, reloads it with importlib.reload()
    %% 3. Returns the module name or None if not loaded
    Code = <<"__import__('importlib').reload(__import__('sys').modules['",
             ModuleBin/binary,
             "']) if '", ModuleBin/binary, "' in __import__('sys').modules else None">>,
    %% Broadcast to all contexts
    NumContexts = py_context_router:num_contexts(),
    Results = [begin
        Ctx = py_context_router:get_context(N),
        py_context:eval(Ctx, Code, #{})
    end || N <- lists:seq(1, NumContexts)],
    %% Check if any failed
    Errors = lists:filtermap(fun
        ({ok, _}) -> false;
        ({error, Reason}) -> {true, Reason}
    end, Results),
    case Errors of
        [] -> ok;
        _ -> {error, [{context, E} || E <- Errors]}
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

%%% ============================================================================
%%% Process-per-context API
%%%
%%% This new architecture uses one Erlang process per Python context.
%%% Each context owns its Python interpreter (subinterpreter on Python 3.12+
%%% or worker on older versions). This eliminates mutex contention and
%%% enables true N-way parallelism.
%%%
%%% Usage:
%%% ```
%%% %% Start the context system (usually done by the application)
%%% {ok, _} = py:start_contexts(),
%%%
%%% %% Get context for current scheduler (automatic routing)
%%% Ctx = py:context(),
%%% {ok, Result} = py:call(Ctx, math, sqrt, [16]),
%%%
%%% %% Or bind a specific context to this process
%%% ok = py:bind_context(py:context(1)),
%%% {ok, Result} = py:call(py:context(), math, sqrt, [16]).
%%% '''
%%% ============================================================================

%% @doc Start the process-per-context system with default settings.
%%
%% Creates one context per scheduler, using auto mode (subinterp on
%% Python 3.12+, worker otherwise).
%%
%% @returns {ok, [Context]} | {error, Reason}
-spec start_contexts() -> {ok, [pid()]} | {error, term()}.
start_contexts() ->
    py_context_router:start().

%% @doc Start the process-per-context system with options.
%%
%% Options:
%% - `contexts' - Number of contexts to create (default: number of schedulers)
%% - `mode' - Context mode: `auto', `subinterp', or `worker' (default: `auto')
%%
%% @param Opts Start options
%% @returns {ok, [Context]} | {error, Reason}
-spec start_contexts(map()) -> {ok, [pid()]} | {error, term()}.
start_contexts(Opts) ->
    py_context_router:start(Opts).

%% @doc Stop the process-per-context system.
-spec stop_contexts() -> ok.
stop_contexts() ->
    py_context_router:stop().

%% @doc Get the context for the current process.
%%
%% If the process has a bound context (via bind_context/1), returns that.
%% Otherwise, selects a context based on the current scheduler ID.
%%
%% This provides automatic load distribution across contexts while
%% maintaining scheduler affinity for cache locality.
%%
%% @returns Context pid
-spec context() -> pid().
context() ->
    py_context_router:get_context().

%% @doc Get a specific context by index.
%%
%% @param N Context index (1 to num_contexts)
%% @returns Context pid
-spec context(pos_integer()) -> pid().
context(N) ->
    py_context_router:get_context(N).

%%% ============================================================================
%%% py_ref API (Python object references with auto-routing)
%%%
%%% These functions work with py_ref references that carry both a Python
%%% object and the interpreter ID that created it. Method calls and
%%% attribute access are automatically routed to the correct context.
%%% ============================================================================

%% @doc Call a method on a Python object reference.
%%
%% The reference carries the interpreter ID, so the call is automatically
%% routed to the correct context.
%%
%% Example:
%% ```
%% {ok, Ref} = py:call(Ctx, builtins, list, [[1,2,3]], #{return => ref}),
%% {ok, 3} = py:call_method(Ref, '__len__', []).
%% '''
%%
%% @param Ref py_ref reference
%% @param Method Method name
%% @param Args Arguments list
%% @returns {ok, Result} | {error, Reason}
-spec call_method(reference(), atom() | binary(), list()) -> py_result().
call_method(Ref, Method, Args) ->
    MethodBin = ensure_binary(Method),
    py_nif:ref_call_method(Ref, MethodBin, Args).

%% @doc Get an attribute from a Python object reference.
%%
%% @param Ref py_ref reference
%% @param Name Attribute name
%% @returns {ok, Value} | {error, Reason}
-spec getattr(reference(), atom() | binary()) -> py_result().
getattr(Ref, Name) ->
    NameBin = ensure_binary(Name),
    py_nif:ref_getattr(Ref, NameBin).

%% @doc Convert a Python object reference to an Erlang term.
%%
%% @param Ref py_ref reference
%% @returns {ok, Term} | {error, Reason}
-spec to_term(reference()) -> py_result().
to_term(Ref) ->
    py_nif:ref_to_term(Ref).

%% @doc Check if a term is a py_ref reference.
%%
%% @param Term Term to check
%% @returns true | false
-spec is_ref(term()) -> boolean().
is_ref(Term) ->
    py_nif:is_ref(Term).

