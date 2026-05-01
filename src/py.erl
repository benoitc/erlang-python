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
    cast/3,
    cast/4,
    cast/5,
    spawn_call/3,
    spawn_call/4,
    spawn_call/5,
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
    stream_start/3,
    stream_start/4,
    stream_cancel/1,
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
    async_gather/2,
    %% Parallel execution + capability probe
    parallel/1,
    subinterp_supported/0,
    %% Virtual environment
    ensure_venv/2,
    ensure_venv/3,
    activate_venv/1,
    %% Process-local Python environment
    get_local_env/1,
    deactivate_venv/0,
    venv_info/0,
    %% Execution info
    execution_mode/0,
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
    contexts_started/0,
    %% py_ref API (Python object references with auto-routing)
    call_method/3,
    getattr/2,
    to_term/1,
    is_ref/1,
    %% File descriptor utilities
    dup_fd/1,
    %% Pool registration API
    register_pool/2,
    unregister_pool/1,
    %% SharedDict API - process-scoped shared dictionary
    shared_dict_new/0,
    shared_dict_get/2,
    shared_dict_get/3,
    shared_dict_set/3,
    shared_dict_del/2,
    shared_dict_keys/1,
    shared_dict_destroy/1
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

%% Process dictionary key for local Python environment
-define(LOCAL_ENV_KEY, py_local_env).

%% @doc Get or create a process-local Python environment for a context.
%%
%% Each Erlang process can have Python environments per interpreter.
%% The environments are stored in the process dictionary keyed by interpreter ID
%% and are automatically freed when the process exits.
%%
%% The environment is created inside the context's interpreter to ensure
%% the correct memory allocator is used. This is critical for subinterpreters
%% where each interpreter has its own memory allocator.
%%
%% @param Ctx Context pid
%% @returns EnvRef - NIF resource reference to the Python environment
-spec get_local_env(pid()) -> reference().
get_local_env(Ctx) when is_pid(Ctx) ->
    {ok, InterpId} = py_context:get_interp_id(Ctx),
    Envs = case get(?LOCAL_ENV_KEY) of
        undefined -> #{};
        M when is_map(M) -> M;
        %% Handle legacy single-ref format (shouldn't happen but be safe)
        _OldRef -> #{}
    end,
    case maps:get(InterpId, Envs, undefined) of
        undefined ->
            {ok, Ref} = py_context:create_local_env(Ctx),
            put(?LOCAL_ENV_KEY, Envs#{InterpId => Ref}),
            Ref;
        Ref ->
            Ref
    end.

%%% ============================================================================
%%% Synchronous API
%%% ============================================================================

%% @doc Call a Python function synchronously.
%%
%% In worker mode, the call uses the process-local Python environment,
%% allowing access to functions defined via py:exec() in the same process.
-spec call(py_module(), py_func(), py_args()) -> py_result().
call(Module, Func, Args) ->
    call(Module, Func, Args, #{}).

%% @doc Call a Python function with keyword arguments or on a named pool.
%%
%% This function has multiple signatures:
%% - `call(Ctx, Module, Func, Args)' - Call using a specific context pid
%% - `call(Pool, Module, Func, Args)' - Call using a named pool (default, io, etc.)
%% - `call(Module, Func, Args, Kwargs)' - Call with keyword arguments on default pool
%%
%% In worker mode, calls use the process-local Python environment.
%%
%% @param CtxOrPoolOrModule Context pid, pool name, or Python module
%% @param ModuleOrFunc Python module or function name
%% @param FuncOrArgs Function name or arguments list
%% @param ArgsOrKwargs Arguments list or keyword arguments
-spec call(pid(), py_module(), py_func(), py_args()) -> py_result()
    ; (py_context_router:pool_name(), py_module(), py_func(), py_args()) -> py_result()
    ; (py_module(), py_func(), py_args(), py_kwargs()) -> py_result().
call(Ctx, Module, Func, Args) when is_pid(Ctx) ->
    EnvRef = get_local_env(Ctx),
    py_context:call(Ctx, Module, Func, Args, #{}, infinity, EnvRef);
call(Pool, Module, Func, Args) when is_atom(Pool), is_atom(Func) ->
    %% Pool-based call (e.g., py:call(io, math, sqrt, [16]))
    call(Pool, Module, Func, Args, #{});
call(Module, Func, Args, Kwargs) ->
    call(Module, Func, Args, Kwargs, ?DEFAULT_TIMEOUT).

%% @doc Call a Python function with keyword arguments and optional timeout or pool.
%%
%% This function has multiple signatures:
%% - `call(Ctx, Module, Func, Args, Opts)' - Call using context with options map
%% - `call(Pool, Module, Func, Args, Kwargs)' - Call using named pool with kwargs
%% - `call(Module, Func, Args, Kwargs, Timeout)' - Call on default pool with timeout
%%
%% Timeout is in milliseconds. Use `infinity' for no timeout.
%% Rate limited via ETS-based semaphore to prevent overload.
-spec call(pid(), py_module(), py_func(), py_args(), map()) -> py_result()
    ; (py_context_router:pool_name(), py_module(), py_func(), py_args(), py_kwargs()) -> py_result()
    ; (py_module(), py_func(), py_args(), py_kwargs(), timeout()) -> py_result().
call(Ctx, Module, Func, Args, Opts) when is_pid(Ctx), is_map(Opts) ->
    Kwargs = maps:get(kwargs, Opts, #{}),
    Timeout = maps:get(timeout, Opts, infinity),
    EnvRef = get_local_env(Ctx),
    py_context:call(Ctx, Module, Func, Args, Kwargs, Timeout, EnvRef);
call(Pool, Module, Func, Args, Kwargs) when is_atom(Pool), is_atom(Func), is_map(Kwargs) ->
    %% Pool-based call with kwargs (e.g., py:call(io, math, pow, [2, 3], #{round => true}))
    do_pool_call(Pool, Module, Func, Args, Kwargs, ?DEFAULT_TIMEOUT);
call(Module, Func, Args, Kwargs, Timeout) ->
    %% Look up which pool to use based on registered module/function
    Pool = py_context_router:lookup_pool(Module, Func),
    do_pool_call(Pool, Module, Func, Args, Kwargs, Timeout).

%% @private
%% Call using a named pool with semaphore protection
%% Uses the process-local environment from the calling process
do_pool_call(Pool, Module, Func, Args, Kwargs, Timeout) ->
    case py_semaphore:acquire(Timeout) of
        ok ->
            try
                Ctx = py_context_router:get_context(Pool),
                EnvRef = get_local_env(Ctx),
                py_context:call(Ctx, Module, Func, Args, Kwargs, Timeout, EnvRef)
            after
                py_semaphore:release()
            end;
        {error, max_concurrent} ->
            {error, {overloaded, py_semaphore:current(), py_semaphore:max_concurrent()}}
    end.

%% @doc Evaluate a Python expression and return the result.
%%
%% In worker mode, evaluation uses the process-local Python environment.
%% Variables defined via exec are visible in eval within the same process.
-spec eval(string() | binary()) -> py_result().
eval(Code) ->
    eval(Code, #{}).

%% @doc Evaluate a Python expression with local variables.
%%
%% When the first argument is a pid (context), evaluates using the new
%% process-per-context architecture with process-local environment.
-spec eval(pid(), string() | binary()) -> py_result()
    ; (string() | binary(), map()) -> py_result().
eval(Ctx, Code) when is_pid(Ctx) ->
    EnvRef = get_local_env(Ctx),
    py_context:eval(Ctx, Code, #{}, infinity, EnvRef);
eval(Code, Locals) ->
    eval(Code, Locals, ?DEFAULT_TIMEOUT).

%% @doc Evaluate a Python expression with local variables and timeout.
%%
%% When the first argument is a pid (context), evaluates using the new
%% process-per-context architecture with process-local environment.
%%
%% Timeout is in milliseconds. Use `infinity' for no timeout.
-spec eval(pid(), string() | binary(), map()) -> py_result()
    ; (string() | binary(), map(), timeout()) -> py_result().
eval(Ctx, Code, Locals) when is_pid(Ctx), is_map(Locals) ->
    EnvRef = get_local_env(Ctx),
    py_context:eval(Ctx, Code, Locals, infinity, EnvRef);
eval(Code, Locals, Timeout) ->
    %% Always route through context process - it handles callbacks inline using
    %% suspension-based approach (no separate callback handler, no blocking)
    Ctx = py_context_router:get_context(),
    EnvRef = get_local_env(Ctx),
    py_context:eval(Ctx, Code, Locals, Timeout, EnvRef).

%% @doc Execute Python statements (no return value expected).
%%
%% In worker mode, the code runs in a process-local Python environment.
%% Variables defined via exec persist within the calling Erlang process.
%% In owngil mode, each context has its own isolated namespace.
-spec exec(string() | binary()) -> ok | {error, term()}.
exec(Code) ->
    %% Always route through context process - it handles callbacks inline using
    %% suspension-based approach (no separate callback handler, no blocking)
    Ctx = py_context_router:get_context(),
    EnvRef = get_local_env(Ctx),
    py_context:exec(Ctx, Code, EnvRef).

%% @doc Execute Python statements using a specific context.
%%
%% This is the explicit context variant of exec/1.
%% Uses the process-local environment for the calling process.
-spec exec(pid(), string() | binary()) -> ok | {error, term()}.
exec(Ctx, Code) when is_pid(Ctx) ->
    EnvRef = get_local_env(Ctx),
    py_context:exec(Ctx, Code, EnvRef).

%%% ============================================================================
%%% Asynchronous API
%%% ============================================================================

%% @doc Fire-and-forget Python function call.
-spec cast(py_module(), py_func(), py_args()) -> ok.
cast(Module, Func, Args) ->
    cast(Module, Func, Args, #{}).

%% @doc Fire-and-forget Python function call with context or kwargs.
-spec cast(pid(), py_module(), py_func(), py_args()) -> ok;
          (py_module(), py_func(), py_args(), py_kwargs()) -> ok.
cast(Ctx, Module, Func, Args) when is_pid(Ctx) ->
    cast(Ctx, Module, Func, Args, #{});
cast(Module, Func, Args, Kwargs) ->
    spawn(fun() ->
        Ctx = py_context_router:get_context(),
        _ = py_context:call(Ctx, Module, Func, Args, Kwargs)
    end),
    ok.

%% @doc Fire-and-forget Python function call with context and kwargs.
-spec cast(pid(), py_module(), py_func(), py_args(), py_kwargs()) -> ok.
cast(Ctx, Module, Func, Args, Kwargs) when is_pid(Ctx) ->
    spawn(fun() ->
        _ = py_context:call(Ctx, Module, Func, Args, Kwargs)
    end),
    ok.

%% @doc Spawn a Python function call, returns immediately with a ref.
-spec spawn_call(py_module(), py_func(), py_args()) -> py_ref().
spawn_call(Module, Func, Args) ->
    spawn_call(Module, Func, Args, #{}).

%% @doc Spawn a Python function call with context or kwargs.
-spec spawn_call(pid(), py_module(), py_func(), py_args()) -> py_ref();
                (py_module(), py_func(), py_args(), py_kwargs()) -> py_ref().
spawn_call(Ctx, Module, Func, Args) when is_pid(Ctx) ->
    spawn_call(Ctx, Module, Func, Args, #{});
spawn_call(Module, Func, Args, Kwargs) ->
    Ref = make_ref(),
    Parent = self(),
    spawn(fun() ->
        Ctx = py_context_router:get_context(),
        Result = py_context:call(Ctx, Module, Func, Args, Kwargs),
        Parent ! {py_response, Ref, Result}
    end),
    Ref.

%% @doc Spawn a Python function call with context and kwargs.
-spec spawn_call(pid(), py_module(), py_func(), py_args(), py_kwargs()) -> py_ref().
spawn_call(Ctx, Module, Func, Args, Kwargs) when is_pid(Ctx) ->
    Ref = make_ref(),
    Parent = self(),
    spawn(fun() ->
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
stream(Module, Func, Args, Kwargs) when map_size(Kwargs) == 0 ->
    %% No kwargs - use stream_start and collect results
    {ok, Ref} = stream_start(Module, Func, Args),
    collect_stream(Ref, []);
stream(Module, Func, Args, Kwargs) ->
    %% With kwargs - use eval approach
    Ctx = py_context_router:get_context(),
    ModuleBin = ensure_binary(Module),
    FuncBin = ensure_binary(Func),
    KwargsCode = format_kwargs(Kwargs),
    ArgsCode = format_args(Args),
    Code = iolist_to_binary([
        <<"list(__import__('">>, ModuleBin, <<"').">>, FuncBin,
        <<"(">>, ArgsCode, KwargsCode, <<"))">>
    ]),
    py_context:eval(Ctx, Code, #{}).

%% @private Collect all stream events into a list
collect_stream(Ref, Acc) ->
    receive
        {py_stream, Ref, {data, Value}} ->
            collect_stream(Ref, [Value | Acc]);
        {py_stream, Ref, done} ->
            {ok, lists:reverse(Acc)};
        {py_stream, Ref, {error, Reason}} ->
            {error, Reason}
    after 30000 ->
        {error, timeout}
    end.

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
%%% True Streaming API (Event-driven)
%%% ============================================================================

%% @doc Start a true streaming iteration from a Python generator.
%%
%% Unlike stream/3,4 which collects all values at once, this function
%% returns immediately with a reference and sends values as events
%% to the calling process as they are yielded.
%%
%% Events sent to the owner process:
%% - `{py_stream, Ref, {data, Value}}' - Each yielded value
%% - `{py_stream, Ref, done}' - Stream completed
%% - `{py_stream, Ref, {error, Reason}}' - Stream error
%%
%% Supports both sync generators and async generators (coroutines).
%%
%% Example:
%% ```
%% {ok, Ref} = py:stream_start(builtins, iter, [[1,2,3,4,5]]),
%% receive_loop(Ref).
%%
%% receive_loop(Ref) ->
%%     receive
%%         {py_stream, Ref, {data, Value}} ->
%%             io:format("Got: ~p~n", [Value]),
%%             receive_loop(Ref);
%%         {py_stream, Ref, done} ->
%%             io:format("Complete~n");
%%         {py_stream, Ref, {error, Reason}} ->
%%             io:format("Error: ~p~n", [Reason])
%%     after 30000 ->
%%         timeout
%%     end.
%% '''
-spec stream_start(py_module(), py_func(), py_args()) -> {ok, reference()}.
stream_start(Module, Func, Args) ->
    stream_start(Module, Func, Args, #{}).

%% @doc Start a true streaming iteration with options.
%%
%% Options:
%% - `owner => pid()' - Process to receive events (default: self())
%%
%% @param Module Python module name
%% @param Func Python function name
%% @param Args Function arguments
%% @param Opts Options map
%% @returns {ok, Ref} where Ref is used to identify stream events
-spec stream_start(py_module(), py_func(), py_args(), map()) -> {ok, reference()}.
stream_start(Module, Func, Args, Opts) ->
    Owner = maps:get(owner, Opts, self()),
    Ref = make_ref(),
    ModuleBin = ensure_binary(Module),
    FuncBin = ensure_binary(Func),
    RefHash = erlang:phash2(Ref),
    %% Store owner and ref for Python to retrieve
    %% Use binary keys because Python strings become binaries
    py_state:store({<<"stream_owner">>, RefHash}, Owner),
    py_state:store({<<"stream_ref">>, RefHash}, Ref),
    py_state:store({<<"stream_args">>, RefHash}, Args),
    %% Spawn an Erlang process to run the streaming iteration
    spawn(fun() ->
        stream_run_python(ModuleBin, FuncBin, RefHash)
    end),
    {ok, Ref}.

%% @private Run the streaming via Python code
stream_run_python(ModuleBin, FuncBin, RefHash) ->
    RefHashBin = integer_to_binary(RefHash),
    %% Build Python code that streams values using callbacks
    Code = iolist_to_binary([
        <<"import erlang\n">>,
        <<"_rh = ">>, RefHashBin, <<"\n">>,
        <<"_args = erlang.call('state_get', ('stream_args', _rh))\n">>,
        <<"if _args is None:\n">>,
        <<"    _args = []\n">>,
        <<"try:\n">>,
        <<"    _mod = __import__('">>, ModuleBin, <<"')\n">>,
        <<"    _fn = getattr(_mod, '">>, FuncBin, <<"')\n">>,
        <<"    _gen = _fn(*_args) if _args else _fn()\n">>,
        <<"    for _val in _gen:\n">>,
        <<"        if erlang.call('_py_stream_cancelled', _rh):\n">>,
        <<"            erlang.call('_py_stream_send', _rh, 'error', 'cancelled')\n">>,
        <<"            break\n">>,
        <<"        erlang.call('_py_stream_send', _rh, 'data', _val)\n">>,
        <<"    else:\n">>,
        <<"        erlang.call('_py_stream_send', _rh, 'done', None)\n">>,
        <<"except Exception as _e:\n">>,
        <<"    erlang.call('_py_stream_send', _rh, 'error', str(_e))\n">>,
        <<"finally:\n">>,
        <<"    erlang.call('_py_stream_cleanup', _rh)\n">>
    ]),
    %% Execute the streaming code
    case exec(Code) of
        ok -> ok;
        {error, Reason} ->
            %% Try to notify owner of error
            case py_state:fetch({<<"stream_owner">>, RefHash}) of
                {ok, Owner} ->
                    case py_state:fetch({<<"stream_ref">>, RefHash}) of
                        {ok, Ref} ->
                            Owner ! {py_stream, Ref, {error, Reason}},
                            py_state:remove({<<"stream_owner">>, RefHash}),
                            py_state:remove({<<"stream_ref">>, RefHash}),
                            py_state:remove({<<"stream_args">>, RefHash});
                        _ -> ok
                    end;
                _ -> ok
            end
    end.

%% @doc Cancel an active stream.
%%
%% Sends a cancellation signal to stop the stream iteration.
%% Any pending values may still be delivered before the stream stops.
%%
%% @param Ref The stream reference from stream_start/3,4
%% @returns ok
-spec stream_cancel(reference()) -> ok.
stream_cancel(Ref) when is_reference(Ref) ->
    %% Store cancellation flag that the streaming task checks
    %% Use hash because we can't pass Erlang refs to Python callbacks easily
    %% Use binary key because Python strings become binaries
    RefHash = erlang:phash2(Ref),
    py_state:store({<<"stream_cancelled_hash">>, RefHash}, true),
    ok.

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
    py_event_loop:create_task(Module, Func, Args, Kwargs).

%% @doc Wait for an async call to complete.
-spec async_await(py_ref()) -> py_result().
async_await(Ref) ->
    async_await(Ref, ?DEFAULT_TIMEOUT).

%% @doc Wait for an async call with timeout.
-spec async_await(py_ref(), timeout()) -> py_result().
async_await(Ref, Timeout) ->
    py_event_loop:await(Ref, Timeout).

%% @doc Execute multiple async Python calls concurrently.
%%
%% Each call is submitted to the event loop independently, so they run
%% concurrently. Results are collected in the order of the input list.
%% Sync functions are accepted and resolve immediately (the event loop
%% short-circuits non-coroutines).
%%
%% Returns `{ok, [Result1, Result2, ...]}' when every call succeeds, where
%% each `ResultN' is the value returned by the corresponding call.
%% Returns `{error, {gather_failed, Errors}}' if any call fails, where
%% `Errors' is a list of `{Index, Reason}' tuples for each failure.
%%
%% Example:
%% ```
%% {ok, [R1, R2, R3]} = py:async_gather([
%%     {aiohttp, get, [Url1]},
%%     {aiohttp, get, [Url2]},
%%     {aiohttp, get, [Url3]}
%% ]).
%% '''
-spec async_gather([{py_module(), py_func(), py_args()}]) -> py_result().
async_gather(Calls) ->
    async_gather(Calls, ?DEFAULT_TIMEOUT).

%% @doc Like async_gather/1 with explicit per-call timeout.
-spec async_gather([{py_module(), py_func(), py_args()}], timeout()) -> py_result().
async_gather(Calls, Timeout) when is_list(Calls) ->
    Refs = [async_call(M, F, A) || {M, F, A} <- Calls],
    Results = [async_await(R, Timeout) || R <- Refs],
    Errors = [{Idx, Reason}
              || {Idx, {error, Reason}} <- lists:zip(lists:seq(1, length(Results)), Results)],
    case Errors of
        [] ->
            Values = [V || {ok, V} <- Results],
            {ok, Values};
        _ ->
            {error, {gather_failed, Errors}}
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

%% @doc Ensure a virtual environment exists and activate it.
%%
%% Creates a venv at `Path' if it doesn't exist, installs dependencies from
%% `RequirementsFile', and activates the venv.
%%
%% RequirementsFile can be:
%% - `"requirements.txt"' - standard pip requirements file
%% - `"pyproject.toml"' - PEP 621 project file (installs with -e .)
%%
%% Example:
%% ```
%% ok = py:ensure_venv("priv/venv", "requirements.txt").
%% '''
-spec ensure_venv(string() | binary(), string() | binary()) -> ok | {error, term()}.
ensure_venv(Path, RequirementsFile) ->
    ensure_venv(Path, RequirementsFile, []).

%% @doc Ensure a virtual environment exists with options.
%%
%% Options:
%% - `{extras, [string()]}' - Install optional dependencies (pyproject.toml)
%% - `{installer, uv | pip}' - Package installer (default: auto-detect)
%% - `{python, string()}' - Python executable for venv creation
%% - `force' - Recreate venv even if it exists
%%
%% Example:
%% ```
%% %% With pyproject.toml and dev extras
%% ok = py:ensure_venv("priv/venv", "pyproject.toml", [
%%     {extras, ["dev", "test"]}
%% ]).
%%
%% %% Force uv installer
%% ok = py:ensure_venv("priv/venv", "requirements.txt", [
%%     {installer, uv}
%% ]).
%% '''
-spec ensure_venv(string() | binary(), string() | binary(), list()) -> ok | {error, term()}.
ensure_venv(Path, RequirementsFile, Opts) ->
    PathStr = to_string(Path),
    ReqFileStr = to_string(RequirementsFile),
    Force = proplists:get_bool(force, Opts),
    %% Create venv if needed
    VenvReady = case venv_exists(PathStr) of
        true when not Force ->
            ok;
        _ ->
            create_venv(PathStr, Opts)
    end,
    case VenvReady of
        ok ->
            %% Always install/update dependencies (pip/uv skip existing)
            case install_deps(PathStr, ReqFileStr, Opts) of
                ok ->
                    activate_venv(PathStr);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private Check if venv exists by looking for pyvenv.cfg
-spec venv_exists(string()) -> boolean().
venv_exists(Path) ->
    filelib:is_file(filename:join(Path, "pyvenv.cfg")).

%% @private Create a new virtual environment
-spec create_venv(string(), list()) -> ok | {error, term()}.
create_venv(Path, Opts) ->
    Installer = detect_installer(Opts),
    Python = case proplists:get_value(python, Opts, undefined) of
        undefined -> get_python_executable();
        P -> P
    end,
    Cmd = case Installer of
        uv ->
            %% uv venv is faster, use --python to match the running interpreter
            io_lib:format("uv venv --python ~s ~s", [quote(Python), quote(Path)]);
        pip ->
            io_lib:format("~s -m venv ~s", [quote(Python), quote(Path)])
    end,
    run_cmd(lists:flatten(Cmd)).

%% @private Get the Python executable path
%% When embedded, sys.executable returns the embedding app (beam.smp)
%% so we reconstruct the path from sys.prefix and version info
-spec get_python_executable() -> string().
get_python_executable() ->
    %% Use a single expression to find the Python executable
    %% Searches for pythonX.Y, python3, python in sys.prefix/bin (Unix)
    %% or python.exe in sys.prefix (Windows)
    Expr = <<"(lambda: (__import__('os').path.join(__import__('sys').prefix, 'python.exe') if __import__('sys').platform == 'win32' and __import__('os').path.isfile(__import__('os').path.join(__import__('sys').prefix, 'python.exe')) else next((p for p in [__import__('os').path.join(__import__('sys').prefix, 'bin', f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'), __import__('os').path.join(__import__('sys').prefix, 'bin', 'python3'), __import__('os').path.join(__import__('sys').prefix, 'bin', 'python')] if __import__('os').path.isfile(p)), 'python3')))()">>,
    case eval(Expr) of
        {ok, Path} when is_binary(Path) -> binary_to_list(Path);
        _ -> "python3"
    end.

%% @private Install dependencies from requirements file
-spec install_deps(string(), string(), list()) -> ok | {error, term()}.
install_deps(Path, RequirementsFile, Opts) ->
    Installer = detect_installer(Opts),
    PipPath = pip_path(Path, Installer),
    Extras = proplists:get_value(extras, Opts, []),

    %% Determine file type and build install command
    Cmd = case filename:extension(RequirementsFile) of
        ".txt" ->
            %% requirements.txt
            io_lib:format("~s install -r ~s", [PipPath, quote(RequirementsFile)]);
        ".toml" ->
            %% pyproject.toml - install as editable
            %% filename:dirname returns "." for files without directory component
            InstallPath = filename:dirname(RequirementsFile),
            case Extras of
                [] ->
                    io_lib:format("~s install -e ~s", [PipPath, quote(InstallPath)]);
                _ ->
                    ExtrasStr = string:join(Extras, ","),
                    io_lib:format("~s install -e \"~s[~s]\"", [PipPath, InstallPath, ExtrasStr])
            end;
        _ ->
            %% Assume requirements.txt format
            io_lib:format("~s install -r ~s", [PipPath, quote(RequirementsFile)])
    end,
    run_cmd(lists:flatten(Cmd)).

%% @private Detect which installer to use (uv or pip)
-spec detect_installer(list()) -> uv | pip.
detect_installer(Opts) ->
    case proplists:get_value(installer, Opts, auto) of
        auto ->
            case os:find_executable("uv") of
                false -> pip;
                _ -> uv
            end;
        Installer ->
            Installer
    end.

%% @private Get pip/uv pip command path
-spec pip_path(string(), uv | pip) -> string().
pip_path(VenvPath, uv) ->
    %% uv pip uses venv from env var or --python flag
    "VIRTUAL_ENV=" ++ quote(VenvPath) ++ " uv pip";
pip_path(VenvPath, pip) ->
    %% Use pip from the venv
    case os:type() of
        {win32, _} ->
            filename:join([VenvPath, "Scripts", "pip"]);
        _ ->
            filename:join([VenvPath, "bin", "pip"])
    end.

%% @private Quote a path for shell
-spec quote(string()) -> string().
quote(S) ->
    "'" ++ S ++ "'".

%% @private Run a shell command and return ok or error
-spec run_cmd(string()) -> ok | {error, term()}.
run_cmd(Cmd) ->
    %% Use os:cmd but check for errors
    Result = os:cmd(Cmd ++ " 2>&1; echo \"::exitcode::$?\""),
    %% Parse exit code from end of output
    case string:split(Result, "::exitcode::", trailing) of
        [Output, ExitCodeStr] ->
            case string:trim(ExitCodeStr) of
                "0" -> ok;
                Code -> {error, {exit_code, list_to_integer(Code), string:trim(Output)}}
            end;
        _ ->
            %% Fallback - assume success if no error marker
            ok
    end.

%% @private Convert to string
-spec to_string(string() | binary()) -> string().
to_string(B) when is_binary(B) -> binary_to_list(B);
to_string(S) when is_list(S) -> S.

%% @doc Activate a Python virtual environment.
%% This modifies sys.path to use packages from the specified venv.
%% The venv path should be the root directory (containing bin/lib folders).
%%
%% `.pth' files in the venv's site-packages directory are processed, so
%% editable installs created by uv, pip, or any PEP 517/660 compliant tool
%% work correctly.  New paths are inserted at the front of sys.path so that
%% venv packages take priority over system packages.
%%
%% Example:
%% ```
%% ok = py:activate_venv(<<"/path/to/myenv">>).
%% {ok, _} = py:call(sentence_transformers, 'SentenceTransformer', [<<"all-MiniLM-L6-v2">>]).
%% '''
-spec activate_venv(string() | binary()) -> ok | {error, term()}.
activate_venv(VenvPath) ->
    VenvBin = ensure_binary(VenvPath),
    %% Find site-packages directory dynamically (venv may use different Python version)
    %% Uses a single expression to avoid multiline code issues
    FindSitePackages = <<"(lambda vp: __import__('os').path.join(vp, 'Lib', 'site-packages') if __import__('os').path.exists(__import__('os').path.join(vp, 'Lib', 'site-packages')) else next((sp for name in (__import__('os').listdir(__import__('os').path.join(vp, 'lib')) if __import__('os').path.isdir(__import__('os').path.join(vp, 'lib')) else []) if name.startswith('python') for sp in [__import__('os').path.join(vp, 'lib', name, 'site-packages')] if __import__('os').path.isdir(sp)), None))(_venv_path)">>,
    case eval(FindSitePackages, #{<<"_venv_path">> => VenvBin}) of
        {ok, SitePackages} when SitePackages =/= none, SitePackages =/= null ->
            activate_venv_with_site_packages(VenvBin, SitePackages);
        {ok, _} ->
            {error, {invalid_venv, no_site_packages_found}};
        Error ->
            Error
    end.

%% @private Activate venv with known site-packages path
activate_venv_with_site_packages(VenvBin, SitePackages) ->
    %% Verify site-packages exists
    case eval(<<"__import__('os').path.isdir(sp)">>, #{sp => SitePackages}) of
        {ok, true} ->
            %% Save original path if not already saved
            {ok, _} = eval(<<"setattr(__import__('sys'), '_original_path', __import__('sys').path.copy()) if not hasattr(__import__('sys'), '_original_path') else None">>),
            %% Set venv info
            {ok, _} = eval(<<"setattr(__import__('sys'), '_active_venv', vp)">>, #{vp => VenvBin}),
            {ok, _} = eval(<<"setattr(__import__('sys'), '_venv_site_packages', sp)">>, #{sp => SitePackages}),
            %% Add site-packages and process .pth files (editable installs)
            %% Note: We embed the site-packages path directly since exec doesn't support
            %% variables and sys attributes may not persist across calls in subinterpreters
            SitePackagesStr = binary_to_list(SitePackages),
            ExecCode = iolist_to_binary([
                <<"import site as _site, sys as _sys\n">>,
                <<"_sp = '">>, escape_python_string(SitePackagesStr), <<"'\n">>,
                <<"_b = frozenset(_sys.path)\n">>,
                <<"_site.addsitedir(_sp)\n">>,
                <<"_sys.path[:] = [p for p in _sys.path if p not in _b] + [p for p in _sys.path if p in _b]\n">>,
                <<"del _site, _sys, _b, _sp\n">>
            ]),
            ok = exec(ExecCode),
            ok;
        {ok, false} ->
            {error, {invalid_venv, SitePackages}};
        Error ->
            Error
    end.

%% @private Escape a string for embedding in Python code
escape_python_string(Str) ->
    lists:flatmap(fun($') -> "\\'";
                     ($\\) -> "\\\\";
                     (C) -> [C]
                  end, Str).

%% @doc Deactivate the current virtual environment.
%% Restores sys.path to its original state.
-spec deactivate_venv() -> ok | {error, term()}.
deactivate_venv() ->
    case eval(<<"hasattr(__import__('sys'), '_original_path')">>) of
        {ok, true} ->
            ok = exec(<<"import sys as _sys\n"
                         "_sys.path[:] = _sys._original_path\n"
                         "del _sys\n">>),
            {ok, _} = eval(<<"delattr(__import__('sys'), '_original_path')">>),
            {ok, _} = eval(<<"delattr(__import__('sys'), '_active_venv') if hasattr(__import__('sys'), '_active_venv') else None">>),
            {ok, _} = eval(<<"delattr(__import__('sys'), '_venv_site_packages') if hasattr(__import__('sys'), '_venv_site_packages') else None">>),
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
    %% Check both attributes exist to handle partial activation/deactivation state
    Code = <<"({'active': True, 'venv_path': __import__('sys')._active_venv, 'site_packages': __import__('sys')._venv_site_packages, 'sys_path': __import__('sys').path} if (hasattr(__import__('sys'), '_active_venv') and hasattr(__import__('sys'), '_venv_site_packages')) else {'active': False})">>,
    eval(Code).

%% @private
ensure_binary(S) ->
    py_util:to_binary(S).

%%% ============================================================================
%%% Execution Info
%%% ============================================================================

%% @doc Get the current execution mode.
%% Returns one of:
%% - `worker': Contexts use dedicated pthread per context (default).
%%   Provides stable thread affinity for numpy/torch/tensorflow compatibility.
%% - `owngil': Contexts use dedicated pthread + subinterpreter with own GIL.
%%   Enables true parallelism (Python 3.12+ with subinterpreter support).
%%
%% The mode is determined by the `context_mode' application config:
%% ```
%% application:set_env(erlang_python, context_mode, owngil).
%% '''
-spec execution_mode() -> worker | owngil.
execution_mode() ->
    case application:get_env(erlang_python, context_mode, worker) of
        owngil -> owngil;
        _ -> worker
    end.

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
%%% %% Or bind a specific context to this process via the router
%%% ok = py_context_router:bind_context(py:context(1)),
%%% {ok, Result} = py:call(py:context(), math, sqrt, [16]).
%%% '''
%%% ============================================================================

%% @doc Start the process-per-context system with default settings.
%%
%% Creates one context per scheduler using worker mode.
%%
%% @returns {ok, [Context]} | {error, Reason}
-spec start_contexts() -> {ok, [pid()]} | {error, term()}.
start_contexts() ->
    py_context_router:start().

%% @doc Start the process-per-context system with options.
%%
%% Options:
%% - `contexts' - Number of contexts to create (default: number of schedulers)
%% - `mode' - Context mode: `worker' or `owngil' (default: `worker')
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

%% @doc Check if contexts have been started.
-spec contexts_started() -> boolean().
contexts_started() ->
    py_context_router:is_started().

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

%%% ============================================================================
%%% File Descriptor Utilities
%%% ============================================================================

%% @doc Duplicate a file descriptor.
%%
%% Creates a copy of an existing file descriptor. Use this when handing off
%% a socket fd to Python while keeping Erlang's ability to close its socket.
%%
%% Example:
%% ```
%% {ok, ClientSock} = gen_tcp:accept(ListenSock),
%% {ok, Fd} = inet:getfd(ClientSock),
%% {ok, DupFd} = py:dup_fd(Fd),
%% py_reactor_context:handoff(DupFd, #{type => tcp}),
%% gen_tcp:close(ClientSock).  %% Safe - Python has its own fd copy
%% '''
%%
%% @param Fd File descriptor to duplicate
%% @returns {ok, DupFd} | {error, Reason}
-spec dup_fd(integer()) -> {ok, integer()} | {error, term()}.
dup_fd(Fd) when is_integer(Fd) ->
    py_nif:dup_fd(Fd).

%%% ============================================================================
%%% Pool Registration API
%%% ============================================================================

%% @doc Register a module or module/function to use a specific pool.
%%
%% After registration, calls to the module (or specific function) are
%% automatically routed to the registered pool without changing call sites.
%%
%% Examples:
%% ```
%% %% Route all requests.* calls to io pool
%% py:register_pool(io, requests).
%%
%% %% Route only aiohttp.get to io pool
%% py:register_pool(io, {aiohttp, get}).
%%
%% %% Calls now automatically route to the correct pool
%% {ok, Resp} = py:call(requests, get, [Url]).  %% -> io pool
%% {ok, 4.0} = py:call(math, sqrt, [16]).       %% -> default pool
%% '''
%%
%% @param Pool Pool name (io, default, or custom)
%% @param ModuleOrTuple Module atom or {Module, Func} tuple
%% @returns ok
-spec register_pool(py_context_router:pool_name(), atom() | {atom(), atom()}) -> ok.
register_pool(Pool, Module) when is_atom(Pool), is_atom(Module) ->
    py_context_router:register_pool(Pool, Module);
register_pool(Pool, {Module, Func}) when is_atom(Pool), is_atom(Module), is_atom(Func) ->
    py_context_router:register_pool(Pool, Module, Func).

%% @doc Unregister a module or module/function from pool routing.
%%
%% After unregistering, calls return to using the default pool.
%%
%% @param ModuleOrTuple Module atom or {Module, Func} tuple
%% @returns ok
-spec unregister_pool(atom() | {atom(), atom()}) -> ok.
unregister_pool(Module) when is_atom(Module) ->
    py_context_router:unregister_pool(Module);
unregister_pool({Module, Func}) when is_atom(Module), is_atom(Func) ->
    py_context_router:unregister_pool(Module, Func).

%%% ============================================================================
%%% SharedDict API - Process-scoped Shared Dictionary
%%% ============================================================================

%% @doc Create a new process-scoped SharedDict.
%%
%% Creates a SharedDict owned by the calling process. The dict is automatically
%% destroyed when the owning process terminates. Values are stored as pickled
%% bytes for cross-interpreter safety.
%%
%% == Example ==
%% ```
%% {ok, SD} = py:shared_dict_new().
%% ok = py:shared_dict_set(SD, <<"config">>, #{host => <<"localhost">>}).
%% #{<<"host">> := <<"localhost">>} = py:shared_dict_get(SD, <<"config">>).
%% '''
%%
%% @returns {ok, Reference} on success, {error, Reason} on failure
-spec shared_dict_new() -> {ok, reference()} | {error, term()}.
shared_dict_new() ->
    py_nif:shared_dict_new().

%% @doc Get a value from SharedDict with default undefined.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @returns Value or undefined if key not found
-spec shared_dict_get(reference(), binary()) -> term().
shared_dict_get(Handle, Key) ->
    shared_dict_get(Handle, Key, undefined).

%% @doc Get a value from SharedDict with custom default.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @param Default Default value if key not found
%% @returns Value or Default
-spec shared_dict_get(reference(), binary(), term()) -> term().
shared_dict_get(Handle, Key, Default) when is_binary(Key) ->
    py_nif:shared_dict_get(Handle, Key, Default).

%% @doc Set a value in SharedDict.
%%
%% The value is pickled for cross-interpreter safety.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @param Value Erlang term value (will be pickled)
%% @returns ok on success
-spec shared_dict_set(reference(), binary(), term()) -> ok | {error, term()}.
shared_dict_set(Handle, Key, Value) when is_binary(Key) ->
    py_nif:shared_dict_set(Handle, Key, Value).

%% @doc Delete a key from SharedDict.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @returns ok (even if key didn't exist)
-spec shared_dict_del(reference(), binary()) -> ok.
shared_dict_del(Handle, Key) when is_binary(Key) ->
    py_nif:shared_dict_del(Handle, Key).

%% @doc Get all keys from SharedDict.
%%
%% @param Handle SharedDict reference
%% @returns List of binary keys
-spec shared_dict_keys(reference()) -> [binary()].
shared_dict_keys(Handle) ->
    py_nif:shared_dict_keys(Handle).

%% @doc Explicitly destroy a SharedDict.
%%
%% Marks the SharedDict as destroyed and clears its Python dict.
%% After destruction, any further operations on this SharedDict will
%% return badarg. This is idempotent - calling on an already-destroyed
%% dict returns ok.
%%
%% @param Handle SharedDict reference
%% @returns ok
-spec shared_dict_destroy(reference()) -> ok.
shared_dict_destroy(Handle) ->
    py_nif:shared_dict_destroy(Handle).

