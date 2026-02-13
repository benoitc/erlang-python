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
    subinterp_supported/0
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
-spec call(py_module(), py_func(), py_args(), py_kwargs()) -> py_result().
call(Module, Func, Args, Kwargs) ->
    call(Module, Func, Args, Kwargs, ?DEFAULT_TIMEOUT).

%% @doc Call a Python function with keyword arguments and custom timeout.
%% Timeout is in milliseconds. Use `infinity' for no timeout.
-spec call(py_module(), py_func(), py_args(), py_kwargs(), timeout()) -> py_result().
call(Module, Func, Args, Kwargs, Timeout) ->
    Ref = make_ref(),
    TimeoutMs = case Timeout of
        infinity -> 0;
        Ms when is_integer(Ms), Ms > 0 -> Ms;
        _ -> ?DEFAULT_TIMEOUT
    end,
    py_pool:request({call, Ref, self(), Module, Func, Args, Kwargs, TimeoutMs}),
    await(Ref, Timeout).

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
    TimeoutMs = case Timeout of
        infinity -> 0;
        Ms when is_integer(Ms), Ms > 0 -> Ms;
        _ -> ?DEFAULT_TIMEOUT
    end,
    py_pool:request({eval, Ref, self(), Code, Locals, TimeoutMs}),
    await(Ref, Timeout).

%% @doc Execute Python statements (no return value expected).
-spec exec(string() | binary()) -> ok | {error, term()}.
exec(Code) ->
    Ref = make_ref(),
    py_pool:request({exec, Ref, self(), Code}),
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
%% Example: py:stream_eval(<<"(x**2 for x in range(10))">>) returns {ok, [0,1,4,9,...]}
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
    Ref = make_ref(),
    py_async_pool:request({async_call, Ref, self(), Module, Func, Args, Kwargs}),
    Ref.

%% @doc Wait for an async call to complete.
-spec async_await(py_ref()) -> py_result().
async_await(Ref) ->
    async_await(Ref, ?DEFAULT_TIMEOUT).

%% @doc Wait for an async call with timeout.
-spec async_await(py_ref(), timeout()) -> py_result().
async_await(Ref, Timeout) ->
    receive
        {py_response, Ref, Result} -> Result;
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
