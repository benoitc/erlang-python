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
    version/0
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
