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

%%% @doc Worker thread pool for high-throughput Python operations.
%%%
%%% This module provides a C-level worker thread pool for executing Python calls
%%% with minimal GIL contention. Each worker has its own subinterpreter
%%% (Python 3.12+) or dedicated GIL-holding thread.
%%%
%%% == Benefits ==
%%% <ul>
%%% <li>No GIL acquire/release per request (workers hold GIL)</li>
%%% <li>Module/callable cached per worker (no reimport)</li>
%%% <li>True parallelism with subinterpreters (each has OWN_GIL)</li>
%%% </ul>
%%%
%%% == Usage ==
%%% ```
%%% %% Start pool with auto-detected workers (CPU count)
%%% ok = py_worker_pool:start_link().
%%%
%%% %% Synchronous call (blocks until result)
%%% {ok, Result} = py_worker_pool:call(math, sqrt, [16]).
%%%
%%% %% Call with keyword arguments
%%% {ok, Result} = py_worker_pool:apply(mymodule, func, [Arg1], #{key => value}).
%%%
%%% %% Async call (returns immediately, receives message later)
%%% {ok, ReqId} = py_worker_pool:call_async(math, sqrt, [16]),
%%% receive
%%%     {py_response, ReqId, Result} -> Result
%%% end.
%%%
%%% %% ASGI request
%%% {ok, {Status, Headers, Body}} = py_worker_pool:asgi_run(
%%%     <<"myapp">>, <<"app">>, Scope, Body).
%%% '''
%%%
%%% @end
-module(py_worker_pool).

-export([
    %% Lifecycle
    start_link/0,
    start_link/1,
    stop/0,

    %% Sync API (blocking)
    call/3,
    call/4,
    apply/4,
    apply/5,
    eval/1,
    eval/2,
    exec/1,
    exec/2,
    asgi_run/4,
    asgi_run/5,
    wsgi_run/4,
    wsgi_run/5,

    %% Async API (non-blocking, returns request_id)
    call_async/3,
    call_async/4,
    apply_async/4,
    apply_async/5,
    eval_async/1,
    eval_async/2,
    exec_async/1,
    exec_async/2,
    asgi_run_async/4,
    asgi_run_async/5,
    wsgi_run_async/4,
    wsgi_run_async/5,

    %% Utilities
    await/1,
    await/2,
    stats/0
]).

-define(DEFAULT_TIMEOUT, 30000).

%%% ============================================================================
%%% Lifecycle
%%% ============================================================================

%% @doc Start the worker pool with auto-detected worker count.
%% Uses the number of CPU cores as the worker count.
-spec start_link() -> ok | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the worker pool with options.
%%
%% Options:
%% <ul>
%% <li>`workers' - Number of worker threads (default: CPU count)</li>
%% </ul>
-spec start_link(map()) -> ok | {error, term()}.
start_link(Opts) ->
    Workers = maps:get(workers, Opts, 0),
    py_nif:pool_start(Workers).

%% @doc Stop the worker pool.
-spec stop() -> ok.
stop() ->
    py_nif:pool_stop().

%%% ============================================================================
%%% Sync API (blocks until result)
%%% ============================================================================

%% @doc Call a Python function synchronously.
%% Blocks until the result is available.
-spec call(atom() | binary(), atom() | binary(), list()) ->
    {ok, term()} | {error, term()}.
call(Module, Func, Args) ->
    call(Module, Func, Args, #{}).

%% @doc Call a Python function synchronously with options.
%% Options:
%% <ul>
%% <li>`timeout' - Timeout in milliseconds (default: 30000)</li>
%% </ul>
-spec call(atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
call(Module, Func, Args, Opts) ->
    {ok, ReqId} = call_async(Module, Func, Args, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    await(ReqId, Timeout).

%% @doc Apply a Python function with keyword arguments synchronously.
-spec apply(atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
apply(Module, Func, Args, Kwargs) ->
    apply(Module, Func, Args, Kwargs, #{}).

%% @doc Apply a Python function with keyword arguments and options.
-spec apply(atom() | binary(), atom() | binary(), list(), map(), map()) ->
    {ok, term()} | {error, term()}.
apply(Module, Func, Args, Kwargs, Opts) ->
    {ok, ReqId} = apply_async(Module, Func, Args, Kwargs, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    await(ReqId, Timeout).

%% @doc Evaluate a Python expression synchronously.
-spec eval(binary()) -> {ok, term()} | {error, term()}.
eval(Code) ->
    eval(Code, #{}).

%% @doc Evaluate a Python expression with options.
%% Options:
%% <ul>
%% <li>`locals' - Local variables map</li>
%% <li>`timeout' - Timeout in milliseconds</li>
%% </ul>
-spec eval(binary(), map()) -> {ok, term()} | {error, term()}.
eval(Code, Opts) ->
    {ok, ReqId} = eval_async(Code, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    await(ReqId, Timeout).

%% @doc Execute Python statements synchronously.
-spec exec(binary()) -> ok | {error, term()}.
exec(Code) ->
    exec(Code, #{}).

%% @doc Execute Python statements with options.
-spec exec(binary(), map()) -> ok | {error, term()}.
exec(Code, Opts) ->
    {ok, ReqId} = exec_async(Code, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    case await(ReqId, Timeout) of
        {ok, none} -> ok;
        {ok, _} -> ok;
        Error -> Error
    end.

%% @doc Run an ASGI application synchronously.
-spec asgi_run(atom() | binary(), atom() | binary(), map(), binary()) ->
    {ok, {integer(), list(), binary()}} | {error, term()}.
asgi_run(Module, Callable, Scope, Body) ->
    asgi_run(Module, Callable, Scope, Body, #{}).

%% @doc Run an ASGI application with options.
%% Options:
%% <ul>
%% <li>`runner' - Runner module name (default: hornbeam_asgi_runner)</li>
%% <li>`timeout' - Timeout in milliseconds</li>
%% </ul>
-spec asgi_run(atom() | binary(), atom() | binary(), map(), binary(), map()) ->
    {ok, {integer(), list(), binary()}} | {error, term()}.
asgi_run(Module, Callable, Scope, Body, Opts) ->
    {ok, ReqId} = asgi_run_async(Module, Callable, Scope, Body, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    await(ReqId, Timeout).

%% @doc Run a WSGI application synchronously.
-spec wsgi_run(atom() | binary(), atom() | binary(), map(), term()) ->
    {ok, {binary(), list(), binary()}} | {error, term()}.
wsgi_run(Module, Callable, Environ, StartResponse) ->
    wsgi_run(Module, Callable, Environ, StartResponse, #{}).

%% @doc Run a WSGI application with options.
-spec wsgi_run(atom() | binary(), atom() | binary(), map(), term(), map()) ->
    {ok, {binary(), list(), binary()}} | {error, term()}.
wsgi_run(Module, Callable, Environ, StartResponse, Opts) ->
    {ok, ReqId} = wsgi_run_async(Module, Callable, Environ, StartResponse, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    await(ReqId, Timeout).

%%% ============================================================================
%%% Async API (returns immediately with {ok, RequestId})
%%% Caller receives {py_response, RequestId, Result} message
%%% ============================================================================

%% @doc Call a Python function asynchronously.
%% Returns immediately with {ok, RequestId}.
%% The result will be sent as {py_response, RequestId, Result}.
-spec call_async(atom() | binary(), atom() | binary(), list()) ->
    {ok, non_neg_integer()} | {error, term()}.
call_async(Module, Func, Args) ->
    call_async(Module, Func, Args, #{}).

%% @doc Call a Python function asynchronously with options.
-spec call_async(atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, non_neg_integer()} | {error, term()}.
call_async(Module, Func, Args, _Opts) ->
    ModuleBin = ensure_binary(Module),
    FuncBin = ensure_binary(Func),
    py_nif:pool_submit(call, ModuleBin, FuncBin, Args, undefined).

%% @doc Apply a Python function with kwargs asynchronously.
-spec apply_async(atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, non_neg_integer()} | {error, term()}.
apply_async(Module, Func, Args, Kwargs) ->
    apply_async(Module, Func, Args, Kwargs, #{}).

%% @doc Apply a Python function with kwargs asynchronously with options.
-spec apply_async(atom() | binary(), atom() | binary(), list(), map(), map()) ->
    {ok, non_neg_integer()} | {error, term()}.
apply_async(Module, Func, Args, Kwargs, _Opts) ->
    ModuleBin = ensure_binary(Module),
    FuncBin = ensure_binary(Func),
    py_nif:pool_submit(apply, ModuleBin, FuncBin, Args, Kwargs).

%% @doc Evaluate a Python expression asynchronously.
-spec eval_async(binary()) -> {ok, non_neg_integer()} | {error, term()}.
eval_async(Code) ->
    eval_async(Code, #{}).

%% @doc Evaluate a Python expression asynchronously with options.
-spec eval_async(binary(), map()) -> {ok, non_neg_integer()} | {error, term()}.
eval_async(Code, Opts) ->
    CodeBin = ensure_binary(Code),
    Locals = maps:get(locals, Opts, undefined),
    py_nif:pool_submit(eval, CodeBin, Locals, undefined, undefined).

%% @doc Execute Python statements asynchronously.
-spec exec_async(binary()) -> {ok, non_neg_integer()} | {error, term()}.
exec_async(Code) ->
    exec_async(Code, #{}).

%% @doc Execute Python statements asynchronously with options.
-spec exec_async(binary(), map()) -> {ok, non_neg_integer()} | {error, term()}.
exec_async(Code, _Opts) ->
    CodeBin = ensure_binary(Code),
    py_nif:pool_submit(exec, CodeBin, undefined, undefined, undefined).

%% @doc Run an ASGI application asynchronously.
-spec asgi_run_async(atom() | binary(), atom() | binary(), map(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
asgi_run_async(Module, Callable, Scope, Body) ->
    asgi_run_async(Module, Callable, Scope, Body, #{}).

%% @doc Run an ASGI application asynchronously with options.
-spec asgi_run_async(atom() | binary(), atom() | binary(), map(), binary(), map()) ->
    {ok, non_neg_integer()} | {error, term()}.
asgi_run_async(Module, Callable, Scope, Body, Opts) ->
    Runner = maps:get(runner, Opts, <<"hornbeam_asgi_runner">>),
    RunnerBin = ensure_binary(Runner),
    ModuleBin = ensure_binary(Module),
    CallableBin = ensure_binary(Callable),
    py_nif:pool_submit(asgi, RunnerBin, ModuleBin, CallableBin, {Scope, Body}).

%% @doc Run a WSGI application asynchronously.
-spec wsgi_run_async(atom() | binary(), atom() | binary(), map(), term()) ->
    {ok, non_neg_integer()} | {error, term()}.
wsgi_run_async(Module, Callable, Environ, _StartResponse) ->
    wsgi_run_async(Module, Callable, Environ, undefined, #{}).

%% @doc Run a WSGI application asynchronously with options.
-spec wsgi_run_async(atom() | binary(), atom() | binary(), map(), term(), map()) ->
    {ok, non_neg_integer()} | {error, term()}.
wsgi_run_async(Module, Callable, Environ, _StartResponse, _Opts) ->
    ModuleBin = ensure_binary(Module),
    CallableBin = ensure_binary(Callable),
    py_nif:pool_submit(wsgi, ModuleBin, CallableBin, Environ, undefined).

%%% ============================================================================
%%% Await - wait for async result
%%% ============================================================================

%% @doc Wait for an async result with default timeout.
-spec await(non_neg_integer()) -> {ok, term()} | {error, term()}.
await(RequestId) ->
    await(RequestId, ?DEFAULT_TIMEOUT).

%% @doc Wait for an async result with specified timeout.
%% Returns the result or {error, timeout}.
-spec await(non_neg_integer(), timeout()) -> {ok, term()} | {error, term()}.
await(RequestId, Timeout) ->
    receive
        {py_response, RequestId, Result} -> Result
    after Timeout ->
        {error, timeout}
    end.

%%% ============================================================================
%%% Statistics
%%% ============================================================================

%% @doc Get pool statistics.
%% Returns a map with:
%% <ul>
%% <li>`num_workers' - Number of worker threads</li>
%% <li>`initialized' - Whether the pool is started</li>
%% <li>`use_subinterpreters' - Whether using subinterpreters</li>
%% <li>`free_threaded' - Whether using free-threaded Python</li>
%% <li>`pending_count' - Number of pending requests</li>
%% <li>`total_enqueued' - Total requests submitted</li>
%% </ul>
-spec stats() -> map().
stats() ->
    py_nif:pool_stats().

%%% ============================================================================
%%% Internal Functions
%%% ============================================================================

-spec ensure_binary(atom() | binary()) -> binary().
ensure_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
ensure_binary(Binary) when is_binary(Binary) ->
    Binary;
ensure_binary(List) when is_list(List) ->
    list_to_binary(List).
