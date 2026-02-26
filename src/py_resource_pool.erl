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

%%% @doc Simple resource pool for Python workers.
%%%
%%% This module provides a lightweight pool of Python worker resources
%%% using ref-counted NIF resources with lock-free round-robin scheduling.
%%%
%%% On Python 3.12+, workers are subinterpreters with per-interpreter GIL
%%% (OWN_GIL) providing true parallelism. On older Python versions, workers
%%% use thread states with shared GIL.
%%%
%%% == Usage ==
%%% ```
%%% %% Start pool with default worker count (CPU cores)
%%% ok = py_resource_pool:start().
%%%
%%% %% Call a Python function
%%% {ok, Result} = py_resource_pool:call(math, sqrt, [16]).
%%%
%%% %% Call with keyword arguments
%%% {ok, Result} = py_resource_pool:call(mymodule, func, [Arg1], #{key => value}).
%%%
%%% %% Run ASGI application
%%% {ok, {Status, Headers, Body}} = py_resource_pool:asgi_run(
%%%     <<"hornbeam_asgi_runner">>, <<"myapp">>, <<"app">>, Scope, ReqBody).
%%%
%%% %% Stop pool
%%% ok = py_resource_pool:stop().
%%% '''
%%%
%%% @end
-module(py_resource_pool).

-export([
    start/0,
    start/1,
    stop/0,
    call/3,
    call/4,
    asgi_run/5,
    stats/0
]).

%% Pool state stored in persistent_term
-record(pool_state, {
    workers :: tuple(),               %% Tuple of worker refs (fast nth access)
    num_workers :: pos_integer(),
    counter :: atomics:atomics_ref(),
    use_subinterp :: boolean()
}).

-define(POOL_KEY, {?MODULE, pool_state}).

%%% ============================================================================
%%% API
%%% ============================================================================

%% @doc Start the pool with default settings (CPU core count workers).
-spec start() -> ok | {error, term()}.
start() ->
    start(#{}).

%% @doc Start the pool with options.
%% Options:
%% - `workers' - Number of workers (default: CPU core count)
%% - `use_subinterp' - Force subinterpreter use (default: auto-detect)
-spec start(map()) -> ok | {error, term()}.
start(Opts) ->
    case persistent_term:get(?POOL_KEY, undefined) of
        undefined ->
            do_start(Opts);
        _ ->
            {error, already_started}
    end.

%% @doc Stop the pool and release all resources.
-spec stop() -> ok.
stop() ->
    case persistent_term:get(?POOL_KEY, undefined) of
        undefined ->
            ok;
        #pool_state{workers = Workers, num_workers = N, use_subinterp = UseSubinterp} ->
            %% Destroy all workers
            lists:foreach(
                fun(Idx) ->
                    Worker = element(Idx, Workers),
                    destroy_worker(Worker, UseSubinterp)
                end,
                lists:seq(1, N)
            ),
            persistent_term:erase(?POOL_KEY),
            ok
    end.

%% @doc Call a Python function.
-spec call(atom() | binary(), atom() | binary(), list()) ->
    {ok, term()} | {error, term()}.
call(Module, Func, Args) ->
    call(Module, Func, Args, #{}).

%% @doc Call a Python function with keyword arguments.
-spec call(atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
call(Module, Func, Args, Kwargs) ->
    {Worker, UseSubinterp} = checkout(),
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    case UseSubinterp of
        true ->
            py_nif:subinterp_call(Worker, ModuleBin, FuncBin, Args, Kwargs);
        false ->
            py_nif:worker_call(Worker, ModuleBin, FuncBin, Args, Kwargs)
    end.

%% @doc Run an ASGI application.
%% Returns {ok, {Status, Headers, Body}} on success.
-spec asgi_run(binary(), atom() | binary(), atom() | binary(), map(), binary()) ->
    {ok, {integer(), list(), binary()}} | {error, term()}.
asgi_run(Runner, Module, Callable, Scope, Body) ->
    {Worker, UseSubinterp} = checkout(),
    RunnerBin = to_binary(Runner),
    ModuleBin = to_binary(Module),
    CallableBin = to_binary(Callable),
    case UseSubinterp of
        true ->
            py_nif:subinterp_asgi_run(Worker, RunnerBin, ModuleBin, CallableBin, Scope, Body);
        false ->
            %% Fallback doesn't use worker ref
            py_nif:asgi_run(RunnerBin, ModuleBin, CallableBin, Scope, Body)
    end.

%% @doc Get pool statistics.
-spec stats() -> map().
stats() ->
    case persistent_term:get(?POOL_KEY, undefined) of
        undefined ->
            #{initialized => false};
        #pool_state{num_workers = N, use_subinterp = UseSubinterp} ->
            #{
                initialized => true,
                num_workers => N,
                use_subinterp => UseSubinterp
            }
    end.

%%% ============================================================================
%%% Internal Functions
%%% ============================================================================

do_start(Opts) ->
    NumWorkers = maps:get(workers, Opts, erlang:system_info(schedulers)),
    UseSubinterp = case maps:get(use_subinterp, Opts, auto) of
        auto -> py_nif:subinterp_supported();
        Bool when is_boolean(Bool) -> Bool
    end,

    case create_workers(NumWorkers, UseSubinterp) of
        {ok, WorkerList} ->
            %% Use tuple for O(1) element access
            Workers = list_to_tuple(WorkerList),
            Counter = atomics:new(1, [{signed, false}]),
            State = #pool_state{
                workers = Workers,
                num_workers = NumWorkers,
                counter = Counter,
                use_subinterp = UseSubinterp
            },
            persistent_term:put(?POOL_KEY, State),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

create_workers(N, UseSubinterp) ->
    create_workers(N, UseSubinterp, []).

create_workers(0, _UseSubinterp, Acc) ->
    {ok, lists:reverse(Acc)};
create_workers(N, UseSubinterp, Acc) ->
    case create_worker(UseSubinterp) of
        {ok, Worker} ->
            create_workers(N - 1, UseSubinterp, [Worker | Acc]);
        {error, Reason} ->
            %% Cleanup already created workers
            lists:foreach(
                fun(W) -> destroy_worker(W, UseSubinterp) end,
                Acc
            ),
            {error, Reason}
    end.

create_worker(true) ->
    py_nif:subinterp_worker_new();
create_worker(false) ->
    py_nif:worker_new().

destroy_worker(Worker, true) ->
    py_nif:subinterp_worker_destroy(Worker);
destroy_worker(Worker, false) ->
    py_nif:worker_destroy(Worker).

checkout() ->
    #pool_state{
        workers = Workers,
        num_workers = N,
        counter = Counter,
        use_subinterp = UseSubinterp
    } = persistent_term:get(?POOL_KEY),
    Idx = atomics:add_get(Counter, 1, 1) rem N + 1,
    {element(Idx, Workers), UseSubinterp}.

to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(Binary) when is_binary(Binary) ->
    Binary;
to_binary(List) when is_list(List) ->
    list_to_binary(List).
