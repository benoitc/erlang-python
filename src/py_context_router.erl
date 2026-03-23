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

%%% @doc Scheduler-affinity router for Python contexts.
%%%
%%% This module provides automatic routing of Python calls to contexts
%%% based on the calling process's scheduler ID. This ensures that
%%% processes on the same scheduler reuse the same context, providing
%%% good cache locality while still enabling N-way parallelism.
%%%
%%% == Architecture ==
%%%
%%% <pre>
%%% Scheduler 1 ---+
%%%                +---> Context 1 (Subinterp/Worker)
%%% Scheduler 2 ---+
%%%                +---> Context 2 (Subinterp/Worker)
%%% Scheduler 3 ---+
%%%                +---> Context 3 (Subinterp/Worker)
%%% ...            |
%%% Scheduler N ---+---> Context N (Subinterp/Worker)
%%% </pre>
%%%
%%% == Usage ==
%%%
%%% <pre>
%%% %% Start the router with default settings
%%% {ok, Contexts} = py_context_router:start(),
%%%
%%% %% Get context for current scheduler (automatic routing)
%%% Ctx = py_context_router:get_context(),
%%% {ok, Result} = py_context:call(Ctx, math, sqrt, [16], #{}),
%%%
%%% %% Or get a specific context by index
%%% Ctx2 = py_context_router:get_context(2),
%%%
%%% %% Bind a specific context to this process
%%% ok = py_context_router:bind_context(Ctx2),
%%% Ctx2 = py_context_router:get_context(), %% Returns bound context
%%%
%%% %% Unbind to return to scheduler-based routing
%%% ok = py_context_router:unbind_context().
%%% </pre>
%%%
%%% @end
-module(py_context_router).

-export([
    start/0,
    start/1,
    stop/0,
    is_started/0,
    get_context/0,
    get_context/1,
    bind_context/1,
    bind_context/2,
    unbind_context/0,
    unbind_context/1,
    num_contexts/0,
    num_contexts/1,
    contexts/0,
    contexts/1,
    %% Pool management
    start_pool/2,
    start_pool/3,
    stop_pool/1,
    pool_started/1,
    %% Pool registration (route module/func to specific pools)
    register_pool/2,
    register_pool/3,
    unregister_pool/1,
    unregister_pool/2,
    lookup_pool/2,
    list_pool_registrations/0,
    init_pool_registry/0
]).

%% Persistent term keys (pool-aware)
-define(POOL_SIZE_KEY(Pool), {?MODULE, pool_size, Pool}).
-define(POOL_CONTEXT_KEY(Pool, N), {?MODULE, pool_context, Pool, N}).
-define(POOL_CONTEXTS_KEY(Pool), {?MODULE, pool_contexts, Pool}).

%% Legacy keys for backward compatibility (default pool)
-define(NUM_CONTEXTS_KEY, {?MODULE, num_contexts}).
-define(CONTEXT_KEY(N), {?MODULE, context, N}).
-define(CONTEXTS_KEY, {?MODULE, all_contexts}).

%% Process dictionary key for bound context (pool-aware)
-define(BOUND_CONTEXT_KEY(Pool), {py_bound_context, Pool}).
-define(BOUND_CONTEXT_KEY, py_bound_context).  %% Legacy for default pool

%% Default pool name
-define(DEFAULT_POOL, default).

%% Pool registry persistent term key
-define(POOL_REGISTRY_KEY, {?MODULE, pool_registry}).

%% ============================================================================
%% Types
%% ============================================================================

-type pool_name() :: default | io | atom().
-type start_opts() :: #{
    contexts => pos_integer(),
    mode => py_context:context_mode()
}.

-export_type([start_opts/0, pool_name/0]).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start the context router with default settings.
%%
%% Creates one context per scheduler using worker mode.
%%
%% @returns {ok, [Context]} | {error, Reason}
-spec start() -> {ok, [pid()]} | {error, term()}.
start() ->
    start(#{}).

%% @doc Start the context router with options.
%%
%% Options:
%% - `contexts' - Number of contexts to create (default: number of schedulers)
%% - `mode' - Context mode: `worker', `subinterp', or `owngil' (default: `worker')
%%
%% @param Opts Start options
%% @returns {ok, [Context]} | {error, Reason}
-spec start(start_opts()) -> {ok, [pid()]} | {error, term()}.
start(Opts) ->
    NumContexts = maps:get(contexts, Opts, erlang:system_info(schedulers)),
    Mode = maps:get(mode, Opts, worker),
    %% Delegate to start_pool for the default pool
    start_pool(?DEFAULT_POOL, NumContexts, Mode).

%% @doc Stop the context router.
%%
%% Stops the default pool and removes persistent_term entries.
-spec stop() -> ok.
stop() ->
    stop_pool(?DEFAULT_POOL).

%% @doc Check if contexts have been started and are still alive.
%%
%% @returns true if the default pool is running, false otherwise
-spec is_started() -> boolean().
is_started() ->
    pool_started(?DEFAULT_POOL).

%% @doc Get the context for the current process from the default pool.
%%
%% If the process has a bound context, returns that context.
%% Otherwise, selects a context based on the current scheduler ID.
%%
%% @returns Context pid
-spec get_context() -> pid().
get_context() ->
    get_context(?DEFAULT_POOL).

%% @doc Get a context by index or from a named pool.
%%
%% When called with an integer, gets the Nth context from the default pool.
%% When called with an atom, gets a scheduler-affinity context from that pool.
%%
%% @param NOrPool Context index (1 to num_contexts) or pool name
%% @returns Context pid
-spec get_context(pos_integer() | pool_name()) -> pid().
get_context(N) when is_integer(N), N > 0 ->
    %% Legacy: get Nth context from default pool
    persistent_term:get(?POOL_CONTEXT_KEY(?DEFAULT_POOL, N));
get_context(Pool) when is_atom(Pool) ->
    %% Get context from named pool
    %% Auto-bind on first access to ensure consistent context for a process
    case get(?BOUND_CONTEXT_KEY(Pool)) of
        undefined ->
            Ctx = select_by_scheduler(Pool),
            put(?BOUND_CONTEXT_KEY(Pool), Ctx),
            Ctx;
        Ctx ->
            %% Verify bound context is still alive
            case is_process_alive(Ctx) of
                true ->
                    Ctx;
                false ->
                    %% Re-bind to a new context
                    NewCtx = select_by_scheduler(Pool),
                    put(?BOUND_CONTEXT_KEY(Pool), NewCtx),
                    NewCtx
            end
    end.

%% @doc Bind a context to the current process for the default pool.
%%
%% After binding, `get_context/0' will always return this context
%% instead of selecting by scheduler.
%%
%% @param Ctx Context pid to bind
%% @returns ok
-spec bind_context(pid()) -> ok.
bind_context(Ctx) when is_pid(Ctx) ->
    bind_context(?DEFAULT_POOL, Ctx).

%% @doc Bind a context to the current process for a specific pool.
%%
%% @param Pool Pool name
%% @param Ctx Context pid to bind
%% @returns ok
-spec bind_context(pool_name(), pid()) -> ok.
bind_context(Pool, Ctx) when is_atom(Pool), is_pid(Ctx) ->
    put(?BOUND_CONTEXT_KEY(Pool), Ctx),
    ok.

%% @doc Unbind the current process's context from the default pool.
%%
%% After unbinding, `get_context/0' will return to scheduler-based
%% selection.
%%
%% @returns ok
-spec unbind_context() -> ok.
unbind_context() ->
    unbind_context(?DEFAULT_POOL).

%% @doc Unbind the current process's context from a specific pool.
%%
%% @param Pool Pool name
%% @returns ok
-spec unbind_context(pool_name()) -> ok.
unbind_context(Pool) when is_atom(Pool) ->
    erase(?BOUND_CONTEXT_KEY(Pool)),
    ok.

%% @doc Get the number of contexts in the default pool.
-spec num_contexts() -> non_neg_integer().
num_contexts() ->
    num_contexts(?DEFAULT_POOL).

%% @doc Get the number of contexts in a pool.
-spec num_contexts(pool_name()) -> non_neg_integer().
num_contexts(Pool) when is_atom(Pool) ->
    persistent_term:get(?POOL_SIZE_KEY(Pool), 0).

%% @doc Get all context pids from the default pool.
-spec contexts() -> [pid()].
contexts() ->
    contexts(?DEFAULT_POOL).

%% @doc Get all context pids from a pool.
-spec contexts(pool_name()) -> [pid()].
contexts(Pool) when is_atom(Pool) ->
    persistent_term:get(?POOL_CONTEXTS_KEY(Pool), []).

%% ============================================================================
%% Pool Management
%% ============================================================================

%% @doc Start a named pool with given size.
%%
%% @param Pool Pool name (default, io, or custom)
%% @param Size Number of contexts in the pool
%% @returns {ok, [Context]} | {error, Reason}
-spec start_pool(pool_name(), pos_integer()) -> {ok, [pid()]} | {error, term()}.
start_pool(Pool, Size) ->
    start_pool(Pool, Size, worker).

%% @doc Start a named pool with given size and mode.
%%
%% @param Pool Pool name (default, io, or custom)
%% @param Size Number of contexts in the pool
%% @param Mode Context mode (worker, subinterp, owngil)
%% @returns {ok, [Context]} | {error, Reason}
-spec start_pool(pool_name(), pos_integer(), py_context:context_mode()) ->
    {ok, [pid()]} | {error, term()}.
start_pool(Pool, Size, Mode) when is_atom(Pool), is_integer(Size), Size > 0 ->
    %% Check if pool already exists
    case persistent_term:get(?POOL_CONTEXTS_KEY(Pool), undefined) of
        undefined ->
            do_start_pool(Pool, Size, Mode);
        Contexts when is_list(Contexts) ->
            %% Verify at least one context is still alive
            case lists:any(fun is_process_alive/1, Contexts) of
                true ->
                    {ok, Contexts};
                false ->
                    %% All dead - restart
                    stop_pool(Pool),
                    do_start_pool(Pool, Size, Mode)
            end
    end.

%% @private
do_start_pool(Pool, Size, Mode) ->
    %% Ensure supervisor is running
    case whereis(py_context_sup) of
        undefined ->
            case py_context_sup:start_link() of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                Error -> throw(Error)
            end;
        _ ->
            ok
    end,

    %% Start contexts for this pool
    try
        Contexts = lists:map(
            fun(N) ->
                %% Use pool-unique ID: {Pool, N}
                Id = {Pool, N},
                case py_context_sup:start_context(Id, Mode) of
                    {ok, Pid} ->
                        persistent_term:put(?POOL_CONTEXT_KEY(Pool, N), Pid),
                        Pid;
                    {error, Reason} ->
                        throw({context_start_failed, Pool, N, Reason})
                end
            end,
            lists:seq(1, Size)
        ),

        %% Store pool metadata
        persistent_term:put(?POOL_SIZE_KEY(Pool), Size),
        persistent_term:put(?POOL_CONTEXTS_KEY(Pool), Contexts),

        {ok, Contexts}
    catch
        throw:Err ->
            stop_pool(Pool),
            {error, Err}
    end.

%% @doc Stop a named pool.
%%
%% @param Pool Pool name to stop
%% @returns ok
-spec stop_pool(pool_name()) -> ok.
stop_pool(Pool) when is_atom(Pool) ->
    %% Stop all contexts in the pool
    case persistent_term:get(?POOL_CONTEXTS_KEY(Pool), undefined) of
        undefined ->
            ok;
        Contexts ->
            lists:foreach(
                fun(Ctx) ->
                    catch py_context_sup:stop_context(Ctx)
                end,
                Contexts
            )
    end,

    %% Clean up persistent terms for this pool
    Size = persistent_term:get(?POOL_SIZE_KEY(Pool), 0),
    lists:foreach(
        fun(N) ->
            catch persistent_term:erase(?POOL_CONTEXT_KEY(Pool, N))
        end,
        lists:seq(1, Size)
    ),
    catch persistent_term:erase(?POOL_SIZE_KEY(Pool)),
    catch persistent_term:erase(?POOL_CONTEXTS_KEY(Pool)),
    ok.

%% @doc Check if a pool has been started and is still alive.
%%
%% @param Pool Pool name to check
%% @returns true if pool is running, false otherwise
-spec pool_started(pool_name()) -> boolean().
pool_started(Pool) when is_atom(Pool) ->
    case persistent_term:get(?POOL_SIZE_KEY(Pool), 0) of
        0 -> false;
        _N ->
            %% Verify at least one context is actually alive
            case persistent_term:get(?POOL_CONTEXT_KEY(Pool, 1), undefined) of
                undefined -> false;
                Pid when is_pid(Pid) -> is_process_alive(Pid);
                _ -> false
            end
    end.

%% ============================================================================
%% Pool Registration (route module/func to specific pools)
%% ============================================================================

%% @doc Initialize the pool registry.
%%
%% This is called automatically on first registration.
%% Safe to call multiple times - does nothing if already initialized.
-spec init_pool_registry() -> ok.
init_pool_registry() ->
    case persistent_term:get(?POOL_REGISTRY_KEY, undefined) of
        undefined ->
            persistent_term:put(?POOL_REGISTRY_KEY, #{}),
            ok;
        _ ->
            ok
    end.

%% @doc Register a module to use a specific pool for all functions.
%%
%% All calls to `Module:*' will be routed to the specified pool.
%%
%% Example:
%% ```
%% %% Route all 'requests' module calls to io pool
%% py_context_router:register_pool(io, requests).
%% '''
%%
%% @param Pool Pool name to route to
%% @param Module Python module name
%% @returns ok
-spec register_pool(pool_name(), atom()) -> ok.
register_pool(Pool, Module) when is_atom(Pool), is_atom(Module) ->
    Registry = get_registry(),
    NewRegistry = maps:put({Module, '_'}, Pool, Registry),
    persistent_term:put(?POOL_REGISTRY_KEY, NewRegistry),
    ok.

%% @doc Register a specific module/function to use a specific pool.
%%
%% Calls to `Module:Func' will be routed to the specified pool.
%% More specific registrations (module+func) take precedence over
%% module-only registrations.
%%
%% Example:
%% ```
%% %% Route requests.get to io pool
%% py_context_router:register_pool(io, requests, get).
%% '''
%%
%% @param Pool Pool name to route to
%% @param Module Python module name
%% @param Func Python function name
%% @returns ok
-spec register_pool(pool_name(), atom(), atom()) -> ok.
register_pool(Pool, Module, Func) when is_atom(Pool), is_atom(Module), is_atom(Func) ->
    Registry = get_registry(),
    NewRegistry = maps:put({Module, Func}, Pool, Registry),
    persistent_term:put(?POOL_REGISTRY_KEY, NewRegistry),
    ok.

%% @doc Unregister a module from pool routing.
%%
%% Calls will return to using the default pool.
%%
%% @param Module Python module name
%% @returns ok
-spec unregister_pool(atom()) -> ok.
unregister_pool(Module) when is_atom(Module) ->
    Registry = get_registry(),
    NewRegistry = maps:remove({Module, '_'}, Registry),
    persistent_term:put(?POOL_REGISTRY_KEY, NewRegistry),
    ok.

%% @doc Unregister a specific module/function from pool routing.
%%
%% @param Module Python module name
%% @param Func Python function name
%% @returns ok
-spec unregister_pool(atom(), atom()) -> ok.
unregister_pool(Module, Func) when is_atom(Module), is_atom(Func) ->
    Registry = get_registry(),
    NewRegistry = maps:remove({Module, Func}, Registry),
    persistent_term:put(?POOL_REGISTRY_KEY, NewRegistry),
    ok.

%% @doc Look up which pool a module/function is registered to.
%%
%% First checks for a specific module+func registration.
%% If not found, checks for a module-only registration.
%% Returns `default' if no registration found.
%%
%% @param Module Python module name
%% @param Func Python function name
%% @returns Pool name
-spec lookup_pool(atom(), atom()) -> pool_name().
lookup_pool(Module, Func) when is_atom(Module), is_atom(Func) ->
    Registry = get_registry(),
    %% First try specific module+func, then module-only, then default
    case maps:get({Module, Func}, Registry, undefined) of
        undefined ->
            case maps:get({Module, '_'}, Registry, undefined) of
                undefined -> ?DEFAULT_POOL;
                Pool -> Pool
            end;
        Pool ->
            Pool
    end.

%% @doc List all pool registrations.
%%
%% Returns a list of `{{Module, Func}, Pool}' tuples.
%% @returns List of registrations
-spec list_pool_registrations() -> [{{atom(), atom() | '_'}, pool_name()}].
list_pool_registrations() ->
    maps:to_list(get_registry()).

%% @private
%% Get the pool registry map from persistent_term
-spec get_registry() -> #{}.
get_registry() ->
    persistent_term:get(?POOL_REGISTRY_KEY, #{}).

%% ============================================================================
%% Internal functions
%% ============================================================================

%% @private
%% Select context based on scheduler ID using modulo
-spec select_by_scheduler(pool_name()) -> pid().
select_by_scheduler(Pool) ->
    SchedId = erlang:system_info(scheduler_id),
    NumCtx = persistent_term:get(?POOL_SIZE_KEY(Pool)),
    Idx = ((SchedId - 1) rem NumCtx) + 1,
    persistent_term:get(?POOL_CONTEXT_KEY(Pool, Idx)).
