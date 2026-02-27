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
%%% ```
%%% Scheduler 1 ──┐
%%%               ├──► Context 1 (Subinterp/Worker)
%%% Scheduler 2 ──┤
%%%               ├──► Context 2 (Subinterp/Worker)
%%% Scheduler 3 ──┤
%%%               ├──► Context 3 (Subinterp/Worker)
%%% ...           │
%%% Scheduler N ──┴──► Context N (Subinterp/Worker)
%%% ```
%%%
%%% == Usage ==
%%%
%%% ```erlang
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
%%% ```
%%%
%%% @end
-module(py_context_router).

-export([
    start/0,
    start/1,
    stop/0,
    get_context/0,
    get_context/1,
    bind_context/1,
    unbind_context/0,
    num_contexts/0,
    contexts/0
]).

%% Persistent term keys
-define(NUM_CONTEXTS_KEY, {?MODULE, num_contexts}).
-define(CONTEXT_KEY(N), {?MODULE, context, N}).
-define(CONTEXTS_KEY, {?MODULE, all_contexts}).

%% Process dictionary key for bound context
-define(BOUND_CONTEXT_KEY, py_bound_context).

%% ============================================================================
%% Types
%% ============================================================================

-type start_opts() :: #{
    contexts => pos_integer(),
    mode => py_context:context_mode()
}.

-export_type([start_opts/0]).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start the context router with default settings.
%%
%% Creates one context per scheduler, using auto mode (subinterp on
%% Python 3.12+, worker otherwise).
%%
%% @returns {ok, [Context]} | {error, Reason}
-spec start() -> {ok, [pid()]} | {error, term()}.
start() ->
    start(#{}).

%% @doc Start the context router with options.
%%
%% Options:
%% - `contexts' - Number of contexts to create (default: number of schedulers)
%% - `mode' - Context mode: `auto', `subinterp', or `worker' (default: `auto')
%%
%% @param Opts Start options
%% @returns {ok, [Context]} | {error, Reason}
-spec start(start_opts()) -> {ok, [pid()]} | {error, term()}.
start(Opts) ->
    %% Check if contexts are already running (idempotent)
    case persistent_term:get(?CONTEXTS_KEY, undefined) of
        undefined ->
            do_start(Opts);
        Contexts when is_list(Contexts) ->
            %% Verify at least one context is still alive
            case lists:any(fun is_process_alive/1, Contexts) of
                true ->
                    {ok, Contexts};
                false ->
                    %% All dead - restart
                    stop(),  %% Clean up stale entries
                    do_start(Opts)
            end
    end.

%% @private
do_start(Opts) ->
    NumContexts = maps:get(contexts, Opts, erlang:system_info(schedulers)),
    Mode = maps:get(mode, Opts, auto),

    %% Start the supervisor if not already running
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

    %% Start contexts
    try
        Contexts = lists:map(
            fun(N) ->
                case py_context_sup:start_context(N, Mode) of
                    {ok, Pid} ->
                        persistent_term:put(?CONTEXT_KEY(N), Pid),
                        Pid;
                    {error, Reason} ->
                        throw({context_start_failed, N, Reason})
                end
            end,
            lists:seq(1, NumContexts)
        ),

        %% Store metadata
        persistent_term:put(?NUM_CONTEXTS_KEY, NumContexts),
        persistent_term:put(?CONTEXTS_KEY, Contexts),

        {ok, Contexts}
    catch
        throw:Err ->
            %% Clean up any contexts that were started
            stop(),
            {error, Err}
    end.

%% @doc Stop the context router.
%%
%% Stops all contexts and removes persistent_term entries.
-spec stop() -> ok.
stop() ->
    %% Stop all contexts
    case persistent_term:get(?CONTEXTS_KEY, undefined) of
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

    %% Clean up persistent terms
    NumContexts = persistent_term:get(?NUM_CONTEXTS_KEY, 0),
    lists:foreach(
        fun(N) ->
            catch persistent_term:erase(?CONTEXT_KEY(N))
        end,
        lists:seq(1, NumContexts)
    ),
    catch persistent_term:erase(?NUM_CONTEXTS_KEY),
    catch persistent_term:erase(?CONTEXTS_KEY),
    ok.

%% @doc Get the context for the current process.
%%
%% If the process has a bound context, returns that context.
%% Otherwise, selects a context based on the current scheduler ID.
%%
%% @returns Context pid
-spec get_context() -> pid().
get_context() ->
    case get(?BOUND_CONTEXT_KEY) of
        undefined ->
            select_by_scheduler();
        Ctx ->
            Ctx
    end.

%% @doc Get a specific context by index.
%%
%% @param N Context index (1 to num_contexts)
%% @returns Context pid
-spec get_context(pos_integer()) -> pid().
get_context(N) when is_integer(N), N > 0 ->
    persistent_term:get(?CONTEXT_KEY(N)).

%% @doc Bind a context to the current process.
%%
%% After binding, `get_context/0' will always return this context
%% instead of selecting by scheduler.
%%
%% @param Ctx Context pid to bind
%% @returns ok
-spec bind_context(pid()) -> ok.
bind_context(Ctx) when is_pid(Ctx) ->
    put(?BOUND_CONTEXT_KEY, Ctx),
    ok.

%% @doc Unbind the current process's context.
%%
%% After unbinding, `get_context/0' will return to scheduler-based
%% selection.
%%
%% @returns ok
-spec unbind_context() -> ok.
unbind_context() ->
    erase(?BOUND_CONTEXT_KEY),
    ok.

%% @doc Get the number of contexts.
-spec num_contexts() -> non_neg_integer().
num_contexts() ->
    persistent_term:get(?NUM_CONTEXTS_KEY, 0).

%% @doc Get all context pids.
-spec contexts() -> [pid()].
contexts() ->
    persistent_term:get(?CONTEXTS_KEY, []).

%% ============================================================================
%% Internal functions
%% ============================================================================

%% @private
%% Select context based on scheduler ID using modulo
-spec select_by_scheduler() -> pid().
select_by_scheduler() ->
    SchedId = erlang:system_info(scheduler_id),
    NumCtx = persistent_term:get(?NUM_CONTEXTS_KEY),
    Idx = ((SchedId - 1) rem NumCtx) + 1,
    persistent_term:get(?CONTEXT_KEY(Idx)).
