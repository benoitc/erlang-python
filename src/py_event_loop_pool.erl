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

%%% @doc Event Loop Worker Pool with Process Affinity.
%%%
%%% This module provides a pool of event loops for parallel Python coroutine
%%% execution, inspired by libuv's "one loop per thread" model. Each loop has
%%% its own worker and maintains its own event ordering.
%%%
%%% Process Affinity: All tasks from the same Erlang process are routed to
%%% the same event loop (via PID hash). This guarantees that timers and
%%% related async operations from a single process execute in order.
%%%
%%% @private
-module(py_event_loop_pool).
-behaviour(gen_server).

%% Pool management
-export([
    start_link/0,
    start_link/1,
    stop/0,
    get_loop/0,
    get_stats/0
]).

%% Distributed task API (pool-aware)
-export([
    create_task/3, create_task/4,
    run/3, run/4,
    spawn_task/3, spawn_task/4,
    await/1, await/2
]).

%% Legacy API
-export([run_async/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {
    num_loops :: non_neg_integer(),
    supported :: boolean(),
    owngil_enabled :: boolean(),
    sessions :: ets:tid() | undefined
}).

%% OWN_GIL session record - maps {PID, LoopIdx} to worker/handle
-record(owngil_session, {
    key :: {pid(), pos_integer()},      %% {CallerPid, LoopIndex}
    worker_id :: non_neg_integer(),     %% Worker thread index
    handle_id :: non_neg_integer(),     %% Namespace handle ID
    monitor_ref :: reference()          %% Process monitor for cleanup
}).

%% Persistent term keys for O(1) access
-define(PT_LOOPS, {?MODULE, loops}).
-define(PT_NUM_LOOPS, {?MODULE, num_loops}).
-define(PT_OWNGIL_ENABLED, {?MODULE, owngil_enabled}).
-define(PT_SESSIONS, {?MODULE, sessions}).

%%% ============================================================================
%%% API
%%% ============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    PoolSize = application:get_env(erlang_python, event_loop_pool_size,
                                   erlang:system_info(schedulers)),
    start_link(PoolSize).

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(NumLoops) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NumLoops], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%% @doc Get an event loop reference for the calling process.
%% Always returns the same loop for the same PID (process affinity).
-spec get_loop() -> {ok, reference()} | {error, not_available}.
get_loop() ->
    case pool_size() of
        0 -> {error, not_available};
        N ->
            %% Hash PID to get consistent loop assignment
            Hash = erlang:phash2(self()),
            Idx = (Hash rem N) + 1,
            {LoopRef, _WorkerPid} = get_loop_by_index(Idx),
            {ok, LoopRef}
    end.

%% @doc Get pool statistics.
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?MODULE, get_stats).

%%% ============================================================================
%%% Distributed Task API (Pool-aware)
%%% ============================================================================

%% @doc Submit an async task and return a reference to await the result.
%% Tasks from the same process always go to the same loop (ordered execution).
%%
%% Example:
%%   Ref = py_event_loop_pool:create_task(my_module, my_async_func, [arg1]),
%%   {ok, Result} = py_event_loop_pool:await(Ref)
-spec create_task(Module :: atom() | binary(), Func :: atom() | binary(),
                  Args :: list()) -> reference().
create_task(Module, Func, Args) ->
    create_task(Module, Func, Args, #{}).

-spec create_task(Module :: atom() | binary(), Func :: atom() | binary(),
                  Args :: list(), Kwargs :: map()) -> reference().
create_task(Module, Func, Args, _Kwargs) ->
    %% Check if OWN_GIL mode is enabled
    case is_owngil_enabled() of
        true ->
            %% OWN_GIL path: route to worker via session
            case ensure_session() of
                {ok, {WorkerId, HandleId, _LoopIdx}} ->
                    Ref = make_ref(),
                    Caller = self(),
                    ModuleBin = py_util:to_binary(Module),
                    FuncBin = py_util:to_binary(Func),
                    ok = py_nif:owngil_submit_task(WorkerId, HandleId, Caller,
                                                   Ref, ModuleBin, FuncBin, Args),
                    Ref;
                {error, _Reason} ->
                    %% Fall back to regular path
                    create_task_regular(Module, Func, Args)
            end;
        false ->
            create_task_regular(Module, Func, Args)
    end.

%% @private Regular (non-OWN_GIL) task creation
-spec create_task_regular(Module :: atom() | binary(), Func :: atom() | binary(),
                          Args :: list()) -> reference().
create_task_regular(Module, Func, Args) ->
    case get_loop() of
        {ok, LoopRef} ->
            create_task_on_loop(LoopRef, Module, Func, Args, #{});
        {error, not_available} ->
            %% Fallback to default event loop
            py_event_loop:create_task(Module, Func, Args, #{})
    end.

%% @doc Submit a task to a specific loop.
-spec create_task_on_loop(LoopRef :: reference(), Module :: atom() | binary(),
                          Func :: atom() | binary(), Args :: list(),
                          Kwargs :: map()) -> reference().
create_task_on_loop(LoopRef, Module, Func, Args, Kwargs) ->
    Ref = make_ref(),
    Caller = self(),
    ModuleBin = py_util:to_binary(Module),
    FuncBin = py_util:to_binary(Func),
    ok = case py_event_loop:get_process_env() of
        undefined ->
            py_nif:submit_task(LoopRef, Caller, Ref, ModuleBin, FuncBin, Args, Kwargs);
        EnvRef ->
            py_nif:submit_task_with_env(LoopRef, Caller, Ref, ModuleBin, FuncBin, Args, Kwargs, EnvRef)
    end,
    Ref.

%% @doc Blocking run of an async Python function.
-spec run(Module :: atom() | binary(), Func :: atom() | binary(), Args :: list()) ->
    {ok, term()} | {error, term()}.
run(Module, Func, Args) ->
    run(Module, Func, Args, #{}).

-spec run(Module :: atom() | binary(), Func :: atom() | binary(),
          Args :: list(), Opts :: map()) -> {ok, term()} | {error, term()}.
run(Module, Func, Args, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Kwargs = maps:get(kwargs, Opts, #{}),
    Ref = create_task(Module, Func, Args, Kwargs),
    await(Ref, Timeout).

%% @doc Fire-and-forget task execution.
-spec spawn_task(Module :: atom() | binary(), Func :: atom() | binary(),
                 Args :: list()) -> ok.
spawn_task(Module, Func, Args) ->
    spawn_task(Module, Func, Args, #{}).

-spec spawn_task(Module :: atom() | binary(), Func :: atom() | binary(),
                 Args :: list(), Kwargs :: map()) -> ok.
spawn_task(Module, Func, Args, _Kwargs) ->
    %% Check if OWN_GIL mode is enabled
    case is_owngil_enabled() of
        true ->
            %% OWN_GIL path: route to worker via session
            case ensure_session() of
                {ok, {WorkerId, HandleId, _LoopIdx}} ->
                    Ref = make_ref(),
                    %% Spawn a receiver that discards the result
                    Receiver = erlang:spawn(fun() ->
                        receive
                            {async_result, _, _} -> ok
                        after 30000 -> ok
                        end
                    end),
                    ModuleBin = py_util:to_binary(Module),
                    FuncBin = py_util:to_binary(Func),
                    ok = py_nif:owngil_submit_task(WorkerId, HandleId, Receiver,
                                                   Ref, ModuleBin, FuncBin, Args),
                    ok;
                {error, _Reason} ->
                    %% Fall back to regular path
                    spawn_task_regular(Module, Func, Args)
            end;
        false ->
            spawn_task_regular(Module, Func, Args)
    end.

%% @private Regular (non-OWN_GIL) spawn_task
-spec spawn_task_regular(Module :: atom() | binary(), Func :: atom() | binary(),
                         Args :: list()) -> ok.
spawn_task_regular(Module, Func, Args) ->
    case get_loop() of
        {ok, LoopRef} ->
            Ref = make_ref(),
            CallerEnv = py_event_loop:get_process_env(),
            Receiver = erlang:spawn(fun() ->
                receive
                    {async_result, _, _} -> ok
                after 30000 -> ok
                end
            end),
            ModuleBin = py_util:to_binary(Module),
            FuncBin = py_util:to_binary(Func),
            ok = case CallerEnv of
                undefined ->
                    py_nif:submit_task(LoopRef, Receiver, Ref, ModuleBin, FuncBin, Args, #{});
                EnvRef ->
                    py_nif:submit_task_with_env(LoopRef, Receiver, Ref, ModuleBin, FuncBin, Args, #{}, EnvRef)
            end,
            ok;
        {error, not_available} ->
            py_event_loop:spawn_task(Module, Func, Args, #{})
    end.

%% @doc Wait for an async task result.
-spec await(Ref :: reference()) -> {ok, term()} | {error, term()}.
await(Ref) ->
    await(Ref, 5000).

-spec await(Ref :: reference(), Timeout :: non_neg_integer() | infinity) ->
    {ok, term()} | {error, term()}.
await(Ref, Timeout) ->
    receive
        {async_result, Ref, Result} -> Result
    after Timeout ->
        {error, timeout}
    end.

%%% ============================================================================
%%% Legacy API
%%% ============================================================================

%% @doc Submit an async request (legacy API for backward compatibility).
-spec run_async(map()) -> ok | {error, term()}.
run_async(Request) ->
    case get_loop() of
        {ok, LoopRef} ->
            py_event_loop:run_async(LoopRef, Request);
        {error, not_available} ->
            {error, event_loop_not_available}
    end.

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([NumLoops]) ->
    process_flag(trap_exit, true),

    %% Check if OWN_GIL mode is enabled
    OwnGilEnabled = application:get_env(erlang_python, event_loop_pool_owngil, false),

    %% Initialize OWN_GIL infrastructure if enabled
    {Sessions, OwnGilReady} = case OwnGilEnabled of
        true ->
            %% Start subinterpreter thread pool
            case py_nif:subinterp_thread_pool_start(NumLoops) of
                ok ->
                    %% Create sessions ETS table
                    Tid = ets:new(?MODULE, [
                        set, public,
                        {keypos, #owngil_session.key},
                        {read_concurrency, true}
                    ]),
                    persistent_term:put(?PT_SESSIONS, Tid),
                    {Tid, true};
                {error, _} ->
                    error_logger:warning_msg("py_event_loop_pool: OWN_GIL pool failed to start~n"),
                    {undefined, false}
            end;
        false ->
            {undefined, false}
    end,

    persistent_term:put(?PT_OWNGIL_ENABLED, OwnGilReady),

    case create_loops(NumLoops, []) of
        {ok, LoopList} ->
            Loops = list_to_tuple(LoopList),
            persistent_term:put(?PT_LOOPS, Loops),
            persistent_term:put(?PT_NUM_LOOPS, NumLoops),
            {ok, #state{
                num_loops = NumLoops,
                supported = true,
                owngil_enabled = OwnGilReady,
                sessions = Sessions
            }};
        {error, Reason} ->
            error_logger:warning_msg("py_event_loop_pool: failed to create loops: ~p~n", [Reason]),
            persistent_term:put(?PT_LOOPS, {}),
            persistent_term:put(?PT_NUM_LOOPS, 0),
            {ok, #state{
                num_loops = 0,
                supported = false,
                owngil_enabled = OwnGilReady,
                sessions = Sessions
            }}
    end.

%% @private Create NumLoops independent event loops with workers
create_loops(0, Acc) ->
    {ok, lists:reverse(Acc)};
create_loops(N, Acc) ->
    case py_nif:event_loop_new() of
        {ok, LoopRef} ->
            WorkerId = iolist_to_binary([<<"pool_">>, integer_to_binary(N)]),
            case py_event_worker:start_link(WorkerId, LoopRef) of
                {ok, WorkerPid} ->
                    ok = py_nif:event_loop_set_worker(LoopRef, WorkerPid),
                    ok = py_nif:event_loop_set_id(LoopRef, WorkerId),
                    create_loops(N - 1, [{LoopRef, WorkerPid} | Acc]);
                {error, Reason} ->
                    {error, {worker_start_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {loop_create_failed, Reason}}
    end.

handle_call(get_stats, _From, State) ->
    %% Build base stats
    BaseStats = #{
        num_loops => State#state.num_loops,
        supported => State#state.supported,
        owngil_enabled => State#state.owngil_enabled
    },

    %% Add OWN_GIL-specific stats if enabled
    Stats = case State#state.owngil_enabled of
        true ->
            %% Get pool stats from NIF
            PoolStats = try py_nif:subinterp_thread_pool_stats() catch _:_ -> #{} end,
            %% Count active sessions
            ActiveSessions = case State#state.sessions of
                undefined -> 0;
                Tid -> ets:info(Tid, size)
            end,
            maps:merge(BaseStats, #{
                active_sessions => ActiveSessions,
                pool_stats => PoolStats
            });
        false ->
            BaseStats
    end,
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', _Pid, _Reason}, State) ->
    {noreply, State#state{supported = false}};

handle_info({'DOWN', MonRef, process, Pid, _Reason}, State) ->
    %% Process died - clean up any OWN_GIL sessions for this process
    case State#state.sessions of
        undefined -> ok;
        Tid ->
            %% Find and remove all sessions for this PID
            MatchSpec = [{#owngil_session{key = {Pid, '_'}, monitor_ref = MonRef, _ = '_'}, [], ['$_']}],
            case ets:select(Tid, MatchSpec) of
                [] ->
                    ok;
                Sessions ->
                    lists:foreach(fun(#owngil_session{key = Key, worker_id = WorkerId, handle_id = HandleId}) ->
                        %% Destroy session in worker
                        catch py_nif:owngil_destroy_session(WorkerId, HandleId),
                        %% Remove from ETS
                        ets:delete(Tid, Key)
                    end, Sessions)
            end
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Clean up OWN_GIL sessions if enabled
    case State#state.sessions of
        undefined -> ok;
        Tid ->
            %% Destroy all sessions
            ets:foldl(fun(#owngil_session{worker_id = WorkerId, handle_id = HandleId}, _) ->
                catch py_nif:owngil_destroy_session(WorkerId, HandleId),
                ok
            end, ok, Tid),
            catch ets:delete(Tid)
    end,

    %% Stop OWN_GIL thread pool if it was started
    case State#state.owngil_enabled of
        true -> catch py_nif:subinterp_thread_pool_stop();
        false -> ok
    end,

    %% Clean up regular event loops
    case persistent_term:get(?PT_LOOPS, {}) of
        {} -> ok;
        Loops ->
            lists:foreach(fun({LoopRef, WorkerPid}) ->
                catch py_event_worker:stop(WorkerPid),
                catch py_nif:event_loop_destroy(LoopRef)
            end, tuple_to_list(Loops))
    end,

    catch persistent_term:erase(?PT_LOOPS),
    catch persistent_term:erase(?PT_NUM_LOOPS),
    catch persistent_term:erase(?PT_OWNGIL_ENABLED),
    catch persistent_term:erase(?PT_SESSIONS),
    ok.

%%% ============================================================================
%%% Internal functions
%%% ============================================================================

%% @private Get the pool size
-spec pool_size() -> non_neg_integer().
pool_size() ->
    persistent_term:get(?PT_NUM_LOOPS, 0).

%% @private Get a loop by 1-based index
-spec get_loop_by_index(pos_integer()) -> {reference(), pid()}.
get_loop_by_index(Idx) ->
    Loops = persistent_term:get(?PT_LOOPS),
    element(Idx, Loops).

%% @private Check if OWN_GIL mode is enabled
-spec is_owngil_enabled() -> boolean().
is_owngil_enabled() ->
    persistent_term:get(?PT_OWNGIL_ENABLED, false).

%% @private Get the sessions ETS table
-spec get_sessions_table() -> ets:tid() | undefined.
get_sessions_table() ->
    persistent_term:get(?PT_SESSIONS, undefined).

%% @private Get or create OWN_GIL session for calling process
%% Returns {ok, {WorkerId, HandleId, LoopIdx}} or {error, Reason}
-spec ensure_session() -> {ok, {non_neg_integer(), non_neg_integer(), pos_integer()}} | {error, term()}.
ensure_session() ->
    Pid = self(),
    N = pool_size(),
    case N of
        0 -> {error, not_available};
        _ ->
            %% Hash PID to get consistent loop assignment
            Hash = erlang:phash2(Pid),
            LoopIdx = (Hash rem N) + 1,
            ensure_session(Pid, LoopIdx)
    end.

%% @private Get or create session for a specific PID and loop index
-spec ensure_session(pid(), pos_integer()) -> {ok, {non_neg_integer(), non_neg_integer(), pos_integer()}} | {error, term()}.
ensure_session(Pid, LoopIdx) ->
    case get_sessions_table() of
        undefined ->
            {error, owngil_not_enabled};
        Tid ->
            Key = {Pid, LoopIdx},
            case ets:lookup(Tid, Key) of
                [#owngil_session{worker_id = WorkerId, handle_id = HandleId}] ->
                    %% Session exists
                    {ok, {WorkerId, HandleId, LoopIdx}};
                [] ->
                    %% Create new session
                    create_session(Tid, Pid, LoopIdx)
            end
    end.

%% @private Create a new OWN_GIL session
-spec create_session(ets:tid(), pid(), pos_integer()) -> {ok, {non_neg_integer(), non_neg_integer(), pos_integer()}} | {error, term()}.
create_session(Tid, Pid, LoopIdx) ->
    %% Create session via NIF - assigns worker and creates namespace
    case py_nif:owngil_create_session(LoopIdx - 1) of  %% Convert to 0-based index
        {ok, WorkerId, HandleId} ->
            %% Monitor the process for cleanup on exit
            MonRef = erlang:monitor(process, Pid),
            Session = #owngil_session{
                key = {Pid, LoopIdx},
                worker_id = WorkerId,
                handle_id = HandleId,
                monitor_ref = MonRef
            },
            %% Use insert_new to handle race conditions
            case ets:insert_new(Tid, Session) of
                true ->
                    {ok, {WorkerId, HandleId, LoopIdx}};
                false ->
                    %% Another process created the session first, destroy ours
                    erlang:demonitor(MonRef, [flush]),
                    catch py_nif:owngil_destroy_session(WorkerId, HandleId),
                    %% Retry lookup
                    case ets:lookup(Tid, {Pid, LoopIdx}) of
                        [#owngil_session{worker_id = W, handle_id = H}] ->
                            {ok, {W, H, LoopIdx}};
                        [] ->
                            {error, session_conflict}
                    end
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Destroy an OWN_GIL session
-spec destroy_session(pid(), pos_integer()) -> ok.
destroy_session(Pid, LoopIdx) ->
    case get_sessions_table() of
        undefined -> ok;
        Tid ->
            Key = {Pid, LoopIdx},
            case ets:lookup(Tid, Key) of
                [#owngil_session{worker_id = WorkerId, handle_id = HandleId, monitor_ref = MonRef}] ->
                    %% Demonitor the process
                    erlang:demonitor(MonRef, [flush]),
                    %% Destroy session in worker
                    catch py_nif:owngil_destroy_session(WorkerId, HandleId),
                    %% Remove from ETS
                    ets:delete(Tid, Key),
                    ok;
                [] ->
                    ok
            end
    end.
