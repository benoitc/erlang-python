%% @doc ETS-based registry for event workers.
%% Provides O(1) worker lookup by loop_id.
-module(py_event_worker_registry).
-behaviour(gen_server).

-export([start_link/0, register/3, unregister/1, lookup/1, lookup_pid/1, list_all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TAB, py_event_workers).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register(LoopId, WorkerPid, LoopRef) ->
    gen_server:call(?MODULE, {register, LoopId, WorkerPid, LoopRef}).

unregister(LoopId) ->
    gen_server:call(?MODULE, {unregister, LoopId}).

lookup(LoopId) ->
    case ets:lookup(?TAB, LoopId) of
        [{LoopId, WorkerPid, LoopRef}] -> {ok, {WorkerPid, LoopRef}};
        [] -> {error, not_found}
    end.

lookup_pid(LoopId) ->
    case ets:lookup(?TAB, LoopId) of
        [{LoopId, WorkerPid, _}] -> {ok, WorkerPid};
        [] -> {error, not_found}
    end.

list_all() -> ets:tab2list(?TAB).

init([]) ->
    ?TAB = ets:new(?TAB, [named_table, public, set, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, LoopId, WorkerPid, LoopRef}, _From, State) ->
    true = ets:insert(?TAB, {LoopId, WorkerPid, LoopRef}),
    MonRef = erlang:monitor(process, WorkerPid),
    NewState = maps:put(WorkerPid, {LoopId, MonRef}, State),
    {reply, ok, NewState};

handle_call({unregister, LoopId}, _From, State) ->
    case ets:lookup(?TAB, LoopId) of
        [{LoopId, WorkerPid, _}] ->
            true = ets:delete(?TAB, LoopId),
            case maps:get(WorkerPid, State, undefined) of
                {LoopId, MonRef} ->
                    erlang:demonitor(MonRef, [flush]),
                    {reply, ok, maps:remove(WorkerPid, State)};
                _ -> {reply, ok, State}
            end;
        [] -> {reply, ok, State}
    end;

handle_call(_, _, State) -> {reply, {error, unknown_request}, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info({'DOWN', MonRef, process, WorkerPid, _Reason}, State) ->
    case maps:get(WorkerPid, State, undefined) of
        {LoopId, MonRef} ->
            true = ets:delete(?TAB, LoopId),
            {noreply, maps:remove(WorkerPid, State)};
        _ -> {noreply, State}
    end;
handle_info(_, State) -> {noreply, State}.

terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.
