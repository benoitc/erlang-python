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

%%% @doc Worker pool manager for Python execution.
%%%
%%% Manages a pool of dirty NIF workers that execute Python code.
%%% Distributes requests across workers using round-robin scheduling.
%%%
%%% @private
-module(py_pool).
-behaviour(gen_server).

-export([
    start_link/1,
    request/1,
    broadcast/1,
    get_stats/0,
    %% Context affinity API
    checkout/1,
    checkin/1,
    lookup_binding/1,
    direct_request/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {
    workers :: queue:queue(pid()),
    num_workers :: pos_integer(),
    pending :: non_neg_integer(),
    worker_sup :: pid(),
    %% Context affinity tracking
    checked_out = #{} :: #{pid() => checkout_info()},
    monitors = #{} :: #{reference() => binding_key()}
}).

-type binding_key() :: {process, pid()} | {context, reference()}.
-type checkout_info() :: #{key := binding_key(), monitor := reference()}.

%%% ============================================================================
%%% API
%%% ============================================================================

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(NumWorkers) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NumWorkers], []).

%% @doc Submit a request to be executed by a worker.
-spec request(term()) -> ok.
request(Request) ->
    gen_server:cast(?MODULE, {request, Request}).

%% @doc Broadcast a request to all workers.
%% Returns a list of results from each worker.
-spec broadcast(term()) -> [{ok, term()} | {error, term()}].
broadcast(Request) ->
    gen_server:call(?MODULE, {broadcast, Request}, infinity).

%% @doc Get pool statistics.
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?MODULE, get_stats).

%% @doc Checkout a worker for exclusive use by a binding key.
%% The worker is removed from the available pool and associated with the key.
-spec checkout(binding_key()) -> {ok, pid()} | {error, no_workers_available}.
checkout(Key) ->
    gen_server:call(?MODULE, {checkout, Key}).

%% @doc Return a checked-out worker to the pool.
%% This is synchronous to ensure the worker is returned before continuing.
-spec checkin(binding_key()) -> ok.
checkin(Key) ->
    gen_server:call(?MODULE, {checkin, Key}).

%% @doc Look up a binding to find the associated worker.
%% Fast O(1) ETS lookup.
-spec lookup_binding(binding_key()) -> {ok, pid()} | not_found.
lookup_binding(Key) ->
    case ets:lookup(py_bindings, Key) of
        [{_, Worker}] -> {ok, Worker};
        [] -> not_found
    end.

%% @doc Send a request directly to a specific worker.
-spec direct_request(pid(), term()) -> ok.
direct_request(Worker, Request) ->
    Worker ! {py_request, Request},
    ok.

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([NumWorkers]) ->
    process_flag(trap_exit, true),

    %% Initialize Python interpreter
    case py_nif:init() of
        ok ->
            %% Create bindings ETS table for fast lookup
            _ = ets:new(py_bindings, [named_table, public, set, {read_concurrency, true}]),

            %% Start worker supervisor
            {ok, WorkerSup} = py_worker_sup:start_link(),

            %% Start workers
            Workers = start_workers(WorkerSup, NumWorkers),

            {ok, #state{
                workers = queue:from_list(Workers),
                num_workers = NumWorkers,
                pending = 0,
                worker_sup = WorkerSup
            }};
        {error, Reason} ->
            {stop, {python_init_failed, Reason}}
    end.

handle_call(get_stats, _From, State) ->
    Stats = #{
        num_workers => State#state.num_workers,
        pending_requests => State#state.pending,
        available_workers => queue:len(State#state.workers),
        checked_out => maps:size(State#state.checked_out)
    },
    {reply, Stats, State};

handle_call({checkout, Key}, {Owner, _}, State) ->
    case queue:out(State#state.workers) of
        {{value, Worker}, Rest} ->
            MonRef = erlang:monitor(process, Owner),
            ets:insert(py_bindings, {Key, Worker}),
            Info = #{key => Key, monitor => MonRef},
            NewState = State#state{
                workers = Rest,
                checked_out = maps:put(Worker, Info, State#state.checked_out),
                monitors = maps:put(MonRef, Key, State#state.monitors)
            },
            {reply, {ok, Worker}, NewState};
        {empty, _} ->
            {reply, {error, no_workers_available}, State}
    end;

handle_call({broadcast, Request}, _From, State) ->
    %% Send request to all workers and collect results
    Workers = queue:to_list(State#state.workers),
    Results = broadcast_to_workers(Workers, Request),
    {reply, Results, State};

handle_call({checkin, Key}, _From, State) ->
    case ets:lookup(py_bindings, Key) of
        [{_, Worker}] ->
            ets:delete(py_bindings, Key),
            case maps:get(Worker, State#state.checked_out, undefined) of
                #{monitor := MonRef} ->
                    erlang:demonitor(MonRef, [flush]),
                    NewState = State#state{
                        workers = queue:in(Worker, State#state.workers),
                        checked_out = maps:remove(Worker, State#state.checked_out),
                        monitors = maps:remove(MonRef, State#state.monitors)
                    },
                    {reply, ok, NewState};
                undefined ->
                    {reply, ok, State}
            end;
        [] ->
            {reply, ok, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({request, Request}, State) ->
    case queue:out(State#state.workers) of
        {{value, Worker}, Rest} ->
            %% Send request to worker
            Worker ! {py_request, Request},
            %% Put worker at end of queue (round-robin)
            NewWorkers = queue:in(Worker, Rest),
            {noreply, State#state{
                workers = NewWorkers,
                pending = State#state.pending + 1
            }};
        {empty, _} ->
            %% No workers available - this shouldn't happen with proper sizing
            %% For now, we'll queue the request (could add backpressure here)
            error_logger:warning_msg("py_pool: no workers available~n"),
            {Ref, Caller, _} = extract_ref_caller(Request),
            Caller ! {py_error, Ref, no_workers_available},
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({worker_done, _WorkerPid}, State) ->
    {noreply, State#state{pending = max(0, State#state.pending - 1)}};

handle_info({'DOWN', MonRef, process, _Pid, _Reason}, State) ->
    %% Bound process died - return worker to pool
    case maps:get(MonRef, State#state.monitors, undefined) of
        undefined ->
            {noreply, State};
        Key ->
            case ets:lookup(py_bindings, Key) of
                [{_, Worker}] ->
                    ets:delete(py_bindings, Key),
                    {noreply, State#state{
                        workers = queue:in(Worker, State#state.workers),
                        checked_out = maps:remove(Worker, State#state.checked_out),
                        monitors = maps:remove(MonRef, State#state.monitors)
                    }};
                [] ->
                    {noreply, State#state{monitors = maps:remove(MonRef, State#state.monitors)}}
            end
    end;

handle_info({'EXIT', Pid, Reason}, State) ->
    error_logger:error_msg("py_pool: worker ~p died: ~p~n", [Pid, Reason]),
    %% Clean up if this was a checked-out worker
    NewState = case maps:get(Pid, State#state.checked_out, undefined) of
        #{key := Key, monitor := MonRef} ->
            ets:delete(py_bindings, Key),
            erlang:demonitor(MonRef, [flush]),
            State#state{
                checked_out = maps:remove(Pid, State#state.checked_out),
                monitors = maps:remove(MonRef, State#state.monitors)
            };
        undefined ->
            State
    end,
    %% Remove dead worker from queue and start a new one
    Workers = queue:filter(fun(W) -> W =/= Pid end, NewState#state.workers),
    NewWorker = py_worker_sup:start_worker(NewState#state.worker_sup),
    NewWorkers = queue:in(NewWorker, Workers),
    {noreply, NewState#state{workers = NewWorkers}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% Finalize Python interpreter
    py_nif:finalize(),
    ok.

%%% ============================================================================
%%% Internal functions
%%% ============================================================================

start_workers(Sup, N) ->
    [py_worker_sup:start_worker(Sup) || _ <- lists:seq(1, N)].

extract_ref_caller({call, Ref, Caller, _, _, _, _}) -> {Ref, Caller, call};
extract_ref_caller({eval, Ref, Caller, _, _}) -> {Ref, Caller, eval};
extract_ref_caller({exec, Ref, Caller, _}) -> {Ref, Caller, exec};
extract_ref_caller({stream, Ref, Caller, _, _, _, _}) -> {Ref, Caller, stream}.

%% @private
%% Send a request to all workers and collect results
broadcast_to_workers(Workers, RequestTemplate) ->
    Self = self(),
    %% Send requests to all workers in parallel
    Refs = lists:map(fun(Worker) ->
        Ref = make_ref(),
        Request = inject_ref_caller(RequestTemplate, Ref, Self),
        Worker ! {py_request, Request},
        Ref
    end, Workers),
    %% Collect all responses
    lists:map(fun(Ref) ->
        receive
            {py_response, Ref, Result} -> Result;
            {py_error, Ref, Error} -> {error, Error}
        after 30000 ->
            {error, timeout}
        end
    end, Refs).

%% @private
%% Inject a reference and caller into a request template
inject_ref_caller({exec, _Ref, _Caller, Code}, NewRef, NewCaller) ->
    {exec, NewRef, NewCaller, Code};
inject_ref_caller({eval, _Ref, _Caller, Code, Locals}, NewRef, NewCaller) ->
    {eval, NewRef, NewCaller, Code, Locals};
inject_ref_caller({eval, _Ref, _Caller, Code, Locals, Timeout}, NewRef, NewCaller) ->
    {eval, NewRef, NewCaller, Code, Locals, Timeout}.
