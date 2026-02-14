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

%%% @doc Worker pool manager for async Python execution.
%%%
%%% Manages a pool of async workers that have background asyncio event loops.
%%% Distributes async requests across workers using round-robin scheduling.
%%%
%%% @private
-module(py_async_pool).
-behaviour(gen_server).

-export([
    start_link/1,
    request/1,
    get_stats/0
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
    worker_sup :: pid()
}).

%%% ============================================================================
%%% API
%%% ============================================================================

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(NumWorkers) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NumWorkers], []).

%% @doc Submit an async request to be executed by a worker.
-spec request(term()) -> ok.
request(Request) ->
    gen_server:cast(?MODULE, {request, Request}).

%% @doc Get pool statistics.
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?MODULE, get_stats).

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([NumWorkers]) ->
    process_flag(trap_exit, true),

    %% Start worker supervisor
    {ok, WorkerSup} = py_async_worker_sup:start_link(),

    %% Start workers
    Workers = start_workers(WorkerSup, NumWorkers),

    {ok, #state{
        workers = queue:from_list(Workers),
        num_workers = NumWorkers,
        pending = 0,
        worker_sup = WorkerSup
    }}.

handle_call(get_stats, _From, State) ->
    Stats = #{
        num_workers => State#state.num_workers,
        pending_requests => State#state.pending,
        available_workers => queue:len(State#state.workers)
    },
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({request, Request}, State) ->
    case queue:out(State#state.workers) of
        {{value, Worker}, Rest} ->
            %% Send request to worker
            Worker ! {py_async_request, Request},
            %% Put worker at end of queue (round-robin)
            NewWorkers = queue:in(Worker, Rest),
            {noreply, State#state{
                workers = NewWorkers,
                pending = State#state.pending + 1
            }};
        {empty, _} ->
            error_logger:warning_msg("py_async_pool: no workers available~n"),
            {Ref, Caller, _} = extract_ref_caller(Request),
            Caller ! {py_error, Ref, no_workers_available},
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({worker_done, _WorkerPid}, State) ->
    {noreply, State#state{pending = max(0, State#state.pending - 1)}};

handle_info({'EXIT', Pid, Reason}, State) ->
    error_logger:error_msg("py_async_pool: worker ~p died: ~p~n", [Pid, Reason]),
    %% Remove dead worker from queue and start a new one
    Workers = queue:filter(fun(W) -> W =/= Pid end, State#state.workers),
    NewWorker = py_async_worker_sup:start_worker(State#state.worker_sup),
    NewWorkers = queue:in(NewWorker, Workers),
    {noreply, State#state{workers = NewWorkers}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Shutdown all workers
    Workers = queue:to_list(State#state.workers),
    lists:foreach(fun(W) -> W ! shutdown end, Workers),
    ok.

%%% ============================================================================
%%% Internal functions
%%% ============================================================================

start_workers(Sup, N) ->
    [py_async_worker_sup:start_worker(Sup) || _ <- lists:seq(1, N)].

extract_ref_caller({async_call, Ref, Caller, _, _, _, _}) -> {Ref, Caller, async_call};
extract_ref_caller({async_gather, Ref, Caller, _}) -> {Ref, Caller, async_gather};
extract_ref_caller({async_stream, Ref, Caller, _, _, _, _}) -> {Ref, Caller, async_stream}.
