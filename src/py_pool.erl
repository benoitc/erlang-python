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

%% @doc Submit a request to be executed by a worker.
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

    %% Initialize Python interpreter
    case py_nif:init() of
        ok ->
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
        available_workers => queue:len(State#state.workers)
    },
    {reply, Stats, State};

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

handle_info({'EXIT', Pid, Reason}, State) ->
    error_logger:error_msg("py_pool: worker ~p died: ~p~n", [Pid, Reason]),
    %% Remove dead worker from queue and start a new one
    Workers = queue:filter(fun(W) -> W =/= Pid end, State#state.workers),
    NewWorker = py_worker_sup:start_worker(State#state.worker_sup),
    NewWorkers = queue:in(NewWorker, Workers),
    {noreply, State#state{workers = NewWorkers}};

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
