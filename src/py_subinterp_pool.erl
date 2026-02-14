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

%%% @doc Worker pool manager for sub-interpreter Python execution.
%%%
%%% Manages a pool of sub-interpreter workers that have their own GIL,
%%% allowing true parallel execution on Python 3.12+.
%%%
%%% @private
-module(py_subinterp_pool).
-behaviour(gen_server).

-export([
    start_link/1,
    request/1,
    parallel/1,
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
    worker_refs :: [reference()],  %% NIF refs for parallel_execute
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

%% @doc Execute multiple calls in parallel using all available sub-interpreters.
%% Returns when all calls are complete.
-spec parallel([{atom() | binary(), atom() | binary(), list()}]) ->
    {ok, list()} | {error, term()}.
parallel(Calls) ->
    gen_server:call(?MODULE, {parallel, Calls}, 60000).

%% @doc Get pool statistics.
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?MODULE, get_stats).

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([NumWorkers]) ->
    process_flag(trap_exit, true),

    %% Check if sub-interpreters are supported
    case py_nif:subinterp_supported() of
        true ->
            %% Start worker supervisor
            {ok, WorkerSup} = py_subinterp_worker_sup:start_link(),

            %% Start workers and collect their NIF refs
            {Workers, WorkerRefs} = start_workers(WorkerSup, NumWorkers),

            {ok, #state{
                workers = queue:from_list(Workers),
                worker_refs = WorkerRefs,
                num_workers = NumWorkers,
                pending = 0,
                worker_sup = WorkerSup
            }};
        false ->
            {stop, {error, subinterpreters_not_supported}}
    end.

handle_call(get_stats, _From, State) ->
    Stats = #{
        num_workers => State#state.num_workers,
        pending_requests => State#state.pending,
        available_workers => queue:len(State#state.workers)
    },
    {reply, Stats, State};

handle_call({parallel, Calls}, From, State) ->
    %% For parallel execution, we use the NIF refs directly
    BinCalls = [{to_binary(M), to_binary(F), A} || {M, F, A} <- Calls],
    %% Execute in a separate process to not block gen_server
    Self = self(),
    WorkerRefs = State#state.worker_refs,
    spawn_link(fun() ->
        Result = py_nif:parallel_execute(WorkerRefs, BinCalls),
        gen_server:reply(From, Result),
        Self ! parallel_done
    end),
    {noreply, State#state{pending = State#state.pending + 1}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({request, Request}, State) ->
    case queue:out(State#state.workers) of
        {{value, Worker}, Rest} ->
            Worker ! {py_subinterp_request, Request},
            NewWorkers = queue:in(Worker, Rest),
            {noreply, State#state{
                workers = NewWorkers,
                pending = State#state.pending + 1
            }};
        {empty, _} ->
            error_logger:warning_msg("py_subinterp_pool: no workers available~n"),
            {Ref, Caller, _} = extract_ref_caller(Request),
            Caller ! {py_error, Ref, no_workers_available},
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(parallel_done, State) ->
    {noreply, State#state{pending = max(0, State#state.pending - 1)}};

handle_info({worker_done, _WorkerPid}, State) ->
    {noreply, State#state{pending = max(0, State#state.pending - 1)}};

handle_info({'EXIT', Pid, Reason}, State) ->
    error_logger:error_msg("py_subinterp_pool: worker ~p died: ~p~n", [Pid, Reason]),
    Workers = queue:filter(fun(W) -> W =/= Pid end, State#state.workers),
    {NewWorker, NewRef} = py_subinterp_worker_sup:start_worker_with_ref(State#state.worker_sup),
    NewWorkers = queue:in(NewWorker, Workers),
    %% Update worker refs (replace the dead one)
    NewRefs = lists:map(fun(R) ->
        %% Note: This is simplified - in production you'd track which ref died
        R
    end, State#state.worker_refs),
    {noreply, State#state{workers = NewWorkers, worker_refs = [NewRef | NewRefs]}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Workers = queue:to_list(State#state.workers),
    lists:foreach(fun(W) -> W ! shutdown end, Workers),
    ok.

%%% ============================================================================
%%% Internal functions
%%% ============================================================================

start_workers(Sup, N) ->
    Results = [py_subinterp_worker_sup:start_worker_with_ref(Sup) || _ <- lists:seq(1, N)],
    {[Pid || {Pid, _Ref} <- Results], [Ref || {_Pid, Ref} <- Results]}.

extract_ref_caller({call, Ref, Caller, _, _, _, _}) -> {Ref, Caller, call}.

to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.
