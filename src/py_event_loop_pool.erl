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

%%% @doc Pool manager for event loop-based async Python execution.
%%%
%%% This module provides a pool of event loops for executing async Python
%%% coroutines. It replaces the pthread+usleep polling model with efficient
%%% event-driven execution using enif_select and erlang.send().
%%%
%%% The pool uses round-robin scheduling to distribute work across event loops.
%%%
%%% @private
-module(py_event_loop_pool).
-behaviour(gen_server).

-export([
    start_link/0,
    start_link/1,
    run_async/1,
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
    loops :: [reference()],
    num_loops :: non_neg_integer(),
    next_idx :: non_neg_integer(),
    supported :: boolean()
}).

-define(DEFAULT_NUM_LOOPS, 1).

%%% ============================================================================
%%% API
%%% ============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(?DEFAULT_NUM_LOOPS).

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(NumLoops) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NumLoops], []).

%% @doc Submit an async request to be executed on the event loop pool.
%% The request should be a map with keys:
%%   ref => reference() - A reference to identify the result
%%   caller => pid() - The pid to send the result to
%%   module => atom() | binary() - Python module name
%%   func => atom() | binary() - Python function name
%%   args => list() - Arguments to pass to the function
%%   kwargs => map() - Keyword arguments (optional)
-spec run_async(map()) -> ok | {error, term()}.
run_async(Request) ->
    gen_server:call(?MODULE, {run_async, Request}).

%% @doc Get pool statistics.
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?MODULE, get_stats).

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([NumLoops]) ->
    process_flag(trap_exit, true),

    %% Get the event loop from py_event_loop module
    case py_event_loop:get_loop() of
        {ok, LoopRef} ->
            %% For now, use a single shared event loop
            %% In the future, we could create multiple loops for parallelism
            Loops = lists:duplicate(NumLoops, LoopRef),
            {ok, #state{
                loops = Loops,
                num_loops = NumLoops,
                next_idx = 0,
                supported = true
            }};
        {error, Reason} ->
            error_logger:warning_msg("py_event_loop_pool: event loop not available: ~p~n", [Reason]),
            {ok, #state{
                loops = [],
                num_loops = 0,
                next_idx = 0,
                supported = false
            }}
    end.

handle_call(get_stats, _From, State) ->
    Stats = #{
        num_loops => State#state.num_loops,
        next_idx => State#state.next_idx,
        supported => State#state.supported
    },
    {reply, Stats, State};

handle_call({run_async, _Request}, _From, #state{supported = false} = State) ->
    {reply, {error, event_loop_not_available}, State};

handle_call({run_async, Request}, _From, State) ->
    %% Get the next loop in round-robin fashion
    Idx = State#state.next_idx rem State#state.num_loops + 1,
    LoopRef = lists:nth(Idx, State#state.loops),

    %% Submit to the event loop
    Result = py_event_loop:run_async(LoopRef, Request),

    NextState = State#state{next_idx = Idx},
    {reply, Result, NextState};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', _Pid, _Reason}, State) ->
    %% Event loop died, mark as unsupported
    {noreply, State#state{supported = false, loops = [], num_loops = 0}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
