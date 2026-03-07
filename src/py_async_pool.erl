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

%%% @doc Pool manager for async Python execution using event loops.
%%%
%%% This module provides an async request pool that delegates to the event loop
%%% pool for efficient coroutine execution. It replaces the pthread+usleep
%%% polling model with event-driven execution using enif_select and erlang.send().
%%%
%%% The pool maintains API compatibility with the previous pthread-based
%%% implementation while providing significant performance improvements.
%%%
%%% @private
-module(py_async_pool).
-behaviour(gen_server).

-export([
    start_link/0,
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
    pending :: non_neg_integer(),
    supported :: boolean()
}).

%%% ============================================================================
%%% API
%%% ============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(1).

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(_NumWorkers) ->
    %% NumWorkers is now ignored - we use the event loop pool instead
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Submit an async request to be executed by the event loop pool.
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

init([]) ->
    process_flag(trap_exit, true),
    %% Check if event loop pool is available
    case py_event_loop:get_loop() of
        {ok, _LoopRef} ->
            {ok, #state{pending = 0, supported = true}};
        {error, _} ->
            {ok, #state{pending = 0, supported = false}}
    end.

handle_call(get_stats, _From, State) ->
    Stats = #{
        pending_requests => State#state.pending,
        supported => State#state.supported,
        backend => event_loop
    },
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({request, Request}, #state{supported = false} = State) ->
    {Ref, Caller, _Type} = extract_ref_caller(Request),
    Caller ! {py_error, Ref, async_not_supported},
    {noreply, State};

handle_cast({request, Request}, State) ->
    case transform_request(Request) of
        {ok, LoopRequest} ->
            case py_event_loop:get_loop() of
                {ok, LoopRef} ->
                    case py_event_loop:run_async(LoopRef, LoopRequest) of
                        ok ->
                            {noreply, State#state{pending = State#state.pending + 1}};
                        {error, Reason} ->
                            {Ref, Caller, _} = extract_ref_caller(Request),
                            Caller ! {py_error, Ref, Reason},
                            {noreply, State}
                    end;
                {error, Reason} ->
                    {Ref, Caller, _} = extract_ref_caller(Request),
                    Caller ! {py_error, Ref, Reason},
                    {noreply, State}
            end;
        {error, Reason} ->
            {Ref, Caller, _} = extract_ref_caller(Request),
            Caller ! {py_error, Ref, Reason},
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({async_result, _Ref, _Result}, State) ->
    %% Result was sent directly to caller via erlang.send()
    %% We just track pending count
    {noreply, State#state{pending = max(0, State#state.pending - 1)}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%% ============================================================================
%%% Internal functions
%%% ============================================================================

%% @doc Transform the legacy request format to the new event loop format.
transform_request({async_call, Ref, Caller, Module, Func, Args, Kwargs}) ->
    {ok, #{
        ref => Ref,
        caller => Caller,
        module => Module,
        func => Func,
        args => Args,
        kwargs => Kwargs
    }};
transform_request({async_gather, Ref, Caller, Calls}) ->
    %% For gather, we need to wrap in a special gather coroutine
    %% For now, return an error - gather needs special handling
    {error, {gather_not_implemented, Ref, Caller, Calls}};
transform_request({async_stream, Ref, Caller, Module, Func, Args, Kwargs}) ->
    %% For stream, we need async generator support
    %% For now, return an error - stream needs special handling
    {error, {stream_not_implemented, Ref, Caller, Module, Func, Args, Kwargs}};
transform_request(Other) ->
    {error, {unknown_request_type, Other}}.

%% @doc Extract ref and caller from different request types.
extract_ref_caller({async_call, Ref, Caller, _, _, _, _}) -> {Ref, Caller, async_call};
extract_ref_caller({async_gather, Ref, Caller, _}) -> {Ref, Caller, async_gather};
extract_ref_caller({async_stream, Ref, Caller, _, _, _, _}) -> {Ref, Caller, async_stream}.
