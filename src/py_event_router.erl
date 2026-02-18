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

%% @doc Event router for Erlang-native asyncio event loop.
%%
%% This gen_server receives:
%% - `{select, FdRes, Ref, ready_input|ready_output}' from enif_select
%% - `{start_timer, DelayMs, CallbackId, TimerRef}' from call_later NIF
%% - Timer expiration messages from erlang:send_after
%%
%% It dispatches these events to the Python event loop via dispatch_callback NIFs.
-module(py_event_router).
-behaviour(gen_server).

%% API
-export([
    start_link/1,
    start_link/2,
    stop/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    loop_ref :: reference(),
    %% Map of TimerRef -> {ErlangTimerRef, CallbackId}
    timers = #{} :: #{reference() => {reference(), non_neg_integer()}}
}).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start the event router with a loop reference.
-spec start_link(reference()) -> {ok, pid()} | {error, term()}.
start_link(LoopRef) ->
    start_link(LoopRef, []).

%% @doc Start the event router with a loop reference and options.
-spec start_link(reference(), list()) -> {ok, pid()} | {error, term()}.
start_link(LoopRef, Opts) ->
    case proplists:get_value(name, Opts) of
        undefined ->
            gen_server:start_link(?MODULE, [LoopRef], []);
        Name ->
            gen_server:start_link({local, Name}, ?MODULE, [LoopRef], [])
    end.

%% @doc Stop the event router.
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init([LoopRef]) ->
    process_flag(trap_exit, true),
    {ok, #state{loop_ref = LoopRef}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle enif_select messages for read readiness
handle_info({select, FdRes, _Ref, ready_input}, State) ->
    #state{loop_ref = LoopRef} = State,
    py_nif:handle_fd_event(FdRes, read),
    %% Re-register for more events (enif_select is one-shot)
    py_nif:reselect_reader(LoopRef, FdRes),
    {noreply, State};

%% Handle enif_select messages for write readiness
handle_info({select, FdRes, _Ref, ready_output}, State) ->
    #state{loop_ref = LoopRef} = State,
    py_nif:handle_fd_event(FdRes, write),
    %% Re-register for more events (enif_select is one-shot)
    py_nif:reselect_writer(LoopRef, FdRes),
    {noreply, State};

%% Handle timer start request from call_later NIF
handle_info({start_timer, DelayMs, CallbackId, TimerRef}, State) ->
    #state{timers = Timers} = State,
    %% Create the actual Erlang timer
    ErlTimerRef = erlang:send_after(DelayMs, self(), {timeout, TimerRef, CallbackId}),
    NewTimers = maps:put(TimerRef, {ErlTimerRef, CallbackId}, Timers),
    {noreply, State#state{timers = NewTimers}};

%% Handle timer cancellation
handle_info({cancel_timer, TimerRef}, State) ->
    #state{timers = Timers} = State,
    case maps:get(TimerRef, Timers, undefined) of
        undefined ->
            {noreply, State};
        {ErlTimerRef, _CallbackId} ->
            erlang:cancel_timer(ErlTimerRef),
            NewTimers = maps:remove(TimerRef, Timers),
            {noreply, State#state{timers = NewTimers}}
    end;

%% Handle timer expiration
handle_info({timeout, TimerRef, CallbackId}, State) ->
    #state{loop_ref = LoopRef, timers = Timers} = State,
    %% Dispatch the timer callback
    py_nif:dispatch_timer(LoopRef, CallbackId),
    %% Remove from active timers
    NewTimers = maps:remove(TimerRef, Timers),
    {noreply, State#state{timers = NewTimers}};

%% Handle select stop notifications
handle_info({select, _FdRes, _Ref, cancelled}, State) ->
    %% fd monitoring was cancelled, nothing to do
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{timers = Timers}) ->
    %% Cancel all pending timers
    maps:foreach(fun(_TimerRef, {ErlTimerRef, _CallbackId}) ->
        erlang:cancel_timer(ErlTimerRef)
    end, Timers),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ============================================================================
%% Internal functions
%% ============================================================================

%% Note: get_fd_callback_id is no longer needed locally since handle_fd_event
%% combines get_callback_id + dispatch + auto-reselect in a single NIF call.
