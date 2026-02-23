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

%% @doc Async driver for unified event loop management.
%%
%% This module provides a high-level interface for submitting async
%% coroutines through the unified ErlangEventLoop architecture.
%%
%% All async operations go through this driver:
%% - py:async_call routes here
%% - py_asgi:run_async routes here
%%
%% The driver owns a py_event_loop_proc and coordinates:
%% - Submitting coroutines via py_nif:submit_coroutine
%% - Receiving results via the event loop process
%% - Dispatching results to waiting callers
%%
%% @private
-module(py_async_driver).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    stop/0,
    submit/4,
    submit/5,
    get_event_proc/0
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
    %% Event loop process for receiving results
    event_proc :: pid(),
    %% Loop reference for the event loop
    loop_ref :: reference()
}).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start the async driver.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the async driver.
-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%% @doc Submit a coroutine for async execution.
%% Returns a reference that will receive the result as:
%%   {py_result, Ref, Result} or {py_error, Ref, Error}
-spec submit(Module, Func, Args, Kwargs) -> {ok, reference()} | {error, term()} when
    Module :: binary() | string() | atom(),
    Func :: binary() | string() | atom(),
    Args :: list(),
    Kwargs :: map().
submit(Module, Func, Args, Kwargs) ->
    submit(Module, Func, Args, Kwargs, #{}).

%% @doc Submit a coroutine with options.
-spec submit(Module, Func, Args, Kwargs, Opts) -> {ok, reference()} | {error, term()} when
    Module :: binary() | string() | atom(),
    Func :: binary() | string() | atom(),
    Args :: list(),
    Kwargs :: map(),
    Opts :: map().
submit(Module, Func, Args, Kwargs, _Opts) ->
    ModBin = to_binary(Module),
    FuncBin = to_binary(Func),

    %% Get the event proc
    case get_event_proc() of
        {ok, EventProc} ->
            %% Generate callback ID and ref
            CallbackId = py_callback_id:next(),
            Ref = make_ref(),

            %% Register to receive the result
            ok = py_event_loop_proc:register_call(EventProc, CallbackId, Ref),

            %% Submit the coroutine
            case py_nif:submit_coroutine(EventProc, CallbackId, ModBin, FuncBin, Args, Kwargs) of
                ok ->
                    {ok, Ref};
                {error, Reason} ->
                    %% Cleanup registration on failure
                    py_event_loop_proc:unregister_call(EventProc, CallbackId),
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Get the event loop process.
%% Uses persistent_term for fast cached lookup instead of gen_server:call.
-spec get_event_proc() -> {ok, pid()} | {error, not_started}.
get_event_proc() ->
    case persistent_term:get({?MODULE, event_proc}, undefined) of
        undefined ->
            %% Fall back to gen_server:call if not yet cached
            gen_server:call(?MODULE, get_event_proc);
        Pid when is_pid(Pid) ->
            {ok, Pid}
    end.

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init([]) ->
    process_flag(trap_exit, true),

    %% Create the event loop reference
    LoopRef = make_ref(),

    %% Start the event loop process
    {ok, EventProc} = py_event_loop_proc:start_link(LoopRef),

    %% Cache the event proc pid for fast lookup
    persistent_term:put({?MODULE, event_proc}, EventProc),

    {ok, #state{
        event_proc = EventProc,
        loop_ref = LoopRef
    }}.

handle_call(get_event_proc, _From, #state{event_proc = EventProc} = State) ->
    {reply, {ok, EventProc}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', EventProc, Reason}, #state{event_proc = EventProc} = State) ->
    %% Event loop process died, restart it
    error_logger:warning_msg("py_async_driver: event loop proc died: ~p, restarting~n", [Reason]),
    LoopRef = make_ref(),
    {ok, NewEventProc} = py_event_loop_proc:start_link(LoopRef),
    %% Update cached pid
    persistent_term:put({?MODULE, event_proc}, NewEventProc),
    {noreply, State#state{event_proc = NewEventProc, loop_ref = LoopRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{event_proc = EventProc}) ->
    %% Clear cached pid
    persistent_term:erase({?MODULE, event_proc}),
    py_event_loop_proc:stop(EventProc),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ============================================================================
%% Internal
%% ============================================================================

to_binary(Bin) when is_binary(Bin) -> Bin;
to_binary(List) when is_list(List) -> list_to_binary(List);
to_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
