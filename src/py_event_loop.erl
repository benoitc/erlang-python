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

%% @doc Erlang-native asyncio event loop manager.
%%
%% This module provides the high-level interface for using the Erlang-backed
%% asyncio event loop. It manages the lifecycle of event loops and routers,
%% and registers callback functions for Python to call.
%%
%% @private
-module(py_event_loop).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    stop/0,
    get_loop/0,
    register_callbacks/0
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
    loop_ref :: reference() | undefined,
    router_pid :: pid() | undefined
}).

%% ============================================================================
%% API
%% ============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

-spec get_loop() -> {ok, reference()} | {error, not_started}.
get_loop() ->
    gen_server:call(?MODULE, get_loop).

%% @doc Register event loop callbacks for Python access.
-spec register_callbacks() -> ok.
register_callbacks() ->
    %% Register all event loop functions as callbacks
    py_callback:register(py_event_loop_new, fun cb_event_loop_new/1),
    py_callback:register(py_event_loop_destroy, fun cb_event_loop_destroy/1),
    py_callback:register(py_event_loop_set_router, fun cb_event_loop_set_router/1),
    py_callback:register(py_event_loop_wakeup, fun cb_event_loop_wakeup/1),
    py_callback:register(py_event_loop_add_reader, fun cb_add_reader/1),
    py_callback:register(py_event_loop_remove_reader, fun cb_remove_reader/1),
    py_callback:register(py_event_loop_add_writer, fun cb_add_writer/1),
    py_callback:register(py_event_loop_remove_writer, fun cb_remove_writer/1),
    py_callback:register(py_event_loop_call_later, fun cb_call_later/1),
    py_callback:register(py_event_loop_cancel_timer, fun cb_cancel_timer/1),
    py_callback:register(py_event_loop_poll_events, fun cb_poll_events/1),
    py_callback:register(py_event_loop_get_pending, fun cb_get_pending/1),
    py_callback:register(py_event_loop_dispatch_callback, fun cb_dispatch_callback/1),
    py_callback:register(py_event_loop_dispatch_timer, fun cb_dispatch_timer/1),
    ok.

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init([]) ->
    %% Register callbacks on startup
    register_callbacks(),

    %% Set isolation mode from app env (default: global)
    IsolationMode = application:get_env(erlang_python, event_loop_isolation, global),
    ok = py_nif:set_isolation_mode(IsolationMode),

    %% Create and initialize the event loop immediately
    case py_nif:event_loop_new() of
        {ok, LoopRef} ->
            {ok, RouterPid} = py_event_router:start_link(LoopRef),
            ok = py_nif:event_loop_set_router(LoopRef, RouterPid),
            %% Set shared router for per-loop created loops
            %% All loops created via _loop_new() in Python will use this router
            ok = py_nif:set_shared_router(RouterPid),
            %% Make the event loop available to Python
            ok = py_nif:set_python_event_loop(LoopRef),
            %% Set ErlangEventLoop as the default asyncio policy
            ok = set_default_policy(),
            {ok, #state{loop_ref = LoopRef, router_pid = RouterPid}};
        {error, Reason} ->
            {stop, {event_loop_init_failed, Reason}}
    end.

%% @doc Set ErlangEventLoop as the default asyncio event loop policy.
set_default_policy() ->
    PrivDir = code:priv_dir(erlang_python),
    Code = iolist_to_binary([
        "import sys\n",
        "priv_dir = '", PrivDir, "'\n",
        "if priv_dir not in sys.path:\n",
        "    sys.path.insert(0, priv_dir)\n",
        "from erlang_loop import get_event_loop_policy\n",
        "import asyncio\n",
        "asyncio.set_event_loop_policy(get_event_loop_policy())\n"
    ]),
    case py:exec(Code) of
        ok -> ok;
        {error, Reason} ->
            error_logger:warning_msg("Failed to set ErlangEventLoop policy: ~p~n", [Reason]),
            ok  %% Non-fatal
    end.

handle_call(get_loop, _From, #state{loop_ref = undefined} = State) ->
    %% Create event loop and router on demand
    case py_nif:event_loop_new() of
        {ok, LoopRef} ->
            {ok, RouterPid} = py_event_router:start_link(LoopRef),
            ok = py_nif:event_loop_set_router(LoopRef, RouterPid),
            %% Make the event loop available to Python
            ok = py_nif:set_python_event_loop(LoopRef),
            NewState = State#state{loop_ref = LoopRef, router_pid = RouterPid},
            {reply, {ok, LoopRef}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call(get_loop, _From, #state{loop_ref = LoopRef} = State) ->
    {reply, {ok, LoopRef}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{loop_ref = LoopRef, router_pid = RouterPid}) ->
    %% Reset asyncio policy back to default before destroying the loop
    reset_default_policy(),
    %% Clean up router
    case RouterPid of
        undefined -> ok;
        Pid -> py_event_router:stop(Pid)
    end,
    %% Clean up event loop
    case LoopRef of
        undefined -> ok;
        Ref -> py_nif:event_loop_destroy(Ref)
    end,
    ok.

%% @doc Reset asyncio back to the default event loop policy.
reset_default_policy() ->
    Code = <<"
import asyncio
asyncio.set_event_loop_policy(None)
">>,
    catch py:exec(Code),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ============================================================================
%% Callback implementations for Python
%% ============================================================================

cb_event_loop_new([]) ->
    py_nif:event_loop_new().

cb_event_loop_destroy([LoopRef]) ->
    py_nif:event_loop_destroy(LoopRef).

cb_event_loop_set_router([LoopRef, RouterPid]) ->
    py_nif:event_loop_set_router(LoopRef, RouterPid).

cb_event_loop_wakeup([LoopRef]) ->
    py_nif:event_loop_wakeup(LoopRef).

cb_add_reader([LoopRef, Fd, CallbackId]) ->
    py_nif:add_reader(LoopRef, Fd, CallbackId).

cb_remove_reader([LoopRef, FdRef]) ->
    py_nif:remove_reader(LoopRef, FdRef).

cb_add_writer([LoopRef, Fd, CallbackId]) ->
    py_nif:add_writer(LoopRef, Fd, CallbackId).

cb_remove_writer([LoopRef, FdRef]) ->
    py_nif:remove_writer(LoopRef, FdRef).

cb_call_later([LoopRef, DelayMs, CallbackId]) ->
    py_nif:call_later(LoopRef, DelayMs, CallbackId).

cb_cancel_timer([LoopRef, TimerRef]) ->
    py_nif:cancel_timer(LoopRef, TimerRef).

cb_poll_events([LoopRef, TimeoutMs]) ->
    py_nif:poll_events(LoopRef, TimeoutMs).

cb_get_pending([LoopRef]) ->
    py_nif:get_pending(LoopRef).

cb_dispatch_callback([LoopRef, CallbackId, Type]) ->
    py_nif:dispatch_callback(LoopRef, CallbackId, Type).

cb_dispatch_timer([LoopRef, CallbackId]) ->
    py_nif:dispatch_timer(LoopRef, CallbackId).
