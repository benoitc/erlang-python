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

%% @doc Event loop v2 - uses py_event_loop_proc for timer/FD event collection.
%%
%% This module provides a drop-in replacement for the traditional
%% py_event_router + pthread_cond architecture. Benefits:
%%
%% - Timer fast path: timers fire directly to event process
%% - FD events: enif_select targets event process directly
%% - Erlang mailbox as event queue (still uses pthread_cond for Python sync)
%%
%% Usage:
%%   {ok, LoopRef, EventProc} = py_event_loop_v2:new(),
%%   %% Python can now use the loop
%%   py_event_loop_v2:destroy(LoopRef, EventProc).
-module(py_event_loop_v2).

-export([
    new/0,
    destroy/2,
    poll/2,
    poll_to_pending/2
]).

%% @doc Create a new event loop with event process.
%% Returns {ok, LoopRef, EventProcPid}.
-spec new() -> {ok, reference(), pid()}.
new() ->
    %% Create the NIF event loop
    {ok, LoopRef} = py_nif:event_loop_new(),

    %% Start the event process
    {ok, EventProc} = py_event_loop_proc:start_link(LoopRef),

    %% Set the event process (this also sets router_pid for FD registration)
    ok = py_nif:event_loop_set_event_proc(LoopRef, EventProc),

    {ok, LoopRef, EventProc}.

%% @doc Destroy the event loop and stop the event process.
-spec destroy(reference(), pid()) -> ok.
destroy(LoopRef, EventProc) ->
    py_event_loop_proc:stop(EventProc),
    py_nif:event_loop_destroy(LoopRef),
    ok.

%% @doc Poll for events with timeout.
%% This polls the event process directly (pure Erlang, no pthread_cond).
-spec poll(pid(), non_neg_integer()) -> [{non_neg_integer(), read | write | timer}].
poll(EventProc, TimeoutMs) ->
    py_event_loop_proc:poll(EventProc, TimeoutMs).

%% @doc Poll events and dispatch to the C pending queue.
%% This bridges the event process to the existing pthread_cond based Python polling.
%% Events are collected from event process and added to the C pending queue,
%% then pthread_cond is signaled so Python's poll_events wakes up.
-spec poll_to_pending(reference(), pid()) -> ok.
poll_to_pending(LoopRef, EventProc) ->
    %% Get events from event process (non-blocking)
    Events = py_event_loop_proc:poll(EventProc, 0),

    %% Dispatch each event to C pending queue
    lists:foreach(fun({CallbackId, Type}) ->
        TypeAtom = case Type of
            read -> read;
            write -> write;
            timer -> timer
        end,
        py_nif:dispatch_callback(LoopRef, CallbackId, TypeAtom)
    end, Events),

    %% Wake up Python if there were events
    case Events of
        [] -> ok;
        _ -> py_nif:event_loop_wakeup(LoopRef)
    end,
    ok.
