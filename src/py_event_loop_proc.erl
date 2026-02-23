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

%% @doc Event loop process - one per Python interpreter/event loop.
%%
%% This process IS the event queue. Instead of using pthread_cond and
%% mutex in C, we use the Erlang mailbox as the event queue.
%%
%% Benefits:
%% - No pthread synchronization needed in C
%% - Timer fast path: erlang:send_after sends directly here
%% - FD events from enif_select come directly here
%% - poll_events just asks this process for pending events
%%
%% Message protocol:
%% - {select, FdRes, Ref, ready_input|ready_output} - FD ready
%% - {timeout, TimerRef} - Timer fired
%% - {poll, From, TimeoutMs} - Get pending events
%% - {start_timer, DelayMs, CallbackId} - Start a timer
%% - {cancel_timer, TimerRef} - Cancel a timer
%% - {register_call, CallbackId, Caller, Ref} - Register call handler
%% - {unregister_call, CallbackId} - Unregister call handler
%% - {call_result, CallbackId, Result} - Dispatch result to caller
%% - {call_error, CallbackId, Error} - Dispatch error to caller
%% - stop - Shutdown
-module(py_event_loop_proc).

-export([
    start_link/1,
    start_link/2,
    stop/1,
    poll/2,
    start_timer/3,
    cancel_timer/2,
    get_pid/1,
    %% Call result handling
    register_call/3,
    unregister_call/2
]).

-record(state, {
    loop_ref :: reference(),
    %% Pending events: [{CallbackId, Type}]
    pending = [] :: [{non_neg_integer(), read | write | timer}],
    %% Active timers: #{TimerRef => {ErlTimerRef, CallbackId}}
    timers = #{} :: #{non_neg_integer() => {reference(), non_neg_integer()}},
    %% FD resources for callback lookup: #{FdRes => {ReadCallbackId, WriteCallbackId}}
    fd_callbacks = #{} :: #{reference() => {non_neg_integer(), non_neg_integer()}},
    %% Waiting poller: {From, Ref, MonRef, TRef} | undefined
    waiter = undefined :: {pid(), reference(), reference(), reference() | undefined} | undefined,
    %% Timer ref counter
    timer_counter = 0 :: non_neg_integer(),
    %% Registered call handlers: #{CallbackId => {Caller, Ref}}
    %% Used to dispatch call_result/call_error to waiting callers
    call_handlers = #{} :: #{non_neg_integer() => {pid(), reference()}}
}).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start the event loop process.
-spec start_link(reference()) -> {ok, pid()}.
start_link(LoopRef) ->
    start_link(LoopRef, []).

%% @doc Start with options.
-spec start_link(reference(), list()) -> {ok, pid()}.
start_link(LoopRef, _Opts) ->
    Pid = spawn_link(fun() -> init(LoopRef) end),
    {ok, Pid}.

%% @doc Stop the event loop process.
-spec stop(pid()) -> ok.
stop(Pid) ->
    Pid ! stop,
    ok.

%% @doc Poll for events with timeout.
%% Returns immediately if events are pending, otherwise waits up to TimeoutMs.
-spec poll(pid(), non_neg_integer()) -> [{non_neg_integer(), read | write | timer}].
poll(Pid, TimeoutMs) ->
    Ref = monitor(process, Pid),
    Pid ! {poll, self(), Ref, TimeoutMs},
    receive
        {events, Ref, Events} ->
            demonitor(Ref, [flush]),
            Events;
        {'DOWN', Ref, process, Pid, Reason} ->
            error({event_loop_down, Reason})
    end.

%% @doc Start a timer. Returns TimerRef.
-spec start_timer(pid(), non_neg_integer(), non_neg_integer()) -> non_neg_integer().
start_timer(Pid, DelayMs, CallbackId) ->
    Ref = make_ref(),
    Pid ! {start_timer, self(), Ref, DelayMs, CallbackId},
    receive
        {timer_started, Ref, TimerRef} -> TimerRef
    after 5000 ->
        error(timeout)
    end.

%% @doc Cancel a timer.
-spec cancel_timer(pid(), non_neg_integer()) -> ok.
cancel_timer(Pid, TimerRef) ->
    Pid ! {cancel_timer, TimerRef},
    ok.

%% @doc Get the PID (for setting as enif_select target).
-spec get_pid(pid()) -> pid().
get_pid(Pid) -> Pid.

%% @doc Register a call handler to receive result/error for CallbackId.
%% When call_result or call_error arrives for this CallbackId,
%% the message {py_result, Ref, Result} or {py_error, Ref, Error}
%% will be sent to Caller.
-spec register_call(pid(), non_neg_integer(), reference()) -> ok.
register_call(Pid, CallbackId, Ref) ->
    Pid ! {register_call, CallbackId, self(), Ref},
    ok.

%% @doc Unregister a call handler. Safe to call even if result already delivered.
-spec unregister_call(pid(), non_neg_integer()) -> ok.
unregister_call(Pid, CallbackId) ->
    Pid ! {unregister_call, CallbackId},
    ok.

%% ============================================================================
%% Internal - Process Loop
%% ============================================================================

init(LoopRef) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    loop(#state{loop_ref = LoopRef}).

loop(State) ->
    receive
        Msg -> handle_msg(Msg, State)
    end.

handle_msg({select, FdRes, _Ref, ready_input}, State) ->
    handle_fd_event(FdRes, read, State);

handle_msg({select, FdRes, _Ref, ready_output}, State) ->
    handle_fd_event(FdRes, write, State);

handle_msg({select, _FdRes, _Ref, cancelled}, State) ->
    %% FD monitoring cancelled, ignore
    loop(State);

handle_msg({timeout, TimerRef}, State) ->
    handle_timer_fired(TimerRef, State);

handle_msg({poll, From, Ref, TimeoutMs}, State) ->
    handle_poll(From, Ref, TimeoutMs, State);

handle_msg({start_timer, From, Ref, DelayMs, CallbackId}, State) when is_pid(From) ->
    %% New format with reply
    handle_start_timer(From, Ref, DelayMs, CallbackId, State);

handle_msg({start_timer, DelayMs, CallbackId, TimerRef}, State) when is_integer(DelayMs) ->
    %% Legacy format from py_schedule_timer (4-tuple, no reply needed)
    handle_start_timer_legacy(DelayMs, CallbackId, TimerRef, State);

handle_msg({start_timer, _LoopRef, DelayMs, CallbackId, TimerRef}, State) ->
    %% Legacy format from py_schedule_timer_for (5-tuple with LoopRef, no reply needed)
    handle_start_timer_legacy(DelayMs, CallbackId, TimerRef, State);

handle_msg({cancel_timer, TimerRef}, State) ->
    handle_cancel_timer(TimerRef, State);

handle_msg({cancel_timer, _LoopRef, TimerRef}, State) ->
    %% Legacy format from py_cancel_timer_for (with LoopRef)
    handle_cancel_timer(TimerRef, State);

handle_msg({register_fd, FdRes, ReadCallbackId, WriteCallbackId}, State) ->
    FdCallbacks = maps:put(FdRes, {ReadCallbackId, WriteCallbackId}, State#state.fd_callbacks),
    loop(State#state{fd_callbacks = FdCallbacks});

handle_msg({unregister_fd, FdRes}, State) ->
    FdCallbacks = maps:remove(FdRes, State#state.fd_callbacks),
    loop(State#state{fd_callbacks = FdCallbacks});

handle_msg({register_call, CallbackId, Caller, Ref}, State) ->
    CallHandlers = maps:put(CallbackId, {Caller, Ref}, State#state.call_handlers),
    loop(State#state{call_handlers = CallHandlers});

handle_msg({unregister_call, CallbackId}, State) ->
    CallHandlers = maps:remove(CallbackId, State#state.call_handlers),
    loop(State#state{call_handlers = CallHandlers});

handle_msg({call_result, CallbackId, Result}, State) ->
    handle_call_result(CallbackId, Result, State);

handle_msg({call_error, CallbackId, Error}, State) ->
    handle_call_error(CallbackId, Error, State);

handle_msg({'DOWN', _MonRef, process, _Pid, _Reason}, State) ->
    %% Monitor down - in loop/1 context, waiter is always undefined
    %% (waiter monitors are handled in wait_loop/1 directly)
    loop(State);

handle_msg(stop, _State) ->
    ok;

handle_msg({'EXIT', _Pid, _Reason}, State) ->
    %% Linked process died, continue
    loop(State);

handle_msg(_Unknown, State) ->
    loop(State).

%% ============================================================================
%% Event Handlers
%% ============================================================================

handle_fd_event(FdRes, Type, State) ->
    %% Get callback ID from fd resource via NIF
    case py_nif:get_fd_callback_id(FdRes, Type) of
        undefined ->
            %% Watcher was removed, ignore
            loop(State);
        CallbackId ->
            %% Add to pending and reselect
            Event = {CallbackId, Type},
            NewPending = [Event | State#state.pending],

            %% Reselect for next event
            case Type of
                read -> py_nif:reselect_reader_fd(FdRes);
                write -> py_nif:reselect_writer_fd(FdRes)
            end,

            %% Wake waiter if any
            State2 = maybe_wake_waiter(State#state{pending = NewPending}),
            loop(State2)
    end.

handle_timer_fired(TimerRef, State) ->
    case maps:get(TimerRef, State#state.timers, undefined) of
        undefined ->
            %% Timer was cancelled
            loop(State);
        {_ErlTimerRef, CallbackId} ->
            %% Add timer event to pending
            Event = {CallbackId, timer},
            NewPending = [Event | State#state.pending],
            NewTimers = maps:remove(TimerRef, State#state.timers),

            %% Wake waiter if any
            State2 = maybe_wake_waiter(State#state{
                pending = NewPending,
                timers = NewTimers
            }),
            loop(State2)
    end.

handle_call_result(CallbackId, Result, State) ->
    case maps:get(CallbackId, State#state.call_handlers, undefined) of
        undefined ->
            %% Handler was unregistered or result already delivered, ignore
            loop(State);
        {Caller, Ref} ->
            Caller ! {py_result, Ref, Result},
            CallHandlers = maps:remove(CallbackId, State#state.call_handlers),
            loop(State#state{call_handlers = CallHandlers})
    end.

handle_call_error(CallbackId, Error, State) ->
    case maps:get(CallbackId, State#state.call_handlers, undefined) of
        undefined ->
            %% Handler was unregistered, ignore
            loop(State);
        {Caller, Ref} ->
            Caller ! {py_error, Ref, Error},
            CallHandlers = maps:remove(CallbackId, State#state.call_handlers),
            loop(State#state{call_handlers = CallHandlers})
    end.

handle_poll(From, Ref, TimeoutMs, State) ->
    case State#state.pending of
        [] when TimeoutMs =:= 0 ->
            %% No events, no wait
            From ! {events, Ref, []},
            loop(State);
        [] ->
            %% No events, wait for timeout or event
            MonRef = monitor(process, From),
            TRef = if
                TimeoutMs > 0 ->
                    erlang:send_after(TimeoutMs, self(), {poll_timeout, Ref});
                true ->
                    undefined
            end,
            wait_loop(State#state{waiter = {From, Ref, MonRef, TRef}});
        Events ->
            %% Return pending events immediately
            From ! {events, Ref, lists:reverse(Events)},
            loop(State#state{pending = []})
    end.

handle_start_timer(From, Ref, DelayMs, CallbackId, State) ->
    TimerRef = State#state.timer_counter + 1,
    ErlTimerRef = erlang:send_after(DelayMs, self(), {timeout, TimerRef}),
    NewTimers = maps:put(TimerRef, {ErlTimerRef, CallbackId}, State#state.timers),
    From ! {timer_started, Ref, TimerRef},
    loop(State#state{
        timers = NewTimers,
        timer_counter = TimerRef
    }).

handle_start_timer_legacy(DelayMs, CallbackId, TimerRef, State) ->
    %% Legacy format: TimerRef comes from caller, no reply needed
    ErlTimerRef = erlang:send_after(DelayMs, self(), {timeout, TimerRef}),
    NewTimers = maps:put(TimerRef, {ErlTimerRef, CallbackId}, State#state.timers),
    loop(State#state{timers = NewTimers}).

handle_cancel_timer(TimerRef, State) ->
    case maps:get(TimerRef, State#state.timers, undefined) of
        undefined ->
            loop(State);
        {ErlTimerRef, _CallbackId} ->
            erlang:cancel_timer(ErlTimerRef),
            NewTimers = maps:remove(TimerRef, State#state.timers),
            loop(State#state{timers = NewTimers})
    end.

%% ============================================================================
%% Wait Loop - Waiting for events or timeout
%% ============================================================================

wait_loop(State = #state{waiter = {From, Ref, MonRef, TRef}}) ->
    receive
        {select, FdRes, _SelectRef, ready_input} ->
            handle_fd_event_in_wait(FdRes, read, State);

        {select, FdRes, _SelectRef, ready_output} ->
            handle_fd_event_in_wait(FdRes, write, State);

        {select, _FdRes, _SelectRef, cancelled} ->
            wait_loop(State);

        {timeout, TimerRef} ->
            handle_timer_in_wait(TimerRef, State);

        {poll_timeout, Ref} ->
            %% Timeout reached, return what we have
            demonitor(MonRef, [flush]),
            From ! {events, Ref, lists:reverse(State#state.pending)},
            loop(State#state{pending = [], waiter = undefined});

        {'DOWN', MonRef, process, From, _Reason} ->
            %% Waiter died
            cancel_poll_timeout(TRef),
            loop(State#state{waiter = undefined});

        {start_timer, TimerFrom, TimerCallRef, DelayMs, CallbackId} ->
            %% Handle timer start even while waiting
            handle_start_timer_in_wait(TimerFrom, TimerCallRef, DelayMs, CallbackId, State);

        {cancel_timer, CancelTimerRef} ->
            %% Inline timer cancellation to stay in wait_loop (don't call handle_cancel_timer
            %% which tail-calls loop/1 and would escape wait mode)
            NewState = case maps:get(CancelTimerRef, State#state.timers, undefined) of
                undefined ->
                    State;
                {ErlTimerRef, _CallbackId} ->
                    erlang:cancel_timer(ErlTimerRef),
                    NewTimers = maps:remove(CancelTimerRef, State#state.timers),
                    State#state{timers = NewTimers}
            end,
            wait_loop(NewState);

        {register_call, CallbackId, Caller, CallRef} ->
            CallHandlers = maps:put(CallbackId, {Caller, CallRef}, State#state.call_handlers),
            wait_loop(State#state{call_handlers = CallHandlers});

        {unregister_call, CallbackId} ->
            CallHandlers = maps:remove(CallbackId, State#state.call_handlers),
            wait_loop(State#state{call_handlers = CallHandlers});

        {call_result, CallbackId, Result} ->
            handle_call_result_in_wait(CallbackId, Result, State);

        {call_error, CallbackId, Error} ->
            handle_call_error_in_wait(CallbackId, Error, State);

        stop ->
            cancel_poll_timeout(TRef),
            demonitor(MonRef, [flush]),
            From ! {events, Ref, []},
            ok;

        _Other ->
            wait_loop(State)
    end.

handle_fd_event_in_wait(FdRes, Type, State = #state{waiter = {From, Ref, MonRef, TRef}}) ->
    case py_nif:get_fd_callback_id(FdRes, Type) of
        undefined ->
            wait_loop(State);
        CallbackId ->
            Event = {CallbackId, Type},
            NewPending = [Event | State#state.pending],

            %% Reselect
            case Type of
                read -> py_nif:reselect_reader_fd(FdRes);
                write -> py_nif:reselect_writer_fd(FdRes)
            end,

            %% Wake waiter immediately
            cancel_poll_timeout(TRef),
            demonitor(MonRef, [flush]),
            From ! {events, Ref, lists:reverse(NewPending)},
            loop(State#state{pending = [], waiter = undefined})
    end.

handle_timer_in_wait(TimerRef, State = #state{waiter = {From, Ref, MonRef, TRef}}) ->
    case maps:get(TimerRef, State#state.timers, undefined) of
        undefined ->
            wait_loop(State);
        {_ErlTimerRef, CallbackId} ->
            Event = {CallbackId, timer},
            NewPending = [Event | State#state.pending],
            NewTimers = maps:remove(TimerRef, State#state.timers),

            %% Wake waiter
            cancel_poll_timeout(TRef),
            demonitor(MonRef, [flush]),
            From ! {events, Ref, lists:reverse(NewPending)},
            loop(State#state{pending = [], timers = NewTimers, waiter = undefined})
    end.

handle_start_timer_in_wait(From, CallRef, DelayMs, CallbackId, State) ->
    TimerRef = State#state.timer_counter + 1,
    ErlTimerRef = erlang:send_after(DelayMs, self(), {timeout, TimerRef}),
    NewTimers = maps:put(TimerRef, {ErlTimerRef, CallbackId}, State#state.timers),
    From ! {timer_started, CallRef, TimerRef},
    wait_loop(State#state{
        timers = NewTimers,
        timer_counter = TimerRef
    }).

handle_call_result_in_wait(CallbackId, Result, State) ->
    case maps:get(CallbackId, State#state.call_handlers, undefined) of
        undefined ->
            %% Handler was unregistered, ignore
            wait_loop(State);
        {Caller, Ref} ->
            Caller ! {py_result, Ref, Result},
            CallHandlers = maps:remove(CallbackId, State#state.call_handlers),
            wait_loop(State#state{call_handlers = CallHandlers})
    end.

handle_call_error_in_wait(CallbackId, Error, State) ->
    case maps:get(CallbackId, State#state.call_handlers, undefined) of
        undefined ->
            wait_loop(State);
        {Caller, Ref} ->
            Caller ! {py_error, Ref, Error},
            CallHandlers = maps:remove(CallbackId, State#state.call_handlers),
            wait_loop(State#state{call_handlers = CallHandlers})
    end.

%% ============================================================================
%% Helpers
%% ============================================================================

%% In loop/1 context, waiter is always undefined - events are dispatched
%% immediately when they occur. The wait_loop/1 handles waking the waiter inline.
%% This function is kept for clarity and potential future use.
-dialyzer({nowarn_function, maybe_wake_waiter/1}).
maybe_wake_waiter(State = #state{waiter = undefined}) ->
    State;
maybe_wake_waiter(State = #state{waiter = {From, Ref, MonRef, TRef}, pending = Pending}) ->
    cancel_poll_timeout(TRef),
    demonitor(MonRef, [flush]),
    From ! {events, Ref, lists:reverse(Pending)},
    State#state{pending = [], waiter = undefined}.

cancel_poll_timeout(undefined) -> ok;
cancel_poll_timeout(TRef) -> erlang:cancel_timer(TRef).
