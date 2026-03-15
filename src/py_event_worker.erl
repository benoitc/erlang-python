%% @doc Event worker for Erlang-native asyncio event loop.
%%
%% This gen_server implements the scalable I/O model with one worker
%% per Python context. Each worker:
%% - Receives `{select, FdRes, Ref, ready_input|ready_output}' directly from enif_select
%% - Handles `{timeout, TimerRef}' messages for timer dispatch
%% - Manages timers via erlang:send_after to self()
-module(py_event_worker).
-behaviour(gen_server).

-export([start_link/2, start_link/3, stop/1, get_loop_ref/1, get_worker_id/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    worker_id :: binary(),
    loop_ref :: reference(),
    timers = #{} :: #{reference() => {reference(), non_neg_integer()}},
    stats = #{select_count => 0, timer_count => 0, dispatch_count => 0} :: map()
}).

start_link(WorkerId, LoopRef) -> start_link(WorkerId, LoopRef, []).
start_link(WorkerId, LoopRef, Opts) ->
    case proplists:get_value(name, Opts) of
        undefined -> gen_server:start_link(?MODULE, [WorkerId, LoopRef], []);
        Name -> gen_server:start_link({local, Name}, ?MODULE, [WorkerId, LoopRef], [])
    end.

stop(Pid) -> gen_server:stop(Pid).
get_loop_ref(Pid) -> gen_server:call(Pid, get_loop_ref).
get_worker_id(Pid) -> gen_server:call(Pid, get_worker_id).

init([WorkerId, LoopRef]) ->
    process_flag(message_queue_data, off_heap),
    process_flag(trap_exit, true),
    {ok, #state{worker_id = WorkerId, loop_ref = LoopRef}}.

handle_call(get_loop_ref, _From, #state{loop_ref = LoopRef} = State) ->
    {reply, {ok, LoopRef}, State};
handle_call(get_worker_id, _From, #state{worker_id = WorkerId} = State) ->
    {reply, {ok, WorkerId}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({select, FdRes, _Ref, ready_input}, State) ->
    py_nif:handle_fd_event_and_reselect(FdRes, read),
    %% Trigger event processing after FD event dispatch
    self() ! task_ready,
    {noreply, State};

handle_info({select, FdRes, _Ref, ready_output}, State) ->
    py_nif:handle_fd_event_and_reselect(FdRes, write),
    %% Trigger event processing after FD event dispatch
    self() ! task_ready,
    {noreply, State};

handle_info({start_timer, _LoopRef, DelayMs, CallbackId, TimerRef}, State) ->
    #state{timers = Timers} = State,
    ErlTimerRef = erlang:send_after(DelayMs, self(), {timeout, TimerRef}),
    NewTimers = maps:put(TimerRef, {ErlTimerRef, CallbackId}, Timers),
    {noreply, State#state{timers = NewTimers}};

handle_info({start_timer, DelayMs, CallbackId, TimerRef}, State) ->
    #state{timers = Timers} = State,
    ErlTimerRef = erlang:send_after(DelayMs, self(), {timeout, TimerRef}),
    NewTimers = maps:put(TimerRef, {ErlTimerRef, CallbackId}, Timers),
    {noreply, State#state{timers = NewTimers}};

handle_info({cancel_timer, TimerRef}, State) ->
    #state{timers = Timers} = State,
    case maps:get(TimerRef, Timers, undefined) of
        undefined -> {noreply, State};
        {ErlTimerRef, _CallbackId} ->
            erlang:cancel_timer(ErlTimerRef),
            NewTimers = maps:remove(TimerRef, Timers),
            {noreply, State#state{timers = NewTimers}}
    end;

handle_info({timeout, TimerRef}, State) ->
    #state{loop_ref = LoopRef, timers = Timers} = State,
    case maps:get(TimerRef, Timers, undefined) of
        undefined -> {noreply, State};
        {_ErlTimerRef, CallbackId} ->
            py_nif:dispatch_timer(LoopRef, CallbackId),
            NewTimers = maps:remove(TimerRef, Timers),
            %% Trigger event processing after timer dispatch
            %% This ensures _run_once is called to handle the timer callback
            self() ! task_ready,
            {noreply, State#state{timers = NewTimers}}
    end;

handle_info({select, _FdRes, _Ref, cancelled}, State) -> {noreply, State};

%% Handle task_ready wakeup from submit_task NIF.
%% This is sent via enif_send when a new async task is submitted.
%% Uses a drain-until-empty loop to handle tasks submitted during processing.
handle_info(task_ready, #state{loop_ref = LoopRef} = State) ->
    drain_tasks_loop(LoopRef),
    {noreply, State};

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, #state{timers = Timers}) ->
    maps:foreach(fun(_TimerRef, {ErlTimerRef, _CallbackId}) ->
        erlang:cancel_timer(ErlTimerRef)
    end, Timers),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @doc Drain tasks until no more task_ready messages are pending.
%% This handles tasks that were submitted during processing.
%%
%% The NIF returns:
%% - ok: all tasks processed, check mailbox for new task_ready messages
%% - more: hit MAX_TASK_BATCH limit, more tasks pending
%% - {error, Reason}: processing failed
drain_tasks_loop(LoopRef) ->
    case py_nif:process_ready_tasks(LoopRef) of
        ok ->
            %% Check if more task_ready messages arrived during processing
            receive
                task_ready -> drain_tasks_loop(LoopRef)
            after 0 ->
                ok
            end;
        more ->
            %% Hit batch limit, more tasks pending.
            %% Send task_ready to self and return, allowing the gen_server
            %% to process other messages (select, timers) before continuing.
            %% This prevents starvation under sustained task traffic.
            self() ! task_ready,
            ok;
        {error, py_loop_not_set} ->
            ok;
        {error, Reason} ->
            error_logger:warning_msg("py_event_worker: task processing failed: ~p~n", [Reason]),
            ok
    end.
