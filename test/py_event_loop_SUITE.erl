%%% @doc Common Test suite for Erlang-native asyncio event loop.
%%%
%%% Tests the enif_select-based event loop implementation that provides
%%% sub-millisecond latency for asyncio operations.
-module(py_event_loop_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_event_loop_create_destroy/1,
    test_event_loop_set_router/1,
    test_add_remove_reader/1,
    test_add_remove_writer/1,
    test_call_later_basic/1,
    test_call_later_cancel/1,
    test_multiple_timers/1,
    test_fd_read_callback/1,
    test_poll_events_timeout/1,
    test_concurrent_fd_and_timer/1,
    test_event_loop_wakeup/1,
    test_get_pending_clears_queue/1,
    %% TCP tests
    test_tcp_listener_create/1,
    test_tcp_connect_accept/1,
    test_tcp_read_write_callback/1,
    test_tcp_echo/1,
    %% Lifecycle tests
    test_cancel_reader_keeps_fd_open/1,
    test_cancel_writer_keeps_fd_open/1,
    test_double_close_is_safe/1,
    test_cancel_then_reselect/1
]).

all() ->
    [
        test_event_loop_create_destroy,
        test_event_loop_set_router,
        test_add_remove_reader,
        test_add_remove_writer,
        test_call_later_basic,
        test_call_later_cancel,
        test_multiple_timers,
        test_fd_read_callback,
        test_poll_events_timeout,
        test_concurrent_fd_and_timer,
        test_event_loop_wakeup,
        test_get_pending_clears_queue,
        %% TCP tests
        test_tcp_listener_create,
        test_tcp_connect_accept,
        test_tcp_read_write_callback,
        test_tcp_echo,
        %% Lifecycle tests
        test_cancel_reader_keeps_fd_open,
        test_cancel_writer_keeps_fd_open,
        test_double_close_is_safe,
        test_cancel_then_reselect
    ].

init_per_suite(Config) ->
    case application:ensure_all_started(erlang_python) of
        {ok, _} ->
            {ok, _} = py:start_contexts(),
            %% Wait for event loop to be fully initialized
            %% This is important for free-threaded Python where initialization
            %% can race with test execution
            case wait_for_event_loop(5000) of
                ok ->
                    Config;
                {error, Reason} ->
                    ct:fail({event_loop_not_ready, Reason})
            end;
        {error, {App, Reason}} ->
            ct:fail({failed_to_start, App, Reason})
    end.

%% Wait for the event loop to be fully initialized
wait_for_event_loop(Timeout) when Timeout =< 0 ->
    {error, timeout};
wait_for_event_loop(Timeout) ->
    case py_event_loop:get_loop() of
        {ok, LoopRef} when is_reference(LoopRef) ->
            %% Verify the event loop is actually functional by checking
            %% we can create and immediately cancel a timer
            case py_nif:event_loop_new() of
                {ok, TestLoop} ->
                    py_nif:event_loop_destroy(TestLoop),
                    ok;
                _ ->
                    timer:sleep(100),
                    wait_for_event_loop(Timeout - 100)
            end;
        _ ->
            timer:sleep(100),
            wait_for_event_loop(Timeout - 100)
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Event Loop Lifecycle Tests
%% ============================================================================

test_event_loop_create_destroy(_Config) ->
    %% Create event loop
    {ok, LoopRef} = py_nif:event_loop_new(),
    true = is_reference(LoopRef),

    %% Destroy it
    ok = py_nif:event_loop_destroy(LoopRef),
    ok.

test_event_loop_set_router(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),

    %% Start a router process
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    true = is_pid(RouterPid),

    %% Set router
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Cleanup
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

%% ============================================================================
%% File Descriptor Monitoring Tests (using pipes - they work with enif_select)
%% ============================================================================

test_add_remove_reader(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create a pipe for testing (pipes work properly with enif_select)
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Add reader
    CallbackId = 1,
    {ok, FdRef} = py_nif:add_reader(LoopRef, ReadFd, CallbackId),

    %% Remove reader (pass FdRef, not Fd)
    ok = py_nif:remove_reader(LoopRef, FdRef),

    %% Cleanup
    py_nif:close_test_fd(ReadFd),
    py_nif:close_test_fd(WriteFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_add_remove_writer(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Writer monitors the write end
    {ok, FdRef} = py_nif:add_writer(LoopRef, WriteFd, 1),

    %% Remove writer (pass FdRef, not Fd)
    ok = py_nif:remove_writer(LoopRef, FdRef),

    py_nif:close_test_fd(ReadFd),
    py_nif:close_test_fd(WriteFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_fd_read_callback(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create pipe - read end for monitoring, write end for triggering
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Register reader callback on the read end
    CallbackId = 42,
    {ok, FdRef} = py_nif:add_reader(LoopRef, ReadFd, CallbackId),

    %% Write data to the write end to trigger read readiness on read end
    ok = py_nif:write_test_fd(WriteFd, <<"hello">>),

    %% Wait for callback dispatch (router receives {select, ...})
    timer:sleep(100),

    %% Get pending events
    Pending = py_nif:get_pending(LoopRef),

    %% Should have received the read callback
    HasReadCallback = lists:any(
        fun({CId, Type}) -> CId == CallbackId andalso Type == read end,
        Pending
    ),
    true = HasReadCallback,

    %% Cleanup - remove reader before closing fd
    ok = py_nif:remove_reader(LoopRef, FdRef),
    py_nif:close_test_fd(ReadFd),
    py_nif:close_test_fd(WriteFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

%% ============================================================================
%% Timer Tests
%% ============================================================================

test_call_later_basic(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    CallbackId = 100,
    DelayMs = 50,

    T1 = erlang:monotonic_time(millisecond),
    {ok, _TimerRef} = py_nif:call_later(LoopRef, DelayMs, CallbackId),

    %% Wait for timer to fire
    timer:sleep(DelayMs + 50),
    T2 = erlang:monotonic_time(millisecond),

    %% Get pending events
    Pending = py_nif:get_pending(LoopRef),

    %% Verify callback was triggered
    HasTimerCallback = lists:any(
        fun({CId, Type}) -> CId == CallbackId andalso Type == timer end,
        Pending
    ),
    true = HasTimerCallback,

    %% Verify timing accuracy (within tolerance)
    Elapsed = T2 - T1,
    true = Elapsed >= DelayMs,

    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_call_later_cancel(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    CallbackId = 101,
    {ok, TimerRef} = py_nif:call_later(LoopRef, 200, CallbackId),

    %% Cancel before it fires
    ok = py_nif:cancel_timer(LoopRef, TimerRef),

    %% Wait past the scheduled time
    timer:sleep(300),

    %% Verify callback was NOT triggered
    Pending = py_nif:get_pending(LoopRef),
    HasTimerCallback = lists:any(
        fun({CId, Type}) -> CId == CallbackId andalso Type == timer end,
        Pending
    ),
    false = HasTimerCallback,

    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_multiple_timers(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create timers with different delays
    %% Use larger delays for CI reliability (especially on free-threaded Python)
    {ok, _} = py_nif:call_later(LoopRef, 20, 1),
    {ok, _} = py_nif:call_later(LoopRef, 50, 2),
    {ok, _} = py_nif:call_later(LoopRef, 80, 3),

    %% Wait for all with margin
    timer:sleep(200),

    Pending = py_nif:get_pending(LoopRef),

    %% All three should have fired
    CallbackIds = [CId || {CId, timer} <- Pending],
    true = lists:member(1, CallbackIds),
    true = lists:member(2, CallbackIds),
    true = lists:member(3, CallbackIds),

    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

%% ============================================================================
%% Poll Events Tests
%% ============================================================================

test_poll_events_timeout(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Poll with short timeout, no events
    T1 = erlang:monotonic_time(millisecond),
    {ok, NumEvents} = py_nif:poll_events(LoopRef, 50),
    T2 = erlang:monotonic_time(millisecond),

    %% Should have 0 events and taken ~50ms
    0 = NumEvents,
    Elapsed = T2 - T1,
    true = Elapsed >= 40,  %% Allow some tolerance

    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_event_loop_wakeup(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Spawn a process to wait
    Self = self(),
    spawn(fun() ->
        T1 = erlang:monotonic_time(millisecond),
        {ok, _} = py_nif:poll_events(LoopRef, 5000),
        T2 = erlang:monotonic_time(millisecond),
        Self ! {poll_done, T2 - T1}
    end),

    %% Give it time to start waiting
    timer:sleep(50),

    %% Wake it up
    ok = py_nif:event_loop_wakeup(LoopRef),

    %% Should receive quickly (not 5 seconds)
    receive
        {poll_done, Elapsed} ->
            true = Elapsed < 1000  %% Should be much less than 5000ms
    after 2000 ->
        ct:fail(poll_not_woken_up)
    end,

    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_get_pending_clears_queue(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create a timer that fires quickly
    %% Use 20ms minimum for CI reliability
    {ok, _} = py_nif:call_later(LoopRef, 20, 999),
    timer:sleep(100),

    %% First get_pending should return the event
    Pending1 = py_nif:get_pending(LoopRef),
    true = length(Pending1) > 0,

    %% Second get_pending should return empty (queue was cleared)
    Pending2 = py_nif:get_pending(LoopRef),
    0 = length(Pending2),

    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_concurrent_fd_and_timer(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Set up pipe and timer
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    FdCallbackId = 1,
    TimerCallbackId = 2,

    %% Keep references to prevent GC
    {ok, FdRef} = py_nif:add_reader(LoopRef, ReadFd, FdCallbackId),
    {ok, TimerRef} = py_nif:call_later(LoopRef, 50, TimerCallbackId),

    %% Trigger fd ready by writing to pipe
    ok = py_nif:write_test_fd(WriteFd, <<"data">>),

    %% Wait for both - use longer timeout for reliability
    timer:sleep(200),

    Pending = py_nif:get_pending(LoopRef),

    %% Should have both callbacks
    HasFdCallback = lists:any(
        fun({CId, Type}) -> CId == FdCallbackId andalso Type == read end,
        Pending
    ),
    HasTimerCallback = lists:any(
        fun({CId, Type}) -> CId == TimerCallbackId andalso Type == timer end,
        Pending
    ),

    %% Keep refs alive until here
    _ = TimerRef,

    true = HasFdCallback,
    true = HasTimerCallback,

    %% Cleanup - remove reader before closing fd
    ok = py_nif:remove_reader(LoopRef, FdRef),
    py_nif:close_test_fd(ReadFd),
    py_nif:close_test_fd(WriteFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

%% ============================================================================
%% TCP Socket Tests
%% ============================================================================

test_tcp_listener_create(_Config) ->
    %% Create a TCP listener on an ephemeral port
    {ok, {ListenFd, Port}} = py_nif:create_test_tcp_listener(0),
    true = is_integer(ListenFd),
    true = ListenFd > 0,
    true = is_integer(Port),
    true = Port > 0,

    %% Cleanup
    py_nif:close_test_fd(ListenFd),
    ok.

test_tcp_connect_accept(_Config) ->
    %% Create listener
    {ok, {ListenFd, Port}} = py_nif:create_test_tcp_listener(0),

    %% Connect client
    {ok, ClientFd} = py_nif:connect_test_tcp(<<"127.0.0.1">>, Port),
    true = is_integer(ClientFd),

    %% Give connection time to complete (non-blocking connect)
    timer:sleep(50),

    %% Accept on server side
    {ok, ServerFd} = py_nif:accept_test_tcp(ListenFd),
    true = is_integer(ServerFd),

    %% Cleanup
    py_nif:close_test_fd(ServerFd),
    py_nif:close_test_fd(ClientFd),
    py_nif:close_test_fd(ListenFd),
    ok.

test_tcp_read_write_callback(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create listener and connect
    {ok, {ListenFd, Port}} = py_nif:create_test_tcp_listener(0),
    {ok, ClientFd} = py_nif:connect_test_tcp(<<"127.0.0.1">>, Port),
    timer:sleep(50),
    {ok, ServerFd} = py_nif:accept_test_tcp(ListenFd),

    %% Register server fd for read monitoring
    ReadCallbackId = 100,
    {ok, FdRef} = py_nif:add_reader(LoopRef, ServerFd, ReadCallbackId),

    %% Write data from client
    ok = py_nif:write_test_fd(ClientFd, <<"hello tcp">>),

    %% Wait for callback dispatch
    timer:sleep(100),

    %% Get pending events
    Pending = py_nif:get_pending(LoopRef),

    %% Should have received the read callback
    HasReadCallback = lists:any(
        fun({CId, Type}) -> CId == ReadCallbackId andalso Type == read end,
        Pending
    ),
    true = HasReadCallback,

    %% Cleanup - remove reader before closing fds
    ok = py_nif:remove_reader(LoopRef, FdRef),
    py_nif:close_test_fd(ServerFd),
    py_nif:close_test_fd(ClientFd),
    py_nif:close_test_fd(ListenFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_tcp_echo(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create listener and connect
    {ok, {ListenFd, Port}} = py_nif:create_test_tcp_listener(0),
    {ok, ClientFd} = py_nif:connect_test_tcp(<<"127.0.0.1">>, Port),
    timer:sleep(50),
    {ok, ServerFd} = py_nif:accept_test_tcp(ListenFd),

    %% Register both for read monitoring
    ClientReadId = 200,
    ServerReadId = 201,
    {ok, ServerFdRef} = py_nif:add_reader(LoopRef, ServerFd, ServerReadId),
    {ok, ClientFdRef} = py_nif:add_reader(LoopRef, ClientFd, ClientReadId),

    %% Client sends data
    TestData = <<"echo test data">>,
    ok = py_nif:write_test_fd(ClientFd, TestData),

    %% Wait for server to receive
    timer:sleep(100),

    %% Server reads and echoes back
    {ok, ReceivedData} = py_nif:read_test_fd(ServerFd, 1024),
    ok = py_nif:write_test_fd(ServerFd, ReceivedData),

    %% Wait for client to receive echo
    timer:sleep(100),

    %% Client reads echo
    {ok, EchoData} = py_nif:read_test_fd(ClientFd, 1024),

    %% Verify echo matches original
    TestData = EchoData,

    %% Verify we got the callbacks
    Pending = py_nif:get_pending(LoopRef),
    HasServerCallback = lists:any(
        fun({CId, Type}) -> CId == ServerReadId andalso Type == read end,
        Pending
    ),
    HasClientCallback = lists:any(
        fun({CId, Type}) -> CId == ClientReadId andalso Type == read end,
        Pending
    ),

    %% At least server callback should have fired
    true = HasServerCallback orelse HasClientCallback,

    %% Cleanup - remove readers before closing fds
    ok = py_nif:remove_reader(LoopRef, ServerFdRef),
    ok = py_nif:remove_reader(LoopRef, ClientFdRef),
    py_nif:close_test_fd(ServerFd),
    py_nif:close_test_fd(ClientFd),
    py_nif:close_test_fd(ListenFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

%% ============================================================================
%% FD Lifecycle Tests
%% ============================================================================

test_cancel_reader_keeps_fd_open(_Config) ->
    %% Test that stop_reader stops monitoring but keeps FD open
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create a pipe
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Add reader
    CallbackId = 100,
    {ok, FdRef} = py_nif:add_reader(LoopRef, ReadFd, CallbackId),

    %% Stop reader (should NOT close FD) - using new API
    ok = py_nif:stop_reader(FdRef),

    %% Write to trigger readiness (but no callback since stopped)
    ok = py_nif:write_test_fd(WriteFd, <<"test">>),
    timer:sleep(50),

    %% FD should still be readable since it wasn't closed
    {ok, Data} = py_nif:read_test_fd(ReadFd, 1024),
    <<"test">> = Data,

    %% Verify no callback was dispatched
    Pending = py_nif:get_pending(LoopRef),
    HasCallback = lists:any(
        fun({CId, _Type}) -> CId == CallbackId end,
        Pending
    ),
    false = HasCallback,

    %% Cleanup - use close_fd since we stopped (remove_reader won't work)
    ok = py_nif:close_fd(FdRef),
    py_nif:close_test_fd(WriteFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_cancel_writer_keeps_fd_open(_Config) ->
    %% Test that stop_writer stops monitoring but keeps FD open
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create a pipe
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Add writer
    CallbackId = 200,
    {ok, FdRef} = py_nif:add_writer(LoopRef, WriteFd, CallbackId),

    %% Stop writer (should NOT close FD) - using new API
    ok = py_nif:stop_writer(FdRef),

    %% FD should still be writable since it wasn't closed
    ok = py_nif:write_test_fd(WriteFd, <<"test">>),

    %% Verify we can read what was written
    {ok, Data} = py_nif:read_test_fd(ReadFd, 1024),
    <<"test">> = Data,

    %% Cleanup
    ok = py_nif:close_fd(FdRef),
    py_nif:close_test_fd(ReadFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_double_close_is_safe(_Config) ->
    %% Test that calling close_fd twice is idempotent (no crash)
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create a pipe
    {ok, {ReadFd, _WriteFd}} = py_nif:create_test_pipe(),

    %% Add reader
    {ok, FdRef} = py_nif:add_reader(LoopRef, ReadFd, 300),

    %% Close once
    ok = py_nif:close_fd(FdRef),

    %% Close again - should be safe (idempotent)
    ok = py_nif:close_fd(FdRef),

    %% Also test remove_reader after close is safe
    ok = py_nif:remove_reader(LoopRef, FdRef),

    %% Note: WriteFd is owned by us and should still work
    %% But we won't test that since the pipe's read end is closed

    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.

test_cancel_then_reselect(_Config) ->
    %% Test that we can stop and then start (resume) monitoring
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create a pipe
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Add reader
    CallbackId = 400,
    {ok, FdRef} = py_nif:add_reader(LoopRef, ReadFd, CallbackId),

    %% Stop reader - using new API
    ok = py_nif:stop_reader(FdRef),

    %% Re-start using start_reader - using new API
    ok = py_nif:start_reader(FdRef),

    %% Now write to trigger the callback
    ok = py_nif:write_test_fd(WriteFd, <<"hello">>),
    timer:sleep(100),

    %% Should have received the callback since we re-started
    Pending = py_nif:get_pending(LoopRef),
    HasCallback = lists:any(
        fun({CId, Type}) -> CId == CallbackId andalso Type == read end,
        Pending
    ),
    true = HasCallback,

    %% Cleanup
    ok = py_nif:remove_reader(LoopRef, FdRef),
    py_nif:close_test_fd(ReadFd),
    py_nif:close_test_fd(WriteFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.
