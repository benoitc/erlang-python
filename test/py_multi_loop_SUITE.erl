%%% @doc Common Test suite for multi-loop isolation.
%%%
%%% Tests that multiple erlang_event_loop_t instances are fully isolated:
%%% - Each loop has its own pending queue
%%% - Events don't cross-dispatch between loops
%%% - Destroying one loop doesn't affect others
%%%
%%% These tests initially fail with global g_python_event_loop coupling
%%% and should pass after per-loop isolation is implemented.
-module(py_multi_loop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_two_loops_concurrent_timers/1,
    test_two_loops_cross_isolation/1,
    test_loop_cleanup_no_leak/1
]).

all() ->
    [
        test_two_loops_concurrent_timers,
        test_two_loops_cross_isolation,
        test_loop_cleanup_no_leak
    ].

init_per_suite(Config) ->
    case application:ensure_all_started(erlang_python) of
        {ok, _} ->
            {ok, _} = py:start_contexts(),
            case wait_for_event_loop(5000) of
                ok ->
                    Config;
                {error, Reason} ->
                    ct:fail({event_loop_not_ready, Reason})
            end;
        {error, {App, Reason}} ->
            ct:fail({failed_to_start, App, Reason})
    end.

wait_for_event_loop(Timeout) when Timeout =< 0 ->
    {error, timeout};
wait_for_event_loop(Timeout) ->
    case py_event_loop:get_loop() of
        {ok, LoopRef} when is_reference(LoopRef) ->
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
%% Test: Two loops with concurrent timers
%% ============================================================================
%%
%% Creates two independent event loops (LoopA and LoopB).
%% Each schedules 100 timers with unique callback IDs.
%% Verifies that:
%% - Each loop receives exactly its own 100 timer events
%% - No timers are lost or duplicated
%% - No cross-dispatch between loops
%%
%% Expected behavior with per-loop isolation:
%%   LoopA receives callback IDs 1-100
%%   LoopB receives callback IDs 1001-1100
%%
%% Current behavior with global coupling:
%%   Both loops share pending queue, events may be lost or misrouted

test_two_loops_concurrent_timers(_Config) ->
    %% Create two independent event loops
    {ok, LoopA} = py_nif:event_loop_new(),
    {ok, LoopB} = py_nif:event_loop_new(),

    %% Start routers for each loop
    {ok, RouterA} = py_event_router:start_link(LoopA),
    {ok, RouterB} = py_event_router:start_link(LoopB),

    ok = py_nif:event_loop_set_router(LoopA, RouterA),
    ok = py_nif:event_loop_set_router(LoopB, RouterB),

    %% Schedule 100 timers on each loop with different callback ID ranges
    NumTimers = 100,
    LoopABase = 1,      %% Callback IDs 1-100
    LoopBBase = 1001,   %% Callback IDs 1001-1100

    %% Schedule timers on LoopA (small delay for quick test)
    _TimerRefsA = [begin
        CallbackId = LoopABase + I - 1,
        {ok, TimerRef} = py_nif:call_later(LoopA, 10, CallbackId),
        TimerRef
    end || I <- lists:seq(1, NumTimers)],

    %% Schedule timers on LoopB
    _TimerRefsB = [begin
        CallbackId = LoopBBase + I - 1,
        {ok, TimerRef} = py_nif:call_later(LoopB, 10, CallbackId),
        TimerRef
    end || I <- lists:seq(1, NumTimers)],

    %% Wait for all timers to fire
    timer:sleep(200),

    %% Collect pending events from each loop
    EventsA = py_nif:get_pending(LoopA),
    EventsB = py_nif:get_pending(LoopB),

    %% Extract callback IDs
    CallbackIdsA = [CallbackId || {CallbackId, timer} <- EventsA],
    CallbackIdsB = [CallbackId || {CallbackId, timer} <- EventsB],

    ct:pal("LoopA received ~p timer events: ~p", [length(CallbackIdsA), lists:sort(CallbackIdsA)]),
    ct:pal("LoopB received ~p timer events: ~p", [length(CallbackIdsB), lists:sort(CallbackIdsB)]),

    %% Verify: LoopA should have exactly IDs 1-100
    ExpectedA = lists:seq(LoopABase, LoopABase + NumTimers - 1),
    %% LoopA should receive 100 timers
    ?assertEqual(NumTimers, length(CallbackIdsA)),
    %% LoopA callback IDs should match expected
    ?assertEqual(ExpectedA, lists:sort(CallbackIdsA)),

    %% Verify: LoopB should have exactly IDs 1001-1100
    ExpectedB = lists:seq(LoopBBase, LoopBBase + NumTimers - 1),
    %% LoopB should receive 100 timers
    ?assertEqual(NumTimers, length(CallbackIdsB)),
    %% LoopB callback IDs should match expected
    ?assertEqual(ExpectedB, lists:sort(CallbackIdsB)),

    %% Verify: No overlap between loops (no cross-dispatch)
    Intersection = lists:filter(fun(Id) -> lists:member(Id, CallbackIdsB) end, CallbackIdsA),
    ?assertEqual([], Intersection),

    %% Cleanup
    py_event_router:stop(RouterA),
    py_event_router:stop(RouterB),
    py_nif:event_loop_destroy(LoopA),
    py_nif:event_loop_destroy(LoopB),
    ok.

%% ============================================================================
%% Test: Cross-isolation verification
%% ============================================================================
%%
%% Verifies that events dispatched to LoopA are never seen by LoopB.
%% Uses fd callbacks which are more direct than timers.
%%
%% Expected behavior with per-loop isolation:
%%   LoopA pending queue receives LoopA events only
%%   LoopB pending queue receives nothing
%%
%% Current behavior with global coupling:
%%   Both loops share the same global pending queue

test_two_loops_cross_isolation(_Config) ->
    {ok, LoopA} = py_nif:event_loop_new(),
    {ok, LoopB} = py_nif:event_loop_new(),

    {ok, RouterA} = py_event_router:start_link(LoopA),
    {ok, RouterB} = py_event_router:start_link(LoopB),

    ok = py_nif:event_loop_set_router(LoopA, RouterA),
    ok = py_nif:event_loop_set_router(LoopB, RouterB),

    %% Create a pipe for LoopA only
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Register reader on LoopA with callback ID 42
    CallbackIdA = 42,
    {ok, FdRefA} = py_nif:add_reader(LoopA, ReadFd, CallbackIdA),

    %% Write to trigger read event
    ok = py_nif:write_test_fd(WriteFd, <<"test data">>),

    %% Wait for event to be dispatched
    timer:sleep(100),

    %% Get pending from both loops
    EventsA = py_nif:get_pending(LoopA),
    EventsB = py_nif:get_pending(LoopB),

    ct:pal("LoopA events: ~p", [EventsA]),
    ct:pal("LoopB events: ~p", [EventsB]),

    %% LoopA should have the read event
    ReadEventsA = [E || {_Cid, read} = E <- EventsA],
    %% LoopA should have 1 read event
    ?assertEqual(1, length(ReadEventsA)),

    %% LoopB should have NO events - this is the isolation test
    ?assertEqual([], EventsB),

    %% Cleanup
    py_nif:remove_reader(LoopA, FdRefA),
    py_nif:close_test_fd(ReadFd),
    py_nif:close_test_fd(WriteFd),
    py_event_router:stop(RouterA),
    py_event_router:stop(RouterB),
    py_nif:event_loop_destroy(LoopA),
    py_nif:event_loop_destroy(LoopB),
    ok.

%% ============================================================================
%% Test: Loop cleanup without leaks
%% ============================================================================
%%
%% Destroys LoopA while LoopB continues operating.
%% Verifies that:
%% - LoopB continues to receive its events
%% - No memory corruption or resource leaks
%% - Events scheduled on destroyed loop don't crash system
%%
%% Expected behavior with per-loop isolation:
%%   LoopB operates independently after LoopA destruction
%%
%% Current behavior with global coupling:
%%   Destroying LoopA may clear g_python_event_loop affecting LoopB

test_loop_cleanup_no_leak(_Config) ->
    {ok, LoopA} = py_nif:event_loop_new(),
    {ok, LoopB} = py_nif:event_loop_new(),

    {ok, RouterA} = py_event_router:start_link(LoopA),
    {ok, RouterB} = py_event_router:start_link(LoopB),

    ok = py_nif:event_loop_set_router(LoopA, RouterA),
    ok = py_nif:event_loop_set_router(LoopB, RouterB),

    %% Schedule a timer on LoopB that will fire after LoopA is destroyed
    CallbackIdB = 999,
    {ok, _TimerRefB} = py_nif:call_later(LoopB, 100, CallbackIdB),

    %% Schedule a timer on LoopA (won't be received since we destroy it)
    {ok, _TimerRefA} = py_nif:call_later(LoopA, 50, 111),

    %% Destroy LoopA before its timer fires
    timer:sleep(20),
    py_event_router:stop(RouterA),
    py_nif:event_loop_destroy(LoopA),

    %% Wait for LoopB timer to fire
    timer:sleep(150),

    %% LoopB should still receive its event
    EventsB = py_nif:get_pending(LoopB),
    ct:pal("LoopB events after LoopA destruction: ~p", [EventsB]),

    %% Verify LoopB still works - should receive its timer after LoopA destroyed
    TimerEventsB = [CallbackId || {CallbackId, timer} <- EventsB],
    ?assertEqual([CallbackIdB], TimerEventsB),

    %% Schedule another timer on LoopB to verify it still works
    CallbackIdB2 = 1000,
    {ok, _} = py_nif:call_later(LoopB, 10, CallbackIdB2),
    timer:sleep(50),

    EventsB2 = py_nif:get_pending(LoopB),
    TimerEventsB2 = [CallbackId || {CallbackId, timer} <- EventsB2],
    %% LoopB should still work after LoopA destroyed
    ?assertEqual([CallbackIdB2], TimerEventsB2),

    %% Cleanup
    py_event_router:stop(RouterB),
    py_nif:event_loop_destroy(LoopB),
    ok.

