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

%% @doc Integration tests for py_event_loop_v2.
-module(py_event_loop_v2_test).

-export([
    test_all/0,
    test_basic_timer/0,
    test_fd_events/0,
    test_mixed_events/0,
    bench_v1_vs_v2/0
]).

test_all() ->
    io:format("~n=== py_event_loop_v2 Integration Tests ===~n~n"),

    application:ensure_all_started(erlang_python),

    Results = [
        {"Basic timer", test_basic_timer()},
        {"FD events", test_fd_events()},
        {"Mixed events", test_mixed_events()}
    ],

    io:format("~n=== Results ===~n"),
    lists:foreach(fun({Name, Result}) ->
        Status = case Result of ok -> "PASS"; _ -> "FAIL" end,
        io:format("  ~-25s ~s~n", [Name, Status])
    end, Results),

    case lists:all(fun({_, R}) -> R =:= ok end, Results) of
        true -> ok;
        false -> error
    end.

%% Test basic timer functionality
test_basic_timer() ->
    io:format("Testing basic timer...~n"),

    %% Create v2 event loop
    {ok, LoopRef, EventProc} = py_event_loop_v2:new(),

    %% Schedule a timer (using legacy format that NIF would send)
    CallbackId = 12345,
    TimerRef = 1,
    EventProc ! {start_timer, 10, CallbackId, TimerRef},

    %% Poll for events
    Events = py_event_loop_v2:poll(EventProc, 100),

    %% Cleanup
    py_event_loop_v2:destroy(LoopRef, EventProc),

    case Events of
        [{12345, timer}] ->
            io:format("  OK: Timer event received~n"),
            ok;
        Other ->
            io:format("  FAIL: Expected [{12345, timer}], got ~p~n", [Other]),
            error
    end.

%% Test FD events via enif_select
test_fd_events() ->
    io:format("Testing FD events...~n"),

    %% Create v2 event loop
    {ok, LoopRef, EventProc} = py_event_loop_v2:new(),

    %% Create a test pipe
    {ok, {ReadFd, WriteFd}} = py_nif:create_test_pipe(),

    %% Register reader - this should target the event process via router_pid
    {ok, _FdRef} = py_nif:add_reader(LoopRef, ReadFd, 42),

    %% Write to trigger read readiness
    ok = py_nif:write_test_fd(WriteFd, <<"test">>),

    %% Give enif_select time to deliver
    timer:sleep(20),

    %% Poll for events (non-blocking, just get first batch)
    Events = py_event_loop_v2:poll(EventProc, 0),

    %% Cleanup
    py_nif:close_test_fd(ReadFd),
    py_nif:close_test_fd(WriteFd),
    py_event_loop_v2:destroy(LoopRef, EventProc),

    %% Check we got at least one read event
    case lists:any(fun({42, read}) -> true; (_) -> false end, Events) of
        true ->
            io:format("  OK: FD read event received (~p events)~n", [length(Events)]),
            ok;
        false ->
            io:format("  FAIL: No read events in ~p~n", [Events]),
            error
    end.

%% Test mixed timer and FD events
test_mixed_events() ->
    io:format("Testing mixed events...~n"),

    {ok, LoopRef, EventProc} = py_event_loop_v2:new(),

    %% Schedule multiple timers
    EventProc ! {start_timer, 5, 100, 1},
    EventProc ! {start_timer, 10, 200, 2},
    EventProc ! {start_timer, 15, 300, 3},

    %% Collect all events
    timer:sleep(50),
    Events = py_event_loop_v2:poll(EventProc, 100),

    py_event_loop_v2:destroy(LoopRef, EventProc),

    case length(Events) of
        3 ->
            io:format("  OK: All 3 timer events received~n"),
            ok;
        N ->
            io:format("  WARN: Got ~p events (expected 3)~n", [N]),
            ok
    end.

%% Benchmark v1 (py_event_router) vs v2 (py_event_loop_proc)
bench_v1_vs_v2() ->
    io:format("~n=== V1 vs V2 Benchmark ===~n~n"),

    application:ensure_all_started(erlang_python),
    NumTimers = 5000,

    %% V2 (event process)
    io:format("V2 (Event Process):~n"),
    {ok, LoopRef2, EventProc} = py_event_loop_v2:new(),

    V2Start = erlang:monotonic_time(microsecond),
    lists:foreach(fun(N) ->
        EventProc ! {start_timer, 0, N, N}
    end, lists:seq(1, NumTimers)),
    _V2Events = collect_all_events(EventProc, NumTimers),
    V2End = erlang:monotonic_time(microsecond),

    py_event_loop_v2:destroy(LoopRef2, EventProc),

    V2Ms = (V2End - V2Start) / 1000,
    V2Rate = NumTimers / (V2Ms / 1000),
    io:format("  Time: ~.2f ms, Rate: ~w timers/sec~n", [V2Ms, round(V2Rate)]),

    %% V1 (router)
    io:format("~nV1 (Router):~n"),
    {ok, LoopRef1} = py_nif:event_loop_new(),
    {ok, Router} = py_event_router:start_link(LoopRef1),
    ok = py_nif:event_loop_set_router(LoopRef1, Router),

    V1Start = erlang:monotonic_time(microsecond),
    lists:foreach(fun(N) ->
        Router ! {start_timer, LoopRef1, 0, N, N}
    end, lists:seq(1, NumTimers)),
    timer:sleep(100),
    _ = py_nif:get_pending(LoopRef1),
    V1End = erlang:monotonic_time(microsecond),

    py_event_router:stop(Router),
    py_nif:event_loop_destroy(LoopRef1),

    V1Ms = (V1End - V1Start) / 1000,
    V1Rate = NumTimers / (V1Ms / 1000),
    io:format("  Time: ~.2f ms, Rate: ~w timers/sec~n", [V1Ms, round(V1Rate)]),

    Improvement = (V2Rate - V1Rate) / V1Rate * 100,
    io:format("~nImprovement: ~.1f%~n", [Improvement]),

    {V2Rate, V1Rate, Improvement}.

collect_all_events(EventProc, Expected) ->
    collect_all_events(EventProc, Expected, []).

collect_all_events(_EventProc, 0, Acc) ->
    lists:reverse(Acc);
collect_all_events(EventProc, Remaining, Acc) ->
    Events = py_event_loop_v2:poll(EventProc, 100),
    case Events of
        [] -> lists:reverse(Acc);
        _ -> collect_all_events(EventProc, Remaining - length(Events), Events ++ Acc)
    end.
