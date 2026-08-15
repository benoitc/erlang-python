%%% @doc Common Test suite for worker loops.
%%%
%%% Covers py_context:start_loop/1,2, stop_loop/1,2, loop_ref/1, submit/4,5,
%%% submit_await/4,5,6, the `preload' option, the erlang.server helper, and
%%% the owngil fixes behind them (per-context event loop, async dispatch,
%%% coroutine injection, fd close after select stop).
%%%
%%% Most cases need owngil (one loop per interpreter, and worker contexts
%%% share the main interpreter); those skip on Python < 3.14.
-module(py_worker_loop_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    %% owngil
    test_loop_ref_per_context/1,
    test_start_stop_loop/1,
    test_start_twice/1,
    test_stop_idle/1,
    test_calls_rejected_while_running/1,
    test_submit_idle_and_running/1,
    test_submit_errors_reported/1,
    test_submit_ordering/1,
    test_tcp_serve_on_dup_fd/1,
    test_udp_serve_on_dup_fd/1,
    test_adopt_accepted_fd/1,
    test_three_workers_one_listen_fd/1,
    test_channel_awaited_in_loop/1,
    test_owner_death_stops_loop/1,
    test_stop_context_while_looping/1,
    test_interrupt_ends_loop/1,
    test_long_call_no_30s_cap/1,
    test_main_pool_unaffected/1,
    test_churn_no_poll_reports/1,
    test_preload_option/1,
    test_bad_fd_rejected/1,
    %% worker mode
    test_worker_mode_single_loop/1
]).

%% logger handler callback used by test_churn_no_poll_reports
-export([log/2]).

-define(HOST, {127, 0, 0, 1}).

all() ->
    [{group, owngil}, {group, worker}].

groups() ->
    [{owngil, [], [
        test_loop_ref_per_context,
        test_start_stop_loop,
        test_start_twice,
        test_stop_idle,
        test_calls_rejected_while_running,
        test_submit_idle_and_running,
        test_submit_errors_reported,
        test_submit_ordering,
        test_tcp_serve_on_dup_fd,
        test_udp_serve_on_dup_fd,
        test_adopt_accepted_fd,
        test_three_workers_one_listen_fd,
        test_channel_awaited_in_loop,
        test_owner_death_stops_loop,
        test_stop_context_while_looping,
        test_interrupt_ends_loop,
        test_long_call_no_30s_cap,
        test_main_pool_unaffected,
        test_churn_no_poll_reports,
        test_preload_option,
        test_bad_fd_rejected
     ]},
     {worker, [], [test_worker_mode_single_loop]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    TestDir = filename:dirname(code:which(?MODULE)),
    [{test_dir, TestDir} | Config].

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(owngil, Config) ->
    case py_nif:owngil_supported() of
        true -> [{mode, owngil} | Config];
        false -> {skip, "worker loops need OWN_GIL (Python 3.14+)"}
    end;
init_per_group(worker, Config) ->
    [{mode, worker} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    flush(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    flush(),
    ok.

%%% ============================================================================
%%% Loop lifecycle
%%% ============================================================================

%% @doc Every owngil context has its own loop, distinct from the main one.
test_loop_ref_per_context(Config) ->
    C1 = new_ctx(Config),
    C2 = new_ctx(Config),
    {ok, L1} = py_context:loop_ref(C1),
    {ok, L2} = py_context:loop_ref(C2),
    {ok, LMain} = py_event_loop:get_loop(),
    true = L1 =/= L2,
    true = L1 =/= LMain,
    true = L2 =/= LMain,
    stop_ctx(C1), stop_ctx(C2),
    ok.

%% @doc start_loop returns once running; stop_loop returns once exited and
%% the owner hears about it; the context is usable again after.
test_start_stop_loop(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, 3} = py_context:submit_await(C, py_test_workerloop, add, [1, 2]),
    ok = py_context:stop_loop(C),
    receive {py_loop_exit, C, {ok, <<"stopped">>}} -> ok
    after 2000 -> ct:fail(no_loop_exit)
    end,
    {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 5000),
    %% and again
    ok = py_context:start_loop(C),
    {ok, 7} = py_context:submit_await(C, py_test_workerloop, add, [3, 4]),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

test_start_twice(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {error, already_running} = py_context:start_loop(C),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

test_stop_idle(Config) ->
    C = new_ctx(Config),
    {error, no_loop} = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%% @doc call/eval/exec/call_method are refused while the loop runs (a timed
%% out call would interrupt the loop) and the loop keeps working.
test_calls_rejected_while_running(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {error, loop_running} = py_context:eval(C, <<"1">>),
    {error, loop_running} = py_context:exec(C, <<"x = 1">>),
    {error, loop_running} = py_context:call(C, math, sqrt, [4.0]),
    {ok, 3} = py_context:submit_await(C, py_test_workerloop, add, [1, 2]),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%%% ============================================================================
%%% submit
%%% ============================================================================

%% @doc submit works on an idle context (event worker steps the loop) and
%% into a running loop, for coroutines and plain functions.
test_submit_idle_and_running(Config) ->
    C = new_ctx(Config),
    {ok, 3} = py_context:submit_await(C, py_test_workerloop, add, [1, 2]),
    {ok, 5} = py_context:submit_await(C, py_test_workerloop, sync_add, [2, 3]),
    ok = py_context:start_loop(C),
    T0 = erlang:monotonic_time(millisecond),
    {ok, 9} = py_context:submit_await(C, py_test_workerloop, add, [4, 5]),
    Latency = erlang:monotonic_time(millisecond) - T0,
    %% injected coroutines wake the loop, no wait for the poll timeout
    true = Latency < 500,
    {ok, 6} = py_context:submit_await(C, py_test_workerloop, sync_add, [1, 5]),
    {ok, TaskRef} = py_context:submit(C, py_test_workerloop, sleep_then, [<<"late">>, 0.05]),
    {ok, <<"late">>} = py_event_loop:await(TaskRef, 2000),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%% @doc Failures to start or run a task are reported, not dropped.
test_submit_errors_reported(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {error, function_not_found} = py_context:submit_await(C, py_test_workerloop, nope, []),
    {error, function_not_found} = py_context:submit_await(C, no_such_module, f, []),
    {error, {'TypeError', _}} = py_context:submit_await(C, math, sqrt, [<<"x">>]),
    {error, _} = py_context:submit_await(C, py_test_workerloop, raise_error, []),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%% @doc A burst of submits from one caller completes and comes back in order.
%% More than MAX_TASK_BATCH (64) tasks are queued before the worker gets to
%% them, so this also covers the running-loop branch returning `more'.
test_submit_ordering(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, LoopRef} = py_context:loop_ref(C),
    Refs = [begin
        R = make_ref(),
        ok = py_nif:submit_task(LoopRef, self(), R, <<"py_test_workerloop">>, <<"add">>, [I, 0], #{}),
        {I, R}
    end || I <- lists:seq(1, 500)],
    %% Collect within one overall deadline so a strand shows up as a count,
    %% not as a timetrap
    Deadline = erlang:monotonic_time(millisecond) + 30000,
    Results = collect_results(Refs, Deadline, #{}),
    Bad = [{I, maps:get(R, Results, missing)} || {I, R} <- Refs,
                                                  maps:get(R, Results, missing) =/= {ok, I}],
    case Bad of
        [] -> ok;
        _ -> ct:log("~p of 500 tasks did not complete: ~p", [length(Bad), lists:sublist(Bad, 10)]),
             ct:log("loop alive (sync): ~p", [py_context:submit_await(C, py_test_workerloop, sync_add, [1, 1])]),
             ct:log("loop alive (coro): ~p", [py_context:submit_await(C, py_test_workerloop, add, [1, 1])]),
             ct:log("mailbox: ~p", [erlang:process_info(self(), message_queue_len)]),
             ct:fail({tasks_incomplete, length(Bad)})
    end,
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%%% ============================================================================
%%% Serving on fds handed over by Erlang
%%% ============================================================================

test_tcp_serve_on_dup_fd(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {LSock, Port, Dup} = listen_dup(),
    {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve, [Dup]),
    [<<"ok:x">> = roundtrip(Port, <<"x">>) || _ <- lists:seq(1, 100)],
    {ok, 100} = py_context:submit_await(C, py_test_workerloop, served_count, []),
    {ok, <<"stopped">>} = py_context:submit_await(C, py_test_workerloop, stop, [Dup]),
    ok = py_context:stop_loop(C),
    gen_tcp:close(LSock),
    stop_ctx(C),
    ok.

test_udp_serve_on_dup_fd(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, USock} = gen_udp:open(0, [binary, {ip, ?HOST}, {active, false}]),
    {ok, Port} = inet:port(USock),
    {ok, Fd} = inet:getfd(USock),
    {ok, Dup} = py:dup_fd(Fd),
    {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve_udp, [Dup]),
    {ok, Client} = gen_udp:open(0, [binary, {ip, ?HOST}, {active, false}]),
    ok = gen_udp:send(Client, ?HOST, Port, <<"ping">>),
    {ok, {_, _, <<"udp:ping">>}} = gen_udp:recv(Client, 0, 2000),
    {ok, <<"stopped">>} = py_context:submit_await(C, py_test_workerloop, stop, [Dup]),
    gen_udp:close(Client),
    gen_udp:close(USock),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%% @doc Erlang accepts, then hands the connection fd to the loop.
test_adopt_accepted_fd(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, ?HOST}, {active, false}]),
    {ok, Port} = inet:port(LSock),
    Self = self(),
    spawn_link(fun() ->
        {ok, S} = gen_tcp:connect(?HOST, Port, [binary, {active, false}], 2000),
        ok = gen_tcp:send(S, <<"adopted?">>),
        Self ! {client, gen_tcp:recv(S, 0, 3000)},
        gen_tcp:close(S)
    end),
    {ok, Conn} = gen_tcp:accept(LSock, 2000),
    {ok, ConnFd} = inet:getfd(Conn),
    {ok, Dup} = py:dup_fd(ConnFd),
    {ok, <<"adopted">>} = py_context:submit_await(C, py_test_workerloop, adopt, [Dup]),
    %% Erlang gives up its copy; Python owns the dup
    gen_tcp:close(Conn),
    receive {client, {ok, <<"ok:adopted?">>}} -> ok
    after 3000 -> ct:fail(no_reply_through_adopted_fd)
    end,
    gen_tcp:close(LSock),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%% @doc gunicorn shape: one listen socket, three workers accepting on dups.
test_three_workers_one_listen_fd(Config) ->
    Ctxs = [new_ctx(Config) || _ <- lists:seq(1, 3)],
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, ?HOST}, {active, false}, {backlog, 512}]),
    {ok, Port} = inet:port(LSock),
    {ok, LFd} = inet:getfd(LSock),
    lists:foreach(fun({I, C}) ->
        ok = py_context:start_loop(C),
        {ok, Dup} = py:dup_fd(LFd),
        Tag = list_to_binary("w" ++ integer_to_list(I) ++ ":"),
        {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve, [Dup, Tag])
    end, lists:zip(lists:seq(1, 3), Ctxs)),
    Replies = [roundtrip(Port, <<"x">>) || _ <- lists:seq(1, 300)],
    300 = length([R || R <- Replies, binary:part(R, byte_size(R) - 4, 4) =:= <<"ok:x">>]),
    Tags = lists:usort([binary:part(R, 0, 3) || R <- Replies]),
    ct:log("workers that served: ~p", [Tags]),
    %% All three workers accept on the same socket
    3 = length(Tags),
    [ok = py_context:stop_loop(C) || C <- Ctxs],
    [stop_ctx(C) || C <- Ctxs],
    gen_tcp:close(LSock),
    ok.

%% @doc A py_channel awaited inside the loop is the Erlang to loop control
%% plane: no polling, message delivered into the running loop.
test_channel_awaited_in_loop(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, Ch} = py_channel:new(),
    {ok, TaskRef} = py_context:submit(C, py_test_workerloop, wait_channel, [Ch]),
    timer:sleep(100),
    ok = py_channel:send(Ch, {adopt, 42}),
    {ok, Msg} = py_event_loop:await(TaskRef, 3000),
    ct:log("channel message seen by the loop: ~p", [Msg]),
    ok = py_channel:close(Ch),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%%% ============================================================================
%%% Failure and shutdown paths
%%% ============================================================================

test_owner_death_stops_loop(Config) ->
    C = new_ctx(Config),
    Owner = spawn(fun() -> receive never -> ok end end),
    ok = py_context:start_loop(C, #{owner => Owner}),
    {ok, 3} = py_context:submit_await(C, py_test_workerloop, add, [1, 2]),
    exit(Owner, kill),
    ok = wait_until(fun() -> py_context:eval(C, <<"1">>, #{}, 1000) =:= {ok, 1} end, 5000),
    stop_ctx(C),
    ok.

%% @doc Stopping the context while its loop runs interrupts the loop first,
%% so context_destroy does not wait 30 s for the thread.
test_stop_context_while_looping(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    T0 = erlang:monotonic_time(millisecond),
    ok = py_context:stop(C),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ct:log("stop while looping took ~p ms", [Elapsed]),
    true = Elapsed < 10000,
    receive {py_loop_exit, C, {error, interrupted}} -> ok
    after 1000 -> ct:fail(no_interrupted_exit)
    end,
    false = is_process_alive(C),
    ok.

test_interrupt_ends_loop(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    ok = py_context:interrupt(C),
    receive {py_loop_exit, C, {error, interrupted}} -> ok
    after 3000 -> ct:fail(no_interrupted_exit)
    end,
    {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 5000),
    stop_ctx(C),
    ok.

%% @doc owngil calls no longer go through the 30 s blocking dispatch, and do
%% not hold a dirty scheduler while they run.
test_long_call_no_30s_cap(Config) ->
    C = new_ctx(Config),
    Other = new_ctx(Config),
    Self = self(),
    spawn_link(fun() ->
        Self ! {long, py_context:eval(C, <<"__import__('time').sleep(32) or 7">>, #{}, infinity)}
    end),
    timer:sleep(200),
    T0 = erlang:monotonic_time(millisecond),
    {ok, 2} = py_context:eval(Other, <<"1+1">>, #{}, 5000),
    true = erlang:monotonic_time(millisecond) - T0 < 1000,
    receive {long, {ok, 7}} -> ok
    after 40000 -> ct:fail(long_call_did_not_return)
    end,
    stop_ctx(C), stop_ctx(Other),
    ok.

%% @doc Starting and stopping owngil contexts must not touch the main loop's
%% worker (they used to re-point it and leave it dead).
test_main_pool_unaffected(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    {ok, 4.0} = py_event_loop:run(math, sqrt, [16.0]),
    ok.

%% @doc Connection churn leaves no fd in the BEAM poll set behind: no
%% "Bad input fd in erts_poll()" or "stealing control" reports.
test_churn_no_poll_reports(Config) ->
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self()}),
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {LSock, Port, Dup} = listen_dup(),
    {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve, [Dup]),
    N = 2000,
    Self = self(),
    Workers = 20,
    [spawn_link(fun() ->
        Fails = length([bad || _ <- lists:seq(1, N div Workers),
                               roundtrip(Port, <<"x">>) =/= <<"ok:x">>]),
        Self ! {churn_done, Fails}
     end) || _ <- lists:seq(1, Workers)],
    Fails = lists:sum([receive {churn_done, F} -> F after 60000 -> N end
                       || _ <- lists:seq(1, Workers)]),
    0 = Fails,
    timer:sleep(300),
    ok = py_context:stop_loop(C),
    gen_tcp:close(LSock),
    stop_ctx(C),
    ok = logger:remove_handler(?MODULE),
    Reports = collect_reports(),
    ct:log("erts_poll reports: ~p", [Reports]),
    [] = Reports,
    ok.

test_preload_option(Config) ->
    Pre = <<"import sys, types\n"
            "_m = types.ModuleType('preloaded_mod')\n"
            "async def hello():\n"
            "    return 'hi'\n"
            "_m.hello = hello\n"
            "sys.modules['preloaded_mod'] = _m\n">>,
    {ok, C} = py_context:new(#{mode => ?config(mode, Config), preload => Pre}),
    ok = py_context:start_loop(C),
    {ok, <<"hi">>} = py_context:submit_await(C, preloaded_mod, hello, []),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

test_bad_fd_rejected(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {error, _} = py_context:submit_await(C, py_test_workerloop, serve, [-1]),
    {error, _} = py_context:submit_await(C, py_test_workerloop, serve, [99999]),
    {error, _} = py_context:submit_await(C, py_test_workerloop, adopt, [<<"x">>]),
    %% loop still fine
    {ok, 3} = py_context:submit_await(C, py_test_workerloop, add, [1, 2]),
    ok = py_context:stop_loop(C),
    stop_ctx(C),
    ok.

%%% ============================================================================
%%% Worker mode
%%% ============================================================================

%% @doc Worker contexts share the main interpreter, which allows one
%% ErlangEventLoop: the first start_loop works, a second context cannot.
test_worker_mode_single_loop(Config) ->
    W1 = new_ctx(Config),
    W2 = new_ctx(Config),
    ok = py_context:start_loop(W1),
    {ok, 4.0} = py_context:submit_await(W1, math, sqrt, [16.0]),
    {ok, 3} = py_context:submit_await(W1, py_test_workerloop, add, [1, 2]),
    {error, _} = py_context:start_loop(W2),
    ok = py_context:stop_loop(W1),
    stop_ctx(W1), stop_ctx(W2),
    ok.

%%% ============================================================================
%%% Logger handler (churn test)
%%% ============================================================================

log(#{msg := Msg}, #{config := Pid}) ->
    Text = case Msg of
        {string, S} -> unicode:characters_to_binary(S);
        {report, R} -> unicode:characters_to_binary(io_lib:format("~p", [R]));
        {Fmt, Args} -> unicode:characters_to_binary(io_lib:format(Fmt, Args))
    end,
    case binary:match(Text, [<<"erts_poll">>, <<"stealing control">>]) of
        nomatch -> ok;
        _ -> Pid ! {poll_report, Text}
    end,
    ok.

collect_reports() ->
    receive {poll_report, T} -> [T | collect_reports()]
    after 200 -> []
    end.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

new_ctx(Config) ->
    {ok, C} = py_context:new(#{mode => ?config(mode, Config)}),
    TestDir = ?config(test_dir, Config),
    Code = iolist_to_binary(io_lib:format(
        "import sys\nif '~s' not in sys.path:\n    sys.path.insert(0, '~s')\n"
        "import py_test_workerloop\n", [TestDir, TestDir])),
    ok = py_context:exec(C, Code),
    C.

stop_ctx(C) ->
    try py_context:stop(C) catch _:_ -> ok end,
    ok.

listen_dup() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, ?HOST}, {active, false}, {backlog, 512}]),
    {ok, Port} = inet:port(LSock),
    {ok, LFd} = inet:getfd(LSock),
    {ok, Dup} = py:dup_fd(LFd),
    {LSock, Port, Dup}.

roundtrip(Port, Data) ->
    {ok, S} = gen_tcp:connect(?HOST, Port, [binary, {active, false}], 5000),
    ok = gen_tcp:send(S, Data),
    R = case gen_tcp:recv(S, 0, 5000) of
        {ok, Bin} -> Bin;
        Other -> Other
    end,
    gen_tcp:close(S),
    R.

collect_results([], _Deadline, Acc) ->
    Acc;
collect_results(Refs, Deadline, Acc) ->
    Wait = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {async_result, R, Res} ->
            case lists:keytake(R, 2, Refs) of
                {value, _, Rest} -> collect_results(Rest, Deadline, Acc#{R => Res});
                false -> collect_results(Refs, Deadline, Acc)
            end
    after Wait ->
        Acc
    end.

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_until_loop(Fun, Deadline).

wait_until_loop(Fun, Deadline) ->
    case Fun() of
        true -> ok;
        false ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> {error, timeout};
                false -> timer:sleep(50), wait_until_loop(Fun, Deadline)
            end
    end.

flush() ->
    receive _ -> flush() after 0 -> ok end.
