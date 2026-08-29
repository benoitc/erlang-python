%%% @doc Common Test suite: asyncio in an isolated context.
%%%
%%% The child runs a plain asyncio loop. This suite mirrors
%%% py_worker_loop_SUITE (start_loop/submit/stop_loop, serving on fds Erlang
%%% owns) and the coroutine cases of py_async_task_SUITE. The headline case,
%%% a loop wedged in a blocking C call that stop_loop/2 kills, has a worker
%%% group counterpart documenting that the embedded loop cannot be stopped
%%% that way.
-module(py_isolated_async_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    end_per_testcase/2
]).

-export([
    test_start_stop_loop/1,
    test_start_twice/1,
    test_stop_idle/1,
    test_submit_idle_and_running/1,
    test_submit_ordering/1,
    test_submit_errors_reported/1,
    test_calls_rejected_while_running/1,
    test_interrupt_ends_loop/1,
    test_owner_death_stops_loop/1,
    test_stop_context_while_looping/1,
    test_long_submitted_call/1,
    test_preload_before_loop/1,
    test_tcp_serve_on_passed_fd/1,
    test_udp_serve_on_passed_fd/1,
    test_adopt_accepted_fd/1,
    test_three_workers_one_listen_fd/1,
    test_pass_fd_invalid/1,
    test_call_awaits_coroutine/1,
    test_gather_is_concurrent/1,
    test_async_error/1,
    test_concurrent_submitted_tasks/1,
    test_large_async_result/1,
    test_async_call_in_coroutine/1,
    test_async_calls_concurrent/1,
    test_async_call_error/1,
    test_send_from_coroutine/1,
    test_run_helper_compat/1,
    test_stream_via_send/1,
    test_blocked_loop_is_killed/1,
    test_blocked_loop_survives_in_worker/1
]).

-define(TEST_MOD, py_test_isolated).
-define(HOST, {127, 0, 0, 1}).

all() ->
    [{group, isolated}, {group, worker_contrast}].

groups() ->
    Cases = [
        test_start_stop_loop,
        test_start_twice,
        test_stop_idle,
        test_submit_idle_and_running,
        test_submit_ordering,
        test_submit_errors_reported,
        test_calls_rejected_while_running,
        test_interrupt_ends_loop,
        test_owner_death_stops_loop,
        test_stop_context_while_looping,
        test_long_submitted_call,
        test_preload_before_loop,
        test_tcp_serve_on_passed_fd,
        test_udp_serve_on_passed_fd,
        test_adopt_accepted_fd,
        test_three_workers_one_listen_fd,
        test_pass_fd_invalid,
        test_call_awaits_coroutine,
        test_gather_is_concurrent,
        test_async_error,
        test_concurrent_submitted_tasks,
        test_large_async_result,
        test_async_call_in_coroutine,
        test_async_calls_concurrent,
        test_async_call_error,
        test_send_from_coroutine,
        test_run_helper_compat,
        test_stream_via_send,
        test_blocked_loop_is_killed
    ],
    [{isolated, [], Cases},
     {worker_contrast, [], [test_blocked_loop_survives_in_worker]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config].

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(worker_contrast, Config) ->
    [{mode, worker} | Config];
init_per_group(Mode, Config) ->
    [{mode, Mode} | Config].

end_per_group(_Group, _Config) ->
    ok.

end_per_testcase(_TestCase, _Config) ->
    flush(),
    ok.

%%% ============================================================================
%%% Worker loop lifecycle
%%% ============================================================================

test_start_stop_loop(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, 3} = py_context:submit_await(C, ?TEST_MOD, async_add, [1, 2]),
    ok = py_context:stop_loop(C),
    receive {py_loop_exit, C, ok} -> ok after 2000 -> ct:fail(no_loop_exit) end,
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

test_start_twice(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {error, already_running} = py_context:start_loop(C),
    ok = py_context:stop_loop(C),
    stop(C).

test_stop_idle(Config) ->
    C = new_ctx(Config),
    {error, no_loop} = py_context:stop_loop(C),
    stop(C).

test_submit_idle_and_running(Config) ->
    C = new_ctx(Config),
    %% Without a running loop, submit reports no loop (the embedded modes
    %% step the loop through the event worker; the child has no such thing)
    {error, no_loop} = py_context:submit_await(C, ?TEST_MOD, async_add, [1, 2]),
    ok = py_context:start_loop(C),
    {ok, 3} = py_context:submit_await(C, ?TEST_MOD, async_add, [1, 2]),
    {ok, 7} = py_context:submit_await(C, ?TEST_MOD, add, [3, 4]),
    ok = py_context:stop_loop(C),
    stop(C).

test_submit_ordering(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    Refs = [begin
        {ok, R} = py_context:submit(C, ?TEST_MOD, async_add, [I, 0]),
        {I, R}
    end || I <- lists:seq(1, 100)],
    lists:foreach(fun({I, R}) ->
        {ok, I} = py_event_loop:await(R, 5000)
    end, Refs),
    ok = py_context:stop_loop(C),
    stop(C).

test_submit_errors_reported(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {error, {'ModuleNotFoundError', _}} = py_context:submit_await(C, no_such_mod_xyz, f, []),
    {error, {'AttributeError', _}} = py_context:submit_await(C, ?TEST_MOD, no_such_fn, []),
    {error, {'KeyError', _}} = py_context:submit_await(C, ?TEST_MOD, async_raise, [<<"k">>]),
    {error, {'ValueError', _}} = py_context:submit_await(C, ?TEST_MOD, raise_value_error, [<<"v">>]),
    ok = py_context:stop_loop(C),
    stop(C).

test_calls_rejected_while_running(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {error, loop_running} = py_context:eval(C, <<"1">>),
    {error, loop_running} = py_context:exec(C, <<"x = 1">>),
    {error, loop_running} = py_context:call(C, math, sqrt, [4]),
    ok = py_context:stop_loop(C),
    {ok, 1} = py_context:eval(C, <<"1">>),
    stop(C).

test_interrupt_ends_loop(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    ok = py_context:interrupt(C),
    receive {py_loop_exit, C, {error, interrupted}} -> ok
    after 3000 -> ct:fail(loop_not_interrupted)
    end,
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

test_owner_death_stops_loop(Config) ->
    C = new_ctx(Config),
    Owner = spawn(fun() -> receive die -> ok end end),
    ok = py_context:start_loop(C, #{owner => Owner}),
    Owner ! die,
    wait_until(fun() -> py_context:eval(C, <<"1">>) =:= {ok, 1} end, 5000),
    stop(C).

test_stop_context_while_looping(Config) ->
    C = new_ctx(Config),
    {ok, #{os_pid := Pid}} = py_context:child_info(C),
    ok = py_context:start_loop(C),
    ok = py_context:stop(C),
    wait_until(fun() -> py_nif:os_kill(Pid, 0) =:= {error, esrch} end, 5000),
    ok.

%% @doc No 30 s cap on a submitted call (the pipe write deadline of the
%% embedded modes does not apply): a 2 s task completes, and the loop keeps
%% answering meanwhile.
test_long_submitted_call(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, R} = py_context:submit(C, ?TEST_MOD, slow_task, [2]),
    {ok, 3} = py_context:submit_await(C, ?TEST_MOD, async_add, [1, 2]),
    {ok, <<"slow_done">>} = py_event_loop:await(R, 10000),
    ok = py_context:stop_loop(C),
    stop(C).

test_preload_before_loop(Config) ->
    C = new_ctx(Config, #{preload => <<"import py_test_isolated\npy_test_isolated.counter_increment(5)">>}),
    ok = py_context:start_loop(C),
    {ok, 5} = py_context:submit_await(C, ?TEST_MOD, counter_value, []),
    ok = py_context:stop_loop(C),
    stop(C).

%%% ============================================================================
%%% Serving on fds Erlang owns
%%% ============================================================================

test_tcp_serve_on_passed_fd(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {LSock, Port, ChildFd} = listen_pass(C),
    {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve, [ChildFd]),
    [<<"ok:x">> = roundtrip(Port, <<"x">>) || _ <- lists:seq(1, 100)],
    {ok, 100} = py_context:submit_await(C, py_test_workerloop, served_count, []),
    {ok, <<"stopped">>} = py_context:submit_await(C, py_test_workerloop, stop, [ChildFd]),
    ok = py_context:stop_loop(C),
    gen_tcp:close(LSock),
    stop(C).

test_udp_serve_on_passed_fd(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, USock} = gen_udp:open(0, [binary, {ip, ?HOST}, {active, false}]),
    {ok, Port} = inet:port(USock),
    {ok, Fd} = inet:getfd(USock),
    {ok, ChildFd} = py_context:pass_fd(C, Fd),
    {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve_udp, [ChildFd]),
    {ok, Client} = gen_udp:open(0, [binary, {ip, ?HOST}, {active, false}]),
    ok = gen_udp:send(Client, ?HOST, Port, <<"ping">>),
    {ok, {_, _, <<"udp:ping">>}} = gen_udp:recv(Client, 0, 2000),
    {ok, <<"stopped">>} = py_context:submit_await(C, py_test_workerloop, stop, [ChildFd]),
    gen_udp:close(Client),
    gen_udp:close(USock),
    ok = py_context:stop_loop(C),
    stop(C).

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
    {ok, ChildFd} = py_context:pass_fd(C, ConnFd),
    {ok, <<"adopted">>} = py_context:submit_await(C, py_test_workerloop, adopt, [ChildFd]),
    gen_tcp:close(Conn),
    receive {client, {ok, <<"ok:adopted?">>}} -> ok
    after 3000 -> ct:fail(no_reply_through_adopted_fd)
    end,
    gen_tcp:close(LSock),
    ok = py_context:stop_loop(C),
    stop(C).

%% @doc gunicorn shape, out of process: one listen socket, three child
%% processes accepting on their copy of it.
test_three_workers_one_listen_fd(Config) ->
    Ctxs = [new_ctx(Config) || _ <- lists:seq(1, 3)],
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, ?HOST}, {active, false}, {backlog, 512}]),
    {ok, Port} = inet:port(LSock),
    {ok, LFd} = inet:getfd(LSock),
    lists:foreach(fun({I, C}) ->
        ok = py_context:start_loop(C),
        {ok, ChildFd} = py_context:pass_fd(C, LFd),
        Tag = list_to_binary("w" ++ integer_to_list(I) ++ ":"),
        {ok, <<"serving">>} = py_context:submit_await(C, py_test_workerloop, serve, [ChildFd, Tag])
    end, lists:zip(lists:seq(1, 3), Ctxs)),
    Replies = [roundtrip(Port, <<"x">>) || _ <- lists:seq(1, 300)],
    300 = length([R || R <- Replies, binary:part(R, byte_size(R) - 4, 4) =:= <<"ok:x">>]),
    Tags = lists:usort([binary:part(R, 0, 3) || R <- Replies]),
    ct:log("workers that served: ~p", [Tags]),
    %% Which child wins accept() is up to the kernel; a fast worker can
    %% starve another over 300 connections. Two distinct workers prove
    %% the socket is shared.
    true = length(Tags) >= 2,
    [ok = py_context:stop_loop(C) || C <- Ctxs],
    [stop(C) || C <- Ctxs],
    gen_tcp:close(LSock),
    ok.

test_pass_fd_invalid(Config) ->
    C = new_ctx(Config),
    {error, _} = py_context:pass_fd(C, 123456),
    {error, {invalid_fd, -1}} = py_context:pass_fd(C, -1),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

%%% ============================================================================
%%% Coroutines
%%% ============================================================================

test_call_awaits_coroutine(Config) ->
    C = new_ctx(Config),
    {ok, 3} = py_context:call(C, ?TEST_MOD, async_add, [1, 2]),
    ok = py_context:exec(C, <<"import py_test_isolated">>),
    {ok, 3} = py_context:eval(C, <<"py_test_isolated.async_add(a, b)">>, #{a => 1, b => 2}),
    stop(C).

test_gather_is_concurrent(Config) ->
    C = new_ctx(Config),
    T0 = erlang:monotonic_time(millisecond),
    {ok, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]} = py_context:call(C, ?TEST_MOD, async_sleep_gather, [10, 0.1]),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ct:log("10 x sleep(0.1) gathered in ~p ms", [Elapsed]),
    true = Elapsed < 600,
    stop(C).

test_async_error(Config) ->
    C = new_ctx(Config),
    {error, {'KeyError', _}} = py_context:call(C, ?TEST_MOD, async_raise, [<<"nope">>]),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

test_concurrent_submitted_tasks(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    Refs = [begin {ok, R} = py_context:submit(C, ?TEST_MOD, task_value, [I]), {I, R} end
            || I <- lists:seq(1, 100)],
    lists:foreach(fun({I, R}) ->
        Expected = I * I,
        {ok, Expected} = py_event_loop:await(R, 10000)
    end, Refs),
    ok = py_context:stop_loop(C),
    stop(C).

test_large_async_result(Config) ->
    C = new_ctx(Config),
    Size = 16 * 1024 * 1024,
    {ok, Bin} = py_context:call(C, ?TEST_MOD, async_big, [Size]),
    Size = byte_size(Bin),
    stop(C).

test_async_call_in_coroutine(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"as_double">>, fun([X]) -> X * 2 end),
    {ok, 84} = py_context:call(C, ?TEST_MOD, async_erlang_call, [<<"as_double">>, 42]),
    py_callback:unregister(<<"as_double">>),
    stop(C).

test_async_calls_concurrent(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"as_double">>, fun([X]) -> X * 2 end),
    Expected = [I * 2 || I <- lists:seq(0, 99)],
    {ok, Expected} = py_context:call(C, ?TEST_MOD, async_erlang_calls, [<<"as_double">>, 100]),
    py_callback:unregister(<<"as_double">>),
    stop(C).

test_async_call_error(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"as_fail">>, fun(_) -> error(deliberate) end),
    {ok, <<"RuntimeError">>} = py_context:call(C, ?TEST_MOD, async_erlang_call_error, [<<"as_fail">>]),
    py_callback:unregister(<<"as_fail">>),
    stop(C).

test_send_from_coroutine(Config) ->
    C = new_ctx(Config),
    {ok, <<"sent">>} = py_context:call(C, ?TEST_MOD, async_send, [self(), coro_msg]),
    receive <<"coro_msg">> -> ok after 2000 -> ct:fail(no_message) end,
    stop(C).

test_run_helper_compat(Config) ->
    C = new_ctx(Config),
    {ok, 3} = py_context:call(C, ?TEST_MOD, run_helper_compat, []),
    stop(C).

%% @doc Streaming out of an isolated context: a submitted coroutine pushes
%% items with erlang.send; order and the done marker are asserted.
test_stream_via_send(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, R} = py_context:submit(C, ?TEST_MOD, stream_to, [self(), 1000]),
    Items = collect_items([]),
    Expected = lists:seq(0, 999),
    Expected = Items,
    {ok, 1000} = py_event_loop:await(R, 5000),
    ok = py_context:stop_loop(C),
    stop(C).

%%% ============================================================================
%%% The headline async case
%%% ============================================================================

%% @doc A coroutine wedged in time.sleep inside the loop: stop_loop/2 asks,
%% interrupts, then kills. The context is usable right after.
test_blocked_loop_is_killed(Config) ->
    C = new_ctx(Config, #{kill_after => 500}),
    {ok, #{os_pid := Pid1}} = py_context:child_info(C),
    ok = py_context:start_loop(C),
    %% The interrupt signal reaches time.sleep; block it so only the
    %% backstop can end the loop
    {ok, _} = py_context:submit(C, ?TEST_MOD, blocked_sleep, [60]),
    timer:sleep(300),
    T0 = erlang:monotonic_time(millisecond),
    Result = py_context:stop_loop(C, 300),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ct:log("stop_loop on a wedged loop: ~p after ~p ms", [Result, Elapsed]),
    ok = Result,
    true = Elapsed < 5000,
    receive {py_loop_exit, C, _} -> ok after 2000 -> ct:fail(no_loop_exit) end,
    {ok, #{os_pid := Pid2}} = py_context:child_info(C),
    true = Pid1 =/= Pid2,
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

%% @doc Contrast: the embedded loop cannot be killed; a wedged loop makes
%% stop_loop/2 time out and the sleep runs to completion.
test_blocked_loop_survives_in_worker(Config) ->
    C = new_ctx(Config),
    ok = py_context:start_loop(C),
    {ok, _} = py_context:submit(C, py_test_workerloop, block_loop, [4]),
    timer:sleep(300),
    Result = py_context:stop_loop(C, 300),
    ct:log("worker stop_loop on a wedged loop: ~p", [Result]),
    {error, timeout} = Result,
    %% Wait out the sleep so the context can be stopped cleanly
    timer:sleep(4500),
    _ = (try py_context:stop(C) catch _:_ -> ok end),
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

listen_pass(C) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, ?HOST}, {active, false}, {backlog, 128}]),
    {ok, Port} = inet:port(LSock),
    {ok, Fd} = inet:getfd(LSock),
    {ok, ChildFd} = py_context:pass_fd(C, Fd),
    {LSock, Port, ChildFd}.

roundtrip(Port, Data) ->
    {ok, S} = gen_tcp:connect(?HOST, Port, [binary, {active, false}], 2000),
    ok = gen_tcp:send(S, Data),
    {ok, Reply} = gen_tcp:recv(S, 0, 3000),
    gen_tcp:close(S),
    Reply.

collect_items(Acc) ->
    receive
        {<<"item">>, I} -> collect_items([I | Acc]);
        <<"done">> -> lists:reverse(Acc)
    after 5000 ->
        ct:fail({incomplete, length(Acc)})
    end.

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_until_loop(Fun, Deadline).

wait_until_loop(Fun, Deadline) ->
    case Fun() of
        true -> ok;
        _ ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> ct:fail(condition_not_met);
                false -> timer:sleep(50), wait_until_loop(Fun, Deadline)
            end
    end.

new_ctx(Config) ->
    new_ctx(Config, #{}).

new_ctx(Config, Extra) ->
    Mode = ?config(mode, Config),
    TestDir = ?config(test_dir, Config),
    Opts = maps:merge(#{mode => Mode, paths => [TestDir]}, Extra),
    {ok, C} = py_context:new(Opts),
    case Mode of
        worker ->
            ok = py_context:exec(C, iolist_to_binary(io_lib:format(
                "import sys\nif '~s' not in sys.path: sys.path.insert(0, '~s')",
                [TestDir, TestDir])));
        _ ->
            ok
    end,
    C.

stop(C) ->
    ok = py_context:stop(C),
    ok.

flush() ->
    receive _ -> flush() after 0 -> ok end.
