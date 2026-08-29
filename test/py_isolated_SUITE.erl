%%% @doc Common Test suite for `isolated' context mode.
%%%
%%% The interpreter runs in a child OS process. Round-trip cases run in a
%%% worker group too, so the two modes are held to the same results. The
%%% isolation cases (kill, segfault, rlimits, reaping, socket break) are what
%%% the embedded modes cannot do; the ones marked "contrast" assert the
%%% embedded behaviour as well, to document the difference.
-module(py_isolated_SUITE).

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
    test_call_eval_exec/1,
    test_state_persists/1,
    test_kwargs/1,
    test_type_round_trip/1,
    test_python_error/1,
    test_missing_module_and_function/1,
    test_large_payloads/1,
    test_callback_round_trip/1,
    test_nested_callback/1,
    test_callback_error/1,
    test_send_to_pid/1,
    test_concurrent_callers/1,
    test_timeout_interrupts_sleep/1,
    test_queued_timeout_does_not_interrupt_others/1,
    test_sys_state_reflects_activity/1,
    test_requests_during_restart_are_served/1,
    test_kill_reply_after_restart/1,
    test_stop_while_busy_and_looping/1,
    test_sys_get_status/1,
    test_context_outlives_creator/1,
    test_pool_of_isolated_contexts/1,
    test_child_info/1,
    test_sleep_is_interrupted/1,
    test_sleep_not_interrupted_in_worker/1,
    test_kill_backstop/1,
    test_kill_restarts_child/1,
    test_segfault_kills_only_child/1,
    test_rlimit_as/1,
    test_rlimit_cpu/1,
    test_rlimit_nofile/1,
    test_reaped_on_stop/1,
    test_reaped_on_crash/1,
    test_reaped_on_kill/1,
    test_no_orphan_when_vm_dies/1,
    test_socket_break_mid_call/1,
    test_restart_false_exits_context/1,
    test_restart_budget/1,
    test_numpy_imports/1,
    test_not_supported_fail_loud/1,
    test_bad_python_fails_at_start/1,
    test_startup_error_reported/1,
    test_cgroup_option_platform/1,
    test_env_option/1,
    test_preload_option/1
]).

-define(TEST_MOD, py_test_isolated).

all() ->
    [{group, worker}, {group, isolated}, {group, isolation}].

groups() ->
    RoundTrip = [
        test_call_eval_exec,
        test_state_persists,
        test_kwargs,
        test_type_round_trip,
        test_python_error,
        test_missing_module_and_function,
        test_large_payloads,
        test_callback_round_trip,
        test_nested_callback,
        test_callback_error,
        test_send_to_pid,
        test_concurrent_callers,
        test_timeout_interrupts_sleep
    ],
    Isolation = [
        test_pool_of_isolated_contexts,
        test_child_info,
        test_sleep_is_interrupted,
        test_sleep_not_interrupted_in_worker,
        test_kill_backstop,
        test_kill_restarts_child,
        test_segfault_kills_only_child,
        test_rlimit_as,
        test_rlimit_cpu,
        test_rlimit_nofile,
        test_reaped_on_stop,
        test_reaped_on_crash,
        test_reaped_on_kill,
        test_no_orphan_when_vm_dies,
        test_socket_break_mid_call,
        test_restart_false_exits_context,
        test_restart_budget,
        test_numpy_imports,
        test_not_supported_fail_loud,
        test_queued_timeout_does_not_interrupt_others,
        test_sys_state_reflects_activity,
        test_requests_during_restart_are_served,
        test_kill_reply_after_restart,
        test_stop_while_busy_and_looping,
        test_sys_get_status,
        test_context_outlives_creator,
        test_bad_python_fails_at_start,
        test_startup_error_reported,
        test_cgroup_option_platform,
        test_env_option,
        test_preload_option
    ],
    [{worker, [], RoundTrip},
     {isolated, [], RoundTrip},
     {isolation, [], Isolation}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    [{test_dir, test_dir()} | Config].

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(isolation, Config) ->
    [{mode, isolated} | Config];
init_per_group(Mode, Config) ->
    [{mode, Mode} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    flush(),
    ok.

%%% ============================================================================
%%% Round-trip cases (both modes)
%%% ============================================================================

test_call_eval_exec(Config) ->
    C = new_ctx(Config),
    {ok, 3} = py_context:call(C, ?TEST_MOD, add, [1, 2]),
    {ok, 4.0} = py_context:call(C, math, sqrt, [16]),
    {ok, 6} = py_context:eval(C, <<"2*3">>),
    {ok, 10} = py_context:eval(C, <<"a + b">>, #{a => 4, b => 6}),
    ok = py_context:exec(C, <<"def twice(x):\n    return 2 * x\n">>),
    {ok, 14} = py_context:call(C, '__main__', twice, [7]),
    stop(C).

test_state_persists(Config) ->
    C = new_ctx(Config),
    ok = py_context:exec(C, <<"counter = 0">>),
    lists:foreach(fun(_) -> ok = py_context:exec(C, <<"counter += 1">>) end,
                  lists:seq(1, 10)),
    {ok, 10} = py_context:eval(C, <<"counter">>),
    stop(C).

test_kwargs(Config) ->
    C = new_ctx(Config),
    {ok, {[1, 2], [{<<"a">>, 3}, {<<"b">>, <<"x">>}]}} =
        py_context:call(C, ?TEST_MOD, kwargs_probe, [1, 2], #{a => 3, b => <<"x">>}),
    stop(C).

test_type_round_trip(Config) ->
    C = new_ctx(Config),
    Probe = fun(V) ->
        {ok, Got} = py_context:call(C, ?TEST_MOD, identity, [V]),
        Got
    end,
    TypeOf = fun(V) ->
        {ok, T} = py_context:call(C, ?TEST_MOD, type_name, [V]),
        T
    end,
    true = Probe(true),
    false = Probe(false),
    none = Probe(none),
    none = Probe(undefined),
    none = Probe(nil),
    <<"str">> = TypeOf(<<"héllo"/utf8>>),
    <<"héllo"/utf8>> = Probe(<<"héllo"/utf8>>),
    <<"bytes">> = TypeOf(<<255, 0, 1>>),
    <<255, 0, 1>> = Probe(<<255, 0, 1>>),
    <<"bytes">> = TypeOf({bytes, <<"abc">>}),
    <<"abc">> = Probe({bytes, <<"abc">>}),
    42 = Probe(42),
    -1 = Probe(-1),
    %% Integers beyond 64 bits: the NIF converter has no bignum path
    %% (worker mode returns none); the ETF codec carries them exactly.
    case ?config(mode, Config) of
        isolated ->
            Big = 1 bsl 100,
            Big = Probe(Big),
            NegBig = -(1 bsl 100),
            NegBig = Probe(NegBig);
        _ ->
            ok
    end,
    3.5 = Probe(3.5),
    [] = Probe([]),
    [1, [2, 3], {4}] = Probe([1, [2, 3], {4}]),
    "abc" = Probe("abc"),
    {1, 2, 3} = Probe({1, 2, 3}),
    #{<<"k">> := [1, 2], 3 := {<<"a">>}} = Probe(#{<<"k">> => [1, 2], 3 => {a}}),
    <<"some_atom">> = Probe(some_atom),
    <<"str">> = TypeOf(some_atom),
    Self = self(),
    Self = Probe(Self),
    <<"Pid">> = TypeOf(Self),
    Ref = make_ref(),
    Ref = Probe(Ref),
    <<"Ref">> = TypeOf(Ref),
    {ok, nan} = py_context:eval(C, <<"float('nan')">>),
    {ok, infinity} = py_context:eval(C, <<"float('inf')">>),
    {ok, neg_infinity} = py_context:eval(C, <<"float('-inf')">>),
    {ok, {1, 2, 3}} = py_context:eval(C, <<"(1, 2, 3)">>),
    stop(C).

test_python_error(Config) ->
    C = new_ctx(Config),
    {error, {'ValueError', Msg}} = py_context:call(C, ?TEST_MOD, raise_value_error, [<<"boom">>]),
    true = lists:prefix("boom", to_list(Msg)),
    {error, {'ZeroDivisionError', _}} = py_context:eval(C, <<"1/0">>),
    {error, {'SyntaxError', _}} = py_context:exec(C, <<"def (:">>),
    %% Still usable
    {ok, 2} = py_context:eval(C, <<"1+1">>),
    stop(C).

test_missing_module_and_function(Config) ->
    C = new_ctx(Config),
    {error, {'ModuleNotFoundError', _}} = py_context:call(C, no_such_module_xyz, f, []),
    {error, {'AttributeError', _}} = py_context:call(C, math, no_such_function, []),
    stop(C).

test_large_payloads(Config) ->
    C = new_ctx(Config),
    lists:foreach(fun(Size) ->
        Bin = crypto:strong_rand_bytes(Size),
        {ok, Bin} = py_context:call(C, ?TEST_MOD, identity, [Bin]),
        {ok, Out} = py_context:call(C, ?TEST_MOD, big_payload, [Size]),
        Size = byte_size(Out)
    end, [1024 * 1024, 16 * 1024 * 1024]),
    stop(C).

test_callback_round_trip(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"iso_double">>, fun([X]) -> X * 2 end),
    {ok, 84} = py_context:call(C, ?TEST_MOD, callback, [<<"iso_double">>, 42]),
    {ok, 84} = py_context:eval(C, <<"__import__('erlang').call('iso_double', 42)">>),
    %% Attribute sugar: erlang.iso_double(...)
    {ok, 84} = py_context:eval(C, <<"__import__('erlang').iso_double(42)">>),
    py_callback:unregister(<<"iso_double">>),
    stop(C).

%% @doc A callback that calls back into the same context (nesting), two
%% levels deep. The socket protocol nests arbitrarily; worker mode's
%% suspension protocol does not, so that group only checks it is loud.
test_nested_callback(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"iso_nested">>, fun([X]) ->
        {ok, R} = py_context:call(C, ?TEST_MOD, add, [X, 1]),
        R
    end),
    py_callback:register(<<"iso_nested2">>, fun([X]) ->
        {ok, R} = py_context:call(C, ?TEST_MOD, callback, [<<"iso_nested">>, X]),
        R + 100
    end),
    case ?config(mode, Config) of
        isolated ->
            {ok, 11} = py_context:call(C, ?TEST_MOD, callback, [<<"iso_nested">>, 10]),
            {ok, 111} = py_context:call(C, ?TEST_MOD, callback, [<<"iso_nested2">>, 10]);
        worker ->
            case py_context:call(C, ?TEST_MOD, callback, [<<"iso_nested">>, 10]) of
                {ok, 11} -> ok;
                {error, _} -> ok
            end
    end,
    py_callback:unregister(<<"iso_nested">>),
    py_callback:unregister(<<"iso_nested2">>),
    stop(C).

test_callback_error(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"iso_crash">>, fun(_) -> error(deliberate) end),
    {ok, <<"RuntimeError">>} = py_context:call(C, ?TEST_MOD, callback_error_type, [<<"iso_crash">>]),
    {ok, <<"RuntimeError">>} = py_context:call(C, ?TEST_MOD, callback_error_type, [<<"iso_not_registered">>]),
    py_callback:unregister(<<"iso_crash">>),
    stop(C).

test_send_to_pid(Config) ->
    C = new_ctx(Config),
    {ok, true} = py_context:call(C, ?TEST_MOD, send, [self(), {hello, 1}]),
    receive {<<"hello">>, 1} -> ok after 2000 -> ct:fail(no_message) end,
    stop(C).

test_concurrent_callers(Config) ->
    C = new_ctx(Config),
    Self = self(),
    N = 20,
    Pids = [spawn_link(fun() ->
        Results = [py_context:call(C, ?TEST_MOD, add, [I, J]) || J <- lists:seq(1, 25)],
        Self ! {done, I, Results}
    end) || I <- lists:seq(1, N)],
    lists:foreach(fun(I) ->
        receive
            {done, I, Results} ->
                Expected = [{ok, I + J} || J <- lists:seq(1, 25)],
                Expected = Results
        after 30000 ->
            ct:fail({timeout, I})
        end
    end, lists:seq(1, N)),
    _ = Pids,
    stop(C).

test_timeout_interrupts_sleep(Config) ->
    C = new_ctx(Config),
    T0 = erlang:monotonic_time(millisecond),
    {error, timeout} = py_context:eval(C, <<"__import__('time').sleep(0.3)">>, #{}, 100),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    %% Both modes return promptly; the sleep itself ends by the time we
    %% call again in worker mode (0.3 s), immediately in isolated mode.
    true = Elapsed < 1500,
    {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 5000),
    stop(C).

%%% ============================================================================
%%% Isolation cases
%%% ============================================================================

test_pool_of_isolated_contexts(_Config) ->
    {ok, Ctxs} = py_context_router:start_pool(iso_pool, 4, isolated),
    4 = length(Ctxs),
    Pids = lists:usort([begin
        {ok, #{os_pid := P}} = py_context:child_info(Cx), P
    end || Cx <- Ctxs]),
    4 = length(Pids),
    [{ok, 4} = py_context:eval(Cx, <<"2+2">>) || Cx <- Ctxs],
    {ok, 4.0} = py:call(iso_pool, math, sqrt, [16]),
    ok = py_context_router:stop_pool(iso_pool),
    timer:sleep(200),
    [false = os_pid_alive(P) || P <- Pids],
    ok.

test_child_info(Config) ->
    C = new_ctx(Config),
    {ok, #{os_pid := Pid, python_version := V, executable := Exe}} = py_context:child_info(C),
    true = is_integer(Pid) andalso Pid > 0,
    true = is_binary(V),
    true = is_binary(Exe),
    true = os_pid_alive(Pid),
    {error, not_isolated} = py_context:child_info(self()),
    stop(C).

%% @doc The case the embedded modes cannot pass: a blocking C call is
%% interrupted at once.
test_sleep_is_interrupted(Config) ->
    C = new_ctx(Config),
    Self = self(),
    spawn_link(fun() ->
        Self ! {result, py_context:call(C, ?TEST_MOD, sleep_then, [60, ok])}
    end),
    timer:sleep(200),
    T0 = erlang:monotonic_time(millisecond),
    ok = py_context:interrupt(C),
    receive
        {result, R} ->
            {error, interrupted} = R,
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            ct:log("interrupted after ~p ms", [Elapsed]),
            true = Elapsed < 1000
    after 5000 ->
        ct:fail(sleep_not_interrupted)
    end,
    %% Same child, still usable, state intact
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

%% @doc Contrast: in worker mode the interrupt only lands when the C call
%% returns, so a 1.5 s sleep takes its full time.
test_sleep_not_interrupted_in_worker(_Config) ->
    {ok, C} = py_context:new(#{mode => worker}),
    Self = self(),
    spawn_link(fun() ->
        Self ! {result, py_context:eval(C, <<"__import__('time').sleep(1.5)">>)}
    end),
    timer:sleep(200),
    T0 = erlang:monotonic_time(millisecond),
    _ = py_context:interrupt(C),
    receive
        {result, _} ->
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            ct:log("worker returned after ~p ms", [Elapsed]),
            true = Elapsed >= 1000
    after 10000 ->
        ct:fail(worker_never_returned)
    end,
    py_context:stop(C),
    ok.

%% @doc Signals blocked in the child: the soft interrupt cannot land and the
%% kill backstop fires after kill_after.
test_kill_backstop(Config) ->
    C = new_ctx(Config, #{kill_after => 300}),
    {ok, #{os_pid := Pid1}} = py_context:child_info(C),
    Self = self(),
    spawn_link(fun() ->
        Self ! {result, py_context:call(C, ?TEST_MOD, blocked_sleep, [60])}
    end),
    timer:sleep(200),
    T0 = erlang:monotonic_time(millisecond),
    ok = py_context:interrupt(C),
    receive
        {result, {error, killed}} ->
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            ct:log("killed after ~p ms", [Elapsed]),
            true = Elapsed < 3000
    after 10000 ->
        ct:fail(backstop_did_not_fire)
    end,
    false = os_pid_alive(Pid1),
    {ok, #{os_pid := Pid2}} = py_context:child_info(C),
    true = Pid1 =/= Pid2,
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

test_kill_restarts_child(Config) ->
    C = new_ctx(Config),
    ok = py_context:exec(C, <<"state = 'before'">>),
    {ok, #{os_pid := Pid1}} = py_context:child_info(C),
    ok = py_context:kill(C),
    false = os_pid_alive(Pid1),
    {ok, #{os_pid := Pid2}} = py_context:child_info(C),
    true = Pid1 =/= Pid2,
    %% State is gone, the context is not
    {error, {'NameError', _}} = py_context:eval(C, <<"state">>),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    {error, not_isolated} = py_context:kill(self()),
    stop(C).

%% @doc The headline case: a segfault kills one child, the node and every
%% other context survive.
test_segfault_kills_only_child(Config) ->
    C = new_ctx(Config),
    Other = new_ctx(Config),
    {ok, W} = py_context:new(#{mode => worker}),
    {ok, #{os_pid := Pid1}} = py_context:child_info(C),
    {error, {child_exited, {signal, Sig}}} = py_context:call(C, ?TEST_MOD, segfault, []),
    true = is_segfault_signal(Sig),
    false = os_pid_alive(Pid1),
    true = is_process_alive(C),
    {ok, #{os_pid := Pid2}} = py_context:child_info(C),
    true = Pid2 =/= Pid1,
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    {ok, 4} = py_context:eval(Other, <<"2+2">>),
    {ok, 4} = py_context:eval(W, <<"2+2">>),
    py_context:stop(W),
    stop(Other),
    stop(C).

%% @doc `as' is enforced by the kernel (Linux, FreeBSD) or by the child's
%% RSS watchdog (macOS); either way the allocation fails or the child dies,
%% the node is unaffected and the context recovers.
test_rlimit_as(Config) ->
    case sanitized_child() of
        true -> {skip, "sanitizer runtime in the child needs unbounded address space"};
        false -> test_rlimit_as_1(Config)
    end.

test_rlimit_as_1(Config) ->
    %% Free-threaded CPython reserves a large address range at startup, so
    %% a limit that a regular build fits in keeps it from even importing
    %% the socket module.
    Probe = new_ctx(Config),
    FreeThreaded = py_context:eval(Probe,
        <<"hasattr(__import__('sys'), '_is_gil_enabled') and not __import__('sys')._is_gil_enabled()">>),
    stop(Probe),
    case FreeThreaded of
        {ok, true} -> {skip, "free-threaded CPython needs an as limit far above test sizes"};
        _ -> test_rlimit_as_2(Config)
    end.

test_rlimit_as_2(Config) ->
    C = new_ctx(Config, #{rlimits => #{as => 1024 * 1024 * 1024}}),
    Result = py_context:call(C, ?TEST_MOD, allocate_and_touch, [2 * 1024 * 1024 * 1024], #{}, 120000),
    ct:log("allocate past as limit: ~p", [Result]),
    case {Result, rlimit_as_enforced()} of
        {{error, {'MemoryError', _}}, _} -> ok;
        {{error, {child_exited, {memory_limit, _}}}, false} -> ok;
        {{error, {child_exited, _}}, true} -> ok;
        Other -> ct:fail({unexpected, Other})
    end,
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

test_rlimit_cpu(Config) ->
    C = new_ctx(Config, #{rlimits => #{cpu => 1}}),
    Result = py_context:call(C, ?TEST_MOD, spin, [30], #{}, 20000),
    ct:log("spin past RLIMIT_CPU: ~p", [Result]),
    %% SIGXCPU (24 on Linux and BSD/macOS)
    {error, {child_exited, {signal, Sig}}} = Result,
    true = Sig =:= 24 orelse Sig =:= 30,
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

test_rlimit_nofile(Config) ->
    C = new_ctx(Config, #{rlimits => #{nofile => 32}}),
    {ok, 32} = py_context:eval(C, <<"__import__('resource').getrlimit(__import__('resource').RLIMIT_NOFILE)[0]">>),
    stop(C).

test_reaped_on_stop(Config) ->
    C = new_ctx(Config),
    {ok, #{os_pid := Pid}} = py_context:child_info(C),
    ok = py_context:stop(C),
    wait_gone(Pid),
    ok.

test_reaped_on_crash(Config) ->
    C = new_ctx(Config, #{restart => false}),
    {ok, #{os_pid := Pid}} = py_context:child_info(C),
    unlink(C),
    Mon = erlang:monitor(process, C),
    _ = py_context:call(C, ?TEST_MOD, segfault, []),
    receive {'DOWN', Mon, process, C, _} -> ok after 5000 -> ct:fail(context_survived) end,
    wait_gone(Pid),
    ok.

test_reaped_on_kill(Config) ->
    C = new_ctx(Config),
    {ok, #{os_pid := Pid}} = py_context:child_info(C),
    ok = py_context:kill(C),
    wait_gone(Pid),
    stop(C).

%% @doc A child of another VM must not outlive that VM.
test_no_orphan_when_vm_dies(_Config) ->
    case peer_available() of
        false ->
            {skip, "peer module not available"};
        true ->
            %% standard_io works without distribution
            {ok, Peer, _Node} = peer:start_link(#{
                connection => standard_io,
                args => lists:append([["-pa", P] || P <- code:get_path()])
            }),
            {ok, _} = peer:call(Peer, application, ensure_all_started, [erlang_python]),
            {ok, C} = peer:call(Peer, py_context, new, [#{mode => isolated}]),
            {ok, #{os_pid := Pid}} = peer:call(Peer, py_context, child_info, [C]),
            true = os_pid_alive(Pid),
            %% Park the child in a blocking C call so only the EOF watchdog
            %% (or PDEATHSIG) can end it
            ok = peer:cast(Peer, py_context, eval, [C, <<"__import__('time').sleep(60)">>]),
            timer:sleep(300),
            %% Hard stop: the peer VM is killed, nothing in it runs cleanup
            peer:stop(Peer),
            wait_gone(Pid),
            ok
    end.

%% @doc The socket breaks under a pending call: it fails with a clear error,
%% the next call does not hang, and the restart recovers.
test_socket_break_mid_call(Config) ->
    C = new_ctx(Config),
    Result = py_context:call(C, ?TEST_MOD, close_control_socket, [], #{}, 10000),
    ct:log("call under socket break: ~p", [Result]),
    {error, {child_exited, _}} = Result,
    T0 = erlang:monotonic_time(millisecond),
    {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 5000),
    true = erlang:monotonic_time(millisecond) - T0 < 3000,
    stop(C).

test_restart_false_exits_context(Config) ->
    C = new_ctx(Config, #{restart => false}),
    unlink(C),
    Mon = erlang:monitor(process, C),
    {error, {child_exited, {signal, Sig}}} = py_context:call(C, ?TEST_MOD, segfault, []),
    true = is_segfault_signal(Sig),
    receive
        {'DOWN', Mon, process, C, {child_exited, {signal, Sig}}} -> ok
    after 5000 ->
        ct:fail(context_did_not_exit)
    end,
    {error, {context_died, _}} = py_context:eval(C, <<"1">>),
    ok.

test_restart_budget(Config) ->
    C = new_ctx(Config, #{max_restarts => 2, restart_period => 60000}),
    unlink(C),
    Mon = erlang:monitor(process, C),
    _ = py_context:call(C, ?TEST_MOD, segfault, []),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    _ = py_context:call(C, ?TEST_MOD, segfault, []),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    %% Third crash exceeds the budget
    _ = py_context:call(C, ?TEST_MOD, segfault, []),
    receive {'DOWN', Mon, process, C, _} -> ok after 5000 -> ct:fail(budget_not_enforced) end,
    ok.

test_numpy_imports(Config) ->
    C = new_ctx(Config),
    case py_context:eval(C, <<"__import__('importlib.util').util.find_spec('numpy') is not None">>) of
        {ok, true} ->
            {ok, 45} = py_context:call(C, ?TEST_MOD, numpy_sum, [10]),
            {ok, [[1, 2], [3, 4]]} = py_context:eval(C, <<"__import__('numpy').array([[1,2],[3,4]])">>),
            stop(C);
        _ ->
            stop(C),
            {skip, "numpy not installed for the child interpreter"}
    end.

%% @doc A caller whose request is still queued times out: its request is
%% dropped, the request that is executing is not interrupted, and the kill
%% backstop does not fire because the context is busy.
test_queued_timeout_does_not_interrupt_others(Config) ->
    C = new_ctx(Config, #{kill_after => 200}),
    {ok, #{os_pid := Pid}} = py_context:child_info(C),
    Self = self(),
    %% Occupies the child for 1.5 s
    spawn_link(fun() ->
        Self ! {long, py_context:call(C, ?TEST_MOD, sleep_then, [1.5, done], #{}, 10000)}
    end),
    timer:sleep(100),
    %% Queued behind it, gives up after 200 ms
    {error, timeout} = py_context:eval(C, <<"'never'">>, #{}, 200),
    receive
        {long, R} -> {ok, <<"done">>} = R
    after 5000 ->
        ct:fail(long_call_lost)
    end,
    %% Same child, no kill happened, and the cancelled eval never ran
    {ok, #{os_pid := Pid}} = py_context:child_info(C),
    {ok, 4} = py_context:eval(C, <<"2+2">>, #{}, 5000),
    stop(C).

%% @doc The gen_statem state names what the context is doing.
test_sys_state_reflects_activity(Config) ->
    C = new_ctx(Config),
    {idle, _} = sys:get_state(C),
    Self = self(),
    spawn_link(fun() -> Self ! {done, py_context:call(C, ?TEST_MOD, sleep_then, [0.5, x])} end),
    timer:sleep(100),
    {{busy, Id}, _} = sys:get_state(C),
    true = is_integer(Id),
    receive {done, {ok, <<"x">>}} -> ok after 5000 -> ct:fail(no_reply) end,
    {idle, _} = sys:get_state(C),
    ok = py_context:start_loop(C),
    {looping, _} = sys:get_state(C),
    ok = py_context:stop_loop(C),
    {idle, _} = sys:get_state(C),
    stop(C).

%% @doc A request arriving while the child restarts waits for the new child
%% instead of failing.
test_requests_during_restart_are_served(Config) ->
    C = new_ctx(Config),
    Self = self(),
    Crasher = spawn_link(fun() -> Self ! {crash, py_context:call(C, ?TEST_MOD, segfault, [])} end),
    %% Sent right behind the segfault: postponed through {restarting, _}
    spawn_link(fun() -> Self ! {next, py_context:eval(C, <<"2+2">>, #{}, 10000)} end),
    receive
        {crash, {error, {child_exited, {signal, Sig}}}} -> true = is_segfault_signal(Sig)
    after 10000 -> ct:fail(no_crash_report)
    end,
    receive {next, {ok, 4}} -> ok after 10000 -> ct:fail(request_not_served_after_restart) end,
    _ = Crasher,
    stop(C).

%% @doc kill/1 answers once the new child is up, so the next call cannot
%% race the restart.
test_kill_reply_after_restart(Config) ->
    C = new_ctx(Config),
    {ok, #{os_pid := Pid1}} = py_context:child_info(C),
    ok = py_context:kill(C),
    {ok, #{os_pid := Pid2}} = py_context:child_info(C),
    true = Pid1 =/= Pid2,
    {idle, _} = sys:get_state(C),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

%% @doc stop/1 from a third process while a call runs and while a loop
%% runs: nothing hangs, waiting callers get a reply or a DOWN.
test_stop_while_busy_and_looping(Config) ->
    C1 = new_ctx(Config),
    Self = self(),
    spawn_link(fun() -> Self ! {busy, py_context:call(C1, ?TEST_MOD, sleep_then, [5, x], #{}, 10000)} end),
    timer:sleep(100),
    ok = py_context:stop(C1),
    receive
        {busy, {error, _}} -> ok
    after 5000 -> ct:fail(busy_caller_hung)
    end,
    C2 = new_ctx(Config),
    ok = py_context:start_loop(C2),
    ok = py_context:stop(C2),
    receive {py_loop_exit, C2, _} -> ok after 5000 -> ct:fail(no_loop_exit_on_stop) end,
    false = is_process_alive(C2),
    ok.

test_sys_get_status(Config) ->
    C = new_ctx(Config),
    {status, C, {module, gen_statem}, _} = sys:get_status(C),
    ok = sys:trace(C, true),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    ok = sys:trace(C, false),
    stop(C).

%% @doc The process that created the context may exit normally; the
%% context keeps serving (as embedded contexts do).
test_context_outlives_creator(Config) ->
    Self = self(),
    spawn(fun() -> Self ! {ctx, new_ctx(Config)} end),
    C = receive {ctx, X} -> X after 15000 -> ct:fail(no_ctx) end,
    timer:sleep(100),
    true = is_process_alive(C),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

test_not_supported_fail_loud(Config) ->
    C = new_ctx(Config),
    {error, not_supported_in_isolated} = py_context:call_method(C, make_ref(), <<"x">>, []),
    {error, {'RuntimeError', Msg}} = py_context:eval(C, <<"__import__('erlang').schedule('x')">>),
    true = string:find(to_list(Msg), "isolated") =/= nomatch,
    {error, {'RuntimeError', _}} = py_context:eval(C, <<"__import__('erlang').Channel()">>),
    {error, not_supported_in_isolated} = py_context:loop_ref(C),
    stop(C).

test_bad_python_fails_at_start(_Config) ->
    {error, {python_not_found, _}} = py_context:new(#{mode => isolated, python => "/no/such/python"}),
    {error, {child_exited_at_start, _, _}} = py_context:new(#{mode => isolated, python => "/bin/sh"}),
    ok.

test_startup_error_reported(_Config) ->
    case sanitized_child() of
        true -> {skip, "sanitizer runtime in the child needs unbounded address space"};
        false -> test_startup_error_reported_1()
    end.

test_startup_error_reported_1() ->
    %% An impossible rlimit is reported, not silently ignored. The kernel
    %% may kill the child before it connects (child_exited_at_start), the
    %% child may report the failed setrlimit (startup_error), or the macOS
    %% watchdog may end it at once; all are loud.
    case py_context:new(#{mode => isolated, rlimits => #{as => 1}}) of
        {error, {startup_error, _}} -> ok;
        {error, {child_exited_at_start, _, _}} -> ok;
        {error, {handshake_failed, _}} -> ok;
        {ok, C} -> py_context:stop(C), ct:fail(limit_ignored);
        Other -> ct:fail({unexpected, Other})
    end.

%% @doc cgroups exist only on Linux: elsewhere the option is refused before
%% a child is spawned, and rlimits remain the way to bound the child.
test_cgroup_option_platform(_Config) ->
    case os:type() of
        {unix, linux} ->
            %% A non-writable path is reported by the child
            {error, {startup_error, [{cgroup, _}]}} =
                py_context:new(#{mode => isolated, cgroup => "/nonexistent/cgroup"}),
            ok;
        {unix, Os} ->
            {error, {cgroup_unsupported, Os}} =
                py_context:new(#{mode => isolated, cgroup => "/sys/fs/cgroup/x"}),
            %% Limits still apply without cgroups
            {ok, C} = py_context:new(#{mode => isolated, rlimits => #{nofile => 48, cpu => 5}}),
            {ok, 48} = py_context:eval(C, <<"__import__('resource').getrlimit(__import__('resource').RLIMIT_NOFILE)[0]">>),
            {ok, 5} = py_context:eval(C, <<"__import__('resource').getrlimit(__import__('resource').RLIMIT_CPU)[0]">>),
            stop(C)
    end.

test_env_option(Config) ->
    C = new_ctx(Config, #{env => #{"PY_ISOLATED_PROBE" => "yes"}}),
    {ok, <<"yes">>} = py_context:eval(C, <<"__import__('os').environ.get('PY_ISOLATED_PROBE')">>),
    stop(C).

test_preload_option(Config) ->
    C = new_ctx(Config, #{preload => <<"preloaded = 'yes'">>}),
    {ok, <<"yes">>} = py_context:eval(C, <<"preloaded">>),
    stop(C).

%%% ============================================================================
%%% Helpers
%%% ============================================================================

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

test_dir() ->
    filename:join(code:lib_dir(erlang_python), "test").

os_pid_alive(Pid) ->
    case py_nif:os_kill(Pid, 0) of
        ok ->
            %% Alive, or a zombie: a zombie shows as Z in ps
            case string:trim(os:cmd("ps -o stat= -p " ++ integer_to_list(Pid))) of
                "" -> false;
                "Z" ++ _ -> zombie;
                _ -> true
            end;
        {error, esrch} ->
            false;
        {error, eperm} ->
            true
    end.

wait_gone(Pid) ->
    wait_gone(Pid, 50).

wait_gone(Pid, 0) ->
    ct:fail({child_still_present, Pid, os_pid_alive(Pid)});
wait_gone(Pid, N) ->
    case os_pid_alive(Pid) of
        false -> ok;
        _ -> timer:sleep(100), wait_gone(Pid, N - 1)
    end.

%% A sanitizer runtime (LD_PRELOAD=libasan in the ASan job) is inherited
%% by the child: it aborts on a segfault and reserves terabytes of address
%% space, so rlimit cases cannot mean anything there.
sanitized_child() ->
    Pre = case os:getenv("LD_PRELOAD") of false -> ""; P -> P end,
    string:find(Pre, "asan") =/= nomatch orelse os:getenv("ASAN_OPTIONS") =/= false.

is_segfault_signal(11) -> true;
is_segfault_signal(6) -> sanitized_child();
is_segfault_signal(_) -> false.

rlimit_as_enforced() ->
    case os:type() of
        {unix, linux} -> true;
        {unix, freebsd} -> true;
        _ -> false
    end.

peer_available() ->
    code:ensure_loaded(peer) =:= {module, peer}.

to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) when is_list(L) -> L.

flush() ->
    receive _ -> flush() after 0 -> ok end.
