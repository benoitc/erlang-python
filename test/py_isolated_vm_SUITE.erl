%%% @doc Common Test suite: an isolated context as a participant in the VM.
%%%
%%% Mirrors, case for case, what py_pid_send_SUITE, py_callback_encoding_SUITE,
%%% py_thread_callback_SUITE and py_actor_SUITE prove for the embedded modes:
%%% pids, erlang.send, whereis, callback result encoding, Python threads
%%% calling Erlang, and actor-style state. Every case runs in a worker group
%%% too, so a divergence between modes fails as a pair.
-module(py_isolated_vm_SUITE).

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
    test_pid_is_pid/1,
    test_pid_equality_and_hash/1,
    test_pid_in_structure/1,
    test_send_simple/1,
    test_send_multiple_ordered/1,
    test_send_complex_term/1,
    test_send_is_nonblocking/1,
    test_send_to_dead_process/1,
    test_send_bad_pid/1,
    test_send_from_coroutine/1,
    test_whereis/1,
    test_suspension_is_base_exception/1,
    test_callback_inside_except_exception/1,
    test_encoding_binary_with_escapes/1,
    test_encoding_binary_non_utf8/1,
    test_encoding_large_binary/1,
    test_encoding_atom_becomes_str/1,
    test_encoding_empty_list/1,
    test_encoding_erlang_string/1,
    test_encoding_nested_containers/1,
    test_encoding_pid_and_ref/1,
    test_encoding_floats/1,
    test_encoding_booleans_and_none/1,
    test_encoding_python_types/1,
    test_threads_call_erlang/1,
    test_threadpool_calls/1,
    test_threadpool_error/1,
    test_threadpool_nested/1,
    test_threads_high_concurrency/1,
    test_counter_actor/1,
    test_state_reset_on_restart/1,
    test_state_isolated_between_contexts/1,
    test_ping_pong/1,
    test_feed_through_callback/1
]).

-define(TEST_MOD, py_test_isolated).

all() ->
    [{group, worker}, {group, isolated}].

groups() ->
    Cases = [
        test_pid_is_pid,
        test_pid_equality_and_hash,
        test_pid_in_structure,
        test_send_simple,
        test_send_multiple_ordered,
        test_send_complex_term,
        test_send_is_nonblocking,
        test_send_to_dead_process,
        test_send_bad_pid,
        test_send_from_coroutine,
        test_whereis,
        test_suspension_is_base_exception,
        test_callback_inside_except_exception,
        test_encoding_binary_with_escapes,
        test_encoding_binary_non_utf8,
        test_encoding_large_binary,
        test_encoding_atom_becomes_str,
        test_encoding_empty_list,
        test_encoding_erlang_string,
        test_encoding_nested_containers,
        test_encoding_pid_and_ref,
        test_encoding_floats,
        test_encoding_booleans_and_none,
        test_encoding_python_types,
        test_threads_call_erlang,
        test_threadpool_calls,
        test_threadpool_error,
        test_threadpool_nested,
        test_threads_high_concurrency,
        test_counter_actor,
        test_state_reset_on_restart,
        test_state_isolated_between_contexts,
        test_ping_pong,
        test_feed_through_callback
    ],
    [{worker, [], Cases}, {isolated, [], Cases}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config].

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(Mode, Config) ->
    [{mode, Mode} | Config].

end_per_group(_Group, _Config) ->
    ok.

end_per_testcase(_TestCase, _Config) ->
    flush(),
    ok.

%%% ============================================================================
%%% Pids, send, whereis
%%% ============================================================================

test_pid_is_pid(Config) ->
    C = new_ctx(Config),
    {ok, true} = py_context:call(C, ?TEST_MOD, is_pid, [self()]),
    {ok, <<"Pid">>} = py_context:call(C, ?TEST_MOD, type_name, [self()]),
    Self = self(),
    {ok, Self} = py_context:call(C, ?TEST_MOD, identity, [Self]),
    stop(C).

test_pid_equality_and_hash(Config) ->
    C = new_ctx(Config),
    Self = self(),
    Other = spawn(fun() -> receive stop -> ok end end),
    {ok, true} = py_context:call(C, ?TEST_MOD, pid_equal, [Self, Self]),
    {ok, false} = py_context:call(C, ?TEST_MOD, pid_equal, [Self, Other]),
    {ok, true} = py_context:call(C, ?TEST_MOD, pid_hash_equal, [Self, Self]),
    Other ! stop,
    stop(C).

test_pid_in_structure(Config) ->
    C = new_ctx(Config),
    Self = self(),
    {ok, #{<<"owner">> := Self, <<"list">> := [Self, {Self, 1}]}} =
        py_context:call(C, ?TEST_MOD, pid_in_structure, [Self]),
    stop(C).

test_send_simple(Config) ->
    C = new_ctx(Config),
    {ok, true} = py_context:call(C, ?TEST_MOD, send, [self(), <<"hello">>]),
    receive <<"hello">> -> ok after 2000 -> ct:fail(no_message) end,
    stop(C).

test_send_multiple_ordered(Config) ->
    C = new_ctx(Config),
    N = 500,
    {ok, N} = py_context:call(C, ?TEST_MOD, send_many, [self(), N]),
    Items = collect_items([]),
    Expected = lists:seq(0, N - 1),
    Expected = Items,
    stop(C).

test_send_complex_term(Config) ->
    C = new_ctx(Config),
    Term = {hello, 42, [1, 2, 3], #{<<"key">> => <<"value">>}, true, none, 1.5},
    {ok, true} = py_context:call(C, ?TEST_MOD, send, [self(), Term]),
    receive
        {<<"hello">>, 42, [1, 2, 3], #{<<"key">> := <<"value">>}, true, none, 1.5} -> ok
    after 2000 ->
        ct:fail(no_message)
    end,
    stop(C).

test_send_is_nonblocking(Config) ->
    C = new_ctx(Config),
    Sink = spawn(fun() -> receive stop -> ok end end),
    {ok, Ms} = py_context:call(C, ?TEST_MOD, send_timing, [Sink, 1000]),
    ct:log("1000 erlang.send took ~.1f ms (~.1f us each)", [Ms, Ms]),
    true = Ms < 5000,
    Sink ! stop,
    stop(C).

test_send_to_dead_process(Config) ->
    C = new_ctx(Config),
    Dead = spawn(fun() -> ok end),
    timer:sleep(50),
    false = is_process_alive(Dead),
    {ok, <<"process_error">>} = py_context:call(C, ?TEST_MOD, send_to_dead, [Dead]),
    stop(C).

test_send_bad_pid(Config) ->
    C = new_ctx(Config),
    {ok, <<"type_error">>} = py_context:call(C, ?TEST_MOD, send_bad_pid, []),
    stop(C).

test_send_from_coroutine(Config) ->
    C = new_ctx(Config),
    {ok, <<"sent">>} = py_context:call(C, ?TEST_MOD, send_from_coroutine, [self(), from_coro]),
    receive <<"from_coro">> -> ok after 2000 -> ct:fail(no_message) end,
    stop(C).

test_whereis(Config) ->
    C = new_ctx(Config),
    Name = py_isolated_vm_probe,
    true = register(Name, self()),
    Self = self(),
    {ok, Self} = py_context:call(C, ?TEST_MOD, whereis, [<<"py_isolated_vm_probe">>]),
    {ok, none} = py_context:call(C, ?TEST_MOD, whereis, [<<"no_such_registered_name_xyz">>]),
    unregister(Name),
    stop(C).

test_suspension_is_base_exception(Config) ->
    C = new_ctx(Config),
    {ok, true} = py_context:call(C, ?TEST_MOD, suspension_is_base_exception, []),
    stop(C).

test_callback_inside_except_exception(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"vm_echo">>, fun([X]) -> X end),
    {ok, {<<"ok">>, 42}} = py_context:call(C, ?TEST_MOD, call_inside_except_exception, [<<"vm_echo">>, 42]),
    py_callback:unregister(<<"vm_echo">>),
    stop(C).

%%% ============================================================================
%%% Callback result encoding (py_callback_encoding_SUITE)
%%% ============================================================================

test_encoding_binary_with_escapes(Config) ->
    C = new_ctx(Config),
    Value = <<"back\\slash \"dq\" 'sq'\nnewline\ttab\r">>,
    Value = probe(C, Value),
    <<"str">> = probe_type(C, Value),
    stop(C).

test_encoding_binary_non_utf8(Config) ->
    C = new_ctx(Config),
    Value = <<0, 1, 255, 254, 128>>,
    Value = probe(C, Value),
    <<"bytes">> = probe_type(C, Value),
    stop(C).

test_encoding_large_binary(Config) ->
    C = new_ctx(Config),
    Value = binary:copy(<<"abcdefghij">>, 20000),
    Value = probe(C, Value),
    stop(C).

test_encoding_atom_becomes_str(Config) ->
    C = new_ctx(Config),
    <<"some_atom">> = probe(C, some_atom),
    <<"str">> = probe_type(C, some_atom),
    stop(C).

test_encoding_empty_list(Config) ->
    C = new_ctx(Config),
    [] = probe(C, []),
    <<"list">> = probe_type(C, []),
    stop(C).

test_encoding_erlang_string(Config) ->
    C = new_ctx(Config),
    "abc" = probe(C, "abc"),
    <<"list">> = probe_type(C, "abc"),
    <<"abc">> = probe(C, <<"abc">>),
    <<"str">> = probe_type(C, <<"abc">>),
    stop(C).

test_encoding_nested_containers(Config) ->
    C = new_ctx(Config),
    Value = #{<<"k">> => [1, 2.5, {a, b}, #{<<"inner">> => [[], {}]}]},
    Expected = #{<<"k">> => [1, 2.5, {<<"a">>, <<"b">>}, #{<<"inner">> => [[], {}]}]},
    Expected = probe(C, Value),
    <<"dict">> = probe_type(C, Value),
    stop(C).

test_encoding_pid_and_ref(Config) ->
    C = new_ctx(Config),
    Pid = self(),
    Pid = probe(C, Pid),
    <<"Pid">> = probe_type(C, Pid),
    Ref = make_ref(),
    Ref = probe(C, Ref),
    <<"Ref">> = probe_type(C, Ref),
    stop(C).

test_encoding_floats(Config) ->
    C = new_ctx(Config),
    lists:foreach(fun(F) -> F = probe(C, F) end,
                  [3.14159265358979, 1.0e-300, 1.7976931348623157e308, -0.0]),
    stop(C).

test_encoding_booleans_and_none(Config) ->
    C = new_ctx(Config),
    true = probe(C, true),
    false = probe(C, false),
    <<"bool">> = probe_type(C, true),
    lists:foreach(fun(A) ->
        none = probe(C, A),
        <<"NoneType">> = probe_type(C, A)
    end, [undefined, nil, none]),
    stop(C).

test_encoding_python_types(Config) ->
    C = new_ctx(Config),
    <<"tuple">> = probe_type(C, {1, 2}),
    <<"int">> = probe_type(C, 42),
    <<"float">> = probe_type(C, 1.5),
    stop(C).

%%% ============================================================================
%%% Python threads calling Erlang (py_thread_callback_SUITE)
%%% ============================================================================

test_threads_call_erlang(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"vm_tid">>, fun([T, I]) -> T * 1000 + I end),
    {ok, {<<"ok">>, true, 40}} = py_context:call(C, ?TEST_MOD, thread_calls, [<<"vm_tid">>, 4, 10], #{}, 30000),
    py_callback:unregister(<<"vm_tid">>),
    stop(C).

test_threadpool_calls(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"vm_double">>, fun([X]) -> X * 2 end),
    {ok, true} = py_context:call(C, ?TEST_MOD, pool_calls, [<<"vm_double">>, 8, 200], #{}, 30000),
    py_callback:unregister(<<"vm_double">>),
    stop(C).

test_threadpool_error(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"vm_fail">>, fun(_) -> throw(deliberate) end),
    {ok, <<"RuntimeError">>} = py_context:call(C, ?TEST_MOD, pool_error, [<<"vm_fail">>]),
    py_callback:unregister(<<"vm_fail">>),
    stop(C).

%% @doc From a pool thread: two erlang.call round trips nested in one
%% expression. (A callback that re-enters the context itself would deadlock
%% in every mode: the main thread is busy waiting on the pool.)
test_threadpool_nested(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"vm_nested">>, fun([X]) -> X + 11 end),
    {ok, 42} = py_context:call(C, ?TEST_MOD, pool_nested, [<<"vm_nested">>], #{}, 30000),
    py_callback:unregister(<<"vm_nested">>),
    stop(C).

test_threads_high_concurrency(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"vm_tid">>, fun([T, I]) -> T * 1000 + I end),
    {ok, {<<"ok">>, true, 1600}} = py_context:call(C, ?TEST_MOD, thread_calls, [<<"vm_tid">>, 32, 50], #{}, 60000),
    py_callback:unregister(<<"vm_tid">>),
    stop(C).

%%% ============================================================================
%%% Actor-style state (py_actor_SUITE)
%%% ============================================================================

test_counter_actor(Config) ->
    C = new_ctx(Config),
    lists:foreach(fun(I) ->
        {ok, I} = py_context:call(C, ?TEST_MOD, counter_increment, [])
    end, lists:seq(1, 100)),
    {ok, 110} = py_context:call(C, ?TEST_MOD, counter_increment, [10]),
    {ok, 110} = py_context:call(C, ?TEST_MOD, counter_value, []),
    stop(C).

test_state_reset_on_restart(Config) ->
    C1 = new_ctx(Config),
    {ok, V0} = py_context:call(C1, ?TEST_MOD, counter_value, []),
    V1 = V0 + 1,
    V2 = V0 + 2,
    {ok, V1} = py_context:call(C1, ?TEST_MOD, counter_increment, []),
    {ok, V2} = py_context:call(C1, ?TEST_MOD, counter_increment, []),
    stop(C1),
    C2 = new_ctx(Config),
    %% A new context: worker mode shares the interpreter, so the module
    %% state persists; isolated mode starts a new process and it does not.
    {ok, V} = py_context:call(C2, ?TEST_MOD, counter_value, []),
    case ?config(mode, Config) of
        isolated -> 0 = V;
        worker -> V2 = V
    end,
    stop(C2).

test_state_isolated_between_contexts(Config) ->
    C1 = new_ctx(Config),
    C2 = new_ctx(Config),
    ok = py_context:exec(C1, <<"who = 'one'">>),
    ok = py_context:exec(C2, <<"who = 'two'">>),
    {ok, <<"one">>} = py_context:eval(C1, <<"who">>),
    {ok, <<"two">>} = py_context:eval(C2, <<"who">>),
    stop(C1),
    stop(C2).

%%% ============================================================================
%%% Message flow both ways
%%% ============================================================================

%% @doc 1000 rounds of Erlang -> Python -> Erlang fun -> Python -> Erlang,
%% ordering asserted and no message left behind.
test_ping_pong(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"vm_incr">>, fun([X]) -> X + 1 end),
    {ok, 1000} = py_context:call(C, ?TEST_MOD, ping_pong, [<<"vm_incr">>, 1000], #{}, 60000),
    py_callback:unregister(<<"vm_incr">>),
    receive Any -> ct:fail({unexpected_message, Any}) after 0 -> ok end,
    stop(C).

%% @doc Erlang feeds terms to the child through a callback it polls.
test_feed_through_callback(Config) ->
    C = new_ctx(Config),
    Feeder = spawn_link(fun() -> feeder(lists:seq(1, 200)) end),
    py_callback:register(<<"vm_next">>, fun([]) ->
        Feeder ! {next, self()},
        receive {item, I} -> I after 5000 -> none end
    end),
    {ok, Items} = py_context:call(C, ?TEST_MOD, poll_feed, [<<"vm_next">>, 200], #{}, 60000),
    Expected = lists:seq(1, 200),
    Expected = Items,
    py_callback:unregister(<<"vm_next">>),
    Feeder ! stop,
    stop(C).

feeder([]) ->
    receive stop -> ok; {next, From} -> From ! {item, none}, feeder([]) end;
feeder([H | T] = L) ->
    receive
        stop -> ok;
        {next, From} -> From ! {item, H}, feeder(T)
    after 10000 ->
        feeder(L)
    end.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

probe(C, Value) ->
    py_callback:register(<<"vm_probe">>, fun(_) -> Value end),
    {ok, Got} = py_context:call(C, ?TEST_MOD, callback, [<<"vm_probe">>]),
    py_callback:unregister(<<"vm_probe">>),
    Got.

probe_type(C, Value) ->
    py_callback:register(<<"vm_probe">>, fun(_) -> Value end),
    {ok, Type} = py_context:call(C, ?TEST_MOD, callback_type, [<<"vm_probe">>]),
    py_callback:unregister(<<"vm_probe">>),
    Type.

collect_items(Acc) ->
    receive
        {<<"item">>, I} -> collect_items([I | Acc]);
        <<"done">> -> lists:reverse(Acc)
    after 5000 ->
        ct:fail({incomplete, length(Acc)})
    end.

new_ctx(Config) ->
    Mode = ?config(mode, Config),
    TestDir = ?config(test_dir, Config),
    {ok, C} = py_context:new(#{mode => Mode, paths => [TestDir]}),
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
