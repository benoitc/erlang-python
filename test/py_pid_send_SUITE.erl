%%% @doc Common Test suite for PID serialization, erlang.send(), and
%%% SuspensionRequired BaseException inheritance.
%%%
%%% Tests three new erlang_python features:
%%% 1. Erlang PIDs serialize to erlang.Pid objects in Python and back
%%% 2. erlang.send(pid, term) for fire-and-forget message passing
%%% 3. SuspensionRequired inherits from BaseException (not Exception)
-module(py_pid_send_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_pid_type_in_python/1,
    test_pid_isinstance_check/1,
    test_pid_round_trip/1,
    test_pid_equality/1,
    test_pid_hash/1,
    test_pid_inequality/1,
    test_send_simple_message/1,
    test_send_multiple_messages/1,
    test_send_complex_term/1,
    test_send_bad_pid_raises/1,
    test_suspension_not_caught_by_except/1,
    test_suspension_is_base_exception/1,
    test_suspension_not_subclass_of_exception/1,
    test_send_to_dead_process/1,
    test_send_dead_process_raises_process_error/1,
    test_process_error_is_exception_subclass/1,
    test_pid_in_complex_structure/1
]).

all() ->
    [
        test_pid_type_in_python,
        test_pid_isinstance_check,
        test_pid_round_trip,
        test_pid_equality,
        test_pid_hash,
        test_pid_inequality,
        test_send_simple_message,
        test_send_multiple_messages,
        test_send_complex_term,
        test_send_bad_pid_raises,
        test_suspension_not_caught_by_except,
        test_suspension_is_base_exception,
        test_suspension_not_subclass_of_exception,
        test_send_to_dead_process,
        test_send_dead_process_raises_process_error,
        test_process_error_is_exception_subclass,
        test_pid_in_complex_structure
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    %% Add test directory to Python path
    TestDir = code:lib_dir(erlang_python, test),
    ok = py:exec(iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    catch py:unregister_function(test_pid_echo),
    ok.

%%% ============================================================================
%%% PID Serialization Tests
%%% ============================================================================

%% @doc Verify that an Erlang PID arrives in Python as an erlang.Pid object.
test_pid_type_in_python(_Config) ->
    {ok, <<"Pid">>} = py:call(py_test_pid_send, get_pid_type, [self()]),
    ok.

%% @doc Verify isinstance(pid, erlang.Pid) returns True.
test_pid_isinstance_check(_Config) ->
    {ok, true} = py:call(py_test_pid_send, is_erlang_pid, [self()]),
    ok.

%% @doc Verify a PID round-trips: Erlang -> Python -> Erlang stays a real PID.
test_pid_round_trip(_Config) ->
    Pid = self(),
    {ok, ReturnedPid} = py:call(py_test_pid_send, round_trip_pid, [Pid]),
    Pid = ReturnedPid,
    ok.

%% @doc Verify two erlang.Pid wrapping the same PID are equal.
test_pid_equality(_Config) ->
    Pid = self(),
    {ok, true} = py:call(py_test_pid_send, pid_equality, [Pid, Pid]),
    ok.

%% @doc Verify equal PIDs produce the same hash (usable in sets/dicts).
test_pid_hash(_Config) ->
    Pid = self(),
    {ok, true} = py:call(py_test_pid_send, pid_hash_equal, [Pid, Pid]),
    ok.

%% @doc Verify two different PIDs are not equal.
test_pid_inequality(_Config) ->
    Pid1 = self(),
    Pid2 = spawn(fun() -> receive stop -> ok end end),
    {ok, true} = py:call(py_test_pid_send, pid_inequality, [Pid1, Pid2]),
    Pid2 ! stop,
    ok.

%%% ============================================================================
%%% erlang.send() Tests
%%% ============================================================================

%% @doc Test erlang.send(pid, term) delivers a simple message.
test_send_simple_message(_Config) ->
    Pid = self(),
    {ok, true} = py:call(py_test_pid_send, send_message, [Pid, <<"hello">>]),
    receive
        <<"hello">> -> ok
    after 5000 ->
        ct:fail(timeout_waiting_for_message)
    end.

%% @doc Test sending multiple messages in sequence.
test_send_multiple_messages(_Config) ->
    Pid = self(),
    {ok, 3} = py:call(py_test_pid_send, send_multiple,
                       [Pid, [<<"one">>, <<"two">>, <<"three">>]]),
    receive <<"one">> -> ok after 5000 -> ct:fail(timeout_msg_1) end,
    receive <<"two">> -> ok after 5000 -> ct:fail(timeout_msg_2) end,
    receive <<"three">> -> ok after 5000 -> ct:fail(timeout_msg_3) end.

%% @doc Test sending a complex compound term.
test_send_complex_term(_Config) ->
    Pid = self(),
    {ok, true} = py:call(py_test_pid_send, send_complex_term, [Pid]),
    receive
        {<<"hello">>, 42, [1, 2, 3], #{<<"key">> := <<"value">>}, true} -> ok
    after 5000 ->
        ct:fail(timeout_waiting_for_complex_message)
    end.

%% @doc Verify erlang.send() with a non-PID raises TypeError.
test_send_bad_pid_raises(_Config) ->
    {ok, true} = py:call(py_test_pid_send, send_bad_pid, []),
    ok.

%% @doc Test sending to a process that has already exited.
test_send_to_dead_process(_Config) ->
    %% Spawn a process that exits immediately
    Pid = spawn(fun() -> ok end),
    timer:sleep(100), %% ensure it's dead
    %% erlang.send() to a dead PID should fail (enif_send returns 0)
    {error, _} = py:call(py_test_pid_send, send_message, [Pid, <<"dead">>]),
    ok.

%% @doc Verify sending to a dead process raises erlang.ProcessError (not RuntimeError).
test_send_dead_process_raises_process_error(_Config) ->
    Pid = spawn(fun() -> ok end),
    timer:sleep(100),
    {ok, true} = py:call(py_test_pid_send, send_dead_process_raises_process_error, [Pid]),
    ok.

%% @doc Verify erlang.ProcessError is a subclass of Exception.
test_process_error_is_exception_subclass(_Config) ->
    {ok, true} = py:call(py_test_pid_send, process_error_is_exception_subclass, []),
    ok.

%%% ============================================================================
%%% PID in Complex Structures
%%% ============================================================================

%% @doc Test PID embedded in lists and maps round-trips correctly.
test_pid_in_complex_structure(_Config) ->
    Pid = self(),
    %% Pass PID inside a list through Python and get it back
    {ok, [ReturnedPid]} = py:call(py_test_pid_send, round_trip_pid, [[Pid]]),
    Pid = ReturnedPid,
    %% Pass PID inside a map
    {ok, #{<<"pid">> := ReturnedPid2}} = py:call(py_test_pid_send,
        round_trip_pid, [#{<<"pid">> => Pid}]),
    Pid = ReturnedPid2,
    ok.

%%% ============================================================================
%%% SuspensionRequired BaseException Tests
%%% ============================================================================

%% @doc Verify SuspensionRequired is NOT caught by `except Exception`.
%% This is the key behavior change: ASGI apps with `except Exception`
%% handlers will no longer intercept the suspension mechanism.
test_suspension_not_caught_by_except(_Config) ->
    %% Register a simple callback
    py:register_function(test_pid_echo, fun([X]) -> X end),
    %% Call Python function that wraps erlang.call in try/except Exception
    {ok, {<<"ok">>, 42}} = py:call(py_test_pid_send,
                                    suspension_not_caught_by_except_exception, []),
    ok.

%% @doc Verify SuspensionRequired IS a subclass of BaseException.
test_suspension_is_base_exception(_Config) ->
    {ok, true} = py:call(py_test_pid_send, suspension_caught_by_except_base, []),
    ok.

%% @doc Verify SuspensionRequired is NOT a subclass of Exception.
test_suspension_not_subclass_of_exception(_Config) ->
    {ok, true} = py:call(py_test_pid_send, suspension_not_subclass_of_exception, []),
    ok.
