%%% @doc Common Test suite for asyncio compatibility validation.
%%%
%%% This suite runs Python unittest tests that verify ErlangEventLoop
%%% is a full drop-in replacement for asyncio's default event loop.
%%%
%%% The tests run within the Erlang VM via py:call() to validate the
%%% complete integration path:
%%%   Python test -> ErlangEventLoop -> py_event_loop NIF -> BEAM scheduler
%%%
%%% Architecture:
%%%   py:call() invokes async_test_runner.run_tests()
%%%       │
%%%       └─→ erlang.run(run_all())
%%%               │
%%%               └─→ ErlangEventLoop._run_once()
%%%                       ├─ Polls Erlang scheduler
%%%                       └─ Dispatches timer callbacks
%%%
%%% The key insight is that the async test runner uses erlang.run() to
%%% properly integrate with Erlang's timer system. This allows timers
%%% scheduled via call_later() to fire correctly, unlike the synchronous
%%% unittest runner which would block the event loop.
%%%
%%% Tests are adapted from uvloop's test suite and run against both:
%%% - ErlangEventLoop (Erlang-backed asyncio event loop)
%%% - AIOTestCase (standard asyncio for comparison)
-module(py_asyncio_compat_SUITE).

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

%% Erlang tests (ErlangEventLoop)
-export([
    test_base_erlang/1,
    test_sockets_erlang/1,
    test_tcp_erlang/1,
    test_udp_erlang/1,
    test_unix_erlang/1,
    test_dns_erlang/1,
    test_executors_erlang/1,
    test_context_erlang/1,
    test_signals_erlang/1,
    test_process_erlang/1,
    test_erlang_api/1
]).

%% Asyncio comparison tests (standard asyncio)
-export([
    test_base_asyncio/1,
    test_sockets_asyncio/1,
    test_tcp_asyncio/1,
    test_udp_asyncio/1,
    test_unix_asyncio/1,
    test_dns_asyncio/1,
    test_executors_asyncio/1,
    test_context_asyncio/1,
    test_signals_asyncio/1,
    test_process_asyncio/1
]).

%% ============================================================================
%% CT Callbacks
%% ============================================================================

all() ->
    [{group, erlang_tests}, {group, comparison_tests}].

groups() ->
    [
        {erlang_tests, [sequence], [
            test_base_erlang,
            test_sockets_erlang,
            test_tcp_erlang,
            test_udp_erlang,
            test_unix_erlang,
            test_dns_erlang,
            test_executors_erlang,
            test_context_erlang,
            test_signals_erlang,
            test_process_erlang,
            test_erlang_api
        ]},
        {comparison_tests, [sequence], [
            test_base_asyncio,
            test_sockets_asyncio,
            test_tcp_asyncio,
            test_udp_asyncio,
            test_unix_asyncio,
            test_dns_asyncio,
            test_executors_asyncio,
            test_context_asyncio,
            test_signals_asyncio,
            test_process_asyncio
        ]}
    ].

init_per_suite(Config) ->
    case application:ensure_all_started(erlang_python) of
        {ok, _} ->
            {ok, _} = py:start_contexts(),
            %% Wait for event loop to be fully initialized
            case wait_for_event_loop(5000) of
                ok ->
                    %% Set up Python path for tests
                    PrivDir = code:priv_dir(erlang_python),
                    ok = setup_python_path(PrivDir),
                    [{priv_dir, PrivDir} | Config];
                {error, Reason} ->
                    ct:fail({event_loop_not_ready, Reason})
            end;
        {error, {App, Reason}} ->
            ct:fail({failed_to_start, App, Reason})
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Erlang Event Loop Tests
%% ============================================================================

test_base_erlang(Config) ->
    run_erlang_tests("tests.test_base", Config).

test_sockets_erlang(Config) ->
    run_erlang_tests("tests.test_sockets", Config).

test_tcp_erlang(Config) ->
    run_erlang_tests("tests.test_tcp", Config).

test_udp_erlang(Config) ->
    run_erlang_tests("tests.test_udp", Config).

test_unix_erlang(Config) ->
    case os:type() of
        {unix, _} ->
            run_erlang_tests("tests.test_unix", Config);
        _ ->
            {skip, "Unix sockets not available on this platform"}
    end.

test_dns_erlang(Config) ->
    run_erlang_tests("tests.test_dns", Config).

test_executors_erlang(Config) ->
    run_erlang_tests("tests.test_executors", Config).

test_context_erlang(Config) ->
    run_erlang_tests("tests.test_context", Config).

test_signals_erlang(Config) ->
    case os:type() of
        {unix, _} ->
            run_erlang_tests("tests.test_signals", Config);
        _ ->
            {skip, "Signal tests not available on this platform"}
    end.

test_process_erlang(Config) ->
    run_erlang_tests("tests.test_process", Config).

test_erlang_api(Config) ->
    %% test_erlang_api has only Erlang-specific tests, run all
    run_python_tests("tests.test_erlang_api", <<"*">>, Config).

%% ============================================================================
%% Asyncio Comparison Tests (standard asyncio)
%% ============================================================================

test_base_asyncio(Config) ->
    run_asyncio_tests("tests.test_base", Config).

test_sockets_asyncio(Config) ->
    run_asyncio_tests("tests.test_sockets", Config).

test_tcp_asyncio(Config) ->
    run_asyncio_tests("tests.test_tcp", Config).

test_udp_asyncio(Config) ->
    run_asyncio_tests("tests.test_udp", Config).

test_unix_asyncio(Config) ->
    case os:type() of
        {unix, _} ->
            run_asyncio_tests("tests.test_unix", Config);
        _ ->
            {skip, "Unix sockets not available on this platform"}
    end.

test_dns_asyncio(Config) ->
    run_asyncio_tests("tests.test_dns", Config).

test_executors_asyncio(Config) ->
    run_asyncio_tests("tests.test_executors", Config).

test_context_asyncio(Config) ->
    run_asyncio_tests("tests.test_context", Config).

test_signals_asyncio(Config) ->
    case os:type() of
        {unix, _} ->
            run_asyncio_tests("tests.test_signals", Config);
        _ ->
            {skip, "Signal tests not available on this platform"}
    end.

test_process_asyncio(Config) ->
    run_asyncio_tests("tests.test_process", Config).

%% ============================================================================
%% Internal Functions
%% ============================================================================

%% Wait for the event loop to be fully initialized
wait_for_event_loop(Timeout) when Timeout =< 0 ->
    {error, timeout};
wait_for_event_loop(Timeout) ->
    case py_event_loop:get_loop() of
        {ok, LoopRef} when is_reference(LoopRef) ->
            %% Verify the event loop is actually functional
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

%% Set up Python path for test discovery
setup_python_path(PrivDir) ->
    Ctx = py:context(1),
    PrivDirBin = to_binary(PrivDir),
    %% Add priv directory to Python path and change working directory
    %% Use exec with inline Python code for path manipulation
    ok = py:exec(Ctx, <<"import sys, os">>),
    {ok, _} = py:call(Ctx, os, chdir, [PrivDirBin]),
    %% Insert the priv directory at the front of sys.path if not present
    Code = <<"sys.path.insert(0, '", PrivDirBin/binary, "') if '",
             PrivDirBin/binary, "' not in sys.path else None">>,
    {ok, _} = py:eval(Ctx, Code),
    ok.

%% Run Erlang event loop tests (TestErlang* classes)
run_erlang_tests(Module, Config) ->
    run_python_tests(Module, <<"TestErlang*">>, Config).

%% Run asyncio comparison tests (TestAIO* classes)
run_asyncio_tests(Module, Config) ->
    run_python_tests(Module, <<"TestAIO*">>, Config).

%% Run Python unittest tests using the async_test_runner module
%% The async runner uses erlang.run() to properly integrate with
%% Erlang's timer system, allowing timers scheduled via call_later()
%% to fire correctly during test execution.
run_python_tests(Module, Pattern, _Config) ->
    Ctx = py:context(1),
    ModuleBin = to_binary(Module),
    %% Per-test timeout in seconds (30 seconds per individual test)
    PerTestTimeout = 30.0,

    %% Use 10 minute timeout for overall test execution
    case py:call(Ctx, 'tests.async_test_runner', run_tests, [ModuleBin, Pattern, PerTestTimeout], #{timeout => 600000}) of
        {ok, Results} ->
            handle_test_results(Module, Pattern, Results);
        {error, Reason} ->
            ct:log("Python execution error for ~s (~s): ~p", [Module, Pattern, Reason]),
            ct:fail({python_error, Module, Reason})
    end.

%% Handle Python test results
handle_test_results(Module, Pattern, Results) ->
    TestsRun = maps:get(<<"tests_run">>, Results, 0),
    Failures = maps:get(<<"failures">>, Results, 0),
    Errors = maps:get(<<"errors">>, Results, 0),
    Skipped = maps:get(<<"skipped">>, Results, 0),
    Success = maps:get(<<"success">>, Results, false),
    Output = maps:get(<<"output">>, Results, <<>>),
    FailureDetails = maps:get(<<"failure_details">>, Results, []),

    %% Log the test output
    ct:log("~s (~s): ~p tests run, ~p failures, ~p errors, ~p skipped~n~n~s",
           [Module, Pattern, TestsRun, Failures, Errors, Skipped, Output]),

    case Success of
        true ->
            ct:log("~s (~s): All ~p tests passed", [Module, Pattern, TestsRun]),
            ok;
        false ->
            %% Log detailed failure information
            lists:foreach(
                fun(Detail) ->
                    Test = maps:get(<<"test">>, Detail, <<"unknown">>),
                    Trace = maps:get(<<"traceback">>, Detail, <<>>),
                    ct:log("FAILED: ~s~n~s", [Test, Trace])
                end,
                FailureDetails
            ),
            ct:fail({tests_failed, Module, Pattern, #{
                tests_run => TestsRun,
                failures => Failures,
                errors => Errors,
                skipped => Skipped
            }})
    end.

%% Convert to binary
to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L) -> list_to_binary(L);
to_binary(A) when is_atom(A) -> atom_to_binary(A, utf8).
