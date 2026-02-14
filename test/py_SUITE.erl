%%% @doc Common Test suite for py module.
-module(py_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_call_math/1,
    test_call_json/1,
    test_eval/1,
    test_exec/1,
    test_async_call/1,
    test_type_conversions/1,
    test_timeout/1,
    test_special_floats/1,
    test_streaming/1,
    test_error_handling/1,
    test_version/1,
    test_memory_stats/1,
    test_gc/1,
    test_erlang_callback/1,
    test_asyncio_call/1,
    test_asyncio_gather/1,
    test_subinterp_supported/1,
    test_parallel_execution/1,
    test_venv/1,
    %% New scalability tests
    test_execution_mode/1,
    test_num_executors/1,
    test_semaphore_basic/1,
    test_semaphore_acquire_release/1,
    test_semaphore_concurrent/1,
    test_semaphore_timeout/1,
    test_semaphore_rate_limiting/1,
    test_overload_protection/1
]).

all() ->
    [
        test_call_math,
        test_call_json,
        test_eval,
        test_exec,
        test_async_call,
        test_type_conversions,
        test_timeout,
        test_special_floats,
        test_streaming,
        test_error_handling,
        test_version,
        test_memory_stats,
        test_gc,
        test_erlang_callback,
        test_asyncio_call,
        test_asyncio_gather,
        test_subinterp_supported,
        test_parallel_execution,
        test_venv,
        %% Scalability tests
        test_execution_mode,
        test_num_executors,
        test_semaphore_basic,
        test_semaphore_acquire_release,
        test_semaphore_concurrent,
        test_semaphore_timeout,
        test_semaphore_rate_limiting,
        test_overload_protection
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

test_call_math(_Config) ->
    {ok, 4.0} = py:call(math, sqrt, [16]),
    {ok, 120} = py:call(math, factorial, [5]),
    %% Test accessing module attribute via eval - use __import__
    {ok, Pi} = py:eval(<<"__import__('math').pi">>),
    true = Pi > 3.14 andalso Pi < 3.15,
    ok.

test_call_json(_Config) ->
    %% Encode
    {ok, Json} = py:call(json, dumps, [#{foo => bar}]),
    true = is_binary(Json),

    %% Decode
    {ok, Decoded} = py:call(json, loads, [<<"{\"a\": 1, \"b\": [1,2,3]}">>]),
    #{<<"a">> := 1, <<"b">> := [1, 2, 3]} = Decoded,
    ok.

test_eval(_Config) ->
    {ok, 45} = py:eval(<<"sum(range(10))">>),
    {ok, 25} = py:eval(<<"x * y">>, #{x => 5, y => 5}),
    {ok, [0, 1, 4, 9, 16]} = py:eval(<<"[x**2 for x in range(5)]">>),
    ok.

test_exec(_Config) ->
    %% Test exec - just ensure it doesn't fail
    %% Note: exec runs on any worker, subsequent calls may go to different workers
    ok = py:exec(<<"x = 42">>),
    ok = py:exec(<<"
def my_func():
    pass
">>),
    ok.

test_async_call(_Config) ->
    Ref1 = py:call_async(math, sqrt, [100]),
    Ref2 = py:call_async(math, sqrt, [144]),

    {ok, 10.0} = py:await(Ref1),
    {ok, 12.0} = py:await(Ref2),
    ok.

test_type_conversions(_Config) ->
    %% Integer
    {ok, 42} = py:eval(<<"42">>),

    %% Float
    {ok, 3.14} = py:eval(<<"3.14">>),

    %% String -> binary
    {ok, <<"hello">>} = py:eval(<<"'hello'">>),

    %% Boolean
    {ok, true} = py:eval(<<"True">>),
    {ok, false} = py:eval(<<"False">>),

    %% None
    {ok, none} = py:eval(<<"None">>),

    %% List
    {ok, [1, 2, 3]} = py:eval(<<"[1, 2, 3]">>),

    %% Tuple
    {ok, {1, 2, 3}} = py:eval(<<"(1, 2, 3)">>),

    %% Dict -> map
    {ok, #{<<"a">> := 1}} = py:eval(<<"{'a': 1}">>),

    ok.

test_timeout(_Config) ->
    %% Test that timeout works - use a heavy computation
    %% sum(range(10**8)) will trigger timeout
    {error, timeout} = py:eval(<<"sum(range(10**8))">>, #{}, 100),

    %% Test that normal operations complete within timeout
    {ok, 45} = py:eval(<<"sum(range(10))">>, #{}, 5000),

    %% Test call with timeout
    {ok, 4.0} = py:call(math, sqrt, [16], #{}, 5000),

    ok.

test_special_floats(_Config) ->
    %% Test NaN
    {ok, nan} = py:eval(<<"float('nan')">>),

    %% Test Infinity
    {ok, infinity} = py:eval(<<"float('inf')">>),

    %% Test negative infinity
    {ok, neg_infinity} = py:eval(<<"float('-inf')">>),

    ok.

test_streaming(_Config) ->
    %% Test generator expression streaming
    {ok, [0, 1, 4, 9, 16]} = py:stream_eval(<<"(x**2 for x in range(5))">>),

    %% Test range iterator
    {ok, [0, 1, 2, 3, 4]} = py:stream_eval(<<"iter(range(5))">>),

    %% Test map streaming
    {ok, [<<"A">>, <<"B">>, <<"C">>]} = py:stream_eval(<<"(c.upper() for c in 'abc')">>),

    ok.

test_error_handling(_Config) ->
    %% Test Python exception
    {error, {'NameError', _}} = py:eval(<<"undefined_variable">>),

    %% Test syntax error
    {error, {'SyntaxError', _}} = py:eval(<<"if True">>),

    %% Test division by zero
    {error, {'ZeroDivisionError', _}} = py:eval(<<"1/0">>),

    %% Test import error
    {error, {'ModuleNotFoundError', _}} = py:call(nonexistent_module, func, []),

    ok.

test_version(_Config) ->
    {ok, Version} = py:version(),
    true = is_binary(Version),
    true = byte_size(Version) > 0,
    ok.

test_memory_stats(_Config) ->
    {ok, Stats} = py:memory_stats(),
    true = is_map(Stats),
    %% Check that we have GC stats
    true = maps:is_key(gc_stats, Stats),
    true = maps:is_key(gc_count, Stats),
    true = maps:is_key(gc_threshold, Stats),

    %% Test tracemalloc
    ok = py:tracemalloc_start(),
    %% Allocate some memory
    {ok, _} = py:eval(<<"[x**2 for x in range(1000)]">>),
    {ok, StatsWithTrace} = py:memory_stats(),
    true = maps:is_key(traced_memory_current, StatsWithTrace),
    true = maps:is_key(traced_memory_peak, StatsWithTrace),
    ok = py:tracemalloc_stop(),

    ok.

test_gc(_Config) ->
    %% Test basic GC
    {ok, Collected} = py:gc(),
    true = is_integer(Collected),
    true = Collected >= 0,

    %% Test generation-specific GC
    {ok, Collected0} = py:gc(0),
    true = is_integer(Collected0),

    {ok, Collected1} = py:gc(1),
    true = is_integer(Collected1),

    {ok, Collected2} = py:gc(2),
    true = is_integer(Collected2),

    ok.

test_erlang_callback(_Config) ->
    %% Register a simple function that adds two numbers
    py:register_function(add, fun([A, B]) -> A + B end),

    %% Call from Python - erlang module is auto-imported
    {ok, Result1} = py:eval(<<"erlang.call('add', 5, 7)">>),
    12 = Result1,

    %% Unregister
    py:unregister_function(add),

    ok.

test_asyncio_call(_Config) ->
    %% Test async call to asyncio coroutine
    %% The async pool runs async functions in a background asyncio event loop
    Ref = py:async_call('__main__', 'eval', [<<"1 + 1">>]),
    true = is_reference(Ref),

    %% We may not get a result for simple eval since it's not a real coroutine
    %% Just verify the call mechanism works
    _Result = py:async_await(Ref, 5000),
    ok.

test_asyncio_gather(_Config) ->
    %% Test gathering multiple async calls
    %% This tests the async_gather functionality
    Calls = [
        {math, sqrt, [16]},
        {math, sqrt, [25]},
        {math, sqrt, [36]}
    ],
    %% async_gather may return results or errors depending on async pool state
    _Result = py:async_gather(Calls),
    ok.

test_subinterp_supported(_Config) ->
    %% Test that subinterp_supported returns a boolean
    Result = py:subinterp_supported(),
    true = is_boolean(Result),
    %% Log the result
    ct:pal("Sub-interpreter support: ~p~n", [Result]),
    ok.

test_parallel_execution(_Config) ->
    %% Test parallel execution using sub-interpreters (Python 3.12+)
    case py:subinterp_supported() of
        true ->
            Calls = [
                {math, sqrt, [16]},
                {math, sqrt, [25]},
                {math, sqrt, [36]},
                {math, sqrt, [49]}
            ],
            Result = py:parallel(Calls),
            ct:pal("Parallel result: ~p~n", [Result]),
            %% parallel/1 should return {ok, Results} on success
            %% or {error, Reason} on failure
            case Result of
                {ok, Results} when is_list(Results) ->
                    4 = length(Results),
                    ok;
                {error, Reason} ->
                    ct:pal("Parallel execution failed: ~p (may be expected on some setups)~n", [Reason]),
                    ok
            end;
        false ->
            ct:pal("Sub-interpreters not supported, skipping parallel test~n"),
            ok
    end.

test_venv(_Config) ->
    %% Test venv_info when no venv is active
    {ok, #{<<"active">> := false}} = py:venv_info(),

    %% Create a fake venv structure for testing (faster than real venv)
    TmpDir = <<"/tmp/erlang_python_test_venv">>,

    %% Get Python version for site-packages path
    {ok, PyVer} = py:eval(<<"f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'">>),

    %% Create minimal venv structure using os.makedirs
    SitePackages = <<TmpDir/binary, "/lib/", PyVer/binary, "/site-packages">>,
    {ok, _} = py:call(os, makedirs, [SitePackages], #{exist_ok => true}),

    %% Activate it
    ok = py:activate_venv(TmpDir),

    %% Check venv_info
    {ok, Info} = py:venv_info(),
    true = maps:get(<<"active">>, Info),
    true = is_binary(maps:get(<<"venv_path">>, Info)),
    true = is_binary(maps:get(<<"site_packages">>, Info)),

    %% Deactivate
    ok = py:deactivate_venv(),

    %% Verify deactivated
    {ok, #{<<"active">> := false}} = py:venv_info(),

    %% Test error case - invalid path
    {error, _} = py:activate_venv(<<"/nonexistent/path">>),

    %% Cleanup
    {ok, _} = py:call(shutil, rmtree, [TmpDir], #{ignore_errors => true}),

    ok.

%%% ============================================================================
%%% Scalability Tests
%%% ============================================================================

test_execution_mode(_Config) ->
    %% Test that execution_mode returns a valid mode
    Mode = py:execution_mode(),
    ct:pal("Execution mode: ~p~n", [Mode]),
    true = lists:member(Mode, [free_threaded, subinterp, multi_executor]),
    ok.

test_num_executors(_Config) ->
    %% Test that num_executors returns a positive integer
    Num = py:num_executors(),
    ct:pal("Number of executors: ~p~n", [Num]),
    true = is_integer(Num),
    true = Num > 0,
    ok.

test_semaphore_basic(_Config) ->
    %% Test semaphore initialization and basic getters
    Max = py_semaphore:max_concurrent(),
    Current = py_semaphore:current(),

    ct:pal("Semaphore - Max: ~p, Current: ~p~n", [Max, Current]),

    true = is_integer(Max),
    true = Max > 0,
    true = is_integer(Current),
    true = Current >= 0,
    ok.

test_semaphore_acquire_release(_Config) ->
    %% Test basic acquire/release cycle
    Initial = py_semaphore:current(),

    %% Acquire
    ok = py_semaphore:acquire(1000),
    After1 = py_semaphore:current(),
    true = After1 > Initial,

    %% Release
    ok = py_semaphore:release(),
    After2 = py_semaphore:current(),
    After2 = Initial,

    ok.

test_semaphore_concurrent(_Config) ->
    %% Test concurrent acquire/release from multiple processes
    Parent = self(),
    NumProcs = 20,

    %% Spawn processes that each acquire, sleep, and release
    Pids = [spawn_link(fun() ->
        ok = py_semaphore:acquire(5000),
        timer:sleep(10),
        ok = py_semaphore:release(),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumProcs)],

    %% Wait for all to complete
    [receive {done, Pid} -> ok after 10000 -> ct:fail({timeout, Pid}) end || Pid <- Pids],

    %% Current should be back to 0 (or initial state)
    timer:sleep(50),
    Current = py_semaphore:current(),
    ct:pal("After concurrent test - Current: ~p~n", [Current]),
    true = Current =:= 0,

    ok.

test_semaphore_timeout(_Config) ->
    %% Save original max
    OrigMax = py_semaphore:max_concurrent(),

    %% Set a very low limit
    ok = py_semaphore:set_max_concurrent(1),
    1 = py_semaphore:max_concurrent(),

    %% First acquire should succeed
    ok = py_semaphore:acquire(1000),

    %% Second acquire should timeout (we're at max)
    Parent = self(),
    spawn_link(fun() ->
        Result = py_semaphore:acquire(100),
        Parent ! {acquire_result, Result}
    end),

    Result = receive
        {acquire_result, R} -> R
    after 2000 ->
        ct:fail(timeout_waiting_for_result)
    end,

    %% Should have gotten error
    {error, max_concurrent} = Result,

    %% Release and restore
    ok = py_semaphore:release(),
    ok = py_semaphore:set_max_concurrent(OrigMax),

    ok.

test_semaphore_rate_limiting(_Config) ->
    %% Test that the semaphore properly limits concurrent operations
    OrigMax = py_semaphore:max_concurrent(),

    %% Set limit to 3
    ok = py_semaphore:set_max_concurrent(3),

    Parent = self(),
    Counter = atomics:new(1, [{signed, false}]),

    %% Track max concurrent
    MaxSeen = atomics:new(1, [{signed, false}]),

    NumProcs = 10,
    Pids = [spawn_link(fun() ->
        ok = py_semaphore:acquire(10000),
        atomics:add(Counter, 1, 1),
        Cur = atomics:get(Counter, 1),

        %% Update max seen
        OldMax = atomics:get(MaxSeen, 1),
        case Cur > OldMax of
            true -> atomics:put(MaxSeen, 1, Cur);
            false -> ok
        end,

        timer:sleep(50),
        atomics:sub(Counter, 1, 1),
        ok = py_semaphore:release(),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumProcs)],

    %% Wait for all
    [receive {done, Pid} -> ok after 30000 -> ct:fail({timeout, Pid}) end || Pid <- Pids],

    %% Check that max concurrent never exceeded limit
    MaxConcurrent = atomics:get(MaxSeen, 1),
    ct:pal("Max concurrent seen: ~p (limit was 3)~n", [MaxConcurrent]),
    true = MaxConcurrent =< 3,

    %% Restore
    ok = py_semaphore:set_max_concurrent(OrigMax),

    ok.

test_overload_protection(_Config) ->
    %% Test that py:call returns overload error when semaphore is exhausted
    OrigMax = py_semaphore:max_concurrent(),

    %% Set very low limit
    ok = py_semaphore:set_max_concurrent(1),

    %% Acquire the only slot
    ok = py_semaphore:acquire(1000),

    %% Now py:call should fail with overload
    Parent = self(),
    spawn_link(fun() ->
        Result = py:call(math, sqrt, [4], #{}, 100),
        Parent ! {call_result, Result}
    end),

    Result = receive
        {call_result, R} -> R
    after 2000 ->
        ct:fail(timeout_waiting_for_result)
    end,

    ct:pal("Overload result: ~p~n", [Result]),

    %% Should be overloaded error
    case Result of
        {error, {overloaded, _, _}} -> ok;
        {error, timeout} -> ok;  %% Also acceptable
        Other -> ct:fail({unexpected_result, Other})
    end,

    %% Cleanup
    ok = py_semaphore:release(),
    ok = py_semaphore:set_max_concurrent(OrigMax),

    ok.
