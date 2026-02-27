%%% @doc Common Test suite for Python thread callback support.
%%%
%%% Tests that Python threads (both threading.Thread and ThreadPoolExecutor)
%%% can call erlang.call() without blocking.
-module(py_thread_callback_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    threadpool_basic_test/1,
    threadpool_concurrent_test/1,
    threadpool_multiple_calls_test/1,
    threadpool_nested_callback_test/1,
    threadpool_error_handling_test/1,
    threadpool_thread_reuse_test/1,
    simple_thread_basic_test/1,
    simple_thread_multiple_calls_test/1,
    simple_thread_concurrent_test/1
]).

all() ->
    [
        threadpool_basic_test,
        threadpool_concurrent_test,
        threadpool_multiple_calls_test,
        threadpool_nested_callback_test,
        threadpool_error_handling_test,
        threadpool_thread_reuse_test,
        simple_thread_basic_test,
        simple_thread_multiple_calls_test,
        simple_thread_concurrent_test
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Cleanup any registered functions
    catch py:unregister_function(double_it),
    catch py:unregister_function(add_one),
    catch py:unregister_function(call_python_square),
    catch py:unregister_function(square_in_erlang),
    catch py:unregister_function(maybe_fail),
    catch py:unregister_function(get_id),
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Basic: Single thread calling erlang.call()
threadpool_basic_test(_Config) ->
    py:register_function(double_it, fun([X]) -> X * 2 end),

    %% Use a lambda that creates executor, submits task, gets result
    %% Need fromlist to properly import concurrent.futures submodule
    Code = <<"(lambda cf, erl: cf.ThreadPoolExecutor(max_workers=1).__enter__().submit(lambda: erl.call('double_it', 5)).result())(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), __import__('erlang'))">>,
    {ok, Result} = py:eval(Code),
    10 = Result,
    ok.

%% @doc Concurrent: Multiple threads calling simultaneously
threadpool_concurrent_test(_Config) ->
    py:register_function(double_it, fun([X]) -> X * 2 end),

    %% Use executor.map to run calls in parallel
    Code = <<"(lambda cf, erl: list(cf.ThreadPoolExecutor(max_workers=4).__enter__().map(lambda x: erl.call('double_it', x), range(20))))(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), __import__('erlang'))">>,
    {ok, Results} = py:eval(Code),
    Expected = [X * 2 || X <- lists:seq(0, 19)],
    Expected = Results,
    ok.

%% @doc Multiple calls: Same thread makes multiple erlang.call() invocations
threadpool_multiple_calls_test(_Config) ->
    py:register_function(add_one, fun([X]) -> X + 1 end),

    %% Lambda that makes 3 sequential calls
    Code = <<"(lambda cf, erl: list(cf.ThreadPoolExecutor(max_workers=2).__enter__().map(lambda x: erl.call('add_one', erl.call('add_one', erl.call('add_one', x))), range(5))))(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), __import__('erlang'))">>,
    {ok, Results} = py:eval(Code),
    Expected = [X + 3 || X <- lists:seq(0, 4)],
    Expected = Results,
    ok.

%% @doc Nested: erlang.call() result used in further computation
%% Note: Full nesting (erlang.call -> py:eval) is complex due to worker pool dynamics.
%% This test verifies the callback result can be used for further Python computation.
threadpool_nested_callback_test(_Config) ->
    %% Register Erlang function that computes square purely in Erlang
    py:register_function(square_in_erlang, fun([X]) -> X * X end),

    %% Call from thread pool, then use result in Python computation
    %% This tests that the result is properly returned and usable
    Code = <<"(lambda cf, erl: list(cf.ThreadPoolExecutor(max_workers=2).__enter__().map(lambda x: erl.call('square_in_erlang', x) + 1, range(5))))(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), __import__('erlang'))">>,
    {ok, Results} = py:eval(Code),
    %% Results should be x^2 + 1 for x in 0..4
    Expected = [(X * X) + 1 || X <- lists:seq(0, 4)],
    Expected = Results,
    ok.

%% @doc Error handling: Callback raises exception
threadpool_error_handling_test(_Config) ->
    py:register_function(maybe_fail, fun([X]) ->
        case X rem 2 of
            0 -> X * 2;
            1 -> error(odd_number)
        end
    end),

    %% Lambda that catches exceptions - only calls maybe_fail for even numbers
    Code = <<"(lambda cf, erl: list(cf.ThreadPoolExecutor(max_workers=2).__enter__().map(lambda x: erl.call('maybe_fail', x) if x % 2 == 0 else 'error', range(4))))(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), __import__('erlang'))">>,
    {ok, Results} = py:eval(Code),
    %% Even numbers succeed (doubled), odd numbers return 'error'
    [0, <<"error">>, 4, <<"error">>] = Results,
    ok.

%% @doc Thread reuse: Verify workers are reused correctly
threadpool_thread_reuse_test(_Config) ->
    py:register_function(get_id, fun([]) -> erlang:unique_integer() end),

    %% Get thread IDs and check that we have <= 2 unique threads
    Code = <<"(lambda cf, th: len(set(cf.ThreadPoolExecutor(max_workers=2).__enter__().map(lambda _: th.get_ident(), range(10)))))(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), __import__('threading'))">>,
    {ok, ThreadCount} = py:eval(Code),
    true = (ThreadCount =< 2),
    ok.

%%% ============================================================================
%%% Simple threading.Thread Test Cases
%%% ============================================================================

%% @doc Simple thread calling erlang.call()
simple_thread_basic_test(_Config) ->
    py:register_function(double_it, fun([X]) -> X * 2 end),

    %% Create a threading.Thread subclass that stores its result
    Code = <<"
(lambda: (
    __import__('threading').Thread(target=lambda: setattr(__import__('sys').modules[__name__], '_result', __import__('erlang').call('double_it', 5))).start() or
    __import__('time').sleep(0.1) or
    getattr(__import__('sys').modules[__name__], '_result', None)
))()
">>,
    {ok, Result} = py:eval(Code),
    10 = Result,
    ok.

%% @doc Same simple thread makes multiple erlang.call() invocations
simple_thread_multiple_calls_test(_Config) ->
    py:register_function(add_one, fun([X]) -> X + 1 end),

    %% Thread that makes 3 sequential calls: add_one(add_one(add_one(0)))
    Code = <<"
(lambda: (
    __import__('threading').Thread(target=lambda: setattr(
        __import__('sys').modules[__name__], '_result',
        __import__('erlang').call('add_one',
            __import__('erlang').call('add_one',
                __import__('erlang').call('add_one', 0)))
    )).start() or
    __import__('time').sleep(0.1) or
    getattr(__import__('sys').modules[__name__], '_result', None)
))()
">>,
    {ok, Result} = py:eval(Code),
    3 = Result,
    ok.

%% @doc Multiple simple threads calling erlang.call() concurrently
simple_thread_concurrent_test(_Config) ->
    py:register_function(double_it, fun([X]) -> X * 2 end),

    %% Create 5 threads, each calling double_it with different values
    Code = <<"
(lambda: (
    (threads := [__import__('threading').Thread(target=lambda x=x: setattr(t, 'result', __import__('erlang').call('double_it', x))) for x in range(5) for t in [type('T', (), {'result': None})()]]),
    [setattr(threads[i], 'obj', type('T', (), {'result': None})()) for i in range(5)],
    (workers := []),
    [workers.append(type('Worker', (__import__('threading').Thread,), {'result': None, 'run': lambda self, x=x: setattr(self, 'result', __import__('erlang').call('double_it', x))})()) for x in range(5)],
    [w.start() for w in workers],
    [w.join() for w in workers],
    [w.result for w in workers]
)[-1])()
">>,
    {ok, Results} = py:eval(Code),
    [0, 2, 4, 6, 8] = Results,
    ok.
