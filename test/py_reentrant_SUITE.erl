%%% @doc Common Test suite for reentrant Python callbacks.
%%%
%%% Tests the suspension/resume mechanism that allows Python→Erlang→Python
%%% callbacks without deadlocking dirty schedulers.
-module(py_reentrant_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_basic_reentrant/1,
    test_nested_callbacks/1,
    test_callback_error_propagation/1,
    test_concurrent_reentrant/1,
    test_callback_with_complex_types/1,
    test_multiple_sequential_callbacks/1,
    test_call_from_non_worker_thread/1
]).

all() ->
    [
        test_basic_reentrant,
        test_nested_callbacks,
        test_callback_error_propagation,
        test_concurrent_reentrant,
        test_callback_with_complex_types,
        test_multiple_sequential_callbacks,
        test_call_from_non_worker_thread
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Cleanup any registered functions
    catch py:unregister_function(double_via_python),
    catch py:unregister_function(triple),
    catch py:unregister_function(call_level),
    catch py:unregister_function(may_fail),
    catch py:unregister_function(transform),
    catch py:unregister_function(add_ten),
    catch py:unregister_function(multiply_by_two),
    catch py:unregister_function(subtract_five),
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Test basic Python→Erlang→Python reentrant callback.
%% This tests the suspension/resume mechanism for a simple case.
test_basic_reentrant(_Config) ->
    %% Register an Erlang function that calls Python using py:eval
    %% (py:eval doesn't require pre-defined functions)
    py:register_function(double_via_python, fun([X]) ->
        Code = iolist_to_binary(io_lib:format("~p * 2", [X])),
        {ok, Result} = py:eval(Code),
        Result
    end),

    %% Test using py:eval: Python calls Erlang which calls Python
    %% The expression: call Erlang's double_via_python(10), then add 1
    {ok, Result} = py:eval(<<"__import__('erlang').call('double_via_python', 10) + 1">>),
    21 = Result,  %% 10 * 2 + 1 = 21

    ok.

%% @doc Test deeply nested callbacks (3+ levels).
%% Tests Erlang→Python→Erlang→Python... nesting
test_nested_callbacks(_Config) ->
    %% Register Erlang function that calls back into Python via py:eval
    %% Each level calls back to Erlang until we reach the target depth
    py:register_function(call_level, fun([Level, N]) ->
        case Level >= N of
            true ->
                Level;  % Base case
            false ->
                %% Call back into Python which will call us again
                Code = iolist_to_binary(io_lib:format(
                    "__import__('erlang').call('call_level', ~p, ~p)",
                    [Level + 1, N])),
                {ok, Result} = py:eval(Code),
                Result
        end
    end),

    %% Test with 3 levels of nesting: Python→Erlang→Python→Erlang→Python→Erlang
    {ok, Result3} = py:eval(<<"__import__('erlang').call('call_level', 1, 3)">>),
    3 = Result3,

    %% Test with 5 levels of nesting
    {ok, Result5} = py:eval(<<"__import__('erlang').call('call_level', 1, 5)">>),
    5 = Result5,

    ok.

%% @doc Test error propagation through reentrant callbacks.
test_callback_error_propagation(_Config) ->
    %% Register Erlang function that may raise an error
    py:register_function(may_fail, fun([ShouldFail]) ->
        case ShouldFail of
            true -> error(intentional_error);
            false -> <<"success">>
        end
    end),

    %% Test successful case - using py:eval with inline call
    {ok, <<"success">>} = py:eval(<<"__import__('erlang').call('may_fail', False)">>),

    %% Test error case - error should propagate back to Python
    {error, _} = py:eval(<<"__import__('erlang').call('may_fail', True)">>),

    ok.

%% @doc Test concurrent reentrant callbacks.
%% Multiple processes calling Python functions that do reentrant callbacks.
%% Note: We use py:eval directly since Python workers don't share namespace.
test_concurrent_reentrant(_Config) ->
    %% Register Erlang function
    py:register_function(triple, fun([X]) ->
        X * 3
    end),

    Self = self(),
    NumProcesses = 10,

    %% Spawn multiple processes that do reentrant calls
    %% Each uses py:eval with inline function definition to ensure it works
    %% regardless of which worker handles the request
    _Pids = [spawn_link(fun() ->
        Input = N * 10,
        %% Use eval to call the erlang function directly
        {ok, Result} = py:eval(iolist_to_binary(
            io_lib:format("__import__('erlang').call('triple', ~p)", [Input]))),
        Expected = Input * 3,
        Self ! {done, N, Result, Expected}
    end) || N <- lists:seq(1, NumProcesses)],

    %% Wait for all processes to complete
    Results = [receive
        {done, N, Result, Expected} -> {N, Result, Expected}
    after 10000 ->
        ct:fail({timeout, N})
    end || N <- lists:seq(1, NumProcesses)],

    %% Verify all results
    lists:foreach(fun({N, Result, Expected}) ->
        case Result of
            Expected -> ok;
            _ -> ct:fail({mismatch, N, expected, Expected, got, Result})
        end
    end, Results),

    %% Verify all processes completed
    NumProcesses = length(Results),

    ok.

%% @doc Test reentrant callbacks with complex types.
test_callback_with_complex_types(_Config) ->
    %% Register Erlang function that transforms data
    py:register_function(transform, fun([Data]) ->
        %% Add a field and modify existing
        case Data of
            #{<<"items">> := Items, <<"count">> := Count} ->
                #{
                    <<"items">> => lists:reverse(Items),
                    <<"count">> => Count * 2,
                    <<"processed">> => true
                };
            _ ->
                #{<<"error">> => <<"unexpected_format">>}
        end
    end),

    %% Test using py:eval with inline dict
    {ok, Result} = py:eval(<<"__import__('erlang').call('transform', {'items': [1, 2, 3], 'count': 5})">>),

    %% Verify result (map with reversed items and doubled count)
    #{
        <<"items">> := [3, 2, 1],
        <<"count">> := 10,
        <<"processed">> := true
    } = Result,

    ok.

%% @doc Test multiple sequential erlang.call() invocations in one Python function.
%% This tests that the nested suspension handling works when Python makes
%% multiple callbacks within a single function execution.
test_multiple_sequential_callbacks(_Config) ->
    %% Register three Erlang functions that will be called sequentially
    py:register_function(add_ten, fun([X]) -> X + 10 end),
    py:register_function(multiply_by_two, fun([X]) -> X * 2 end),
    py:register_function(subtract_five, fun([X]) -> X - 5 end),

    %% Use py:eval with a lambda that makes 3 sequential erlang.call() invocations.
    %% Each call triggers a suspension/resume cycle, and the second/third calls
    %% require the nested suspension fix to work correctly.
    %%
    %% The lambda pattern: (lambda x: subtract_five(multiply_by_two(add_ten(x))))(input)
    %% This is a single expression that makes 3 sequential callbacks.

    %% Test with x=5: ((5 + 10) * 2) - 5 = 25
    Code1 = <<"(lambda erl: erl.call('subtract_five', erl.call('multiply_by_two', erl.call('add_ten', 5))))(__import__('erlang'))">>,
    {ok, Result1} = py:eval(Code1),
    25 = Result1,

    %% Test with x=10: ((10 + 10) * 2) - 5 = 35
    Code2 = <<"(lambda erl: erl.call('subtract_five', erl.call('multiply_by_two', erl.call('add_ten', 10))))(__import__('erlang'))">>,
    {ok, Result2} = py:eval(Code2),
    35 = Result2,

    %% Test with x=0: ((0 + 10) * 2) - 5 = 15
    Code3 = <<"(lambda erl: erl.call('subtract_five', erl.call('multiply_by_two', erl.call('add_ten', 0))))(__import__('erlang'))">>,
    {ok, Result3} = py:eval(Code3),
    15 = Result3,

    ok.

%% @doc Test that erlang.call() from a non-worker thread gives a helpful error.
%% When Python spawns a thread (e.g., via ThreadPoolExecutor) and tries to call
%% erlang.call() from that thread, it should fail with an informative message.
test_call_from_non_worker_thread(_Config) ->
    %% Register a simple function to call
    py:register_function(simple_add, fun([A, B]) -> A + B end),

    %% Use an inline lambda to test calling from a thread
    %% The lambda imports, creates executor, runs in thread, catches error
    Code = <<"(lambda cf, erlang: (lambda executor: (lambda future: (('expected_error', str(e)) if 'worker thread' in str(e := future.exception()) and 'execute_async' in str(e) else ('wrong_error', str(e))) if future.exception() else ('unexpected_success', future.result()))(executor.submit(lambda: erlang.call('simple_add', 1, 2))))(cf.ThreadPoolExecutor(max_workers=1).__enter__()))(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']), __import__('erlang'))">>,
    {ok, Result} = py:eval(Code),

    %% Verify we got the expected error with helpful message
    case Result of
        {<<"expected_error">>, Msg} ->
            ct:log("Got expected error message: ~s", [Msg]),
            ok;
        {<<"wrong_error">>, Msg} ->
            ct:fail({wrong_error_message, Msg});
        {<<"unexpected_success">>, Val} ->
            ct:fail({should_have_failed, Val})
    end,

    %% Cleanup
    py:unregister_function(simple_add),
    ok.
