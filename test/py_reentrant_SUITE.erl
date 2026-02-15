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
    test_callback_with_complex_types/1
]).

all() ->
    [
        test_basic_reentrant,
        test_nested_callbacks,
        test_callback_error_propagation,
        test_concurrent_reentrant,
        test_callback_with_complex_types
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
