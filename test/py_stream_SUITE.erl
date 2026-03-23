%%% @doc Common Test suite for py:stream_start/3,4 true streaming API.
-module(py_stream_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_stream_start_basic/1,
    test_stream_start_range/1,
    test_stream_start_generator/1,
    test_stream_start_with_owner/1,
    test_stream_cancel/1,
    test_stream_error/1,
    test_stream_empty/1,
    test_stream_large/1
]).

all() ->
    [
        test_stream_start_basic,
        test_stream_start_range,
        test_stream_start_generator,
        test_stream_start_with_owner,
        test_stream_cancel,
        test_stream_error,
        test_stream_empty,
        test_stream_large
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

%% Helper to collect all stream events
collect_stream(Ref) ->
    collect_stream(Ref, [], 5000).

collect_stream(Ref, Acc, Timeout) ->
    receive
        {py_stream, Ref, {data, Value}} ->
            collect_stream(Ref, [Value | Acc], Timeout);
        {py_stream, Ref, done} ->
            {ok, lists:reverse(Acc)};
        {py_stream, Ref, {error, Reason}} ->
            {error, Reason}
    after Timeout ->
        {error, timeout}
    end.

%% Test basic streaming with iter()
test_stream_start_basic(_Config) ->
    {ok, Ref} = py:stream_start(builtins, iter, [[1, 2, 3, 4, 5]]),
    {ok, Values} = collect_stream(Ref),
    [1, 2, 3, 4, 5] = Values,
    ok.

%% Test streaming with range()
test_stream_start_range(_Config) ->
    {ok, Ref} = py:stream_start(builtins, range, [5]),
    {ok, Values} = collect_stream(Ref),
    [0, 1, 2, 3, 4] = Values,
    ok.

%% Test streaming with a filter - uses filter() which returns an iterator
test_stream_start_generator(_Config) ->
    %% Stream only items that don't raise StopIteration
    %% Use enumerate to get (index, value) pairs from a string
    {ok, Ref} = py:stream_start(builtins, enumerate, [<<"hello">>]),
    {ok, Values} = collect_stream(Ref),
    %% enumerate returns tuples of (index, char)
    5 = length(Values),
    {0, <<"h">>} = hd(Values),
    ok.

%% Test streaming with custom owner process
test_stream_start_with_owner(_Config) ->
    Self = self(),
    Receiver = spawn(fun() ->
        receive
            {collect, Ref} ->
                Result = collect_stream(Ref),
                Self ! {result, Result}
        end
    end),
    {ok, Ref} = py:stream_start(builtins, iter, [[10, 20, 30]], #{owner => Receiver}),
    Receiver ! {collect, Ref},
    receive
        {result, {ok, Values}} ->
            [10, 20, 30] = Values,
            ok;
        {result, Error} ->
            ct:fail({unexpected_error, Error})
    after 5000 ->
        ct:fail(timeout)
    end.

%% Test stream cancellation
test_stream_cancel(_Config) ->
    %% Use a large range that we'll cancel partway through
    {ok, Ref} = py:stream_start(builtins, range, [1000000]),
    %% Receive a few values then cancel
    receive
        {py_stream, Ref, {data, 0}} -> ok
    after 5000 ->
        ct:fail(no_first_value)
    end,
    %% Cancel the stream
    ok = py:stream_cancel(Ref),
    %% Drain remaining messages (may receive a few more before cancellation takes effect)
    drain_stream(Ref),
    ok.

%% Helper to drain stream messages
drain_stream(Ref) ->
    receive
        {py_stream, Ref, _} -> drain_stream(Ref)
    after 1000 ->
        ok
    end.

%% Test error handling in generator
test_stream_error(_Config) ->
    %% Call a function that doesn't exist - should get an error about attribute
    {ok, Ref} = py:stream_start(builtins, nonexistent_function, []),
    %% Should get an error about missing attribute
    receive
        {py_stream, Ref, {error, Reason}} ->
            true = is_binary(Reason) orelse is_list(Reason),
            %% Error should mention something about attribute not found
            ok;
        {py_stream, Ref, done} ->
            ct:fail(expected_error_not_done);
        {py_stream, Ref, {data, _}} ->
            ct:fail(expected_error_not_data)
    after 5000 ->
        ct:fail(no_error)
    end.

%% Test empty generator
test_stream_empty(_Config) ->
    {ok, Ref} = py:stream_start(builtins, iter, [[]]),
    {ok, Values} = collect_stream(Ref),
    [] = Values,
    ok.

%% Test streaming a larger sequence
test_stream_large(_Config) ->
    {ok, Ref} = py:stream_start(builtins, range, [1000]),
    {ok, Values} = collect_stream(Ref, [], 30000),
    1000 = length(Values),
    %% Verify first and last values
    0 = hd(Values),
    999 = lists:last(Values),
    ok.
