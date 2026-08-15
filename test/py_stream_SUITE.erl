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
    test_stream_large/1,
    test_stream_rejects_injection/1,
    test_stream_async_generator/1,
    test_stream_async_generator_args/1,
    test_stream_async_generator_empty/1,
    test_stream_async_generator_error/1,
    test_stream_async_cancel/1
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
        test_stream_large,
        test_stream_rejects_injection,
        test_stream_async_generator,
        test_stream_async_generator_args,
        test_stream_async_generator_empty,
        test_stream_async_generator_error,
        test_stream_async_cancel
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    %% Make test/py_test_agen.py importable for the async generator cases
    TestDir = filename:join(code:lib_dir(erlang_python), "test"),
    ok = py:exec(iolist_to_binary(io_lib:format(
        "import sys; sys.path.insert(0, '~s')", [TestDir]))),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

%% @doc A module/func name (or kwarg key) that isn't a valid Python identifier
%% must be rejected, not interpolated into the generated source where it could
%% inject code. Regression for the stream source-builder hardening.
test_stream_rejects_injection(_Config) ->
    {'EXIT', {{invalid_python_identifier, _}, _}} =
        (catch py:stream(<<"os'); __import__('os').system('x">>, <<"walk">>, [], #{k => 1})),
    {'EXIT', {{invalid_python_identifier, _}, _}} =
        (catch py:stream(<<"math">>, <<"sqrt'); evil(">>, [], #{k => 1})),
    {'EXIT', {{invalid_python_identifier, _}, _}} =
        (catch py:stream(<<"math">>, <<"sqrt">>, [], #{<<"bad key)">> => 1})),
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

%%% ============================================================================
%%% Async generators
%%%
%%% stream_start/3,4 drives an async generator on a private event loop. The
%%% collect-style py:stream/4-with-kwargs and py:stream_eval/1,2 wrap the call
%%% in list() and remain sync-only.
%%% ============================================================================

%% Test streaming from an async generator
test_stream_async_generator(_Config) ->
    {ok, Ref} = py:stream_start(<<"py_test_agen">>, <<"counter">>, [4]),
    {ok, Values} = collect_stream(Ref),
    [0, 1, 2, 3] = Values,
    ok.

%% Test an async generator taking several arguments
test_stream_async_generator_args(_Config) ->
    {ok, Ref} = py:stream_start(<<"py_test_agen">>, <<"scaled">>, [3, 10]),
    {ok, Values} = collect_stream(Ref),
    [0, 10, 20] = Values,
    ok.

%% An async generator that yields nothing still completes
test_stream_async_generator_empty(_Config) ->
    {ok, Ref} = py:stream_start(<<"py_test_agen">>, <<"empty">>, []),
    {ok, Values} = collect_stream(Ref),
    [] = Values,
    ok.

%% An exception raised mid-iteration is reported as a stream error, after the
%% values yielded before it
test_stream_async_generator_error(_Config) ->
    {ok, Ref} = py:stream_start(<<"py_test_agen">>, <<"failing">>, [5]),
    {error, Reason} = collect_stream(Ref),
    true = is_binary(Reason),
    {_, _} = binary:match(Reason, <<"agen boom">>),
    ok.

%% Cancelling an async stream stops it with {error, cancelled}
test_stream_async_cancel(_Config) ->
    {ok, Ref} = py:stream_start(<<"py_test_agen">>, <<"slow">>, [50]),
    receive
        {py_stream, Ref, {data, _}} -> ok
    after 5000 ->
        ct:fail(no_first_value)
    end,
    ok = py:stream_cancel(Ref),
    {error, <<"cancelled">>} = collect_stream(Ref),
    drain_stream(Ref),
    ok.
