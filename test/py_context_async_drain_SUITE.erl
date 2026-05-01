%%% @doc Pin the stale-{py_result,_,_}-drain behavior in py_context.
%%%
%%% wait_for_async_result/2 returns {error, async_timeout} after 5 minutes
%%% but the C worker may eventually finish and deliver a {py_result, OldId,
%%% _} message anyway. Without a drain, those messages would pile up on
%%% the context process's mailbox forever. This suite injects a stale
%%% message directly into the context's mailbox and asserts it is gone
%%% after the next legitimate dispatch.
-module(py_context_async_drain_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    drain_stale_results/1
]).

all() ->
    [drain_stale_results].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

drain_stale_results(_Config) ->
    Ctx = py:context(1),
    %% Warm up the context: ensure math is importable, then exercise
    %% an async-dispatch call so the loop is fully primed.
    {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16]),

    %% Inject a stale result message directly into the context's mailbox.
    %% py_context's outer loop matches on specific tags only; an
    %% unrecognized {py_result, FakeId, _} is left in place by selective
    %% receive and would accumulate forever without the drain.
    FakeId = make_ref(),
    Ctx ! {py_result, FakeId, junk_should_be_drained},
    {message_queue_len, QLenBefore} =
        erlang:process_info(Ctx, message_queue_len),
    true = QLenBefore >= 1,

    %% Trigger an async-dispatch call (py_context:call/4 -> handle_call_
    %% with_suspension -> wait_for_async_result/2). The drain runs first
    %% and consumes the stale message; the matching receive gets the
    %% real result.
    {ok, 5.0} = py_context:call(Ctx, math, sqrt, [25]),

    %% Brief settle to let any in-flight worker message land.
    timer:sleep(50),
    {message_queue_len, QLenAfter} =
        erlang:process_info(Ctx, message_queue_len),
    {messages, Msgs} = erlang:process_info(Ctx, messages),
    ct:pal("Ctx mailbox after drain: len=~p msgs=~p", [QLenAfter, Msgs]),
    0 = QLenAfter,
    ok.
