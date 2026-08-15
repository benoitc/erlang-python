%%% @doc Common Test suite for interrupting running Python code.
%%%
%%% Covers py_nif:context_interrupt/1 and the automatic interrupt that
%%% py_context issues when a call timeout expires. Every case runs against
%%% both context modes; the owngil group is skipped when the runtime does
%%% not support it.
-module(py_interrupt_SUITE).

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

-export([
    test_timeout_interrupts_loop/1,
    test_context_reusable_after_interrupt/1,
    test_explicit_interrupt_returns_interrupted/1,
    test_interrupt_idle_context/1,
    test_base_exception_not_swallowed/1,
    test_no_stale_reply_in_mailbox/1,
    test_interrupt_does_not_leak_to_next_request/1,
    test_interrupt_during_callback/1,
    test_blocking_c_call_recovers/1
]).

%% Python that spins forever without allocating or calling into C.
-define(BUSY, <<"sum(1 for _ in iter(int, 1))">>).

all() ->
    [{group, worker}, {group, owngil}].

groups() ->
    Cases = [
        test_timeout_interrupts_loop,
        test_context_reusable_after_interrupt,
        test_explicit_interrupt_returns_interrupted,
        test_interrupt_idle_context,
        test_base_exception_not_swallowed,
        test_no_stale_reply_in_mailbox,
        test_interrupt_does_not_leak_to_next_request,
        test_interrupt_during_callback,
        test_blocking_c_call_recovers
    ],
    [{worker, [], Cases}, {owngil, [], Cases}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(owngil, Config) ->
    case py_nif:owngil_supported() of
        true -> [{mode, owngil} | Config];
        false -> {skip, "OWN_GIL requires Python 3.14+"}
    end;
init_per_group(worker, Config) ->
    [{mode, worker} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, Ctx} = py_context:new(#{mode => ?config(mode, Config)}),
    [{ctx, Ctx} | Config].

end_per_testcase(_TestCase, Config) ->
    catch py_context:stop(?config(ctx, Config)),
    flush(),
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc A timed-out call stops burning CPU instead of running to completion.
test_timeout_interrupts_loop(Config) ->
    Ctx = ?config(ctx, Config),
    T0 = erlang:monotonic_time(millisecond),
    {error, timeout} = py_context:eval(Ctx, ?BUSY, #{}, 500),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    %% Returns near the deadline, not after the grace period expires
    true = Elapsed < 500 + 900,
    ok.

%% @doc The context is immediately usable again after an interrupt. If the
%% Python loop were still running, this call would queue behind it.
test_context_reusable_after_interrupt(Config) ->
    Ctx = ?config(ctx, Config),
    {error, timeout} = py_context:eval(Ctx, ?BUSY, #{}, 300),
    T0 = erlang:monotonic_time(millisecond),
    {ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 5000),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    true = Elapsed < 1000,
    ok.

%% @doc An explicit interrupt surfaces to the caller as {error, interrupted}.
test_explicit_interrupt_returns_interrupted(Config) ->
    Ctx = ?config(ctx, Config),
    Me = self(),
    spawn(fun() -> Me ! {res, py_context:eval(Ctx, ?BUSY, #{}, infinity)} end),
    timer:sleep(300),
    ok = py:interrupt(Ctx),
    receive
        {res, Result} -> {error, interrupted} = Result
    after 5000 ->
        ct:fail(no_reply_after_interrupt)
    end,
    ok.

%% @doc Interrupting an idle context is a no-op and does not affect later calls.
test_interrupt_idle_context(Config) ->
    Ctx = ?config(ctx, Config),
    not_running = py:interrupt(Ctx),
    {ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 5000),
    ok.

%% @doc KeyboardInterrupt is a BaseException, so a bare `except Exception`
%% around the hot loop does not swallow the interrupt.
test_base_exception_not_swallowed(Config) ->
    Ctx = ?config(ctx, Config),
    Code = <<"
def _guarded():
    while True:
        try:
            pass
        except Exception:
            pass
_guarded()">>,
    {error, timeout} = py_context:eval(Ctx, <<"exec('''", Code/binary, "''')">>, #{}, 500),
    {ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 5000),
    ok.

%% @doc The reply from the interrupted call is drained, not left in the
%% caller's mailbox.
test_no_stale_reply_in_mailbox(Config) ->
    Ctx = ?config(ctx, Config),
    {error, timeout} = py_context:eval(Ctx, ?BUSY, #{}, 300),
    {ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 5000),
    timer:sleep(200),
    {messages, []} = process_info(self(), messages),
    ok.

%% @doc An interrupt that lands as a request completes must not be delivered
%% to the next request on the same context.
test_interrupt_does_not_leak_to_next_request(Config) ->
    Ctx = ?config(ctx, Config),
    Bad = lists:foldl(fun(_, Acc) ->
        %% Very short timeout so the interrupt races request completion
        _ = py_context:eval(Ctx, ?BUSY, #{}, 20),
        case py_context:eval(Ctx, <<"1+1">>, #{}, 5000) of
            {ok, 2} -> Acc;
            Other -> [Other | Acc]
        end
    end, [], lists:seq(1, 50)),
    [] = Bad,
    ok.

%% @doc Interrupting Python that is suspended in an Erlang callback. The
%% callback path executes on a dirty scheduler via py_context_acquire, a
%% different code path from the context worker thread.
test_interrupt_during_callback(Config) ->
    Ctx = ?config(ctx, Config),
    ok = py:register_function(<<"interrupt_test_echo">>, fun([X]) -> X end),
    Code = <<"
import erlang
_n = 0
while True:
    _n = erlang.call('interrupt_test_echo', _n) + 1
">>,
    {error, timeout} = py_context:eval(Ctx, <<"exec('''", Code/binary, "''')">>, #{}, 500),
    {ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 10000),
    py:unregister_function(<<"interrupt_test_echo">>),
    ok.

%% @doc Documented limitation: an async exception is delivered at a bytecode
%% boundary, so code blocked in a C call is only interrupted once that call
%% returns. The call still times out and the context recovers afterwards.
test_blocking_c_call_recovers(Config) ->
    Ctx = ?config(ctx, Config),
    {error, timeout} = py_context:eval(
        Ctx, <<"__import__('time').sleep(2) or 1">>, #{}, 200),
    %% Once the sleep finishes the context accepts work again
    {ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 10000),
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

flush() ->
    receive _ -> flush()
    after 0 -> ok
    end.
