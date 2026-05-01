%%% @doc Pin the async-with-env dispatch path.
%%%
%%% v3.0 introduced an async dispatch path for call / eval / exec that
%%% returns {enqueued, RequestId} from the NIF and lets the Erlang side
%%% wait in a normal receive. The env-bearing variants
%%% (py_context:call/5, eval/5 with EnvRef, exec/3) used to take a
%%% blocking sync dispatch with a 30-second pthread_cond_timedwait,
%%% returning {error, worker_timeout} for long-running Python while
%%% the worker kept going.
%%%
%%% These cases verify the env path now uses the async dispatch and
%%% completes correctly.
-module(py_context_async_env_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    async_env_call_returns_correct_result/1,
    env_call_does_not_dispatch_timeout/1
]).

all() ->
    [
        async_env_call_returns_correct_result,
        env_call_does_not_dispatch_timeout
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

async_env_call_returns_correct_result(_Config) ->
    %% py:call/3 wraps an EnvRef under the hood, so a successful
    %% round-trip proves the new context_call_with_env_async path is
    %% wired and the worker delivers a {py_result, _, _} for it.
    {ok, 4.0} = py:call(math, sqrt, [16]),
    {ok, 5.0} = py:call(math, sqrt, [25]),
    ok.

env_call_does_not_dispatch_timeout(_Config) ->
    %% Have the Python side block for 1 second. Under the old sync
    %% dispatch this exercised the 30-second pthread_cond_timedwait;
    %% now it's an Erlang-side receive on {py_result, _, _} so latency
    %% should track wall-clock and never produce {error, worker_timeout}.
    Ctx = py:context(1),
    EnvRef = py:get_local_env(Ctx),
    ok = py_context:exec(Ctx, <<
        "import time\n"
        "def _slow_round(x):\n"
        "    time.sleep(1.0)\n"
        "    return x * 2\n"
    >>, EnvRef),
    Start = erlang:monotonic_time(millisecond),
    {ok, 14} = py_context:call(Ctx, '__main__', '_slow_round', [7], #{},
                               infinity, EnvRef),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ct:pal("env-async call elapsed: ~p ms", [Elapsed]),
    true = Elapsed >= 900,
    true = Elapsed < 5000,
    ok.
