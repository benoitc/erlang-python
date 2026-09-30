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
    loop_namespace_freed_after_process_exit_owngil/1,
    loop_task_env_freed_after_process_exit/1,
    env_freed_after_process_exit_worker/1,
    env_freed_after_process_exit_owngil/1,
    env_outlives_owngil_context/1,
    async_env_call_returns_correct_result/1,
    env_call_does_not_dispatch_timeout/1
]).

all() ->
    [
        async_env_call_returns_correct_result,
        env_call_does_not_dispatch_timeout,
        env_freed_after_process_exit_worker,
        env_freed_after_process_exit_owngil,
        env_outlives_owngil_context,
        loop_namespace_freed_after_process_exit_owngil,
        loop_task_env_freed_after_process_exit
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


%% A process-local env holds its objects until the process exits; then they
%% are released. Each env registers an object in a WeakSet kept on `sys'
%% (shared by all envs of the interpreter): it empties once they are freed.
%% The class is defined in the context, not in the envs: on free-threaded
%% builds a class created in an env keeps its globals until CPython reclaims
%% the class on its own schedule, which is not what is tested here.
env_freed_after_process_exit_worker(_Config) ->
    envs_freed(worker).

%% owngil envs used to be kept until the context stopped: the destructor runs
%% on a thread that cannot take that interpreter's GIL. It now hands them to
%% the context thread, which releases them before its next request.
env_freed_after_process_exit_owngil(_Config) ->
    case py_nif:owngil_supported() of
        true -> envs_freed(owngil);
        false -> {skip, "OWN_GIL requires Python 3.14+"}
    end.

envs_freed(Mode) ->
    {ok, C} = py_context:new(#{mode => Mode}),
    ok = py_context:exec(C, <<"import sys, weakref\nsys._env_probe = weakref.WeakSet()\n"
                              "class Held: pass\nsys._EnvHeld = Held">>),
    [begin
         {P, M} = spawn_monitor(fun() ->
             ok = py:exec(C, <<"import sys\nheld = sys._EnvHeld()\nsys._env_probe.add(held)">>),
             {ok, true} = py:eval(C, <<"held in sys._env_probe">>)
         end),
         receive {'DOWN', M, process, P, normal} -> ok end
     end || _ <- lists:seq(1, 50)],
    ok = wait_probe_empty(C, 250),
    py_context:stop(C).

wait_probe_empty(C, 0) ->
    ct:fail({envs_not_freed, py_context:eval(C, <<"len(__import__('sys')._env_probe)">>)});
wait_probe_empty(C, N) ->
    erlang:garbage_collect(),
    %% the next request on the context releases what the dead envs held
    case py_context:eval(C, <<"(__import__('gc').collect(), len(__import__('sys')._env_probe))[1]">>) of
        {ok, 0} -> ok;
        _ -> timer:sleep(20), wait_probe_empty(C, N - 1)
    end.

%% A process still holding an owngil env when its context stops: the env is
%% released after the interpreter is gone, without touching it.
env_outlives_owngil_context(_Config) ->
    case py_nif:owngil_supported() of
        false ->
            {skip, "OWN_GIL requires Python 3.14+"};
        true ->
            {ok, C} = py_context:new(#{mode => owngil}),
            Self = self(),
            Holders = [spawn(fun() ->
                           ok = py:exec(C, <<"blob = 'x' * 100000">>),
                           Self ! {ready, self()},
                           receive go -> ok end
                       end) || _ <- lists:seq(1, 10)],
            [receive {ready, H} -> ok after 5000 -> ct:fail(no_env) end || H <- Holders],
            ok = py_context:stop(C),
            Mons = [erlang:monitor(process, H) || H <- Holders],
            [H ! go || H <- Holders],
            [receive {'DOWN', M, process, _, normal} -> ok after 5000 -> ct:fail(holder_stuck) end
             || M <- Mons],
            erlang:garbage_collect(),
            %% the node is fine and new owngil contexts work
            {ok, C2} = py_context:new(#{mode => owngil}),
            {ok, 2} = py_context:eval(C2, <<"1 + 1">>),
            py_context:stop(C2)
    end.


%% The namespace a process gets on an owngil context's event loop
%% (py_event_loop:exec/eval) is released when the process exits. Those
%% namespaces are created in the main interpreter even on a subinterpreter
%% loop; they used to be dropped without being released.
loop_namespace_freed_after_process_exit_owngil(_Config) ->
    case py_nif:owngil_supported() of
        false ->
            {skip, "OWN_GIL requires Python 3.14+"};
        true ->
            {ok, C} = py_context:new(#{mode => owngil}),
            {ok, Loop} = py_context:loop_ref(C),
            ok = py_event_loop:exec(Loop, <<"import sys, weakref\nsys._loop_probe = weakref.WeakSet()\n"
                                            "class Held: pass\nsys._LoopHeld = Held">>),
            [begin
                 {P, M} = spawn_monitor(fun() ->
                     ok = py_event_loop:exec(Loop, <<"import sys\nheld = sys._LoopHeld()\nsys._loop_probe.add(held)">>)
                 end),
                 receive {'DOWN', M, process, P, normal} -> ok end
             end || _ <- lists:seq(1, 30)],
            ok = wait_loop_probe_empty(Loop, 250),
            py_context:stop(C)
    end.

wait_loop_probe_empty(Loop, 0) ->
    ct:fail({loop_namespaces_not_freed,
             py_event_loop:eval(Loop, <<"len(__import__('sys')._loop_probe)">>)});
wait_loop_probe_empty(Loop, N) ->
    erlang:garbage_collect(),
    case py_event_loop:eval(Loop, <<"(__import__('gc').collect(), len(__import__('sys')._loop_probe))[1]">>) of
        {ok, 0} -> ok;
        _ -> timer:sleep(20), wait_loop_probe_empty(Loop, N - 1)
    end.


%% A task submitted to the event loop from a process that has a local env
%% (py_event_loop:create_task after py:exec) registers that env for the
%% process. The registration used to keep the env for the loop's lifetime;
%% it is dropped when the process exits.
loop_task_env_freed_after_process_exit(_Config) ->
    ok = py:exec(<<"import sys, weakref\nsys._task_env_probe = weakref.WeakSet()\n"
                   "class Held: pass\nsys._TaskHeld = Held">>),
    [begin
         {P, M} = spawn_monitor(fun() ->
             ok = py:exec(<<"import sys\nheld = sys._TaskHeld()\nsys._task_env_probe.add(held)">>),
             %% the process has an env, so the task registers it; the task
             %% itself calls a library coroutine (no function defined in the
             %% env, see envs_freed/1 about free-threaded builds)
             Ref = py_event_loop:create_task(asyncio, sleep, [0]),
             {ok, none} = py_event_loop:await(Ref, 5000)
         end),
         receive {'DOWN', M, process, P, normal} -> ok after 10000 -> ct:fail(task_hung) end
     end || _ <- lists:seq(1, 20)],
    ok = wait_task_probe_empty(250).

wait_task_probe_empty(0) ->
    ct:fail({task_envs_not_freed, py:eval(<<"len(__import__('sys')._task_env_probe)">>)});
wait_task_probe_empty(N) ->
    erlang:garbage_collect(),
    case py:eval(<<"(__import__('gc').collect(), len(__import__('sys')._task_env_probe))[1]">>) of
        {ok, 0} -> ok;
        _ -> timer:sleep(20), wait_task_probe_empty(N - 1)
    end.
