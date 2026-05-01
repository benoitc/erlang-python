%%% @doc Thread-affinity invariants for the per-context worker model.
%%%
%%% After the v3.0 simplification each context owns a dedicated pthread
%%% that handles all of its Python operations. These tests assert the
%%% invariants that motivated the rework so we don't regress numpy /
%%% torch / tensorflow thread-local state safety:
%%%
%%%   - exec / eval / call on the same context all share one OS thread
%%%   - calls from N different Erlang processes targeting the same
%%%     context all converge on that context's worker thread
%%%   - distinct contexts get distinct worker threads
%%%   - the same invariants hold under owngil mode when supported
-module(py_thread_affinity_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    exec_eval_call_share_thread/1,
    multi_process_share_context_thread/1,
    distinct_contexts_distinct_threads/1,
    owngil_thread_affinity/1
]).

all() ->
    [
        exec_eval_call_share_thread,
        multi_process_share_context_thread,
        distinct_contexts_distinct_threads,
        owngil_thread_affinity
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

native_id(Ctx) ->
    case py_context:eval(Ctx, <<"_pta_get_tid()">>, #{}) of
        {ok, Tid} -> Tid;
        Other -> ct:fail({native_id_failed, Other})
    end.

setup_helper(Ctx) ->
    ok = py_context:exec(Ctx, <<
        "import threading\n"
        "def _pta_get_tid():\n"
        "    return threading.get_native_id()\n"
    >>).

%%% ============================================================================
%%% Tests
%%% ============================================================================

exec_eval_call_share_thread(_Config) ->
    Ctx = py:context(1),
    setup_helper(Ctx),
    %% Stash a thread id from exec, then read it back via eval.
    ok = py_context:exec(Ctx, <<
        "import threading\n"
        "_pta_exec_tid = threading.get_native_id()\n"
    >>),
    {ok, ExecTid} = py_context:eval(Ctx, <<"_pta_exec_tid">>, #{}),
    EvalTid = native_id(Ctx),
    {ok, CallTid} = py_context:call(Ctx, '__main__', '_pta_get_tid', []),
    ct:pal("exec=~p eval=~p call=~p", [ExecTid, EvalTid, CallTid]),
    ExecTid = EvalTid,
    EvalTid = CallTid,
    ok.

multi_process_share_context_thread(_Config) ->
    Ctx = py:context(1),
    setup_helper(Ctx),
    Parent = self(),
    N = 8,
    Pids = [spawn(fun() ->
                  Tid = native_id(Ctx),
                  Parent ! {tid, self(), Tid}
              end) || _ <- lists:seq(1, N)],
    Tids = [receive {tid, Pid, T} -> T after 5000 -> ct:fail(timeout) end
            || Pid <- Pids],
    ct:pal("tids = ~p", [Tids]),
    [Single] = lists:usort(Tids),
    true = is_integer(Single),
    ok.

distinct_contexts_distinct_threads(_Config) ->
    case py_context_router:num_contexts() of
        N when N >= 2 ->
            Ctx1 = py:context(1),
            Ctx2 = py:context(2),
            setup_helper(Ctx1),
            setup_helper(Ctx2),
            T1 = native_id(Ctx1),
            T2 = native_id(Ctx2),
            ct:pal("ctx1=~p ctx2=~p", [T1, T2]),
            true = T1 =/= T2,
            ok;
        _ ->
            {skip, "needs at least 2 contexts"}
    end.

owngil_thread_affinity(_Config) ->
    case py:subinterp_supported() of
        false ->
            {skip, "subinterpreters not supported on this Python build"};
        true ->
            case py_context:new(#{mode => owngil}) of
                {error, owngil_requires_python314} ->
                    {skip, "owngil requires Python 3.14+"};
                {error, Reason} ->
                    ct:fail({owngil_create_failed, Reason});
                {ok, Ctx} ->
                    try
                        setup_helper(Ctx),
                        ok = py_context:exec(Ctx, <<
                            "import threading\n"
                            "_pta_exec_tid = threading.get_native_id()\n"
                        >>),
                        {ok, ExecTid} = py_context:eval(Ctx,
                                                          <<"_pta_exec_tid">>, #{}),
                        EvalTid = native_id(Ctx),
                        {ok, CallTid} = py_context:call(Ctx,
                                                          '__main__',
                                                          '_pta_get_tid', []),
                        ct:pal("owngil exec=~p eval=~p call=~p",
                               [ExecTid, EvalTid, CallTid]),
                        ExecTid = EvalTid,
                        EvalTid = CallTid,

                        Parent = self(),
                        Pids = [spawn(fun() ->
                                          Tid = native_id(Ctx),
                                          Parent ! {tid, self(), Tid}
                                      end) || _ <- lists:seq(1, 4)],
                        Tids = [receive {tid, Pid, T} -> T
                                after 5000 -> ct:fail(timeout)
                                end || Pid <- Pids],
                        ct:pal("owngil multi-proc tids = ~p", [Tids]),
                        [Single] = lists:usort(Tids),
                        Single = ExecTid,
                        ok
                    after
                        py_context:stop(Ctx)
                    end
            end
    end.
