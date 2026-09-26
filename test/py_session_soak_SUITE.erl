%%% @doc Soak test for sessions: many callers opening, using, crashing,
%%% killing and closing sessions from fork and spawn templates for a while,
%%% then resource counters checked against their baseline. It shows that
%%% every operation returns (no deadlock) and that nothing leaks: Erlang
%%% processes, ports, VM file descriptors, session children, zombies and
%%% scratch directories.
%%%
%%% Duration is 30 s by default; set `PY_SESSION_SOAK_SECONDS' to change it.
-module(py_session_soak_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([test_session_churn_no_leak/1]).

-define(MOD, py_test_session).

all() -> [test_session_churn_no_leak].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    py:register_function(sess_reenter, fun([Ctx, N]) ->
        {ok, R} = py_context:call(Ctx, ?MOD, reenter, [N - 1], #{}, 20000),
        R
    end),
    [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config].

end_per_suite(_Config) ->
    py:unregister_function(sess_reenter),
    ok.

test_session_churn_no_leak(Config) ->
    Seconds = case os:getenv("PY_SESSION_SOAK_SECONDS") of
        false -> 30;
        S -> list_to_integer(S)
    end,
    TestDir = ?config(test_dir, Config),
    {ok, Fork} = py_session:template(#{start => fork, zygotes => 2, paths => [TestDir],
                                       imports => [?MOD]}),
    {ok, Spawn} = py_session:template(#{start => spawn, warm => 2, paths => [TestDir],
                                        imports => [?MOD]}),
    timer:sleep(1000),
    Base = counters(),
    ct:print("baseline: ~p", [Base]),
    Deadline = erlang:monotonic_time(millisecond) + Seconds * 1000,
    Self = self(),
    Workers = [spawn_link(fun() ->
                   rand:seed(exsss, {I, I * 7, I * 13}),
                   T = case I rem 4 of 0 -> Spawn; _ -> Fork end,
                   Self ! {done, self(), churn(T, Deadline, 0, [])}
               end) || I <- lists:seq(1, 12)],
    Results = [receive {done, W, R} -> R after (Seconds + 120) * 1000 -> ct:fail(worker_hung) end
               || W <- Workers],
    Ops = lists:sum([N || {N, _} <- Results]),
    Errs = lists:append([E || {_, E} <- Results]),
    ct:print("soak: ~p sessions in ~p s, ~p unexpected results: ~p",
             [Ops, Seconds, length(Errs), lists:sublist(Errs, 5)]),
    [] = Errs,
    true = Ops > 0,
    #{sessions := 0} = wait_idle(Fork, 100),
    py_session:stop_template(Fork),
    py_session:stop_template(Spawn),
    timer:sleep(1000),
    After = counters(),
    ct:print("after: ~p", [After]),
    check_no_growth(Base, After).

%% One session per round, with a random use of it
churn(T, Deadline, N, Errs) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        false ->
            {N, Errs};
        true ->
            {ok, S} = py_session:new(T),
            Got = use(rand:uniform(8), S),
            ok = py_session:close(S),
            churn(T, Deadline, N + 1, case Got of ok -> Errs; Bad -> [Bad | Errs] end)
    end.

use(1, S) -> expect({ok, 3}, py_context:call(S, ?MOD, reenter, [3]));
use(2, S) -> expect({error, {child_exited, {signal, 6}}}, py_context:call(S, ?MOD, abort, []));
use(3, S) -> expect(ok, py_context:kill(S));
use(4, S) -> expect({error, timeout}, py_context:call(S, ?MOD, sleep, [10], #{}, 50));
use(5, S) ->
    %% a thread left running and a file written, then closed
    _ = py_context:call(S, ?MOD, start_thread, []),
    expect({ok, [<<"f">>]}, py_context:call(S, ?MOD, write_file, [<<"f">>, <<"x">>]));
use(6, S) -> expect({ok, none}, py_context:call(S, ?MOD, peek, []));
use(7, S) -> expect(ok, py_context:exec(S, <<"import json; json.dumps(list(range(1000)))">>));
use(8, _S) -> ok.

expect(Want, Want) -> ok;
expect(Want, Got) -> {Want, Got}.

wait_idle(T, 0) ->
    py_session:info(T);
wait_idle(T, N) ->
    case py_session:info(T) of
        #{sessions := 0} = I -> I;
        _ -> timer:sleep(50), wait_idle(T, N - 1)
    end.

%%% ============================================================================
%%% Counters
%%% ============================================================================

counters() ->
    erlang:garbage_collect(),
    #{processes => erlang:system_info(process_count),
      ports => erlang:system_info(port_count),
      refs => ets:info(py_context_refs, size),
      fds => beam_fd_count(),
      children => session_children(),
      zombies => zombies(),
      scratch => length(filelib:wildcard(filename:join(py_child:sock_dir(), "sess_*")))}.

check_no_growth(Base, After) ->
    [begin
         B = maps:get(K, Base), A = maps:get(K, After),
         A =< B + Slack orelse ct:fail({leak, K, B, A})
     end || {K, Slack} <- [{processes, 5}, {ports, 0}, {refs, 0}, {children, 0},
                           {zombies, 0}, {scratch, 0}, {fds, 8}]],
    ok.

beam_fd_count() ->
    case os:type() of
        {unix, linux} ->
            length(filelib:wildcard("/proc/" ++ os:getpid() ++ "/fd/*"));
        _ ->
            Out = os:cmd("lsof -p " ++ os:getpid() ++ " 2>/dev/null | wc -l"),
            list_to_integer(string:trim(Out)) - 1
    end.

%% Python processes started by this node: spawned children, zygotes and the
%% sessions they forked (their command line is the zygote's)
session_children() ->
    Out = os:cmd("ps -ax -o command= 2>/dev/null | grep -E 'py_zygote.py|py_isolated_child.py' "
                 "| grep erlang_python_" ++ os:getpid() ++ " | grep -v grep | wc -l"),
    list_to_integer(string:trim(Out)).

zombies() ->
    Out = os:cmd("ps -ax -o stat=,command= 2>/dev/null | grep -E '^Z' "
                 "| grep -E 'python|py_zygote' | grep -v grep | wc -l"),
    list_to_integer(string:trim(Out)).
