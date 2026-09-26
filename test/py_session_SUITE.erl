%%% @doc Isolated sessions (py_session): a fresh process per session, started
%%% from a template, by fork from a zygote or by spawning a child.
%%%
%%% The `fork' and `spawn' groups run the same cases; `fork_only' and
%%% `spawn_only' cover what differs.
-module(py_session_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2, end_per_testcase/2]).

%% logger handler used by test_print_is_logged
-export([log/2]).

-export([
    test_sessions_are_isolated/1,
    test_same_start_state/1,
    test_env_replaced/1,
    test_hash_seed/1,
    test_random_differs/1,
    test_context_api/1,
    test_reentrance_depth/1,
    test_concurrent_reentrance/1,
    test_cross_session_call/1,
    test_interrupt_nested_call/1,
    test_session_crash/1,
    test_kill_session/1,
    test_close_reaps_and_cleans/1,
    test_caller_crash_stops_session/1,
    test_run_helper/1,
    test_print_is_logged/1,
    test_rlimits_apply/1,
    test_refresh_picks_new_code/1,
    test_bad_template_options/1,
    test_zygote_crash_rebuilds/1,
    test_thread_at_import_refused/1,
    test_erlang_call_during_preload/1,
    test_info_counts_forks/1,
    test_warm_pool/1,
    test_thread_at_import_spawned/1
]).

-define(MOD, py_test_session).

all() ->
    [{group, fork}, {group, spawn}, {group, fork_only}, {group, spawn_only}].

groups() ->
    Common = [
        test_sessions_are_isolated,
        test_same_start_state,
        test_env_replaced,
        test_hash_seed,
        test_random_differs,
        test_context_api,
        test_reentrance_depth,
        test_concurrent_reentrance,
        test_cross_session_call,
        test_interrupt_nested_call,
        test_session_crash,
        test_kill_session,
        test_close_reaps_and_cleans,
        test_caller_crash_stops_session,
        test_run_helper,
        test_print_is_logged,
        test_rlimits_apply,
        test_refresh_picks_new_code,
        test_bad_template_options
    ],
    [{fork, [], Common},
     {spawn, [], Common},
     {fork_only, [], [test_zygote_crash_rebuilds,
                      test_thread_at_import_refused,
                      test_erlang_call_during_preload,
                      test_info_counts_forks]},
     {spawn_only, [], [test_warm_pool,
                       test_thread_at_import_spawned]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    TestDir = filename:join(code:lib_dir(erlang_python), "test"),
    py:register_function(sess_reenter, fun([Ctx, N]) ->
        {ok, R} = py_context:call(Ctx, ?MOD, reenter, [N - 1]),
        R
    end),
    py:register_function(sess_nested_sleep, fun([Ctx, Seconds]) ->
        py_context:call(Ctx, ?MOD, sleep, [Seconds], #{}, 200)
    end),
    [{test_dir, TestDir} | Config].

end_per_suite(_Config) ->
    py:unregister_function(sess_reenter),
    py:unregister_function(sess_nested_sleep),
    ok.

init_per_group(G, Config) when G =:= fork; G =:= fork_only ->
    [{start, fork} | Config];
init_per_group(_G, Config) ->
    [{start, spawn} | Config].

end_per_group(_G, _Config) ->
    ok.

end_per_testcase(_Case, _Config) ->
    [py_session:stop_template(T) || {_, T, _, _} <- supervisor:which_children(py_session_sup)],
    ok.

%%% ============================================================================
%%% Common cases
%%% ============================================================================

%% Nothing a session changes is seen by the next one: module globals,
%% sys.modules, os.environ, threads, files in its directory.
test_sessions_are_isolated(Config) ->
    T = template(Config),
    Prev = lists:foldl(fun(I, Prev) ->
        {ok, S} = py_session:new(T),
        Mark = integer_to_binary(I),
        {ok, Prev} = py_context:call(S, ?MOD, peek, []),
        {ok, Mark} = py_context:call(S, ?MOD, mark, [Mark]),
        py_session:close(S),
        none
    end, none, lists:seq(1, 100)),
    none = Prev,
    {ok, A} = py_session:new(T),
    {ok, true} = py_context:call(A, ?MOD, put_module, [<<"sess_marker">>]),
    {ok, <<"1">>} = py_context:call(A, ?MOD, env_set, [<<"SESS_WRITE">>, <<"1">>]),
    {ok, N0} = py_context:call(A, ?MOD, thread_count, []),
    {ok, _} = py_context:call(A, ?MOD, start_thread, []),
    {ok, [<<"f">>]} = py_context:call(A, ?MOD, write_file, [<<"f">>, <<"x">>]),
    {ok, B} = py_session:new(T),
    {ok, false} = py_context:call(B, ?MOD, has_module, [<<"sess_marker">>]),
    {ok, none} = py_context:call(B, ?MOD, env_get, [<<"SESS_WRITE">>]),
    {ok, N0} = py_context:call(B, ?MOD, thread_count, []),
    {ok, []} = py_context:call(B, ?MOD, list_cwd, []),
    {ok, PA} = py_context:call(A, ?MOD, pid, []),
    {ok, PB} = py_context:call(B, ?MOD, pid, []),
    true = PA =/= PB,
    py_session:close(A),
    py_session:close(B).

%% Every session starts with the template's imports and preload; with fork
%% they ran once, in the zygote.
test_same_start_state(Config) ->
    T = template(Config, #{preload => <<"PRELOADED = 'yes'">>}),
    {ok, A} = py_session:new(T),
    {ok, B} = py_session:new(T),
    {ok, <<"yes">>} = py_context:eval(A, <<"PRELOADED">>),
    {ok, <<"yes">>} = py_context:eval(B, <<"PRELOADED">>),
    {ok, ImportedA} = py_context:call(A, ?MOD, imported_in, []),
    {ok, ImportedB} = py_context:call(B, ?MOD, imported_in, []),
    {ok, PidA} = py_context:call(A, ?MOD, pid, []),
    case ?config(start, Config) of
        fork ->
            %% imported once, by the zygote, before either session existed
            ImportedA = ImportedB,
            true = ImportedA =/= PidA;
        spawn ->
            ImportedA = PidA
    end,
    py_session:close(A),
    py_session:close(B).

%% The VM's environment does not reach a session: it sees the template's env.
test_env_replaced(Config) ->
    true = os:putenv("SESS_VM_ONLY", "leak"),
    try
        T = template(Config, #{env => #{"SESS_DECLARED" => "1"}}),
        {ok, S} = py_session:new(T),
        {ok, none} = py_context:call(S, ?MOD, env_get, [<<"SESS_VM_ONLY">>]),
        {ok, <<"1">>} = py_context:call(S, ?MOD, env_get, [<<"SESS_DECLARED">>]),
        {ok, Names} = py_context:call(S, ?MOD, env_names, []),
        false = lists:member(<<"HOME">>, Names),
        py_session:close(S)
    after
        os:unsetenv("SESS_VM_ONLY")
    end.

%% One hash seed per template: every session hashes strings and orders sets
%% the same way; another seed changes it.
test_hash_seed(Config) ->
    T1 = template(Config, #{hash_seed => 11}),
    T2 = template(Config, #{hash_seed => 12}),
    Probe = fun(T) ->
        {ok, S} = py_session:new(T),
        {ok, R} = py_context:call(S, ?MOD, hash_probe, []),
        py_session:close(S),
        R
    end,
    P = Probe(T1),
    P = Probe(T1),
    true = P =/= Probe(T2),
    ok.

%% A forked session does not inherit the zygote's random state as is.
test_random_differs(Config) ->
    T = template(Config),
    Values = [begin
                  {ok, S} = py_session:new(T),
                  {ok, V} = py_context:call(S, ?MOD, random_value, []),
                  py_session:close(S),
                  V
              end || _ <- lists:seq(1, 5)],
    5 = length(lists:usort(Values)),
    ok.

%% A session is an isolated context: the whole context API works on it.
test_context_api(Config) ->
    T = template(Config),
    {ok, S} = py_session:new(T),
    {ok, 4} = py_context:eval(S, <<"2 + 2">>),
    ok = py_context:exec(S, <<"x = 21">>),
    {ok, 42} = py_context:eval(S, <<"x * 2">>),
    {ok, 3} = py_context:call(S, ?MOD, async_add, [1, 2]),
    %% timeout interrupts the call; the session keeps working
    {error, timeout} = py_context:call(S, ?MOD, sleep, [30], #{}, 300),
    {ok, 1} = py_context:eval(S, <<"1">>),
    {ok, LSock} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
    {ok, Fd} = inet:getfd(LSock),
    {ok, ChildFd} = py_context:pass_fd(S, Fd),
    {ok, true} = py_context:call(S, ?MOD, fd_is_open, [ChildFd]),
    gen_tcp:close(LSock),
    ok = py_context:start_loop(S),
    {ok, 7} = py_context:submit_await(S, ?MOD, async_add, [3, 4]),
    ok = py_context:stop_loop(S),
    {ok, Info} = py_context:child_info(S),
    true = is_integer(maps:get(os_pid, Info)),
    py_session:close(S).

%% Python -> Erlang -> the same session -> Erlang -> ... ten levels deep.
test_reentrance_depth(Config) ->
    T = template(Config),
    {ok, S} = py_session:new(T),
    {ok, 10} = py_context:call(S, ?MOD, reenter, [10]),
    {ok, 1} = py_context:eval(S, <<"1">>),
    py_session:close(S).

test_concurrent_reentrance(Config) ->
    T = template(Config),
    Self = self(),
    Pids = [spawn_link(fun() ->
                {ok, S} = py_session:new(T),
                R = [py_context:call(S, ?MOD, reenter, [5]) || _ <- lists:seq(1, 10)],
                py_session:close(S),
                Self ! {self(), R}
            end) || _ <- lists:seq(1, 16)],
    [receive {P, R} -> R = lists:duplicate(10, {ok, 5}) after 30000 -> ct:fail(timeout) end
     || P <- Pids],
    ok.

%% A callback of session A calls into session B.
test_cross_session_call(Config) ->
    T = template(Config),
    {ok, A} = py_session:new(T),
    {ok, B} = py_session:new(T),
    {ok, <<"b">>} = py_context:call(B, ?MOD, mark, [<<"b">>]),
    py:register_function(sess_cross, fun([_N]) ->
        {ok, V} = py_context:call(B, ?MOD, peek, []),
        V
    end),
    try
        {ok, <<"b">>} = py_context:call(A, ?MOD, cross, [1]),
        {ok, none} = py_context:call(A, ?MOD, peek, [])
    after
        py:unregister_function(sess_cross)
    end,
    py_session:close(A),
    py_session:close(B).

%% A nested call that times out is interrupted alone; the outer call it
%% runs inside of completes.
test_interrupt_nested_call(Config) ->
    T = template(Config),
    {ok, S} = py_session:new(T),
    %% {error, timeout} crossed into Python and back: atoms are strings there
    {ok, {<<"outer-done">>, {<<"error">>, <<"timeout">>}}} =
        py_context:call(S, ?MOD, nested_sleep_then, [30]),
    {ok, 1} = py_context:eval(S, <<"1">>),
    py_session:close(S).

%% A session whose child dies answers with the reason until closed; the
%% template and other sessions are untouched.
test_session_crash(Config) ->
    T = template(Config),
    {ok, A} = py_session:new(T),
    {ok, B} = py_session:new(T),
    {error, {child_exited, {signal, 6}}} = py_context:call(A, ?MOD, abort, []),
    {error, {child_exited, {signal, 6}}} = py_context:eval(A, <<"1">>),
    true = is_process_alive(A),
    {ok, 1} = py_context:eval(B, <<"1">>),
    {ok, C} = py_session:new(T),
    {ok, 1} = py_context:eval(C, <<"1">>),
    [py_session:close(X) || X <- [A, B, C]],
    false = is_process_alive(A),
    ok.

test_kill_session(Config) ->
    T = template(Config),
    {ok, S} = py_session:new(T),
    {ok, Pid} = py_context:call(S, ?MOD, pid, []),
    ok = py_context:kill(S),
    {error, killed} = py_context:eval(S, <<"1">>),
    ok = wait_gone(Pid),
    py_session:close(S).

%% close/1 kills the child (reaped, no zombie) and removes its directory.
test_close_reaps_and_cleans(Config) ->
    T = template(Config),
    {ok, S} = py_session:new(T),
    {ok, Pid} = py_context:call(S, ?MOD, pid, []),
    {ok, Cwd} = py_context:call(S, ?MOD, cwd, []),
    true = filelib:is_dir(Cwd),
    ok = py_session:close(S),
    ok = wait_gone(Pid),
    ok = wait_no_dir(Cwd, 100),
    ok.

%% removed by the context process right after close/1 returns
wait_no_dir(Dir, 0) ->
    ct:fail({still_there, Dir});
wait_no_dir(Dir, N) ->
    case filelib:is_dir(Dir) of
        false -> ok;
        true -> timer:sleep(20), wait_no_dir(Dir, N - 1)
    end.

%% A session is linked to the process that created it.
test_caller_crash_stops_session(Config) ->
    T = template(Config),
    Self = self(),
    {Owner, Mon} = spawn_monitor(fun() ->
        {ok, S} = py_session:new(T),
        {ok, Pid} = py_context:call(S, ?MOD, pid, []),
        Self ! {session, S, Pid},
        receive never -> ok end
    end),
    {S, Pid} = receive {session, S0, P0} -> {S0, P0} after 10000 -> ct:fail(no_session) end,
    SMon = erlang:monitor(process, S),
    exit(Owner, crash),
    receive {'DOWN', Mon, process, Owner, crash} -> ok end,
    receive {'DOWN', SMon, process, S, _} -> ok after 5000 -> ct:fail(session_survived) end,
    ok = wait_gone(Pid).

test_run_helper(Config) ->
    T = template(Config),
    {ok, 42} = py_session:run(T, builtins, int, [<<"42">>]),
    {ok, none} = py_session:run(T, ?MOD, peek, []),
    {error, {'ValueError', _}} = py_session:run(T, builtins, int, [<<"x">>]),
    %% nothing left behind
    #{sessions := 0} = wait_sessions(T, 0),
    ok.

%% print() in a session ends up in the Erlang logger: through the port for a
%% spawned child, as log events for a forked one (whose stdio is detached).
test_print_is_logged(Config) ->
    #{level := Level} = logger:get_primary_config(),
    ok = logger:set_primary_config(level, info),
    ok = logger:add_handler(sess_capture, ?MODULE, #{config => #{pid => self()}}),
    try
        T = template(Config),
        {ok, S} = py_session:new(T),
        {ok, none} = py_context:eval(S, <<"print('hello-from-session', flush=True)">>),
        ok = wait_logged(<<"hello-from-session">>, 50),
        py_session:close(S)
    after
        logger:remove_handler(sess_capture),
        logger:set_primary_config(level, Level)
    end.

log(#{msg := Msg}, #{config := #{pid := Pid}}) ->
    Text = case Msg of
        {string, S} -> S;
        {report, R} -> io_lib:format("~p", [R]);
        {Format, Args} -> io_lib:format(Format, Args)
    end,
    Pid ! {logged, unicode:characters_to_binary(Text)},
    ok.

wait_logged(_Needle, 0) ->
    ct:fail(not_logged);
wait_logged(Needle, N) ->
    receive
        {logged, Text} ->
            case binary:match(Text, Needle) of
                nomatch -> wait_logged(Needle, N);
                _ -> ok
            end
    after 100 ->
        wait_logged(Needle, N - 1)
    end.

test_rlimits_apply(Config) ->
    T = template(Config, #{rlimits => #{nofile => 64}}),
    {ok, S} = py_session:new(T),
    {ok, 64} = py_context:eval(S, <<"__import__('resource').getrlimit(__import__('resource').RLIMIT_NOFILE)[0]">>),
    py_session:close(S).

%% refresh/1 prepares the template again: new sessions see the new code,
%% live ones keep what they started with.
test_refresh_picks_new_code(Config) ->
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(?config(start, Config))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    File = filename:join(Dir, "sess_versioned.py"),
    ok = file:write_file(File, <<"VERSION = 1\n">>),
    T = template(Config, #{paths => [Dir], imports => [sess_versioned]}),
    {ok, Old} = py_session:new(T),
    {ok, 1} = py_context:eval(Old, <<"__import__('sess_versioned').VERSION">>),
    ok = file:write_file(File, <<"VERSION = 2\n">>),
    _ = file:del_dir_r(filename:join(Dir, "__pycache__")),
    ok = py_session:refresh(T),
    {ok, New} = py_session:new(T),
    {ok, 2} = py_context:eval(New, <<"__import__('sess_versioned').VERSION">>),
    {ok, 1} = py_context:eval(Old, <<"__import__('sess_versioned').VERSION">>),
    py_session:close(Old),
    py_session:close(New).

test_bad_template_options(Config) ->
    Start = ?config(start, Config),
    {error, {badarg, {start, nope}}} = py_session:template(#{start => nope}),
    {error, {badarg, {hash_seed, -1}}} = py_session:template(#{start => Start, hash_seed => -1}),
    {error, {python_not_found, _}} = py_session:template(#{start => Start, python => "/no/such/python"}),
    case Start of
        fork ->
            {error, {template_failed, {init_failed, {'ModuleNotFoundError', _}}}} =
                py_session:template(#{start => fork, imports => [no_such_module_xyz]});
        spawn ->
            ok
    end,
    ok.

%%% ============================================================================
%%% fork only
%%% ============================================================================

%% A zygote that dies is rebuilt; the sessions it forked keep working.
test_zygote_crash_rebuilds(Config) ->
    T = template(Config),
    {ok, S} = py_session:new(T),
    #{zygotes := [#{os_pid := Z1}]} = py_session:info(T),
    _ = py_nif:os_kill(Z1, 9),
    Z2 = wait_new_zygote(T, Z1, 50),
    true = Z2 =/= Z1,
    {ok, 1} = py_context:eval(S, <<"1">>),
    {ok, Pid} = py_context:call(S, ?MOD, pid, []),
    {ok, S2} = py_session:new(T),
    {ok, 2} = py_context:eval(S2, <<"2">>),
    %% the orphaned session is still reaped when closed
    py_session:close(S),
    ok = wait_gone(Pid),
    py_session:close(S2).

%% A thread started at import would be lost in every fork: refused.
test_thread_at_import_refused(Config) ->
    {error, {template_failed, {init_failed, {threads, [<<"import-time-thread">>]}}}} =
        py_session:template(#{start => fork, paths => [?config(test_dir, Config)],
                              imports => [py_test_session_thread]}),
    ok.

%% `erlang' can be imported by template code, but not called before a
%% session exists.
test_erlang_call_during_preload(Config) ->
    T = template(Config, #{preload =>
        <<"import py_test_session\nPRELOAD_CALL = py_test_session.call_during_preload()">>}),
    {ok, S} = py_session:new(T),
    {ok, Msg} = py_context:eval(S, <<"PRELOAD_CALL">>),
    {match, _} = re:run(Msg, <<"not connected">>),
    py_session:close(S).

test_info_counts_forks(Config) ->
    T = template(Config, #{zygotes => 2}),
    #{start := fork, zygotes := [_, _], forks := 0} = py_session:info(T),
    Ss = [begin {ok, S} = py_session:new(T), S end || _ <- lists:seq(1, 4)],
    #{forks := 4, sessions := 4} = py_session:info(T),
    [py_session:close(S) || S <- Ss],
    #{sessions := 0} = wait_sessions(T, 0),
    ok.

%%% ============================================================================
%%% spawn only
%%% ============================================================================

test_warm_pool(Config) ->
    T = template(Config, #{warm => 2}),
    #{warm := 2} = wait_warm(T, 2, 100),
    {Us, {ok, S}} = timer:tc(fun() -> py_session:new(T) end),
    ct:pal("session from the warm pool in ~p us", [Us]),
    {links, Links} = process_info(S, links),
    true = lists:member(self(), Links),
    false = lists:member(T, Links),
    {ok, 1} = py_context:eval(S, <<"1">>),
    #{warm := 2} = wait_warm(T, 2, 100),
    py_session:close(S).

%% spawn starts each session from scratch, so import-time threads are fine.
test_thread_at_import_spawned(Config) ->
    T = template(Config, #{imports => [py_test_session_thread]}),
    {ok, S} = py_session:new(T),
    {ok, 2} = py_context:call(S, ?MOD, thread_count, []),
    py_session:close(S).

%%% ============================================================================
%%% Helpers
%%% ============================================================================

template(Config) ->
    template(Config, #{}).

template(Config, Extra) ->
    TestDir = ?config(test_dir, Config),
    Paths = [TestDir | maps:get(paths, Extra, [])],
    Imports = [?MOD | maps:get(imports, Extra, [])],
    Opts = maps:merge(#{start => ?config(start, Config)},
                      Extra#{paths => Paths, imports => Imports}),
    {ok, T} = py_session:template(Opts),
    T.

wait_gone(Pid) ->
    wait_gone(Pid, 100).

wait_gone(Pid, 0) ->
    ct:fail({still_alive, Pid});
wait_gone(Pid, N) ->
    case py_child:os_pid_alive(Pid) of
        false -> ok;
        true -> timer:sleep(20), wait_gone(Pid, N - 1)
    end.

wait_new_zygote(T, Old, 0) ->
    ct:fail({zygote_not_rebuilt, Old, py_session:info(T)});
wait_new_zygote(T, Old, N) ->
    case py_session:info(T) of
        #{zygotes := [#{os_pid := P}]} when P =/= Old -> P;
        _ -> timer:sleep(50), wait_new_zygote(T, Old, N - 1)
    end.

wait_sessions(T, Want) ->
    wait_sessions(T, Want, 100).

wait_sessions(T, _Want, 0) ->
    py_session:info(T);
wait_sessions(T, Want, N) ->
    case py_session:info(T) of
        #{sessions := Want} = I -> I;
        _ -> timer:sleep(20), wait_sessions(T, Want, N - 1)
    end.

wait_warm(T, _Want, 0) ->
    py_session:info(T);
wait_warm(T, Want, N) ->
    case py_session:info(T) of
        #{warm := Want} = I -> I;
        _ -> timer:sleep(50), wait_warm(T, Want, N - 1)
    end.
