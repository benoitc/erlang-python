%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc A prepared Python environment that sessions start from.
%%%
%%% `start => fork' keeps one or more zygotes (priv/py_zygote.py): Python
%%% processes that ran the template's imports and preload once and fork a
%%% fresh child per session. The zygote reports each child's exit here and
%%% the template passes it to the session's context as
%%% `{py_session_exited, OsPid, Code}'.
%%%
%%% `start => spawn' starts each session as a new isolated child and can keep
%%% `warm' of them started ahead of time; each is handed out once.
%%%
%%% `start => reimport' keeps `contexts' worker or owngil contexts; each
%%% py_session:run/5 runs in a fresh module dictionary on one of them
%%% (priv/_erlang_impl/_reimport.py), in the way of Temporal's workflow
%%% sandbox. There is no session process: new/1 is not available.
%%%
%%% A crashed zygote is rebuilt; the sessions it forked are separate
%%% processes and keep running.
%%%
%%% @private
%%%
%%% Owns: the zygote ports and control sockets, or the warm contexts.
%%% Talks to: `py_isolated' (fork requests, exit reports), `py_session'.
%%% Never: runs session requests; those go straight to the session context.
-module(py_session_template).

-behaviour(gen_server).

-export([start_link/1,
         fork/4,
         checkout/2,
         refresh/1,
         info/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(STATUS_REQUEST, 0).
-define(STATUS_ERROR, 1).
-define(STATUS_OK, 2).
-define(STATUS_EVENT, 4).
-define(DEFAULT_START_TIMEOUT_MS, 10000).
-define(MAX_REBUILDS, 5).
-define(REBUILD_PERIOD_MS, 10000).

-record(zygote, {
    port :: port(),
    os_pid :: non_neg_integer(),
    listener :: socket:socket(),
    sock :: socket:socket(),
    buf = <<>> :: binary(),
    info = #{} :: map()
}).

-record(st, {
    opts :: map(),
    start :: fork | spawn | reimport,
    %% start => reimport: the contexts runs go to, and what they share
    contexts = [] :: [pid()],
    passthrough = [] :: [binary()],
    zygotes = [] :: [#zygote{}],
    next = 0 :: non_neg_integer(),
    next_id = 1 :: pos_integer(),
    %% Id => From of fork requests the zygote has
    pending = #{} :: #{pos_integer() => {gen_server:from(), pid()}},
    %% OsPid => context pid of live forked sessions
    owners = #{} :: #{pos_integer() => pid()},
    %% start => spawn: contexts started ahead of time
    warm = [] :: [pid()],
    forks = 0 :: non_neg_integer(),
    build_ms = 0 :: non_neg_integer(),
    rebuilds = [] :: [integer()]
}).

%% ============================================================================
%% API
%% ============================================================================

-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%% @doc Fork a session child that connects to SockPath. Called by the
%% session's context process, which receives the child's exit.
-spec fork(pid(), file:filename(), map(), timeout()) -> {ok, pos_integer()} | {error, term()}.
fork(T, SockPath, ForkOpts, Timeout) ->
    try gen_server:call(T, {fork, SockPath, ForkOpts}, Timeout)
    catch
        exit:{timeout, _} -> {error, fork_timeout};
        exit:{noproc, _} -> {error, template_stopped};
        exit:{Reason, _} -> {error, {template_down, Reason}}
    end.

%% @doc A context started ahead of time (`{ok, Ctx}', linked to the
%% template until handed over), or the options to start one.
-spec checkout(pid(), timeout()) ->
    {ok, pid()} | {start, map()} | {reimport, pid(), [binary()]} | {error, term()}.
checkout(T, Timeout) ->
    try gen_server:call(T, checkout, Timeout)
    catch
        exit:{noproc, _} -> {error, template_stopped};
        exit:{Reason, _} -> {error, {template_down, Reason}}
    end.

-spec refresh(pid()) -> ok | {error, term()}.
refresh(T) ->
    gen_server:call(T, refresh, infinity).

-spec info(pid()) -> map().
info(T) ->
    gen_server:call(T, info).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    case check_opts(Opts) of
        ok ->
            St = #st{opts = Opts, start = maps:get(start, Opts, fork)},
            case build(St) of
                {ok, St1} -> {ok, St1};
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

handle_call({fork, _Path, _ForkOpts}, _From, #st{start = spawn} = St) ->
    {reply, {error, not_a_fork_template}, St};
handle_call({fork, _Path, _ForkOpts}, _From, #st{zygotes = []} = St) ->
    {reply, {error, no_zygote}, St};
handle_call({fork, Path, ForkOpts}, {Pid, _} = From, St) ->
    #st{zygotes = Zs, next = N, next_id = Id, pending = Pending} = St,
    Z = lists:nth(N rem length(Zs) + 1, Zs),
    Term = {fork, py_child:to_bin(Path), Pid, fork_opts(ForkOpts)},
    case py_child:send_frame(Z#zygote.sock, Id, ?STATUS_REQUEST, Term) of
        ok ->
            {noreply, St#st{next = N + 1, next_id = Id + 1,
                            pending = Pending#{Id => {From, Pid}}}};
        {error, Reason} ->
            {reply, {error, {zygote_unreachable, Reason}}, St}
    end;
handle_call(checkout, _From, #st{start = reimport, contexts = Cs, next = N,
                                 passthrough = PT} = St) ->
    {reply, {reimport, lists:nth(N rem length(Cs) + 1, Cs), PT}, St#st{next = N + 1}};
handle_call(checkout, _From, #st{start = fork} = St) ->
    {reply, {start, context_opts(St)}, St};
handle_call(checkout, _From, #st{warm = [Ctx | Rest]} = St) ->
    self() ! fill_warm,
    {reply, {ok, Ctx}, St#st{warm = Rest}};
handle_call(checkout, _From, #st{warm = []} = St) ->
    self() ! fill_warm,
    {reply, {start, context_opts(St)}, St};
handle_call(refresh, _From, #st{start = fork, zygotes = Old} = St) ->
    case build(St#st{zygotes = []}) of
        {ok, St1} ->
            %% The old zygotes exit when their control socket closes; the
            %% sessions they forked keep running and are watched by pid
            [retire(Z) || Z <- Old],
            {reply, ok, St1#st{owners = #{}}};
        {error, Reason} ->
            {reply, {error, Reason}, St}
    end;
handle_call(refresh, _From, #st{start = reimport, contexts = Old} = St) ->
    case build(St#st{contexts = []}) of
        {ok, St1} ->
            [begin unlink(C), py_context:stop(C) end || C <- Old],
            {reply, ok, St1};
        {error, Reason} ->
            {reply, {error, Reason}, St}
    end;
handle_call(refresh, _From, #st{start = spawn, warm = Warm} = St) ->
    [begin unlink(C), py_context:stop(C) end || C <- Warm],
    self() ! fill_warm,
    {reply, ok, St#st{warm = []}};
handle_call(info, _From, St) ->
    {reply, info_map(St), St};
handle_call(_Other, _From, St) ->
    {reply, {error, unknown_request}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({'$socket', S, select, _}, St) ->
    case lists:keyfind(S, #zygote.sock, St#st.zygotes) of
        false -> {noreply, St};
        Z -> {noreply, drain(Z, St)}
    end;
handle_info({'$socket', _, _, _}, St) ->
    %% abort: the port's exit_status follows and drives the rebuild
    {noreply, St};
handle_info({Port, {data, Out}}, St) when is_port(Port) ->
    [logger:info("py_session template ~p: ~s", [self(), L])
     || L <- binary:split(Out, <<"\n">>, [global, trim_all])],
    {noreply, St};
handle_info({Port, {exit_status, Status}}, St) when is_port(Port) ->
    case lists:keytake(Port, #zygote.port, St#st.zygotes) of
        {value, Z, Rest} -> zygote_exited(Z, py_child:exit_reason(Status), St#st{zygotes = Rest});
        false -> {noreply, St}
    end;
handle_info(fill_warm, #st{start = spawn, warm = Warm, opts = Opts} = St) ->
    case length(Warm) < maps:get(warm, Opts, 0) of
        true ->
            case py_context:start_link(erlang:unique_integer([positive]), isolated,
                                       context_opts(St)) of
                {ok, Ctx} ->
                    self() ! fill_warm,
                    {noreply, St#st{warm = Warm ++ [Ctx]}};
                {error, Reason} ->
                    logger:warning("py_session template ~p: warm session failed: ~p",
                                   [self(), Reason]),
                    {noreply, St}
            end;
        false ->
            {noreply, St}
    end;
handle_info(fill_warm, St) ->
    {noreply, St};
handle_info({'EXIT', Pid, Reason}, #st{start = reimport, contexts = Cs} = St) ->
    case lists:member(Pid, Cs) of
        true ->
            %% A context of the template stopped: put a new one in its place
            logger:warning("py_session template ~p: context ~p exited: ~p",
                           [self(), Pid, Reason]),
            case start_context(St#st.opts) of
                {ok, C} ->
                    {noreply, St#st{contexts = [C | lists:delete(Pid, Cs)]}};
                {error, Why} ->
                    {stop, {context_restart_failed, Why}, St}
            end;
        false ->
            {noreply, St}
    end;
handle_info({'EXIT', Pid, _Reason}, #st{warm = Warm} = St) ->
    %% A warm session died before anyone took it
    {noreply, St#st{warm = lists:delete(Pid, Warm)}};
handle_info(_Msg, St) ->
    {noreply, St}.

terminate(_Reason, #st{zygotes = Zs, warm = Warm, contexts = Cs}) ->
    [retire(Z) || Z <- Zs],
    [py_context:stop(C) || C <- Warm ++ Cs],
    ok.

%% ============================================================================
%% Building
%% ============================================================================

check_opts(Opts) ->
    Checks = [
        fun() -> case maps:get(start, Opts, fork) of
                     S when S =:= fork; S =:= spawn; S =:= reimport -> ok;
                     S -> {error, {badarg, {start, S}}}
                 end end,
        fun() -> case {maps:get(start, Opts, fork), maps:get(mode, Opts, worker)} of
                     {reimport, M} when M =:= worker; M =:= owngil -> ok;
                     {reimport, M} -> {error, {badarg, {mode, M}}};
                     _ -> ok
                 end end,
        fun() -> case maps:get(contexts, Opts, 1) of
                     N when is_integer(N), N >= 1 -> ok;
                     N -> {error, {badarg, {contexts, N}}}
                 end end,
        fun() -> case maps:get(zygotes, Opts, 1) of
                     N when is_integer(N), N >= 1 -> ok;
                     N -> {error, {badarg, {zygotes, N}}}
                 end end,
        fun() -> case maps:get(warm, Opts, 0) of
                     N when is_integer(N), N >= 0 -> ok;
                     N -> {error, {badarg, {warm, N}}}
                 end end,
        fun() -> py_child:check_env_opts(env_opts(Opts)) end,
        fun() -> case {maps:get(cgroup, Opts, undefined), os:type()} of
                     {undefined, _} -> ok;
                     {_, {unix, linux}} -> ok;
                     {_, {unix, Os}} -> {error, {cgroup_unsupported, Os}}
                 end end,
        fun() -> case py_child:python_executable(Opts) of
                     {error, _} = Err -> Err;
                     _ -> ok
                 end end
    ],
    lists:foldl(fun(Check, ok) -> Check(); (_, Err) -> Err end, ok, Checks).

%% Sessions see only the template's env unless it asks otherwise
env_opts(Opts) ->
    maps:merge(#{clear_env => true}, maps:with([env, clear_env, hash_seed], Opts)).

build(#st{start = spawn} = St) ->
    self() ! fill_warm,
    {ok, St};
build(#st{start = reimport, opts = Opts} = St) ->
    T0 = erlang:monotonic_time(millisecond),
    Started = [start_context(Opts) || _ <- lists:seq(1, maps:get(contexts, Opts, 1))],
    case [E || {error, _} = E <- Started] of
        [] ->
            PT = lists:usort([py_child:to_bin(M) || M <- maps:get(imports, Opts, [])
                                                    ++ maps:get(passthrough, Opts, [])]),
            {ok, St#st{contexts = [C || {ok, C} <- Started], passthrough = PT,
                       build_ms = erlang:monotonic_time(millisecond) - T0}};
        [Err | _] ->
            [py_context:stop(C) || {ok, C} <- Started],
            Err
    end;
build(#st{opts = Opts} = St) ->
    T0 = erlang:monotonic_time(millisecond),
    case build_zygotes(maps:get(zygotes, Opts, 1), St#st.opts, []) of
        {ok, Zs} ->
            {ok, St#st{zygotes = Zs, build_ms = erlang:monotonic_time(millisecond) - T0}};
        {error, _} = Err ->
            Err
    end.

build_zygotes(0, _Opts, Acc) ->
    {ok, lists:reverse(Acc)};
build_zygotes(N, Opts, Acc) ->
    case start_zygote(Opts) of
        {ok, Z} ->
            build_zygotes(N - 1, Opts, [Z | Acc]);
        {error, _} = Err ->
            [retire(Z) || Z <- Acc],
            Err
    end.

start_zygote(Opts) ->
    Python = py_child:python_executable(Opts),
    Path = py_child:new_sock_path("tpl_"),
    Timeout = maps:get(start_timeout, Opts, ?DEFAULT_START_TIMEOUT_MS),
    case py_child:listen(Path) of
        {ok, L} ->
            Script = filename:join(py_child:priv_dir(), "py_zygote.py"),
            Port = open_port({spawn_executable, Python},
                             [exit_status, stderr_to_stdout, binary, use_stdio,
                              {args, [Script, Path]},
                              {env, py_child:port_env(env_opts(Opts))}]),
            OsPid = case erlang:port_info(Port, os_pid) of
                {os_pid, P} -> P;
                _ -> 0
            end,
            Result = case py_child:accept(L, {port, Port}, Timeout) of
                {ok, S} ->
                    py_child:tune_socket(S),
                    Z = #zygote{port = Port, os_pid = OsPid, listener = L, sock = S},
                    prepare(Z, Opts, Timeout);
                {error, _} = Err ->
                    Err
            end,
            py_child:delete_file(Path),
            case Result of
                {ok, _} = Ok ->
                    Ok;
                {error, Reason} ->
                    socket:close(L),
                    py_child:kill_os_pid(OsPid),
                    close_port(Port),
                    {error, {template_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {template_failed, Reason}}
    end.

%% A context for re-import runs: the paths, the imports (shared by every
%% run) and the preload are applied once.
start_context(Opts) ->
    CtxOpts = maps:merge(#{mode => maps:get(mode, Opts, worker)},
                         maps:with([preload], Opts)),
    case py_context:new(CtxOpts) of
        {ok, C} ->
            Paths = [py_child:to_list(P) || P <- maps:get(paths, Opts, [])],
            Setup = iolist_to_binary(io_lib:format(
                "import sys, importlib
"
                "for _p in reversed(~p):
"
                "    if _p not in sys.path: sys.path.insert(0, _p)
"
                "for _m in ~p: importlib.import_module(_m)
"
                "import _erlang_impl._reimport
",
                [Paths, [py_child:to_list(M) || M <- maps:get(imports, Opts, [])]])),
            case py_context:exec(C, Setup) of
                ok ->
                    {ok, C};
                {error, Reason} ->
                    py_context:stop(C),
                    {error, {template_failed, {init_failed, Reason}}}
            end;
        {error, Reason} ->
            {error, {template_failed, Reason}}
    end.

%% Wait for `ready', run the imports and preload, then arm the socket.
prepare(Z, Opts, Timeout) ->
    case recv_sync(Z, Timeout) of
        {ok, {0, ?STATUS_EVENT, {ready, Info}}, Z1} ->
            Paths = [py_child:to_bin(P) || P <- py_import:all_paths()]
                    ++ [py_child:to_bin(P) || P <- maps:get(paths, Opts, [])],
            Imports = lists:usort([py_child:to_bin(M) || {M, _} <- py_import:all_imports()]
                                  ++ [py_child:to_bin(M) || M <- maps:get(imports, Opts, [])]),
            Preload = iolist_to_binary([preload_code(), <<"\n">>,
                                        maps:get(preload, Opts, <<>>)]),
            ok = py_child:send_frame(Z1#zygote.sock, 1, ?STATUS_REQUEST,
                                     {init, Paths, Imports, Preload}),
            case recv_sync(Z1, Timeout) of
                {ok, {1, ?STATUS_OK, _}, Z2} ->
                    {ok, arm(Z2#zygote{info = Info})};
                {ok, {1, ?STATUS_ERROR, Why}, _} ->
                    {error, {init_failed, Why}};
                {ok, Other, _} ->
                    {error, {unexpected_frame, Other}};
                {error, _} = Err ->
                    Err
            end;
        {ok, Other, _} ->
            {error, {unexpected_frame, Other}};
        {error, _} = Err ->
            Err
    end.

preload_code() ->
    try py_preload:get_code() of
        Code when is_binary(Code) -> Code;
        _ -> <<>>
    catch
        _:_ -> <<>>
    end.

recv_sync(#zygote{sock = S, buf = Buf, port = Port} = Z, Timeout) ->
    case py_child:parse_frame(Buf) of
        {ok, Frame, Rest} ->
            {ok, Frame, Z#zygote{buf = Rest}};
        more ->
            case socket:recv(S, 0, Timeout) of
                {ok, Data} ->
                    recv_sync(Z#zygote{buf = <<Buf/binary, Data/binary>>}, Timeout);
                {error, Reason} ->
                    {error, {Reason, drain_output(Port, [])}}
            end;
        {error, _} = Err ->
            Err
    end.

drain_output(Port, Acc) ->
    receive
        {Port, {data, D}} -> drain_output(Port, [D | Acc])
    after 50 ->
        iolist_to_binary(lists:reverse(Acc))
    end.

%% Arm the select; frames already buffered are processed first
arm(#zygote{} = Z) ->
    self() ! {'$socket', Z#zygote.sock, select, arm},
    Z.

retire(#zygote{sock = S, listener = L, port = Port}) ->
    _ = socket:close(S),
    _ = socket:close(L),
    close_port(Port).

close_port(Port) ->
    try port_close(Port) catch error:badarg -> ok end,
    ok.

%% ============================================================================
%% Frames from a zygote
%% ============================================================================

drain(#zygote{sock = S, buf = Buf} = Z, St) ->
    case socket:recv(S, 0, nowait) of
        {ok, Data} ->
            drain(Z#zygote{buf = <<Buf/binary, Data/binary>>}, St);
        {select, _} ->
            frames(Z, St);
        {error, _} ->
            %% The port's exit_status follows
            frames(Z, St)
    end.

frames(#zygote{buf = Buf} = Z, St) ->
    case py_child:parse_frame(Buf) of
        {ok, Frame, Rest} ->
            St1 = frame(Frame, St),
            frames(Z#zygote{buf = Rest}, St1);
        more ->
            store(Z, St);
        {error, Reason} ->
            logger:error("py_session template ~p: bad frame from zygote ~p: ~p",
                         [self(), Z#zygote.os_pid, Reason]),
            py_child:kill_os_pid(Z#zygote.os_pid),
            store(Z#zygote{buf = <<>>}, St)
    end.

store(Z, #st{zygotes = Zs} = St) ->
    St#st{zygotes = lists:keyreplace(Z#zygote.sock, #zygote.sock, Zs, Z)}.

frame({Id, Status, Term}, #st{pending = Pending, owners = Owners} = St)
        when Status =:= ?STATUS_OK; Status =:= ?STATUS_ERROR ->
    case maps:take(Id, Pending) of
        {{From, Pid}, Rest} when Status =:= ?STATUS_OK ->
            gen_server:reply(From, {ok, Term}),
            St#st{pending = Rest, owners = Owners#{Term => Pid}, forks = St#st.forks + 1};
        {{From, _Pid}, Rest} ->
            gen_server:reply(From, {error, {fork_failed, Term}}),
            St#st{pending = Rest};
        error ->
            St
    end;
frame({_, ?STATUS_EVENT, {exited, OsPid, Code}}, #st{owners = Owners} = St) ->
    case maps:take(OsPid, Owners) of
        {Pid, Rest} ->
            Pid ! {py_session_exited, OsPid, Code},
            St#st{owners = Rest};
        error ->
            St
    end;
frame(_, St) ->
    St.

%% ============================================================================
%% Zygote failure
%% ============================================================================

zygote_exited(Z, Reason, #st{pending = Pending} = St) ->
    logger:warning("py_session template ~p: zygote ~p exited: ~p",
                   [self(), Z#zygote.os_pid, Reason]),
    retire(Z),
    %% Forks it had not answered fail; the sessions it forked keep running
    %% and their contexts watch them by pid
    [gen_server:reply(From, {error, {zygote_exited, Reason}}) || {From, _} <- maps:values(Pending)],
    St1 = St#st{pending = #{}},
    Now = erlang:monotonic_time(millisecond),
    Recent = [T || T <- St1#st.rebuilds, Now - T =< ?REBUILD_PERIOD_MS],
    case length(Recent) < ?MAX_REBUILDS of
        true ->
            case start_zygote(St1#st.opts) of
                {ok, NewZ} ->
                    {noreply, St1#st{zygotes = St1#st.zygotes ++ [NewZ],
                                     rebuilds = [Now | Recent]}};
                {error, Why} ->
                    {stop, {zygote_rebuild_failed, Why}, St1}
            end;
        false ->
            {stop, {zygote_exited, Reason}, St1}
    end.

%% ============================================================================
%% Helpers
%% ============================================================================

%% Options of the isolated context behind each session
context_opts(#st{start = fork, opts = Opts}) ->
    maps:merge(maps:with([rlimits, cgroup, start_timeout, kill_after], Opts),
               #{mode => isolated, session => true, restart => false,
                 origin => {fork, self()}});
context_opts(#st{start = spawn, opts = Opts}) ->
    maps:merge(maps:with([python, paths, preload, rlimits, cgroup, start_timeout,
                          kill_after], Opts),
               (env_opts(Opts))#{mode => isolated, session => true, restart => false,
                                 origin => spawn}).

fork_opts(ForkOpts) ->
    maps:fold(fun(rlimits, V, Acc) -> Acc#{rlimits => V};
                 (cgroup, V, Acc) -> Acc#{cgroup => py_child:to_bin(V)};
                 (cd, V, Acc) -> Acc#{cd => py_child:to_bin(V)};
                 (_, _, Acc) -> Acc
              end, #{}, ForkOpts).

info_map(#st{start = Start, zygotes = Zs, owners = Owners, forks = Forks,
             build_ms = BuildMs, warm = Warm, opts = Opts, contexts = Cs}) ->
    #{start => Start,
      contexts => Cs,
      zygotes => [Info#{os_pid => P} || #zygote{os_pid = P, info = Info} <- Zs],
      sessions => map_size(Owners),
      forks => Forks,
      build_ms => BuildMs,
      warm => length(Warm),
      hash_seed => maps:get(hash_seed, Opts, random)}.
