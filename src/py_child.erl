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

%%% @doc Plumbing shared by the processes that drive a Python child over a
%%% Unix socket: `py_isolated' (one context, one child) and
%%% `py_session_template' (the zygote sessions are forked from).
%%%
%%% Frames are `<<Id:64/native, Len:32/native, Status:8, ETF/binary>>',
%%% the format of the blocking callback pipe.
%%%
%%% @private
%%%
%%% Owns: nothing; pure helpers and the listen/accept sequence.
%%% Never: keeps state between calls.
-module(py_child).

-export([python_executable/1,
         priv_dir/0,
         sock_dir/0,
         new_sock_path/1,
         listen/1,
         accept/3,
         tune_socket/1,
         port_env/1,
         check_env_opts/1,
         rlimit_args/1,
         cgroup_args/1,
         frame/3,
         send_frame/4,
         parse_frame/1,
         exit_reason/1,
         kill_os_pid/1,
         to_bin/1,
         to_list/1]).

-define(SOCKET_BUF, 1024 * 1024).

%% @doc Python executable used for children: the `python' option, then the
%% `isolated_python' application env, then the interpreter matching the
%% embedded runtime, then `python3' from PATH.
-spec python_executable(map()) -> string() | {error, term()}.
python_executable(Opts) ->
    Candidate = case maps:get(python, Opts, undefined) of
        undefined ->
            case application:get_env(erlang_python, isolated_python) of
                {ok, P} -> P;
                undefined -> default_python()
            end;
        P -> P
    end,
    resolve_exe(to_list(Candidate)).

default_python() ->
    case persistent_term:get({?MODULE, python}, undefined) of
        undefined ->
            Exe = try py:python_executable() catch _:_ -> "python3" end,
            persistent_term:put({?MODULE, python}, Exe),
            Exe;
        Exe ->
            Exe
    end.

resolve_exe(Exe) ->
    case filename:pathtype(Exe) of
        absolute ->
            case filelib:is_file(Exe) of
                true -> Exe;
                false -> {error, {python_not_found, Exe}}
            end;
        _ ->
            case os:find_executable(Exe) of
                false -> {error, {python_not_found, Exe}};
                Found -> Found
            end
    end.

-spec priv_dir() -> file:filename().
priv_dir() ->
    case code:priv_dir(erlang_python) of
        {error, bad_name} ->
            filename:join(filename:dirname(filename:dirname(code:which(?MODULE))), "priv");
        Dir ->
            Dir
    end.

%% @doc Private directory for the sockets of this node (mode 0700). Kept
%% under `$TMPDIR': a Unix socket path is limited to 104 bytes.
-spec sock_dir() -> file:filename().
sock_dir() ->
    Base = case os:getenv("TMPDIR") of
        false -> "/tmp";
        T -> T
    end,
    Dir = filename:join(Base, "erlang_python_" ++ os:getpid()),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    _ = file:change_mode(Dir, 8#700),
    Dir.

-spec new_sock_path(string()) -> file:filename().
new_sock_path(Prefix) ->
    filename:join(sock_dir(),
                  Prefix ++ integer_to_list(erlang:unique_integer([positive])) ++ ".sock").

%% @doc Listening socket at Path, for one child to connect to.
-spec listen(file:filename()) -> {ok, socket:socket()} | {error, term()}.
listen(Path) ->
    _ = file:delete(Path),
    case socket:open(local, stream, default) of
        {ok, L} ->
            case socket:bind(L, #{family => local, path => Path}) of
                ok ->
                    case socket:listen(L) of
                        ok -> {ok, L};
                        {error, _} = Err -> socket:close(L), Err
                    end;
                {error, _} = Err ->
                    socket:close(L), Err
            end;
        {error, Reason} ->
            {error, {socket_open_failed, Reason}}
    end.

%% @doc Accept the child's connection while watching its port, so a child
%% that dies before connecting (bad interpreter, missing script) is reported
%% with its output instead of timing out.
-spec accept(socket:socket(), {port, port()}, timeout()) ->
    {ok, socket:socket()} | {error, term()}.
accept(L, Watch, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    accept(L, Watch, Deadline, []).

accept(L, Watch, Deadline, Out) ->
    case socket:accept(L, nowait) of
        {ok, S} ->
            %% Output printed before connecting is still worth logging
            {port, Port} = Watch,
            [self() ! {Port, {data, D}} || D <- lists:reverse(Out)],
            {ok, S};
        {select, {select_info, _, Handle}} ->
            Left = max(0, Deadline - erlang:monotonic_time(millisecond)),
            receive
                {'$socket', L, select, Handle} ->
                    accept(L, Watch, Deadline, Out);
                {Port, {exit_status, Status}} when Watch =:= {port, Port} ->
                    _ = socket:cancel(L, {select_info, accept, Handle}),
                    {error, {child_exited_at_start, exit_reason(Status),
                             drain_port_output(Port, Out)}};
                {Port, {data, D}} when Watch =:= {port, Port} ->
                    %% Keep it here, not in the mailbox: re-sending it would
                    %% make this receive return at once and never time out
                    accept(L, Watch, Deadline, [D | Out])
            after Left ->
                {port, Port} = Watch,
                _ = socket:cancel(L, {select_info, accept, Handle}),
                {error, {start_timeout, drain_port_output(Port, Out)}}
            end;
        {error, Reason} ->
            {error, {accept_failed, Reason}}
    end.

drain_port_output(Port, Acc) ->
    receive
        {Port, {data, D}} -> drain_port_output(Port, [D | Acc])
    after 50 ->
        iolist_to_binary(lists:reverse(Acc))
    end.

%% @doc Default Unix socket buffers are small (8 KB on macOS); large payloads
%% would cross in hundreds of wakeups. Best effort: the kernel clamps.
-spec tune_socket(socket:socket()) -> ok.
tune_socket(S) ->
    _ = socket:setopt(S, {otp, rcvbuf}, ?SOCKET_BUF),
    _ = socket:setopt(S, {socket, rcvbuf}, ?SOCKET_BUF),
    _ = socket:setopt(S, {socket, sndbuf}, ?SOCKET_BUF),
    ok.

%% @doc The `{env, ...}' port option for a child.
%%
%% `env' adds variables to what the child inherits from the VM. With
%% `clear_env => true' nothing is inherited: every variable of the VM not
%% named in `env' is unset. `hash_seed' sets PYTHONHASHSEED; `random' (the
%% default) leaves the choice to Python.
-spec port_env(map()) -> [{string(), string() | false}].
port_env(Opts) ->
    Env = [{to_list(K), to_list(V)} || {K, V} <- maps:to_list(maps:get(env, Opts, #{}))],
    Seed = case maps:get(hash_seed, Opts, random) of
        random -> [];
        N -> [{"PYTHONHASHSEED", integer_to_list(N)}]
    end,
    Set = Env ++ Seed,
    Clear = case maps:get(clear_env, Opts, false) of
        true ->
            Keep = [K || {K, _} <- Set],
            lists:usort([{K, false} || KV <- os:getenv(),
                                       K <- [hd(string:split(KV, "="))],
                                       K =/= "", not lists:member(K, Keep)]);
        false ->
            []
    end,
    Clear ++ Set.

%% @doc Check the options port_env/1 reads.
-spec check_env_opts(map()) -> ok | {error, term()}.
check_env_opts(Opts) ->
    case {maps:get(hash_seed, Opts, random), maps:get(clear_env, Opts, false)} of
        {Seed, _} when Seed =/= random,
                       not (is_integer(Seed) andalso Seed >= 0 andalso Seed =< 4294967295) ->
            {error, {badarg, {hash_seed, Seed}}};
        {_, Clear} when not is_boolean(Clear) ->
            {error, {badarg, {clear_env, Clear}}};
        _ ->
            ok
    end.

-spec rlimit_args(map()) -> [string()].
rlimit_args(Opts) ->
    Limits = maps:get(rlimits, Opts, #{}),
    lists:append([case maps:get(K, Limits, undefined) of
                      undefined -> [];
                      V when is_integer(V), V >= 0 -> ["--rlimit-" ++ atom_to_list(K), integer_to_list(V)]
                  end || K <- [as, cpu, nofile]]).

-spec cgroup_args(map()) -> [string()].
cgroup_args(Opts) ->
    case maps:get(cgroup, Opts, undefined) of
        undefined -> [];
        Dir -> ["--cgroup", to_list(Dir)]
    end.

-spec frame(non_neg_integer(), byte(), binary()) -> binary().
frame(Id, Status, Payload) ->
    Body = <<Status:8, Payload/binary>>,
    <<Id:64/native, (byte_size(Body)):32/native, Body/binary>>.

-spec send_frame(socket:socket(), non_neg_integer(), byte(), term()) -> ok | {error, term()}.
send_frame(S, Id, Status, Term) ->
    case socket:send(S, frame(Id, Status, term_to_binary(Term))) of
        ok -> ok;
        {error, {Reason, _Rest}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

-spec parse_frame(binary()) ->
    {ok, {non_neg_integer(), byte(), term()}, binary()} | more | {error, term()}.
parse_frame(<<Id:64/native, Len:32/native, Body:Len/binary, Rest/binary>>) ->
    case Body of
        <<Status:8, Payload/binary>> ->
            try
                Term = case Payload of
                    <<>> -> undefined;
                    _ -> binary_to_term(Payload)
                end,
                {ok, {Id, Status, Term}, Rest}
            catch
                error:badarg -> {error, bad_etf}
            end;
        <<>> ->
            {error, empty_body}
    end;
parse_frame(_) ->
    more.

%% @doc Exit reason from a port exit status (128 + N is signal N).
-spec exit_reason(integer()) -> {signal, pos_integer()} | {exit_status, integer()}.
exit_reason(Status) when Status > 128 -> {signal, Status - 128};
exit_reason(Status) -> {exit_status, Status}.

-spec kill_os_pid(integer()) -> ok.
kill_os_pid(OsPid) when is_integer(OsPid), OsPid > 0 ->
    _ = py_nif:os_kill(OsPid, 9),
    ok;
kill_os_pid(_) ->
    ok.

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(L) when is_list(L) -> unicode:characters_to_binary(L);
to_bin(B) when is_binary(B) -> B.

to_list(A) when is_atom(A) -> atom_to_list(A);
to_list(B) when is_binary(B) -> unicode:characters_to_list(B);
to_list(I) when is_integer(I) -> integer_to_list(I);
to_list(L) when is_list(L) -> L.
