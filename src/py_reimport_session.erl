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

%%% @doc A session of a re-import template: several calls against one fresh
%%% module dictionary, kept in a worker or owngil context.
%%%
%%% The process is the handle py_session:new/1 returns. It answers the
%%% messages py_context sends to a context, so py_context:call/eval/exec,
%%% py:call(S, ...), timeouts and py_session:close/1 work on it. It never
%%% waits on Python: each call is sent to the context under a tag of its
%%% own and the reply is passed on to the caller when it comes. Nested calls
%%% (a callback calling back into the session) reach the context the same
%%% way and are served while it waits on the callback.
%%%
%%% A timeout does not interrupt: sessions share their context, and an
%%% embedded interrupt stops whatever the context runs, which may be another
%%% session's call. The caller stops waiting, the call finishes on its own
%%% and its late reply is dropped here.
%%%
%%% The session's modules and `__main__' live in the context
%%% (priv/_erlang_impl/_reimport.py, by session id) until close/1, the
%%% owner's crash, or, if this process is killed, the template's monitor.
%%% If the context dies the session answers `{error, {context_died, R}}'
%%% until it is closed.
%%%
%%% @private
%%%
%%% Owns: one session id in one context.
%%% Talks to: its context (forwarded requests), its template (registration).
%%% Never: runs Python itself or waits for a forwarded call.
-module(py_reimport_session).

-behaviour(gen_server).

-export([start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(REF_TAB, py_context_refs).
-define(IMPL, <<"_erlang_impl._reimport">>).
%% get_interp_id of a session: apart from real interpreter ids
-define(INTERP_ID_BASE, (1 bsl 48)).

-record(st, {
    ctx :: pid(),
    ctx_mon :: reference(),
    sid :: pos_integer(),
    mode :: worker | owngil,
    parent :: pid(),
    %% calls sent to the context: Tag => {From, MRef, Kind}
    pending = #{} :: #{reference() => {pid(), reference(), call | exec}},
    %% set once the context is gone
    exited :: term()
}).

%% @doc Open a session on Ctx, linked to the caller.
-spec start_link(pid(), pid(), [binary()], worker | owngil) -> {ok, pid()} | {error, term()}.
start_link(Template, Ctx, Passthrough, Mode) ->
    Parent = self(),
    Pid = proc_lib:spawn_link(fun() -> init_it(Parent, Template, Ctx, Passthrough, Mode) end),
    MRef = erlang:monitor(process, Pid),
    receive
        {Pid, started} ->
            erlang:demonitor(MRef, [flush]),
            {ok, Pid};
        {Pid, {error, Reason}} ->
            erlang:demonitor(MRef, [flush]),
            unlink(Pid),
            {error, Reason};
        {'DOWN', MRef, process, Pid, Reason} ->
            unlink(Pid),
            {error, Reason}
    end.

init_it(Parent, Template, Ctx, Passthrough, Mode) ->
    process_flag(trap_exit, true),
    %% Outlive a creator that exits normally, as other contexts do; its
    %% crash stops the session (EXIT clause below)
    put('$ancestors', [self() | case get('$ancestors') of
                                    L when is_list(L) -> L;
                                    _ -> []
                                end]),
    Sid = erlang:unique_integer([positive]),
    case py_context:call(Ctx, ?IMPL, session_open, [Sid, Passthrough]) of
        {ok, true} ->
            ets:insert(?REF_TAB, {self(), reimport_session}),
            Template ! {register_session, self(), Ctx, Sid},
            St = #st{ctx = Ctx, ctx_mon = erlang:monitor(process, Ctx), sid = Sid,
                     mode = Mode, parent = Parent},
            Parent ! {self(), started},
            gen_server:enter_loop(?MODULE, [], St);
        {error, Reason} ->
            Parent ! {self(), {error, {session_open_failed, Reason}}}
    end.

%% Not used: started through start_link/4 and enter_loop
init(_) ->
    {stop, use_start_link}.

handle_call(_Msg, _From, St) ->
    {reply, {error, unknown_request}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% ---- requests, forwarded ---------------------------------------------------

handle_info({call, From, MRef, M, F, A, K}, St) ->
    forward(From, MRef, session_call, [M, F, A, K], St);
handle_info({call, From, MRef, M, F, A, K, _EnvRef}, St) ->
    forward(From, MRef, session_call, [M, F, A, K], St);
handle_info({eval, From, MRef, Code, Locals}, St) ->
    forward(From, MRef, session_eval, [Code, Locals], St);
handle_info({eval, From, MRef, Code, Locals, _EnvRef}, St) ->
    forward(From, MRef, session_eval, [Code, Locals], St);
handle_info({exec, From, MRef, Code}, St) ->
    forward(From, MRef, session_exec, [Code], St);
handle_info({exec, From, MRef, Code, _EnvRef}, St) ->
    forward(From, MRef, session_exec, [Code], St);
handle_info({Tag, Reply}, #st{pending = Pending} = St) when is_map_key(Tag, Pending) ->
    {{From, MRef, Kind}, Rest} = maps:take(Tag, Pending),
    From ! {MRef, reply(Kind, Reply)},
    {noreply, St#st{pending = Rest}};

%% ---- introspection ---------------------------------------------------------

handle_info({get_interp_id, From, MRef}, #st{sid = Sid} = St) ->
    %% py:get_local_env/1 keys environments by this: one per session
    From ! {MRef, {ok, ?INTERP_ID_BASE + Sid}},
    {noreply, St};
handle_info({is_subinterp, From, MRef}, #st{mode = Mode} = St) ->
    From ! {MRef, Mode =:= owngil},
    {noreply, St};
handle_info({create_local_env, From, MRef}, St) ->
    %% The session's namespace is the environment: the ref is not used
    From ! {MRef, {ok, make_ref()}},
    {noreply, St};
handle_info({child_info, From, MRef}, #st{ctx = Ctx, sid = Sid, mode = Mode} = St) ->
    From ! {MRef, {ok, #{context => Ctx, session => Sid, mode => Mode}}},
    {noreply, St};

%% ---- controls --------------------------------------------------------------

handle_info({interrupt, From, MRef}, #st{exited = undefined, ctx = Ctx} = St) ->
    %% An embedded interrupt stops what the context runs now
    From ! {MRef, py_context:interrupt(Ctx)},
    {noreply, St};
handle_info({interrupt, From, MRef}, St) ->
    From ! {MRef, not_running},
    {noreply, St};
handle_info({interrupt_request, ReqMRef}, #st{pending = Pending} = St) ->
    %% The caller timed out. No interrupt (it could stop another session's
    %% call): forget the request so its late reply is dropped
    Drop = [Tag || {Tag, {_, M, _}} <- maps:to_list(Pending), M =:= ReqMRef],
    {noreply, St#st{pending = maps:without(Drop, Pending)}};
handle_info({kill, From, MRef}, St) ->
    From ! {MRef, {error, not_supported}},
    {noreply, St};
handle_info({Op, From, MRef, _}, St)
        when Op =:= pass_fd; Op =:= start_loop ->
    From ! {MRef, {error, not_supported_in_reimport}},
    {noreply, St};
handle_info({stop_loop, From, MRef, _GraceMs}, St) ->
    From ! {MRef, {error, no_loop}},
    {noreply, St};
handle_info({Op, From, MRef}, St) when Op =:= get_nif_ref; Op =:= loop_ref ->
    From ! {MRef, {error, not_supported_in_reimport}},
    {noreply, St};
handle_info({call_method, From, MRef, _, _, _}, St) ->
    From ! {MRef, {error, not_supported_in_reimport}},
    {noreply, St};
handle_info({submit, From, MRef, _TaskRef, _M, _F, _A, _K}, St) ->
    From ! {MRef, {error, not_supported_in_reimport}},
    {noreply, St};
handle_info({cancel_ctrl, _MRef}, St) ->
    {noreply, St};

%% ---- lifecycle -------------------------------------------------------------

handle_info({stop, From, MRef}, St) ->
    close_session(St),
    From ! {MRef, ok},
    {stop, normal, St#st{exited = closed}};
handle_info({'DOWN', Mon, process, _Ctx, Reason}, #st{ctx_mon = Mon, pending = Pending} = St) ->
    %% The context is gone and the session's modules with it: answer with
    %% that until closed (stopping would take a linked caller down)
    Exited = {context_died, Reason},
    [From ! {MRef, {error, Exited}} || {From, MRef, _} <- maps:values(Pending)],
    {noreply, St#st{exited = Exited, pending = #{}}};
handle_info({'EXIT', Parent, Reason}, #st{parent = Parent} = St) when Reason =/= normal ->
    {stop, Reason, St};
handle_info({'EXIT', _Pid, Reason}, St)
        when Reason =:= shutdown; Reason =:= kill ->
    {stop, Reason, St};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, St) ->
    close_session(St),
    try ets:delete(?REF_TAB, self()) catch error:badarg -> ok end,
    ok.

%% ============================================================================
%% Internal
%% ============================================================================

forward(From, MRef, _Fun, _Args, #st{exited = Exited} = St) when Exited =/= undefined ->
    From ! {MRef, {error, Exited}},
    {noreply, St};
forward(From, MRef, Fun, Args, #st{ctx = Ctx, sid = Sid, pending = Pending} = St) ->
    %% The context replies {Tag, Result} to this process, which does not
    %% wait for it
    Tag = make_ref(),
    Ctx ! {call, self(), Tag, ?IMPL, atom_to_binary(Fun), [Sid | Args], #{}},
    Kind = case Fun of session_exec -> exec; _ -> call end,
    {noreply, St#st{pending = Pending#{Tag => {From, MRef, Kind}}}}.

%% py_context:exec/2 returns ok, not {ok, _}
reply(exec, {ok, _}) -> ok;
reply(_Kind, Reply) -> Reply.

%% Idempotent on the Python side; nothing to do once the context is gone
close_session(#st{exited = undefined, ctx = Ctx, sid = Sid}) ->
    _ = py_context:call(Ctx, ?IMPL, session_close, [Sid], #{}, 5000),
    ok;
close_session(_St) ->
    ok.
