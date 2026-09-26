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

%%% @doc Isolated sessions: a fresh Python process per session, started from
%%% a prepared template.
%%%
%%% A template holds what every session starts with: interpreter, `sys.path',
%%% imports, preload code, environment, hash seed and limits. Each call to
%%% new/1 gives a new child process in which none of the state of another
%%% session exists; close/1 kills it.
%%%
%%% ```
%%% {ok, T} = py_session:template(#{paths => [Dir], imports => [orders],
%%%                                 hash_seed => 0}),
%%% {ok, S} = py_session:new(T),
%%% {ok, R} = py_context:call(S, orders, handle, [Event, State]),
%%% ok = py_session:close(S).
%%% '''
%%%
%%% A session is an isolated context: every py_context function works on it.
%%% It is linked to the process that created it and is never restarted; if
%%% its child dies, requests answer `{error, Reason}' until it is closed.
%%%
%%% Template options:
%%% <ul>
%%%   <li>`start' - `fork' (default): sessions are forked from a zygote that
%%%       already ran the imports and preload. `spawn': each session starts
%%%       a new interpreter; use it when the template cannot be forked
%%%       (threads started at import, Objective-C on macOS). `reimport':
%%%       no process per session; run/5 imports the function's module
%%%       again in a fresh module dictionary on one of the template's
%%%       `contexts' (`mode => worker | owngil'), as Temporal's workflow
%%%       sandbox does. Modules in `imports' and `passthrough', and the
%%%       standard library, are shared by every run.</li>
%%%   <li>`python', `paths', `imports', `preload' - the prepared state.</li>
%%%   <li>`env' - the environment of every session. Nothing is inherited
%%%       from the VM unless `clear_env => false'.</li>
%%%   <li>`hash_seed' - PYTHONHASHSEED shared by every session (default
%%%       `random', chosen once per zygote).</li>
%%%   <li>`zygotes' - zygotes forking in turn (default 1).</li>
%%%   <li>`warm' - with `start => spawn', sessions kept started ahead.</li>
%%%   <li>`rlimits', `cgroup', `start_timeout', `kill_after' - as for
%%%       isolated contexts, applied to each session.</li>
%%% </ul>
-module(py_session).

-export([template/1,
         stop_template/1,
         new/1,
         new/2,
         close/1,
         run/4,
         run/5,
         refresh/1,
         info/1]).

-type template() :: pid().
-type session() :: pid().
-export_type([template/0, session/0]).

%% @doc Build a template. With `start => fork' this starts the zygotes and
%% runs the imports and preload before returning.
-spec template(map()) -> {ok, template()} | {error, term()}.
template(Opts) when is_map(Opts) ->
    py_session_sup:start_template(Opts).

-spec stop_template(template()) -> ok.
stop_template(T) ->
    py_session_sup:stop_template(T).

%% @doc A new session: a fresh isolated process, linked to the caller.
-spec new(template()) -> {ok, session()} | {error, term()}.
new(T) ->
    new(T, #{}).

%% @doc A new session. `Opts' may carry `timeout' (milliseconds to get it).
-spec new(template(), map()) -> {ok, session()} | {error, term()}.
new(T, Opts) ->
    Timeout = maps:get(timeout, Opts, 15000),
    case py_session_template:checkout(T, Timeout) of
        {ok, Ctx} ->
            %% Started ahead by the template: take it over
            MRef = erlang:monitor(process, Ctx),
            Ctx ! {set_parent, self(), MRef, self()},
            receive
                {MRef, ok} ->
                    erlang:demonitor(MRef, [flush]),
                    {ok, Ctx};
                {'DOWN', MRef, process, Ctx, _} ->
                    new(T, Opts)
            after Timeout ->
                erlang:demonitor(MRef, [flush]),
                {error, timeout}
            end;
        {start, CtxOpts} ->
            py_context:new(maps:merge(CtxOpts, maps:with([start_timeout], Opts)));
        {reimport, _, _} ->
            %% a re-import run is not a process: use run/5
            {error, {not_supported, reimport}};
        {error, _} = Err ->
            Err
    end.

%% @doc Kill the session's process. A session is never reused.
-spec close(session()) -> ok.
close(S) ->
    py_context:stop(S).

%% @doc Run one call in a new session and close it.
-spec run(template(), atom() | binary(), atom() | binary(), list()) ->
    {ok, term()} | {error, term()}.
run(T, Module, Func, Args) ->
    run(T, Module, Func, Args, #{}).

%% @doc Run one call in a new session and close it. `Opts': `kwargs',
%% `timeout' (for the call), plus the options of new/2.
-spec run(template(), atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
run(T, Module, Func, Args, Opts) ->
    case py_session_template:checkout(T, maps:get(timeout, Opts, 15000)) of
        {reimport, Ctx, Passthrough} ->
            py_context:call(Ctx, '_erlang_impl._reimport', run,
                            [Passthrough, py_child:to_bin(Module), py_child:to_bin(Func),
                             Args, maps:get(kwargs, Opts, #{})],
                            #{}, maps:get(timeout, Opts, infinity));
        {ok, Ctx} ->
            %% taken from the warm pool, handed over as new/2 does
            run_in(T, Ctx, Module, Func, Args, Opts);
        _ ->
            run_new(T, Module, Func, Args, Opts)
    end.

run_in(T, Ctx, Module, Func, Args, Opts) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {set_parent, self(), MRef, self()},
    receive
        {MRef, ok} ->
            erlang:demonitor(MRef, [flush]),
            try
                py_context:call(Ctx, Module, Func, Args, maps:get(kwargs, Opts, #{}),
                                maps:get(timeout, Opts, infinity))
            after
                close(Ctx)
            end;
        {'DOWN', MRef, process, Ctx, _} ->
            run(T, Module, Func, Args, Opts)
    end.

run_new(T, Module, Func, Args, Opts) ->
    case new(T, Opts) of
        {ok, S} ->
            try
                py_context:call(S, Module, Func, Args, maps:get(kwargs, Opts, #{}),
                                maps:get(timeout, Opts, infinity))
            after
                close(S)
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Rebuild the template: new zygotes (code is imported again) or new
%% warm sessions. Live sessions are not touched.
-spec refresh(template()) -> ok | {error, term()}.
refresh(T) ->
    py_session_template:refresh(T).

%% @doc What the template runs: start mode, zygotes, sessions alive, forks.
-spec info(template()) -> map().
info(T) ->
    py_session_template:info(T).
