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

%%% @doc Initializes the context router during application startup.
%%%
%%% This module provides a supervisor-compatible start function that
%%% initializes the context pool and returns `ignore' (since no
%%% process needs to stay running after initialization).
%%%
%%% == Pools ==
%%%
%%% The `default' pool is started automatically, sized to number of schedulers.
%%% Additional pools can be created on demand via `py_context_router:start_pool/3'.
%%%
%%% Pool size can be configured via application env:
%%% ```
%%% {erlang_python, [
%%%     {default_pool_size, 4}         % Number of contexts (default: schedulers)
%%% ]}.
%%% '''
%%% @private
-module(py_context_init).

-export([start_link/1]).

%% @doc Start the context pools.
%%
%% This function is called by the supervisor to initialize the
%% py_context_router pools. After starting the contexts, it returns
%% `ignore' since no process needs to remain running.
%%
%% @param Opts Options for the default pool
%% @returns {ok, pid()} | ignore | {error, Reason}
-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    %% Start default pool
    DefaultSize = maps:get(contexts, Opts, erlang:system_info(schedulers)),
    DefaultMode = maps:get(mode, Opts, worker),

    case py_context_router:start_pool(default, DefaultSize, DefaultMode) of
        {ok, _DefaultContexts} ->
            %% The contexts are supervised by py_context_sup
            %% We don't need a process here, just return ignore
            ignore;
        {error, Reason} ->
            {error, {default_pool_start_failed, Reason}}
    end.
