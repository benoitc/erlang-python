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

%%% @doc Supervisor for py_context processes.
%%%
%%% This is a simple_one_for_one supervisor that manages py_context
%%% processes. New contexts are started via start_context/2.
%%%
%%% @end
-module(py_context_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    start_context/2,
    stop_context/1,
    which_contexts/0
]).

%% Supervisor callbacks
-export([init/1]).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start the supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a new py_context under this supervisor.
%%
%% @param Id Unique identifier for the context
%% @param Mode Context mode (auto | subinterp | worker)
%% @returns {ok, Pid} | {error, Reason}
-spec start_context(pos_integer(), py_context:context_mode()) ->
    {ok, pid()} | {error, term()}.
start_context(Id, Mode) ->
    supervisor:start_child(?MODULE, [Id, Mode]).

%% @doc Stop a context by its PID.
-spec stop_context(pid()) -> ok | {error, term()}.
stop_context(Pid) when is_pid(Pid) ->
    case supervisor:terminate_child(?MODULE, Pid) of
        ok -> ok;
        {error, not_found} -> ok;
        Error -> Error
    end.

%% @doc List all running context PIDs.
-spec which_contexts() -> [pid()].
which_contexts() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(?MODULE),
            is_pid(Pid)].

%% ============================================================================
%% Supervisor callbacks
%% ============================================================================

%% @private
init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 5,
        period => 10
    },
    ChildSpec = #{
        id => py_context,
        start => {py_context, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [py_context]
    },
    {ok, {SupFlags, [ChildSpec]}}.
