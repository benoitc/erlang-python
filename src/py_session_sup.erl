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

%%% @doc Supervisor of session templates.
%%%
%%% @private
-module(py_session_sup).

-behaviour(supervisor).

-export([start_link/0, start_template/1, stop_template/1]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_template(map()) -> {ok, pid()} | {error, term()}.
start_template(Opts) ->
    case supervisor:start_child(?MODULE, [Opts]) of
        {ok, Pid} -> {ok, Pid};
        {error, _} = Err -> Err
    end.

-spec stop_template(pid()) -> ok.
stop_template(T) when is_pid(T) ->
    _ = supervisor:terminate_child(?MODULE, T),
    ok.

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 5, period => 10},
    ChildSpec = #{
        id => py_session_template,
        start => {py_session_template, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [py_session_template]
    },
    {ok, {SupFlags, [ChildSpec]}}.
