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

%%% @doc Simple supervisor for async Python workers.
%%% @private
-module(py_async_worker_sup).
-behaviour(supervisor).

-export([
    start_link/0,
    start_worker/1
]).

-export([init/1]).

start_link() ->
    supervisor:start_link(?MODULE, []).

start_worker(Sup) ->
    case supervisor:start_child(Sup, []) of
        {ok, Pid} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.

init([]) ->
    WorkerSpec = #{
        id => py_async_worker,
        start => {py_async_worker, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [py_async_worker]
    },

    {ok, {
        #{strategy => simple_one_for_one, intensity => 10, period => 60},
        [WorkerSpec]
    }}.
