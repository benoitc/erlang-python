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

-module(erlang_python_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    NumWorkers = application:get_env(erlang_python, num_workers, 4),
    NumAsyncWorkers = application:get_env(erlang_python, num_async_workers, 2),
    NumSubinterpWorkers = application:get_env(erlang_python, num_subinterp_workers, 4),

    %% Initialize the semaphore ETS table for rate limiting
    ok = py_semaphore:init(),

    %% Callback registry - must start before pool
    CallbackSpec = #{
        id => py_callback,
        start => {py_callback, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_callback]
    },

    %% Main worker pool
    PoolSpec = #{
        id => py_pool,
        start => {py_pool, start_link, [NumWorkers]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_pool]
    },

    %% Async worker pool (for asyncio coroutines)
    AsyncPoolSpec = #{
        id => py_async_pool,
        start => {py_async_pool, start_link, [NumAsyncWorkers]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_async_pool]
    },

    %% Sub-interpreter pool (for true parallelism with per-interpreter GIL)
    SubinterpPoolSpec = #{
        id => py_subinterp_pool,
        start => {py_subinterp_pool, start_link, [NumSubinterpWorkers]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_subinterp_pool]
    },

    Children = [CallbackSpec, PoolSpec, AsyncPoolSpec, SubinterpPoolSpec],

    {ok, {
        #{strategy => one_for_all, intensity => 5, period => 10},
        Children
    }}.
