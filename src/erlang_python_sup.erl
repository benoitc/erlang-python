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

%%% @doc Top-level supervisor for erlang_python.
%%%
%%% Manages the worker pools for Python execution:
%%% <ul>
%%%   <li>py_callback - Callback registry for Python to Erlang calls</li>
%%%   <li>py_state - Shared state storage accessible from Python</li>
%%%   <li>py_pool - Main worker pool for synchronous Python calls</li>
%%%   <li>py_async_driver - Unified event-driven async driver</li>
%%%   <li>py_subinterp_pool - Worker pool for sub-interpreter parallelism</li>
%%% </ul>
%%% @private
-module(erlang_python_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    NumWorkers = application:get_env(erlang_python, num_workers, 4),
    NumSubinterpWorkers = application:get_env(erlang_python, num_subinterp_workers, 4),

    %% Initialize the semaphore ETS table for rate limiting
    ok = py_semaphore:init(),

    %% Initialize callback registry ETS table (owned by supervisor for resilience)
    ok = py_callback:init_tab(),

    %% Initialize shared state ETS table (owned by supervisor for resilience)
    ok = py_state:init_tab(),

    %% Register state functions as callbacks for Python access
    ok = py_state:register_callbacks(),

    %% Initialize callback ID generator for event-driven operations
    ok = py_callback_id:init(),

    %% Callback registry - must start before pool
    CallbackSpec = #{
        id => py_callback,
        start => {py_callback, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_callback]
    },

    %% Thread worker coordinator (for ThreadPoolExecutor support)
    ThreadHandlerSpec = #{
        id => py_thread_handler,
        start => {py_thread_handler, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_thread_handler]
    },

    %% Python logging integration
    LoggerSpec = #{
        id => py_logger,
        start => {py_logger, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_logger]
    },

    %% Python tracing integration
    TracerSpec = #{
        id => py_tracer,
        start => {py_tracer, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_tracer]
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

    %% Sub-interpreter pool (for true parallelism with per-interpreter GIL)
    SubinterpPoolSpec = #{
        id => py_subinterp_pool,
        start => {py_subinterp_pool, start_link, [NumSubinterpWorkers]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_subinterp_pool]
    },

    %% Event loop manager (for Erlang-native asyncio)
    EventLoopSpec = #{
        id => py_event_loop,
        start => {py_event_loop, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_event_loop]
    },

    %% Async driver (unified event-driven async)
    AsyncDriverSpec = #{
        id => py_async_driver,
        start => {py_async_driver, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_async_driver]
    },

    Children = [CallbackSpec, ThreadHandlerSpec, LoggerSpec, TracerSpec,
                PoolSpec, SubinterpPoolSpec, EventLoopSpec, AsyncDriverSpec],

    {ok, {
        #{strategy => one_for_all, intensity => 5, period => 10},
        Children
    }}.
