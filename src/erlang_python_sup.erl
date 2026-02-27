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
%%% Manages Python execution components:
%%% <ul>
%%%   <li>py_callback - Callback registry for Python to Erlang calls</li>
%%%   <li>py_state - Shared state storage accessible from Python</li>
%%%   <li>py_context_sup - Supervisor for process-per-context workers</li>
%%%   <li>py_async_pool - Worker pool for asyncio coroutines</li>
%%% </ul>
%%% @private
-module(erlang_python_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    NumContexts = application:get_env(erlang_python, num_contexts,
                                       erlang:system_info(schedulers)),
    ContextMode = application:get_env(erlang_python, context_mode, auto),
    NumAsyncWorkers = application:get_env(erlang_python, num_async_workers, 2),

    %% Initialize Python runtime first
    ok = py_nif:init(),

    %% Initialize the semaphore ETS table for rate limiting
    ok = py_semaphore:init(),

    %% Initialize callback registry ETS table (owned by supervisor for resilience)
    ok = py_callback:init_tab(),

    %% Initialize shared state ETS table (owned by supervisor for resilience)
    ok = py_state:init_tab(),

    %% Register state functions as callbacks for Python access
    ok = py_state:register_callbacks(),

    %% Callback registry - must start before contexts
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

    %% Process-per-context supervisor (replaces py_pool and py_subinterp_pool)
    ContextSupSpec = #{
        id => py_context_sup,
        start => {py_context_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [py_context_sup]
    },

    %% Context router initialization (starts contexts under py_context_sup)
    ContextRouterInitSpec = #{
        id => py_context_init,
        start => {py_context_init, start_link, [#{contexts => NumContexts, mode => ContextMode}]},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [py_context_init]
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

    %% Event worker registry (for scalable I/O model)
    WorkerRegistrySpec = #{
        id => py_event_worker_registry,
        start => {py_event_worker_registry, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_event_worker_registry]
    },

    %% Event worker supervisor (for dynamic workers)
    WorkerSupSpec = #{
        id => py_event_worker_sup,
        start => {py_event_worker_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [py_event_worker_sup]
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

    Children = [CallbackSpec, ThreadHandlerSpec, LoggerSpec, TracerSpec,
                ContextSupSpec, ContextRouterInitSpec, AsyncPoolSpec,
                WorkerRegistrySpec, WorkerSupSpec, EventLoopSpec],

    {ok, {
        #{strategy => one_for_all, intensity => 5, period => 10},
        Children
    }}.
