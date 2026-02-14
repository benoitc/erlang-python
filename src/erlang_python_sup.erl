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

    %% Base children (async pool disabled for now - GIL threading issues)
    %% TODO: Fix async event loop GIL management
    BaseChildren = [CallbackSpec, PoolSpec],  %% AsyncPoolSpec temporarily disabled

    %% Sub-interpreter pool also temporarily disabled for testing
    %% TODO: Fix sub-interpreter GIL management
    Children = BaseChildren,

    {ok, {
        #{strategy => one_for_all, intensity => 5, period => 10},
        Children
    }}.
