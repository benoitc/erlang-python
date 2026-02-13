-module(erlang_python_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    NumWorkers = application:get_env(erlang_python, num_workers, 4),

    PoolSpec = #{
        id => py_pool,
        start => {py_pool, start_link, [NumWorkers]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [py_pool]
    },

    {ok, {
        #{strategy => one_for_one, intensity => 5, period => 10},
        [PoolSpec]
    }}.
