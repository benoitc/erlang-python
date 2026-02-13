%%% @doc Simple supervisor for Python workers.
%%% @private
-module(py_worker_sup).
-behaviour(supervisor).

-export([
    start_link/0,
    start_worker/1
]).

-export([init/1]).

start_link() ->
    supervisor:start_link(?MODULE, []).

start_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    Pid.

init([]) ->
    WorkerSpec = #{
        id => py_worker,
        start => {py_worker, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [py_worker]
    },

    {ok, {
        #{strategy => simple_one_for_one, intensity => 10, period => 60},
        [WorkerSpec]
    }}.
