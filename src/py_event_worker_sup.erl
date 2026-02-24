%% @doc Supervisor for dynamic event workers.
-module(py_event_worker_sup).
-behaviour(supervisor).

-export([start_link/0, start_worker/2, stop_worker/1]).
-export([init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_worker(WorkerId, LoopRef) ->
    case supervisor:start_child(?MODULE, [WorkerId, LoopRef]) of
        {ok, Pid} ->
            ok = py_event_worker_registry:register(WorkerId, Pid, LoopRef),
            {ok, Pid};
        {error, _} = Error -> Error
    end.

stop_worker(WorkerPid) ->
    supervisor:terminate_child(?MODULE, WorkerPid).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 60},
    WorkerSpec = #{
        id => py_event_worker,
        start => {py_event_worker, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [py_event_worker]
    },
    {ok, {SupFlags, [WorkerSpec]}}.
