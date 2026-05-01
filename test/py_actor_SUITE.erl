%% @doc Tests for process-bound Python actors (documentation examples).
-module(py_actor_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    test_counter_actor/1,
    test_process_isolation/1,
    test_reset_via_termination/1
]).

all() ->
    [
        test_counter_actor,
        test_process_isolation,
        test_reset_via_termination
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    timer:sleep(500),
    Config.

end_per_suite(_Config) ->
    ok.

%% Test the py_counter actor pattern from documentation
test_counter_actor(_Config) ->
    Ctx = py:context(),

    %% Initialize Counter class
    ok = py:exec(Ctx, <<"
class Counter:
    def __init__(self):
        self.value = 0
    def increment(self):
        self.value += 1
        return self.value
    def decrement(self):
        self.value -= 1
        return self.value
    def get(self):
        return self.value

counter = Counter()
">>),

    %% Test increment
    {ok, 1} = py:eval(Ctx, <<"counter.increment()">>),
    {ok, 2} = py:eval(Ctx, <<"counter.increment()">>),

    %% Test decrement
    {ok, 1} = py:eval(Ctx, <<"counter.decrement()">>),

    %% Test get
    {ok, 1} = py:eval(Ctx, <<"counter.get()">>),

    ct:pal("Counter actor pattern works correctly"),
    ok.

%% Test that processes have isolated Python environments
test_process_isolation(_Config) ->
    Ctx = py:context(1),
    Self = self(),

    %% Set a value in this process
    ok = py:exec(Ctx, <<"isolation_test = 'main_process'">>),
    {ok, <<"main_process">>} = py:eval(Ctx, <<"isolation_test">>),

    %% Spawn another process using the same context
    _Pid = spawn(fun() ->
        %% This process should have its own environment
        ok = py:exec(Ctx, <<"isolation_test = 'spawned_process'">>),
        {ok, Value} = py:eval(Ctx, <<"isolation_test">>),
        Self ! {result, Value}
    end),

    receive
        {result, <<"spawned_process">>} -> ok
    after 5000 ->
        ct:fail("Spawned process did not respond")
    end,

    %% Verify main process still has its own value
    {ok, <<"main_process">>} = py:eval(Ctx, <<"isolation_test">>),

    ct:pal("Process isolation verified - same context, different environments"),
    ok.

%% Test that terminating a process resets its Python environment
test_reset_via_termination(_Config) ->
    Ctx = py:context(1),
    Self = self(),

    %% Spawn a process that sets state
    Pid1 = spawn(fun() ->
        ok = py:exec(Ctx, <<"reset_test = 42">>),
        {ok, 42} = py:eval(Ctx, <<"reset_test">>),
        Self ! {ready, self()},
        receive stop -> ok end
    end),

    receive {ready, Pid1} -> ok end,

    %% Stop the process
    Pid1 ! stop,
    timer:sleep(100),

    %% Spawn a new process - should have fresh environment
    _Pid2 = spawn(fun() ->
        %% reset_test should not exist in this new process's environment
        Result = py:eval(Ctx, <<"reset_test">>),
        Self ! {result, Result}
    end),

    receive
        {result, {error, _}} ->
            ct:pal("Reset via termination works - new process has fresh environment");
        {result, {ok, 42}} ->
            ct:fail("Environment was not reset - state leaked between processes")
    after 5000 ->
        ct:fail("Process did not respond")
    end,

    ok.
