%%% @doc Common Test suite for py_context_router module.
%%%
%%% Tests scheduler-affinity routing for Python contexts.
-module(py_context_router_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_start_stop/1,
    test_scheduler_affinity/1,
    test_explicit_context/1,
    test_bind_unbind/1,
    test_cross_scheduler_distribution/1,
    test_context_calls/1,
    test_multiple_start_stop/1,
    test_custom_num_contexts/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        test_start_stop,
        test_scheduler_affinity,
        test_explicit_context,
        test_bind_unbind,
        test_cross_scheduler_distribution,
        test_context_calls,
        test_multiple_start_stop,
        test_custom_num_contexts
    ].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Ensure router is stopped before each test
    catch py_context_router:stop(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Clean up after each test
    catch py_context_router:stop(),
    ok.

%% ============================================================================
%% Test cases
%% ============================================================================

%% @doc Test basic start and stop.
test_start_stop(_Config) ->
    %% Start should succeed
    {ok, Contexts} = py_context_router:start(),
    true = is_list(Contexts),
    true = length(Contexts) > 0,

    %% All contexts should be alive
    lists:foreach(fun(Ctx) ->
        true = is_process_alive(Ctx)
    end, Contexts),

    %% num_contexts should match
    NumContexts = py_context_router:num_contexts(),
    NumContexts = length(Contexts),

    %% contexts() should return the same list
    Contexts = py_context_router:contexts(),

    %% Stop should succeed
    ok = py_context_router:stop(),

    %% Contexts should be dead after stop
    timer:sleep(100),
    lists:foreach(fun(Ctx) ->
        false = is_process_alive(Ctx)
    end, Contexts),

    %% num_contexts should be 0 after stop
    0 = py_context_router:num_contexts().

%% @doc Test that same scheduler gets same context.
test_scheduler_affinity(_Config) ->
    {ok, _} = py_context_router:start(),

    %% Same process should get same context repeatedly
    Ctx1 = py_context_router:get_context(),
    Ctx2 = py_context_router:get_context(),
    Ctx3 = py_context_router:get_context(),
    Ctx1 = Ctx2,
    Ctx2 = Ctx3.

%% @doc Test explicit context selection by index.
test_explicit_context(_Config) ->
    {ok, Contexts} = py_context_router:start(#{contexts => 4}),

    %% Get specific contexts by index
    Ctx1 = py_context_router:get_context(1),
    Ctx2 = py_context_router:get_context(2),
    Ctx3 = py_context_router:get_context(3),
    Ctx4 = py_context_router:get_context(4),

    %% Should match the returned list
    [Ctx1, Ctx2, Ctx3, Ctx4] = Contexts,

    %% All should be different
    true = Ctx1 =/= Ctx2,
    true = Ctx2 =/= Ctx3,
    true = Ctx3 =/= Ctx4.

%% @doc Test bind and unbind functionality.
test_bind_unbind(_Config) ->
    {ok, _} = py_context_router:start(#{contexts => 4}),

    %% Get two different contexts
    Ctx1 = py_context_router:get_context(1),
    Ctx2 = py_context_router:get_context(2),
    true = Ctx1 =/= Ctx2,

    %% Bind to Ctx1
    ok = py_context_router:bind_context(Ctx1),
    Ctx1 = py_context_router:get_context(),

    %% Bind to Ctx2 (override)
    ok = py_context_router:bind_context(Ctx2),
    Ctx2 = py_context_router:get_context(),

    %% Unbind - should return to scheduler-based
    ok = py_context_router:unbind_context(),
    _SchedulerCtx = py_context_router:get_context(),

    %% Multiple unbinds should be safe
    ok = py_context_router:unbind_context(),
    ok = py_context_router:unbind_context().

%% @doc Test that different schedulers use different contexts.
test_cross_scheduler_distribution(_Config) ->
    NumContexts = min(4, erlang:system_info(schedulers)),
    {ok, _} = py_context_router:start(#{contexts => NumContexts}),

    %% Spawn many processes and collect their contexts
    Parent = self(),
    NumProcs = 100,

    Pids = [spawn(fun() ->
        Ctx = py_context_router:get_context(),
        Parent ! {self(), Ctx}
    end) || _ <- lists:seq(1, NumProcs)],

    Contexts = [receive
        {Pid, Ctx} -> Ctx
    after 5000 ->
        ct:fail("Timeout waiting for context")
    end || Pid <- Pids],

    %% Should have used multiple different contexts
    UniqueContexts = lists:usort(Contexts),
    ct:pal("Used ~p unique contexts out of ~p", [length(UniqueContexts), NumContexts]),

    %% With 100 processes, we should have used more than 1 context
    %% (unless system has only 1 scheduler)
    case erlang:system_info(schedulers) of
        1 -> true = length(UniqueContexts) >= 1;
        _ -> true = length(UniqueContexts) > 1
    end.

%% @doc Test that context calls work through the router.
test_context_calls(_Config) ->
    {ok, _} = py_context_router:start(),

    %% Get context and make calls
    Ctx = py_context_router:get_context(),

    %% Test call
    {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{}),

    %% Test eval
    {ok, 6} = py_context:eval(Ctx, <<"2 + 4">>, #{}),

    %% Test exec
    ok = py_context:exec(Ctx, <<"router_test_var = 42">>),
    {ok, 42} = py_context:eval(Ctx, <<"router_test_var">>, #{}).

%% @doc Test multiple start/stop cycles.
test_multiple_start_stop(_Config) ->
    lists:foreach(fun(_) ->
        {ok, Contexts1} = py_context_router:start(#{contexts => 2}),
        2 = length(Contexts1),

        %% Make a call to verify it works
        Ctx = py_context_router:get_context(),
        {ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{}),

        ok = py_context_router:stop(),
        timer:sleep(50)
    end, lists:seq(1, 3)).

%% @doc Test with custom number of contexts.
test_custom_num_contexts(_Config) ->
    %% Test with 2 contexts
    {ok, Contexts2} = py_context_router:start(#{contexts => 2}),
    2 = length(Contexts2),
    2 = py_context_router:num_contexts(),
    ok = py_context_router:stop(),

    %% Test with 8 contexts
    {ok, Contexts8} = py_context_router:start(#{contexts => 8}),
    8 = length(Contexts8),
    8 = py_context_router:num_contexts(),
    ok = py_context_router:stop().
