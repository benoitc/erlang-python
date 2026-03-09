%%% @doc Common Test suite for dual pool support.
%%%
%%% Tests the default and io pool separation to ensure:
%%% - Pools are independent
%%% - Pool-based calls work correctly
%%% - Backward compatibility is maintained
%%% - I/O operations don't block CPU operations
-module(py_pool_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_pools_started/1,
    test_default_pool_call/1,
    test_io_pool_call/1,
    test_pool_isolation/1,
    test_backward_compatibility/1,
    test_pool_call_with_kwargs/1,
    test_io_doesnt_block_default/1,
    test_explicit_context_call/1,
    test_pool_sizes/1,
    %% Registration tests
    test_register_module_to_pool/1,
    test_register_function_to_pool/1,
    test_unregister_pool/1,
    test_registration_priority/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        test_pools_started,
        test_default_pool_call,
        test_io_pool_call,
        test_pool_isolation,
        test_backward_compatibility,
        test_pool_call_with_kwargs,
        test_io_doesnt_block_default,
        test_explicit_context_call,
        test_pool_sizes,
        %% Registration tests
        test_register_module_to_pool,
        test_register_function_to_pool,
        test_unregister_pool,
        test_registration_priority
    ].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Test cases
%% ============================================================================

%% @doc Verify both pools are started on application start.
test_pools_started(_Config) ->
    %% Default pool should be started
    true = py_context_router:pool_started(default),
    DefaultSize = py_context_router:num_contexts(default),
    true = DefaultSize > 0,
    ct:pal("Default pool size: ~p", [DefaultSize]),

    %% IO pool should be started
    true = py_context_router:pool_started(io),
    IoSize = py_context_router:num_contexts(io),
    true = IoSize > 0,
    ct:pal("IO pool size: ~p", [IoSize]).

%% @doc Test basic call on default pool.
test_default_pool_call(_Config) ->
    %% Implicit default pool (backward compatible)
    {ok, 4.0} = py:call(math, sqrt, [16]),

    %% Explicit default pool
    {ok, 4.0} = py:call(default, math, sqrt, [16]).

%% @doc Test basic call on io pool.
test_io_pool_call(_Config) ->
    %% Explicit io pool
    {ok, 4.0} = py:call(io, math, sqrt, [16]),

    %% Another call to verify consistency
    {ok, 9.0} = py:call(io, math, sqrt, [81]).

%% @doc Test that pools have separate contexts.
test_pool_isolation(_Config) ->
    %% Get context from each pool
    DefaultCtx = py_context_router:get_context(default),
    IoCtx = py_context_router:get_context(io),

    %% They should be different processes
    true = DefaultCtx =/= IoCtx,
    ct:pal("Default context: ~p, IO context: ~p", [DefaultCtx, IoCtx]),

    %% Both should be alive
    true = is_process_alive(DefaultCtx),
    true = is_process_alive(IoCtx).

%% @doc Test backward compatibility - existing code should work unchanged.
test_backward_compatibility(_Config) ->
    %% py:call/3 should work
    {ok, 4.0} = py:call(math, sqrt, [16]),

    %% py:call/4 with kwargs should work
    {ok, 8.0} = py:call(math, pow, [2, 3], #{}),

    %% py:eval should work
    {ok, 6} = py:eval(<<"2 + 4">>),

    %% Context by index should work (legacy API)
    Ctx1 = py_context_router:get_context(1),
    true = is_pid(Ctx1).

%% @doc Test pool calls with keyword arguments.
test_pool_call_with_kwargs(_Config) ->
    %% IO pool with kwargs
    {ok, _} = py:call(io, json, dumps, [[1, 2, 3]], #{indent => 2}),

    %% Default pool with kwargs (explicit)
    {ok, _} = py:call(default, json, dumps, [[1, 2, 3]], #{}).

%% @doc Test that slow I/O operations don't block the default pool.
test_io_doesnt_block_default(_Config) ->
    Parent = self(),

    %% Start a slow operation on io pool
    IoRef = spawn_monitor(fun() ->
        %% Sleep for 500ms in io pool
        py:call(io, time, sleep, [0.5]),
        Parent ! {io_done, self()}
    end),

    %% Immediately start a quick operation on default pool
    StartTime = erlang:monotonic_time(millisecond),
    {ok, 4.0} = py:call(default, math, sqrt, [16]),
    EndTime = erlang:monotonic_time(millisecond),

    %% Default pool call should complete quickly (< 100ms)
    %% not wait for the io pool sleep
    Duration = EndTime - StartTime,
    ct:pal("Default pool call duration: ~pms", [Duration]),
    true = Duration < 100,

    %% Wait for io operation to complete
    receive
        {io_done, _} -> ok
    after 5000 ->
        %% Clean up monitor
        {_, MonRef} = IoRef,
        erlang:demonitor(MonRef, [flush]),
        ct:fail("IO operation timeout")
    end.

%% @doc Test using explicit context from a pool.
test_explicit_context_call(_Config) ->
    %% Get specific context from io pool
    IoCtx = py_context_router:get_context(io),

    %% Call using explicit context
    {ok, 4.0} = py:call(IoCtx, math, sqrt, [16]),

    %% Get specific context from default pool
    DefaultCtx = py_context_router:get_context(default),
    {ok, 9.0} = py:call(DefaultCtx, math, sqrt, [81]).

%% @doc Verify pool sizes are correct.
test_pool_sizes(_Config) ->
    %% Default pool should be sized to schedulers (unless configured otherwise)
    DefaultSize = py_context_router:num_contexts(default),
    ExpectedDefault = erlang:system_info(schedulers),
    ct:pal("Default pool: expected ~p, actual ~p", [ExpectedDefault, DefaultSize]),

    %% IO pool should be 10 by default (from py_context_init)
    IoSize = py_context_router:num_contexts(io),
    ct:pal("IO pool size: ~p", [IoSize]),
    true = IoSize > 0,

    %% Verify we can list all contexts from each pool
    DefaultContexts = py_context_router:contexts(default),
    IoContexts = py_context_router:contexts(io),

    DefaultSize = length(DefaultContexts),
    IoSize = length(IoContexts),

    %% All contexts should be alive
    lists:foreach(fun(Ctx) ->
        true = is_process_alive(Ctx)
    end, DefaultContexts ++ IoContexts).

%% ============================================================================
%% Registration tests
%% ============================================================================

%% @doc Test registering a module to a pool routes all its functions.
test_register_module_to_pool(_Config) ->
    %% Register 'time' module to io pool
    ok = py:register_pool(io, time),

    %% Verify lookup returns io pool
    io = py_context_router:lookup_pool(time, sleep),
    io = py_context_router:lookup_pool(time, time),
    io = py_context_router:lookup_pool(time, any_func),

    %% Unregistered modules should still go to default
    default = py_context_router:lookup_pool(math, sqrt),

    %% Call should work and use io pool (we can't easily verify which pool
    %% was used, but the call should succeed)
    {ok, _} = py:call(time, time, []),

    %% Cleanup
    ok = py:unregister_pool(time).

%% @doc Test registering a specific module/function to a pool.
test_register_function_to_pool(_Config) ->
    %% Register only json.loads to io pool
    ok = py:register_pool(io, {json, loads}),

    %% Verify lookup
    io = py_context_router:lookup_pool(json, loads),
    %% Other json functions should use default
    default = py_context_router:lookup_pool(json, dumps),

    %% Calls should work
    {ok, #{<<"a">> := 1}} = py:call(json, loads, [<<"{\"a\": 1}">>]),
    {ok, _} = py:call(json, dumps, [[1, 2, 3]]),

    %% Cleanup
    ok = py:unregister_pool({json, loads}).

%% @doc Test unregistering returns to default pool.
test_unregister_pool(_Config) ->
    %% Register and verify
    ok = py:register_pool(io, os),
    io = py_context_router:lookup_pool(os, getcwd),

    %% Unregister module
    ok = py:unregister_pool(os),
    default = py_context_router:lookup_pool(os, getcwd),

    %% Register function and verify
    ok = py:register_pool(io, {sys, path}),
    io = py_context_router:lookup_pool(sys, path),

    %% Unregister function
    ok = py:unregister_pool({sys, path}),
    default = py_context_router:lookup_pool(sys, path).

%% @doc Test that function-specific registration takes priority over module.
test_registration_priority(_Config) ->
    %% Register entire json module to io pool
    ok = py:register_pool(io, json),

    %% Register json.dumps specifically to default pool
    ok = py:register_pool(default, {json, dumps}),

    %% json.dumps should use default (function-specific wins)
    default = py_context_router:lookup_pool(json, dumps),
    %% json.loads should use io (module registration)
    io = py_context_router:lookup_pool(json, loads),
    %% other json functions use io
    io = py_context_router:lookup_pool(json, load),

    %% Cleanup
    ok = py:unregister_pool(json),
    ok = py:unregister_pool({json, dumps}).
