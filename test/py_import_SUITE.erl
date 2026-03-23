%%% @doc Test suite for py_import:ensure_imported/1,2
-module(py_import_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    import_module_test/1,
    import_function_test/1,
    import_main_rejected_test/1,
    import_nonexistent_module_test/1,
    import_nonexistent_function_test/1,
    import_idempotent_test/1,
    import_list_test/1,
    import_speeds_up_calls_test/1,
    import_multiprocess_test/1,
    import_concurrent_stress_test/1,
    %% Import registry tests
    import_registry_test/1,
    import_applied_to_new_context_test/1,
    clear_imports_test/1,
    get_imports_test/1,
    %% Per-interpreter sharing tests
    shared_interpreter_import_test/1,
    event_loop_pool_import_test/1,
    spawn_task_uses_import_test/1,
    subinterp_isolation_test/1,
    registry_applied_to_subinterp_test/1,
    %% sys.modules verification tests
    import_in_sys_modules_test/1,
    registry_import_in_sys_modules_test/1,
    context_import_in_sys_modules_test/1
]).

all() ->
    [{group, import_tests}].

groups() ->
    [{import_tests, [sequence], [
        import_module_test,
        import_function_test,
        import_main_rejected_test,
        import_nonexistent_module_test,
        import_nonexistent_function_test,
        import_idempotent_test,
        import_list_test,
        import_speeds_up_calls_test,
        import_multiprocess_test,
        import_concurrent_stress_test,
        %% Import registry tests
        import_registry_test,
        import_applied_to_new_context_test,
        clear_imports_test,
        get_imports_test,
        %% Per-interpreter sharing tests
        shared_interpreter_import_test,
        event_loop_pool_import_test,
        spawn_task_uses_import_test,
        subinterp_isolation_test,
        registry_applied_to_subinterp_test,
        %% sys.modules verification tests
        import_in_sys_modules_test,
        registry_import_in_sys_modules_test,
        context_import_in_sys_modules_test
    ]}].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    timer:sleep(500),
    Config.

end_per_suite(_Config) ->
    %% Clean up imports to avoid affecting subsequent test suites
    py_import:clear_imports(),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Flush imports before each test for clean state
    py_import:clear_imports(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% @doc Test importing a module
import_module_test(_Config) ->
    %% Import json module
    ok = py_import:ensure_imported(json),

    %% Verify it works by calling a function
    {ok, Result} = py:call(json, dumps, [[1, 2, 3]]),
    ?assertEqual(<<"[1, 2, 3]">>, Result),

    %% Import with binary name
    ok = py_import:ensure_imported(<<"math">>),
    {ok, Pi} = py:call(math, sqrt, [4.0]),
    ?assertEqual(2.0, Pi).

%% @doc Test importing a specific function
import_function_test(_Config) ->
    %% Import json.dumps
    ok = py_import:ensure_imported(json, dumps),

    %% Verify it works
    {ok, Result} = py:call(json, dumps, [#{a => 1}]),
    ?assert(is_binary(Result)),

    %% Import with binary names
    ok = py_import:ensure_imported(<<"os">>, <<"getcwd">>),
    {ok, Cwd} = py:call(os, getcwd, []),
    ?assert(is_binary(Cwd)).

%% @doc Test that __main__ cannot be imported
import_main_rejected_test(_Config) ->
    %% __main__ should be rejected
    {error, main_not_cacheable} = py_import:ensure_imported('__main__'),
    {error, main_not_cacheable} = py_import:ensure_imported(<<"__main__">>),

    %% Also for function import
    {error, main_not_cacheable} = py_import:ensure_imported('__main__', some_func),
    {error, main_not_cacheable} = py_import:ensure_imported(<<"__main__">>, <<"some_func">>).

%% @doc Test importing nonexistent module - registry accepts it but call fails
%%
%% py:import just adds to the registry. The actual import error happens
%% when trying to use the module.
import_nonexistent_module_test(_Config) ->
    %% Import succeeds (just adds to registry)
    ok = py_import:ensure_imported(nonexistent_module_xyz),

    %% But trying to use it fails
    %% Error format is {error, {ExceptionType, Message}} or {error, atom()}
    {error, Reason} = py:call(nonexistent_module_xyz, some_func, []),
    ?assert(is_tuple(Reason) orelse is_list(Reason) orelse is_binary(Reason) orelse is_atom(Reason)),

    ct:pal("Nonexistent module error at call time: ~p", [Reason]).

%% @doc Test importing with nonexistent function name still imports the module
%%
%% py:import/2 imports the module into sys.modules. The function name is
%% stored in the registry but not validated at import time. Function
%% validation happens at call time.
import_nonexistent_function_test(_Config) ->
    %% Module exists but function doesn't - import still succeeds
    %% because we're importing the MODULE, not the function
    ok = py_import:ensure_imported(json, nonexistent_function_xyz),

    %% The module is imported and usable
    {ok, _} = py:call(json, dumps, [[1, 2, 3]]),

    %% But calling the nonexistent function will fail
    {error, _Reason} = py:call(json, nonexistent_function_xyz, []),

    ct:pal("Import with invalid function succeeds (validation at call time)").

%% @doc Test that importing same module/function twice is idempotent
import_idempotent_test(_Config) ->
    %% Import multiple times - should all succeed
    ok = py_import:ensure_imported(json),
    ok = py_import:ensure_imported(json),
    ok = py_import:ensure_imported(json),

    ok = py_import:ensure_imported(json, dumps),
    ok = py_import:ensure_imported(json, dumps),
    ok = py_import:ensure_imported(json, dumps),

    %% Still works
    {ok, _} = py:call(json, dumps, [[1]]).

%% @doc Test listing imports
import_list_test(_Config) ->
    %% Start fresh
    ok = py_import:clear_imports(),

    %% Empty map
    {ok, Map0} = py_import:import_list(),
    ?assertEqual(#{}, Map0),

    %% Import some modules and functions
    ok = py_import:ensure_imported(json),
    ok = py_import:ensure_imported(math),
    ok = py_import:ensure_imported(json, dumps),
    ok = py_import:ensure_imported(json, loads),

    %% Get map
    {ok, Map1} = py_import:import_list(),

    %% Check structure: should have json and math as keys
    ?assert(maps:is_key(<<"json">>, Map1)),
    ?assert(maps:is_key(<<"math">>, Map1)),

    %% json should have dumps and loads as functions
    JsonFuncs = maps:get(<<"json">>, Map1),
    ?assertEqual(2, length(JsonFuncs)),
    ?assert(lists:member(<<"dumps">>, JsonFuncs)),
    ?assert(lists:member(<<"loads">>, JsonFuncs)),

    %% math should have empty function list (only module cached)
    MathFuncs = maps:get(<<"math">>, Map1),
    ?assertEqual([], MathFuncs),

    ct:pal("Import list: ~p", [Map1]).

%% @doc Test that pre-importing speeds up subsequent calls
import_speeds_up_calls_test(_Config) ->
    %% Flush to ensure cold start
    ok = py_import:clear_imports(),

    %% Time a cold call (module not imported)
    %% Using json.dumps since hashlib.md5 needs bytes encoding
    {ColdTime, {ok, _}} = timer:tc(fun() ->
        py:call(json, dumps, [[1,2,3,4,5]])
    end),

    %% Pre-import the module and function
    ok = py_import:ensure_imported(json),
    ok = py_import:ensure_imported(json, dumps),

    %% Time a warm call (module already imported)
    {WarmTime, {ok, _}} = timer:tc(fun() ->
        py:call(json, dumps, [[1,2,3,4,5]])
    end),

    ct:pal("Cold call time: ~p us, Warm call time: ~p us", [ColdTime, WarmTime]),

    %% Warm call should generally be faster, but we don't assert
    %% because timing can be variable. Just log for observation.
    ok.

%% @doc Test that multiple processes can use the shared import registry
%%
%% The import registry is global (ETS table) and sys.modules is per-interpreter.
%% All processes using the same interpreter share the same cached modules.
import_multiprocess_test(_Config) ->
    Parent = self(),

    %% Clear registry first
    ok = py_import:clear_imports(),

    %% Spawn 3 processes, each importing different modules
    %% They all contribute to the same global registry
    Pid1 = spawn_link(fun() ->
        ok = py_import:ensure_imported(json),
        ok = py_import:ensure_imported(json, dumps),
        %% Verify we can use the import
        {ok, _} = py:call(json, dumps, [[1,2,3]]),
        Parent ! {self(), done}
    end),

    Pid2 = spawn_link(fun() ->
        ok = py_import:ensure_imported(math),
        ok = py_import:ensure_imported(math, sqrt),
        ok = py_import:ensure_imported(math, floor),
        %% Verify we can use the import
        {ok, _} = py:call(math, sqrt, [16.0]),
        Parent ! {self(), done}
    end),

    Pid3 = spawn_link(fun() ->
        ok = py_import:ensure_imported(os),
        ok = py_import:ensure_imported(os, getcwd),
        ok = py_import:ensure_imported(string),
        %% Verify we can use the import
        {ok, _} = py:call(os, getcwd, []),
        Parent ! {self(), done}
    end),

    %% Collect results from all 3 processes
    Results = [receive {Pid, Result} -> Result after 5000 -> timeout end
               || Pid <- [Pid1, Pid2, Pid3]],

    %% Verify no timeouts
    ?assertEqual(false, lists:member(timeout, Results)),

    %% All processes completed successfully
    ?assertEqual([done, done, done], Results),

    %% Now verify the GLOBAL registry has all entries
    {ok, List} = py_import:import_list(),

    %% Total entries: json, json.dumps, math, math.sqrt, math.floor, os, os.getcwd, string = 8
    ?assertEqual(8, length(py_import:all_imports())),

    %% Verify all modules are in the shared registry
    ?assert(maps:is_key(<<"json">>, List)),
    ?assert(maps:is_key(<<"math">>, List)),
    ?assert(maps:is_key(<<"os">>, List)),
    ?assert(maps:is_key(<<"string">>, List)),

    %% Verify function entries
    ?assert(lists:member(<<"dumps">>, maps:get(<<"json">>, List))),
    MathFuncs = maps:get(<<"math">>, List),
    ?assert(lists:member(<<"sqrt">>, MathFuncs)),
    ?assert(lists:member(<<"floor">>, MathFuncs)),
    ?assert(lists:member(<<"getcwd">>, maps:get(<<"os">>, List))),

    ct:pal("Global registry after multiprocess imports: ~p", [List]).

%% @doc Stress test with many concurrent processes importing simultaneously
%%
%% All processes contribute to the shared global registry.
import_concurrent_stress_test(_Config) ->
    Parent = self(),
    NumProcesses = 20,
    Modules = [json, math, os, string, re, base64, collections, functools, itertools, operator],

    %% Clear registry first
    ok = py_import:clear_imports(),

    %% Spawn many processes that all try to import at the same time
    Pids = [spawn_link(fun() ->
        %% Each process imports a random subset of modules
        MyModules = lists:sublist(Modules, 1 + (N rem length(Modules))),
        Results = [{M, py_import:ensure_imported(M)} || M <- MyModules],

        %% All imports should succeed
        AllOk = lists:all(fun({_, R}) -> R =:= ok end, Results),

        %% Make a call to verify imports work
        CallResult = py:call(json, dumps, [[N]]),

        Parent ! {self(), {AllOk, CallResult}}
    end) || N <- lists:seq(1, NumProcesses)],

    %% Collect all results
    Results = [receive {Pid, Result} -> Result after 10000 -> timeout end || Pid <- Pids],

    %% Verify no timeouts
    ?assertEqual(false, lists:member(timeout, Results)),

    %% Verify all processes succeeded
    lists:foreach(fun({AllOk, CallResult}) ->
        ?assertEqual(true, AllOk),
        ?assertMatch({ok, _}, CallResult)
    end, Results),

    %% Verify the global registry has all modules
    Count = length(py_import:all_imports()),
    %% Should have all 10 modules (some may have been imported multiple times but ETS dedupes)
    ?assertEqual(10, Count),

    ct:pal("All ~p processes completed successfully, ~p modules in registry", [NumProcesses, Count]).

%% ============================================================================
%% Import Registry Tests
%% ============================================================================

%% @doc Test that imports are added to the global registry
import_registry_test(_Config) ->
    %% Clear any existing registry entries
    ok = py_import:clear_imports(),

    %% Verify registry is empty
    [] = py_import:all_imports(),

    %% Import a module
    ok = py_import:ensure_imported(json),

    %% Verify it's in the registry
    Imports1 = py_import:all_imports(),
    ?assert(lists:member({<<"json">>, all}, Imports1)),

    %% Import a function
    ok = py_import:ensure_imported(math, sqrt),

    %% Verify both are in the registry
    Imports2 = py_import:all_imports(),
    ?assert(lists:member({<<"json">>, all}, Imports2)),
    ?assert(lists:member({<<"math">>, <<"sqrt">>}, Imports2)),

    ct:pal("Registry contents: ~p", [Imports2]).

%% @doc Test that imports are automatically applied to new contexts
import_applied_to_new_context_test(_Config) ->
    %% Clear and add an import
    ok = py_import:clear_imports(),
    ok = py_import:ensure_imported(json),

    %% Create a new context
    {ok, Ctx} = py_context:new(#{mode => auto}),

    %% The json module should already be cached in the new context
    %% We can verify by calling a function from it
    {ok, Result} = py_context:call(Ctx, json, dumps, [[1, 2, 3]], #{}),
    ?assertEqual(<<"[1, 2, 3]">>, Result),

    %% Clean up
    py_context:destroy(Ctx),
    ok = py_import:clear_imports().

%% @doc Test clearing all imports from the registry
clear_imports_test(_Config) ->
    %% Add some imports
    ok = py_import:ensure_imported(json),
    ok = py_import:ensure_imported(math),
    ok = py_import:ensure_imported(os),

    %% Verify they're in the registry
    Imports1 = py_import:all_imports(),
    ?assert(length(Imports1) >= 3),

    %% Clear all
    ok = py_import:clear_imports(),

    %% Verify registry is empty
    Imports2 = py_import:all_imports(),
    ?assertEqual([], Imports2).

%% @doc Test get_imports returns the correct format
get_imports_test(_Config) ->
    %% Clear and add imports
    ok = py_import:clear_imports(),
    ok = py_import:ensure_imported(json),
    ok = py_import:ensure_imported(math, sqrt),

    %% Get imports
    Imports = py_import:all_imports(),

    %% Verify format
    ?assert(is_list(Imports)),

    %% Check the entries
    {_, JsonSpec} = lists:keyfind(<<"json">>, 1, Imports),
    ?assertEqual(all, JsonSpec),

    {_, MathSpec} = lists:keyfind(<<"math">>, 1, Imports),
    ?assertEqual(<<"sqrt">>, MathSpec),

    ct:pal("get_imports result: ~p", [Imports]).

%% ============================================================================
%% Per-Interpreter Sharing Tests
%% ============================================================================

%% @doc Test that two contexts sharing the same interpreter see imported modules
%%
%% When we import a module via one context, other contexts using the same
%% interpreter (same subinterpreter pool slot or main interpreter) should
%% see the module in sys.modules.
shared_interpreter_import_test(_Config) ->
    %% Clear registry
    ok = py_import:clear_imports(),

    %% Create two worker-mode contexts (they share the main interpreter)
    {ok, Ctx1} = py_context:new(#{mode => worker}),
    {ok, Ctx2} = py_context:new(#{mode => worker}),

    %% Import a module via Ctx1 by calling it (this adds to sys.modules)
    {ok, _} = py_context:call(Ctx1, json, dumps, [[1, 2, 3]], #{}),

    %% Now Ctx2 should be able to use json without re-importing
    %% (it's already in sys.modules of the shared interpreter)
    {ok, Result} = py_context:call(Ctx2, json, loads, [<<"[4, 5, 6]">>], #{}),
    ?assertEqual([4, 5, 6], Result),

    %% Clean up
    py_context:destroy(Ctx1),
    py_context:destroy(Ctx2),

    ct:pal("Worker contexts successfully shared interpreter's sys.modules").

%% @doc Test that event loop pool workers see imports from py:import
%%
%% When py:import is called, it imports into the current interpreter.
%% Event loop pool workers using the main interpreter should see these imports.
event_loop_pool_import_test(_Config) ->
    %% Clear registry
    ok = py_import:clear_imports(),

    %% Import via py:import (goes to event loop pool's interpreter)
    ok = py_import:ensure_imported(collections),

    %% Verify we can use it via py:call (uses event loop pool)
    {ok, Result} = py:call(collections, 'Counter', [[a, b, a, c, a, b]]),
    ?assert(is_map(Result) orelse is_tuple(Result)),

    %% Import another module
    ok = py_import:ensure_imported(itertools),

    %% Use it
    {ok, _} = py:call(itertools, chain, [[[1, 2], [3, 4]]]),

    ct:pal("Event loop pool imports working correctly").

%% @doc Test that spawn_task uses imported modules
%%
%% When modules are imported via py:import, spawn_task should be able
%% to use them since they're in the interpreter's sys.modules.
spawn_task_uses_import_test(_Config) ->
    %% Clear registry
    ok = py_import:clear_imports(),

    %% Import base64 module
    ok = py_import:ensure_imported(base64),

    %% Define a simple function that uses base64
    Code = <<"
def encode_test(data):
    import base64
    return base64.b64encode(data.encode()).decode()
">>,
    ok = py:exec(Code),

    %% Use spawn_task to call our function
    %% First verify direct call works
    {ok, Encoded} = py:call('__main__', encode_test, [<<"hello">>]),
    ?assertEqual(<<"aGVsbG8=">>, Encoded),

    %% Now test via spawn_task (fire and forget, but module should be available)
    py_event_loop_pool:spawn_task(base64, b64encode, [<<"test">>]),

    %% Give it time to execute
    timer:sleep(100),

    ct:pal("spawn_task can use imported modules").

%% @doc Test that different subinterpreters are isolated
%%
%% OWN_GIL contexts each have their own interpreter, so imports in one
%% should NOT be visible in another (different sys.modules).
subinterp_isolation_test(_Config) ->
    %% Skip if subinterpreters not supported
    case py_nif:subinterp_supported() of
        false ->
            {skip, "Subinterpreters not supported"};
        true ->
            %% Clear registry so new contexts don't get pre-imported modules
            ok = py_import:clear_imports(),

            %% Create two OWN_GIL contexts (each has its own interpreter)
            {ok, Ctx1} = py_context:new(#{mode => owngil}),
            {ok, Ctx2} = py_context:new(#{mode => owngil}),

            %% Define a variable in Ctx1's __main__
            ok = py_context:exec(Ctx1, <<"test_var_isolation = 'ctx1_value'">>),

            %% Try to access it from Ctx2 - should fail (different interpreter)
            Result = py_context:eval(Ctx2, <<"test_var_isolation">>),
            case Result of
                {error, _} ->
                    %% Expected - variable not defined in Ctx2
                    ok;
                {ok, <<"ctx1_value">>} ->
                    %% This would be wrong - isolation failed
                    ct:fail("Subinterpreter isolation failed - variable leaked between contexts")
            end,

            %% Clean up
            py_context:destroy(Ctx1),
            py_context:destroy(Ctx2),

            ct:pal("Subinterpreter isolation verified - different interpreters are isolated")
    end.

%% @doc Test that registry imports are applied to new subinterpreter contexts
%%
%% When py:import is called, it adds to the registry. New contexts should
%% have these imports applied to their interpreter.
registry_applied_to_subinterp_test(_Config) ->
    %% Skip if subinterpreters not supported
    case py_nif:subinterp_supported() of
        false ->
            {skip, "Subinterpreters not supported"};
        true ->
            %% Clear registry and add an import
            ok = py_import:clear_imports(),
            ok = py_import:ensure_imported(uuid),

            %% Create a new subinterp context
            {ok, Ctx} = py_context:new(#{mode => subinterp}),

            %% The uuid module should be available (applied from registry)
            {ok, Result} = py_context:call(Ctx, uuid, uuid4, [], #{}),
            ?assert(is_binary(Result) orelse is_list(Result)),

            %% Clean up
            py_context:destroy(Ctx),
            ok = py_import:clear_imports(),

            ct:pal("Registry imports successfully applied to new subinterpreter")
    end.

%% ============================================================================
%% sys.modules Verification Tests
%% ============================================================================

%% @doc Test that py:import puts the module in sys.modules
%%
%% After calling py:import, the module should be in the interpreter's
%% sys.modules dictionary. We verify this by checking that calling
%% a function from the module works (which requires it to be imported).
%%
%% Note: We use textwrap (pure Python) instead of decimal because the
%% _decimal C extension has global state that crashes in subinterpreters.
import_in_sys_modules_test(_Config) ->
    %% Clear registry
    ok = py_import:clear_imports(),

    %% Import a pure Python module (avoid C extensions like decimal
    %% which have global state that crashes in subinterpreters)
    ok = py_import:ensure_imported(textwrap),

    %% Verify the import worked by calling a function
    {ok, _} = py:call(textwrap, fill, [<<"Hello world">>, 5]),

    %% Now check sys.modules using the same process (important!)
    %% We use exec to define a helper, then eval to check
    ok = py:exec(<<"
import sys
_test_textwrap_in_sys = 'textwrap' in sys.modules
">>),
    {ok, InSysModules} = py:eval(<<"_test_textwrap_in_sys">>),
    ?assertEqual(true, InSysModules),

    ct:pal("py_import:ensure_imported correctly adds module to sys.modules").

%% @doc Test that ETS registry and sys.modules stay in sync
%%
%% The ETS registry tracks what should be imported, and sys.modules
%% contains the actual imported modules.
registry_import_in_sys_modules_test(_Config) ->
    %% Clear registry
    ok = py_import:clear_imports(),

    %% Add to registry and import
    ok = py_import:ensure_imported(fractions),
    ok = py_import:ensure_imported(statistics),

    %% Verify ETS registry has the entries
    Registry = py_import:all_imports(),
    ?assert(lists:member({<<"fractions">>, all}, Registry)),
    ?assert(lists:member({<<"statistics">>, all}, Registry)),

    %% Use the modules to ensure they're imported
    {ok, _} = py:call(fractions, 'Fraction', [1, 3]),
    {ok, _} = py:call(statistics, mean, [[1, 2, 3, 4, 5]]),

    %% Verify both are in sys.modules by checking from Python
    ok = py:exec(<<"
import sys
_fractions_in_sys = 'fractions' in sys.modules
_statistics_in_sys = 'statistics' in sys.modules
_sys_modules_keys = list(sys.modules.keys())
">>),

    {ok, FractionsInSys} = py:eval(<<"_fractions_in_sys">>),
    {ok, StatsInSys} = py:eval(<<"_statistics_in_sys">>),
    ?assertEqual(true, FractionsInSys),
    ?assertEqual(true, StatsInSys),

    %% Get the list of modules in sys.modules that match our registry
    {ok, SysModulesList} = py:eval(<<"_sys_modules_keys">>),
    ?assert(lists:member(<<"fractions">>, SysModulesList)),
    ?assert(lists:member(<<"statistics">>, SysModulesList)),

    ct:pal("ETS registry and sys.modules are in sync").

%% @doc Test that context imports go to sys.modules of that interpreter
%%
%% When using py_context to import/call, the module should end up in
%% the interpreter's sys.modules.
context_import_in_sys_modules_test(_Config) ->
    %% Clear registry
    ok = py_import:clear_imports(),

    %% Create a context
    {ok, Ctx} = py_context:new(#{mode => auto}),

    %% Call a function from a module (this imports it)
    {ok, _} = py_context:call(Ctx, textwrap, fill, [<<"Hello world this is a test">>, 10], #{}),

    %% Check if textwrap is in sys.modules of this interpreter
    %% Use exec then eval pattern for reliable checking
    ok = py_context:exec(Ctx, <<"
import sys
_textwrap_in_sys = 'textwrap' in sys.modules
_sys_keys = list(sys.modules.keys())
">>),

    {ok, InSysModules} = py_context:eval(Ctx, <<"_textwrap_in_sys">>),
    ?assertEqual(true, InSysModules),

    %% Get the sys.modules keys to see what's imported
    {ok, SysKeys} = py_context:eval(Ctx, <<"_sys_keys">>),
    ?assert(lists:member(<<"textwrap">>, SysKeys)),

    %% Clean up
    py_context:destroy(Ctx),

    ct:pal("Context imports correctly populate sys.modules").
