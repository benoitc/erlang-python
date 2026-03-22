%%% @doc Test suite for py:import/1,2 and py:flush_imports/0
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
    flush_imports_test/1,
    import_stats_test/1,
    import_list_test/1,
    import_speeds_up_calls_test/1,
    import_multiprocess_test/1,
    import_concurrent_stress_test/1
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
        flush_imports_test,
        import_stats_test,
        import_list_test,
        import_speeds_up_calls_test,
        import_multiprocess_test,
        import_concurrent_stress_test
    ]}].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    timer:sleep(500),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Flush imports before each test for clean state
    py:flush_imports(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% @doc Test importing a module
import_module_test(_Config) ->
    %% Import json module
    ok = py:import(json),

    %% Verify it works by calling a function
    {ok, Result} = py:call(json, dumps, [[1, 2, 3]]),
    ?assertEqual(<<"[1, 2, 3]">>, Result),

    %% Import with binary name
    ok = py:import(<<"math">>),
    {ok, Pi} = py:call(math, sqrt, [4.0]),
    ?assertEqual(2.0, Pi).

%% @doc Test importing a specific function
import_function_test(_Config) ->
    %% Import json.dumps
    ok = py:import(json, dumps),

    %% Verify it works
    {ok, Result} = py:call(json, dumps, [#{a => 1}]),
    ?assert(is_binary(Result)),

    %% Import with binary names
    ok = py:import(<<"os">>, <<"getcwd">>),
    {ok, Cwd} = py:call(os, getcwd, []),
    ?assert(is_binary(Cwd)).

%% @doc Test that __main__ cannot be imported
import_main_rejected_test(_Config) ->
    %% __main__ should be rejected
    {error, main_not_cacheable} = py:import('__main__'),
    {error, main_not_cacheable} = py:import(<<"__main__">>),

    %% Also for function import
    {error, main_not_cacheable} = py:import('__main__', some_func),
    {error, main_not_cacheable} = py:import(<<"__main__">>, <<"some_func">>).

%% @doc Test importing nonexistent module returns error
import_nonexistent_module_test(_Config) ->
    {error, Reason} = py:import(nonexistent_module_xyz),
    ?assert(is_list(Reason) orelse is_binary(Reason) orelse is_atom(Reason)),
    ct:pal("Import error for nonexistent module: ~p", [Reason]).

%% @doc Test importing nonexistent function returns error
import_nonexistent_function_test(_Config) ->
    %% Module exists but function doesn't
    {error, Reason} = py:import(json, nonexistent_function_xyz),
    ?assert(is_list(Reason) orelse is_binary(Reason) orelse is_atom(Reason)),
    ct:pal("Import error for nonexistent function: ~p", [Reason]).

%% @doc Test that importing same module/function twice is idempotent
import_idempotent_test(_Config) ->
    %% Import multiple times - should all succeed
    ok = py:import(json),
    ok = py:import(json),
    ok = py:import(json),

    ok = py:import(json, dumps),
    ok = py:import(json, dumps),
    ok = py:import(json, dumps),

    %% Still works
    {ok, _} = py:call(json, dumps, [[1]]).

%% @doc Test flushing imports
flush_imports_test(_Config) ->
    %% Import some modules
    ok = py:import(json),
    ok = py:import(math),
    ok = py:import(json, dumps),

    %% Get stats before flush
    {ok, StatsBefore} = py:import_stats(),
    CountBefore = maps:get(count, StatsBefore, 0),
    ?assert(CountBefore >= 3),

    %% Flush
    ok = py:flush_imports(),

    %% Stats should show empty cache
    {ok, StatsAfter} = py:import_stats(),
    CountAfter = maps:get(count, StatsAfter, 0),
    ?assertEqual(0, CountAfter),

    %% Calls still work (they re-import)
    {ok, _} = py:call(json, dumps, [[1]]).

%% @doc Test import stats
import_stats_test(_Config) ->
    %% Start fresh
    ok = py:flush_imports(),

    %% Check empty stats
    {ok, Stats0} = py:import_stats(),
    ?assertEqual(0, maps:get(count, Stats0, 0)),

    %% Import some modules
    ok = py:import(json),
    ok = py:import(math),
    ok = py:import(os),

    %% Check stats
    {ok, Stats1} = py:import_stats(),
    Count1 = maps:get(count, Stats1, 0),
    ?assertEqual(3, Count1),

    %% Import functions
    ok = py:import(json, dumps),
    ok = py:import(json, loads),

    %% Check updated stats (3 modules + 2 functions = 5)
    {ok, Stats2} = py:import_stats(),
    Count2 = maps:get(count, Stats2, 0),
    ?assertEqual(5, Count2).

%% @doc Test listing imports
import_list_test(_Config) ->
    %% Start fresh
    ok = py:flush_imports(),

    %% Empty map
    {ok, Map0} = py:import_list(),
    ?assertEqual(#{}, Map0),

    %% Import some modules and functions
    ok = py:import(json),
    ok = py:import(math),
    ok = py:import(json, dumps),
    ok = py:import(json, loads),

    %% Get map
    {ok, Map1} = py:import_list(),

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
    ok = py:flush_imports(),

    %% Time a cold call (module not imported)
    %% Using json.dumps since hashlib.md5 needs bytes encoding
    {ColdTime, {ok, _}} = timer:tc(fun() ->
        py:call(json, dumps, [[1,2,3,4,5]])
    end),

    %% Pre-import the module and function
    ok = py:import(json),
    ok = py:import(json, dumps),

    %% Time a warm call (module already imported)
    {WarmTime, {ok, _}} = timer:tc(fun() ->
        py:call(json, dumps, [[1,2,3,4,5]])
    end),

    ct:pal("Cold call time: ~p us, Warm call time: ~p us", [ColdTime, WarmTime]),

    %% Warm call should generally be faster, but we don't assert
    %% because timing can be variable. Just log for observation.
    ok.

%% @doc Test that 3 processes can independently cache imports
import_multiprocess_test(_Config) ->
    Parent = self(),

    %% Spawn 3 processes, each importing different modules
    Pid1 = spawn_link(fun() ->
        ok = py:import(json),
        ok = py:import(json, dumps),
        {ok, Stats} = py:import_stats(),
        {ok, List} = py:import_list(),
        %% Verify we can use the cached import
        {ok, _} = py:call(json, dumps, [[1,2,3]]),
        Parent ! {self(), {Stats, List}}
    end),

    Pid2 = spawn_link(fun() ->
        ok = py:import(math),
        ok = py:import(math, sqrt),
        ok = py:import(math, floor),
        {ok, Stats} = py:import_stats(),
        {ok, List} = py:import_list(),
        %% Verify we can use the cached import
        {ok, _} = py:call(math, sqrt, [16.0]),
        Parent ! {self(), {Stats, List}}
    end),

    Pid3 = spawn_link(fun() ->
        ok = py:import(os),
        ok = py:import(os, getcwd),
        ok = py:import(string),
        {ok, Stats} = py:import_stats(),
        {ok, List} = py:import_list(),
        %% Verify we can use the cached import
        {ok, _} = py:call(os, getcwd, []),
        Parent ! {self(), {Stats, List}}
    end),

    %% Collect results from all 3 processes
    Results = [receive {Pid, Result} -> {Pid, Result} after 5000 -> timeout end
               || Pid <- [Pid1, Pid2, Pid3]],

    %% Verify no timeouts
    ?assertEqual(false, lists:member(timeout, Results)),

    %% Extract and verify each process's cache
    [{Pid1, {Stats1, List1}}, {Pid2, {Stats2, List2}}, {Pid3, {Stats3, List3}}] = Results,

    %% Process 1: json + json.dumps = 2 entries
    ?assertEqual(2, maps:get(count, Stats1)),
    ?assert(maps:is_key(<<"json">>, List1)),
    ?assertEqual([<<"dumps">>], maps:get(<<"json">>, List1)),

    %% Process 2: math + math.sqrt + math.floor = 3 entries
    ?assertEqual(3, maps:get(count, Stats2)),
    ?assert(maps:is_key(<<"math">>, List2)),
    MathFuncs = maps:get(<<"math">>, List2),
    ?assertEqual(2, length(MathFuncs)),
    ?assert(lists:member(<<"sqrt">>, MathFuncs)),
    ?assert(lists:member(<<"floor">>, MathFuncs)),

    %% Process 3: os + os.getcwd + string = 3 entries
    ?assertEqual(3, maps:get(count, Stats3)),
    ?assert(maps:is_key(<<"os">>, List3)),
    ?assert(maps:is_key(<<"string">>, List3)),
    ?assertEqual([<<"getcwd">>], maps:get(<<"os">>, List3)),
    ?assertEqual([], maps:get(<<"string">>, List3)),

    ct:pal("Process 1 cache: ~p", [List1]),
    ct:pal("Process 2 cache: ~p", [List2]),
    ct:pal("Process 3 cache: ~p", [List3]).

%% @doc Stress test with many concurrent processes importing simultaneously
import_concurrent_stress_test(_Config) ->
    Parent = self(),
    NumProcesses = 20,
    Modules = [json, math, os, string, re, base64, collections, functools, itertools, operator],

    %% Spawn many processes that all try to import at the same time
    Pids = [spawn_link(fun() ->
        %% Each process imports a random subset of modules
        MyModules = lists:sublist(Modules, 1 + (N rem length(Modules))),
        Results = [{M, py:import(M)} || M <- MyModules],

        %% All imports should succeed
        AllOk = lists:all(fun({_, R}) -> R =:= ok end, Results),

        %% Get stats and verify
        {ok, Stats} = py:import_stats(),
        Count = maps:get(count, Stats),

        %% Make a call to verify cache works
        CallResult = py:call(json, dumps, [[N]]),

        Parent ! {self(), {AllOk, Count, CallResult}}
    end) || N <- lists:seq(1, NumProcesses)],

    %% Collect all results
    Results = [receive {Pid, Result} -> Result after 10000 -> timeout end || Pid <- Pids],

    %% Verify no timeouts
    ?assertEqual(false, lists:member(timeout, Results)),

    %% Verify all processes succeeded
    lists:foreach(fun({AllOk, Count, CallResult}) ->
        ?assertEqual(true, AllOk),
        ?assert(Count >= 1),
        ?assertMatch({ok, _}, CallResult)
    end, Results),

    ct:pal("All ~p processes completed successfully", [NumProcesses]).
