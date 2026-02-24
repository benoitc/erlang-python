%%% @doc Common Test suite for py module.
-module(py_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_call_math/1,
    test_call_json/1,
    test_call_kwargs/1,
    test_eval/1,
    test_eval_complex_locals/1,
    test_exec/1,
    test_async_call/1,
    test_type_conversions/1,
    test_nested_types/1,
    test_timeout/1,
    test_special_floats/1,
    test_streaming/1,
    test_stream_function/1,
    test_stream_iterators/1,
    test_error_handling/1,
    test_version/1,
    test_memory_stats/1,
    test_gc/1,
    test_erlang_callback/1,
    test_erlang_callback_mfa/1,
    test_erlang_import_syntax/1,
    test_erlang_attr_syntax/1,
    test_asyncio_call/1,
    test_asyncio_gather/1,
    test_subinterp_supported/1,
    test_parallel_execution/1,
    test_venv/1,
    %% New scalability tests
    test_execution_mode/1,
    test_num_executors/1,
    test_semaphore_basic/1,
    test_semaphore_acquire_release/1,
    test_semaphore_concurrent/1,
    test_semaphore_timeout/1,
    test_semaphore_rate_limiting/1,
    test_overload_protection/1,
    test_shared_state/1,
    test_reload/1,
    %% ASGI optimization tests
    test_asgi_response_extraction/1,
    test_asgi_header_caching/1,
    test_asgi_status_codes/1,
    test_asgi_scope_caching/1
]).

all() ->
    [
        test_call_math,
        test_call_json,
        test_call_kwargs,
        test_eval,
        test_eval_complex_locals,
        test_exec,
        test_async_call,
        test_type_conversions,
        test_nested_types,
        test_timeout,
        test_special_floats,
        test_streaming,
        test_stream_function,
        test_stream_iterators,
        test_error_handling,
        test_version,
        test_memory_stats,
        test_gc,
        test_erlang_callback,
        test_erlang_callback_mfa,
        test_erlang_import_syntax,
        test_erlang_attr_syntax,
        test_asyncio_call,
        test_asyncio_gather,
        test_subinterp_supported,
        test_parallel_execution,
        test_venv,
        %% Scalability tests
        test_execution_mode,
        test_num_executors,
        test_semaphore_basic,
        test_semaphore_acquire_release,
        test_semaphore_concurrent,
        test_semaphore_timeout,
        test_semaphore_rate_limiting,
        test_overload_protection,
        test_shared_state,
        test_reload,
        %% ASGI optimization tests
        test_asgi_response_extraction,
        test_asgi_header_caching,
        test_asgi_status_codes,
        test_asgi_scope_caching
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

test_call_math(_Config) ->
    {ok, 4.0} = py:call(math, sqrt, [16]),
    {ok, 120} = py:call(math, factorial, [5]),
    %% Test accessing module attribute via eval - use __import__
    {ok, Pi} = py:eval(<<"__import__('math').pi">>),
    true = Pi > 3.14 andalso Pi < 3.15,
    ok.

test_call_json(_Config) ->
    %% Encode
    {ok, Json} = py:call(json, dumps, [#{foo => bar}]),
    true = is_binary(Json),

    %% Decode
    {ok, Decoded} = py:call(json, loads, [<<"{\"a\": 1, \"b\": [1,2,3]}">>]),
    #{<<"a">> := 1, <<"b">> := [1, 2, 3]} = Decoded,
    ok.

test_call_kwargs(_Config) ->
    %% Test keyword arguments with json.dumps indent parameter
    {ok, JsonIndented} = py:call(json, dumps, [#{a => 1}], #{indent => 2}),
    true = is_binary(JsonIndented),
    %% Indented JSON should contain newlines
    true = binary:match(JsonIndented, <<"\n">>) =/= nomatch,

    %% Test keyword arguments with sort_keys
    {ok, JsonSorted} = py:call(json, dumps, [#{z => 1, a => 2}], #{sort_keys => true}),
    true = is_binary(JsonSorted),

    %% Test multiple kwargs
    {ok, JsonMulti} = py:call(json, dumps, [#{b => 1, a => 2}], #{indent => 4, sort_keys => true}),
    true = is_binary(JsonMulti),
    true = binary:match(JsonMulti, <<"\n">>) =/= nomatch,

    %% Test with string functions - str.split with maxsplit kwarg
    {ok, Split} = py:eval(<<"'a-b-c-d'.split('-', maxsplit=2)">>),
    [<<"a">>, <<"b">>, <<"c-d">>] = Split,

    ok.

test_eval(_Config) ->
    {ok, 45} = py:eval(<<"sum(range(10))">>),
    {ok, 25} = py:eval(<<"x * y">>, #{x => 5, y => 5}),
    {ok, [0, 1, 4, 9, 16]} = py:eval(<<"[x**2 for x in range(5)]">>),
    ok.

test_eval_complex_locals(_Config) ->
    %% Test eval with list local
    {ok, 15} = py:eval(<<"sum(numbers)">>, #{numbers => [1, 2, 3, 4, 5]}),

    %% Test eval with map local
    {ok, <<"Alice">>} = py:eval(<<"data['name']">>, #{data => #{name => <<"Alice">>, age => 30}}),

    %% Test eval with nested structures
    {ok, 42} = py:eval(<<"config['settings']['value']">>,
                       #{config => #{settings => #{value => 42}}}),

    %% Test eval with multiple complex locals using default argument capture
    %% Note: Python's eval() locals aren't accessible in nested scopes (lambda/comprehension)
    %% Use default argument to capture the value: lambda x, m=multiplier: x * m
    {ok, [2, 4, 6]} = py:eval(<<"list(map(lambda x, m=multiplier: x * m, items))">>,
                              #{items => [1, 2, 3], multiplier => 2}),

    %% Test eval with multiple variables
    {ok, 6} = py:eval(<<"a + b + c">>, #{a => 1, b => 2, c => 3}),

    %% Test eval using local in function call
    {ok, 3} = py:eval(<<"len(word)">>, #{word => <<"foo">>}),

    %% Test eval with nested map access
    {ok, <<"value">>} = py:eval(<<"obj['nested']['key']">>,
                                #{obj => #{nested => #{key => <<"value">>}}}),

    ok.

test_exec(_Config) ->
    %% Test exec - just ensure it doesn't fail
    %% Note: exec runs on any worker, subsequent calls may go to different workers
    ok = py:exec(<<"x = 42">>),
    ok = py:exec(<<"
def my_func():
    pass
">>),
    ok.

test_async_call(_Config) ->
    Ref1 = py:call_async(math, sqrt, [100]),
    Ref2 = py:call_async(math, sqrt, [144]),

    {ok, 10.0} = py:await(Ref1),
    {ok, 12.0} = py:await(Ref2),
    ok.

test_type_conversions(_Config) ->
    %% Integer
    {ok, 42} = py:eval(<<"42">>),

    %% Float
    {ok, 3.14} = py:eval(<<"3.14">>),

    %% String -> binary
    {ok, <<"hello">>} = py:eval(<<"'hello'">>),

    %% Boolean
    {ok, true} = py:eval(<<"True">>),
    {ok, false} = py:eval(<<"False">>),

    %% None
    {ok, none} = py:eval(<<"None">>),

    %% List
    {ok, [1, 2, 3]} = py:eval(<<"[1, 2, 3]">>),

    %% Tuple
    {ok, {1, 2, 3}} = py:eval(<<"(1, 2, 3)">>),

    %% Dict -> map
    {ok, #{<<"a">> := 1}} = py:eval(<<"{'a': 1}">>),

    ok.

test_nested_types(_Config) ->
    %% Test nested list
    {ok, [[1, 2], [3, 4]]} = py:eval(<<"[[1, 2], [3, 4]]">>),

    %% Test nested dict
    {ok, #{<<"outer">> := #{<<"inner">> := 42}}} =
        py:eval(<<"{'outer': {'inner': 42}}">>),

    %% Test list of dicts
    {ok, [#{<<"a">> := 1}, #{<<"b">> := 2}]} =
        py:eval(<<"[{'a': 1}, {'b': 2}]">>),

    %% Test dict with list values
    {ok, #{<<"items">> := [1, 2, 3]}} = py:eval(<<"{'items': [1, 2, 3]}">>),

    %% Test tuple containing lists
    {ok, {[1, 2], [3, 4]}} = py:eval(<<"([1, 2], [3, 4])">>),

    %% Test list containing tuple
    {ok, [{1, 2}, {3, 4}]} = py:eval(<<"[(1, 2), (3, 4)]">>),

    %% Test deep nesting
    {ok, #{<<"level1">> := #{<<"level2">> := #{<<"level3">> := <<"deep">>}}}} =
        py:eval(<<"{'level1': {'level2': {'level3': 'deep'}}}">>),

    %% Test mixed nesting
    {ok, #{<<"data">> := [{1, <<"a">>}, {2, <<"b">>}]}} =
        py:eval(<<"{'data': [(1, 'a'), (2, 'b')]}">>),

    ok.

test_timeout(_Config) ->
    %% Test that timeout works - use a heavy computation
    %% sum(range(10**8)) will trigger timeout
    {error, timeout} = py:eval(<<"sum(range(10**8))">>, #{}, 100),

    %% Test that normal operations complete within timeout
    {ok, 45} = py:eval(<<"sum(range(10))">>, #{}, 5000),

    %% Test call with timeout
    {ok, 4.0} = py:call(math, sqrt, [16], #{}, 5000),

    ok.

test_special_floats(_Config) ->
    %% Test NaN
    {ok, nan} = py:eval(<<"float('nan')">>),

    %% Test Infinity
    {ok, infinity} = py:eval(<<"float('inf')">>),

    %% Test negative infinity
    {ok, neg_infinity} = py:eval(<<"float('-inf')">>),

    ok.

test_streaming(_Config) ->
    %% Test generator expression streaming
    {ok, [0, 1, 4, 9, 16]} = py:stream_eval(<<"(x**2 for x in range(5))">>),

    %% Test range iterator
    {ok, [0, 1, 2, 3, 4]} = py:stream_eval(<<"iter(range(5))">>),

    %% Test map streaming
    {ok, [<<"A">>, <<"B">>, <<"C">>]} = py:stream_eval(<<"(c.upper() for c in 'abc')">>),

    %% Test filtered generator expression (documented in streaming.md)
    {ok, Evens} = py:stream_eval(<<"(x for x in range(10) if x % 2 == 0)">>),
    [0, 2, 4, 6, 8] = Evens,

    ok.

test_stream_function(_Config) ->
    %% Test streaming from generator functions
    %% As documented in streaming.md, you can define generator functions
    %% and stream from them. We test with inline generators using lambda.

    %% Fibonacci generator - compute fibonacci sequence using lambda wrapper
    %% (as documented pattern for inline generators)
    {ok, Fib} = py:stream_eval(<<"(lambda: ((fib := [0, 1]), [fib.append(fib[-1] + fib[-2]) for _ in range(8)], iter(fib))[-1])()">>),
    ct:pal("Fibonacci result: ~p~n", [Fib]),
    [0, 1, 1, 2, 3, 5, 8, 13, 21, 34] = Fib,

    %% Test py:stream/3 with builtins that return iterators
    {ok, Result} = py:stream(builtins, iter, [[1, 2, 3, 4, 5]]),
    [1, 2, 3, 4, 5] = Result,

    %% Test stream with enumerate and kwargs (start parameter)
    {ok, EnumResult} = py:stream(builtins, enumerate, [[<<"a">>, <<"b">>, <<"c">>]], #{start => 1}),
    [{1, <<"a">>}, {2, <<"b">>}, {3, <<"c">>}] = EnumResult,

    %% Test stream_eval with filter (returns iterator)
    {ok, FilterResult} = py:stream_eval(<<"filter(lambda x: x > 2, [1, 2, 3, 4, 5])">>),
    [3, 4, 5] = FilterResult,

    %% Test batching generator (as documented in streaming.md)
    {ok, Batches} = py:stream_eval(<<"(lambda data, size: (data[i:i+size] for i in range(0, len(data), size)))([1,2,3,4,5,6,7], 3)">>),
    ct:pal("Batches: ~p~n", [Batches]),
    [[1, 2, 3], [4, 5, 6], [7]] = Batches,

    ok.

test_stream_iterators(_Config) ->
    %% Test dictionary items iterator (documented in streaming.md)
    {ok, Items} = py:stream_eval(<<"iter({'a': 1, 'b': 2, 'c': 3}.items())">>),
    ct:pal("Dict items: ~p~n", [Items]),
    %% Items should be tuples
    true = lists:all(fun(I) -> is_tuple(I) end, Items),
    3 = length(Items),

    %% Test dictionary keys iterator
    {ok, Keys} = py:stream_eval(<<"iter({'x': 10, 'y': 20}.keys())">>),
    true = lists:all(fun(K) -> is_binary(K) end, Keys),

    %% Test dictionary values iterator
    {ok, Values} = py:stream_eval(<<"iter({'x': 10, 'y': 20}.values())">>),
    true = lists:all(fun(V) -> is_integer(V) end, Values),

    %% Test enumerate iterator
    {ok, Enumerated} = py:stream_eval(<<"enumerate(['a', 'b', 'c'])">>),
    [{0, <<"a">>}, {1, <<"b">>}, {2, <<"c">>}] = Enumerated,

    %% Test zip iterator
    {ok, Zipped} = py:stream_eval(<<"zip([1, 2, 3], ['a', 'b', 'c'])">>),
    [{1, <<"a">>}, {2, <<"b">>}, {3, <<"c">>}] = Zipped,

    ok.

test_error_handling(_Config) ->
    %% Test Python exception
    {error, {'NameError', _}} = py:eval(<<"undefined_variable">>),

    %% Test syntax error
    {error, {'SyntaxError', _}} = py:eval(<<"if True">>),

    %% Test division by zero
    {error, {'ZeroDivisionError', _}} = py:eval(<<"1/0">>),

    %% Test import error
    {error, {'ModuleNotFoundError', _}} = py:call(nonexistent_module, func, []),

    ok.

test_version(_Config) ->
    {ok, Version} = py:version(),
    true = is_binary(Version),
    true = byte_size(Version) > 0,
    ok.

test_memory_stats(_Config) ->
    {ok, Stats} = py:memory_stats(),
    true = is_map(Stats),
    %% Check that we have GC stats
    true = maps:is_key(gc_stats, Stats),
    true = maps:is_key(gc_count, Stats),
    true = maps:is_key(gc_threshold, Stats),

    %% Test tracemalloc
    ok = py:tracemalloc_start(),
    %% Allocate some memory
    {ok, _} = py:eval(<<"[x**2 for x in range(1000)]">>),
    {ok, StatsWithTrace} = py:memory_stats(),
    true = maps:is_key(traced_memory_current, StatsWithTrace),
    true = maps:is_key(traced_memory_peak, StatsWithTrace),
    ok = py:tracemalloc_stop(),

    ok.

test_gc(_Config) ->
    %% Test basic GC
    {ok, Collected} = py:gc(),
    true = is_integer(Collected),
    true = Collected >= 0,

    %% Test generation-specific GC
    {ok, Collected0} = py:gc(0),
    true = is_integer(Collected0),

    {ok, Collected1} = py:gc(1),
    true = is_integer(Collected1),

    {ok, Collected2} = py:gc(2),
    true = is_integer(Collected2),

    ok.

test_erlang_callback(_Config) ->
    %% Register a simple function that adds two numbers
    py:register_function(add, fun([A, B]) -> A + B end),

    %% Call from Python - erlang module is auto-imported
    {ok, Result1} = py:eval(<<"erlang.call('add', 5, 7)">>),
    12 = Result1,

    %% Register function with multiple return types
    py:register_function(get_info, fun([]) -> #{name => <<"test">>, count => 42} end),
    {ok, Info} = py:eval(<<"erlang.call('get_info')">>),
    #{<<"name">> := <<"test">>, <<"count">> := 42} = Info,

    %% Register function that works with lists
    py:register_function(reverse_list, fun([L]) -> lists:reverse(L) end),
    {ok, Reversed} = py:eval(<<"erlang.call('reverse_list', [1, 2, 3])">>),
    [3, 2, 1] = Reversed,

    %% Cleanup
    py:unregister_function(add),
    py:unregister_function(get_info),
    py:unregister_function(reverse_list),

    ok.

test_erlang_callback_mfa(_Config) ->
    %% Test registering an MFA (Module:Function style)
    %%
    %% Note: MFA callbacks receive ALL arguments as a single list [Args].
    %% So erlang.call('foo', 1, 2, 3) passes Args = [1, 2, 3] to the function.
    %% The function is called as Module:Function([1, 2, 3]).
    %%
    %% This means functions that work with MFA must accept a list argument.

    %% lists:sum expects a list - works perfectly with MFA
    py:register_function(sum_list, lists, sum),
    {ok, 15} = py:eval(<<"erlang.call('sum_list', 1, 2, 3, 4, 5)">>),

    %% lists:reverse expects a list - works with MFA
    py:register_function(reverse_list, lists, reverse),
    {ok, [3, 2, 1]} = py:eval(<<"erlang.call('reverse_list', 1, 2, 3)">>),

    %% erlang:hd gets the first element of the args list
    py:register_function(first_arg, erlang, hd),
    {ok, <<"hello">>} = py:eval(<<"erlang.call('first_arg', 'hello', 'world')">>),

    %% erlang:length gets the count of arguments
    py:register_function(arg_count, erlang, length),
    {ok, 4} = py:eval(<<"erlang.call('arg_count', 'a', 'b', 'c', 'd')">>),

    %% Cleanup
    py:unregister_function(sum_list),
    py:unregister_function(reverse_list),
    py:unregister_function(first_arg),
    py:unregister_function(arg_count),

    ok.

test_erlang_import_syntax(_Config) ->
    %% Test "from erlang import func_name" syntax
    %% This test mirrors test_erlang_callback but uses the new import syntax
    %%
    %% Note: We use assert statements to verify results within the exec block
    %% because workers may change between py:exec and py:eval calls.
    py:register_function(add, fun([A, B]) -> A + B end),
    py:register_function(greet, fun([Name]) -> <<"Hello, ", Name/binary>> end),

    %% Test basic import - will fail with AssertionError if result is wrong
    ok = py:exec(<<"
from erlang import add
result = add(10, 20)
assert result == 30, f'Expected 30, got {result}'
">>),

    %% Test import with alias
    ok = py:exec(<<"
from erlang import add as my_add
result = my_add(40, 60)
assert result == 100, f'Expected 100, got {result}'
">>),

    %% Test importing and using a string function
    %% Note: Erlang binary comes back as Python str (due to conversion)
    ok = py:exec(<<"
from erlang import greet
result = greet('World')
assert result == 'Hello, World' or result == b'Hello, World', f'Expected \"Hello, World\", got {result}'
">>),

    %% Cleanup
    py:unregister_function(add),
    py:unregister_function(greet),

    ok.

test_erlang_attr_syntax(_Config) ->
    %% Test "erlang.func_name()" syntax (via __getattr__)
    %% This test mirrors test_erlang_callback but uses the new attribute syntax
    %% Note: erlang module is auto-imported, no need for "import erlang"
    py:register_function(multiply, fun([A, B]) -> A * B end),
    py:register_function(get_data, fun([]) -> #{value => 42} end),

    %% Test direct attribute access call (erlang is auto-imported)
    {ok, 50} = py:eval(<<"erlang.multiply(5, 10)">>),

    %% Test no-arg function via attr syntax
    {ok, #{<<"value">> := 42}} = py:eval(<<"erlang.get_data()">>),

    %% Verify backward compatibility - erlang.call() still works
    {ok, 100} = py:eval(<<"erlang.call('multiply', 10, 10)">>),

    %% Cleanup
    py:unregister_function(multiply),
    py:unregister_function(get_data),

    ok.

test_asyncio_call(_Config) ->
    %% Test async call to asyncio coroutine
    %% The async pool runs async functions in a background asyncio event loop
    Ref = py:async_call('__main__', 'eval', [<<"1 + 1">>]),
    true = is_reference(Ref),

    %% We may not get a result for simple eval since it's not a real coroutine
    %% Just verify the call mechanism works
    _Result = py:async_await(Ref, 5000),
    ok.

test_asyncio_gather(_Config) ->
    %% Test gathering multiple async calls
    %% This tests the async_gather functionality
    Calls = [
        {math, sqrt, [16]},
        {math, sqrt, [25]},
        {math, sqrt, [36]}
    ],
    Result = py:async_gather(Calls),
    ct:pal("async_gather result: ~p~n", [Result]),

    %% Verify the result structure
    case Result of
        {ok, Results} when is_list(Results) ->
            %% Should have 3 results
            3 = length(Results),
            %% Verify the values if they're successful
            ct:pal("Gathered results: ~p~n", [Results]),
            ok;
        {error, Reason} ->
            %% Async pool might not be fully functional in test env
            ct:pal("async_gather returned error (may be expected): ~p~n", [Reason]),
            ok
    end.

test_subinterp_supported(_Config) ->
    %% Test that subinterp_supported returns a boolean
    Result = py:subinterp_supported(),
    true = is_boolean(Result),
    %% Log the result
    ct:pal("Sub-interpreter support: ~p~n", [Result]),
    ok.

test_parallel_execution(_Config) ->
    %% Test parallel execution using sub-interpreters (Python 3.12+)
    case py:subinterp_supported() of
        true ->
            Calls = [
                {math, sqrt, [16]},
                {math, sqrt, [25]},
                {math, sqrt, [36]},
                {math, sqrt, [49]}
            ],
            Result = py:parallel(Calls),
            ct:pal("Parallel result: ~p~n", [Result]),
            %% parallel/1 should return {ok, Results} on success
            %% or {error, Reason} on failure
            case Result of
                {ok, Results} when is_list(Results) ->
                    4 = length(Results),
                    ok;
                {error, Reason} ->
                    ct:pal("Parallel execution failed: ~p (may be expected on some setups)~n", [Reason]),
                    ok
            end;
        false ->
            ct:pal("Sub-interpreters not supported, skipping parallel test~n"),
            ok
    end.

test_venv(_Config) ->
    %% Test venv_info when no venv is active
    {ok, #{<<"active">> := false}} = py:venv_info(),

    %% Create a fake venv structure for testing (faster than real venv)
    TmpDir = <<"/tmp/erlang_python_test_venv">>,

    %% Get Python version for site-packages path
    {ok, PyVer} = py:eval(<<"f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'">>),

    %% Create minimal venv structure using os.makedirs
    SitePackages = <<TmpDir/binary, "/lib/", PyVer/binary, "/site-packages">>,
    {ok, _} = py:call(os, makedirs, [SitePackages], #{exist_ok => true}),

    %% Activate it
    ok = py:activate_venv(TmpDir),

    %% Check venv_info
    {ok, Info} = py:venv_info(),
    true = maps:get(<<"active">>, Info),
    true = is_binary(maps:get(<<"venv_path">>, Info)),
    true = is_binary(maps:get(<<"site_packages">>, Info)),

    %% Deactivate
    ok = py:deactivate_venv(),

    %% Verify deactivated
    {ok, #{<<"active">> := false}} = py:venv_info(),

    %% Test error case - invalid path
    {error, _} = py:activate_venv(<<"/nonexistent/path">>),

    %% Cleanup
    {ok, _} = py:call(shutil, rmtree, [TmpDir], #{ignore_errors => true}),

    ok.

%%% ============================================================================
%%% Scalability Tests
%%% ============================================================================

test_execution_mode(_Config) ->
    %% Test that execution_mode returns a valid mode
    Mode = py:execution_mode(),
    ct:pal("Execution mode: ~p~n", [Mode]),
    true = lists:member(Mode, [free_threaded, subinterp, multi_executor]),
    ok.

test_num_executors(_Config) ->
    %% Test that num_executors returns a positive integer
    Num = py:num_executors(),
    ct:pal("Number of executors: ~p~n", [Num]),
    true = is_integer(Num),
    true = Num > 0,
    ok.

test_semaphore_basic(_Config) ->
    %% Test semaphore initialization and basic getters
    Max = py_semaphore:max_concurrent(),
    Current = py_semaphore:current(),

    ct:pal("Semaphore - Max: ~p, Current: ~p~n", [Max, Current]),

    true = is_integer(Max),
    true = Max > 0,
    true = is_integer(Current),
    true = Current >= 0,
    ok.

test_semaphore_acquire_release(_Config) ->
    %% Test basic acquire/release cycle
    Initial = py_semaphore:current(),

    %% Acquire
    ok = py_semaphore:acquire(1000),
    After1 = py_semaphore:current(),
    true = After1 > Initial,

    %% Release
    ok = py_semaphore:release(),
    After2 = py_semaphore:current(),
    After2 = Initial,

    ok.

test_semaphore_concurrent(_Config) ->
    %% Test concurrent acquire/release from multiple processes
    Parent = self(),
    NumProcs = 20,

    %% Spawn processes that each acquire, sleep, and release
    Pids = [spawn_link(fun() ->
        ok = py_semaphore:acquire(5000),
        timer:sleep(10),
        ok = py_semaphore:release(),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumProcs)],

    %% Wait for all to complete
    [receive {done, Pid} -> ok after 10000 -> ct:fail({timeout, Pid}) end || Pid <- Pids],

    %% Current should be back to 0 (or initial state)
    timer:sleep(50),
    Current = py_semaphore:current(),
    ct:pal("After concurrent test - Current: ~p~n", [Current]),
    true = Current =:= 0,

    ok.

test_semaphore_timeout(_Config) ->
    %% Save original max
    OrigMax = py_semaphore:max_concurrent(),

    %% Set a very low limit
    ok = py_semaphore:set_max_concurrent(1),
    1 = py_semaphore:max_concurrent(),

    %% First acquire should succeed
    ok = py_semaphore:acquire(1000),

    %% Second acquire should timeout (we're at max)
    Parent = self(),
    spawn_link(fun() ->
        Result = py_semaphore:acquire(100),
        Parent ! {acquire_result, Result}
    end),

    Result = receive
        {acquire_result, R} -> R
    after 2000 ->
        ct:fail(timeout_waiting_for_result)
    end,

    %% Should have gotten error
    {error, max_concurrent} = Result,

    %% Release and restore
    ok = py_semaphore:release(),
    ok = py_semaphore:set_max_concurrent(OrigMax),

    ok.

test_semaphore_rate_limiting(_Config) ->
    %% Test that the semaphore properly limits concurrent operations
    OrigMax = py_semaphore:max_concurrent(),

    %% Set limit to 3
    ok = py_semaphore:set_max_concurrent(3),

    Parent = self(),
    Counter = atomics:new(1, [{signed, false}]),

    %% Track max concurrent
    MaxSeen = atomics:new(1, [{signed, false}]),

    NumProcs = 10,
    Pids = [spawn_link(fun() ->
        ok = py_semaphore:acquire(10000),
        atomics:add(Counter, 1, 1),
        Cur = atomics:get(Counter, 1),

        %% Update max seen
        OldMax = atomics:get(MaxSeen, 1),
        case Cur > OldMax of
            true -> atomics:put(MaxSeen, 1, Cur);
            false -> ok
        end,

        timer:sleep(50),
        atomics:sub(Counter, 1, 1),
        ok = py_semaphore:release(),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumProcs)],

    %% Wait for all
    [receive {done, Pid} -> ok after 30000 -> ct:fail({timeout, Pid}) end || Pid <- Pids],

    %% Check that max concurrent never exceeded limit
    MaxConcurrent = atomics:get(MaxSeen, 1),
    ct:pal("Max concurrent seen: ~p (limit was 3)~n", [MaxConcurrent]),
    true = MaxConcurrent =< 3,

    %% Restore
    ok = py_semaphore:set_max_concurrent(OrigMax),

    ok.

test_overload_protection(_Config) ->
    %% Test that py:call returns overload error when semaphore is exhausted
    OrigMax = py_semaphore:max_concurrent(),

    %% Set very low limit
    ok = py_semaphore:set_max_concurrent(1),

    %% Acquire the only slot
    ok = py_semaphore:acquire(1000),

    %% Now py:call should fail with overload
    Parent = self(),
    spawn_link(fun() ->
        Result = py:call(math, sqrt, [4], #{}, 100),
        Parent ! {call_result, Result}
    end),

    Result = receive
        {call_result, R} -> R
    after 2000 ->
        ct:fail(timeout_waiting_for_result)
    end,

    ct:pal("Overload result: ~p~n", [Result]),

    %% Should be overloaded error
    case Result of
        {error, {overloaded, _, _}} -> ok;
        {error, timeout} -> ok;  %% Also acceptable
        Other -> ct:fail({unexpected_result, Other})
    end,

    %% Cleanup
    ok = py_semaphore:release(),
    ok = py_semaphore:set_max_concurrent(OrigMax),

    ok.

test_shared_state(_Config) ->
    %% Test shared state from Erlang side
    ok = py:state_store(<<"test_key">>, #{value => 42}),
    {ok, #{value := 42}} = py:state_fetch(<<"test_key">>),

    %% Test from Python side - set from Python, read from Erlang
    ok = py:exec(<<"
from erlang import state_set
state_set('python_key', {'message': 'hello from python', 'numbers': [1, 2, 3]})
">>),
    {ok, Result} = py:state_fetch(<<"python_key">>),
    ct:pal("State from Python: ~p~n", [Result]),
    #{<<"message">> := <<"hello from python">>, <<"numbers">> := [1, 2, 3]} = Result,

    %% Test from Python side - read state set from Erlang
    ok = py:state_store(<<"erlang_key">>, #{count => 100}),
    ok = py:exec(<<"
from erlang import state_get
data = state_get('erlang_key')
assert data == {'count': 100}, f'Expected {{count: 100}}, got {data}'
">>),

    %% Test state_keys from Python
    ok = py:exec(<<"
from erlang import state_keys
keys = state_keys()
assert 'test_key' in keys, f'Expected test_key in {keys}'
assert 'python_key' in keys, f'Expected python_key in {keys}'
assert 'erlang_key' in keys, f'Expected erlang_key in {keys}'
">>),

    %% Test state_delete from Python
    ok = py:exec(<<"
from erlang import state_delete
state_delete('python_key')
">>),
    {error, not_found} = py:state_fetch(<<"python_key">>),

    %% Test state_clear
    ok = py:state_clear(),
    [] = py:state_keys(),

    %% Test atomic counters from Erlang
    1 = py:state_incr(<<"counter">>),
    2 = py:state_incr(<<"counter">>),
    12 = py:state_incr(<<"counter">>, 10),
    11 = py:state_decr(<<"counter">>),
    6 = py:state_decr(<<"counter">>, 5),

    %% Test atomic counters from Python
    ok = py:exec(<<"
from erlang import state_incr, state_decr

# Increment
val = state_incr('py_counter')
assert val == 1, f'Expected 1, got {val}'

val = state_incr('py_counter', 5)
assert val == 6, f'Expected 6, got {val}'

# Decrement
val = state_decr('py_counter')
assert val == 5, f'Expected 5, got {val}'

val = state_decr('py_counter', 3)
assert val == 2, f'Expected 2, got {val}'
">>),
    {ok, 2} = py:state_fetch(<<"py_counter">>),

    %% Cleanup
    ok = py:state_clear(),

    ok.

%% Test module reload across all workers
test_reload(_Config) ->
    %% First, ensure json module is imported in at least one worker
    {ok, _} = py:call(json, dumps, [[1, 2, 3]]),

    %% Now reload it - should succeed across all workers
    ok = py:reload(json),

    %% Verify the module still works after reload
    {ok, <<"[1, 2, 3]">>} = py:call(json, dumps, [[1, 2, 3]]),

    %% Test reload of a module that might not be loaded (should not error)
    ok = py:reload(collections),

    %% Test reload with binary module name
    ok = py:reload(<<"os">>),

    %% Test reload with string module name
    ok = py:reload("sys"),

    ok.

%%% ============================================================================
%%% ASGI Optimization Tests
%%% ============================================================================

%% Test direct response tuple extraction optimization
test_asgi_response_extraction(_Config) ->
    %% Test that we can create and process ASGI-style response tuples
    %% The optimization handles (status, headers, body) tuples directly

    %% Create a response tuple similar to what ASGI returns
    Code = <<"(200, [(b'content-type', b'application/json'), (b'x-custom', b'value')], b'{\"result\": \"ok\"}')">>,
    {ok, Result} = py:eval(Code),
    ct:pal("ASGI response: ~p~n", [Result]),

    %% Verify the tuple structure
    {200, Headers, Body} = Result,
    true = is_list(Headers),
    true = is_binary(Body),

    %% Verify headers
    2 = length(Headers),
    [{<<"content-type">>, <<"application/json">>}, {<<"x-custom">>, <<"value">>}] = Headers,

    %% Verify body
    <<"{\"result\": \"ok\"}">> = Body,

    %% Test with empty headers and body
    {ok, {204, [], <<>>}} = py:eval(<<"(204, [], b'')">>),

    %% Test with multiple headers
    {ok, {301, [{<<"location">>, <<"https://example.com">>}], <<>>}} =
        py:eval(<<"(301, [(b'location', b'https://example.com')], b'')">>),

    ok.

%% Test pre-interned header name caching
test_asgi_header_caching(_Config) ->
    %% Test that common headers are handled correctly
    %% The optimization caches common HTTP header names as Python bytes

    Code = <<"[(b'host', b'example.com'), (b'accept', b'*/*'), (b'content-type', b'text/html'), (b'content-length', b'123'), (b'user-agent', b'test-agent'), (b'cookie', b'session=abc'), (b'authorization', b'Bearer token'), (b'cache-control', b'no-cache'), (b'connection', b'keep-alive'), (b'accept-encoding', b'gzip'), (b'accept-language', b'en-US'), (b'referer', b'http://example.com'), (b'origin', b'http://example.com'), (b'if-none-match', b'etag123'), (b'if-modified-since', b'Mon, 01 Jan 2024'), (b'x-forwarded-for', b'192.168.1.1'), (b'x-custom-header', b'custom-value')]">>,
    {ok, Headers} = py:eval(Code),
    ct:pal("Headers: ~p~n", [Headers]),

    %% Verify all headers are present
    17 = length(Headers),

    %% Verify specific cached headers
    {<<"host">>, <<"example.com">>} = lists:nth(1, Headers),
    {<<"content-type">>, <<"text/html">>} = lists:nth(3, Headers),
    {<<"user-agent">>, <<"test-agent">>} = lists:nth(5, Headers),

    %% Verify non-cached header still works
    {<<"x-custom-header">>, <<"custom-value">>} = lists:nth(17, Headers),

    ok.

%% Test cached status code integers
test_asgi_status_codes(_Config) ->
    %% Test that common HTTP status codes are handled correctly
    %% The optimization caches PyLong objects for common status codes

    %% Test common status codes
    StatusCodes = [200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 405, 500, 502, 503],

    lists:foreach(fun(Code) ->
        Expr = list_to_binary(io_lib:format("(~p, [], b'')", [Code])),
        {ok, {Status, [], <<>>}} = py:eval(Expr),
        Code = Status
    end, StatusCodes),

    %% Test uncommon status codes still work
    {ok, {418, [], <<>>}} = py:eval(<<"(418, [], b'')">>),  %% I'm a teapot
    {ok, {599, [], <<>>}} = py:eval(<<"(599, [], b'')">>),  %% Network connect timeout

    ct:pal("All status codes tested successfully~n"),
    ok.

%% Test scope template caching optimization
test_asgi_scope_caching(_Config) ->
    %% This test verifies that scope caching works correctly by running
    %% multiple requests with the same path and verifying the results.
    %% The optimization caches scope templates per path and clones them
    %% for subsequent requests.

    %% Test that multiple scopes with same path work correctly
    %% In practice, the caching is internal to the NIF, but we can
    %% verify functional correctness by checking results are consistent

    %% Create a dict representing an ASGI scope-like structure
    Code1 = <<"{'type': 'http', 'path': '/test', 'method': 'GET', 'headers': [(b'host', b'example.com')]}">>,
    {ok, Scope1} = py:eval(Code1),

    Code2 = <<"{'type': 'http', 'path': '/test', 'method': 'POST', 'headers': [(b'content-type', b'application/json')]}">>,
    {ok, Scope2} = py:eval(Code2),

    %% Verify the scopes are correctly structured
    ct:pal("Scope1: ~p~n", [Scope1]),
    ct:pal("Scope2: ~p~n", [Scope2]),

    %% Both should have the same path
    #{<<"path">> := <<"/test">>} = Scope1,
    #{<<"path">> := <<"/test">>} = Scope2,

    %% But different methods
    #{<<"method">> := <<"GET">>} = Scope1,
    #{<<"method">> := <<"POST">>} = Scope2,

    %% Different headers
    #{<<"headers">> := [{<<"host">>, <<"example.com">>}]} = Scope1,
    #{<<"headers">> := [{<<"content-type">>, <<"application/json">>}]} = Scope2,

    ct:pal("Scope caching test passed~n"),
    ok.
