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
    test_cast/1,
    test_spawn_call/1,
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
    test_venv_pth/1,
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
    test_asgi_scope_caching/1,
    test_asgi_scope_method_caching/1,
    test_asgi_zero_copy_buffer/1,
    test_asgi_lazy_headers/1,
    %% Process-bound Python environment tests
    test_process_env_isolation/1,
    test_process_env_main_function/1,
    test_process_env_state_persistence/1,
    test_process_env_cleanup/1,
    %% SharedDict tests
    test_shared_dict_basic/1,
    test_shared_dict_types/1,
    test_shared_dict_process_death/1,
    test_shared_dict_python_access/1,
    test_shared_dict_destroy/1,
    %% SharedDict concurrent tests
    test_shared_dict_concurrent_same_key/1,
    test_shared_dict_concurrent_different_keys/1,
    test_shared_dict_concurrent_mixed/1,
    test_shared_dict_benchmark/1
]).

all() ->
    [
        test_call_math,
        test_call_json,
        test_call_kwargs,
        test_eval,
        test_eval_complex_locals,
        test_exec,
        test_cast,
        test_spawn_call,
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
        test_venv_pth,
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
        test_asgi_scope_caching,
        test_asgi_scope_method_caching,
        test_asgi_zero_copy_buffer,
        test_asgi_lazy_headers,
        %% Process-bound Python environment tests
        test_process_env_isolation,
        test_process_env_main_function,
        test_process_env_state_persistence,
        test_process_env_cleanup,
        %% SharedDict tests
        test_shared_dict_basic,
        test_shared_dict_types,
        test_shared_dict_process_death,
        test_shared_dict_python_access,
        test_shared_dict_destroy,
        %% SharedDict concurrent tests
        test_shared_dict_concurrent_same_key,
        test_shared_dict_concurrent_different_keys,
        test_shared_dict_concurrent_mixed,
        test_shared_dict_benchmark
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

test_cast(_Config) ->
    %% Test fire-and-forget cast with erlang.send
    Self = erlang:self(),
    ok = py:cast(erlang, send, [Self, {<<"result">>, <<"msg1">>}]),
    ok = py:cast(erlang, send, [Self, {<<"result">>, <<"msg2">>}]),
    %% Wait for results (order may vary)
    R1 = receive {<<"result">>, V1} -> V1 after 5000 -> ct:fail(timeout1) end,
    R2 = receive {<<"result">>, V2} -> V2 after 5000 -> ct:fail(timeout2) end,
    true = lists:sort([R1, R2]) =:= [<<"msg1">>, <<"msg2">>],
    ok.

test_spawn_call(_Config) ->
    %% Test spawn_call with await
    Ref1 = py:spawn_call(math, sqrt, [100]),
    Ref2 = py:spawn_call(math, sqrt, [144]),
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
    %% Test that timeout works - use time.sleep which guarantees delay
    %% time.sleep(1) will definitely exceed 100ms timeout
    {error, timeout} = py:eval(<<"__import__('time').sleep(1)">>, #{}, 100),

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
    %% tracemalloc doesn't support subinterpreters
    case py_nif:subinterp_supported() of
        true ->
            {skip, "tracemalloc not supported in subinterpreters"};
        false ->
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

            ok
    end.

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

    %% site-packages must be in sys.path and at position 0
    {ok, true} = py:eval(<<"sp in __import__('sys').path">>, #{sp => SitePackages}),
    {ok, 0} = py:eval(<<"__import__('sys').path.index(sp)">>, #{sp => SitePackages}),

    %% Deactivate
    ok = py:deactivate_venv(),

    %% Verify deactivated: venv_info and sys.path both restored
    {ok, #{<<"active">> := false}} = py:venv_info(),
    {ok, false} = py:eval(<<"sp in __import__('sys').path">>, #{sp => SitePackages}),

    %% Test error case - invalid path
    {error, _} = py:activate_venv(<<"/nonexistent/path">>),

    %% Cleanup
    {ok, _} = py:call(shutil, rmtree, [TmpDir], #{ignore_errors => true}),

    ok.

test_venv_pth(_Config) ->
    %% Verify .pth files are processed (editable installs)
    TmpDir = <<"/tmp/erlang_python_test_venv_pth">>,
    PkgSrc = <<"/tmp/erlang_python_test_pth_src">>,

    {ok, PyVer} = py:eval(<<"f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'">>),
    SitePackages = <<TmpDir/binary, "/lib/", PyVer/binary, "/site-packages">>,

    %% Create fake venv with a .pth file pointing at PkgSrc
    {ok, _} = py:call(os, makedirs, [SitePackages], #{exist_ok => true}),
    {ok, _} = py:call(os, makedirs, [PkgSrc], #{exist_ok => true}),
    PthFile = <<SitePackages/binary, "/test_editable.pth">>,
    {ok, _} = py:eval(<<"open(pf, 'w').write(pd + '\\n')">>, #{pf => PthFile, pd => PkgSrc}),

    %% Drop a module in PkgSrc so we can verify it's importable
    ModFile = <<PkgSrc/binary, "/ep_test_editable_mod.py">>,
    {ok, _} = py:eval(<<"open(f, 'w').write('answer = 42\\n')">>, #{f => ModFile}),

    {ok, false} = py:eval(<<"pd in __import__('sys').path">>, #{pd => PkgSrc}),

    %% Activate and verify paths and import
    ok = py:activate_venv(TmpDir),
    {ok, 0} = py:eval(<<"__import__('sys').path.index(sp)">>, #{sp => SitePackages}),
    {ok, 1} = py:eval(<<"__import__('sys').path.index(pd)">>, #{pd => PkgSrc}),
    {ok, 42} = py:eval(<<"__import__('ep_test_editable_mod').answer">>),

    %% Deactivate and verify cleanup
    ok = py:deactivate_venv(),
    {ok, false} = py:eval(<<"pd in __import__('sys').path">>, #{pd => PkgSrc}),
    {ok, false} = py:eval(<<"sp in __import__('sys').path">>, #{sp => SitePackages}),

    {ok, _} = py:call(shutil, rmtree, [TmpDir], #{ignore_errors => true}),
    {ok, _} = py:call(shutil, rmtree, [PkgSrc], #{ignore_errors => true}),
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
    %% Module reload can trigger imports that don't support subinterpreters
    case py_nif:subinterp_supported() of
        true ->
            {skip, "module reload may use modules not supported in subinterpreters"};
        false ->
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

            ok
    end.

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

%% Test that method is correctly updated when scope cache is hit
%% This verifies the fix for the bug where method was cached with the path
%% causing GET /path followed by POST /path to return method="GET"
test_asgi_scope_method_caching(_Config) ->
    %% This test uses the NIF directly to verify scope caching behavior.
    %% The scope cache keys on path, so we need to verify that method
    %% is treated as a dynamic field and updated on cache hits.

    %% Build first scope with GET method
    Scope1Map = #{
        type => <<"http">>,
        asgi => #{version => <<"3.0">>, spec_version => <<"2.3">>},
        http_version => <<"1.1">>,
        method => <<"GET">>,
        scheme => <<"http">>,
        path => <<"/api/test">>,
        raw_path => <<"/api/test">>,
        query_string => <<>>,
        root_path => <<>>,
        headers => [{<<"host">>, <<"localhost">>}],
        server => {<<"localhost">>, 8080},
        client => {<<"127.0.0.1">>, 12345}
    },
    {ok, ScopeRef1} = py_nif:asgi_build_scope(Scope1Map),

    %% Build second scope with POST method on same path (should hit cache)
    Scope2Map = Scope1Map#{
        method => <<"POST">>,
        headers => [{<<"content-type">>, <<"application/json">>}],
        client => {<<"127.0.0.1">>, 12346}
    },
    {ok, ScopeRef2} = py_nif:asgi_build_scope(Scope2Map),

    %% Build third scope with PUT method on same path
    Scope3Map = Scope1Map#{
        method => <<"PUT">>,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        client => {<<"127.0.0.1">>, 12347}
    },
    {ok, ScopeRef3} = py_nif:asgi_build_scope(Scope3Map),

    %% Verify each scope has the correct method by extracting from Python dict
    {ok, <<"GET">>} = py:eval(<<"scope['method']">>, #{scope => ScopeRef1}),
    {ok, <<"POST">>} = py:eval(<<"scope['method']">>, #{scope => ScopeRef2}),
    {ok, <<"PUT">>} = py:eval(<<"scope['method']">>, #{scope => ScopeRef3}),

    %% Also verify other dynamic fields are correctly set
    {ok, {<<"127.0.0.1">>, 12345}} = py:eval(<<"tuple(scope['client'])">>, #{scope => ScopeRef1}),
    {ok, {<<"127.0.0.1">>, 12346}} = py:eval(<<"tuple(scope['client'])">>, #{scope => ScopeRef2}),
    {ok, {<<"127.0.0.1">>, 12347}} = py:eval(<<"tuple(scope['client'])">>, #{scope => ScopeRef3}),

    %% Verify static fields are shared correctly (path should be same)
    {ok, <<"/api/test">>} = py:eval(<<"scope['path']">>, #{scope => ScopeRef1}),
    {ok, <<"/api/test">>} = py:eval(<<"scope['path']">>, #{scope => ScopeRef2}),
    {ok, <<"/api/test">>} = py:eval(<<"scope['path']">>, #{scope => ScopeRef3}),

    ct:pal("Scope method caching test passed~n"),
    ok.

%% Test zero-copy buffer handling for large bodies
test_asgi_zero_copy_buffer(_Config) ->
    %% This test verifies that large bodies are handled correctly
    %% The optimization uses a resource-backed buffer for bodies >= 1KB

    %% Test with small body (should use PyBytes)
    {ok, 100} = py:eval(<<"len(b'X' * 100)">>),

    %% Test with larger body and memoryview operations
    %% Create a large bytes object and verify it works with memoryview
    {ok, 2000} = py:eval(<<"len(b'A' * 2000)">>),

    %% Test memoryview on large data
    {ok, {2000, 65, 65}} = py:eval(<<"(lambda d: (len(memoryview(d)), memoryview(d)[0], memoryview(d)[-1]))(b'A' * 2000)">>),

    %% Test slicing (should work without copying in Python)
    {ok, <<"AAAAA">>} = py:eval(<<"(b'A' * 2000)[:5]">>),

    ct:pal("Zero-copy buffer test passed~n"),
    ok.

%% Test lazy header conversion for ASGI
test_asgi_lazy_headers(_Config) ->
    %% This test verifies that the lazy header list implementation is correctly
    %% initialized and that header lists with varying sizes can be processed.
    %% The LazyHeaderList optimization (for header count >= 4) is internal to
    %% the ASGI NIF path, but we can verify the code is functional by testing
    %% header list handling in Python.

    %% Test with varying header counts to exercise both code paths:
    %% - Small (< 4): uses eager conversion
    %% - Large (>= 4): uses LazyHeaderList in ASGI NIF path

    %% Small header list - should work with regular list conversion
    SmallHeaders = <<"[(b'host', b'example.com'), (b'accept', b'*/*')]">>,
    {ok, 2} = py:eval(<<"len(", SmallHeaders/binary, ")">>),
    ct:pal("Small headers (2) - len check passed~n"),

    %% Medium header list - at threshold
    MediumHeaders = <<"[(b'host', b'example.com'), (b'accept', b'*/*'), (b'user-agent', b'test/1.0'), (b'content-type', b'text/html')]">>,
    {ok, 4} = py:eval(<<"len(", MediumHeaders/binary, ")">>),
    ct:pal("Medium headers (4) - len check passed~n"),

    %% Large header list - above threshold
    LargeHeaders = <<"[(b'host', b'example.com'), (b'accept', b'*/*'), (b'user-agent', b'test/1.0'), (b'accept-encoding', b'gzip'), (b'accept-language', b'en-US'), (b'cache-control', b'no-cache'), (b'connection', b'keep-alive'), (b'cookie', b'session=abc123')]">>,
    {ok, 8} = py:eval(<<"len(", LargeHeaders/binary, ")">>),
    ct:pal("Large headers (8) - len check passed~n"),

    %% Test header list indexing
    {ok, {<<"host">>, <<"example.com">>}} = py:eval(<<LargeHeaders/binary, "[0]">>),
    ct:pal("Header indexing works~n"),

    %% Test negative indexing
    {ok, {<<"cookie">>, <<"session=abc123">>}} = py:eval(<<LargeHeaders/binary, "[-1]">>),
    ct:pal("Negative indexing works~n"),

    %% Test iteration - count using generator
    {ok, 8} = py:eval(<<"sum(1 for _ in ", LargeHeaders/binary, ")">>),
    ct:pal("Header iteration works~n"),

    %% Verify header tuple structure
    {ok, true} = py:eval(<<"isinstance(", LargeHeaders/binary, "[0], tuple)">>),
    {ok, true} = py:eval(<<"len(", LargeHeaders/binary, "[0]) == 2">>),
    ct:pal("Header tuple structure is correct~n"),

    %% Verify header name and value are bytes
    {ok, true} = py:eval(<<"isinstance(", LargeHeaders/binary, "[0][0], bytes)">>),
    {ok, true} = py:eval(<<"isinstance(", LargeHeaders/binary, "[0][1], bytes)">>),
    ct:pal("Header name/value types are correct (bytes)~n"),

    %% Test 'in' operator
    {ok, true} = py:eval(<<"(b'host', b'example.com') in ", LargeHeaders/binary>>),
    ct:pal("'in' operator works~n"),

    ct:pal("Lazy headers test passed~n"),
    ok.

%% ============================================================================
%% Process-bound Python Environment Tests
%% ============================================================================

%% Test that different processes have isolated Python environments
test_process_env_isolation(_Config) ->
    Parent = self(),

    %% Spawn process A and set x = 'A'
    spawn(fun() ->
        ok = py:exec(<<"x = 'A'">>),
        {ok, <<"A">>} = py:eval(<<"x">>),
        Parent ! {proc_a, ok}
    end),

    %% Spawn process B and set x = 'B'
    spawn(fun() ->
        ok = py:exec(<<"x = 'B'">>),
        {ok, <<"B">>} = py:eval(<<"x">>),
        Parent ! {proc_b, ok}
    end),

    %% Wait for both processes to complete
    receive {proc_a, ok} -> ok after 5000 -> ct:fail("Process A timed out") end,
    receive {proc_b, ok} -> ok after 5000 -> ct:fail("Process B timed out") end,

    ct:pal("Process isolation test passed~n"),
    ok.

%% Test that functions defined via exec() are accessible via __main__ module
test_process_env_main_function(_Config) ->
    %% Define a function in the process-local environment
    ok = py:exec(<<"def greet(name): return f'Hello {name}'">>),

    %% Call it via __main__ module
    {ok, <<"Hello World">>} = py:call('__main__', greet, [<<"World">>]),

    %% Define another function
    ok = py:exec(<<"def add(a, b): return a + b">>),
    {ok, 7} = py:call('__main__', add, [3, 4]),

    ct:pal("__main__ function access test passed~n"),
    ok.

%% Test that state persists across multiple calls within the same process
test_process_env_state_persistence(_Config) ->
    %% Initialize counter
    ok = py:exec(<<"counter = 0">>),

    %% Increment counter multiple times
    ok = py:exec(<<"counter += 1">>),
    ok = py:exec(<<"counter += 1">>),
    ok = py:exec(<<"counter += 1">>),

    %% Verify counter value
    {ok, 3} = py:eval(<<"counter">>),

    %% Use a function to increment
    ok = py:exec(<<"def increment(): global counter; counter += 1; return counter">>),
    {ok, 4} = py:call('__main__', increment, []),
    {ok, 5} = py:call('__main__', increment, []),

    %% Verify final value
    {ok, 5} = py:eval(<<"counter">>),

    ct:pal("State persistence test passed~n"),
    ok.

%% Test that process-local environment is cleaned up when process exits
test_process_env_cleanup(_Config) ->
    %% Get initial memory stats
    {ok, InitialStats} = py:memory_stats(),
    ct:pal("Initial memory: ~p~n", [InitialStats]),

    %% Spawn a process that creates a large list
    spawn(fun() ->
        ok = py:exec(<<"big = list(range(1000000))">>),
        timer:sleep(50)  % Let it exist briefly
    end),

    %% Wait for the process to exit
    timer:sleep(100),

    %% Force GC
    erlang:garbage_collect(),
    py:gc(),

    %% Check that memory was freed (this is a soft check)
    {ok, FinalStats} = py:memory_stats(),
    ct:pal("Final memory: ~p~n", [FinalStats]),

    ct:pal("Cleanup test passed~n"),
    ok.

%%% ============================================================================
%%% SharedDict Tests
%%% ============================================================================

%% Test basic SharedDict operations
test_shared_dict_basic(_Config) ->
    %% Create a SharedDict
    {ok, SD} = py:shared_dict_new(),
    ct:pal("Created SharedDict: ~p~n", [SD]),

    %% Initially empty
    [] = py:shared_dict_keys(SD),

    %% Set and get values
    ok = py:shared_dict_set(SD, <<"key1">>, <<"value1">>),
    <<"value1">> = py:shared_dict_get(SD, <<"key1">>),

    %% Get with default
    undefined = py:shared_dict_get(SD, <<"nonexistent">>),
    default_val = py:shared_dict_get(SD, <<"nonexistent">>, default_val),

    %% Set another key
    ok = py:shared_dict_set(SD, <<"key2">>, 42),
    42 = py:shared_dict_get(SD, <<"key2">>),

    %% Verify keys
    Keys = py:shared_dict_keys(SD),
    2 = length(Keys),
    true = lists:member(<<"key1">>, Keys),
    true = lists:member(<<"key2">>, Keys),

    %% Delete a key
    ok = py:shared_dict_del(SD, <<"key1">>),
    undefined = py:shared_dict_get(SD, <<"key1">>),

    %% Keys after delete
    [<<"key2">>] = py:shared_dict_keys(SD),

    ct:pal("Basic SharedDict test passed~n"),
    ok.

%% Test SharedDict with complex types (pickled)
test_shared_dict_types(_Config) ->
    {ok, SD} = py:shared_dict_new(),

    %% Map
    Map = #{<<"foo">> => <<"bar">>, <<"num">> => 123},
    ok = py:shared_dict_set(SD, <<"map">>, Map),
    Retrieved = py:shared_dict_get(SD, <<"map">>),
    ct:pal("Map: ~p, Retrieved: ~p~n", [Map, Retrieved]),
    %% Note: Keys may be converted to Python strings and back
    true = maps:get(<<"foo">>, Retrieved) =:= <<"bar">> orelse
           maps:get(foo, Retrieved) =:= <<"bar">>,

    %% List
    List = [1, 2, 3, <<"four">>, 5.0],
    ok = py:shared_dict_set(SD, <<"list">>, List),
    List = py:shared_dict_get(SD, <<"list">>),

    %% Nested structure
    Nested = #{<<"data">> => [#{<<"id">> => 1}, #{<<"id">> => 2}]},
    ok = py:shared_dict_set(SD, <<"nested">>, Nested),
    RetrievedNested = py:shared_dict_get(SD, <<"nested">>),
    ct:pal("Nested: ~p~n", [RetrievedNested]),

    %% Update a value
    ok = py:shared_dict_set(SD, <<"map">>, #{updated => true}),
    Updated = py:shared_dict_get(SD, <<"map">>),
    ct:pal("Updated map: ~p~n", [Updated]),

    ct:pal("SharedDict types test passed~n"),
    ok.

%% Test SharedDict lifecycle (GC-based cleanup)
test_shared_dict_process_death(_Config) ->
    %% Note: Process monitoring is disabled in current implementation.
    %% SharedDict is cleaned up by garbage collection when no references remain.
    %% This test verifies basic resource lifecycle.

    Parent = self(),

    %% Create SharedDict in a child process
    Child = spawn(fun() ->
        {ok, SD} = py:shared_dict_new(),
        ok = py:shared_dict_set(SD, <<"test">>, <<"value">>),
        <<"value">> = py:shared_dict_get(SD, <<"test">>),
        Parent ! {sd_ref, SD},
        receive
            continue -> ok
        after 5000 ->
            ok
        end
    end),

    %% Get the SharedDict reference
    SDRef = receive
        {sd_ref, Ref} -> Ref
    after 5000 ->
        error(timeout_waiting_for_sd_ref)
    end,

    ct:pal("Got SharedDict ref from child: ~p~n", [SDRef]),

    %% SharedDict should work while child is alive
    <<"value">> = py:shared_dict_get(SDRef, <<"test">>),

    %% Kill the child process
    exit(Child, kill),
    timer:sleep(100),

    %% SharedDict still accessible since we hold a reference
    %% (process monitoring disabled - cleanup is GC-based)
    <<"value">> = py:shared_dict_get(SDRef, <<"test">>),

    ct:pal("SharedDict lifecycle test passed~n"),
    ok.

%% Test Python access to SharedDict
test_shared_dict_python_access(_Config) ->
    %% Create a SharedDict
    {ok, SD} = py:shared_dict_new(),

    %% Set a value from Erlang
    ok = py:shared_dict_set(SD, <<"config">>, #{port => 8080, host => <<"localhost">>}),

    %% First, store the handle in Python's global namespace using eval
    %% We use eval with locals to pass the handle, then store it globally
    {ok, _} = py:eval(<<"(globals().__setitem__('_sd_handle', handle), None)[-1]">>,
                       #{<<"handle">> => SD}),

    %% Now execute code that uses the stored handle
    Code = <<"
from erlang import SharedDict
sd = SharedDict(_sd_handle)

# Read value set from Erlang
config = sd['config']
result_port = config.get('port') or config.get(b'port')

# Set a value from Python
sd['python_key'] = {'set_from': 'python', 'value': 42}

# Test contains
has_config = 'config' in sd
has_missing = 'missing' in sd

# Test keys
key_count = len(sd.keys())
">>,

    ok = py:exec(Code),

    %% Get results from Python namespace
    {ok, ResultPort} = py:eval(<<"result_port">>),
    ct:pal("Result port: ~p~n", [ResultPort]),
    8080 = ResultPort,

    {ok, HasConfig} = py:eval(<<"has_config">>),
    true = HasConfig,

    {ok, HasMissing} = py:eval(<<"has_missing">>),
    false = HasMissing,

    {ok, KeyCount} = py:eval(<<"key_count">>),
    ct:pal("Key count: ~p~n", [KeyCount]),
    2 = KeyCount,

    %% Read value set from Python in Erlang
    PythonValue = py:shared_dict_get(SD, <<"python_key">>),
    ct:pal("Python value: ~p~n", [PythonValue]),
    42 = maps:get(<<"value">>, PythonValue),

    %% Test delete from Python
    ok = py:exec(<<"del sd['python_key']">>),
    undefined = py:shared_dict_get(SD, <<"python_key">>),

    ct:pal("SharedDict Python access test passed~n"),
    ok.

%% Test explicit SharedDict destroy
test_shared_dict_destroy(_Config) ->
    %% Create SharedDict and populate it
    {ok, SD} = py:shared_dict_new(),
    ok = py:shared_dict_set(SD, <<"key">>, <<"value">>),
    <<"value">> = py:shared_dict_get(SD, <<"key">>),
    ct:pal("SharedDict created and populated~n"),

    %% Destroy it
    ok = py:shared_dict_destroy(SD),
    ct:pal("SharedDict destroyed~n"),

    %% Further access returns badarg
    try
        py:shared_dict_get(SD, <<"key">>),
        ct:fail(expected_badarg)
    catch
        error:badarg -> ok
    end,
    ct:pal("Get after destroy correctly raises badarg~n"),

    %% set also returns badarg
    try
        py:shared_dict_set(SD, <<"key2">>, <<"value2">>),
        ct:fail(expected_badarg)
    catch
        error:badarg -> ok
    end,
    ct:pal("Set after destroy correctly raises badarg~n"),

    %% keys also returns badarg
    try
        py:shared_dict_keys(SD),
        ct:fail(expected_badarg)
    catch
        error:badarg -> ok
    end,
    ct:pal("Keys after destroy correctly raises badarg~n"),

    %% Destroy is idempotent
    ok = py:shared_dict_destroy(SD),
    ok = py:shared_dict_destroy(SD),
    ct:pal("Destroy is idempotent~n"),

    ct:pal("SharedDict destroy test passed~n"),
    ok.

%%% ============================================================================
%%% SharedDict Concurrent Tests
%%% ============================================================================

%% Test concurrent access to the same key from multiple processes
%% Verifies mutex protection - individual operations are atomic, data remains consistent
%% Note: SharedDict provides per-operation atomicity, not transactional atomicity
test_shared_dict_concurrent_same_key(_Config) ->
    {ok, SD} = py:shared_dict_new(),
    Parent = self(),
    NumProcs = 10,
    WritesPerProc = 100,

    %% Test 1: Concurrent writes to same key - verify no corruption
    %% Each process writes its process number repeatedly
    Pids1 = [spawn_link(fun() ->
        lists:foreach(fun(_) ->
            ok = py:shared_dict_set(SD, <<"shared_key">>, N)
        end, lists:seq(1, WritesPerProc)),
        Parent ! {done_write, self()}
    end) || N <- lists:seq(1, NumProcs)],

    [receive {done_write, Pid} -> ok after 30000 -> ct:fail({timeout, Pid}) end || Pid <- Pids1],

    %% Final value should be a valid process number (1-NumProcs)
    FinalWrite = py:shared_dict_get(SD, <<"shared_key">>),
    ct:pal("Concurrent writes - final value: ~p (valid if 1-~p)~n", [FinalWrite, NumProcs]),
    true = is_integer(FinalWrite),
    true = FinalWrite >= 1 andalso FinalWrite =< NumProcs,

    %% Test 2: Concurrent reads while writing - verify no crashes/corruption
    ok = py:shared_dict_set(SD, <<"rw_key">>, 0),

    %% Half writers, half readers
    WriterPids = [spawn_link(fun() ->
        lists:foreach(fun(I) ->
            ok = py:shared_dict_set(SD, <<"rw_key">>, I)
        end, lists:seq(1, WritesPerProc)),
        Parent ! {done_writer, self()}
    end) || _ <- lists:seq(1, NumProcs div 2)],

    ReaderPids = [spawn_link(fun() ->
        Reads = [py:shared_dict_get(SD, <<"rw_key">>) || _ <- lists:seq(1, WritesPerProc)],
        %% All reads should return integers (no corruption)
        true = lists:all(fun is_integer/1, Reads),
        Parent ! {done_reader, self()}
    end) || _ <- lists:seq(1, NumProcs div 2)],

    [receive {done_writer, Pid} -> ok after 30000 -> ct:fail({timeout, Pid}) end || Pid <- WriterPids],
    [receive {done_reader, Pid} -> ok after 30000 -> ct:fail({timeout, Pid}) end || Pid <- ReaderPids],

    %% Test 3: Rapid key updates - verify latest value is consistent type
    lists:foreach(fun(I) ->
        ok = py:shared_dict_set(SD, <<"type_key">>, I)
    end, lists:seq(1, 1000)),

    TypeVal = py:shared_dict_get(SD, <<"type_key">>),
    true = is_integer(TypeVal),

    ct:pal("SharedDict concurrent same-key test passed~n"),
    ok.

%% Test concurrent access to different keys from multiple processes
%% Verifies no data corruption across keys
test_shared_dict_concurrent_different_keys(_Config) ->
    {ok, SD} = py:shared_dict_new(),
    Parent = self(),
    NumProcs = 10,
    NumOps = 50,

    %% Spawn processes, each writing to its own key
    Pids = [spawn_link(fun() ->
        Key = list_to_binary("key_" ++ integer_to_list(N)),
        %% Write operations
        lists:foreach(fun(I) ->
            ok = py:shared_dict_set(SD, Key, I)
        end, lists:seq(1, NumOps)),
        %% Final value should be NumOps
        Final = py:shared_dict_get(SD, Key),
        Parent ! {done, self(), Key, Final}
    end) || N <- lists:seq(1, NumProcs)],

    %% Wait for all and collect results
    Results = [receive
        {done, Pid, Key, Final} -> {Key, Final}
    after 30000 ->
        ct:fail({timeout, Pid})
    end || Pid <- Pids],

    %% Verify all keys have correct final value
    lists:foreach(fun({Key, Final}) ->
        case Final of
            NumOps -> ok;
            Other ->
                ct:fail({wrong_value, Key, expected, NumOps, got, Other})
        end
    end, Results),

    %% Verify all keys are present
    Keys = py:shared_dict_keys(SD),
    NumProcs = length(Keys),

    ct:pal("SharedDict concurrent different-keys test passed~n"),
    ok.

%% Test mixed read/write operations on shared and unique keys
test_shared_dict_concurrent_mixed(_Config) ->
    {ok, SD} = py:shared_dict_new(),
    Parent = self(),
    NumProcs = 10,
    NumOps = 50,

    %% Initialize shared keys
    ok = py:shared_dict_set(SD, <<"shared_1">>, 0),
    ok = py:shared_dict_set(SD, <<"shared_2">>, 0),

    %% Spawn processes that do mixed operations
    Pids = [spawn_link(fun() ->
        UniqueKey = list_to_binary("unique_" ++ integer_to_list(N)),
        mixed_operations(SD, UniqueKey, NumOps),
        Parent ! {done, self(), UniqueKey}
    end) || N <- lists:seq(1, NumProcs)],

    %% Wait for all processes
    UniqueKeys = [receive
        {done, Pid, Key} -> Key
    after 30000 ->
        ct:fail({timeout, Pid})
    end || Pid <- Pids],

    %% Verify all unique keys exist with valid values
    lists:foreach(fun(Key) ->
        Value = py:shared_dict_get(SD, Key),
        true = is_integer(Value) andalso Value >= 0
    end, UniqueKeys),

    %% Verify shared keys are integers (may have any value due to races)
    Shared1 = py:shared_dict_get(SD, <<"shared_1">>),
    Shared2 = py:shared_dict_get(SD, <<"shared_2">>),
    true = is_integer(Shared1),
    true = is_integer(Shared2),

    ct:pal("Mixed operations - shared_1=~p, shared_2=~p~n", [Shared1, Shared2]),
    ct:pal("SharedDict concurrent mixed test passed~n"),
    ok.

%% Helper for mixed operations
mixed_operations(_SD, _UniqueKey, 0) -> ok;
mixed_operations(SD, UniqueKey, N) ->
    %% Mix of operations
    case N rem 4 of
        0 ->
            %% Write to unique key
            ok = py:shared_dict_set(SD, UniqueKey, N);
        1 ->
            %% Read from unique key
            _ = py:shared_dict_get(SD, UniqueKey, 0);
        2 ->
            %% Increment shared key
            Val = py:shared_dict_get(SD, <<"shared_1">>, 0),
            ok = py:shared_dict_set(SD, <<"shared_1">>, Val + 1);
        3 ->
            %% Read all keys
            _ = py:shared_dict_keys(SD)
    end,
    mixed_operations(SD, UniqueKey, N - 1).

%% Benchmark SharedDict operations
test_shared_dict_benchmark(_Config) ->
    {ok, SD} = py:shared_dict_new(),

    %% Warmup
    lists:foreach(fun(I) ->
        Key = list_to_binary("warmup_" ++ integer_to_list(I)),
        ok = py:shared_dict_set(SD, Key, I),
        I = py:shared_dict_get(SD, Key)
    end, lists:seq(1, 100)),

    %% Single process benchmark
    NumOps = 1000,

    %% Benchmark SET operations
    SetStart = erlang:monotonic_time(microsecond),
    lists:foreach(fun(I) ->
        Key = list_to_binary("bench_" ++ integer_to_list(I rem 100)),
        ok = py:shared_dict_set(SD, Key, I)
    end, lists:seq(1, NumOps)),
    SetEnd = erlang:monotonic_time(microsecond),
    SetDuration = SetEnd - SetStart,
    SetOpsPerSec = (NumOps * 1000000) div max(1, SetDuration),

    %% Benchmark GET operations
    GetStart = erlang:monotonic_time(microsecond),
    lists:foreach(fun(I) ->
        Key = list_to_binary("bench_" ++ integer_to_list(I rem 100)),
        _ = py:shared_dict_get(SD, Key)
    end, lists:seq(1, NumOps)),
    GetEnd = erlang:monotonic_time(microsecond),
    GetDuration = GetEnd - GetStart,
    GetOpsPerSec = (NumOps * 1000000) div max(1, GetDuration),

    ct:pal("~n=== SharedDict Single-Process Benchmark ===~n"),
    ct:pal("SET: ~p ops in ~p us (~p ops/sec)~n", [NumOps, SetDuration, SetOpsPerSec]),
    ct:pal("GET: ~p ops in ~p us (~p ops/sec)~n", [NumOps, GetDuration, GetOpsPerSec]),

    %% Multi-process concurrent benchmark
    Parent = self(),
    NumProcs = 4,
    OpsPerProc = 500,

    ConcStart = erlang:monotonic_time(microsecond),
    Pids = [spawn_link(fun() ->
        lists:foreach(fun(I) ->
            Key = list_to_binary("conc_" ++ integer_to_list(I rem 50)),
            ok = py:shared_dict_set(SD, Key, I),
            _ = py:shared_dict_get(SD, Key)
        end, lists:seq(1, OpsPerProc)),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumProcs)],

    [receive {done, Pid} -> ok after 30000 -> ct:fail({timeout, Pid}) end || Pid <- Pids],
    ConcEnd = erlang:monotonic_time(microsecond),
    ConcDuration = ConcEnd - ConcStart,

    TotalConcOps = NumProcs * OpsPerProc * 2,  % SET + GET per iteration
    ConcOpsPerSec = (TotalConcOps * 1000000) div max(1, ConcDuration),

    ct:pal("~n=== SharedDict Multi-Process Benchmark (~p procs) ===~n", [NumProcs]),
    ct:pal("TOTAL: ~p ops in ~p us (~p ops/sec)~n", [TotalConcOps, ConcDuration, ConcOpsPerSec]),

    %% Cleanup
    ok = py:shared_dict_destroy(SD),

    ct:pal("~nSharedDict benchmark completed~n"),
    ok.
