#!/usr/bin/env escript
%%% @doc Erlang Concurrency example - Call Erlang from Python with async.
%%%
%%% This example demonstrates:
%%%   - Registering Erlang functions callable from Python
%%%   - Spawning Erlang processes from Python to achieve parallelism
%%%   - Comparing sequential vs parallel execution performance
%%%   - Leveraging Erlang's lightweight processes from Python
%%%
%%% Key insight: Parallelism happens on the Erlang side!
%%% Python calls Erlang with a batch of work, and Erlang spawns
%%% lightweight processes to handle each item concurrently.
%%%
%%% Prerequisites:
%%%   1. Build the project: rebar3 compile
%%%
%%% Run from project root:
%%%   escript examples/erlang_concurrency.erl

-mode(compile).

main(_Args) ->
    setup_paths(),
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Erlang Concurrency from Python Example ===~n~n"),

    %% Add examples directory to Python path
    ExamplesDir = examples_dir(),
    ok = add_to_python_path(ExamplesDir),

    %% Register Erlang functions that Python can call
    io:format("Registering Erlang functions...~n"),
    register_functions(),

    %% Run the demonstrations
    demo_basic_callbacks(),
    demo_parallel_execution(),
    demo_erlang_processes(),
    demo_distributed_work(),

    io:format("~n=== Done ===~n~n"),
    cleanup().

setup_paths() ->
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir).

examples_dir() ->
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    list_to_binary(filename:join(ProjectRoot, "examples")).

add_to_python_path(Dir) ->
    {ok, _} = py:eval(<<"(__import__('sys').path.insert(0, path) if path not in __import__('sys').path else None, True)[1]">>, #{path => Dir}),
    ok.

cleanup() ->
    %% Unregister all functions
    lists:foreach(fun(Name) ->
        py:unregister_function(Name)
    end, [slow_compute, fast_compute, spawn_workers, parallel_fetch,
          parallel_map, fib, prime_check, process_item, echo]),
    ok = application:stop(erlang_python).

%%% ============================================================================
%%% Register Erlang Functions for Python
%%% ============================================================================

register_functions() ->
    %% Simple echo function
    py:register_function(echo, fun([Msg]) -> Msg end),

    %% Simulated slow computation (sleeps to simulate I/O or heavy work)
    py:register_function(slow_compute, fun([N]) ->
        timer:sleep(100),  % 100ms delay
        N * N
    end),

    %% Fast computation (no delay)
    py:register_function(fast_compute, fun([N]) ->
        N * N
    end),

    %% Fibonacci calculation (CPU-bound)
    py:register_function(fib, fun([N]) ->
        fib(N)
    end),

    %% Prime check (CPU-bound)
    py:register_function(prime_check, fun([N]) ->
        is_prime(N)
    end),

    %% CORE FUNCTION: Parallel map using Erlang processes
    %% This is the key function that enables Python to use Erlang's concurrency
    py:register_function(parallel_map, fun([FuncName, Items]) ->
        parallel_map_impl(FuncName, Items)
    end),

    %% Process an item with spawned Erlang process
    py:register_function(process_item, fun([Item]) ->
        Parent = self(),
        Ref = make_ref(),
        spawn(fun() ->
            %% Simulate processing
            timer:sleep(50),
            Result = process_data(Item),
            Parent ! {Ref, Result}
        end),
        receive
            {Ref, Result} -> Result
        after 5000 ->
            {error, timeout}
        end
    end),

    %% Spawn multiple Erlang workers and collect results
    py:register_function(spawn_workers, fun([Tasks]) ->
        Parent = self(),
        Refs = lists:map(fun(Task) ->
            Ref = make_ref(),
            spawn(fun() ->
                Result = execute_task(Task),
                Parent ! {Ref, Result}
            end),
            Ref
        end, Tasks),
        %% Collect all results
        lists:map(fun(Ref) ->
            receive
                {Ref, Result} -> Result
            after 5000 ->
                {error, timeout}
            end
        end, Refs)
    end),

    %% Parallel fetch simulation using Erlang processes
    py:register_function(parallel_fetch, fun([Urls]) ->
        Parent = self(),
        Refs = lists:map(fun(Url) ->
            Ref = make_ref(),
            spawn(fun() ->
                %% Simulate HTTP fetch with variable latency
                Latency = rand:uniform(100) + 50,
                timer:sleep(Latency),
                Result = #{url => Url, status => 200, latency_ms => Latency},
                Parent ! {Ref, Result}
            end),
            Ref
        end, Urls),
        lists:map(fun(Ref) ->
            receive
                {Ref, Result} -> Result
            after 5000 ->
                {error, timeout}
            end
        end, Refs)
    end),

    io:format("  Registered: echo, slow_compute, fast_compute, fib, prime_check~n"),
    io:format("  Registered: parallel_map, process_item, spawn_workers, parallel_fetch~n"),
    ok.

%% Implementation of parallel_map: spawns one process per item
parallel_map_impl(FuncName, Items) when is_binary(FuncName) ->
    parallel_map_impl(binary_to_atom(FuncName, utf8), Items);
parallel_map_impl(FuncName, Items) ->
    Parent = self(),
    %% Spawn a process for each item
    RefsAndItems = lists:map(fun(Item) ->
        Ref = make_ref(),
        spawn(fun() ->
            %% Execute the named function
            Result = execute_function(FuncName, Item),
            Parent ! {Ref, Result}
        end),
        {Ref, Item}
    end, Items),
    %% Collect results in order
    lists:map(fun({Ref, _Item}) ->
        receive
            {Ref, Result} -> Result
        after 5000 ->
            {error, timeout}
        end
    end, RefsAndItems).

%% Execute a registered function by name
execute_function(slow_compute, N) ->
    timer:sleep(100),
    N * N;
execute_function(fast_compute, N) ->
    N * N;
execute_function(fib, N) ->
    fib(N);
execute_function(prime_check, N) ->
    is_prime(N);
execute_function(process_item, Item) ->
    timer:sleep(50),
    process_data(Item);
execute_function(Name, Arg) ->
    %% Fallback: try to call registered function
    case py_callback:lookup(atom_to_binary(Name, utf8)) of
        {ok, Fun} when is_function(Fun, 1) ->
            Fun([Arg]);
        {ok, {Module, Function}} ->
            apply(Module, Function, [[Arg]]);
        {error, not_found} ->
            {error, {unknown_function, Name}}
    end.

%%% ============================================================================
%%% Demonstrations
%%% ============================================================================

demo_basic_callbacks() ->
    io:format("~n--- Demo 1: Basic Erlang Callbacks ---~n"),

    %% Test echo
    {ok, Echo} = py:eval(<<"__import__('erlang_concurrency').call_erlang('echo', 'Hello from Python!')">>),
    io:format("  Echo result: ~s~n", [Echo]),

    %% Test computation
    {ok, Square} = py:eval(<<"__import__('erlang_concurrency').call_erlang('fast_compute', 7)">>),
    io:format("  fast_compute(7) = ~p~n", [Square]),

    %% Test Fibonacci
    {ok, Fib10} = py:eval(<<"__import__('erlang_concurrency').call_erlang('fib', 10)">>),
    io:format("  fib(10) = ~p~n", [Fib10]),

    ok.

demo_parallel_execution() ->
    io:format("~n--- Demo 2: Sequential vs Parallel Execution ---~n"),

    %% Prepare test data
    {ok, NumbersPy} = py:eval(<<"list(range(1, 11))">>),

    io:format("  Processing 10 items with slow_compute (100ms each)...~n"),

    %% Sequential execution (from Python, calling Erlang one at a time)
    io:format("~n  Sequential execution (10 x 100ms = ~~1 second expected):~n"),
    {ok, {SeqResults, SeqTime}} = py:eval(
        <<"__import__('erlang_concurrency').demo_sequential('slow_compute', items)">>,
        #{items => NumbersPy}
    ),
    io:format("    Results: ~p~n", [SeqResults]),
    io:format("    Time: ~.2f seconds~n", [SeqTime]),

    %% Parallel execution (Python calls Erlang once, Erlang spawns 10 processes)
    io:format("~n  Parallel execution (10 processes, ~~0.1 second expected):~n"),
    {ok, {ParResults, ParTime}} = py:eval(
        <<"__import__('erlang_concurrency').demo_parallel('slow_compute', items)">>,
        #{items => NumbersPy}
    ),
    io:format("    Results: ~p~n", [ParResults]),
    io:format("    Time: ~.2f seconds~n", [ParTime]),

    Speedup = SeqTime / max(ParTime, 0.001),
    io:format("~n  Speedup: ~.1fx faster with Erlang parallel processes!~n", [Speedup]),

    ok.

demo_erlang_processes() ->
    io:format("~n--- Demo 3: Spawning Erlang Processes from Python ---~n"),

    %% Process items using Erlang processes spawned in parallel
    Items = [<<"task_a">>, <<"task_b">>, <<"task_c">>, <<"task_d">>, <<"task_e">>],

    io:format("  Processing ~p items via parallel Erlang processes...~n", [length(Items)]),

    StartTime = erlang:monotonic_time(millisecond),
    {ok, Results} = py:eval(
        <<"__import__('erlang_concurrency').parallel_map('process_item', items)">>,
        #{items => Items}
    ),
    EndTime = erlang:monotonic_time(millisecond),

    io:format("  Completed ~p items in ~p ms (each takes 50ms, parallel!)~n",
              [length(Items), EndTime - StartTime]),
    io:format("  Results:~n"),
    lists:foreach(fun(R) ->
        io:format("    ~p~n", [R])
    end, Results),

    ok.

demo_distributed_work() ->
    io:format("~n--- Demo 4: Distributed Work with Erlang Workers ---~n"),

    %% Use spawn_workers to demonstrate Erlang process parallelism
    Tasks = [
        #{type => <<"compute">>, value => 100},
        #{type => <<"compute">>, value => 200},
        #{type => <<"io">>, value => <<"fetch_data">>},
        #{type => <<"compute">>, value => 300},
        #{type => <<"io">>, value => <<"process_file">>}
    ],

    io:format("  Submitting ~p tasks to Erlang worker pool...~n", [length(Tasks)]),

    StartTime = erlang:monotonic_time(millisecond),
    {ok, Results} = py:eval(
        <<"__import__('erlang_concurrency').call_erlang('spawn_workers', tasks)">>,
        #{tasks => Tasks}
    ),
    EndTime = erlang:monotonic_time(millisecond),

    io:format("  Completed in ~p ms~n", [EndTime - StartTime]),
    io:format("  Results:~n"),
    lists:foreach(fun(R) ->
        io:format("    ~p~n", [R])
    end, Results),

    %% Demonstrate parallel_fetch
    io:format("~n  Simulating parallel HTTP fetches via Erlang processes...~n"),
    Urls = [
        <<"https://api.example.com/users">>,
        <<"https://api.example.com/posts">>,
        <<"https://api.example.com/comments">>,
        <<"https://api.example.com/albums">>,
        <<"https://api.example.com/photos">>
    ],

    StartTime2 = erlang:monotonic_time(millisecond),
    {ok, FetchResults} = py:eval(
        <<"__import__('erlang_concurrency').batch_fetch(urls)">>,
        #{urls => Urls}
    ),
    EndTime2 = erlang:monotonic_time(millisecond),

    %% Calculate total latency if sequential
    TotalLatency = lists:sum([maps:get(<<"latency_ms">>, R, 0) || R <- FetchResults]),
    io:format("  Fetched ~p URLs in ~p ms (would be ~p ms sequential):~n",
              [length(Urls), EndTime2 - StartTime2, TotalLatency]),
    lists:foreach(fun(R) ->
        io:format("    ~s: ~p ms~n",
                  [maps:get(<<"url">>, R), maps:get(<<"latency_ms">>, R)])
    end, FetchResults),

    ok.

%%% ============================================================================
%%% Helper Functions
%%% ============================================================================

fib(0) -> 0;
fib(1) -> 1;
fib(N) when N > 1 -> fib(N-1) + fib(N-2).

is_prime(N) when N < 2 -> false;
is_prime(2) -> true;
is_prime(N) when N rem 2 =:= 0 -> false;
is_prime(N) -> is_prime(N, 3).

is_prime(N, I) when I * I > N -> true;
is_prime(N, I) when N rem I =:= 0 -> false;
is_prime(N, I) -> is_prime(N, I + 2).

process_data(Item) when is_binary(Item) ->
    %% Simulate data processing
    #{
        original => Item,
        processed => <<Item/binary, "_processed">>,
        timestamp => erlang:system_time(millisecond)
    };
process_data(Item) ->
    #{original => Item, processed => Item}.

execute_task(#{<<"type">> := <<"compute">>, <<"value">> := V}) ->
    timer:sleep(50),  % Simulate computation
    #{result => V * 2, type => compute};
execute_task(#{<<"type">> := <<"io">>, <<"value">> := V}) ->
    timer:sleep(rand:uniform(100)),  % Simulate I/O with variable latency
    #{result => V, type => io};
execute_task(Task) ->
    #{result => Task, type => unknown}.
