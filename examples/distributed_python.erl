#!/usr/bin/env escript
%%% @doc Distributed Python Execution Example
%%%
%%% This example demonstrates running Python code across distributed
%%% Erlang nodes, similar to patterns shown in Dashbit's Livebook/Pythonx
%%% but using erlang_python's native capabilities.
%%%
%%% Features demonstrated:
%%%   - Remote Python execution via rpc:call
%%%   - Parallel execution across nodes
%%%   - Environment setup on remote nodes
%%%   - Fault-tolerant distributed inference
%%%   - Data partitioning and aggregation
%%%
%%% Prerequisites:
%%%   1. Build the project: rebar3 compile
%%%   2. Start worker nodes (see instructions below)
%%%
%%% Starting worker nodes:
%%%   Terminal 1: erl -sname worker1 -setcookie demo
%%%   Terminal 2: erl -sname worker2 -setcookie demo
%%%   Then on each worker: application:ensure_all_started(erlang_python).
%%%
%%% Run coordinator:
%%%   erl -sname coordinator -setcookie demo
%%%   1> c(distributed_python).
%%%   2> distributed_python:demo().
%%%
%%% Or run standalone (simulated distribution on single node):
%%%   escript examples/distributed_python.erl

-mode(compile).

-export([
    demo/0,
    demo_local/0,
    setup_remote_node/1,
    remote_call/4,
    parallel_map/4,
    distributed_inference/2,
    cluster_status/0
]).

main(_Args) ->
    setup_paths(),
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Distributed Python Execution Example ===~n~n"),

    %% Run local demo (simulates distribution on single node)
    demo_local(),

    io:format("~n=== Done ===~n~n"),
    application:stop(erlang_python).

setup_paths() ->
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir).

%%% ============================================================================
%%% Main Demo Functions
%%% ============================================================================

%% @doc Run demo with actual remote nodes (requires worker nodes to be started)
demo() ->
    Workers = discover_workers(),
    case Workers of
        [] ->
            io:format("No worker nodes found. Running local demo instead.~n"),
            io:format("To run distributed demo:~n"),
            io:format("  1. Start workers: erl -sname worker1 -setcookie demo~n"),
            io:format("  2. On each worker: application:ensure_all_started(erlang_python).~n"),
            demo_local();
        _ ->
            io:format("Found ~p worker nodes: ~p~n~n", [length(Workers), Workers]),
            demo_distributed(Workers)
    end.

%% @doc Run demo locally (simulates distribution patterns on single node)
demo_local() ->
    io:format("--- Running Local Demo (simulated distribution) ---~n~n"),

    %% Demo 1: Basic remote-style execution
    demo_remote_execution(),

    %% Demo 2: Parallel map over data
    demo_parallel_map(),

    %% Demo 3: Async task submission
    demo_async_tasks(),

    %% Demo 4: Simulated cluster status
    demo_cluster_status(),

    ok.

demo_distributed(Workers) ->
    io:format("--- Running Distributed Demo ---~n~n"),

    %% Setup Python on all workers
    io:format("Setting up Python environments on workers...~n"),
    lists:foreach(fun(Node) ->
        case setup_remote_node(Node) of
            ok -> io:format("  ~p: ready~n", [Node]);
            {error, Reason} -> io:format("  ~p: failed (~p)~n", [Node, Reason])
        end
    end, Workers),

    %% Demo 1: Distribute computation across nodes
    demo_distributed_compute(Workers),

    %% Demo 2: Parallel inference
    demo_distributed_inference(Workers),

    %% Demo 3: Pipeline processing
    demo_distributed_pipeline(Workers),

    %% Demo 4: Cluster status
    io:format("~nCluster Status:~n"),
    Status = cluster_status(),
    lists:foreach(fun({Node, S}) ->
        io:format("  ~p: ~p~n", [Node, S])
    end, Status),

    ok.

%%% ============================================================================
%%% Local Demo Functions
%%% ============================================================================

demo_remote_execution() ->
    io:format("1. Remote-style Execution~n"),
    io:format("   (Using local node to demonstrate the API pattern)~n~n"),

    %% This is how you'd call Python on a remote node
    Node = node(),

    %% Mathematical computation
    {ok, Sqrt} = remote_call(Node, math, sqrt, [256]),
    io:format("   math.sqrt(256) = ~p~n", [Sqrt]),

    %% Expression evaluation
    {ok, Result} = rpc:call(Node, py, eval, [<<"2 ** 20">>]),
    io:format("   2 ** 20 = ~p~n", [Result]),

    %% NumPy operation (if available)
    case rpc:call(Node, py, eval, [<<"__import__('numpy').mean([1,2,3,4,5])">>]) of
        {ok, Mean} ->
            io:format("   numpy.mean([1,2,3,4,5]) = ~p~n", [Mean]);
        {error, _} ->
            io:format("   (numpy not available)~n")
    end,

    io:format("~n").

demo_parallel_map() ->
    io:format("2. Parallel Map Pattern~n"),
    io:format("   (Distributing work across contexts)~n~n"),

    %% Data to process
    Numbers = lists:seq(1, 10),

    %% Sequential for comparison
    SeqStart = erlang:monotonic_time(millisecond),
    SeqResults = [begin
        timer:sleep(50),  %% Simulate work
        N * N
    end || N <- Numbers],
    SeqTime = erlang:monotonic_time(millisecond) - SeqStart,

    %% Parallel using multiple Erlang processes
    ParStart = erlang:monotonic_time(millisecond),
    ParResults = parallel_map_local(fun(N) ->
        timer:sleep(50),  %% Simulate work
        N * N
    end, Numbers),
    ParTime = erlang:monotonic_time(millisecond) - ParStart,

    io:format("   Input: ~p~n", [Numbers]),
    io:format("   Sequential: ~p (~p ms)~n", [SeqResults, SeqTime]),
    io:format("   Parallel:   ~p (~p ms)~n", [ParResults, ParTime]),
    io:format("   Speedup: ~.1fx~n~n", [SeqTime / max(ParTime, 1)]).

demo_async_tasks() ->
    io:format("3. Async Task API~n"),
    io:format("   (Non-blocking task submission)~n~n"),

    %% Submit multiple async tasks
    io:format("   Submitting 5 concurrent tasks...~n"),

    Tasks = [
        {math, pow, [2, I]}
        || I <- lists:seq(1, 5)
    ],

    Start = erlang:monotonic_time(millisecond),

    %% Create tasks (non-blocking)
    Refs = [
        py_event_loop:create_task(M, F, A)
        || {M, F, A} <- Tasks
    ],

    %% Await all results
    Results = [py_event_loop:await(Ref, 5000) || Ref <- Refs],
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    io:format("   Results: ~p~n", [[R || {ok, R} <- Results]]),
    io:format("   Completed in ~p ms~n~n", [Elapsed]).

demo_cluster_status() ->
    io:format("4. Cluster Status~n~n"),

    Status = local_status(),
    io:format("   Node: ~p~n", [node()]),
    io:format("   Execution Mode: ~p~n", [maps:get(execution_mode, Status)]),
    io:format("   Contexts: ~p~n", [maps:get(contexts, Status)]),
    io:format("   Current Load: ~p/~p~n", [
        maps:get(load, Status),
        maps:get(max_load, Status)
    ]),
    io:format("~n").

%%% ============================================================================
%%% Distributed Demo Functions
%%% ============================================================================

demo_distributed_compute(Workers) ->
    io:format("~n1. Distributed Computation~n"),
    io:format("   Distributing work across ~p nodes...~n~n", [length(Workers)]),

    %% Data to process
    Data = lists:seq(1, 100),

    %% Partition across workers and use numpy.sqrt which handles arrays
    Start = erlang:monotonic_time(millisecond),
    Results = parallel_map_numpy(Workers, Data),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    io:format("   Processed ~p items in ~p ms~n", [length(Data), Elapsed]),
    io:format("   Sample results: ~p...~n~n", [lists:sublist(Results, 5)]).

%% Use numpy.sqrt for batch processing (handles arrays)
parallel_map_numpy(Nodes, Items) ->
    Partitions = partition(Items, length(Nodes)),
    NodePartitions = lists:zip(Nodes, Partitions),

    %% Submit work to each node asynchronously using numpy
    Keys = [
        {Node, Batch, rpc:async_call(Node, py, eval,
            [<<"list(__import__('numpy').sqrt(data))">>, #{data => Batch}])}
        || {Node, Batch} <- NodePartitions, Batch =/= []
    ],

    %% Collect results
    lists:flatten([
        case rpc:yield(Key) of
            {ok, R} when is_list(R) -> R;
            _ -> []
        end
        || {_Node, _Batch, Key} <- Keys
    ]).

demo_distributed_inference(Workers) ->
    io:format("2. Distributed Inference~n"),
    io:format("   Running ML inference across cluster...~n~n"),

    %% Simulated input data (batch of feature vectors)
    Inputs = [
        [rand:uniform() || _ <- lists:seq(1, 10)]
        || _ <- lists:seq(1, 20)
    ],

    Start = erlang:monotonic_time(millisecond),
    Results = distributed_inference(Workers, Inputs),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    SuccessCount = length([ok || {ok, _} <- Results]),
    io:format("   Processed ~p inputs in ~p ms~n", [length(Inputs), Elapsed]),
    io:format("   Success rate: ~p/~p~n~n", [SuccessCount, length(Inputs)]).

demo_distributed_pipeline(Workers) ->
    io:format("3. Distributed Pipeline~n"),
    io:format("   Running multi-stage pipeline...~n~n"),

    %% Input data
    RawData = lists:seq(1, 50),

    %% Define pipeline stages using numpy (each runs on a different node)
    Stages = [
        {lists:nth(1, Workers), <<"list(__import__('numpy').sqrt(data))">>},
        {lists:nth((2 rem length(Workers)) + 1, Workers), <<"list(__import__('numpy').floor(data))">>}
    ],

    Start = erlang:monotonic_time(millisecond),
    Results = pipeline(RawData, Stages),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    io:format("   Input:  ~p...~n", [lists:sublist(RawData, 5)]),
    io:format("   Output: ~p...~n", [lists:sublist(Results, 5)]),
    io:format("   Pipeline completed in ~p ms~n~n", [Elapsed]).

%%% ============================================================================
%%% Core Distributed Functions
%%% ============================================================================

%% @doc Discover available worker nodes
discover_workers() ->
    [N || N <- nodes(), is_python_ready(N)].

%% @doc Check if a node has erlang_python ready
is_python_ready(Node) ->
    case rpc:call(Node, py, eval, [<<"1 + 1">>], 5000) of
        {ok, 2} -> true;
        _ -> false
    end.

%% @doc Setup Python environment on a remote node
setup_remote_node(Node) ->
    case rpc:call(Node, application, ensure_all_started, [erlang_python], 30000) of
        {ok, _} ->
            %% Verify Python is working
            case rpc:call(Node, py, eval, [<<"1 + 1">>], 5000) of
                {ok, 2} -> ok;
                Error -> {error, {python_check_failed, Error}}
            end;
        Error ->
            {error, {start_failed, Error}}
    end.

%% @doc Execute Python on a remote node with error handling
remote_call(Node, Module, Func, Args) ->
    remote_call(Node, Module, Func, Args, 30000).

remote_call(Node, Module, Func, Args, _Timeout) when Node =:= node() ->
    py:call(Module, Func, Args);
remote_call(Node, Module, Func, Args, Timeout) ->
    case rpc:call(Node, py, call, [Module, Func, Args], Timeout) of
        {badrpc, Reason} ->
            {error, {remote_error, Node, Reason}};
        Result ->
            Result
    end.

%% @doc Map a function over items using multiple remote nodes
parallel_map(Nodes, Module, Func, Items) ->
    %% Partition items across nodes
    Partitions = partition(Items, length(Nodes)),
    NodePartitions = lists:zip(Nodes, Partitions),

    %% Submit work to each node asynchronously
    Keys = [
        {Node, Batch, rpc:async_call(Node, py, call, [Module, Func, [Batch]])}
        || {Node, Batch} <- NodePartitions, Batch =/= []
    ],

    %% Collect results in order
    Results = lists:flatmap(fun({_Node, Batch, Key}) ->
        case rpc:yield(Key) of
            {ok, R} when is_list(R) -> R;
            {ok, R} -> [R || _ <- Batch];
            {badrpc, _} -> [{error, failed} || _ <- Batch]
        end
    end, Keys),

    Results.

%% @doc Distributed ML inference with load balancing
distributed_inference(Nodes, Inputs) ->
    %% Simple round-robin distribution
    IndexedInputs = lists:zip(lists:seq(1, length(Inputs)), Inputs),
    NodeCount = length(Nodes),

    %% Assign each input to a node
    Assignments = [
        {lists:nth(((I - 1) rem NodeCount) + 1, Nodes), Input}
        || {I, Input} <- IndexedInputs
    ],

    %% Submit all inference requests
    Refs = [
        {rpc:async_call(Node, py, eval, [
            <<"sum(x) / len(x)">>,  %% Simple "inference" - compute mean
            #{x => Input}
        ]), Node}
        || {Node, Input} <- Assignments
    ],

    %% Collect results with timeout handling
    [
        case rpc:nb_yield(Key, 5000) of
            {value, Result} -> Result;
            timeout -> {error, timeout}
        end
        || {Key, _Node} <- Refs
    ].

%% @doc Get cluster status
cluster_status() ->
    Nodes = [node() | nodes()],
    [{Node, node_status(Node)} || Node <- Nodes].

node_status(Node) when Node =:= node() ->
    local_status();
node_status(Node) ->
    case rpc:call(Node, distributed_python, local_status, [], 5000) of
        {badrpc, Reason} -> #{status => down, reason => Reason};
        Status -> Status
    end.

local_status() ->
    #{
        status => up,
        execution_mode => py:execution_mode(),
        contexts => py_context_router:num_contexts(),
        load => py_semaphore:current(),
        max_load => py_semaphore:max_concurrent()
    }.

%%% ============================================================================
%%% Pipeline Processing
%%% ============================================================================

%% @doc Run data through a pipeline of stages across nodes
pipeline(Data, Stages) ->
    lists:foldl(fun({Node, Expr}, Acc) ->
        case rpc:call(Node, py, eval, [Expr, #{data => Acc}], 30000) of
            {ok, Result} when is_list(Result) -> Result;
            {ok, Result} -> [Result];
            {badrpc, _} -> Acc;
            {error, _} -> Acc
        end
    end, Data, Stages).

%%% ============================================================================
%%% Helper Functions
%%% ============================================================================

%% @doc Partition a list into N roughly equal parts
partition(List, N) when N > 0 ->
    Len = length(List),
    Size = max(1, (Len + N - 1) div N),
    partition_impl(List, Size, []).

partition_impl([], _Size, Acc) ->
    lists:reverse(Acc);
partition_impl(List, Size, Acc) ->
    {Chunk, Rest} = lists:split(min(Size, length(List)), List),
    partition_impl(Rest, Size, [Chunk | Acc]).

%% @doc Local parallel map using Erlang processes
parallel_map_local(Fun, Items) ->
    Parent = self(),
    Refs = [
        begin
            Ref = make_ref(),
            spawn(fun() ->
                Result = Fun(Item),
                Parent ! {Ref, Result}
            end),
            Ref
        end
        || Item <- Items
    ],
    [receive {Ref, Result} -> Result after 5000 -> {error, timeout} end
     || Ref <- Refs].
