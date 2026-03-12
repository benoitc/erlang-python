# Distributed Python Execution

This guide covers running Python code across distributed Erlang nodes, enabling horizontal scaling of Python workloads using Erlang's built-in distribution.

## Overview

erlang_python integrates with Erlang's distribution to run Python code on remote nodes. This enables:

- **Horizontal scaling** - Distribute Python workloads across a cluster
- **Resource isolation** - Run memory-intensive ML models on dedicated nodes
- **Fault tolerance** - Leverage Erlang supervision across nodes
- **Location transparency** - Same API for local and remote execution

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Distributed Python with Erlang                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Node A (Coordinator)              Node B (Worker)         Node C (Worker) │
│   ────────────────────              ────────────────         ────────────── │
│                                                                              │
│   ┌──────────────────┐              ┌──────────────┐        ┌──────────────┐│
│   │  Erlang Process  │──rpc:call───▶│ py:call(M,F) │        │ py:call(M,F) ││
│   │  (orchestrator)  │              │      │       │        │      │       ││
│   └──────────────────┘              │      ▼       │        │      ▼       ││
│           │                         │  ┌────────┐  │        │  ┌────────┐  ││
│           │                         │  │ Python │  │        │  │ Python │  ││
│           │                         │  │Context │  │        │  │Context │  ││
│           │                         │  └────────┘  │        │  └────────┘  ││
│           │                         └──────────────┘        └──────────────┘│
│           │                                │                       │        │
│           │◀───────────────────────────────┴───────────────────────┘        │
│           │            Results via Erlang distribution                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Features

| Feature | API |
|---------|-----|
| Remote execution | `rpc:call(Node, py, call, [M, F, A])` |
| Async tasks | `rpc:call(Node, py_event_loop, create_task, ...)` |
| Venv management | `py:ensure_venv/2,3` |
| Data streaming | `py_channel` API |
| Pool routing | Dual pool (default/io) |

## Basic Remote Execution

### Using rpc:call

The simplest way to run Python on a remote node:

```erlang
%% Execute Python on a remote node
{ok, Result} = rpc:call('worker@host', py, call, [math, sqrt, [16]]).
%% => {ok, 4.0}

%% Evaluate expression remotely
{ok, Value} = rpc:call('worker@host', py, eval, [<<"2 ** 10">>]).
%% => {ok, 1024}

%% Execute statement remotely (side effects)
ok = rpc:call('worker@host', py, exec, [<<"import numpy as np">>]).
```

### Async Remote Calls

Use `rpc:async_call` for non-blocking remote execution:

```erlang
%% Submit multiple remote calls concurrently
Keys = [
    rpc:async_call('node1@host', py, call, [model, predict, [Data1]]),
    rpc:async_call('node2@host', py, call, [model, predict, [Data2]]),
    rpc:async_call('node3@host', py, call, [model, predict, [Data3]])
],

%% Collect results
Results = [rpc:yield(Key) || Key <- Keys].
```

### Using the Async Task API Remotely

The Async Task API works seamlessly across nodes:

```erlang
%% Create async task on remote node
Ref = rpc:call('worker@host', py_event_loop, create_task,
    [aiohttp, get, [<<"https://api.example.com">>]]),

%% Do other work locally...

%% Await result from remote task
{ok, Response} = rpc:call('worker@host', py_event_loop, await, [Ref, 10000]).
```

### Elixir Example

Distributed Python works seamlessly from Elixir:

```elixir
defmodule DistributedPython do
  @moduledoc "Distributed Python execution helpers"

  @doc "Execute Python on a remote node"
  def remote_call(node, module, func, args) do
    :rpc.call(node, :py, :call, [module, func, args])
  end

  @doc "Parallel map across cluster nodes"
  def parallel_map(nodes, module, func, items) do
    # Partition items across nodes
    chunks = Enum.chunk_every(items, ceil(length(items) / length(nodes)))

    # Submit work to each node concurrently
    tasks =
      Enum.zip(nodes, chunks)
      |> Enum.map(fn {node, chunk} ->
        Task.async(fn ->
          :rpc.call(node, :py, :eval, [
            "list(__import__('numpy').sqrt(data))",
            %{data: chunk}
          ])
        end)
      end)

    # Collect results
    tasks
    |> Task.await_many(30_000)
    |> Enum.flat_map(fn
      {:ok, result} -> result
      _ -> []
    end)
  end

  @doc "Run inference across GPU nodes"
  def distributed_inference(nodes, inputs) do
    inputs
    |> Enum.with_index()
    |> Enum.map(fn {input, i} ->
      node = Enum.at(nodes, rem(i, length(nodes)))
      Task.async(fn ->
        :rpc.call(node, :py, :call, [:model, :predict, [input]])
      end)
    end)
    |> Task.await_many(60_000)
  end
end

# Usage
nodes = [:"worker1@host", :"worker2@host"]

# Parallel computation
results = DistributedPython.parallel_map(nodes, :math, :sqrt, Enum.to_list(1..100))

# Distributed inference
predictions = DistributedPython.distributed_inference(nodes, test_data)
```

## Environment Management

### Setting Up Remote Nodes

Each node needs erlang_python started with the same Python environment:

```erlang
%% On coordinator node - setup worker
setup_worker(Node) ->
    %% Ensure application is started
    ok = rpc:call(Node, application, ensure_all_started, [erlang_python]),

    %% Setup virtual environment with dependencies
    {ok, VenvPath} = rpc:call(Node, py, ensure_venv, [
        <<"/opt/app/venv">>,
        [<<"numpy">>, <<"pandas">>, <<"scikit-learn">>]
    ]),

    %% Verify setup
    {ok, true} = rpc:call(Node, py, eval, [<<"'numpy' in dir()">>]),
    ok.
```

### Reproducible Environments

Create identical environments across all nodes:

```erlang
%% Define environment specification
-define(PYTHON_DEPS, [
    <<"numpy==1.26.0">>,
    <<"pandas==2.1.0">>,
    <<"scikit-learn==1.3.0">>,
    <<"torch==2.1.0">>
]).

%% Setup all worker nodes
setup_cluster(Nodes) ->
    lists:foreach(fun(Node) ->
        {ok, _} = rpc:call(Node, py, ensure_venv, [
            <<"/opt/app/venv">>,
            ?PYTHON_DEPS
        ])
    end, Nodes).
```

## Parallel Execution Patterns

### Map-Reduce Style

Distribute work across nodes and aggregate results:

```erlang
%% Partition data across nodes
parallel_map(Nodes, Module, Func, Items) ->
    Partitions = partition(Items, length(Nodes)),
    NodePartitions = lists:zip(Nodes, Partitions),

    %% Submit work to each node
    Keys = [
        {Node, rpc:async_call(Node, py, call, [Module, Func, [Batch]])}
        || {Node, Batch} <- NodePartitions
    ],

    %% Collect and flatten results
    Results = [rpc:yield(Key) || {_Node, Key} <- Keys],
    lists:flatten([R || {ok, R} <- Results]).

%% Helper: partition list into N roughly equal parts
partition(List, N) ->
    Len = length(List),
    Size = (Len + N - 1) div N,
    partition(List, Size, []).

partition([], _Size, Acc) ->
    lists:reverse(Acc);
partition(List, Size, Acc) ->
    {Chunk, Rest} = lists:split(min(Size, length(List)), List),
    partition(Rest, Size, [Chunk | Acc]).
```

### ML Inference Distribution

Distribute inference across GPU nodes:

```erlang
%% Route inference to available GPU nodes
-module(distributed_inference).
-export([predict/2, predict_batch/2]).

predict(Model, Input) ->
    Node = select_gpu_node(),
    rpc:call(Node, py, call, [Model, predict, [Input]]).

predict_batch(Model, Inputs) ->
    Nodes = gpu_nodes(),
    parallel_map(Nodes, Model, predict, Inputs).

%% Select least loaded GPU node
select_gpu_node() ->
    Nodes = gpu_nodes(),
    Loads = [{Node, get_load(Node)} || Node <- Nodes],
    {Node, _} = lists:min(fun({_, L1}, {_, L2}) -> L1 =< L2 end, Loads),
    Node.

get_load(Node) ->
    case rpc:call(Node, py_semaphore, current, []) of
        {badrpc, _} -> infinity;
        Load -> Load
    end.

gpu_nodes() ->
    [N || N <- nodes(), is_gpu_node(N)].

is_gpu_node(Node) ->
    case rpc:call(Node, py, eval, [<<"__import__('torch').cuda.is_available()">>]) of
        {ok, true} -> true;
        _ -> false
    end.
```

### Pipeline Processing

Chain processing stages across specialized nodes:

```erlang
%% Run a multi-stage pipeline across nodes
pipeline(Data, Stages) ->
    lists:foldl(fun({Node, Module, Func}, Acc) ->
        {ok, Result} = rpc:call(Node, py, call, [Module, Func, [Acc]]),
        Result
    end, Data, Stages).

%% Example: ML pipeline
run_ml_pipeline(RawData) ->
    pipeline(RawData, [
        {'preprocess@cluster', preprocessor, clean_data},
        {'feature@cluster', feature_eng, extract_features},
        {'model@cluster', classifier, predict},
        {'postprocess@cluster', formatter, format_output}
    ]).
```

## Data Transfer Patterns

### Efficient Binary Transfer

For large data, use binary encoding:

```erlang
%% Transfer numpy array efficiently
transfer_array(Node, Array) ->
    %% Convert to binary on source
    {ok, Binary} = py:call(numpy, ndarray, tobytes, [Array]),
    {ok, Shape} = py:call(Array, shape, []),
    {ok, Dtype} = py:eval(<<"str(arr.dtype)">>, #{arr => Array}),

    %% Reconstruct on target
    rpc:call(Node, py, eval, [
        <<"__import__('numpy').frombuffer(data, dtype=dtype).reshape(shape)">>,
        #{data => Binary, dtype => Dtype, shape => Shape}
    ]).
```

### Streaming with Channels

Use channels for streaming data between nodes:

```erlang
%% Stream data from remote node
stream_from_node(Node, Generator, Args) ->
    %% Create channel on remote node
    {ok, Channel} = rpc:call(Node, py_channel, new, [#{max_size => 100}]),

    %% Start generator on remote node (sends to channel)
    spawn(Node, fun() ->
        py:call(Generator, run, [Channel | Args])
    end),

    %% Receive items locally
    receive_stream(Node, Channel, []).

receive_stream(Node, Channel, Acc) ->
    case rpc:call(Node, py_channel, recv, [Channel, 5000]) of
        {ok, Item} ->
            receive_stream(Node, Channel, [Item | Acc]);
        {error, closed} ->
            lists:reverse(Acc);
        {error, timeout} ->
            lists:reverse(Acc)
    end.
```

## Fault Tolerance

### Supervised Remote Execution

Wrap remote calls with supervision:

```erlang
-module(remote_py_worker).
-behaviour(gen_server).

-export([start_link/1, call/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(Node) ->
    gen_server:start_link(?MODULE, Node, []).

call(Pid, Module, Func, Args) ->
    gen_server:call(Pid, {call, Module, Func, Args}, 30000).

init(Node) ->
    %% Monitor node connection
    net_kernel:monitor_nodes(true),
    {ok, #{node => Node, connected => net_adm:ping(Node) =:= pong}}.

handle_call({call, M, F, A}, _From, #{node := Node, connected := true} = State) ->
    case rpc:call(Node, py, call, [M, F, A], 30000) of
        {badrpc, Reason} ->
            {reply, {error, {remote_error, Reason}}, State};
        Result ->
            {reply, Result, State}
    end;
handle_call({call, _, _, _}, _From, #{connected := false} = State) ->
    {reply, {error, node_disconnected}, State}.

handle_info({nodedown, Node}, #{node := Node} = State) ->
    {noreply, State#{connected => false}};
handle_info({nodeup, Node}, #{node := Node} = State) ->
    {noreply, State#{connected => true}};
handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.
```

### Retry with Fallback

Implement retry logic with fallback nodes:

```erlang
call_with_retry(Nodes, Module, Func, Args) ->
    call_with_retry(Nodes, Module, Func, Args, 3).

call_with_retry([], _M, _F, _A, _Retries) ->
    {error, all_nodes_failed};
call_with_retry([Node | Rest], M, F, A, Retries) ->
    case rpc:call(Node, py, call, [M, F, A], 10000) of
        {ok, Result} ->
            {ok, Result};
        {badrpc, nodedown} ->
            %% Try next node
            call_with_retry(Rest, M, F, A, Retries);
        {badrpc, _} when Retries > 0 ->
            %% Retry same node
            timer:sleep(100),
            call_with_retry([Node | Rest], M, F, A, Retries - 1);
        Error ->
            {error, Error}
    end.
```

## Monitoring and Observability

### Cluster Status

Monitor Python availability across the cluster:

```erlang
cluster_status() ->
    Nodes = [node() | nodes()],
    [{Node, node_status(Node)} || Node <- Nodes].

node_status(Node) when Node =:= node() ->
    local_status();
node_status(Node) ->
    case rpc:call(Node, ?MODULE, local_status, [], 5000) of
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
```

### Distributed Metrics

Aggregate metrics from all nodes:

```erlang
aggregate_metrics() ->
    Nodes = [node() | nodes()],
    Metrics = [get_node_metrics(N) || N <- Nodes],
    #{
        total_calls => lists:sum([maps:get(calls, M, 0) || M <- Metrics]),
        total_errors => lists:sum([maps:get(errors, M, 0) || M <- Metrics]),
        avg_latency => avg([maps:get(avg_latency, M, 0) || M <- Metrics]),
        nodes => length(Metrics)
    }.

get_node_metrics(Node) when Node =:= node() ->
    py_metrics:get();
get_node_metrics(Node) ->
    case rpc:call(Node, py_metrics, get, [], 5000) of
        {badrpc, _} -> #{};
        Metrics -> Metrics
    end.

avg([]) -> 0;
avg(L) -> lists:sum(L) / length(L).
```

## Configuration

### Cluster Setup

Configure nodes for distributed Python:

```erlang
%% sys.config for worker nodes
[
    {erlang_python, [
        %% More contexts for dedicated Python workers
        {num_contexts, 16},

        %% Larger IO pool for network operations
        {io_pool_size, 20},

        %% Higher concurrency limit
        {max_concurrent, 100}
    ]},

    {kernel, [
        %% Enable distribution
        {distributed, [
            {erlang_python, 5000, ['worker1@host', 'worker2@host']}
        ]}
    ]}
].
```

### Node Naming

Start nodes with appropriate names:

```bash
# Coordinator node
erl -name coordinator@192.168.1.10 -setcookie cluster_secret

# Worker nodes
erl -name worker1@192.168.1.11 -setcookie cluster_secret
erl -name worker2@192.168.1.12 -setcookie cluster_secret
```

## Docker-Based Testing

A Docker Compose setup is provided to easily test distributed Python execution without setting up multiple machines.

### Quick Start

```bash
# Run the demo automatically
./docker/run-distributed-demo.sh

# Or start an interactive shell
./docker/run-distributed-demo.sh shell
```

### Manual Docker Setup

Start the cluster manually:

```bash
# Build and start worker nodes
docker compose -f docker/docker-compose.distributed.yml up -d worker1 worker2

# Wait for workers to initialize
sleep 5

# Start coordinator with interactive shell
docker compose -f docker/docker-compose.distributed.yml run --rm coordinator
```

In the coordinator shell:

```erlang
%% Connect to workers
net_adm:ping('worker1@worker1').
net_adm:ping('worker2@worker2').

%% Verify connections
nodes().
%% => ['worker1@worker1', 'worker2@worker2']

%% Run the demo
distributed_python:demo().

%% Or run individual operations
rpc:call('worker1@worker1', py, call, [math, sqrt, [16]]).
%% => {ok, 4.0}
```

### Cluster Architecture

The Docker setup creates:

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ coordinator  │  │   worker1    │  │   worker2    │       │
│  │              │  │              │  │              │       │
│  │  Erlang +    │  │  Erlang +    │  │  Erlang +    │       │
│  │  Python      │  │  Python      │  │  Python      │       │
│  │              │  │  (numpy)     │  │  (numpy)     │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│         │                  │                  │              │
│         └──────────────────┼──────────────────┘              │
│                   Erlang Distribution                        │
│                   (cookie: distributed_demo)                 │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Cleanup

```bash
# Stop and remove containers
./docker/run-distributed-demo.sh clean

# Or manually
docker compose -f docker/docker-compose.distributed.yml down
```

## Example: Distributed ML Inference

See `examples/distributed_python.erl` for a complete example demonstrating:

- Setting up Python environments on remote nodes
- Distributing ML inference across a cluster
- Handling failures and retries
- Aggregating results

```bash
# Start worker nodes first, then run:
rebar3 shell
1> distributed_python:demo().
```

## Best Practices

1. **Environment consistency** - Use `py:ensure_venv` with pinned versions on all nodes

2. **Minimize data transfer** - Process data where it lives when possible

3. **Use async calls** - `rpc:async_call` for parallel remote execution

4. **Handle failures** - Nodes can disconnect; implement retry logic

5. **Monitor load** - Check `py_semaphore:current()` before routing

6. **Batch operations** - Send batches rather than individual items

7. **Use channels for streaming** - Avoid loading large datasets into memory

## See Also

- [Scalability](scalability.md) - Execution modes and parallel execution
- [Pools](pools.md) - Dual pool configuration
- [Asyncio](asyncio.md) - Async Task API
- [Channel](channel.md) - Streaming data between Erlang and Python
