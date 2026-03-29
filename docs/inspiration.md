# Inspiration Guide

**Erlang's reliability meets Python's AI ecosystem.**

erlang_python lets you build fault-tolerant, distributed Python workloads with the battle-tested BEAM runtime. This guide showcases what's possible.

## AI/ML at Scale

### Parallel Embedding Generation

Leverage Erlang's lightweight processes to parallelize embedding generation:

```erlang
%% Sequential: 10 items x 100ms = 1 second
Sequential = [embed(Item) || Item <- Items].

%% Parallel with Erlang processes: ~100ms total (10x speedup)
py:register_function(parallel_map, fun([FuncName, Items]) ->
    Parent = self(),
    Refs = [begin
        Ref = make_ref(),
        spawn(fun() ->
            Result = process(FuncName, Item),
            Parent ! {Ref, Result}
        end),
        Ref
    end || Item <- Items],
    [receive {Ref, R} -> R after 5000 -> timeout end || Ref <- Refs]
end).

%% Call from Python
{ok, Results} = py:eval(<<"erlang.call('parallel_map', 'embed', texts)">>).
```

Real benchmark results:
```
Sequential (10 items x 100ms): 1.01 seconds
Parallel (10 Erlang processes): 0.10 seconds
Speedup: 10x faster
```

### RAG System with Distributed Retrieval

Build production RAG systems with Erlang orchestration:

```erlang
-module(rag).

query(Question, Index) ->
    %% 1. Embed the question
    {ok, QueryEmb} = py:call(ai_helpers, embed_single, [Question]),

    %% 2. Retrieve top-k similar documents (parallel across nodes)
    TopK = parallel_search(QueryEmb, Index, 3),
    Context = iolist_to_binary([Doc || {Doc, _Score} <- TopK]),

    %% 3. Generate answer with LLM
    {ok, Answer} = py:call(ai_helpers, generate, [Question, Context]),
    Answer.
```

### LLM Integration

Works with OpenAI, Anthropic, and local models:

```erlang
%% OpenAI
py:exec(<<"
from openai import OpenAI
client = OpenAI()
">>),
{ok, Response} = py:eval(<<"
client.chat.completions.create(
    model='gpt-4',
    messages=[{'role': 'user', 'content': prompt}]
).choices[0].message.content
">>, #{prompt => <<"Explain OTP">>}).

%% Local with Ollama
{ok, Response} = py:eval(<<"
import requests
requests.post('http://localhost:11434/api/generate',
    json={'model': 'llama3.2', 'prompt': prompt, 'stream': False}
).json()['response']
">>, #{prompt => <<"What is the BEAM?">>}).
```

## Real-time Systems

### Event-Driven Async

Sub-millisecond latency with zero-polling I/O:

```erlang
%% Create async task
Ref = py_event_loop:create_task(aiohttp, get, [Url]),

%% Do other work while waiting...
process_other_requests(),

%% Await result when needed
{ok, Response} = py_event_loop:await(Ref, 5000).
```

The `ErlangEventLoop` integrates with Erlang's scheduler - no polling threads, no busy waiting.

### High-Throughput Channels

6.2M ops/sec message passing between Erlang and Python:

```erlang
%% Create channel with backpressure
{ok, Ch} = py_channel:new(#{max_size => 100000}),

%% Stream data to Python
[py_channel:send(Ch, Item) || Item <- Items],
py_channel:close(Ch).
```

```python
from erlang.channel import Channel

async def process_stream(channel_ref):
    ch = Channel(channel_ref)
    async for item in ch:
        result = await process(item)
        yield result
```

| Message Size | Throughput |
|-------------|------------|
| 64 bytes    | 6.2M ops/s |
| 1KB         | 3.8M ops/s |
| 16KB        | 1.1M ops/s |

### Protocol Servers with Reactor Pattern

Build TCP/UDP servers with Python protocol logic:

```erlang
%% Erlang handles TCP accept and I/O scheduling
{ok, Ctx} = py_reactor_context:start_link(1, auto),

py:exec(Ctx, <<"
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

reactor.set_protocol_factory(EchoProtocol)
">>),

%% Accept connections
{ok, Sock} = gen_tcp:accept(LSock),
{ok, Fd} = prim_inet:getfd(Sock),
Ctx ! {fd_handoff, Fd, #{addr => Addr}}.
```

## Distributed Computing

### Location-Transparent Python Calls

Same API for local and remote execution via `rpc:call`:

```erlang
%% Execute Python on a remote node
{ok, Result} = rpc:call('worker@host', py, call, [math, sqrt, [16]]).
%% => {ok, 4.0}

%% Parallel across cluster
Keys = [
    rpc:async_call('node1@host', py, call, [model, predict, [Data1]]),
    rpc:async_call('node2@host', py, call, [model, predict, [Data2]]),
    rpc:async_call('node3@host', py, call, [model, predict, [Data3]])
],
Results = [rpc:yield(Key) || Key <- Keys].
```

### GPU Nodes for ML Inference

Route inference to GPU-equipped nodes:

```erlang
select_gpu_node() ->
    Nodes = [N || N <- nodes(), is_gpu_node(N)],
    Loads = [{N, get_load(N)} || N <- Nodes],
    element(1, lists:min(Loads)).

is_gpu_node(Node) ->
    case rpc:call(Node, py, eval, [<<"torch.cuda.is_available()">>]) of
        {ok, true} -> true;
        _ -> false
    end.

%% Route inference
Node = select_gpu_node(),
{ok, Prediction} = rpc:call(Node, py, call, [model, predict, [Input]]).
```

### Fault-Tolerant Worker Supervision

Wrap Python workers in OTP supervision:

```erlang
-module(py_worker).
-behaviour(gen_server).

init([Node]) ->
    net_kernel:monitor_nodes(true),
    {ok, #{node => Node, connected => net_adm:ping(Node) =:= pong}}.

handle_call({call, M, F, A}, _From, #{node := Node, connected := true} = State) ->
    case rpc:call(Node, py, call, [M, F, A], 30000) of
        {badrpc, Reason} -> {reply, {error, Reason}, State};
        Result -> {reply, Result, State}
    end.

handle_info({nodedown, Node}, #{node := Node} = State) ->
    {noreply, State#{connected => false}};
handle_info({nodeup, Node}, #{node := Node} = State) ->
    {noreply, State#{connected => true}}.
```

## Web Applications

### Streaming Responses from Python Generators

```erlang
%% Stream chunks from Python generator
stream_response(Prompt) ->
    {ok, Chunks} = py:stream('__main__', stream_chat, [Prompt]),
    lists:foreach(fun(Chunk) ->
        send_to_client(Chunk)
    end, Chunks).
```

### Bidirectional Communication via Channels

WebSocket-like patterns without the overhead:

```erlang
%% Erlang: Send request, receive response
{ok, Ch} = py_channel:new(),
py_channel:send(Ch, {request, self(), Data}),
receive
    {response, Result} -> Result
end.
```

```python
from erlang.channel import Channel, reply

def handle_requests(channel_ref):
    ch = Channel(channel_ref)
    for msg in ch:
        _, sender_pid, data = msg
        result = process(data)
        reply(sender_pid, ('response', result))
```

## Quick Wins

### 3-Line Semantic Search

```erlang
{ok, Emb} = py:call(ai_helpers, embed_single, [<<"concurrent programming">>]),
Scored = [{Doc, cosine_sim(Emb, DocEmb)} || {Doc, DocEmb} <- Index],
lists:sublist(lists:reverse(lists:keysort(2, Scored)), 5).
```

### Parallel Image Processing

```erlang
%% Process 100 images in parallel
Images = [read_image(Path) || Path <- Paths],
{ok, Results} = py:parallel([{cv2, process, [Img]} || Img <- Images]).
```

### Distributed Task Queue

```erlang
%% Submit to least-loaded worker
Workers = [worker1@host, worker2@host, worker3@host],
Loads = [{W, rpc:call(W, py_semaphore, current, [])} || W <- Workers],
{Worker, _} = lists:min(Loads),
rpc:call(Worker, py, call, [processor, run, [Task]]).
```

## Architecture Patterns

### Python as a Capability Layer

Erlang orchestrates, Python computes:

```
+------------------+     +------------------+
|     Erlang       |     |     Python       |
|   (Control)      |     |   (Compute)      |
|                  |     |                  |
| - Supervision    |<--->| - ML inference   |
| - Distribution   |     | - Data science   |
| - Fault handling |     | - AI/LLM calls   |
| - Message routing|     | - Image/video    |
+------------------+     +------------------+
```

### Process-per-Context Isolation

Each Python context runs in isolation:

```erlang
%% Multiple independent contexts
{ok, Ctx1} = py_context:start_link(1, auto),
{ok, Ctx2} = py_context:start_link(2, auto),

%% Failures in Ctx1 don't affect Ctx2
py:exec(Ctx1, <<"import dangerous_lib">>),
py:exec(Ctx2, <<"import stable_lib">>).
```

### Pool Routing for CPU vs I/O

Separate pools prevent I/O from blocking compute:

```erlang
%% CPU-bound pool (for ML inference)
{ok, _} = py_pool:start_link(cpu_pool, #{size => 4}),

%% I/O-bound pool (for API calls)
{ok, _} = py_pool:start_link(io_pool, #{size => 16}),

%% Route accordingly
py:call(cpu_pool, model, predict, [Data]),
py:call(io_pool, api, fetch, [Url]).
```

## Performance Highlights

| Metric | Value |
|--------|-------|
| Channel throughput | 6.2M ops/sec |
| Event loop latency | <1ms |
| Parallel speedup | 10x with BEAM processes |
| Async task throughput | 417K tasks/sec |

## Why erlang_python?

| Feature | erlang_python | Standalone Python | Celery | Ray |
|---------|--------------|-------------------|--------|-----|
| Fault tolerance | OTP supervision | Manual | Limited | Limited |
| Distribution | Built-in (rpc:call) | External | Redis/RabbitMQ | Cluster setup |
| Zero-polling I/O | Yes (enif_select) | No | No | No |
| Process isolation | Per-context | GIL contention | Per-worker | Per-actor |
| Hot code reload | Erlang native | Restart required | Restart | Restart |
| Latency | Sub-millisecond | Variable | Queue delay | Scheduler delay |
| BEAM integration | Native | Port/NIF | External | External |

**Unique advantages:**

- **Supervision trees** - Python failures trigger Erlang restart strategies
- **Location transparency** - Same API for local and distributed calls
- **Zero-copy channels** - enif_ioq for efficient message passing
- **Erlang event loop** - Python asyncio without polling threads
- **Free-threading ready** - Supports Python 3.13+ free-threaded mode

## Next Steps

- [Getting Started](getting-started.md) - Installation and first steps
- [AI Integration](ai-integration.md) - Detailed AI/ML patterns
- [Distributed](distributed.md) - Cluster setup and patterns
- [Channel](channel.md) - High-throughput messaging
- [Scalability](scalability.md) - Execution modes and tuning
