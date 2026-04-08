# Scalability and Parallelism

This guide covers the scalability features of erlang_python, including execution modes, rate limiting, and parallel execution.

## Execution Modes

erlang_python supports two execution modes:

```erlang
%% Check current execution mode
py:execution_mode().
%% => worker | owngil
```

### Mode Comparison

| Mode | Description | Parallelism | GIL Behavior | Best For |
|------|-------------|-------------|--------------|----------|
| **worker** | Dedicated pthread per context | GIL contention | Shared GIL | Default, maximum compatibility |
| **owngil** | Dedicated pthread + subinterpreter | True N-way | Per-interpreter GIL | CPU-bound parallel (Python 3.14+) |

### Worker Mode (Default)

Each context gets a dedicated pthread that handles all Python operations. This provides stable thread affinity, which is critical for libraries like numpy, torch, and tensorflow that maintain thread-local state.

### OWN_GIL Mode (Python 3.12+)

Creates dedicated pthreads with independent GILs for true parallel Python execution. Each OWN_GIL context runs in its own thread, enabling CPU parallelism.

**Architecture:**
- Each context gets a dedicated pthread with its own subinterpreter and GIL
- Requests dispatched via mutex/condvar IPC (not dirty schedulers)
- True parallel execution across multiple OWN_GIL contexts
- Higher per-call latency (~10μs vs ~2.5μs) but better parallelism

**Usage:**
```erlang
%% Create OWN_GIL contexts for parallel execution
{ok, Ctx1} = py_context:start_link(1, owngil),
{ok, Ctx2} = py_context:start_link(2, owngil),

%% These execute in parallel with independent GILs
spawn(fun() -> py_context:call(Ctx1, heavy_compute, run, [Data1]) end),
spawn(fun() -> py_context:call(Ctx2, heavy_compute, run, [Data2]) end).
```

**Process-Local Environments:**
```erlang
%% Multiple processes can share an OWN_GIL context with isolated namespaces
{ok, Env} = py_context:create_local_env(Ctx),
CtxRef = py_context:get_nif_ref(Ctx),
ok = py_nif:context_exec(CtxRef, <<"x = 42">>, Env),
{ok, 42} = py_nif:context_eval(CtxRef, <<"x">>, #{}, Env).
```

**When to use OWN_GIL:**
- CPU-bound Python workloads that benefit from parallelism
- Long-running computations
- When you need true concurrent Python execution
- Scientific computing, ML inference, data processing

**See also:** [OWN_GIL Internals](owngil_internals.md) for architecture details.

**Explicit Context Selection:**
```erlang
%% Get a specific context by index (1-based)
Ctx = py:context(1),
{ok, Result} = py:call(Ctx, math, sqrt, [16]).

%% Or use automatic scheduler-affinity routing
{ok, Result} = py:call(math, sqrt, [16]).
```

## Choosing the Right Mode

### When to Use Each Mode

**Use Worker Mode (default) when:**
- You need maximum module compatibility
- Running libraries like numpy, torch, tensorflow
- High call frequency with low latency
- Shared state between contexts is needed

**Use OWN_GIL Mode when:**
- You need true CPU parallelism across Python contexts
- Running long computations (ML inference, data processing)
- Workload benefits from multiple independent Python interpreters
- You can tolerate higher per-call latency for better throughput

### Pros and Cons

**Worker Mode Pros:**
- Maximum module compatibility (all C extensions work)
- Stable thread affinity for numpy/torch/tensorflow
- Low memory overhead (single interpreter)
- Shared state between contexts

**Worker Mode Cons:**
- GIL contention limits parallelism
- No isolation between contexts

**OWN_GIL Mode Pros:**
- True parallelism without GIL contention
- Complete isolation (crashes don't affect other contexts)
- Each context has clean namespace (no state bleed)

**OWN_GIL Mode Cons:**
- Higher memory usage (each interpreter loads modules separately)
- Some C extensions don't support subinterpreters
- Requires Python 3.14+

## Subinterpreter Architecture

### Design Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Erlang VM (BEAM)                            │
├─────────────────────────────────────────────────────────────────┤
│  py_context_router                                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Scheduler 1 ──► Context 1 (pid)                        │   │
│  │  Scheduler 2 ──► Context 2 (pid)                        │   │
│  │  Scheduler N ──► Context N (pid)                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│         │              │              │                         │
│         ▼              ▼              ▼                         │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                   │
│  │ Context  │   │ Context  │   │ Context  │                   │
│  │ Process  │   │ Process  │   │ Process  │                   │
│  │ (gen_srv)│   │ (gen_srv)│   │ (gen_srv)│                   │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘                   │
└───────┼──────────────┼──────────────┼───────────────────────────┘
        │              │              │
        ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Subinterpreter Thread Pool                     │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Thread 1   │  │   Thread 2   │  │   Thread N   │         │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │         │
│  │ │  Interp  │ │  │ │  Interp  │ │  │ │  Interp  │ │         │
│  │ │  (GIL 1) │ │  │ │  (GIL 2) │ │  │ │  (GIL N) │ │         │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                 │
│  Each thread owns its interpreter's GIL (Py_GIL_OWN)           │
│  No GIL contention between threads                              │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

**py_context_router**: Routes requests to context processes based on scheduler affinity or explicit binding.

**py_context_process**: Gen_server that owns a Python context reference and handles call/eval/exec operations.

**Subinterpreter Thread Pool (C)**: Manages N threads, each with its own Python subinterpreter created with `Py_NewInterpreterFromConfig()` and `Py_GIL_OWN`.

### Request Flow

1. Erlang process calls `py:call(Module, Func, Args)`
2. `py_context_router` selects context based on scheduler ID
3. Request sent to `py_context_process` gen_server
4. Gen_server calls NIF which executes on subinterpreter's thread
5. Result returned through gen_server to caller

### Pool Size

The subinterpreter pool size is configured at two levels:

| Level | Default | Max |
|-------|---------|-----|
| **Erlang (py_context_router)** | `erlang:system_info(schedulers)` | configurable |
| **C pool (py_subinterp_pool)** | 32 | 64 |

On a typical 8-core machine, 8 context processes are started, each with one subinterpreter slot.

**Configuration via sys.config:**
```erlang
{erlang_python, [
    {num_contexts, 16}  %% Override scheduler count
]}
```

**Configuration at runtime:**
```erlang
%% Start with custom pool size
py_context_router:start(#{contexts => 16}).
```

### Thread Safety

- Each subinterpreter has its own GIL (no cross-interpreter contention)
- NIF calls are serialized per-context via gen_server
- Erlang message passing provides synchronization
- C code uses atomics for cross-thread state (`thread_running` flag)

## Rate Limiting

All Python calls pass through an ETS-based counting semaphore that prevents overload:

```erlang
%% Check semaphore status
py_semaphore:max_concurrent().  %% => 29 (schedulers * 2 + 1)
py_semaphore:current().         %% => 0 (currently running)

%% Dynamically adjust limit
py_semaphore:set_max_concurrent(50).
```

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                      py_semaphore                           │
│                                                             │
│  ┌─────────┐    ┌─────────────────────────────────────┐    │
│  │ Counter │◄───│  ets:update_counter (atomic)        │    │
│  │  [29]   │    │  {write_concurrency, true}          │    │
│  └─────────┘    └─────────────────────────────────────┘    │
│                                                             │
│  acquire(Timeout) ──► increment ──► check ≤ max?           │
│                       │                 │                   │
│                       │             yes │ no                │
│                       │                 │  │                │
│                       │              ok │  └──► backoff     │
│                       │                 │       loop        │
│  release() ──────────►└──── decrement ──┘                   │
└─────────────────────────────────────────────────────────────┘
```

### Overload Protection

When the semaphore is exhausted, `py:call` returns an overload error instead of blocking forever:

```erlang
{error, {overloaded, Current, Max}} = py:call(module, func, []).
```

This allows your application to implement backpressure or shed load gracefully.

## Configuration

```erlang
%% sys.config
[
    {erlang_python, [
        %% Maximum concurrent Python operations (semaphore limit)
        %% Default: erlang:system_info(schedulers) * 2 + 1
        {max_concurrent, 50},

        %% Context mode: worker | owngil
        %% Default: worker
        {context_mode, worker},

        %% Number of contexts
        %% Default: erlang:system_info(schedulers)
        {num_contexts, 8}
    ]}
].
```

## Parallel Execution with Sub-interpreters

For CPU-bound workloads on Python 3.12+, erlang_python provides true parallelism via OWN_GIL subinterpreters.

### Check Support

```erlang
%% Check if subinterpreters are supported (Python 3.12+)
true = py:subinterp_supported().

%% Check current execution mode
subinterp = py:execution_mode().
```

### Using the Context Router

The context router automatically distributes calls across subinterpreters:

```erlang
%% Start contexts (usually done by application startup)
{ok, _} = py:start_contexts().

%% Calls are automatically routed to subinterpreters
{ok, 4.0} = py:call(math, sqrt, [16]).
{ok, 6} = py:eval(<<"2 + 4">>).
ok = py:exec(<<"x = 42">>).
```

### Explicit Context Selection

For fine-grained control, use explicit context selection:

```erlang
%% Get a specific context by index (1-based)
Ctx = py:context(1),

%% All operations on this context share state
ok = py:exec(Ctx, <<"my_var = 'hello'">>),
{ok, <<"hello">>} = py:eval(Ctx, <<"my_var">>),
{ok, 4.0} = py:call(Ctx, math, sqrt, [16]).

%% Different context has isolated state
Ctx2 = py:context(2),
{error, _} = py:eval(Ctx2, <<"my_var">>).  %% Not defined in Ctx2
```

### Context Router API

```erlang
%% Start router with default number of contexts (scheduler count)
{ok, Contexts} = py_context_router:start().

%% Start with custom number of contexts
{ok, Contexts} = py_context_router:start(#{contexts => 8}).

%% Get context for current scheduler (automatic affinity)
Ctx = py_context_router:get_context().

%% Get specific context by index
Ctx = py_context_router:get_context(1).

%% Bind current process to a specific context
ok = py_context_router:bind_context(Ctx).

%% Unbind (return to scheduler-based routing)
ok = py_context_router:unbind_context().

%% Get number of active contexts
N = py_context_router:num_contexts().

%% Stop all contexts
ok = py_context_router:stop().
```

### Parallel Execution

Execute multiple calls in parallel across subinterpreters:

```erlang
%% Execute multiple calls in parallel
{ok, Results} = py:parallel([
    {math, sqrt, [16]},
    {math, sqrt, [25]},
    {math, sqrt, [36]}
]).
%% => {ok, [{ok, 4.0}, {ok, 5.0}, {ok, 6.0}]}
```

Each call runs in its own sub-interpreter with its own GIL, enabling true parallelism.

## Testing with Free-Threading

To test with a free-threaded Python build:

### 1. Install Python 3.13+ with Free-Threading

```bash
# macOS with Homebrew
brew install python@3.13 --with-freethreading

# Or build from source
./configure --disable-gil
make && make install

# Or use pyenv
PYTHON_CONFIGURE_OPTS="--disable-gil" pyenv install 3.13.0
```

### 2. Verify Free-Threading is Enabled

```bash
python3 -c "import sys; print('GIL disabled:', hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled())"
```

### 3. Rebuild erlang_python

```bash
# Clean and rebuild with free-threaded Python
rebar3 clean
PYTHON_CONFIG=/path/to/python3.13-config rebar3 compile
```

### 4. Verify Mode

```erlang
1> application:ensure_all_started(erlang_python).
2> py:execution_mode().
free_threaded
```

## Performance Tuning

### For CPU-Bound Workloads

- Use `py:parallel/1` with sub-interpreters (Python 3.12+)
- Or use free-threaded Python (3.13+)
- Increase `max_concurrent` to match available CPU cores

### For I/O-Bound Workloads

- Worker mode works well (GIL released during I/O)
- Use asyncio integration for async I/O
- Increase `num_contexts` for more concurrent I/O capacity

### For Mixed Workloads

- Balance `max_concurrent` based on memory constraints
- Monitor `py_semaphore:current()` for load metrics
- Implement application-level backpressure based on overload errors

## Monitoring

```erlang
%% Current load
Load = py_semaphore:current(),
Max = py_semaphore:max_concurrent(),
Utilization = Load / Max * 100,
io:format("Python load: ~.1f%~n", [Utilization]).

%% Execution mode info
Mode = py:execution_mode(),
io:format("Mode: ~p~n", [Mode]).

%% Memory stats
{ok, Stats} = py:memory_stats(),
io:format("GC stats: ~p~n", [maps:get(gc_stats, Stats)]).
```

## Shared State

Since workers (and sub-interpreters) have isolated namespaces, erlang_python provides
ETS-backed shared state accessible from both Python and Erlang:

```python
from erlang import state_set, state_get, state_incr, state_decr

# Share configuration across workers
config = state_get('app_config')

# Thread-safe metrics
state_incr('requests_total')
state_incr('bytes_processed', len(data))
```

```erlang
%% Set config that all workers can read
py:state_store(<<"app_config">>, #{model => <<"gpt-4">>, timeout => 30000}).

%% Read metrics
{ok, Total} = py:state_fetch(<<"requests_total">>).
```

The state is backed by ETS with `{write_concurrency, true}`, making atomic
counter operations fast and lock-free. See [Getting Started](getting-started.md#shared-state)
for the full API.

## Reentrant Callbacks

erlang_python supports reentrant callbacks where Python code calls Erlang functions
that themselves call back into Python. This is handled without deadlocking through
a suspension/resume mechanism:

```erlang
%% Register an Erlang function that calls Python
py:register_function(compute_via_python, fun([X]) ->
    {ok, Result} = py:call('__main__', complex_compute, [X]),
    Result * 2  %% Erlang post-processing
end).

%% Python code that uses the callback
py:exec(<<"
def process(x):
    from erlang import call
    # Calls Erlang, which calls Python's complex_compute
    result = call('compute_via_python', x)
    return result + 1
">>).
```

### How Reentrant Callbacks Work

```
┌─────────────────────────────────────────────────────────────────┐
│                     Reentrant Callback Flow                      │
│                                                                 │
│  1. Python calls erlang.call('func', args)                      │
│     └──► Returns suspension marker, frees dirty scheduler       │
│                                                                 │
│  2. Erlang executes the registered callback                     │
│     └──► May call py:call() to run Python (on different worker) │
│                                                                 │
│  3. Erlang calls resume_callback with result                    │
│     └──► Schedules dirty NIF to return result to Python         │
│                                                                 │
│  4. Python continues with the callback result                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Benefits

- **No Deadlocks**: Dirty schedulers are freed during callback execution
- **Nested Callbacks**: Multiple levels of Python→Erlang→Python→... are supported
- **Transparent**: From Python's perspective, `erlang.call()` appears synchronous
- **No Configuration**: Works automatically with all execution modes

### Performance Considerations

- Reentrant callbacks have slightly higher overhead due to suspension/resume
- For tight loops, consider batching operations to reduce callback overhead
- Concurrent reentrant calls are fully supported and scale well

### Example: Nested Callbacks

```erlang
%% Each level alternates between Erlang and Python
py:register_function(level, fun([N, Max]) ->
    case N >= Max of
        true -> N;
        false ->
            {ok, Result} = py:call('__main__', next_level, [N + 1, Max]),
            Result
    end
end).

py:exec(<<"
def next_level(n, max):
    from erlang import call
    return call('level', n, max)

def start(max):
    from erlang import call
    return call('level', 1, max)
">>).

%% Test 10 levels of nesting
{ok, 10} = py:call('__main__', start, [10]).
```

### Example

See `examples/reentrant_demo.erl` and `examples/reentrant_demo.py` for a complete
demonstration including:

- Basic reentrant calls with arithmetic expressions
- Fibonacci with Erlang memoization
- Deeply nested callbacks (10+ levels)
- OOP-style class method callbacks

```bash
# Run the demo
rebar3 shell
1> reentrant_demo:start().
2> reentrant_demo:demo_all().
```

## Building for Performance

### Standard Build

```bash
rebar3 compile
```

Uses `-O2` optimization and standard compiler flags.

### Performance Build

For production deployments where maximum performance is needed:

```bash
# Clean and rebuild with aggressive optimizations
rm -rf _build/cmake
mkdir -p _build/cmake && cd _build/cmake
cmake ../../c_src -DPERF_BUILD=ON
cmake --build . -j$(nproc)
```

The `PERF_BUILD` option enables:

| Flag | Effect |
|------|--------|
| `-O3` | Aggressive optimization level |
| `-flto` | Link-Time Optimization |
| `-march=native` | CPU-specific instruction set |
| `-ffast-math` | Relaxed floating-point math |
| `-funroll-loops` | Loop unrolling |

**Caveats:**
- Binaries are not portable (tied to build machine's CPU)
- Build time increases due to LTO
- `-ffast-math` may affect floating-point precision

### Verifying the Build

```erlang
%% Check that the NIF loaded successfully
1> application:ensure_all_started(erlang_python).
{ok, [erlang_python]}

%% Run basic verification
2> py:eval("1 + 1").
{ok, 2}
```

## See Also

- [Getting Started](getting-started.md) - Basic usage
- [Memory Management](memory.md) - GC and memory debugging
- [Streaming](streaming.md) - Working with generators
- [Asyncio](asyncio.md) - Event loop performance details
