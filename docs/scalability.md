# Scalability and Parallelism

This guide covers the scalability features of erlang_python, including execution modes, rate limiting, and parallel execution.

## Execution Modes

By default, erlang_python uses worker mode which works with any Python version:

```erlang
%% Check current execution mode
py:execution_mode().
%% => free_threaded | multi_executor

%% Check number of executor threads
py:num_executors().
%% => 4 (default)
```

### Mode Comparison

| Mode | Python Version | Parallelism | Best For |
|------|----------------|-------------|----------|
| **free_threaded** | 3.13t+ (nogil) | True N-way | Maximum throughput |
| **multi_executor** | Any | GIL contention | I/O-bound, compatibility |

### True Parallel Execution

For true parallel Python execution, you have two options:

**1. Free-Threaded Python (3.13t+)**

When running on a free-threaded Python build (compiled with `--disable-gil`), erlang_python executes Python calls directly without any executor routing. This provides maximum parallelism for CPU-bound workloads.

**2. Parallel Pool Build (Python 3.14+)**

Build with `ENABLE_PARALLEL_PYTHON=ON` for parallel execution via subinterpreters:

```bash
CMAKE_OPTIONS="-DENABLE_PARALLEL_PYTHON=ON" rebar3 compile
```

This creates a pool of subinterpreters with independent GILs for true parallelism.

### Multi-Executor Mode (Default)

Runs N executor threads that share the GIL. Requests are distributed round-robin across executors. Good for I/O-bound workloads where Python releases the GIL during I/O operations.

## Choosing the Right Mode

| Aspect | Free-Threaded | Multi-Executor |
|--------|---------------|----------------|
| **Parallelism** | True N-way | GIL contention |
| **State Isolation** | Shared | Shared |
| **Memory Overhead** | Low | Low |
| **Module Compatibility** | Limited | All modules |
| **Python Version** | 3.13t+ (nogil) | Any |

### When to Use Each Mode

**Use Free-Threaded (Python 3.13t+) when:**
- You need maximum parallelism with shared state
- Your libraries are GIL-free compatible
- You're running CPU-bound workloads

**Use Parallel Pool Build (Python 3.14+) when:**
- You need true CPU parallelism
- Running long computations (ML inference, data processing)
- Your libraries support subinterpreters

**Use Multi-Executor (default) when:**
- Running on standard Python versions
- Your workload is I/O-bound (GIL released during I/O)
- You need compatibility with all Python modules

## Context Architecture

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
│  └──────────┘   └──────────┘   └──────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

**py_context_router**: Routes requests to context processes based on scheduler affinity or explicit binding.

**py_context**: Process that owns a Python context reference and handles call/eval/exec operations.

### Request Flow

1. Erlang process calls `py:call(Module, Func, Args)`
2. `py_context_router` selects context based on scheduler ID
3. Request sent to `py_context` process
4. Context executes Python code and returns result

### Pool Size

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

        %% Number of executor threads (multi_executor mode only)
        %% Default: 4
        {num_executors, 8},

        %% Worker pool sizes
        {num_workers, 4},
        {num_async_workers, 2}
    ]}
].
```

## Parallel Execution

The `py:parallel/1` function distributes calls across available contexts:

```erlang
%% Execute multiple calls in parallel
{ok, Results} = py:parallel([
    {math, sqrt, [16]},
    {math, sqrt, [25]},
    {math, sqrt, [36]}
]).
%% => {ok, [{ok, 4.0}, {ok, 5.0}, {ok, 6.0}]}
```

For true parallelism, use free-threaded Python (3.13t+) or build with `ENABLE_PARALLEL_PYTHON=ON` (Python 3.14+).

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

- Multi-executor mode works well (GIL released during I/O)
- Increase `num_executors` to handle more concurrent I/O
- Use asyncio integration for async I/O

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
Executors = py:num_executors(),
io:format("Mode: ~p, Executors: ~p~n", [Mode, Executors]).

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
