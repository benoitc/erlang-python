# Scalability and Parallelism

This guide covers the scalability features of erlang_python, including execution modes, rate limiting, and parallel execution.

## Execution Modes

erlang_python automatically detects the optimal execution mode based on your Python version:

```erlang
%% Check current execution mode
py:execution_mode().
%% => free_threaded | subinterp | multi_executor

%% Check number of executor threads
py:num_executors().
%% => 4 (default)
```

### Mode Comparison

| Mode | Python Version | Parallelism | GIL Behavior | Best For |
|------|----------------|-------------|--------------|----------|
| **free_threaded** | 3.13+ (nogil build) | True N-way | None | Maximum throughput |
| **subinterp** | 3.12+ | True N-way | Per-interpreter | CPU-bound, isolation |
| **multi_executor** | Any | GIL contention | Shared, round-robin | I/O-bound, compatibility |

### Free-Threaded Mode (Python 3.13+)

When running on a free-threaded Python build (compiled with `--disable-gil`), erlang_python executes Python calls directly without any executor routing. This provides maximum parallelism for CPU-bound workloads.

### Sub-interpreter Mode (Python 3.12+)

Uses Python's sub-interpreter feature with per-interpreter GIL. Each sub-interpreter has its own GIL, allowing true parallel execution across interpreters.

**Note:** Each sub-interpreter has isolated state. Use the [Shared State](#shared-state) API to share data between workers.

### Multi-Executor Mode (Python < 3.12)

Runs N executor threads that share the GIL. Requests are distributed round-robin across executors. Good for I/O-bound workloads where Python releases the GIL during I/O operations.

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
        {num_async_workers, 2},
        {num_subinterp_workers, 4}
    ]}
].
```

## Parallel Execution with Sub-interpreters

For CPU-bound workloads on Python 3.12+, use explicit parallel execution:

```erlang
%% Check if supported
true = py:subinterp_supported().

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

## See Also

- [Getting Started](getting-started.md) - Basic usage
- [Memory Management](memory.md) - GC and memory debugging
- [Streaming](streaming.md) - Working with generators
