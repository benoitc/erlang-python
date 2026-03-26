# Parallel Execution

This guide explains how erlang_python achieves parallel Python execution and how to configure it for your workload.

## Overview

erlang_python supports three execution modes:

| Mode | Python Version | Parallelism | Build Flag |
|------|----------------|-------------|------------|
| **Worker** (default) | Any | GIL-limited | None |
| **Free-threaded** | 3.13t+ | True N-way | Automatic |
| **Parallel Pool** | 3.14+ | True N-way | `ENABLE_PARALLEL_PYTHON=ON` |

All modes use the same API - the execution mode is determined at build time and runtime detection.

## Quick Start

### Default Build (Worker Mode)

```bash
rebar3 compile
```

Works with any Python version. Multiple contexts share the main GIL, so Python execution is serialized. Good for I/O-bound workloads where Python releases the GIL.

### Parallel Build

```bash
CMAKE_OPTIONS="-DENABLE_PARALLEL_PYTHON=ON" rebar3 compile
```

Requires Python 3.14+. Creates a pool of independent interpreters, each with its own GIL. Enables true parallel Python execution.

### Free-Threaded Python

```bash
# Build Python 3.13+ with --disable-gil
# Then compile erlang_python normally
rebar3 compile
```

Automatically detected. No GIL means true parallelism without special build flags.

## Architecture

### Worker Mode

```
┌─────────────────────────────────────────────────────────────┐
│                     Erlang VM (BEAM)                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Context 1 ─┐                                                │
│  Context 2 ─┼──► Main Python Interpreter [Single GIL]       │
│  Context N ─┘                                                │
│                                                              │
│  All contexts share the GIL - execution is serialized        │
└─────────────────────────────────────────────────────────────┘
```

Each context has its own namespace (`globals`/`locals` dicts) but shares the main interpreter and GIL. When one context executes Python, others must wait.

### Parallel Pool Mode

```
┌─────────────────────────────────────────────────────────────┐
│                     Erlang VM (BEAM)                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Context 1 ──► Slot 0 [GIL 0] ◄── Independent interpreter   │
│  Context 2 ──► Slot 1 [GIL 1] ◄── Independent interpreter   │
│  Context 3 ──► Slot 2 [GIL 2] ◄── Independent interpreter   │
│  Context 4 ──► Slot 0 [GIL 0] ◄── Shares slot with Context 1│
│                                                              │
│  Different slots execute in parallel - true N-way            │
└─────────────────────────────────────────────────────────────┘
```

The parallel pool creates N slots (default: number of Erlang schedulers, max 64). Each slot is an independent Python subinterpreter with its own GIL. Contexts are assigned to slots round-robin.

**Key properties:**
- Slots execute truly in parallel (no GIL contention between slots)
- Multiple contexts can share a slot (serialized by that slot's GIL)
- Each slot has isolated `sys.modules` and namespace
- Slot count matches Erlang scheduler count for optimal affinity

### Free-Threaded Mode

```
┌─────────────────────────────────────────────────────────────┐
│                     Erlang VM (BEAM)                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Context 1 ──┐                                               │
│  Context 2 ──┼──► Main Interpreter [No GIL]                 │
│  Context N ──┘                                               │
│                                                              │
│  No GIL - all contexts execute in parallel                   │
└─────────────────────────────────────────────────────────────┘
```

With free-threaded Python (3.13t+), the GIL is disabled entirely. All contexts can execute simultaneously without any serialization.

## Context Routing

### Scheduler Affinity

By default, contexts are selected based on the current Erlang scheduler:

```erlang
%% Process on Scheduler 1 always uses Context 1
%% Process on Scheduler 2 always uses Context 2
%% etc.

{ok, Result} = py:call(math, sqrt, [16]).
```

This provides good cache locality - the same Erlang process consistently uses the same Python context.

### Explicit Context Binding

Override automatic routing for a process:

```erlang
Ctx = py_context_router:get_context(),
ok = py_context_router:bind_context(Ctx),

%% All calls from this process now use Ctx
{ok, R1} = py:call(math, sqrt, [16]),
{ok, R2} = py:call(math, pow, [2, 10]),

ok = py_context_router:unbind_context().
```

### Pool-Based Routing

Route specific modules or functions to dedicated pools:

```erlang
%% Create a pool for I/O-bound operations
{ok, _} = py_context_router:start_pool(io, 10, worker),

%% Route all 'requests' module calls to io pool
ok = py_context_router:register_pool(io, requests),

%% Route specific function to gpu pool
ok = py_context_router:register_pool(gpu, torch, cuda_malloc),

%% Calls automatically route to registered pools
{ok, Resp} = py:call(requests, get, [Url]).  %% Goes to io pool
```

## Configuration

### Build-Time Options

```bash
# Enable parallel pool (requires Python 3.14+)
CMAKE_OPTIONS="-DENABLE_PARALLEL_PYTHON=ON" rebar3 compile

# Performance build with optimizations
CMAKE_OPTIONS="-DPERF_BUILD=ON" rebar3 compile

# Both
CMAKE_OPTIONS="-DENABLE_PARALLEL_PYTHON=ON -DPERF_BUILD=ON" rebar3 compile
```

### Runtime Configuration

```erlang
%% sys.config
[
    {erlang_python, [
        %% Number of contexts in default pool
        {num_contexts, 8},

        %% Maximum concurrent Python operations
        {max_concurrent, 50},

        %% Number of executor threads (worker mode)
        {num_executors, 4}
    ]}
].
```

### Runtime Detection

```erlang
%% Check execution mode
py:execution_mode().
%% => multi_executor | free_threaded

%% Check parallel support
py:subinterp_supported().
%% => true | false

%% Check current load
py_semaphore:current().      %% Currently executing
py_semaphore:max_concurrent(). %% Maximum allowed
```

## Performance Characteristics

### Throughput Comparison

| Mode | Sync Call Throughput | Parallelism |
|------|---------------------|-------------|
| Worker | ~50-100K ops/sec | Single-threaded |
| Parallel Pool | ~400K+ ops/sec | N-way parallel |
| Free-threaded | ~500K+ ops/sec | N-way parallel |

### When to Use Each Mode

**Worker Mode (default):**
- I/O-bound workloads (GIL released during I/O)
- Quick operations (< 1ms)
- Maximum compatibility with C extensions
- Simple deployment (any Python version)

**Parallel Pool:**
- CPU-bound workloads (ML inference, data processing)
- Long-running operations
- Need isolation between contexts
- Python 3.14+ available

**Free-threaded:**
- Maximum parallelism with shared state
- Libraries are GIL-free compatible
- Python 3.13t+ available

## Isolation and State

### Worker Mode

- **Namespace:** Each context has separate `globals`/`locals` dicts
- **Modules:** Shared `sys.modules` across all contexts
- **Objects:** Can share Python objects between contexts (same interpreter)

### Parallel Pool Mode

- **Namespace:** Each slot has completely isolated namespace
- **Modules:** Each slot has its own `sys.modules`
- **Objects:** Cannot share Python objects between slots (different interpreters)

### Cross-Context Communication

For sharing data between contexts, use the state API:

```python
from erlang import state_set, state_get

# Set from any context
state_set('config', {'model': 'gpt-4', 'timeout': 30})

# Get from any context (even different slots)
config = state_get('config')
```

```erlang
%% From Erlang
py:state_store(<<"config">>, #{model => <<"gpt-4">>}),
{ok, Config} = py:state_fetch(<<"config">>).
```

## Implementation Details

### Parallel Slot Structure

Each slot in the parallel pool contains:

```c
typedef struct {
    int slot_id;                  // 0 to N-1
    PyInterpreterState *interp;   // Independent interpreter
    PyThreadState *tstate;        // Thread state
    PyObject *globals;            // Global namespace
    PyObject *locals;             // Local namespace
    PyObject *module_cache;       // Import cache
    _Atomic int active_count;     // GIL holder count
} parallel_slot_t;
```

### GIL Acquisition Flow

**Worker mode:**
```c
PyGILState_STATE gstate = PyGILState_Ensure();
// Execute Python (blocks other contexts)
PyGILState_Release(gstate);
```

**Parallel mode:**
```c
parallel_slot_t *slot = parallel_pool_get(ctx->slot_id);
parallel_slot_acquire(slot);  // Acquire slot's OWN_GIL
// Execute Python (other slots run in parallel)
parallel_slot_release(slot);
```

### Dirty Scheduler Integration

All Python execution happens on Erlang dirty schedulers:

```erlang
%% NIFs are marked as dirty CPU-bound
-nifs([
    {context_call, 5, [dirty_cpu]},
    {context_eval, 4, [dirty_cpu]},
    {context_exec, 3, [dirty_cpu]}
]).
```

This ensures Python execution never blocks Erlang's main schedulers, maintaining BEAM responsiveness.

## Troubleshooting

### "ENABLE_PARALLEL_PYTHON requires Python 3.14+"

The parallel pool requires Python 3.14+ with OWN_GIL support. Earlier versions have bugs with C extensions in OWN_GIL subinterpreters.

**Solutions:**
1. Upgrade to Python 3.14+
2. Use worker mode (remove `ENABLE_PARALLEL_PYTHON`)
3. Use free-threaded Python 3.13t+

### Module not compatible with subinterpreters

Some C extensions don't support subinterpreters:

```
ImportError: module does not support subinterpreters
```

**Solutions:**
1. Check if library has subinterpreter-compatible version
2. Use worker mode for incompatible modules
3. Isolate incompatible code to a single context

### Performance not scaling

If parallel mode isn't providing expected speedup:

1. **Check mode:** `py:execution_mode()` should show the expected mode
2. **Check slot count:** Ensure enough slots for your workload
3. **Check contention:** Multiple contexts on same slot serialize
4. **Profile:** Short operations may not benefit from parallelism

## See Also

- [Scalability](scalability.md) - Rate limiting and load management
- [Context Affinity](context-affinity.md) - Context binding patterns
- [Pool Support](pools.md) - Custom pool configuration
- [Process-Bound Environments](process-bound-envs.md) - Per-process isolation
