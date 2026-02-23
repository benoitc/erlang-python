# Architecture

This document describes the internal architecture of erlang_python, focusing on how Python execution is integrated with Erlang's concurrency model.

## Overview

erlang_python provides high-performance Python integration for Erlang/Elixir applications. The architecture is designed to:

1. Never block Erlang schedulers
2. Maximize throughput for async operations
3. Support multiple parallelism modes (sub-interpreters, free-threaded, multi-executor)
4. Provide seamless bidirectional communication

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         erlang_python_sup                           │
├─────────────────────────────────────────────────────────────────────┤
│ py_pool          │ py_subinterp_pool │ py_async_driver              │
│ (sync calls)     │ (CPU parallelism) │ (all async)                  │
│                  │                   │                              │
│ py_worker x N    │ subinterp x N     │ └─ py_event_loop_proc        │
│ (dirty NIFs)     │ (own GIL each)    │    (unified event queue)     │
└─────────────────────────────────────────────────────────────────────┘
```

### py_pool (Synchronous Calls)

The main worker pool handles synchronous `py:call/3,4,5` operations:

- **py_worker processes**: Each worker owns a Python execution context
- **Dirty NIFs**: Python calls run on dirty I/O schedulers, never blocking normal schedulers
- **Process affinity**: `py:bind/0` binds a process to a specific worker for state preservation
- **Round-robin distribution**: Unbound calls are distributed across workers

### py_async_driver (Unified Async Architecture)

All async operations go through `py_async_driver`, which manages the unified event-driven architecture:

- **py_event_loop_proc**: Erlang process that owns the native event loop
- **Callback ID generation**: Lock-free atomic counter for correlating requests/responses
- **Non-blocking submit NIFs**: `submit_call/6` and `submit_coroutine/6` queue work without blocking
- **Direct result delivery**: Results sent via `enif_send` directly to waiting processes

Operations using this path:
- `py:async_call/3,4` - Async function calls
- `py:async_gather/1` - Concurrent async calls
- `py:async_stream/3,4` - Async generator consumption
- `py_asgi:run_async/4,5` - Async ASGI request handling

### py_subinterp_pool (True Parallelism)

For CPU-bound Python work, sub-interpreters provide true parallelism:

- **Python 3.12+**: Each sub-interpreter has its own GIL
- **py:parallel/1**: Execute multiple calls truly in parallel
- **Isolated state**: Sub-interpreters don't share Python objects

## Execution Modes

The library auto-detects the best execution mode:

| Mode | Python Version | How It Works |
|------|----------------|--------------|
| `free_threaded` | 3.13+ (nogil) | No GIL, true parallel execution |
| `subinterp` | 3.12+ | Per-interpreter GIL, parallel via isolation |
| `multi_executor` | Any | Single GIL, N executor threads |

Check current mode: `py:execution_mode/0`

## Event-Driven Async Flow

```
  Erlang Process                    py_async_driver              Python
       │                                  │                         │
       │ py:async_call(M, F, A)           │                         │
       ├─────────────────────────────────>│                         │
       │                                  │                         │
       │ {ok, Ref}                        │                         │
       │<─────────────────────────────────│                         │
       │                                  │                         │
       │                    submit_coroutine(CallbackId, ...)       │
       │                                  ├────────────────────────>│
       │                                  │                         │
       │                                  │    execute coroutine    │
       │                                  │                         │
       │                                  │  enif_send(py_result)   │
       │<─────────────────────────────────┼─────────────────────────│
       │                                  │                         │
       │ {py_result, Ref, Result}         │                         │
       │                                  │                         │
```

Key benefits of this architecture:
- **No polling**: Results delivered via Erlang messages
- **No blocking**: NIFs return immediately after queueing work
- **Efficient correlation**: Atomic callback IDs with O(1) lookup
- **Scalable**: Single event loop handles thousands of concurrent operations

## NIF Architecture

### Dirty Schedulers

All Python-executing NIFs run on dirty schedulers:
- `ERL_NIF_DIRTY_JOB_IO_BOUND` for I/O-heavy operations
- `ERL_NIF_DIRTY_JOB_CPU_BOUND` for CPU-heavy operations

### GIL Management

```c
// Release GIL while waiting for Erlang
Py_BEGIN_ALLOW_THREADS
// Wait for callback response
pthread_cond_wait(&cond, &mutex);
Py_END_ALLOW_THREADS
```

### Result Delivery

Results are sent directly to Erlang processes:
```c
enif_send(env, &caller_pid, msg_env,
    enif_make_tuple3(msg_env,
        enif_make_atom(msg_env, "py_result"),
        callback_ref,
        result_term));
```

## ASGI Integration

ASGI applications can be run synchronously or asynchronously:

### Synchronous (`py_asgi:run/4`)

- Direct NIF execution
- Blocking (on dirty scheduler)
- Uses optimized scope building with interned keys

### Asynchronous (`py_asgi:run_async/4`)

- Uses `py_async_driver` for execution
- Non-blocking from caller's perspective
- Supports high concurrency

## Callbacks (Python → Erlang)

When Python calls an Erlang function:

1. Python calls `erlang.my_func(args)`
2. NIF suspends Python execution
3. Message sent to callback registry
4. Erlang function executes
5. Result written back via pipe
6. Python execution resumes

This supports arbitrary nesting depth without deadlocks.

## Memory Management

### Python Objects

- Reference counting via `Py_INCREF`/`Py_DECREF`
- Resource tracking via Erlang NIF resources
- Destructor callbacks for cleanup

### Shared State

- ETS tables with `{write_concurrency, true}`
- Atomic counters for metrics
- No Python-side state sharing between workers

## Configuration

```erlang
{erlang_python, [
    {num_workers, 4},           % Sync worker pool size
    {num_subinterp_workers, 4}  % Sub-interpreter pool size
]}
```

## Performance Characteristics

| Operation | Typical Throughput | Notes |
|-----------|-------------------|-------|
| `py:call` (sync) | 80-100K ops/sec | Bound by GIL |
| `py:async_call` | 15-20K ops/sec | Event loop overhead |
| `py:async_gather` | Higher per-op | Amortizes submit cost |
| `py:parallel` | Linear scaling | Sub-interpreter count |

Run benchmarks: `py_unified_bench:run_all()`
