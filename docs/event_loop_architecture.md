# Event Loop Architecture

## Overview

The erlang_python event loop is a hybrid system where Erlang acts as the reactor
(I/O multiplexing via `enif_select`) and Python runs callbacks with proper GIL
management.

## Architecture Diagram

```
                              ERLANG SIDE                              PYTHON SIDE
    ========================================================================================

    +------------------+                                    +-------------------------+
    |  Erlang Process  |                                    |   ErlangEventLoop       |
    |  (user code)     |                                    |   (Python asyncio)      |
    +--------+---------+                                    +------------+------------+
             |                                                           |
             | py_event_loop:create_task(mod, func, args)                |
             v                                                           |
    +------------------+                                                 |
    |  py_event_loop   |  1. Serialize task to binary                   |
    |  (gen_server)    |  2. Submit to task_queue (no GIL)              |
    +--------+---------+  3. Send 'task_ready' message                   |
             |                                                           |
             v                                                           |
    +------------------+     enif_send (no GIL needed)                   |
    |  Task Queue      |  ======================================>        |
    |  (ErlNifIOQueue) |     thread-safe, lock-free                     |
    +------------------+                                                 |
                                                                         |
    +------------------+                                                 |
    |  Event Worker    |  4. Receives 'task_ready'                      |
    |  (gen_server)    |  5. Calls nif_process_ready_tasks              |
    +--------+---------+                                                 |
             |                                                           |
             v                                                           |
    +------------------+                                    +------------v------------+
    | process_ready_   |  6. Check task_count (atomic)     |                         |
    | tasks (NIF)      |     - If 0: return immediately    |   GIL ACQUIRED          |
    +--------+---------+       (no GIL needed!)            |   ===============       |
             |                                              |                         |
             | 7. Acquire GIL                              |  8. Use cached imports  |
             |    (only if tasks pending)                  |     (asyncio, run_and_  |
             v                                              |      send)              |
    +------------------+                                    |                         |
    | For each task:   |                                    |  9. For each task:      |
    | - Dequeue        |  --------------------------------> |     - Import module     |
    | - Deserialize    |                                    |     - Get function      |
    |                  |                                    |     - Convert args      |
    +------------------+                                    |     - Call function     |
                                                            |                         |
                                                            |  10. If coroutine:      |
                                                            |      - Wrap with        |
                                                            |        _run_and_send    |
                                                            |      - Schedule on loop |
                                                            |                         |
                                                            |  11. If sync result:    |
                                                            |      - Send directly    |
                                                            |        via enif_send    |
                                                            +------------+------------+
                                                                         |
             +-----------------------------------------------------------+
             |
             v
    +------------------+                                    +-------------------------+
    | _run_once(0)     |  12. Called with timeout=0        |   _run_once() Python    |
    | (from C)         |      (don't block, work pending)  +------------+------------+
    +------------------+                                                 |
                                                            13. Update cached time   |
                                                            14. Run ready callbacks  |
                                                                (from handle pool)   |
                                                            15. Poll for I/O events  |
                                                                (releases GIL!)      |
                                                            16. Dispatch events      |
                                                                         |
    +------------------+     GIL RELEASED                   +------------v------------+
    | poll_events_wait |  <================================ |   Py_BEGIN_ALLOW_       |
    | (C code)         |     pthread_cond_wait              |   THREADS               |
    +------------------+     (no Python, no GIL)            +-------------------------+
             |
             v
    +------------------+
    | enif_select      |  17. Wait for I/O events
    | (kernel: epoll/  |      (Erlang scheduler integration)
    |  kqueue)         |
    +------------------+
             |
             | I/O ready or timer fires
             v
    +------------------+
    | Erlang sends     |  18. Send {select, ...} or {timeout, ...}
    | message to       |      to worker process
    | worker           |
    +------------------+
             |
             v
    +------------------+                                    +-------------------------+
    | Worker receives  |  19. Wake up, dispatch callback   |   Callback executed     |
    | event message    |  --------------------------------> |   Result sent back      |
    +------------------+                                    +------------+------------+
                                                                         |
                                                            20. enif_send(caller,    |
                                                                {async_result, Ref,  |
                                                                 {ok, Result}})      |
                                                                         |
    +------------------+                                                 |
    | Caller process   |  <----------------------------------------------+
    | receives result  |
    +------------------+
```

## Key Optimizations (uvloop-style)

### 1. Early GIL Check
```
Before:
  - Always acquire GIL
  - Check if work exists
  - Release GIL if not

After:
  - Check atomic task_count FIRST
  - Only acquire GIL if task_count > 0
  - Saves expensive GIL acquisition when idle
```

### 2. Cached Python Imports
```c
// Stored in erlang_event_loop_t:
PyObject *cached_asyncio;      // asyncio module
PyObject *cached_run_and_send; // _run_and_send function
bool py_cache_valid;

// Avoids PyImport_ImportModule on every call
```

### 3. Handle Pooling
```python
# In ErlangEventLoop:
_handle_pool = []      # Pool of reusable Handle objects
_handle_pool_max = 150

def _get_handle(callback, args, context):
    if _handle_pool:
        handle = _handle_pool.pop()  # Reuse!
        handle._callback = callback
        return handle
    return events.Handle(...)  # Allocate only if pool empty

def _return_handle(handle):
    if len(_handle_pool) < _handle_pool_max:
        handle._callback = None  # Clear refs
        _handle_pool.append(handle)
```

### 4. Time Caching
```python
# In _run_once():
self._cached_time = time.monotonic()  # Once per iteration

def time(self):
    return self._cached_time  # No syscall!
```

### 5. Timeout Hint
```c
// C code passes timeout=0 after scheduling coroutines
PyObject_CallMethod(loop->py_loop, "_run_once", "i", 0);
// Python doesn't block waiting for I/O, processes work immediately
```

## GIL Management Summary

```
OPERATION                          GIL NEEDED?
=================================================
submit_task (enqueue)              NO  - uses ErlNifIOQueue
enif_send (wakeup)                 NO  - Erlang message passing
Check task_count (atomic)          NO  - atomic load
Dequeue tasks (Phase 1)            NO  - NIF operations only
  - enif_ioq_peek/deq             NO
  - enif_binary_to_term           NO
  - enif_alloc_env                NO
Process tasks (Phase 2)            YES - Python API calls
poll_events_wait                   NO  - releases GIL during wait
Dispatch callbacks                 YES - Python code execution
Send result (enif_send)            NO  - Erlang message passing
```

### Two-Phase Processing (New)

```
PHASE 1: Dequeue (NO GIL)          PHASE 2: Process (WITH GIL)
========================           ============================
pthread_mutex_lock                 PyGILState_Ensure
while (tasks < 64):                for each task:
  - peek queue                       - import module
  - deserialize term                 - call function
  - store in array                   - schedule coroutine
  - dequeue                        _run_once(0)
pthread_mutex_unlock               PyGILState_Release
```

## Data Flow

```
1. User: py_event_loop:create_task(math, sqrt, [2.0])
   |
2. Erlang serializes: {CallerPid, Ref, <<"math">>, <<"sqrt">>, [2.0], #{}}
   |
3. NIF enqueues to task_queue (lock-free)
   |
4. enif_send: worker ! task_ready
   |
5. Worker calls nif_process_ready_tasks
   |
6. [Check: task_count > 0?] -- NO --> return ok (no GIL)
   |
   YES
   |
7. Acquire GIL
   |
8. Dequeue task, call math.sqrt(2.0)
   |
9. Result is not a coroutine, send immediately:
   enif_send(CallerPid, {async_result, Ref, {ok, 1.414...}})
   |
10. Release GIL
    |
11. Caller receives: {async_result, Ref, {ok, 1.414...}}
```

## Performance Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| Sync task throughput | ~300K/sec | Direct call, no coroutine |
| Async task throughput | ~150K/sec | create_task + await |
| Concurrent (20 procs) | ~350K/sec | Parallel submission |
| GIL acquisitions | 1 per batch | Not per-task |
| Handle allocations | ~0 (pooled) | After warmup |
| Time syscalls | 1 per iteration | Cached within iteration |

## Per-Process Namespace Management

Each Erlang process can have an isolated Python namespace within an event loop. These namespaces are tracked in a linked list protected by `namespaces_mutex`.

### Usage

Define functions and state for async tasks in your process's namespace:

```erlang
%% Get event loop reference
{ok, LoopRef} = py_event_loop:get_loop(),

%% Define async functions in this process's namespace
ok = py_nif:event_loop_exec(LoopRef, <<"
import asyncio

async def process_data(items):
    results = []
    for item in items:
        await asyncio.sleep(0.01)  # Simulate async I/O
        results.append(item * 2)
    return results

# State persists across calls
call_count = 0

async def tracked_call(x):
    global call_count
    call_count += 1
    return {'result': x, 'call_number': call_count}
">>),

%% Use the functions via create_task with __main__ module
{ok, Ref1} = py_event_loop:create_task(Loop, '__main__', process_data, [[1,2,3]]),
{ok, [2,4,6]} = py_event_loop:await(Ref1),

%% State is maintained
{ok, Ref2} = py_event_loop:create_task(Loop, '__main__', tracked_call, [42]),
{ok, #{<<"result">> := 42, <<"call_number">> := 1}} = py_event_loop:await(Ref2).
```

### Evaluating Expressions

```erlang
%% Quick evaluation in the process namespace
{ok, 100} = py_nif:event_loop_eval(LoopRef, <<"50 * 2">>),

%% Access previously defined variables
ok = py_nif:event_loop_exec(LoopRef, <<"config = {'timeout': 30}">>),
{ok, #{<<"timeout">> := 30}} = py_nif:event_loop_eval(LoopRef, <<"config">>).
```

### Process Isolation

Each Erlang process has its own isolated namespace:

```erlang
%% Two processes define the same variable name - no conflict
Pids = [spawn(fun() ->
    ok = py_nif:event_loop_exec(LoopRef, <<"my_id = ", (integer_to_binary(N))/binary>>),
    {ok, N} = py_nif:event_loop_eval(LoopRef, <<"my_id">>),
    io:format("Process ~p has my_id = ~p~n", [self(), N])
end) || N <- lists:seq(1, 5)].
```

### Lock Ordering

To prevent ABBA deadlocks, locks must always be acquired in this order:

```
1. GIL (PyGILState_Ensure)
2. namespaces_mutex (pthread_mutex_lock)
```

This ordering is enforced in:
- `ensure_process_namespace()` - Called with GIL held, then acquires mutex
- `event_loop_down()` - Acquires GIL first, then mutex for cleanup
- `event_loop_destructor()` - Acquires GIL first, then mutex for cleanup

### Cleanup Behavior

When a monitored process dies (`event_loop_down`) or the event loop is destroyed:

**For main interpreter (`interp_id == 0`):**
```c
PyGILState_STATE gstate = PyGILState_Ensure();
pthread_mutex_lock(&loop->namespaces_mutex);
// Py_XDECREF(ns->globals), etc.
pthread_mutex_unlock(&loop->namespaces_mutex);
PyGILState_Release(gstate);
```

**For subinterpreters (`interp_id != 0`):**
```c
pthread_mutex_lock(&loop->namespaces_mutex);
// Skip Py_XDECREF - cannot safely acquire subinterpreter GIL
// Objects freed when interpreter is destroyed
enif_free(ns);
pthread_mutex_unlock(&loop->namespaces_mutex);
```

This design accepts a minor memory leak (Python dicts not decrefd) to avoid the complexity and risk of acquiring a subinterpreter's GIL from an arbitrary thread.

## Event Loop Pool with OWN_GIL Mode

For workloads requiring true parallel Python execution, the event loop pool can be configured to use OWN_GIL subinterpreters. Each worker thread has its own Python GIL, enabling parallel CPU-bound execution.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Main Interpreter                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │   Event Loop Pool (coordination only)                   │   │
│  │   - Session registry (PID -> worker mapping)            │   │
│  │   - Process monitoring for cleanup                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │ dispatch via pipe
                              ▼
┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
│ OWN_GIL    │ │ OWN_GIL    │ │ OWN_GIL    │ │ OWN_GIL    │
│ Worker 0   │ │ Worker 1   │ │ Worker 2   │ │ Worker N   │
│ (GIL_0)    │ │ (GIL_1)    │ │ (GIL_2)    │ │ (GIL_N)    │
│            │ │            │ │            │ │            │
│ Sessions:  │ │ Sessions:  │ │ Sessions:  │ │ Sessions:  │
│ - PID_A    │ │ - PID_B    │ │ - PID_C    │ │ - PID_D    │
└────────────┘ └────────────┘ └────────────┘ └────────────┘
```

### Configuration

Enable OWN_GIL mode in sys.config:

```erlang
{erlang_python, [
    {event_loop_pool_size, 4},
    {event_loop_pool_owngil, true}
]}
```

### Usage

The API remains unchanged - OWN_GIL mode is transparent:

```erlang
%% Submit async task (routes to OWN_GIL worker automatically)
Ref = py_event_loop_pool:create_task(math, sqrt, [16.0]),
{ok, 4.0} = py_event_loop_pool:await(Ref).

%% Blocking run
{ok, Result} = py_event_loop_pool:run(my_module, compute, [Args]).

%% Fire-and-forget
ok = py_event_loop_pool:spawn_task(my_module, background_work, []).
```

### Process Affinity

Each Erlang process is consistently mapped to the same worker based on PID hash:

```erlang
%% All tasks from this process go to the same worker
Ref1 = py_event_loop_pool:create_task(math, sqrt, [4.0]),
Ref2 = py_event_loop_pool:create_task(math, sqrt, [9.0]),
Ref3 = py_event_loop_pool:create_task(math, sqrt, [16.0]),
%% Executes in order on a single worker
```

### Session Management

Sessions are created automatically on first task submission and cleaned up when the process exits:

1. **Creation**: First `create_task` from a PID creates a session
2. **Routing**: Session maps PID to specific worker and namespace
3. **Cleanup**: Process monitor triggers session destruction on exit

### Performance

Benchmark comparison (4 workers):

| Workload | Regular Pool | OWN_GIL Pool | Speedup |
|----------|--------------|--------------|---------|
| Sequential | ~64K/sec | ~137K/sec | 2.1x |
| Concurrent (4 procs) | ~107K/sec | ~196K/sec | 1.8x |
| CPU-bound parallel | 207ms | 105ms | 2.0x |

Run benchmark: `escript examples/bench_owngil_pool.erl`

### When to Use OWN_GIL Mode

**Use OWN_GIL when:**
- Running CPU-bound Python code from multiple Erlang processes
- Need true parallel execution (not just concurrency)
- Python 3.12+ is available

**Use regular mode when:**
- Primarily I/O-bound operations
- Single-process workload
- Need shared state across all tasks
