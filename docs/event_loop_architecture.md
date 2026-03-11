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
