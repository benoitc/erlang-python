# OWN_GIL Mode Internals

## Overview

OWN_GIL mode provides true parallel Python execution using Python 3.12+ per-interpreter GIL (`PyInterpreterConfig_OWN_GIL`). Each OWN_GIL context runs in a dedicated pthread with its own subinterpreter and GIL.

## Quick Start

```erlang
%% Create an OWN_GIL context (requires Python 3.12+)
{ok, Ctx} = py_context:start_link(1, owngil),

%% Basic operations work the same as other modes
{ok, 4.0} = py_context:call(Ctx, math, sqrt, [16], #{}),
ok = py_context:exec(Ctx, <<"x = 42">>),
{ok, 42} = py_context:eval(Ctx, <<"x">>),

%% True parallelism: multiple OWN_GIL contexts execute simultaneously
{ok, Ctx2} = py_context:start_link(2, owngil),
%% Ctx and Ctx2 run in parallel with independent GILs

%% Process-local environments for namespace isolation
{ok, Env} = py_context:create_local_env(Ctx),
CtxRef = py_context:get_nif_ref(Ctx),
ok = py_nif:context_exec(CtxRef, <<"my_var = 'isolated'">>  , Env),

%% Cleanup
py_context:stop(Ctx),
py_context:stop(Ctx2).
```

## Feature Compatibility

All major erlang_python features work with OWN_GIL mode:

| Feature | Status | Notes |
|---------|--------|-------|
| `py_context:call/5` | Full | Function calls |
| `py_context:eval/2` | Full | Expression evaluation |
| `py_context:exec/2` | Full | Statement execution |
| Channels (`py_channel`) | Full | Bidirectional messaging |
| Buffers (`py_buffer`) | Full | Zero-copy streaming |
| Callbacks (`erlang.call`) | Partial | Uses thread_worker, not re-entrant |
| PIDs (`erlang.Pid`) | Full | Round-trip serialization |
| Send (`erlang.send`) | Full | Fire-and-forget messaging |
| Reactor (`erlang.reactor`) | Full | FD-based protocols |
| Async Tasks | Full | `py_event_loop:create_task` |
| Asyncio | Full | `asyncio.sleep`, `gather`, etc. |
| Process-local envs | Full | Namespace isolation |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Erlang VM                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Process A                    Process B                            │
│   py_context:call(Ctx1, ...)   py_context:call(Ctx2, ...)          │
│        │                            │                               │
│        ▼                            ▼                               │
│   ┌─────────────┐             ┌─────────────┐                       │
│   │ Dirty Sched │             │ Dirty Sched │                       │
│   └──────┬──────┘             └──────┬──────┘                       │
│          │                           │                              │
└──────────┼───────────────────────────┼──────────────────────────────┘
           │                           │
           │ dispatch_to_owngil_thread │
           ▼                           ▼
┌──────────────────────┐    ┌──────────────────────┐
│  OWN_GIL Thread 1    │    │  OWN_GIL Thread 2    │
│  ┌────────────────┐  │    │  ┌────────────────┐  │
│  │ Subinterpreter │  │    │  │ Subinterpreter │  │
│  │ (own GIL)      │  │    │  │ (own GIL)      │  │
│  └────────────────┘  │    └──┴────────────────┘  │
│  Parallel Execution! │    │  Parallel Execution! │
└──────────────────────┘    └──────────────────────┘
```

## Comparison with Other Modes

| Mode | Thread Model | GIL | Parallelism |
|------|-------------|-----|-------------|
| `worker` | Dirty scheduler | Main interpreter GIL | None |
| `subinterp` | Dirty scheduler | Shared GIL | None (isolated namespaces) |
| `owngil` | Dedicated pthread | Per-interpreter GIL | True parallel |

## Key Data Structures

### py_context_t (OWN_GIL fields)

```c
typedef struct {
    // ... common fields ...

    bool uses_own_gil;              // OWN_GIL mode flag
    pthread_t own_gil_thread;       // Dedicated pthread
    PyThreadState *own_gil_tstate;  // Thread state
    PyInterpreterState *own_gil_interp; // Interpreter state

    // IPC synchronization
    pthread_mutex_t request_mutex;
    pthread_cond_t request_ready;   // Signal: request available
    pthread_cond_t response_ready;  // Signal: response ready

    // Request/response state
    int request_type;               // CTX_REQ_* enum
    ErlNifEnv *shared_env;          // Zero-copy term passing
    ERL_NIF_TERM request_term;
    ERL_NIF_TERM response_term;
    bool response_ok;

    // Process-local env support
    void *local_env_ptr;            // py_env_resource_t*

    // Lifecycle
    _Atomic bool thread_running;
    _Atomic bool shutdown_requested;
} py_context_t;
```

### Request Types

```c
typedef enum {
    CTX_REQ_CALL,            // Call Python function
    CTX_REQ_EVAL,            // Evaluate expression
    CTX_REQ_EXEC,            // Execute statements
    CTX_REQ_REACTOR_READ,    // Reactor on_read_ready
    CTX_REQ_REACTOR_WRITE,   // Reactor on_write_ready
    CTX_REQ_REACTOR_INIT,    // Reactor init_connection
    CTX_REQ_CALL_WITH_ENV,   // Call with process-local env
    CTX_REQ_EVAL_WITH_ENV,   // Eval with process-local env
    CTX_REQ_EXEC_WITH_ENV,   // Exec with process-local env
    CTX_REQ_CREATE_LOCAL_ENV,// Create process-local env dicts
    CTX_REQ_SHUTDOWN         // Shutdown thread
} ctx_request_type_t;
```

## Request Flow

### 1. Context Creation

```
nif_context_create(env, "owngil")
    └── owngil_context_init(ctx)
        ├── Initialize mutex/condvars
        ├── Create shared_env
        └── pthread_create(owngil_context_thread_main)
            └── owngil_context_thread_main(ctx)
                ├── Py_NewInterpreterFromConfig(OWN_GIL)
                ├── Initialize globals/locals
                ├── Register py_event_loop module
                └── Enter request loop
```

### 2. Request Dispatch

```
nif_context_call(env, ctx, module, func, args, kwargs)
    │
    ├── [ctx->uses_own_gil == true]
    │   └── dispatch_to_owngil_thread(env, ctx, CTX_REQ_CALL, request)
    │       ├── pthread_mutex_lock(&ctx->request_mutex)
    │       ├── Copy request term to shared_env
    │       ├── Set ctx->request_type = CTX_REQ_CALL
    │       ├── pthread_cond_signal(&ctx->request_ready)
    │       ├── pthread_cond_wait(&ctx->response_ready)  // Block
    │       ├── Copy response from shared_env
    │       └── pthread_mutex_unlock(&ctx->request_mutex)
    │
    └── [ctx->uses_own_gil == false]
        └── Direct execution with GIL (worker/subinterp mode)
```

### 3. Request Processing (OWN_GIL Thread)

```
owngil_context_thread_main(ctx)
    while (!shutdown_requested) {
        pthread_cond_wait(&ctx->request_ready)

        owngil_execute_request(ctx)
            switch (ctx->request_type) {
                case CTX_REQ_CALL: owngil_execute_call(ctx); break;
                case CTX_REQ_EVAL: owngil_execute_eval(ctx); break;
                case CTX_REQ_EXEC: owngil_execute_exec(ctx); break;
                // ... other cases
            }

        pthread_cond_signal(&ctx->response_ready)
    }
```

## Process-Local Environments

OWN_GIL contexts support process-local environments for namespace isolation:

```
                Erlang Process A          Erlang Process B
                     │                         │
                     ▼                         ▼
             ┌───────────────┐         ┌───────────────┐
             │ py_env_res_t  │         │ py_env_res_t  │
             │ globals_A     │         │ globals_B     │
             │ locals_A      │         │ locals_B      │
             └───────┬───────┘         └───────┬───────┘
                     │                         │
                     └─────────┬───────────────┘
                               ▼
                    ┌─────────────────────┐
                    │   OWN_GIL Context   │
                    │   (shared context,  │
                    │   isolated envs)    │
                    └─────────────────────┘
```

### Creating Process-Local Env

```
py_context:create_local_env(Ctx)
    └── nif_create_local_env(CtxRef)
        └── dispatch_create_local_env_to_owngil(env, ctx, res)
            └── owngil_execute_create_local_env(ctx)
                ├── res->globals = PyDict_New()
                ├── res->locals = PyDict_New()
                └── res->interp_id = ctx->interp_id
```

### Using Process-Local Env

```erlang
{ok, Env} = py_context:create_local_env(Ctx),
CtxRef = py_context:get_nif_ref(Ctx),
ok = py_nif:context_exec(CtxRef, <<"x = 1">>, Env),
{ok, 1} = py_nif:context_eval(CtxRef, <<"x">>, #{}, Env).
```

## Thread Lifecycle

### Startup

1. `Py_NewInterpreterFromConfig` with `PyInterpreterConfig_OWN_GIL`
2. Save thread state and interpreter state
3. Initialize `__builtins__` in globals
4. Register `py_event_loop` module for reactor callbacks
5. Release GIL and enter request loop

### Request Loop

```c
while (!shutdown_requested) {
    pthread_mutex_lock(&request_mutex);
    while (!request_pending && !shutdown_requested) {
        pthread_cond_wait(&request_ready, &request_mutex);
    }

    if (shutdown_requested) break;

    // Process request (GIL already held within subinterpreter)
    owngil_execute_request(ctx);

    pthread_cond_signal(&response_ready);
    pthread_mutex_unlock(&request_mutex);
}
```

### Shutdown

1. Set `shutdown_requested = true`
2. Signal `request_ready` to wake thread
3. Thread exits loop, acquires GIL
4. Call `Py_EndInterpreter` to destroy subinterpreter
5. pthread terminates

## Memory Management

### Shared Environment

- `ctx->shared_env` is used for zero-copy term passing
- Request terms copied into shared_env by caller
- Response terms created in shared_env by OWN_GIL thread
- Caller copies response back to their env

### Process-Local Env Cleanup

```c
py_env_resource_dtor(env, res) {
    if (res->pool_slot >= 0) {
        // Shared-GIL subinterpreter: DECREF with pool GIL
    } else if (res->interp_id != 0) {
        // OWN_GIL subinterpreter: skip DECREF
        // Py_EndInterpreter cleans up all objects
    } else {
        // Worker mode: DECREF with main GIL
    }
}
```

## Reactor / Event Loop Integration

OWN_GIL contexts support the reactor pattern for I/O-driven protocols. The `py_event_loop` module is registered in each OWN_GIL subinterpreter during startup.

### Why Event Loop Registration Matters

Each Python subinterpreter has its own module namespace. The `py_event_loop` module provides:
- `erlang.reactor` protocol callbacks (`on_read_ready`, `on_write_ready`, `init_connection`)
- Per-interpreter state for cached function references
- Module state isolation between interpreters

### Reactor Request Flow

```
┌────────────────────────────────────────────────────────────────────────┐
│                           Erlang                                        │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  py_reactor_context                                                     │
│       │                                                                 │
│       │  {select, FdRes, Ref, ready_input}                             │
│       ▼                                                                 │
│  handle_info                                                            │
│       │                                                                 │
│       ├── Read data from fd into ReactorBuffer                         │
│       │                                                                 │
│       └── py_nif:reactor_on_read_ready(CtxRef, Fd)                     │
│                │                                                        │
└────────────────┼────────────────────────────────────────────────────────┘
                 │
                 │ [ctx->uses_own_gil == true]
                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│  dispatch_reactor_read_to_owngil(env, ctx, fd, buffer_ptr)             │
│       │                                                                 │
│       ├── ctx->reactor_buffer_ptr = buffer_ptr                         │
│       ├── ctx->request_type = CTX_REQ_REACTOR_READ                     │
│       ├── pthread_cond_signal(&request_ready)                          │
│       └── pthread_cond_wait(&response_ready)                           │
└────────────────────────────────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│  OWN_GIL Thread                                                         │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  owngil_execute_reactor_read(ctx)                                       │
│       │                                                                 │
│       ├── Create ReactorBuffer Python object                           │
│       │                                                                 │
│       ├── Get module state (per-interpreter reactor cache)             │
│       │   state = get_module_state()                                   │
│       │   ensure_reactor_cached_for_interp(state)                      │
│       │                                                                 │
│       └── Call Python: state->reactor_on_read(fd, buffer)              │
│                │                                                        │
│                ▼                                                        │
│           erlang.reactor.on_read_ready(fd, data)                        │
│                │                                                        │
│                ▼                                                        │
│           Protocol.data_received(data)                                  │
│                │                                                        │
│                └── Returns action: "continue" | "write_pending" | ...   │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Module State Per-Interpreter

Each OWN_GIL subinterpreter maintains its own cached references:

```c
typedef struct {
    PyObject *reactor_module;      // erlang.reactor module
    PyObject *reactor_on_read;     // Cached on_read_ready function
    PyObject *reactor_on_write;    // Cached on_write_ready function
    PyObject *reactor_init_conn;   // Cached init_connection function
    // ...
} py_event_loop_module_state_t;
```

The `ensure_reactor_cached_for_interp()` function lazily imports `erlang.reactor` and caches the callback functions on first use within each interpreter.

### Reactor Request Types

| Request Type | Dispatch Function | Execute Function |
|--------------|-------------------|------------------|
| `CTX_REQ_REACTOR_READ` | `dispatch_reactor_read_to_owngil` | `owngil_execute_reactor_read` |
| `CTX_REQ_REACTOR_WRITE` | `dispatch_reactor_write_to_owngil` | `owngil_execute_reactor_write` |
| `CTX_REQ_REACTOR_INIT` | `dispatch_reactor_init_to_owngil` | `owngil_execute_reactor_init` |

### Buffer Handling

For read operations, the `ReactorBuffer` (zero-copy buffer) is passed through:

1. `py_reactor_context` reads data into a `reactor_buffer_resource_t`
2. Buffer pointer stored in `ctx->reactor_buffer_ptr`
3. OWN_GIL thread wraps it in a Python `ReactorBuffer` object
4. Python protocol receives data via buffer protocol (zero-copy)

### Example: TCP Echo Server with OWN_GIL

```erlang
%% Start OWN_GIL context for protocol handling
{ok, Ctx} = py_context:start_link(1, owngil),

%% Define protocol in Python
py_context:exec(Ctx, <<"
import erlang.reactor as reactor

class EchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write(data)  # Echo back
        return 'write_pending'
">>),

%% Start reactor with the context
{ok, Reactor} = py_reactor_context:start_link(#{
    context => Ctx,
    protocol_class => <<"EchoProtocol">>
}).
```

## Performance Characteristics

| Operation | Shared-GIL | OWN_GIL |
|-----------|-----------|---------|
| Call overhead | ~2.5μs | ~10μs |
| Throughput (single) | 400K/s | 100K/s |
| Parallelism | None | True |
| Resource usage | Lower | Higher (1 pthread per context) |

Use OWN_GIL when:
- CPU-bound Python work that benefits from parallelism
- Long-running computations
- Need true concurrent Python execution

Use shared-GIL (subinterp) when:
- I/O-bound or short operations
- High call frequency
- Resource constraints

## Benchmarking

Run the benchmark to compare modes on your system:

```bash
rebar3 compile && escript examples/bench_owngil.erl
```

Example output:
```
========================================================
  OWN_GIL vs SHARED_GIL Benchmark
========================================================

System Information
------------------
  Erlang/OTP:       27
  Schedulers:       8
  Python:           3.14.0
  Subinterp:        true

1. Single Context Latency (1000 calls to math.sqrt)
   Mode            us/call    calls/sec
   ----            -------    ---------
   subinterp           2.5       400000
   owngil             10.2        98000

2. Parallel Throughput (4 contexts, 10000 calls each)
   Mode            total_ms   calls/sec
   ----            --------   ---------
   subinterp          100.5       398000
   owngil              28.3      1415000   <- 3.5x faster

3. CPU-Bound Speedup (fibonacci(30) x 4 contexts)
   Mode            total_ms   speedup
   ----            --------   -------
   subinterp          800.2      1.0x
   owngil             205.1      3.9x     <- near-linear scaling
```

## Safety Mechanisms

### Interpreter ID Validation

Process-local environments (`py_env_resource_t`) store the Python interpreter ID when created. Before execution, OWN_GIL functions validate that the env belongs to the current interpreter:

```c
PyInterpreterState *current_interp = PyInterpreterState_Get();
if (current_interp != NULL && penv->interp_id != PyInterpreterState_GetID(current_interp)) {
    // Return {error, env_wrong_interpreter}
}
```

This prevents dangling pointer access when an env resource outlives its interpreter.

### Lock Ordering (ABBA Deadlock Prevention)

Lock ordering must be consistent to prevent deadlocks:

**Correct order: GIL first, then namespaces_mutex**

Normal execution path:
```
PyGILState_Ensure()     // 1. Acquire GIL
pthread_mutex_lock()     // 2. Acquire mutex
// ... work ...
pthread_mutex_unlock()   // 3. Release mutex
PyGILState_Release()     // 4. Release GIL
```

Cleanup paths (`event_loop_down`, `event_loop_destructor`) follow the same order:
```c
// For main interpreter: GIL first, then mutex
PyGILState_STATE gstate = PyGILState_Ensure();
pthread_mutex_lock(&loop->namespaces_mutex);
// ... cleanup with Py_XDECREF ...
pthread_mutex_unlock(&loop->namespaces_mutex);
PyGILState_Release(gstate);
```

For subinterpreters (where `PyGILState_Ensure` cannot be used), cleanup skips `Py_DECREF` - the objects will be freed when the interpreter is destroyed.

### Callback Re-entry Limitation

OWN_GIL contexts do not support the suspension/resume protocol used for `erlang.call()` callbacks. When Python code in an OWN_GIL context calls `erlang.call()`:

1. The call is routed to `thread_worker_call()` (not the OWN_GIL thread)
2. The call executes on a thread worker, not the calling OWN_GIL context
3. Re-entrant calls back to the same OWN_GIL context are not supported

This is because the OWN_GIL thread cannot be suspended - it owns its GIL and must remain responsive to process requests.

## Files

| File | Description |
|------|-------------|
| `c_src/py_nif.h` | Structure definitions, request types |
| `c_src/py_nif.c` | Thread main, dispatch, execute functions |
| `c_src/py_callback.c` | Callback handling, thread worker dispatch |
| `c_src/py_event_loop.c` | Event loop and namespace management |
| `src/py_context.erl` | Erlang API for context management |
| `test/py_owngil_features_SUITE.erl` | Test suite |
