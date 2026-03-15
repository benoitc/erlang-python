# OWN_GIL Mode Internals

## Overview

OWN_GIL mode provides true parallel Python execution using Python 3.12+ per-interpreter GIL (`PyInterpreterConfig_OWN_GIL`). Each OWN_GIL context runs in a dedicated pthread with its own subinterpreter and GIL.

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

## Files

| File | Description |
|------|-------------|
| `c_src/py_nif.h` | Structure definitions, request types |
| `c_src/py_nif.c` | Thread main, dispatch, execute functions |
| `src/py_context.erl` | Erlang API for context management |
| `test/py_owngil_features_SUITE.erl` | Test suite |
