/*
 * Copyright 2026 Benoit Chesneau
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file py_nif.h
 * @brief Shared header for the Python-Erlang NIF integration
 * @author Benoit Chesneau
 *
 * @mainpage Python-Erlang NIF Integration
 *
 * @section intro_sec Introduction
 *
 * This NIF (Native Implemented Function) library provides seamless integration
 * between Erlang/OTP and Python. It embeds a Python interpreter within the
 * Erlang VM and provides bidirectional communication capabilities.
 *
 * @section arch_sec Architecture
 *
 * The implementation follows a modular design with four main components:
 *
 * - **py_nif.h** - Shared types, macros, and declarations
 * - **py_convert.c** - Bidirectional type conversion (Python ↔ Erlang)
 * - **py_exec.c** - Python execution engine and GIL management
 * - **py_callback.c** - Erlang callback support and asyncio integration
 *
 * @section modes_sec Execution Modes
 *
 * The library supports three execution modes based on Python version:
 *
 * | Mode | Python Version | Description |
 * |------|----------------|-------------|
 * | FREE_THREADED | 3.13+ (no-GIL) | Direct execution without GIL |
 * | SUBINTERP | 3.12+ | Per-interpreter GIL isolation |
 * | MULTI_EXECUTOR | Any | Multiple executor threads with GIL |
 *
 * @section gil_sec GIL Management
 *
 * The GIL (Global Interpreter Lock) is managed following PyO3/Granian patterns:
 *
 * - `Py_BEGIN_ALLOW_THREADS` / `Py_END_ALLOW_THREADS` around blocking ops
 * - Executor threads hold the GIL and process queued requests
 * - Dirty I/O schedulers are used for Python-calling NIFs
 *
 * @section callback_sec Callback Mechanism
 *
 * Python code can call back to Erlang using a suspension/resume pattern:
 *
 * 1. Python calls `erlang.call('func', args)`
 * 2. NIF raises `SuspensionRequired` exception
 * 3. Dirty scheduler is released, callback sent to Erlang
 * 4. Erlang processes callback, calls `resume_callback/2`
 * 5. Python execution resumes with cached result
 *
 * @section mem_sec Memory Management
 *
 * - Erlang resources wrap Python objects (prevent GC)
 * - Thread-local storage for callback context
 * - Proper cleanup in resource destructors
 */

#ifndef PY_NIF_H
#define PY_NIF_H

/**
 * @defgroup includes Required Headers
 * @brief System and library headers required for the NIF
 * @{
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <erl_nif.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

#if defined(__linux__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#include <dlfcn.h>
/** @brief Flag indicating dlopen with RTLD_GLOBAL is needed for Python extensions */
#define NEED_DLOPEN_GLOBAL 1
#endif

#include <sys/select.h>
/** @} */

/* ============================================================================
 * Feature Detection Macros
 * ============================================================================ */

/**
 * @defgroup features Feature Detection
 * @brief Compile-time feature detection based on Python version
 * @{
 */

/**
 * @def HAVE_SUBINTERPRETERS
 * @brief Defined when Python 3.12+ sub-interpreter support is available
 *
 * Python 3.12 introduced per-interpreter GIL support, allowing true
 * parallelism between sub-interpreters.
 */
#if PY_VERSION_HEX >= 0x030C0000
#define HAVE_SUBINTERPRETERS 1
#endif

/**
 * @def HAVE_FREE_THREADED
 * @brief Defined when Python 3.13+ free-threaded (no-GIL) build is detected
 *
 * Python 3.13 introduced experimental free-threaded builds where the GIL
 * is completely disabled, allowing true multi-threaded Python execution.
 */
#if PY_VERSION_HEX >= 0x030D0000
#ifdef Py_GIL_DISABLED
#define HAVE_FREE_THREADED 1
#endif
#endif

/**
 * Py_NewRef was added in Python 3.10. Provide compatibility macro for older versions.
 */
#if PY_VERSION_HEX < 0x030A0000
static inline PyObject *Py_NewRef(PyObject *o) {
    Py_INCREF(o);
    return o;
}
#endif

/** @} */

/* Include subinterpreter thread pool header for OWN_GIL parallelism */
#include "py_subinterp_thread.h"

/* ============================================================================
 * Execution Mode
 * ============================================================================ */

/**
 * @defgroup exec_mode Execution Mode
 * @brief Python execution mode selection
 * @{
 */

/**
 * @enum py_execution_mode_t
 * @brief Execution mode for Python operations
 *
 * The execution mode determines how Python code is executed and how
 * the GIL is managed. The mode is auto-detected at initialization based
 * on the Python version and build configuration.
 */
typedef enum {
    /**
     * @brief Free-threaded mode (Python 3.13+ no-GIL build)
     *
     * In this mode, Python has no GIL and code executes directly
     * without any GIL acquisition. This provides maximum parallelism.
     */
    PY_MODE_FREE_THREADED,

    /**
     * @brief Conventional GIL mode (every other supported build)
     *
     * Coordinator-side work runs through the single executor thread.
     * Per-context worker / OWN_GIL pthreads handle the public context
     * APIs directly; this mode label only governs the coordinator path.
     */
    PY_MODE_GIL
} py_execution_mode_t;

/** @} */

/* ============================================================================
 * Runtime State Machine
 * ============================================================================ */

/**
 * @defgroup runtime_state Runtime State Machine
 * @brief Atomic state machine for Python runtime lifecycle
 * @{
 */

/**
 * @enum py_runtime_state_t
 * @brief Runtime state for Python interpreter lifecycle
 *
 * State transitions are performed atomically using CAS operations to ensure
 * thread-safe initialization and shutdown. The state machine prevents race
 * conditions during concurrent init/finalize calls.
 *
 * Valid transitions:
 *   UNINIT -> INITING (only one thread wins)
 *   INITING -> RUNNING (on success)
 *   INITING -> STOPPED (on failure)
 *   RUNNING -> SHUTTING_DOWN (only one thread wins)
 *   SHUTTING_DOWN -> STOPPED (after cleanup)
 */
typedef enum {
    /** @brief Initial state, Python not initialized */
    PY_STATE_UNINIT = 0,

    /** @brief Initialization in progress (transitional) */
    PY_STATE_INITING = 1,

    /** @brief Python running and ready for work */
    PY_STATE_RUNNING = 2,

    /** @brief Shutdown in progress, rejecting new work */
    PY_STATE_SHUTTING_DOWN = 3,

    /** @brief Fully stopped, safe to reinitialize */
    PY_STATE_STOPPED = 4
} py_runtime_state_t;

/**
 * @brief Atomically transition runtime state using CAS
 * @param from Expected current state
 * @param to Desired new state
 * @return true if transition succeeded, false if current state != from
 */
static inline bool runtime_transition(py_runtime_state_t from, py_runtime_state_t to) {
    extern _Atomic py_runtime_state_t g_runtime_state;
    py_runtime_state_t expected = from;
    return atomic_compare_exchange_strong(&g_runtime_state, &expected, to);
}

/**
 * @brief Get current runtime state
 * @return Current py_runtime_state_t value
 */
static inline py_runtime_state_t runtime_state(void) {
    extern _Atomic py_runtime_state_t g_runtime_state;
    return atomic_load(&g_runtime_state);
}

/**
 * @brief Check if runtime is in RUNNING state
 * @return true if Python is running and accepting work
 */
static inline bool runtime_is_running(void) {
    return runtime_state() == PY_STATE_RUNNING;
}

/**
 * @brief Check if runtime is shutting down or stopped
 * @return true if runtime is in SHUTTING_DOWN or STOPPED state
 */
static inline bool runtime_is_shutting_down(void) {
    py_runtime_state_t state = runtime_state();
    return state >= PY_STATE_SHUTTING_DOWN;
}

/** @} */

/* ============================================================================
 * Invariant Counters (Debugging/Diagnostics)
 * ============================================================================ */

/**
 * @defgroup invariants Invariant Counters
 * @brief Atomic counters for tracking resource lifecycle and detecting leaks
 * @{
 */

/**
 * @struct py_invariant_counters_t
 * @brief Atomic counters for debugging and leak detection
 *
 * These counters track paired operations (acquire/release, create/destroy)
 * to help detect resource leaks and imbalanced operations. At shutdown,
 * paired counters should be equal.
 */
typedef struct {
    /* GIL operations */
    _Atomic uint64_t gil_ensure_count;      /**< PyGILState_Ensure calls */
    _Atomic uint64_t gil_release_count;     /**< PyGILState_Release calls */

    /* Python object references */
    _Atomic uint64_t pyobj_created;         /**< py_object_t created */
    _Atomic uint64_t pyobj_destroyed;       /**< py_object_t destroyed */

    /* py_ref_t resources */
    _Atomic uint64_t pyref_created;         /**< py_ref_t created */
    _Atomic uint64_t pyref_destroyed;       /**< py_ref_t destroyed */

    /* Context resources */
    _Atomic uint64_t ctx_created;           /**< py_context_t created */
    _Atomic uint64_t ctx_destroyed;         /**< py_context_t destroyed */
    _Atomic uint64_t ctx_keep_count;        /**< enif_keep_resource(ctx) calls */
    _Atomic uint64_t ctx_release_count;     /**< enif_release_resource(ctx) calls */

    /* Suspended states */
    _Atomic uint64_t suspended_created;     /**< Suspended states created */
    _Atomic uint64_t suspended_resumed;     /**< Suspended states resumed */
    _Atomic uint64_t suspended_destroyed;   /**< Suspended states destroyed */

    /* Executor queue operations */
    _Atomic uint64_t enqueue_count;         /**< Requests enqueued */
    _Atomic uint64_t complete_count;        /**< Requests completed */
    _Atomic uint64_t rejected_count;        /**< Requests rejected (shutdown) */
} py_invariant_counters_t;

/** @brief Global invariant counters for debugging */
extern py_invariant_counters_t g_counters;

/** @} */

/* ============================================================================
 * Core Type Definitions
 * ============================================================================ */

/**
 * @defgroup types Core Types
 * @brief Primary data structures for the NIF
 * @{
 */

/**
 * @struct py_worker_t
 * @brief Represents a Python worker with its own namespace
 *
 * A worker encapsulates a Python execution context with isolated
 * global and local namespaces. Workers are created per-process in
 * Erlang and can execute Python code independently.
 *
 * @note Workers should be created via `py:worker_new/0` and destroyed
 *       via `py:worker_destroy/1` or automatically via GC.
 *
 * @see nif_worker_new
 * @see nif_worker_destroy
 */
typedef struct {
    /** @brief Python thread state for this worker */
    PyThreadState *thread_state;

    /** @brief Global namespace dictionary (`__globals__`) */
    PyObject *globals;

    /** @brief Local namespace dictionary (`__locals__`) */
    PyObject *locals;

    /** @brief Whether this worker currently owns the GIL */
    bool owns_gil;

    /* Callback support fields */

    /**
     * @brief Pipe file descriptors for callback IPC
     *
     * - `callback_pipe[0]` - Read end (Python reads responses)
     * - `callback_pipe[1]` - Write end (Erlang writes responses)
     */
    int callback_pipe[2];

    /** @brief PID of the Erlang callback handler process */
    ErlNifPid callback_handler;

    /** @brief Whether a callback handler is registered */
    bool has_callback_handler;

    /** @brief Environment for building callback messages */
    ErlNifEnv *callback_env;
} py_worker_t;

/* async_pending_t and py_async_worker_t removed - async workers replaced by event loop model */

/**
 * @struct py_object_t
 * @brief Wrapper for Python objects preventing garbage collection
 *
 * Erlang resources wrapping Python objects to prevent them from
 * being collected by Python's GC while Erlang holds a reference.
 *
 * @warning The wrapped object must be properly DECREF'd in the
 *          resource destructor.
 */
typedef struct {
    /** @brief The wrapped Python object (owned reference) */
    PyObject *obj;

    /** @brief Interpreter ID that owns this object (0 = main, >0 = subinterp) */
    uint32_t interp_id;
} py_object_t;

/**
 * @struct py_ref_t
 * @brief Python object reference with interpreter ID for auto-routing
 *
 * This extends py_object_t by adding the interpreter ID that created
 * the object. This allows automatic routing of method calls and
 * attribute access to the correct context.
 *
 * @note The interp_id is used by py_context_router to find the owning context
 * @warning Operations on this ref must be performed in the correct interpreter
 */
typedef struct {
    /** @brief The wrapped Python object (owned reference) */
    PyObject *obj;

    /** @brief Interpreter ID that owns this object (for routing) */
    uint32_t interp_id;
} py_ref_t;

/** @} */

/* ============================================================================
 * Request Types and Structures
 * ============================================================================ */

/**
 * @defgroup requests Request Handling
 * @brief Structures for executor request processing
 * @{
 */

/* Forward declaration for py_context_t (defined later) */
typedef struct py_context py_context_t;

/**
 * @enum py_request_type_t
 * @brief Types of requests that can be submitted to the executor
 */
typedef enum {
    PY_REQ_CALL,         /**< Call a Python function */
    PY_REQ_EVAL,         /**< Evaluate a Python expression */
    PY_REQ_EXEC,         /**< Execute Python statements */
    PY_REQ_NEXT,         /**< Get next value from iterator/generator */
    PY_REQ_IMPORT,       /**< Import a Python module */
    PY_REQ_GETATTR,      /**< Get attribute from Python object */
    PY_REQ_MEMORY_STATS, /**< Get Python memory statistics */
    PY_REQ_GC,           /**< Trigger Python garbage collection */
    PY_REQ_SHUTDOWN      /**< Signal executor shutdown */
} py_request_type_t;

/**
 * @struct py_request_t
 * @brief Request submitted to the executor thread for processing
 *
 * Encapsulates all information needed to execute a Python operation.
 * The caller thread blocks on the condition variable until the
 * executor signals completion.
 *
 * @note Requests are allocated on the stack by the caller NIF and
 *       passed to the executor. The executor processes them with
 *       the GIL held.
 */
typedef struct py_request {
    /** @brief Type of operation to perform */
    py_request_type_t type;

    /* Synchronization primitives */

    /** @brief Mutex for condition variable */
    pthread_mutex_t mutex;

    /** @brief Condition variable for completion signaling */
    pthread_cond_t cond;

    /** @brief Flag set when processing is complete */
    volatile bool completed;

    /* Common parameters */

    /** @brief Worker context (may be NULL for global ops) */
    py_worker_t *worker;

    /** @brief Context for process-owned operations (may be NULL) */
    py_context_t *context;

    /** @brief Caller's NIF environment for term creation */
    ErlNifEnv *env;

    /* Call/Import parameters */

    /** @brief Module name as binary */
    ErlNifBinary module_bin;

    /** @brief Function name as binary */
    ErlNifBinary func_bin;

    /** @brief Code string for eval/exec */
    ErlNifBinary code_bin;

    /** @brief Arguments list term */
    ERL_NIF_TERM args_term;

    /** @brief Keyword arguments map term */
    ERL_NIF_TERM kwargs_term;

    /** @brief Local variables map for eval */
    ERL_NIF_TERM locals_term;

    /** @brief Execution timeout in milliseconds (0 = no timeout) */
    unsigned long timeout_ms;

    /* Iterator parameters */

    /** @brief Generator/iterator wrapper for PY_REQ_NEXT */
    py_object_t *gen_wrapper;

    /* Getattr parameters */

    /** @brief Object wrapper for PY_REQ_GETATTR */
    py_object_t *obj_wrapper;

    /** @brief Attribute name as binary */
    ErlNifBinary attr_bin;

    /* GC parameters */

    /** @brief Generation to collect (0, 1, or 2) */
    int gc_generation;

    /* Result */

    /** @brief Result term set by executor */
    ERL_NIF_TERM result;

    /* Queue linkage */

    /** @brief Next request in executor queue */
    struct py_request *next;
} py_request_t;

/** @} */

/* ============================================================================
 * Callback/Suspension State
 * ============================================================================ */

/**
 * @defgroup callback Callback Support
 * @brief Structures for Python-to-Erlang callbacks
 * @{
 */

/**
 * @struct suspended_state_t
 * @brief State for a suspended Python execution awaiting callback result
 *
 * When Python code calls `erlang.call()`, execution is suspended and
 * this structure captures all state needed to resume after Erlang
 * processes the callback.
 *
 * @par Suspension Flow:
 * 1. Python calls `erlang.call('func', args)`
 * 2. `erlang_call_impl` raises `SuspensionRequired` exception
 * 3. `process_request` catches exception, creates `suspended_state_t`
 * 4. Returns `{suspended, CallbackId, StateRef, {Func, Args}}` to Erlang
 * 5. Erlang executes callback, calls `resume_callback(StateRef, Result)`
 * 6. `nif_resume_callback_dirty` replays Python with cached result
 *
 * @see erlang_call_impl
 * @see nif_resume_callback
 */
typedef struct {
    /** @brief Worker context for replay */
    py_worker_t *worker;

    /** @brief Unique identifier for this callback */
    uint64_t callback_id;

    /* Callback invocation info */

    /** @brief Name of Erlang function being called */
    char *callback_func_name;

    /** @brief Length of callback_func_name */
    size_t callback_func_len;

    /** @brief Arguments passed to the callback */
    PyObject *callback_args;

    /* Original request context for replay */

    /** @brief Original module name binary */
    ErlNifBinary orig_module;

    /** @brief Original function name binary */
    ErlNifBinary orig_func;

    /** @brief Original arguments (copied to orig_env) */
    ERL_NIF_TERM orig_args;

    /** @brief Original keyword arguments */
    ERL_NIF_TERM orig_kwargs;

    /** @brief Environment owning copied terms */
    ErlNifEnv *orig_env;

    /** @brief Original timeout setting */
    int orig_timeout_ms;

    /** @brief Original request type (PY_REQ_CALL, PY_REQ_EVAL) */
    int request_type;

    /** @brief Original code for eval/exec replay */
    ErlNifBinary orig_code;

    /** @brief Original locals map for eval replay */
    ERL_NIF_TERM orig_locals;

    /* Callback result */

    /** @brief Raw result data from Erlang callback */
    unsigned char *result_data;

    /** @brief Length of result_data */
    size_t result_len;

    /** @brief Flag: result is available for replay */
    _Atomic bool has_result;

    /** @brief Flag: result represents an error */
    _Atomic bool is_error;

    /* Synchronization */

    /** @brief Mutex for result access */
    pthread_mutex_t mutex;

    /** @brief Condition for blocking callback mode */
    pthread_cond_t cond;
} suspended_state_t;

/** @} */

/* ============================================================================
 * Sub-interpreter Support
 * ============================================================================ */

/**
 * @defgroup subinterp Sub-interpreter Support
 * @brief Structures for Python 3.12+ sub-interpreters
 * @{
 */

/**
 * @enum py_cmd_type_t
 * @brief Command types for thread-per-context dispatch
 *
 * Commands are dispatched from the dirty scheduler to the subinterpreter's
 * dedicated thread via mutex/condvar signaling.
 */
typedef enum {
    PY_CMD_NONE = 0,      /**< No command (initial state) */
    PY_CMD_CALL,          /**< Call a Python function */
    PY_CMD_EVAL,          /**< Evaluate a Python expression */
    PY_CMD_EXEC,          /**< Execute Python statements */
    PY_CMD_SHUTDOWN       /**< Shutdown the thread */
} py_cmd_type_t;

/**
 * @enum ctx_request_type_t
 * @brief Request types for OWN_GIL context thread dispatch
 *
 * Used by OWN_GIL contexts to communicate between the NIF (dirty scheduler)
 * and the dedicated pthread that owns the subinterpreter.
 */
typedef enum {
    CTX_REQ_NONE = 0,           /**< No request (idle state) */
    CTX_REQ_CALL,               /**< Call a Python function */
    CTX_REQ_EVAL,               /**< Evaluate a Python expression */
    CTX_REQ_EXEC,               /**< Execute Python statements */
    CTX_REQ_CALLBACK_RESULT,    /**< Erlang callback result available */
    CTX_REQ_SHUTDOWN,           /**< Shutdown the thread */
    /* Reactor dispatch requests for OWN_GIL mode */
    CTX_REQ_REACTOR_ON_READ_READY,   /**< Handle read ready event */
    CTX_REQ_REACTOR_ON_WRITE_READY,  /**< Handle write ready event */
    CTX_REQ_REACTOR_INIT_CONNECTION, /**< Initialize a connection */
    /* Process-local environment requests for OWN_GIL mode */
    CTX_REQ_CALL_WITH_ENV,      /**< Call with process-local environment */
    CTX_REQ_EVAL_WITH_ENV,      /**< Eval with process-local environment */
    CTX_REQ_EXEC_WITH_ENV,      /**< Exec with process-local environment */
    CTX_REQ_CREATE_LOCAL_ENV,   /**< Create process-local env dicts */
    CTX_REQ_APPLY_IMPORTS,      /**< Apply imports to module cache */
    CTX_REQ_APPLY_PATHS         /**< Apply paths to sys.path */
} ctx_request_type_t;

/**
 * @struct ctx_request_t
 * @brief Heap-allocated request for worker/owngil context queue
 *
 * Each request is heap-allocated with its own mutex/condvar for completion
 * signaling. This replaces the single-slot pattern that had race conditions
 * with multiple concurrent callers.
 *
 * Lifecycle:
 * 1. Caller allocates request with ctx_request_create()
 * 2. Caller fills in request data and copies terms to request_env
 * 3. Caller enqueues request and increments refcount (now 2: caller + queue)
 * 4. Worker dequeues request, processes it, fills result_env/result
 * 5. Worker sends result via enif_send() and releases queue's ref
 * 6. Caller receives result and releases its ref
 * 7. When refcount hits 0, request is freed
 *
 * For OWN_GIL mode, the worker thread sends results via enif_send() to avoid
 * blocking dirty schedulers. For worker mode (main interpreter), the same
 * pattern is used for consistency.
 */
typedef struct ctx_request {
    /** @brief Type of request */
    ctx_request_type_t type;

    /** @brief Per-request mutex for completion synchronization */
    pthread_mutex_t mutex;

    /** @brief Per-request condition for completion signaling */
    pthread_cond_t cond;

    /** @brief Set by worker when done (for blocking wait mode) */
    _Atomic bool completed;

    /** @brief Set by caller on timeout/destroy to skip processing */
    _Atomic bool cancelled;

    /* Request data (owned by this struct, not caller) */

    /** @brief Environment for request terms (created by caller) */
    ErlNifEnv *request_env;

    /** @brief Request parameters (in request_env) */
    ERL_NIF_TERM request_data;

    /** @brief Process-local env pointer for WITH_ENV requests */
    void *local_env_ptr;

    /** @brief Reactor buffer pointer for reactor requests */
    void *reactor_buffer_ptr;

    /** @brief FD for reactor requests */
    int reactor_fd;

    /* Result data (owned by this struct) */

    /** @brief Environment for result terms (created by worker) */
    ErlNifEnv *result_env;

    /** @brief Result term (in result_env) */
    ERL_NIF_TERM result;

    /** @brief True if request succeeded */
    bool success;

    /* Async delivery (for non-blocking dispatch) */

    /** @brief Caller's PID for async result delivery */
    ErlNifPid caller_pid;

    /** @brief Request ID for correlating async responses */
    ERL_NIF_TERM request_id;

    /** @brief Whether to use async delivery vs blocking wait */
    bool async_mode;

    /* Queue management */

    /** @brief Reference count (2=caller+queue, 1=one side, 0=free) */
    _Atomic int refcount;

    /** @brief Next request in queue */
    struct ctx_request *next;
} ctx_request_t;

/**
 * @brief Create a new context request
 *
 * Rolls back partial state on any init failure: pthread_mutex_init,
 * pthread_cond_init, or enif_alloc_env() can each fail under resource
 * pressure. Returning NULL keeps callers safe — every call site
 * already tests the result.
 *
 * @return Newly allocated request with refcount=1, or NULL on failure
 */
static inline ctx_request_t *ctx_request_create(void) {
    ctx_request_t *req = enif_alloc(sizeof(ctx_request_t));
    if (req == NULL) {
        return NULL;
    }
    memset(req, 0, sizeof(ctx_request_t));

    if (pthread_mutex_init(&req->mutex, NULL) != 0) {
        enif_free(req);
        return NULL;
    }
    if (pthread_cond_init(&req->cond, NULL) != 0) {
        pthread_mutex_destroy(&req->mutex);
        enif_free(req);
        return NULL;
    }
    req->request_env = enif_alloc_env();
    if (req->request_env == NULL) {
        pthread_cond_destroy(&req->cond);
        pthread_mutex_destroy(&req->mutex);
        enif_free(req);
        return NULL;
    }

    atomic_store(&req->completed, false);
    atomic_store(&req->cancelled, false);
    atomic_store(&req->refcount, 1);
    req->result_env = NULL;  /* Created by worker when processing */
    req->next = NULL;
    req->async_mode = false;
    req->reactor_fd = -1;
    req->local_env_ptr = NULL;
    req->reactor_buffer_ptr = NULL;

    return req;
}

/**
 * @brief Add a reference to a context request
 * @param req The request
 */
static inline void ctx_request_addref(ctx_request_t *req) {
    if (req) {
        atomic_fetch_add(&req->refcount, 1);
    }
}

/**
 * @brief Release a reference to a context request
 * @param req The request (may be NULL)
 *
 * When refcount reaches 0, frees mutex/cond/envs and the request struct.
 */
static inline void ctx_request_release(ctx_request_t *req) {
    if (req == NULL) return;

    int prev = atomic_fetch_sub(&req->refcount, 1);
    if (prev == 1) {
        /* Last reference - free everything */
        pthread_mutex_destroy(&req->mutex);
        pthread_cond_destroy(&req->cond);
        if (req->request_env) {
            enif_free_env(req->request_env);
        }
        if (req->result_env) {
            enif_free_env(req->result_env);
        }
        enif_free(req);
    }
}

/**
 * @struct py_cmd_t
 * @brief Command structure for thread-per-context dispatch
 *
 * This structure is used to pass commands from the dirty scheduler
 * to the subinterpreter's dedicated thread. The thread executes the
 * command and stores the result in result_env/result.
 */
typedef struct {
    /** @brief Type of command to execute */
    py_cmd_type_t type;

    /** @brief Caller's NIF environment (for reading terms) */
    ErlNifEnv *caller_env;

    /* Call parameters */
    ERL_NIF_TERM module;      /**< Module name term (for CALL) */
    ERL_NIF_TERM func;        /**< Function name term (for CALL) */
    ERL_NIF_TERM args;        /**< Arguments list (for CALL) */
    ERL_NIF_TERM kwargs;      /**< Keyword arguments map (for CALL) */

    /* Eval/Exec parameters */
    ERL_NIF_TERM code;        /**< Code string (for EVAL/EXEC) */
    ERL_NIF_TERM locals;      /**< Local variables map (for EVAL) */

    /* Result */
    ErlNifEnv *result_env;    /**< Environment for result term (allocated by thread) */
    ERL_NIF_TERM result;      /**< Result term (in result_env) */
    bool success;             /**< True if command succeeded */
    bool completed;           /**< True when command execution finished (even on error) */

    /* Copied command environment for cross-thread safety */
    ErlNifEnv *cmd_env;       /**< Environment for copied command terms (owned by dispatcher) */
} py_cmd_t;

/**
 * @struct py_context_t
 * @brief Process-owned Python context with shared-GIL subinterpreter pool
 *
 * A py_context_t is owned by a single Erlang process, which serializes
 * all access to it. For subinterpreters, contexts reference a slot in
 * the pre-created subinterpreter pool (shared GIL model).
 *
 * Execution happens directly on dirty schedulers using PyThreadState_Swap()
 * to switch to the subinterpreter's thread state. This avoids:
 * - Dedicated pthread per context
 * - Mutex/condvar dispatch overhead
 * - Term copying between environments
 *
 * @note Python 3.12+ uses shared-GIL subinterpreters via pool slots
 * @note Older Python uses worker mode with main interpreter namespace
 *
 * @see nif_context_create
 * @see nif_context_call
 * @see subinterp_pool_alloc
 */
struct py_context {
    /** @brief Unique interpreter ID for routing (0 = main, >0 = subinterp) */
    uint32_t interp_id;

    /** @brief Context mode: true=subinterpreter, false=worker */
    bool is_subinterp;

    /** @brief Flag indicating context has been destroyed (atomic for thread safety) */
    _Atomic bool destroyed;

    /** @brief Flag: context resources leaked due to unresponsive worker */
    _Atomic bool leaked;

    /** @brief Flag: callback handler is configured */
    bool has_callback_handler;

    /** @brief PID of Erlang process handling callbacks */
    ErlNifPid callback_handler;

    /** @brief Pipe for callback responses [read, write] */
    int callback_pipe[2];

    /* ========== Worker thread fields (used by both worker and owngil modes) ========== */

    /** @brief Dedicated pthread for this context */
    pthread_t worker_thread;

    /** @brief True when worker thread is running */
    _Atomic bool worker_running;

    /** @brief True when shutdown has been requested */
    _Atomic bool shutdown_requested;

    /** @brief True if this context uses a dedicated worker thread (worker mode) */
    bool uses_worker_thread;

    /** @brief True if thread initialization failed */
    _Atomic bool init_error;

    /* ========== Request queue (replaces single-slot pattern) ========== */

    /** @brief Mutex protecting the request queue */
    pthread_mutex_t queue_mutex;

    /** @brief Condition variable: work available in queue */
    pthread_cond_t queue_not_empty;

    /** @brief Head of request queue (dequeue from here) */
    ctx_request_t *queue_head;

    /** @brief Tail of request queue (enqueue here) */
    ctx_request_t *queue_tail;

    /** @brief Environment for sending messages back to Erlang */
    ErlNifEnv *msg_env;

    /* ========== Legacy compatibility fields (populated from queue request) ========== */
    /* These fields are populated by the worker thread from the current request
     * for compatibility with existing execute functions. They will be removed
     * once all execute functions are refactored to use ctx_request_t directly. */

    /** @brief Shared env for current request (points to current req->request_env) */
    ErlNifEnv *shared_env;

    /** @brief Current request type */
    int request_type;

    /** @brief Current request data term */
    ERL_NIF_TERM request_term;

    /** @brief Response term for current request */
    ERL_NIF_TERM response_term;

    /** @brief Success flag for current request */
    bool response_ok;

    /** @brief Reactor buffer pointer for current request */
    void *reactor_buffer_ptr;

    /** @brief Process-local env pointer for current request */
    void *local_env_ptr;

#ifdef HAVE_SUBINTERPRETERS
    /* ========== OWN_GIL specific fields ========== */

    /** @brief Whether this context uses OWN_GIL mode (subinterpreter with own GIL) */
    bool uses_own_gil;

    /** @brief Thread state for OWN_GIL subinterpreter */
    PyThreadState *own_gil_tstate;

    /** @brief Interpreter state for OWN_GIL subinterpreter */
    PyInterpreterState *own_gil_interp;
#else
    /** @brief Worker thread state (non-subinterp mode, kept for compatibility) */
    PyThreadState *thread_state;
#endif

    /** @brief Global namespace dictionary */
    PyObject *globals;

    /** @brief Local namespace dictionary */
    PyObject *locals;

    /** @brief Module cache (Dict: module_name -> PyModule) */
    PyObject *module_cache;
};

/* ============================================================================
 * Shared-GIL Pool Architecture for Subinterpreters
 * ============================================================================
 *
 * Architecture: Subinterpreters share the GIL but provide namespace isolation.
 * A pool of pre-created subinterpreters is initialized at startup.
 * Contexts allocate slots from this pool.
 *
 *   Erlang Process --> Dirty Scheduler --> PyGILState_Ensure()
 *                                          PyThreadState_Swap(slot->tstate)
 *                                          [execute in subinterpreter]
 *                                          PyThreadState_Swap(saved)
 *                                          PyGILState_Release()
 *
 * Benefits over OWN_GIL thread-per-context model:
 * - No dedicated pthread per context (resource savings)
 * - No mutex/condvar dispatch overhead (direct execution)
 * - No term copying between environments (safe enif_make_* on dirty scheduler)
 * - ~4x better performance (400K vs 100K calls/sec)
 *
 * Lifecycle:
 * 1. Pool init: create N subinterpreters with shared GIL at Python startup
 * 2. Context creation: allocate slot from pool
 * 3. Each operation: swap to slot's tstate, execute, swap back
 * 4. Context destroy: release slot back to pool
 * 5. Pool shutdown: destroy all subinterpreters at Python finalization
 */

/* ============================================================================
 * Thread State Guard
 * ============================================================================
 *
 * For both worker mode and subinterpreter mode (shared GIL), we use
 * PyGILState_Ensure/Release. For subinterpreters, we additionally swap
 * to the subinterpreter's thread state.
 */

/**
 * @enum py_guard_mode_t
 * @brief Acquisition mode for py_context_guard_t
 */
typedef enum {
    /** @brief Failed to acquire - guard.acquired will be false */
    PY_GUARD_FAILED = 0,

    /** @brief Worker mode: Uses PyGILState_Ensure/Release (main interpreter) */
    PY_GUARD_WORKER,

    /** @brief OWN_GIL mode: dispatch to dedicated pthread with its own GIL */
    PY_GUARD_OWN_GIL
} py_guard_mode_t;

/**
 * @struct py_context_guard_t
 * @brief Thread state guard for context execution
 *
 * Use py_context_acquire() to obtain a guard before Python operations,
 * and py_context_release() when done.
 *
 * For worker mode: acquires GIL via PyGILState_Ensure
 * For subinterp mode: acquires GIL + swaps to subinterpreter's tstate
 */
typedef struct {
    /** @brief Context being guarded */
    py_context_t *ctx;

    /** @brief Acquisition mode - determines release behavior */
    py_guard_mode_t mode;

    /** @brief GIL state from PyGILState_Ensure */
    PyGILState_STATE gstate;

    /** @brief Saved thread state before swap (subinterp mode) */
    PyThreadState *saved_tstate;

    /** @brief Success flag: true if acquisition succeeded */
    bool acquired;
} py_context_guard_t;

/**
 * @brief Acquire thread state for context execution
 *
 * For worker mode: acquires GIL via PyGILState_Ensure
 * For subinterp mode: acquires GIL + swaps to pool slot's tstate
 *
 * @param ctx The context to acquire (may be NULL)
 * @return Guard structure with acquired=true on success, false on failure
 *
 * @note Caller MUST call py_context_release() when done with Python work
 *
 * @par Usage:
 * @code
 * py_context_guard_t guard = py_context_acquire(ctx);
 * if (!guard.acquired) {
 *     return make_error(env, "acquire_failed");
 * }
 * // ... do Python work using ctx->globals, ctx->locals ...
 * py_context_release(&guard);
 * @endcode
 */
static inline py_context_guard_t py_context_acquire(py_context_t *ctx) {
    py_context_guard_t guard = {
        .ctx = ctx,
        .mode = PY_GUARD_FAILED,
        .gstate = PyGILState_UNLOCKED,
        .saved_tstate = NULL,
        .acquired = false
    };

    if (ctx == NULL || atomic_load(&ctx->destroyed)) {
        return guard;
    }

    /* Acquire the GIL first */
    guard.gstate = PyGILState_Ensure();

    /* Worker mode: just use the GIL we acquired */
    guard.mode = PY_GUARD_WORKER;
    guard.acquired = true;
    return guard;
}

/**
 * @brief Release thread state acquired by py_context_acquire
 *
 * For worker mode: releases GIL via PyGILState_Release
 * For subinterp mode: swaps back to saved tstate + releases GIL
 *
 * @param guard Pointer to guard structure from py_context_acquire()
 *
 * @note Safe to call multiple times - checks acquired flag
 */
static inline void py_context_release(py_context_guard_t *guard) {
    if (!guard->acquired) {
        return;
    }

    /* Release the GIL */
    PyGILState_Release(guard->gstate);
    guard->acquired = false;
}

/**
 * @struct suspended_context_state_t
 * @brief State for a suspended Python context execution awaiting callback result
 *
 * Similar to suspended_state_t but for the process-per-context architecture.
 * When Python code in a context calls `erlang.call()`, execution is suspended
 * and this structure captures all state needed to resume after the context
 * process handles the callback inline.
 *
 * @par Key Difference from suspended_state_t:
 * This uses py_context_t (no mutex) instead of py_worker_t, and is designed
 * for the recursive receive pattern where the context process handles
 * callbacks inline without blocking.
 *
 * @see nif_context_resume
 */
typedef struct {
    /** @brief Context for replay */
    py_context_t *ctx;

    /** @brief Unique identifier for this callback */
    uint64_t callback_id;

    /* Callback invocation info */

    /** @brief Name of Erlang function being called */
    char *callback_func_name;

    /** @brief Length of callback_func_name */
    size_t callback_func_len;

    /** @brief Arguments passed to the callback */
    PyObject *callback_args;

    /* Original request context for replay */

    /** @brief Original request type (PY_REQ_CALL or PY_REQ_EVAL) */
    int request_type;

    /** @brief Original module name binary (for PY_REQ_CALL) */
    ErlNifBinary orig_module;

    /** @brief Original function name binary (for PY_REQ_CALL) */
    ErlNifBinary orig_func;

    /** @brief Original arguments (copied to orig_env) */
    ERL_NIF_TERM orig_args;

    /** @brief Original keyword arguments */
    ERL_NIF_TERM orig_kwargs;

    /** @brief Original code for eval replay (for PY_REQ_EVAL) */
    ErlNifBinary orig_code;

    /** @brief Original locals map for eval replay */
    ERL_NIF_TERM orig_locals;

    /** @brief Environment owning copied terms */
    ErlNifEnv *orig_env;

    /* Callback result (set before resume) */

    /** @brief Raw result data from Erlang callback (current callback) */
    unsigned char *result_data;

    /** @brief Length of result_data */
    size_t result_len;

    /** @brief Flag: result is available for replay */
    _Atomic bool has_result;

    /** @brief Flag: result represents an error */
    _Atomic bool is_error;

    /* Sequential callback support - stores all accumulated callback results */

    /** @brief Current callback result index for replay */
    size_t callback_result_index;

    /** @brief Number of cached callback results (from previous callbacks) */
    size_t num_callback_results;

    /** @brief Capacity of callback_results array */
    size_t callback_results_capacity;

    /** @brief Cached callback results array (grows with sequential callbacks) */
    struct {
        unsigned char *data;
        size_t len;
    } *callback_results;
} suspended_context_state_t;

/* ============================================================================
 * Inline Continuation Support
 * ============================================================================
 *
 * Inline continuations allow Python functions to chain directly via
 * enif_schedule_nif() without returning to Erlang messaging. This provides
 * significant performance improvements for tight loops that need to yield
 * to the scheduler.
 *
 * Flow comparison:
 *   schedule_py: Python -> ScheduleMarker -> NIF -> Erlang -> message -> NIF -> Python
 *   schedule_inline: Python -> InlineScheduleMarker -> NIF -> enif_schedule_nif -> NIF -> Python
 */

/**
 * @def MAX_INLINE_CONTINUATION_DEPTH
 * @brief Maximum depth for chained inline continuations
 *
 * Prevents stack overflow from unbounded recursion. When this depth is
 * exceeded, an error is returned to Erlang.
 */
#define MAX_INLINE_CONTINUATION_DEPTH 1000

/**
 * @struct inline_continuation_t
 * @brief State for an inline scheduled continuation
 *
 * Captures all state needed to continue Python execution via
 * enif_schedule_nif() without returning to Erlang messaging.
 */
typedef struct {
    /** @brief Context for execution */
    py_context_t *ctx;

    /** @brief Process-local environment (may be NULL) */
    void *local_env;  /* py_env_resource_t* - forward declared */

    /** @brief Module name to call */
    char *module_name;

    /** @brief Length of module_name */
    size_t module_len;

    /** @brief Function name to call */
    char *func_name;

    /** @brief Length of func_name */
    size_t func_len;

    /** @brief Arguments (Python tuple, owned reference) */
    PyObject *args;

    /** @brief Keyword arguments (Python dict or NULL, owned reference) */
    PyObject *kwargs;

    /** @brief Captured globals from caller's frame (owned reference) */
    PyObject *globals;

    /** @brief Captured locals from caller's frame (owned reference) */
    PyObject *locals;

    /** @brief Continuation depth (overflow protection) */
    uint32_t depth;

    /** @brief Interpreter ID (subinterpreter support) */
    uint32_t interp_id;
} inline_continuation_t;

/** @} */

/* ============================================================================
 * Global State Declarations
 * ============================================================================ */

/**
 * @defgroup globals Global State
 * @brief External declarations for global variables
 * @{
 */

/** @brief Resource type for py_worker_t */
extern ErlNifResourceType *WORKER_RESOURCE_TYPE;

/** @brief Resource type for py_object_t */
extern ErlNifResourceType *PYOBJ_RESOURCE_TYPE;

/* ASYNC_WORKER_RESOURCE_TYPE removed - async workers replaced by event loop model */

/** @brief Resource type for suspended_state_t */
extern ErlNifResourceType *SUSPENDED_STATE_RESOURCE_TYPE;

/** @brief Resource type for py_context_t (process-per-context) */
extern ErlNifResourceType *PY_CONTEXT_RESOURCE_TYPE;

/** @brief Resource type for py_ref_t (Python object with interp_id) */
extern ErlNifResourceType *PY_REF_RESOURCE_TYPE;

/** @brief Resource type for suspended_context_state_t (context suspension) */
extern ErlNifResourceType *PY_CONTEXT_SUSPENDED_RESOURCE_TYPE;

/** @brief Resource type for inline_continuation_t (inline scheduler continuation) */
extern ErlNifResourceType *INLINE_CONTINUATION_RESOURCE_TYPE;

/**
 * @struct py_env_resource_t
 * @brief Process-local Python environment (globals/locals)
 *
 * Stored in process dictionary as py_local_env. When the process exits,
 * Erlang GC drops the reference, triggering the destructor which frees
 * the Python dicts.
 */
typedef struct {
    /** @brief Global namespace dictionary */
    PyObject *globals;
    /** @brief Local namespace dictionary (same as globals for module-level execution) */
    PyObject *locals;
    /** @brief Interpreter ID that owns these dicts (0 = main interpreter) */
    int64_t interp_id;
} py_env_resource_t;

/** @brief Resource type for py_env_resource_t (process-local Python environment) */
extern ErlNifResourceType *PY_ENV_RESOURCE_TYPE;

/**
 * @struct py_shared_dict_t
 * @brief Process-scoped shared dictionary resource
 *
 * A SharedDict is owned by an Erlang process and automatically destroyed
 * when the process dies. Values are stored as pickled bytes for
 * cross-interpreter safety (each subinterpreter has its own object space).
 *
 * @note Thread-safe: mutex protects all dict operations
 * @note Process-monitored: destroyed flag set on owner process death
 */
typedef struct {
    /** @brief PID of the owning Erlang process */
    ErlNifPid owner_pid;

    /** @brief Monitor handle for process monitoring */
    ErlNifMonitor monitor;

    /** @brief Whether the monitor is currently active */
    bool monitor_active;

    /** @brief Mutex for thread-safe dict operations */
    pthread_mutex_t mutex;

    /** @brief Python dict storing keys -> pickled bytes */
    PyObject *dict;

    /** @brief Atomic flag: true if dict has been destroyed (owner died) */
    _Atomic bool destroyed;
} py_shared_dict_t;

/** @brief Resource type for py_shared_dict_t (process-scoped shared dictionary) */
extern ErlNifResourceType *PY_SHARED_DICT_RESOURCE_TYPE;

/** @brief Get the PY_ENV_RESOURCE_TYPE (for use by other modules) */
ErlNifResourceType *get_env_resource_type(void);

/** @brief Atomic counter for unique interpreter IDs */
extern _Atomic uint32_t g_context_id_counter;

/** @brief Atomic runtime state for thread-safe lifecycle management */
extern _Atomic py_runtime_state_t g_runtime_state;

/** @brief Main Python thread state (saved on init) */
extern PyThreadState *g_main_thread_state;

/** @brief Current execution mode */
extern py_execution_mode_t g_execution_mode;

/* Single executor state */

/** @brief Single executor thread handle */
extern pthread_t g_executor_thread;

/** @brief Single executor queue mutex */
extern pthread_mutex_t g_executor_mutex;

/** @brief Single executor queue condition */
extern pthread_cond_t g_executor_cond;

/** @brief Single executor queue head */
extern py_request_t *g_executor_queue_head;

/** @brief Single executor queue tail */
extern py_request_t *g_executor_queue_tail;

/** @brief Single executor running flag (atomic for thread-safe access) */
extern _Atomic bool g_executor_running;

/** @brief Single executor shutdown flag (atomic for thread-safe access) */
extern _Atomic bool g_executor_shutdown;

/** @brief Global counter for unique callback IDs */
extern _Atomic uint64_t g_callback_id_counter;

/** @brief Python exception class for suspension */
extern PyObject *SuspensionRequiredException;

/** @brief Python exception for dead/unreachable process */
extern PyObject *ProcessErrorException;

/** @brief Python type for opaque Erlang PIDs */
typedef struct { PyObject_HEAD; ErlNifPid pid; } ErlangPidObject;
extern PyTypeObject ErlangPidType;

/** @brief Python type for opaque Erlang references (stored as serialized binary) */
typedef struct {
    PyObject_HEAD;
    unsigned char *data;  /* Serialized reference data */
    size_t size;          /* Size of serialized data */
} ErlangRefObject;
extern PyTypeObject ErlangRefType;

/** @brief Python type for Erlang atoms (stored as string) */
typedef struct {
    PyObject_HEAD;
    char *name;           /* Atom name (null-terminated) */
} ErlangAtomObject;
extern PyTypeObject ErlangAtomType;

/** @brief Cached numpy.ndarray type for fast isinstance checks (NULL if numpy unavailable) */
extern PyObject *g_numpy_ndarray_type;

/* Thread-local state */

/** @brief Current worker for callback context (legacy) */
extern __thread py_worker_t *tl_current_worker;

/** @brief Current context for callback context (new process-per-context API) */
extern __thread py_context_t *tl_current_context;

/** @brief Current NIF environment for callbacks */
extern __thread ErlNifEnv *tl_callback_env;

/** @brief Current suspended state (for replay) */
extern __thread suspended_state_t *tl_current_suspended;

/** @brief Flag: suspension is allowed in current context */
extern __thread bool tl_allow_suspension;

/** @brief Flag: pending callback detected (checked before exception type) */
extern __thread bool tl_pending_callback;
extern __thread uint64_t tl_pending_callback_id;
extern __thread char *tl_pending_func_name;
extern __thread size_t tl_pending_func_name_len;
extern __thread PyObject *tl_pending_args;

/** @brief Timeout deadline (nanoseconds, monotonic clock) */
extern __thread uint64_t tl_timeout_deadline;

/* Logging/Tracing global state */

/** @brief PID of the Erlang process receiving log messages */
extern ErlNifPid g_log_receiver_pid;

/** @brief Flag: log receiver is registered */
extern volatile bool g_has_log_receiver;

/** @brief Minimum log level threshold (Python levelno) */
extern volatile int g_log_level_threshold;

/** @brief PID of the Erlang process receiving trace spans */
extern ErlNifPid g_trace_receiver_pid;

/** @brief Flag: trace receiver is registered */
extern volatile bool g_has_trace_receiver;

/** @brief Flag: timeout checking is enabled */
extern __thread bool tl_timeout_enabled;

/** @} */

/* ============================================================================
 * Atom Declarations
 * ============================================================================ */

/**
 * @defgroup atoms Erlang Atoms
 * @brief Pre-created atoms for efficient term construction
 * @{
 */

extern ERL_NIF_TERM ATOM_OK;             /**< @brief `ok` atom */
extern ERL_NIF_TERM ATOM_ERROR;          /**< @brief `error` atom */
extern ERL_NIF_TERM ATOM_TRUE;           /**< @brief `true` atom */
extern ERL_NIF_TERM ATOM_FALSE;          /**< @brief `false` atom */
extern ERL_NIF_TERM ATOM_NONE;           /**< @brief `none` atom (Python None) */
extern ERL_NIF_TERM ATOM_NIL;            /**< @brief `nil` atom (Elixir nil) */
extern ERL_NIF_TERM ATOM_UNDEFINED;      /**< @brief `undefined` atom */
extern ERL_NIF_TERM ATOM_NIF_NOT_LOADED; /**< @brief `nif_not_loaded` atom */
extern ERL_NIF_TERM ATOM_GENERATOR;      /**< @brief `generator` atom */
extern ERL_NIF_TERM ATOM_STOP_ITERATION; /**< @brief `stop_iteration` atom */
extern ERL_NIF_TERM ATOM_TIMEOUT;        /**< @brief `timeout` atom */
extern ERL_NIF_TERM ATOM_NAN;            /**< @brief `nan` atom (float NaN) */
extern ERL_NIF_TERM ATOM_INFINITY;       /**< @brief `infinity` atom */
extern ERL_NIF_TERM ATOM_NEG_INFINITY;   /**< @brief `neg_infinity` atom */
extern ERL_NIF_TERM ATOM_ERLANG_CALLBACK;/**< @brief `erlang_callback` atom */
extern ERL_NIF_TERM ATOM_ASYNC_RESULT;   /**< @brief `async_result` atom */
extern ERL_NIF_TERM ATOM_ASYNC_ERROR;    /**< @brief `async_error` atom */
extern ERL_NIF_TERM ATOM_SUSPENDED;      /**< @brief `suspended` atom */
extern ERL_NIF_TERM ATOM_SCHEDULE;       /**< @brief `schedule` atom */
extern ERL_NIF_TERM ATOM_MORE;           /**< @brief `more` atom (more tasks pending) */

/* Logging atoms */
extern ERL_NIF_TERM ATOM_PY_LOG;         /**< @brief `py_log` atom */
extern ERL_NIF_TERM ATOM_SPAN_START;     /**< @brief `span_start` atom */
extern ERL_NIF_TERM ATOM_SPAN_END;       /**< @brief `span_end` atom */
extern ERL_NIF_TERM ATOM_SPAN_EVENT;     /**< @brief `span_event` atom */

/** @} */

/* ============================================================================
 * Type Conversion Functions (py_convert.c)
 * ============================================================================ */

/**
 * @defgroup convert Type Conversion
 * @brief Functions for converting between Python and Erlang types
 * @{
 */

/**
 * @brief Convert a Python object to an Erlang term
 *
 * Recursively converts Python objects to their Erlang equivalents:
 *
 * | Python Type | Erlang Type |
 * |-------------|-------------|
 * | None | `none` atom |
 * | True/False | `true`/`false` atoms |
 * | int | integer |
 * | float | float (with NaN/Inf handling) |
 * | str | binary |
 * | bytes | binary |
 * | list | list |
 * | tuple | tuple |
 * | dict | map |
 * | numpy.ndarray | nested list |
 * | other | string representation |
 *
 * @param env NIF environment for term allocation
 * @param obj Python object to convert (borrowed reference)
 * @return Erlang term representing the Python object
 *
 * @note Does not consume a reference to obj
 * @note May return ATOM_ERROR on allocation failure
 */
extern ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj);

/**
 * @brief Convert an Erlang term to a Python object
 *
 * Recursively converts Erlang terms to Python objects:
 *
 * | Erlang Type | Python Type |
 * |-------------|-------------|
 * | atom `true` | True |
 * | atom `false` | False |
 * | atom `nil`/`none`/`undefined` | None |
 * | other atoms | str |
 * | integer | int |
 * | float | float |
 * | binary | str (UTF-8) |
 * | list | list |
 * | tuple | tuple |
 * | map | dict |
 * | resource (py_object) | unwrapped object |
 *
 * @param env NIF environment containing the term
 * @param term Erlang term to convert
 * @return New Python object (caller owns reference), or NULL on error
 *
 * @warning Caller must DECREF the returned object
 */
static PyObject *term_to_py(ErlNifEnv *env, ERL_NIF_TERM term);

/**
 * @brief Create an error tuple `{error, Reason}`
 *
 * @param env NIF environment for term allocation
 * @param reason Error reason as C string (converted to atom)
 * @return `{error, reason}` tuple
 */
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *reason);

/**
 * @brief Create an error tuple from the current Python exception
 *
 * Fetches the current Python exception, formats it as an Erlang
 * error tuple, and clears the Python error state.
 *
 * @param env NIF environment for term allocation
 * @return `{error, {ExceptionType, Message}}` tuple
 *
 * @note Always clears the Python exception state
 */
static ERL_NIF_TERM make_py_error(ErlNifEnv *env);

/**
 * @brief Convert ErlNifBinary to null-terminated C string
 *
 * Allocates a new buffer using enif_alloc(), copies the binary
 * data, and adds a null terminator.
 *
 * @param bin Binary to convert
 * @return Newly allocated null-terminated string, or NULL on failure
 *
 * @warning Caller must call enif_free() on the returned string
 */
static char *binary_to_string(const ErlNifBinary *bin) {
    char *str = enif_alloc(bin->size + 1);
    if (str != NULL) {
        memcpy(str, bin->data, bin->size);
        str[bin->size] = '\0';
    }
    return str;
}

/**
 * @brief Read exactly @p count bytes from a file descriptor with a deadline.
 *
 * Loops on partial reads and EINTR until @p count bytes are received or
 * the deadline expires. Uses a monotonic deadline (not a per-call
 * timeout) so retries cannot extend the wait indefinitely.
 *
 * @param fd         File descriptor to read from
 * @param buf        Buffer to read into
 * @param count      Number of bytes to read
 * @param timeout_ms Total timeout in milliseconds (0 = no timeout)
 *
 * @return Bytes read so far on timeout/EOF (0 to count), -1 on hard error.
 *         A return value < count with errno == ETIMEDOUT signals timeout;
 *         < count with errno == 0 signals clean EOF.
 *
 * @note Callers MUST treat any return < count as a desynchronised pipe.
 *       There is no in-band recovery: the leftover bytes (if any) of a
 *       partial frame stay in the pipe.
 */
static ssize_t read_with_timeout(int fd, void *buf, size_t count, int timeout_ms) {
    char *p = buf;
    size_t got = 0;
    struct timespec deadline = {0};
    bool have_deadline = (timeout_ms > 0);

    if (have_deadline) {
        clock_gettime(CLOCK_MONOTONIC, &deadline);
        deadline.tv_sec  += timeout_ms / 1000;
        deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec  += 1;
            deadline.tv_nsec -= 1000000000L;
        }
    }

    while (got < count) {
        if (have_deadline) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            long remain_ms =
                (deadline.tv_sec  - now.tv_sec)  * 1000L +
                (deadline.tv_nsec - now.tv_nsec) / 1000000L;
            if (remain_ms <= 0) {
                errno = ETIMEDOUT;
                return (ssize_t)got;
            }
            struct timeval tv;
            tv.tv_sec  = remain_ms / 1000;
            tv.tv_usec = (remain_ms % 1000) * 1000;
            fd_set fds;
            FD_ZERO(&fds);
            FD_SET(fd, &fds);
            int s = select(fd + 1, &fds, NULL, NULL, &tv);
            if (s < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            if (s == 0) {
                errno = ETIMEDOUT;
                return (ssize_t)got;
            }
        }
        ssize_t n = read(fd, p + got, count - got);
        if (n > 0) {
            got += (size_t)n;
            continue;
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        /* n == 0: writer closed pipe (clean EOF) */
        errno = 0;
        return (ssize_t)got;
    }
    return (ssize_t)got;
}

/**
 * @brief Read length-prefixed data from a file descriptor.
 *
 * Wire format: 4-byte length prefix (native endianness) followed by
 * @p len bytes of payload. Built on top of read_with_timeout, so
 * partial reads / EINTR are handled transparently.
 *
 * @param fd         File descriptor to read from
 * @param data_out   Pointer to store allocated data buffer
 * @param len_out    Pointer to store data length
 * @param timeout_ms Total timeout in milliseconds (0 = no timeout)
 *
 * @return 0 on success, -1 on read error/timeout, -2 on allocation failure
 *
 * @note Updated contract: on -1, the calling layer MUST treat the pipe as
 *       desynchronised. There is no resync attempt here. Callers (sync
 *       thread-callback, suspended-callback) react: poison the worker or
 *       destroy the context. On success with len > 0, caller must free
 *       *data_out via enif_free().
 */
static int read_length_prefixed_data(int fd, char **data_out, uint32_t *len_out, int timeout_ms) {
    uint32_t len;
    ssize_t n = read_with_timeout(fd, &len, sizeof(len), timeout_ms);
    if (n != (ssize_t)sizeof(len)) {
        return -1;
    }

    *data_out = NULL;
    *len_out = len;

    if (len > 0) {
        *data_out = enif_alloc(len);
        if (*data_out == NULL) {
            return -2;
        }

        n = read_with_timeout(fd, *data_out, len, timeout_ms);
        if (n != (ssize_t)len) {
            enif_free(*data_out);
            *data_out = NULL;
            return -1;
        }
    }

    return 0;
}

/**
 * @brief Write status codes for write_all_with_deadline().
 */
typedef enum {
    WRITE_OK      = 0,
    WRITE_TIMEOUT = -1,
    WRITE_ERROR   = -2
} write_result_t;

/**
 * @brief Write exactly @p count bytes to a (typically non-blocking) fd
 *        with a deadline.
 *
 * Loops on partial writes / EINTR / EAGAIN. On EAGAIN, uses select() for
 * write-readiness with the remaining deadline. Used by the thread-worker
 * write path to avoid pinning a dirty I/O scheduler thread on a stalled
 * Python reader.
 *
 * @param fd         File descriptor to write to
 * @param buf        Buffer to write
 * @param count      Number of bytes to write
 * @param timeout_ms Total timeout in milliseconds (0 = no timeout)
 *
 * @return WRITE_OK on full write, WRITE_TIMEOUT on deadline expiry,
 *         WRITE_ERROR on hard error (errno preserved).
 */
static write_result_t write_all_with_deadline(int fd, const void *buf,
                                              size_t count, int timeout_ms) {
    const char *p = buf;
    size_t sent = 0;
    struct timespec deadline = {0};
    bool have_deadline = (timeout_ms > 0);

    if (have_deadline) {
        clock_gettime(CLOCK_MONOTONIC, &deadline);
        deadline.tv_sec  += timeout_ms / 1000;
        deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec  += 1;
            deadline.tv_nsec -= 1000000000L;
        }
    }

    while (sent < count) {
        ssize_t n = write(fd, p + sent, count - sent);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                return WRITE_ERROR;
            }
            /* Pipe full: wait for write-readiness within the deadline. */
            if (!have_deadline) {
                return WRITE_ERROR;       /* would block, no deadline */
            }
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            long remain_ms =
                (deadline.tv_sec  - now.tv_sec)  * 1000L +
                (deadline.tv_nsec - now.tv_nsec) / 1000000L;
            if (remain_ms <= 0) return WRITE_TIMEOUT;
            struct timeval tv;
            tv.tv_sec  = remain_ms / 1000;
            tv.tv_usec = (remain_ms % 1000) * 1000;
            fd_set fds;
            FD_ZERO(&fds);
            FD_SET(fd, &fds);
            int s = select(fd + 1, NULL, &fds, NULL, &tv);
            if (s < 0) {
                if (errno == EINTR) continue;
                return WRITE_ERROR;
            }
            if (s == 0) return WRITE_TIMEOUT;
            /* fd writable; loop back. */
            continue;
        }
        /* n == 0: shouldn't happen for write(); treat as error. */
        return WRITE_ERROR;
    }
    return WRITE_OK;
}

/** @} */

/* ============================================================================
 * Timeout Support (py_exec.c)
 * ============================================================================ */

/**
 * @defgroup timeout Timeout Support
 * @brief Functions for Python execution timeout handling
 * @{
 */

/**
 * @brief Get current monotonic time in nanoseconds
 *
 * Uses CLOCK_MONOTONIC for timeout calculations to avoid issues
 * with system clock adjustments.
 *
 * @return Current time in nanoseconds
 */
static inline uint64_t get_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/**
 * @brief Start timeout monitoring for Python execution
 *
 * Sets up a trace callback that checks elapsed time and raises
 * `TimeoutError` if the deadline is exceeded.
 *
 * @param timeout_ms Timeout in milliseconds (0 = no timeout)
 *
 * @see stop_timeout
 */
static void start_timeout(unsigned long timeout_ms);

/**
 * @brief Stop timeout monitoring
 *
 * Removes the trace callback and resets timeout state.
 *
 * @see start_timeout
 */
static void stop_timeout(void);

/**
 * @brief Check if current Python exception is a timeout error
 *
 * @return true if TimeoutError is pending, false otherwise
 */
static bool check_timeout_error(void);

/** @} */

/* ============================================================================
 * Executor Functions (py_exec.c)
 * ============================================================================ */

/**
 * @defgroup exec Executor Functions
 * @brief Functions for the executor thread pool
 * @{
 */

/**
 * @brief Process a single request with GIL held
 *
 * Main dispatch function called by executor threads. Handles all
 * request types and stores results in the request structure.
 *
 * @param req Request to process (must not be NULL)
 *
 * @note Caller must hold the GIL
 * @note Sets req->result on completion
 */
static void process_request(py_request_t *req);

/**
 * @brief Submit a request to the executor
 *
 * Routes the request based on execution mode:
 * - FREE_THREADED: Execute directly
 * - MULTI_EXECUTOR: Route to executor pool
 * - SUBINTERP: Use single executor
 *
 * @param req Request to submit
 */
static int executor_enqueue(py_request_t *req);

/**
 * @brief Wait for a request to complete
 *
 * Blocks until the executor signals completion by setting
 * req->completed and signaling req->cond.
 *
 * @param req Request to wait for
 */
static void executor_wait(py_request_t *req);

/**
 * @brief Initialize a request structure
 *
 * Zeroes the structure and initializes mutex/condvar.
 *
 * @param req Request to initialize
 */
static void request_init(py_request_t *req);

/**
 * @brief Clean up a request structure
 *
 * Destroys mutex and condvar. Does not free the request itself.
 *
 * @param req Request to clean up
 */
static void request_cleanup(py_request_t *req);

/**
 * @brief Start the single executor thread
 *
 * Creates and starts the executor thread, waiting for it to
 * become ready before returning.
 *
 * @return 0 on success, -1 on failure
 */
static int executor_start(void);

/**
 * @brief Stop the single executor thread
 *
 * Sends shutdown request and waits for thread to terminate.
 */
static void executor_stop(void);

/** @} */

/* ============================================================================
 * Callback Functions (py_callback.c)
 * ============================================================================ */

/**
 * @defgroup cb Callback Functions
 * @brief Functions for Python-to-Erlang callback support
 * @{
 */

/**
 * @brief Create and register the 'erlang' Python module
 *
 * Creates a Python module named 'erlang' that provides:
 * - `erlang.call(name, *args)` - Call Erlang function
 * - `erlang.func_name(*args)` - Shorthand syntax
 * - `erlang.SuspensionRequired` - Internal exception
 *
 * @return 0 on success, -1 on failure
 */
static int create_erlang_module(void);

/**
 * @brief Implementation of `erlang.call(name, *args)`
 *
 * Allows Python code to call registered Erlang callback functions.
 * Uses suspension/resume mechanism to free dirty schedulers.
 *
 * @param self Module reference (unused)
 * @param args Tuple: (func_name, arg1, arg2, ...)
 * @return Result from Erlang, or NULL with exception set
 *
 * @see create_suspended_state
 */
static PyObject *erlang_call_impl(PyObject *self, PyObject *args);

/**
 * @brief Module __getattr__ for `erlang.func_name` syntax
 *
 * Enables calling Erlang functions as `erlang.my_func(args)`.
 *
 * @param module Module reference
 * @param name Attribute name
 * @return ErlangFunction wrapper object
 */
static PyObject *erlang_module_getattr(PyObject *module, PyObject *name);

/* async_event_loop_thread removed - replaced by event loop model */

/**
 * @brief Create suspended state for callback handling
 *
 * Captures all state needed to resume Python execution after
 * Erlang processes the callback.
 *
 * @param env NIF environment
 * @param exc_args Exception args tuple (callback_id, func_name, args)
 * @param req Original request being processed
 * @return New suspended state resource, or NULL on error
 */
static suspended_state_t *create_suspended_state(ErlNifEnv *env, PyObject *exc_args,
                                                  py_request_t *req);

/**
 * @brief Parse callback response from Erlang
 *
 * Response format: status_byte (0=ok, 1=error) + python_repr_string
 *
 * @param response_data Raw response bytes
 * @param response_len Length of response
 * @return Parsed Python object, or NULL with exception set
 */
static PyObject *parse_callback_response(unsigned char *response_data, size_t response_len);

/**
 * @brief Detect and set the execution mode
 *
 * Auto-detects based on Python version and build configuration.
 */
static void detect_execution_mode(void);

/** @} */

/* ============================================================================
 * Thread Worker Functions (py_thread_worker.c)
 * ============================================================================ */

/**
 * @defgroup thread_worker Thread Worker Support
 * @brief Functions for ThreadPoolExecutor thread support
 * @{
 */

/**
 * @brief Initialize the thread worker system
 *
 * Creates pthread key for automatic cleanup on thread exit.
 * Called during NIF initialization.
 *
 * @return 0 on success, -1 on failure
 */
static int thread_worker_init(void);

/**
 * @brief Clean up the thread worker system
 *
 * Releases all workers in the pool and destroys the pthread key.
 * Called during NIF unload.
 */
static void thread_worker_cleanup(void);

/**
 * @brief Set the thread coordinator PID
 *
 * @param pid PID of the coordinator process
 */
static void thread_worker_set_coordinator(ErlNifPid pid);

/**
 * @brief Execute an erlang.call() from a spawned thread
 *
 * Called when a Python thread that is NOT an executor thread
 * tries to call erlang.call().
 *
 * @param func_name Name of the Erlang function to call
 * @param func_name_len Length of function name
 * @param call_args Python tuple of arguments
 * @return Python result object, or NULL with exception set
 */
static PyObject *thread_worker_call(const char *func_name, size_t func_name_len,
                                    PyObject *call_args);

/** @} */

/* ============================================================================
 * Safe GIL/Thread State Acquisition
 * ============================================================================
 *
 * These helpers provide safe GIL acquisition that works across:
 * - Python 3.9-3.11 (standard GIL)
 * - Python 3.12-3.13 (stricter thread state checks)
 * - Python 3.13t free-threaded (no GIL, but thread attachment required)
 *
 * The pattern follows PyO3's Python::attach() approach:
 * 1. Check if already attached via PyGILState_Check()
 * 2. If not, use PyGILState_Ensure() to attach
 * 3. Track whether we acquired so we release correctly
 *
 * Per Python docs, PyGILState_Ensure/Release work in free-threaded builds
 * to manage thread attachment even without a GIL.
 */

/**
 * @defgroup gil_helpers Safe GIL/Thread State Helpers
 * @brief Thread-safe GIL acquisition for Python 3.12+ compatibility
 * @{
 */

/**
 * @brief Guard structure for safe GIL acquisition/release
 */
typedef struct {
    PyGILState_STATE gstate;  /**< GIL state from PyGILState_Ensure */
    int acquired;             /**< 1 if we acquired, 0 if already held */
} gil_guard_t;

/**
 * @brief Safely acquire the GIL/attach to Python runtime.
 *
 * This function is reentrant - if the current thread already holds the GIL
 * (or is attached in free-threaded builds), it returns immediately without
 * double-acquiring.
 *
 * @return Guard structure that must be passed to gil_release()
 */
static inline gil_guard_t gil_acquire(void) {
    gil_guard_t guard = {.gstate = PyGILState_UNLOCKED, .acquired = 0};

    /* Check if already attached to Python runtime */
    if (PyGILState_Check()) {
        return guard;
    }

    /* Attach to Python runtime (acquires GIL in GIL-enabled builds) */
    guard.gstate = PyGILState_Ensure();
    guard.acquired = 1;
    return guard;
}

/**
 * @brief Release the GIL/detach from Python runtime.
 *
 * Only releases if we actually acquired in gil_acquire().
 *
 * @param guard The guard structure returned by gil_acquire()
 */
static inline void gil_release(gil_guard_t guard) {
    if (guard.acquired) {
        PyGILState_Release(guard.gstate);
    }
}

/** @} */

/* ============================================================================
 * Debug Helpers
 * ============================================================================
 */

/**
 * @brief Log Python error details before clearing
 *
 * When PyErr_Occurred() is true, this logs the error type and message to stderr
 * with the given context string, then clears the error. Useful for debugging
 * when errors are being swallowed.
 *
 * @param context Short description of where the error occurred (e.g., "OWN_GIL init")
 */
static inline void log_and_clear_python_error(const char *context) {
    if (!PyErr_Occurred()) {
        return;
    }

    PyObject *type, *value, *traceback;
    PyErr_Fetch(&type, &value, &traceback);

    const char *type_name = "UnknownError";
    if (type != NULL && PyType_Check(type)) {
        type_name = ((PyTypeObject *)type)->tp_name;
    }

    const char *msg = "";
    PyObject *str_value = NULL;
    if (value != NULL) {
        str_value = PyObject_Str(value);
        if (str_value != NULL) {
            msg = PyUnicode_AsUTF8(str_value);
            if (msg == NULL) {
                msg = "(unable to convert error message)";
            }
        }
    }

    fprintf(stderr, "[Python Error] %s: %s: %s\n", context, type_name, msg);

    Py_XDECREF(str_value);
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(traceback);
}

/* ============================================================================
 * OWN_GIL Reactor Dispatch
 * ============================================================================
 * Functions for dispatching reactor operations to OWN_GIL threads.
 */

#ifdef HAVE_SUBINTERPRETERS

/**
 * @brief Dispatch reactor on_read_ready to OWN_GIL thread
 *
 * @param env Caller's NIF environment
 * @param ctx OWN_GIL context
 * @param fd File descriptor
 * @param buffer_ptr Reactor buffer resource (ownership transferred)
 * @return Result term
 */
ERL_NIF_TERM dispatch_reactor_read_to_owngil(ErlNifEnv *env, py_context_t *ctx,
                                              int fd, void *buffer_ptr);

/**
 * @brief Dispatch reactor on_write_ready to OWN_GIL thread
 *
 * @param env Caller's NIF environment
 * @param ctx OWN_GIL context
 * @param fd File descriptor
 * @return Result term
 */
ERL_NIF_TERM dispatch_reactor_write_to_owngil(ErlNifEnv *env, py_context_t *ctx,
                                               int fd);

/**
 * @brief Dispatch reactor init_connection to OWN_GIL thread
 *
 * @param env Caller's NIF environment
 * @param ctx OWN_GIL context
 * @param fd File descriptor
 * @param client_info Client info map term
 * @return Result term
 */
ERL_NIF_TERM dispatch_reactor_init_to_owngil(ErlNifEnv *env, py_context_t *ctx,
                                              int fd, ERL_NIF_TERM client_info);

#endif /* HAVE_SUBINTERPRETERS */

#endif /* PY_NIF_H */
