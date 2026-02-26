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

/** @} */

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
     * @brief Sub-interpreter mode (Python 3.12+)
     *
     * Each sub-interpreter has its own GIL, allowing parallel execution
     * across interpreters while maintaining GIL semantics within each.
     */
    PY_MODE_SUBINTERP,

    /**
     * @brief Multi-executor mode (all Python versions)
     *
     * Multiple executor threads share the GIL using a work-stealing
     * pattern. This is the fallback mode for older Python versions.
     */
    PY_MODE_MULTI_EXECUTOR
} py_execution_mode_t;

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

/**
 * @struct async_pending_t
 * @brief Represents a pending asynchronous Python operation
 *
 * Used to track asyncio coroutines submitted to the event loop.
 * Forms a linked list for efficient queue management.
 */
typedef struct async_pending {
    /** @brief Unique identifier for this async operation */
    uint64_t id;

    /** @brief Python Future object from `asyncio.run_coroutine_threadsafe` */
    PyObject *future;

    /** @brief PID of the Erlang process awaiting the result */
    ErlNifPid caller;

    /** @brief Next pending operation in the queue */
    struct async_pending *next;
} async_pending_t;

/**
 * @struct py_async_worker_t
 * @brief Async worker managing an asyncio event loop
 *
 * Provides support for Python async/await operations by running
 * an asyncio event loop in a dedicated background thread.
 *
 * @see nif_async_worker_new
 * @see nif_async_call
 */
typedef struct {
    /** @brief Background thread running the event loop */
    pthread_t loop_thread;

    /** @brief Python asyncio event loop object */
    PyObject *event_loop;

    /**
     * @brief Notification pipe for waking the event loop
     *
     * - `notify_pipe[0]` - Read end (event loop monitors)
     * - `notify_pipe[1]` - Write end (main thread signals)
     */
    int notify_pipe[2];

    /** @brief Flag indicating the event loop is running */
    volatile bool loop_running;

    /** @brief Flag to signal shutdown */
    volatile bool shutdown;

    /** @brief Mutex protecting the pending queue */
    pthread_mutex_t queue_mutex;

    /** @brief Head of pending operations queue */
    async_pending_t *pending_head;

    /** @brief Tail of pending operations queue */
    async_pending_t *pending_tail;

    /** @brief Environment for sending async result messages */
    ErlNifEnv *msg_env;
} py_async_worker_t;

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
} py_object_t;

/** @} */

/* ============================================================================
 * Request Types and Structures
 * ============================================================================ */

/**
 * @defgroup requests Request Handling
 * @brief Structures for executor request processing
 * @{
 */

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
    volatile bool has_result;

    /** @brief Flag: result represents an error */
    volatile bool is_error;

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

#ifdef HAVE_SUBINTERPRETERS
/**
 * @struct py_subinterp_worker_t
 * @brief Worker running in an isolated sub-interpreter
 *
 * Sub-interpreters provide true isolation with their own GIL,
 * enabling parallel Python execution on Python 3.12+.
 *
 * The mutex ensures thread-safe access when multiple dirty scheduler
 * threads attempt to use the same worker concurrently.
 *
 * @note Only available when compiled with Python 3.12+
 *
 * @see nif_subinterp_worker_new
 * @see nif_subinterp_call
 */
typedef struct {
    /** @brief Mutex for thread-safe access from multiple dirty schedulers */
    pthread_mutex_t mutex;

    /** @brief Python interpreter state */
    PyInterpreterState *interp;

    /** @brief Thread state for this interpreter */
    PyThreadState *tstate;

    /** @brief Global namespace dictionary */
    PyObject *globals;

    /** @brief Local namespace dictionary */
    PyObject *locals;
} py_subinterp_worker_t;
#endif

/** @} */

/* ============================================================================
 * Executor Pool
 * ============================================================================ */

/**
 * @defgroup executor Executor Pool
 * @brief Multi-executor thread pool for GIL management
 * @{
 */

/**
 * @def MAX_EXECUTORS
 * @brief Maximum number of executor threads in the pool
 */
#define MAX_EXECUTORS 16

/**
 * @struct executor_t
 * @brief Single executor thread in the multi-executor pool
 *
 * Each executor has its own request queue and processes requests
 * independently. The GIL is acquired/released around queue operations.
 */
typedef struct {
    /** @brief Executor thread handle */
    pthread_t thread;

    /** @brief Mutex protecting the request queue */
    pthread_mutex_t mutex;

    /** @brief Condition variable for queue signaling */
    pthread_cond_t cond;

    /** @brief Head of request queue */
    struct py_request *queue_head;

    /** @brief Tail of request queue */
    struct py_request *queue_tail;

    /** @brief Flag: executor is running */
    volatile bool running;

    /** @brief Flag: executor should shut down */
    volatile bool shutdown;

    /** @brief Executor ID (0 to MAX_EXECUTORS-1) */
    int id;
} executor_t;

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

/** @brief Resource type for py_async_worker_t */
extern ErlNifResourceType *ASYNC_WORKER_RESOURCE_TYPE;

/** @brief Resource type for suspended_state_t */
extern ErlNifResourceType *SUSPENDED_STATE_RESOURCE_TYPE;

#ifdef HAVE_SUBINTERPRETERS
/** @brief Resource type for py_subinterp_worker_t */
extern ErlNifResourceType *SUBINTERP_WORKER_RESOURCE_TYPE;
#endif

/** @brief Flag: Python interpreter is initialized */
extern bool g_python_initialized;

/** @brief Main Python thread state (saved on init) */
extern PyThreadState *g_main_thread_state;

/** @brief Current execution mode */
extern py_execution_mode_t g_execution_mode;

/** @brief Number of active executors */
extern int g_num_executors;

/** @brief Multi-executor pool array */
extern executor_t g_executors[MAX_EXECUTORS];

/** @brief Round-robin counter for executor selection */
extern _Atomic int g_next_executor;

/** @brief Flag: multi-executor pool is initialized */
extern bool g_multi_executor_initialized;

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

/** @brief Single executor running flag */
extern volatile bool g_executor_running;

/** @brief Single executor shutdown flag */
extern volatile bool g_executor_shutdown;

/** @brief Global counter for unique callback IDs */
extern _Atomic uint64_t g_callback_id_counter;

/** @brief Python exception class for suspension */
extern PyObject *SuspensionRequiredException;

/** @brief Cached numpy.ndarray type for fast isinstance checks (NULL if numpy unavailable) */
extern PyObject *g_numpy_ndarray_type;

/* Thread-local state */

/** @brief Current worker for callback context */
extern __thread py_worker_t *tl_current_worker;

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
static ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj);

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
 * @brief Read from a file descriptor with optional timeout
 *
 * Uses select() to implement a timeout on blocking read operations.
 * This prevents indefinite blocking on pipe reads.
 *
 * @param fd         File descriptor to read from
 * @param buf        Buffer to read into
 * @param count      Number of bytes to read
 * @param timeout_ms Timeout in milliseconds (0 = no timeout, wait indefinitely)
 *
 * @return Number of bytes read on success, 0 on timeout, -1 on error
 *
 * @note On timeout, errno is set to ETIMEDOUT
 */
static ssize_t read_with_timeout(int fd, void *buf, size_t count, int timeout_ms) {
    if (timeout_ms > 0) {
        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;

        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(fd, &fds);

        int ret = select(fd + 1, &fds, NULL, NULL, &tv);
        if (ret < 0) {
            return -1;  /* select error */
        }
        if (ret == 0) {
            errno = ETIMEDOUT;
            return 0;  /* timeout */
        }
    }

    return read(fd, buf, count);
}

/**
 * @brief Read length-prefixed data from a file descriptor
 *
 * Reads a 4-byte length prefix followed by the data payload.
 * Uses read_with_timeout for optional timeout support.
 *
 * @param fd         File descriptor to read from
 * @param data_out   Pointer to store allocated data buffer (caller must free with enif_free)
 * @param len_out    Pointer to store data length
 * @param timeout_ms Timeout in milliseconds (0 = no timeout)
 *
 * @return 0 on success, -1 on read error/timeout, -2 on allocation failure
 *
 * @note On success with len > 0, caller must call enif_free(*data_out)
 */
static int read_length_prefixed_data(int fd, char **data_out, uint32_t *len_out, int timeout_ms) {
    uint32_t len;
    ssize_t n = read_with_timeout(fd, &len, sizeof(len), timeout_ms);
    if (n != sizeof(len)) {
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
static void executor_enqueue(py_request_t *req);

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

/**
 * @brief Main function for multi-executor threads
 *
 * Thread entry point for executor pool threads. Processes
 * requests from its queue until shutdown.
 *
 * @param arg Pointer to executor_t for this thread
 * @return NULL
 */
static void *multi_executor_thread_main(void *arg);

/**
 * @brief Start the multi-executor pool
 *
 * Creates and starts num_executors threads.
 *
 * @param num_executors Number of executors (capped at MAX_EXECUTORS)
 * @return 0 on success, -1 on failure
 */
static int multi_executor_start(int num_executors);

/**
 * @brief Stop the multi-executor pool
 *
 * Signals shutdown and waits for all executor threads.
 */
static void multi_executor_stop(void);

/**
 * @brief Select an executor using round-robin
 *
 * @return Executor index (0 to g_num_executors-1)
 */
static int select_executor(void);

/**
 * @brief Submit a request to a specific executor
 *
 * @param exec_id Executor index
 * @param req Request to submit
 */
static void multi_executor_enqueue(int exec_id, struct py_request *req);

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

/**
 * @brief Background thread running asyncio event loop
 *
 * Manages async Python operations submitted via async_call.
 *
 * @param arg Pointer to py_async_worker_t
 * @return NULL
 */
static void *async_event_loop_thread(void *arg);

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

#endif /* PY_NIF_H */
