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
 * @file py_worker_pool.h
 * @brief Worker thread pool for Python operations
 * @author Benoit Chesneau
 *
 * @section overview Overview
 *
 * This module implements a general-purpose worker thread pool for ALL Python
 * calls (ASGI, WSGI, py:call, py:eval). Each worker has its own subinterpreter
 * (Python 3.12+) or dedicated GIL-holding thread, processing requests from a
 * shared queue.
 *
 * @section architecture Architecture
 *
 * ```
 * Erlang Processes          Lock-free Queue       Python Workers
 *      [P1]--enqueue--+     +---------+          +-------------------------+
 *      [P2]--enqueue--+---->| Request |<--poll---| Worker 0 (Subinterp+GIL)|
 *      [P3]--enqueue--+     |  Queue  |          |  - Holds GIL            |
 *      ...            |     | (MPSC)  |<--poll---| Worker 1 (Subinterp+GIL)|
 *      [PN]--enqueue--+     +---------+          +-------------------------+
 * ```
 *
 * @section benefits Key Benefits
 *
 * - No GIL acquire/release per request (workers hold GIL)
 * - Module/callable cached per worker (no reimport)
 * - True parallelism with subinterpreters (each has OWN_GIL)
 *
 * @section modes Python Mode Support
 *
 * | Mode | Python Version | Strategy |
 * |------|----------------|----------|
 * | FREE_THREADED | 3.13+ (no-GIL) | N workers, no GIL needed |
 * | SUBINTERP | 3.12+ | N subinterpreters, each OWN_GIL |
 * | FALLBACK | <3.12 | N workers share GIL, batching reduces overhead |
 */

#ifndef PY_WORKER_POOL_H
#define PY_WORKER_POOL_H

#include "py_nif.h"
#include "py_asgi.h"
#include "py_wsgi.h"

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @def POOL_MAX_WORKERS
 * @brief Maximum number of workers in the pool
 */
#define POOL_MAX_WORKERS 32

/**
 * @def POOL_QUEUE_SIZE
 * @brief Size of the request queue (power of 2 for efficient modulo)
 */
#define POOL_QUEUE_SIZE 4096

/**
 * @def POOL_DEFAULT_WORKERS
 * @brief Default number of workers (0 = use CPU count)
 */
#define POOL_DEFAULT_WORKERS 0

/* ============================================================================
 * Request Types
 * ============================================================================ */

/**
 * @enum py_pool_request_type_t
 * @brief Types of requests that can be submitted to the worker pool
 */
typedef enum {
    PY_POOL_REQ_CALL,      /**< py:call(Module, Func, Args) */
    PY_POOL_REQ_APPLY,     /**< py:apply(Module, Func, Args, Kwargs) */
    PY_POOL_REQ_EVAL,      /**< py:eval(Code) */
    PY_POOL_REQ_EXEC,      /**< py:exec(Code) */
    PY_POOL_REQ_ASGI,      /**< ASGI request */
    PY_POOL_REQ_WSGI,      /**< WSGI request */
    PY_POOL_REQ_SHUTDOWN   /**< Shutdown signal */
} py_pool_request_type_t;

/* ============================================================================
 * Request Structure
 * ============================================================================ */

/**
 * @struct py_pool_request_t
 * @brief Request submitted to the worker pool
 *
 * Contains all information needed to process a Python operation.
 * The result is sent back to the caller via enif_send().
 */
typedef struct py_pool_request {
    /** @brief Unique request ID for correlation */
    uint64_t request_id;

    /** @brief Type of operation to perform */
    py_pool_request_type_t type;

    /** @brief PID of the calling Erlang process */
    ErlNifPid caller_pid;

    /** @brief Environment for building result terms (thread-safe copy) */
    ErlNifEnv *msg_env;

    /* ========== CALL/APPLY parameters ========== */

    /** @brief Module name (heap-allocated, NULL-terminated) */
    char *module_name;

    /** @brief Function name (heap-allocated, NULL-terminated) */
    char *func_name;

    /** @brief Arguments list term (copied to msg_env) */
    ERL_NIF_TERM args_term;

    /** @brief Keyword arguments map term (copied to msg_env, optional) */
    ERL_NIF_TERM kwargs_term;

    /* ========== EVAL/EXEC parameters ========== */

    /** @brief Python code to evaluate/execute (heap-allocated, NULL-terminated) */
    char *code;

    /** @brief Local variables for eval (copied to msg_env) */
    ERL_NIF_TERM locals_term;

    /* ========== ASGI/WSGI parameters ========== */

    /** @brief Runner module name for ASGI (heap-allocated) */
    char *runner_name;

    /** @brief ASGI callable name (heap-allocated) */
    char *callable_name;

    /** @brief ASGI scope term (copied to msg_env) */
    ERL_NIF_TERM scope_term;

    /** @brief Request body binary data */
    unsigned char *body_data;

    /** @brief Body data length */
    size_t body_len;

    /* ========== WSGI-specific parameters ========== */

    /** @brief WSGI environ term (copied to msg_env) */
    ERL_NIF_TERM environ_term;

    /* ========== Timeout ========== */

    /** @brief Timeout in milliseconds (0 = no timeout) */
    unsigned long timeout_ms;

    /* ========== Queue linkage ========== */

    /** @brief Next request in queue (for linked list) */
    struct py_pool_request *next;
} py_pool_request_t;

/* ============================================================================
 * Worker Structure
 * ============================================================================ */

/**
 * @struct py_pool_worker_t
 * @brief Single worker thread in the pool
 *
 * Each worker runs in its own thread and optionally has its own
 * subinterpreter (Python 3.12+) for true parallelism.
 */
typedef struct {
    /** @brief Worker thread handle */
    pthread_t thread;

    /** @brief Worker ID (0 to num_workers-1) */
    int worker_id;

    /** @brief Flag: worker is running */
    volatile bool running;

    /** @brief Flag: worker should shut down */
    volatile bool shutdown;

#ifdef HAVE_SUBINTERPRETERS
    /** @brief Python interpreter for this worker */
    PyInterpreterState *interp;

    /** @brief Thread state in this interpreter */
    PyThreadState *tstate;
#endif

    /* ========== Cached state per worker ========== */

    /** @brief Module cache (Dict: module_name -> PyModule) */
    PyObject *module_cache;

    /** @brief Global namespace for eval/exec */
    PyObject *globals;

    /** @brief Local namespace for eval/exec */
    PyObject *locals;

    /* ========== ASGI/WSGI state ========== */

    /** @brief Per-worker ASGI state (interned keys, etc.) */
    asgi_interp_state_t *asgi_state;

    /* ========== Statistics ========== */

    /** @brief Total requests processed by this worker */
    _Atomic uint64_t requests_processed;

    /** @brief Total processing time in nanoseconds */
    _Atomic uint64_t total_processing_ns;
} py_pool_worker_t;

/* ============================================================================
 * Request Queue Structure
 * ============================================================================ */

/**
 * @struct py_pool_queue_t
 * @brief MPSC (Multi-Producer Single-Consumer) queue for requests
 *
 * Uses a simple linked list with mutex protection. Workers dequeue
 * using condition variable waits.
 */
typedef struct {
    /** @brief Queue head (oldest request) */
    py_pool_request_t *head;

    /** @brief Queue tail (newest request) */
    py_pool_request_t *tail;

    /** @brief Number of pending requests */
    _Atomic uint64_t pending_count;

    /** @brief Total requests enqueued */
    _Atomic uint64_t total_enqueued;

    /** @brief Mutex protecting the queue */
    pthread_mutex_t mutex;

    /** @brief Condition variable for worker notification */
    pthread_cond_t cond;
} py_pool_queue_t;

/* ============================================================================
 * Worker Pool Structure
 * ============================================================================ */

/**
 * @struct py_worker_pool_t
 * @brief The main worker pool structure
 */
typedef struct {
    /** @brief Array of workers */
    py_pool_worker_t workers[POOL_MAX_WORKERS];

    /** @brief Number of active workers */
    int num_workers;

    /** @brief Request queue */
    py_pool_queue_t queue;

    /** @brief Flag: pool is initialized */
    volatile bool initialized;

    /** @brief Flag: pool is shutting down */
    volatile bool shutting_down;

    /** @brief Request ID counter */
    _Atomic uint64_t request_id_counter;

    /** @brief Mode: use subinterpreters */
    bool use_subinterpreters;

    /** @brief Mode: free-threaded Python */
    bool free_threaded;
} py_worker_pool_t;

/* ============================================================================
 * Global Pool Instance
 * ============================================================================ */

/** @brief Global worker pool instance */
extern py_worker_pool_t g_pool;

/* ============================================================================
 * Pool Lifecycle Functions
 * ============================================================================ */

/**
 * @brief Initialize the worker pool
 *
 * Creates and starts num_workers worker threads. If num_workers is 0,
 * uses the number of CPU cores.
 *
 * @param num_workers Number of workers (0 = auto-detect CPU count)
 * @return 0 on success, -1 on failure
 */
static int py_pool_init(int num_workers);

/**
 * @brief Shut down the worker pool
 *
 * Signals all workers to stop and waits for them to terminate.
 * Processes any remaining requests with error responses.
 */
static void py_pool_shutdown(void);

/**
 * @brief Check if pool is initialized
 *
 * @return true if pool is ready to accept requests
 */
static bool py_pool_is_initialized(void);

/* ============================================================================
 * Request Submission Functions
 * ============================================================================ */

/**
 * @brief Submit a request to the pool
 *
 * Thread-safe enqueue operation. The request is processed by an
 * available worker and the result is sent to caller_pid.
 *
 * @param req Request to submit (ownership transferred to pool)
 * @return 0 on success, -1 if pool not initialized
 */
static int py_pool_enqueue(py_pool_request_t *req);

/**
 * @brief Create a new pool request
 *
 * Allocates and initializes a request structure.
 *
 * @param type Request type
 * @param caller_pid Calling process PID
 * @return New request, or NULL on allocation failure
 */
static py_pool_request_t *py_pool_request_new(py_pool_request_type_t type,
                                               ErlNifPid caller_pid);

/**
 * @brief Free a pool request
 *
 * Releases all resources associated with the request.
 *
 * @param req Request to free
 */
static void py_pool_request_free(py_pool_request_t *req);

/* ============================================================================
 * Worker Functions
 * ============================================================================ */

/**
 * @brief Worker thread main function
 *
 * Entry point for worker threads. Processes requests until shutdown.
 *
 * @param arg Pointer to py_pool_worker_t
 * @return NULL
 */
static void *py_pool_worker_thread(void *arg);

/**
 * @brief Process a single request
 *
 * Dispatches based on request type and sends result to caller.
 *
 * @param worker Worker processing the request
 * @param req Request to process
 */
static void py_pool_process_request(py_pool_worker_t *worker,
                                     py_pool_request_t *req);

/**
 * @brief Send response to caller
 *
 * Uses enif_send() to send result back to calling process.
 *
 * @param req Request with caller info
 * @param result Result term to send
 */
static void py_pool_send_response(py_pool_request_t *req, ERL_NIF_TERM result);

/* ============================================================================
 * Request Processing Functions
 * ============================================================================ */

/**
 * @brief Process CALL request
 *
 * @param worker Worker processing request
 * @param req Request with module, func, args
 * @return Result term
 */
static ERL_NIF_TERM py_pool_process_call(py_pool_worker_t *worker,
                                          py_pool_request_t *req);

/**
 * @brief Process APPLY request
 *
 * @param worker Worker processing request
 * @param req Request with module, func, args, kwargs
 * @return Result term
 */
static ERL_NIF_TERM py_pool_process_apply(py_pool_worker_t *worker,
                                           py_pool_request_t *req);

/**
 * @brief Process EVAL request
 *
 * @param worker Worker processing request
 * @param req Request with code
 * @return Result term
 */
static ERL_NIF_TERM py_pool_process_eval(py_pool_worker_t *worker,
                                          py_pool_request_t *req);

/**
 * @brief Process EXEC request
 *
 * @param worker Worker processing request
 * @param req Request with code
 * @return Result term
 */
static ERL_NIF_TERM py_pool_process_exec(py_pool_worker_t *worker,
                                          py_pool_request_t *req);

/**
 * @brief Process ASGI request
 *
 * @param worker Worker processing request
 * @param req Request with runner, callable, scope, body
 * @return Result term
 */
static ERL_NIF_TERM py_pool_process_asgi(py_pool_worker_t *worker,
                                          py_pool_request_t *req);

/**
 * @brief Process WSGI request
 *
 * @param worker Worker processing request
 * @param req Request with module, callable, environ
 * @return Result term
 */
static ERL_NIF_TERM py_pool_process_wsgi(py_pool_worker_t *worker,
                                          py_pool_request_t *req);

/* ============================================================================
 * Module Caching
 * ============================================================================ */

/**
 * @brief Get or import a Python module
 *
 * Checks the worker's module cache first, imports if not cached.
 *
 * @param worker Worker with module cache
 * @param module_name Module name to get
 * @return Borrowed reference to module, or NULL on error
 */
static PyObject *py_pool_get_module(py_pool_worker_t *worker,
                                     const char *module_name);

/**
 * @brief Clear module cache for a worker
 *
 * @param worker Worker to clear cache for
 */
static void py_pool_clear_module_cache(py_pool_worker_t *worker);

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Pool statistics structure
 */
typedef struct {
    int num_workers;
    bool initialized;
    bool use_subinterpreters;
    bool free_threaded;
    uint64_t pending_count;
    uint64_t total_enqueued;
    struct {
        uint64_t requests_processed;
        uint64_t total_processing_ns;
    } worker_stats[POOL_MAX_WORKERS];
} py_pool_stats_t;

/**
 * @brief Get pool statistics
 *
 * @param stats Output structure for statistics
 */
static void py_pool_get_stats(py_pool_stats_t *stats);

/* ============================================================================
 * NIF Functions
 * ============================================================================ */

/**
 * @brief NIF: Start the worker pool
 *
 * py_nif:pool_start(NumWorkers) -> ok | {error, Reason}
 */
static ERL_NIF_TERM nif_pool_start(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]);

/**
 * @brief NIF: Stop the worker pool
 *
 * py_nif:pool_stop() -> ok
 */
static ERL_NIF_TERM nif_pool_stop(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]);

/**
 * @brief NIF: Submit a request to the pool
 *
 * py_nif:pool_submit(Type, Arg1, Arg2, Arg3, Arg4) -> {ok, RequestId} | {error, Reason}
 */
static ERL_NIF_TERM nif_pool_submit(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]);

/**
 * @brief NIF: Get pool statistics
 *
 * py_nif:pool_stats() -> StatsMap
 */
static ERL_NIF_TERM nif_pool_stats(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]);

#endif /* PY_WORKER_POOL_H */
