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
 * @file py_subinterp_thread.h
 * @brief OWN_GIL subinterpreter thread pool for true parallelism
 * @author Benoit Chesneau
 *
 * This module implements a pthread pool where each thread owns a Python
 * subinterpreter with OWN_GIL. This provides true parallelism for CPU-bound
 * Python code since each subinterpreter has its own GIL.
 *
 * Architecture:
 *   - N pthreads created at startup, each with an OWN_GIL subinterpreter
 *   - Each worker has command/result pipes for IPC
 *   - Erlang handles (py_subinterp_handle_t) bind to workers round-robin
 *   - Multiple handles can share a worker (serialized via dispatch_mutex)
 *   - Each handle has isolated namespace within its worker
 *
 * Use cases:
 *   - CPU-bound parallel Python execution (NumPy, ML inference)
 *   - Isolated execution environments
 *   - When true parallelism is needed beyond shared-GIL pool model
 */

#ifndef PY_SUBINTERP_THREAD_H
#define PY_SUBINTERP_THREAD_H

#include <Python.h>
#include <erl_nif.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#ifdef HAVE_SUBINTERPRETERS

/* ============================================================================
 * Constants
 * ============================================================================ */

/** @brief Maximum workers in the thread pool */
#define SUBINTERP_THREAD_POOL_MAX 32

/** @brief Default number of workers */
#define SUBINTERP_THREAD_POOL_DEFAULT 8

/** @brief Magic number for protocol validation: "PYOG" */
#define OWNGIL_MAGIC 0x50594F47

/** @brief Protocol version */
#define OWNGIL_PROTOCOL_VERSION 1

/* ============================================================================
 * Protocol Message Types
 * ============================================================================ */

/**
 * @enum owngil_msg_type_t
 * @brief Message types for IPC protocol
 */
typedef enum {
    MSG_REQUEST         = 0x01,  /**< NIF -> pthread: execute request */
    MSG_RESPONSE        = 0x02,  /**< pthread -> NIF: success result */
    MSG_ERROR           = 0x03,  /**< pthread -> NIF: error result */
    MSG_CALLBACK        = 0x04,  /**< pthread -> NIF: erlang.call() request */
    MSG_CALLBACK_RESULT = 0x05,  /**< NIF -> pthread: erlang.call() response */
} owngil_msg_type_t;

/**
 * @enum owngil_req_type_t
 * @brief Request types for execution
 */
typedef enum {
    REQ_CALL            = 1,   /**< Sync call - wait for response on result_pipe */
    REQ_EVAL            = 2,   /**< Sync eval */
    REQ_EXEC            = 3,   /**< Sync exec */
    REQ_CAST            = 4,   /**< Fire-and-forget - no response */
    REQ_ASYNC_CALL      = 5,   /**< Async - response via erlang.send() */
    REQ_CREATE_NS       = 10,  /**< Create namespace for handle */
    REQ_DESTROY_NS      = 11,  /**< Destroy namespace for handle */
    REQ_SHUTDOWN        = 99,  /**< Shutdown the worker */
} owngil_req_type_t;

/* ============================================================================
 * Protocol Header
 * ============================================================================ */

/**
 * @struct owngil_header_t
 * @brief Wire protocol header (28 bytes, packed)
 *
 * All multi-byte fields are in host byte order (same machine IPC).
 */
typedef struct __attribute__((packed)) {
    uint32_t magic;          /**< 0x50594F47 "PYOG" */
    uint8_t version;         /**< Protocol version (1) */
    uint8_t msg_type;        /**< owngil_msg_type_t */
    uint8_t req_type;        /**< owngil_req_type_t */
    uint8_t reserved;        /**< Padding for alignment */
    uint64_t request_id;     /**< Unique ID for correlation */
    uint64_t handle_id;      /**< Handle ID for namespace lookup */
    uint32_t payload_len;    /**< ETF payload length in bytes */
} owngil_header_t;

/* ============================================================================
 * Worker State
 * ============================================================================ */

/**
 * @struct subinterp_namespace_t
 * @brief Namespace for a handle within a worker
 */
typedef struct {
    uint64_t handle_id;          /**< Handle ID (key) */
    PyObject *globals;           /**< Global namespace dict */
    PyObject *locals;            /**< Local namespace dict */
    PyObject *module_cache;      /**< Module cache dict */
    PyObject *asyncio_loop;      /**< Asyncio event loop for this namespace */
    ErlNifPid owner_pid;         /**< Owner PID for routing callbacks */
    bool initialized;            /**< Whether namespace is ready */
} subinterp_namespace_t;

/** @brief Maximum namespaces per worker */
#define MAX_NAMESPACES_PER_WORKER 64

/**
 * @struct subinterp_thread_worker_t
 * @brief A worker thread with OWN_GIL subinterpreter
 */
typedef struct {
    /* Thread identity */
    pthread_t thread;            /**< Worker pthread */
    int worker_id;               /**< Worker index (0 to N-1) */

    /* Python state - owned exclusively by this thread */
    PyInterpreterState *interp;  /**< Python interpreter state */
    PyThreadState *tstate;       /**< Thread state for this worker */

    /* Asyncio support */
    PyObject *asyncio_module;    /**< Cached asyncio import */
    PyObject *asyncio_loop;      /**< Worker's asyncio event loop */

    /* Namespaces for handles bound to this worker */
    subinterp_namespace_t namespaces[MAX_NAMESPACES_PER_WORKER];
    int num_namespaces;          /**< Number of active namespaces */
    pthread_mutex_t ns_mutex;    /**< Protects namespace array */

    /* Communication pipes */
    int cmd_pipe[2];             /**< [0]=read (pthread), [1]=write (NIF) */
    int result_pipe[2];          /**< [0]=read (NIF), [1]=write (pthread) */

    /* Lifecycle state */
    _Atomic bool running;              /**< Worker is running */
    _Atomic bool shutdown_requested;   /**< Shutdown signal sent */
    _Atomic bool initialized;          /**< Worker fully initialized */

    /* Dispatch lock - blocks callers when worker busy (like dirty scheduler) */
    pthread_mutex_t dispatch_mutex;

    /* Statistics */
    _Atomic uint64_t requests_processed;  /**< Total requests handled */
    _Atomic uint64_t errors_count;        /**< Total errors */
} subinterp_thread_worker_t;

/**
 * @struct subinterp_thread_pool_t
 * @brief Global thread pool state
 */
typedef struct {
    subinterp_thread_worker_t workers[SUBINTERP_THREAD_POOL_MAX];
    int num_workers;                /**< Configured worker count */
    _Atomic bool initialized;       /**< Pool is ready */
    _Atomic uint64_t next_worker;   /**< Round-robin counter */
    _Atomic uint64_t next_handle_id; /**< Counter for unique handle IDs */
    _Atomic uint64_t next_request_id; /**< Counter for request IDs */
} subinterp_thread_pool_t;

/* ============================================================================
 * Handle Resource (Erlang side)
 * ============================================================================ */

/**
 * @struct py_subinterp_handle_t
 * @brief Erlang resource representing a subinterpreter handle
 *
 * A handle is bound to a specific worker at creation and has its own
 * isolated namespace within that worker.
 */
typedef struct {
    int worker_id;               /**< Bound worker index (fixed at creation) */
    uint64_t handle_id;          /**< Unique ID for namespace lookup */
    _Atomic bool destroyed;      /**< Handle has been destroyed */
} py_subinterp_handle_t;

/* ============================================================================
 * Pool Management API
 * ============================================================================ */

/**
 * @brief Initialize the OWN_GIL thread pool
 *
 * Creates num_workers pthreads, each with an OWN_GIL subinterpreter.
 * Must be called after Python is initialized.
 *
 * @param num_workers Number of workers (0 = default, capped at MAX)
 * @return 0 on success, -1 on failure
 */
int subinterp_thread_pool_init(int num_workers);

/**
 * @brief Shutdown the thread pool
 *
 * Signals all workers to shut down and waits for threads to exit.
 * Cleans up all subinterpreters.
 */
void subinterp_thread_pool_shutdown(void);

/**
 * @brief Check if pool is initialized
 *
 * @return true if pool is ready for use
 */
bool subinterp_thread_pool_is_ready(void);

/**
 * @brief Get pool statistics
 *
 * @param num_workers Output: number of workers
 * @param total_requests Output: total requests processed
 * @param total_errors Output: total errors
 */
void subinterp_thread_pool_stats(int *num_workers, uint64_t *total_requests,
                                  uint64_t *total_errors);

/* ============================================================================
 * Handle Management API
 * ============================================================================ */

/**
 * @brief Create a new subinterpreter handle
 *
 * Allocates a handle bound to a worker (round-robin selection) and
 * creates a namespace for it within that worker.
 *
 * @param handle Output: handle structure to initialize
 * @return 0 on success, -1 on failure
 */
int subinterp_thread_handle_create(py_subinterp_handle_t *handle);

/**
 * @brief Destroy a subinterpreter handle
 *
 * Cleans up the handle's namespace within its worker.
 *
 * @param handle Handle to destroy
 */
void subinterp_thread_handle_destroy(py_subinterp_handle_t *handle);

/* ============================================================================
 * Execution API
 * ============================================================================ */

/**
 * @brief Synchronous call through subinterpreter handle
 *
 * Sends a call request to the worker and blocks until response.
 * The dispatch_mutex ensures serialization per worker.
 *
 * @param env NIF environment
 * @param handle Subinterpreter handle
 * @param module Module name term (atom or binary)
 * @param func Function name term (atom or binary)
 * @param args Arguments list term
 * @param kwargs Keyword arguments map term
 * @return Result term: {ok, Result} | {error, Reason}
 */
ERL_NIF_TERM subinterp_thread_call(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM module, ERL_NIF_TERM func,
                                    ERL_NIF_TERM args, ERL_NIF_TERM kwargs);

/**
 * @brief Synchronous eval through subinterpreter handle
 *
 * @param env NIF environment
 * @param handle Subinterpreter handle
 * @param code Code string term (binary)
 * @param locals Local variables map term
 * @return Result term: {ok, Result} | {error, Reason}
 */
ERL_NIF_TERM subinterp_thread_eval(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM code, ERL_NIF_TERM locals);

/**
 * @brief Synchronous exec through subinterpreter handle
 *
 * @param env NIF environment
 * @param handle Subinterpreter handle
 * @param code Code string term (binary)
 * @return Result term: ok | {error, Reason}
 */
ERL_NIF_TERM subinterp_thread_exec(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM code);

/**
 * @brief Fire-and-forget call (no result)
 *
 * Sends request to worker but returns immediately without waiting.
 * Used for side-effects where result is not needed.
 *
 * @param env NIF environment
 * @param handle Subinterpreter handle
 * @param module Module name term
 * @param func Function name term
 * @param args Arguments list term
 * @return ok
 */
ERL_NIF_TERM subinterp_thread_cast(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM module, ERL_NIF_TERM func,
                                    ERL_NIF_TERM args);

/**
 * @brief Async call - returns immediately with reference
 *
 * Sends request to worker. Worker uses erlang.send() to deliver result
 * to caller_pid with the given ref.
 *
 * @param env NIF environment
 * @param handle Subinterpreter handle
 * @param module Module name term
 * @param func Function name term
 * @param args Arguments list term
 * @param caller_pid PID to send result to
 * @param ref Reference for result correlation
 * @return ok
 */
ERL_NIF_TERM subinterp_thread_async_call(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                          ERL_NIF_TERM module, ERL_NIF_TERM func,
                                          ERL_NIF_TERM args, ErlNifPid *caller_pid,
                                          ERL_NIF_TERM ref);

/* ============================================================================
 * Global Pool Instance
 * ============================================================================ */

/** @brief Global thread pool (defined in py_subinterp_thread.c) */
extern subinterp_thread_pool_t g_thread_pool;

/** @brief Resource type for py_subinterp_handle_t */
extern ErlNifResourceType *PY_SUBINTERP_HANDLE_RESOURCE_TYPE;

#endif /* HAVE_SUBINTERPRETERS */

#endif /* PY_SUBINTERP_THREAD_H */
