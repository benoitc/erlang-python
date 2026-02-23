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
 * @file py_submit.h
 * @brief Non-blocking Python call submission with event-driven results
 *
 * This module provides NIFs for submitting Python calls that deliver
 * results via enif_send to an event loop process, rather than blocking
 * the calling NIF. This enables the unified event-driven architecture.
 *
 * Flow:
 * 1. Erlang calls submit_call/submit_coroutine with CallbackId
 * 2. Request is queued to worker thread
 * 3. Worker executes Python code
 * 4. Result sent via enif_send({call_result, CallbackId, Result})
 * 5. py_event_loop_proc dispatches to original caller
 */

#ifndef PY_SUBMIT_H
#define PY_SUBMIT_H

#include <erl_nif.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

/* ============================================================================
 * Submit Request Structure
 * ============================================================================ */

/**
 * @enum submit_request_type_t
 * @brief Types of submit requests
 */
typedef enum {
    SUBMIT_CALL,      /**< Regular Python function call */
    SUBMIT_COROUTINE  /**< Asyncio coroutine */
} submit_request_type_t;

/**
 * @struct submit_request_t
 * @brief Request for non-blocking Python execution
 *
 * Contains all information needed to execute a Python call and
 * deliver the result to the event loop process.
 */
typedef struct submit_request {
    /** @brief Type of request */
    submit_request_type_t type;

    /** @brief Unique callback ID for correlating with caller */
    uint64_t callback_id;

    /** @brief PID of event loop process to send result */
    ErlNifPid event_proc_pid;

    /** @brief Module name */
    char *module;

    /** @brief Function name */
    char *func;

    /** @brief Arguments (Python object, owned reference) */
    void *args;  /* PyObject* */

    /** @brief Keyword arguments (Python object, owned reference) */
    void *kwargs;  /* PyObject* */

    /** @brief Environment for building result messages */
    ErlNifEnv *msg_env;

    /** @brief Next request in queue */
    struct submit_request *next;
} submit_request_t;

/* ============================================================================
 * Submit Queue State
 * ============================================================================ */

/**
 * @struct submit_queue_t
 * @brief Thread-safe queue for submit requests
 */
typedef struct {
    /** @brief Mutex protecting the queue */
    pthread_mutex_t mutex;

    /** @brief Condition variable for queue signaling */
    pthread_cond_t cond;

    /** @brief Head of request queue */
    submit_request_t *head;

    /** @brief Tail of request queue */
    submit_request_t *tail;

    /** @brief Worker thread handle */
    pthread_t worker_thread;

    /** @brief Flag: worker thread is running */
    volatile bool running;

    /** @brief Flag: shutdown requested */
    volatile bool shutdown;
} submit_queue_t;

/* ============================================================================
 * Initialization Functions
 * ============================================================================ */

/**
 * @brief Initialize atoms used by the submit module
 *
 * Called during NIF load.
 *
 * @param env NIF environment
 */
void submit_init_atoms(ErlNifEnv *env);

/**
 * @brief Initialize the submit module
 *
 * Creates the submit queue but does NOT start the worker thread.
 * Worker thread is started lazily by submit_start_worker().
 *
 * @return 0 on success, -1 on failure
 */
int submit_init(void);

/**
 * @brief Start the submit worker thread
 *
 * Must be called after Python is initialized.
 * Safe to call multiple times - will only start thread once.
 *
 * @return 0 on success, -1 on failure
 */
int submit_start_worker(void);

/**
 * @brief Clean up the submit module
 *
 * Shuts down the worker thread and frees resources.
 */
void submit_cleanup(void);

/* ============================================================================
 * Submit NIF Functions
 * ============================================================================ */

/**
 * @brief Submit a Python function call for non-blocking execution
 *
 * Queues a call to be executed by the worker thread. Result will be
 * sent to the event loop process as:
 *   {call_result, CallbackId, Result} or
 *   {call_error, CallbackId, Error}
 *
 * NIF: submit_call(EventProcPid, CallbackId, Module, Func, Args, Kwargs)
 *      -> ok | {error, Reason}
 *
 * @param env NIF environment
 * @param argc Argument count (6)
 * @param argv Arguments
 * @return ok or {error, Reason}
 */
ERL_NIF_TERM nif_submit_call(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]);

/**
 * @brief Submit an asyncio coroutine for non-blocking execution
 *
 * Queues a coroutine to be executed in the asyncio event loop.
 * Result delivery is the same as submit_call.
 *
 * NIF: submit_coroutine(EventProcPid, CallbackId, Module, Func, Args, Kwargs)
 *      -> ok | {error, Reason}
 *
 * @param env NIF environment
 * @param argc Argument count (6)
 * @param argv Arguments
 * @return ok or {error, Reason}
 */
ERL_NIF_TERM nif_submit_coroutine(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]);

/* ============================================================================
 * Internal Functions
 * ============================================================================ */

/**
 * @brief Worker thread main function
 *
 * Processes submit requests from the queue.
 *
 * @param arg Unused
 * @return NULL
 */
void *submit_worker_thread(void *arg);

/**
 * @brief Process a single submit request
 *
 * Executes the Python call and sends result to event loop process.
 *
 * @param req Request to process
 */
void process_submit_request(submit_request_t *req);

/**
 * @brief Free a submit request
 *
 * Releases all resources held by the request.
 *
 * @param req Request to free
 */
void free_submit_request(submit_request_t *req);

#endif /* PY_SUBMIT_H */
