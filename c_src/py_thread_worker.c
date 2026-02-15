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
 * @file py_thread_worker.c
 * @brief Thread worker pool for Python thread support
 * @author Benoit Chesneau
 *
 * This module enables any spawned Python thread to call erlang.call() without
 * blocking. Supported thread types include:
 * - threading.Thread instances
 * - concurrent.futures.ThreadPoolExecutor workers
 * - Any other Python threads
 *
 * Each spawned thread lazily acquires a dedicated "thread worker" channel
 * for communicating with Erlang.
 *
 * @par Architecture
 *
 * One Erlang process per Python thread - lightweight, isolated, simple.
 *
 * @par Thread Worker Lifecycle
 *
 * 1. Python thread calls erlang.call() for the first time
 * 2. Thread acquires a worker from pool (or creates new one)
 * 3. Worker's Erlang handler process is spawned via NIF
 * 4. All callbacks from that thread go to its handler process
 * 5. When Python thread exits, pthread_key destructor releases worker
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Thread Worker Types
 * ============================================================================ */

/**
 * @struct thread_worker_t
 * @brief Represents a thread worker for spawned Python threads
 *
 * Each worker has a dedicated pipe for receiving responses from Erlang
 * and an Erlang handler process that executes callbacks.
 */
typedef struct thread_worker {
    /** @brief Unique worker ID */
    uint64_t id;

    /**
     * @brief Response pipe file descriptors
     * - [0] = read end (Python reads responses)
     * - [1] = write end (Erlang writes responses)
     */
    int response_pipe[2];

    /** @brief Mutex protecting this worker */
    pthread_mutex_t mutex;

    /** @brief Flag: worker is currently in use */
    bool in_use;

    /** @brief PID of the Erlang handler process for this worker */
    ErlNifPid handler_pid;

    /** @brief Flag: handler has been spawned */
    bool has_handler;

    /** @brief Next worker in pool linked list */
    struct thread_worker *next;
} thread_worker_t;

/* ============================================================================
 * Global State for Thread Worker Pool
 * ============================================================================ */

/** @brief Mutex protecting the thread worker pool */
static pthread_mutex_t g_thread_pool_mutex = PTHREAD_MUTEX_INITIALIZER;

/** @brief Head of the thread worker pool linked list */
static thread_worker_t *g_thread_pool_head = NULL;

/** @brief Counter for generating unique worker IDs */
static _Atomic uint64_t g_thread_worker_id_counter = 1;

/** @brief pthread key for thread-local worker cleanup */
static pthread_key_t g_thread_worker_key;

/** @brief Flag: thread worker key has been created */
static bool g_thread_worker_key_created = false;

/** @brief Thread-local sticky worker (hybrid model) */
static __thread thread_worker_t *tl_thread_worker = NULL;

/** @brief PID of the Erlang thread coordinator process */
static ErlNifPid g_thread_coordinator_pid;

/** @brief Flag: coordinator PID has been set */
static bool g_has_thread_coordinator = false;

/* ============================================================================
 * Thread Worker Destructor (pthread_key cleanup)
 * ============================================================================ */

/**
 * @brief Cleanup function called when a thread exits
 *
 * This is registered with pthread_key_create and automatically called
 * when a thread that has used a thread worker terminates.
 *
 * @param arg Pointer to the thread_worker_t to release
 */
static void thread_worker_destructor(void *arg) {
    thread_worker_t *tw = (thread_worker_t *)arg;
    if (tw == NULL) {
        return;
    }

    pthread_mutex_lock(&g_thread_pool_mutex);

    /* Mark as not in use so it can be reused */
    pthread_mutex_lock(&tw->mutex);
    tw->in_use = false;
    pthread_mutex_unlock(&tw->mutex);

    pthread_mutex_unlock(&g_thread_pool_mutex);

    /* Clear thread-local pointer */
    tl_thread_worker = NULL;
}

/* ============================================================================
 * Thread Worker Pool Management
 * ============================================================================ */

/**
 * @brief Initialize the thread worker system
 *
 * Creates the pthread key for automatic cleanup on thread exit.
 * Called during NIF initialization.
 *
 * @return 0 on success, -1 on failure
 */
static int thread_worker_init(void) {
    if (g_thread_worker_key_created) {
        return 0;  /* Already initialized */
    }

    if (pthread_key_create(&g_thread_worker_key, thread_worker_destructor) != 0) {
        return -1;
    }

    g_thread_worker_key_created = true;
    return 0;
}

/**
 * @brief Clean up the thread worker system
 *
 * Releases all workers in the pool and destroys the pthread key.
 * Called during NIF unload.
 */
static void thread_worker_cleanup(void) {
    pthread_mutex_lock(&g_thread_pool_mutex);

    thread_worker_t *tw = g_thread_pool_head;
    while (tw != NULL) {
        thread_worker_t *next = tw->next;

        /* Close pipes */
        if (tw->response_pipe[0] >= 0) {
            close(tw->response_pipe[0]);
        }
        if (tw->response_pipe[1] >= 0) {
            close(tw->response_pipe[1]);
        }

        pthread_mutex_destroy(&tw->mutex);
        enif_free(tw);
        tw = next;
    }

    g_thread_pool_head = NULL;
    pthread_mutex_unlock(&g_thread_pool_mutex);

    if (g_thread_worker_key_created) {
        pthread_key_delete(g_thread_worker_key);
        g_thread_worker_key_created = false;
    }
}

/**
 * @brief Set the thread coordinator PID
 *
 * The coordinator is the Erlang process that spawns handler processes
 * for new thread workers.
 *
 * @param pid PID of the coordinator process
 */
static void thread_worker_set_coordinator(ErlNifPid pid) {
    g_thread_coordinator_pid = pid;
    g_has_thread_coordinator = true;
}

/**
 * @brief Create a new thread worker
 *
 * Allocates a new worker with response pipe and adds to the pool.
 *
 * @return Pointer to new worker, or NULL on failure
 */
static thread_worker_t *create_thread_worker(void) {
    thread_worker_t *tw = enif_alloc(sizeof(thread_worker_t));
    if (tw == NULL) {
        return NULL;
    }

    memset(tw, 0, sizeof(thread_worker_t));
    tw->id = atomic_fetch_add(&g_thread_worker_id_counter, 1);
    tw->response_pipe[0] = -1;
    tw->response_pipe[1] = -1;
    tw->in_use = true;
    tw->has_handler = false;

    /* Create response pipe */
    if (pipe(tw->response_pipe) < 0) {
        enif_free(tw);
        return NULL;
    }

    pthread_mutex_init(&tw->mutex, NULL);

    /* Add to pool */
    tw->next = g_thread_pool_head;
    g_thread_pool_head = tw;

    return tw;
}

/**
 * @brief Acquire a thread worker from the pool
 *
 * First tries to find an existing unused worker in the pool.
 * If none available, creates a new one.
 *
 * @return Pointer to acquired worker, or NULL on failure
 */
static thread_worker_t *acquire_thread_worker(void) {
    pthread_mutex_lock(&g_thread_pool_mutex);

    /* Look for unused worker */
    thread_worker_t *tw = g_thread_pool_head;
    while (tw != NULL) {
        pthread_mutex_lock(&tw->mutex);
        if (!tw->in_use) {
            tw->in_use = true;
            pthread_mutex_unlock(&tw->mutex);
            pthread_mutex_unlock(&g_thread_pool_mutex);
            return tw;
        }
        pthread_mutex_unlock(&tw->mutex);
        tw = tw->next;
    }

    /* No unused workers - create new one */
    tw = create_thread_worker();
    pthread_mutex_unlock(&g_thread_pool_mutex);

    return tw;
}

/**
 * @brief Release a thread worker back to the pool
 *
 * Marks the worker as available for reuse.
 *
 * @param tw Worker to release
 */
static void release_thread_worker(thread_worker_t *tw) {
    if (tw == NULL) {
        return;
    }

    pthread_mutex_lock(&tw->mutex);
    tw->in_use = false;
    pthread_mutex_unlock(&tw->mutex);
}

/* ============================================================================
 * Thread Worker Call Implementation
 * ============================================================================ */

/**
 * @brief Spawn an Erlang handler process for a thread worker
 *
 * Sends a message to the coordinator to spawn a handler process.
 * The coordinator responds via the response pipe.
 *
 * @param tw Thread worker to spawn handler for
 * @return 0 on success, -1 on failure
 */
static int thread_worker_spawn_handler(thread_worker_t *tw) {
    if (!g_has_thread_coordinator) {
        return -1;
    }

    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        return -1;
    }

    /* Send spawn request: {thread_worker_spawn, WorkerId, WriteFd} */
    ERL_NIF_TERM msg = enif_make_tuple3(msg_env,
        enif_make_atom(msg_env, "thread_worker_spawn"),
        enif_make_uint64(msg_env, tw->id),
        enif_make_int(msg_env, tw->response_pipe[1]));

    if (!enif_send(NULL, &g_thread_coordinator_pid, msg_env, msg)) {
        enif_free_env(msg_env);
        return -1;
    }
    enif_free_env(msg_env);

    /* Read handler PID from pipe (Erlang writes it after spawning) */
    uint32_t response_len = 0;
    ssize_t n = read(tw->response_pipe[0], &response_len, sizeof(response_len));
    if (n != sizeof(response_len) || response_len == 0) {
        /* Handler spawned successfully - pid info is in the length as a signal */
        tw->has_handler = true;
        return 0;
    }

    /* If response_len > 0, there might be error data */
    if (response_len > 1000) {
        /* Sanity check - something went wrong */
        return -1;
    }

    tw->has_handler = true;
    return 0;
}

/**
 * @brief Execute an erlang.call() from a spawned thread
 *
 * This is called when a Python thread that is NOT an executor thread
 * (i.e., tl_current_worker == NULL) tries to call erlang.call().
 *
 * @param func_name Name of the Erlang function to call
 * @param func_name_len Length of function name
 * @param call_args Python tuple of arguments
 * @return Python result object, or NULL with exception set
 */
static PyObject *thread_worker_call(const char *func_name, size_t func_name_len,
                                    PyObject *call_args) {
    /* Get or create sticky worker for this thread */
    if (tl_thread_worker == NULL) {
        tl_thread_worker = acquire_thread_worker();
        if (tl_thread_worker == NULL) {
            PyErr_SetString(PyExc_RuntimeError, "No thread workers available");
            return NULL;
        }

        /* Register for cleanup on thread exit */
        if (g_thread_worker_key_created) {
            pthread_setspecific(g_thread_worker_key, tl_thread_worker);
        }
    }

    thread_worker_t *tw = tl_thread_worker;

    /* Ensure handler is spawned */
    if (!tw->has_handler) {
        if (thread_worker_spawn_handler(tw) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to spawn thread handler");
            return NULL;
        }
    }

    /* Build callback message and send to coordinator */
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate message environment");
        return NULL;
    }

    /* Create function name binary */
    ERL_NIF_TERM func_term;
    unsigned char *fn_buf = enif_make_new_binary(msg_env, func_name_len, &func_term);
    if (fn_buf == NULL) {
        enif_free_env(msg_env);
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate function name");
        return NULL;
    }
    memcpy(fn_buf, func_name, func_name_len);

    /* Convert args to Erlang term */
    ERL_NIF_TERM args_term = py_to_term(msg_env, call_args);

    /* Generate callback ID */
    uint64_t callback_id = atomic_fetch_add(&g_callback_id_counter, 1);
    ERL_NIF_TERM id_term = enif_make_uint64(msg_env, callback_id);

    /* Send message: {thread_callback, WorkerId, CallbackId, FuncName, Args} */
    ERL_NIF_TERM msg = enif_make_tuple5(msg_env,
        enif_make_atom(msg_env, "thread_callback"),
        enif_make_uint64(msg_env, tw->id),
        id_term,
        func_term,
        args_term);

    ssize_t n;
    uint32_t response_len = 0;
    char *response_data = NULL;

    /* Send message to coordinator (can be done with GIL held) */
    if (!enif_send(NULL, &g_thread_coordinator_pid, msg_env, msg)) {
        enif_free_env(msg_env);
        PyErr_SetString(PyExc_RuntimeError, "Failed to send callback message");
        return NULL;
    }
    enif_free_env(msg_env);

    /* Release GIL while waiting for response */
    Py_BEGIN_ALLOW_THREADS

    /* Read response from pipe */
    n = read(tw->response_pipe[0], &response_len, sizeof(response_len));

    Py_END_ALLOW_THREADS

    if (n != sizeof(response_len)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to read callback response length");
        return NULL;
    }

    response_data = enif_alloc(response_len);
    if (response_data == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate response buffer");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    n = read(tw->response_pipe[0], response_data, response_len);
    Py_END_ALLOW_THREADS

    if (n != (ssize_t)response_len) {
        enif_free(response_data);
        PyErr_SetString(PyExc_RuntimeError, "Failed to read callback response data");
        return NULL;
    }

    /* Parse response using existing function */
    PyObject *result = parse_callback_response((unsigned char *)response_data, response_len);
    enif_free(response_data);

    return result;
}

/* ============================================================================
 * Thread Worker NIFs
 * ============================================================================ */

/**
 * @brief NIF to set the thread worker coordinator PID
 *
 * Args: Pid
 * Called during application startup to register the coordinator.
 */
static ERL_NIF_TERM nif_thread_worker_set_coordinator(ErlNifEnv *env, int argc,
                                                        const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifPid pid;

    if (!enif_get_local_pid(env, argv[0], &pid)) {
        return make_error(env, "invalid_pid");
    }

    thread_worker_set_coordinator(pid);
    return ATOM_OK;
}

/**
 * @brief NIF to write response to a thread worker's pipe
 *
 * Args: WriteFd, ResponseBinary
 * Called by Erlang handler to send callback result back to Python thread.
 */
static ERL_NIF_TERM nif_thread_worker_write(ErlNifEnv *env, int argc,
                                             const ERL_NIF_TERM argv[]) {
    (void)argc;
    int fd;
    ErlNifBinary response;

    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    if (!enif_inspect_binary(env, argv[1], &response)) {
        return make_error(env, "invalid_response");
    }

    /* Write length then data */
    uint32_t len = (uint32_t)response.size;
    ssize_t n = write(fd, &len, sizeof(len));
    if (n != sizeof(len)) {
        return make_error(env, "write_length_failed");
    }

    n = write(fd, response.data, response.size);
    if (n != (ssize_t)response.size) {
        return make_error(env, "write_data_failed");
    }

    return ATOM_OK;
}

/**
 * @brief NIF to signal that a handler has been spawned
 *
 * Args: WriteFd
 * Called by Erlang after spawning a handler to signal readiness.
 */
static ERL_NIF_TERM nif_thread_worker_signal_ready(ErlNifEnv *env, int argc,
                                                    const ERL_NIF_TERM argv[]) {
    (void)argc;
    int fd;

    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    /* Write a zero-length response to signal readiness */
    uint32_t len = 0;
    ssize_t n = write(fd, &len, sizeof(len));
    if (n != sizeof(len)) {
        return make_error(env, "write_failed");
    }

    return ATOM_OK;
}
