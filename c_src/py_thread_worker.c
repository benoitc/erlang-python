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
     * - [0] = read end (Python reads responses, blocking)
     * - [1] = write end (Erlang writes responses, O_NONBLOCK)
     */
    int response_pipe[2];

    /** @brief Mutex protecting this worker */
    pthread_mutex_t mutex;

    /** @brief Flag: worker is currently in use */
    bool in_use;

    /**
     * @brief Flag: worker is permanently retired due to a callback
     * synchronisation failure. Skipped by acquire_thread_worker; not
     * recycled until NIF unload.
     */
    bool poisoned;

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
ErlNifPid g_thread_coordinator_pid;

/** @brief Flag: coordinator PID has been set */
bool g_has_thread_coordinator = false;

/**
 * @brief Counter of poisoned (permanently retired) workers.
 *
 * Bounded diagnostic ceiling: when this exceeds MAX_POISONED_WORKERS we
 * surface a loud error to the user instead of silently leaking pipe pairs.
 * Reaching this threshold means the callback subsystem is genuinely broken
 * and warrants a bug report.
 */
static _Atomic uint64_t g_poisoned_workers_count = 0;

/** @brief Diagnostic ceiling on poisoned-worker accumulation. */
#define MAX_POISONED_WORKERS 64

/** @brief Read/write timeout for thread-callback pipe traffic (30s). */
#define THREAD_WORKER_IO_TIMEOUT_MS 30000

/** @brief Stack-buffer threshold for combined-frame writes. */
#define THREAD_WORKER_STACK_FRAME_LIMIT 1024

/* PY_THREAD_CB_TRACE: optional stderr tracing for callback I/O.
 * Compile-time gate, default OFF. Enable with CMake
 *   -DENABLE_PY_THREAD_CB_TRACE=ON
 * which sets PY_THREAD_CB_TRACE=1.
 */
#ifdef PY_THREAD_CB_TRACE
#  define PY_THREAD_CB_LOG(fmt, ...)                                   \
    do {                                                               \
        struct timespec _ts;                                           \
        clock_gettime(CLOCK_MONOTONIC, &_ts);                          \
        enif_fprintf(stderr,                                           \
            "[py_thread_cb %ld.%09ld tid=0x%08x] " fmt "\n",           \
            (long)_ts.tv_sec, (long)_ts.tv_nsec,                       \
            (unsigned)((uintptr_t)pthread_self() & 0xffffffff),        \
            ##__VA_ARGS__);                                            \
    } while (0)
#else
#  define PY_THREAD_CB_LOG(fmt, ...) ((void)0)
#endif

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
 * When a new coordinator is registered (e.g., after app restart), we must
 * reset all existing workers' has_handler flag since the old handler
 * processes are dead.
 *
 * @param pid PID of the coordinator process
 */
static void thread_worker_set_coordinator(ErlNifPid pid) {
    g_thread_coordinator_pid = pid;
    g_has_thread_coordinator = true;

    /* Reset has_handler on all existing workers since old handlers are dead */
    pthread_mutex_lock(&g_thread_pool_mutex);
    thread_worker_t *tw = g_thread_pool_head;
    while (tw != NULL) {
        pthread_mutex_lock(&tw->mutex);
        tw->has_handler = false;
        pthread_mutex_unlock(&tw->mutex);
        tw = tw->next;
    }
    pthread_mutex_unlock(&g_thread_pool_mutex);
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
    tw->poisoned = false;

    /* Create response pipe */
    if (pipe(tw->response_pipe) < 0) {
        enif_free(tw);
        return NULL;
    }

    /* Set the WRITE end non-blocking. The Erlang handler runs on a dirty
     * I/O scheduler thread (see py_nif.c registration); making the write
     * end non-blocking lets write_all_with_deadline() return on a stalled
     * Python reader instead of pinning a dirty thread indefinitely. */
    int wflags = fcntl(tw->response_pipe[1], F_GETFL, 0);
    if (wflags >= 0) {
        (void)fcntl(tw->response_pipe[1], F_SETFL, wflags | O_NONBLOCK);
    }

    pthread_mutex_init(&tw->mutex, NULL);

    /* Add to pool */
    tw->next = g_thread_pool_head;
    g_thread_pool_head = tw;

    return tw;
}

/**
 * @brief Drain any leftover bytes from a pipe read end (non-blocking).
 *
 * Called when reusing a worker so the new owner does not inherit stale
 * bytes from a previous owner that exited mid-frame. Always restores
 * the original flags, even on intermediate read errors.
 */
static void drain_pipe(int fd) {
    int orig = fcntl(fd, F_GETFL, 0);
    if (orig < 0) return;
    if (fcntl(fd, F_SETFL, orig | O_NONBLOCK) < 0) return;

    char scratch[256];
    for (;;) {
        ssize_t n = read(fd, scratch, sizeof(scratch));
        if (n  > 0) continue;
        if (n  < 0 && errno == EINTR) continue;
        break;                /* EAGAIN, 0, or hard error */
    }
    (void)fcntl(fd, F_SETFL, orig);
}

/**
 * @brief Acquire a thread worker from the pool
 *
 * First tries to find an existing unused, non-poisoned worker in the
 * pool. If none available, creates a new one. Poisoned workers stay in
 * the linked list but are never returned.
 *
 * @return Pointer to acquired worker, or NULL on failure
 */
static thread_worker_t *acquire_thread_worker(void) {
    pthread_mutex_lock(&g_thread_pool_mutex);

    /* Look for unused, non-poisoned worker */
    thread_worker_t *tw = g_thread_pool_head;
    while (tw != NULL) {
        pthread_mutex_lock(&tw->mutex);
        if (!tw->in_use && !tw->poisoned) {
            tw->in_use = true;
            int read_fd = tw->response_pipe[0];
            pthread_mutex_unlock(&tw->mutex);
            pthread_mutex_unlock(&g_thread_pool_mutex);
            /* Drain any stale bytes the previous owner left behind. */
            if (read_fd >= 0) drain_pipe(read_fd);
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
 * @brief Permanently retire a worker after a callback synchronisation
 *        failure.
 *
 * Called by thread_worker_call when the read path cannot guarantee
 * frame integrity (id mismatch + corrupted payload, partial-frame
 * timeout, EOF mid-frame). Closes both pipe ends so any further write
 * by the existing handler fails loudly, marks the worker as out of
 * service, and bumps the global counter for diagnostics.
 *
 * @note Caller must clear tl_thread_worker before returning to Python so
 *       the next call from this thread acquires a fresh worker.
 */
static void poison_thread_worker(thread_worker_t *tw) {
    pthread_mutex_lock(&tw->mutex);
    if (tw->poisoned) {
        pthread_mutex_unlock(&tw->mutex);
        return;
    }
    tw->poisoned = true;
    tw->in_use = false;
    int read_fd  = tw->response_pipe[0];
    int write_fd = tw->response_pipe[1];
    tw->response_pipe[0] = -1;
    tw->response_pipe[1] = -1;
    pthread_mutex_unlock(&tw->mutex);

    if (read_fd  >= 0) close(read_fd);
    if (write_fd >= 0) close(write_fd);

    uint64_t prev = atomic_fetch_add(&g_poisoned_workers_count, 1);
    if (prev + 1 >= MAX_POISONED_WORKERS) {
        enif_fprintf(stderr,
            "[erlang_python] WARNING: %llu thread-callback workers have been "
            "poisoned. The thread-callback subsystem is unhealthy; please "
            "file a bug report with the failing test case.\n",
            (unsigned long long)(prev + 1));
    }
    PY_THREAD_CB_LOG("poison tw_id=%llu count=%llu",
        (unsigned long long)tw->id, (unsigned long long)(prev + 1));
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

    /* Read readiness signal: 4 bytes of length == 0. Strict success
     * condition (Defect 5): both the byte count must match AND the
     * value must be zero. Short reads are not silently accepted. */
    uint32_t response_len = 0;
    ssize_t n = read_with_timeout(tw->response_pipe[0], &response_len,
                                  sizeof(response_len), 10000);
    if (n != (ssize_t)sizeof(response_len) || response_len != 0) {
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
 * Wire format (response from Erlang handler):
 *   <<callback_id:8/binary, len:4/binary, payload:len/binary>>
 *
 * The callback_id is matched against the request's expected id; on
 * mismatch, the rest of the frame is consumed and the loop continues.
 * Any partial-frame failure poisons the worker (see
 * poison_thread_worker) so subsequent calls from this thread acquire a
 * fresh one.
 *
 * @param func_name Name of the Erlang function to call
 * @param func_name_len Length of function name
 * @param call_args Python tuple of arguments
 * @return Python result object, or NULL with exception set
 */
static PyObject *thread_worker_call(const char *func_name, size_t func_name_len,
                                    PyObject *call_args) {
    /* Diagnostic ceiling: surface the unhealthy subsystem to the user. */
    if (atomic_load(&g_poisoned_workers_count) >= MAX_POISONED_WORKERS) {
        PyErr_SetString(PyExc_RuntimeError,
            "thread-callback subsystem unhealthy "
            "(too many poisoned workers; see stderr)");
        return NULL;
    }

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
    uint64_t expected_id = atomic_fetch_add(&g_callback_id_counter, 1);
    ERL_NIF_TERM id_term = enif_make_uint64(msg_env, expected_id);

    /* Send message: {thread_callback, WorkerId, CallbackId, FuncName, Args} */
    ERL_NIF_TERM msg = enif_make_tuple5(msg_env,
        enif_make_atom(msg_env, "thread_callback"),
        enif_make_uint64(msg_env, tw->id),
        id_term,
        func_term,
        args_term);

    /* Send message to coordinator (can be done with GIL held) */
    if (!enif_send(NULL, &g_thread_coordinator_pid, msg_env, msg)) {
        enif_free_env(msg_env);
        PyErr_SetString(PyExc_RuntimeError, "Failed to send callback message");
        return NULL;
    }
    enif_free_env(msg_env);
    PY_THREAD_CB_LOG("send tw_id=%llu cb_id=%llu",
        (unsigned long long)tw->id, (unsigned long long)expected_id);

    /* Read frames until we get the one matching expected_id, or fail. */
    char *response_data = NULL;
    uint32_t response_len = 0;
    int desync = 0;            /* 1 = poison the worker before returning */

    Py_BEGIN_ALLOW_THREADS
    while (1) {
        uint64_t got_id = 0;
        ssize_t nid = read_with_timeout(tw->response_pipe[0],
                                        &got_id, sizeof(got_id),
                                        THREAD_WORKER_IO_TIMEOUT_MS);
        if (nid != (ssize_t)sizeof(got_id)) {
            desync = 1;
            break;
        }
        int r = read_length_prefixed_data(
            tw->response_pipe[0], &response_data, &response_len,
            THREAD_WORKER_IO_TIMEOUT_MS);
        if (r != 0) {
            desync = 1;
            break;
        }
        if (got_id == expected_id) {
            break;             /* response_data + response_len ready */
        }
        /* Stale frame: discard and continue draining. */
        if (response_data != NULL) {
            enif_free(response_data);
            response_data = NULL;
            response_len = 0;
        }
    }
    Py_END_ALLOW_THREADS

    if (desync) {
        if (response_data != NULL) {
            enif_free(response_data);
        }
        PY_THREAD_CB_LOG("desync tw_id=%llu cb_id=%llu errno=%d",
            (unsigned long long)tw->id,
            (unsigned long long)expected_id, errno);
        poison_thread_worker(tw);
        tl_thread_worker = NULL;
        if (g_thread_worker_key_created) {
            pthread_setspecific(g_thread_worker_key, NULL);
        }
        PyErr_SetString(PyExc_RuntimeError,
            "callback synchronisation lost; retry");
        return NULL;
    }

    PY_THREAD_CB_LOG("recv tw_id=%llu cb_id=%llu len=%u",
        (unsigned long long)tw->id, (unsigned long long)expected_id,
        (unsigned)response_len);

    PyObject *result = parse_callback_response(
        (unsigned char *)response_data, response_len);
    if (response_data != NULL) {
        enif_free(response_data);
    }
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
 * @brief NIF to write an id-prefixed response to a thread worker's pipe.
 *
 * Args: WriteFd, CallbackId, ResponseBinary
 *
 * Wire format (matches thread_worker_call's reader):
 *   <<callback_id:8/binary, len:4/binary, data/binary>>
 *
 * Combining the three pieces into one looped write closes the
 * length-then-data race (Defect 3) and lets the reader correlate
 * responses by callback_id. The write end is non-blocking and the NIF
 * is registered as ERL_NIF_DIRTY_JOB_IO_BOUND, so a stalled Python
 * reader produces a write_timeout instead of pinning a scheduler.
 */
static ERL_NIF_TERM nif_thread_worker_write_with_id(ErlNifEnv *env, int argc,
                                                    const ERL_NIF_TERM argv[]) {
    (void)argc;
    int fd;
    ErlNifUInt64 callback_id;
    ErlNifBinary response;

    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }
    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }
    if (!enif_inspect_binary(env, argv[2], &response)) {
        return make_error(env, "invalid_response");
    }

    size_t total = sizeof(callback_id) + sizeof(uint32_t) + response.size;
    char  stack_buf[THREAD_WORKER_STACK_FRAME_LIMIT];
    char *buf = (total <= sizeof(stack_buf))
        ? stack_buf
        : enif_alloc(total);
    if (buf == NULL) {
        return make_error(env, "alloc_failed");
    }

    uint64_t cb_id = (uint64_t)callback_id;
    uint32_t len   = (uint32_t)response.size;
    memcpy(buf,                                &cb_id, sizeof(cb_id));
    memcpy(buf + sizeof(cb_id),                &len,   sizeof(len));
    if (response.size > 0) {
        memcpy(buf + sizeof(cb_id) + sizeof(len),
               response.data, response.size);
    }

    write_result_t r = write_all_with_deadline(
        fd, buf, total, THREAD_WORKER_IO_TIMEOUT_MS);

    if (buf != stack_buf) {
        enif_free(buf);
    }

    switch (r) {
        case WRITE_OK:      return ATOM_OK;
        case WRITE_TIMEOUT: return make_error(env, "write_timeout");
        case WRITE_ERROR:   return make_error(env, "write_failed");
    }
    return make_error(env, "write_failed");
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

/**
 * @brief NIF to write an async callback response.
 *
 * Args: WriteFd, CallbackId, ResponseBinary
 *
 * Wire format (matches process_async_callback_response in py_callback.c):
 *   <<callback_id:8/binary, len:4/binary, data/binary>>
 *
 * Atomicity (review #4): the kernel does NOT guarantee per-write()
 * atomicity beyond PIPE_BUF, so frame-on-the-wire integrity is
 * guaranteed by the *single-writer process invariant* on the Erlang
 * side (one async_writer_loop per WriteFd; see py_thread_handler.erl).
 * This NIF does one looped non-blocking write of the combined buffer;
 * even when the kernel chunks it, no other writer can interleave.
 *
 * Registered as ERL_NIF_DIRTY_JOB_IO_BOUND (see py_nif.c). The write
 * end of async_callback_pipe is set non-blocking on the C side
 * (py_callback.c init); a stalled Python reader produces
 * write_timeout instead of holding a scheduler thread.
 */
static ERL_NIF_TERM nif_async_callback_response(ErlNifEnv *env, int argc,
                                                 const ERL_NIF_TERM argv[]) {
    (void)argc;
    int fd;
    ErlNifUInt64 callback_id;
    ErlNifBinary response;

    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }
    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }
    if (!enif_inspect_binary(env, argv[2], &response)) {
        return make_error(env, "invalid_response");
    }

    size_t total = sizeof(callback_id) + sizeof(uint32_t) + response.size;
    char  stack_buf[THREAD_WORKER_STACK_FRAME_LIMIT];
    char *buf = (total <= sizeof(stack_buf))
        ? stack_buf
        : enif_alloc(total);
    if (buf == NULL) {
        return make_error(env, "alloc_failed");
    }

    uint64_t cb_id = (uint64_t)callback_id;
    uint32_t len   = (uint32_t)response.size;
    memcpy(buf,                                &cb_id, sizeof(cb_id));
    memcpy(buf + sizeof(cb_id),                &len,   sizeof(len));
    if (response.size > 0) {
        memcpy(buf + sizeof(cb_id) + sizeof(len),
               response.data, response.size);
    }

    write_result_t r = write_all_with_deadline(
        fd, buf, total, THREAD_WORKER_IO_TIMEOUT_MS);

    if (buf != stack_buf) {
        enif_free(buf);
    }

    switch (r) {
        case WRITE_OK:      return ATOM_OK;
        case WRITE_TIMEOUT: return make_error(env, "write_timeout");
        case WRITE_ERROR:   return make_error(env, "write_failed");
    }
    return make_error(env, "write_failed");
}
