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
 * @file py_event_loop.c
 * @brief Erlang-native asyncio event loop implementation using enif_select
 *
 * This module implements an asyncio-compatible event loop that delegates
 * I/O multiplexing to Erlang's scheduler via enif_select. This provides:
 *
 * - Sub-millisecond latency (vs 10ms polling in the old approach)
 * - Zero CPU usage when idle (no polling)
 * - Full GIL release during waits
 * - Native Erlang scheduler integration
 *
 * The flow is:
 * 1. Python calls add_reader(fd, callback) -> enif_select(fd, READ)
 * 2. Erlang scheduler monitors fd
 * 3. When fd is ready, Erlang sends {select, Res, Ref, ready_input}
 * 4. py_event_router receives message, calls dispatch_callback NIF
 * 5. Python callback is invoked
 */

#include "py_nif.h"
#include "py_event_loop.h"

/* ============================================================================
 * Global State
 * ============================================================================ */

/** Resource type for event loops */
ErlNifResourceType *EVENT_LOOP_RESOURCE_TYPE = NULL;

/** Resource type for fd monitoring */
ErlNifResourceType *FD_RESOURCE_TYPE = NULL;

/** Resource type for timers */
ErlNifResourceType *TIMER_RESOURCE_TYPE = NULL;

/** Atoms for event loop messages */
ERL_NIF_TERM ATOM_SELECT;
ERL_NIF_TERM ATOM_READY_INPUT;
ERL_NIF_TERM ATOM_READY_OUTPUT;
ERL_NIF_TERM ATOM_READ;
ERL_NIF_TERM ATOM_WRITE;
ERL_NIF_TERM ATOM_TIMER;
ERL_NIF_TERM ATOM_START_TIMER;
ERL_NIF_TERM ATOM_CANCEL_TIMER;
ERL_NIF_TERM ATOM_EVENT_LOOP;
ERL_NIF_TERM ATOM_DISPATCH;

/* ============================================================================
 * Per-Interpreter Event Loop Storage
 * ============================================================================
 *
 * Event loop references are stored as module attributes in py_event_loop,
 * using PyCapsule for safe C pointer storage. This approach:
 *
 * - Works uniformly for main interpreter and sub-interpreters
 * - Each interpreter has its own py_event_loop module with its own attribute
 * - Thread-safe for free-threading (Python 3.13+)
 * - Uses gil_acquire()/gil_release() for safe GIL management
 *
 * Flow:
 *   NIF set_python_event_loop() -> stores capsule in py_event_loop._loop
 *   Python _is_initialized() -> checks if _loop attribute exists and is valid
 *   Python operations -> retrieve loop from py_event_loop._loop
 */

/** @brief Name for the PyCapsule storing event loop pointer */
static const char *EVENT_LOOP_CAPSULE_NAME = "erlang_python.event_loop";

/** @brief Module attribute name for storing the event loop */
static const char *EVENT_LOOP_ATTR_NAME = "_loop";

/* Forward declaration for fallback in get_interpreter_event_loop */
static erlang_event_loop_t *g_python_event_loop;

/* Global flag for isolation mode - set by Erlang via NIF */
static volatile int g_isolation_mode = 0;  /* 0 = global, 1 = per_loop */

/* Global shared router PID - set during init, used by all loops in per_loop mode */
static ErlNifPid g_shared_router;
static volatile int g_shared_router_valid = 0;

/**
 * Get the py_event_loop module for the current interpreter.
 * MUST be called with GIL held.
 * Returns borrowed reference.
 */
static PyObject *get_event_loop_module(void) {
    PyObject *modules = PyImport_GetModuleDict();
    if (modules == NULL) {
        return NULL;
    }
    return PyDict_GetItemString(modules, "py_event_loop");
}

/**
 * Get the event loop for the current Python interpreter.
 * MUST be called with GIL held.
 *
 * For now, we use the global g_python_event_loop directly. Per-interpreter
 * storage via module attributes was causing issues on some Python versions.
 * The global approach works correctly since all Python code in the main
 * interpreter shares the same event loop.
 *
 * TODO: Implement proper per-interpreter storage for sub-interpreter support.
 *
 * @return Event loop pointer or NULL if not set
 */
static erlang_event_loop_t *get_interpreter_event_loop(void) {
    return g_python_event_loop;
}

/**
 * Set the event loop for the current interpreter.
 * MUST be called with GIL held.
 * Stores as py_event_loop._loop module attribute.
 *
 * @param loop Event loop to set
 * @return 0 on success, -1 on error
 */
static int set_interpreter_event_loop(erlang_event_loop_t *loop) {
    PyObject *module = get_event_loop_module();
    if (module == NULL) {
        return -1;
    }

    if (loop == NULL) {
        /* Clear the event loop attribute */
        if (PyObject_SetAttrString(module, EVENT_LOOP_ATTR_NAME, Py_None) < 0) {
            PyErr_Clear();
        }
        return 0;
    }

    PyObject *capsule = PyCapsule_New(loop, EVENT_LOOP_CAPSULE_NAME, NULL);
    if (capsule == NULL) {
        return -1;
    }

    int result = PyObject_SetAttrString(module, EVENT_LOOP_ATTR_NAME, capsule);
    Py_DECREF(capsule);

    if (result < 0) {
        PyErr_Clear();
        return -1;
    }

    return 0;
}

/* ============================================================================
 * Resource Callbacks
 * ============================================================================ */

/* Forward declaration */
int create_default_event_loop(ErlNifEnv *env);

/**
 * @brief Destructor for event loop resources
 */
void event_loop_destructor(ErlNifEnv *env, void *obj) {
    erlang_event_loop_t *loop = (erlang_event_loop_t *)obj;

    /* If this is the active Python event loop, clear references */
    if (g_python_event_loop == loop) {
        g_python_event_loop = NULL;
        /* Clear per-interpreter storage if we can acquire GIL.
         * Don't create new loop in destructor - let next Python call handle it. */
        PyGILState_STATE gstate = PyGILState_Ensure();
        erlang_event_loop_t *interp_loop = get_interpreter_event_loop();
        if (interp_loop == loop) {
            set_interpreter_event_loop(NULL);
        }
        PyGILState_Release(gstate);
    }

    /* Signal shutdown */
    loop->shutdown = true;

    /* Wake up any waiting threads */
    pthread_mutex_lock(&loop->mutex);
    pthread_cond_broadcast(&loop->event_cond);
    pthread_mutex_unlock(&loop->mutex);

    /* Clear pending events (returns them to freelist) */
    event_loop_clear_pending(loop);

    /* Free the freelist itself (Phase 7: clear freelist on destruction) */
    pending_event_t *freelist_item = loop->event_freelist;
    while (freelist_item != NULL) {
        pending_event_t *next = freelist_item->next;
        enif_free(freelist_item);
        freelist_item = next;
    }
    loop->event_freelist = NULL;
    loop->freelist_count = 0;

    /* Free message environment */
    if (loop->msg_env != NULL) {
        enif_free_env(loop->msg_env);
        loop->msg_env = NULL;
    }

    /* Destroy synchronization primitives */
    pthread_mutex_destroy(&loop->mutex);
    pthread_cond_destroy(&loop->event_cond);
}

/**
 * @brief Destructor for fd resources
 *
 * Safety net: close the FD if it's still open when the resource is GC'd.
 * This should rarely happen if proper lifecycle management is used.
 */
void fd_resource_destructor(ErlNifEnv *env, void *obj) {
    fd_resource_t *fd_res = (fd_resource_t *)obj;
    (void)env;

    /* Safety net: close if still open and we own the FD */
    int state = atomic_load(&fd_res->closing_state);
    if (state != FD_STATE_CLOSED && fd_res->owns_fd && fd_res->fd >= 0) {
        close(fd_res->fd);
        fd_res->fd = -1;
    }
}

/**
 * @brief Stop callback for fd resources (enif_select stop event)
 *
 * Called when ERL_NIF_SELECT_STOP is issued. Performs proper cleanup:
 * - Atomically transitions to CLOSED state
 * - Closes FD if we own it
 * - Demonitors owner process if active
 */
void fd_resource_stop(ErlNifEnv *env, void *obj, ErlNifEvent event,
                      int is_direct_call) {
    fd_resource_t *fd_res = (fd_resource_t *)obj;
    (void)event;
    (void)is_direct_call;

    /* Atomically transition to CLOSED state */
    int expected = FD_STATE_OPEN;
    if (!atomic_compare_exchange_strong(&fd_res->closing_state,
                                        &expected, FD_STATE_CLOSED)) {
        /* Try from CLOSING state */
        expected = FD_STATE_CLOSING;
        if (!atomic_compare_exchange_strong(&fd_res->closing_state,
                                            &expected, FD_STATE_CLOSED)) {
            /* Already closed, nothing to do */
            return;
        }
    }

    /* Close FD if we own it */
    if (fd_res->owns_fd && fd_res->fd >= 0) {
        close(fd_res->fd);
        fd_res->fd = -1;
    }

    /* Demonitor if active */
    if (fd_res->monitor_active && env != NULL) {
        enif_demonitor_process(env, fd_res, &fd_res->owner_monitor);
        fd_res->monitor_active = false;
    }

    fd_res->reader_active = false;
    fd_res->writer_active = false;
}

/**
 * @brief Down callback for fd resources (owner process died)
 *
 * Called when the monitored owner process dies. Initiates cleanup:
 * - Marks monitor as inactive
 * - Transitions to CLOSING state
 * - Triggers ERL_NIF_SELECT_STOP or closes FD directly
 */
void fd_resource_down(ErlNifEnv *env, void *obj, ErlNifPid *pid,
                      ErlNifMonitor *mon) {
    fd_resource_t *fd_res = (fd_resource_t *)obj;
    (void)pid;
    (void)mon;

    /* Mark monitor as inactive */
    fd_res->monitor_active = false;

    /* Transition to CLOSING state */
    int expected = FD_STATE_OPEN;
    if (!atomic_compare_exchange_strong(&fd_res->closing_state,
                                        &expected, FD_STATE_CLOSING)) {
        /* Already closing or closed */
        return;
    }

    /* Take ownership for cleanup */
    fd_res->owns_fd = true;

    /* If select is active, trigger stop via ERL_NIF_SELECT_STOP */
    if (fd_res->reader_active || fd_res->writer_active) {
        enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_STOP,
                    fd_res, NULL, enif_make_atom(env, "owner_down"));
    } else if (fd_res->fd >= 0) {
        /* No active select, close directly */
        int exp = FD_STATE_CLOSING;
        if (atomic_compare_exchange_strong(&fd_res->closing_state,
                                           &exp, FD_STATE_CLOSED)) {
            close(fd_res->fd);
            fd_res->fd = -1;
        }
    }
}

/**
 * @brief Destructor for timer resources
 */
void timer_resource_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    (void)obj;
    /* Timer cleanup is handled via cancel_timer */
}

/* ============================================================================
 * Initialization
 * ============================================================================ */

int event_loop_init(ErlNifEnv *env) {
    /* Create event loop resource type */
    ErlNifResourceTypeInit loop_init = {
        .dtor = event_loop_destructor,
        .stop = NULL,
        .down = NULL,
        .members = 1
    };

    EVENT_LOOP_RESOURCE_TYPE = enif_init_resource_type(
        env, "erlang_event_loop", &loop_init,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (EVENT_LOOP_RESOURCE_TYPE == NULL) {
        return -1;
    }

    /* Create fd resource type with select support */
    ErlNifResourceTypeInit fd_init = {
        .dtor = fd_resource_destructor,
        .stop = fd_resource_stop,
        .down = fd_resource_down,
        .members = 3
    };

    FD_RESOURCE_TYPE = enif_init_resource_type(
        env, "fd_resource", &fd_init,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (FD_RESOURCE_TYPE == NULL) {
        return -1;
    }

    /* Create timer resource type */
    ErlNifResourceTypeInit timer_init = {
        .dtor = timer_resource_destructor,
        .stop = NULL,
        .down = NULL,
        .members = 1
    };

    TIMER_RESOURCE_TYPE = enif_init_resource_type(
        env, "timer_resource", &timer_init,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (TIMER_RESOURCE_TYPE == NULL) {
        return -1;
    }

    /* Create atoms */
    ATOM_SELECT = enif_make_atom(env, "select");
    ATOM_READY_INPUT = enif_make_atom(env, "ready_input");
    ATOM_READY_OUTPUT = enif_make_atom(env, "ready_output");
    ATOM_READ = enif_make_atom(env, "read");
    ATOM_WRITE = enif_make_atom(env, "write");
    ATOM_TIMER = enif_make_atom(env, "timer");
    ATOM_START_TIMER = enif_make_atom(env, "start_timer");
    ATOM_CANCEL_TIMER = enif_make_atom(env, "cancel_timer");
    ATOM_EVENT_LOOP = enif_make_atom(env, "event_loop");
    ATOM_DISPATCH = enif_make_atom(env, "dispatch");

    return 0;
}

void event_loop_cleanup(void) {
    /* Resource types are cleaned up by the runtime */
}

/* ============================================================================
 * Event Loop NIF Implementations
 * ============================================================================ */

/**
 * event_loop_new() -> {ok, LoopRef}
 */
ERL_NIF_TERM nif_event_loop_new(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    /* Allocate event loop resource */
    erlang_event_loop_t *loop = enif_alloc_resource(
        EVENT_LOOP_RESOURCE_TYPE, sizeof(erlang_event_loop_t));

    if (loop == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Initialize fields */
    memset(loop, 0, sizeof(erlang_event_loop_t));

    if (pthread_mutex_init(&loop->mutex, NULL) != 0) {
        enif_release_resource(loop);
        return make_error(env, "mutex_init_failed");
    }

    if (pthread_cond_init(&loop->event_cond, NULL) != 0) {
        pthread_mutex_destroy(&loop->mutex);
        enif_release_resource(loop);
        return make_error(env, "cond_init_failed");
    }

    loop->msg_env = enif_alloc_env();
    if (loop->msg_env == NULL) {
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_release_resource(loop);
        return make_error(env, "env_alloc_failed");
    }

    atomic_store(&loop->next_callback_id, 1);
    atomic_store(&loop->pending_count, 0);
    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    loop->shutdown = false;
    loop->has_router = false;
    loop->has_self = false;

    /* Create result */
    ERL_NIF_TERM loop_term = enif_make_resource(env, loop);
    enif_release_resource(loop);

    return enif_make_tuple2(env, ATOM_OK, loop_term);
}

/**
 * event_loop_destroy(LoopRef) -> ok
 */
ERL_NIF_TERM nif_event_loop_destroy(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    /* Signal shutdown */
    loop->shutdown = true;

    /* Wake up any waiting threads */
    pthread_mutex_lock(&loop->mutex);
    pthread_cond_broadcast(&loop->event_cond);
    pthread_mutex_unlock(&loop->mutex);

    return ATOM_OK;
}

/**
 * event_loop_set_router(LoopRef, RouterPid) -> ok
 */
ERL_NIF_TERM nif_event_loop_set_router(ErlNifEnv *env, int argc,
                                       const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    if (!enif_get_local_pid(env, argv[1], &loop->router_pid)) {
        return make_error(env, "invalid_pid");
    }

    loop->has_router = true;

    return ATOM_OK;
}

/**
 * event_loop_set_event_proc(LoopRef, EventProcPid) -> ok
 *
 * Set the event process for the new architecture.
 */
ERL_NIF_TERM nif_event_loop_set_event_proc(ErlNifEnv *env, int argc,
                                            const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    if (!enif_get_local_pid(env, argv[1], &loop->event_proc_pid)) {
        return make_error(env, "invalid_pid");
    }

    loop->has_event_proc = true;

    /* Also set as router for compatibility with FD registration */
    loop->router_pid = loop->event_proc_pid;
    loop->has_router = true;

    return ATOM_OK;
}

/**
 * poll_via_proc(LoopRef, TimeoutMs) -> [{CallbackId, Type}]
 *
 * Poll for events via the event process. This NIF:
 * 1. Sends {poll, self(), Ref, TimeoutMs} to event process
 * 2. Waits for {events, Ref, Events} response
 * 3. Converts Events to Erlang term and returns
 *
 * This replaces the pthread_cond based waiting with Erlang message passing.
 */
ERL_NIF_TERM nif_poll_via_proc(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    if (!loop->has_event_proc) {
        return make_error(env, "no_event_proc");
    }

    int timeout_ms;
    if (!enif_get_int(env, argv[1], &timeout_ms)) {
        return make_error(env, "invalid_timeout");
    }

    if (loop->shutdown) {
        return enif_make_list(env, 0);
    }

    /* Create message env for sending to event process */
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Create unique ref for this poll request */
    ERL_NIF_TERM ref = enif_make_ref(msg_env);

    /* Get self PID */
    ErlNifPid self_pid;
    if (enif_self(env, &self_pid) == NULL) {
        enif_free_env(msg_env);
        return make_error(env, "no_self");
    }

    /* Send {poll, From, Ref, TimeoutMs} to event process */
    ERL_NIF_TERM poll_msg = enif_make_tuple4(
        msg_env,
        enif_make_atom(msg_env, "poll"),
        enif_make_pid(msg_env, &self_pid),
        ref,
        enif_make_int(msg_env, timeout_ms)
    );

    if (!enif_send(env, &loop->event_proc_pid, msg_env, poll_msg)) {
        enif_free_env(msg_env);
        return make_error(env, "send_failed");
    }

    enif_free_env(msg_env);

    /* The actual waiting happens in Erlang - this NIF returns the ref
     * and the caller should do a receive for {events, Ref, Events} */
    return enif_make_tuple2(env, ATOM_OK, ref);
}

/**
 * add_reader(LoopRef, Fd, CallbackId) -> {ok, FdRef}
 */
ERL_NIF_TERM nif_add_reader(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    int fd;
    if (!enif_get_int(env, argv[1], &fd)) {
        return make_error(env, "invalid_fd");
    }

    ErlNifUInt64 callback_id;
    if (!enif_get_uint64(env, argv[2], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }

    if (!loop->has_router) {
        return make_error(env, "no_router");
    }

    /* Allocate fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE,
                                                 sizeof(fd_resource_t));
    if (fd_res == NULL) {
        return make_error(env, "alloc_failed");
    }

    fd_res->fd = fd;
    fd_res->read_callback_id = callback_id;
    fd_res->write_callback_id = 0;
    fd_res->owner_pid = loop->router_pid;
    fd_res->reader_active = true;
    fd_res->writer_active = false;
    fd_res->loop = loop;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Monitor owner process for cleanup on death */
    if (enif_monitor_process(env, fd_res, &loop->router_pid,
                             &fd_res->owner_monitor) == 0) {
        fd_res->monitor_active = true;
    }

    /* Register with Erlang scheduler for read monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd, ERL_NIF_SELECT_READ,
                          fd_res, &loop->router_pid, enif_make_ref(env));

    if (ret < 0) {
        if (fd_res->monitor_active) {
            enif_demonitor_process(env, fd_res, &fd_res->owner_monitor);
        }
        enif_release_resource(fd_res);
        return make_error(env, "select_failed");
    }

    ERL_NIF_TERM fd_term = enif_make_resource(env, fd_res);
    /* Keep the owner's reference - will be released in remove_reader.
     * This ensures the fd_res stays alive while registered for select. */

    return enif_make_tuple2(env, ATOM_OK, fd_term);
}

/**
 * remove_reader(LoopRef, FdRef) -> ok
 *
 * FdRef must be the same resource returned by add_reader.
 */
ERL_NIF_TERM nif_remove_reader(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[1], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    if (!fd_res->reader_active) {
        return ATOM_OK;  /* Already removed */
    }

    /* Stop monitoring for reads using the same resource */
    enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_STOP,
                fd_res, NULL, enif_make_atom(env, "undefined"));

    fd_res->reader_active = false;

    /* Release the owner's reference that was kept in add_reader */
    if (!fd_res->writer_active) {
        enif_release_resource(fd_res);
    }

    return ATOM_OK;
}

/**
 * add_writer(LoopRef, Fd, CallbackId) -> {ok, FdRef}
 */
ERL_NIF_TERM nif_add_writer(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    int fd;
    if (!enif_get_int(env, argv[1], &fd)) {
        return make_error(env, "invalid_fd");
    }

    ErlNifUInt64 callback_id;
    if (!enif_get_uint64(env, argv[2], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }

    if (!loop->has_router) {
        return make_error(env, "no_router");
    }

    /* Allocate fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE,
                                                 sizeof(fd_resource_t));
    if (fd_res == NULL) {
        return make_error(env, "alloc_failed");
    }

    fd_res->fd = fd;
    fd_res->read_callback_id = 0;
    fd_res->write_callback_id = callback_id;
    fd_res->owner_pid = loop->router_pid;
    fd_res->reader_active = false;
    fd_res->writer_active = true;
    fd_res->loop = loop;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Monitor owner process for cleanup on death */
    if (enif_monitor_process(env, fd_res, &loop->router_pid,
                             &fd_res->owner_monitor) == 0) {
        fd_res->monitor_active = true;
    }

    /* Register with Erlang scheduler for write monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd, ERL_NIF_SELECT_WRITE,
                          fd_res, &loop->router_pid, enif_make_ref(env));

    if (ret < 0) {
        if (fd_res->monitor_active) {
            enif_demonitor_process(env, fd_res, &fd_res->owner_monitor);
        }
        enif_release_resource(fd_res);
        return make_error(env, "select_failed");
    }

    ERL_NIF_TERM fd_term = enif_make_resource(env, fd_res);
    /* Keep the owner's reference - will be released in remove_writer.
     * This ensures the fd_res stays alive while registered for select. */

    return enif_make_tuple2(env, ATOM_OK, fd_term);
}

/**
 * remove_writer(LoopRef, FdRef) -> ok
 *
 * FdRef must be the same resource returned by add_writer.
 */
ERL_NIF_TERM nif_remove_writer(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[1], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    if (!fd_res->writer_active) {
        return ATOM_OK;  /* Already removed */
    }

    /* Stop monitoring for writes using the same resource */
    enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_STOP,
                fd_res, NULL, enif_make_atom(env, "undefined"));

    fd_res->writer_active = false;

    /* Release the owner's reference that was kept in add_writer */
    if (!fd_res->reader_active) {
        enif_release_resource(fd_res);
    }

    return ATOM_OK;
}

/**
 * call_later(LoopRef, DelayMs, CallbackId) -> {ok, TimerRef}
 *
 * Sends a message to the router to create a timer using erlang:send_after.
 */
ERL_NIF_TERM nif_call_later(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    int delay_ms;
    if (!enif_get_int(env, argv[1], &delay_ms)) {
        return make_error(env, "invalid_delay");
    }

    ErlNifUInt64 callback_id;
    if (!enif_get_uint64(env, argv[2], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }

    if (!loop->has_router) {
        return make_error(env, "no_router");
    }

    /* Create timer reference */
    ERL_NIF_TERM timer_ref = enif_make_ref(env);

    /* Send message to router: {start_timer, DelayMs, CallbackId, TimerRef} */
    ERL_NIF_TERM msg = enif_make_tuple4(
        env,
        ATOM_START_TIMER,
        enif_make_int(env, delay_ms),
        enif_make_uint64(env, callback_id),
        timer_ref
    );

    if (!enif_send(env, &loop->router_pid, NULL, msg)) {
        return make_error(env, "send_failed");
    }

    return enif_make_tuple2(env, ATOM_OK, timer_ref);
}

/**
 * cancel_timer(LoopRef, TimerRef) -> ok
 */
ERL_NIF_TERM nif_cancel_timer(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    ERL_NIF_TERM timer_ref = argv[1];

    if (!loop->has_router) {
        return make_error(env, "no_router");
    }

    /* Send message to router: {cancel_timer, TimerRef} */
    ERL_NIF_TERM msg = enif_make_tuple2(env, ATOM_CANCEL_TIMER, timer_ref);

    if (!enif_send(env, &loop->router_pid, NULL, msg)) {
        return make_error(env, "send_failed");
    }

    return ATOM_OK;
}

/**
 * Helper function to wait for events (called with or without GIL)
 */
static int poll_events_wait(erlang_event_loop_t *loop, int timeout_ms) {
    int num_events = 0;

    pthread_mutex_lock(&loop->mutex);

    int current_count = atomic_load(&loop->pending_count);
    if (current_count == 0 && !loop->shutdown) {
        /* No events, wait with timeout */
        if (timeout_ms > 0) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += timeout_ms / 1000;
            ts.tv_nsec += (timeout_ms % 1000) * 1000000;
            if (ts.tv_nsec >= 1000000000) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000;
            }
            pthread_cond_timedwait(&loop->event_cond, &loop->mutex, &ts);
        } else if (timeout_ms == 0) {
            /* No wait, just check */
        } else {
            /* Infinite wait */
            pthread_cond_wait(&loop->event_cond, &loop->mutex);
        }
    }

    num_events = atomic_load(&loop->pending_count);
    pthread_mutex_unlock(&loop->mutex);

    return num_events;
}

/**
 * poll_events(LoopRef, TimeoutMs) -> {ok, NumEvents}
 *
 * Waits for events with timeout. If called with GIL held (from Python),
 * releases GIL while waiting.
 * This is marked as a dirty NIF (ERL_NIF_DIRTY_JOB_IO_BOUND).
 */
ERL_NIF_TERM nif_poll_events(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    int timeout_ms;
    if (!enif_get_int(env, argv[1], &timeout_ms)) {
        return make_error(env, "invalid_timeout");
    }

    if (loop->shutdown) {
        return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, 0));
    }

    int num_events = 0;

    /*
     * Check if we have a valid Python thread state AND the GIL before releasing it.
     * PyGILState_Check() can return true even with NULL thread state on some platforms.
     * PyGILState_GetThisThreadState() returns NULL if thread has no Python state.
     */
    PyThreadState *tstate = PyGILState_GetThisThreadState();
    if (tstate != NULL && g_python_initialized && PyGILState_Check()) {
        /* We have valid thread state and GIL - release it while waiting */
        Py_BEGIN_ALLOW_THREADS
        num_events = poll_events_wait(loop, timeout_ms);
        Py_END_ALLOW_THREADS
    } else {
        /* No GIL or invalid thread state - just wait directly */
        num_events = poll_events_wait(loop, timeout_ms);
    }

    return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, num_events));
}

/* Forward declaration for hash set clear function (defined with other hash functions) */
static inline void pending_hash_clear(erlang_event_loop_t *loop);

/**
 * get_pending(LoopRef) -> [{CallbackId, Type}]
 *
 * Returns and clears the list of pending events.
 */
ERL_NIF_TERM nif_get_pending(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return enif_make_list(env, 0);
    }

    /*
     * Phase 1: Detach pending list under lock (fast - just pointer swap)
     * This minimizes lock contention by doing minimal work under the mutex.
     */
    pthread_mutex_lock(&loop->mutex);

    pending_event_t *snapshot_head = loop->pending_head;
    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);

    /* Clear the hash set since we're consuming all pending events */
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);

    /*
     * Phase 2: Build Erlang list outside lock (no contention)
     * Term creation and memory operations happen without holding the mutex.
     */
    ERL_NIF_TERM list = enif_make_list(env, 0);
    pending_event_t *current = snapshot_head;

    while (current != NULL) {
        ERL_NIF_TERM type_atom;
        switch (current->type) {
            case EVENT_TYPE_READ:
                type_atom = ATOM_READ;
                break;
            case EVENT_TYPE_WRITE:
                type_atom = ATOM_WRITE;
                break;
            case EVENT_TYPE_TIMER:
                type_atom = ATOM_TIMER;
                break;
            default:
                type_atom = ATOM_UNDEFINED;
        }

        ERL_NIF_TERM event = enif_make_tuple2(
            env,
            enif_make_uint64(env, current->callback_id),
            type_atom
        );

        list = enif_make_list_cell(env, event, list);
        pending_event_t *next = current->next;
        enif_free(current);
        current = next;
    }

    /* Reverse the list to maintain order */
    ERL_NIF_TERM reversed = enif_make_list(env, 0);
    ERL_NIF_TERM head;
    while (enif_get_list_cell(env, list, &head, &list)) {
        reversed = enif_make_list_cell(env, head, reversed);
    }

    return reversed;
}

/**
 * dispatch_callback(LoopRef, CallbackId, Type) -> ok
 *
 * Called by py_event_router when an event occurs.
 */
ERL_NIF_TERM nif_dispatch_callback(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    ErlNifUInt64 callback_id;
    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }

    /* Determine event type from atom */
    event_type_t type = EVENT_TYPE_READ;
    if (enif_compare(argv[2], ATOM_WRITE) == 0) {
        type = EVENT_TYPE_WRITE;
    } else if (enif_compare(argv[2], ATOM_TIMER) == 0) {
        type = EVENT_TYPE_TIMER;
    }

    event_loop_add_pending(loop, type, callback_id, -1);

    return ATOM_OK;
}

/**
 * dispatch_timer(LoopRef, CallbackId) -> ok
 *
 * Called when a timer expires.
 */
ERL_NIF_TERM nif_dispatch_timer(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    ErlNifUInt64 callback_id;
    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }

    event_loop_add_pending(loop, EVENT_TYPE_TIMER, callback_id, -1);

    return ATOM_OK;
}

/**
 * handle_fd_event(FdRes, Type) -> ok | {error, Reason}
 *
 * Handles a select event by dispatching callback to pending queue.
 * This combines get_fd_callback_id + dispatch_callback into one NIF call.
 * Called by py_event_router when receiving {select, FdRes, Ref, ready_input/output}.
 *
 * NOTE: Does NOT auto-reselect to avoid infinite loops with level-triggered FDs.
 * Python should call start_reader/start_writer after processing the callback
 * to re-enable monitoring for the next event.
 *
 * Type: read | write
 */
ERL_NIF_TERM nif_handle_fd_event(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Check if FD is still open */
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return ATOM_OK;  /* Silently ignore events on closing FDs */
    }

    erlang_event_loop_t *loop = fd_res->loop;
    if (loop == NULL) {
        return make_error(env, "no_loop");
    }

    /* Determine type and get callback ID */
    bool is_read = enif_compare(argv[1], ATOM_READ) == 0;
    uint64_t callback_id;
    bool is_active;

    if (is_read) {
        callback_id = fd_res->read_callback_id;
        is_active = fd_res->reader_active;
    } else {
        callback_id = fd_res->write_callback_id;
        is_active = fd_res->writer_active;
    }

    if (!is_active || callback_id == 0) {
        return ATOM_OK;  /* Watcher was stopped, ignore */
    }

    /* Add to pending queue */
    event_type_t event_type = is_read ? EVENT_TYPE_READ : EVENT_TYPE_WRITE;
    event_loop_add_pending(loop, event_type, callback_id, fd_res->fd);

    /* Note: No auto-reselect here. Python event loop should call start_reader/start_writer
     * after reading/writing data to re-enable monitoring. This prevents infinite loops
     * when FD remains ready (level-triggered behavior). */

    return ATOM_OK;
}

/**
 * handle_fd_event_and_reselect(FdRes, Type) -> ok | {error, Reason}
 *
 * Combined NIF that handles a select event and reselects in one call.
 * This reduces NIF overhead by combining:
 * 1. Get callback ID from fd_res
 * 2. Dispatch to pending queue
 * 3. Re-register with enif_select
 *
 * Type: read | write
 */
ERL_NIF_TERM nif_handle_fd_event_and_reselect(ErlNifEnv *env, int argc,
                                               const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Check if FD is still open */
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return ATOM_OK;  /* Silently ignore events on closing FDs */
    }

    erlang_event_loop_t *loop = fd_res->loop;
    if (loop == NULL) {
        return make_error(env, "no_loop");
    }

    /* Determine type and get callback ID */
    bool is_read = enif_compare(argv[1], ATOM_READ) == 0;
    uint64_t callback_id;
    bool is_active;

    if (is_read) {
        callback_id = fd_res->read_callback_id;
        is_active = fd_res->reader_active;
    } else {
        callback_id = fd_res->write_callback_id;
        is_active = fd_res->writer_active;
    }

    if (!is_active || callback_id == 0) {
        return ATOM_OK;  /* Watcher was stopped, ignore */
    }

    /* Add to pending queue */
    event_type_t event_type = is_read ? EVENT_TYPE_READ : EVENT_TYPE_WRITE;
    event_loop_add_pending(loop, event_type, callback_id, fd_res->fd);

    /* Re-register with enif_select for next event */
    if (!loop->has_router) {
        return make_error(env, "no_router");
    }

    int select_mode = is_read ? ERL_NIF_SELECT_READ : ERL_NIF_SELECT_WRITE;
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, select_mode,
                          fd_res, &loop->router_pid, enif_make_ref(env));

    if (ret < 0) {
        /* Event was queued but reselect failed - log but don't fail */
        return make_error(env, "reselect_failed");
    }

    return ATOM_OK;
}

/**
 * event_loop_wakeup(LoopRef) -> ok
 *
 * Wakes up any threads waiting in poll_events.
 */
ERL_NIF_TERM nif_event_loop_wakeup(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    pthread_mutex_lock(&loop->mutex);
    pthread_cond_broadcast(&loop->event_cond);
    pthread_mutex_unlock(&loop->mutex);

    return ATOM_OK;
}

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

/* ============================================================================
 * Pending Event Freelist (Phase 7 optimization)
 *
 * Avoid malloc/free overhead by maintaining a freelist of pending_event_t
 * structures. Events are returned to the freelist after processing.
 * ============================================================================ */

/**
 * @brief Get a pending_event_t from freelist or allocate new
 *
 * @param loop Event loop containing the freelist
 * @return pending_event_t* or NULL on allocation failure
 */
static inline pending_event_t *get_pending_event(erlang_event_loop_t *loop) {
    if (loop->event_freelist != NULL) {
        pending_event_t *event = loop->event_freelist;
        loop->event_freelist = event->next;
        loop->freelist_count--;
        return event;
    }
    return enif_alloc(sizeof(pending_event_t));
}

/**
 * @brief Return a pending_event_t to the freelist or free it
 *
 * @param loop Event loop containing the freelist
 * @param event Event to return
 */
static inline void return_pending_event(erlang_event_loop_t *loop,
                                         pending_event_t *event) {
    if (loop->freelist_count < EVENT_FREELIST_SIZE) {
        event->next = loop->event_freelist;
        loop->event_freelist = event;
        loop->freelist_count++;
    } else {
        enif_free(event);
    }
}

/* ============================================================================
 * Pending Event Hash Set (O(1) duplicate detection)
 *
 * Uses open addressing with linear probing. Key is (callback_id, type)
 * combined into a single uint64_t.
 * ============================================================================ */

/**
 * @brief Compute hash key from callback_id and event type
 */
static inline uint64_t pending_hash_key(uint64_t callback_id, event_type_t type) {
    /* Combine callback_id and type into a single key */
    return (callback_id << 2) | (uint64_t)type;
}

/**
 * @brief Compute hash bucket index
 */
static inline uint32_t pending_hash_index(uint64_t key) {
    /* Simple hash: XOR fold and modulo */
    return (uint32_t)((key ^ (key >> 32)) % PENDING_HASH_SIZE);
}

/**
 * @brief Check if a (callback_id, type) pair exists in the hash set
 *
 * @param loop Event loop containing the hash set
 * @param callback_id Callback ID to check
 * @param type Event type to check
 * @return true if exists, false otherwise
 */
static inline bool pending_hash_contains(erlang_event_loop_t *loop,
                                          uint64_t callback_id, event_type_t type) {
    if (loop->pending_hash_count == 0) {
        return false;
    }

    uint64_t key = pending_hash_key(callback_id, type);
    uint32_t idx = pending_hash_index(key);

    /* Linear probing */
    for (int i = 0; i < PENDING_HASH_SIZE; i++) {
        uint32_t probe = (idx + i) % PENDING_HASH_SIZE;
        if (!loop->pending_hash_occupied[probe]) {
            return false;  /* Empty slot means key not present */
        }
        if (loop->pending_hash_keys[probe] == key) {
            return true;
        }
    }
    return false;  /* Table full, key not found */
}

/**
 * @brief Insert a (callback_id, type) pair into the hash set
 *
 * @param loop Event loop containing the hash set
 * @param callback_id Callback ID to insert
 * @param type Event type to insert
 * @return true if inserted, false if already exists or table full
 */
static inline bool pending_hash_insert(erlang_event_loop_t *loop,
                                        uint64_t callback_id, event_type_t type) {
    /* Don't insert if table is too full (load factor > 0.75) */
    if (loop->pending_hash_count >= (PENDING_HASH_SIZE * 3) / 4) {
        return false;
    }

    uint64_t key = pending_hash_key(callback_id, type);
    uint32_t idx = pending_hash_index(key);

    /* Linear probing */
    for (int i = 0; i < PENDING_HASH_SIZE; i++) {
        uint32_t probe = (idx + i) % PENDING_HASH_SIZE;
        if (!loop->pending_hash_occupied[probe]) {
            loop->pending_hash_keys[probe] = key;
            loop->pending_hash_occupied[probe] = true;
            loop->pending_hash_count++;
            return true;
        }
        if (loop->pending_hash_keys[probe] == key) {
            return false;  /* Already exists */
        }
    }
    return false;  /* Table full */
}

/**
 * @brief Clear the pending hash set
 *
 * @param loop Event loop containing the hash set
 */
static inline void pending_hash_clear(erlang_event_loop_t *loop) {
    if (loop->pending_hash_count > 0) {
        memset(loop->pending_hash_occupied, 0, sizeof(loop->pending_hash_occupied));
        loop->pending_hash_count = 0;
    }
}

void event_loop_add_pending(erlang_event_loop_t *loop, event_type_t type,
                            uint64_t callback_id, int fd) {
    pthread_mutex_lock(&loop->mutex);

    /* O(1) duplicate check using hash set */
    if (pending_hash_contains(loop, callback_id, type)) {
        /* Already have this event pending, skip */
        pthread_mutex_unlock(&loop->mutex);
        return;
    }

    /* Get event from freelist or allocate new (Phase 7 optimization) */
    pending_event_t *event = get_pending_event(loop);
    if (event == NULL) {
        pthread_mutex_unlock(&loop->mutex);
        return;
    }

    event->type = type;
    event->callback_id = callback_id;
    event->fd = fd;
    event->next = NULL;

    /* Track if queue was empty before insert for wake optimization */
    bool was_empty = (loop->pending_head == NULL);

    if (loop->pending_tail == NULL) {
        loop->pending_head = event;
        loop->pending_tail = event;
    } else {
        loop->pending_tail->next = event;
        loop->pending_tail = event;
    }

    /* Add to hash set for future O(1) duplicate checks */
    pending_hash_insert(loop, callback_id, type);

    atomic_fetch_add(&loop->pending_count, 1);

    /* Only wake poller on 0->1 transition to reduce contention */
    if (was_empty) {
        pthread_cond_signal(&loop->event_cond);
    }

    pthread_mutex_unlock(&loop->mutex);
}

void event_loop_clear_pending(erlang_event_loop_t *loop) {
    pthread_mutex_lock(&loop->mutex);

    pending_event_t *current = loop->pending_head;
    while (current != NULL) {
        pending_event_t *next = current->next;
        /* Return to freelist for reuse (Phase 7 optimization) */
        return_pending_event(loop, current);
        current = next;
    }

    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);

    /* Clear the hash set since all pending events are cleared */
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);
}

/**
 * get_fd_callback_id(FdRes, Type) -> CallbackId | undefined
 *
 * Get the callback ID from an fd resource.
 */
ERL_NIF_TERM nif_get_fd_callback_id(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return ATOM_UNDEFINED;
    }

    /* Determine which callback ID to return based on type */
    uint64_t callback_id = 0;
    if (enif_compare(argv[1], ATOM_READ) == 0) {
        if (fd_res->reader_active) {
            callback_id = fd_res->read_callback_id;
        } else {
            return ATOM_UNDEFINED;
        }
    } else if (enif_compare(argv[1], ATOM_WRITE) == 0) {
        if (fd_res->writer_active) {
            callback_id = fd_res->write_callback_id;
        } else {
            return ATOM_UNDEFINED;
        }
    } else {
        return ATOM_UNDEFINED;
    }

    return enif_make_uint64(env, callback_id);
}

/**
 * reselect_reader(LoopRef, FdRes) -> ok | {error, Reason}
 *
 * Re-register an fd for read monitoring after an event is delivered.
 * enif_select is one-shot, so we need to re-register after each event.
 */
ERL_NIF_TERM nif_reselect_reader(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[1], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Don't reselect if reader was removed or FD is closing */
    if (!fd_res->reader_active) {
        return ATOM_OK;
    }
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return ATOM_OK;
    }

    /* Re-register with Erlang scheduler for read monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_READ,
                          fd_res, &loop->router_pid, enif_make_ref(env));

    if (ret < 0) {
        return make_error(env, "reselect_failed");
    }

    return ATOM_OK;
}

/**
 * reselect_writer(LoopRef, FdRes) -> ok | {error, Reason}
 *
 * Re-register an fd for write monitoring after an event is delivered.
 * enif_select is one-shot, so we need to re-register after each event.
 */
ERL_NIF_TERM nif_reselect_writer(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[1], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Don't reselect if writer was removed or FD is closing */
    if (!fd_res->writer_active) {
        return ATOM_OK;
    }
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return ATOM_OK;
    }

    /* Re-register with Erlang scheduler for write monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_WRITE,
                          fd_res, &loop->router_pid, enif_make_ref(env));

    if (ret < 0) {
        return make_error(env, "reselect_failed");
    }

    return ATOM_OK;
}

/**
 * reselect_reader_fd(FdRes) -> ok | {error, Reason}
 *
 * Re-register an fd for read monitoring using fd_res->loop.
 * This variant doesn't require LoopRef since the fd resource
 * already has a back-reference to its parent loop.
 */
ERL_NIF_TERM nif_reselect_reader_fd(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Don't reselect if reader was removed or FD is closing */
    if (!fd_res->reader_active) {
        return ATOM_OK;
    }
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return ATOM_OK;
    }

    /* Use the loop stored in the fd resource */
    erlang_event_loop_t *loop = fd_res->loop;
    if (loop == NULL || !loop->has_router) {
        return make_error(env, "no_loop");
    }

    /* Re-register with Erlang scheduler for read monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_READ,
                          fd_res, &loop->router_pid, enif_make_ref(env));

    if (ret < 0) {
        return make_error(env, "reselect_failed");
    }

    return ATOM_OK;
}

/**
 * reselect_writer_fd(FdRes) -> ok | {error, Reason}
 *
 * Re-register an fd for write monitoring using fd_res->loop.
 * This variant doesn't require LoopRef since the fd resource
 * already has a back-reference to its parent loop.
 */
ERL_NIF_TERM nif_reselect_writer_fd(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Don't reselect if writer was removed or FD is closing */
    if (!fd_res->writer_active) {
        return ATOM_OK;
    }
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return ATOM_OK;
    }

    /* Use the loop stored in the fd resource */
    erlang_event_loop_t *loop = fd_res->loop;
    if (loop == NULL || !loop->has_router) {
        return make_error(env, "no_loop");
    }

    /* Re-register with Erlang scheduler for write monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_WRITE,
                          fd_res, &loop->router_pid, enif_make_ref(env));

    if (ret < 0) {
        return make_error(env, "reselect_failed");
    }

    return ATOM_OK;
}

/**
 * stop_reader(FdRef) -> ok | {error, Reason}
 *
 * Stop/pause read monitoring WITHOUT closing the FD.
 * The watcher still exists and can be restarted with start_reader.
 * (Alias: cancel_reader for backward compatibility)
 */
ERL_NIF_TERM nif_stop_reader(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    if (!fd_res->reader_active) {
        return ATOM_OK;  /* Already stopped */
    }

    /* Check if FD is closing */
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return make_error(env, "fd_closing");
    }

    /* Cancel read monitoring using ERL_NIF_SELECT_CANCEL */
    enif_select(env, (ErlNifEvent)fd_res->fd,
                ERL_NIF_SELECT_CANCEL | ERL_NIF_SELECT_READ,
                fd_res, NULL, ATOM_UNDEFINED);

    fd_res->reader_active = false;

    return ATOM_OK;
}

/**
 * start_reader(FdRef) -> ok | {error, Reason}
 *
 * Start/resume read monitoring on an existing watcher.
 * Must have been created with add_reader first.
 */
ERL_NIF_TERM nif_start_reader(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Check if FD is closing */
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return make_error(env, "fd_closing");
    }

    erlang_event_loop_t *loop = fd_res->loop;
    if (loop == NULL || !loop->has_router) {
        return make_error(env, "no_loop");
    }

    /* Register with Erlang scheduler for read monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_READ,
                          fd_res, &loop->router_pid, ATOM_UNDEFINED);

    if (ret < 0) {
        return make_error(env, "select_failed");
    }

    fd_res->reader_active = true;

    return ATOM_OK;
}

/**
 * stop_writer(FdRef) -> ok | {error, Reason}
 *
 * Stop/pause write monitoring WITHOUT closing the FD.
 * The watcher still exists and can be restarted with start_writer.
 * (Alias: cancel_writer for backward compatibility)
 */
ERL_NIF_TERM nif_stop_writer(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    if (!fd_res->writer_active) {
        return ATOM_OK;  /* Already stopped */
    }

    /* Check if FD is closing */
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return make_error(env, "fd_closing");
    }

    /* Cancel write monitoring using ERL_NIF_SELECT_CANCEL */
    enif_select(env, (ErlNifEvent)fd_res->fd,
                ERL_NIF_SELECT_CANCEL | ERL_NIF_SELECT_WRITE,
                fd_res, NULL, ATOM_UNDEFINED);

    fd_res->writer_active = false;

    return ATOM_OK;
}

/**
 * start_writer(FdRef) -> ok | {error, Reason}
 *
 * Start/resume write monitoring on an existing watcher.
 * Must have been created with add_writer first.
 */
ERL_NIF_TERM nif_start_writer(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Check if FD is closing */
    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return make_error(env, "fd_closing");
    }

    erlang_event_loop_t *loop = fd_res->loop;
    if (loop == NULL || !loop->has_router) {
        return make_error(env, "no_loop");
    }

    /* Register with Erlang scheduler for write monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_WRITE,
                          fd_res, &loop->router_pid, ATOM_UNDEFINED);

    if (ret < 0) {
        return make_error(env, "select_failed");
    }

    fd_res->writer_active = true;

    return ATOM_OK;
}

/* Legacy aliases for backward compatibility */
ERL_NIF_TERM nif_cancel_reader(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
    /* cancel_reader(Loop, FdRef) -> stop_reader(FdRef) */
    (void)argc;
    ERL_NIF_TERM new_argv[1] = {argv[1]};  /* Skip Loop arg */
    return nif_stop_reader(env, 1, new_argv);
}

ERL_NIF_TERM nif_cancel_writer(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
    /* cancel_writer(Loop, FdRef) -> stop_writer(FdRef) */
    (void)argc;
    ERL_NIF_TERM new_argv[1] = {argv[1]};  /* Skip Loop arg */
    return nif_stop_writer(env, 1, new_argv);
}

/**
 * close_fd(FdRef) -> ok
 *
 * Explicitly close an FD with proper lifecycle cleanup.
 * Transfers ownership and triggers proper cleanup via ERL_NIF_SELECT_STOP.
 * Safe to call multiple times (idempotent).
 */
ERL_NIF_TERM nif_close_fd(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    /* Atomically transition to CLOSING state */
    int expected = FD_STATE_OPEN;
    if (!atomic_compare_exchange_strong(&fd_res->closing_state,
                                        &expected, FD_STATE_CLOSING)) {
        /* Already closing or closed - this is idempotent */
        return ATOM_OK;
    }

    /* Take ownership of the FD for cleanup */
    fd_res->owns_fd = true;

    /* If select is active, trigger stop which will close the FD */
    if (fd_res->reader_active || fd_res->writer_active) {
        enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_STOP,
                    fd_res, NULL, enif_make_atom(env, "explicit_close"));
    } else {
        /* No active select, close directly */
        atomic_store(&fd_res->closing_state, FD_STATE_CLOSED);
        if (fd_res->fd >= 0) {
            close(fd_res->fd);
            fd_res->fd = -1;
        }
        /* Demonitor if active */
        if (fd_res->monitor_active) {
            enif_demonitor_process(env, fd_res, &fd_res->owner_monitor);
            fd_res->monitor_active = false;
        }
    }

    return ATOM_OK;
}

/* ============================================================================
 * Test Helper Functions (for testing fd monitoring with pipes)
 * ============================================================================ */

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <string.h>

/**
 * create_test_pipe() -> {ok, {ReadFd, WriteFd}} | {error, Reason}
 *
 * Creates a pipe for testing fd monitoring. Pipes work well with enif_select
 * unlike gen_tcp sockets which have internal Erlang management that conflicts.
 */
ERL_NIF_TERM nif_create_test_pipe(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    int pipefd[2];
    if (pipe(pipefd) == -1) {
        return make_error(env, "pipe_failed");
    }

    /* Set both ends to non-blocking */
    int flags = fcntl(pipefd[0], F_GETFL, 0);
    if (flags != -1) {
        fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);
    }
    flags = fcntl(pipefd[1], F_GETFL, 0);
    if (flags != -1) {
        fcntl(pipefd[1], F_SETFL, flags | O_NONBLOCK);
    }

    ERL_NIF_TERM result = enif_make_tuple2(
        env,
        enif_make_int(env, pipefd[0]),  /* read end */
        enif_make_int(env, pipefd[1])   /* write end */
    );

    return enif_make_tuple2(env, ATOM_OK, result);
}

/**
 * close_test_fd(Fd) -> ok | {error, Reason}
 *
 * Closes a file descriptor created by create_test_pipe.
 */
ERL_NIF_TERM nif_close_test_fd(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    if (close(fd) == -1) {
        return make_error(env, "close_failed");
    }

    return ATOM_OK;
}

/**
 * write_test_fd(Fd, Data) -> ok | {error, Reason}
 *
 * Writes binary data to a file descriptor.
 */
ERL_NIF_TERM nif_write_test_fd(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, argv[1], &bin)) {
        return make_error(env, "invalid_data");
    }

    ssize_t written = write(fd, bin.data, bin.size);
    if (written == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return make_error(env, "would_block");
        }
        return make_error(env, "write_failed");
    }

    return ATOM_OK;
}

/**
 * read_test_fd(Fd, MaxSize) -> {ok, Data} | {error, Reason}
 *
 * Reads data from a file descriptor.
 */
ERL_NIF_TERM nif_read_test_fd(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    int max_size;
    if (!enif_get_int(env, argv[1], &max_size)) {
        return make_error(env, "invalid_size");
    }

    if (max_size <= 0 || max_size > 65536) {
        max_size = 4096;
    }

    ErlNifBinary bin;
    if (!enif_alloc_binary(max_size, &bin)) {
        return make_error(env, "alloc_failed");
    }

    ssize_t n = read(fd, bin.data, bin.size);
    if (n == -1) {
        enif_release_binary(&bin);
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return make_error(env, "would_block");
        }
        return make_error(env, "read_failed");
    }

    if (n == 0) {
        enif_release_binary(&bin);
        return make_error(env, "eof");
    }

    if ((size_t)n < bin.size) {
        enif_realloc_binary(&bin, n);
    }

    return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
}

/* ============================================================================
 * TCP Test Helper Functions
 * ============================================================================ */

/**
 * create_test_tcp_listener(Port) -> {ok, {ListenFd, ActualPort}} | {error, Reason}
 *
 * Creates a TCP listener socket for testing. If Port is 0, an ephemeral port
 * is assigned by the OS.
 */
ERL_NIF_TERM nif_create_test_tcp_listener(ErlNifEnv *env, int argc,
                                           const ERL_NIF_TERM argv[]) {
    (void)argc;

    int port;
    if (!enif_get_int(env, argv[0], &port)) {
        return make_error(env, "invalid_port");
    }

    /* Create socket */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        return make_error(env, "socket_failed");
    }

    /* Set SO_REUSEADDR */
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(listen_fd);
        return make_error(env, "setsockopt_failed");
    }

    /* Set non-blocking */
    int flags = fcntl(listen_fd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);
    }

    /* Bind */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(listen_fd);
        return make_error(env, "bind_failed");
    }

    /* Get actual port if ephemeral */
    socklen_t addr_len = sizeof(addr);
    if (getsockname(listen_fd, (struct sockaddr *)&addr, &addr_len) < 0) {
        close(listen_fd);
        return make_error(env, "getsockname_failed");
    }
    int actual_port = ntohs(addr.sin_port);

    /* Listen */
    if (listen(listen_fd, 5) < 0) {
        close(listen_fd);
        return make_error(env, "listen_failed");
    }

    ERL_NIF_TERM result = enif_make_tuple2(
        env,
        enif_make_int(env, listen_fd),
        enif_make_int(env, actual_port)
    );

    return enif_make_tuple2(env, ATOM_OK, result);
}

/**
 * accept_test_tcp(ListenFd) -> {ok, ClientFd} | {error, Reason}
 *
 * Accepts a connection on a TCP listener socket.
 */
ERL_NIF_TERM nif_accept_test_tcp(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    int listen_fd;
    if (!enif_get_int(env, argv[0], &listen_fd)) {
        return make_error(env, "invalid_fd");
    }

    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
    if (client_fd == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return make_error(env, "would_block");
        }
        return make_error(env, "accept_failed");
    }

    /* Set non-blocking */
    int flags = fcntl(client_fd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
    }

    /* Disable Nagle's algorithm for lower latency */
    int opt = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, client_fd));
}

/**
 * connect_test_tcp(Host, Port) -> {ok, Fd} | {error, Reason}
 *
 * Connects to a TCP server. Host should be "127.0.0.1" or similar.
 */
ERL_NIF_TERM nif_connect_test_tcp(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
    (void)argc;

    /* Get host - we expect a binary like <<"127.0.0.1">> */
    ErlNifBinary host_bin;
    if (!enif_inspect_binary(env, argv[0], &host_bin)) {
        return make_error(env, "invalid_host");
    }

    /* Null-terminate the host string */
    char host[256];
    size_t host_len = host_bin.size < 255 ? host_bin.size : 255;
    memcpy(host, host_bin.data, host_len);
    host[host_len] = '\0';

    int port;
    if (!enif_get_int(env, argv[1], &port)) {
        return make_error(env, "invalid_port");
    }

    /* Create socket */
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd == -1) {
        return make_error(env, "socket_failed");
    }

    /* Set non-blocking before connect for async connect */
    int flags = fcntl(sock_fd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);
    }

    /* Disable Nagle's algorithm */
    int opt = 1;
    setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    /* Connect */
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &server_addr.sin_addr) <= 0) {
        close(sock_fd);
        return make_error(env, "invalid_address");
    }

    int ret = connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    if (ret < 0 && errno != EINPROGRESS) {
        close(sock_fd);
        return make_error(env, "connect_failed");
    }

    return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, sock_fd));
}

/* ============================================================================
 * UDP Test Helper Functions
 * ============================================================================ */

/**
 * create_test_udp_socket(Port) -> {ok, {Fd, ActualPort}} | {error, Reason}
 *
 * Creates a UDP socket for testing. If Port is 0, an ephemeral port
 * is assigned by the OS.
 */
ERL_NIF_TERM nif_create_test_udp_socket(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    int port;
    if (!enif_get_int(env, argv[0], &port)) {
        return make_error(env, "invalid_port");
    }

    /* Create UDP socket */
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd == -1) {
        return make_error(env, "socket_failed");
    }

    /* Set SO_REUSEADDR */
    int opt = 1;
    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(sock_fd);
        return make_error(env, "setsockopt_failed");
    }

    /* Set non-blocking */
    int flags = fcntl(sock_fd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);
    }

    /* Bind */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock_fd);
        return make_error(env, "bind_failed");
    }

    /* Get actual port if ephemeral */
    socklen_t addr_len = sizeof(addr);
    if (getsockname(sock_fd, (struct sockaddr *)&addr, &addr_len) < 0) {
        close(sock_fd);
        return make_error(env, "getsockname_failed");
    }
    int actual_port = ntohs(addr.sin_port);

    ERL_NIF_TERM result = enif_make_tuple2(
        env,
        enif_make_int(env, sock_fd),
        enif_make_int(env, actual_port)
    );

    return enif_make_tuple2(env, ATOM_OK, result);
}

/**
 * recvfrom_test_udp(Fd, MaxSize) -> {ok, {Data, {HostBinary, Port}}} | {error, Reason}
 *
 * Receives data from a UDP socket, returning the data and source address.
 */
ERL_NIF_TERM nif_recvfrom_test_udp(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    int max_size;
    if (!enif_get_int(env, argv[1], &max_size)) {
        return make_error(env, "invalid_size");
    }

    if (max_size <= 0 || max_size > 65536) {
        max_size = 4096;
    }

    ErlNifBinary bin;
    if (!enif_alloc_binary(max_size, &bin)) {
        return make_error(env, "alloc_failed");
    }

    struct sockaddr_in from_addr;
    socklen_t from_len = sizeof(from_addr);

    ssize_t n = recvfrom(fd, bin.data, bin.size, 0,
                         (struct sockaddr *)&from_addr, &from_len);
    if (n == -1) {
        enif_release_binary(&bin);
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return make_error(env, "would_block");
        }
        return make_error(env, "recvfrom_failed");
    }

    if ((size_t)n < bin.size) {
        enif_realloc_binary(&bin, n);
    }

    /* Convert source address to binary */
    char host_str[INET_ADDRSTRLEN];
    if (inet_ntop(AF_INET, &from_addr.sin_addr, host_str, sizeof(host_str)) == NULL) {
        enif_release_binary(&bin);
        return make_error(env, "inet_ntop_failed");
    }

    ERL_NIF_TERM host_bin;
    size_t host_len = strlen(host_str);
    unsigned char *host_buf = enif_make_new_binary(env, host_len, &host_bin);
    memcpy(host_buf, host_str, host_len);

    int from_port = ntohs(from_addr.sin_port);

    /* Return {ok, {Data, {HostBinary, Port}}} */
    ERL_NIF_TERM addr_tuple = enif_make_tuple2(env, host_bin,
                                                enif_make_int(env, from_port));
    ERL_NIF_TERM result = enif_make_tuple2(env, enif_make_binary(env, &bin),
                                            addr_tuple);

    return enif_make_tuple2(env, ATOM_OK, result);
}

/**
 * sendto_test_udp(Fd, Data, Host, Port) -> ok | {error, Reason}
 *
 * Sends data to a UDP destination address.
 */
ERL_NIF_TERM nif_sendto_test_udp(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    ErlNifBinary data;
    if (!enif_inspect_binary(env, argv[1], &data)) {
        return make_error(env, "invalid_data");
    }

    /* Get host - we expect a binary like <<"127.0.0.1">> */
    ErlNifBinary host_bin;
    if (!enif_inspect_binary(env, argv[2], &host_bin)) {
        return make_error(env, "invalid_host");
    }

    /* Null-terminate the host string */
    char host[256];
    size_t host_len = host_bin.size < 255 ? host_bin.size : 255;
    memcpy(host, host_bin.data, host_len);
    host[host_len] = '\0';

    int port;
    if (!enif_get_int(env, argv[3], &port)) {
        return make_error(env, "invalid_port");
    }

    /* Build destination address */
    struct sockaddr_in to_addr;
    memset(&to_addr, 0, sizeof(to_addr));
    to_addr.sin_family = AF_INET;
    to_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &to_addr.sin_addr) <= 0) {
        return make_error(env, "invalid_address");
    }

    ssize_t n = sendto(fd, data.data, data.size, 0,
                       (struct sockaddr *)&to_addr, sizeof(to_addr));
    if (n == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return make_error(env, "would_block");
        }
        return make_error(env, "sendto_failed");
    }

    return ATOM_OK;
}

/**
 * set_udp_broadcast(Fd, Enable) -> ok | {error, Reason}
 *
 * Enable or disable SO_BROADCAST on a UDP socket.
 */
ERL_NIF_TERM nif_set_udp_broadcast(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    int enable;
    /* Accept boolean atom or integer */
    if (enif_compare(argv[1], ATOM_TRUE) == 0) {
        enable = 1;
    } else if (enif_compare(argv[1], ATOM_FALSE) == 0) {
        enable = 0;
    } else if (!enif_get_int(env, argv[1], &enable)) {
        return make_error(env, "invalid_enable");
    }

    if (setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &enable, sizeof(enable)) < 0) {
        return make_error(env, "setsockopt_failed");
    }

    return ATOM_OK;
}

/* ============================================================================
 * Python Module: py_event_loop
 *
 * This provides Python-callable functions for the event loop, allowing
 * Python's asyncio to use the Erlang-native event loop.
 * ============================================================================ */

/**
 * Initialize the global Python event loop.
 * Note: This function is currently unused (dead code).
 */
int py_event_loop_init_python(ErlNifEnv *env, erlang_event_loop_t *loop) {
    (void)env;
    g_python_event_loop = loop;
    return 0;
}

/**
 * NIF to set the global Python event loop.
 * Called from Erlang: py_nif:set_python_event_loop(LoopRef)
 *
 * Updates both the global C variable (for NIF calls) and the per-interpreter
 * storage (for Python code). Acquires GIL to set per-interpreter storage.
 */
ERL_NIF_TERM nif_set_python_event_loop(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE, (void **)&loop)) {
        return make_error(env, "invalid_event_loop");
    }

    /* Set global C variable for fast access from C code */
    g_python_event_loop = loop;

    /* Also set per-interpreter storage so Python code uses the correct loop */
    PyGILState_STATE gstate = PyGILState_Ensure();
    set_interpreter_event_loop(loop);
    PyGILState_Release(gstate);

    return ATOM_OK;
}

/**
 * set_isolation_mode(Mode) -> ok
 *
 * Set the event loop isolation mode.
 * Called from Erlang: py_nif:set_isolation_mode(global | per_loop)
 */
ERL_NIF_TERM nif_set_isolation_mode(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {
    (void)argc;

    if (enif_is_atom(env, argv[0])) {
        char atom_buf[32];
        if (enif_get_atom(env, argv[0], atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
            if (strcmp(atom_buf, "per_loop") == 0) {
                g_isolation_mode = 1;
            } else {
                g_isolation_mode = 0;  /* global or any other value */
            }
            return ATOM_OK;
        }
    }
    return make_error(env, "invalid_mode");
}

/**
 * Set the shared router PID for per-loop created loops.
 * This router will be used by all loops created via _loop_new().
 */
ERL_NIF_TERM nif_set_shared_router(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;

    if (!enif_get_local_pid(env, argv[0], &g_shared_router)) {
        return make_error(env, "invalid_pid");
    }
    g_shared_router_valid = 1;
    return ATOM_OK;
}

/* Python function: _poll_events(timeout_ms) -> num_events */
static PyObject *py_poll_events(PyObject *self, PyObject *args) {
    (void)self;
    int timeout_ms;

    if (!PyArg_ParseTuple(args, "i", &timeout_ms)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop not initialized");
        return NULL;
    }

    if (loop->shutdown) {
        return PyLong_FromLong(0);
    }

    int num_events;

    /* Release GIL while waiting */
    Py_BEGIN_ALLOW_THREADS
    num_events = poll_events_wait(loop, timeout_ms);
    Py_END_ALLOW_THREADS

    return PyLong_FromLong(num_events);
}

/* Python function: _get_pending() -> [(callback_id, type_str), ...] */
static PyObject *py_get_pending(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        return PyList_New(0);
    }

    pthread_mutex_lock(&loop->mutex);

    /* Count pending events */
    int count = 0;
    pending_event_t *current = loop->pending_head;
    while (current != NULL) {
        count++;
        current = current->next;
    }

    PyObject *list = PyList_New(count);
    if (list == NULL) {
        pthread_mutex_unlock(&loop->mutex);
        return NULL;
    }

    current = loop->pending_head;
    int i = 0;
    while (current != NULL) {
        const char *type_str;
        switch (current->type) {
            case EVENT_TYPE_READ: type_str = "read"; break;
            case EVENT_TYPE_WRITE: type_str = "write"; break;
            case EVENT_TYPE_TIMER: type_str = "timer"; break;
            default: type_str = "unknown";
        }

        PyObject *tuple = Py_BuildValue("(Ks)",
            (unsigned long long)current->callback_id, type_str);
        if (tuple == NULL) {
            Py_DECREF(list);
            pthread_mutex_unlock(&loop->mutex);
            return NULL;
        }
        PyList_SET_ITEM(list, i++, tuple);

        pending_event_t *next = current->next;
        /* Return to freelist for reuse (Phase 7 optimization) */
        return_pending_event(loop, current);
        current = next;
    }

    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);

    /* Clear the hash set since we're consuming all pending events */
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);

    return list;
}

/* Python function: _wakeup() -> None */
static PyObject *py_wakeup(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        Py_RETURN_NONE;
    }

    pthread_mutex_lock(&loop->mutex);
    pthread_cond_broadcast(&loop->event_cond);
    pthread_mutex_unlock(&loop->mutex);

    Py_RETURN_NONE;
}

/* Python function: _add_pending(callback_id, type_str) -> None */
static PyObject *py_add_pending(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long callback_id;
    const char *type_str;

    if (!PyArg_ParseTuple(args, "Ks", &callback_id, &type_str)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        Py_RETURN_NONE;
    }

    event_type_t type;
    if (strcmp(type_str, "read") == 0) {
        type = EVENT_TYPE_READ;
    } else if (strcmp(type_str, "write") == 0) {
        type = EVENT_TYPE_WRITE;
    } else {
        type = EVENT_TYPE_TIMER;
    }

    event_loop_add_pending(loop, type, callback_id, -1);

    Py_RETURN_NONE;
}

/* Python function: _is_initialized() -> bool */
static PyObject *py_is_initialized(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop != NULL) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

/* Python function: _get_isolation_mode() -> str ("global" or "per_loop") */
static PyObject *py_get_isolation_mode(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    if (g_isolation_mode == 1) {
        return PyUnicode_FromString("per_loop");
    }
    return PyUnicode_FromString("global");
}

/* Python function: _add_reader(fd, callback_id) -> fd_key */
static PyObject *py_add_reader(PyObject *self, PyObject *args) {
    (void)self;
    int fd;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "iK", &fd, &callback_id)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop not initialized");
        return NULL;
    }

    /* Create fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE, sizeof(fd_resource_t));
    if (fd_res == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate fd resource");
        return NULL;
    }

    fd_res->fd = fd;
    fd_res->loop = loop;
    fd_res->read_callback_id = callback_id;
    fd_res->write_callback_id = 0;
    fd_res->reader_active = true;
    fd_res->writer_active = false;
    fd_res->owner_pid = loop->router_pid;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Register with enif_select using the loop's persistent msg_env */
    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_READ, fd_res, &loop->router_pid, ATOM_UNDEFINED);

    if (ret < 0) {
        enif_release_resource(fd_res);
        PyErr_SetString(PyExc_RuntimeError, "Failed to register fd for reading");
        return NULL;
    }

    /* Return a key that can be used to remove the reader */
    unsigned long long key = (unsigned long long)(uintptr_t)fd_res;
    return PyLong_FromUnsignedLongLong(key);
}

/* Python function: _remove_reader(fd_key) -> None */
static PyObject *py_remove_reader(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long fd_key;

    if (!PyArg_ParseTuple(args, "K", &fd_key)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup - but still allow cleanup even if loop is gone */
    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res != NULL && fd_res->loop != NULL) {
        enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                    ERL_NIF_SELECT_STOP, fd_res, NULL, ATOM_UNDEFINED);
        fd_res->reader_active = false;
        enif_release_resource(fd_res);
    }

    Py_RETURN_NONE;
}

/* Python function: _add_writer(fd, callback_id) -> fd_key */
static PyObject *py_add_writer(PyObject *self, PyObject *args) {
    (void)self;
    int fd;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "iK", &fd, &callback_id)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop not initialized");
        return NULL;
    }

    /* Create fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE, sizeof(fd_resource_t));
    if (fd_res == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate fd resource");
        return NULL;
    }

    fd_res->fd = fd;
    fd_res->loop = loop;
    fd_res->read_callback_id = 0;
    fd_res->write_callback_id = callback_id;
    fd_res->reader_active = false;
    fd_res->writer_active = true;
    fd_res->owner_pid = loop->router_pid;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Register with enif_select using the loop's persistent msg_env */
    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_WRITE, fd_res, &loop->router_pid, ATOM_UNDEFINED);

    if (ret < 0) {
        enif_release_resource(fd_res);
        PyErr_SetString(PyExc_RuntimeError, "Failed to register fd for writing");
        return NULL;
    }

    /* Return a key that can be used to remove the writer */
    unsigned long long key = (unsigned long long)(uintptr_t)fd_res;
    return PyLong_FromUnsignedLongLong(key);
}

/* Python function: _remove_writer(fd_key) -> None */
static PyObject *py_remove_writer(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long fd_key;

    if (!PyArg_ParseTuple(args, "K", &fd_key)) {
        return NULL;
    }

    /* Use fd_res->loop directly - allows cleanup even if interpreter's loop is gone */
    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res != NULL && fd_res->loop != NULL) {
        enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                    ERL_NIF_SELECT_STOP, fd_res, NULL, ATOM_UNDEFINED);
        fd_res->writer_active = false;
        enif_release_resource(fd_res);
    }

    Py_RETURN_NONE;
}

/* Python function: _schedule_timer(delay_ms, callback_id) -> timer_ref */
static PyObject *py_schedule_timer(PyObject *self, PyObject *args) {
    (void)self;
    int delay_ms;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "iK", &delay_ms, &callback_id)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL || !loop->has_router) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop not initialized");
        return NULL;
    }
    if (delay_ms < 0) delay_ms = 0;

    uint64_t timer_ref_id = atomic_fetch_add(&loop->next_callback_id, 1);

    /* Use per-call env for thread safety in free-threaded Python */
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate message env");
        return NULL;
    }

    ERL_NIF_TERM msg = enif_make_tuple4(
        msg_env,
        ATOM_START_TIMER,
        enif_make_int(msg_env, delay_ms),
        enif_make_uint64(msg_env, callback_id),
        enif_make_uint64(msg_env, timer_ref_id)
    );

    int send_result = enif_send(NULL, &loop->router_pid, msg_env, msg);
    enif_free_env(msg_env);

    if (!send_result) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send timer message");
        return NULL;
    }

    return PyLong_FromUnsignedLongLong(timer_ref_id);
}

/* Python function: _cancel_timer(timer_ref) -> None */
static PyObject *py_cancel_timer(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long timer_ref_id;

    if (!PyArg_ParseTuple(args, "K", &timer_ref_id)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL || !loop->has_router) {
        Py_RETURN_NONE;
    }

    /* Use per-call env for thread safety in free-threaded Python */
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        Py_RETURN_NONE;  /* Best effort - don't fail on cancel */
    }

    ERL_NIF_TERM msg = enif_make_tuple2(
        msg_env,
        ATOM_CANCEL_TIMER,
        enif_make_uint64(msg_env, timer_ref_id)
    );

    enif_send(NULL, &loop->router_pid, msg_env, msg);
    enif_free_env(msg_env);
    Py_RETURN_NONE;
}

/**
 * @brief Create an event tuple (callback_id, event_type)
 *
 * Direct tuple creation using PyTuple_New + PyTuple_SET_ITEM instead of
 * Py_BuildValue to avoid format string parsing overhead.
 *
 * Note: Event types 1, 2, 3 are in CPython's small-int cache range,
 * so PyLong_FromLong() returns cached immortal objects (no allocation).
 *
 * @param callback_id The callback ID (uint64_t)
 * @param event_type The event type (int)
 * @return New reference to tuple, or NULL on error
 */
static inline PyObject *make_event_tuple(uint64_t callback_id, int event_type) {
    PyObject *tuple = PyTuple_New(2);
    if (!tuple) return NULL;

    PyObject *cid = PyLong_FromUnsignedLongLong(callback_id);
    if (!cid) { Py_DECREF(tuple); return NULL; }
    PyTuple_SET_ITEM(tuple, 0, cid);  /* steals ref */

    PyObject *etype = PyLong_FromLong(event_type);  /* small-int cached */
    if (!etype) { Py_DECREF(tuple); return NULL; }
    PyTuple_SET_ITEM(tuple, 1, etype);  /* steals ref */

    return tuple;
}

/**
 * py_run_once(timeout_ms) -> [(callback_id, event_type_int), ...]
 *
 * Combined poll + get_pending in a single NIF call for optimal performance.
 * Returns integer event types instead of strings:
 *   EVENT_TYPE_READ=1, EVENT_TYPE_WRITE=2, EVENT_TYPE_TIMER=3
 *
 * Uses pre-allocated list based on atomic pending_count for single traversal.
 */
static PyObject *py_run_once(PyObject *self, PyObject *args) {
    (void)self;
    int timeout_ms;

    if (!PyArg_ParseTuple(args, "i", &timeout_ms)) {
        return NULL;
    }

    /* Use per-interpreter event loop lookup */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop not initialized");
        return NULL;
    }

    if (loop->shutdown) {
        return PyList_New(0);
    }

    /* Release GIL while waiting for events */
    Py_BEGIN_ALLOW_THREADS
    poll_events_wait(loop, timeout_ms);
    Py_END_ALLOW_THREADS

    /*
     * Phase 1: Snapshot pending list under lock (fast - just pointer swap)
     * This minimizes lock contention by doing minimal work under the mutex.
     */
    pthread_mutex_lock(&loop->mutex);

    pending_event_t *snapshot_head = loop->pending_head;
    int count = atomic_load(&loop->pending_count);

    /* Clear the queue under lock */
    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);

    /*
     * Phase 2: Build Python list outside lock (no contention)
     * Memory allocation and Python operations happen without holding the mutex.
     */
    if (count == 0 || snapshot_head == NULL) {
        return PyList_New(0);
    }

    PyObject *list = PyList_New(count);
    if (list == NULL) {
        /* Return events to freelist on error */
        pthread_mutex_lock(&loop->mutex);
        pending_event_t *current = snapshot_head;
        while (current != NULL) {
            pending_event_t *next = current->next;
            return_pending_event(loop, current);
            current = next;
        }
        pthread_mutex_unlock(&loop->mutex);
        return NULL;
    }

    pending_event_t *current = snapshot_head;
    int i = 0;
    while (current != NULL && i < count) {
        /* Use optimized direct tuple creation (Phase 9+10 optimization) */
        PyObject *tuple = make_event_tuple(current->callback_id, (int)current->type);
        if (tuple == NULL) {
            Py_DECREF(list);
            /* Return remaining events to freelist (Phase 7 optimization) */
            pthread_mutex_lock(&loop->mutex);
            while (current != NULL) {
                pending_event_t *next = current->next;
                return_pending_event(loop, current);
                current = next;
            }
            pthread_mutex_unlock(&loop->mutex);
            return NULL;
        }
        PyList_SET_ITEM(list, i++, tuple);
        current = current->next;
    }

    /*
     * Phase 3: Return events to freelist under lock
     */
    pthread_mutex_lock(&loop->mutex);
    current = snapshot_head;
    while (current != NULL) {
        pending_event_t *next = current->next;
        return_pending_event(loop, current);
        current = next;
    }
    pthread_mutex_unlock(&loop->mutex);

    return list;
}

/* ============================================================================
 * Handle-Based Python API (_for methods)
 * ============================================================================
 *
 * These methods take an explicit loop handle (PyCapsule) as the first argument,
 * allowing Python code to operate on multiple independent event loops.
 */

/** @brief Capsule name for event loop handles */
static const char *LOOP_CAPSULE_NAME = "erlang_python.event_loop";

/**
 * Extract event loop pointer from a PyCapsule.
 *
 * @param capsule The PyCapsule containing an erlang_event_loop_t pointer
 * @return Pointer to the event loop, or NULL if invalid
 */
static erlang_event_loop_t *loop_from_capsule(PyObject *capsule) {
    if (!PyCapsule_CheckExact(capsule)) {
        PyErr_SetString(PyExc_TypeError, "Expected event loop capsule");
        return NULL;
    }
    void *ptr = PyCapsule_GetPointer(capsule, LOOP_CAPSULE_NAME);
    if (ptr == NULL) {
        /* PyCapsule_GetPointer sets appropriate error */
        return NULL;
    }
    return (erlang_event_loop_t *)ptr;
}

/**
 * Get the default event loop for backward compatibility.
 * Used by legacy API methods.
 */
static erlang_event_loop_t *default_loop_for_compat(void) {
    return get_interpreter_event_loop();
}

/**
 * Destructor callback for loop capsules.
 * Called when the capsule is garbage collected.
 */
static void loop_capsule_destructor(PyObject *capsule) {
    erlang_event_loop_t *loop = (erlang_event_loop_t *)PyCapsule_GetPointer(
        capsule, LOOP_CAPSULE_NAME);
    if (loop != NULL) {
        /* Signal shutdown */
        loop->shutdown = true;

        /* Wake up any waiting threads */
        pthread_mutex_lock(&loop->mutex);
        pthread_cond_broadcast(&loop->event_cond);
        pthread_mutex_unlock(&loop->mutex);

        /* Release the NIF resource reference */
        enif_release_resource(loop);
    }
}

/* Python function: _loop_new() -> capsule */
static PyObject *py_loop_new(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    /* Allocate event loop resource */
    erlang_event_loop_t *loop = enif_alloc_resource(
        EVENT_LOOP_RESOURCE_TYPE, sizeof(erlang_event_loop_t));

    if (loop == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate event loop");
        return NULL;
    }

    /* Initialize fields */
    memset(loop, 0, sizeof(erlang_event_loop_t));

    if (pthread_mutex_init(&loop->mutex, NULL) != 0) {
        enif_release_resource(loop);
        PyErr_SetString(PyExc_RuntimeError, "Failed to initialize mutex");
        return NULL;
    }

    if (pthread_cond_init(&loop->event_cond, NULL) != 0) {
        pthread_mutex_destroy(&loop->mutex);
        enif_release_resource(loop);
        PyErr_SetString(PyExc_RuntimeError, "Failed to initialize condition variable");
        return NULL;
    }

    loop->msg_env = enif_alloc_env();
    if (loop->msg_env == NULL) {
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_release_resource(loop);
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate message environment");
        return NULL;
    }

    atomic_store(&loop->next_callback_id, 1);
    atomic_store(&loop->pending_count, 0);
    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    loop->shutdown = false;
    loop->has_router = false;
    loop->has_self = false;
    loop->event_freelist = NULL;
    loop->freelist_count = 0;

    /* Use shared router if available (for per-loop mode) */
    if (g_shared_router_valid) {
        loop->router_pid = g_shared_router;
        loop->has_router = true;
    }

    /* Create a capsule wrapping the loop pointer */
    PyObject *capsule = PyCapsule_New(loop, LOOP_CAPSULE_NAME, loop_capsule_destructor);
    if (capsule == NULL) {
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_free_env(loop->msg_env);
        enif_release_resource(loop);
        return NULL;
    }

    /* Keep a reference to the NIF resource (capsule destructor will release) */
    enif_keep_resource(loop);

    return capsule;
}

/* Python function: _loop_destroy(capsule) -> None */
static PyObject *py_loop_destroy(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    /* Signal shutdown */
    loop->shutdown = true;

    /* Wake up any waiting threads */
    pthread_mutex_lock(&loop->mutex);
    pthread_cond_broadcast(&loop->event_cond);
    pthread_mutex_unlock(&loop->mutex);

    Py_RETURN_NONE;
}

/* Python function: _run_once_native_for(capsule, timeout_ms) -> [(callback_id, event_type), ...] */
static PyObject *py_run_once_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    int timeout_ms;

    if (!PyArg_ParseTuple(args, "Oi", &capsule, &timeout_ms)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    if (loop->shutdown) {
        return PyList_New(0);
    }

    /* Release GIL while waiting for events */
    Py_BEGIN_ALLOW_THREADS
    poll_events_wait(loop, timeout_ms);
    Py_END_ALLOW_THREADS

    /*
     * Phase 1: Snapshot pending list under lock (fast - just pointer swap)
     * This minimizes lock contention by doing minimal work under the mutex.
     */
    pthread_mutex_lock(&loop->mutex);

    pending_event_t *snapshot_head = loop->pending_head;
    int count = atomic_load(&loop->pending_count);

    /* Clear the queue under lock */
    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);

    /*
     * Phase 2: Build Python list outside lock (no contention)
     * Memory allocation and Python operations happen without holding the mutex.
     */
    if (count == 0 || snapshot_head == NULL) {
        return PyList_New(0);
    }

    PyObject *list = PyList_New(count);
    if (list == NULL) {
        /* Return events to freelist on error */
        pthread_mutex_lock(&loop->mutex);
        pending_event_t *current = snapshot_head;
        while (current != NULL) {
            pending_event_t *next = current->next;
            return_pending_event(loop, current);
            current = next;
        }
        pthread_mutex_unlock(&loop->mutex);
        return NULL;
    }

    pending_event_t *current = snapshot_head;
    int i = 0;
    while (current != NULL && i < count) {
        PyObject *tuple = make_event_tuple(current->callback_id, (int)current->type);
        if (tuple == NULL) {
            Py_DECREF(list);
            /* Return remaining events to freelist */
            pthread_mutex_lock(&loop->mutex);
            while (current != NULL) {
                pending_event_t *next = current->next;
                return_pending_event(loop, current);
                current = next;
            }
            pthread_mutex_unlock(&loop->mutex);
            return NULL;
        }
        PyList_SET_ITEM(list, i++, tuple);
        current = current->next;
    }

    /*
     * Phase 3: Return events to freelist under lock
     */
    pthread_mutex_lock(&loop->mutex);
    current = snapshot_head;
    while (current != NULL) {
        pending_event_t *next = current->next;
        return_pending_event(loop, current);
        current = next;
    }
    pthread_mutex_unlock(&loop->mutex);

    return list;
}

/* Python function: _add_reader_for(capsule, fd, callback_id) -> fd_key */
static PyObject *py_add_reader_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    int fd;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "OiK", &capsule, &fd, &callback_id)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    if (!loop->has_router) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop has no router");
        return NULL;
    }

    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE, sizeof(fd_resource_t));
    if (fd_res == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate fd resource");
        return NULL;
    }

    fd_res->fd = fd;
    fd_res->loop = loop;
    fd_res->read_callback_id = callback_id;
    fd_res->write_callback_id = 0;
    fd_res->reader_active = true;
    fd_res->writer_active = false;
    fd_res->owner_pid = loop->router_pid;
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_READ, fd_res, &loop->router_pid, ATOM_UNDEFINED);

    if (ret < 0) {
        enif_release_resource(fd_res);
        PyErr_SetString(PyExc_RuntimeError, "Failed to register fd for reading");
        return NULL;
    }

    unsigned long long key = (unsigned long long)(uintptr_t)fd_res;
    return PyLong_FromUnsignedLongLong(key);
}

/* Python function: _remove_reader_for(capsule, fd_key) -> None */
static PyObject *py_remove_reader_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    unsigned long long fd_key;

    if (!PyArg_ParseTuple(args, "OK", &capsule, &fd_key)) {
        return NULL;
    }

    /* Validate capsule (but we use fd_res->loop directly) */
    if (!PyCapsule_CheckExact(capsule)) {
        PyErr_SetString(PyExc_TypeError, "Expected event loop capsule");
        return NULL;
    }

    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res != NULL && fd_res->loop != NULL) {
        enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                    ERL_NIF_SELECT_STOP, fd_res, NULL, ATOM_UNDEFINED);
        fd_res->reader_active = false;
        enif_release_resource(fd_res);
    }

    Py_RETURN_NONE;
}

/* Python function: _add_writer_for(capsule, fd, callback_id) -> fd_key */
static PyObject *py_add_writer_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    int fd;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "OiK", &capsule, &fd, &callback_id)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    if (!loop->has_router) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop has no router");
        return NULL;
    }

    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE, sizeof(fd_resource_t));
    if (fd_res == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate fd resource");
        return NULL;
    }

    fd_res->fd = fd;
    fd_res->loop = loop;
    fd_res->read_callback_id = 0;
    fd_res->write_callback_id = callback_id;
    fd_res->reader_active = false;
    fd_res->writer_active = true;
    fd_res->owner_pid = loop->router_pid;
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_WRITE, fd_res, &loop->router_pid, ATOM_UNDEFINED);

    if (ret < 0) {
        enif_release_resource(fd_res);
        PyErr_SetString(PyExc_RuntimeError, "Failed to register fd for writing");
        return NULL;
    }

    unsigned long long key = (unsigned long long)(uintptr_t)fd_res;
    return PyLong_FromUnsignedLongLong(key);
}

/* Python function: _remove_writer_for(capsule, fd_key) -> None */
static PyObject *py_remove_writer_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    unsigned long long fd_key;

    if (!PyArg_ParseTuple(args, "OK", &capsule, &fd_key)) {
        return NULL;
    }

    /* Validate capsule (but we use fd_res->loop directly) */
    if (!PyCapsule_CheckExact(capsule)) {
        PyErr_SetString(PyExc_TypeError, "Expected event loop capsule");
        return NULL;
    }

    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res != NULL && fd_res->loop != NULL) {
        enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                    ERL_NIF_SELECT_STOP, fd_res, NULL, ATOM_UNDEFINED);
        fd_res->writer_active = false;
        enif_release_resource(fd_res);
    }

    Py_RETURN_NONE;
}

/* Python function: _schedule_timer_for(capsule, delay_ms, callback_id) -> timer_ref */
static PyObject *py_schedule_timer_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    int delay_ms;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "OiK", &capsule, &delay_ms, &callback_id)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    if (!loop->has_router) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop has no router");
        return NULL;
    }

    if (delay_ms < 0) delay_ms = 0;

    uint64_t timer_ref_id = atomic_fetch_add(&loop->next_callback_id, 1);

    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate message env");
        return NULL;
    }

    /* Include loop resource in message so router dispatches to correct loop */
    ERL_NIF_TERM loop_term = enif_make_resource(msg_env, loop);

    ERL_NIF_TERM msg = enif_make_tuple5(
        msg_env,
        ATOM_START_TIMER,
        loop_term,
        enif_make_int(msg_env, delay_ms),
        enif_make_uint64(msg_env, callback_id),
        enif_make_uint64(msg_env, timer_ref_id)
    );

    int send_result = enif_send(NULL, &loop->router_pid, msg_env, msg);
    enif_free_env(msg_env);

    if (!send_result) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send timer message");
        return NULL;
    }

    return PyLong_FromUnsignedLongLong(timer_ref_id);
}

/* Python function: _cancel_timer_for(capsule, timer_ref) -> None */
static PyObject *py_cancel_timer_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    unsigned long long timer_ref_id;

    if (!PyArg_ParseTuple(args, "OK", &capsule, &timer_ref_id)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        PyErr_Clear();  /* Don't fail on cancel */
        Py_RETURN_NONE;
    }

    if (!loop->has_router) {
        Py_RETURN_NONE;
    }

    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        Py_RETURN_NONE;
    }

    ERL_NIF_TERM msg = enif_make_tuple2(
        msg_env,
        ATOM_CANCEL_TIMER,
        enif_make_uint64(msg_env, timer_ref_id)
    );

    enif_send(NULL, &loop->router_pid, msg_env, msg);
    enif_free_env(msg_env);
    Py_RETURN_NONE;
}

/* Python function: _wakeup_for(capsule) -> None */
static PyObject *py_wakeup_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        PyErr_Clear();
        Py_RETURN_NONE;
    }

    pthread_mutex_lock(&loop->mutex);
    pthread_cond_broadcast(&loop->event_cond);
    pthread_mutex_unlock(&loop->mutex);

    Py_RETURN_NONE;
}

/* Python function: _is_initialized_for(capsule) -> bool */
static PyObject *py_is_initialized_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }

    if (!PyCapsule_CheckExact(capsule)) {
        Py_RETURN_FALSE;
    }

    void *ptr = PyCapsule_GetPointer(capsule, LOOP_CAPSULE_NAME);
    if (ptr == NULL) {
        PyErr_Clear();
        Py_RETURN_FALSE;
    }

    erlang_event_loop_t *loop = (erlang_event_loop_t *)ptr;
    if (loop->shutdown) {
        Py_RETURN_FALSE;
    }

    Py_RETURN_TRUE;
}

/* Python function: _get_pending_for(capsule) -> [(callback_id, event_type), ...] */
static PyObject *py_get_pending_for(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        PyErr_Clear();
        return PyList_New(0);
    }

    pthread_mutex_lock(&loop->mutex);

    int count = 0;
    pending_event_t *current = loop->pending_head;
    while (current != NULL) {
        count++;
        current = current->next;
    }

    PyObject *list = PyList_New(count);
    if (list == NULL) {
        pthread_mutex_unlock(&loop->mutex);
        return NULL;
    }

    current = loop->pending_head;
    int i = 0;
    while (current != NULL) {
        PyObject *tuple = make_event_tuple(current->callback_id, (int)current->type);
        if (tuple == NULL) {
            Py_DECREF(list);
            pthread_mutex_unlock(&loop->mutex);
            return NULL;
        }
        PyList_SET_ITEM(list, i++, tuple);

        pending_event_t *next = current->next;
        return_pending_event(loop, current);
        current = next;
    }

    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);

    return list;
}

/* Module method definitions */
static PyMethodDef PyEventLoopMethods[] = {
    /* Legacy API (uses global event loop) */
    {"_poll_events", py_poll_events, METH_VARARGS, "Wait for events with timeout"},
    {"_get_pending", py_get_pending, METH_NOARGS, "Get and clear pending events"},
    {"_run_once_native", py_run_once, METH_VARARGS, "Combined poll + get_pending with int event types"},
    {"_wakeup", py_wakeup, METH_NOARGS, "Wake up the event loop"},
    {"_add_pending", py_add_pending, METH_VARARGS, "Add a pending event"},
    {"_is_initialized", py_is_initialized, METH_NOARGS, "Check if event loop is initialized"},
    {"_get_isolation_mode", py_get_isolation_mode, METH_NOARGS, "Get event loop isolation mode (global or per_loop)"},
    {"_add_reader", py_add_reader, METH_VARARGS, "Register fd for read monitoring"},
    {"_remove_reader", py_remove_reader, METH_VARARGS, "Stop monitoring fd for reads"},
    {"_add_writer", py_add_writer, METH_VARARGS, "Register fd for write monitoring"},
    {"_remove_writer", py_remove_writer, METH_VARARGS, "Stop monitoring fd for writes"},
    {"_schedule_timer", py_schedule_timer, METH_VARARGS, "Schedule a timer with Erlang"},
    {"_cancel_timer", py_cancel_timer, METH_VARARGS, "Cancel an Erlang timer"},
    /* Handle-based API (takes explicit loop capsule) */
    {"_loop_new", py_loop_new, METH_NOARGS, "Create a new event loop, returns capsule"},
    {"_loop_destroy", py_loop_destroy, METH_VARARGS, "Destroy an event loop"},
    {"_run_once_native_for", py_run_once_for, METH_VARARGS, "Combined poll + get_pending for specific loop"},
    {"_get_pending_for", py_get_pending_for, METH_VARARGS, "Get and clear pending events for specific loop"},
    {"_wakeup_for", py_wakeup_for, METH_VARARGS, "Wake up specific event loop"},
    {"_is_initialized_for", py_is_initialized_for, METH_VARARGS, "Check if specific loop is initialized"},
    {"_add_reader_for", py_add_reader_for, METH_VARARGS, "Register fd for read monitoring on specific loop"},
    {"_remove_reader_for", py_remove_reader_for, METH_VARARGS, "Stop monitoring fd for reads on specific loop"},
    {"_add_writer_for", py_add_writer_for, METH_VARARGS, "Register fd for write monitoring on specific loop"},
    {"_remove_writer_for", py_remove_writer_for, METH_VARARGS, "Stop monitoring fd for writes on specific loop"},
    {"_schedule_timer_for", py_schedule_timer_for, METH_VARARGS, "Schedule timer on specific loop"},
    {"_cancel_timer_for", py_cancel_timer_for, METH_VARARGS, "Cancel timer on specific loop"},
    {NULL, NULL, 0, NULL}
};

/* Module definition */
static struct PyModuleDef PyEventLoopModuleDef = {
    PyModuleDef_HEAD_INIT,
    "py_event_loop",
    "Erlang-native asyncio event loop",
    -1,
    PyEventLoopMethods
};

/**
 * Create and register the py_event_loop module in Python.
 * Also creates a default event loop so g_python_event_loop is always available.
 * Called during Python initialization.
 */
int create_py_event_loop_module(void) {
    PyObject *module = PyModule_Create(&PyEventLoopModuleDef);
    if (module == NULL) {
        return -1;
    }

    /* Add module to sys.modules */
    PyObject *sys_modules = PyImport_GetModuleDict();
    if (PyDict_SetItemString(sys_modules, "py_event_loop", module) < 0) {
        Py_DECREF(module);
        return -1;
    }

    return 0;
}

/**
 * Create a default event loop and set it as g_python_event_loop.
 * This ensures the event loop is always available for Python asyncio.
 * Called after NIF is fully loaded (with GIL held).
 */
int create_default_event_loop(ErlNifEnv *env) {
    /* Check per-interpreter storage first for sub-interpreter support */
    erlang_event_loop_t *existing = get_interpreter_event_loop();
    if (existing != NULL) {
        return 0;  /* Already have an event loop for this interpreter */
    }

    /* Also check global for backward compatibility */
    if (g_python_event_loop != NULL) {
        /* Global exists but not set for this interpreter - set it now */
        set_interpreter_event_loop(g_python_event_loop);
        return 0;
    }

    /* Allocate event loop resource */
    erlang_event_loop_t *loop = enif_alloc_resource(
        EVENT_LOOP_RESOURCE_TYPE, sizeof(erlang_event_loop_t));

    if (loop == NULL) {
        return -1;
    }

    /* Initialize fields */
    memset(loop, 0, sizeof(erlang_event_loop_t));

    if (pthread_mutex_init(&loop->mutex, NULL) != 0) {
        enif_release_resource(loop);
        return -1;
    }

    if (pthread_cond_init(&loop->event_cond, NULL) != 0) {
        pthread_mutex_destroy(&loop->mutex);
        enif_release_resource(loop);
        return -1;
    }

    loop->msg_env = enif_alloc_env();
    if (loop->msg_env == NULL) {
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_release_resource(loop);
        return -1;
    }

    atomic_store(&loop->next_callback_id, 1);
    atomic_store(&loop->pending_count, 0);
    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    loop->shutdown = false;
    loop->has_router = false;
    loop->has_self = false;

    /* Set as global Python event loop (backward compatibility for NIF calls) */
    g_python_event_loop = loop;

    /* Store in per-interpreter storage for Python code to access */
    set_interpreter_event_loop(loop);

    /* Keep a reference to prevent garbage collection */
    /* Note: This loop will be replaced when py_event_loop:init runs */

    return 0;
}
