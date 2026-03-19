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
#include "py_reactor_buffer.h"

/* ============================================================================
 * Global State
 * ============================================================================ */

/** Resource type for event loops */
ErlNifResourceType *EVENT_LOOP_RESOURCE_TYPE = NULL;

/** Resource type for fd monitoring */
ErlNifResourceType *FD_RESOURCE_TYPE = NULL;

/** Resource type for timers */
ErlNifResourceType *TIMER_RESOURCE_TYPE = NULL;

/** @brief Global priv_dir path for module imports in subinterpreters */
static char g_priv_dir[1024] = {0};
static bool g_priv_dir_set = false;

/**
 * Thread-local for current event loop namespace during task execution.
 * This allows reentrant calls (erlang.call -> Python) to use the same namespace.
 */
__thread process_namespace_t *tl_current_event_loop_namespace = NULL;

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

/* ============================================================================
 * Module State Structure
 * ============================================================================
 *
 * Instead of using global variables, we store state in the Python module.
 * This enables proper per-interpreter/per-context isolation.
 */
typedef struct {
    /** @brief Event loop for this interpreter */
    erlang_event_loop_t *event_loop;

    /** @brief Shared router PID for loops created via _loop_new() */
    ErlNifPid shared_router;

    /** @brief Whether shared_router has been set */
    bool shared_router_valid;

    /** @brief Isolation mode: 0=global, 1=per_loop */
    int isolation_mode;

    /* ========== Per-Interpreter Reactor Cache ========== */

    /** @brief Cached erlang.reactor module for this interpreter */
    PyObject *reactor_module;

    /** @brief Cached on_read_ready callable */
    PyObject *reactor_on_read;

    /** @brief Cached on_write_ready callable */
    PyObject *reactor_on_write;

    /** @brief Whether reactor cache has been initialized */
    bool reactor_initialized;
} py_event_loop_module_state_t;

/* ============================================================================
 * Global Shared Router
 * ============================================================================
 *
 * A global shared router that can be used by all interpreters (main and sub).
 * This is separate from the per-module state to allow subinterpreters to
 * access the router even when their module state doesn't have it set.
 */
static ErlNifPid g_global_shared_router;
static bool g_global_shared_router_valid = false;
static pthread_mutex_t g_global_router_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Global shared worker for scalable I/O model.
 * Used by dispatch_timer to send task_ready, ensuring process_ready_tasks
 * is called after timer events. This centralizes the wakeup mechanism
 * so both router-dispatched and worker-dispatched timers work correctly.
 */
static ErlNifPid g_global_shared_worker;
static bool g_global_shared_worker_valid = false;
static pthread_mutex_t g_global_worker_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================================
 * Per-Interpreter Reactor Cache
 * ============================================================================
 *
 * Reactor callables (erlang.reactor.on_read_ready, on_write_ready) are cached
 * per-interpreter in the module state. This ensures that subinterpreters use
 * their own reactor module instance rather than the main interpreter's.
 *
 * The cache is populated lazily on first reactor operation within each
 * interpreter.
 */

/**
 * Initialize cached reactor callables for the current interpreter.
 * MUST be called with GIL held.
 *
 * Uses the module state to cache per-interpreter reactor references.
 * This is safe for subinterpreters since each has its own module state.
 *
 * @param state Module state for current interpreter
 * @return true if callables are cached and ready, false on error
 */
static bool ensure_reactor_cached_for_interp(py_event_loop_module_state_t *state) {
    if (state == NULL) {
        return false;
    }

    /* Fast path: already cached for this interpreter */
    if (state->reactor_initialized) {
        return true;
    }

    /* Import erlang.reactor module in THIS interpreter */
    PyObject *module = PyImport_ImportModule("erlang.reactor");
    if (module == NULL) {
        return false;
    }

    /* Get on_read_ready function */
    PyObject *on_read = PyObject_GetAttrString(module, "on_read_ready");
    if (on_read == NULL || !PyCallable_Check(on_read)) {
        Py_XDECREF(on_read);
        Py_DECREF(module);
        return false;
    }

    /* Get on_write_ready function */
    PyObject *on_write = PyObject_GetAttrString(module, "on_write_ready");
    if (on_write == NULL || !PyCallable_Check(on_write)) {
        Py_XDECREF(on_write);
        Py_DECREF(on_read);
        Py_DECREF(module);
        return false;
    }

    /* Store cached references in module state */
    state->reactor_module = module;
    state->reactor_on_read = on_read;
    state->reactor_on_write = on_write;
    state->reactor_initialized = true;

    return true;
}

/**
 * Clean up reactor cache in module state.
 * Called during module deallocation.
 */
static void cleanup_reactor_cache(py_event_loop_module_state_t *state) {
    if (state == NULL) {
        return;
    }

    Py_XDECREF(state->reactor_module);
    Py_XDECREF(state->reactor_on_read);
    Py_XDECREF(state->reactor_on_write);
    state->reactor_module = NULL;
    state->reactor_on_read = NULL;
    state->reactor_on_write = NULL;
    state->reactor_initialized = false;
}

/* Forward declaration for module state access */
static py_event_loop_module_state_t *get_module_state(void);
static py_event_loop_module_state_t *get_module_state_from_module(PyObject *module);

/* Forward declarations for callable cache */
static void callable_cache_clear(erlang_event_loop_t *loop);
static PyObject *callable_cache_lookup(erlang_event_loop_t *loop,
                                        const char *module_name,
                                        const char *func_name);
static bool callable_cache_insert(erlang_event_loop_t *loop,
                                   const char *module_name,
                                   const char *func_name,
                                   PyObject *callable);

/**
 * Try to acquire a router for the event loop.
 *
 * If the loop doesn't have a router/worker configured, check the global
 * shared router and use it if available. This allows subinterpreters
 * to use the main interpreter's router.
 *
 * @param loop Event loop to check/update
 * @return true if a router/worker is available, false otherwise
 */
static bool event_loop_ensure_router(erlang_event_loop_t *loop) {
    if (loop == NULL) {
        return false;
    }

    /* Already have a router or worker */
    if (loop->has_router || loop->has_worker) {
        return true;
    }

    /* Try to get the global shared router */
    pthread_mutex_lock(&g_global_router_mutex);
    if (g_global_shared_router_valid) {
        loop->router_pid = g_global_shared_router;
        loop->has_router = true;
    }
    pthread_mutex_unlock(&g_global_router_mutex);

    return loop->has_router || loop->has_worker;
}

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
 * Get module state from a module object.
 * MUST be called with GIL held.
 *
 * @param module The py_event_loop module object
 * @return Module state or NULL if not available
 */
static py_event_loop_module_state_t *get_module_state_from_module(PyObject *module) {
    if (module == NULL) {
        return NULL;
    }
    void *state = PyModule_GetState(module);
    return (py_event_loop_module_state_t *)state;
}

/**
 * Get module state for the current interpreter.
 * MUST be called with GIL held.
 *
 * @return Module state or NULL if not available
 */
static py_event_loop_module_state_t *get_module_state(void) {
    PyObject *module = get_event_loop_module();
    return get_module_state_from_module(module);
}

/**
 * Get the event loop for the current Python interpreter.
 * MUST be called with GIL held.
 *
 * Uses module state for proper per-interpreter isolation.
 *
 * @return Event loop pointer or NULL if not set
 */
static erlang_event_loop_t *get_interpreter_event_loop(void) {
    py_event_loop_module_state_t *state = get_module_state();
    if (state == NULL) {
        return NULL;
    }
    return state->event_loop;
}

/**
 * Set the event loop for the current interpreter.
 * MUST be called with GIL held.
 * Stores in module state for proper per-interpreter isolation.
 *
 * @param loop Event loop to set (NULL to clear)
 * @return 0 on success, -1 on error
 */
static int set_interpreter_event_loop(erlang_event_loop_t *loop) {
    py_event_loop_module_state_t *state = get_module_state();
    if (state == NULL) {
        return -1;
    }
    state->event_loop = loop;
    return 0;
}

/* ============================================================================
 * Resource Callbacks
 * ============================================================================ */

/* Forward declaration */
int create_default_event_loop(ErlNifEnv *env);

/**
 * @brief Destructor for event loop resources
 *
 * Memory/Resource Management Note:
 * This destructor intentionally skips Python object cleanup (Py_DECREF) in
 * certain scenarios to avoid crashes:
 *
 * 1. Subinterpreter event loops (interp_id > 0): The subinterpreter may have
 *    been destroyed by Py_EndInterpreter before this destructor runs (which
 *    runs on the Erlang GC thread). Calling PyGILState_Ensure would crash.
 *
 * 2. Runtime shutdown: If runtime_is_running() returns false, Python is
 *    shutting down or stopped. Calling Python C API would crash.
 *
 * 3. Thread state issues: If PyGILState_Check() returns true, we already
 *    hold the GIL from somewhere else - calling PyGILState_Ensure would
 *    deadlock or corrupt thread state.
 *
 * In all these cases, we accept a small memory leak (the Python objects)
 * rather than risking a crash. This is the standard Python embedding pattern
 * for destructor-time cleanup from non-Python threads.
 *
 * The leaked Python objects will be reclaimed when the Python runtime fully
 * shuts down via Py_FinalizeEx().
 */
void event_loop_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    erlang_event_loop_t *loop = (erlang_event_loop_t *)obj;

#ifdef HAVE_SUBINTERPRETERS
    /* For subinterpreter event loops, skip Python API calls.
     * PyGILState_Ensure only works for the main interpreter. The subinterpreter
     * may already be destroyed via Py_EndInterpreter before this destructor runs. */
    if (loop->interp_id > 0) {
        goto cleanup_native;
    }
#endif

    /* Main interpreter: safe to use PyGILState_Ensure if runtime is running
     * and this thread doesn't already have Python state bound and GIL is not held. */
    if (runtime_is_running() &&
        PyGILState_GetThisThreadState() == NULL &&
        !PyGILState_Check()) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        erlang_event_loop_t *interp_loop = get_interpreter_event_loop();
        if (interp_loop == loop) {
            set_interpreter_event_loop(NULL);
        }
        PyGILState_Release(gstate);
    }

#ifdef HAVE_SUBINTERPRETERS
cleanup_native:
#endif
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

    /* Clean up async task queue (uvloop-inspired) */
    if (loop->task_queue_initialized) {
        pthread_mutex_destroy(&loop->task_queue_mutex);
        loop->task_queue_initialized = false;
    }
    if (loop->task_queue != NULL) {
        enif_ioq_destroy(loop->task_queue);
        loop->task_queue = NULL;
    }

    /* Release Python loop reference if held */
    if (loop->py_loop_valid && loop->py_loop != NULL) {
        /* Only decref if Python runtime is still running and we can safely acquire GIL */
        if (runtime_is_running() && loop->interp_id == 0 &&
            PyGILState_GetThisThreadState() == NULL &&
            !PyGILState_Check()) {
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_DECREF(loop->py_loop);
            /* Also release cached Python objects (uvloop-style cache cleanup) */
            if (loop->py_cache_valid) {
                Py_XDECREF(loop->cached_asyncio);
                Py_XDECREF(loop->cached_run_and_send);
                loop->cached_asyncio = NULL;
                loop->cached_run_and_send = NULL;
                loop->py_cache_valid = false;
            }
            /* Clear callable cache */
            callable_cache_clear(loop);
            PyGILState_Release(gstate);
        }
        loop->py_loop = NULL;
        loop->py_loop_valid = false;
    }

    /* Free message environment */
    if (loop->msg_env != NULL) {
        enif_free_env(loop->msg_env);
        loop->msg_env = NULL;
    }

    /*
     * Clean up per-process namespaces.
     *
     * Lock ordering: GIL first, then namespaces_mutex (consistent with normal path).
     * This prevents ABBA deadlock with execution paths that acquire GIL then mutex.
     *
     * For subinterpreters (interp_id != 0), we can't use PyGILState_Ensure.
     * Just free the native structs without Py_DECREF - Python objects will be
     * cleaned up when the interpreter is destroyed.
     */
    if (runtime_is_running() && loop->interp_id == 0 &&
        PyGILState_GetThisThreadState() == NULL &&
        !PyGILState_Check()) {
        /* Main interpreter: GIL first, then mutex */
        PyGILState_STATE gstate = PyGILState_Ensure();
        pthread_mutex_lock(&loop->namespaces_mutex);

        process_namespace_t *ns = loop->namespaces_head;
        while (ns != NULL) {
            process_namespace_t *next = ns->next;
            Py_XDECREF(ns->globals);
            Py_XDECREF(ns->locals);
            Py_XDECREF(ns->module_cache);
            enif_free(ns);
            ns = next;
        }
        loop->namespaces_head = NULL;

        /* Clean up PID-to-env mappings */
        pid_env_mapping_t *mapping = loop->pid_env_head;
        while (mapping != NULL) {
            pid_env_mapping_t *next = mapping->next;
            if (mapping->env != NULL) {
                enif_release_resource(mapping->env);
            }
            enif_free(mapping);
            mapping = next;
        }
        loop->pid_env_head = NULL;

        pthread_mutex_unlock(&loop->namespaces_mutex);
        PyGILState_Release(gstate);
    } else {
        /* Subinterpreter or runtime not running: just free structs */
        pthread_mutex_lock(&loop->namespaces_mutex);

        process_namespace_t *ns = loop->namespaces_head;
        while (ns != NULL) {
            process_namespace_t *next = ns->next;
            /* Skip Py_XDECREF - can't safely acquire GIL */
            enif_free(ns);
            ns = next;
        }
        loop->namespaces_head = NULL;

        /* Clean up PID-to-env mappings */
        pid_env_mapping_t *mapping = loop->pid_env_head;
        while (mapping != NULL) {
            pid_env_mapping_t *next = mapping->next;
            if (mapping->env != NULL) {
                enif_release_resource(mapping->env);
            }
            enif_free(mapping);
            mapping = next;
        }
        loop->pid_env_head = NULL;

        pthread_mutex_unlock(&loop->namespaces_mutex);
    }
    pthread_mutex_destroy(&loop->namespaces_mutex);

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
 * Per-Process Namespace Management
 * ============================================================================ */

/**
 * @brief Down callback for event loop resources (process monitor)
 *
 * Called when a monitored process dies. Cleans up the process's namespace.
 *
 * Lock ordering: GIL first, then namespaces_mutex (consistent with normal path)
 */
void event_loop_down(ErlNifEnv *env, void *obj, ErlNifPid *pid,
                     ErlNifMonitor *mon) {
    (void)env;
    (void)mon;
    erlang_event_loop_t *loop = (erlang_event_loop_t *)obj;

    /*
     * For subinterpreters (interp_id != 0), we can't use PyGILState_Ensure.
     * Just remove from the list without Py_DECREF - the Python objects will
     * be cleaned up when the interpreter is destroyed.
     */
    if (!runtime_is_running() || loop->interp_id != 0) {
        pthread_mutex_lock(&loop->namespaces_mutex);

        process_namespace_t **pp = &loop->namespaces_head;
        while (*pp != NULL) {
            if (enif_compare_pids(&(*pp)->owner_pid, pid) == 0) {
                process_namespace_t *to_free = *pp;
                *pp = to_free->next;
                /* Skip Py_XDECREF - can't safely acquire GIL for subinterp */
                enif_free(to_free);
                break;
            }
            pp = &(*pp)->next;
        }

        pthread_mutex_unlock(&loop->namespaces_mutex);
        return;
    }

    /*
     * For main interpreter: acquire GIL FIRST to maintain consistent lock
     * ordering with the normal execution path (which acquires GIL, then mutex).
     * This prevents ABBA deadlock.
     */
    PyGILState_STATE gstate = PyGILState_Ensure();
    pthread_mutex_lock(&loop->namespaces_mutex);

    /* Find and remove namespace for this pid */
    process_namespace_t **pp = &loop->namespaces_head;
    while (*pp != NULL) {
        if (enif_compare_pids(&(*pp)->owner_pid, pid) == 0) {
            process_namespace_t *to_free = *pp;
            *pp = to_free->next;

            Py_XDECREF(to_free->globals);
            Py_XDECREF(to_free->locals);
            Py_XDECREF(to_free->module_cache);

            enif_free(to_free);
            break;
        }
        pp = &(*pp)->next;
    }

    pthread_mutex_unlock(&loop->namespaces_mutex);
    PyGILState_Release(gstate);
}

/**
 * @brief Look up namespace for a process (without creating)
 *
 * @param loop Event loop containing namespace registry
 * @param pid Process to look up
 * @return Namespace or NULL if not found
 *
 * @note Thread-safe (uses namespaces_mutex)
 */
static process_namespace_t *lookup_process_namespace(
    erlang_event_loop_t *loop,
    ErlNifPid *pid
) {
    pthread_mutex_lock(&loop->namespaces_mutex);

    process_namespace_t *ns = loop->namespaces_head;
    while (ns != NULL) {
        if (enif_compare_pids(&ns->owner_pid, pid) == 0) {
            pthread_mutex_unlock(&loop->namespaces_mutex);
            return ns;
        }
        ns = ns->next;
    }

    pthread_mutex_unlock(&loop->namespaces_mutex);
    return NULL;
}

/**
 * @brief Get or create namespace for a process
 *
 * Each Erlang process gets its own isolated Python namespace (globals/locals).
 * The namespace is automatically cleaned up when the process exits.
 *
 * @param env NIF environment (for monitoring)
 * @param loop Event loop containing namespace registry
 * @param pid Process to get namespace for
 * @return Namespace or NULL on failure
 *
 * @note Must be called with GIL held
 * @note Thread-safe (uses namespaces_mutex)
 */
static process_namespace_t *ensure_process_namespace(
    ErlNifEnv *env,
    erlang_event_loop_t *loop,
    ErlNifPid *pid
) {
    pthread_mutex_lock(&loop->namespaces_mutex);

    /* Search for existing namespace */
    process_namespace_t *ns = loop->namespaces_head;
    while (ns != NULL) {
        if (enif_compare_pids(&ns->owner_pid, pid) == 0) {
            pthread_mutex_unlock(&loop->namespaces_mutex);
            return ns;
        }
        ns = ns->next;
    }

    /* Create new namespace */
    ns = enif_alloc(sizeof(process_namespace_t));
    if (ns == NULL) {
        pthread_mutex_unlock(&loop->namespaces_mutex);
        return NULL;
    }

    ns->owner_pid = *pid;
    ns->globals = PyDict_New();
    ns->locals = PyDict_New();
    ns->module_cache = PyDict_New();

    if (ns->globals == NULL || ns->locals == NULL || ns->module_cache == NULL) {
        Py_XDECREF(ns->globals);
        Py_XDECREF(ns->locals);
        Py_XDECREF(ns->module_cache);
        enif_free(ns);
        pthread_mutex_unlock(&loop->namespaces_mutex);
        return NULL;
    }

    /* Import builtins into globals */
    PyObject *builtins = PyEval_GetBuiltins();
    if (builtins != NULL) {
        PyDict_SetItemString(ns->globals, "__builtins__", builtins);
    }

    /* Import erlang module into globals */
    PyObject *erlang_module = PyImport_ImportModule("erlang");
    if (erlang_module != NULL) {
        PyDict_SetItemString(ns->globals, "erlang", erlang_module);
        Py_DECREF(erlang_module);
    }

    /* Monitor process for cleanup */
    if (enif_monitor_process(env, loop, pid, &ns->monitor) != 0) {
        Py_DECREF(ns->globals);
        Py_DECREF(ns->locals);
        Py_DECREF(ns->module_cache);
        enif_free(ns);
        pthread_mutex_unlock(&loop->namespaces_mutex);
        return NULL;
    }

    /* Add to list */
    ns->next = loop->namespaces_head;
    loop->namespaces_head = ns;

    pthread_mutex_unlock(&loop->namespaces_mutex);
    return ns;
}

/**
 * @brief Look up function in process namespace or module
 *
 * For __main__ module, looks in process namespace first.
 * For other modules, uses PyImport_ImportModule.
 *
 * @param loop Event loop (for callable cache)
 * @param ns Process namespace (may be NULL)
 * @param module_name Module name
 * @param func_name Function name
 * @return New reference to callable, or NULL on failure
 *
 * @note Must be called with GIL held
 */
static PyObject *get_function_for_task(
    erlang_event_loop_t *loop,
    process_namespace_t *ns,
    const char *module_name,
    const char *func_name
) {
    PyObject *func = NULL;

    /* For __main__ or _process_, check process namespace first */
    if (ns != NULL &&
        (strcmp(module_name, "__main__") == 0 ||
         strcmp(module_name, "_process_") == 0)) {
        func = PyDict_GetItemString(ns->globals, func_name);
        if (func != NULL) {
            Py_INCREF(func);
            return func;
        }
    }

    /* Try callable cache (uvloop-style optimization) */
    func = callable_cache_lookup(loop, module_name, func_name);
    if (func != NULL) {
        Py_INCREF(func);
        return func;
    }

    /* Cache miss - import module and get function */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        PyErr_Clear();
        return NULL;
    }

    func = PyObject_GetAttrString(module, func_name);
    Py_DECREF(module);

    if (func == NULL) {
        PyErr_Clear();
        return NULL;
    }

    /* Cache for next lookup (only for non-__main__ modules) */
    if (strcmp(module_name, "__main__") != 0 &&
        strcmp(module_name, "_process_") != 0) {
        callable_cache_insert(loop, module_name, func_name, func);
    }

    return func;
}

/* ============================================================================
 * Initialization
 * ============================================================================ */

int event_loop_init(ErlNifEnv *env) {
    /* Create event loop resource type with down callback for process monitors */
    ErlNifResourceTypeInit loop_init = {
        .dtor = event_loop_destructor,
        .stop = NULL,
        .down = event_loop_down,
        .members = 3
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

/**
 * set_event_loop_priv_dir(Path) -> ok
 *
 * Store the priv_dir path for use when importing modules in subinterpreters.
 * Called from Erlang during application startup.
 */
ERL_NIF_TERM nif_set_event_loop_priv_dir(ErlNifEnv *env, int argc,
                                          const ERL_NIF_TERM argv[]) {
    (void)argc;

    ErlNifBinary path_bin;
    if (!enif_inspect_binary(env, argv[0], &path_bin) &&
        !enif_inspect_iolist_as_binary(env, argv[0], &path_bin)) {
        return make_error(env, "invalid_path");
    }

    size_t len = path_bin.size;
    if (len >= sizeof(g_priv_dir)) {
        return make_error(env, "path_too_long");
    }

    memcpy(g_priv_dir, path_bin.data, len);
    g_priv_dir[len] = '\0';
    g_priv_dir_set = true;

    return ATOM_OK;
}

/**
 * @brief Ensure sys.path includes priv_dir before importing modules.
 *
 * This is needed for subinterpreters in shared GIL mode where each
 * interpreter has its own sys.path that doesn't inherit from main.
 *
 * @return true if priv_dir was added or already present, false on error
 */
static bool ensure_priv_dir_in_sys_path(void) {
    if (!g_priv_dir_set || g_priv_dir[0] == '\0') {
        return true;  /* No priv_dir set, skip (will try import anyway) */
    }

    PyObject *sys = PyImport_ImportModule("sys");
    if (sys == NULL) {
        PyErr_Clear();
        return false;
    }

    PyObject *path = PyObject_GetAttrString(sys, "path");
    Py_DECREF(sys);
    if (path == NULL || !PyList_Check(path)) {
        PyErr_Clear();
        Py_XDECREF(path);
        return false;
    }

    /* Check if priv_dir is already in sys.path */
    PyObject *priv_dir_str = PyUnicode_FromString(g_priv_dir);
    if (priv_dir_str == NULL) {
        PyErr_Clear();
        Py_DECREF(path);
        return false;
    }

    int contains = PySequence_Contains(path, priv_dir_str);
    if (contains == 1) {
        /* Already in path */
        Py_DECREF(priv_dir_str);
        Py_DECREF(path);
        return true;
    }

    /* Insert at front of sys.path */
    if (PyList_Insert(path, 0, priv_dir_str) < 0) {
        PyErr_Clear();
        Py_DECREF(priv_dir_str);
        Py_DECREF(path);
        return false;
    }

    Py_DECREF(priv_dir_str);
    Py_DECREF(path);
    return true;
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

    /* Initialize pending_capacity (memset zeros it, but we need the initial value) */
    loop->pending_capacity = INITIAL_PENDING_CAPACITY;

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
    loop->pending_capacity = INITIAL_PENDING_CAPACITY;
    loop->shutdown = false;
    loop->has_router = false;
    loop->has_self = false;
    loop->interp_id = 0;  /* Main interpreter */

    /* Initialize async task queue (uvloop-inspired) */
    loop->task_queue = enif_ioq_create(ERL_NIF_IOQ_NORMAL);
    if (loop->task_queue == NULL) {
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_free_env(loop->msg_env);
        enif_release_resource(loop);
        return make_error(env, "task_queue_alloc_failed");
    }

    if (pthread_mutex_init(&loop->task_queue_mutex, NULL) != 0) {
        enif_ioq_destroy(loop->task_queue);
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_free_env(loop->msg_env);
        enif_release_resource(loop);
        return make_error(env, "task_queue_mutex_init_failed");
    }

    loop->task_queue_initialized = true;
    atomic_store(&loop->task_count, 0);
    atomic_store(&loop->task_wake_pending, false);
    loop->py_loop = NULL;
    loop->py_loop_valid = false;

    /* Initialize Python cache (uvloop-style optimization) */
    loop->cached_asyncio = NULL;
    loop->cached_run_and_send = NULL;
    loop->py_cache_valid = false;

    /* Initialize callable cache */
    memset(loop->callable_cache, 0, sizeof(loop->callable_cache));
    loop->callable_cache_count = 0;

    /* Initialize per-process namespace registry */
    loop->namespaces_head = NULL;
    loop->pid_env_head = NULL;
    if (pthread_mutex_init(&loop->namespaces_mutex, NULL) != 0) {
        pthread_mutex_destroy(&loop->task_queue_mutex);
        enif_ioq_destroy(loop->task_queue);
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_free_env(loop->msg_env);
        enif_release_resource(loop);
        return make_error(env, "namespaces_mutex_init_failed");
    }

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
 * event_loop_set_worker(LoopRef, WorkerPid) -> ok
 * Scalable I/O model: set the worker process for direct event routing.
 */
ERL_NIF_TERM nif_event_loop_set_worker(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    if (!enif_get_local_pid(env, argv[1], &loop->worker_pid)) {
        return make_error(env, "invalid_pid");
    }

    loop->has_worker = true;

    /* Also set as router for compatibility */
    if (!loop->has_router) {
        loop->router_pid = loop->worker_pid;
        loop->has_router = true;
    }

    return ATOM_OK;
}

/**
 * event_loop_set_id(LoopRef, LoopId) -> ok
 */
ERL_NIF_TERM nif_event_loop_set_id(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    ErlNifBinary id_bin;
    if (!enif_inspect_binary(env, argv[1], &id_bin)) {
        char atom_buf[64];
        if (!enif_get_atom(env, argv[1], atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
            return make_error(env, "invalid_id");
        }
        strncpy(loop->loop_id, atom_buf, sizeof(loop->loop_id) - 1);
        loop->loop_id[sizeof(loop->loop_id) - 1] = '\0';
    } else {
        size_t copy_len = id_bin.size < sizeof(loop->loop_id) - 1 ?
                          id_bin.size : sizeof(loop->loop_id) - 1;
        memcpy(loop->loop_id, id_bin.data, copy_len);
        loop->loop_id[copy_len] = '\0';
    }

    return ATOM_OK;
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

    /* Scalable I/O: prefer worker, fall back to router */
    if (!event_loop_ensure_router(loop)) {
        return make_error(env, "no_router");
    }
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;

    /* Allocate fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE,
                                                 sizeof(fd_resource_t));
    if (fd_res == NULL) {
        return make_error(env, "alloc_failed");
    }

    fd_res->fd = fd;
    fd_res->read_callback_id = callback_id;
    fd_res->write_callback_id = 0;
    fd_res->owner_pid = *target_pid;
    fd_res->reader_active = true;
    fd_res->writer_active = false;
    fd_res->loop = loop;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Monitor owner process for cleanup on death */
    if (enif_monitor_process(env, fd_res, target_pid,
                             &fd_res->owner_monitor) == 0) {
        fd_res->monitor_active = true;
    }

    /* Register with Erlang scheduler for read monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd, ERL_NIF_SELECT_READ,
                          fd_res, target_pid, enif_make_ref(env));

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

    /* Scalable I/O: prefer worker, fall back to router */
    if (!event_loop_ensure_router(loop)) {
        return make_error(env, "no_router");
    }
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;

    /* Allocate fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE,
                                                 sizeof(fd_resource_t));
    if (fd_res == NULL) {
        return make_error(env, "alloc_failed");
    }

    fd_res->fd = fd;
    fd_res->read_callback_id = 0;
    fd_res->write_callback_id = callback_id;
    fd_res->owner_pid = *target_pid;
    fd_res->reader_active = false;
    fd_res->writer_active = true;
    fd_res->loop = loop;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Monitor owner process for cleanup on death */
    if (enif_monitor_process(env, fd_res, target_pid,
                             &fd_res->owner_monitor) == 0) {
        fd_res->monitor_active = true;
    }

    /* Register with Erlang scheduler for write monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd, ERL_NIF_SELECT_WRITE,
                          fd_res, target_pid, enif_make_ref(env));

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

    /* Scalable I/O: prefer worker, fall back to router */
    if (!event_loop_ensure_router(loop)) {
        return make_error(env, "no_router");
    }
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;

    /* Create timer reference */
    ERL_NIF_TERM timer_ref = enif_make_ref(env);

    /* Send message to target: {start_timer, DelayMs, CallbackId, TimerRef} */
    ERL_NIF_TERM msg = enif_make_tuple4(
        env,
        ATOM_START_TIMER,
        enif_make_int(env, delay_ms),
        enif_make_uint64(env, callback_id),
        timer_ref
    );

    if (!enif_send(env, target_pid, NULL, msg)) {
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

    /* Scalable I/O: prefer worker, fall back to router */
    if (!event_loop_ensure_router(loop)) {
        return make_error(env, "no_router");
    }
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;

    /* Send message to target: {cancel_timer, TimerRef} */
    ERL_NIF_TERM msg = enif_make_tuple2(env, ATOM_CANCEL_TIMER, timer_ref);

    if (!enif_send(env, target_pid, NULL, msg)) {
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

    /* Reset wake_pending flag since we're about to process events */
    atomic_store(&loop->wake_pending, false);

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
    if (tstate != NULL && runtime_is_running() && PyGILState_Check()) {
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
     *
     * Optimization: Count elements first, then use enif_make_list_from_array
     * to build the list in O(n) instead of O(2n) with build-then-reverse.
     */

    /* Count events in the snapshot */
    size_t count = 0;
    pending_event_t *current = snapshot_head;
    while (current != NULL) {
        count++;
        current = current->next;
    }

    if (count == 0) {
        return enif_make_list(env, 0);
    }

    /* Allocate array for terms - use stack for small counts, heap for large */
    ERL_NIF_TERM *terms;
    ERL_NIF_TERM stack_terms[64];
    bool heap_allocated = false;

    if (count <= 64) {
        terms = stack_terms;
    } else {
        terms = enif_alloc(count * sizeof(ERL_NIF_TERM));
        if (terms == NULL) {
            /* Fallback: free events and return empty list */
            current = snapshot_head;
            while (current != NULL) {
                pending_event_t *next = current->next;
                enif_free(current);
                current = next;
            }
            return enif_make_list(env, 0);
        }
        heap_allocated = true;
    }

    /* Build terms array in forward order (matching linked list order) */
    current = snapshot_head;
    size_t i = 0;
    while (current != NULL && i < count) {
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

        terms[i] = enif_make_tuple2(
            env,
            enif_make_uint64(env, current->callback_id),
            type_atom
        );

        pending_event_t *next = current->next;
        enif_free(current);
        current = next;
        i++;
    }

    /* Build list from array in O(n) */
    ERL_NIF_TERM result = enif_make_list_from_array(env, terms, (unsigned int)i);

    if (heap_allocated) {
        enif_free(terms);
    }

    return result;
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
 * Adds timer event to pending queue and sends task_ready to worker
 * to trigger process_ready_tasks. This ensures _run_once is called
 * to handle the timer callback.
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

    /* Note: We rely on event_loop_add_pending signaling the condition variable
     * to wake up poll_events_wait. This works for both:
     * - erlang.run() inside py:exec: Python loop is waiting on poll_events_wait
     * - create_task: The worker is triggered by its own timer handling
     *
     * We don't send task_ready to the global worker here because dispatch_timer
     * may be called on a loop different from the one the global worker manages. */

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
 * Combined operation: handles FD event AND reselects for next event.
 * This eliminates one roundtrip - the worker can dispatch and reselect
 * in a single NIF call.
 *
 * Safe because:
 * - Duplicate detection in pending queue prevents flooding
 * - OTP 28+ has optimized pollset for frequently re-enabled FDs
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

    /* Add to pending queue (has duplicate detection) */
    event_type_t event_type = is_read ? EVENT_TYPE_READ : EVENT_TYPE_WRITE;
    event_loop_add_pending(loop, event_type, callback_id, fd_res->fd);

    /* Immediately reselect for next event.
     * Use ATOM_UNDEFINED instead of enif_make_ref to avoid per-event allocation.
     * The ref is ignored by the worker anyway. */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int select_flags = is_read ? ERL_NIF_SELECT_READ : ERL_NIF_SELECT_WRITE;
    enif_select(env, (ErlNifEvent)fd_res->fd, select_flags,
                fd_res, target_pid, ATOM_UNDEFINED);

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

/**
 * event_loop_run_async(LoopRef, CallerPid, Ref, Module, Func, Args, Kwargs) -> ok | {error, Reason}
 *
 * Submit an async coroutine to run on the event loop. When the coroutine
 * completes, the result is sent to CallerPid via erlang.send().
 *
 * This replaces the pthread+usleep polling model with direct message passing.
 */
ERL_NIF_TERM nif_event_loop_run_async(ErlNifEnv *env, int argc,
                                       const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    ErlNifPid caller_pid;
    ErlNifBinary module_bin, func_bin;

    /* Reject work if Python runtime is shutting down */
    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    /* Get loop reference */
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    /* Get caller PID */
    if (!enif_get_local_pid(env, argv[1], &caller_pid)) {
        return make_error(env, "invalid_caller_pid");
    }

    /* argv[2] is the reference - we'll pass it to Python */
    ERL_NIF_TERM ref_term = argv[2];

    /* Get module and function names */
    if (!enif_inspect_binary(env, argv[3], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[4], &func_bin)) {
        return make_error(env, "invalid_func");
    }

    /* Convert args list - argv[5] */
    /* Convert kwargs map - argv[6] */

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Convert module/func names to C strings */
    char *module_name = enif_alloc(module_bin.size + 1);
    char *func_name = enif_alloc(func_bin.size + 1);
    if (module_name == NULL || func_name == NULL) {
        enif_free(module_name);
        enif_free(func_name);
        PyGILState_Release(gstate);
        return make_error(env, "alloc_failed");
    }
    memcpy(module_name, module_bin.data, module_bin.size);
    module_name[module_bin.size] = '\0';
    memcpy(func_name, func_bin.data, func_bin.size);
    func_name[func_bin.size] = '\0';

    ERL_NIF_TERM result;

    /* Import module and get function */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    PyObject *func = PyObject_GetAttrString(module, func_name);
    Py_DECREF(module);
    if (func == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert args list to Python tuple */
    unsigned int args_len;
    if (!enif_get_list_length(env, argv[5], &args_len)) {
        Py_DECREF(func);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[5];
    for (unsigned int i = 0; i < args_len; i++) {
        enif_get_list_cell(env, tail, &head, &tail);
        PyObject *arg = term_to_py(env, head);
        if (arg == NULL) {
            Py_DECREF(args);
            Py_DECREF(func);
            result = make_error(env, "arg_conversion_failed");
            goto cleanup;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }

    /* Convert kwargs */
    PyObject *kwargs = NULL;
    if (argc > 6 && enif_is_map(env, argv[6])) {
        kwargs = term_to_py(env, argv[6]);
    }

    /* Call the function to get coroutine */
    PyObject *coro = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (coro == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Check if result is a coroutine */
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (asyncio == NULL) {
        Py_DECREF(coro);
        result = make_error(env, "asyncio_import_failed");
        goto cleanup;
    }

    PyObject *iscoroutine = PyObject_CallMethod(asyncio, "iscoroutine", "O", coro);
    bool is_coro = iscoroutine != NULL && PyObject_IsTrue(iscoroutine);
    Py_XDECREF(iscoroutine);

    if (!is_coro) {
        Py_DECREF(asyncio);
        /* Not a coroutine - convert result and send immediately */
        PyObject *erlang_mod = PyImport_ImportModule("erlang");
        if (erlang_mod == NULL) {
            Py_DECREF(coro);
            result = make_py_error(env);
            goto cleanup;
        }

        /* Create the caller PID object */
        extern PyTypeObject ErlangPidType;
        ErlangPidObject *pid_obj = PyObject_New(ErlangPidObject, &ErlangPidType);
        if (pid_obj == NULL) {
            Py_DECREF(erlang_mod);
            Py_DECREF(coro);
            result = make_error(env, "pid_alloc_failed");
            goto cleanup;
        }
        pid_obj->pid = caller_pid;

        /* Convert ref and result to Python */
        PyObject *py_ref = term_to_py(env, ref_term);
        if (py_ref == NULL) {
            Py_DECREF((PyObject *)pid_obj);
            Py_DECREF(erlang_mod);
            Py_DECREF(coro);
            result = make_error(env, "ref_conversion_failed");
            goto cleanup;
        }

        /* Build result tuple: ('async_result', ref, ('ok', result)) */
        PyObject *ok_str = PyUnicode_FromString("ok");
        PyObject *async_result_str = PyUnicode_FromString("async_result");
        if (ok_str == NULL || async_result_str == NULL) {
            Py_XDECREF(ok_str);
            Py_XDECREF(async_result_str);
            Py_DECREF(py_ref);
            Py_DECREF((PyObject *)pid_obj);
            Py_DECREF(erlang_mod);
            Py_DECREF(coro);
            result = make_error(env, "string_alloc_failed");
            goto cleanup;
        }

        PyObject *ok_tuple = PyTuple_Pack(2, ok_str, coro);
        Py_DECREF(ok_str);  /* PyTuple_Pack increments refcount */
        if (ok_tuple == NULL) {
            Py_DECREF(async_result_str);
            Py_DECREF(py_ref);
            Py_DECREF((PyObject *)pid_obj);
            Py_DECREF(erlang_mod);
            Py_DECREF(coro);
            result = make_error(env, "tuple_alloc_failed");
            goto cleanup;
        }

        PyObject *msg = PyTuple_Pack(3, async_result_str, py_ref, ok_tuple);
        Py_DECREF(async_result_str);  /* PyTuple_Pack increments refcount */
        Py_DECREF(ok_tuple);  /* PyTuple_Pack increments refcount */
        if (msg == NULL) {
            Py_DECREF(py_ref);
            Py_DECREF((PyObject *)pid_obj);
            Py_DECREF(erlang_mod);
            Py_DECREF(coro);
            result = make_error(env, "tuple_alloc_failed");
            goto cleanup;
        }

        /* Send via erlang.send() */
        PyObject *send_result = PyObject_CallMethod(erlang_mod, "send", "OO",
                                                     (PyObject *)pid_obj, msg);
        Py_XDECREF(send_result);
        Py_DECREF(msg);
        Py_DECREF(py_ref);
        Py_DECREF((PyObject *)pid_obj);
        Py_DECREF(erlang_mod);
        Py_DECREF(coro);

        result = ATOM_OK;
        goto cleanup;
    }

    /* Import erlang_loop to get _run_and_send */
    /* Ensure priv_dir is in sys.path for subinterpreter contexts */
    ensure_priv_dir_in_sys_path();

    PyObject *erlang_loop = PyImport_ImportModule("erlang_loop");
    if (erlang_loop == NULL) {
        /* Try _erlang_impl._loop as fallback */
        PyErr_Clear();
        erlang_loop = PyImport_ImportModule("_erlang_impl._loop");
    }
    if (erlang_loop == NULL) {
        Py_DECREF(asyncio);
        Py_DECREF(coro);
        result = make_error(env, "erlang_loop_import_failed");
        goto cleanup;
    }

    PyObject *run_and_send = PyObject_GetAttrString(erlang_loop, "_run_and_send");
    Py_DECREF(erlang_loop);
    if (run_and_send == NULL) {
        Py_DECREF(asyncio);
        Py_DECREF(coro);
        result = make_error(env, "run_and_send_not_found");
        goto cleanup;
    }

    /* Create the caller PID object */
    extern PyTypeObject ErlangPidType;
    ErlangPidObject *pid_obj = PyObject_New(ErlangPidObject, &ErlangPidType);
    if (pid_obj == NULL) {
        Py_DECREF(run_and_send);
        Py_DECREF(asyncio);
        Py_DECREF(coro);
        result = make_error(env, "pid_alloc_failed");
        goto cleanup;
    }
    pid_obj->pid = caller_pid;

    /* Convert ref to Python */
    PyObject *py_ref = term_to_py(env, ref_term);
    if (py_ref == NULL) {
        Py_DECREF((PyObject *)pid_obj);
        Py_DECREF(run_and_send);
        Py_DECREF(asyncio);
        Py_DECREF(coro);
        result = make_error(env, "ref_conversion_failed");
        goto cleanup;
    }

    /* Create wrapped coroutine: _run_and_send(coro, caller_pid, ref) */
    PyObject *wrapped_coro = PyObject_CallFunction(run_and_send, "OOO",
                                                    coro, (PyObject *)pid_obj, py_ref);
    Py_DECREF(run_and_send);
    Py_DECREF(coro);
    Py_DECREF((PyObject *)pid_obj);
    Py_DECREF(py_ref);

    if (wrapped_coro == NULL) {
        Py_DECREF(asyncio);
        result = make_py_error(env);
        goto cleanup;
    }

    /* Get the running event loop and create a task */
    PyObject *get_loop = PyObject_CallMethod(asyncio, "get_event_loop", NULL);
    if (get_loop == NULL) {
        PyErr_Clear();
        /* Try to use the event loop policy instead */
        get_loop = PyObject_CallMethod(asyncio, "get_running_loop", NULL);
    }

    if (get_loop == NULL) {
        PyErr_Clear();
        Py_DECREF(wrapped_coro);
        Py_DECREF(asyncio);
        result = make_error(env, "no_running_loop");
        goto cleanup;
    }

    /* Schedule the task on the loop */
    PyObject *task = PyObject_CallMethod(get_loop, "create_task", "O", wrapped_coro);
    Py_DECREF(wrapped_coro);
    Py_DECREF(get_loop);
    Py_DECREF(asyncio);

    if (task == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    Py_DECREF(task);
    result = ATOM_OK;

cleanup:
    enif_free(module_name);
    enif_free(func_name);
    PyGILState_Release(gstate);

    return result;
}

/* ============================================================================
 * Callable Cache (uvloop-style optimization)
 * ============================================================================ */

/**
 * @brief Hash function for callable cache lookup
 *
 * Simple djb2-style hash combining module and function names.
 */
static inline uint32_t callable_cache_hash(const char *module, const char *func) {
    uint32_t hash = 5381;
    const char *c = module;
    while (*c) {
        hash = ((hash << 5) + hash) + (uint8_t)*c++;
    }
    c = func;
    while (*c) {
        hash = ((hash << 5) + hash) + (uint8_t)*c++;
    }
    return hash % CALLABLE_CACHE_SIZE;
}

/**
 * @brief Look up a cached callable
 *
 * @param loop Event loop containing the cache
 * @param module Module name
 * @param func Function name
 * @return Cached callable or NULL if not found
 */
static PyObject *callable_cache_lookup(erlang_event_loop_t *loop,
                                        const char *module, const char *func) {
    if (loop->callable_cache_count == 0) {
        return NULL;
    }

    uint32_t idx = callable_cache_hash(module, func);

    /* Linear probing with wraparound */
    for (int i = 0; i < CALLABLE_CACHE_SIZE; i++) {
        uint32_t probe = (idx + i) % CALLABLE_CACHE_SIZE;
        cached_callable_t *entry = &loop->callable_cache[probe];

        if (entry->callable == NULL) {
            return NULL;  /* Empty slot, not found */
        }

        if (strcmp(entry->module_name, module) == 0 &&
            strcmp(entry->func_name, func) == 0) {
            entry->hits++;
            return entry->callable;
        }
    }
    return NULL;
}

/**
 * @brief Insert a callable into the cache
 *
 * @param loop Event loop containing the cache
 * @param module Module name
 * @param func Function name
 * @param callable Python callable to cache (borrowed reference)
 * @return true if inserted, false if cache full
 */
static bool callable_cache_insert(erlang_event_loop_t *loop,
                                   const char *module, const char *func,
                                   PyObject *callable) {
    /* Don't insert if cache is full (load factor > 0.75) */
    if (loop->callable_cache_count >= (CALLABLE_CACHE_SIZE * 3) / 4) {
        return false;
    }

    /* Check name lengths */
    if (strlen(module) >= CALLABLE_NAME_MAX || strlen(func) >= CALLABLE_NAME_MAX) {
        return false;
    }

    uint32_t idx = callable_cache_hash(module, func);

    /* Linear probing to find empty slot */
    for (int i = 0; i < CALLABLE_CACHE_SIZE; i++) {
        uint32_t probe = (idx + i) % CALLABLE_CACHE_SIZE;
        cached_callable_t *entry = &loop->callable_cache[probe];

        if (entry->callable == NULL) {
            /* Found empty slot */
            strncpy(entry->module_name, module, CALLABLE_NAME_MAX - 1);
            entry->module_name[CALLABLE_NAME_MAX - 1] = '\0';
            strncpy(entry->func_name, func, CALLABLE_NAME_MAX - 1);
            entry->func_name[CALLABLE_NAME_MAX - 1] = '\0';
            Py_INCREF(callable);
            entry->callable = callable;
            entry->hits = 0;
            loop->callable_cache_count++;
            return true;
        }

        /* Check if already cached (duplicate insert) */
        if (strcmp(entry->module_name, module) == 0 &&
            strcmp(entry->func_name, func) == 0) {
            return true;  /* Already cached */
        }
    }
    return false;
}

/**
 * @brief Clear the callable cache
 *
 * Called during loop destruction to release cached references.
 */
static void callable_cache_clear(erlang_event_loop_t *loop) {
    for (int i = 0; i < CALLABLE_CACHE_SIZE; i++) {
        cached_callable_t *entry = &loop->callable_cache[i];
        if (entry->callable != NULL) {
            Py_DECREF(entry->callable);
            entry->callable = NULL;
        }
        entry->module_name[0] = '\0';
        entry->func_name[0] = '\0';
        entry->hits = 0;
    }
    loop->callable_cache_count = 0;
}

/* ============================================================================
 * Async Task Queue NIFs (uvloop-inspired)
 * ============================================================================ */

/** Atom for task_ready wakeup message */
static ERL_NIF_TERM ATOM_TASK_READY;

/**
 * submit_task(LoopRef, CallerPid, Ref, Module, Func, Args, Kwargs) -> ok | {error, Reason}
 *
 * Thread-safe task submission. Serializes task info, enqueues to the task_queue,
 * and sends 'task_ready' wakeup to the worker via enif_send.
 *
 * This works from any thread including dirty schedulers because:
 * 1. enif_ioq operations are thread-safe
 * 2. enif_send works without GIL and from any thread
 * 3. No Python API calls are made
 */
ERL_NIF_TERM nif_submit_task(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    if (!loop->task_queue_initialized) {
        return make_error(env, "task_queue_not_initialized");
    }

    /* Validate caller_pid */
    ErlNifPid caller_pid;
    if (!enif_get_local_pid(env, argv[1], &caller_pid)) {
        return make_error(env, "invalid_caller_pid");
    }

    /* Create task tuple: {CallerPid, Ref, Module, Func, Args, Kwargs} */
    /* argv[1] = CallerPid, argv[2] = Ref, argv[3] = Module,
     * argv[4] = Func, argv[5] = Args, argv[6] = Kwargs */
    ERL_NIF_TERM task_tuple = enif_make_tuple6(env,
        argv[1], argv[2], argv[3], argv[4], argv[5], argv[6]);

    /* Serialize to binary */
    ErlNifBinary task_bin;
    if (!enif_term_to_binary(env, task_tuple, &task_bin)) {
        return make_error(env, "serialization_failed");
    }

    /* Thread-safe enqueue */
    pthread_mutex_lock(&loop->task_queue_mutex);
    int enq_result = enif_ioq_enq_binary(loop->task_queue, &task_bin, 0);
    pthread_mutex_unlock(&loop->task_queue_mutex);

    if (enq_result != 1) {
        enif_release_binary(&task_bin);
        return make_error(env, "enqueue_failed");
    }

    /* Increment task count */
    atomic_fetch_add(&loop->task_count, 1);

    /*
     * Coalesced wakeup (uvloop-style): Only send task_ready if we're the
     * first task since the last drain. This reduces message traffic under
     * high task submission rates.
     */
    if (loop->has_worker) {
        if (!atomic_exchange(&loop->task_wake_pending, true)) {
            /* We're the first since last drain - send wakeup */
            ErlNifEnv *msg_env = enif_alloc_env();
            if (msg_env != NULL) {
                /* Initialize ATOM_TASK_READY if needed (safe to do multiple times) */
                if (ATOM_TASK_READY == 0) {
                    ATOM_TASK_READY = enif_make_atom(msg_env, "task_ready");
                }
                ERL_NIF_TERM msg = enif_make_atom(msg_env, "task_ready");
                enif_send(NULL, &loop->worker_pid, msg_env, msg);
                enif_free_env(msg_env);
            }
        }
        /* If wake_pending was already true, another task_ready message
         * is already in flight, so no need to send another */
    }

    return ATOM_OK;
}

/* ============================================================================
 * PID-to-Env Mapping Helpers
 * ============================================================================ */

/**
 * @brief Register or update an env mapping for a PID
 *
 * Increments refcount if mapping exists, otherwise creates new mapping.
 * Calls enif_keep_resource to keep the env alive.
 *
 * @param loop Event loop containing the mapping registry
 * @param pid PID to register
 * @param env_res Environment resource (will be kept via enif_keep_resource)
 * @return true on success, false on allocation failure
 */
static bool register_pid_env(erlang_event_loop_t *loop, const ErlNifPid *pid,
                              void *env_res) {
    pthread_mutex_lock(&loop->namespaces_mutex);

    /* Check if mapping already exists */
    pid_env_mapping_t *mapping = loop->pid_env_head;
    while (mapping != NULL) {
        if (enif_compare_pids(&mapping->pid, pid) == 0) {
            /* Found existing mapping - increment refcount */
            mapping->refcount++;
            pthread_mutex_unlock(&loop->namespaces_mutex);
            return true;
        }
        mapping = mapping->next;
    }

    /* Create new mapping */
    mapping = enif_alloc(sizeof(pid_env_mapping_t));
    if (mapping == NULL) {
        pthread_mutex_unlock(&loop->namespaces_mutex);
        return false;
    }

    mapping->pid = *pid;
    mapping->env = env_res;
    mapping->refcount = 1;
    mapping->next = loop->pid_env_head;
    loop->pid_env_head = mapping;

    /* Keep the resource alive */
    enif_keep_resource(env_res);

    pthread_mutex_unlock(&loop->namespaces_mutex);
    return true;
}

/**
 * @brief Look up env for a PID
 *
 * @param loop Event loop containing the mapping registry
 * @param pid PID to look up
 * @return Environment resource or NULL if not found
 */
static void *lookup_pid_env(erlang_event_loop_t *loop, const ErlNifPid *pid) {
    pthread_mutex_lock(&loop->namespaces_mutex);

    pid_env_mapping_t *mapping = loop->pid_env_head;
    while (mapping != NULL) {
        if (enif_compare_pids(&mapping->pid, pid) == 0) {
            void *env_res = mapping->env;
            pthread_mutex_unlock(&loop->namespaces_mutex);
            return env_res;
        }
        mapping = mapping->next;
    }

    pthread_mutex_unlock(&loop->namespaces_mutex);
    return NULL;
}

/**
 * submit_task_with_env(LoopRef, CallerPid, Ref, Module, Func, Args, Kwargs, EnvRef) -> ok | {error, Reason}
 *
 * Like submit_task but registers the process-local env for the caller PID.
 * The env's globals dict is used for function lookup, allowing functions
 * defined via py:exec() to be called from the event loop.
 *
 * Note: The env resource is stored in a PID->env mapping, not serialized.
 * This avoids the issue of resource references not surviving serialization.
 */
ERL_NIF_TERM nif_submit_task_with_env(ErlNifEnv *env, int argc,
                                       const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    if (!loop->task_queue_initialized) {
        return make_error(env, "task_queue_not_initialized");
    }

    /* Validate caller_pid */
    ErlNifPid caller_pid;
    if (!enif_get_local_pid(env, argv[1], &caller_pid)) {
        return make_error(env, "invalid_caller_pid");
    }

    /* Get and register the env resource */
    void *env_res;
    if (!enif_get_resource(env, argv[7], get_env_resource_type(), &env_res)) {
        return make_error(env, "invalid_env");
    }

    /* Register the env for this PID (increments refcount if exists) */
    if (!register_pid_env(loop, &caller_pid, env_res)) {
        return make_error(env, "env_registration_failed");
    }

    /* Create task tuple: {CallerPid, Ref, Module, Func, Args, Kwargs}
     * Note: We use 6-tuple, NOT 7-tuple. The env is looked up by PID. */
    ERL_NIF_TERM task_tuple = enif_make_tuple6(env,
        argv[1], argv[2], argv[3], argv[4], argv[5], argv[6]);

    /* Serialize to binary */
    ErlNifBinary task_bin;
    if (!enif_term_to_binary(env, task_tuple, &task_bin)) {
        return make_error(env, "serialization_failed");
    }

    /* Thread-safe enqueue */
    pthread_mutex_lock(&loop->task_queue_mutex);
    int enq_result = enif_ioq_enq_binary(loop->task_queue, &task_bin, 0);
    pthread_mutex_unlock(&loop->task_queue_mutex);

    if (enq_result != 1) {
        enif_release_binary(&task_bin);
        return make_error(env, "enqueue_failed");
    }

    /* Increment task count */
    atomic_fetch_add(&loop->task_count, 1);

    /* Coalesced wakeup (uvloop-style) */
    if (loop->has_worker) {
        if (!atomic_exchange(&loop->task_wake_pending, true)) {
            ErlNifEnv *msg_env = enif_alloc_env();
            if (msg_env != NULL) {
                if (ATOM_TASK_READY == 0) {
                    ATOM_TASK_READY = enif_make_atom(msg_env, "task_ready");
                }
                ERL_NIF_TERM msg = enif_make_atom(msg_env, "task_ready");
                enif_send(NULL, &loop->worker_pid, msg_env, msg);
                enif_free_env(msg_env);
            }
        }
    }

    return ATOM_OK;
}

/**
 * Maximum tasks to dequeue in one batch before acquiring GIL.
 * This bounds memory usage while still amortizing GIL acquisition cost.
 */
#define MAX_TASK_BATCH 64

/**
 * Structure to hold a dequeued task (before GIL acquisition).
 */
typedef struct {
    ErlNifEnv *term_env;
    ERL_NIF_TERM task_term;
} dequeued_task_t;

/**
 * process_ready_tasks(LoopRef) -> ok | {error, Reason}
 *
 * Called by the event worker when it receives 'task_ready' message.
 * Dequeues all pending tasks, creates coroutines, and schedules them on py_loop.
 *
 * Optimizations (uvloop-style):
 * - Dequeue ALL tasks BEFORE acquiring GIL (NIF ops don't need GIL)
 * - Acquire GIL once, process entire batch, release
 * - Cache Python imports (asyncio, _run_and_send) across calls
 * - Only call _run_once if coroutines were actually scheduled
 */
ERL_NIF_TERM nif_process_ready_tasks(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    if (!loop->task_queue_initialized) {
        return make_error(env, "task_queue_not_initialized");
    }

    /*
     * Reset wake_pending flag at START of processing.
     * This allows submit_task to send new wakeups for tasks submitted during
     * our processing. The worker's drain-until-empty loop will catch them.
     *
     * IMPORTANT: Must be cleared BEFORE the task_count check to avoid a race:
     * - Worker receives task_ready, calls process_ready_tasks
     * - Tasks processed, wake_pending cleared, new tasks submitted (wake sent)
     * - Worker receives task_ready in drain loop, calls process_ready_tasks
     * - task_count == 0 (already processed), but wake_pending still true!
     * - Early return leaves wake_pending true, blocking future wakeups
     */
    atomic_store(&loop->task_wake_pending, false);

    /* OPTIMIZATION: Check if there's any work BEFORE acquiring GIL
     * This avoids expensive GIL acquisition when there's nothing to do.
     *
     * We need to check BOTH:
     * - task_count: new tasks submitted via submit_task
     * - pending_count: timer/FD events dispatched via dispatch_timer/handle_fd_event
     *
     * If either has work, we need to proceed and call _run_once. */
    uint_fast64_t task_count = atomic_load(&loop->task_count);
    int pending_count = atomic_load(&loop->pending_count);
    if (task_count == 0 && pending_count == 0) {
        return ATOM_OK;  /* Nothing to process, skip GIL entirely */
    }

    /* Check if Python runtime is running */
    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    /* ========================================================================
     * PHASE 1: Dequeue all tasks WITHOUT GIL (NIF operations only)
     * ======================================================================== */

    dequeued_task_t tasks[MAX_TASK_BATCH];
    int num_tasks = 0;

    pthread_mutex_lock(&loop->task_queue_mutex);

    SysIOVec *iov;
    int iovcnt;

    while (num_tasks < MAX_TASK_BATCH && enif_ioq_size(loop->task_queue) > 0) {
        iov = enif_ioq_peek(loop->task_queue, &iovcnt);
        if (iov == NULL || iovcnt == 0) {
            break;
        }

        /* Get the first IOVec element */
        ErlNifBinary task_bin;
        task_bin.data = iov[0].iov_base;
        task_bin.size = iov[0].iov_len;

        /* Deserialize task tuple (NIF operation, no GIL needed) */
        ErlNifEnv *term_env = enif_alloc_env();
        if (term_env == NULL) {
            break;  /* Will process what we have so far */
        }

        ERL_NIF_TERM task_term;
        if (enif_binary_to_term(term_env, task_bin.data, task_bin.size,
                                &task_term, 0) == 0) {
            enif_free_env(term_env);
            /* Dequeue and skip this malformed task */
            enif_ioq_deq(loop->task_queue, iov[0].iov_len, NULL);
            atomic_fetch_sub(&loop->task_count, 1);
            continue;
        }

        /* Store for later processing */
        tasks[num_tasks].term_env = term_env;
        tasks[num_tasks].task_term = task_term;
        num_tasks++;

        /* Dequeue (we've copied the data) */
        enif_ioq_deq(loop->task_queue, iov[0].iov_len, NULL);
        atomic_fetch_sub(&loop->task_count, 1);
    }

    pthread_mutex_unlock(&loop->task_queue_mutex);

    /* NOTE: We do NOT return early here even if num_tasks == 0.
     * We may have pending timer/FD events that need _run_once to process.
     * The first check (task_count == 0 && pending_count == 0) at the start
     * of this function already handles the case where there's truly no work. */

    /* ========================================================================
     * PHASE 2: Process all tasks WITH GIL (Python operations)
     * ======================================================================== */

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* OPTIMIZATION: Use cached Python imports (uvloop-style)
     * Avoids PyImport_ImportModule on every call */
    PyObject *asyncio;
    PyObject *run_and_send;

    /* For thread-local event loop context (dirty NIF scheduler workaround) */
    PyObject *events_module = NULL;
    PyObject *old_running_loop = NULL;

    if (loop->py_cache_valid && loop->cached_asyncio != NULL && loop->cached_run_and_send != NULL) {
        /* Use cached references */
        asyncio = loop->cached_asyncio;
        run_and_send = loop->cached_run_and_send;
    } else {
        /* First call or cache invalidated - populate cache */
        asyncio = PyImport_ImportModule("asyncio");
        if (asyncio == NULL) {
            /* Cleanup dequeued tasks */
            for (int i = 0; i < num_tasks; i++) {
                enif_free_env(tasks[i].term_env);
            }
            PyGILState_Release(gstate);
            return make_error(env, "asyncio_import_failed");
        }

        /* Ensure priv_dir is in sys.path for subinterpreter contexts */
        ensure_priv_dir_in_sys_path();

        PyObject *erlang_loop_mod = PyImport_ImportModule("_erlang_impl._loop");
        if (erlang_loop_mod == NULL) {
            PyErr_Clear();
            erlang_loop_mod = PyImport_ImportModule("erlang_loop");
        }
        if (erlang_loop_mod == NULL) {
            Py_DECREF(asyncio);
            for (int i = 0; i < num_tasks; i++) {
                enif_free_env(tasks[i].term_env);
            }
            PyGILState_Release(gstate);
            return make_error(env, "erlang_loop_import_failed");
        }

        run_and_send = PyObject_GetAttrString(erlang_loop_mod, "_run_and_send");
        Py_DECREF(erlang_loop_mod);
        if (run_and_send == NULL) {
            Py_DECREF(asyncio);
            for (int i = 0; i < num_tasks; i++) {
                enif_free_env(tasks[i].term_env);
            }
            PyGILState_Release(gstate);
            return make_error(env, "run_and_send_not_found");
        }

        /* Store in cache */
        loop->cached_asyncio = asyncio;
        loop->cached_run_and_send = run_and_send;
        loop->py_cache_valid = true;
    }

    /* Lazy loop creation (uvloop-style): create Python loop on first use */
    if (!loop->py_loop_valid || loop->py_loop == NULL) {
        /* Create ErlangEventLoop directly instead of via asyncio.new_event_loop().
         * This is necessary because dirty NIF scheduler threads don't have the
         * event loop policy set. asyncio.new_event_loop() would create a
         * SelectorEventLoop instead of our ErlangEventLoop. */
        PyObject *erlang_loop_mod = PyImport_ImportModule("_erlang_impl._loop");
        if (erlang_loop_mod == NULL) {
            PyErr_Clear();
            erlang_loop_mod = PyImport_ImportModule("erlang_loop");
        }
        if (erlang_loop_mod == NULL) {
            PyErr_Clear();
            for (int i = 0; i < num_tasks; i++) {
                enif_free_env(tasks[i].term_env);
            }
            PyGILState_Release(gstate);
            return make_error(env, "loop_module_import_failed");
        }

        PyObject *loop_class = PyObject_GetAttrString(erlang_loop_mod, "ErlangEventLoop");
        Py_DECREF(erlang_loop_mod);
        if (loop_class == NULL) {
            PyErr_Clear();
            for (int i = 0; i < num_tasks; i++) {
                enif_free_env(tasks[i].term_env);
            }
            PyGILState_Release(gstate);
            return make_error(env, "loop_class_not_found");
        }

        PyObject *new_loop = PyObject_CallNoArgs(loop_class);
        Py_DECREF(loop_class);
        if (new_loop == NULL) {
            PyErr_Clear();
            for (int i = 0; i < num_tasks; i++) {
                enif_free_env(tasks[i].term_env);
            }
            PyGILState_Release(gstate);
            return make_error(env, "loop_creation_failed");
        }

        /* Set as current event loop for this thread */
        PyObject *set_result = PyObject_CallMethod(asyncio, "set_event_loop", "O", new_loop);
        Py_XDECREF(set_result);

        /* ErlangEventLoop.__init__ should have called _set_global_loop_ref,
         * which sets loop->py_loop and loop->py_loop_valid = true */
        if (!loop->py_loop_valid || loop->py_loop == NULL) {
            /* Fallback: manually set the loop reference */
            if (loop->py_loop != NULL) {
                Py_DECREF(loop->py_loop);
            }
            loop->py_loop = new_loop;  /* Transfer ownership */
            loop->py_loop_valid = true;
        } else {
            Py_DECREF(new_loop);
        }
    }

    /* ========================================================================
     * Set event loop in current thread's context (dirty NIF scheduler fix)
     *
     * process_ready_tasks runs on dirty NIF scheduler threads (named 'Dummy-X'),
     * not the main thread. Python's asyncio uses thread-local storage for event
     * loops, so we must explicitly set our loop as both the current event loop
     * and the running loop for this thread.
     *
     * This mirrors what Python's asyncio.run() does internally (see _loop.py).
     * ======================================================================== */
    events_module = PyImport_ImportModule("asyncio.events");
    if (events_module != NULL) {
        /* Set our loop as current event loop for this thread */
        PyObject *set_result = PyObject_CallMethod(asyncio, "set_event_loop", "O", loop->py_loop);
        Py_XDECREF(set_result);

        /* Save and set running loop (needed for asyncio.Task creation) */
        old_running_loop = PyObject_CallMethod(events_module, "_get_running_loop", NULL);
        if (old_running_loop == NULL) {
            PyErr_Clear();
            old_running_loop = Py_NewRef(Py_None);
        }
        PyObject *set_running = PyObject_CallMethod(events_module, "_set_running_loop", "O", loop->py_loop);
        Py_XDECREF(set_running);
    }

    /* Process all dequeued tasks */
    ERL_NIF_TERM result = ATOM_OK;
    int coros_scheduled = 0;  /* Track if any coroutines were scheduled */

    for (int task_idx = 0; task_idx < num_tasks; task_idx++) {
        ErlNifEnv *term_env = tasks[task_idx].term_env;
        ERL_NIF_TERM task_term = tasks[task_idx].task_term;

        /* Extract: {CallerPid, Ref, Module, Func, Args, Kwargs} */
        int arity;
        const ERL_NIF_TERM *tuple_elems;
        if (!enif_get_tuple(term_env, task_term, &arity, &tuple_elems) ||
            arity != 6) {
            enif_free_env(term_env);
            continue;
        }

        ErlNifPid caller_pid;
        if (!enif_get_local_pid(term_env, tuple_elems[0], &caller_pid)) {
            enif_free_env(term_env);
            continue;
        }

        ErlNifBinary module_bin, func_bin;
        if (!enif_inspect_binary(term_env, tuple_elems[2], &module_bin) ||
            !enif_inspect_binary(term_env, tuple_elems[3], &func_bin)) {
            enif_free_env(term_env);
            continue;
        }

        /* Look up env by PID (registered via submit_task_with_env) */
        py_env_resource_t *task_env = (py_env_resource_t *)lookup_pid_env(loop, &caller_pid);

        /* Convert module/func to C strings */
        char *module_name = enif_alloc(module_bin.size + 1);
        char *func_name = enif_alloc(func_bin.size + 1);
        if (module_name == NULL || func_name == NULL) {
            enif_free(module_name);
            enif_free(func_name);
            enif_free_env(term_env);
            continue;
        }
        memcpy(module_name, module_bin.data, module_bin.size);
        module_name[module_bin.size] = '\0';
        memcpy(func_name, func_bin.data, func_bin.size);
        func_name[func_bin.size] = '\0';

        /* Look up namespace for caller process (used for reentrant calls) */
        process_namespace_t *ns = lookup_process_namespace(loop, &caller_pid);

        /* Look up function - check task_env first, then process namespace, then import */
        PyObject *func = NULL;

        /* First, check the passed env's globals (from py:exec) */
        if (task_env != NULL && task_env->globals != NULL) {
            if (strcmp(module_name, "__main__") == 0 ||
                strcmp(module_name, "_process_") == 0) {
                func = PyDict_GetItemString(task_env->globals, func_name);
                if (func != NULL) {
                    Py_INCREF(func);
                }
            }
        }

        /* Fallback to process namespace and cache/import */
        if (func == NULL) {
            func = get_function_for_task(loop, ns, module_name, func_name);
        }

        enif_free(module_name);
        enif_free(func_name);

        if (func == NULL) {
            enif_free_env(term_env);
            continue;
        }

        /* Convert args list to Python tuple */
        unsigned int args_len;
        if (!enif_get_list_length(term_env, tuple_elems[4], &args_len)) {
            Py_DECREF(func);
            enif_free_env(term_env);
            continue;
        }

        PyObject *args = PyTuple_New(args_len);
        ERL_NIF_TERM head, tail = tuple_elems[4];
        bool args_ok = true;
        for (unsigned int i = 0; i < args_len && args_ok; i++) {
            enif_get_list_cell(term_env, tail, &head, &tail);
            PyObject *arg = term_to_py(term_env, head);
            if (arg == NULL) {
                PyErr_Clear();
                args_ok = false;
            } else {
                PyTuple_SET_ITEM(args, i, arg);
            }
        }

        if (!args_ok) {
            Py_DECREF(args);
            Py_DECREF(func);
            enif_free_env(term_env);
            continue;
        }

        /* Convert kwargs */
        PyObject *kwargs = NULL;
        if (enif_is_map(term_env, tuple_elems[5])) {
            kwargs = term_to_py(term_env, tuple_elems[5]);
        }

        /* Set current namespace for reentrant calls (erlang.call -> Python) */
        process_namespace_t *prev_namespace = tl_current_event_loop_namespace;
        tl_current_event_loop_namespace = ns;

        /* Call the function to get coroutine */
        PyObject *coro = PyObject_Call(func, args, kwargs);

        /* Restore previous namespace */
        tl_current_event_loop_namespace = prev_namespace;

        Py_DECREF(func);
        Py_DECREF(args);
        Py_XDECREF(kwargs);

        if (coro == NULL) {
            PyErr_Clear();
            enif_free_env(term_env);
            continue;
        }

        /* Check if result is a coroutine */
        PyObject *iscoroutine = PyObject_CallMethod(asyncio, "iscoroutine", "O", coro);
        bool is_coro = iscoroutine != NULL && PyObject_IsTrue(iscoroutine);
        Py_XDECREF(iscoroutine);

        /* Create caller PID object */
        extern PyTypeObject ErlangPidType;
        ErlangPidObject *pid_obj = PyObject_New(ErlangPidObject, &ErlangPidType);
        if (pid_obj == NULL) {
            Py_DECREF(coro);
            enif_free_env(term_env);
            continue;
        }
        /* Copy PID */
        pid_obj->pid = caller_pid;

        /* Convert ref to Python */
        PyObject *py_ref = term_to_py(term_env, tuple_elems[1]);
        if (py_ref == NULL) {
            PyErr_Clear();
            Py_DECREF((PyObject *)pid_obj);
            Py_DECREF(coro);
            enif_free_env(term_env);
            continue;
        }

        if (is_coro) {
            /* Wrap with _run_and_send and schedule */
            PyObject *wrapped_coro = PyObject_CallFunction(run_and_send, "OOO",
                                                            coro, (PyObject *)pid_obj, py_ref);
            Py_DECREF(coro);

            if (wrapped_coro != NULL) {
                /* Schedule on py_loop */
                PyObject *task = PyObject_CallMethod(loop->py_loop, "create_task", "O", wrapped_coro);
                Py_DECREF(wrapped_coro);
                Py_XDECREF(task);
                coros_scheduled++;
            } else {
                PyErr_Clear();
            }
        } else {
            /* Not a coroutine - send result immediately via enif_send */
            /* Use enif_send directly so we can use proper Erlang atoms */
            /* Use the original Erlang ref term (tuple_elems[1]), not the Python conversion */
            ErlNifEnv *send_env = enif_alloc_env();
            if (send_env != NULL) {
                /* Convert Python result to Erlang term */
                ERL_NIF_TERM result_term = py_to_term(send_env, coro);

                /* Copy original ref from term_env to send_env */
                ERL_NIF_TERM ref_copy = enif_make_copy(send_env, tuple_elems[1]);

                /* Build message: {async_result, Ref, {ok, Result}} */
                ERL_NIF_TERM ok_tuple = enif_make_tuple2(send_env,
                    enif_make_atom(send_env, "ok"),
                    result_term);
                ERL_NIF_TERM msg = enif_make_tuple3(send_env,
                    enif_make_atom(send_env, "async_result"),
                    ref_copy,
                    ok_tuple);

                enif_send(NULL, &caller_pid, send_env, msg);
                enif_free_env(send_env);
            }
            Py_DECREF(coro);
        }

        Py_DECREF(py_ref);
        Py_DECREF((PyObject *)pid_obj);
        enif_free_env(term_env);
    }

    /* NOTE: We don't DECREF asyncio and run_and_send here because they're cached
     * in the loop structure. They'll be freed when the loop is destroyed. */

    /* Check if Python loop is already running (e.g., from erlang.run() in py:exec).
     * If so, skip calling _run_once - the running loop will handle events itself
     * when poll_events_wait returns. Calling _run_once on a running loop is not
     * safe because _run_once is not reentrant. */
    PyObject *is_running = PyObject_CallMethod(loop->py_loop, "is_running", NULL);
    if (is_running != NULL) {
        int running = PyObject_IsTrue(is_running);
        Py_DECREF(is_running);
        if (running) {
            /* Loop is already running - just signal it and clean up.
             * The pending events were already added by dispatch_timer/handle_fd_event,
             * and the condition variable was signaled. The running loop will wake up
             * and process them. */
            if (events_module != NULL) {
                Py_XDECREF(old_running_loop);
                Py_DECREF(events_module);
            }
            PyGILState_Release(gstate);
            return ATOM_OK;
        }
    } else {
        PyErr_Clear();
    }

    /* Run the event loop until there's no more immediate work.
     *
     * We need to keep calling _run_once because:
     * 1. First call may schedule timers (coroutine hits await asyncio.sleep)
     * 2. Timer dispatch adds callback to _ready queue
     * 3. Next _run_once processes the _ready queue (resumes coroutine)
     * 4. Coroutine may complete and send result, or schedule more work
     *
     * We loop until both pending_count AND Python's _ready queue are empty.
     * Pass timeout_hint=0 so we don't block. */
    int current_pending = atomic_load(&loop->pending_count);
    int py_ready = 0;
    int iterations = 0;
    const int max_iterations = 100;  /* Safety limit */

    /* Loop while there's work: new coroutines, pending events, OR ready callbacks */
    while ((coros_scheduled > 0 || current_pending > 0 || py_ready > 0) && iterations < max_iterations) {
        iterations++;

        PyObject *run_result = PyObject_CallMethod(loop->py_loop, "_run_once", "i", 0);
        if (run_result != NULL) {
            Py_DECREF(run_result);
        } else {
            PyErr_Print();
            PyErr_Clear();
            break;
        }

        /* Check if Python loop has more ready callbacks */
        PyObject *ready_len = PyObject_CallMethod(loop->py_loop, "_get_ready_count", NULL);
        py_ready = 0;
        if (ready_len != NULL) {
            py_ready = (int)PyLong_AsLong(ready_len);
            Py_DECREF(ready_len);
            if (PyErr_Occurred()) {
                PyErr_Clear();
                py_ready = 0;
            }
        }

        current_pending = atomic_load(&loop->pending_count);
        coros_scheduled = 0;  /* Already processed on first iteration */
    }

    /* Restore original event loop context before releasing GIL */
    if (events_module != NULL) {
        PyObject *restore = PyObject_CallMethod(events_module, "_set_running_loop", "O",
                                                old_running_loop ? old_running_loop : Py_None);
        Py_XDECREF(restore);
        Py_XDECREF(old_running_loop);
        Py_DECREF(events_module);
    }

    PyGILState_Release(gstate);

    /*
     * Check if there are more tasks remaining (we hit MAX_TASK_BATCH limit).
     * Return 'more' so the Erlang side can loop immediately without waiting
     * for a new task_ready message.
     */
    if (atomic_load(&loop->task_count) > 0) {
        return ATOM_MORE;
    }

    return result;
}

/**
 * event_loop_set_py_loop(LoopRef, PyLoopRef) -> ok | {error, Reason}
 *
 * Store a reference to the Python ErlangEventLoop in the C struct.
 * This avoids thread-local lookup issues when processing tasks.
 *
 * PyLoopRef should be the resource reference containing the Python loop.
 * This NIF must be called from Python after creating the ErlangEventLoop.
 */
ERL_NIF_TERM nif_event_loop_set_py_loop(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    /* argv[1] should be a PyCapsule containing the Python loop object */
    /* For now, we'll store it via a different mechanism - from Python side */

    /* This NIF is called from Python, so we're already in the right context.
     * The actual py_loop is set via py_set_loop_ref() Python function */

    return ATOM_OK;
}

/**
 * event_loop_exec(LoopRef, Code) -> ok | {error, Reason}
 *
 * Execute Python code in the calling process's namespace.
 * This allows defining functions that can be called via create_task.
 *
 * The namespace is isolated per Erlang process and automatically
 * cleaned up when the process exits.
 *
 * @param LoopRef Event loop resource reference
 * @param Code Binary containing Python code to execute
 * @return ok on success, {error, Reason} on failure
 */
ERL_NIF_TERM nif_event_loop_exec(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    /* Get code binary */
    ErlNifBinary code_bin;
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        /* Try iolist */
        if (!enif_inspect_iolist_as_binary(env, argv[1], &code_bin)) {
            return make_error(env, "invalid_code");
        }
    }

    /* Convert to C string */
    char *code = enif_alloc(code_bin.size + 1);
    if (code == NULL) {
        return make_error(env, "alloc_failed");
    }
    memcpy(code, code_bin.data, code_bin.size);
    code[code_bin.size] = '\0';

    /* Get caller PID */
    ErlNifPid caller_pid;
    if (enif_self(env, &caller_pid) == NULL) {
        enif_free(code);
        return make_error(env, "no_self");
    }

    /* Acquire GIL */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Get or create namespace for this process */
    process_namespace_t *ns = ensure_process_namespace(env, loop, &caller_pid);
    if (ns == NULL) {
        PyGILState_Release(gstate);
        enif_free(code);
        return make_error(env, "namespace_failed");
    }

    /* Execute code in process namespace */
    PyObject *result = PyRun_String(code, Py_file_input, ns->globals, ns->globals);
    enif_free(code);

    if (result == NULL) {
        /* Get error info */
        PyObject *exc_type, *exc_value, *exc_tb;
        PyErr_Fetch(&exc_type, &exc_value, &exc_tb);

        ERL_NIF_TERM error_term;
        if (exc_value != NULL) {
            PyObject *str = PyObject_Str(exc_value);
            if (str != NULL) {
                const char *err_str = PyUnicode_AsUTF8(str);
                if (err_str != NULL) {
                    error_term = enif_make_string(env, err_str, ERL_NIF_LATIN1);
                } else {
                    error_term = enif_make_atom(env, "exec_failed");
                }
                Py_DECREF(str);
            } else {
                error_term = enif_make_atom(env, "exec_failed");
            }
        } else {
            error_term = enif_make_atom(env, "exec_failed");
        }

        Py_XDECREF(exc_type);
        Py_XDECREF(exc_value);
        Py_XDECREF(exc_tb);
        PyGILState_Release(gstate);

        return enif_make_tuple2(env, enif_make_atom(env, "error"), error_term);
    }

    Py_DECREF(result);
    PyGILState_Release(gstate);

    return ATOM_OK;
}

/**
 * event_loop_eval(LoopRef, Expr) -> {ok, Result} | {error, Reason}
 *
 * Evaluate a Python expression in the calling process's namespace.
 *
 * @param LoopRef Event loop resource reference
 * @param Expr Binary containing Python expression to evaluate
 * @return {ok, Result} on success, {error, Reason} on failure
 */
ERL_NIF_TERM nif_event_loop_eval(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE,
                           (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    /* Get expression binary */
    ErlNifBinary expr_bin;
    if (!enif_inspect_binary(env, argv[1], &expr_bin)) {
        if (!enif_inspect_iolist_as_binary(env, argv[1], &expr_bin)) {
            return make_error(env, "invalid_expr");
        }
    }

    /* Convert to C string */
    char *expr = enif_alloc(expr_bin.size + 1);
    if (expr == NULL) {
        return make_error(env, "alloc_failed");
    }
    memcpy(expr, expr_bin.data, expr_bin.size);
    expr[expr_bin.size] = '\0';

    /* Get caller PID */
    ErlNifPid caller_pid;
    if (enif_self(env, &caller_pid) == NULL) {
        enif_free(expr);
        return make_error(env, "no_self");
    }

    /* Acquire GIL */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Get or create namespace for this process */
    process_namespace_t *ns = ensure_process_namespace(env, loop, &caller_pid);
    if (ns == NULL) {
        PyGILState_Release(gstate);
        enif_free(expr);
        return make_error(env, "namespace_failed");
    }

    /* Evaluate expression in process namespace */
    PyObject *result = PyRun_String(expr, Py_eval_input, ns->globals, ns->locals);
    enif_free(expr);

    if (result == NULL) {
        PyObject *exc_type, *exc_value, *exc_tb;
        PyErr_Fetch(&exc_type, &exc_value, &exc_tb);

        ERL_NIF_TERM error_term;
        if (exc_value != NULL) {
            PyObject *str = PyObject_Str(exc_value);
            if (str != NULL) {
                const char *err_str = PyUnicode_AsUTF8(str);
                if (err_str != NULL) {
                    error_term = enif_make_string(env, err_str, ERL_NIF_LATIN1);
                } else {
                    error_term = enif_make_atom(env, "eval_failed");
                }
                Py_DECREF(str);
            } else {
                error_term = enif_make_atom(env, "eval_failed");
            }
        } else {
            error_term = enif_make_atom(env, "eval_failed");
        }

        Py_XDECREF(exc_type);
        Py_XDECREF(exc_value);
        Py_XDECREF(exc_tb);
        PyGILState_Release(gstate);

        return enif_make_tuple2(env, enif_make_atom(env, "error"), error_term);
    }

    /* Convert result to Erlang term */
    ERL_NIF_TERM result_term = py_to_term(env, result);
    Py_DECREF(result);
    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, result_term);
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
 *
 * Note: PENDING_HASH_SIZE must be a power of 2 for bitwise AND to work.
 * Using AND instead of modulo is faster (single instruction vs division).
 */
static inline uint32_t pending_hash_index(uint64_t key) {
    /* Simple hash: XOR fold and bitwise AND (faster than modulo) */
    return (uint32_t)((key ^ (key >> 32)) & (PENDING_HASH_SIZE - 1));
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

    /* Linear probing with bitwise AND for wrap-around */
    for (int i = 0; i < PENDING_HASH_SIZE; i++) {
        uint32_t probe = (idx + i) & (PENDING_HASH_SIZE - 1);
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

    /* Linear probing with bitwise AND for wrap-around */
    for (int i = 0; i < PENDING_HASH_SIZE; i++) {
        uint32_t probe = (idx + i) & (PENDING_HASH_SIZE - 1);
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

bool event_loop_add_pending(erlang_event_loop_t *loop, event_type_t type,
                            uint64_t callback_id, int fd) {
    int current_count = atomic_load(&loop->pending_count);

    /* Backpressure: check if we need to grow capacity */
    if ((size_t)current_count >= loop->pending_capacity) {
        /* Try to grow capacity (up to MAX_PENDING_CAPACITY) */
        if (loop->pending_capacity < MAX_PENDING_CAPACITY) {
            size_t new_capacity = loop->pending_capacity * 2;
            if (new_capacity > MAX_PENDING_CAPACITY) {
                new_capacity = MAX_PENDING_CAPACITY;
            }
            loop->pending_capacity = new_capacity;
            /* Note: Linked list doesn't need realloc, just the capacity limit */
        } else {
            /* At hard cap - warn and reject */
            fprintf(stderr, "event_loop_add_pending: queue at maximum capacity (%zu), rejecting event\n",
                    (size_t)MAX_PENDING_CAPACITY);
            return false;
        }
    }

    pthread_mutex_lock(&loop->mutex);

    /* O(1) duplicate check using hash set */
    if (pending_hash_contains(loop, callback_id, type)) {
        /* Already have this event pending - this counts as success since
         * the event will be delivered */
        pthread_mutex_unlock(&loop->mutex);
        return true;
    }

    /* Get event from freelist or allocate new (Phase 7 optimization) */
    pending_event_t *event = get_pending_event(loop);
    if (event == NULL) {
        pthread_mutex_unlock(&loop->mutex);
        return false;  /* Allocation failed */
    }

    event->type = type;
    event->callback_id = callback_id;
    event->fd = fd;
    event->next = NULL;

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

    /*
     * Coalesced wakeup (uvloop-style): Only signal if no wakeup is pending.
     * This reduces condition variable signals under high event rates.
     */
    if (!atomic_exchange(&loop->wake_pending, true)) {
        pthread_cond_signal(&loop->event_cond);
    }

    pthread_mutex_unlock(&loop->mutex);

    /*
     * Also send task_ready to the worker if one exists.
     * This is needed for create_task: Python is not waiting on the condition
     * variable, so we need to notify the Erlang worker to call process_ready_tasks.
     *
     * Uses the same coalescing logic as submit_task to avoid message floods.
     */
    if (loop->has_worker) {
        if (!atomic_exchange(&loop->task_wake_pending, true)) {
            ErlNifEnv *msg_env = enif_alloc_env();
            if (msg_env != NULL) {
                ERL_NIF_TERM msg = enif_make_atom(msg_env, "task_ready");
                enif_send(NULL, &loop->worker_pid, msg_env, msg);
                enif_free_env(msg_env);
            }
        }
    }

    return true;
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

    /* Re-register with Erlang scheduler for read monitoring.
     * Use ATOM_UNDEFINED instead of enif_make_ref to avoid per-event allocation. */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_READ,
                          fd_res, target_pid, ATOM_UNDEFINED);

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

    /* Re-register with Erlang scheduler for write monitoring.
     * Use ATOM_UNDEFINED instead of enif_make_ref to avoid per-event allocation. */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_WRITE,
                          fd_res, target_pid, ATOM_UNDEFINED);

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
    if (loop == NULL || !event_loop_ensure_router(loop)) {
        return make_error(env, "no_loop");
    }

    /* Re-register with Erlang scheduler for read monitoring.
     * Use ATOM_UNDEFINED instead of enif_make_ref to avoid per-event allocation. */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_READ,
                          fd_res, target_pid, ATOM_UNDEFINED);

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
    if (loop == NULL || !event_loop_ensure_router(loop)) {
        return make_error(env, "no_loop");
    }

    /* Re-register with Erlang scheduler for write monitoring.
     * Use ATOM_UNDEFINED instead of enif_make_ref to avoid per-event allocation. */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_WRITE,
                          fd_res, target_pid, ATOM_UNDEFINED);

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
    if (loop == NULL || !event_loop_ensure_router(loop)) {
        return make_error(env, "no_loop");
    }

    /* Register with Erlang scheduler for read monitoring */
    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_READ,
                          fd_res, target_pid, ATOM_UNDEFINED);

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
    if (loop == NULL || !event_loop_ensure_router(loop)) {
        return make_error(env, "no_loop");
    }

    /* Register with Erlang scheduler for write monitoring */
    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_WRITE,
                          fd_res, target_pid, ATOM_UNDEFINED);

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
 * dup_fd(Fd) -> {ok, DupFd} | {error, Reason}
 *
 * Duplicates a file descriptor.
 */
ERL_NIF_TERM nif_dup_fd(ErlNifEnv *env, int argc,
                        const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    int dup_fd = dup(fd);
    if (dup_fd == -1) {
        return make_error(env, "dup_failed");
    }

    return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, dup_fd));
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
 * Context Event Loop Access
 *
 * These NIFs allow Erlang to access the event loop for a subinterpreter context
 * ============================================================================ */

/**
 * context_get_event_loop(ContextRef) -> {ok, LoopRef} | {error, Reason}
 *
 * Get the event loop for a subinterpreter context.
 * This allows Erlang to create a dedicated event worker for the context.
 *
 * Note: With the thread-per-context model, event loop access from Erlang
 * is limited. The event loop is owned by the subinterpreter's dedicated thread.
 */
ERL_NIF_TERM nif_context_get_event_loop(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_context_t *ctx;
    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* Only OWN_GIL subinterpreters have their own event loop */
    if (!ctx->is_subinterp) {
        return make_error(env, "not_subinterp");
    }

    /* With shared-GIL pool model, event loop operations work on dirty schedulers.
     * py_context_acquire handles PyThreadState_Swap to the subinterpreter. */

    /* Acquire thread state (handles both worker mode and subinterpreter mode) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        return make_error(env, "acquire_failed");
    }

    erlang_event_loop_t *loop = get_interpreter_event_loop();

    py_context_release(&guard);

    if (loop == NULL) {
        return make_error(env, "no_event_loop");
    }

    /* Return reference to the event loop */
    ERL_NIF_TERM loop_term = enif_make_resource(env, loop);
    return enif_make_tuple2(env, ATOM_OK, loop_term);
#else
    return make_error(env, "not_subinterp");
#endif
}

/* ============================================================================
 * Reactor NIFs - Erlang-as-Reactor Architecture
 *
 * These NIFs support the Erlang-as-Reactor pattern where:
 * - Erlang manages TCP accept and routing via gen_tcp
 * - FDs are passed to py_reactor_context processes
 * - Python handles HTTP parsing and ASGI/WSGI execution
 * - Erlang handles I/O readiness via enif_select
 * ============================================================================ */

/**
 * reactor_register_fd(ContextRef, Fd, OwnerPid) -> {ok, FdRef} | {error, Reason}
 *
 * Register an FD for reactor monitoring. The FD is owned by the context
 * and receives {select, FdRes, Ref, ready_input/ready_output} messages.
 * Initial registration is for read events.
 */
ERL_NIF_TERM nif_reactor_register_fd(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]) {
    (void)argc;

    /* Get context reference - we need PY_CONTEXT_RESOURCE_TYPE from py_nif.h */
    py_context_t *ctx;
    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    int fd;
    if (!enif_get_int(env, argv[1], &fd)) {
        return make_error(env, "invalid_fd");
    }

    ErlNifPid owner_pid;
    if (!enif_get_local_pid(env, argv[2], &owner_pid)) {
        return make_error(env, "invalid_pid");
    }

    /* Allocate fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE,
                                                 sizeof(fd_resource_t));
    if (fd_res == NULL) {
        return make_error(env, "alloc_failed");
    }

    fd_res->fd = fd;
    fd_res->read_callback_id = 0;  /* Not used for reactor mode */
    fd_res->write_callback_id = 0;
    fd_res->owner_pid = owner_pid;
    fd_res->reader_active = true;
    fd_res->writer_active = false;
    fd_res->loop = NULL;  /* No event loop needed for reactor mode */

    /* Initialize lifecycle management */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;  /* Caller owns the fd */

    /* Monitor owner process for cleanup on death */
    if (enif_monitor_process(env, fd_res, &owner_pid,
                             &fd_res->owner_monitor) == 0) {
        fd_res->monitor_active = true;
    }

    /* Register with Erlang scheduler for read monitoring */
    int ret = enif_select(env, (ErlNifEvent)fd, ERL_NIF_SELECT_READ,
                          fd_res, &owner_pid, enif_make_ref(env));

    if (ret < 0) {
        if (fd_res->monitor_active) {
            enif_demonitor_process(env, fd_res, &fd_res->owner_monitor);
        }
        enif_release_resource(fd_res);
        return make_error(env, "select_failed");
    }

    ERL_NIF_TERM fd_term = enif_make_resource(env, fd_res);
    /* Don't release - keep reference while registered */

    return enif_make_tuple2(env, ATOM_OK, fd_term);
}

/**
 * reactor_reselect_read(FdRef) -> ok | {error, Reason}
 *
 * Re-register for read events after a one-shot event was delivered.
 */
ERL_NIF_TERM nif_reactor_reselect_read(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return make_error(env, "fd_closing");
    }

    if (fd_res->fd < 0) {
        return make_error(env, "fd_closed");
    }

    /* Re-register for read events */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_READ,
                          fd_res, &fd_res->owner_pid, enif_make_ref(env));

    if (ret < 0) {
        return make_error(env, "select_failed");
    }

    fd_res->reader_active = true;

    return ATOM_OK;
}

/**
 * reactor_select_write(FdRef) -> ok | {error, Reason}
 *
 * Switch to write monitoring for response sending.
 */
ERL_NIF_TERM nif_reactor_select_write(ErlNifEnv *env, int argc,
                                       const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    if (atomic_load(&fd_res->closing_state) != FD_STATE_OPEN) {
        return make_error(env, "fd_closing");
    }

    if (fd_res->fd < 0) {
        return make_error(env, "fd_closed");
    }

    /* Register for write events */
    int ret = enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_WRITE,
                          fd_res, &fd_res->owner_pid, enif_make_ref(env));

    if (ret < 0) {
        return make_error(env, "select_failed");
    }

    fd_res->writer_active = true;
    fd_res->reader_active = false;  /* Typically stop reading when writing */

    return ATOM_OK;
}

/**
 * get_fd_from_resource(FdRef) -> Fd | {error, Reason}
 *
 * Extract the file descriptor integer from an FD resource.
 */
ERL_NIF_TERM nif_get_fd_from_resource(ErlNifEnv *env, int argc,
                                       const ERL_NIF_TERM argv[]) {
    (void)argc;

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[0], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    if (fd_res->fd < 0) {
        return make_error(env, "fd_closed");
    }

    return enif_make_int(env, fd_res->fd);
}

/**
 * reactor_on_read_ready(ContextRef, Fd) -> {ok, Action} | {error, Reason}
 *
 * Read data from fd and call Python's erlang_reactor.on_read_ready(fd, data).
 * Uses zero-copy ReactorBuffer for efficient data transfer.
 * Action is one of: <<"continue">>, <<"write_pending">>, <<"close">>
 *
 * This is a dirty NIF since it acquires the GIL and calls Python.
 */
ERL_NIF_TERM nif_reactor_on_read_ready(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_context_t *ctx;
    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    int fd;
    if (!enif_get_int(env, argv[1], &fd)) {
        return make_error(env, "invalid_fd");
    }

    /* Read data from fd into buffer resource (before acquiring GIL) */
    reactor_buffer_resource_t *buffer = NULL;
    size_t bytes_read = 0;
    int read_result = reactor_buffer_read_fd(fd, REACTOR_MAX_READ_SIZE,
                                              &buffer, &bytes_read);

    if (read_result < 0) {
        return make_error(env, "read_failed");
    }

    if (read_result == 1 || (read_result == 0 && buffer == NULL)) {
        /* EOF or would block with no data */
        return enif_make_tuple2(env, ATOM_OK,
            enif_make_atom(env, read_result == 1 ? "close" : "continue"));
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_reactor_read_to_owngil(env, ctx, fd, buffer);
    }
#endif

    /* Acquire context (handles both worker mode and subinterpreter mode) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_release_resource(buffer);
        return make_error(env, "acquire_failed");
    }

    /* Create ReactorBuffer Python object wrapping the resource */
    PyObject *py_buffer = ReactorBuffer_from_resource(buffer, buffer);
    /* Release our reference - Python now owns the only reference */
    enif_release_resource(buffer);

    if (py_buffer == NULL) {
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "buffer_creation_failed");
    }

    /* Get module state for THIS interpreter's reactor cache */
    py_event_loop_module_state_t *state = get_module_state();
    if (!ensure_reactor_cached_for_interp(state)) {
        PyErr_Clear();
        Py_DECREF(py_buffer);
        py_context_release(&guard);
        return make_error(env, "reactor_cache_init_failed");
    }

    /* Call cached on_read_ready(fd, data) - uses THIS interpreter's reactor */
    PyObject *py_fd = PyLong_FromLong(fd);
    if (py_fd == NULL) {
        PyErr_Clear();
        Py_DECREF(py_buffer);
        py_context_release(&guard);
        return make_error(env, "fd_conversion_failed");
    }

    PyObject *result = PyObject_CallFunctionObjArgs(state->reactor_on_read, py_fd, py_buffer, NULL);
    Py_DECREF(py_fd);
    Py_DECREF(py_buffer);

    if (result == NULL) {
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "on_read_ready_failed");
    }

    /* Convert result to Erlang term */
    ERL_NIF_TERM action;
    if (PyUnicode_Check(result)) {
        const char *str = PyUnicode_AsUTF8(result);
        if (str != NULL) {
            size_t len = strlen(str);
            unsigned char *buf = enif_make_new_binary(env, len, &action);
            memcpy(buf, str, len);
        } else {
            action = enif_make_atom(env, "unknown");
        }
    } else {
        action = enif_make_atom(env, "unknown");
    }

    Py_DECREF(result);
    py_context_release(&guard);

    return enif_make_tuple2(env, ATOM_OK, action);
}

/**
 * reactor_on_write_ready(ContextRef, Fd) -> {ok, Action} | {error, Reason}
 *
 * Call Python's erlang_reactor.on_write_ready(fd) and return the action.
 * Action is one of: <<"continue">>, <<"read_pending">>, <<"close">>
 *
 * This is a dirty NIF since it acquires the GIL and calls Python.
 */
ERL_NIF_TERM nif_reactor_on_write_ready(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_context_t *ctx;
    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    int fd;
    if (!enif_get_int(env, argv[1], &fd)) {
        return make_error(env, "invalid_fd");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_reactor_write_to_owngil(env, ctx, fd);
    }
#endif

    /* Acquire context (handles both worker mode and subinterpreter mode) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        return make_error(env, "acquire_failed");
    }

    /* Get module state for THIS interpreter's reactor cache */
    py_event_loop_module_state_t *state = get_module_state();
    if (!ensure_reactor_cached_for_interp(state)) {
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "reactor_cache_init_failed");
    }

    /* Call cached on_write_ready(fd) - uses THIS interpreter's reactor */
    PyObject *py_fd = PyLong_FromLong(fd);
    if (py_fd == NULL) {
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "fd_conversion_failed");
    }

    PyObject *result = PyObject_CallFunctionObjArgs(state->reactor_on_write, py_fd, NULL);
    Py_DECREF(py_fd);

    if (result == NULL) {
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "on_write_ready_failed");
    }

    /* Convert result to Erlang term */
    ERL_NIF_TERM action;
    if (PyUnicode_Check(result)) {
        const char *str = PyUnicode_AsUTF8(result);
        if (str != NULL) {
            size_t len = strlen(str);
            unsigned char *buf = enif_make_new_binary(env, len, &action);
            memcpy(buf, str, len);
        } else {
            action = enif_make_atom(env, "unknown");
        }
    } else {
        action = enif_make_atom(env, "unknown");
    }

    Py_DECREF(result);
    py_context_release(&guard);

    return enif_make_tuple2(env, ATOM_OK, action);
}

/**
 * reactor_init_connection(ContextRef, Fd, ClientInfo) -> ok | {error, Reason}
 *
 * Initialize a Python protocol handler for a new connection.
 * ClientInfo is a map with keys: addr, port
 *
 * This is a dirty NIF since it acquires the GIL and calls Python.
 */
ERL_NIF_TERM nif_reactor_init_connection(ErlNifEnv *env, int argc,
                                          const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_context_t *ctx;
    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    int fd;
    if (!enif_get_int(env, argv[1], &fd)) {
        return make_error(env, "invalid_fd");
    }

    /* Convert client_info map to Python dict */
    if (!enif_is_map(env, argv[2])) {
        return make_error(env, "invalid_client_info");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_reactor_init_to_owngil(env, ctx, fd, argv[2]);
    }
#endif

    /* Acquire context (handles both worker mode and subinterpreter mode) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        return make_error(env, "acquire_failed");
    }

    /* Convert Erlang map to Python dict */
    PyObject *client_info = term_to_py(env, argv[2]);
    if (client_info == NULL) {
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "client_info_conversion_failed");
    }

    /* Import erlang.reactor module */
    PyObject *reactor_module = PyImport_ImportModule("erlang.reactor");
    if (reactor_module == NULL) {
        Py_DECREF(client_info);
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "import_erlang_reactor_failed");
    }

    /* Call init_connection(fd, client_info) */
    PyObject *result = PyObject_CallMethod(reactor_module, "init_connection",
                                            "iO", fd, client_info);
    Py_DECREF(reactor_module);
    Py_DECREF(client_info);

    if (result == NULL) {
        PyErr_Clear();
        py_context_release(&guard);
        return make_error(env, "init_connection_failed");
    }

    Py_DECREF(result);
    py_context_release(&guard);

    return ATOM_OK;
}

/**
 * reactor_close_fd(ContextRef, FdRef) -> ok | {error, Reason}
 *
 * Close an FD and clean up the protocol handler.
 * Calls Python's erlang_reactor.close_connection(fd) if registered.
 */
ERL_NIF_TERM nif_reactor_close_fd(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_context_t *ctx;
    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    fd_resource_t *fd_res;
    if (!enif_get_resource(env, argv[1], FD_RESOURCE_TYPE, (void **)&fd_res)) {
        return make_error(env, "invalid_fd_ref");
    }

    int fd = fd_res->fd;

    /* Atomically transition to CLOSING state */
    int expected = FD_STATE_OPEN;
    if (!atomic_compare_exchange_strong(&fd_res->closing_state,
                                        &expected, FD_STATE_CLOSING)) {
        /* Already closing or closed */
        return ATOM_OK;
    }

    /* Call Python to clean up protocol handler */
    if (fd >= 0) {
        py_context_guard_t guard = py_context_acquire(ctx);
        if (guard.acquired) {
            PyObject *reactor_module = PyImport_ImportModule("erlang.reactor");
            if (reactor_module != NULL) {
                PyObject *result = PyObject_CallMethod(reactor_module,
                                                        "close_connection", "i", fd);
                Py_XDECREF(result);
                Py_DECREF(reactor_module);
                PyErr_Clear();  /* Ignore errors during cleanup */
            } else {
                PyErr_Clear();
            }

            py_context_release(&guard);
        }
    }

    /* Take ownership for cleanup */
    fd_res->owns_fd = true;

    /* Stop select and close */
    if (fd_res->reader_active || fd_res->writer_active) {
        enif_select(env, (ErlNifEvent)fd_res->fd, ERL_NIF_SELECT_STOP,
                    fd_res, NULL, enif_make_atom(env, "reactor_close"));
    } else {
        atomic_store(&fd_res->closing_state, FD_STATE_CLOSED);
        if (fd_res->fd >= 0) {
            close(fd_res->fd);
            fd_res->fd = -1;
        }
        if (fd_res->monitor_active) {
            enif_demonitor_process(env, fd_res, &fd_res->owner_monitor);
            fd_res->monitor_active = false;
        }
    }

    return ATOM_OK;
}

/* ============================================================================
 * Direct FD Operations
 *
 * These functions provide direct FD read/write for proxy/bridge use cases.
 * ============================================================================ */

/**
 * fd_read(Fd, Size) -> {ok, Data} | {error, Reason}
 *
 * Read up to Size bytes from a file descriptor.
 */
ERL_NIF_TERM nif_fd_read(ErlNifEnv *env, int argc,
                          const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    unsigned long size;
    if (!enif_get_ulong(env, argv[1], &size)) {
        return make_error(env, "invalid_size");
    }

    if (size > 1024 * 1024) {
        size = 1024 * 1024;
    }

    ErlNifBinary bin;
    if (!enif_alloc_binary(size, &bin)) {
        return make_error(env, "alloc_failed");
    }

    ssize_t n = read(fd, bin.data, bin.size);
    if (n < 0) {
        enif_release_binary(&bin);
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return make_error(env, "eagain");
        }
        return make_error(env, strerror(errno));
    }

    if ((size_t)n < bin.size) {
        enif_realloc_binary(&bin, n);
    }

    ERL_NIF_TERM data = enif_make_binary(env, &bin);
    return enif_make_tuple2(env, ATOM_OK, data);
}

/**
 * fd_write(Fd, Data) -> {ok, Written} | {error, Reason}
 *
 * Write data to a file descriptor.
 */
ERL_NIF_TERM nif_fd_write(ErlNifEnv *env, int argc,
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

    ssize_t n = write(fd, bin.data, bin.size);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return make_error(env, "eagain");
        }
        return make_error(env, strerror(errno));
    }

    return enif_make_tuple2(env, ATOM_OK, enif_make_long(env, n));
}

/**
 * fd_select_read(Fd) -> {ok, FdRef} | {error, Reason}
 *
 * Register FD for read selection. Caller receives {select, FdRef, Ref, ready_input}.
 * Returns a resource reference that must be kept alive while monitoring.
 */
ERL_NIF_TERM nif_fd_select_read(ErlNifEnv *env, int argc,
                                 const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    ErlNifPid caller_pid;
    if (!enif_self(env, &caller_pid)) {
        return make_error(env, "no_caller_pid");
    }

    /* Allocate fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE,
                                                 sizeof(fd_resource_t));
    if (fd_res == NULL) {
        return make_error(env, "alloc_failed");
    }

    fd_res->fd = fd;
    fd_res->read_callback_id = 0;
    fd_res->write_callback_id = 0;
    fd_res->owner_pid = caller_pid;
    fd_res->reader_active = true;
    fd_res->writer_active = false;
    fd_res->loop = NULL;
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;  /* Caller owns the fd */

    int ret = enif_select(env, (ErlNifEvent)fd, ERL_NIF_SELECT_READ,
                          fd_res, &caller_pid, enif_make_ref(env));
    if (ret < 0) {
        enif_release_resource(fd_res);
        return make_error(env, "select_failed");
    }

    ERL_NIF_TERM fd_term = enif_make_resource(env, fd_res);
    enif_release_resource(fd_res);  /* Term now holds the reference */

    return enif_make_tuple2(env, ATOM_OK, fd_term);
}

/**
 * fd_select_write(Fd) -> {ok, FdRef} | {error, Reason}
 *
 * Register FD for write selection. Caller receives {select, FdRef, Ref, ready_output}.
 * Returns a resource reference that must be kept alive while monitoring.
 */
ERL_NIF_TERM nif_fd_select_write(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    ErlNifPid caller_pid;
    if (!enif_self(env, &caller_pid)) {
        return make_error(env, "no_caller_pid");
    }

    /* Allocate fd resource */
    fd_resource_t *fd_res = enif_alloc_resource(FD_RESOURCE_TYPE,
                                                 sizeof(fd_resource_t));
    if (fd_res == NULL) {
        return make_error(env, "alloc_failed");
    }

    fd_res->fd = fd;
    fd_res->read_callback_id = 0;
    fd_res->write_callback_id = 0;
    fd_res->owner_pid = caller_pid;
    fd_res->reader_active = false;
    fd_res->writer_active = true;
    fd_res->loop = NULL;
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;  /* Caller owns the fd */

    int ret = enif_select(env, (ErlNifEvent)fd, ERL_NIF_SELECT_WRITE,
                          fd_res, &caller_pid, enif_make_ref(env));
    if (ret < 0) {
        enif_release_resource(fd_res);
        return make_error(env, "select_failed");
    }

    ERL_NIF_TERM fd_term = enif_make_resource(env, fd_res);
    enif_release_resource(fd_res);  /* Term now holds the reference */

    return enif_make_tuple2(env, ATOM_OK, fd_term);
}

/**
 * socketpair() -> {ok, {Fd1, Fd2}} | {error, Reason}
 *
 * Create a Unix socketpair for bidirectional communication.
 */
ERL_NIF_TERM nif_socketpair(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
        return make_error(env, strerror(errno));
    }

    int flags1 = fcntl(fds[0], F_GETFL, 0);
    int flags2 = fcntl(fds[1], F_GETFL, 0);
    fcntl(fds[0], F_SETFL, flags1 | O_NONBLOCK);
    fcntl(fds[1], F_SETFL, flags2 | O_NONBLOCK);

    ERL_NIF_TERM fd_tuple = enif_make_tuple2(env,
        enif_make_int(env, fds[0]),
        enif_make_int(env, fds[1]));

    return enif_make_tuple2(env, ATOM_OK, fd_tuple);
}

/**
 * fd_close(Fd) -> ok | {error, Reason}
 *
 * Close a raw file descriptor (integer).
 */
ERL_NIF_TERM nif_fd_close(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[]) {
    (void)argc;

    int fd;
    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    if (close(fd) < 0) {
        return make_error(env, strerror(errno));
    }

    return ATOM_OK;
}

/* ============================================================================
 * OWN_GIL Reactor Dispatch Functions
 * ============================================================================
 * These functions are called from the OWN_GIL thread in py_nif.c.
 * The GIL is already held when these are called.
 */

/**
 * Execute reactor on_read_ready in OWN_GIL thread.
 * Called with GIL already held.
 */
ERL_NIF_TERM owngil_reactor_on_read_ready(ErlNifEnv *env, int fd, void *buffer_ptr) {
    reactor_buffer_resource_t *buffer = (reactor_buffer_resource_t *)buffer_ptr;

    /* Create ReactorBuffer Python object wrapping the resource */
    PyObject *py_buffer = ReactorBuffer_from_resource(buffer, buffer);
    /* Release our reference - Python now owns the only reference */
    enif_release_resource(buffer);

    if (py_buffer == NULL) {
        PyErr_Clear();
        return make_error(env, "buffer_creation_failed");
    }

    /* Get module state for THIS interpreter's reactor cache */
    py_event_loop_module_state_t *state = get_module_state();
    if (!ensure_reactor_cached_for_interp(state)) {
        PyErr_Clear();
        Py_DECREF(py_buffer);
        return make_error(env, "reactor_cache_init_failed");
    }

    /* Call cached on_read_ready(fd, data) */
    PyObject *py_fd = PyLong_FromLong(fd);
    if (py_fd == NULL) {
        PyErr_Clear();
        Py_DECREF(py_buffer);
        return make_error(env, "fd_conversion_failed");
    }

    PyObject *result = PyObject_CallFunctionObjArgs(state->reactor_on_read, py_fd, py_buffer, NULL);
    Py_DECREF(py_fd);
    Py_DECREF(py_buffer);

    if (result == NULL) {
        PyErr_Clear();
        return make_error(env, "on_read_ready_failed");
    }

    /* Convert result to Erlang term */
    ERL_NIF_TERM action;
    if (PyUnicode_Check(result)) {
        const char *str = PyUnicode_AsUTF8(result);
        if (str != NULL) {
            size_t len = strlen(str);
            unsigned char *buf = enif_make_new_binary(env, len, &action);
            memcpy(buf, str, len);
        } else {
            action = enif_make_atom(env, "unknown");
        }
    } else {
        action = enif_make_atom(env, "unknown");
    }

    Py_DECREF(result);
    return enif_make_tuple2(env, ATOM_OK, action);
}

/**
 * Execute reactor on_write_ready in OWN_GIL thread.
 * Called with GIL already held.
 */
ERL_NIF_TERM owngil_reactor_on_write_ready(ErlNifEnv *env, int fd) {
    /* Get module state for THIS interpreter's reactor cache */
    py_event_loop_module_state_t *state = get_module_state();
    if (!ensure_reactor_cached_for_interp(state)) {
        PyErr_Clear();
        return make_error(env, "reactor_cache_init_failed");
    }

    /* Call cached on_write_ready(fd) */
    PyObject *py_fd = PyLong_FromLong(fd);
    if (py_fd == NULL) {
        PyErr_Clear();
        return make_error(env, "fd_conversion_failed");
    }

    PyObject *result = PyObject_CallFunctionObjArgs(state->reactor_on_write, py_fd, NULL);
    Py_DECREF(py_fd);

    if (result == NULL) {
        PyErr_Clear();
        return make_error(env, "on_write_ready_failed");
    }

    /* Convert result to Erlang term */
    ERL_NIF_TERM action;
    if (PyUnicode_Check(result)) {
        const char *str = PyUnicode_AsUTF8(result);
        if (str != NULL) {
            size_t len = strlen(str);
            unsigned char *buf = enif_make_new_binary(env, len, &action);
            memcpy(buf, str, len);
        } else {
            action = enif_make_atom(env, "unknown");
        }
    } else {
        action = enif_make_atom(env, "unknown");
    }

    Py_DECREF(result);
    return enif_make_tuple2(env, ATOM_OK, action);
}

/**
 * Execute reactor init_connection in OWN_GIL thread.
 * Called with GIL already held.
 */
ERL_NIF_TERM owngil_reactor_init_connection(ErlNifEnv *env, int fd,
                                             ERL_NIF_TERM client_info_term) {
    /* Convert client_info to Python dict */
    PyObject *py_client_info = term_to_py(env, client_info_term);
    if (py_client_info == NULL) {
        PyErr_Clear();
        return make_error(env, "client_info_conversion_failed");
    }

    /* Import erlang.reactor module */
    PyObject *reactor_module = PyImport_ImportModule("erlang.reactor");
    if (reactor_module == NULL) {
        Py_DECREF(py_client_info);
        PyErr_Clear();
        return make_error(env, "import_erlang_reactor_failed");
    }

    /* Call init_connection(fd, client_info) */
    PyObject *result = PyObject_CallMethod(reactor_module, "init_connection",
                                            "iO", fd, py_client_info);
    Py_DECREF(reactor_module);
    Py_DECREF(py_client_info);

    if (result == NULL) {
        PyErr_Clear();
        return make_error(env, "init_connection_failed");
    }

    Py_DECREF(result);
    return ATOM_OK;
}

/* ============================================================================
 * Python Module: py_event_loop
 *
 * This provides Python-callable functions for the event loop, allowing
 * Python's asyncio to use the Erlang-native event loop.
 * ============================================================================ */

/**
 * Initialize the Python event loop.
 * Note: This function is deprecated - use nif_set_python_event_loop instead.
 */
int py_event_loop_init_python(ErlNifEnv *env, erlang_event_loop_t *loop) {
    (void)env;
    /* This is called from C code which should have GIL */
    return set_interpreter_event_loop(loop);
}

/**
 * NIF to set the Python event loop.
 * Called from Erlang: py_nif:set_python_event_loop(LoopRef)
 *
 * Stores the event loop in module state for per-interpreter isolation.
 */
ERL_NIF_TERM nif_set_python_event_loop(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]) {
    (void)argc;

    erlang_event_loop_t *loop;
    if (!enif_get_resource(env, argv[0], EVENT_LOOP_RESOURCE_TYPE, (void **)&loop)) {
        return make_error(env, "invalid_event_loop");
    }

    /* Store in module state with GIL held */
    PyGILState_STATE gstate = PyGILState_Ensure();
    int result = set_interpreter_event_loop(loop);
    PyGILState_Release(gstate);

    if (result < 0) {
        return make_error(env, "failed_to_set_event_loop");
    }

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
            int mode = 0;
            if (strcmp(atom_buf, "per_loop") == 0) {
                mode = 1;
            }

            /* Store in module state */
            PyGILState_STATE gstate = PyGILState_Ensure();
            py_event_loop_module_state_t *state = get_module_state();
            if (state != NULL) {
                state->isolation_mode = mode;
            }
            PyGILState_Release(gstate);

            return ATOM_OK;
        }
    }
    return make_error(env, "invalid_mode");
}

/**
 * Set the shared router PID for per-loop created loops.
 * This router will be used by all loops created via _loop_new().
 * Stores in both module state (for the current interpreter) and
 * global variable (for subinterpreters).
 */
ERL_NIF_TERM nif_set_shared_router(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;

    ErlNifPid router_pid;
    if (!enif_get_local_pid(env, argv[0], &router_pid)) {
        return make_error(env, "invalid_pid");
    }

    /* Store in global variable (accessible from all interpreters) */
    pthread_mutex_lock(&g_global_router_mutex);
    g_global_shared_router = router_pid;
    g_global_shared_router_valid = true;
    pthread_mutex_unlock(&g_global_router_mutex);

    /* Also store in module state for backward compatibility */
    PyGILState_STATE gstate = PyGILState_Ensure();
    py_event_loop_module_state_t *state = get_module_state();
    if (state != NULL) {
        state->shared_router = router_pid;
        state->shared_router_valid = true;
    }
    PyGILState_Release(gstate);

    return ATOM_OK;
}

/**
 * Set the shared worker PID for task_ready notifications.
 * This worker receives task_ready messages from dispatch_timer and other
 * event sources to trigger process_ready_tasks.
 */
ERL_NIF_TERM nif_set_shared_worker(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;

    ErlNifPid worker_pid;
    if (!enif_get_local_pid(env, argv[0], &worker_pid)) {
        return make_error(env, "invalid_pid");
    }

    pthread_mutex_lock(&g_global_worker_mutex);
    g_global_shared_worker = worker_pid;
    g_global_shared_worker_valid = true;
    pthread_mutex_unlock(&g_global_worker_mutex);

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

    /*
     * Phase 1: Snapshot-detach under lock (O(1) pointer swap)
     * This minimizes lock contention by doing minimal work under the mutex.
     */
    pthread_mutex_lock(&loop->mutex);

    pending_event_t *snapshot_head = loop->pending_head;
    int count = atomic_load(&loop->pending_count);

    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);

    /*
     * Phase 2: Build PyList outside lock (no contention)
     * All Python allocations and list building happen without the mutex.
     */
    if (count == 0 || snapshot_head == NULL) {
        return PyList_New(0);
    }

    PyObject *list = PyList_New(count);
    bool build_failed = (list == NULL);

    if (!build_failed) {
        pending_event_t *current = snapshot_head;
        int i = 0;
        while (current != NULL && i < count) {
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
                list = NULL;
                build_failed = true;
                break;
            }
            PyList_SET_ITEM(list, i++, tuple);
            current = current->next;
        }
    }

    /*
     * Phase 3: Return ALL events to freelist (always, even on failure)
     * This prevents memory leaks and keeps freelist populated.
     */
    pthread_mutex_lock(&loop->mutex);
    pending_event_t *current = snapshot_head;
    while (current != NULL) {
        pending_event_t *next = current->next;
        return_pending_event(loop, current);
        current = next;
    }
    pthread_mutex_unlock(&loop->mutex);

    return build_failed ? NULL : list;
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

    py_event_loop_module_state_t *state = get_module_state();
    if (state != NULL && state->isolation_mode == 1) {
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
    /* Use worker_pid when available for scalable I/O */
    fd_res->owner_pid = loop->has_worker ? loop->worker_pid : loop->router_pid;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Register with enif_select using the loop's persistent msg_env */
    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_READ, fd_res, target_pid, ATOM_UNDEFINED);

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
    /* Use worker_pid when available for scalable I/O */
    fd_res->owner_pid = loop->has_worker ? loop->worker_pid : loop->router_pid;

    /* Initialize lifecycle management fields */
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Register with enif_select using the loop's persistent msg_env */
    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_WRITE, fd_res, target_pid, ATOM_UNDEFINED);

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
    if (loop == NULL || !event_loop_ensure_router(loop)) {
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

    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int send_result = enif_send(NULL, target_pid, msg_env, msg);
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
    if (loop == NULL || !event_loop_ensure_router(loop)) {
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

    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    enif_send(NULL, target_pid, msg_env, msg);
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

    /* Build pending list with GIL held */
    pthread_mutex_lock(&loop->mutex);

    /* Pre-allocate using atomic counter - single traversal */
    int count = atomic_load(&loop->pending_count);
    if (count == 0) {
        pthread_mutex_unlock(&loop->mutex);
        return PyList_New(0);
    }

    PyObject *list = PyList_New(count);
    if (list == NULL) {
        pthread_mutex_unlock(&loop->mutex);
        return NULL;
    }

    pending_event_t *current = loop->pending_head;
    int i = 0;
    while (current != NULL && i < count) {
        /* Use optimized direct tuple creation (Phase 9+10 optimization) */
        PyObject *tuple = make_event_tuple(current->callback_id, (int)current->type);
        if (tuple == NULL) {
            Py_DECREF(list);
            /* Return remaining events to freelist (Phase 7 optimization) */
            while (current != NULL) {
                pending_event_t *next = current->next;
                return_pending_event(loop, current);
                current = next;
            }
            loop->pending_head = NULL;
            loop->pending_tail = NULL;
            atomic_store(&loop->pending_count, 0);
            pending_hash_clear(loop);
            pthread_mutex_unlock(&loop->mutex);
            return NULL;
        }
        PyList_SET_ITEM(list, i++, tuple);

        pending_event_t *next = current->next;
        /* Return to freelist for reuse (Phase 7 optimization) */
        return_pending_event(loop, current);
        current = next;
    }

    /* Handle any remaining events (if count was stale) */
    while (current != NULL) {
        pending_event_t *next = current->next;
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

/**
 * Destructor for global loop capsules.
 * Only releases reference - does NOT signal shutdown since the global
 * loop is shared and managed by Erlang, not Python.
 */
static void global_loop_capsule_destructor(PyObject *capsule) {
    erlang_event_loop_t *loop = (erlang_event_loop_t *)PyCapsule_GetPointer(
        capsule, LOOP_CAPSULE_NAME);
    if (loop != NULL) {
        /* Only release the reference, don't shutdown */
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

    /* Initialize pending_capacity (memset zeros it, but we need the initial value) */
    loop->pending_capacity = INITIAL_PENDING_CAPACITY;

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

    /* Initialize async task queue (uvloop-inspired) */
    loop->task_queue = enif_ioq_create(ERL_NIF_IOQ_NORMAL);
    if (loop->task_queue == NULL) {
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_free_env(loop->msg_env);
        enif_release_resource(loop);
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate task queue");
        return NULL;
    }

    if (pthread_mutex_init(&loop->task_queue_mutex, NULL) != 0) {
        enif_ioq_destroy(loop->task_queue);
        pthread_cond_destroy(&loop->event_cond);
        pthread_mutex_destroy(&loop->mutex);
        enif_free_env(loop->msg_env);
        enif_release_resource(loop);
        PyErr_SetString(PyExc_RuntimeError, "Failed to initialize task queue mutex");
        return NULL;
    }

    loop->task_queue_initialized = true;
    atomic_store(&loop->task_count, 0);
    loop->py_loop = NULL;
    loop->py_loop_valid = false;

    /* Initialize Python cache (uvloop-style optimization) */
    loop->cached_asyncio = NULL;
    loop->cached_run_and_send = NULL;
    loop->py_cache_valid = false;

#ifdef HAVE_SUBINTERPRETERS
    /* Detect if this is being called from a subinterpreter */
    PyInterpreterState *current_interp = PyInterpreterState_Get();
    PyInterpreterState *main_interp = PyInterpreterState_Main();
    if (current_interp != main_interp) {
        loop->interp_id = (uint32_t)PyInterpreterState_GetID(current_interp);
    } else {
        loop->interp_id = 0;  /* Main interpreter */
    }
#else
    loop->interp_id = 0;  /* Main interpreter */
#endif

    /* Use shared router if available from module state (for per-loop mode) */
    py_event_loop_module_state_t *state = get_module_state();
    if (state != NULL && state->shared_router_valid) {
        loop->router_pid = state->shared_router;
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

/* Python function: _set_loop_ref(capsule, py_loop) -> None
 *
 * Store a reference to the Python ErlangEventLoop in the C struct.
 * This enables direct access to the loop from process_ready_tasks
 * without thread-local lookup issues.
 */
static PyObject *py_set_loop_ref(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    PyObject *py_loop;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &py_loop)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    /* Release old reference if any */
    if (loop->py_loop_valid && loop->py_loop != NULL) {
        Py_DECREF(loop->py_loop);
    }

    /* Store new reference */
    Py_INCREF(py_loop);
    loop->py_loop = py_loop;
    loop->py_loop_valid = true;

    Py_RETURN_NONE;
}

/* Python function: _set_global_loop_ref(py_loop) -> None
 *
 * Store a reference to the Python ErlangEventLoop in the global interpreter loop.
 * This is used when ErlangEventLoop is created by Python's asyncio policy
 * and needs to be associated with the global loop for process_ready_tasks.
 */
static PyObject *py_set_global_loop_ref(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *py_loop;

    if (!PyArg_ParseTuple(args, "O", &py_loop)) {
        return NULL;
    }

    /* Get the global interpreter event loop */
    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "No global event loop initialized");
        return NULL;
    }

    /* Release old reference if any */
    if (loop->py_loop_valid && loop->py_loop != NULL) {
        Py_DECREF(loop->py_loop);
    }

    /* Store new reference */
    Py_INCREF(py_loop);
    loop->py_loop = py_loop;
    loop->py_loop_valid = true;

    Py_RETURN_NONE;
}

/**
 * Python function: _clear_loop_ref(capsule)
 *
 * Clear the Python loop reference from an event loop capsule.
 * Should be called when the Python loop is closed to allow
 * creating a new loop later.
 */
static PyObject *py_clear_loop_ref(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    /* Clear the Python loop reference */
    if (loop->py_loop != NULL) {
        Py_DECREF(loop->py_loop);
        loop->py_loop = NULL;
    }
    loop->py_loop_valid = false;

    Py_RETURN_NONE;
}

/* Python function: _get_global_loop_capsule() -> capsule
 *
 * Returns a capsule for the global interpreter event loop.
 * This is the loop created by Erlang that has has_worker=true.
 * Python's ErlangEventLoop should use this capsule instead of creating
 * a new one, so that timers and FD events are properly dispatched to
 * the worker which triggers process_ready_tasks.
 */
static PyObject *py_get_global_loop_capsule(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    erlang_event_loop_t *loop = get_interpreter_event_loop();
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Global event loop not initialized");
        return NULL;
    }

    /* Keep the resource alive while capsule exists */
    enif_keep_resource(loop);

    return PyCapsule_New(loop, LOOP_CAPSULE_NAME, global_loop_capsule_destructor);
}

/**
 * Python function: _has_loop_ref(capsule) -> bool
 *
 * Check if a loop capsule has an ACTIVE Python loop reference.
 * Returns True only if there's a valid loop that is currently RUNNING.
 * This prevents multiple concurrent loops while allowing sequential
 * loop replacement (e.g., between test cases).
 *
 * The key insight is that the event confusion bug occurs when multiple
 * loops are running simultaneously. A non-running loop (even if not
 * explicitly closed) can be safely replaced.
 */
static PyObject *py_has_loop_ref(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }

    erlang_event_loop_t *loop = loop_from_capsule(capsule);
    if (loop == NULL) {
        return NULL;
    }

    if (loop->py_loop_valid && loop->py_loop != NULL) {
        /* Check if the existing loop is running - only block if running */
        PyObject *is_running = PyObject_CallMethod(loop->py_loop, "is_running", NULL);
        if (is_running != NULL) {
            int running = PyObject_IsTrue(is_running);
            Py_DECREF(is_running);
            if (running) {
                /* Loop is still running - prevent concurrent loop creation */
                Py_RETURN_TRUE;
            }
        } else {
            /* Error calling is_running - clear error and check is_closed as fallback */
            PyErr_Clear();
        }

        /* Loop exists but is not running - check if closed for cleanup */
        PyObject *is_closed = PyObject_CallMethod(loop->py_loop, "is_closed", NULL);
        if (is_closed != NULL) {
            int closed = PyObject_IsTrue(is_closed);
            Py_DECREF(is_closed);
            if (closed) {
                /* Loop is closed, clean up reference */
                Py_DECREF(loop->py_loop);
                loop->py_loop = NULL;
                loop->py_loop_valid = false;
            }
        } else {
            PyErr_Clear();
        }
        /* Not running, allow replacement */
        Py_RETURN_FALSE;
    }
    Py_RETURN_FALSE;
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
     * Phase 1: Snapshot-detach under lock (O(1) pointer swap)
     * This minimizes lock contention by doing minimal work under the mutex.
     */
    pthread_mutex_lock(&loop->mutex);

    pending_event_t *snapshot_head = loop->pending_head;
    int count = atomic_load(&loop->pending_count);

    loop->pending_head = NULL;
    loop->pending_tail = NULL;
    atomic_store(&loop->pending_count, 0);
    pending_hash_clear(loop);

    pthread_mutex_unlock(&loop->mutex);

    /*
     * Phase 2: Build PyList outside lock (no contention)
     * All Python allocations and list building happen without the mutex.
     */
    if (count == 0 || snapshot_head == NULL) {
        return PyList_New(0);
    }

    PyObject *list = PyList_New(count);
    bool build_failed = (list == NULL);

    if (!build_failed) {
        pending_event_t *current = snapshot_head;
        int i = 0;
        while (current != NULL && i < count) {
            PyObject *tuple = make_event_tuple(current->callback_id, (int)current->type);
            if (tuple == NULL) {
                Py_DECREF(list);
                list = NULL;
                build_failed = true;
                break;
            }
            PyList_SET_ITEM(list, i++, tuple);
            current = current->next;
        }
    }

    /*
     * Phase 3: Return ALL events to freelist (always, even on failure)
     * This prevents memory leaks and keeps freelist populated.
     */
    pthread_mutex_lock(&loop->mutex);
    pending_event_t *current = snapshot_head;
    while (current != NULL) {
        pending_event_t *next = current->next;
        return_pending_event(loop, current);
        current = next;
    }
    pthread_mutex_unlock(&loop->mutex);

    return build_failed ? NULL : list;
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

    if (!event_loop_ensure_router(loop)) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop has no router or worker");
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
    /* Use worker_pid when available for scalable I/O */
    fd_res->owner_pid = loop->has_worker ? loop->worker_pid : loop->router_pid;
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_READ, fd_res, target_pid, ATOM_UNDEFINED);

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

    if (!event_loop_ensure_router(loop)) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop has no router or worker");
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
    /* Use worker_pid when available for scalable I/O */
    fd_res->owner_pid = loop->has_worker ? loop->worker_pid : loop->router_pid;
    atomic_store(&fd_res->closing_state, FD_STATE_OPEN);
    fd_res->monitor_active = false;
    fd_res->owns_fd = false;

    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    int ret = enif_select(loop->msg_env, (ErlNifEvent)fd,
                          ERL_NIF_SELECT_WRITE, fd_res, target_pid, ATOM_UNDEFINED);

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

/**
 * Update read callback on existing fd_resource and re-register with enif_select.
 * Python function: _update_fd_read(fd_key, callback_id) -> None
 */
static PyObject *py_update_fd_read(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long fd_key;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "KK", &fd_key, &callback_id)) {
        return NULL;
    }

    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res == NULL || fd_res->loop == NULL) {
        PyErr_SetString(PyExc_ValueError, "Invalid fd resource");
        return NULL;
    }

    fd_res->read_callback_id = callback_id;
    fd_res->reader_active = true;

    /* Re-register for read events (may already be registered, that's OK) */
    ErlNifPid *target_pid = fd_res->loop->has_worker ?
        &fd_res->loop->worker_pid : &fd_res->loop->router_pid;
    enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                ERL_NIF_SELECT_READ, fd_res, target_pid, ATOM_UNDEFINED);

    Py_RETURN_NONE;
}

/**
 * Update write callback on existing fd_resource and re-register with enif_select.
 * Python function: _update_fd_write(fd_key, callback_id) -> None
 */
static PyObject *py_update_fd_write(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long fd_key;
    unsigned long long callback_id;

    if (!PyArg_ParseTuple(args, "KK", &fd_key, &callback_id)) {
        return NULL;
    }

    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res == NULL || fd_res->loop == NULL) {
        PyErr_SetString(PyExc_ValueError, "Invalid fd resource");
        return NULL;
    }

    fd_res->write_callback_id = callback_id;
    fd_res->writer_active = true;

    /* Re-register for write events */
    ErlNifPid *target_pid = fd_res->loop->has_worker ?
        &fd_res->loop->worker_pid : &fd_res->loop->router_pid;
    enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                ERL_NIF_SELECT_WRITE, fd_res, target_pid, ATOM_UNDEFINED);

    Py_RETURN_NONE;
}

/**
 * Clear read monitoring on fd_resource (cancel READ select).
 * Python function: _clear_fd_read(fd_key) -> None
 */
static PyObject *py_clear_fd_read(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long fd_key;

    if (!PyArg_ParseTuple(args, "K", &fd_key)) {
        return NULL;
    }

    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res == NULL || fd_res->loop == NULL) {
        Py_RETURN_NONE;  /* Already cleaned up */
    }

    if (fd_res->reader_active) {
        enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                    ERL_NIF_SELECT_CANCEL | ERL_NIF_SELECT_READ,
                    fd_res, NULL, ATOM_UNDEFINED);
        fd_res->reader_active = false;
        fd_res->read_callback_id = 0;
    }

    Py_RETURN_NONE;
}

/**
 * Clear write monitoring on fd_resource (cancel WRITE select).
 * Python function: _clear_fd_write(fd_key) -> None
 */
static PyObject *py_clear_fd_write(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long fd_key;

    if (!PyArg_ParseTuple(args, "K", &fd_key)) {
        return NULL;
    }

    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res == NULL || fd_res->loop == NULL) {
        Py_RETURN_NONE;  /* Already cleaned up */
    }

    if (fd_res->writer_active) {
        enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                    ERL_NIF_SELECT_CANCEL | ERL_NIF_SELECT_WRITE,
                    fd_res, NULL, ATOM_UNDEFINED);
        fd_res->writer_active = false;
        fd_res->write_callback_id = 0;
    }

    Py_RETURN_NONE;
}

/**
 * Release fd_resource (stop all monitoring and release).
 * Python function: _release_fd_resource(fd_key) -> None
 */
static PyObject *py_release_fd_resource(PyObject *self, PyObject *args) {
    (void)self;
    unsigned long long fd_key;

    if (!PyArg_ParseTuple(args, "K", &fd_key)) {
        return NULL;
    }

    fd_resource_t *fd_res = (fd_resource_t *)(uintptr_t)fd_key;
    if (fd_res != NULL && fd_res->loop != NULL) {
        enif_select(fd_res->loop->msg_env, (ErlNifEvent)fd_res->fd,
                    ERL_NIF_SELECT_STOP, fd_res, NULL, ATOM_UNDEFINED);
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

    /* For timer scheduling, we need to use the global interpreter loop which
     * has the worker process. The capsule's loop may be a Python-created loop
     * that doesn't have has_worker set, which would cause timer dispatches
     * to go to the router instead of the worker, breaking the event loop flow.
     *
     * The global loop (created by Erlang) has has_worker=true and its worker
     * properly triggers process_ready_tasks after timer dispatch. */
    erlang_event_loop_t *target_loop = get_interpreter_event_loop();
    if (target_loop == NULL) {
        /* Fall back to capsule's loop if global not available */
        target_loop = loop;
    }

    if (!event_loop_ensure_router(target_loop)) {
        PyErr_SetString(PyExc_RuntimeError, "Event loop has no router or worker");
        return NULL;
    }

    if (delay_ms < 0) delay_ms = 0;

    uint64_t timer_ref_id = atomic_fetch_add(&target_loop->next_callback_id, 1);

    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate message env");
        return NULL;
    }

    /* Include the target loop resource in message so dispatch goes to correct loop */
    ERL_NIF_TERM loop_term = enif_make_resource(msg_env, target_loop);

    ERL_NIF_TERM msg = enif_make_tuple5(
        msg_env,
        ATOM_START_TIMER,
        loop_term,
        enif_make_int(msg_env, delay_ms),
        enif_make_uint64(msg_env, callback_id),
        enif_make_uint64(msg_env, timer_ref_id)
    );

    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = target_loop->has_worker ? &target_loop->worker_pid : &target_loop->router_pid;
    int send_result = enif_send(NULL, target_pid, msg_env, msg);
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

    if (!event_loop_ensure_router(loop)) {
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

    /* Use worker_pid when available for scalable I/O */
    ErlNifPid *target_pid = loop->has_worker ? &loop->worker_pid : &loop->router_pid;
    enif_send(NULL, target_pid, msg_env, msg);
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

    /* Check shutdown flag before accessing mutex - loop may be in teardown.
     * This is a safety net for any stray executor callbacks that might
     * arrive after loop destruction has begun. */
    if (loop->shutdown) {
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
    {"_get_global_loop_capsule", py_get_global_loop_capsule, METH_NOARGS, "Get capsule for global event loop"},
    {"_has_loop_ref", py_has_loop_ref, METH_VARARGS, "Check if loop capsule has Python loop reference"},
    {"_clear_loop_ref", py_clear_loop_ref, METH_VARARGS, "Clear Python loop reference from C struct"},
    {"_loop_destroy", py_loop_destroy, METH_VARARGS, "Destroy an event loop"},
    {"_set_loop_ref", py_set_loop_ref, METH_VARARGS, "Store Python loop reference in C struct"},
    {"_set_global_loop_ref", py_set_global_loop_ref, METH_VARARGS, "Store Python loop reference in global loop"},
    {"_run_once_native_for", py_run_once_for, METH_VARARGS, "Combined poll + get_pending for specific loop"},
    {"_get_pending_for", py_get_pending_for, METH_VARARGS, "Get and clear pending events for specific loop"},
    {"_wakeup_for", py_wakeup_for, METH_VARARGS, "Wake up specific event loop"},
    {"_is_initialized_for", py_is_initialized_for, METH_VARARGS, "Check if specific loop is initialized"},
    {"_add_reader_for", py_add_reader_for, METH_VARARGS, "Register fd for read monitoring on specific loop"},
    {"_remove_reader_for", py_remove_reader_for, METH_VARARGS, "Stop monitoring fd for reads on specific loop"},
    {"_add_writer_for", py_add_writer_for, METH_VARARGS, "Register fd for write monitoring on specific loop"},
    {"_remove_writer_for", py_remove_writer_for, METH_VARARGS, "Stop monitoring fd for writes on specific loop"},
    /* Shared fd resource management (for read+write on same fd) */
    {"_update_fd_read", py_update_fd_read, METH_VARARGS, "Update read callback on fd resource"},
    {"_update_fd_write", py_update_fd_write, METH_VARARGS, "Update write callback on fd resource"},
    {"_clear_fd_read", py_clear_fd_read, METH_VARARGS, "Clear read monitoring on fd resource"},
    {"_clear_fd_write", py_clear_fd_write, METH_VARARGS, "Clear write monitoring on fd resource"},
    {"_release_fd_resource", py_release_fd_resource, METH_VARARGS, "Release fd resource"},
    {"_schedule_timer_for", py_schedule_timer_for, METH_VARARGS, "Schedule timer on specific loop"},
    {"_cancel_timer_for", py_cancel_timer_for, METH_VARARGS, "Cancel timer on specific loop"},
    {NULL, NULL, 0, NULL}
};

/**
 * Module free callback - cleans up per-interpreter state.
 * Called when the module is being deallocated.
 */
static void py_event_loop_module_free(void *module) {
    py_event_loop_module_state_t *state = PyModule_GetState((PyObject *)module);
    if (state != NULL) {
        cleanup_reactor_cache(state);
    }
}

/* Module definition with module state for per-interpreter isolation */
static struct PyModuleDef PyEventLoopModuleDef = {
    PyModuleDef_HEAD_INIT,
    .m_name = "py_event_loop",
    .m_doc = "Erlang-native asyncio event loop",
    .m_size = sizeof(py_event_loop_module_state_t),
    .m_methods = PyEventLoopMethods,
    .m_free = py_event_loop_module_free,
};

/**
 * Create and register the py_event_loop module in Python.
 * Initializes module state for per-interpreter isolation.
 * Called during Python initialization.
 */
int create_py_event_loop_module(void) {
    /* Check if already registered in this interpreter (idempotent) */
    PyObject *sys_modules = PyImport_GetModuleDict();
    PyObject *existing = PyDict_GetItemString(sys_modules, "py_event_loop");
    if (existing != NULL) {
        return 0;  /* Already registered */
    }

    PyObject *module = PyModule_Create(&PyEventLoopModuleDef);
    if (module == NULL) {
        return -1;
    }

    /* Initialize module state */
    py_event_loop_module_state_t *state = PyModule_GetState(module);
    if (state != NULL) {
        state->event_loop = NULL;
        state->shared_router_valid = false;
        state->isolation_mode = 0;  /* global mode by default */
        /* Initialize reactor cache (will be populated lazily) */
        state->reactor_module = NULL;
        state->reactor_on_read = NULL;
        state->reactor_on_write = NULL;
        state->reactor_initialized = false;
    }

    /* Add module to sys.modules (reuse sys_modules from idempotency check) */
    if (PyDict_SetItemString(sys_modules, "py_event_loop", module) < 0) {
        Py_DECREF(module);
        return -1;
    }

    return 0;
}

/**
 * Create a default event loop and store in module state.
 * This ensures the event loop is always available for Python asyncio.
 * Called after NIF is fully loaded (with GIL held).
 */
int create_default_event_loop(ErlNifEnv *env) {
    /* Check module state first */
    erlang_event_loop_t *existing = get_interpreter_event_loop();
    if (existing != NULL) {
        return 0;  /* Already have an event loop for this interpreter */
    }

    /* Allocate event loop resource */
    erlang_event_loop_t *loop = enif_alloc_resource(
        EVENT_LOOP_RESOURCE_TYPE, sizeof(erlang_event_loop_t));

    if (loop == NULL) {
        return -1;
    }

    /* Initialize fields */
    memset(loop, 0, sizeof(erlang_event_loop_t));

    /* Initialize pending_capacity (memset zeros it, but we need the initial value) */
    loop->pending_capacity = INITIAL_PENDING_CAPACITY;

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

#ifdef HAVE_SUBINTERPRETERS
    /* Check if this is a subinterpreter by comparing to main interpreter */
    PyInterpreterState *current_interp = PyInterpreterState_Get();
    PyInterpreterState *main_interp = PyInterpreterState_Main();
    if (current_interp != main_interp) {
        /* Use interpreter's unique ID for subinterpreters (always > 0) */
        loop->interp_id = (uint32_t)PyInterpreterState_GetID(current_interp);
    } else {
        loop->interp_id = 0;  /* Main interpreter */
    }
#else
    loop->interp_id = 0;  /* Main interpreter */
#endif

    /* Try to use the global shared router if available (for subinterpreters) */
    pthread_mutex_lock(&g_global_router_mutex);
    if (g_global_shared_router_valid) {
        loop->router_pid = g_global_shared_router;
        loop->has_router = true;
    }
    pthread_mutex_unlock(&g_global_router_mutex);

    /* Store in module state for Python code to access */
    set_interpreter_event_loop(loop);

    /* Keep a reference to prevent garbage collection */
    /* Note: This loop will be replaced when py_event_loop:init runs */

    return 0;
}

/**
 * Initialize event loop for a subinterpreter.
 *
 * Creates the py_event_loop module and a default event loop for the
 * current subinterpreter. This must be called after creating a new
 * subinterpreter to enable asyncio.sleep() and timer functionality.
 *
 * @param env NIF environment (can be NULL for worker pool threads)
 * @return 0 on success, -1 on failure
 */
int init_subinterpreter_event_loop(ErlNifEnv *env) {
    if (create_py_event_loop_module() < 0) {
        return -1;
    }
    if (create_default_event_loop(env) < 0) {
        return -1;
    }
    return 0;
}
