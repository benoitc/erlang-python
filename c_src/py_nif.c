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
 * py_nif.c - Python integration NIF for Erlang
 *
 * This NIF embeds Python and allows Erlang processes to execute Python code
 * using dirty I/O schedulers. The design follows patterns from Granian:
 *
 * - GIL is released while waiting for Erlang messages
 * - Workers run on dirty I/O schedulers
 * - Type conversion between Erlang terms and Python objects
 *
 * Key patterns:
 * - Py_BEGIN_ALLOW_THREADS / Py_END_ALLOW_THREADS around blocking ops
 * - Resource types for Python objects to ensure proper cleanup
 * - Dirty NIF flags for GIL-holding operations
 *
 * This file is the main entry point. It includes the following modules:
 * - py_nif.h: Shared header with types and declarations
 * - py_convert.c: Type conversion (Python <-> Erlang)
 * - py_exec.c: Python execution and GIL management
 * - py_callback.c: Callback system and asyncio support
 */

#include "py_nif.h"
#include "py_asgi.h"
#include "py_wsgi.h"

/* ============================================================================
 * Global state definitions
 * ============================================================================ */

ErlNifResourceType *WORKER_RESOURCE_TYPE = NULL;
ErlNifResourceType *PYOBJ_RESOURCE_TYPE = NULL;
ErlNifResourceType *ASYNC_WORKER_RESOURCE_TYPE = NULL;
ErlNifResourceType *SUSPENDED_STATE_RESOURCE_TYPE = NULL;
#ifdef HAVE_SUBINTERPRETERS
ErlNifResourceType *SUBINTERP_WORKER_RESOURCE_TYPE = NULL;
#endif

/* Process-per-context resource type (no mutex) */
ErlNifResourceType *PY_CONTEXT_RESOURCE_TYPE = NULL;

/* py_ref resource type (Python object with interp_id for auto-routing) */
ErlNifResourceType *PY_REF_RESOURCE_TYPE = NULL;

/* suspended_context_state_t resource type (context suspension for callbacks) */
ErlNifResourceType *PY_CONTEXT_SUSPENDED_RESOURCE_TYPE = NULL;

_Atomic uint32_t g_context_id_counter = 1;

bool g_python_initialized = false;
PyThreadState *g_main_thread_state = NULL;

/* Execution mode */
py_execution_mode_t g_execution_mode = PY_MODE_MULTI_EXECUTOR;
int g_num_executors = 4;

/* Multi-executor pool */
executor_t g_executors[MAX_EXECUTORS];
_Atomic int g_next_executor = 0;
bool g_multi_executor_initialized = false;

/* Single executor state */
pthread_t g_executor_thread;
pthread_mutex_t g_executor_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_executor_cond = PTHREAD_COND_INITIALIZER;
py_request_t *g_executor_queue_head = NULL;
py_request_t *g_executor_queue_tail = NULL;
volatile bool g_executor_running = false;
volatile bool g_executor_shutdown = false;

/* Global counter for callback IDs */
_Atomic uint64_t g_callback_id_counter = 1;

/* Custom exception for suspension */
PyObject *SuspensionRequiredException = NULL;

/* Custom exception for dead/unreachable processes */
PyObject *ProcessErrorException = NULL;

/* Cached numpy.ndarray type for fast isinstance checks (NULL if numpy not available) */
PyObject *g_numpy_ndarray_type = NULL;

/* Thread-local callback context */
__thread py_worker_t *tl_current_worker = NULL;
__thread py_context_t *tl_current_context = NULL;
__thread ErlNifEnv *tl_callback_env = NULL;
__thread suspended_state_t *tl_current_suspended = NULL;
__thread suspended_context_state_t *tl_current_context_suspended = NULL;
__thread bool tl_allow_suspension = false;

/* Thread-local pending callback state (flag-based detection, not exception-based) */
__thread bool tl_pending_callback = false;
__thread uint64_t tl_pending_callback_id = 0;
__thread char *tl_pending_func_name = NULL;
__thread size_t tl_pending_func_name_len = 0;
__thread PyObject *tl_pending_args = NULL;

/* Thread-local timeout state */
__thread uint64_t tl_timeout_deadline = 0;
__thread bool tl_timeout_enabled = false;

/* Atoms */
ERL_NIF_TERM ATOM_OK;
ERL_NIF_TERM ATOM_ERROR;
ERL_NIF_TERM ATOM_TRUE;
ERL_NIF_TERM ATOM_FALSE;
ERL_NIF_TERM ATOM_NONE;
ERL_NIF_TERM ATOM_NIL;
ERL_NIF_TERM ATOM_UNDEFINED;
ERL_NIF_TERM ATOM_NIF_NOT_LOADED;
ERL_NIF_TERM ATOM_GENERATOR;
ERL_NIF_TERM ATOM_STOP_ITERATION;
ERL_NIF_TERM ATOM_TIMEOUT;
ERL_NIF_TERM ATOM_NAN;
ERL_NIF_TERM ATOM_INFINITY;
ERL_NIF_TERM ATOM_NEG_INFINITY;
ERL_NIF_TERM ATOM_ERLANG_CALLBACK;
ERL_NIF_TERM ATOM_ASYNC_RESULT;
ERL_NIF_TERM ATOM_ASYNC_ERROR;
ERL_NIF_TERM ATOM_SUSPENDED;

/* Logging atoms */
ERL_NIF_TERM ATOM_PY_LOG;
ERL_NIF_TERM ATOM_SPAN_START;
ERL_NIF_TERM ATOM_SPAN_END;
ERL_NIF_TERM ATOM_SPAN_EVENT;

/* ============================================================================
 * Forward declarations for cross-module functions
 * ============================================================================ */

/* From py_callback.c - needed by py_exec.c */
static PyObject *build_pending_callback_exc_args(void);
static ERL_NIF_TERM build_suspended_result(ErlNifEnv *env, suspended_state_t *suspended);

/* ============================================================================
 * Include module implementations
 * ============================================================================ */

#include "py_convert.c"
#include "py_exec.c"
#include "py_logging.c"
#include "py_callback.c"
#include "py_thread_worker.c"
#include "py_event_loop.c"
#include "py_asgi.c"
#include "py_wsgi.c"
#include "py_worker_pool.h"
#include "py_worker_pool.c"

/* ============================================================================
 * Resource callbacks
 * ============================================================================ */

static void worker_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_worker_t *worker = (py_worker_t *)obj;

    /* Close callback pipes */
    if (worker->callback_pipe[0] >= 0) {
        close(worker->callback_pipe[0]);
    }
    if (worker->callback_pipe[1] >= 0) {
        close(worker->callback_pipe[1]);
    }

    /* Only clean up Python state if Python is still initialized */
    if (worker->thread_state != NULL && g_python_initialized) {
        PyEval_RestoreThread(worker->thread_state);
        Py_XDECREF(worker->globals);
        Py_XDECREF(worker->locals);
        PyThreadState_Clear(worker->thread_state);
        PyThreadState_DeleteCurrent();
    }
}

static void pyobj_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_object_t *wrapper = (py_object_t *)obj;

    if (wrapper->obj != NULL && g_python_initialized) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_DECREF(wrapper->obj);
        PyGILState_Release(gstate);
    }
}

static void async_worker_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_async_worker_t *worker = (py_async_worker_t *)obj;

    /* Signal shutdown */
    worker->shutdown = true;

    /* Write to pipe to wake up event loop */
    if (worker->notify_pipe[1] >= 0) {
        char c = 'q';
        (void)write(worker->notify_pipe[1], &c, 1);
    }

    /* Wait for thread to finish */
    if (worker->loop_running) {
        pthread_join(worker->loop_thread, NULL);
    }

    /* Clean up pending requests */
    pthread_mutex_lock(&worker->queue_mutex);
    async_pending_t *p = worker->pending_head;
    while (p != NULL) {
        async_pending_t *next = p->next;
        if (g_python_initialized && p->future != NULL) {
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_DECREF(p->future);
            PyGILState_Release(gstate);
        }
        enif_free(p);
        p = next;
    }
    pthread_mutex_unlock(&worker->queue_mutex);

    pthread_mutex_destroy(&worker->queue_mutex);

    /* Close pipes */
    if (worker->notify_pipe[0] >= 0) close(worker->notify_pipe[0]);
    if (worker->notify_pipe[1] >= 0) close(worker->notify_pipe[1]);

    if (worker->msg_env != NULL) {
        enif_free_env(worker->msg_env);
    }

    /* Clean up event loop */
    if (g_python_initialized && worker->event_loop != NULL) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_DECREF(worker->event_loop);
        PyGILState_Release(gstate);
    }
}

#ifdef HAVE_SUBINTERPRETERS
static void subinterp_worker_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_subinterp_worker_t *worker = (py_subinterp_worker_t *)obj;

    if (worker->tstate != NULL && g_python_initialized) {
        /* Switch to this interpreter's thread state */
        PyThreadState *old_tstate = PyThreadState_Swap(worker->tstate);

        Py_XDECREF(worker->globals);
        Py_XDECREF(worker->locals);

        /* End the interpreter */
        Py_EndInterpreter(worker->tstate);

        /* Restore previous thread state */
        PyThreadState_Swap(old_tstate);
    }

    /* Destroy the mutex */
    pthread_mutex_destroy(&worker->mutex);
}
#endif

/**
 * @brief Destructor for py_context_t (process-per-context)
 *
 * Note: NO MUTEX to destroy - the process ownership model eliminates
 * the need for mutex locking.
 */
static void context_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_context_t *ctx = (py_context_t *)obj;

    /* Close callback pipes if open */
    if (ctx->callback_pipe[0] >= 0) {
        close(ctx->callback_pipe[0]);
        ctx->callback_pipe[0] = -1;
    }
    if (ctx->callback_pipe[1] >= 0) {
        close(ctx->callback_pipe[1]);
        ctx->callback_pipe[1] = -1;
    }

    /* Skip if already destroyed by nif_context_destroy */
    if (ctx->destroyed) {
        return;
    }

    if (!g_python_initialized) {
        return;
    }

    /* If we reach here, the context wasn't properly destroyed via
     * nif_context_destroy. This can happen if:
     * - The py_context process crashed
     * - The resource was leaked
     *
     * For subinterpreters with OWN_GIL, cleanup from a different thread
     * is problematic. We skip cleanup and let Python clean up on exit.
     * For worker mode, we can safely clean up with PyGILState_Ensure.
     */

#ifdef HAVE_SUBINTERPRETERS
    if (ctx->is_subinterp) {
        /* Can't safely destroy OWN_GIL subinterpreter from arbitrary thread.
         * The interpreter and its objects will be cleaned up when Python
         * finalizes. Log a warning if debugging is enabled. */
        #ifdef DEBUG
        fprintf(stderr, "Warning: py_context subinterpreter %u leaked - "
                "not destroyed via py_context:stop/1\n", ctx->interp_id);
        #endif
        return;
    }
#endif

    /* Worker mode - clean up with main GIL */
    PyGILState_STATE gstate = PyGILState_Ensure();
    Py_XDECREF(ctx->module_cache);
    Py_XDECREF(ctx->globals);
    Py_XDECREF(ctx->locals);
#ifndef HAVE_SUBINTERPRETERS
    if (ctx->thread_state != NULL) {
        PyThreadState_Clear(ctx->thread_state);
        PyThreadState_Delete(ctx->thread_state);
    }
#endif
    PyGILState_Release(gstate);
}

/**
 * @brief Destructor for py_ref_t (Python object with interp_id)
 *
 * This destructor properly cleans up the Python object reference.
 * The interp_id is used for routing but doesn't need cleanup.
 */
static void py_ref_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_ref_t *ref = (py_ref_t *)obj;

    if (g_python_initialized && ref->obj != NULL) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_XDECREF(ref->obj);
        PyGILState_Release(gstate);
    }
}

/**
 * @brief Destructor for suspended_context_state_t
 *
 * Cleans up all resources associated with a suspended context state.
 */
static void suspended_context_state_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    suspended_context_state_t *state = (suspended_context_state_t *)obj;

    /* Clean up Python objects if Python is still initialized */
    if (g_python_initialized && state->callback_args != NULL) {
#ifdef HAVE_SUBINTERPRETERS
        /* For subinterpreters, we must switch to the correct interpreter's
         * thread state before releasing Python objects. Using PyGILState_Ensure
         * would acquire the main interpreter's GIL, causing memory corruption
         * when the object belongs to a subinterpreter with its own GIL. */
        if (state->ctx != NULL && state->ctx->is_subinterp &&
            !state->ctx->destroyed && state->ctx->tstate != NULL) {
            /* Switch to the subinterpreter's thread state */
            PyThreadState *old_tstate = PyThreadState_Swap(state->ctx->tstate);
            Py_XDECREF(state->callback_args);
            /* Restore previous thread state */
            PyThreadState_Swap(old_tstate);
        } else
#endif
        {
            /* Main interpreter or fallback: use standard GIL */
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_XDECREF(state->callback_args);
            PyGILState_Release(gstate);
        }
    }

    /* Free allocated memory */
    if (state->callback_func_name != NULL) {
        enif_free(state->callback_func_name);
    }
    if (state->result_data != NULL) {
        enif_free(state->result_data);
    }

    /* Free sequential callback results array */
    if (state->callback_results != NULL) {
        for (size_t i = 0; i < state->num_callback_results; i++) {
            if (state->callback_results[i].data != NULL) {
                enif_free(state->callback_results[i].data);
            }
        }
        enif_free(state->callback_results);
    }

    /* Free original context environment */
    if (state->orig_env != NULL) {
        enif_free_env(state->orig_env);
    }

    /* Release binaries */
    if (state->orig_module.data != NULL) {
        enif_release_binary(&state->orig_module);
    }
    if (state->orig_func.data != NULL) {
        enif_release_binary(&state->orig_func);
    }
    if (state->orig_code.data != NULL) {
        enif_release_binary(&state->orig_code);
    }
}

static void suspended_state_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    suspended_state_t *state = (suspended_state_t *)obj;

    /* Clean up Python objects if Python is still initialized */
    if (g_python_initialized && state->callback_args != NULL) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_XDECREF(state->callback_args);
        PyGILState_Release(gstate);
    }

    /* Free allocated memory */
    if (state->callback_func_name != NULL) {
        enif_free(state->callback_func_name);
    }
    if (state->result_data != NULL) {
        enif_free(state->result_data);
    }

    /* Free original context environment */
    if (state->orig_env != NULL) {
        enif_free_env(state->orig_env);
    }

    /* Destroy synchronization primitives */
    pthread_mutex_destroy(&state->mutex);
    pthread_cond_destroy(&state->cond);
}

/* ============================================================================
 * Initialization
 * ============================================================================ */

static ERL_NIF_TERM nif_py_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (g_python_initialized) {
        return ATOM_OK;
    }

#ifdef NEED_DLOPEN_GLOBAL
    /* On Linux/FreeBSD/etc, we need to load libpython with RTLD_GLOBAL so that Python
     * extension modules can find Python symbols when dynamically loaded.
     * Without this, modules like _socket.so fail with "undefined symbol: PyByteArray_Type" */
    {
        void *handle = NULL;

#ifdef PYTHON_LIBRARY_PATH
        /* Use CMake-discovered library path (most reliable) */
        handle = dlopen(PYTHON_LIBRARY_PATH, RTLD_NOW | RTLD_GLOBAL);
#endif

        /* Fallback: try pattern-based discovery if CMake path didn't work */
        if (!handle) {
            char libpython[256];
#ifdef Py_GIL_DISABLED
            /* Free-threaded Python has 't' suffix in library name (e.g., libpython3.13t.so) */
            const char *patterns[] = {
                "libpython%d.%dt.so.1.0",  /* Linux free-threaded with full version */
                "libpython%d.%dt.so",      /* Linux/FreeBSD free-threaded */
                "libpython%d.%dt.so.1",    /* Some systems free-threaded */
                "libpython%d.%d.so.1.0",   /* Fallback: Linux with full version */
                "libpython%d.%d.so",       /* Fallback: Linux/FreeBSD */
                "libpython%d.%d.so.1",     /* Fallback: Some systems */
                NULL
            };
#else
            /* Standard Python library names */
            const char *patterns[] = {
                "libpython%d.%d.so.1.0",  /* Linux with full version */
                "libpython%d.%d.so",      /* Linux/FreeBSD */
                "libpython%d.%d.so.1",    /* Some systems */
                NULL
            };
#endif

            for (int i = 0; patterns[i] && !handle; i++) {
                snprintf(libpython, sizeof(libpython), patterns[i],
                         PY_MAJOR_VERSION, PY_MINOR_VERSION);
                handle = dlopen(libpython, RTLD_NOW | RTLD_GLOBAL);
            }
        }
        /* It's OK if this fails - the symbols might already be global */
    }
#endif

    /* Initialize Python with thread support */
    PyConfig config;
    PyConfig_InitPythonConfig(&config);

    /* Parse options from argv[0] if provided */
    if (argc > 0 && enif_is_map(env, argv[0])) {
        ERL_NIF_TERM key, value;
        ErlNifMapIterator iter;

        enif_map_iterator_create(env, argv[0], &iter, ERL_NIF_MAP_ITERATOR_FIRST);
        while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
            /* Handle python_home, python_path, etc. */
            enif_map_iterator_next(env, &iter);
        }
        enif_map_iterator_destroy(env, &iter);
    }

    PyStatus status = Py_InitializeFromConfig(&config);
    PyConfig_Clear(&config);

    if (PyStatus_Exception(status)) {
        return make_error(env, "python_init_failed");
    }

    g_python_initialized = true;

    /* Create the 'erlang' module for callbacks */
    if (create_erlang_module() < 0) {
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "erlang_module_creation_failed");
    }

    /* Create the 'py_event_loop' module for asyncio integration */
    if (create_py_event_loop_module() < 0) {
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "event_loop_module_creation_failed");
    }

    /* Initialize ASGI scope key cache for optimized marshalling */
    if (asgi_scope_init() < 0) {
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "asgi_scope_init_failed");
    }

    /* Initialize WSGI scope key cache for optimized marshalling */
    if (wsgi_scope_init() < 0) {
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "wsgi_scope_init_failed");
    }

    /* Create a default event loop so Python asyncio always has one available */
    if (create_default_event_loop(env) < 0) {
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "default_event_loop_creation_failed");
    }

    /* Set ErlangEventLoop as the default asyncio event loop policy.
     * This is done via the erlang_loop module which is loaded from priv/.
     * The priv directory path is passed via init options or environment. */

    /* Cache numpy.ndarray type for fast isinstance checks in py_to_term.
     * This avoids slow PyObject_HasAttrString calls on every object. */
    {
        PyObject *numpy_module = PyImport_ImportModule("numpy");
        if (numpy_module != NULL) {
            g_numpy_ndarray_type = PyObject_GetAttrString(numpy_module, "ndarray");
            Py_DECREF(numpy_module);
            /* Note: We keep a reference to g_numpy_ndarray_type for the lifetime of the process */
        } else {
            /* numpy not available - clear any import error */
            PyErr_Clear();
            g_numpy_ndarray_type = NULL;
        }
    }

    /* Detect execution mode based on Python version and build */
    detect_execution_mode();

    /* Save main thread state and release GIL for other threads */
    g_main_thread_state = PyEval_SaveThread();

    /* Start executors based on execution mode */
    int executor_result = 0;
    switch (g_execution_mode) {
        case PY_MODE_FREE_THREADED:
            /* No executor needed - direct execution */
            break;

        case PY_MODE_SUBINTERP:
            /* Use single executor for coordinator operations */
            executor_result = executor_start();
            break;

        case PY_MODE_MULTI_EXECUTOR:
        default:
            /* Start multiple executors for GIL contention mode */
            {
                int num_exec = 4;  /* Default */
                /* Check for config */
                if (argc > 0 && enif_is_map(env, argv[0])) {
                    ERL_NIF_TERM key = enif_make_atom(env, "num_executors");
                    ERL_NIF_TERM value;
                    if (enif_get_map_value(env, argv[0], key, &value)) {
                        enif_get_int(env, value, &num_exec);
                    }
                }
                executor_result = multi_executor_start(num_exec);
                if (executor_result < 0) {
                    /* Fallback to single executor */
                    executor_result = executor_start();
                }
            }
            break;
    }

    if (executor_result < 0) {
        PyEval_RestoreThread(g_main_thread_state);
        g_main_thread_state = NULL;
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "executor_start_failed");
    }

    /* Initialize thread worker system for ThreadPoolExecutor support */
    if (thread_worker_init() < 0) {
        /* Non-fatal - thread worker support just won't be available */
    }

    return ATOM_OK;
}

static ERL_NIF_TERM nif_finalize(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!g_python_initialized) {
        return ATOM_OK;
    }

    /* Clean up thread worker system */
    thread_worker_cleanup();

    /* Clean up ASGI and WSGI scope key caches */
    PyGILState_STATE gstate = PyGILState_Ensure();
    asgi_scope_cleanup();
    wsgi_scope_cleanup();

    /* Clean up numpy type cache */
    Py_XDECREF(g_numpy_ndarray_type);
    g_numpy_ndarray_type = NULL;

    PyGILState_Release(gstate);

    /* Stop executors based on mode */
    switch (g_execution_mode) {
        case PY_MODE_FREE_THREADED:
            /* No executor to stop */
            break;

        case PY_MODE_SUBINTERP:
            executor_stop();
            break;

        case PY_MODE_MULTI_EXECUTOR:
        default:
            if (g_multi_executor_initialized) {
                multi_executor_stop();
            } else {
                executor_stop();
            }
            break;
    }

    /* Restore main thread state before finalizing */
    if (g_main_thread_state != NULL) {
        PyEval_RestoreThread(g_main_thread_state);
        g_main_thread_state = NULL;
    }

    /* For embedded Python, Py_Finalize() can cause issues with threading module
     * shutdown when executor threads have used PyGILState_Ensure/Release.
     * The process will clean up resources on exit, so we skip finalization.
     *
     * Note: If explicit cleanup is needed in the future, consider using
     * Py_FinalizeEx() or manually clearing atexit handlers before finalize. */
#if 0
    Py_Finalize();
#endif
    g_python_initialized = false;

    return ATOM_OK;
}

/* ============================================================================
 * Worker management
 * ============================================================================ */

static ERL_NIF_TERM nif_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_worker_t *worker = enif_alloc_resource(WORKER_RESOURCE_TYPE, sizeof(py_worker_t));
    if (worker == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Acquire GIL to create thread state */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Create a new thread state for this worker */
    PyInterpreterState *interp = PyInterpreterState_Get();
    worker->thread_state = PyThreadState_New(interp);

    /* Create global/local namespaces */
    worker->globals = PyDict_New();
    worker->locals = PyDict_New();

    /* Import __builtins__ into globals */
    PyObject *builtins = PyEval_GetBuiltins();
    PyDict_SetItemString(worker->globals, "__builtins__", builtins);

    /* Import erlang module into worker's namespace for callbacks */
    PyObject *erlang_module = PyImport_ImportModule("erlang");
    if (erlang_module != NULL) {
        PyDict_SetItemString(worker->globals, "erlang", erlang_module);
        Py_DECREF(erlang_module);
    }

    worker->owns_gil = false;

    /* Initialize callback state */
    worker->callback_pipe[0] = -1;
    worker->callback_pipe[1] = -1;
    worker->has_callback_handler = false;
    worker->callback_env = NULL;

    PyGILState_Release(gstate);

    ERL_NIF_TERM result = enif_make_resource(env, worker);
    enif_release_resource(worker);

    return enif_make_tuple2(env, ATOM_OK, result);
}

static ERL_NIF_TERM nif_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Resource destructor will handle cleanup */
    return ATOM_OK;
}

/* ============================================================================
 * Python execution (dirty NIFs)
 * ============================================================================ */

static ERL_NIF_TERM nif_worker_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Build request and route to executor */
    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_CALL;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.module_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &req.func_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_func");
    }

    req.args_term = argv[3];
    req.kwargs_term = (argc > 4) ? argv[4] : 0;
    req.timeout_ms = 0;

    if (argc > 5) {
        enif_get_ulong(env, argv[5], &req.timeout_ms);
    }

    /* Submit to executor and wait */
    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_worker_eval(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_EVAL;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.code_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_code");
    }

    req.locals_term = (argc > 2) ? argv[2] : 0;
    req.timeout_ms = 0;
    if (argc > 3) {
        enif_get_ulong(env, argv[3], &req.timeout_ms);
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_worker_exec(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_EXEC;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.code_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_code");
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_worker_next(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_worker_t *worker;
    py_object_t *gen_wrapper;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_get_resource(env, argv[1], PYOBJ_RESOURCE_TYPE, (void **)&gen_wrapper)) {
        return make_error(env, "invalid_generator");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_NEXT;
    req.worker = worker;
    req.env = env;
    req.gen_wrapper = gen_wrapper;

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_import_module(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_IMPORT;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.module_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_module");
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_get_attr(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_worker_t *worker;
    py_object_t *obj_wrapper;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_get_resource(env, argv[1], PYOBJ_RESOURCE_TYPE, (void **)&obj_wrapper)) {
        return make_error(env, "invalid_object");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_GETATTR;
    req.worker = worker;
    req.env = env;
    req.obj_wrapper = obj_wrapper;

    if (!enif_inspect_binary(env, argv[2], &req.attr_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_attr");
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/* ============================================================================
 * Info NIFs
 * ============================================================================ */

static ERL_NIF_TERM nif_version(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    const char *version = Py_GetVersion();
    ERL_NIF_TERM version_bin;

    unsigned char *buf = enif_make_new_binary(env, strlen(version), &version_bin);
    memcpy(buf, version, strlen(version));

    return enif_make_tuple2(env, ATOM_OK, version_bin);
}

static ERL_NIF_TERM nif_memory_stats(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_MEMORY_STATS;
    req.env = env;

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_gc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_GC;
    req.env = env;
    req.gc_generation = 2;  /* Full collection by default */
    if (argc > 0) {
        enif_get_int(env, argv[0], &req.gc_generation);
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_tracemalloc_start(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *tracemalloc = PyImport_ImportModule("tracemalloc");
    if (tracemalloc == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "tracemalloc_import_failed");
    }

    int nframe = 1;
    if (argc > 0) {
        enif_get_int(env, argv[0], &nframe);
    }

    PyObject *result = PyObject_CallMethod(tracemalloc, "start", "i", nframe);
    Py_DECREF(tracemalloc);

    ERL_NIF_TERM ret;
    if (result == NULL) {
        ret = make_py_error(env);
    } else {
        Py_DECREF(result);
        ret = ATOM_OK;
    }

    PyGILState_Release(gstate);
    return ret;
}

static ERL_NIF_TERM nif_tracemalloc_stop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *tracemalloc = PyImport_ImportModule("tracemalloc");
    if (tracemalloc == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "tracemalloc_import_failed");
    }

    PyObject *result = PyObject_CallMethod(tracemalloc, "stop", NULL);
    Py_DECREF(tracemalloc);

    ERL_NIF_TERM ret;
    if (result == NULL) {
        ret = make_py_error(env);
    } else {
        Py_DECREF(result);
        ret = ATOM_OK;
    }

    PyGILState_Release(gstate);
    return ret;
}

static ERL_NIF_TERM nif_execution_mode(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    const char *mode_str;
    switch (g_execution_mode) {
        case PY_MODE_FREE_THREADED:
            mode_str = "free_threaded";
            break;
        case PY_MODE_SUBINTERP:
            mode_str = "subinterp";
            break;
        case PY_MODE_MULTI_EXECUTOR:
        default:
            mode_str = "multi_executor";
            break;
    }
    return enif_make_atom(env, mode_str);
}

static ERL_NIF_TERM nif_num_executors(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    return enif_make_int(env, g_num_executors);
}

/* ============================================================================
 * Callback support NIFs
 * ============================================================================ */

static ERL_NIF_TERM nif_set_callback_handler(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    if (!enif_get_local_pid(env, argv[1], &worker->callback_handler)) {
        return make_error(env, "invalid_pid");
    }

    /* Create pipe for callback responses */
    if (pipe(worker->callback_pipe) < 0) {
        return make_error(env, "pipe_failed");
    }

    worker->has_callback_handler = true;

    /* Return the write end of the pipe as a file descriptor for Erlang to use */
    return enif_make_tuple2(env, ATOM_OK,
        enif_make_int(env, worker->callback_pipe[1]));
}

static ERL_NIF_TERM nif_send_callback_response(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
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

/* ============================================================================
 * Async worker NIFs
 * ============================================================================ */

static ERL_NIF_TERM nif_async_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_async_worker_t *worker = enif_alloc_resource(ASYNC_WORKER_RESOURCE_TYPE, sizeof(py_async_worker_t));
    if (worker == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Initialize fields */
    worker->event_loop = NULL;
    worker->loop_running = false;
    worker->shutdown = false;
    worker->pending_head = NULL;
    worker->pending_tail = NULL;
    worker->msg_env = enif_alloc_env();

    /* Create notification pipe */
    if (pipe(worker->notify_pipe) < 0) {
        enif_free_env(worker->msg_env);
        enif_release_resource(worker);
        return make_error(env, "pipe_failed");
    }

    /* Initialize mutex */
    pthread_mutex_init(&worker->queue_mutex, NULL);

    /* Start the event loop thread */
    if (pthread_create(&worker->loop_thread, NULL, async_event_loop_thread, worker) != 0) {
        close(worker->notify_pipe[0]);
        close(worker->notify_pipe[1]);
        pthread_mutex_destroy(&worker->queue_mutex);
        enif_free_env(worker->msg_env);
        enif_release_resource(worker);
        return make_error(env, "thread_create_failed");
    }

    /* Wait for event loop to be ready */
    int max_wait = 100;  /* 1 second max */
    while (!worker->loop_running && max_wait-- > 0) {
        usleep(10000);  /* 10ms */
    }

    if (!worker->loop_running) {
        worker->shutdown = true;
        pthread_join(worker->loop_thread, NULL);
        close(worker->notify_pipe[0]);
        close(worker->notify_pipe[1]);
        pthread_mutex_destroy(&worker->queue_mutex);
        enif_release_resource(worker);
        return make_error(env, "event_loop_start_failed");
    }

    ERL_NIF_TERM result = enif_make_resource(env, worker);
    enif_release_resource(worker);

    return enif_make_tuple2(env, ATOM_OK, result);
}

static ERL_NIF_TERM nif_async_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_async_worker_t *worker;

    if (!enif_get_resource(env, argv[0], ASYNC_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Resource destructor will handle cleanup */
    return ATOM_OK;
}

/* Counter for unique async call IDs */
static uint64_t g_async_id_counter = 0;

static ERL_NIF_TERM nif_async_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_async_worker_t *worker;
    ErlNifBinary module_bin, func_bin;
    ErlNifPid caller;

    if (!enif_get_resource(env, argv[0], ASYNC_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!worker->loop_running) {
        return make_error(env, "event_loop_not_running");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &func_bin)) {
        return make_error(env, "invalid_func");
    }
    if (!enif_get_local_pid(env, argv[5], &caller)) {
        return make_error(env, "invalid_caller");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Convert module/func names */
    char *module_name = binary_to_string(&module_bin);
    char *func_name = binary_to_string(&func_bin);
    if (module_name == NULL || func_name == NULL) {
        enif_free(module_name);
        enif_free(func_name);
        PyGILState_Release(gstate);
        return make_error(env, "alloc_failed");
    }

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
    if (!enif_get_list_length(env, argv[3], &args_len)) {
        Py_DECREF(func);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[3];
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
    if (argc > 4 && enif_is_map(env, argv[4])) {
        kwargs = term_to_py(env, argv[4]);
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
        /* Not a coroutine - return result directly */
        ERL_NIF_TERM term_result = py_to_term(env, coro);
        Py_DECREF(coro);
        result = enif_make_tuple2(env, ATOM_OK,
            enif_make_tuple2(env, enif_make_atom(env, "immediate"), term_result));
        goto cleanup;
    }

    /* Submit coroutine to event loop using run_coroutine_threadsafe */
    PyObject *future = PyObject_CallMethod(asyncio, "run_coroutine_threadsafe",
        "OO", coro, worker->event_loop);
    Py_DECREF(coro);
    Py_DECREF(asyncio);

    if (future == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Create pending entry */
    uint64_t async_id = __sync_fetch_and_add(&g_async_id_counter, 1);

    async_pending_t *pending = enif_alloc(sizeof(async_pending_t));
    if (pending == NULL) {
        Py_DECREF(future);
        result = make_error(env, "alloc_failed");
        goto cleanup;
    }
    pending->id = async_id;
    pending->future = future;
    pending->caller = caller;
    pending->next = NULL;

    /* Add to pending list */
    pthread_mutex_lock(&worker->queue_mutex);
    if (worker->pending_tail == NULL) {
        worker->pending_head = pending;
        worker->pending_tail = pending;
    } else {
        worker->pending_tail->next = pending;
        worker->pending_tail = pending;
    }
    pthread_mutex_unlock(&worker->queue_mutex);

    result = enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, async_id));

cleanup:
    enif_free(module_name);
    enif_free(func_name);
    PyGILState_Release(gstate);

    return result;
}

static ERL_NIF_TERM nif_async_gather(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_async_worker_t *worker;
    ErlNifPid caller;

    if (!enif_get_resource(env, argv[0], ASYNC_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!worker->loop_running) {
        return make_error(env, "event_loop_not_running");
    }
    if (!enif_get_local_pid(env, argv[2], &caller)) {
        return make_error(env, "invalid_caller");
    }

    unsigned int calls_len;
    if (!enif_get_list_length(env, argv[1], &calls_len)) {
        return make_error(env, "invalid_calls_list");
    }

    if (calls_len == 0) {
        return enif_make_tuple2(env, ATOM_OK,
            enif_make_tuple2(env, enif_make_atom(env, "immediate"), enif_make_list(env, 0)));
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Import asyncio */
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (asyncio == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "asyncio_import_failed");
    }

    /* Build list of coroutines */
    PyObject *coros = PyList_New(calls_len);
    ERL_NIF_TERM head, tail = argv[1];

    for (unsigned int i = 0; i < calls_len; i++) {
        enif_get_list_cell(env, tail, &head, &tail);

        int arity;
        const ERL_NIF_TERM *tuple;
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity < 3) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "invalid_call_tuple");
        }

        ErlNifBinary module_bin, func_bin;
        if (!enif_inspect_binary(env, tuple[0], &module_bin) ||
            !enif_inspect_binary(env, tuple[1], &func_bin)) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "invalid_module_or_func");
        }

        char module_name[256], func_name[256];
        if (module_bin.size >= 256 || func_bin.size >= 256) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "name_too_long");
        }
        memcpy(module_name, module_bin.data, module_bin.size);
        module_name[module_bin.size] = '\0';
        memcpy(func_name, func_bin.data, func_bin.size);
        func_name[func_bin.size] = '\0';

        /* Import module and get function */
        PyObject *module = PyImport_ImportModule(module_name);
        if (module == NULL) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            ERL_NIF_TERM err = make_py_error(env);
            PyGILState_Release(gstate);
            return err;
        }

        PyObject *func = PyObject_GetAttrString(module, func_name);
        Py_DECREF(module);
        if (func == NULL) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            ERL_NIF_TERM err = make_py_error(env);
            PyGILState_Release(gstate);
            return err;
        }

        /* Convert args */
        unsigned int args_len;
        if (!enif_get_list_length(env, tuple[2], &args_len)) {
            Py_DECREF(func);
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "invalid_args");
        }

        PyObject *args = PyTuple_New(args_len);
        ERL_NIF_TERM arg_head, arg_tail = tuple[2];
        for (unsigned int j = 0; j < args_len; j++) {
            enif_get_list_cell(env, arg_tail, &arg_head, &arg_tail);
            PyObject *arg = term_to_py(env, arg_head);
            if (arg == NULL) {
                Py_DECREF(args);
                Py_DECREF(func);
                Py_DECREF(coros);
                Py_DECREF(asyncio);
                PyGILState_Release(gstate);
                return make_error(env, "arg_conversion_failed");
            }
            PyTuple_SET_ITEM(args, j, arg);
        }

        /* Call function to get coroutine */
        PyObject *coro = PyObject_Call(func, args, NULL);
        Py_DECREF(func);
        Py_DECREF(args);

        if (coro == NULL) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            ERL_NIF_TERM err = make_py_error(env);
            PyGILState_Release(gstate);
            return err;
        }

        PyList_SET_ITEM(coros, i, coro);
    }

    /* Create asyncio.gather(*coros) */
    PyObject *gather_args = PyTuple_New(calls_len);
    for (unsigned int i = 0; i < calls_len; i++) {
        PyObject *coro = PyList_GetItem(coros, i);
        Py_INCREF(coro);
        PyTuple_SET_ITEM(gather_args, i, coro);
    }

    PyObject *gather_func = PyObject_GetAttrString(asyncio, "gather");
    PyObject *gather_coro = PyObject_Call(gather_func, gather_args, NULL);
    Py_DECREF(gather_func);
    Py_DECREF(gather_args);
    Py_DECREF(coros);

    if (gather_coro == NULL) {
        Py_DECREF(asyncio);
        ERL_NIF_TERM err = make_py_error(env);
        PyGILState_Release(gstate);
        return err;
    }

    /* Submit to event loop */
    PyObject *future = PyObject_CallMethod(asyncio, "run_coroutine_threadsafe",
        "OO", gather_coro, worker->event_loop);
    Py_DECREF(gather_coro);
    Py_DECREF(asyncio);

    if (future == NULL) {
        ERL_NIF_TERM err = make_py_error(env);
        PyGILState_Release(gstate);
        return err;
    }

    /* Create pending entry */
    uint64_t async_id = __sync_fetch_and_add(&g_async_id_counter, 1);

    async_pending_t *pending = enif_alloc(sizeof(async_pending_t));
    if (pending == NULL) {
        Py_DECREF(future);
        PyGILState_Release(gstate);
        return make_error(env, "alloc_failed");
    }
    pending->id = async_id;
    pending->future = future;
    pending->caller = caller;
    pending->next = NULL;

    /* Add to pending list */
    pthread_mutex_lock(&worker->queue_mutex);
    if (worker->pending_tail == NULL) {
        worker->pending_head = pending;
        worker->pending_tail = pending;
    } else {
        worker->pending_tail->next = pending;
        worker->pending_tail = pending;
    }
    pthread_mutex_unlock(&worker->queue_mutex);

    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, async_id));
}

static ERL_NIF_TERM nif_async_stream(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* For now, delegate to async_call - async generators will be handled
     * in the Erlang layer by collecting results */
    return nif_async_call(env, argc, argv);
}

/* ============================================================================
 * Sub-interpreter support (Python 3.12+)
 * ============================================================================ */

static ERL_NIF_TERM nif_subinterp_supported(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

#ifdef HAVE_SUBINTERPRETERS
    return ATOM_TRUE;
#else
    return ATOM_FALSE;
#endif
}

#ifdef HAVE_SUBINTERPRETERS

static ERL_NIF_TERM nif_subinterp_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_subinterp_worker_t *worker = enif_alloc_resource(SUBINTERP_WORKER_RESOURCE_TYPE,
                                                         sizeof(py_subinterp_worker_t));
    if (worker == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Initialize mutex for thread-safe access */
    if (pthread_mutex_init(&worker->mutex, NULL) != 0) {
        enif_release_resource(worker);
        return make_error(env, "mutex_init_failed");
    }

    /* Need the main GIL to create sub-interpreter */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Save current thread state so we can restore it after creating sub-interp */
    PyThreadState *main_tstate = PyThreadState_Get();

    /* Configure sub-interpreter with its own GIL */
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 1,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_OWN_GIL,  /* This is the key - own GIL! */
    };

    PyThreadState *tstate = NULL;
    PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);

    if (PyStatus_Exception(status) || tstate == NULL) {
        /* We're still in main interpreter on error */
        PyGILState_Release(gstate);
        enif_release_resource(worker);
        return make_error(env, "subinterp_create_failed");
    }

    worker->interp = PyThreadState_GetInterpreter(tstate);
    worker->tstate = tstate;

    /* Create global/local namespaces in the new interpreter */
    worker->globals = PyDict_New();
    worker->locals = PyDict_New();

    /* Import __builtins__ */
    PyObject *builtins = PyEval_GetBuiltins();
    PyDict_SetItemString(worker->globals, "__builtins__", builtins);

    /* Switch back to main interpreter */
    PyThreadState_Swap(NULL);
    PyThreadState_Swap(main_tstate);

    PyGILState_Release(gstate);

    ERL_NIF_TERM result = enif_make_resource(env, worker);
    enif_release_resource(worker);

    return enif_make_tuple2(env, ATOM_OK, result);
}

static ERL_NIF_TERM nif_subinterp_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_subinterp_worker_t *worker;

    if (!enif_get_resource(env, argv[0], SUBINTERP_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Resource destructor will handle cleanup */
    return ATOM_OK;
}

static ERL_NIF_TERM nif_subinterp_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_subinterp_worker_t *worker;
    ErlNifBinary module_bin, func_bin;

    if (!enif_get_resource(env, argv[0], SUBINTERP_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &func_bin)) {
        return make_error(env, "invalid_func");
    }

    /* Lock mutex for thread-safe access */
    pthread_mutex_lock(&worker->mutex);

    /* Enter the sub-interpreter */
    PyThreadState *saved_tstate = PyThreadState_Swap(NULL);
    PyThreadState_Swap(worker->tstate);

    char *module_name = binary_to_string(&module_bin);
    char *func_name = binary_to_string(&func_bin);
    if (module_name == NULL || func_name == NULL) {
        enif_free(module_name);
        enif_free(func_name);
        PyThreadState_Swap(NULL);
        PyThreadState_Swap(saved_tstate);
        pthread_mutex_unlock(&worker->mutex);
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

    /* Import module */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Get function */
    PyObject *func = PyObject_GetAttrString(module, func_name);
    Py_DECREF(module);
    if (func == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert args */
    unsigned int args_len;
    if (!enif_get_list_length(env, argv[3], &args_len)) {
        Py_DECREF(func);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[3];
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
    if (argc > 4 && enif_is_map(env, argv[4])) {
        kwargs = term_to_py(env, argv[4]);
    }

    /* Call the function */
    PyObject *py_result = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (py_result == NULL) {
        result = make_py_error(env);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    enif_free(module_name);
    enif_free(func_name);

    /* Exit the sub-interpreter */
    PyThreadState_Swap(NULL);
    if (saved_tstate != NULL) {
        PyThreadState_Swap(saved_tstate);
    }

    /* Unlock mutex */
    pthread_mutex_unlock(&worker->mutex);

    return result;
}

static ERL_NIF_TERM nif_parallel_execute(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    unsigned int workers_len, calls_len;

    if (!enif_get_list_length(env, argv[0], &workers_len)) {
        return make_error(env, "invalid_workers_list");
    }
    if (!enif_get_list_length(env, argv[1], &calls_len)) {
        return make_error(env, "invalid_calls_list");
    }
    if (workers_len == 0 || calls_len == 0) {
        return enif_make_tuple2(env, ATOM_OK, enif_make_list(env, 0));
    }
    if (workers_len < calls_len) {
        return make_error(env, "not_enough_workers");
    }

    ERL_NIF_TERM *results = enif_alloc(sizeof(ERL_NIF_TERM) * calls_len);
    if (results == NULL) {
        return make_error(env, "alloc_failed");
    }
    ERL_NIF_TERM worker_head, worker_tail = argv[0];
    ERL_NIF_TERM call_head, call_tail = argv[1];

    for (unsigned int i = 0; i < calls_len; i++) {
        enif_get_list_cell(env, worker_tail, &worker_head, &worker_tail);
        enif_get_list_cell(env, call_tail, &call_head, &call_tail);

        int arity;
        const ERL_NIF_TERM *tuple;
        if (!enif_get_tuple(env, call_head, &arity, &tuple) || arity < 3) {
            enif_free(results);
            return make_error(env, "invalid_call_tuple");
        }

        /* Build args array for subinterp_call */
        ERL_NIF_TERM call_args[5] = {worker_head, tuple[0], tuple[1], tuple[2],
                                      (arity > 3) ? tuple[3] : enif_make_new_map(env)};

        results[i] = nif_subinterp_call(env, 5, call_args);
    }

    ERL_NIF_TERM result_list = enif_make_list_from_array(env, results, calls_len);
    enif_free(results);

    return enif_make_tuple2(env, ATOM_OK, result_list);
}

/**
 * @brief Run an ASGI application in a subinterpreter
 *
 * Args: WorkerRef, Runner, Module, Callable, ScopeMap, Body
 *
 * This runs ASGI in a subinterpreter with its own GIL for true parallelism.
 */
static ERL_NIF_TERM nif_subinterp_asgi_run(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc < 6) {
        return make_error(env, "badarg");
    }

    py_subinterp_worker_t *worker;
    if (!enif_get_resource(env, argv[0], SUBINTERP_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    ErlNifBinary runner_bin, module_bin, callable_bin, body_bin;
    if (!enif_inspect_binary(env, argv[1], &runner_bin)) {
        return make_error(env, "invalid_runner");
    }
    if (!enif_inspect_binary(env, argv[2], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[3], &callable_bin)) {
        return make_error(env, "invalid_callable");
    }
    if (!enif_inspect_binary(env, argv[5], &body_bin)) {
        return make_error(env, "invalid_body");
    }

    /* Lock mutex for thread-safe access */
    pthread_mutex_lock(&worker->mutex);

    /* Enter the sub-interpreter */
    PyThreadState *saved_tstate = PyThreadState_Swap(NULL);
    PyThreadState_Swap(worker->tstate);

    char *runner_name = binary_to_string(&runner_bin);
    char *module_name = binary_to_string(&module_bin);
    char *callable_name = binary_to_string(&callable_bin);
    if (runner_name == NULL || module_name == NULL || callable_name == NULL) {
        enif_free(runner_name);
        enif_free(module_name);
        enif_free(callable_name);
        PyThreadState_Swap(NULL);
        if (saved_tstate != NULL) {
            PyThreadState_Swap(saved_tstate);
        }
        pthread_mutex_unlock(&worker->mutex);
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

    /* Build scope dict from Erlang map */
    PyObject *scope = asgi_scope_from_map(env, argv[4]);
    if (scope == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert body binary to Python bytes */
    PyObject *body = PyBytes_FromStringAndSize((const char *)body_bin.data, body_bin.size);
    if (body == NULL) {
        Py_DECREF(scope);
        result = make_py_error(env);
        goto cleanup;
    }

    /* Import the ASGI runner module */
    PyObject *runner_module = PyImport_ImportModule(runner_name);
    if (runner_module == NULL) {
        Py_DECREF(body);
        Py_DECREF(scope);
        result = make_py_error(env);
        goto cleanup;
    }

    /* Call _run_asgi_sync(module_name, callable_name, scope, body)
     * or run_asgi(module_name, callable_name, scope, body) depending on runner */
    PyObject *run_result = PyObject_CallMethod(
        runner_module, "run_asgi", "ssOO",
        module_name, callable_name, scope, body);

    /* Fallback to _run_asgi_sync if run_asgi doesn't exist */
    if (run_result == NULL && PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        run_result = PyObject_CallMethod(
            runner_module, "_run_asgi_sync", "ssOO",
            module_name, callable_name, scope, body);
    }

    Py_DECREF(runner_module);
    Py_DECREF(body);
    Py_DECREF(scope);

    if (run_result == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert result to Erlang term using optimized extraction */
    ERL_NIF_TERM term_result = extract_asgi_response(env, run_result);
    Py_DECREF(run_result);

    result = enif_make_tuple2(env, ATOM_OK, term_result);

cleanup:
    enif_free(runner_name);
    enif_free(module_name);
    enif_free(callable_name);

    /* Exit the sub-interpreter */
    PyThreadState_Swap(NULL);
    if (saved_tstate != NULL) {
        PyThreadState_Swap(saved_tstate);
    }

    /* Unlock mutex */
    pthread_mutex_unlock(&worker->mutex);

    return result;
}

#else /* !HAVE_SUBINTERPRETERS */

/* Stub implementations for older Python versions */
static ERL_NIF_TERM nif_subinterp_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "subinterpreters_not_supported");
}

static ERL_NIF_TERM nif_subinterp_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "subinterpreters_not_supported");
}

static ERL_NIF_TERM nif_subinterp_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "subinterpreters_not_supported");
}

static ERL_NIF_TERM nif_parallel_execute(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "subinterpreters_not_supported");
}

static ERL_NIF_TERM nif_subinterp_asgi_run(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "subinterpreters_not_supported");
}

#endif /* HAVE_SUBINTERPRETERS */

/* ============================================================================
 * Process-per-context NIFs (NO MUTEX)
 *
 * These NIFs are designed for the process-per-context architecture.
 * Each Erlang process owns one context and serializes access through
 * message passing, eliminating the need for mutex locking.
 * ============================================================================ */

/**
 * @brief Create a new Python context
 *
 * nif_context_create(Mode) -> {ok, ContextRef, InterpId} | {error, Reason}
 * Mode: subinterp | worker
 */
static ERL_NIF_TERM nif_context_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    /* Parse mode atom */
    char mode_str[32];
    if (!enif_get_atom(env, argv[0], mode_str, sizeof(mode_str), ERL_NIF_LATIN1)) {
        return make_error(env, "invalid_mode");
    }

    bool use_subinterp = (strcmp(mode_str, "subinterp") == 0);

    /* Allocate context resource */
    py_context_t *ctx = enif_alloc_resource(PY_CONTEXT_RESOURCE_TYPE, sizeof(py_context_t));
    if (ctx == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Initialize fields */
    ctx->interp_id = atomic_fetch_add(&g_context_id_counter, 1);
    ctx->is_subinterp = use_subinterp;
    ctx->destroyed = false;
    ctx->has_callback_handler = false;
    ctx->callback_pipe[0] = -1;
    ctx->callback_pipe[1] = -1;
    ctx->globals = NULL;
    ctx->locals = NULL;
    ctx->module_cache = NULL;

    /* Create callback pipe for blocking callback responses */
    if (pipe(ctx->callback_pipe) < 0) {
        enif_release_resource(ctx);
        return make_error(env, "pipe_create_failed");
    }

#ifdef HAVE_SUBINTERPRETERS
    ctx->interp = NULL;
    ctx->tstate = NULL;

    if (use_subinterp) {
        /* Need the main GIL to create sub-interpreter */
        PyGILState_STATE gstate = PyGILState_Ensure();

        /* Save current thread state */
        PyThreadState *main_tstate = PyThreadState_Get();

        /* Configure sub-interpreter with its own GIL */
        PyInterpreterConfig config = {
            .use_main_obmalloc = 0,
            .allow_fork = 0,
            .allow_exec = 0,
            .allow_threads = 1,
            .allow_daemon_threads = 0,
            .check_multi_interp_extensions = 1,
            .gil = PyInterpreterConfig_OWN_GIL,  /* This is the key - own GIL! */
        };

        PyThreadState *tstate = NULL;
        PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);

        if (PyStatus_Exception(status) || tstate == NULL) {
            PyGILState_Release(gstate);
            enif_release_resource(ctx);
            return make_error(env, "subinterp_create_failed");
        }

        ctx->interp = PyThreadState_GetInterpreter(tstate);
        ctx->tstate = tstate;

        /* Create global/local namespaces in the new interpreter */
        ctx->globals = PyDict_New();
        ctx->locals = PyDict_New();
        ctx->module_cache = PyDict_New();

        /* Import __builtins__ */
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(ctx->globals, "__builtins__", builtins);

        /* Create erlang module in this subinterpreter */
        if (create_erlang_module() >= 0) {
            /* Import erlang module into globals */
            PyObject *erlang_module = PyImport_ImportModule("erlang");
            if (erlang_module != NULL) {
                PyDict_SetItemString(ctx->globals, "erlang", erlang_module);
                Py_DECREF(erlang_module);
            }
        }

        /* Switch back to main interpreter */
        PyThreadState_Swap(NULL);
        PyThreadState_Swap(main_tstate);

        PyGILState_Release(gstate);
    } else
#else
    /* Pre-3.12 Python - ignore subinterp mode request */
    (void)use_subinterp;
#endif
    {
        /* Worker mode - create a thread state in main interpreter */
        PyGILState_STATE gstate = PyGILState_Ensure();

#ifndef HAVE_SUBINTERPRETERS
        PyInterpreterState *interp = PyInterpreterState_Get();
        ctx->thread_state = PyThreadState_New(interp);
#endif

        ctx->globals = PyDict_New();
        ctx->locals = PyDict_New();
        ctx->module_cache = PyDict_New();

        /* Import __builtins__ into globals */
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(ctx->globals, "__builtins__", builtins);

        /* Import erlang module into globals for worker mode */
        PyObject *erlang_module = PyImport_ImportModule("erlang");
        if (erlang_module != NULL) {
            PyDict_SetItemString(ctx->globals, "erlang", erlang_module);
            Py_DECREF(erlang_module);
        }

        PyGILState_Release(gstate);
    }

    ERL_NIF_TERM ref = enif_make_resource(env, ctx);
    enif_release_resource(ctx);

    return enif_make_tuple3(env, ATOM_OK, ref, enif_make_uint(env, ctx->interp_id));
}

/**
 * @brief Destroy a Python context
 *
 * nif_context_destroy(ContextRef) -> ok
 *
 * This function does the actual cleanup because it's called from the
 * owning process's thread, which is the same thread that created the
 * context. This is important for OWN_GIL subinterpreters where the
 * thread state is tied to the creating thread.
 */
static ERL_NIF_TERM nif_context_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    /* Skip if already destroyed */
    if (ctx->destroyed) {
        return ATOM_OK;
    }

    if (!g_python_initialized) {
        ctx->destroyed = true;
        return ATOM_OK;
    }

#ifdef HAVE_SUBINTERPRETERS
    if (ctx->is_subinterp && ctx->tstate != NULL) {
        /* For subinterpreters with OWN_GIL, we're on the same thread
         * that created the context, so we can use the original tstate.
         *
         * 1. Switch to the subinterpreter's thread state
         * 2. Clean up objects
         * 3. End the interpreter
         * 4. Restore thread state (NULL is fine)
         */
        PyThreadState *old_tstate = PyThreadState_Swap(ctx->tstate);

        /* Clean up Python objects while holding the subinterpreter's GIL */
        Py_XDECREF(ctx->module_cache);
        ctx->module_cache = NULL;
        Py_XDECREF(ctx->globals);
        ctx->globals = NULL;
        Py_XDECREF(ctx->locals);
        ctx->locals = NULL;

        /* End the interpreter - this releases its GIL */
        Py_EndInterpreter(ctx->tstate);
        ctx->tstate = NULL;
        ctx->interp = NULL;

        /* Restore previous thread state if any */
        if (old_tstate != NULL && old_tstate != ctx->tstate) {
            PyThreadState_Swap(old_tstate);
        }
    } else
#endif
    {
        /* Worker mode - clean up with main GIL */
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_XDECREF(ctx->module_cache);
        ctx->module_cache = NULL;
        Py_XDECREF(ctx->globals);
        ctx->globals = NULL;
        Py_XDECREF(ctx->locals);
        ctx->locals = NULL;
#ifndef HAVE_SUBINTERPRETERS
        if (ctx->thread_state != NULL) {
            PyThreadState_Clear(ctx->thread_state);
            PyThreadState_Delete(ctx->thread_state);
            ctx->thread_state = NULL;
        }
#endif
        PyGILState_Release(gstate);
    }

    ctx->destroyed = true;
    return ATOM_OK;
}

/**
 * @brief Get module from cache or import it
 *
 * Helper function - no mutex needed since context is process-owned.
 */
static PyObject *context_get_module(py_context_t *ctx, const char *module_name) {
    /* Check cache first */
    if (ctx->module_cache != NULL) {
        PyObject *cached = PyDict_GetItemString(ctx->module_cache, module_name);
        if (cached != NULL) {
            return cached;  /* Borrowed reference */
        }
    }

    /* Import module */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        return NULL;
    }

    /* Cache it */
    if (ctx->module_cache != NULL) {
        PyDict_SetItemString(ctx->module_cache, module_name, module);
        Py_DECREF(module);  /* Dict now owns the reference */
        return PyDict_GetItemString(ctx->module_cache, module_name);
    }

    return module;  /* Caller must DECREF if not cached */
}

/**
 * @brief Call a Python function in a context
 *
 * nif_context_call(ContextRef, Module, Func, Args, Kwargs) -> {ok, Result} | {error, Reason} | {suspended, ...}
 *
 * NO MUTEX - caller must ensure exclusive access (process ownership)
 *
 * When Python code calls erlang.call(), this NIF may return {suspended, CallbackId, StateRef, {FuncName, Args}}
 * indicating that the context process should handle the callback and then call context_resume to continue.
 */
static ERL_NIF_TERM nif_context_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_context_t *ctx;
    ErlNifBinary module_bin, func_bin;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &func_bin)) {
        return make_error(env, "invalid_func");
    }

    char *module_name = binary_to_string(&module_bin);
    char *func_name = binary_to_string(&func_bin);
    if (module_name == NULL || func_name == NULL) {
        enif_free(module_name);
        enif_free(func_name);
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

#ifdef HAVE_SUBINTERPRETERS
    PyThreadState *saved_tstate = NULL;
    if (ctx->is_subinterp) {
        /* Enter the sub-interpreter - NO MUTEX LOCK */
        saved_tstate = PyThreadState_Swap(NULL);
        PyThreadState_Swap(ctx->tstate);
    } else {
        PyGILState_Ensure();
    }
#else
    PyGILState_STATE gstate = PyGILState_Ensure();
#endif

    /* Set thread-local context for callback support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;

    /* Enable suspension for callback support */
    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    /* Get or import module */
    PyObject *module = context_get_module(ctx, module_name);
    if (module == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Get function */
    PyObject *func = PyObject_GetAttrString(module, func_name);
    if (func == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert args */
    unsigned int args_len;
    if (!enif_get_list_length(env, argv[3], &args_len)) {
        Py_DECREF(func);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[3];
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
    if (argc > 4 && enif_is_map(env, argv[4])) {
        kwargs = term_to_py(env, argv[4]);
    }

    /* Call the function */
    PyObject *py_result = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (py_result == NULL) {
        /* Check for pending callback (flag-based detection) */
        if (tl_pending_callback) {
            PyErr_Clear();  /* Clear whatever exception is set */

            /* Create suspended context state */
            suspended_context_state_t *suspended = create_suspended_context_state_for_call(
                env, ctx, &module_bin, &func_bin, argv[3],
                argc > 4 ? argv[4] : enif_make_new_map(env));

            if (suspended == NULL) {
                tl_pending_callback = false;
                result = make_error(env, "create_suspended_state_failed");
            } else {
                result = build_suspended_context_result(env, suspended);
            }
        } else {
            result = make_py_error(env);
        }
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;

    enif_free(module_name);
    enif_free(func_name);

#ifdef HAVE_SUBINTERPRETERS
    if (ctx->is_subinterp) {
        /* Exit the sub-interpreter - NO MUTEX UNLOCK */
        PyThreadState_Swap(NULL);
        if (saved_tstate != NULL) {
            PyThreadState_Swap(saved_tstate);
        }
    } else {
        PyGILState_Release(PyGILState_UNLOCKED);
    }
#else
    PyGILState_Release(gstate);
#endif

    return result;
}

/**
 * @brief Evaluate a Python expression in a context
 *
 * nif_context_eval(ContextRef, Code, Locals) -> {ok, Result} | {error, Reason} | {suspended, ...}
 *
 * NO MUTEX - caller must ensure exclusive access (process ownership)
 *
 * When Python code calls erlang.call(), this NIF may return {suspended, CallbackId, StateRef, {FuncName, Args}}
 * indicating that the context process should handle the callback and then call context_resume to continue.
 */
static ERL_NIF_TERM nif_context_eval(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_context_t *ctx;
    ErlNifBinary code_bin;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

#ifdef HAVE_SUBINTERPRETERS
    PyThreadState *saved_tstate = NULL;
    if (ctx->is_subinterp) {
        saved_tstate = PyThreadState_Swap(NULL);
        PyThreadState_Swap(ctx->tstate);
    } else {
        PyGILState_Ensure();
    }
#else
    PyGILState_STATE gstate = PyGILState_Ensure();
#endif

    /* Set thread-local context for callback support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;

    /* Enable suspension for callback support */
    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    /* Update locals if provided */
    ERL_NIF_TERM locals_term = argc > 2 ? argv[2] : enif_make_new_map(env);
    if (argc > 2 && enif_is_map(env, argv[2])) {
        PyObject *new_locals = term_to_py(env, argv[2]);
        if (new_locals != NULL && PyDict_Check(new_locals)) {
            PyDict_Update(ctx->locals, new_locals);
            Py_DECREF(new_locals);
        }
    }

    /* Compile and evaluate */
    PyObject *py_result = PyRun_String(code, Py_eval_input, ctx->globals, ctx->locals);

    if (py_result == NULL) {
        /* Check for pending callback (flag-based detection) */
        if (tl_pending_callback) {
            PyErr_Clear();  /* Clear whatever exception is set */

            /* Create suspended context state */
            suspended_context_state_t *suspended = create_suspended_context_state_for_eval(
                env, ctx, &code_bin, locals_term);

            if (suspended == NULL) {
                tl_pending_callback = false;
                result = make_error(env, "create_suspended_state_failed");
            } else {
                result = build_suspended_context_result(env, suspended);
            }
        } else {
            result = make_py_error(env);
        }
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;

    enif_free(code);

#ifdef HAVE_SUBINTERPRETERS
    if (ctx->is_subinterp) {
        PyThreadState_Swap(NULL);
        if (saved_tstate != NULL) {
            PyThreadState_Swap(saved_tstate);
        }
    } else {
        PyGILState_Release(PyGILState_UNLOCKED);
    }
#else
    PyGILState_Release(gstate);
#endif

    return result;
}

/**
 * @brief Execute Python statements in a context
 *
 * nif_context_exec(ContextRef, Code) -> ok | {error, Reason}
 *
 * NO MUTEX - caller must ensure exclusive access (process ownership)
 */
static ERL_NIF_TERM nif_context_exec(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    ErlNifBinary code_bin;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

#ifdef HAVE_SUBINTERPRETERS
    PyThreadState *saved_tstate = NULL;
    if (ctx->is_subinterp) {
        saved_tstate = PyThreadState_Swap(NULL);
        PyThreadState_Swap(ctx->tstate);
    } else {
        PyGILState_Ensure();
    }
#else
    PyGILState_STATE gstate = PyGILState_Ensure();
#endif

    /* Set thread-local context for callback support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;

    /* Execute statements.
     * Use globals for both globals and locals to simulate module-level execution.
     * This ensures imports are accessible from function definitions. */
    PyObject *py_result = PyRun_String(code, Py_file_input, ctx->globals, ctx->globals);

    if (py_result == NULL) {
        result = make_py_error(env);
    } else {
        Py_DECREF(py_result);
        result = ATOM_OK;
    }

    /* Restore previous context */
    tl_current_context = prev_context;

    enif_free(code);

#ifdef HAVE_SUBINTERPRETERS
    if (ctx->is_subinterp) {
        PyThreadState_Swap(NULL);
        if (saved_tstate != NULL) {
            PyThreadState_Swap(saved_tstate);
        }
    } else {
        PyGILState_Release(PyGILState_UNLOCKED);
    }
#else
    PyGILState_Release(gstate);
#endif

    return result;
}

/**
 * @brief Call a method on a Python object in a context
 *
 * nif_context_call_method(ContextRef, ObjRef, Method, Args) -> {ok, Result} | {error, Reason}
 *
 * NO MUTEX - caller must ensure exclusive access (process ownership)
 */
static ERL_NIF_TERM nif_context_call_method(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    py_object_t *obj_wrapper;
    ErlNifBinary method_bin;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_get_resource(env, argv[1], PYOBJ_RESOURCE_TYPE, (void **)&obj_wrapper)) {
        return make_error(env, "invalid_object");
    }
    if (!enif_inspect_binary(env, argv[2], &method_bin)) {
        return make_error(env, "invalid_method");
    }

    char *method_name = binary_to_string(&method_bin);
    if (method_name == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

#ifdef HAVE_SUBINTERPRETERS
    PyThreadState *saved_tstate = NULL;
    if (ctx->is_subinterp) {
        saved_tstate = PyThreadState_Swap(NULL);
        PyThreadState_Swap(ctx->tstate);
    } else {
        PyGILState_Ensure();
    }
#else
    PyGILState_STATE gstate = PyGILState_Ensure();
#endif

    /* Get method */
    PyObject *method = PyObject_GetAttrString(obj_wrapper->obj, method_name);
    if (method == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert args */
    unsigned int args_len;
    if (!enif_get_list_length(env, argv[3], &args_len)) {
        Py_DECREF(method);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[3];
    for (unsigned int i = 0; i < args_len; i++) {
        enif_get_list_cell(env, tail, &head, &tail);
        PyObject *arg = term_to_py(env, head);
        if (arg == NULL) {
            Py_DECREF(args);
            Py_DECREF(method);
            result = make_error(env, "arg_conversion_failed");
            goto cleanup;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }

    /* Call method */
    PyObject *py_result = PyObject_Call(method, args, NULL);
    Py_DECREF(method);
    Py_DECREF(args);

    if (py_result == NULL) {
        result = make_py_error(env);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    enif_free(method_name);

#ifdef HAVE_SUBINTERPRETERS
    if (ctx->is_subinterp) {
        PyThreadState_Swap(NULL);
        if (saved_tstate != NULL) {
            PyThreadState_Swap(saved_tstate);
        }
    } else {
        PyGILState_Release(PyGILState_UNLOCKED);
    }
#else
    PyGILState_Release(gstate);
#endif

    return result;
}

/**
 * @brief Convert a Python object reference to an Erlang term
 *
 * nif_context_to_term(ObjRef) -> {ok, Term} | {error, Reason}
 */
static ERL_NIF_TERM nif_context_to_term(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_object_t *obj_wrapper;

    if (!enif_get_resource(env, argv[0], PYOBJ_RESOURCE_TYPE, (void **)&obj_wrapper)) {
        return make_error(env, "invalid_object");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();
    ERL_NIF_TERM term_result = py_to_term(env, obj_wrapper->obj);
    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, term_result);
}

/**
 * @brief Get the interpreter ID from a context reference
 *
 * nif_context_interp_id(ContextRef) -> InterpId
 */
static ERL_NIF_TERM nif_context_interp_id(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    return enif_make_uint(env, ctx->interp_id);
}

/**
 * @brief Set the callback handler for a context
 *
 * nif_context_set_callback_handler(ContextRef, Pid) -> ok | {error, Reason}
 *
 * This must be called before the context can handle erlang.call() callbacks.
 */
static ERL_NIF_TERM nif_context_set_callback_handler(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    ErlNifPid pid;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_get_local_pid(env, argv[1], &pid)) {
        return make_error(env, "invalid_pid");
    }

    ctx->callback_handler = pid;
    ctx->has_callback_handler = true;

    return ATOM_OK;
}

/**
 * @brief Get the callback pipe write FD for a context
 *
 * nif_context_get_callback_pipe(ContextRef) -> {ok, WriteFd} | {error, Reason}
 *
 * Returns the write end of the callback pipe for sending responses.
 */
static ERL_NIF_TERM nif_context_get_callback_pipe(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    if (ctx->callback_pipe[1] < 0) {
        return make_error(env, "pipe_not_initialized");
    }

    return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, ctx->callback_pipe[1]));
}

/**
 * @brief Write a callback response to the context's pipe
 *
 * nif_context_write_callback_response(ContextRef, Data) -> ok | {error, Reason}
 *
 * Writes a length-prefixed binary response to the callback pipe.
 */
static ERL_NIF_TERM nif_context_write_callback_response(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    ErlNifBinary data;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_inspect_binary(env, argv[1], &data)) {
        return make_error(env, "invalid_data");
    }

    if (ctx->callback_pipe[1] < 0) {
        return make_error(env, "pipe_not_initialized");
    }

    /* Write length prefix (4 bytes, native endianness - must match read_length_prefixed_data) */
    uint32_t len = (uint32_t)data.size;
    ssize_t written = write(ctx->callback_pipe[1], &len, sizeof(len));
    if (written != sizeof(len)) {
        return make_error(env, "write_failed");
    }

    written = write(ctx->callback_pipe[1], data.data, data.size);
    if (written != (ssize_t)data.size) {
        return make_error(env, "write_failed");
    }

    return ATOM_OK;
}

/**
 * @brief Resume a suspended context with callback result
 *
 * nif_context_resume(ContextRef, StateRef, ResultBinary) -> {ok, Result} | {error, Reason} | {suspended, ...}
 *
 * This NIF resumes Python execution after a callback has been handled.
 * The ResultBinary contains the callback result that will be returned to Python.
 *
 * If Python code makes another erlang.call() during resume, this NIF may
 * return {suspended, ...} again for nested callback handling.
 */
static ERL_NIF_TERM nif_context_resume(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    suspended_context_state_t *state;
    ErlNifBinary result_bin;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_get_resource(env, argv[1], PY_CONTEXT_SUSPENDED_RESOURCE_TYPE, (void **)&state)) {
        return make_error(env, "invalid_state_ref");
    }
    if (!enif_inspect_binary(env, argv[2], &result_bin)) {
        return make_error(env, "invalid_result");
    }

    /* Verify state belongs to this context */
    if (state->ctx != ctx) {
        return make_error(env, "context_mismatch");
    }

    /* Store the callback result */
    state->result_data = enif_alloc(result_bin.size);
    if (state->result_data == NULL) {
        return make_error(env, "alloc_failed");
    }
    memcpy(state->result_data, result_bin.data, result_bin.size);
    state->result_len = result_bin.size;
    state->has_result = true;

    ERL_NIF_TERM result;

#ifdef HAVE_SUBINTERPRETERS
    PyThreadState *saved_tstate = NULL;
    if (ctx->is_subinterp) {
        saved_tstate = PyThreadState_Swap(NULL);
        PyThreadState_Swap(ctx->tstate);
    } else {
        PyGILState_Ensure();
    }
#else
    PyGILState_STATE gstate = PyGILState_Ensure();
#endif

    /* Set thread-local state for replay */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;

    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    suspended_context_state_t *prev_suspended = tl_current_context_suspended;
    tl_current_context_suspended = state;

    /* Reset callback result index for this replay */
    state->callback_result_index = 0;

    if (state->request_type == PY_REQ_CALL) {
        /* Replay a py:call */
        char *module_name = enif_alloc(state->orig_module.size + 1);
        char *func_name = enif_alloc(state->orig_func.size + 1);

        if (module_name == NULL || func_name == NULL) {
            enif_free(module_name);
            enif_free(func_name);
            result = make_error(env, "alloc_failed");
            goto cleanup;
        }

        memcpy(module_name, state->orig_module.data, state->orig_module.size);
        module_name[state->orig_module.size] = '\0';
        memcpy(func_name, state->orig_func.data, state->orig_func.size);
        func_name[state->orig_func.size] = '\0';

        /* Get the function */
        PyObject *func = NULL;
        PyObject *module = context_get_module(ctx, module_name);
        if (module == NULL) {
            enif_free(module_name);
            enif_free(func_name);
            result = make_py_error(env);
            goto cleanup;
        }

        func = PyObject_GetAttrString(module, func_name);
        if (func == NULL) {
            enif_free(module_name);
            enif_free(func_name);
            result = make_py_error(env);
            goto cleanup;
        }

        /* Convert args */
        unsigned int args_len;
        if (!enif_get_list_length(state->orig_env, state->orig_args, &args_len)) {
            Py_DECREF(func);
            enif_free(module_name);
            enif_free(func_name);
            result = make_error(env, "invalid_args");
            goto cleanup;
        }

        PyObject *args = PyTuple_New(args_len);
        ERL_NIF_TERM head, tail = state->orig_args;
        for (unsigned int i = 0; i < args_len; i++) {
            enif_get_list_cell(state->orig_env, tail, &head, &tail);
            PyObject *arg = term_to_py(state->orig_env, head);
            if (arg == NULL) {
                Py_DECREF(args);
                Py_DECREF(func);
                enif_free(module_name);
                enif_free(func_name);
                result = make_error(env, "arg_conversion_failed");
                goto cleanup;
            }
            PyTuple_SET_ITEM(args, i, arg);
        }

        /* Convert kwargs */
        PyObject *kwargs = NULL;
        if (enif_is_map(state->orig_env, state->orig_kwargs)) {
            kwargs = term_to_py(state->orig_env, state->orig_kwargs);
        }

        /* Call the function (replay with cached result) */
        PyObject *py_result = PyObject_Call(func, args, kwargs);
        Py_DECREF(func);
        Py_DECREF(args);
        Py_XDECREF(kwargs);
        enif_free(module_name);
        enif_free(func_name);

        if (py_result == NULL) {
            /* Check for pending callback (nested callback during replay) */
            if (tl_pending_callback) {
                PyErr_Clear();

                /* Create new suspended context state for nested callback */
                suspended_context_state_t *nested = create_suspended_context_state_for_call(
                    env, ctx, &state->orig_module, &state->orig_func,
                    state->orig_args, state->orig_kwargs);

                if (nested == NULL) {
                    tl_pending_callback = false;
                    result = make_error(env, "create_nested_suspended_state_failed");
                } else {
                    /* Copy accumulated callback results from parent to nested state */
                    if (copy_callback_results_to_nested(nested, state) != 0) {
                        enif_release_resource(nested);
                        tl_pending_callback = false;
                        result = make_error(env, "copy_callback_results_failed");
                    } else {
                        result = build_suspended_context_result(env, nested);
                    }
                }
            } else {
                result = make_py_error(env);
            }
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, py_result);
            Py_DECREF(py_result);
            result = enif_make_tuple2(env, ATOM_OK, term_result);
        }

    } else if (state->request_type == PY_REQ_EVAL) {
        /* Replay a py:eval */
        char *code = enif_alloc(state->orig_code.size + 1);
        if (code == NULL) {
            result = make_error(env, "alloc_failed");
            goto cleanup;
        }
        memcpy(code, state->orig_code.data, state->orig_code.size);
        code[state->orig_code.size] = '\0';

        /* Update locals if provided */
        if (enif_is_map(state->orig_env, state->orig_locals)) {
            PyObject *new_locals = term_to_py(state->orig_env, state->orig_locals);
            if (new_locals != NULL && PyDict_Check(new_locals)) {
                PyDict_Update(ctx->locals, new_locals);
                Py_DECREF(new_locals);
            }
        }

        /* Compile and evaluate (replay with cached result) */
        PyObject *py_result = PyRun_String(code, Py_eval_input, ctx->globals, ctx->locals);
        enif_free(code);

        if (py_result == NULL) {
            /* Check for pending callback (nested callback during replay) */
            if (tl_pending_callback) {
                PyErr_Clear();

                /* Create new suspended context state for nested callback */
                suspended_context_state_t *nested = create_suspended_context_state_for_eval(
                    env, ctx, &state->orig_code, state->orig_locals);

                if (nested == NULL) {
                    tl_pending_callback = false;
                    result = make_error(env, "create_nested_suspended_state_failed");
                } else {
                    /* Copy accumulated callback results from parent to nested state */
                    if (copy_callback_results_to_nested(nested, state) != 0) {
                        enif_release_resource(nested);
                        tl_pending_callback = false;
                        result = make_error(env, "copy_callback_results_failed");
                    } else {
                        result = build_suspended_context_result(env, nested);
                    }
                }
            } else {
                result = make_py_error(env);
            }
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, py_result);
            Py_DECREF(py_result);
            result = enif_make_tuple2(env, ATOM_OK, term_result);
        }

    } else {
        result = make_error(env, "unsupported_request_type");
    }

cleanup:
    /* Restore thread-local state */
    tl_current_context_suspended = prev_suspended;
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;

#ifdef HAVE_SUBINTERPRETERS
    if (ctx->is_subinterp) {
        PyThreadState_Swap(NULL);
        if (saved_tstate != NULL) {
            PyThreadState_Swap(saved_tstate);
        }
    } else {
        PyGILState_Release(PyGILState_UNLOCKED);
    }
#else
    PyGILState_Release(gstate);
#endif

    return result;
}

/**
 * @brief Cancel a suspended context resume (cleanup on error)
 *
 * nif_context_cancel_resume(ContextRef, StateRef) -> ok
 *
 * Called when callback execution fails and resume won't be called.
 * Allows proper cleanup of the suspended state.
 */
static ERL_NIF_TERM nif_context_cancel_resume(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    suspended_context_state_t *state;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_get_resource(env, argv[1], PY_CONTEXT_SUSPENDED_RESOURCE_TYPE, (void **)&state)) {
        return make_error(env, "invalid_state_ref");
    }

    /* Verify state belongs to this context */
    if (state->ctx != ctx) {
        return make_error(env, "context_mismatch");
    }

    /* Mark as error so destructor knows to clean up properly */
    state->is_error = true;

    /* The resource destructor will clean up when the resource is GC'd */
    return ATOM_OK;
}

/* ============================================================================
 * py_ref NIFs - Python object references with interp_id for auto-routing
 * ============================================================================ */

/**
 * @brief Wrap a Python result as a py_ref with interp_id
 *
 * This is called internally when return => ref is specified.
 * nif_ref_wrap(ContextRef, PyObjTerm) -> RefTerm
 */
static ERL_NIF_TERM nif_ref_wrap(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    py_object_t *py_obj;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }
    if (!enif_get_resource(env, argv[1], PYOBJ_RESOURCE_TYPE, (void **)&py_obj)) {
        return make_error(env, "invalid_pyobj");
    }

    /* Allocate py_ref resource */
    py_ref_t *ref = enif_alloc_resource(PY_REF_RESOURCE_TYPE, sizeof(py_ref_t));
    if (ref == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Copy the PyObject reference and interp_id */
    ref->obj = py_obj->obj;
    ref->interp_id = ctx->interp_id;

    /* Increment reference count since we're taking ownership */
    PyGILState_STATE gstate = PyGILState_Ensure();
    Py_INCREF(ref->obj);
    PyGILState_Release(gstate);

    ERL_NIF_TERM ref_term = enif_make_resource(env, ref);
    enif_release_resource(ref);

    return enif_make_tuple2(env, ATOM_OK, ref_term);
}

/**
 * @brief Check if a term is a py_ref
 *
 * nif_is_ref(Term) -> true | false
 */
static ERL_NIF_TERM nif_is_ref(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_ref_t *ref;

    if (enif_get_resource(env, argv[0], PY_REF_RESOURCE_TYPE, (void **)&ref)) {
        return ATOM_TRUE;
    }
    return ATOM_FALSE;
}

/**
 * @brief Get the interpreter ID from a py_ref
 *
 * nif_ref_interp_id(Ref) -> InterpId
 *
 * This is fast - no GIL needed, just reads the stored interp_id.
 */
static ERL_NIF_TERM nif_ref_interp_id(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_ref_t *ref;

    if (!enif_get_resource(env, argv[0], PY_REF_RESOURCE_TYPE, (void **)&ref)) {
        return make_error(env, "invalid_ref");
    }

    return enif_make_uint(env, ref->interp_id);
}

/**
 * @brief Convert a py_ref to an Erlang term
 *
 * nif_ref_to_term(Ref) -> {ok, Term} | {error, Reason}
 */
static ERL_NIF_TERM nif_ref_to_term(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_ref_t *ref;

    if (!enif_get_resource(env, argv[0], PY_REF_RESOURCE_TYPE, (void **)&ref)) {
        return make_error(env, "invalid_ref");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();
    ERL_NIF_TERM result = py_to_term(env, ref->obj);
    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, result);
}

/**
 * @brief Get an attribute from a py_ref object
 *
 * nif_ref_getattr(Ref, AttrName) -> {ok, Value} | {error, Reason}
 */
static ERL_NIF_TERM nif_ref_getattr(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_ref_t *ref;
    ErlNifBinary attr_bin;

    if (!enif_get_resource(env, argv[0], PY_REF_RESOURCE_TYPE, (void **)&ref)) {
        return make_error(env, "invalid_ref");
    }
    if (!enif_inspect_binary(env, argv[1], &attr_bin)) {
        return make_error(env, "invalid_attr");
    }

    char *attr_name = binary_to_string(&attr_bin);
    if (attr_name == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;
    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *attr = PyObject_GetAttrString(ref->obj, attr_name);
    if (attr == NULL) {
        result = make_py_error(env);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, attr);
        Py_DECREF(attr);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

    PyGILState_Release(gstate);
    enif_free(attr_name);

    return result;
}

/**
 * @brief Call a method on a py_ref object
 *
 * nif_ref_call_method(Ref, Method, Args) -> {ok, Result} | {error, Reason}
 */
static ERL_NIF_TERM nif_ref_call_method(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_ref_t *ref;
    ErlNifBinary method_bin;

    if (!enif_get_resource(env, argv[0], PY_REF_RESOURCE_TYPE, (void **)&ref)) {
        return make_error(env, "invalid_ref");
    }
    if (!enif_inspect_binary(env, argv[1], &method_bin)) {
        return make_error(env, "invalid_method");
    }

    char *method_name = binary_to_string(&method_bin);
    if (method_name == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Get method */
    PyObject *method = PyObject_GetAttrString(ref->obj, method_name);
    if (method == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert args */
    unsigned int args_len;
    if (!enif_get_list_length(env, argv[2], &args_len)) {
        Py_DECREF(method);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[2];
    for (unsigned int i = 0; i < args_len; i++) {
        enif_get_list_cell(env, tail, &head, &tail);
        PyObject *arg = term_to_py(env, head);
        if (arg == NULL) {
            Py_DECREF(args);
            Py_DECREF(method);
            result = make_error(env, "arg_conversion_failed");
            goto cleanup;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }

    /* Call method */
    PyObject *py_result = PyObject_Call(method, args, NULL);
    Py_DECREF(method);
    Py_DECREF(args);

    if (py_result == NULL) {
        result = make_py_error(env);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    PyGILState_Release(gstate);
    enif_free(method_name);

    return result;
}

/* ============================================================================
 * NIF setup
 * ============================================================================ */

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info) {
    (void)priv_data;
    (void)load_info;

    /* Create resource types */
    WORKER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_worker", worker_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    PYOBJ_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_object", pyobj_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    ASYNC_WORKER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_async_worker", async_worker_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    SUSPENDED_STATE_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_suspended_state", suspended_state_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

#ifdef HAVE_SUBINTERPRETERS
    SUBINTERP_WORKER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_subinterp_worker", subinterp_worker_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
#endif

    /* Process-per-context resource type (no mutex) */
    PY_CONTEXT_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_context", context_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    /* py_ref resource type (Python object with interp_id for auto-routing) */
    PY_REF_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_ref", py_ref_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    /* suspended_context_state_t resource type (context suspension for callbacks) */
    PY_CONTEXT_SUSPENDED_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_context_suspended", suspended_context_state_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (WORKER_RESOURCE_TYPE == NULL || PYOBJ_RESOURCE_TYPE == NULL ||
        ASYNC_WORKER_RESOURCE_TYPE == NULL || SUSPENDED_STATE_RESOURCE_TYPE == NULL ||
        PY_CONTEXT_RESOURCE_TYPE == NULL || PY_REF_RESOURCE_TYPE == NULL ||
        PY_CONTEXT_SUSPENDED_RESOURCE_TYPE == NULL) {
        return -1;
    }
#ifdef HAVE_SUBINTERPRETERS
    if (SUBINTERP_WORKER_RESOURCE_TYPE == NULL) {
        return -1;
    }
#endif

    /* Initialize atoms */
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_NONE = enif_make_atom(env, "none");
    ATOM_NIL = enif_make_atom(env, "nil");
    ATOM_UNDEFINED = enif_make_atom(env, "undefined");
    ATOM_NIF_NOT_LOADED = enif_make_atom(env, "nif_not_loaded");
    ATOM_GENERATOR = enif_make_atom(env, "generator");
    ATOM_STOP_ITERATION = enif_make_atom(env, "stop_iteration");
    ATOM_TIMEOUT = enif_make_atom(env, "timeout");
    ATOM_NAN = enif_make_atom(env, "nan");
    ATOM_INFINITY = enif_make_atom(env, "infinity");
    ATOM_NEG_INFINITY = enif_make_atom(env, "neg_infinity");
    ATOM_ERLANG_CALLBACK = enif_make_atom(env, "erlang_callback");
    ATOM_ASYNC_RESULT = enif_make_atom(env, "async_result");
    ATOM_ASYNC_ERROR = enif_make_atom(env, "async_error");
    ATOM_SUSPENDED = enif_make_atom(env, "suspended");

    /* Logging atoms */
    ATOM_PY_LOG = enif_make_atom(env, "py_log");
    ATOM_SPAN_START = enif_make_atom(env, "span_start");
    ATOM_SPAN_END = enif_make_atom(env, "span_end");
    ATOM_SPAN_EVENT = enif_make_atom(env, "span_event");

    /* ASGI scope atoms */
    ATOM_ASGI_PATH = enif_make_atom(env, "path");
    ATOM_ASGI_HEADERS = enif_make_atom(env, "headers");
    ATOM_ASGI_CLIENT = enif_make_atom(env, "client");
    ATOM_ASGI_QUERY_STRING = enif_make_atom(env, "query_string");
    ATOM_ASGI_METHOD = enif_make_atom(env, "method");

    /* Worker pool atoms */
    pool_atoms_init(env);

    /* ASGI buffer resource type for zero-copy body handling */
    ASGI_BUFFER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "asgi_buffer",
        asgi_buffer_resource_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    /* ASGI lazy headers resource type for on-demand header conversion */
    ASGI_LAZY_HEADERS_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "asgi_lazy_headers",
        lazy_headers_resource_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    /* Initialize event loop module */
    if (event_loop_init(env) < 0) {
        return -1;
    }

    return 0;
}

static int upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data,
                   ERL_NIF_TERM load_info) {
    (void)old_priv_data;
    return load(env, priv_data, load_info);
}

static void unload(ErlNifEnv *env, void *priv_data) {
    (void)env;
    (void)priv_data;
    /* Clean up cached function references */
    cleanup_callback_cache();
    /* Clean up callback name registry */
    cleanup_callback_registry();
    /* Other cleanup handled by finalize */
}

static ErlNifFunc nif_funcs[] = {
    /* Initialization */
    {"init", 0, nif_py_init, 0},
    {"init", 1, nif_py_init, 0},
    {"finalize", 0, nif_finalize, 0},

    /* Worker management */
    {"worker_new", 0, nif_worker_new, 0},
    {"worker_new", 1, nif_worker_new, 0},
    {"worker_destroy", 1, nif_worker_destroy, 0},

    /* Python execution - dirty I/O NIFs */
    {"worker_call", 5, nif_worker_call, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_call", 6, nif_worker_call, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_eval", 3, nif_worker_eval, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_eval", 4, nif_worker_eval, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_exec", 2, nif_worker_exec, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_next", 2, nif_worker_next, ERL_NIF_DIRTY_JOB_IO_BOUND},

    /* Module operations */
    {"import_module", 2, nif_import_module, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"get_attr", 3, nif_get_attr, ERL_NIF_DIRTY_JOB_IO_BOUND},

    /* Info */
    {"version", 0, nif_version, 0},

    /* Memory and GC */
    {"memory_stats", 0, nif_memory_stats, 0},
    {"gc", 0, nif_gc, 0},
    {"gc", 1, nif_gc, 0},
    {"tracemalloc_start", 0, nif_tracemalloc_start, 0},
    {"tracemalloc_start", 1, nif_tracemalloc_start, 0},
    {"tracemalloc_stop", 0, nif_tracemalloc_stop, 0},

    /* Callback support */
    {"set_callback_handler", 2, nif_set_callback_handler, 0},
    {"send_callback_response", 2, nif_send_callback_response, 0},
    {"resume_callback", 2, nif_resume_callback, 0},

    /* Async worker management */
    {"async_worker_new", 0, nif_async_worker_new, 0},
    {"async_worker_destroy", 1, nif_async_worker_destroy, 0},

    /* Async execution - dirty I/O NIFs */
    {"async_call", 6, nif_async_call, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"async_gather", 3, nif_async_gather, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"async_stream", 6, nif_async_stream, ERL_NIF_DIRTY_JOB_IO_BOUND},

    /* Sub-interpreter support */
    {"subinterp_supported", 0, nif_subinterp_supported, 0},
    {"subinterp_worker_new", 0, nif_subinterp_worker_new, 0},
    {"subinterp_worker_destroy", 1, nif_subinterp_worker_destroy, 0},
    {"subinterp_call", 5, nif_subinterp_call, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"parallel_execute", 2, nif_parallel_execute, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"subinterp_asgi_run", 6, nif_subinterp_asgi_run, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    /* Execution mode info */
    {"execution_mode", 0, nif_execution_mode, 0},
    {"num_executors", 0, nif_num_executors, 0},

    /* Thread worker support (ThreadPoolExecutor) */
    {"thread_worker_set_coordinator", 1, nif_thread_worker_set_coordinator, 0},
    {"thread_worker_write", 2, nif_thread_worker_write, 0},
    {"thread_worker_signal_ready", 1, nif_thread_worker_signal_ready, 0},

    /* Async callback support (for erlang.async_call) */
    {"async_callback_response", 3, nif_async_callback_response, 0},

    /* Callback name registry (prevents torch introspection issues) */
    {"register_callback_name", 1, nif_register_callback_name, 0},
    {"unregister_callback_name", 1, nif_unregister_callback_name, 0},

    /* Logging and tracing */
    {"set_log_receiver", 2, nif_set_log_receiver, 0},
    {"clear_log_receiver", 0, nif_clear_log_receiver, 0},
    {"set_trace_receiver", 1, nif_set_trace_receiver, 0},
    {"clear_trace_receiver", 0, nif_clear_trace_receiver, 0},

    /* Erlang-native event loop NIFs */
    {"event_loop_new", 0, nif_event_loop_new, 0},
    {"event_loop_destroy", 1, nif_event_loop_destroy, 0},
    {"event_loop_set_router", 2, nif_event_loop_set_router, 0},
    {"event_loop_set_worker", 2, nif_event_loop_set_worker, 0},
    {"event_loop_set_id", 2, nif_event_loop_set_id, 0},
    {"event_loop_wakeup", 1, nif_event_loop_wakeup, 0},
    {"add_reader", 3, nif_add_reader, 0},
    {"remove_reader", 2, nif_remove_reader, 0},
    {"add_writer", 3, nif_add_writer, 0},
    {"remove_writer", 2, nif_remove_writer, 0},
    {"call_later", 3, nif_call_later, 0},
    {"cancel_timer", 2, nif_cancel_timer, 0},
    {"poll_events", 2, nif_poll_events, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"get_pending", 1, nif_get_pending, 0},
    {"dispatch_callback", 3, nif_dispatch_callback, 0},
    {"dispatch_timer", 2, nif_dispatch_timer, 0},
    {"dispatch_sleep_complete", 2, nif_dispatch_sleep_complete, 0},
    {"get_fd_callback_id", 2, nif_get_fd_callback_id, 0},
    {"reselect_reader", 2, nif_reselect_reader, 0},
    {"reselect_writer", 2, nif_reselect_writer, 0},
    {"reselect_reader_fd", 1, nif_reselect_reader_fd, 0},
    {"reselect_writer_fd", 1, nif_reselect_writer_fd, 0},
    /* FD lifecycle management (uvloop-like API) */
    {"handle_fd_event", 2, nif_handle_fd_event, 0},
    {"handle_fd_event_and_reselect", 2, nif_handle_fd_event_and_reselect, 0},
    {"stop_reader", 1, nif_stop_reader, 0},
    {"start_reader", 1, nif_start_reader, 0},
    {"stop_writer", 1, nif_stop_writer, 0},
    {"start_writer", 1, nif_start_writer, 0},
    {"cancel_reader", 2, nif_cancel_reader, 0},  /* Legacy alias */
    {"cancel_writer", 2, nif_cancel_writer, 0},  /* Legacy alias */
    {"close_fd", 1, nif_close_fd, 0},
    /* Test helpers for fd monitoring (using pipes) */
    {"create_test_pipe", 0, nif_create_test_pipe, 0},
    {"close_test_fd", 1, nif_close_test_fd, 0},
    {"write_test_fd", 2, nif_write_test_fd, 0},
    {"read_test_fd", 2, nif_read_test_fd, 0},
    /* TCP test helpers */
    {"create_test_tcp_listener", 1, nif_create_test_tcp_listener, 0},
    {"accept_test_tcp", 1, nif_accept_test_tcp, 0},
    {"connect_test_tcp", 2, nif_connect_test_tcp, 0},
    /* UDP test helpers */
    {"create_test_udp_socket", 1, nif_create_test_udp_socket, 0},
    {"recvfrom_test_udp", 2, nif_recvfrom_test_udp, 0},
    {"sendto_test_udp", 4, nif_sendto_test_udp, 0},
    {"set_udp_broadcast", 2, nif_set_udp_broadcast, 0},
    /* Python event loop integration */
    {"set_python_event_loop", 1, nif_set_python_event_loop, 0},
    {"set_isolation_mode", 1, nif_set_isolation_mode, 0},
    {"set_shared_router", 1, nif_set_shared_router, 0},

    /* ASGI optimizations */
    {"asgi_build_scope", 1, nif_asgi_build_scope, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"asgi_run", 5, nif_asgi_run, ERL_NIF_DIRTY_JOB_IO_BOUND},
#ifdef ASGI_PROFILING
    {"asgi_profile_stats", 0, nif_asgi_profile_stats, 0},
    {"asgi_profile_reset", 0, nif_asgi_profile_reset, 0},
#endif

    /* WSGI optimizations */
    {"wsgi_run", 4, nif_wsgi_run, ERL_NIF_DIRTY_JOB_IO_BOUND},

    /* Worker pool */
    {"pool_start", 1, nif_pool_start, 0},
    {"pool_stop", 0, nif_pool_stop, 0},
    {"pool_submit", 5, nif_pool_submit, 0},
    {"pool_stats", 0, nif_pool_stats, 0},

    /* Process-per-context API (no mutex) */
    {"context_create", 1, nif_context_create, 0},
    {"context_destroy", 1, nif_context_destroy, 0},
    {"context_call", 5, nif_context_call, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_eval", 3, nif_context_eval, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_exec", 2, nif_context_exec, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_call_method", 4, nif_context_call_method, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_to_term", 1, nif_context_to_term, 0},
    {"context_interp_id", 1, nif_context_interp_id, 0},
    {"context_set_callback_handler", 2, nif_context_set_callback_handler, 0},
    {"context_get_callback_pipe", 1, nif_context_get_callback_pipe, 0},
    {"context_write_callback_response", 2, nif_context_write_callback_response, 0},
    {"context_resume", 3, nif_context_resume, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_cancel_resume", 2, nif_context_cancel_resume, 0},

    /* py_ref API (Python object references with interp_id) */
    {"ref_wrap", 2, nif_ref_wrap, 0},
    {"is_ref", 1, nif_is_ref, 0},
    {"ref_interp_id", 1, nif_ref_interp_id, 0},
    {"ref_to_term", 1, nif_ref_to_term, 0},
    {"ref_getattr", 2, nif_ref_getattr, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"ref_call_method", 3, nif_ref_call_method, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(py_nif, nif_funcs, load, NULL, upgrade, unload)
