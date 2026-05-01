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
#include "py_util.h"
#include "py_event_loop.h"
#include "py_channel.h"
#include "py_buffer.h"

/* ============================================================================
 * Global state definitions
 * ============================================================================ */

ErlNifResourceType *WORKER_RESOURCE_TYPE = NULL;
ErlNifResourceType *PYOBJ_RESOURCE_TYPE = NULL;
/* ASYNC_WORKER_RESOURCE_TYPE removed - async workers replaced by event loop model */
ErlNifResourceType *SUSPENDED_STATE_RESOURCE_TYPE = NULL;

/* Process-per-context resource type (no mutex) */
ErlNifResourceType *PY_CONTEXT_RESOURCE_TYPE = NULL;

/* py_ref resource type (Python object with interp_id for auto-routing) */
ErlNifResourceType *PY_REF_RESOURCE_TYPE = NULL;

/* suspended_context_state_t resource type (context suspension for callbacks) */
ErlNifResourceType *PY_CONTEXT_SUSPENDED_RESOURCE_TYPE = NULL;

/* inline_continuation_t resource type (inline scheduler continuation) */
ErlNifResourceType *INLINE_CONTINUATION_RESOURCE_TYPE = NULL;

/* Process-local Python environment resource type */
ErlNifResourceType *PY_ENV_RESOURCE_TYPE = NULL;

/* Process-scoped shared dictionary resource type */
ErlNifResourceType *PY_SHARED_DICT_RESOURCE_TYPE = NULL;

/* Getter for PY_ENV_RESOURCE_TYPE (used by py_event_loop.c) */
ErlNifResourceType *get_env_resource_type(void) {
    return PY_ENV_RESOURCE_TYPE;
}

_Atomic uint32_t g_context_id_counter = 1;

/* ============================================================================
 * Process-local Python Environment
 * ============================================================================
 * Each Erlang process can have its own Python globals/locals dict via a NIF
 * resource stored in the process dictionary. When the process exits, the
 * resource destructor frees the Python dicts.
 */

/* py_env_resource_t is now defined in py_nif.h */

/**
 * @brief Destructor for py_env_resource_t
 *
 * Called when the resource reference is garbage collected (process exits).
 * Acquires GIL and decrefs the Python dicts.
 *
 * For subinterpreters, we must DECREF in the correct interpreter context.
 * If the interpreter was destroyed (context freed), we skip DECREF since
 * the objects were already freed with the interpreter.
 */
static void py_env_resource_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_env_resource_t *res = (py_env_resource_t *)obj;

    if (!runtime_is_running()) {
        res->globals = NULL;
        res->locals = NULL;
        return;
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

#ifdef HAVE_SUBINTERPRETERS
    if (res->interp_id != 0) {
        /* OWN_GIL subinterpreter: interp_id != 0
         * These dicts were created in an OWN_GIL interpreter. We cannot safely
         * DECREF them here because:
         * 1. The interpreter might already be destroyed
         * 2. We cannot switch to its thread state from this thread
         * When the OWN_GIL context is destroyed, Py_EndInterpreter cleans up
         * all objects, so we skip DECREF to avoid double-free or invalid access. */
    } else
#endif
    {
        /* Main interpreter */
        Py_XDECREF(res->globals);
        Py_XDECREF(res->locals);
    }

    PyGILState_Release(gstate);
    res->globals = NULL;
    res->locals = NULL;
}

/* Invariant counters for debugging and leak detection */
py_invariant_counters_t g_counters = {0};

_Atomic py_runtime_state_t g_runtime_state = PY_STATE_UNINIT;
PyThreadState *g_main_thread_state = NULL;

/* Execution mode */
py_execution_mode_t g_execution_mode = PY_MODE_GIL;

/* Single executor state */
pthread_t g_executor_thread;
pthread_mutex_t g_executor_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_executor_cond = PTHREAD_COND_INITIALIZER;
py_request_t *g_executor_queue_head = NULL;
py_request_t *g_executor_queue_tail = NULL;
_Atomic bool g_executor_running = false;
_Atomic bool g_executor_shutdown = false;

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

/**
 * Clear all pending callback thread-local state.
 *
 * Must be called at context boundaries while still in the correct interpreter
 * context, to prevent cross-interpreter contamination if Python code caught
 * and swallowed SuspensionRequiredException.
 */
static inline void clear_pending_callback_tls(void) {
    tl_pending_callback = false;
    tl_pending_callback_id = 0;
    if (tl_pending_func_name != NULL) {
        enif_free(tl_pending_func_name);
        tl_pending_func_name = NULL;
    }
    tl_pending_func_name_len = 0;
    Py_CLEAR(tl_pending_args);
}

/* Thread-local timeout state */
__thread uint64_t tl_timeout_deadline = 0;
__thread bool tl_timeout_enabled = false;

/* Thread-local variable to track current local env during reentrant calls */
__thread py_env_resource_t *tl_current_local_env = NULL;

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
ERL_NIF_TERM ATOM_SCHEDULE;
ERL_NIF_TERM ATOM_MORE;

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

/* Schedule marker type and helper - from py_callback.c, needed by py_exec.c */
typedef struct {
    PyObject_HEAD
    PyObject *callback_name;  /* Registered callback name (string) */
    PyObject *args;           /* Arguments (tuple) */
} ScheduleMarkerObject;
static int is_schedule_marker(PyObject *obj);

/* Inline schedule marker type and helper - from py_callback.c, needed by py_exec.c */
typedef struct {
    PyObject_HEAD
    PyObject *module;      /* Module name (string) */
    PyObject *func;        /* Function name (string) */
    PyObject *args;        /* Arguments (tuple or None) */
    PyObject *kwargs;      /* Keyword arguments (dict or None) */
    PyObject *globals;     /* Captured globals from caller's frame */
    PyObject *locals;      /* Captured locals from caller's frame */
} InlineScheduleMarkerObject;
static int is_inline_schedule_marker(PyObject *obj);

/* ============================================================================
 * Include module implementations
 * ============================================================================ */

#include "py_util.c"
#include "py_convert.c"
#include "py_exec.c"
#include "py_logging.c"
#include "py_shared_dict.c"
#include "py_callback.c"
#include "py_thread_worker.c"
#include "py_event_loop.c"
#include "py_worker_pool.h"
#include "py_worker_pool.c"
#include "py_subinterp_thread.c"
#include "py_reactor_buffer.c"
#include "py_channel.c"
#include "py_buffer.c"

/* ============================================================================
 * Resource callbacks
 * ============================================================================ */

static void worker_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_worker_t *worker = (py_worker_t *)obj;

    /* Close callback pipes */
    close_pipe_pair(worker->callback_pipe);

    /* Only clean up Python state if Python is still initialized */
    if (worker->thread_state != NULL && runtime_is_running()) {
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

    if (wrapper->obj != NULL && runtime_is_running()) {
#ifdef HAVE_SUBINTERPRETERS
        /* For subinterpreter-owned objects (interp_id > 0):
         * Objects are cleaned up by Py_EndInterpreter when context is destroyed.
         * Skip eager cleanup here - let Python GC handle it.
         *
         * For main-interpreter objects (interp_id == 0):
         * Safe to use PyGILState_Ensure for cleanup. */
        if (wrapper->interp_id > 0) {
            atomic_fetch_add(&g_counters.pyobj_destroyed, 1);
            return;
        }
#endif
        /* Main interpreter (or no subinterpreters): safe to use PyGILState_Ensure */
        PyThreadState *existing = PyGILState_GetThisThreadState();
        if (existing != NULL || PyGILState_Check()) {
            atomic_fetch_add(&g_counters.pyobj_destroyed, 1);
            return;
        }

        PyGILState_STATE gstate = PyGILState_Ensure();

        /* Skip DECREF for generators, coroutines, and async generators */
        if (!PyGen_Check(wrapper->obj) && !PyCoro_CheckExact(wrapper->obj) &&
            !PyAsyncGen_CheckExact(wrapper->obj)) {
            Py_DECREF(wrapper->obj);
            wrapper->obj = NULL;
        }

        PyGILState_Release(gstate);
    }
    atomic_fetch_add(&g_counters.pyobj_destroyed, 1);
}

/* async_worker_destructor and subinterp_worker_destructor removed —
 * async workers replaced by event loop model; subinterp_worker resource
 * type retired with the explicit handle API. */

/**
 * @brief Destructor for py_context_t (process-per-context)
 *
 * Safety net: If the context wasn't properly destroyed via nif_context_destroy,
 * we attempt cleanup here. For subinterpreter mode, we release the pool slot.
 */
static void context_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_context_t *ctx = (py_context_t *)obj;

    /* Close callback pipes if open */
    close_pipe_pair(ctx->callback_pipe);

    /* Skip if already destroyed by nif_context_destroy */
    if (ctx->destroyed) {
        return;
    }

    if (!runtime_is_running()) {
        return;
    }

#ifdef HAVE_SUBINTERPRETERS
    /* Worker-mode contexts in HAVE_SUBINTERPRETERS builds: clean up
     * Python dicts with GIL. */
    if (PyGILState_GetThisThreadState() != NULL || PyGILState_Check()) {
        return;
    }

    {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_XDECREF(ctx->module_cache);
        Py_XDECREF(ctx->globals);
        Py_XDECREF(ctx->locals);
        PyGILState_Release(gstate);
    }
#else
    /* Non-HAVE_SUBINTERPRETERS: all contexts are worker mode */
    /* Worker mode: safe to use PyGILState_Ensure */
    if (PyGILState_GetThisThreadState() != NULL || PyGILState_Check()) {
        return;
    }

    PyGILState_STATE gstate = PyGILState_Ensure();
    Py_XDECREF(ctx->module_cache);
    Py_XDECREF(ctx->globals);
    Py_XDECREF(ctx->locals);
    if (ctx->thread_state != NULL) {
        PyThreadState_Clear(ctx->thread_state);
        PyThreadState_Delete(ctx->thread_state);
    }
    PyGILState_Release(gstate);
#endif
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

    if (runtime_is_running() && ref->obj != NULL) {
#ifdef HAVE_SUBINTERPRETERS
        /* For subinterpreter-owned objects (interp_id > 0):
         * Objects are cleaned up by Py_EndInterpreter when context is destroyed.
         *
         * For main-interpreter objects (interp_id == 0):
         * Safe to use PyGILState_Ensure for cleanup. */
        if (ref->interp_id > 0) {
            atomic_fetch_add(&g_counters.pyref_destroyed, 1);
            return;
        }
#endif
        /* Main interpreter (or no subinterpreters): safe to use PyGILState_Ensure */
        if (PyGILState_GetThisThreadState() != NULL || PyGILState_Check()) {
            atomic_fetch_add(&g_counters.pyref_destroyed, 1);
            return;
        }

        PyGILState_STATE gstate = PyGILState_Ensure();

        /* Skip DECREF for generators, coroutines, and async generators */
        if (!PyGen_Check(ref->obj) && !PyCoro_CheckExact(ref->obj) &&
            !PyAsyncGen_CheckExact(ref->obj)) {
            Py_XDECREF(ref->obj);
            ref->obj = NULL;
        }

        PyGILState_Release(gstate);
    }
    atomic_fetch_add(&g_counters.pyref_destroyed, 1);
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
    if (runtime_is_running() && state->callback_args != NULL) {
#ifdef HAVE_SUBINTERPRETERS
        /* For subinterpreter contexts: defer cleanup to Py_EndInterpreter.
         * For main-interpreter contexts: safe to use PyGILState_Ensure. */
        if (state->ctx != NULL && state->ctx->is_subinterp) {
            state->callback_args = NULL;
        } else
#endif
        {
            /* Main interpreter (or no subinterpreters): safe to use PyGILState_Ensure */
            if (PyGILState_GetThisThreadState() != NULL || PyGILState_Check()) {
                state->callback_args = NULL;
            } else {
                PyGILState_STATE gstate = PyGILState_Ensure();
                Py_XDECREF(state->callback_args);
                state->callback_args = NULL;
                PyGILState_Release(gstate);
            }
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

    /* Release the context resource (was kept in create_suspended_context_state_*) */
    if (state->ctx != NULL) {
        enif_release_resource(state->ctx);
        state->ctx = NULL;
    }

    atomic_fetch_add(&g_counters.suspended_destroyed, 1);
}

static void suspended_state_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    suspended_state_t *state = (suspended_state_t *)obj;

    /* Clean up Python objects if Python is still initialized.
     * suspended_state_t is used with the worker-based API which runs in
     * the main interpreter, so we always use PyGILState_Ensure. */
    if (runtime_is_running() && state->callback_args != NULL) {
        if (PyGILState_GetThisThreadState() != NULL || PyGILState_Check()) {
            state->callback_args = NULL;
        } else {
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_XDECREF(state->callback_args);
            state->callback_args = NULL;
            PyGILState_Release(gstate);
        }
    }

    /* Free allocated memory */
    if (state->callback_func_name != NULL) {
        enif_free(state->callback_func_name);
        state->callback_func_name = NULL;
    }
    if (state->result_data != NULL) {
        enif_free(state->result_data);
        state->result_data = NULL;
    }

    /* Free original context environment */
    if (state->orig_env != NULL) {
        enif_free_env(state->orig_env);
        state->orig_env = NULL;
    }

    /* Destroy synchronization primitives */
    pthread_mutex_destroy(&state->mutex);
    pthread_cond_destroy(&state->cond);

    atomic_fetch_add(&g_counters.suspended_destroyed, 1);
}

/* ============================================================================
 * Inline Continuation Support
 * ============================================================================
 *
 * Inline continuations allow Python functions to chain directly via
 * enif_schedule_nif() without returning to Erlang messaging.
 */

/**
 * @brief Destructor for inline_continuation_t resource
 *
 * Frees all resources associated with an inline continuation.
 */
static void inline_continuation_destructor(ErlNifEnv *env, void *obj) {
    (void)env;
    inline_continuation_t *cont = (inline_continuation_t *)obj;

    /* Free string allocations */
    if (cont->module_name != NULL) {
        enif_free(cont->module_name);
        cont->module_name = NULL;
    }
    if (cont->func_name != NULL) {
        enif_free(cont->func_name);
        cont->func_name = NULL;
    }

    /* Clean up Python objects if Python is still initialized */
    if (runtime_is_running() && (cont->args != NULL || cont->kwargs != NULL ||
                                  cont->globals != NULL || cont->locals != NULL)) {
        /* For subinterpreter contexts: defer cleanup to Py_EndInterpreter */
#ifdef HAVE_SUBINTERPRETERS
        if (cont->ctx != NULL && cont->ctx->is_subinterp) {
            cont->args = NULL;
            cont->kwargs = NULL;
            cont->globals = NULL;
            cont->locals = NULL;
        } else
#endif
        {
            /* Main interpreter: safe to use PyGILState_Ensure */
            if (PyGILState_GetThisThreadState() == NULL && !PyGILState_Check()) {
                PyGILState_STATE gstate = PyGILState_Ensure();
                Py_XDECREF(cont->args);
                Py_XDECREF(cont->kwargs);
                Py_XDECREF(cont->globals);
                Py_XDECREF(cont->locals);
                cont->args = NULL;
                cont->kwargs = NULL;
                cont->globals = NULL;
                cont->locals = NULL;
                PyGILState_Release(gstate);
            } else {
                cont->args = NULL;
                cont->kwargs = NULL;
                cont->globals = NULL;
                cont->locals = NULL;
            }
        }
    }

    /* Release the context resource if held */
    if (cont->ctx != NULL) {
        enif_release_resource(cont->ctx);
        cont->ctx = NULL;
    }

    /* Release the local_env resource if held */
    if (cont->local_env != NULL) {
        enif_release_resource(cont->local_env);
        cont->local_env = NULL;
    }
}

/**
 * @brief Create an inline continuation resource
 *
 * @param ctx Context for execution (will be kept)
 * @param local_env Optional process-local environment (will be kept if non-NULL)
 * @param marker The InlineScheduleMarker containing call info
 * @param depth Current continuation depth
 * @return inline_continuation_t* or NULL on failure
 *
 * @note Caller must release the resource when done
 */
static inline_continuation_t *create_inline_continuation(
    py_context_t *ctx,
    void *local_env,  /* py_env_resource_t* */
    PyObject *marker_obj,
    uint32_t depth) {

    InlineScheduleMarkerObject *marker = (InlineScheduleMarkerObject *)marker_obj;

    inline_continuation_t *cont = enif_alloc_resource(
        INLINE_CONTINUATION_RESOURCE_TYPE, sizeof(inline_continuation_t));
    if (cont == NULL) {
        return NULL;
    }

    memset(cont, 0, sizeof(inline_continuation_t));

    /* Copy module name */
    Py_ssize_t module_len;
    const char *module_str = PyUnicode_AsUTF8AndSize(marker->module, &module_len);
    if (module_str == NULL) {
        enif_release_resource(cont);
        return NULL;
    }
    cont->module_name = enif_alloc(module_len + 1);
    if (cont->module_name == NULL) {
        enif_release_resource(cont);
        return NULL;
    }
    memcpy(cont->module_name, module_str, module_len);
    cont->module_name[module_len] = '\0';
    cont->module_len = module_len;

    /* Copy func name */
    Py_ssize_t func_len;
    const char *func_str = PyUnicode_AsUTF8AndSize(marker->func, &func_len);
    if (func_str == NULL) {
        enif_release_resource(cont);
        return NULL;
    }
    cont->func_name = enif_alloc(func_len + 1);
    if (cont->func_name == NULL) {
        enif_release_resource(cont);
        return NULL;
    }
    memcpy(cont->func_name, func_str, func_len);
    cont->func_name[func_len] = '\0';
    cont->func_len = func_len;

    /* INCREF args and kwargs */
    if (marker->args != Py_None) {
        Py_INCREF(marker->args);
        cont->args = marker->args;
    } else {
        cont->args = NULL;
    }
    if (marker->kwargs != Py_None) {
        Py_INCREF(marker->kwargs);
        cont->kwargs = marker->kwargs;
    } else {
        cont->kwargs = NULL;
    }

    /* Store captured globals and locals */
    if (marker->globals != NULL) {
        Py_INCREF(marker->globals);
        cont->globals = marker->globals;
    } else {
        cont->globals = NULL;
    }
    if (marker->locals != NULL) {
        Py_INCREF(marker->locals);
        cont->locals = marker->locals;
    } else {
        cont->locals = NULL;
    }

    /* Store context (keep resource reference) */
    cont->ctx = ctx;
    enif_keep_resource(ctx);

    /* Store local_env if provided */
    if (local_env != NULL) {
        cont->local_env = local_env;
        enif_keep_resource(local_env);
    }

    cont->depth = depth;
    cont->interp_id = ctx->interp_id;

    return cont;
}

/**
 * @brief NIF: Execute inline continuation
 *
 * This is the continuation function called by enif_schedule_nif().
 * It executes the Python function and handles the result:
 * - InlineScheduleMarker: chain via another enif_schedule_nif
 * - ScheduleMarker: return {schedule, ...} to Erlang
 * - Suspension: return {suspended, ...} to Erlang
 * - Normal result: return {ok, Result}
 */
static ERL_NIF_TERM nif_inline_continuation(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    inline_continuation_t *cont;
    if (!enif_get_resource(env, argv[0], INLINE_CONTINUATION_RESOURCE_TYPE, (void **)&cont)) {
        return make_error(env, "invalid_continuation");
    }

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    /* Check depth limit */
    if (cont->depth >= MAX_INLINE_CONTINUATION_DEPTH) {
        return make_error(env, "inline_continuation_depth_exceeded");
    }

    py_context_t *ctx = cont->ctx;
    if (ctx == NULL || ctx->destroyed) {
        return make_error(env, "context_destroyed");
    }

    /* Acquire thread state */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        return make_error(env, "acquire_failed");
    }

    /* Set thread-local context for callback support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;

    /* Enable suspension for callback support */
    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    /* Set callback env for consume_time_slice */
    ErlNifEnv *prev_callback_env = tl_callback_env;
    tl_callback_env = env;

    ERL_NIF_TERM result;

    /* Import module and get function */
    PyObject *func = NULL;
    PyObject *module = NULL;

    /* Priority for __main__ lookups:
     * 1. Captured globals/locals from the marker (caller's frame)
     * 2. local_env globals (process-local environment)
     * 3. ctx->globals/locals (context defaults)
     */
    py_env_resource_t *local_env = (py_env_resource_t *)cont->local_env;

    if (strcmp(cont->module_name, "__main__") == 0) {
        /* Try captured globals first (from caller's frame) */
        if (cont->globals != NULL) {
            func = PyDict_GetItemString(cont->globals, cont->func_name);
        }
        /* Try captured locals */
        if (func == NULL && cont->locals != NULL) {
            func = PyDict_GetItemString(cont->locals, cont->func_name);
        }
        /* Fallback to local_env globals */
        if (func == NULL && local_env != NULL) {
            func = PyDict_GetItemString(local_env->globals, cont->func_name);
        }
        /* Fallback to context globals/locals */
        if (func == NULL) {
            func = PyDict_GetItemString(ctx->globals, cont->func_name);
        }
        if (func == NULL) {
            func = PyDict_GetItemString(ctx->locals, cont->func_name);
        }
        if (func != NULL) {
            Py_INCREF(func);
        } else {
            PyErr_Format(PyExc_NameError, "name '%s' is not defined", cont->func_name);
        }
    } else {
        module = PyImport_ImportModule(cont->module_name);
        if (module != NULL) {
            func = PyObject_GetAttrString(module, cont->func_name);
            Py_DECREF(module);
        }
    }

    if (func == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Build args tuple */
    PyObject *args = cont->args;
    if (args == NULL) {
        args = PyTuple_New(0);
        if (args == NULL) {
            Py_DECREF(func);
            result = make_py_error(env);
            goto cleanup;
        }
    } else {
        Py_INCREF(args);
    }

    /* Get kwargs */
    PyObject *kwargs = cont->kwargs;

    /* Call the function */
    PyObject *py_result = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);

    if (py_result == NULL) {
        /* Check for pending callback */
        if (tl_pending_callback) {
            PyErr_Clear();

            /* Create suspended context state for callback handling */
            ErlNifBinary module_bin, func_bin;
            enif_alloc_binary(cont->module_len, &module_bin);
            memcpy(module_bin.data, cont->module_name, cont->module_len);
            enif_alloc_binary(cont->func_len, &func_bin);
            memcpy(func_bin.data, cont->func_name, cont->func_len);

            /* Convert args to Erlang term for replay */
            ERL_NIF_TERM args_term = enif_make_list(env, 0);
            if (cont->args != NULL) {
                args_term = py_to_term(env, cont->args);
            }

            ERL_NIF_TERM kwargs_term = enif_make_new_map(env);
            if (cont->kwargs != NULL) {
                kwargs_term = py_to_term(env, cont->kwargs);
            }

            suspended_context_state_t *suspended = create_suspended_context_state_for_call(
                env, ctx, &module_bin, &func_bin, args_term, kwargs_term);

            enif_release_binary(&module_bin);
            enif_release_binary(&func_bin);

            if (suspended == NULL) {
                tl_pending_callback = false;
                Py_CLEAR(tl_pending_args);
                result = make_error(env, "create_suspended_state_failed");
            } else {
                result = build_suspended_context_result(env, suspended);
            }
        } else {
            result = make_py_error(env);
        }
    } else if (is_inline_schedule_marker(py_result)) {
        /* Chain via another enif_schedule_nif */
        inline_continuation_t *next_cont = create_inline_continuation(
            ctx, cont->local_env, py_result, cont->depth + 1);
        Py_DECREF(py_result);

        if (next_cont == NULL) {
            result = make_error(env, "create_continuation_failed");
        } else {
            ERL_NIF_TERM cont_ref = enif_make_resource(env, next_cont);
            enif_release_resource(next_cont);

            /* Restore thread-local state before scheduling */
            tl_allow_suspension = prev_allow_suspension;
            tl_current_context = prev_context;
            tl_callback_env = prev_callback_env;
            clear_pending_callback_tls();

            py_context_release(&guard);

            return enif_schedule_nif(env, "inline_continuation",
                ERL_NIF_DIRTY_JOB_IO_BOUND, nif_inline_continuation, 1, &cont_ref);
        }
    } else if (is_schedule_marker(py_result)) {
        /* Switch to schedule_py path */
        ScheduleMarkerObject *marker = (ScheduleMarkerObject *)py_result;
        ERL_NIF_TERM callback_name = py_to_term(env, marker->callback_name);
        ERL_NIF_TERM callback_args = py_to_term(env, marker->args);
        Py_DECREF(py_result);
        result = enif_make_tuple3(env, ATOM_SCHEDULE, callback_name, callback_args);
    } else {
        /* Normal result */
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;
    tl_callback_env = prev_callback_env;

    /* Clear pending callback TLS */
    clear_pending_callback_tls();

    /* Release thread state */
    py_context_release(&guard);

    return result;
}

/* ============================================================================
 * Initialization
 * ============================================================================ */

static ERL_NIF_TERM nif_py_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Try to transition UNINIT -> INITING (only one thread wins) */
    if (!runtime_transition(PY_STATE_UNINIT, PY_STATE_INITING)) {
        /* Check if already running (idempotent success) */
        if (runtime_is_running()) {
            return ATOM_OK;
        }
        /* Also allow reinit from STOPPED state */
        if (!runtime_transition(PY_STATE_STOPPED, PY_STATE_INITING)) {
            /* Another thread is initializing or shutting down */
            return make_error(env, "init_in_progress");
        }
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

    /* Initialize Python with thread support.
     * If Python is already initialized (e.g., after app restart without
     * calling Py_Finalize), skip initialization to avoid corruption. */
    if (!Py_IsInitialized()) {
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
            atomic_store(&g_runtime_state, PY_STATE_STOPPED);
            return make_error(env, "python_init_failed");
        }
    }

    /* Create the 'erlang' module for callbacks */
    if (create_erlang_module() < 0) {
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
        return make_error(env, "erlang_module_creation_failed");
    }

    /* Create the 'py_event_loop' module for asyncio integration */
    if (create_py_event_loop_module() < 0) {
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
        return make_error(env, "event_loop_module_creation_failed");
    }

    /* Initialize ReactorBuffer Python type for zero-copy read handling */
    if (ReactorBuffer_init_type() < 0) {
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
        return make_error(env, "reactor_buffer_init_failed");
    }

    /* Register ReactorBuffer type with erlang module for testing access */
    if (ReactorBuffer_register_with_reactor() < 0) {
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
        return make_error(env, "reactor_buffer_register_failed");
    }

    /* Initialize PyBuffer Python type for zero-copy input */
    if (PyBuffer_init_type() < 0) {
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
        return make_error(env, "py_buffer_init_failed");
    }

    /* Register PyBuffer type with erlang module */
    if (PyBuffer_register_with_module() < 0) {
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
        return make_error(env, "py_buffer_register_failed");
    }

    /* Create a default event loop so Python asyncio always has one available */
    if (create_default_event_loop(env) < 0) {
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
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

    /* Start single executor for coordinator operations.
     * Context operations use per-context worker threads (see worker_context_init).
     * The single executor handles legacy worker API and coordinator tasks. */
    int executor_result = 0;
    if (g_execution_mode != PY_MODE_FREE_THREADED) {
        executor_result = executor_start();
    }

    if (executor_result < 0) {
        PyEval_RestoreThread(g_main_thread_state);
        g_main_thread_state = NULL;
        Py_Finalize();
        atomic_store(&g_runtime_state, PY_STATE_STOPPED);
        return make_error(env, "executor_start_failed");
    }

    /* Initialize thread worker system for ThreadPoolExecutor support */
    if (thread_worker_init() < 0) {
        /* Non-fatal - thread worker support just won't be available */
    }

    /* Transition to RUNNING - initialization complete */
    atomic_store(&g_runtime_state, PY_STATE_RUNNING);

    return ATOM_OK;
}

static ERL_NIF_TERM nif_finalize(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    /* Try to transition RUNNING -> SHUTTING_DOWN (only one thread wins) */
    if (!runtime_transition(PY_STATE_RUNNING, PY_STATE_SHUTTING_DOWN)) {
        /* Check current state - if already shutdown, return success */
        py_runtime_state_t state = runtime_state();
        if (state == PY_STATE_STOPPED || state == PY_STATE_UNINIT) {
            return ATOM_OK;
        }
        /* Another thread is shutting down - let it finish */
        if (state == PY_STATE_SHUTTING_DOWN) {
            return ATOM_OK;
        }
        /* If still initializing, can't finalize yet */
        return make_error(env, "python_not_running");
    }

    /*
     * SHUTDOWN SEQUENCE - ORDER MATTERS:
     * 1. Stop executors first (they finish in-flight work, join threads)
     * 2. Clean up thread worker system
     * 3. Then clean up caches with GIL (no active work at this point)
     */

    /* Step 1: Stop executor - it will finish in-flight requests and exit */
    if (g_execution_mode != PY_MODE_FREE_THREADED) {
        executor_stop();
    }

    /* Step 2: Clean up thread worker system */
    thread_worker_cleanup();

    /* Step 3: Clean up caches with GIL - no executor threads are running now.
     *
     * IMPORTANT: After subinterpreter operations, PyGILState_Ensure may not
     * work correctly on this thread. Use PyEval_RestoreThread with the saved
     * main thread state instead if available. */
    if (g_main_thread_state != NULL) {
        PyEval_RestoreThread(g_main_thread_state);

        /* Clean up numpy type cache */
        Py_XDECREF(g_numpy_ndarray_type);
        g_numpy_ndarray_type = NULL;

        g_main_thread_state = PyEval_SaveThread();
    } else {
        /* Fallback to PyGILState if no main thread state saved */
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_XDECREF(g_numpy_ndarray_type);
        g_numpy_ndarray_type = NULL;
        PyGILState_Release(gstate);
    }

    /* Restore main thread state before marking as stopped */
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

    /* Transition to STOPPED - shutdown complete */
    atomic_store(&g_runtime_state, PY_STATE_STOPPED);

    return ATOM_OK;
}

/* ============================================================================
 * Worker management
 * ============================================================================ */

static ERL_NIF_TERM nif_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
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
    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
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

    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
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

    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
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

    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
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

    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
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

    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
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

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_MEMORY_STATS;
    req.env = env;

    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Get invariant counters for debugging and leak detection.
 * Returns a map with counter names as keys and values as integers.
 */
static ERL_NIF_TERM nif_get_debug_counters(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    ERL_NIF_TERM keys[14];
    ERL_NIF_TERM vals[14];
    int i = 0;

    /* GIL operations */
    keys[i] = enif_make_atom(env, "gil_ensure");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.gil_ensure_count));
    keys[i] = enif_make_atom(env, "gil_release");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.gil_release_count));

    /* Python objects */
    keys[i] = enif_make_atom(env, "pyobj_created");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.pyobj_created));
    keys[i] = enif_make_atom(env, "pyobj_destroyed");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.pyobj_destroyed));

    /* py_ref_t */
    keys[i] = enif_make_atom(env, "pyref_created");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.pyref_created));
    keys[i] = enif_make_atom(env, "pyref_destroyed");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.pyref_destroyed));

    /* Contexts */
    keys[i] = enif_make_atom(env, "ctx_created");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.ctx_created));
    keys[i] = enif_make_atom(env, "ctx_destroyed");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.ctx_destroyed));

    /* Suspended states */
    keys[i] = enif_make_atom(env, "suspended_created");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.suspended_created));
    keys[i] = enif_make_atom(env, "suspended_destroyed");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.suspended_destroyed));

    /* Executor operations */
    keys[i] = enif_make_atom(env, "enqueue_count");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.enqueue_count));
    keys[i] = enif_make_atom(env, "complete_count");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.complete_count));
    keys[i] = enif_make_atom(env, "rejected_count");
    vals[i++] = enif_make_uint64(env, atomic_load(&g_counters.rejected_count));

    ERL_NIF_TERM result;
    enif_make_map_from_arrays(env, keys, vals, i, &result);
    return result;
}

static ERL_NIF_TERM nif_gc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_GC;
    req.env = env;
    req.gc_generation = 2;  /* Full collection by default */
    if (argc > 0) {
        enif_get_int(env, argv[0], &req.gc_generation);
    }

    if (executor_enqueue(&req) != 0) {
        request_cleanup(&req);
        return make_error(env, "runtime_shutting_down");
    }
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

static ERL_NIF_TERM nif_tracemalloc_start(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
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

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
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

    const char *mode_str = (g_execution_mode == PY_MODE_FREE_THREADED)
                           ? "free_threaded"
                           : "gil";
    return enif_make_atom(env, mode_str);
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
 * Async worker NIFs (deprecated - replaced by event loop model)
 *
 * These NIFs are deprecated and return errors. Use py_event_loop_pool and
 * py_event_loop:run_async/2 instead.
 * ============================================================================ */

static ERL_NIF_TERM nif_async_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "async_workers_deprecated_use_event_loop");
}

static ERL_NIF_TERM nif_async_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return ATOM_OK;
}

static ERL_NIF_TERM nif_async_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "async_workers_deprecated_use_event_loop");
}

static ERL_NIF_TERM nif_async_gather(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "async_workers_deprecated_use_event_loop");
}

static ERL_NIF_TERM nif_async_stream(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return make_error(env, "async_workers_deprecated_use_event_loop");
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

/**
 * @brief Check if OWN_GIL mode is supported (Python 3.14+)
 *
 * OWN_GIL requires Python 3.14+ due to C extension global state bugs
 * in earlier versions (e.g., _decimal). See gh-106078.
 */
static ERL_NIF_TERM nif_owngil_supported(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

#ifdef HAVE_OWNGIL
    return ATOM_TRUE;
#else
    return ATOM_FALSE;
#endif
}


/* ============================================================================
 * Shared-GIL Pool Model for Subinterpreters
 *
 * Subinterpreters share the GIL but provide namespace isolation. Execution
 * happens on dirty schedulers using PyThreadState_Swap() to switch to the
 * subinterpreter's thread state from the pool.
 * ============================================================================ */

/* Forward declaration - defined later in this file */
static PyObject *context_get_module(py_context_t *ctx, const char *module_name);

/* Old thread-per-context functions removed - now using shared-GIL pool model */

/* ============================================================================
 * OWN_GIL Context Support
 *
 * OWN_GIL contexts create a dedicated pthread with its own Python subinterpreter
 * that has an independent GIL. This enables true parallel Python execution.
 *
 * Architecture:
 *   - Each OWN_GIL context gets its own pthread at creation time
 *   - The pthread creates an OWN_GIL subinterpreter and runs a request loop
 *   - Dirty schedulers dispatch requests via condition variables
 *   - Terms are passed via enif_make_copy() (zero serialization overhead)
 * ============================================================================ */

/* ============================================================================
 * Context Request Queue Operations
 *
 * These functions manage the request queue for worker/owngil contexts.
 * They replace the single-slot pattern that had race conditions.
 * Available for all Python versions to support worker thread mode.
 * ============================================================================ */

/**
 * @brief Enqueue a request to a context's request queue
 *
 * Thread-safe. Adds request to tail of queue and signals worker.
 * Caller must have already set refcount to 2 (caller + queue).
 *
 * @param ctx The context
 * @param req The request (refcount should be 2)
 */
static void ctx_queue_enqueue(py_context_t *ctx, ctx_request_t *req) {
    pthread_mutex_lock(&ctx->queue_mutex);

    req->next = NULL;
    if (ctx->queue_tail == NULL) {
        ctx->queue_head = req;
        ctx->queue_tail = req;
    } else {
        ctx->queue_tail->next = req;
        ctx->queue_tail = req;
    }

    pthread_cond_signal(&ctx->queue_not_empty);
    pthread_mutex_unlock(&ctx->queue_mutex);
}

/**
 * @brief Dequeue a request from a context's request queue
 *
 * Blocks until a request is available or shutdown is requested.
 * Returns NULL if shutdown requested and queue is empty.
 *
 * @param ctx The context
 * @return The dequeued request, or NULL on shutdown
 */
static ctx_request_t *ctx_queue_dequeue(py_context_t *ctx) {
    pthread_mutex_lock(&ctx->queue_mutex);

    while (ctx->queue_head == NULL && !atomic_load(&ctx->shutdown_requested)) {
        pthread_cond_wait(&ctx->queue_not_empty, &ctx->queue_mutex);
    }

    ctx_request_t *req = ctx->queue_head;
    if (req != NULL) {
        ctx->queue_head = req->next;
        if (ctx->queue_head == NULL) {
            ctx->queue_tail = NULL;
        }
        req->next = NULL;
    }

    pthread_mutex_unlock(&ctx->queue_mutex);
    return req;
}

/**
 * @brief Cancel all pending requests in a context's queue
 *
 * Called during context destruction. Sets cancelled flag on all
 * pending requests and signals their condition variables.
 *
 * @param ctx The context
 */
static void ctx_queue_cancel_all(py_context_t *ctx) {
    pthread_mutex_lock(&ctx->queue_mutex);

    ctx_request_t *req = ctx->queue_head;
    while (req != NULL) {
        ctx_request_t *next = req->next;
        atomic_store(&req->cancelled, true);

        /* Signal waiters that request is done (cancelled) */
        pthread_mutex_lock(&req->mutex);
        atomic_store(&req->completed, true);
        pthread_cond_signal(&req->cond);
        pthread_mutex_unlock(&req->mutex);

        /* Release queue's reference */
        ctx_request_release(req);
        req = next;
    }

    ctx->queue_head = NULL;
    ctx->queue_tail = NULL;

    pthread_mutex_unlock(&ctx->queue_mutex);
}

/* ============================================================================
 * OWN_GIL execute helpers
 *
 * Each OWN_GIL worker thread dequeues a ctx_request_t and copies the request
 * fields onto the owning context (ctx->shared_env, ctx->request_term, etc.)
 * before calling these helpers. Helpers consume those fields and write the
 * response back into ctx->response_term / ctx->response_ok.
 * ============================================================================ */

/**
 * @brief Execute a call request in the OWN_GIL thread
 */
static void owngil_execute_call(py_context_t *ctx) {
    /* Decode request from shared_env */
    ERL_NIF_TERM module_term, func_term, args_term, kwargs_term;
    const ERL_NIF_TERM *tuple_terms;
    int tuple_arity;

    if (!enif_get_tuple(ctx->shared_env, ctx->request_term, &tuple_arity, &tuple_terms) ||
        tuple_arity < 4) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_request"));
        ctx->response_ok = false;
        return;
    }

    module_term = tuple_terms[0];
    func_term = tuple_terms[1];
    args_term = tuple_terms[2];
    kwargs_term = tuple_terms[3];

    ErlNifBinary module_bin, func_bin;
    if (!enif_inspect_binary(ctx->shared_env, module_term, &module_bin) ||
        !enif_inspect_binary(ctx->shared_env, func_term, &func_bin)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_module_or_func"));
        ctx->response_ok = false;
        return;
    }

    char *module_name = binary_to_string(&module_bin);
    char *func_name_str = binary_to_string(&func_bin);

    if (module_name == NULL || func_name_str == NULL) {
        enif_free(module_name);
        enif_free(func_name_str);
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "alloc_failed"));
        ctx->response_ok = false;
        return;
    }

    PyObject *module = NULL;
    PyObject *func = NULL;

    /* Special handling for __main__ module - check ctx->globals first */
    if (strcmp(module_name, "__main__") == 0) {
        func = PyDict_GetItemString(ctx->globals, func_name_str);  /* Borrowed ref */
        if (func != NULL) {
            Py_INCREF(func);
        }
    }

    if (func == NULL) {
        /* Get or import module */
        module = context_get_module(ctx, module_name);
        if (module == NULL) {
            ctx->response_term = make_py_error(ctx->shared_env);
            ctx->response_ok = false;
            enif_free(module_name);
            enif_free(func_name_str);
            return;
        }

        /* Get function */
        func = PyObject_GetAttrString(module, func_name_str);
        if (func == NULL) {
            ctx->response_term = make_py_error(ctx->shared_env);
            ctx->response_ok = false;
            enif_free(module_name);
            enif_free(func_name_str);
            return;
        }
    }

    enif_free(module_name);
    enif_free(func_name_str);

    /* Convert args */
    unsigned int args_len;
    if (!enif_get_list_length(ctx->shared_env, args_term, &args_len)) {
        Py_DECREF(func);
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_args"));
        ctx->response_ok = false;
        return;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = args_term;
    for (unsigned int i = 0; i < args_len; i++) {
        enif_get_list_cell(ctx->shared_env, tail, &head, &tail);
        PyObject *arg = term_to_py(ctx->shared_env, head);
        if (arg == NULL) {
            Py_DECREF(args);
            Py_DECREF(func);
            ctx->response_term = enif_make_tuple2(ctx->shared_env,
                enif_make_atom(ctx->shared_env, "error"),
                enif_make_atom(ctx->shared_env, "arg_conversion_failed"));
            ctx->response_ok = false;
            return;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }

    /* Convert kwargs */
    PyObject *kwargs = NULL;
    if (enif_is_map(ctx->shared_env, kwargs_term)) {
        kwargs = term_to_py(ctx->shared_env, kwargs_term);
    }

    /* Call the function */
    PyObject *py_result = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (py_result == NULL) {
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
    } else {
        ERL_NIF_TERM term_result = py_to_term(ctx->shared_env, py_result);
        Py_DECREF(py_result);
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "ok"), term_result);
        ctx->response_ok = true;
    }
}

/**
 * @brief Execute an eval request in the OWN_GIL thread
 */
static void owngil_execute_eval(py_context_t *ctx) {
    /* Decode request: {Code, Locals} */
    const ERL_NIF_TERM *tuple_terms;
    int tuple_arity;

    if (!enif_get_tuple(ctx->shared_env, ctx->request_term, &tuple_arity, &tuple_terms) ||
        tuple_arity < 2) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_request"));
        ctx->response_ok = false;
        return;
    }

    ErlNifBinary code_bin;
    if (!enif_inspect_binary(ctx->shared_env, tuple_terms[0], &code_bin)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_code"));
        ctx->response_ok = false;
        return;
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "alloc_failed"));
        ctx->response_ok = false;
        return;
    }

    /* Merge locals into context's locals */
    if (enif_is_map(ctx->shared_env, tuple_terms[1])) {
        PyObject *locals_map = term_to_py(ctx->shared_env, tuple_terms[1]);
        if (locals_map != NULL && PyDict_Check(locals_map)) {
            PyDict_Merge(ctx->locals, locals_map, 1);
            Py_DECREF(locals_map);
        }
    }

    /* Compile and evaluate */
    PyObject *compiled = Py_CompileString(code, "<eval>", Py_eval_input);
    enif_free(code);

    if (compiled == NULL) {
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
        return;
    }

    PyObject *py_result = PyEval_EvalCode(compiled, ctx->globals, ctx->locals);
    Py_DECREF(compiled);

    if (py_result == NULL) {
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
    } else {
        ERL_NIF_TERM term_result = py_to_term(ctx->shared_env, py_result);
        Py_DECREF(py_result);
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "ok"), term_result);
        ctx->response_ok = true;
    }
}

/**
 * @brief Execute an exec request in the OWN_GIL thread
 */
static void owngil_execute_exec(py_context_t *ctx) {
    ErlNifBinary code_bin;
    if (!enif_inspect_binary(ctx->shared_env, ctx->request_term, &code_bin)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_code"));
        ctx->response_ok = false;
        return;
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "alloc_failed"));
        ctx->response_ok = false;
        return;
    }

    /* Compile and execute */
    PyObject *compiled = Py_CompileString(code, "<exec>", Py_file_input);
    enif_free(code);

    if (compiled == NULL) {
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
        return;
    }

    /* Use globals for both globals and locals to simulate module-level execution.
     * This ensures imports are accessible from subsequent code. */
    PyObject *py_result = PyEval_EvalCode(compiled, ctx->globals, ctx->globals);
    Py_DECREF(compiled);

    if (py_result == NULL) {
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
    } else {
        Py_DECREF(py_result);
        ctx->response_term = enif_make_atom(ctx->shared_env, "ok");
        ctx->response_ok = true;
    }
}

/**
 * @brief Execute a reactor on_read_ready request in OWN_GIL thread
 */
static void owngil_execute_reactor_read(py_context_t *ctx) {
    /* Extract fd from request term (it's just an integer) */
    int fd;
    if (!enif_get_int(ctx->shared_env, ctx->request_term, &fd)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_fd"));
        ctx->response_ok = false;
        return;
    }

    /* Get buffer from auxiliary pointer */
    void *buffer_ptr = ctx->reactor_buffer_ptr;
    ctx->reactor_buffer_ptr = NULL;  /* Transfer ownership */

    if (buffer_ptr == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "no_buffer"));
        ctx->response_ok = false;
        return;
    }

    /* Call the OWN_GIL reactor function */
    ctx->response_term = owngil_reactor_on_read_ready(ctx->shared_env, fd, buffer_ptr);
    ctx->response_ok = true;
}

/**
 * @brief Execute a reactor on_write_ready request in OWN_GIL thread
 */
static void owngil_execute_reactor_write(py_context_t *ctx) {
    /* Extract fd from request term */
    int fd;
    if (!enif_get_int(ctx->shared_env, ctx->request_term, &fd)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_fd"));
        ctx->response_ok = false;
        return;
    }

    /* Call the OWN_GIL reactor function */
    ctx->response_term = owngil_reactor_on_write_ready(ctx->shared_env, fd);
    ctx->response_ok = true;
}

/**
 * @brief Execute a reactor init_connection request in OWN_GIL thread
 */
static void owngil_execute_reactor_init(py_context_t *ctx) {
    /* Extract {Fd, ClientInfo} from request term */
    const ERL_NIF_TERM *tuple;
    int arity;
    if (!enif_get_tuple(ctx->shared_env, ctx->request_term, &arity, &tuple) || arity != 2) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_request"));
        ctx->response_ok = false;
        return;
    }

    int fd;
    if (!enif_get_int(ctx->shared_env, tuple[0], &fd)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_fd"));
        ctx->response_ok = false;
        return;
    }

    /* Call the OWN_GIL reactor function */
    ctx->response_term = owngil_reactor_init_connection(ctx->shared_env, fd, tuple[1]);
    ctx->response_ok = true;
}

/**
 * @brief Execute an exec request with process-local env in the OWN_GIL thread
 *
 * Uses penv->globals/locals instead of ctx->globals/locals
 */
static void owngil_execute_exec_with_env(py_context_t *ctx) {
    py_env_resource_t *penv = (py_env_resource_t *)ctx->local_env_ptr;
    ctx->local_env_ptr = NULL;  /* Clear after use */

    if (penv == NULL || penv->globals == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_env"));
        ctx->response_ok = false;
        return;
    }

    /* Verify interpreter ownership - prevent dangling pointer access.
     * Compare env's interp_id with the current Python interpreter's ID. */
    PyInterpreterState *current_interp = PyInterpreterState_Get();
    if (current_interp != NULL && penv->interp_id != PyInterpreterState_GetID(current_interp)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "env_wrong_interpreter"));
        ctx->response_ok = false;
        return;
    }

    ErlNifBinary code_bin;
    if (!enif_inspect_binary(ctx->shared_env, ctx->request_term, &code_bin)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_code"));
        ctx->response_ok = false;
        return;
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "alloc_failed"));
        ctx->response_ok = false;
        return;
    }

    /* Set thread-local env for callback support */
    py_env_resource_t *prev_local_env = tl_current_local_env;
    tl_current_local_env = penv;

    /* Compile and execute using process-local environment */
    PyObject *compiled = Py_CompileString(code, "<exec>", Py_file_input);
    enif_free(code);

    if (compiled == NULL) {
        tl_current_local_env = prev_local_env;
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
        return;
    }

    /* Use penv->globals for both to simulate module-level execution */
    PyObject *py_result = PyEval_EvalCode(compiled, penv->globals, penv->globals);
    Py_DECREF(compiled);

    tl_current_local_env = prev_local_env;

    if (py_result == NULL) {
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
    } else {
        Py_DECREF(py_result);
        ctx->response_term = enif_make_atom(ctx->shared_env, "ok");
        ctx->response_ok = true;
    }
}

/**
 * @brief Execute an eval request with process-local env in the OWN_GIL thread
 *
 * Uses penv->globals/locals instead of ctx->globals/locals
 */
static void owngil_execute_eval_with_env(py_context_t *ctx) {
    py_env_resource_t *penv = (py_env_resource_t *)ctx->local_env_ptr;
    ctx->local_env_ptr = NULL;  /* Clear after use */

    if (penv == NULL || penv->globals == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_env"));
        ctx->response_ok = false;
        return;
    }

    /* Verify interpreter ownership - prevent dangling pointer access.
     * Compare env's interp_id with the current Python interpreter's ID. */
    PyInterpreterState *current_interp = PyInterpreterState_Get();
    if (current_interp != NULL && penv->interp_id != PyInterpreterState_GetID(current_interp)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "env_wrong_interpreter"));
        ctx->response_ok = false;
        return;
    }

    /* Decode request: {Code, Locals} */
    const ERL_NIF_TERM *tuple_terms;
    int tuple_arity;

    if (!enif_get_tuple(ctx->shared_env, ctx->request_term, &tuple_arity, &tuple_terms) ||
        tuple_arity < 2) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_request"));
        ctx->response_ok = false;
        return;
    }

    ErlNifBinary code_bin;
    if (!enif_inspect_binary(ctx->shared_env, tuple_terms[0], &code_bin)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_code"));
        ctx->response_ok = false;
        return;
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "alloc_failed"));
        ctx->response_ok = false;
        return;
    }

    /* Set thread-local state for callback/suspension support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;
    py_env_resource_t *prev_local_env = tl_current_local_env;
    tl_current_local_env = penv;
    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    /* Build eval_locals from penv->globals + any passed locals */
    PyObject *eval_locals = PyDict_Copy(penv->globals);
    if (enif_is_map(ctx->shared_env, tuple_terms[1])) {
        PyObject *locals_map = term_to_py(ctx->shared_env, tuple_terms[1]);
        if (locals_map != NULL && PyDict_Check(locals_map)) {
            PyDict_Merge(eval_locals, locals_map, 1);
            Py_DECREF(locals_map);
        }
    }

    /* Compile and evaluate using process-local globals */
    PyObject *compiled = Py_CompileString(code, "<eval>", Py_eval_input);
    enif_free(code);

    if (compiled == NULL) {
        Py_DECREF(eval_locals);
        tl_allow_suspension = prev_allow_suspension;
        tl_current_context = prev_context;
        tl_current_local_env = prev_local_env;
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
        return;
    }

    PyObject *py_result = PyEval_EvalCode(compiled, penv->globals, eval_locals);
    Py_DECREF(compiled);
    Py_DECREF(eval_locals);

    if (py_result == NULL) {
        /* Check for pending callback (suspension) */
        if (tl_pending_callback) {
            PyErr_Clear();
            /* Create suspended state for callback handling */
            suspended_context_state_t *suspended = create_suspended_context_state_for_eval(
                ctx->shared_env, ctx, &code_bin, tuple_terms[1]);
            if (suspended == NULL) {
                tl_pending_callback = false;
                Py_CLEAR(tl_pending_args);
                ctx->response_term = enif_make_tuple2(ctx->shared_env,
                    enif_make_atom(ctx->shared_env, "error"),
                    enif_make_atom(ctx->shared_env, "create_suspended_state_failed"));
                ctx->response_ok = false;
            } else {
                ctx->response_term = build_suspended_context_result(ctx->shared_env, suspended);
                ctx->response_ok = true;  /* Suspended is a valid response */
            }
        } else {
            ctx->response_term = make_py_error(ctx->shared_env);
            ctx->response_ok = false;
        }
    } else if (is_inline_schedule_marker(py_result)) {
        /* Inline schedule marker: execute continuation directly in worker thread.
         * Loop until we get a final result or a suspension. */
        int depth = 0;
        while (is_inline_schedule_marker(py_result) && depth < MAX_INLINE_CONTINUATION_DEPTH) {
            inline_continuation_t *cont = create_inline_continuation(ctx, penv, py_result, depth);
            Py_DECREF(py_result);
            py_result = NULL;

            if (cont == NULL) {
                ctx->response_term = enif_make_tuple2(ctx->shared_env,
                    enif_make_atom(ctx->shared_env, "error"),
                    enif_make_atom(ctx->shared_env, "create_continuation_failed"));
                ctx->response_ok = false;
                goto cleanup;
            }

            /* Execute the continuation function */
            PyObject *func = NULL;
            PyObject *module = NULL;

            if (strcmp(cont->module_name, "__main__") == 0) {
                /* Try captured globals first */
                if (cont->globals != NULL) {
                    func = PyDict_GetItemString(cont->globals, cont->func_name);
                }
                if (func == NULL && cont->locals != NULL) {
                    func = PyDict_GetItemString(cont->locals, cont->func_name);
                }
                if (func == NULL && penv != NULL) {
                    func = PyDict_GetItemString(penv->globals, cont->func_name);
                }
                if (func == NULL && ctx->globals != NULL) {
                    func = PyDict_GetItemString(ctx->globals, cont->func_name);
                }
                if (func != NULL) {
                    Py_INCREF(func);
                } else {
                    PyErr_Format(PyExc_NameError, "name '%s' is not defined", cont->func_name);
                }
            } else {
                module = PyImport_ImportModule(cont->module_name);
                if (module != NULL) {
                    func = PyObject_GetAttrString(module, cont->func_name);
                    Py_DECREF(module);
                }
            }

            if (func == NULL) {
                enif_release_resource(cont);
                ctx->response_term = make_py_error(ctx->shared_env);
                ctx->response_ok = false;
                goto cleanup;
            }

            /* Build args and call */
            PyObject *args = cont->args ? cont->args : PyTuple_New(0);
            if (args == NULL) {
                Py_DECREF(func);
                enif_release_resource(cont);
                ctx->response_term = make_py_error(ctx->shared_env);
                ctx->response_ok = false;
                goto cleanup;
            }
            if (cont->args) Py_INCREF(args);

            py_result = PyObject_Call(func, args, cont->kwargs);
            Py_DECREF(func);
            Py_DECREF(args);
            enif_release_resource(cont);
            depth++;
        }

        if (depth >= MAX_INLINE_CONTINUATION_DEPTH) {
            Py_XDECREF(py_result);
            ctx->response_term = enif_make_tuple2(ctx->shared_env,
                enif_make_atom(ctx->shared_env, "error"),
                enif_make_atom(ctx->shared_env, "inline_continuation_depth_exceeded"));
            ctx->response_ok = false;
            goto cleanup;
        }

        /* Handle final result (or error/suspension from continuation) */
        if (py_result == NULL) {
            if (tl_pending_callback) {
                PyErr_Clear();
                suspended_context_state_t *suspended = create_suspended_context_state_for_eval(
                    ctx->shared_env, ctx, &code_bin, tuple_terms[1]);
                if (suspended == NULL) {
                    tl_pending_callback = false;
                    Py_CLEAR(tl_pending_args);
                    ctx->response_term = enif_make_tuple2(ctx->shared_env,
                        enif_make_atom(ctx->shared_env, "error"),
                        enif_make_atom(ctx->shared_env, "create_suspended_state_failed"));
                    ctx->response_ok = false;
                } else {
                    ctx->response_term = build_suspended_context_result(ctx->shared_env, suspended);
                    ctx->response_ok = true;
                }
            } else {
                ctx->response_term = make_py_error(ctx->shared_env);
                ctx->response_ok = false;
            }
        } else if (is_schedule_marker(py_result)) {
            ScheduleMarkerObject *marker = (ScheduleMarkerObject *)py_result;
            ERL_NIF_TERM callback_name = py_to_term(ctx->shared_env, marker->callback_name);
            ERL_NIF_TERM callback_args = py_to_term(ctx->shared_env, marker->args);
            Py_DECREF(py_result);
            ctx->response_term = enif_make_tuple3(ctx->shared_env,
                enif_make_atom(ctx->shared_env, "schedule"),
                callback_name, callback_args);
            ctx->response_ok = true;
        } else {
            ERL_NIF_TERM term_result = py_to_term(ctx->shared_env, py_result);
            Py_DECREF(py_result);
            ctx->response_term = enif_make_tuple2(ctx->shared_env,
                enif_make_atom(ctx->shared_env, "ok"), term_result);
            ctx->response_ok = true;
        }
        goto cleanup;
    } else if (is_schedule_marker(py_result)) {
        /* Schedule marker: return {schedule, callback_name, args} */
        ScheduleMarkerObject *marker = (ScheduleMarkerObject *)py_result;
        ERL_NIF_TERM callback_name = py_to_term(ctx->shared_env, marker->callback_name);
        ERL_NIF_TERM callback_args = py_to_term(ctx->shared_env, marker->args);
        Py_DECREF(py_result);
        ctx->response_term = enif_make_tuple3(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "schedule"),
            callback_name, callback_args);
        ctx->response_ok = true;
    } else {
        ERL_NIF_TERM term_result = py_to_term(ctx->shared_env, py_result);
        Py_DECREF(py_result);
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "ok"), term_result);
        ctx->response_ok = true;
    }

cleanup:
    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;
    tl_current_local_env = prev_local_env;
    clear_pending_callback_tls();
}

/**
 * @brief Execute a call request with process-local env in the OWN_GIL thread
 *
 * Uses penv->globals for function lookup in __main__ module
 */
static void owngil_execute_call_with_env(py_context_t *ctx) {
    py_env_resource_t *penv = (py_env_resource_t *)ctx->local_env_ptr;
    ctx->local_env_ptr = NULL;  /* Clear after use */

    if (penv == NULL || penv->globals == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_env"));
        ctx->response_ok = false;
        return;
    }

    /* Verify interpreter ownership - prevent dangling pointer access.
     * Compare env's interp_id with the current Python interpreter's ID. */
    PyInterpreterState *current_interp = PyInterpreterState_Get();
    if (current_interp != NULL && penv->interp_id != PyInterpreterState_GetID(current_interp)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "env_wrong_interpreter"));
        ctx->response_ok = false;
        return;
    }

    /* Decode request from shared_env: {Module, Func, Args, Kwargs} */
    ERL_NIF_TERM module_term, func_term, args_term, kwargs_term;
    const ERL_NIF_TERM *tuple_terms;
    int tuple_arity;

    if (!enif_get_tuple(ctx->shared_env, ctx->request_term, &tuple_arity, &tuple_terms) ||
        tuple_arity < 4) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_request"));
        ctx->response_ok = false;
        return;
    }

    module_term = tuple_terms[0];
    func_term = tuple_terms[1];
    args_term = tuple_terms[2];
    kwargs_term = tuple_terms[3];

    ErlNifBinary module_bin, func_bin;
    if (!enif_inspect_binary(ctx->shared_env, module_term, &module_bin) ||
        !enif_inspect_binary(ctx->shared_env, func_term, &func_bin)) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_module_or_func"));
        ctx->response_ok = false;
        return;
    }

    char *module_name = binary_to_string(&module_bin);
    char *func_name_str = binary_to_string(&func_bin);

    if (module_name == NULL || func_name_str == NULL) {
        enif_free(module_name);
        enif_free(func_name_str);
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "alloc_failed"));
        ctx->response_ok = false;
        return;
    }

    /* Set thread-local env for callback support */
    py_env_resource_t *prev_local_env = tl_current_local_env;
    tl_current_local_env = penv;

    PyObject *func = NULL;

    /* Special handling for __main__ module - look up in process-local globals */
    if (strcmp(module_name, "__main__") == 0) {
        func = PyDict_GetItemString(penv->globals, func_name_str);  /* Borrowed ref */
        if (func != NULL) {
            Py_INCREF(func);
        }
    }

    if (func == NULL) {
        /* Get or import module from context cache */
        PyObject *module = context_get_module(ctx, module_name);
        if (module == NULL) {
            enif_free(module_name);
            enif_free(func_name_str);
            tl_current_local_env = prev_local_env;
            ctx->response_term = make_py_error(ctx->shared_env);
            ctx->response_ok = false;
            return;
        }

        /* Get function */
        func = PyObject_GetAttrString(module, func_name_str);
        if (func == NULL) {
            enif_free(module_name);
            enif_free(func_name_str);
            tl_current_local_env = prev_local_env;
            ctx->response_term = make_py_error(ctx->shared_env);
            ctx->response_ok = false;
            return;
        }
    }

    enif_free(module_name);
    enif_free(func_name_str);

    /* Convert args */
    unsigned int args_len;
    if (!enif_get_list_length(ctx->shared_env, args_term, &args_len)) {
        Py_DECREF(func);
        tl_current_local_env = prev_local_env;
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_args"));
        ctx->response_ok = false;
        return;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = args_term;
    for (unsigned int i = 0; i < args_len; i++) {
        enif_get_list_cell(ctx->shared_env, tail, &head, &tail);
        PyObject *arg = term_to_py(ctx->shared_env, head);
        if (arg == NULL) {
            Py_DECREF(args);
            Py_DECREF(func);
            tl_current_local_env = prev_local_env;
            ctx->response_term = enif_make_tuple2(ctx->shared_env,
                enif_make_atom(ctx->shared_env, "error"),
                enif_make_atom(ctx->shared_env, "arg_conversion_failed"));
            ctx->response_ok = false;
            return;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }

    /* Convert kwargs */
    PyObject *kwargs = NULL;
    if (enif_is_map(ctx->shared_env, kwargs_term)) {
        kwargs = term_to_py(ctx->shared_env, kwargs_term);
    }

    /* Call the function */
    PyObject *py_result = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    tl_current_local_env = prev_local_env;

    if (py_result == NULL) {
        ctx->response_term = make_py_error(ctx->shared_env);
        ctx->response_ok = false;
    } else {
        ERL_NIF_TERM term_result = py_to_term(ctx->shared_env, py_result);
        Py_DECREF(py_result);
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "ok"), term_result);
        ctx->response_ok = true;
    }
}

/**
 * @brief Create process-local env dicts in the OWN_GIL thread
 *
 * Creates globals/locals dicts in the correct interpreter context.
 * The py_env_resource_t is passed via local_env_ptr.
 */
static void owngil_execute_create_local_env(py_context_t *ctx) {
    py_env_resource_t *res = (py_env_resource_t *)ctx->local_env_ptr;
    ctx->local_env_ptr = NULL;  /* Clear after use */

    if (res == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "invalid_env_resource"));
        ctx->response_ok = false;
        return;
    }

    /* Store interpreter info for destructor */
    PyInterpreterState *interp = PyInterpreterState_Get();
    if (interp != NULL) {
        res->interp_id = PyInterpreterState_GetID(interp);
    }

    /* Copy globals from context to inherit preloaded code */
    res->globals = PyDict_Copy(ctx->globals);
    if (res->globals == NULL) {
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "globals_copy_failed"));
        ctx->response_ok = false;
        return;
    }

    /* Ensure __builtins__ is present */
    if (PyDict_GetItemString(res->globals, "__builtins__") == NULL) {
        PyObject *builtins = PyEval_GetBuiltins();
        if (builtins != NULL) {
            PyDict_SetItemString(res->globals, "__builtins__", builtins);
        }
    }

    /* Ensure __name__ = '__main__' is set */
    if (PyDict_GetItemString(res->globals, "__name__") == NULL) {
        PyObject *main_name = PyUnicode_FromString("__main__");
        if (main_name != NULL) {
            PyDict_SetItemString(res->globals, "__name__", main_name);
            Py_DECREF(main_name);
        }
    }

    /* Ensure erlang module is available */
    if (PyDict_GetItemString(res->globals, "erlang") == NULL) {
        PyObject *erlang = PyImport_ImportModule("erlang");
        if (erlang != NULL) {
            PyDict_SetItemString(res->globals, "erlang", erlang);
            Py_DECREF(erlang);
        }
    }

    /* Use the same dict for locals (module-level execution) */
    res->locals = res->globals;
    Py_INCREF(res->locals);

    ctx->response_term = enif_make_atom(ctx->shared_env, "ok");
    ctx->response_ok = true;
}

/**
 * @brief Execute apply_imports in OWN_GIL context
 *
 * Applies a list of imports to the interpreter's sys.modules.
 * The imports list is passed via request_term.
 *
 * Note: OWN_GIL contexts have their own dedicated interpreter,
 * so sys.modules is per-context in this mode.
 */
static void owngil_execute_apply_imports(py_context_t *ctx) {
    /* Process each import from request_term */
    ERL_NIF_TERM head, tail = ctx->request_term;
    int arity;
    const ERL_NIF_TERM *tuple;

    while (enif_get_list_cell(ctx->shared_env, tail, &head, &tail)) {
        if (!enif_get_tuple(ctx->shared_env, head, &arity, &tuple) || arity != 2) {
            continue;
        }

        ErlNifBinary module_bin;
        if (!enif_inspect_binary(ctx->shared_env, tuple[0], &module_bin)) {
            continue;
        }

        /* Convert to C string */
        char *module_name = enif_alloc(module_bin.size + 1);
        if (module_name == NULL) continue;
        memcpy(module_name, module_bin.data, module_bin.size);
        module_name[module_bin.size] = '\0';

        /* Skip __main__ */
        if (strcmp(module_name, "__main__") == 0) {
            enif_free(module_name);
            continue;
        }

        /* Import the module - caches in this interpreter's sys.modules */
        PyObject *mod = PyImport_ImportModule(module_name);
        if (mod != NULL) {
            Py_DECREF(mod);  /* sys.modules holds the reference */
        } else {
            /* Clear error - import failure is not fatal */
            PyErr_Clear();
        }

        enif_free(module_name);
    }

    ctx->response_term = enif_make_atom(ctx->shared_env, "ok");
    ctx->response_ok = true;
}

/**
 * @brief Apply paths to sys.path in OWN_GIL context
 *
 * Paths are inserted at the beginning of sys.path.
 */
static void owngil_execute_apply_paths(py_context_t *ctx) {
    /* Get sys.path */
    PyObject *sys_module = PyImport_ImportModule("sys");
    if (sys_module == NULL) {
        PyErr_Clear();
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "sys_import_failed"));
        ctx->response_ok = false;
        return;
    }

    PyObject *sys_path = PyObject_GetAttrString(sys_module, "path");
    Py_DECREF(sys_module);
    if (sys_path == NULL || !PyList_Check(sys_path)) {
        Py_XDECREF(sys_path);
        PyErr_Clear();
        ctx->response_term = enif_make_tuple2(ctx->shared_env,
            enif_make_atom(ctx->shared_env, "error"),
            enif_make_atom(ctx->shared_env, "sys_path_not_list"));
        ctx->response_ok = false;
        return;
    }

    /* Count paths first */
    ERL_NIF_TERM head, tail = ctx->request_term;
    int path_count = 0;
    while (enif_get_list_cell(ctx->shared_env, tail, &head, &tail)) {
        path_count++;
    }

    /* Insert in reverse order so first path ends up first */
    for (int i = 0; i < path_count; i++) {
        /* Skip to the i-th element from the end */
        ERL_NIF_TERM current = ctx->request_term;
        for (int j = 0; j < path_count - 1 - i; j++) {
            enif_get_list_cell(ctx->shared_env, current, &head, &current);
        }
        enif_get_list_cell(ctx->shared_env, current, &head, &current);

        ErlNifBinary path_bin;
        if (!enif_inspect_binary(ctx->shared_env, head, &path_bin)) {
            continue;
        }

        /* Convert to Python string */
        PyObject *path_str = PyUnicode_FromStringAndSize((char *)path_bin.data, path_bin.size);
        if (path_str == NULL) {
            PyErr_Clear();
            continue;
        }

        /* Check if already in sys.path */
        int already_present = PySequence_Contains(sys_path, path_str);
        if (already_present <= 0) {
            /* Insert at position 0 */
            PyList_Insert(sys_path, 0, path_str);
        }
        Py_DECREF(path_str);
    }

    Py_DECREF(sys_path);
    ctx->response_term = enif_make_atom(ctx->shared_env, "ok");
    ctx->response_ok = true;
}

/**
 * @brief Execute a request based on its type
 */
static void owngil_execute_request(py_context_t *ctx) {
    switch (ctx->request_type) {
        case CTX_REQ_CALL:
            owngil_execute_call(ctx);
            break;
        case CTX_REQ_EVAL:
            owngil_execute_eval(ctx);
            break;
        case CTX_REQ_EXEC:
            owngil_execute_exec(ctx);
            break;
        case CTX_REQ_REACTOR_ON_READ_READY:
            owngil_execute_reactor_read(ctx);
            break;
        case CTX_REQ_REACTOR_ON_WRITE_READY:
            owngil_execute_reactor_write(ctx);
            break;
        case CTX_REQ_REACTOR_INIT_CONNECTION:
            owngil_execute_reactor_init(ctx);
            break;
        case CTX_REQ_EXEC_WITH_ENV:
            owngil_execute_exec_with_env(ctx);
            break;
        case CTX_REQ_EVAL_WITH_ENV:
            owngil_execute_eval_with_env(ctx);
            break;
        case CTX_REQ_CALL_WITH_ENV:
            owngil_execute_call_with_env(ctx);
            break;
        case CTX_REQ_CREATE_LOCAL_ENV:
            owngil_execute_create_local_env(ctx);
            break;
        case CTX_REQ_APPLY_IMPORTS:
            owngil_execute_apply_imports(ctx);
            break;
        case CTX_REQ_APPLY_PATHS:
            owngil_execute_apply_paths(ctx);
            break;
        default:
            ctx->response_term = enif_make_tuple2(ctx->shared_env,
                enif_make_atom(ctx->shared_env, "error"),
                enif_make_atom(ctx->shared_env, "unknown_request_type"));
            ctx->response_ok = false;
            break;
    }
}

/* ============================================================================
 * Worker Thread Implementation (main interpreter, all Python versions)
 *
 * Worker mode uses a dedicated pthread that acquires the GIL for each request.
 * This provides stable thread affinity for numpy/torch/tensorflow without
 * requiring subinterpreter support.
 * ============================================================================ */

/**
 * @brief Main loop for worker context thread (main interpreter mode)
 *
 * This function runs in a dedicated pthread. It processes requests from the
 * request queue, acquiring the GIL for each request using PyGILState_Ensure.
 *
 * Unlike owngil mode, worker mode uses the main interpreter and shares the GIL
 * with other Python threads. The benefit is stable thread affinity and
 * compatibility with all Python extensions.
 */
static void *worker_context_thread_main(void *arg) {
    py_context_t *ctx = (py_context_t *)arg;

    /* Create namespace dictionaries on the worker thread under GIL */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Create namespace dictionaries if not already created */
    if (ctx->globals == NULL) {
        ctx->globals = PyDict_New();
        ctx->locals = PyDict_New();
        ctx->module_cache = PyDict_New();

        if (ctx->globals == NULL || ctx->locals == NULL || ctx->module_cache == NULL) {
            PyGILState_Release(gstate);
            atomic_store(&ctx->init_error, true);
            atomic_store(&ctx->worker_running, false);
            return NULL;
        }

        /* Import __builtins__ into globals */
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(ctx->globals, "__builtins__", builtins);

        /* Import erlang module into globals */
        PyObject *erlang_module = PyImport_ImportModule("erlang");
        if (erlang_module != NULL) {
            PyDict_SetItemString(ctx->globals, "erlang", erlang_module);
            Py_DECREF(erlang_module);
        } else {
            log_and_clear_python_error("worker erlang module import");
        }
    }

    PyGILState_Release(gstate);

    /* Signal that we're ready */
    atomic_store(&ctx->worker_running, true);

    /* Main request loop - uses queue instead of single-slot */
    while (!atomic_load(&ctx->shutdown_requested)) {
        /* Dequeue next request (blocks until available or shutdown) */
        ctx_request_t *req = ctx_queue_dequeue(ctx);

        if (req == NULL) {
            /* Queue empty and shutdown requested */
            break;
        }

        if (req->type == CTX_REQ_SHUTDOWN) {
            /* Shutdown sentinel - signal completion and exit */
            pthread_mutex_lock(&req->mutex);
            atomic_store(&req->completed, true);
            pthread_cond_signal(&req->cond);
            pthread_mutex_unlock(&req->mutex);
            ctx_request_release(req);
            break;
        }

        /* Check if request was cancelled while queued */
        if (atomic_load(&req->cancelled)) {
            /* Request cancelled - deliver error without processing */
            if (req->async_mode) {
                /* Async mode: send cancellation message */
                enif_clear_env(ctx->msg_env);
                ERL_NIF_TERM cancel_msg = enif_make_tuple3(ctx->msg_env,
                    enif_make_atom(ctx->msg_env, "py_result"),
                    enif_make_copy(ctx->msg_env, req->request_id),
                    enif_make_tuple2(ctx->msg_env,
                        enif_make_atom(ctx->msg_env, "error"),
                        enif_make_atom(ctx->msg_env, "cancelled")));
                enif_send(NULL, &req->caller_pid, ctx->msg_env, cancel_msg);
            } else {
                /* Blocking mode: signal condvar */
                req->result_env = enif_alloc_env();
                if (req->result_env) {
                    req->result = enif_make_tuple2(req->result_env,
                        enif_make_atom(req->result_env, "error"),
                        enif_make_atom(req->result_env, "cancelled"));
                }
                req->success = false;

                pthread_mutex_lock(&req->mutex);
                atomic_store(&req->completed, true);
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
            }

            ctx_request_release(req);
            continue;
        }

        /* Populate legacy compatibility fields from request */
        ctx->shared_env = req->request_env;
        ctx->request_type = req->type;
        ctx->request_term = req->request_data;
        ctx->reactor_buffer_ptr = req->reactor_buffer_ptr;
        ctx->local_env_ptr = req->local_env_ptr;
        ctx->response_ok = false;
        ctx->response_term = 0;

        /* Acquire GIL and process the request */
        gstate = PyGILState_Ensure();
        owngil_execute_request(ctx);  /* Reuse execute functions */
        PyGILState_Release(gstate);

        /* Copy response to request struct */
        req->result_env = enif_alloc_env();
        if (req->result_env && ctx->response_term != 0) {
            req->result = enif_make_copy(req->result_env, ctx->response_term);
        } else if (req->result_env) {
            req->result = enif_make_tuple2(req->result_env,
                enif_make_atom(req->result_env, "error"),
                enif_make_atom(req->result_env, "no_response"));
        }
        req->success = ctx->response_ok;

        /* Clear legacy fields */
        ctx->shared_env = NULL;
        ctx->request_type = CTX_REQ_NONE;
        ctx->request_term = 0;
        ctx->reactor_buffer_ptr = NULL;
        ctx->local_env_ptr = NULL;

        /* Deliver result - async or blocking */
        if (req->async_mode) {
            /* Async mode: send result message to caller */
            enif_clear_env(ctx->msg_env);
            ERL_NIF_TERM result_msg = enif_make_tuple3(ctx->msg_env,
                enif_make_atom(ctx->msg_env, "py_result"),
                enif_make_copy(ctx->msg_env, req->request_id),
                req->result_env ? enif_make_copy(ctx->msg_env, req->result)
                    : enif_make_tuple2(ctx->msg_env,
                        enif_make_atom(ctx->msg_env, "error"),
                        enif_make_atom(ctx->msg_env, "no_result")));
            enif_send(NULL, &req->caller_pid, ctx->msg_env, result_msg);
        } else {
            /* Blocking mode: signal condvar */
            pthread_mutex_lock(&req->mutex);
            atomic_store(&req->completed, true);
            pthread_cond_signal(&req->cond);
            pthread_mutex_unlock(&req->mutex);
        }

        /* Release queue's reference to request */
        ctx_request_release(req);
    }

    /* Cleanup: release namespace dictionaries under GIL */
    gstate = PyGILState_Ensure();
    Py_XDECREF(ctx->module_cache);
    Py_XDECREF(ctx->globals);
    Py_XDECREF(ctx->locals);
    ctx->globals = NULL;
    ctx->locals = NULL;
    ctx->module_cache = NULL;
    PyGILState_Release(gstate);

    atomic_store(&ctx->worker_running, false);
    return NULL;
}

/**
 * @brief Initialize worker thread mode for a context
 *
 * @param ctx Context to initialize
 * @return 0 on success, -1 on failure
 */
static int worker_context_init(py_context_t *ctx) {
    ctx->uses_worker_thread = true;

    /* Initialize worker thread state */
    atomic_store(&ctx->worker_running, false);
    atomic_store(&ctx->shutdown_requested, false);
    atomic_store(&ctx->leaked, false);

    /* Initialize request queue */
    ctx->queue_head = NULL;
    ctx->queue_tail = NULL;

    /* Initialize legacy compatibility fields */
    ctx->shared_env = NULL;
    ctx->request_type = CTX_REQ_NONE;
    ctx->request_term = 0;
    ctx->response_term = 0;
    ctx->response_ok = false;
    ctx->local_env_ptr = NULL;
    ctx->reactor_buffer_ptr = NULL;

    /* Initialize queue mutex */
    if (pthread_mutex_init(&ctx->queue_mutex, NULL) != 0) {
        return -1;
    }

    /* Initialize queue condition variable */
    if (pthread_cond_init(&ctx->queue_not_empty, NULL) != 0) {
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    /* Create message environment for async responses */
    ctx->msg_env = enif_alloc_env();
    if (ctx->msg_env == NULL) {
        pthread_cond_destroy(&ctx->queue_not_empty);
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    /* Globals/locals will be created by the worker thread */
    ctx->globals = NULL;
    ctx->locals = NULL;
    ctx->module_cache = NULL;

    /* Start the worker thread */
    if (pthread_create(&ctx->worker_thread, NULL, worker_context_thread_main, ctx) != 0) {
        enif_free_env(ctx->msg_env);
        ctx->msg_env = NULL;
        pthread_cond_destroy(&ctx->queue_not_empty);
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    /* Wait for thread to initialize or fail */
    int wait_count = 0;
    while (!atomic_load(&ctx->worker_running) &&
           !atomic_load(&ctx->init_error) &&
           wait_count < 2000) {
        usleep(1000);  /* 1ms */
        wait_count++;
    }

    if (atomic_load(&ctx->init_error) || !atomic_load(&ctx->worker_running)) {
        /* Thread failed to start */
        pthread_join(ctx->worker_thread, NULL);
        if (ctx->msg_env != NULL) {
            enif_free_env(ctx->msg_env);
            ctx->msg_env = NULL;
        }
        pthread_cond_destroy(&ctx->queue_not_empty);
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    return 0;
}

/**
 * @brief Shutdown worker thread mode and clean up resources
 *
 * Uses the join-or-leak pattern: if the worker thread doesn't respond
 * within the timeout, we mark the context as leaked and do NOT free
 * shared resources to avoid use-after-free.
 *
 * @param ctx Context to shutdown
 */
#define WORKER_SHUTDOWN_TIMEOUT_SECS 30

static void worker_context_shutdown(py_context_t *ctx) {
    if (!ctx->uses_worker_thread) {
        return;
    }

    /* Signal shutdown and wake any worker parked on the condvar.
     *
     * We deliberately don't enqueue a CTX_REQ_SHUTDOWN sentinel:
     *   - the worker loop predicate already exits once
     *     shutdown_requested is true, so a broadcast is sufficient;
     *   - if the worker is mid-process_request when we set the flag,
     *     it returns to the top of the loop, sees !shutdown_requested
     *     == false, and exits without dequeuing — leaving any
     *     sentinel as an orphan ctx_request_t in the queue.
     * Broadcasting under the mutex avoids the lost-wakeup race.
     */
    atomic_store(&ctx->shutdown_requested, true);
    ctx_queue_cancel_all(ctx);
    pthread_mutex_lock(&ctx->queue_mutex);
    pthread_cond_broadcast(&ctx->queue_not_empty);
    pthread_mutex_unlock(&ctx->queue_mutex);

    /* Wait for thread to exit with timeout */
    bool join_succeeded = false;

#if defined(__linux__)
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WORKER_SHUTDOWN_TIMEOUT_SECS;
    int rc = pthread_timedjoin_np(ctx->worker_thread, NULL, &deadline);
    join_succeeded = (rc == 0);
#else
    /* macOS/other: poll worker_running flag with timeout */
    int wait_ms = 0;
    while (atomic_load(&ctx->worker_running) &&
           wait_ms < WORKER_SHUTDOWN_TIMEOUT_SECS * 1000) {
        usleep(100000);  /* 100ms */
        wait_ms += 100;
    }
    if (!atomic_load(&ctx->worker_running)) {
        pthread_join(ctx->worker_thread, NULL);
        join_succeeded = true;
    }
#endif

    if (!join_succeeded) {
        /* Worker thread is unresponsive - leak the context so the
         * stuck pthread doesn't UAF when the BEAM frees the
         * resource. Pin the resource: enif_keep_resource pushes the
         * refcount above zero permanently, so context_destructor
         * never runs and the BEAM keeps the memory alive for the
         * thread that still holds a raw pointer to it.
         *
         * The leaked thread also keeps using ctx->callback_pipe[]
         * (see nif_context_destroy: pipe close is gated on
         * !ctx->leaked for the same reason). Future cleanup happens
         * at VM exit. */
        fprintf(stderr, "Worker thread shutdown timeout after %d seconds, leaking context\n",
                WORKER_SHUTDOWN_TIMEOUT_SECS);
        atomic_store(&ctx->leaked, true);
        enif_keep_resource(ctx);
        return;
    }

    /* Clean shutdown succeeded - safe to free resources */
    if (ctx->msg_env != NULL) {
        enif_free_env(ctx->msg_env);
        ctx->msg_env = NULL;
    }

    pthread_cond_destroy(&ctx->queue_not_empty);
    pthread_mutex_destroy(&ctx->queue_mutex);

    ctx->uses_worker_thread = false;
}

/**
 * @brief Dispatch a request to the worker thread and wait for response
 *
 * Uses the queue-based pattern: creates a request, enqueues it, waits for
 * completion, and copies the result back to the caller's environment.
 *
 * @param env Caller's NIF environment
 * @param ctx Context with worker thread
 * @param req_type Request type (CTX_REQ_CALL, CTX_REQ_EVAL, CTX_REQ_EXEC, etc.)
 * @param request_data Request data term
 * @return Result term copied back to caller's env
 */
#define WORKER_DISPATCH_TIMEOUT_SECS 30

/**
 * @brief Dispatch a request to the worker thread with optional local environment
 *
 * @param env NIF environment
 * @param ctx Context to dispatch to
 * @param req_type Request type
 * @param request_data Request data term
 * @param local_env Optional local environment (NULL for default)
 * @return Result term
 */
static ERL_NIF_TERM dispatch_to_worker_thread_impl(
    ErlNifEnv *env,
    py_context_t *ctx,
    ctx_request_type_t req_type,
    ERL_NIF_TERM request_data,
    void *local_env
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = req_type;
    req->request_data = enif_make_copy(req->request_env, request_data);
    req->local_env_ptr = local_env;

    /* Add extra reference for queue (caller holds 1, queue holds 1) */
    ctx_request_addref(req);
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WORKER_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            /* Timeout - mark as cancelled and return error */
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);
            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's environment */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    /* Release caller's reference */
    ctx_request_release(req);

    return result;
}

/**
 * @brief Convenience wrapper for dispatch without local environment
 */
static ERL_NIF_TERM dispatch_to_worker_thread(
    ErlNifEnv *env,
    py_context_t *ctx,
    ctx_request_type_t req_type,
    ERL_NIF_TERM request_data
) {
    return dispatch_to_worker_thread_impl(env, ctx, req_type, request_data, NULL);
}

/**
 * @brief Async dispatch to worker thread (non-blocking)
 *
 * Enqueues the request and returns immediately. The worker thread will
 * send a {py_result, RequestId, Result} message to the caller when done.
 *
 * @param env NIF environment
 * @param ctx Context
 * @param req_type Request type
 * @param request_data Request data term
 * @param caller_pid Caller's PID for result delivery
 * @param request_id Request ID for correlation
 * @param local_env Optional local environment (NULL for default)
 * @return {enqueued, RequestId} on success, {error, Reason} on failure
 */
static ERL_NIF_TERM dispatch_to_worker_thread_async(
    ErlNifEnv *env,
    py_context_t *ctx,
    ctx_request_type_t req_type,
    ERL_NIF_TERM request_data,
    ErlNifPid caller_pid,
    ERL_NIF_TERM request_id,
    void *local_env
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = req_type;
    req->request_data = enif_make_copy(req->request_env, request_data);
    req->local_env_ptr = local_env;

    /* Set async mode */
    req->async_mode = true;
    req->caller_pid = caller_pid;
    req->request_id = enif_make_copy(req->request_env, request_id);

    /* Add to queue (queue owns one reference, no caller reference needed) */
    ctx_queue_enqueue(ctx, req);

    /* Return immediately - no blocking! */
    return enif_make_tuple2(env,
        enif_make_atom(env, "enqueued"),
        request_id);
}

#ifdef HAVE_SUBINTERPRETERS
/**
 * @brief Main loop for OWN_GIL context thread
 *
 * This function runs in a dedicated pthread. It creates an OWN_GIL subinterpreter,
 * then enters a request loop where it processes requests from the request queue.
 *
 * The queue-based pattern replaces the old single-slot pattern which had race
 * conditions when multiple callers dispatched concurrently.
 */
static void *owngil_context_thread_main(void *arg) {
    py_context_t *ctx = (py_context_t *)arg;

    /* Attach to Python runtime to create the subinterpreter.
     * We need to hold the main GIL while creating the subinterpreter. */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Create OWN_GIL subinterpreter */
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 1,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_OWN_GIL,
    };

    PyStatus status = Py_NewInterpreterFromConfig(&ctx->own_gil_tstate, &config);
    if (PyStatus_IsError(status)) {
        fprintf(stderr, "OWN_GIL: Py_NewInterpreterFromConfig failed: %s\n",
                status.err_msg ? status.err_msg : "unknown error");
        PyGILState_Release(gstate);
        atomic_store(&ctx->init_error, true);
        atomic_store(&ctx->worker_running, false);
        return NULL;
    }

    ctx->own_gil_interp = PyThreadState_GetInterpreter(ctx->own_gil_tstate);

    /* After Py_NewInterpreterFromConfig, we are now in the new interpreter's
     * thread state and hold its GIL. The main interpreter's gstate is no longer
     * relevant for this thread. */

    /* Register erlang module in this subinterpreter */
    if (create_erlang_module() < 0) {
        fprintf(stderr, "OWN_GIL: create_erlang_module failed\n");
        PyErr_Print();
        Py_EndInterpreter(ctx->own_gil_tstate);
        atomic_store(&ctx->init_error, true);
        atomic_store(&ctx->worker_running, false);
        return NULL;
    }

    /* Register py_event_loop module for reactor support */
    if (create_py_event_loop_module() < 0) {
        fprintf(stderr, "OWN_GIL: create_py_event_loop_module failed\n");
        PyErr_Print();
        Py_EndInterpreter(ctx->own_gil_tstate);
        atomic_store(&ctx->init_error, true);
        atomic_store(&ctx->worker_running, false);
        return NULL;
    }

    /* Create namespace dictionaries */
    ctx->globals = PyDict_New();
    ctx->locals = PyDict_New();
    ctx->module_cache = PyDict_New();

    if (ctx->globals == NULL || ctx->locals == NULL || ctx->module_cache == NULL) {
        fprintf(stderr, "OWN_GIL: PyDict_New failed for namespace dicts\n");
        Py_XDECREF(ctx->globals);
        Py_XDECREF(ctx->locals);
        Py_XDECREF(ctx->module_cache);
        Py_EndInterpreter(ctx->own_gil_tstate);
        atomic_store(&ctx->init_error, true);
        atomic_store(&ctx->worker_running, false);
        return NULL;
    }

    /* Import __builtins__ into globals */
    PyObject *builtins = PyEval_GetBuiltins();
    PyDict_SetItemString(ctx->globals, "__builtins__", builtins);

    /* Import erlang module into globals */
    PyObject *erlang_module = PyImport_ImportModule("erlang");
    if (erlang_module != NULL) {
        PyDict_SetItemString(ctx->globals, "erlang", erlang_module);
        Py_DECREF(erlang_module);
    } else {
        /* Non-fatal - basic operations still work, but log for debugging */
        log_and_clear_python_error("OWN_GIL erlang module import");
    }

    /* Release our OWN_GIL (we'll reacquire when processing requests) */
    PyEval_SaveThread();

    /* Signal that we're ready */
    atomic_store(&ctx->worker_running, true);

    /* Main request loop - uses queue instead of single-slot */
    while (!atomic_load(&ctx->shutdown_requested)) {
        /* Dequeue next request (blocks until available or shutdown) */
        ctx_request_t *req = ctx_queue_dequeue(ctx);

        if (req == NULL) {
            /* Queue empty and shutdown requested */
            break;
        }

        if (req->type == CTX_REQ_SHUTDOWN) {
            /* Shutdown sentinel - signal completion and exit */
            pthread_mutex_lock(&req->mutex);
            atomic_store(&req->completed, true);
            pthread_cond_signal(&req->cond);
            pthread_mutex_unlock(&req->mutex);
            ctx_request_release(req);
            break;
        }

        /* Check if request was cancelled while queued */
        if (atomic_load(&req->cancelled)) {
            /* Request cancelled - signal completion without processing */
            req->result_env = enif_alloc_env();
            if (req->result_env) {
                req->result = enif_make_tuple2(req->result_env,
                    enif_make_atom(req->result_env, "error"),
                    enif_make_atom(req->result_env, "cancelled"));
            }
            req->success = false;

            pthread_mutex_lock(&req->mutex);
            atomic_store(&req->completed, true);
            pthread_cond_signal(&req->cond);
            pthread_mutex_unlock(&req->mutex);

            ctx_request_release(req);
            continue;
        }

        /* Populate legacy compatibility fields from request */
        ctx->shared_env = req->request_env;
        ctx->request_type = req->type;
        ctx->request_term = req->request_data;
        ctx->reactor_buffer_ptr = req->reactor_buffer_ptr;
        ctx->local_env_ptr = req->local_env_ptr;
        ctx->response_ok = false;
        ctx->response_term = 0;

        /* Acquire our GIL and process the request */
        PyEval_RestoreThread(ctx->own_gil_tstate);
        owngil_execute_request(ctx);
        PyEval_SaveThread();

        /* Copy response to request struct */
        req->result_env = enif_alloc_env();
        if (req->result_env && ctx->response_term != 0) {
            req->result = enif_make_copy(req->result_env, ctx->response_term);
        } else if (req->result_env) {
            req->result = enif_make_tuple2(req->result_env,
                enif_make_atom(req->result_env, "error"),
                enif_make_atom(req->result_env, "no_response"));
        }
        req->success = ctx->response_ok;

        /* Clear legacy fields */
        ctx->shared_env = NULL;
        ctx->request_type = CTX_REQ_NONE;
        ctx->request_term = 0;
        ctx->reactor_buffer_ptr = NULL;
        ctx->local_env_ptr = NULL;

        /* Signal completion */
        pthread_mutex_lock(&req->mutex);
        atomic_store(&req->completed, true);
        pthread_cond_signal(&req->cond);
        pthread_mutex_unlock(&req->mutex);

        /* Release queue's reference to request */
        ctx_request_release(req);
    }

    /* Cleanup: acquire our OWN_GIL and destroy interpreter */
    PyEval_RestoreThread(ctx->own_gil_tstate);
    Py_XDECREF(ctx->module_cache);
    Py_XDECREF(ctx->globals);
    Py_XDECREF(ctx->locals);
    ctx->globals = NULL;
    ctx->locals = NULL;
    ctx->module_cache = NULL;

    /* End interpreter - this releases our GIL and cleans up */
    Py_EndInterpreter(ctx->own_gil_tstate);
    ctx->own_gil_tstate = NULL;
    ctx->own_gil_interp = NULL;

    /* Don't call PyGILState_Release(gstate) here!
     * After Py_NewInterpreterFromConfig switched us to the OWN_GIL interpreter,
     * the original gstate is no longer valid. Py_EndInterpreter handles cleanup. */

    atomic_store(&ctx->worker_running, false);
    return NULL;
}

/**
 * Timeout for OWN_GIL dispatch in seconds.
 * If worker thread doesn't respond within this time, assume it's dead.
 */
#define OWNGIL_DISPATCH_TIMEOUT_SECS 30

/**
 * @brief Dispatch a request to the worker thread and wait for response
 *
 * Uses the queue-based pattern: creates a request, enqueues it, waits for
 * completion, and copies the result back to the caller's environment.
 *
 * This replaces the old single-slot pattern which had race conditions when
 * multiple callers dispatched concurrently.
 *
 * @param env Caller's NIF environment
 * @param ctx Context with worker thread
 * @param req_type Request type (CTX_REQ_CALL, CTX_REQ_EVAL, CTX_REQ_EXEC, etc.)
 * @param request_data Request data term
 * @return Result term copied back to caller's env
 */
static ERL_NIF_TERM dispatch_to_owngil_thread(
    ErlNifEnv *env,
    py_context_t *ctx,
    ctx_request_type_t req_type,
    ERL_NIF_TERM request_data
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = req_type;
    req->request_data = enif_make_copy(req->request_env, request_data);

    /* Add ref for queue (now refcount = 2: caller + queue) */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            /* Worker thread is unresponsive - mark request as cancelled */
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            /* Don't mark worker as dead - it might still be processing
             * a long-running Python operation. Just fail this request. */
            fprintf(stderr, "OWN_GIL dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);  /* Release caller's ref */
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    /* Release caller's ref */
    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch reactor on_read_ready to OWN_GIL thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
ERL_NIF_TERM dispatch_reactor_read_to_owngil(ErlNifEnv *env, py_context_t *ctx,
                                              int fd, void *buffer_ptr) {
    if (!atomic_load(&ctx->worker_running)) {
        enif_release_resource(buffer_ptr);
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        enif_release_resource(buffer_ptr);
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        enif_release_resource(buffer_ptr);
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = CTX_REQ_REACTOR_ON_READ_READY;
    req->request_data = enif_make_int(req->request_env, fd);
    req->reactor_buffer_ptr = buffer_ptr;  /* Transfer ownership */
    req->reactor_fd = fd;

    /* Add ref for queue (now refcount = 2: caller + queue) */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            /* Request timeout - mark as cancelled but don't release buffer
             * (worker will handle it when it gets to this request) */
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL reactor dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);  /* Release caller's ref */
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    /* Release caller's ref */
    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch reactor on_write_ready to OWN_GIL thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
ERL_NIF_TERM dispatch_reactor_write_to_owngil(ErlNifEnv *env, py_context_t *ctx,
                                               int fd) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = CTX_REQ_REACTOR_ON_WRITE_READY;
    req->request_data = enif_make_int(req->request_env, fd);
    req->reactor_fd = fd;

    /* Add ref for queue (now refcount = 2: caller + queue) */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL reactor write dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch reactor init_connection to OWN_GIL thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
ERL_NIF_TERM dispatch_reactor_init_to_owngil(ErlNifEnv *env, py_context_t *ctx,
                                              int fd, ERL_NIF_TERM client_info) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = CTX_REQ_REACTOR_INIT_CONNECTION;
    ERL_NIF_TERM fd_term = enif_make_int(req->request_env, fd);
    ERL_NIF_TERM info_copy = enif_make_copy(req->request_env, client_info);
    req->request_data = enif_make_tuple2(req->request_env, fd_term, info_copy);
    req->reactor_fd = fd;

    /* Add ref for queue (now refcount = 2: caller + queue) */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL reactor init dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch exec_with_env to OWN_GIL thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
static ERL_NIF_TERM dispatch_exec_with_env_to_owngil(
    ErlNifEnv *env, py_context_t *ctx,
    ERL_NIF_TERM code, py_env_resource_t *penv
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = CTX_REQ_EXEC_WITH_ENV;
    req->request_data = enif_make_copy(req->request_env, code);
    req->local_env_ptr = penv;

    /* Add ref for queue */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL exec_with_env dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch eval_with_env to OWN_GIL thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
static ERL_NIF_TERM dispatch_eval_with_env_to_owngil(
    ErlNifEnv *env, py_context_t *ctx,
    ERL_NIF_TERM code, ERL_NIF_TERM locals,
    py_env_resource_t *penv
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request: {Code, Locals} */
    req->type = CTX_REQ_EVAL_WITH_ENV;
    ERL_NIF_TERM code_copy = enif_make_copy(req->request_env, code);
    ERL_NIF_TERM locals_copy = enif_make_copy(req->request_env, locals);
    req->request_data = enif_make_tuple2(req->request_env, code_copy, locals_copy);
    req->local_env_ptr = penv;

    /* Add ref for queue */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL eval_with_env dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch call_with_env to OWN_GIL thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
static ERL_NIF_TERM dispatch_call_with_env_to_owngil(
    ErlNifEnv *env, py_context_t *ctx,
    ERL_NIF_TERM module, ERL_NIF_TERM func,
    ERL_NIF_TERM args, ERL_NIF_TERM kwargs,
    py_env_resource_t *penv
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request: {Module, Func, Args, Kwargs} */
    req->type = CTX_REQ_CALL_WITH_ENV;
    ERL_NIF_TERM module_copy = enif_make_copy(req->request_env, module);
    ERL_NIF_TERM func_copy = enif_make_copy(req->request_env, func);
    ERL_NIF_TERM args_copy = enif_make_copy(req->request_env, args);
    ERL_NIF_TERM kwargs_copy = enif_make_copy(req->request_env, kwargs);
    req->request_data = enif_make_tuple4(req->request_env,
        module_copy, func_copy, args_copy, kwargs_copy);
    req->local_env_ptr = penv;

    /* Add ref for queue */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL call_with_env dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch create_local_env to OWN_GIL thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
static ERL_NIF_TERM dispatch_create_local_env_to_owngil(
    ErlNifEnv *env, py_context_t *ctx,
    py_env_resource_t *res
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = CTX_REQ_CREATE_LOCAL_ENV;
    req->local_env_ptr = res;

    /* Add ref for queue */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL create_local_env dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch apply_imports to OWN_GIL worker thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
static ERL_NIF_TERM dispatch_apply_imports_to_owngil(
    ErlNifEnv *env, py_context_t *ctx, ERL_NIF_TERM imports_term
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = CTX_REQ_APPLY_IMPORTS;
    req->request_data = enif_make_copy(req->request_env, imports_term);

    /* Add ref for queue */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL apply_imports dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

/**
 * @brief Dispatch apply_paths request to OWN_GIL worker thread
 *
 * Uses queue-based dispatch with per-request synchronization.
 */
static ERL_NIF_TERM dispatch_apply_paths_to_owngil(
    ErlNifEnv *env, py_context_t *ctx, ERL_NIF_TERM paths_term
) {
    if (!atomic_load(&ctx->worker_running)) {
        return make_error(env, "thread_not_running");
    }

    if (atomic_load(&ctx->destroyed)) {
        return make_error(env, "context_destroyed");
    }

    /* Create request struct */
    ctx_request_t *req = ctx_request_create();
    if (req == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Populate request */
    req->type = CTX_REQ_APPLY_PATHS;
    req->request_data = enif_make_copy(req->request_env, paths_term);

    /* Add ref for queue */
    ctx_request_addref(req);

    /* Enqueue the request */
    ctx_queue_enqueue(ctx, req);

    /* Wait for completion with timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_DISPATCH_TIMEOUT_SECS;

    ERL_NIF_TERM result;
    pthread_mutex_lock(&req->mutex);

    while (!atomic_load(&req->completed)) {
        int rc = pthread_cond_timedwait(&req->cond, &req->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            atomic_store(&req->cancelled, true);
            pthread_mutex_unlock(&req->mutex);

            fprintf(stderr, "OWN_GIL apply_paths dispatch timeout after %d seconds\n",
                    OWNGIL_DISPATCH_TIMEOUT_SECS);

            ctx_request_release(req);
            return make_error(env, "worker_timeout");
        }
    }

    pthread_mutex_unlock(&req->mutex);

    /* Copy result to caller's env */
    if (req->result_env != NULL) {
        result = enif_make_copy(env, req->result);
    } else {
        result = make_error(env, "no_result");
    }

    ctx_request_release(req);

    return result;
}

#endif /* HAVE_SUBINTERPRETERS */

/**
 * @brief Initialize OWN_GIL fields in a context and start the worker thread
 *
 * @param ctx Context to initialize
 * @return 0 on success, -1 on failure
 */
#ifdef HAVE_SUBINTERPRETERS
static int owngil_context_init(py_context_t *ctx) {
    ctx->uses_own_gil = true;
    ctx->own_gil_tstate = NULL;
    ctx->own_gil_interp = NULL;

    /* Initialize worker thread state */
    atomic_store(&ctx->worker_running, false);
    atomic_store(&ctx->init_error, false);
    atomic_store(&ctx->shutdown_requested, false);
    atomic_store(&ctx->leaked, false);

    /* Initialize request queue */
    ctx->queue_head = NULL;
    ctx->queue_tail = NULL;

    /* Initialize legacy compatibility fields */
    ctx->shared_env = NULL;
    ctx->request_type = CTX_REQ_NONE;
    ctx->request_term = 0;
    ctx->response_term = 0;
    ctx->response_ok = false;
    ctx->local_env_ptr = NULL;
    ctx->reactor_buffer_ptr = NULL;

    /* Initialize queue mutex */
    if (pthread_mutex_init(&ctx->queue_mutex, NULL) != 0) {
        return -1;
    }

    /* Initialize queue condition variable */
    if (pthread_cond_init(&ctx->queue_not_empty, NULL) != 0) {
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    /* Create message environment for async responses */
    ctx->msg_env = enif_alloc_env();
    if (ctx->msg_env == NULL) {
        pthread_cond_destroy(&ctx->queue_not_empty);
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    /* Start the worker thread */
    if (pthread_create(&ctx->worker_thread, NULL, owngil_context_thread_main, ctx) != 0) {
        enif_free_env(ctx->msg_env);
        ctx->msg_env = NULL;
        pthread_cond_destroy(&ctx->queue_not_empty);
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    /* Wait for thread to initialize or fail */
    int wait_count = 0;
    while (!atomic_load(&ctx->worker_running) &&
           !atomic_load(&ctx->init_error) &&
           wait_count < 2000) {
        usleep(1000);  /* 1ms */
        wait_count++;
    }

    if (atomic_load(&ctx->init_error) || !atomic_load(&ctx->worker_running)) {
        /* Thread failed to start */
        pthread_join(ctx->worker_thread, NULL);
        if (ctx->msg_env != NULL) {
            enif_free_env(ctx->msg_env);
            ctx->msg_env = NULL;
        }
        pthread_cond_destroy(&ctx->queue_not_empty);
        pthread_mutex_destroy(&ctx->queue_mutex);
        return -1;
    }

    return 0;
}

/**
 * @brief Shutdown OWN_GIL context and clean up resources
 *
 * Uses the join-or-leak pattern: if the worker thread doesn't respond
 * within the timeout, we mark the context as leaked and do NOT free
 * shared resources to avoid use-after-free.
 *
 * @param ctx Context to shutdown
 */
#define OWNGIL_SHUTDOWN_TIMEOUT_SECS 30

static void owngil_context_shutdown(py_context_t *ctx) {
    if (!ctx->uses_own_gil) {
        return;
    }

    /* Signal shutdown and wake any worker parked on the condvar.
     * See worker_context_shutdown for why we broadcast instead of
     * enqueuing a CTX_REQ_SHUTDOWN sentinel. */
    atomic_store(&ctx->shutdown_requested, true);
    ctx_queue_cancel_all(ctx);
    pthread_mutex_lock(&ctx->queue_mutex);
    pthread_cond_broadcast(&ctx->queue_not_empty);
    pthread_mutex_unlock(&ctx->queue_mutex);

    /* Wait for thread to exit with timeout */
    bool join_succeeded = false;

#if defined(__linux__)
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += OWNGIL_SHUTDOWN_TIMEOUT_SECS;
    int rc = pthread_timedjoin_np(ctx->worker_thread, NULL, &deadline);
    join_succeeded = (rc == 0);
#else
    /* macOS/other: poll worker_running flag with timeout */
    int wait_ms = 0;
    while (atomic_load(&ctx->worker_running) &&
           wait_ms < OWNGIL_SHUTDOWN_TIMEOUT_SECS * 1000) {
        usleep(100000);  /* 100ms */
        wait_ms += 100;
    }
    if (!atomic_load(&ctx->worker_running)) {
        pthread_join(ctx->worker_thread, NULL);
        join_succeeded = true;
    }
#endif

    if (!join_succeeded) {
        /* Worker thread is unresponsive - leak the context. Pin the
         * resource so the BEAM doesn't free its memory under the
         * stuck pthread (UAF). See worker_context_shutdown for the
         * full rationale. */
        fprintf(stderr, "OWN_GIL shutdown timeout after %d seconds, leaking context\n",
                OWNGIL_SHUTDOWN_TIMEOUT_SECS);
        atomic_store(&ctx->leaked, true);
        enif_keep_resource(ctx);
        return;
    }

    /* Clean shutdown succeeded - safe to free resources */
    if (ctx->msg_env != NULL) {
        enif_free_env(ctx->msg_env);
        ctx->msg_env = NULL;
    }

    pthread_cond_destroy(&ctx->queue_not_empty);
    pthread_mutex_destroy(&ctx->queue_mutex);

    ctx->uses_own_gil = false;
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
 * Mode: worker | owngil
 *
 * For owngil mode: creates a dedicated pthread with an OWN_GIL subinterpreter.
 * This enables true parallel Python execution across contexts.
 * Requires Python 3.14+; returns {error, owngil_requires_python314} otherwise.
 *
 * For worker mode: creates a namespace in the main interpreter, dispatched
 * through the context's dedicated worker pthread.
 */
static ERL_NIF_TERM nif_context_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    /* Parse mode atom — reject anything other than worker | owngil so
     * callers that bypass py_context (e.g. py_reactor_context) get the
     * same strict validation py_context:create_context/1 already enforces. */
    char mode_str[32];
    if (!enif_get_atom(env, argv[0], mode_str, sizeof(mode_str), ERL_NIF_LATIN1)) {
        return make_error(env, "invalid_mode");
    }

    bool use_owngil;
    if (strcmp(mode_str, "worker") == 0) {
        use_owngil = false;
    } else if (strcmp(mode_str, "owngil") == 0) {
        use_owngil = true;
    } else {
        return enif_make_tuple2(
            env, ATOM_ERROR,
            enif_make_tuple2(env, enif_make_atom(env, "invalid_mode"), argv[0]));
    }

    /* Allocate context resource */
    py_context_t *ctx = enif_alloc_resource(PY_CONTEXT_RESOURCE_TYPE, sizeof(py_context_t));
    if (ctx == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Initialize fields */
    ctx->interp_id = atomic_fetch_add(&g_context_id_counter, 1);
    ctx->is_subinterp = use_owngil;
    atomic_store(&ctx->destroyed, false);
    atomic_store(&ctx->leaked, false);
    atomic_store(&ctx->init_error, false);
    ctx->has_callback_handler = false;
    ctx->callback_pipe[0] = -1;
    ctx->callback_pipe[1] = -1;
    ctx->globals = NULL;
    ctx->locals = NULL;
    ctx->module_cache = NULL;
    ctx->uses_worker_thread = false;

    /* Create callback pipe for blocking callback responses */
    if (pipe(ctx->callback_pipe) < 0) {
        enif_release_resource(ctx);
        return make_error(env, "pipe_create_failed");
    }

#ifdef HAVE_SUBINTERPRETERS
    ctx->uses_own_gil = false;

    if (use_owngil) {
        /* OWN_GIL mode: create dedicated pthread with OWN_GIL subinterpreter */
        if (owngil_context_init(ctx) != 0) {
            close(ctx->callback_pipe[0]);
            close(ctx->callback_pipe[1]);
            enif_release_resource(ctx);
            return make_error(env, "owngil_init_failed");
        }

        ERL_NIF_TERM ref = enif_make_resource(env, ctx);
        enif_release_resource(ctx);
        atomic_fetch_add(&g_counters.ctx_created, 1);
        return enif_make_tuple3(env, ATOM_OK, ref, enif_make_uint(env, ctx->interp_id));
    }
#endif

    /* Worker mode: create dedicated pthread with main interpreter
     * This provides stable thread affinity for numpy/torch/tensorflow */
    if (worker_context_init(ctx) != 0) {
        close(ctx->callback_pipe[0]);
        close(ctx->callback_pipe[1]);
        enif_release_resource(ctx);
        return make_error(env, "worker_init_failed");
    }

    ERL_NIF_TERM ref = enif_make_resource(env, ctx);
    enif_release_resource(ctx);

    atomic_fetch_add(&g_counters.ctx_created, 1);
    return enif_make_tuple3(env, ATOM_OK, ref, enif_make_uint(env, ctx->interp_id));
}

/**
 * @brief Destroy a Python context
 *
 * nif_context_destroy(ContextRef) -> ok
 *
 * For owngil mode: shuts down the dedicated OWN_GIL thread.
 * For worker mode: shuts down the dedicated worker thread.
 *
 * Both modes use the join-or-leak pattern for safe shutdown.
 */
static ERL_NIF_TERM nif_context_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    /* Skip if already destroyed */
    if (atomic_load(&ctx->destroyed)) {
        return ATOM_OK;
    }

    /* Mark as destroyed early to prevent new operations */
    atomic_store(&ctx->destroyed, true);

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: shutdown the dedicated thread */
    if (ctx->uses_own_gil) {
        owngil_context_shutdown(ctx);
        /* Close callback pipes only on a clean shutdown. If the
         * worker timed out (ctx->leaked == true) it may still write
         * to / read from these fds; closing them here would let the
         * kernel reissue the fd numbers to unrelated files and
         * silently corrupt them. */
        if (!atomic_load(&ctx->leaked)) {
            if (ctx->callback_pipe[0] >= 0) {
                close(ctx->callback_pipe[0]);
                ctx->callback_pipe[0] = -1;
            }
            if (ctx->callback_pipe[1] >= 0) {
                close(ctx->callback_pipe[1]);
                ctx->callback_pipe[1] = -1;
            }
        }
        atomic_fetch_add(&g_counters.ctx_destroyed, 1);
        return ATOM_OK;
    }
#endif

    /* Worker mode: shutdown the dedicated worker thread */
    if (ctx->uses_worker_thread) {
        worker_context_shutdown(ctx);
        /* Close callback pipes (see OWN_GIL branch for why this is
         * gated on !ctx->leaked). */
        if (!atomic_load(&ctx->leaked)) {
            if (ctx->callback_pipe[0] >= 0) {
                close(ctx->callback_pipe[0]);
                ctx->callback_pipe[0] = -1;
            }
            if (ctx->callback_pipe[1] >= 0) {
                close(ctx->callback_pipe[1]);
                ctx->callback_pipe[1] = -1;
            }
        }
        atomic_fetch_add(&g_counters.ctx_destroyed, 1);
        return ATOM_OK;
    }

    /* Legacy mode (should not reach here with new architecture) */
    if (runtime_is_running()) {
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

    /* Close callback pipes */
    if (ctx->callback_pipe[0] >= 0) {
        close(ctx->callback_pipe[0]);
        ctx->callback_pipe[0] = -1;
    }
    if (ctx->callback_pipe[1] >= 0) {
        close(ctx->callback_pipe[1]);
        ctx->callback_pipe[1] = -1;
    }

    atomic_fetch_add(&g_counters.ctx_destroyed, 1);
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

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to dedicated thread */
    if (ctx->uses_own_gil) {
        /* Build request tuple: {Module, Func, Args, Kwargs} */
        ERL_NIF_TERM kwargs = (argc > 4 && enif_is_map(env, argv[4]))
            ? argv[4] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple4(env,
            argv[1],  /* Module */
            argv[2],  /* Func */
            argv[3],  /* Args */
            kwargs);
        return dispatch_to_owngil_thread(env, ctx, CTX_REQ_CALL, request);
    }
#endif

    /* Worker thread mode: dispatch to dedicated thread */
    if (ctx->uses_worker_thread) {
        /* Build request tuple: {Module, Func, Args, Kwargs} */
        ERL_NIF_TERM kwargs = (argc > 4 && enif_is_map(env, argv[4]))
            ? argv[4] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple4(env,
            argv[1],  /* Module */
            argv[2],  /* Func */
            argv[3],  /* Args */
            kwargs);
        return dispatch_to_worker_thread(env, ctx, CTX_REQ_CALL, request);
    }

    /* Legacy mode: direct execution with py_context_acquire.
     * For subinterpreters, py_context_acquire handles PyThreadState_Swap
     * to switch to the pool slot's interpreter. */
    ErlNifBinary module_bin, func_bin;
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

    /* Acquire thread state using centralized guard (worker mode only) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(module_name);
        enif_free(func_name);
        return make_error(env, "acquire_failed");
    }

    /* Set thread-local context for callback support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;

    /* Enable suspension for callback support */
    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    PyObject *module = NULL;
    PyObject *func = NULL;

    /* Special handling for __main__ module - check ctx->globals first */
    if (strcmp(module_name, "__main__") == 0) {
        func = PyDict_GetItemString(ctx->globals, func_name);  /* Borrowed ref */
        if (func != NULL) {
            Py_INCREF(func);
        }
    }

    if (func == NULL) {
        /* Get or import module */
        module = context_get_module(ctx, module_name);
        if (module == NULL) {
            result = make_py_error(env);
            goto cleanup;
        }

        /* Get function */
        func = PyObject_GetAttrString(module, func_name);
        if (func == NULL) {
            result = make_py_error(env);
            goto cleanup;
        }
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
                Py_CLEAR(tl_pending_args);
                result = make_error(env, "create_suspended_state_failed");
            } else {
                result = build_suspended_context_result(env, suspended);
            }
        } else {
            result = make_py_error(env);
        }
    } else if (is_inline_schedule_marker(py_result)) {
        /* Inline schedule marker: chain via enif_schedule_nif without Erlang messaging */
        inline_continuation_t *cont = create_inline_continuation(ctx, NULL, py_result, 0);
        Py_DECREF(py_result);

        if (cont == NULL) {
            result = make_error(env, "create_continuation_failed");
        } else {
            ERL_NIF_TERM cont_ref = enif_make_resource(env, cont);
            enif_release_resource(cont);

            /* Restore thread-local state before scheduling */
            tl_allow_suspension = prev_allow_suspension;
            tl_current_context = prev_context;
            clear_pending_callback_tls();
            enif_free(module_name);
            enif_free(func_name);
            py_context_release(&guard);

            return enif_schedule_nif(env, "inline_continuation",
                ERL_NIF_DIRTY_JOB_IO_BOUND, nif_inline_continuation, 1, &cont_ref);
        }
    } else if (is_schedule_marker(py_result)) {
        /* Schedule marker: release dirty scheduler, continue via callback */
        ScheduleMarkerObject *marker = (ScheduleMarkerObject *)py_result;
        ERL_NIF_TERM callback_name = py_to_term(env, marker->callback_name);
        ERL_NIF_TERM callback_args = py_to_term(env, marker->args);
        Py_DECREF(py_result);
        result = enif_make_tuple3(env, ATOM_SCHEDULE, callback_name, callback_args);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;

    /* Clear pending callback TLS before releasing context */
    clear_pending_callback_tls();

    enif_free(module_name);
    enif_free(func_name);

    /* Release thread state using centralized guard */
    py_context_release(&guard);

    return result;
}

/**
 * @brief Async call - enqueue and return immediately
 *
 * nif_context_call_async(ContextRef, CallerPid, RequestId, Module, Func, Args, Kwargs)
 *     -> {enqueued, RequestId} | {error, Reason}
 *
 * The worker thread will send {py_result, RequestId, Result} to CallerPid when done.
 */
static ERL_NIF_TERM nif_context_call_async(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_context_t *ctx;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (argc < 6) {
        return make_error(env, "badarg");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    /* Get caller PID */
    ErlNifPid caller_pid;
    if (!enif_get_local_pid(env, argv[1], &caller_pid)) {
        return make_error(env, "invalid_pid");
    }

    /* RequestId is argv[2] - can be any term */
    ERL_NIF_TERM request_id = argv[2];

    /* Worker thread mode: dispatch async */
    if (ctx->uses_worker_thread) {
        /* Build request tuple: {Module, Func, Args, Kwargs} */
        ERL_NIF_TERM kwargs = (argc > 6 && enif_is_map(env, argv[6]))
            ? argv[6] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple4(env,
            argv[3],  /* Module */
            argv[4],  /* Func */
            argv[5],  /* Args */
            kwargs);
        return dispatch_to_worker_thread_async(env, ctx, CTX_REQ_CALL,
            request, caller_pid, request_id, NULL);
    }

    /* Not using worker thread - fall back to blocking call */
    return make_error(env, "async_requires_worker_thread");
}

/**
 * @brief Async eval - enqueue and return immediately
 *
 * nif_context_eval_async(ContextRef, CallerPid, RequestId, Code, Locals)
 *     -> {enqueued, RequestId} | {error, Reason}
 *
 * The worker thread will send {py_result, RequestId, Result} to CallerPid when done.
 */
static ERL_NIF_TERM nif_context_eval_async(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_context_t *ctx;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (argc < 4) {
        return make_error(env, "badarg");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    /* Get caller PID */
    ErlNifPid caller_pid;
    if (!enif_get_local_pid(env, argv[1], &caller_pid)) {
        return make_error(env, "invalid_pid");
    }

    /* RequestId is argv[2] - can be any term */
    ERL_NIF_TERM request_id = argv[2];

    /* Worker thread mode: dispatch async */
    if (ctx->uses_worker_thread) {
        /* Build request tuple: {Code, Locals} */
        ERL_NIF_TERM locals = (argc > 4 && enif_is_map(env, argv[4]))
            ? argv[4] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple2(env, argv[3], locals);
        return dispatch_to_worker_thread_async(env, ctx, CTX_REQ_EVAL,
            request, caller_pid, request_id, NULL);
    }

    /* Not using worker thread - fall back to blocking call */
    return make_error(env, "async_requires_worker_thread");
}

/**
 * @brief Async exec - enqueue and return immediately
 *
 * nif_context_exec_async(ContextRef, CallerPid, RequestId, Code)
 *     -> {enqueued, RequestId} | {error, Reason}
 *
 * The worker thread will send {py_result, RequestId, Result} to CallerPid when done.
 */
static ERL_NIF_TERM nif_context_exec_async(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_context_t *ctx;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (argc < 4) {
        return make_error(env, "badarg");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    /* Get caller PID */
    ErlNifPid caller_pid;
    if (!enif_get_local_pid(env, argv[1], &caller_pid)) {
        return make_error(env, "invalid_pid");
    }

    /* RequestId is argv[2] - can be any term */
    ERL_NIF_TERM request_id = argv[2];

    /* Worker thread mode: dispatch async */
    if (ctx->uses_worker_thread) {
        return dispatch_to_worker_thread_async(env, ctx, CTX_REQ_EXEC,
            argv[3], caller_pid, request_id, NULL);
    }

    /* Not using worker thread - fall back to blocking call */
    return make_error(env, "async_requires_worker_thread");
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

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to dedicated thread */
    if (ctx->uses_own_gil) {
        /* Build request tuple: {Code, Locals} */
        ERL_NIF_TERM locals = (argc > 2 && enif_is_map(env, argv[2]))
            ? argv[2] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple2(env, argv[1], locals);
        return dispatch_to_owngil_thread(env, ctx, CTX_REQ_EVAL, request);
    }
#endif

    /* Worker thread mode: dispatch to dedicated thread */
    if (ctx->uses_worker_thread) {
        /* Build request tuple: {Code, Locals} */
        ERL_NIF_TERM locals = (argc > 2 && enif_is_map(env, argv[2]))
            ? argv[2] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple2(env, argv[1], locals);
        return dispatch_to_worker_thread(env, ctx, CTX_REQ_EVAL, request);
    }

    /* Legacy mode: direct execution with py_context_acquire.
     * For subinterpreters, py_context_acquire handles PyThreadState_Swap
     * to switch to the pool slot's interpreter. */
    ErlNifBinary code_bin;
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

    /* Acquire thread state using centralized guard (worker mode only) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(code);
        return make_error(env, "acquire_failed");
    }

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
                Py_CLEAR(tl_pending_args);
                result = make_error(env, "create_suspended_state_failed");
            } else {
                result = build_suspended_context_result(env, suspended);
            }
        } else {
            result = make_py_error(env);
        }
    } else if (is_inline_schedule_marker(py_result)) {
        /* Inline schedule marker: chain via enif_schedule_nif without Erlang messaging */
        inline_continuation_t *cont = create_inline_continuation(ctx, NULL, py_result, 0);
        Py_DECREF(py_result);

        if (cont == NULL) {
            result = make_error(env, "create_continuation_failed");
        } else {
            ERL_NIF_TERM cont_ref = enif_make_resource(env, cont);
            enif_release_resource(cont);

            /* Restore thread-local state before scheduling */
            tl_allow_suspension = prev_allow_suspension;
            tl_current_context = prev_context;
            clear_pending_callback_tls();
            enif_free(code);
            py_context_release(&guard);

            return enif_schedule_nif(env, "inline_continuation",
                ERL_NIF_DIRTY_JOB_IO_BOUND, nif_inline_continuation, 1, &cont_ref);
        }
    } else if (is_schedule_marker(py_result)) {
        /* Schedule marker: release dirty scheduler, continue via callback */
        ScheduleMarkerObject *marker = (ScheduleMarkerObject *)py_result;
        ERL_NIF_TERM callback_name = py_to_term(env, marker->callback_name);
        ERL_NIF_TERM callback_args = py_to_term(env, marker->args);
        Py_DECREF(py_result);
        result = enif_make_tuple3(env, ATOM_SCHEDULE, callback_name, callback_args);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;

    /* Clear pending callback TLS before releasing context */
    clear_pending_callback_tls();

    enif_free(code);

    /* Release thread state using centralized guard */
    py_context_release(&guard);

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

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_to_owngil_thread(env, ctx, CTX_REQ_EXEC, argv[1]);
    }
#endif

    /* Worker thread mode: dispatch to dedicated thread */
    if (ctx->uses_worker_thread) {
        return dispatch_to_worker_thread(env, ctx, CTX_REQ_EXEC, argv[1]);
    }

    /* Legacy mode: direct execution with py_context_acquire.
     * For subinterpreters, py_context_acquire handles PyThreadState_Swap
     * to switch to the pool slot's interpreter. */
    ErlNifBinary code_bin;
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

    /* Acquire thread state using centralized guard (worker mode only) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(code);
        return make_error(env, "acquire_failed");
    }

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

    /* Release thread state using centralized guard */
    py_context_release(&guard);

    return result;
}

/* ============================================================================
 * Process-local Environment NIFs
 * ============================================================================ */

/**
 * @brief Create a new process-local Python environment
 *
 * nif_create_local_env(ContextRef) -> {ok, EnvRef} | {error, Reason}
 *
 * Creates a new Python globals/locals dict pair for use as a process-local
 * environment. The dicts are created inside the context's interpreter to
 * ensure correct memory allocator is used.
 *
 * The returned resource should be stored in the process dictionary, keyed
 * by the interpreter ID.
 */
static ERL_NIF_TERM nif_create_local_env(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    py_env_resource_t *res = enif_alloc_resource(PY_ENV_RESOURCE_TYPE,
                                                  sizeof(py_env_resource_t));
    if (res == NULL) {
        return make_error(env, "alloc_failed");
    }

    res->globals = NULL;
    res->locals = NULL;
    res->interp_id = 0;

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to the dedicated thread to create dicts */
    if (ctx->uses_own_gil) {
        ERL_NIF_TERM dispatch_result = dispatch_create_local_env_to_owngil(env, ctx, res);

        /* Check if dispatch succeeded */
        ERL_NIF_TERM error_atom = enif_make_atom(env, "error");
        const ERL_NIF_TERM *tuple_elems;
        int arity;
        if (enif_get_tuple(env, dispatch_result, &arity, &tuple_elems) &&
            arity == 2 && enif_is_identical(tuple_elems[0], error_atom)) {
            /* Dispatch failed - release resource and return error */
            enif_release_resource(res);
            return dispatch_result;
        }

        /* Success - return the resource */
        ERL_NIF_TERM ref = enif_make_resource(env, res);
        enif_release_resource(res);  /* Ref now owns it */
        return enif_make_tuple2(env, ATOM_OK, ref);
    }
#endif

    /* Acquire context to switch to correct interpreter */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_release_resource(res);
        return make_error(env, "acquire_failed");
    }

    /* Copy globals from context to inherit preloaded code */
    res->globals = PyDict_Copy(ctx->globals);
    if (res->globals == NULL) {
        py_context_release(&guard);
        enif_release_resource(res);
        return make_error(env, "globals_copy_failed");
    }

    /* Ensure __builtins__ is present (may not be in subinterpreter mode) */
    if (PyDict_GetItemString(res->globals, "__builtins__") == NULL) {
        PyObject *builtins = PyEval_GetBuiltins();
        if (builtins != NULL) {
            PyDict_SetItemString(res->globals, "__builtins__", builtins);
        }
    }

    /* Ensure __name__ = '__main__' is set */
    if (PyDict_GetItemString(res->globals, "__name__") == NULL) {
        PyObject *main_name = PyUnicode_FromString("__main__");
        if (main_name != NULL) {
            PyDict_SetItemString(res->globals, "__name__", main_name);
            Py_DECREF(main_name);
        }
    }

    /* Ensure erlang module is available */
    if (PyDict_GetItemString(res->globals, "erlang") == NULL) {
        PyObject *erlang = PyImport_ImportModule("erlang");
        if (erlang != NULL) {
            PyDict_SetItemString(res->globals, "erlang", erlang);
            Py_DECREF(erlang);
        }
    }

    /* Use the same dict for locals (module-level execution) */
    res->locals = res->globals;
    Py_INCREF(res->locals);

    py_context_release(&guard);

    ERL_NIF_TERM ref = enif_make_resource(env, res);
    enif_release_resource(res);  /* Ref now owns it */

    return enif_make_tuple2(env, ATOM_OK, ref);
}

/**
 * @brief Apply a list of imports to an interpreter's sys.modules
 *
 * nif_interp_apply_imports(Ref, Imports) -> ok | {error, Reason}
 *
 * Imports: [{ModuleBin, FuncBin | 'all'}, ...]
 * Imports modules into the interpreter's sys.modules (shared by all
 * contexts/loops using this interpreter).
 *
 * Note: This imports into the INTERPRETER's module cache (sys.modules),
 * not a per-context cache. All contexts using this interpreter will
 * see the imported modules.
 */
static ERL_NIF_TERM nif_interp_apply_imports(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    if (ctx->destroyed) {
        return make_error(env, "context_destroyed");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to the dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_apply_imports_to_owngil(env, ctx, argv[1]);
    }
#endif

    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        return make_error(env, "acquire_failed");
    }

    /* Process each import - imports go into interpreter's sys.modules */
    ERL_NIF_TERM head, tail = argv[1];
    int arity;
    const ERL_NIF_TERM *tuple;

    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity != 2) {
            continue;
        }

        ErlNifBinary module_bin;
        if (!enif_inspect_binary(env, tuple[0], &module_bin)) {
            continue;
        }

        /* Convert to C string */
        char *module_name = enif_alloc(module_bin.size + 1);
        if (module_name == NULL) continue;
        memcpy(module_name, module_bin.data, module_bin.size);
        module_name[module_bin.size] = '\0';

        /* Skip __main__ */
        if (strcmp(module_name, "__main__") == 0) {
            enif_free(module_name);
            continue;
        }

        /* Import the module - this caches in interpreter's sys.modules
         * which is shared by all contexts using this interpreter */
        PyObject *mod = PyImport_ImportModule(module_name);
        if (mod != NULL) {
            Py_DECREF(mod);  /* sys.modules holds the reference */
        } else {
            /* Clear error - import failure is not fatal */
            PyErr_Clear();
        }

        enif_free(module_name);
    }

    py_context_release(&guard);
    return ATOM_OK;
}

/**
 * @brief Apply a list of paths to an interpreter's sys.path
 *
 * nif_interp_apply_paths(Ref, Paths) -> ok | {error, Reason}
 *
 * Paths: [PathBin, ...]
 * Inserts paths at the beginning of sys.path so they take precedence.
 */
static ERL_NIF_TERM nif_interp_apply_paths(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    if (ctx->destroyed) {
        return make_error(env, "context_destroyed");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to the dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_apply_paths_to_owngil(env, ctx, argv[1]);
    }
#endif

    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        return make_error(env, "acquire_failed");
    }

    /* Get sys.path */
    PyObject *sys_module = PyImport_ImportModule("sys");
    if (sys_module == NULL) {
        py_context_release(&guard);
        return make_error(env, "sys_import_failed");
    }

    PyObject *sys_path = PyObject_GetAttrString(sys_module, "path");
    Py_DECREF(sys_module);
    if (sys_path == NULL || !PyList_Check(sys_path)) {
        Py_XDECREF(sys_path);
        py_context_release(&guard);
        return make_error(env, "sys_path_not_list");
    }

    /* Process each path - insert at beginning in reverse order */
    /* First, collect all paths */
    ERL_NIF_TERM head, tail = argv[1];
    int path_count = 0;
    ERL_NIF_TERM paths_list = argv[1];

    /* Count paths */
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        path_count++;
    }

    /* Insert in reverse order so first path ends up first */
    tail = paths_list;
    for (int i = 0; i < path_count; i++) {
        /* Skip to the i-th element from the end */
        ERL_NIF_TERM current = paths_list;
        for (int j = 0; j < path_count - 1 - i; j++) {
            enif_get_list_cell(env, current, &head, &current);
        }
        enif_get_list_cell(env, current, &head, &current);

        ErlNifBinary path_bin;
        if (!enif_inspect_binary(env, head, &path_bin)) {
            continue;
        }

        /* Convert to Python string */
        PyObject *path_str = PyUnicode_FromStringAndSize((char *)path_bin.data, path_bin.size);
        if (path_str == NULL) {
            PyErr_Clear();
            continue;
        }

        /* Check if already in sys.path */
        int already_present = PySequence_Contains(sys_path, path_str);
        if (already_present <= 0) {
            /* Insert at position 0 */
            PyList_Insert(sys_path, 0, path_str);
        }
        Py_DECREF(path_str);
    }

    Py_DECREF(sys_path);
    py_context_release(&guard);
    return ATOM_OK;
}

/**
 * @brief Execute Python statements using a process-local environment
 *
 * nif_context_exec_with_env(ContextRef, Code, EnvRef) -> ok | {error, Reason}
 *
 * In worker mode, uses the process-local environment's globals/locals.
 * In subinterpreter mode, the EnvRef is ignored (each subinterp is isolated).
 *
 * The tl_current_local_env thread-local is set during execution to support
 * reentrant calls - when Python calls erlang.call() which calls back to Python,
 * the same environment is used.
 */
static ERL_NIF_TERM nif_context_exec_with_env(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    py_env_resource_t *penv;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    ErlNifBinary code_bin;
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    /* Get process-local environment */
    if (!enif_get_resource(env, argv[2], PY_ENV_RESOURCE_TYPE, (void **)&penv)) {
        return make_error(env, "invalid_env");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to the dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_exec_with_env_to_owngil(env, ctx, argv[1], penv);
    }
#endif

    /* Worker thread mode: dispatch to dedicated thread with local env */
    if (ctx->uses_worker_thread) {
        /* For exec, we just pass the code binary */
        return dispatch_to_worker_thread_impl(env, ctx, CTX_REQ_EXEC_WITH_ENV, argv[1], penv);
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

    /* Acquire thread state */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(code);
        return make_error(env, "acquire_failed");
    }

    /* Set thread-local context and env for callback/reentrant support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;
    py_env_resource_t *prev_local_env = tl_current_local_env;
    tl_current_local_env = penv;

    /* Always use process-local environment */
    PyObject *exec_globals = penv->globals;
    PyObject *exec_locals = penv->globals;

    /* Execute statements */
    PyObject *py_result = PyRun_String(code, Py_file_input, exec_globals, exec_locals);

    if (py_result == NULL) {
        result = make_py_error(env);
    } else {
        Py_DECREF(py_result);
        result = ATOM_OK;
    }

    /* Restore thread-local state */
    tl_current_context = prev_context;
    tl_current_local_env = prev_local_env;

    enif_free(code);
    py_context_release(&guard);

    return result;
}

/**
 * @brief Evaluate a Python expression using a process-local environment
 *
 * nif_context_eval_with_env(ContextRef, Code, Locals, EnvRef) -> {ok, Result} | {error, Reason}
 *
 * In worker mode, uses the process-local environment's globals/locals.
 * In subinterpreter mode, the EnvRef is ignored (each subinterp is isolated).
 */
static ERL_NIF_TERM nif_context_eval_with_env(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    py_env_resource_t *penv;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    ErlNifBinary code_bin;
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    /* Get process-local environment (argv[3]) */
    if (!enif_get_resource(env, argv[3], PY_ENV_RESOURCE_TYPE, (void **)&penv)) {
        return make_error(env, "invalid_env");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to the dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_eval_with_env_to_owngil(env, ctx, argv[1], argv[2], penv);
    }
#endif

    /* Worker thread mode: dispatch to dedicated thread with local env */
    if (ctx->uses_worker_thread) {
        /* Build request tuple: {Code, Locals} */
        ERL_NIF_TERM locals = (argc > 2 && enif_is_map(env, argv[2]))
            ? argv[2] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple2(env, argv[1], locals);
        return dispatch_to_worker_thread_impl(env, ctx, CTX_REQ_EVAL_WITH_ENV, request, penv);
    }

    char *code = binary_to_string(&code_bin);
    if (code == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

    /* Acquire thread state */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(code);
        return make_error(env, "acquire_failed");
    }

    /* Set thread-local context and env for callback/reentrant support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;
    py_env_resource_t *prev_local_env = tl_current_local_env;
    tl_current_local_env = penv;

    /* Enable suspension for callback support */
    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    /* Always use process-local environment */
    PyObject *eval_globals = penv->globals;

    /* Build locals dict from Erlang map (if provided) */
    PyObject *eval_locals = PyDict_Copy(eval_globals);
    if (enif_is_map(env, argv[2])) {
        ErlNifMapIterator iter;
        ERL_NIF_TERM key, value;

        enif_map_iterator_create(env, argv[2], &iter, ERL_NIF_MAP_ITERATOR_FIRST);
        while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
            PyObject *py_key = term_to_py(env, key);
            PyObject *py_value = term_to_py(env, value);
            if (py_key != NULL && py_value != NULL) {
                PyDict_SetItem(eval_locals, py_key, py_value);
            }
            Py_XDECREF(py_key);
            Py_XDECREF(py_value);
            enif_map_iterator_next(env, &iter);
        }
        enif_map_iterator_destroy(env, &iter);
    }

    /* Evaluate expression */
    PyObject *py_result = PyRun_String(code, Py_eval_input, eval_globals, eval_locals);
    Py_DECREF(eval_locals);

    if (py_result == NULL) {
        /* Check for pending callback (flag-based detection) */
        if (tl_pending_callback) {
            PyErr_Clear();
            /* Create suspended state for callback handling */
            suspended_context_state_t *suspended = create_suspended_context_state_for_eval(
                env, ctx, &code_bin, argv[2]);
            if (suspended == NULL) {
                tl_pending_callback = false;
                Py_CLEAR(tl_pending_args);
                result = make_error(env, "create_suspended_state_failed");
            } else {
                result = build_suspended_context_result(env, suspended);
            }
        } else {
            result = make_py_error(env);
        }
    } else if (is_inline_schedule_marker(py_result)) {
        /* Inline schedule marker: chain via enif_schedule_nif with local_env */
        inline_continuation_t *cont = create_inline_continuation(ctx, penv, py_result, 0);
        Py_DECREF(py_result);

        if (cont == NULL) {
            result = make_error(env, "create_continuation_failed");
        } else {
            ERL_NIF_TERM cont_ref = enif_make_resource(env, cont);
            enif_release_resource(cont);

            /* Restore thread-local state before scheduling */
            tl_allow_suspension = prev_allow_suspension;
            tl_current_context = prev_context;
            tl_current_local_env = prev_local_env;
            clear_pending_callback_tls();
            enif_free(code);
            py_context_release(&guard);

            return enif_schedule_nif(env, "inline_continuation",
                ERL_NIF_DIRTY_JOB_IO_BOUND, nif_inline_continuation, 1, &cont_ref);
        }
    } else if (is_schedule_marker(py_result)) {
        /* Schedule marker: release dirty scheduler, continue via callback */
        ScheduleMarkerObject *marker = (ScheduleMarkerObject *)py_result;
        ERL_NIF_TERM callback_name = py_to_term(env, marker->callback_name);
        ERL_NIF_TERM callback_args = py_to_term(env, marker->args);
        Py_DECREF(py_result);
        result = enif_make_tuple3(env, ATOM_SCHEDULE, callback_name, callback_args);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;
    tl_current_local_env = prev_local_env;

    clear_pending_callback_tls();
    enif_free(code);
    py_context_release(&guard);

    return result;
}

/**
 * @brief Call a Python function using a process-local environment
 *
 * nif_context_call_with_env(ContextRef, Module, Func, Args, Kwargs, EnvRef) -> {ok, Result} | {error, Reason}
 *
 * In worker mode, uses the process-local environment's globals for module lookup.
 * In subinterpreter mode, the EnvRef is ignored (each subinterp is isolated).
 *
 * For __main__ module, functions defined via exec() in the process-local env
 * are accessible.
 */
static ERL_NIF_TERM nif_context_call_with_env(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_context_t *ctx;
    py_env_resource_t *penv;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    ErlNifBinary module_bin, func_bin;
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &func_bin)) {
        return make_error(env, "invalid_func");
    }

    /* Get process-local environment (argv[5]) */
    if (!enif_get_resource(env, argv[5], PY_ENV_RESOURCE_TYPE, (void **)&penv)) {
        return make_error(env, "invalid_env");
    }

#ifdef HAVE_SUBINTERPRETERS
    /* OWN_GIL mode: dispatch to the dedicated thread */
    if (ctx->uses_own_gil) {
        return dispatch_call_with_env_to_owngil(env, ctx, argv[1], argv[2], argv[3], argv[4], penv);
    }
#endif

    /* Worker thread mode: dispatch to dedicated thread with local env */
    if (ctx->uses_worker_thread) {
        /* Build request tuple: {Module, Func, Args, Kwargs} */
        ERL_NIF_TERM kwargs = (argc > 4 && enif_is_map(env, argv[4]))
            ? argv[4] : enif_make_new_map(env);
        ERL_NIF_TERM request = enif_make_tuple4(env,
            argv[1],  /* Module */
            argv[2],  /* Func */
            argv[3],  /* Args */
            kwargs);
        return dispatch_to_worker_thread_impl(env, ctx, CTX_REQ_CALL_WITH_ENV, request, penv);
    }

    char *module_name = binary_to_string(&module_bin);
    char *func_name = binary_to_string(&func_bin);
    if (module_name == NULL || func_name == NULL) {
        enif_free(module_name);
        enif_free(func_name);
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM result;

    /* Acquire thread state */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(module_name);
        enif_free(func_name);
        return make_error(env, "acquire_failed");
    }

    /* Set thread-local context and env for callback/reentrant support */
    py_context_t *prev_context = tl_current_context;
    tl_current_context = ctx;
    py_env_resource_t *prev_local_env = tl_current_local_env;
    tl_current_local_env = penv;

    /* Enable suspension for callback support */
    bool prev_allow_suspension = tl_allow_suspension;
    tl_allow_suspension = true;

    /* Always use process-local environment */
    PyObject *lookup_globals = penv->globals;

    PyObject *module = NULL;
    PyObject *func = NULL;

    /* Special handling for __main__ module - look up in process-local globals */
    if (strcmp(module_name, "__main__") == 0) {
        func = PyDict_GetItemString(lookup_globals, func_name);  /* Borrowed ref */
        if (func != NULL) {
            Py_INCREF(func);
        }
    }

    if (func == NULL) {
        /* Get or import module from context cache */
        module = context_get_module(ctx, module_name);
        if (module == NULL) {
            result = make_py_error(env);
            goto cleanup;
        }

        /* Get function */
        func = PyObject_GetAttrString(module, func_name);
        if (func == NULL) {
            result = make_py_error(env);
            goto cleanup;
        }
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
        /* Check for pending callback */
        if (tl_pending_callback) {
            PyErr_Clear();
            suspended_context_state_t *suspended = create_suspended_context_state_for_call(
                env, ctx, &module_bin, &func_bin, argv[3],
                argc > 4 ? argv[4] : enif_make_new_map(env));
            if (suspended == NULL) {
                tl_pending_callback = false;
                Py_CLEAR(tl_pending_args);
                result = make_error(env, "create_suspended_state_failed");
            } else {
                result = build_suspended_context_result(env, suspended);
            }
        } else {
            result = make_py_error(env);
        }
    } else if (is_inline_schedule_marker(py_result)) {
        /* Inline schedule marker: chain via enif_schedule_nif with local_env */
        inline_continuation_t *cont = create_inline_continuation(ctx, penv, py_result, 0);
        Py_DECREF(py_result);

        if (cont == NULL) {
            result = make_error(env, "create_continuation_failed");
        } else {
            ERL_NIF_TERM cont_ref = enif_make_resource(env, cont);
            enif_release_resource(cont);

            /* Restore thread-local state before scheduling */
            tl_allow_suspension = prev_allow_suspension;
            tl_current_context = prev_context;
            tl_current_local_env = prev_local_env;
            clear_pending_callback_tls();
            enif_free(module_name);
            enif_free(func_name);
            py_context_release(&guard);

            return enif_schedule_nif(env, "inline_continuation",
                ERL_NIF_DIRTY_JOB_IO_BOUND, nif_inline_continuation, 1, &cont_ref);
        }
    } else if (is_schedule_marker(py_result)) {
        ScheduleMarkerObject *marker = (ScheduleMarkerObject *)py_result;
        ERL_NIF_TERM callback_name = py_to_term(env, marker->callback_name);
        ERL_NIF_TERM callback_args = py_to_term(env, marker->args);
        Py_DECREF(py_result);
        result = enif_make_tuple3(env, ATOM_SCHEDULE, callback_name, callback_args);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    /* Restore thread-local state */
    tl_allow_suspension = prev_allow_suspension;
    tl_current_context = prev_context;
    tl_current_local_env = prev_local_env;

    clear_pending_callback_tls();
    enif_free(module_name);
    enif_free(func_name);
    py_context_release(&guard);

    return result;
}

/**
 * @brief Call a method on a Python object in a context
 *
 * nif_context_call_method(ContextRef, ObjRef, Method, Args) -> {ok, Result} | {error, Reason}
 *
 * NO MUTEX - caller must ensure exclusive access (process ownership)
 *
 * NOTE: For OWN_GIL subinterpreters, this function is not supported because
 * py_context_acquire uses PyGILState_Ensure which doesn't work with
 * subinterpreter GILs. A proper implementation would dispatch to the
 * dedicated thread, but this is not yet implemented.
 */
static ERL_NIF_TERM nif_context_call_method(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    py_object_t *obj_wrapper;
    ErlNifBinary method_bin;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    /* Both worker mode and subinterpreter mode use py_context_acquire.
     * For subinterpreters, py_context_acquire handles PyThreadState_Swap
     * to switch to the pool slot's interpreter. */

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

    /* Acquire thread state using centralized guard (worker mode only) */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(method_name);
        return make_error(env, "acquire_failed");
    }

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

    /* Release thread state using centralized guard */
    py_context_release(&guard);

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

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

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
 *
 * NOTE: For OWN_GIL subinterpreters, this function is not yet supported.
 * A proper implementation would add PY_CMD_RESUME and dispatch to the
 * dedicated thread.
 */
static ERL_NIF_TERM nif_context_resume(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    suspended_context_state_t *state;
    ErlNifBinary result_bin;

    if (!runtime_is_running()) {
        return make_error(env, "python_not_running");
    }

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    /* Both worker mode and subinterpreter mode use py_context_acquire.
     * For subinterpreters, py_context_acquire handles PyThreadState_Swap
     * to switch to the pool slot's interpreter. */

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

    /* Acquire thread state using centralized guard */
    py_context_guard_t guard = py_context_acquire(ctx);
    if (!guard.acquired) {
        enif_free(state->result_data);
        state->result_data = NULL;
        state->has_result = false;
        return make_error(env, "acquire_failed");
    }

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
                    Py_CLEAR(tl_pending_args);
                    result = make_error(env, "create_nested_suspended_state_failed");
                } else {
                    /* Copy accumulated callback results from parent to nested state */
                    if (copy_callback_results_to_nested(nested, state) != 0) {
                        enif_release_resource(nested);
                        tl_pending_callback = false;
                        Py_CLEAR(tl_pending_args);
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
                    Py_CLEAR(tl_pending_args);
                    result = make_error(env, "create_nested_suspended_state_failed");
                } else {
                    /* Copy accumulated callback results from parent to nested state */
                    if (copy_callback_results_to_nested(nested, state) != 0) {
                        enif_release_resource(nested);
                        tl_pending_callback = false;
                        Py_CLEAR(tl_pending_args);
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

    /* Clear pending callback TLS before releasing context */
    clear_pending_callback_tls();

    /* Release thread state using centralized guard */
    py_context_release(&guard);

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

    atomic_fetch_add(&g_counters.pyref_created, 1);
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

#ifdef HAVE_SUBINTERPRETERS
    /* For subinterpreter objects, PyGILState_Ensure only works for main interpreter.
     * These operations must go through the owning context. */
    if (ref->interp_id > 0) {
        return make_error(env, "subinterp_ref_requires_context");
    }
#endif

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

#ifdef HAVE_SUBINTERPRETERS
    /* For subinterpreter objects, PyGILState_Ensure only works for main interpreter.
     * These operations must go through the owning context. */
    if (ref->interp_id > 0) {
        return make_error(env, "subinterp_ref_requires_context");
    }
#endif

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

#ifdef HAVE_SUBINTERPRETERS
    /* For subinterpreter objects, PyGILState_Ensure only works for main interpreter.
     * These operations must go through the owning context. */
    if (ref->interp_id > 0) {
        return make_error(env, "subinterp_ref_requires_context");
    }
#endif

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
 * OWN_GIL Subinterpreter Thread Pool NIFs
 * ============================================================================ */

#ifdef HAVE_SUBINTERPRETERS

/**
 * @brief NIF: Check if OWN_GIL thread pool is available
 */
static ERL_NIF_TERM nif_subinterp_thread_pool_ready(ErlNifEnv *env, int argc,
                                                      const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    return subinterp_thread_pool_is_ready() ? ATOM_TRUE : ATOM_FALSE;
}

/**
 * @brief NIF: Start the OWN_GIL thread pool
 */
static ERL_NIF_TERM nif_subinterp_thread_pool_start(ErlNifEnv *env, int argc,
                                                      const ERL_NIF_TERM argv[]) {
    int num_workers = SUBINTERP_THREAD_POOL_DEFAULT;

    if (argc > 0) {
        if (!enif_get_int(env, argv[0], &num_workers)) {
            return enif_make_badarg(env);
        }
    }

    if (subinterp_thread_pool_init(num_workers) != 0) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "init_failed"));
    }

    return ATOM_OK;
}

/**
 * @brief NIF: Stop the OWN_GIL thread pool
 */
static ERL_NIF_TERM nif_subinterp_thread_pool_stop(ErlNifEnv *env, int argc,
                                                     const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    subinterp_thread_pool_shutdown();
    return ATOM_OK;
}

/**
 * @brief NIF: Get OWN_GIL thread pool statistics
 */
static ERL_NIF_TERM nif_subinterp_thread_pool_stats(ErlNifEnv *env, int argc,
                                                      const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    int num_workers;
    uint64_t total_requests, total_errors;
    subinterp_thread_pool_stats(&num_workers, &total_requests, &total_errors);

    ERL_NIF_TERM map = enif_make_new_map(env);
    enif_make_map_put(env, map, enif_make_atom(env, "num_workers"),
                      enif_make_int(env, num_workers), &map);
    enif_make_map_put(env, map, enif_make_atom(env, "total_requests"),
                      enif_make_uint64(env, total_requests), &map);
    enif_make_map_put(env, map, enif_make_atom(env, "total_errors"),
                      enif_make_uint64(env, total_errors), &map);
    enif_make_map_put(env, map, enif_make_atom(env, "initialized"),
                      subinterp_thread_pool_is_ready() ? ATOM_TRUE : ATOM_FALSE, &map);

    return map;
}

/**
 * @brief NIF: Create OWN_GIL session for event loop pool
 *
 * Creates a new namespace in a worker thread for a calling process.
 * Uses the worker_hint for worker assignment (typically loop index).
 *
 * Returns {ok, WorkerId, HandleId} on success.
 */
static ERL_NIF_TERM nif_owngil_create_session(ErlNifEnv *env, int argc,
                                               const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }

    if (!subinterp_thread_pool_is_ready()) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "pool_not_ready"));
    }

    unsigned int worker_hint;
    if (!enif_get_uint(env, argv[0], &worker_hint)) {
        return enif_make_badarg(env);
    }

    /* Use worker_hint to select worker (modulo num_workers for safety) */
    int num_workers = g_thread_pool.num_workers;
    if (num_workers <= 0) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "no_workers"));
    }

    int worker_id = worker_hint % num_workers;
    uint64_t handle_id = atomic_fetch_add(&g_thread_pool.next_handle_id, 1);

    /* Send create namespace request to worker */
    subinterp_thread_worker_t *w = &g_thread_pool.workers[worker_id];

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_CREATE_NS,
        .request_id = request_id,
        .handle_id = handle_id,
        .payload_len = 0,
    };

    /* Write header */
    if (write(w->cmd_pipe[1], &header, sizeof(header)) != sizeof(header)) {
        pthread_mutex_unlock(&w->dispatch_mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "write_failed"));
    }

    /* Wait for response */
    owngil_header_t resp;
    if (read(w->result_pipe[0], &resp, sizeof(resp)) != sizeof(resp)) {
        pthread_mutex_unlock(&w->dispatch_mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "read_failed"));
    }

    pthread_mutex_unlock(&w->dispatch_mutex);

    if (resp.msg_type != MSG_RESPONSE) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "create_failed"));
    }

    return enif_make_tuple3(env, ATOM_OK,
                            enif_make_uint(env, worker_id),
                            enif_make_uint64(env, handle_id));
}

/**
 * @brief NIF: Submit async task to OWN_GIL worker
 *
 * Submits a task to run in the worker's asyncio event loop.
 * Result is sent to CallerPid as {async_result, Ref, Result}.
 */
static ERL_NIF_TERM nif_owngil_submit_task(ErlNifEnv *env, int argc,
                                            const ERL_NIF_TERM argv[]) {
    if (argc != 7) {
        return enif_make_badarg(env);
    }

    if (!subinterp_thread_pool_is_ready()) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "pool_not_ready"));
    }

    unsigned int worker_id;
    ErlNifUInt64 handle_id;
    ErlNifPid caller_pid;

    if (!enif_get_uint(env, argv[0], &worker_id) ||
        !enif_get_uint64(env, argv[1], &handle_id) ||
        !enif_get_local_pid(env, argv[2], &caller_pid)) {
        return enif_make_badarg(env);
    }

    if (worker_id >= (unsigned int)g_thread_pool.num_workers) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "invalid_worker"));
    }

    /* Build payload tuple: {Module, Func, Args, Kwargs, CallerPid, Ref} */
    ERL_NIF_TERM caller_pid_term = enif_make_pid(env, &caller_pid);
    ERL_NIF_TERM kwargs = enif_make_new_map(env);
    ERL_NIF_TERM payload_tuple = enif_make_tuple6(env,
        argv[4],  /* Module */
        argv[5],  /* Func */
        argv[6],  /* Args */
        kwargs,   /* Kwargs */
        caller_pid_term,
        argv[3]   /* Ref */
    );

    /* Serialize to ETF */
    ErlNifBinary payload_bin;
    if (!enif_term_to_binary(env, payload_tuple, &payload_bin)) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "serialization_failed"));
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[worker_id];

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_ASYNC_CALL,
        .request_id = request_id,
        .handle_id = handle_id,
        .payload_len = payload_bin.size,
    };

    /* Write header and payload */
    if (write(w->cmd_pipe[1], &header, sizeof(header)) != sizeof(header) ||
        write(w->cmd_pipe[1], payload_bin.data, payload_bin.size) != (ssize_t)payload_bin.size) {
        pthread_mutex_unlock(&w->dispatch_mutex);
        enif_release_binary(&payload_bin);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "write_failed"));
    }

    enif_release_binary(&payload_bin);
    pthread_mutex_unlock(&w->dispatch_mutex);

    /* For async, we don't wait for response - worker sends directly to caller */
    return ATOM_OK;
}

/**
 * @brief NIF: Destroy OWN_GIL session
 *
 * Cleans up the namespace in the worker thread.
 */
static ERL_NIF_TERM nif_owngil_destroy_session(ErlNifEnv *env, int argc,
                                                const ERL_NIF_TERM argv[]) {
    if (argc != 2) {
        return enif_make_badarg(env);
    }

    if (!subinterp_thread_pool_is_ready()) {
        return ATOM_OK;  /* Nothing to clean up */
    }

    unsigned int worker_id;
    ErlNifUInt64 handle_id;

    if (!enif_get_uint(env, argv[0], &worker_id) ||
        !enif_get_uint64(env, argv[1], &handle_id)) {
        return enif_make_badarg(env);
    }

    if (worker_id >= (unsigned int)g_thread_pool.num_workers) {
        return ATOM_OK;  /* Invalid worker, nothing to do */
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[worker_id];

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_DESTROY_NS,
        .request_id = request_id,
        .handle_id = handle_id,
        .payload_len = 0,
    };

    /* Write header */
    if (write(w->cmd_pipe[1], &header, sizeof(header)) == sizeof(header)) {
        /* Wait for response */
        owngil_header_t resp;
        read(w->result_pipe[0], &resp, sizeof(resp));
    }

    pthread_mutex_unlock(&w->dispatch_mutex);

    return ATOM_OK;
}

/**
 * @brief NIF: Apply imports to OWN_GIL session
 *
 * Imports modules into the worker's sys.modules.
 * Args: WorkerId, HandleId, Imports (list of {ModuleBin, FuncBin | all})
 */
static ERL_NIF_TERM nif_owngil_apply_imports(ErlNifEnv *env, int argc,
                                              const ERL_NIF_TERM argv[]) {
    if (argc != 3) {
        return enif_make_badarg(env);
    }

    if (!subinterp_thread_pool_is_ready()) {
        return ATOM_OK;  /* Silently succeed if pool not ready */
    }

    unsigned int worker_id;
    ErlNifUInt64 handle_id;

    if (!enif_get_uint(env, argv[0], &worker_id) ||
        !enif_get_uint64(env, argv[1], &handle_id)) {
        return enif_make_badarg(env);
    }

    if (worker_id >= (unsigned int)g_thread_pool.num_workers) {
        return ATOM_OK;  /* Invalid worker, silently succeed */
    }

    /* Serialize imports list to ETF */
    ErlNifBinary payload_bin;
    if (!enif_term_to_binary(env, argv[2], &payload_bin)) {
        return ATOM_OK;  /* Serialization failed, silently succeed */
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[worker_id];

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_APPLY_IMPORTS,
        .request_id = request_id,
        .handle_id = handle_id,
        .payload_len = payload_bin.size,
    };

    /* Write header and payload */
    if (write(w->cmd_pipe[1], &header, sizeof(header)) == sizeof(header)) {
        write(w->cmd_pipe[1], payload_bin.data, payload_bin.size);
        /* Wait for response */
        owngil_header_t resp;
        read(w->result_pipe[0], &resp, sizeof(resp));
    }

    enif_release_binary(&payload_bin);
    pthread_mutex_unlock(&w->dispatch_mutex);

    return ATOM_OK;
}

/**
 * @brief NIF: Apply paths to OWN_GIL session
 *
 * Adds paths to the worker's sys.path.
 * Args: WorkerId, HandleId, Paths (list of path binaries)
 */
static ERL_NIF_TERM nif_owngil_apply_paths(ErlNifEnv *env, int argc,
                                            const ERL_NIF_TERM argv[]) {
    if (argc != 3) {
        return enif_make_badarg(env);
    }

    if (!subinterp_thread_pool_is_ready()) {
        return ATOM_OK;  /* Silently succeed if pool not ready */
    }

    unsigned int worker_id;
    ErlNifUInt64 handle_id;

    if (!enif_get_uint(env, argv[0], &worker_id) ||
        !enif_get_uint64(env, argv[1], &handle_id)) {
        return enif_make_badarg(env);
    }

    if (worker_id >= (unsigned int)g_thread_pool.num_workers) {
        return ATOM_OK;  /* Invalid worker, silently succeed */
    }

    /* Serialize paths list to ETF */
    ErlNifBinary payload_bin;
    if (!enif_term_to_binary(env, argv[2], &payload_bin)) {
        return ATOM_OK;  /* Serialization failed, silently succeed */
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[worker_id];

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_APPLY_PATHS,
        .request_id = request_id,
        .handle_id = handle_id,
        .payload_len = payload_bin.size,
    };

    /* Write header and payload */
    if (write(w->cmd_pipe[1], &header, sizeof(header)) == sizeof(header)) {
        write(w->cmd_pipe[1], payload_bin.data, payload_bin.size);
        /* Wait for response */
        owngil_header_t resp;
        read(w->result_pipe[0], &resp, sizeof(resp));
    }

    enif_release_binary(&payload_bin);
    pthread_mutex_unlock(&w->dispatch_mutex);

    return ATOM_OK;
}

#else /* !HAVE_SUBINTERPRETERS */

/* Stub implementations for Python < 3.12 */

static ERL_NIF_TERM nif_subinterp_thread_pool_ready(ErlNifEnv *env, int argc,
                                                      const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return ATOM_FALSE;
}

static ERL_NIF_TERM nif_subinterp_thread_pool_start(ErlNifEnv *env, int argc,
                                                      const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_atom(env, "not_supported"));
}

static ERL_NIF_TERM nif_subinterp_thread_pool_stop(ErlNifEnv *env, int argc,
                                                     const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return ATOM_OK;
}

static ERL_NIF_TERM nif_subinterp_thread_pool_stats(ErlNifEnv *env, int argc,
                                                      const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    ERL_NIF_TERM map = enif_make_new_map(env);
    enif_make_map_put(env, map, enif_make_atom(env, "supported"), ATOM_FALSE, &map);
    return map;
}

/* OWN_GIL session stubs for non-subinterpreter builds */
static ERL_NIF_TERM nif_owngil_create_session(ErlNifEnv *env, int argc,
                                               const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_atom(env, "not_supported"));
}

static ERL_NIF_TERM nif_owngil_submit_task(ErlNifEnv *env, int argc,
                                            const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_atom(env, "not_supported"));
}

static ERL_NIF_TERM nif_owngil_destroy_session(ErlNifEnv *env, int argc,
                                                const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return ATOM_OK;
}

static ERL_NIF_TERM nif_owngil_apply_imports(ErlNifEnv *env, int argc,
                                              const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return ATOM_OK;
}

static ERL_NIF_TERM nif_owngil_apply_paths(ErlNifEnv *env, int argc,
                                            const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    return ATOM_OK;
}

#endif /* HAVE_SUBINTERPRETERS */

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

    /* ASYNC_WORKER_RESOURCE_TYPE removed - replaced by event loop model */

    SUSPENDED_STATE_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_suspended_state", suspended_state_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

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

    /* Process-local environment resource type */
    PY_ENV_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_env", py_env_resource_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    /* Inline continuation resource type */
    INLINE_CONTINUATION_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "inline_continuation", inline_continuation_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    /* Process-scoped shared dictionary resource type. GC-scoped: the
     * destructor releases the Python dict when the last term ref
     * drops. No per-process monitor — explicit shared_dict_destroy/1
     * is the eager-release path. */
    PY_SHARED_DICT_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_shared_dict", shared_dict_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (WORKER_RESOURCE_TYPE == NULL || PYOBJ_RESOURCE_TYPE == NULL ||
        SUSPENDED_STATE_RESOURCE_TYPE == NULL ||
        PY_CONTEXT_RESOURCE_TYPE == NULL || PY_REF_RESOURCE_TYPE == NULL ||
        PY_CONTEXT_SUSPENDED_RESOURCE_TYPE == NULL ||
        PY_ENV_RESOURCE_TYPE == NULL ||
        INLINE_CONTINUATION_RESOURCE_TYPE == NULL ||
        PY_SHARED_DICT_RESOURCE_TYPE == NULL) {
        return -1;
    }

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
    ATOM_SCHEDULE = enif_make_atom(env, "schedule");
    ATOM_MORE = enif_make_atom(env, "more");

    /* Logging atoms */
    ATOM_PY_LOG = enif_make_atom(env, "py_log");
    ATOM_SPAN_START = enif_make_atom(env, "span_start");
    ATOM_SPAN_END = enif_make_atom(env, "span_end");
    ATOM_SPAN_EVENT = enif_make_atom(env, "span_event");

    /* Worker pool atoms */
    pool_atoms_init(env);

    /* Reactor buffer resource type for zero-copy read handling */
    REACTOR_BUFFER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "reactor_buffer",
        reactor_buffer_resource_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    /* Channel resource type for bidirectional message passing */
    CHANNEL_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_channel",
        channel_resource_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (CHANNEL_RESOURCE_TYPE == NULL) {
        return -1;
    }

    /* PyBuffer resource type for zero-copy input */
    PY_BUFFER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_buffer",
        py_buffer_resource_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (PY_BUFFER_RESOURCE_TYPE == NULL) {
        return -1;
    }

    /* Initialize channel module atoms */
    if (channel_init(env) < 0) {
        return -1;
    }

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

    /* Clean up cached function references - requires GIL */
    if (runtime_is_running()) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        cleanup_callback_cache();
        PyGILState_Release(gstate);
    }

    /* Clean up callback name registry (no GIL needed - pure C data) */
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
    {"get_debug_counters", 0, nif_get_debug_counters, 0},
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

    /* Subinterpreter capability probes */
    {"subinterp_supported", 0, nif_subinterp_supported, 0},
    {"owngil_supported", 0, nif_owngil_supported, 0},

    /* OWN_GIL thread pool (used internally by py_event_loop_pool) */
    {"subinterp_thread_pool_start", 0, nif_subinterp_thread_pool_start, 0},
    {"subinterp_thread_pool_start", 1, nif_subinterp_thread_pool_start, 0},
    {"subinterp_thread_pool_stop", 0, nif_subinterp_thread_pool_stop, 0},
    {"subinterp_thread_pool_ready", 0, nif_subinterp_thread_pool_ready, 0},
    {"subinterp_thread_pool_stats", 0, nif_subinterp_thread_pool_stats, 0},

    /* OWN_GIL session management for event loop pool */
    {"owngil_create_session", 1, nif_owngil_create_session, 0},
    {"owngil_submit_task", 7, nif_owngil_submit_task, 0},
    {"owngil_destroy_session", 2, nif_owngil_destroy_session, 0},
    {"owngil_apply_imports", 3, nif_owngil_apply_imports, 0},
    {"owngil_apply_paths", 3, nif_owngil_apply_paths, 0},

    /* Execution mode info */
    {"execution_mode", 0, nif_execution_mode, 0},

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
    {"set_event_loop_priv_dir", 1, nif_set_event_loop_priv_dir, 0},
    {"event_loop_new", 0, nif_event_loop_new, 0},
    {"event_loop_destroy", 1, nif_event_loop_destroy, 0},
    {"event_loop_set_router", 2, nif_event_loop_set_router, 0},
    {"event_loop_set_worker", 2, nif_event_loop_set_worker, 0},
    {"event_loop_set_id", 2, nif_event_loop_set_id, 0},
    {"event_loop_wakeup", 1, nif_event_loop_wakeup, 0},
    {"event_loop_run_async", 7, nif_event_loop_run_async, ERL_NIF_DIRTY_JOB_IO_BOUND},
    /* Async task queue NIFs (uvloop-inspired) */
    {"submit_task", 7, nif_submit_task, 0},  /* Thread-safe, no GIL needed */
    {"submit_task_with_env", 8, nif_submit_task_with_env, 0},  /* With process-local env */
    {"process_ready_tasks", 1, nif_process_ready_tasks, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"event_loop_set_py_loop", 2, nif_event_loop_set_py_loop, 0},
    /* Per-process namespace NIFs */
    {"event_loop_exec", 2, nif_event_loop_exec, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"event_loop_eval", 2, nif_event_loop_eval, ERL_NIF_DIRTY_JOB_IO_BOUND},
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
    {"dup_fd", 1, nif_dup_fd, 0},
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
    {"set_shared_worker", 1, nif_set_shared_worker, 0},

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
    {"context_exec", 3, nif_context_exec_with_env, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_eval", 4, nif_context_eval_with_env, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_call", 6, nif_context_call_with_env, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    /* Async dispatch - non-blocking, returns immediately */
    {"context_call_async", 7, nif_context_call_async, 0},
    {"context_eval_async", 5, nif_context_eval_async, 0},
    {"context_exec_async", 4, nif_context_exec_async, 0},
    {"create_local_env", 1, nif_create_local_env, 0},
    {"interp_apply_imports", 2, nif_interp_apply_imports, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"interp_apply_paths", 2, nif_interp_apply_paths, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_call_method", 4, nif_context_call_method, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_to_term", 1, nif_context_to_term, 0},
    {"context_interp_id", 1, nif_context_interp_id, 0},
    {"context_set_callback_handler", 2, nif_context_set_callback_handler, 0},
    {"context_get_callback_pipe", 1, nif_context_get_callback_pipe, 0},
    {"context_write_callback_response", 2, nif_context_write_callback_response, 0},
    {"context_resume", 3, nif_context_resume, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"context_cancel_resume", 2, nif_context_cancel_resume, 0},
    {"context_get_event_loop", 1, nif_context_get_event_loop, 0},

    /* py_ref API (Python object references with interp_id) */
    {"ref_wrap", 2, nif_ref_wrap, 0},
    {"is_ref", 1, nif_is_ref, 0},
    {"ref_interp_id", 1, nif_ref_interp_id, 0},
    {"ref_to_term", 1, nif_ref_to_term, 0},
    {"ref_getattr", 2, nif_ref_getattr, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"ref_call_method", 3, nif_ref_call_method, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    /* Reactor NIFs - Erlang-as-Reactor architecture */
    {"reactor_register_fd", 3, nif_reactor_register_fd, 0},
    {"reactor_reselect_read", 1, nif_reactor_reselect_read, 0},
    {"reactor_select_write", 1, nif_reactor_select_write, 0},
    {"get_fd_from_resource", 1, nif_get_fd_from_resource, 0},
    {"reactor_on_read_ready", 2, nif_reactor_on_read_ready, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"reactor_on_write_ready", 2, nif_reactor_on_write_ready, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"reactor_init_connection", 3, nif_reactor_init_connection, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"reactor_close_fd", 2, nif_reactor_close_fd, 0},

    /* Direct FD operations */
    {"fd_read", 2, nif_fd_read, 0},
    {"fd_write", 2, nif_fd_write, 0},
    {"fd_select_read", 1, nif_fd_select_read, 0},
    {"fd_select_write", 1, nif_fd_select_write, 0},
    {"fd_close", 1, nif_fd_close, 0},
    {"socketpair", 0, nif_socketpair, 0},

    /* Channel API - bidirectional message passing */
    {"channel_create", 0, nif_channel_create, 0},
    {"channel_create", 1, nif_channel_create, 0},
    {"channel_send", 2, nif_channel_send, 0},
    {"channel_receive", 2, nif_channel_receive, 0},
    {"channel_try_receive", 1, nif_channel_try_receive, 0},
    {"channel_reply", 3, nif_channel_reply, 0},
    {"channel_close", 1, nif_channel_close, 0},
    {"channel_info", 1, nif_channel_info, 0},
    {"channel_wait", 3, nif_channel_wait, 0},
    {"channel_cancel_wait", 2, nif_channel_cancel_wait, 0},
    {"channel_register_sync_waiter", 1, nif_channel_register_sync_waiter, 0},

    /* ByteChannel API - raw bytes, no term conversion */
    {"byte_channel_send_bytes", 2, nif_byte_channel_send_bytes, 0},
    {"byte_channel_try_receive_bytes", 1, nif_byte_channel_try_receive_bytes, 0},
    {"byte_channel_wait_bytes", 3, nif_byte_channel_wait_bytes, 0},

    /* PyBuffer API - zero-copy input */
    {"py_buffer_create", 1, nif_py_buffer_create, 0},
    {"py_buffer_write", 2, nif_py_buffer_write, 0},
    {"py_buffer_close", 1, nif_py_buffer_close, 0},

    /* SharedDict API - process-scoped shared dictionary */
    {"shared_dict_new", 0, nif_shared_dict_new, 0},
    {"shared_dict_get", 3, nif_shared_dict_get, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"shared_dict_set", 3, nif_shared_dict_set, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"shared_dict_del", 2, nif_shared_dict_del, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"shared_dict_keys", 1, nif_shared_dict_keys, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"shared_dict_destroy", 1, nif_shared_dict_destroy, 0}
};

ERL_NIF_INIT(py_nif, nif_funcs, load, NULL, upgrade, unload)
