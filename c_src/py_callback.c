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
 * @file py_callback.c
 * @brief Erlang callback support and asyncio integration
 * @author Benoit Chesneau
 *
 * @ingroup cb
 *
 * This module implements bidirectional calling between Python and Erlang,
 * enabling Python code to invoke Erlang functions and await their results.
 *
 * @par Features
 *
 * - **erlang module**: Python module providing `erlang.call()` and `erlang.func()`
 * - **Suspension/Resume**: Reentrant callbacks without blocking dirty schedulers
 * - **Asyncio support**: Background event loop for async Python operations
 *
 * @par Suspension Mechanism
 *
 * When Python calls `erlang.call('func', args)`:
 *
 * ```
 * ┌────────────┐         ┌─────────────┐         ┌──────────────┐
 * │   Python   │ raises  │  Executor   │ returns │    Erlang    │
 * │   Code     │ ──────> │  Catches    │ ──────> │   Callback   │
 * └────────────┘ Suspend │  Exception  │ suspend └──────────────┘
 *                        └─────────────┘    │           │
 *                                           │           │ result
 *                        ┌─────────────┐    │           │
 *                        │   Resume    │ <──────────────┘
 *                        │   Replay    │
 *                        └─────────────┘
 *                              │
 *                              v
 *                        ┌────────────┐
 *                        │  Continue  │
 *                        │   Python   │
 *                        └────────────┘
 * ```
 *
 * @par Why Suspension?
 *
 * Without suspension, Python calling Erlang would block a dirty scheduler
 * while waiting for the Erlang callback to complete. With suspension:
 *
 * 1. Dirty scheduler is released immediately
 * 2. Erlang callback runs on normal scheduler
 * 3. Result is stored, Python is replayed on dirty scheduler
 *
 * @par The 'erlang' Python Module
 *
 * Provides two calling syntaxes:
 *
 * ```python
 * # Explicit call
 * result = erlang.call('my_function', arg1, arg2)
 *
 * # Attribute-style call (via __getattr__)
 * result = erlang.my_function(arg1, arg2)
 * ```
 *
 * @par Thread Safety
 *
 * - Thread-local storage tracks current worker and suspended state
 * - Async event loop runs in dedicated thread
 * - Pending futures queue protected by mutex
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Cached Python Function References
 *
 * Cache frequently-used Python functions to avoid repeated module import
 * and attribute lookup overhead on every callback.
 * ============================================================================ */

/** @brief Cached reference to ast.literal_eval function */
static PyObject *g_ast_literal_eval = NULL;

/**
 * @brief Initialize cached Python function references
 *
 * Called during module initialization. Must be called with GIL held.
 */
static void init_callback_cache(void) {
    if (g_ast_literal_eval == NULL) {
        PyObject *ast_mod = PyImport_ImportModule("ast");
        if (ast_mod != NULL) {
            g_ast_literal_eval = PyObject_GetAttrString(ast_mod, "literal_eval");
            Py_DECREF(ast_mod);
        }
        if (g_ast_literal_eval == NULL) {
            PyErr_Clear();  /* Non-fatal if unavailable */
        }
    }
}

/**
 * @brief Cleanup cached Python function references
 *
 * Called during module cleanup. Must be called with GIL held.
 */
static void cleanup_callback_cache(void) {
    Py_XDECREF(g_ast_literal_eval);
    g_ast_literal_eval = NULL;
}

/* ============================================================================
 * Callback Name Registry
 *
 * Maintains a C-side registry of registered callback function names.
 * This allows erlang_module_getattr to only return ErlangFunction wrappers
 * for actually registered functions, preventing introspection issues with
 * libraries like torch that probe module attributes.
 * ============================================================================ */

/**
 * @def CALLBACK_REGISTRY_BUCKETS
 * @brief Number of hash buckets for the callback registry
 */
#define CALLBACK_REGISTRY_BUCKETS 64

/**
 * @struct callback_name_entry_t
 * @brief Entry in the callback name registry hash table
 */
typedef struct callback_name_entry {
    char *name;                        /**< Callback name (owned) */
    size_t name_len;                   /**< Length of name */
    struct callback_name_entry *next;  /**< Next entry in bucket chain */
} callback_name_entry_t;

/** @brief Hash table buckets for callback registry */
static callback_name_entry_t *g_callback_registry[CALLBACK_REGISTRY_BUCKETS] = {NULL};

/** @brief Mutex protecting the callback registry */
static pthread_mutex_t g_callback_registry_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief Simple hash function for callback names
 */
static unsigned int callback_name_hash(const char *name, size_t len) {
    unsigned int hash = 5381;
    for (size_t i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)name[i];
    }
    return hash % CALLBACK_REGISTRY_BUCKETS;
}

/**
 * @brief Check if a callback name is registered
 *
 * Thread-safe lookup in the callback registry.
 *
 * @param name Callback name to check
 * @param len Length of name
 * @return true if registered, false otherwise
 */
static bool is_callback_registered(const char *name, size_t len) {
    unsigned int bucket = callback_name_hash(name, len);
    bool found = false;

    pthread_mutex_lock(&g_callback_registry_mutex);

    callback_name_entry_t *entry = g_callback_registry[bucket];
    while (entry != NULL) {
        if (entry->name_len == len && memcmp(entry->name, name, len) == 0) {
            found = true;
            break;
        }
        entry = entry->next;
    }

    pthread_mutex_unlock(&g_callback_registry_mutex);
    return found;
}

/**
 * @brief Register a callback name
 *
 * Thread-safe addition to the callback registry.
 *
 * @param name Callback name to register
 * @param len Length of name
 * @return 0 on success, -1 on failure
 */
static int register_callback_name(const char *name, size_t len) {
    /* Check if already registered */
    if (is_callback_registered(name, len)) {
        return 0;  /* Already registered, success */
    }

    /* Allocate new entry */
    callback_name_entry_t *entry = enif_alloc(sizeof(callback_name_entry_t));
    if (entry == NULL) {
        return -1;
    }

    entry->name = enif_alloc(len + 1);
    if (entry->name == NULL) {
        enif_free(entry);
        return -1;
    }

    memcpy(entry->name, name, len);
    entry->name[len] = '\0';
    entry->name_len = len;

    unsigned int bucket = callback_name_hash(name, len);

    pthread_mutex_lock(&g_callback_registry_mutex);

    entry->next = g_callback_registry[bucket];
    g_callback_registry[bucket] = entry;

    pthread_mutex_unlock(&g_callback_registry_mutex);

    return 0;
}

/**
 * @brief Unregister a callback name
 *
 * Thread-safe removal from the callback registry.
 *
 * @param name Callback name to unregister
 * @param len Length of name
 */
static void unregister_callback_name(const char *name, size_t len) {
    unsigned int bucket = callback_name_hash(name, len);

    pthread_mutex_lock(&g_callback_registry_mutex);

    callback_name_entry_t **pp = &g_callback_registry[bucket];
    while (*pp != NULL) {
        callback_name_entry_t *entry = *pp;
        if (entry->name_len == len && memcmp(entry->name, name, len) == 0) {
            *pp = entry->next;
            enif_free(entry->name);
            enif_free(entry);
            break;
        }
        pp = &entry->next;
    }

    pthread_mutex_unlock(&g_callback_registry_mutex);
}

/**
 * @brief Clean up the callback registry
 *
 * Frees all entries. Called during NIF unload.
 */
static void cleanup_callback_registry(void) {
    pthread_mutex_lock(&g_callback_registry_mutex);

    for (int i = 0; i < CALLBACK_REGISTRY_BUCKETS; i++) {
        callback_name_entry_t *entry = g_callback_registry[i];
        while (entry != NULL) {
            callback_name_entry_t *next = entry->next;
            enif_free(entry->name);
            enif_free(entry);
            entry = next;
        }
        g_callback_registry[i] = NULL;
    }

    pthread_mutex_unlock(&g_callback_registry_mutex);
}

/* ============================================================================
 * Suspended state management
 * ============================================================================ */

/**
 * Source type for suspended state creation.
 * Indicates whether the source is a request or an existing suspended state.
 */
typedef enum {
    SUSPENDED_SOURCE_REQUEST,   /* Source is py_request_t */
    SUSPENDED_SOURCE_EXISTING   /* Source is suspended_state_t */
} suspended_source_type_t;

/**
 * Source union for suspended state creation.
 * Contains pointers to either request or existing suspended state.
 */
typedef struct {
    suspended_source_type_t type;
    union {
        py_request_t *req;           /* For SUSPENDED_SOURCE_REQUEST */
        suspended_state_t *existing; /* For SUSPENDED_SOURCE_EXISTING */
    } data;
} suspended_source_t;

/**
 * Internal cleanup helper for suspended state creation failure.
 */
static void cleanup_suspended_state_partial(suspended_state_t *state, PyObject *callback_args) {
    if (state->orig_env != NULL) {
        enif_free_env(state->orig_env);
    }
    if (state->callback_args != NULL) {
        Py_DECREF(state->callback_args);
    } else if (callback_args != NULL) {
        Py_DECREF(callback_args);
    }
    if (state->callback_func_name != NULL) {
        enif_free(state->callback_func_name);
    }
    enif_release_resource(state);
}

/**
 * Create a suspended state resource from exception args.
 * Args tuple format: (callback_id, func_name, args)
 *
 * This unified function handles both:
 * - Creating from a request (initial suspension)
 * - Creating from an existing suspended state (nested suspension during replay)
 *
 * @param env NIF environment
 * @param exc_args Exception args tuple from erlang.call()
 * @param source Source of original request data
 * @return suspended_state_t* or NULL on error
 */
static suspended_state_t *create_suspended_state_ex(
    ErlNifEnv *env, PyObject *exc_args, const suspended_source_t *source) {

    (void)env;  /* Only needed for future extensions */

    if (!PyTuple_Check(exc_args) || PyTuple_Size(exc_args) != 3) {
        return NULL;
    }

    PyObject *callback_id_obj = PyTuple_GetItem(exc_args, 0);
    PyObject *func_name_obj = PyTuple_GetItem(exc_args, 1);
    PyObject *callback_args = PyTuple_GetItem(exc_args, 2);

    if (!PyLong_Check(callback_id_obj) || !PyUnicode_Check(func_name_obj)) {
        return NULL;
    }

    /* Allocate the suspended state resource */
    suspended_state_t *state = enif_alloc_resource(
        SUSPENDED_STATE_RESOURCE_TYPE, sizeof(suspended_state_t));
    if (state == NULL) {
        return NULL;
    }

    /* Initialize the state */
    memset(state, 0, sizeof(suspended_state_t));

    /* Set worker based on source type */
    if (source->type == SUSPENDED_SOURCE_REQUEST) {
        state->worker = tl_current_worker;
    } else {
        state->worker = source->data.existing->worker;
    }

    state->callback_id = PyLong_AsUnsignedLongLong(callback_id_obj);

    /* Copy callback function name */
    Py_ssize_t len;
    const char *func_name = PyUnicode_AsUTF8AndSize(func_name_obj, &len);
    if (func_name == NULL) {
        enif_release_resource(state);
        return NULL;
    }
    state->callback_func_name = enif_alloc(len + 1);
    if (state->callback_func_name == NULL) {
        enif_release_resource(state);
        return NULL;
    }
    memcpy(state->callback_func_name, func_name, len);
    state->callback_func_name[len] = '\0';
    state->callback_func_len = len;

    /* Store reference to callback args */
    Py_INCREF(callback_args);
    state->callback_args = callback_args;

    /* Get request type and timeout based on source */
    int request_type;
    unsigned long timeout_ms;

    if (source->type == SUSPENDED_SOURCE_REQUEST) {
        request_type = source->data.req->type;
        timeout_ms = source->data.req->timeout_ms;
    } else {
        request_type = source->data.existing->request_type;
        timeout_ms = source->data.existing->orig_timeout_ms;
    }

    state->request_type = request_type;
    state->orig_timeout_ms = timeout_ms;

    /* Create environment to hold copied terms */
    state->orig_env = enif_alloc_env();
    if (state->orig_env == NULL) {
        cleanup_suspended_state_partial(state, NULL);
        return NULL;
    }

    /* Copy request-specific data based on source type and request type */
    if (request_type == PY_REQ_CALL) {
        ErlNifBinary *src_module, *src_func;
        ERL_NIF_TERM src_args, src_kwargs;
        ErlNifEnv *src_env;

        if (source->type == SUSPENDED_SOURCE_REQUEST) {
            src_module = &source->data.req->module_bin;
            src_func = &source->data.req->func_bin;
            src_args = source->data.req->args_term;
            src_kwargs = source->data.req->kwargs_term;
            src_env = source->data.req->env;
        } else {
            src_module = &source->data.existing->orig_module;
            src_func = &source->data.existing->orig_func;
            src_args = source->data.existing->orig_args;
            src_kwargs = source->data.existing->orig_kwargs;
            src_env = source->data.existing->orig_env;
        }

        /* Copy module binary */
        if (!enif_alloc_binary(src_module->size, &state->orig_module)) {
            cleanup_suspended_state_partial(state, NULL);
            return NULL;
        }
        memcpy(state->orig_module.data, src_module->data, src_module->size);

        /* Copy function binary */
        if (!enif_alloc_binary(src_func->size, &state->orig_func)) {
            enif_release_binary(&state->orig_module);
            cleanup_suspended_state_partial(state, NULL);
            return NULL;
        }
        memcpy(state->orig_func.data, src_func->data, src_func->size);

        /* Copy args and kwargs to our environment */
        state->orig_args = enif_make_copy(state->orig_env, src_args);
        state->orig_kwargs = enif_make_copy(state->orig_env, src_kwargs);
        (void)src_env;  /* Used implicitly by enif_make_copy */

    } else if (request_type == PY_REQ_EVAL) {
        ErlNifBinary *src_code;
        ERL_NIF_TERM src_locals;
        ErlNifEnv *src_env;

        if (source->type == SUSPENDED_SOURCE_REQUEST) {
            src_code = &source->data.req->code_bin;
            src_locals = source->data.req->locals_term;
            src_env = source->data.req->env;
        } else {
            src_code = &source->data.existing->orig_code;
            src_locals = source->data.existing->orig_locals;
            src_env = source->data.existing->orig_env;
        }

        /* Copy code binary */
        if (!enif_alloc_binary(src_code->size, &state->orig_code)) {
            cleanup_suspended_state_partial(state, NULL);
            return NULL;
        }
        memcpy(state->orig_code.data, src_code->data, src_code->size);

        /* Copy locals */
        state->orig_locals = enif_make_copy(state->orig_env, src_locals);
        (void)src_env;  /* Used implicitly by enif_make_copy */
    }

    /* Initialize synchronization primitives */
    pthread_mutex_init(&state->mutex, NULL);
    pthread_cond_init(&state->cond, NULL);

    state->result_data = NULL;
    state->result_len = 0;
    state->has_result = false;
    state->is_error = false;

    return state;
}

/**
 * Create a suspended state resource from a request.
 * Wrapper for create_suspended_state_ex for initial suspension.
 */
static suspended_state_t *create_suspended_state(ErlNifEnv *env, PyObject *exc_args,
                                                  py_request_t *req) {
    suspended_source_t source = {
        .type = SUSPENDED_SOURCE_REQUEST,
        .data.req = req
    };
    return create_suspended_state_ex(env, exc_args, &source);
}

/**
 * Create a new suspended state from an existing one (for nested suspensions).
 * Wrapper for create_suspended_state_ex for nested suspension during replay.
 */
static suspended_state_t *create_suspended_state_from_existing(
    ErlNifEnv *env, PyObject *exc_args, suspended_state_t *existing) {
    suspended_source_t source = {
        .type = SUSPENDED_SOURCE_EXISTING,
        .data.existing = existing
    };
    return create_suspended_state_ex(env, exc_args, &source);
}

/**
 * Build exception args tuple from thread-local pending callback state.
 *
 * This helper extracts the common pattern of building the exc_args tuple
 * (callback_id, func_name, args) from thread-local storage.
 *
 * @return PyObject* tuple on success, NULL on failure
 * @note On failure, tl_pending_callback is cleared
 * @note Caller must Py_DECREF the returned tuple when done
 */
static PyObject *build_pending_callback_exc_args(void) {
    PyObject *exc_args = PyTuple_New(3);
    if (exc_args == NULL) {
        tl_pending_callback = false;
        return NULL;
    }

    PyObject *callback_id_obj = PyLong_FromUnsignedLongLong(tl_pending_callback_id);
    PyObject *func_name_obj = PyUnicode_FromStringAndSize(
        tl_pending_func_name, tl_pending_func_name_len);

    if (callback_id_obj == NULL || func_name_obj == NULL) {
        Py_XDECREF(callback_id_obj);
        Py_XDECREF(func_name_obj);
        Py_DECREF(exc_args);
        tl_pending_callback = false;
        return NULL;
    }

    PyTuple_SET_ITEM(exc_args, 0, callback_id_obj);
    PyTuple_SET_ITEM(exc_args, 1, func_name_obj);
    Py_INCREF(tl_pending_args);  /* Tuple takes ownership */
    PyTuple_SET_ITEM(exc_args, 2, tl_pending_args);

    return exc_args;
}

/**
 * Build the {suspended, ...} result term from a suspended state.
 *
 * Common helper for creating the suspension result after a callback
 * is detected during Python execution.
 *
 * @param env NIF environment
 * @param suspended Suspended state (resource will be released)
 * @return ERL_NIF_TERM {suspended, CallbackId, StateRef, {FuncName, Args}}
 * @note Clears tl_pending_callback
 */
static ERL_NIF_TERM build_suspended_result(ErlNifEnv *env, suspended_state_t *suspended) {
    ERL_NIF_TERM state_ref = enif_make_resource(env, suspended);
    enif_release_resource(suspended);

    ERL_NIF_TERM callback_id_term = enif_make_uint64(env, tl_pending_callback_id);

    ERL_NIF_TERM func_name_term;
    unsigned char *fn_buf = enif_make_new_binary(env, tl_pending_func_name_len, &func_name_term);
    memcpy(fn_buf, tl_pending_func_name, tl_pending_func_name_len);

    ERL_NIF_TERM args_term = py_to_term(env, tl_pending_args);

    tl_pending_callback = false;

    return enif_make_tuple4(env,
        ATOM_SUSPENDED,
        callback_id_term,
        state_ref,
        enif_make_tuple2(env, func_name_term, args_term));
}

/* ============================================================================
 * Context suspension helpers (for process-per-context architecture)
 *
 * These functions handle suspension/resume for py_context_t-based execution.
 * Unlike worker suspension, context suspension doesn't use mutex or condvar -
 * the context process handles callbacks inline via recursive receive.
 * ============================================================================ */

/**
 * Create a suspended context state for a py:call.
 *
 * Called when Python code in a context calls erlang.call() and suspension
 * is required. Captures all state needed to resume after callback completes.
 *
 * @param env NIF environment
 * @param ctx Context executing the Python code
 * @param module_bin Original module binary
 * @param func_bin Original function binary
 * @param args_term Original args term
 * @param kwargs_term Original kwargs term
 * @return suspended_context_state_t* or NULL on error
 */
static suspended_context_state_t *create_suspended_context_state_for_call(
    ErlNifEnv *env,
    py_context_t *ctx,
    ErlNifBinary *module_bin,
    ErlNifBinary *func_bin,
    ERL_NIF_TERM args_term,
    ERL_NIF_TERM kwargs_term) {

    /* Allocate the suspended context state resource */
    suspended_context_state_t *state = enif_alloc_resource(
        PY_CONTEXT_SUSPENDED_RESOURCE_TYPE, sizeof(suspended_context_state_t));
    if (state == NULL) {
        return NULL;
    }

    /* Initialize to zero */
    memset(state, 0, sizeof(suspended_context_state_t));

    state->ctx = ctx;
    state->callback_id = tl_pending_callback_id;
    state->request_type = PY_REQ_CALL;

    /* Copy callback function name */
    state->callback_func_name = enif_alloc(tl_pending_func_name_len + 1);
    if (state->callback_func_name == NULL) {
        enif_release_resource(state);
        return NULL;
    }
    memcpy(state->callback_func_name, tl_pending_func_name, tl_pending_func_name_len);
    state->callback_func_name[tl_pending_func_name_len] = '\0';
    state->callback_func_len = tl_pending_func_name_len;

    /* Store callback args reference */
    Py_INCREF(tl_pending_args);
    state->callback_args = tl_pending_args;

    /* Create environment to hold copied terms */
    state->orig_env = enif_alloc_env();
    if (state->orig_env == NULL) {
        Py_DECREF(state->callback_args);
        enif_free(state->callback_func_name);
        enif_release_resource(state);
        return NULL;
    }

    /* Copy module binary */
    if (!enif_alloc_binary(module_bin->size, &state->orig_module)) {
        enif_free_env(state->orig_env);
        Py_DECREF(state->callback_args);
        enif_free(state->callback_func_name);
        enif_release_resource(state);
        return NULL;
    }
    memcpy(state->orig_module.data, module_bin->data, module_bin->size);

    /* Copy function binary */
    if (!enif_alloc_binary(func_bin->size, &state->orig_func)) {
        enif_release_binary(&state->orig_module);
        enif_free_env(state->orig_env);
        Py_DECREF(state->callback_args);
        enif_free(state->callback_func_name);
        enif_release_resource(state);
        return NULL;
    }
    memcpy(state->orig_func.data, func_bin->data, func_bin->size);

    /* Copy args and kwargs to our environment */
    state->orig_args = enif_make_copy(state->orig_env, args_term);
    state->orig_kwargs = enif_make_copy(state->orig_env, kwargs_term);

    return state;
}

/**
 * Create a suspended context state for a py:eval.
 *
 * Called when Python code in a context calls erlang.call() during eval
 * and suspension is required.
 *
 * @param env NIF environment
 * @param ctx Context executing the Python code
 * @param code_bin Original code binary
 * @param locals_term Original locals term
 * @return suspended_context_state_t* or NULL on error
 */
static suspended_context_state_t *create_suspended_context_state_for_eval(
    ErlNifEnv *env,
    py_context_t *ctx,
    ErlNifBinary *code_bin,
    ERL_NIF_TERM locals_term) {

    (void)env;

    /* Allocate the suspended context state resource */
    suspended_context_state_t *state = enif_alloc_resource(
        PY_CONTEXT_SUSPENDED_RESOURCE_TYPE, sizeof(suspended_context_state_t));
    if (state == NULL) {
        return NULL;
    }

    /* Initialize to zero */
    memset(state, 0, sizeof(suspended_context_state_t));

    state->ctx = ctx;
    state->callback_id = tl_pending_callback_id;
    state->request_type = PY_REQ_EVAL;

    /* Copy callback function name */
    state->callback_func_name = enif_alloc(tl_pending_func_name_len + 1);
    if (state->callback_func_name == NULL) {
        enif_release_resource(state);
        return NULL;
    }
    memcpy(state->callback_func_name, tl_pending_func_name, tl_pending_func_name_len);
    state->callback_func_name[tl_pending_func_name_len] = '\0';
    state->callback_func_len = tl_pending_func_name_len;

    /* Store callback args reference */
    Py_INCREF(tl_pending_args);
    state->callback_args = tl_pending_args;

    /* Create environment to hold copied terms */
    state->orig_env = enif_alloc_env();
    if (state->orig_env == NULL) {
        Py_DECREF(state->callback_args);
        enif_free(state->callback_func_name);
        enif_release_resource(state);
        return NULL;
    }

    /* Copy code binary */
    if (!enif_alloc_binary(code_bin->size, &state->orig_code)) {
        enif_free_env(state->orig_env);
        Py_DECREF(state->callback_args);
        enif_free(state->callback_func_name);
        enif_release_resource(state);
        return NULL;
    }
    memcpy(state->orig_code.data, code_bin->data, code_bin->size);

    /* Copy locals to our environment */
    state->orig_locals = enif_make_copy(state->orig_env, locals_term);

    return state;
}

/**
 * Build the {suspended, ...} result term from a suspended context state.
 *
 * @param env NIF environment
 * @param suspended Suspended context state (resource will be released)
 * @return ERL_NIF_TERM {suspended, CallbackId, StateRef, {FuncName, Args}}
 * @note Clears tl_pending_callback
 */
static ERL_NIF_TERM build_suspended_context_result(ErlNifEnv *env, suspended_context_state_t *suspended) {
    ERL_NIF_TERM state_ref = enif_make_resource(env, suspended);
    enif_release_resource(suspended);

    ERL_NIF_TERM callback_id_term = enif_make_uint64(env, tl_pending_callback_id);

    ERL_NIF_TERM func_name_term;
    unsigned char *fn_buf = enif_make_new_binary(env, tl_pending_func_name_len, &func_name_term);
    memcpy(fn_buf, tl_pending_func_name, tl_pending_func_name_len);

    ERL_NIF_TERM args_term = py_to_term(env, tl_pending_args);

    tl_pending_callback = false;

    return enif_make_tuple4(env,
        ATOM_SUSPENDED,
        callback_id_term,
        state_ref,
        enif_make_tuple2(env, func_name_term, args_term));
}

/**
 * Copy accumulated callback results from parent state to nested state.
 *
 * When a sequential callback occurs during replay, the nested suspended state
 * needs to include all callback results from the parent PLUS the current result.
 * This function copies parent's callback_results array and adds the parent's
 * current result (result_data) to the end.
 *
 * @param nested The nested suspended state being created
 * @param parent The parent suspended state (current tl_current_context_suspended)
 * @return 0 on success, -1 on memory allocation failure
 */
static int copy_callback_results_to_nested(suspended_context_state_t *nested,
                                           suspended_context_state_t *parent) {
    if (parent == NULL) {
        /* No parent state - nothing to copy */
        return 0;
    }

    /*
     * Calculate total results needed: parent's array + parent's current result.
     *
     * IMPORTANT: We check result_data != NULL instead of has_result because
     * has_result may have been set to false when the result was consumed
     * during replay, but the result data is still valid and needs to be
     * copied to the nested state for subsequent replays.
     */
    size_t total_results = parent->num_callback_results;
    bool has_current_result = (parent->result_data != NULL && parent->result_len > 0);
    if (has_current_result) {
        total_results += 1;
    }

    if (total_results == 0) {
        /* No results to copy */
        return 0;
    }

    /* Allocate results array */
    nested->callback_results = enif_alloc(total_results * sizeof(nested->callback_results[0]));
    if (nested->callback_results == NULL) {
        return -1;
    }
    nested->callback_results_capacity = total_results;
    nested->num_callback_results = total_results;
    nested->callback_result_index = 0;

    /* Copy parent's accumulated results */
    for (size_t i = 0; i < parent->num_callback_results; i++) {
        size_t len = parent->callback_results[i].len;
        nested->callback_results[i].data = enif_alloc(len);
        if (nested->callback_results[i].data == NULL) {
            /* Cleanup on failure */
            for (size_t j = 0; j < i; j++) {
                enif_free(nested->callback_results[j].data);
            }
            enif_free(nested->callback_results);
            nested->callback_results = NULL;
            nested->num_callback_results = 0;
            nested->callback_results_capacity = 0;
            return -1;
        }
        memcpy(nested->callback_results[i].data, parent->callback_results[i].data, len);
        nested->callback_results[i].len = len;
    }

    /* Add parent's current result (result_data) as the last element */
    if (has_current_result) {
        size_t idx = parent->num_callback_results;
        nested->callback_results[idx].data = enif_alloc(parent->result_len);
        if (nested->callback_results[idx].data == NULL) {
            /* Cleanup on failure */
            for (size_t j = 0; j < idx; j++) {
                enif_free(nested->callback_results[j].data);
            }
            enif_free(nested->callback_results);
            nested->callback_results = NULL;
            nested->num_callback_results = 0;
            nested->callback_results_capacity = 0;
            return -1;
        }
        memcpy(nested->callback_results[idx].data, parent->result_data, parent->result_len);
        nested->callback_results[idx].len = parent->result_len;
    }

    return 0;
}

/**
 * Helper to convert __etf__:base64 strings to Python objects.
 * Used for encoding pids and references in callback responses.
 * Returns a NEW reference on success, NULL with exception on error.
 */
static PyObject *decode_etf_string(const char *str, Py_ssize_t len) {
    /* Check for __etf__: prefix (8 chars) */
    const char *prefix = "__etf__:";
    size_t prefix_len = 8;

    if (len <= (Py_ssize_t)prefix_len || strncmp(str, prefix, prefix_len) != 0) {
        return NULL;  /* Not an ETF string */
    }

    /* Extract base64 portion */
    const char *b64_data = str + prefix_len;
    size_t b64_len = len - prefix_len;

    /* Import base64 module and decode */
    PyObject *base64_mod = PyImport_ImportModule("base64");
    if (base64_mod == NULL) {
        PyErr_Clear();
        return NULL;
    }

    PyObject *b64decode = PyObject_GetAttrString(base64_mod, "b64decode");
    Py_DECREF(base64_mod);
    if (b64decode == NULL) {
        PyErr_Clear();
        return NULL;
    }

    PyObject *b64_str = PyUnicode_FromStringAndSize(b64_data, b64_len);
    if (b64_str == NULL) {
        Py_DECREF(b64decode);
        PyErr_Clear();
        return NULL;
    }

    PyObject *decoded = PyObject_CallFunctionObjArgs(b64decode, b64_str, NULL);
    Py_DECREF(b64decode);
    Py_DECREF(b64_str);

    if (decoded == NULL) {
        PyErr_Clear();
        return NULL;
    }

    /* Get the binary data */
    char *bin_data;
    Py_ssize_t bin_len;
    if (PyBytes_AsStringAndSize(decoded, &bin_data, &bin_len) < 0) {
        Py_DECREF(decoded);
        PyErr_Clear();
        return NULL;
    }

    /* Create a temporary NIF environment to decode the term */
    ErlNifEnv *tmp_env = enif_alloc_env();
    if (tmp_env == NULL) {
        Py_DECREF(decoded);
        return NULL;
    }

    /* Decode the ETF binary to an Erlang term */
    ERL_NIF_TERM term;
    if (enif_binary_to_term(tmp_env, (unsigned char *)bin_data, bin_len, &term, 0) == 0) {
        /* Decoding failed */
        enif_free_env(tmp_env);
        Py_DECREF(decoded);
        return NULL;
    }

    Py_DECREF(decoded);

    /* Convert the term to a Python object */
    PyObject *result = term_to_py(tmp_env, term);
    enif_free_env(tmp_env);

    return result;
}

/**
 * Recursively convert __etf__:base64 strings in a Python object.
 * Handles nested tuples, lists, and dicts.
 * Returns a NEW reference with ETF strings converted, or the original object
 * with its refcount incremented if no conversion was needed.
 */
static PyObject *convert_etf_strings(PyObject *obj) {
    if (obj == NULL) {
        return NULL;
    }

    /* Check if it's a string that might be an ETF encoding */
    if (PyUnicode_Check(obj)) {
        Py_ssize_t len;
        const char *str = PyUnicode_AsUTF8AndSize(obj, &len);
        if (str != NULL && len > 8 && strncmp(str, "__etf__:", 8) == 0) {
            PyObject *decoded = decode_etf_string(str, len);
            if (decoded != NULL) {
                return decoded;  /* Return the decoded object */
            }
            /* If decoding failed, fall through and return original */
        }
        Py_INCREF(obj);
        return obj;
    }

    /* Handle tuples */
    if (PyTuple_Check(obj)) {
        Py_ssize_t size = PyTuple_Size(obj);
        int needs_conversion = 0;

        /* First pass: check if any element needs conversion */
        for (Py_ssize_t i = 0; i < size; i++) {
            PyObject *item = PyTuple_GET_ITEM(obj, i);
            if (PyUnicode_Check(item)) {
                Py_ssize_t len;
                const char *str = PyUnicode_AsUTF8AndSize(item, &len);
                if (str != NULL && len > 8 && strncmp(str, "__etf__:", 8) == 0) {
                    needs_conversion = 1;
                    break;
                }
            } else if (PyTuple_Check(item) || PyList_Check(item) || PyDict_Check(item)) {
                needs_conversion = 1;  /* Might need recursive conversion */
                break;
            }
        }

        if (!needs_conversion) {
            Py_INCREF(obj);
            return obj;
        }

        /* Create new tuple with converted elements */
        PyObject *new_tuple = PyTuple_New(size);
        if (new_tuple == NULL) {
            return NULL;
        }

        for (Py_ssize_t i = 0; i < size; i++) {
            PyObject *item = PyTuple_GET_ITEM(obj, i);
            PyObject *converted = convert_etf_strings(item);
            if (converted == NULL) {
                Py_DECREF(new_tuple);
                return NULL;
            }
            PyTuple_SET_ITEM(new_tuple, i, converted);  /* Steals reference */
        }
        return new_tuple;
    }

    /* Handle lists */
    if (PyList_Check(obj)) {
        Py_ssize_t size = PyList_Size(obj);
        PyObject *new_list = PyList_New(size);
        if (new_list == NULL) {
            return NULL;
        }

        for (Py_ssize_t i = 0; i < size; i++) {
            PyObject *item = PyList_GET_ITEM(obj, i);
            PyObject *converted = convert_etf_strings(item);
            if (converted == NULL) {
                Py_DECREF(new_list);
                return NULL;
            }
            PyList_SET_ITEM(new_list, i, converted);  /* Steals reference */
        }
        return new_list;
    }

    /* Handle dicts */
    if (PyDict_Check(obj)) {
        PyObject *new_dict = PyDict_New();
        if (new_dict == NULL) {
            return NULL;
        }

        PyObject *key, *value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(obj, &pos, &key, &value)) {
            PyObject *conv_key = convert_etf_strings(key);
            PyObject *conv_value = convert_etf_strings(value);
            if (conv_key == NULL || conv_value == NULL) {
                Py_XDECREF(conv_key);
                Py_XDECREF(conv_value);
                Py_DECREF(new_dict);
                return NULL;
            }
            PyDict_SetItem(new_dict, conv_key, conv_value);
            Py_DECREF(conv_key);
            Py_DECREF(conv_value);
        }
        return new_dict;
    }

    /* For all other types, just return with incremented refcount */
    Py_INCREF(obj);
    return obj;
}

/**
 * Helper to parse callback response data into a Python object.
 * Response format: status_byte (0=ok, 1=error) + python_repr_string
 */
static PyObject *parse_callback_response(unsigned char *response_data, size_t response_len) {
    if (response_len < 1) {
        PyErr_SetString(PyExc_RuntimeError, "Empty callback response");
        return NULL;
    }

    uint8_t status = response_data[0];

    if (response_len < 2) {
        if (status == 0) {
            Py_RETURN_NONE;
        } else {
            PyErr_SetString(PyExc_RuntimeError, "Erlang callback failed");
            return NULL;
        }
    }

    char *result_str = (char *)response_data + 1;
    size_t result_len = response_len - 1;

    PyObject *result = NULL;
    if (status == 0) {
        /* Try to evaluate the result string as Python literal.
         * Import ast.literal_eval fresh to support subinterpreters
         * (the cached g_ast_literal_eval may be from a different interpreter). */
        PyObject *ast_mod = PyImport_ImportModule("ast");
        if (ast_mod != NULL) {
            PyObject *literal_eval = PyObject_GetAttrString(ast_mod, "literal_eval");
            Py_DECREF(ast_mod);
            if (literal_eval != NULL) {
                PyObject *arg = PyUnicode_FromStringAndSize(result_str, result_len);
                if (arg != NULL) {
                    result = PyObject_CallFunctionObjArgs(literal_eval, arg, NULL);
                    Py_DECREF(arg);
                    if (result == NULL) {
                        /* If literal_eval fails, return as string */
                        PyErr_Clear();
                        result = PyUnicode_FromStringAndSize(result_str, result_len);
                    } else {
                        /* Post-process result to convert __etf__: strings to Python objects.
                         * This handles pids, references, and other Erlang terms that can't
                         * be represented as Python literals. */
                        PyObject *converted = convert_etf_strings(result);
                        Py_DECREF(result);
                        result = converted;
                    }
                }
                Py_DECREF(literal_eval);
            }
        } else {
            PyErr_Clear();
        }
        if (result == NULL) {
            result = PyUnicode_FromStringAndSize(result_str, result_len);
        }
    } else {
        /* Error case */
        char *err_msg = enif_alloc(result_len + 1);
        if (err_msg != NULL) {
            memcpy(err_msg, result_str, result_len);
            err_msg[result_len] = '\0';
            PyErr_SetString(PyExc_RuntimeError, err_msg);
            enif_free(err_msg);
        } else {
            PyErr_SetString(PyExc_RuntimeError, "Erlang callback failed");
        }
    }

    return result;
}

/* ============================================================================
 * Erlang callback module for Python
 * ============================================================================ */

/* ErlangFunction - callable wrapper for registered Erlang functions */
typedef struct {
    PyObject_HEAD
    PyObject *name;  /* Function name as Python string */
} ErlangFunctionObject;

static void ErlangFunction_dealloc(ErlangFunctionObject *self) {
    Py_XDECREF(self->name);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* Forward declaration - implemented after erlang_call_impl */
static PyObject *ErlangFunction_call(ErlangFunctionObject *self, PyObject *args, PyObject *kwds);

static PyObject *ErlangFunction_repr(ErlangFunctionObject *self) {
    return PyUnicode_FromFormat("<erlang function '%U'>", self->name);
}

static PyTypeObject ErlangFunctionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "erlang.Function",
    .tp_doc = "Wrapper for registered Erlang function",
    .tp_basicsize = sizeof(ErlangFunctionObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)ErlangFunction_dealloc,
    .tp_call = (ternaryfunc)ErlangFunction_call,
    .tp_repr = (reprfunc)ErlangFunction_repr,
};

/* Helper to create ErlangFunction instance */
static PyObject *ErlangFunction_New(PyObject *name) {
    ErlangFunctionObject *self = PyObject_New(ErlangFunctionObject, &ErlangFunctionType);
    if (self != NULL) {
        Py_INCREF(name);
        self->name = name;
    }
    return (PyObject *)self;
}

/* ============================================================================
 * ErlangPid - opaque wrapper for Erlang process identifiers
 *
 * ErlangPidObject is defined in py_nif.h for use by py_convert.c
 * ============================================================================ */

static PyObject *ErlangPid_repr(ErlangPidObject *self) {
    /* Show the raw term value for debugging — not a stable external format,
       but distinguishes different PIDs in logs and repls. */
    return PyUnicode_FromFormat("<erlang.Pid 0x%lx>",
                                (unsigned long)self->pid.pid);
}

static PyObject *ErlangPid_richcompare(PyObject *a, PyObject *b, int op) {
    if (!Py_IS_TYPE(b, &ErlangPidType)) {
        Py_RETURN_NOTIMPLEMENTED;
    }
    ErlangPidObject *pa = (ErlangPidObject *)a;
    ErlangPidObject *pb = (ErlangPidObject *)b;
    int eq = enif_is_identical(pa->pid.pid, pb->pid.pid);
    switch (op) {
        case Py_EQ: return PyBool_FromLong(eq);
        case Py_NE: return PyBool_FromLong(!eq);
        default:    Py_RETURN_NOTIMPLEMENTED;
    }
}

static Py_hash_t ErlangPid_hash(ErlangPidObject *self) {
    Py_hash_t h = (Py_hash_t)enif_hash(ERL_NIF_PHASH2, self->pid.pid, 0);
    if (h == -1) h = -2;  /* -1 is reserved for errors in Python */
    return h;
}

PyTypeObject ErlangPidType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "erlang.Pid",
    .tp_basicsize = sizeof(ErlangPidObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_repr = (reprfunc)ErlangPid_repr,
    .tp_richcompare = ErlangPid_richcompare,
    .tp_hash = (hashfunc)ErlangPid_hash,
    .tp_doc = "Opaque Erlang process identifier",
};

/**
 * Python implementation of erlang.call(name, *args)
 *
 * This function allows Python code to call registered Erlang functions.
 *
 * The implementation uses a suspension/resume mechanism to avoid holding
 * dirty schedulers during callbacks:
 *
 * 1. If a suspended state exists with a cached result, return it immediately
 * 2. Otherwise, create a suspended state, send callback message, wait on condvar
 * 3. When resume_callback is called, the condvar is signaled with the result
 * 4. Parse and return the result
 *
 * This allows the dirty scheduler to be freed while waiting for the callback.
 */
static PyObject *erlang_call_impl(PyObject *self, PyObject *args) {
    (void)self;

    /*
     * Check if we have a callback handler available.
     * Priority:
     * 1. tl_current_context with suspension enabled (new process-per-context API)
     * 2. tl_current_context with callback_handler (old blocking pipe mode)
     * 3. tl_current_worker (legacy worker API)
     * 4. thread_worker_call (spawned threads)
     */
    bool has_context_suspension = (tl_current_context != NULL && tl_allow_suspension);
    bool has_context_handler = (tl_current_context != NULL && tl_current_context->has_callback_handler);
    bool has_worker_handler = (tl_current_worker != NULL && tl_current_worker->has_callback_handler);

    if (!has_context_suspension && !has_context_handler && !has_worker_handler) {
        /*
         * Not an executor thread - use thread worker path.
         * This enables any spawned Python thread to call erlang.call():
         * - threading.Thread instances
         * - concurrent.futures.ThreadPoolExecutor workers
         * - Any other Python threads
         */
        Py_ssize_t nargs = PyTuple_Size(args);
        if (nargs < 1) {
            PyErr_SetString(PyExc_TypeError, "erlang.call requires at least a function name");
            return NULL;
        }

        PyObject *name_obj = PyTuple_GetItem(args, 0);
        if (!PyUnicode_Check(name_obj)) {
            PyErr_SetString(PyExc_TypeError, "Function name must be a string");
            return NULL;
        }
        const char *func_name = PyUnicode_AsUTF8(name_obj);
        if (func_name == NULL) {
            return NULL;
        }
        size_t func_name_len = strlen(func_name);

        /* Build args list (remaining args) */
        PyObject *call_args = PyTuple_GetSlice(args, 1, nargs);
        if (call_args == NULL) {
            return NULL;
        }

        /* Use thread worker call */
        PyObject *result = thread_worker_call(func_name, func_name_len, call_args);
        Py_DECREF(call_args);
        return result;
    }

    Py_ssize_t nargs = PyTuple_Size(args);
    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "erlang.call requires at least a function name");
        return NULL;
    }

    /* Get function name (first arg) */
    PyObject *name_obj = PyTuple_GetItem(args, 0);
    if (!PyUnicode_Check(name_obj)) {
        PyErr_SetString(PyExc_TypeError, "Function name must be a string");
        return NULL;
    }
    const char *func_name = PyUnicode_AsUTF8(name_obj);
    if (func_name == NULL) {
        return NULL;
    }
    size_t func_name_len = strlen(func_name);

    /* Check if we have a suspended state with a cached result (replay case) */
    if (tl_current_suspended != NULL && tl_current_suspended->has_result) {
        /* Verify this is the same callback */
        if (tl_current_suspended->callback_func_len == func_name_len &&
            memcmp(tl_current_suspended->callback_func_name, func_name, func_name_len) == 0) {
            /* Return the cached result - parse using ast.literal_eval */
            PyObject *result = parse_callback_response(
                tl_current_suspended->result_data,
                tl_current_suspended->result_len);
            /* Mark result as consumed (don't clear tl_current_suspended yet,
             * as we might need it for nested callbacks in the future) */
            tl_current_suspended->has_result = false;
            return result;
        }
    }

    /* Check for context-based suspended state with cached results (context replay case) */
    if (tl_current_context_suspended != NULL) {
        /*
         * Sequential callback support:
         * When replaying Python code with multiple sequential erlang.call()s,
         * we need to return results in the same order they were executed.
         * The callback_results array stores results from previous callbacks,
         * indexed in call order. The has_result field holds the CURRENT callback's
         * result (the one that triggered this resume).
         *
         * Example: f(g(h(x)))
         * - Replay 1: h(x) suspended, resumed with h_result
         *   callback_results = [], has_result = h_result
         *   h(x) returns h_result, g(...) suspends
         *
         * - Replay 2: nested state has callback_results = [h_result], has_result = g_result
         *   h(x) returns callback_results[0] = h_result
         *   g(...) returns has_result = g_result
         *   f(...) suspends
         *
         * - Replay 3: nested state has callback_results = [h_result, g_result], has_result = f_result
         *   h(x) returns callback_results[0] = h_result
         *   g(...) returns callback_results[1] = g_result
         *   f(...) returns has_result = f_result
         *   Done!
         */

        /* First, check if we have a cached result from a PREVIOUS callback */
        if (tl_current_context_suspended->callback_result_index <
            tl_current_context_suspended->num_callback_results) {
            /* Return cached result from previous callback, advance index */
            size_t idx = tl_current_context_suspended->callback_result_index++;
            PyObject *result = parse_callback_response(
                tl_current_context_suspended->callback_results[idx].data,
                tl_current_context_suspended->callback_results[idx].len);
            return result;
        }

        /* Next, check if this is the CURRENT callback (the one that triggered resume) */
        if (tl_current_context_suspended->has_result) {
            /* Verify this is the same callback */
            if (tl_current_context_suspended->callback_func_len == func_name_len &&
                memcmp(tl_current_context_suspended->callback_func_name, func_name, func_name_len) == 0) {
                /* Return the current callback result */
                PyObject *result = parse_callback_response(
                    tl_current_context_suspended->result_data,
                    tl_current_context_suspended->result_len);
                /* Mark result as consumed */
                tl_current_context_suspended->has_result = false;
                return result;
            }
        }
        /* If we get here, this is a NEW callback - will suspend below */
    }

    /*
     * FIX for multiple sequential erlang.call():
     * If we're in WORKER replay context (tl_current_suspended != NULL) but didn't get
     * a cache hit above, this is a SUBSEQUENT call (e.g., second erlang.call()
     * in the same Python function). For WORKER mode, the callback handler process
     * is still running and will handle this via blocking pipe.
     *
     * For CONTEXT replay (tl_current_context_suspended != NULL), we CANNOT block
     * because there's no callback handler process. Instead, we must suspend again
     * and let the context process handle the subsequent callback. This works because
     * the context process re-replays from the beginning, and each callback result
     * is returned via the cached result mechanism on subsequent replays.
     */
    bool force_blocking = (tl_current_suspended != NULL);
    /* Note: tl_current_context_suspended is NOT included here - context mode
     * always uses suspension for callbacks, allowing unlimited nesting via replay */

    /* Build args list (remaining args) */
    PyObject *call_args = PyTuple_GetSlice(args, 1, nargs);
    if (call_args == NULL) {
        return NULL;
    }

    /*
     * Check if suspension is allowed.
     * Suspension is only safe when the result will be directly examined by the
     * executor (PY_REQ_CALL or PY_REQ_EVAL). For PY_REQ_EXEC or nested Python
     * code, we must block and wait for the result.
     *
     * Also block if force_blocking is set (replay context with no cache hit).
     */
    if (!tl_allow_suspension || force_blocking) {
        /* Fall back to blocking behavior - send message and wait on pipe */
        ErlNifEnv *msg_env = enif_alloc_env();
        if (msg_env == NULL) {
            Py_DECREF(call_args);
            PyErr_SetString(PyExc_MemoryError, "Failed to allocate message environment");
            return NULL;
        }
        ERL_NIF_TERM func_term;
        {
            unsigned char *buf = enif_make_new_binary(msg_env, func_name_len, &func_term);
            memcpy(buf, func_name, func_name_len);
        }

        ERL_NIF_TERM args_term = py_to_term(msg_env, call_args);
        Py_DECREF(call_args);

        uint64_t callback_id = atomic_fetch_add(&g_callback_id_counter, 1);
        ERL_NIF_TERM id_term = enif_make_uint64(msg_env, callback_id);

        ERL_NIF_TERM msg = enif_make_tuple4(msg_env,
            ATOM_ERLANG_CALLBACK,
            id_term,
            func_term,
            args_term);

        char *response_data = NULL;
        uint32_t response_len = 0;
        int read_result;

        /* Get callback handler and pipe from context or worker */
        ErlNifPid *handler_pid;
        int read_fd;
        if (has_context_handler) {
            handler_pid = &tl_current_context->callback_handler;
            read_fd = tl_current_context->callback_pipe[0];
        } else {
            handler_pid = &tl_current_worker->callback_handler;
            read_fd = tl_current_worker->callback_pipe[0];
        }

        Py_BEGIN_ALLOW_THREADS
        enif_send(NULL, handler_pid, msg_env, msg);
        enif_free_env(msg_env);
        /* Use 30 second timeout to prevent indefinite blocking */
        read_result = read_length_prefixed_data(
            read_fd,
            &response_data, &response_len, 30000);
        Py_END_ALLOW_THREADS

        if (read_result == -1) {
            if (errno == ETIMEDOUT) {
                PyErr_SetString(PyExc_TimeoutError, "Callback response timed out");
            } else {
                PyErr_SetString(PyExc_RuntimeError, "Failed to read callback response");
            }
            return NULL;
        }
        if (read_result == -2) {
            PyErr_SetString(PyExc_MemoryError, "Failed to allocate response buffer");
            return NULL;
        }

        PyObject *result = parse_callback_response((unsigned char *)response_data, response_len);
        if (response_data != NULL) {
            enif_free(response_data);
        }
        return result;
    }

    /*
     * Flag-based suspension: set thread-local flag and raise exception.
     *
     * Unlike checking exception type (which fails if frameworks catch exceptions),
     * we set a thread-local flag that the C executor checks FIRST. This way:
     * 1. Python code can catch/re-raise the exception - we don't care
     * 2. The flag tells us a callback is pending
     * 3. Executor handles it before looking at exception type
     *
     * The exception is just to abort Python execution cleanly.
     */
    uint64_t callback_id = atomic_fetch_add(&g_callback_id_counter, 1);

    /* Set pending callback flag and store info */
    tl_pending_callback = true;
    tl_pending_callback_id = callback_id;

    /* Store function name (make a copy) */
    if (tl_pending_func_name != NULL) {
        enif_free(tl_pending_func_name);
    }
    tl_pending_func_name = enif_alloc(func_name_len + 1);
    if (tl_pending_func_name == NULL) {
        tl_pending_callback = false;
        Py_DECREF(call_args);
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate function name");
        return NULL;
    }
    memcpy(tl_pending_func_name, func_name, func_name_len);
    tl_pending_func_name[func_name_len] = '\0';
    tl_pending_func_name_len = func_name_len;

    /* Store args (take ownership) */
    Py_XDECREF(tl_pending_args);
    tl_pending_args = call_args;  /* Takes ownership, don't decref */

    /* Raise exception to abort Python execution */
    PyErr_SetString(SuspensionRequiredException, "callback pending");
    return NULL;
}

/* ============================================================================
 * erlang.send() - Fire-and-forget message passing
 *
 * Sends a message directly to an Erlang process mailbox via enif_send().
 * No suspension, no blocking, no reply needed.
 * ============================================================================ */

/**
 * @brief Python: erlang.send(pid, term) -> None
 *
 * Fire-and-forget message send to an Erlang process.
 *
 * @param self Module reference (unused)
 * @param args Tuple: (pid:erlang.Pid, term:any)
 * @return None on success, NULL with exception on failure
 */
static PyObject *erlang_send_impl(PyObject *self, PyObject *args) {
    (void)self;

    if (PyTuple_Size(args) != 2) {
        PyErr_SetString(PyExc_TypeError,
            "erlang.send requires exactly 2 arguments: (pid, term)");
        return NULL;
    }

    PyObject *pid_obj = PyTuple_GetItem(args, 0);
    PyObject *term_obj = PyTuple_GetItem(args, 1);

    /* Validate PID type */
    if (!Py_IS_TYPE(pid_obj, &ErlangPidType)) {
        PyErr_SetString(PyExc_TypeError, "First argument must be an erlang.Pid");
        return NULL;
    }

    ErlangPidObject *pid = (ErlangPidObject *)pid_obj;

    /* Allocate a message environment and convert the term */
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate message environment");
        return NULL;
    }

    ERL_NIF_TERM msg = py_to_term(msg_env, term_obj);

    if (PyErr_Occurred()) {
        enif_free_env(msg_env);
        return NULL;
    }

    /* Fire-and-forget send */
    if (!enif_send(NULL, &pid->pid, msg_env, msg)) {
        enif_free_env(msg_env);
        PyErr_SetString(ProcessErrorException,
            "Failed to send message: process may not exist");
        return NULL;
    }

    enif_free_env(msg_env);
    Py_RETURN_NONE;
}

/* ============================================================================
 * Async callback support for asyncio integration
 *
 * This provides erlang.async_call() which returns an asyncio.Future that
 * resolves when the Erlang callback completes. Unlike erlang.call():
 * - No exceptions raised for control flow
 * - Integrates with asyncio event loop
 * - Releases dirty NIF thread while waiting
 * ============================================================================ */

/*
 * Forward declarations for thread worker variables (defined in py_thread_worker.c)
 * These are needed because py_callback.c is included before py_thread_worker.c.
 */
extern ErlNifPid g_thread_coordinator_pid;
extern bool g_has_thread_coordinator;

/* Global state for async callbacks */
static int g_async_callback_pipe[2] = {-1, -1};  /* [0]=read, [1]=write */
static PyObject *g_async_pending_futures = NULL;  /* Dict: callback_id -> Future */
static pthread_mutex_t g_async_futures_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Thread-safe initialization using pthread_once */
static pthread_once_t g_async_callback_init_once = PTHREAD_ONCE_INIT;
static int g_async_callback_init_result = 0;

/**
 * Internal initialization function called by pthread_once.
 * Thread-safe: only called once by pthread_once.
 */
static void async_callback_init_impl(void) {
    if (pipe(g_async_callback_pipe) < 0) {
        g_async_callback_init_result = -1;
        return;
    }

    /* Set the read end to non-blocking for asyncio compatibility */
    int flags = fcntl(g_async_callback_pipe[0], F_GETFL, 0);
    if (flags >= 0) {
        fcntl(g_async_callback_pipe[0], F_SETFL, flags | O_NONBLOCK);
    }

    g_async_pending_futures = PyDict_New();
    if (g_async_pending_futures == NULL) {
        close(g_async_callback_pipe[0]);
        close(g_async_callback_pipe[1]);
        g_async_callback_pipe[0] = -1;
        g_async_callback_pipe[1] = -1;
        g_async_callback_init_result = -1;
        return;
    }

    g_async_callback_init_result = 0;
}

/**
 * Initialize async callback system.
 * Creates the response pipe and pending futures dict.
 * Thread-safe: uses pthread_once for initialization.
 */
static int async_callback_init(void) {
    pthread_once(&g_async_callback_init_once, async_callback_init_impl);
    return g_async_callback_init_result;
}

/**
 * Process a single async callback response from the pipe.
 * Called by the asyncio reader callback.
 * Returns: 1 if processed, 0 if no data, -1 on error
 */
static int process_async_callback_response(void) {
    /* Read callback_id (8 bytes) + response_len (4 bytes) + response_data */
    uint64_t callback_id;
    uint32_t response_len;
    ssize_t n;

    n = read(g_async_callback_pipe[0], &callback_id, sizeof(callback_id));
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;  /* No data available (non-blocking) */
        }
        return -1;  /* Error */
    }
    if (n == 0) {
        return 0;  /* EOF / No data */
    }
    if (n != sizeof(callback_id)) {
        return -1;  /* Partial read - error */
    }

    n = read(g_async_callback_pipe[0], &response_len, sizeof(response_len));
    if (n != sizeof(response_len)) {
        return -1;
    }

    char *response_data = NULL;
    if (response_len > 0) {
        response_data = enif_alloc(response_len);
        if (response_data == NULL) {
            return -1;
        }
        n = read(g_async_callback_pipe[0], response_data, response_len);
        if (n != (ssize_t)response_len) {
            enif_free(response_data);
            return -1;
        }
    }

    /* Look up and resolve the Future */
    pthread_mutex_lock(&g_async_futures_mutex);

    PyObject *key = PyLong_FromUnsignedLongLong(callback_id);
    PyObject *future = PyDict_GetItem(g_async_pending_futures, key);

    if (future != NULL) {
        Py_INCREF(future);  /* Keep reference while we use it */
        PyDict_DelItem(g_async_pending_futures, key);
    }
    Py_DECREF(key);

    pthread_mutex_unlock(&g_async_futures_mutex);

    if (future != NULL) {
        /* Parse response and resolve Future */
        PyObject *result = NULL;
        if (response_data != NULL) {
            result = parse_callback_response((unsigned char *)response_data, response_len);
        } else {
            Py_INCREF(Py_None);
            result = Py_None;
        }

        if (result != NULL) {
            /* Call future.set_result(result) */
            PyObject *set_result = PyObject_GetAttrString(future, "set_result");
            if (set_result != NULL) {
                PyObject *ret = PyObject_CallFunctionObjArgs(set_result, result, NULL);
                Py_XDECREF(ret);
                Py_DECREF(set_result);
            }
            Py_DECREF(result);
        } else {
            /* Error occurred - set exception on Future */
            PyObject *exc_type, *exc_value, *exc_tb;
            PyErr_Fetch(&exc_type, &exc_value, &exc_tb);

            PyObject *set_exception = PyObject_GetAttrString(future, "set_exception");
            if (set_exception != NULL) {
                if (exc_value != NULL) {
                    PyObject *ret = PyObject_CallFunctionObjArgs(set_exception, exc_value, NULL);
                    Py_XDECREF(ret);
                } else {
                    PyObject *runtime_err = PyObject_CallFunction(PyExc_RuntimeError,
                        "s", "Erlang callback failed");
                    PyObject *ret = PyObject_CallFunctionObjArgs(set_exception, runtime_err, NULL);
                    Py_XDECREF(ret);
                    Py_XDECREF(runtime_err);
                }
                Py_DECREF(set_exception);
            }

            Py_XDECREF(exc_type);
            Py_XDECREF(exc_value);
            Py_XDECREF(exc_tb);
            PyErr_Clear();
        }

        Py_DECREF(future);
    }

    if (response_data != NULL) {
        enif_free(response_data);
    }

    return 1;
}

/**
 * Python callback for asyncio reader.
 * Called when data is available on the async callback pipe.
 */
static PyObject *async_callback_reader(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    /* Process all available responses */
    while (process_async_callback_response() > 0) {
        /* Continue processing */
    }

    Py_RETURN_NONE;
}

/**
 * Get the read file descriptor for the async callback pipe.
 * Used by Python to register with asyncio.
 */
static PyObject *get_async_callback_fd(PyObject *self, PyObject *args) {
    (void)self;
    (void)args;

    /* async_callback_init uses pthread_once, so it's safe to call multiple times */
    if (async_callback_init() < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to initialize async callback system");
        return NULL;
    }

    return PyLong_FromLong(g_async_callback_pipe[0]);
}

/**
 * Send an async callback request to Erlang.
 * Returns the callback_id for tracking.
 */
static PyObject *send_async_callback_request(PyObject *self, PyObject *args) {
    (void)self;

    PyObject *name_obj;
    PyObject *call_args;

    if (!PyArg_ParseTuple(args, "OO", &name_obj, &call_args)) {
        return NULL;
    }

    if (!PyUnicode_Check(name_obj)) {
        PyErr_SetString(PyExc_TypeError, "Function name must be a string");
        return NULL;
    }
    if (!PyTuple_Check(call_args)) {
        PyErr_SetString(PyExc_TypeError, "Arguments must be a tuple");
        return NULL;
    }

    const char *func_name = PyUnicode_AsUTF8(name_obj);
    if (func_name == NULL) {
        return NULL;
    }
    size_t func_name_len = strlen(func_name);

    /* Check if thread worker coordinator is available */
    if (!g_has_thread_coordinator) {
        PyErr_SetString(PyExc_RuntimeError,
            "Thread worker coordinator not initialized. "
            "Ensure erlang_python application is started.");
        return NULL;
    }

    /* Generate callback ID */
    uint64_t callback_id = atomic_fetch_add(&g_callback_id_counter, 1);

    /* Send callback request to Erlang via thread worker coordinator */
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
    ERL_NIF_TERM id_term = enif_make_uint64(msg_env, callback_id);

    /* Send message: {async_callback, CallbackId, FuncName, Args, WriteFd}
     * The WriteFd is the async callback pipe write end */
    ERL_NIF_TERM msg = enif_make_tuple5(msg_env,
        enif_make_atom(msg_env, "async_callback"),
        id_term,
        func_term,
        args_term,
        enif_make_int(msg_env, g_async_callback_pipe[1]));

    if (!enif_send(NULL, &g_thread_coordinator_pid, msg_env, msg)) {
        enif_free_env(msg_env);
        PyErr_SetString(PyExc_RuntimeError, "Failed to send async callback message");
        return NULL;
    }
    enif_free_env(msg_env);

    return PyLong_FromUnsignedLongLong(callback_id);
}

/**
 * Register a Future for an async callback.
 */
static PyObject *register_async_future(PyObject *self, PyObject *args) {
    (void)self;

    unsigned long long callback_id;
    PyObject *future;

    if (!PyArg_ParseTuple(args, "KO", &callback_id, &future)) {
        return NULL;
    }

    pthread_mutex_lock(&g_async_futures_mutex);

    PyObject *key = PyLong_FromUnsignedLongLong(callback_id);
    Py_INCREF(future);
    PyDict_SetItem(g_async_pending_futures, key, future);
    Py_DECREF(key);

    pthread_mutex_unlock(&g_async_futures_mutex);

    Py_RETURN_NONE;
}

/**
 * ErlangFunction.__call__ - forward to erlang_call_impl
 */
static PyObject *ErlangFunction_call(ErlangFunctionObject *self, PyObject *args, PyObject *kwds) {
    (void)kwds;  /* Unused */

    /* Build new args tuple: (name, *args) */
    Py_ssize_t nargs = PyTuple_Size(args);
    PyObject *new_args = PyTuple_New(nargs + 1);
    if (new_args == NULL) {
        return NULL;
    }

    Py_INCREF(self->name);
    PyTuple_SET_ITEM(new_args, 0, self->name);

    for (Py_ssize_t i = 0; i < nargs; i++) {
        PyObject *item = PyTuple_GET_ITEM(args, i);
        Py_INCREF(item);
        PyTuple_SET_ITEM(new_args, i + 1, item);
    }

    /* Call existing erlang_call_impl */
    PyObject *result = erlang_call_impl(NULL, new_args);
    Py_DECREF(new_args);
    return result;
}

/**
 * Module __getattr__ - enables "from erlang import func_name" and "erlang.func_name()"
 *
 * Only returns ErlangFunction wrapper for REGISTERED callback names.
 * This prevents torch and other libraries that introspect module attributes
 * from getting callable objects for arbitrary attribute names.
 */
static PyObject *erlang_module_getattr(PyObject *module, PyObject *name) {
    (void)module;  /* Unused */

    /* Get the name as a C string */
    const char *name_str = PyUnicode_AsUTF8(name);
    if (name_str == NULL) {
        return NULL;  /* Exception already set */
    }
    size_t name_len = strlen(name_str);

    /* Check if this callback is registered */
    if (!is_callback_registered(name_str, name_len)) {
        PyErr_Format(PyExc_AttributeError,
            "module 'erlang' has no attribute '%s'", name_str);
        return NULL;
    }

    /* Return an ErlangFunction wrapper for registered callbacks */
    return ErlangFunction_New(name);
}

/* Python method definitions for erlang module */
static PyMethodDef ErlangModuleMethods[] = {
    {"call", erlang_call_impl, METH_VARARGS,
     "Call a registered Erlang function.\n\n"
     "Usage: erlang.call('func_name', arg1, arg2, ...)\n"
     "Returns: The result from the Erlang function."},
    {"send", erlang_send_impl, METH_VARARGS,
     "Send a message to an Erlang process (fire-and-forget).\n\n"
     "Usage: erlang.send(pid, term)\n"
     "The pid must be an erlang.Pid object."},
    {"_get_async_callback_fd", get_async_callback_fd, METH_NOARGS,
     "Get the file descriptor for async callback responses.\n"
     "Used internally by async_call() to register with asyncio."},
    {"_async_callback_reader", async_callback_reader, METH_NOARGS,
     "Process pending async callback responses.\n"
     "Called by asyncio when the callback pipe has data."},
    {"_send_async_request", send_async_callback_request, METH_VARARGS,
     "Send an async callback request to Erlang.\n"
     "Returns the callback_id for tracking."},
    {"_register_async_future", register_async_future, METH_VARARGS,
     "Register a Future for an async callback.\n"
     "Usage: erlang._register_async_future(callback_id, future)"},
    /* Logging and tracing (from py_logging.c) */
    {"_log", erlang_log_impl, METH_VARARGS,
     "Log message to Erlang logger (fire-and-forget).\n"
     "Usage: erlang._log(level, logger_name, message, metadata)"},
    {"_trace_start", erlang_trace_start_impl, METH_VARARGS,
     "Start a trace span.\n"
     "Usage: erlang._trace_start(name, span_id, parent_id, attrs)"},
    {"_trace_end", erlang_trace_end_impl, METH_VARARGS,
     "End a trace span.\n"
     "Usage: erlang._trace_end(span_id, status, attrs)"},
    {"_trace_event", erlang_trace_event_impl, METH_VARARGS,
     "Add event to a span.\n"
     "Usage: erlang._trace_event(span_id, name, attrs)"},
    {NULL, NULL, 0, NULL}
};

/* Module __getattr__ method definition (for adding to module dict) */
static PyMethodDef getattr_method = {
    "__getattr__", erlang_module_getattr, METH_O,
    "Get an Erlang function wrapper by name."
};

/* Module definition */
static struct PyModuleDef ErlangModuleDef = {
    PyModuleDef_HEAD_INIT,
    "erlang",                           /* Module name */
    "Interface for calling Erlang functions from Python.",  /* Docstring */
    -1,                                 /* Size of per-interpreter state (-1 = global) */
    ErlangModuleMethods                 /* Methods */
};

/**
 * Create and register the 'erlang' module in Python.
 * Called during Python initialization.
 */
static int create_erlang_module(void) {
    /* Initialize cached Python function references */
    init_callback_cache();

    /* Initialize ErlangFunction type */
    if (PyType_Ready(&ErlangFunctionType) < 0) {
        return -1;
    }

    /* Initialize ErlangPid type */
    if (PyType_Ready(&ErlangPidType) < 0) {
        return -1;
    }

    PyObject *module = PyModule_Create(&ErlangModuleDef);
    if (module == NULL) {
        return -1;
    }

    /* Create the SuspensionRequired exception.
     * This exception is raised internally when erlang.call() needs to suspend.
     * It carries callback info in args: (callback_id, func_name, args_tuple) */
    SuspensionRequiredException = PyErr_NewException(
        "erlang.SuspensionRequired", PyExc_BaseException, NULL);
    if (SuspensionRequiredException == NULL) {
        Py_DECREF(module);
        return -1;
    }
    Py_INCREF(SuspensionRequiredException);
    if (PyModule_AddObject(module, "SuspensionRequired", SuspensionRequiredException) < 0) {
        Py_DECREF(SuspensionRequiredException);
        Py_DECREF(module);
        return -1;
    }

    /* Create erlang.ProcessError for dead/unreachable processes */
    ProcessErrorException = PyErr_NewException(
        "erlang.ProcessError", NULL, NULL);
    if (ProcessErrorException == NULL) {
        Py_DECREF(module);
        return -1;
    }
    Py_INCREF(ProcessErrorException);
    if (PyModule_AddObject(module, "ProcessError", ProcessErrorException) < 0) {
        Py_DECREF(ProcessErrorException);
        Py_DECREF(module);
        return -1;
    }

    /* Add ErlangFunction type to module (for introspection) */
    Py_INCREF(&ErlangFunctionType);
    if (PyModule_AddObject(module, "Function", (PyObject *)&ErlangFunctionType) < 0) {
        Py_DECREF(&ErlangFunctionType);
        Py_DECREF(module);
        return -1;
    }

    /* Add ErlangPid type to module */
    Py_INCREF(&ErlangPidType);
    if (PyModule_AddObject(module, "Pid", (PyObject *)&ErlangPidType) < 0) {
        Py_DECREF(&ErlangPidType);
        Py_DECREF(module);
        return -1;
    }

    /* Add __getattr__ to enable "from erlang import name" and "erlang.name()" syntax
     * Module __getattr__ (PEP 562) needs to be set as an attribute on the module dict */
    PyObject *getattr_func = PyCFunction_New(&getattr_method, module);
    if (getattr_func == NULL) {
        Py_DECREF(module);
        return -1;
    }
    if (PyModule_AddObject(module, "__getattr__", getattr_func) < 0) {
        Py_DECREF(getattr_func);
        Py_DECREF(module);
        return -1;
    }

    /* Add module to sys.modules */
    PyObject *sys_modules = PyImport_GetModuleDict();
    if (PyDict_SetItemString(sys_modules, "erlang", module) < 0) {
        Py_DECREF(module);
        return -1;
    }

    /* Add the async_call() coroutine function.
     * This is implemented in Python for easier asyncio integration. */
    const char *async_call_code =
        "import asyncio\n"
        "import erlang\n"
        "\n"
        "# Track if we've registered the reader with the event loop\n"
        "_async_reader_registered = {}\n"
        "\n"
        "async def async_call(func_name, *args):\n"
        "    '''\n"
        "    Call an Erlang function asynchronously.\n"
        "    \n"
        "    This is safe to use from asyncio code:\n"
        "    - No exceptions raised for control flow\n"
        "    - Integrates with asyncio event loop\n"
        "    - Releases dirty NIF thread while waiting\n"
        "    \n"
        "    Usage:\n"
        "        result = await erlang.async_call('my_function', arg1, arg2)\n"
        "    \n"
        "    Args:\n"
        "        func_name: Name of the registered Erlang function\n"
        "        *args: Arguments to pass to the function\n"
        "    \n"
        "    Returns:\n"
        "        The result from the Erlang function\n"
        "    '''\n"
        "    loop = asyncio.get_running_loop()\n"
        "    \n"
        "    # Ensure the reader is registered with this event loop\n"
        "    loop_id = id(loop)\n"
        "    if loop_id not in _async_reader_registered:\n"
        "        fd = erlang._get_async_callback_fd()\n"
        "        loop.add_reader(fd, erlang._async_callback_reader)\n"
        "        _async_reader_registered[loop_id] = True\n"
        "    \n"
        "    # Create a Future for this call\n"
        "    future = loop.create_future()\n"
        "    \n"
        "    # Send the request and get callback_id\n"
        "    callback_id = erlang._send_async_request(func_name, args)\n"
        "    \n"
        "    # Register the Future\n"
        "    erlang._register_async_future(callback_id, future)\n"
        "    \n"
        "    # Wait for the result\n"
        "    return await future\n"
        "\n"
        "# Add async_call to the erlang module\n"
        "erlang.async_call = async_call\n"
        "erlang._async_reader_registered = _async_reader_registered\n";

    PyObject *globals = PyDict_New();
    if (globals == NULL) {
        /* Non-fatal - async_call just won't be available */
        PyErr_Clear();
    } else {
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(globals, "__builtins__", builtins);

        PyObject *result = PyRun_String(async_call_code, Py_file_input, globals, globals);
        if (result == NULL) {
            /* Non-fatal - async_call just won't be available */
            PyErr_Print();
            PyErr_Clear();
        } else {
            Py_DECREF(result);
        }
        Py_DECREF(globals);
    }

    /* Inject logging and tracing Python code */
    const char *erlang_logging_code =
        "import logging\n"
        "import threading\n"
        "import random\n"
        "\n"
        "class ErlangHandler(logging.Handler):\n"
        "    '''Logging handler that forwards log records to Erlang logger.'''\n"
        "    def emit(self, record):\n"
        "        try:\n"
        "            msg = self.format(record)\n"
        "            meta = {'module': record.module, 'lineno': record.lineno,\n"
        "                    'funcName': record.funcName}\n"
        "            erlang._log(record.levelno, record.name, msg, meta)\n"
        "        except:\n"
        "            pass\n"
        "\n"
        "def setup_logging(level=10, format=None):\n"
        "    '''Set up Python logging to forward to Erlang.\n"
        "    \n"
        "    Args:\n"
        "        level: Minimum log level (10=DEBUG, 20=INFO, 30=WARNING, etc.)\n"
        "        format: Optional format string\n"
        "    \n"
        "    Returns:\n"
        "        The created ErlangHandler instance\n"
        "    '''\n"
        "    handler = ErlangHandler()\n"
        "    if format:\n"
        "        handler.setFormatter(logging.Formatter(format))\n"
        "    else:\n"
        "        handler.setFormatter(logging.Formatter('%(message)s'))\n"
        "    root = logging.getLogger()\n"
        "    root.addHandler(handler)\n"
        "    root.setLevel(level)\n"
        "    return handler\n"
        "\n"
        "# Thread-local span context for tracing\n"
        "_span_ctx = threading.local()\n"
        "\n"
        "class Span:\n"
        "    '''Context manager for tracing spans.\n"
        "    \n"
        "    Usage:\n"
        "        with erlang.Span('operation-name', key='value') as span:\n"
        "            do_work()\n"
        "            span.event('checkpoint', items=10)\n"
        "    '''\n"
        "    def __init__(self, name, **attrs):\n"
        "        self.name = name\n"
        "        self.span_id = random.getrandbits(64)\n"
        "        self.attrs = attrs\n"
        "        self._prev = None\n"
        "\n"
        "    def __enter__(self):\n"
        "        self._prev = getattr(_span_ctx, 'current', None)\n"
        "        _span_ctx.current = self.span_id\n"
        "        erlang._trace_start(self.name, self.span_id, self._prev, self.attrs)\n"
        "        return self\n"
        "\n"
        "    def __exit__(self, et, ev, tb):\n"
        "        status = 'error' if et else 'ok'\n"
        "        attrs = {}\n"
        "        if et:\n"
        "            attrs['exception'] = str(ev)\n"
        "        erlang._trace_end(self.span_id, status, attrs)\n"
        "        _span_ctx.current = self._prev\n"
        "        return False\n"
        "\n"
        "    def event(self, name, **attrs):\n"
        "        '''Add an event to this span.'''\n"
        "        erlang._trace_event(self.span_id, name, attrs)\n"
        "\n"
        "def trace(name=None):\n"
        "    '''Decorator to trace a function.\n"
        "    \n"
        "    Usage:\n"
        "        @erlang.trace()\n"
        "        def my_function():\n"
        "            pass\n"
        "    '''\n"
        "    def decorator(fn):\n"
        "        span_name = name or f'{fn.__module__}.{fn.__qualname__}'\n"
        "        def wrapper(*a, **kw):\n"
        "            with Span(span_name):\n"
        "                return fn(*a, **kw)\n"
        "        return wrapper\n"
        "    return decorator\n"
        "\n"
        "# Add to erlang module\n"
        "erlang.ErlangHandler = ErlangHandler\n"
        "erlang.setup_logging = setup_logging\n"
        "erlang.Span = Span\n"
        "erlang.trace = trace\n";

    PyObject *log_globals = PyDict_New();
    if (log_globals != NULL) {
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(log_globals, "__builtins__", builtins);

        /* Import erlang module into globals so the code can reference it */
        PyObject *sys_modules = PySys_GetObject("modules");
        if (sys_modules != NULL) {
            PyObject *erlang_mod = PyDict_GetItemString(sys_modules, "erlang");
            if (erlang_mod != NULL) {
                PyDict_SetItemString(log_globals, "erlang", erlang_mod);
            }
        }

        PyObject *result = PyRun_String(erlang_logging_code, Py_file_input, log_globals, log_globals);
        if (result == NULL) {
            /* Non-fatal - logging features just won't be available */
            PyErr_Print();
            PyErr_Clear();
        } else {
            Py_DECREF(result);
        }
        Py_DECREF(log_globals);
    }

    /* Add helper to extend erlang module with Python package exports.
     * Called from Erlang after priv_dir is added to sys.path.
     * Follows uvloop's minimal export pattern.
     */
    const char *extend_code =
        "def _extend_erlang_module(priv_dir):\n"
        "    '''\n"
        "    Extend the C erlang module with Python event loop exports.\n"
        "    \n"
        "    Called from Erlang after priv_dir is set up in sys.path.\n"
        "    This allows the C 'erlang' module to also provide:\n"
        "      - erlang.run()\n"
        "      - erlang.new_event_loop()\n"
        "      - erlang.get_event_loop_policy()\n"
        "      - erlang.install()\n"
        "      - erlang.EventLoopPolicy\n"
        "      - erlang.ErlangEventLoop\n"
        "    \n"
        "    Args:\n"
        "        priv_dir: Path to erlang_python priv directory (bytes or str)\n"
        "    \n"
        "    Returns:\n"
        "        True on success, False on failure\n"
        "    '''\n"
        "    import sys\n"
        "    # Handle bytes from Erlang\n"
        "    if isinstance(priv_dir, bytes):\n"
        "        priv_dir = priv_dir.decode('utf-8')\n"
        "    if priv_dir not in sys.path:\n"
        "        sys.path.insert(0, priv_dir)\n"
        "    try:\n"
        "        import _erlang_impl\n"
        "        import erlang\n"
        "        # Primary exports (uvloop-compatible)\n"
        "        erlang.run = _erlang_impl.run\n"
        "        erlang.new_event_loop = _erlang_impl.new_event_loop\n"
        "        erlang.ErlangEventLoop = _erlang_impl.ErlangEventLoop\n"
        "        # Deprecated (Python < 3.16)\n"
        "        erlang.install = _erlang_impl.install\n"
        "        erlang.EventLoopPolicy = _erlang_impl.EventLoopPolicy\n"
        "        erlang.ErlangEventLoopPolicy = _erlang_impl.ErlangEventLoopPolicy\n"
        "        # Additional exports for compatibility\n"
        "        erlang.get_event_loop_policy = _erlang_impl.get_event_loop_policy\n"
        "        erlang.detect_mode = _erlang_impl.detect_mode\n"
        "        erlang.ExecutionMode = _erlang_impl.ExecutionMode\n"
        "        # Reactor for fd-based protocol handling\n"
        "        erlang.reactor = _erlang_impl.reactor\n"
        "        # Make erlang behave as a package for 'import erlang.reactor' syntax\n"
        "        erlang.__path__ = [priv_dir]\n"
        "        sys.modules['erlang.reactor'] = erlang.reactor\n"
        "        return True\n"
        "    except ImportError as e:\n"
        "        import sys\n"
        "        sys.stderr.write(f'Failed to extend erlang module: {e}\\n')\n"
        "        return False\n"
        "\n"
        "import erlang\n"
        "erlang._extend_erlang_module = _extend_erlang_module\n";

    PyObject *ext_globals = PyDict_New();
    if (ext_globals != NULL) {
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(ext_globals, "__builtins__", builtins);

        /* Import erlang module into globals so the code can reference it */
        PyObject *sys_modules = PySys_GetObject("modules");
        if (sys_modules != NULL) {
            PyObject *erlang_mod = PyDict_GetItemString(sys_modules, "erlang");
            if (erlang_mod != NULL) {
                PyDict_SetItemString(ext_globals, "erlang", erlang_mod);
            }
        }

        PyObject *result = PyRun_String(extend_code, Py_file_input, ext_globals, ext_globals);
        if (result == NULL) {
            /* Non-fatal - extension will be called from Erlang */
            PyErr_Print();
            PyErr_Clear();
        } else {
            Py_DECREF(result);
        }
        Py_DECREF(ext_globals);
    }

    return 0;
}

/* ============================================================================
 * Asyncio support (DEPRECATED - replaced by event loop model)
 *
 * The async_future_callback and async_event_loop_thread functions have been
 * removed. Async coroutine execution is now handled by py_event_loop and
 * py_event_loop_pool using enif_select and erlang.send() for efficient
 * event-driven operation without pthread polling.
 * ============================================================================ */

/* ============================================================================
 * Resume callback NIFs
 * ============================================================================ */

/* Forward declaration for the dirty resume NIF */
static ERL_NIF_TERM nif_resume_callback_dirty(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/**
 * Resume a suspended callback by storing the result and scheduling replay.
 *
 * Args: StateRef, ResultBinary
 *
 * This NIF stores the callback result in the suspended state and schedules
 * a dirty NIF (nif_resume_callback_dirty) to replay the Python code.
 */
static ERL_NIF_TERM nif_resume_callback(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    suspended_state_t *state;
    ErlNifBinary result_bin;

    if (!enif_get_resource(env, argv[0], SUSPENDED_STATE_RESOURCE_TYPE, (void **)&state)) {
        return make_error(env, "invalid_state_ref");
    }

    if (!enif_inspect_binary(env, argv[1], &result_bin)) {
        return make_error(env, "invalid_result");
    }

    /* Store the result in the suspended state */
    pthread_mutex_lock(&state->mutex);

    /* Copy result data */
    state->result_data = enif_alloc(result_bin.size);
    if (state->result_data == NULL) {
        pthread_mutex_unlock(&state->mutex);
        return make_error(env, "alloc_failed");
    }
    memcpy(state->result_data, result_bin.data, result_bin.size);
    state->result_len = result_bin.size;
    state->has_result = true;
    state->is_error = false;

    pthread_mutex_unlock(&state->mutex);

    /*
     * Schedule the dirty resume NIF.
     * This allows the current NIF to return immediately, and the dirty NIF
     * will handle the Python replay on a dirty scheduler.
     */
    ERL_NIF_TERM new_argv[1] = { argv[0] };  /* Pass StateRef to dirty NIF */
    return enif_schedule_nif(env, "resume_callback_dirty",
        ERL_NIF_DIRTY_JOB_IO_BOUND, nif_resume_callback_dirty, 1, new_argv);
}

/**
 * Dirty NIF that replays Python code with the cached callback result.
 *
 * This is scheduled by nif_resume_callback and runs on a dirty I/O scheduler.
 * It sets tl_current_suspended so erlang_call_impl can return the cached result,
 * then re-runs the original Python code. When Python hits erlang.call() again,
 * it gets the cached result and continues normally.
 */
static ERL_NIF_TERM nif_resume_callback_dirty(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    suspended_state_t *state;

    if (!enif_get_resource(env, argv[0], SUSPENDED_STATE_RESOURCE_TYPE, (void **)&state)) {
        return make_error(env, "invalid_state_ref");
    }

    /* Verify the state has a result */
    if (!state->has_result) {
        return make_error(env, "no_result");
    }

    /* Set up thread-local state for replay */
    tl_current_worker = state->worker;
    tl_callback_env = env;
    tl_current_suspended = state;  /* erlang_call_impl will check this */
    tl_allow_suspension = true;

    ERL_NIF_TERM result;

    if (state->request_type == PY_REQ_CALL) {
        /* Replay a py:call */
        char *module_name = enif_alloc(state->orig_module.size + 1);
        char *func_name = enif_alloc(state->orig_func.size + 1);

        if (module_name == NULL || func_name == NULL) {
            enif_free(module_name);
            enif_free(func_name);
            tl_current_suspended = NULL;
            return make_error(env, "alloc_failed");
        }

        memcpy(module_name, state->orig_module.data, state->orig_module.size);
        module_name[state->orig_module.size] = '\0';
        memcpy(func_name, state->orig_func.data, state->orig_func.size);
        func_name[state->orig_func.size] = '\0';

        PyGILState_STATE gstate = PyGILState_Ensure();

        PyObject *func = NULL;

        /* Get the function (same logic as process_request) */
        if (strcmp(module_name, "__main__") == 0) {
            func = PyDict_GetItemString(state->worker->locals, func_name);
            if (func == NULL) {
                func = PyDict_GetItemString(state->worker->globals, func_name);
            }
            if (func != NULL) {
                Py_INCREF(func);
            } else {
                PyErr_Format(PyExc_NameError, "name '%s' is not defined", func_name);
                result = make_py_error(env);
                goto call_cleanup;
            }
        } else {
            PyObject *module = PyImport_ImportModule(module_name);
            if (module == NULL) {
                result = make_py_error(env);
                goto call_cleanup;
            }
            func = PyObject_GetAttrString(module, func_name);
            Py_DECREF(module);
        }

        if (func == NULL) {
            result = make_py_error(env);
            goto call_cleanup;
        }

        /* Convert args */
        unsigned int args_len;
        if (!enif_get_list_length(state->orig_env, state->orig_args, &args_len)) {
            Py_DECREF(func);
            result = make_error(env, "invalid_args");
            goto call_cleanup;
        }

        PyObject *args = PyTuple_New(args_len);
        ERL_NIF_TERM head, tail = state->orig_args;
        for (unsigned int i = 0; i < args_len; i++) {
            enif_get_list_cell(state->orig_env, tail, &head, &tail);
            PyObject *arg = term_to_py(state->orig_env, head);
            if (arg == NULL) {
                Py_DECREF(args);
                Py_DECREF(func);
                result = make_error(env, "arg_conversion_failed");
                goto call_cleanup;
            }
            PyTuple_SET_ITEM(args, i, arg);
        }

        /* Convert kwargs */
        PyObject *kwargs = NULL;
        if (enif_is_map(state->orig_env, state->orig_kwargs)) {
            kwargs = term_to_py(state->orig_env, state->orig_kwargs);
        }

        /* Call the function (this will hit erlang.call which returns cached result) */
        PyObject *py_result = PyObject_Call(func, args, kwargs);

        Py_DECREF(func);
        Py_DECREF(args);
        Py_XDECREF(kwargs);

        if (py_result == NULL) {
            if (tl_pending_callback) {
                /*
                 * Flag-based callback detection during replay.
                 * Check flag FIRST, not exception type - this works even if
                 * Python code caught and re-raised the exception.
                 */
                PyErr_Clear();  /* Clear whatever exception is set */

                /* Build exc_args tuple from thread-local storage */
                PyObject *exc_args = build_pending_callback_exc_args();
                if (exc_args == NULL) {
                    result = make_error(env, "build_exc_args_failed");
                } else {
                    suspended_state_t *new_suspended = create_suspended_state_from_existing(env, exc_args, state);
                    Py_DECREF(exc_args);
                    if (new_suspended == NULL) {
                        tl_pending_callback = false;
                        result = make_error(env, "create_nested_suspended_state_failed");
                    } else {
                        result = build_suspended_result(env, new_suspended);
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

    call_cleanup:
        PyGILState_Release(gstate);
        enif_free(module_name);
        enif_free(func_name);

    } else if (state->request_type == PY_REQ_EVAL) {
        /* Replay a py:eval */
        char *code = enif_alloc(state->orig_code.size + 1);
        if (code == NULL) {
            tl_current_suspended = NULL;
            return make_error(env, "alloc_failed");
        }
        memcpy(code, state->orig_code.data, state->orig_code.size);
        code[state->orig_code.size] = '\0';

        PyGILState_STATE gstate = PyGILState_Ensure();

        /* Update locals if provided */
        if (enif_is_map(state->orig_env, state->orig_locals)) {
            PyObject *new_locals = term_to_py(state->orig_env, state->orig_locals);
            if (new_locals != NULL && PyDict_Check(new_locals)) {
                PyDict_Update(state->worker->locals, new_locals);
                Py_DECREF(new_locals);
            }
        }

        /* Compile and evaluate */
        PyObject *compiled = Py_CompileString(code, "<erlang>", Py_eval_input);

        if (compiled == NULL) {
            result = make_py_error(env);
        } else {
            PyObject *py_result = PyEval_EvalCode(compiled, state->worker->globals,
                                                   state->worker->locals);
            Py_DECREF(compiled);

            if (py_result == NULL) {
                if (tl_pending_callback) {
                    /*
                     * Flag-based callback detection during eval replay.
                     * Check flag FIRST, not exception type - this works even if
                     * Python code caught and re-raised the exception.
                     */
                    PyErr_Clear();  /* Clear whatever exception is set */

                    /* Build exc_args tuple from thread-local storage */
                    PyObject *exc_args = build_pending_callback_exc_args();
                    if (exc_args == NULL) {
                        result = make_error(env, "build_exc_args_failed");
                    } else {
                        suspended_state_t *new_suspended = create_suspended_state_from_existing(env, exc_args, state);
                        Py_DECREF(exc_args);
                        if (new_suspended == NULL) {
                            tl_pending_callback = false;
                            result = make_error(env, "create_nested_suspended_state_failed");
                        } else {
                            result = build_suspended_result(env, new_suspended);
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
        }

        PyGILState_Release(gstate);
        enif_free(code);

    } else {
        result = make_error(env, "unsupported_request_type");
    }

    /* Clear thread-local state */
    tl_current_worker = NULL;
    tl_callback_env = NULL;
    tl_current_suspended = NULL;
    tl_allow_suspension = false;

    return result;
}

/* ============================================================================
 * NIF functions for callback name registration
 * ============================================================================ */

/**
 * @brief NIF to register a callback name in the C-side registry
 *
 * This allows the erlang module's __getattr__ to return ErlangFunction
 * wrappers only for registered callbacks, preventing introspection issues.
 *
 * Args: Name (binary or atom)
 * Returns: ok | {error, Reason}
 */
static ERL_NIF_TERM nif_register_callback_name(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    ErlNifBinary name_bin;
    char atom_buf[256];

    const char *name;
    size_t name_len;

    if (enif_inspect_binary(env, argv[0], &name_bin)) {
        name = (const char *)name_bin.data;
        name_len = name_bin.size;
    } else if (enif_get_atom(env, argv[0], atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
        name = atom_buf;
        name_len = strlen(atom_buf);
    } else {
        return make_error(env, "invalid_name");
    }

    if (register_callback_name(name, name_len) < 0) {
        return make_error(env, "registration_failed");
    }

    return ATOM_OK;
}

/**
 * @brief NIF to unregister a callback name from the C-side registry
 *
 * Args: Name (binary or atom)
 * Returns: ok
 */
static ERL_NIF_TERM nif_unregister_callback_name(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    ErlNifBinary name_bin;
    char atom_buf[256];

    const char *name;
    size_t name_len;

    if (enif_inspect_binary(env, argv[0], &name_bin)) {
        name = (const char *)name_bin.data;
        name_len = name_bin.size;
    } else if (enif_get_atom(env, argv[0], atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
        name = atom_buf;
        name_len = strlen(atom_buf);
    } else {
        return make_error(env, "invalid_name");
    }

    unregister_callback_name(name, name_len);

    return ATOM_OK;
}
