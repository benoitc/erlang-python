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
 * Suspended state management
 * ============================================================================ */

/**
 * Check if a SuspensionRequired exception is pending.
 * Returns true if the exception is set and matches SuspensionRequiredException.
 */
static bool is_suspension_exception(void) {
    if (!PyErr_Occurred()) {
        return false;
    }
    return PyErr_ExceptionMatches(SuspensionRequiredException);
}

/**
 * Extract suspension info from the pending SuspensionRequired exception.
 * Returns the exception args tuple (callback_id, func_name, args) or NULL.
 * Clears the exception if successful.
 */
static PyObject *get_suspension_args(void) {
    PyObject *exc_type, *exc_value, *exc_tb;
    PyErr_Fetch(&exc_type, &exc_value, &exc_tb);

    if (exc_value == NULL) {
        Py_XDECREF(exc_type);
        Py_XDECREF(exc_tb);
        return NULL;
    }

    /* Get the args from the exception - it's a tuple (callback_id, func_name, args) */
    PyObject *args = PyObject_GetAttrString(exc_value, "args");
    Py_DECREF(exc_type);
    Py_DECREF(exc_value);
    Py_XDECREF(exc_tb);

    if (args == NULL || !PyTuple_Check(args) || PyTuple_Size(args) != 3) {
        Py_XDECREF(args);
        return NULL;
    }

    return args;  /* Caller owns this reference */
}

/**
 * Create a suspended state resource from exception args.
 * Args tuple format: (callback_id, func_name, args)
 * Also stores the original request context for replay.
 * Returns the suspended state or NULL on error.
 */
static suspended_state_t *create_suspended_state(ErlNifEnv *env, PyObject *exc_args,
                                                  py_request_t *req) {
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
    state->worker = tl_current_worker;
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

    /* Store original request context for replay */
    state->request_type = req->type;
    state->orig_timeout_ms = req->timeout_ms;

    /* Create environment to hold copied terms */
    state->orig_env = enif_alloc_env();
    if (state->orig_env == NULL) {
        Py_DECREF(callback_args);
        state->callback_args = NULL;
        enif_free(state->callback_func_name);
        state->callback_func_name = NULL;
        enif_release_resource(state);
        return NULL;
    }

    /* Copy request-specific data */
    if (req->type == PY_REQ_CALL) {
        /* Copy module and function binaries */
        if (!enif_alloc_binary(req->module_bin.size, &state->orig_module)) {
            Py_DECREF(callback_args);
            state->callback_args = NULL;
            enif_free(state->callback_func_name);
            state->callback_func_name = NULL;
            enif_free_env(state->orig_env);
            state->orig_env = NULL;
            enif_release_resource(state);
            return NULL;
        }
        memcpy(state->orig_module.data, req->module_bin.data, req->module_bin.size);

        if (!enif_alloc_binary(req->func_bin.size, &state->orig_func)) {
            enif_release_binary(&state->orig_module);
            Py_DECREF(callback_args);
            state->callback_args = NULL;
            enif_free(state->callback_func_name);
            state->callback_func_name = NULL;
            enif_free_env(state->orig_env);
            state->orig_env = NULL;
            enif_release_resource(state);
            return NULL;
        }
        memcpy(state->orig_func.data, req->func_bin.data, req->func_bin.size);

        /* Copy args and kwargs to our environment */
        state->orig_args = enif_make_copy(state->orig_env, req->args_term);
        state->orig_kwargs = enif_make_copy(state->orig_env, req->kwargs_term);
    } else if (req->type == PY_REQ_EVAL) {
        /* Copy code binary */
        if (!enif_alloc_binary(req->code_bin.size, &state->orig_code)) {
            Py_DECREF(callback_args);
            state->callback_args = NULL;
            enif_free(state->callback_func_name);
            state->callback_func_name = NULL;
            enif_free_env(state->orig_env);
            state->orig_env = NULL;
            enif_release_resource(state);
            return NULL;
        }
        memcpy(state->orig_code.data, req->code_bin.data, req->code_bin.size);

        /* Copy locals */
        state->orig_locals = enif_make_copy(state->orig_env, req->locals_term);
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
 * Create a new suspended state from an existing one (for nested suspensions).
 * Used when a second erlang.call() is made during replay.
 */
static suspended_state_t *create_suspended_state_from_existing(
    ErlNifEnv *env, PyObject *exc_args, suspended_state_t *existing) {

    if (!PyTuple_Check(exc_args) || PyTuple_Size(exc_args) != 3) {
        return NULL;
    }

    PyObject *callback_id_obj = PyTuple_GetItem(exc_args, 0);
    PyObject *func_name_obj = PyTuple_GetItem(exc_args, 1);
    PyObject *callback_args = PyTuple_GetItem(exc_args, 2);

    if (!PyLong_Check(callback_id_obj) || !PyUnicode_Check(func_name_obj)) {
        return NULL;
    }

    /* Allocate the new suspended state resource */
    suspended_state_t *state = enif_alloc_resource(
        SUSPENDED_STATE_RESOURCE_TYPE, sizeof(suspended_state_t));
    if (state == NULL) {
        return NULL;
    }

    /* Initialize the state */
    memset(state, 0, sizeof(suspended_state_t));
    state->worker = existing->worker;  /* Same worker */
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

    /* Copy original request context from existing state */
    state->request_type = existing->request_type;
    state->orig_timeout_ms = existing->orig_timeout_ms;

    /* Create environment to hold copied terms */
    state->orig_env = enif_alloc_env();
    if (state->orig_env == NULL) {
        Py_DECREF(callback_args);
        state->callback_args = NULL;
        enif_free(state->callback_func_name);
        state->callback_func_name = NULL;
        enif_release_resource(state);
        return NULL;
    }

    /* Copy request-specific data from existing state */
    if (existing->request_type == PY_REQ_CALL) {
        /* Copy module binary */
        if (!enif_alloc_binary(existing->orig_module.size, &state->orig_module)) {
            Py_DECREF(callback_args);
            state->callback_args = NULL;
            enif_free(state->callback_func_name);
            state->callback_func_name = NULL;
            enif_free_env(state->orig_env);
            state->orig_env = NULL;
            enif_release_resource(state);
            return NULL;
        }
        memcpy(state->orig_module.data, existing->orig_module.data, existing->orig_module.size);

        /* Copy function binary */
        if (!enif_alloc_binary(existing->orig_func.size, &state->orig_func)) {
            enif_release_binary(&state->orig_module);
            Py_DECREF(callback_args);
            state->callback_args = NULL;
            enif_free(state->callback_func_name);
            state->callback_func_name = NULL;
            enif_free_env(state->orig_env);
            state->orig_env = NULL;
            enif_release_resource(state);
            return NULL;
        }
        memcpy(state->orig_func.data, existing->orig_func.data, existing->orig_func.size);

        /* Copy args and kwargs to our environment */
        state->orig_args = enif_make_copy(state->orig_env, existing->orig_args);
        state->orig_kwargs = enif_make_copy(state->orig_env, existing->orig_kwargs);
    } else if (existing->request_type == PY_REQ_EVAL) {
        /* Copy code binary */
        if (!enif_alloc_binary(existing->orig_code.size, &state->orig_code)) {
            Py_DECREF(callback_args);
            state->callback_args = NULL;
            enif_free(state->callback_func_name);
            state->callback_func_name = NULL;
            enif_free_env(state->orig_env);
            state->orig_env = NULL;
            enif_release_resource(state);
            return NULL;
        }
        memcpy(state->orig_code.data, existing->orig_code.data, existing->orig_code.size);

        /* Copy locals */
        state->orig_locals = enif_make_copy(state->orig_env, existing->orig_locals);
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
 * Helper to build {suspended, CallbackId, StateRef, {FuncName, Args}} term.
 */
static ERL_NIF_TERM make_suspended_term(ErlNifEnv *env, suspended_state_t *suspended,
                                         PyObject *exc_args) {
    PyObject *callback_id_obj = PyTuple_GetItem(exc_args, 0);
    PyObject *func_name_obj = PyTuple_GetItem(exc_args, 1);
    PyObject *call_args_obj = PyTuple_GetItem(exc_args, 2);

    uint64_t callback_id = PyLong_AsUnsignedLongLong(callback_id_obj);
    Py_ssize_t fn_len;
    const char *fn = PyUnicode_AsUTF8AndSize(func_name_obj, &fn_len);

    ERL_NIF_TERM state_ref = enif_make_resource(env, suspended);
    enif_release_resource(suspended);  /* Erlang now holds the reference */

    ERL_NIF_TERM callback_id_term = enif_make_uint64(env, callback_id);

    ERL_NIF_TERM func_name_term;
    unsigned char *fn_buf = enif_make_new_binary(env, fn_len, &func_name_term);
    memcpy(fn_buf, fn, fn_len);

    ERL_NIF_TERM args_term = py_to_term(env, call_args_obj);

    return enif_make_tuple4(env,
        ATOM_SUSPENDED,
        callback_id_term,
        state_ref,
        enif_make_tuple2(env, func_name_term, args_term));
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
        /* Try to evaluate the result string as Python literal */
        PyObject *ast_module = PyImport_ImportModule("ast");
        if (ast_module != NULL) {
            PyObject *literal_eval = PyObject_GetAttrString(ast_module, "literal_eval");
            if (literal_eval != NULL) {
                PyObject *arg = PyUnicode_FromStringAndSize(result_str, result_len);
                if (arg != NULL) {
                    result = PyObject_CallFunctionObjArgs(literal_eval, arg, NULL);
                    Py_DECREF(arg);
                    if (result == NULL) {
                        /* If literal_eval fails, return as string */
                        PyErr_Clear();
                        result = PyUnicode_FromStringAndSize(result_str, result_len);
                    }
                }
                Py_DECREF(literal_eval);
            }
            Py_DECREF(ast_module);
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
     * Check if this is a call from an executor thread (normal path) or
     * from a spawned thread (thread worker path).
     */
    if (tl_current_worker == NULL || !tl_current_worker->has_callback_handler) {
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

    /*
     * FIX for multiple sequential erlang.call():
     * If we're in replay context (tl_current_suspended != NULL) but didn't get
     * a cache hit above, this is a SUBSEQUENT call (e.g., second erlang.call()
     * in the same Python function). We MUST NOT suspend again - that would
     * cause an infinite loop where replay always hits this second call.
     * Instead, fall through to blocking pipe behavior for subsequent calls.
     */
    bool force_blocking = (tl_current_suspended != NULL);

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

        uint32_t response_len = 0;
        ssize_t n;
        char *response_data = NULL;

        Py_BEGIN_ALLOW_THREADS
        enif_send(NULL, &tl_current_worker->callback_handler, msg_env, msg);
        enif_free_env(msg_env);
        n = read(tl_current_worker->callback_pipe[0], &response_len, sizeof(response_len));
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
        n = read(tl_current_worker->callback_pipe[0], response_data, response_len);
        Py_END_ALLOW_THREADS

        if (n != (ssize_t)response_len) {
            enif_free(response_data);
            PyErr_SetString(PyExc_RuntimeError, "Failed to read callback response data");
            return NULL;
        }

        PyObject *result = parse_callback_response((unsigned char *)response_data, response_len);
        enif_free(response_data);
        return result;
    }

    /*
     * Suspension is allowed - raise SuspensionRequired exception.
     *
     * Unlike returning a marker tuple, raising an exception properly interrupts
     * Python execution even in the middle of an expression like:
     *   erlang.call('foo', x) + 1
     *
     * The executor (process_request) catches this exception and:
     * 1. Creates a suspended state resource
     * 2. Returns {suspended, CallbackId, StateRef, {Func, Args}} to Erlang
     * 3. The dirty scheduler is freed
     *
     * When Erlang calls resume_callback with the result:
     * 1. The result is stored in the suspended state
     * 2. A dirty NIF is scheduled to re-run the Python code
     * 3. On re-run, this function finds the cached result and returns it
     */
    uint64_t callback_id = atomic_fetch_add(&g_callback_id_counter, 1);

    /* Create exception args tuple: (callback_id, func_name, args) */
    PyObject *exc_args = PyTuple_New(3);
    if (exc_args == NULL) {
        Py_DECREF(call_args);
        return NULL;
    }

    PyObject *callback_id_obj = PyLong_FromUnsignedLongLong(callback_id);
    PyObject *func_name_obj = PyUnicode_FromString(func_name);

    if (callback_id_obj == NULL || func_name_obj == NULL) {
        Py_XDECREF(callback_id_obj);
        Py_XDECREF(func_name_obj);
        Py_DECREF(call_args);
        Py_DECREF(exc_args);
        return NULL;
    }

    PyTuple_SET_ITEM(exc_args, 0, callback_id_obj); /* Takes ownership */
    PyTuple_SET_ITEM(exc_args, 1, func_name_obj);   /* Takes ownership */
    PyTuple_SET_ITEM(exc_args, 2, call_args);       /* Takes ownership */

    /* Raise the exception - Python will unwind the stack */
    PyErr_SetObject(SuspensionRequiredException, exc_args);
    Py_DECREF(exc_args);  /* SetObject increfs, so we decref our reference */

    return NULL;  /* Signals exception was raised */
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
 */
static PyObject *erlang_module_getattr(PyObject *module, PyObject *name) {
    (void)module;  /* Unused */
    /* Return an ErlangFunction wrapper for any attribute access */
    return ErlangFunction_New(name);
}

/* Python method definitions for erlang module */
static PyMethodDef ErlangModuleMethods[] = {
    {"call", erlang_call_impl, METH_VARARGS,
     "Call a registered Erlang function.\n\n"
     "Usage: erlang.call('func_name', arg1, arg2, ...)\n"
     "Returns: The result from the Erlang function."},
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
    /* Initialize ErlangFunction type */
    if (PyType_Ready(&ErlangFunctionType) < 0) {
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
        "erlang.SuspensionRequired", NULL, NULL);
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

    /* Add ErlangFunction type to module (for introspection) */
    Py_INCREF(&ErlangFunctionType);
    if (PyModule_AddObject(module, "Function", (PyObject *)&ErlangFunctionType) < 0) {
        Py_DECREF(&ErlangFunctionType);
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

    return 0;
}

/* ============================================================================
 * Asyncio support
 * ============================================================================ */

/**
 * Callback function that gets invoked when a future completes.
 * This is called from within the event loop thread.
 */
static void async_future_callback(py_async_worker_t *worker, async_pending_t *pending) {
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        /* Cannot send result - just log and return */
        return;
    }
    PyObject *py_result = PyObject_CallMethod(pending->future, "result", NULL);

    ERL_NIF_TERM result_term;
    if (py_result == NULL) {
        /* Exception occurred */
        PyObject *exc = PyObject_CallMethod(pending->future, "exception", NULL);
        if (exc != NULL && exc != Py_None) {
            PyObject *str = PyObject_Str(exc);
            const char *err_msg = str ? PyUnicode_AsUTF8(str) : "unknown";
            result_term = enif_make_tuple2(msg_env, ATOM_ERROR,
                enif_make_string(msg_env, err_msg, ERL_NIF_LATIN1));
            Py_XDECREF(str);
        } else {
            result_term = enif_make_tuple2(msg_env, ATOM_ERROR,
                enif_make_atom(msg_env, "unknown"));
        }
        Py_XDECREF(exc);
        PyErr_Clear();
    } else {
        result_term = enif_make_tuple2(msg_env, ATOM_OK,
            py_to_term(msg_env, py_result));
        Py_DECREF(py_result);
    }

    /* Send message: {async_result, Id, Result} */
    ERL_NIF_TERM msg = enif_make_tuple3(msg_env,
        ATOM_ASYNC_RESULT,
        enif_make_uint64(msg_env, pending->id),
        result_term);
    enif_send(NULL, &pending->caller, msg_env, msg);
    enif_free_env(msg_env);
}

/**
 * Background thread running the asyncio event loop.
 * This thread owns the event loop and processes coroutines.
 */
static void *async_event_loop_thread(void *arg) {
    py_async_worker_t *worker = (py_async_worker_t *)arg;

    /* Acquire GIL for this thread */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Import asyncio */
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (asyncio == NULL) {
        PyErr_Print();
        PyGILState_Release(gstate);
        worker->loop_running = false;
        return NULL;
    }

    /* Create new event loop */
    PyObject *loop = PyObject_CallMethod(asyncio, "new_event_loop", NULL);
    if (loop == NULL) {
        PyErr_Print();
        Py_DECREF(asyncio);
        PyGILState_Release(gstate);
        worker->loop_running = false;
        return NULL;
    }

    /* Set as current loop */
    PyObject *set_result = PyObject_CallMethod(asyncio, "set_event_loop", "O", loop);
    Py_XDECREF(set_result);

    worker->event_loop = loop;
    Py_INCREF(loop);  /* Keep extra ref for worker struct */

    Py_DECREF(asyncio);

    worker->loop_running = true;

    /* Run the event loop with proper GIL management */
    while (!worker->shutdown) {
        /* Release GIL while sleeping (allow other Python threads to run) */
        Py_BEGIN_ALLOW_THREADS
        usleep(10000);  /* 10ms sleep without holding GIL */
        Py_END_ALLOW_THREADS

        /* Run one iteration of the event loop with GIL held */
        PyObject *asyncio_mod = PyImport_ImportModule("asyncio");
        if (asyncio_mod != NULL) {
            PyObject *sleep_coro = PyObject_CallMethod(asyncio_mod, "sleep", "d", 0.0);
            if (sleep_coro != NULL) {
                PyObject *task = PyObject_CallMethod(loop, "create_task", "O", sleep_coro);
                Py_DECREF(sleep_coro);
                if (task != NULL) {
                    PyObject *run_result = PyObject_CallMethod(loop, "run_until_complete", "O", task);
                    Py_DECREF(task);
                    Py_XDECREF(run_result);
                }
            }
            Py_DECREF(asyncio_mod);
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }

        /* Check for completed futures (GIL held) */
        pthread_mutex_lock(&worker->queue_mutex);
        async_pending_t *prev = NULL;
        async_pending_t *p = worker->pending_head;
        while (p != NULL) {
            if (p->future != NULL) {
                PyObject *done = PyObject_CallMethod(p->future, "done", NULL);
                if (done != NULL && PyObject_IsTrue(done)) {
                    Py_DECREF(done);

                    /* Future is complete - process it */
                    async_future_callback(worker, p);

                    /* Remove from list */
                    Py_DECREF(p->future);
                    if (prev == NULL) {
                        worker->pending_head = p->next;
                    } else {
                        prev->next = p->next;
                    }
                    if (p == worker->pending_tail) {
                        worker->pending_tail = prev;
                    }
                    async_pending_t *to_free = p;
                    p = p->next;
                    enif_free(to_free);
                    continue;
                }
                Py_XDECREF(done);
            }
            prev = p;
            p = p->next;
        }
        pthread_mutex_unlock(&worker->queue_mutex);
    }

    /* Stop and close the event loop */
    PyObject_CallMethod(loop, "stop", NULL);
    PyObject_CallMethod(loop, "close", NULL);
    Py_DECREF(loop);

    worker->loop_running = false;
    PyGILState_Release(gstate);

    return NULL;
}

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
            if (is_suspension_exception()) {
                /*
                 * Another suspension during replay - Python made a second erlang.call().
                 * Create a new suspended state and return {suspended, ...} so Erlang
                 * can handle this callback and resume again.
                 */
                PyObject *exc_args = get_suspension_args();  /* Clears exception */
                if (exc_args == NULL) {
                    result = make_error(env, "get_suspension_args_failed");
                } else {
                    suspended_state_t *new_suspended = create_suspended_state_from_existing(env, exc_args, state);
                    if (new_suspended == NULL) {
                        Py_DECREF(exc_args);
                        result = make_error(env, "create_nested_suspended_state_failed");
                    } else {
                        result = make_suspended_term(env, new_suspended, exc_args);
                        Py_DECREF(exc_args);
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
                if (is_suspension_exception()) {
                    /*
                     * Another suspension during replay - Python made a second erlang.call().
                     * Create a new suspended state and return {suspended, ...} so Erlang
                     * can handle this callback and resume again.
                     */
                    PyObject *exc_args = get_suspension_args();  /* Clears exception */
                    if (exc_args == NULL) {
                        result = make_error(env, "get_suspension_args_failed");
                    } else {
                        suspended_state_t *new_suspended = create_suspended_state_from_existing(env, exc_args, state);
                        if (new_suspended == NULL) {
                            Py_DECREF(exc_args);
                            result = make_error(env, "create_nested_suspended_state_failed");
                        } else {
                            result = make_suspended_term(env, new_suspended, exc_args);
                            Py_DECREF(exc_args);
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
