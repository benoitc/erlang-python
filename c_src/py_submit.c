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
 * @file py_submit.c
 * @brief Non-blocking Python call submission with event-driven results
 *
 * This module implements the submit work queue for the unified event-driven
 * architecture. Calls are queued to a background worker thread which
 * executes Python code and sends results via enif_send.
 *
 * Phase 3 of unified event-driven architecture.
 */

#include "py_nif.h"
#include "py_submit.h"

/* ============================================================================
 * Global State
 * ============================================================================ */

/** @brief Global submit queue (initialized in submit_init) */
static submit_queue_t g_submit_queue;

/** @brief Whether submit module is initialized */
static bool g_submit_initialized = false;

/** @brief Atom for call_result message */
static ERL_NIF_TERM ATOM_CALL_RESULT;

/** @brief Atom for call_error message */
static ERL_NIF_TERM ATOM_CALL_ERROR;

/* ============================================================================
 * Initialization
 * ============================================================================ */

int submit_init(void) {
    if (g_submit_initialized) {
        return 0;
    }

    memset(&g_submit_queue, 0, sizeof(g_submit_queue));

    if (pthread_mutex_init(&g_submit_queue.mutex, NULL) != 0) {
        return -1;
    }

    if (pthread_cond_init(&g_submit_queue.cond, NULL) != 0) {
        pthread_mutex_destroy(&g_submit_queue.mutex);
        return -1;
    }

    g_submit_queue.head = NULL;
    g_submit_queue.tail = NULL;
    g_submit_queue.running = false;
    g_submit_queue.shutdown = false;

    /* Note: Worker thread is NOT started here - it will be started lazily
     * by submit_start_worker() when Python is initialized and first request
     * comes in, or explicitly during Python init. */

    g_submit_initialized = true;
    return 0;
}

/**
 * @brief Start the submit worker thread
 *
 * Must be called after Python is initialized.
 * Safe to call multiple times - will only start thread once.
 *
 * @return 0 on success, -1 on failure
 */
int submit_start_worker(void) {
    if (!g_submit_initialized) {
        return -1;
    }

    pthread_mutex_lock(&g_submit_queue.mutex);
    if (g_submit_queue.running) {
        /* Already running */
        pthread_mutex_unlock(&g_submit_queue.mutex);
        return 0;
    }

    g_submit_queue.running = true;
    if (pthread_create(&g_submit_queue.worker_thread, NULL,
                       submit_worker_thread, NULL) != 0) {
        g_submit_queue.running = false;
        pthread_mutex_unlock(&g_submit_queue.mutex);
        return -1;
    }
    pthread_mutex_unlock(&g_submit_queue.mutex);

    return 0;
}

void submit_cleanup(void) {
    if (!g_submit_initialized) {
        return;
    }

    /* Signal shutdown */
    pthread_mutex_lock(&g_submit_queue.mutex);
    g_submit_queue.shutdown = true;
    pthread_cond_signal(&g_submit_queue.cond);
    pthread_mutex_unlock(&g_submit_queue.mutex);

    /* Wait for worker thread */
    if (g_submit_queue.running) {
        pthread_join(g_submit_queue.worker_thread, NULL);
        g_submit_queue.running = false;
    }

    /* Free any remaining requests */
    submit_request_t *req = g_submit_queue.head;
    while (req != NULL) {
        submit_request_t *next = req->next;
        free_submit_request(req);
        req = next;
    }

    pthread_cond_destroy(&g_submit_queue.cond);
    pthread_mutex_destroy(&g_submit_queue.mutex);

    g_submit_initialized = false;
}

/* ============================================================================
 * Submit Atoms Initialization
 * ============================================================================ */

void submit_init_atoms(ErlNifEnv *env) {
    ATOM_CALL_RESULT = enif_make_atom(env, "call_result");
    ATOM_CALL_ERROR = enif_make_atom(env, "call_error");
}

/* ============================================================================
 * Queue Operations
 * ============================================================================ */

static void enqueue_request(submit_request_t *req) {
    pthread_mutex_lock(&g_submit_queue.mutex);

    req->next = NULL;
    if (g_submit_queue.tail == NULL) {
        g_submit_queue.head = req;
        g_submit_queue.tail = req;
    } else {
        g_submit_queue.tail->next = req;
        g_submit_queue.tail = req;
    }

    pthread_cond_signal(&g_submit_queue.cond);
    pthread_mutex_unlock(&g_submit_queue.mutex);
}

static submit_request_t *dequeue_request(void) {
    pthread_mutex_lock(&g_submit_queue.mutex);

    while (g_submit_queue.head == NULL && !g_submit_queue.shutdown) {
        pthread_cond_wait(&g_submit_queue.cond, &g_submit_queue.mutex);
    }

    submit_request_t *req = NULL;
    if (g_submit_queue.head != NULL) {
        req = g_submit_queue.head;
        g_submit_queue.head = req->next;
        if (g_submit_queue.head == NULL) {
            g_submit_queue.tail = NULL;
        }
        req->next = NULL;
    }

    pthread_mutex_unlock(&g_submit_queue.mutex);
    return req;
}

/* ============================================================================
 * Worker Thread
 * ============================================================================ */

void *submit_worker_thread(void *arg) {
    (void)arg;

    /* Attach to Python runtime */
    PyGILState_STATE gstate = PyGILState_Ensure();

    while (!g_submit_queue.shutdown) {
        submit_request_t *req = NULL;

        /* Release GIL while waiting for work */
        Py_BEGIN_ALLOW_THREADS

        req = dequeue_request();

        Py_END_ALLOW_THREADS

        if (req == NULL) {
            /* Shutdown signaled */
            break;
        }

        /* Process the request with GIL held */
        process_submit_request(req);
        free_submit_request(req);
    }

    PyGILState_Release(gstate);
    return NULL;
}

/* ============================================================================
 * Request Processing
 * ============================================================================ */

void process_submit_request(submit_request_t *req) {
    PyObject *result = NULL;
    ERL_NIF_TERM msg;

    /* Import module */
    PyObject *py_module = PyImport_ImportModule(req->module);
    if (py_module == NULL) {
        goto handle_error;
    }

    /* Get function */
    PyObject *py_func = PyObject_GetAttrString(py_module, req->func);
    Py_DECREF(py_module);
    if (py_func == NULL) {
        goto handle_error;
    }

    /* Build args tuple */
    PyObject *args = (PyObject *)req->args;
    PyObject *kwargs = (PyObject *)req->kwargs;

    if (req->type == SUBMIT_COROUTINE) {
        /* For coroutines, call the function and run the coroutine */
        PyObject *coro = PyObject_Call(py_func, args ? args : PyTuple_New(0), kwargs);
        Py_DECREF(py_func);

        if (coro == NULL) {
            goto handle_error;
        }

        /* Get the current event loop and run the coroutine */
        PyObject *asyncio = PyImport_ImportModule("asyncio");
        if (asyncio == NULL) {
            Py_DECREF(coro);
            goto handle_error;
        }

        /* Get running loop or create new one */
        PyObject *loop = PyObject_CallMethod(asyncio, "get_event_loop", NULL);
        if (loop == NULL) {
            PyErr_Clear();
            loop = PyObject_CallMethod(asyncio, "new_event_loop", NULL);
        }

        if (loop == NULL) {
            Py_DECREF(asyncio);
            Py_DECREF(coro);
            goto handle_error;
        }

        /* Run the coroutine to completion */
        result = PyObject_CallMethod(loop, "run_until_complete", "O", coro);
        Py_DECREF(coro);
        Py_DECREF(loop);
        Py_DECREF(asyncio);

        if (result == NULL) {
            goto handle_error;
        }
    } else {
        /* Regular function call */
        result = PyObject_Call(py_func, args ? args : PyTuple_New(0), kwargs);
        Py_DECREF(py_func);

        if (result == NULL) {
            goto handle_error;
        }
    }

    /* Convert result to Erlang term and send */
    ERL_NIF_TERM result_term = py_to_term(req->msg_env, result);
    Py_DECREF(result);

    /* Build message: {call_result, CallbackId, Result} */
    msg = enif_make_tuple3(req->msg_env,
                           enif_make_atom(req->msg_env, "call_result"),
                           enif_make_uint64(req->msg_env, req->callback_id),
                           result_term);

    /* Send to event process */
    enif_send(NULL, &req->event_proc_pid, req->msg_env, msg);
    return;

handle_error:
    /* Get error info */
    {
        PyObject *type, *value, *traceback;
        PyErr_Fetch(&type, &value, &traceback);
        PyErr_NormalizeException(&type, &value, &traceback);

        ERL_NIF_TERM error_term;
        if (value != NULL) {
            PyObject *str = PyObject_Str(value);
            if (str != NULL) {
                const char *msg_str = PyUnicode_AsUTF8(str);
                if (msg_str != NULL) {
                    error_term = enif_make_string(req->msg_env, msg_str, ERL_NIF_LATIN1);
                } else {
                    error_term = enif_make_atom(req->msg_env, "unknown_error");
                }
                Py_DECREF(str);
            } else {
                error_term = enif_make_atom(req->msg_env, "unknown_error");
            }
        } else {
            error_term = enif_make_atom(req->msg_env, "unknown_error");
        }

        Py_XDECREF(type);
        Py_XDECREF(value);
        Py_XDECREF(traceback);
        PyErr_Clear();

        /* Build message: {call_error, CallbackId, Error} */
        msg = enif_make_tuple3(req->msg_env,
                               enif_make_atom(req->msg_env, "call_error"),
                               enif_make_uint64(req->msg_env, req->callback_id),
                               error_term);

        /* Send to event process */
        enif_send(NULL, &req->event_proc_pid, req->msg_env, msg);
    }
}

void free_submit_request(submit_request_t *req) {
    if (req == NULL) return;

    /* Need GIL to decref Python objects */
    gil_guard_t guard = gil_acquire();

    if (req->module != NULL) {
        enif_free(req->module);
    }
    if (req->func != NULL) {
        enif_free(req->func);
    }
    if (req->args != NULL) {
        Py_DECREF((PyObject *)req->args);
    }
    if (req->kwargs != NULL) {
        Py_DECREF((PyObject *)req->kwargs);
    }
    if (req->msg_env != NULL) {
        enif_free_env(req->msg_env);
    }

    gil_release(guard);

    enif_free(req);
}

/* ============================================================================
 * NIF Functions
 * ============================================================================ */

/**
 * submit_call(EventProcPid, CallbackId, Module, Func, Args, Kwargs)
 */
ERL_NIF_TERM nif_submit_call(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]) {
    if (argc != 6) {
        return enif_make_badarg(env);
    }

    if (!g_submit_initialized) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "not_initialized"));
    }

    /* Lazily start worker thread if not running (requires Python to be initialized) */
    if (!g_submit_queue.running && g_python_initialized) {
        if (submit_start_worker() != 0) {
            return enif_make_tuple2(env, ATOM_ERROR,
                                    enif_make_atom(env, "worker_start_failed"));
        }
    }

    if (!g_submit_queue.running) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "python_not_initialized"));
    }

    /* Get event proc PID */
    ErlNifPid event_proc_pid;
    if (!enif_get_local_pid(env, argv[0], &event_proc_pid)) {
        return enif_make_badarg(env);
    }

    /* Get callback ID */
    ErlNifUInt64 callback_id;
    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return enif_make_badarg(env);
    }

    /* Get module name */
    ErlNifBinary module_bin;
    if (!enif_inspect_binary(env, argv[2], &module_bin)) {
        return enif_make_badarg(env);
    }

    /* Get function name */
    ErlNifBinary func_bin;
    if (!enif_inspect_binary(env, argv[3], &func_bin)) {
        return enif_make_badarg(env);
    }

    /* Allocate request */
    submit_request_t *req = enif_alloc(sizeof(submit_request_t));
    if (req == NULL) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }
    memset(req, 0, sizeof(submit_request_t));

    req->type = SUBMIT_CALL;
    req->callback_id = callback_id;
    req->event_proc_pid = event_proc_pid;

    /* Copy module name */
    req->module = enif_alloc(module_bin.size + 1);
    if (req->module == NULL) {
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }
    memcpy(req->module, module_bin.data, module_bin.size);
    req->module[module_bin.size] = '\0';

    /* Copy function name */
    req->func = enif_alloc(func_bin.size + 1);
    if (req->func == NULL) {
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }
    memcpy(req->func, func_bin.data, func_bin.size);
    req->func[func_bin.size] = '\0';

    /* Convert args to Python (need GIL) */
    gil_guard_t guard = gil_acquire();

    PyObject *args_obj = term_to_py(env, argv[4]);
    if (args_obj == NULL && !enif_is_empty_list(env, argv[4])) {
        PyErr_Clear();
        gil_release(guard);
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "invalid_args"));
    }

    /* Convert args list to tuple */
    if (args_obj != NULL && PyList_Check(args_obj)) {
        /* Convert list to tuple */
        req->args = PyList_AsTuple(args_obj);
        Py_DECREF(args_obj);
    } else if (args_obj != NULL && PyTuple_Check(args_obj)) {
        req->args = args_obj;
    } else if (args_obj == NULL) {
        req->args = PyTuple_New(0);
    } else {
        /* Single value - wrap in tuple */
        req->args = PyTuple_Pack(1, args_obj);
        Py_DECREF(args_obj);
    }

    /* Convert kwargs to Python dict */
    if (!enif_is_empty_list(env, argv[5])) {
        req->kwargs = term_to_py(env, argv[5]);
        if (req->kwargs == NULL || !PyDict_Check(req->kwargs)) {
            PyErr_Clear();
            gil_release(guard);
            free_submit_request(req);
            return enif_make_tuple2(env, ATOM_ERROR,
                                    enif_make_atom(env, "invalid_kwargs"));
        }
    }

    gil_release(guard);

    /* Create message environment */
    req->msg_env = enif_alloc_env();
    if (req->msg_env == NULL) {
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }

    /* Enqueue request */
    enqueue_request(req);

    return ATOM_OK;
}

/**
 * submit_coroutine(EventProcPid, CallbackId, Module, Func, Args, Kwargs)
 */
ERL_NIF_TERM nif_submit_coroutine(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
    if (argc != 6) {
        return enif_make_badarg(env);
    }

    if (!g_submit_initialized) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "not_initialized"));
    }

    /* Lazily start worker thread if not running (requires Python to be initialized) */
    if (!g_submit_queue.running && g_python_initialized) {
        if (submit_start_worker() != 0) {
            return enif_make_tuple2(env, ATOM_ERROR,
                                    enif_make_atom(env, "worker_start_failed"));
        }
    }

    if (!g_submit_queue.running) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "python_not_initialized"));
    }

    /* Get event proc PID */
    ErlNifPid event_proc_pid;
    if (!enif_get_local_pid(env, argv[0], &event_proc_pid)) {
        return enif_make_badarg(env);
    }

    /* Get callback ID */
    ErlNifUInt64 callback_id;
    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return enif_make_badarg(env);
    }

    /* Get module name */
    ErlNifBinary module_bin;
    if (!enif_inspect_binary(env, argv[2], &module_bin)) {
        return enif_make_badarg(env);
    }

    /* Get function name */
    ErlNifBinary func_bin;
    if (!enif_inspect_binary(env, argv[3], &func_bin)) {
        return enif_make_badarg(env);
    }

    /* Allocate request */
    submit_request_t *req = enif_alloc(sizeof(submit_request_t));
    if (req == NULL) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }
    memset(req, 0, sizeof(submit_request_t));

    req->type = SUBMIT_COROUTINE;
    req->callback_id = callback_id;
    req->event_proc_pid = event_proc_pid;

    /* Copy module name */
    req->module = enif_alloc(module_bin.size + 1);
    if (req->module == NULL) {
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }
    memcpy(req->module, module_bin.data, module_bin.size);
    req->module[module_bin.size] = '\0';

    /* Copy function name */
    req->func = enif_alloc(func_bin.size + 1);
    if (req->func == NULL) {
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }
    memcpy(req->func, func_bin.data, func_bin.size);
    req->func[func_bin.size] = '\0';

    /* Convert args to Python (need GIL) */
    gil_guard_t guard = gil_acquire();

    PyObject *args_obj = term_to_py(env, argv[4]);
    if (args_obj == NULL && !enif_is_empty_list(env, argv[4])) {
        PyErr_Clear();
        gil_release(guard);
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "invalid_args"));
    }

    /* Convert args list to tuple */
    if (args_obj != NULL && PyList_Check(args_obj)) {
        /* Convert list to tuple */
        req->args = PyList_AsTuple(args_obj);
        Py_DECREF(args_obj);
    } else if (args_obj != NULL && PyTuple_Check(args_obj)) {
        req->args = args_obj;
    } else if (args_obj == NULL) {
        req->args = PyTuple_New(0);
    } else {
        /* Single value - wrap in tuple */
        req->args = PyTuple_Pack(1, args_obj);
        Py_DECREF(args_obj);
    }

    /* Convert kwargs to Python dict */
    if (!enif_is_empty_list(env, argv[5])) {
        req->kwargs = term_to_py(env, argv[5]);
        if (req->kwargs == NULL || !PyDict_Check(req->kwargs)) {
            PyErr_Clear();
            gil_release(guard);
            free_submit_request(req);
            return enif_make_tuple2(env, ATOM_ERROR,
                                    enif_make_atom(env, "invalid_kwargs"));
        }
    }

    gil_release(guard);

    /* Create message environment */
    req->msg_env = enif_alloc_env();
    if (req->msg_env == NULL) {
        free_submit_request(req);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "enomem"));
    }

    /* Enqueue request */
    enqueue_request(req);

    return ATOM_OK;
}
