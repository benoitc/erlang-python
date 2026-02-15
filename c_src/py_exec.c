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
 * @file py_exec.c
 * @brief Python execution engine and GIL management
 * @author Benoit Chesneau
 *
 * @ingroup exec
 *
 * This module implements the core Python execution engine, handling:
 *
 * - **Timeout support**: Trace-based execution timeout monitoring
 * - **Executor threads**: Single and multi-executor pool management
 * - **Request processing**: Dispatch for call/eval/exec/import operations
 * - **Free-threaded mode**: Support for Python 3.13+ no-GIL builds
 *
 * @par Architecture
 *
 * The execution model uses a request/response pattern:
 *
 * ```
 * ┌─────────────┐    enqueue     ┌──────────────┐    process    ┌────────────┐
 * │  NIF Call   │ ────────────>  │   Executor   │ ────────────> │   Python   │
 * │  (Erlang)   │                │    Thread    │               │    Code    │
 * └─────────────┘                └──────────────┘               └────────────┘
 *       │                              │                              │
 *       │         wait(cond)           │                              │
 *       │<─────────────────────────────│<─────────────────────────────│
 *       │                          completed                       result
 * ```
 *
 * @par GIL Management Patterns
 *
 * Following PyO3/Granian best practices:
 *
 * - **Py_BEGIN_ALLOW_THREADS**: Release GIL during blocking waits
 * - **Py_END_ALLOW_THREADS**: Re-acquire GIL before Python calls
 * - **PyGILState_Ensure/Release**: For callbacks from non-Python threads
 *
 * @par Execution Modes
 *
 * | Mode | Description | GIL Handling |
 * |------|-------------|--------------|
 * | FREE_THREADED | Python 3.13+ no-GIL | Direct execution |
 * | SUBINTERP | Python 3.12+ | Per-interpreter GIL |
 * | MULTI_EXECUTOR | Traditional | N executor threads |
 *
 * @par Thread Safety
 *
 * - Executor queues protected by pthread mutexes
 * - Request completion signaled via condition variables
 * - Thread-local storage for timeout and callback state
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Timeout Support
 *
 * Python execution timeout is implemented using PyEval_SetTrace(), which
 * installs a callback invoked at each Python instruction. This allows
 * cooperative timeout checking without requiring signal handlers.
 * ============================================================================ */

/**
 * @brief Trace callback for timeout checking
 *
 * Called by Python at each line/call/return event when tracing is enabled.
 * Checks if the deadline has passed and raises TimeoutError if so.
 *
 * @param obj   Trace function argument (unused)
 * @param frame Current execution frame (unused)
 * @param what  Event type (call/line/return/exception)
 * @param arg   Event-specific argument (unused)
 *
 * @return 0 to continue, -1 to abort with exception
 *
 * @note Called with GIL held
 * @note Uses thread-local storage for deadline
 */
static int python_trace_callback(PyObject *obj, PyFrameObject *frame, int what, PyObject *arg) {
    (void)obj;
    (void)frame;
    (void)what;
    (void)arg;
    if (tl_timeout_enabled && tl_timeout_deadline > 0) {
        if (get_monotonic_ns() > tl_timeout_deadline) {
            PyErr_SetString(PyExc_TimeoutError, "execution timeout");
            return -1;  /* Abort execution */
        }
    }
    return 0;
}

/**
 * @brief Enable timeout monitoring for Python execution
 *
 * Installs a trace callback that checks elapsed time against a deadline.
 * If the deadline is exceeded, TimeoutError is raised in Python.
 *
 * @param timeout_ms Timeout in milliseconds (0 = no timeout)
 *
 * @par Implementation
 *
 * Uses thread-local storage to avoid global state:
 * - `tl_timeout_deadline`: Absolute deadline (monotonic ns)
 * - `tl_timeout_enabled`: Flag to enable checking
 *
 * @note Must be paired with stop_timeout()
 * @note GIL must be held
 *
 * @see stop_timeout()
 * @see python_trace_callback()
 */
static void start_timeout(unsigned long timeout_ms) {
    if (timeout_ms > 0) {
        tl_timeout_deadline = get_monotonic_ns() + (timeout_ms * 1000000ULL);
        tl_timeout_enabled = true;
        PyEval_SetTrace(python_trace_callback, NULL);
    }
}

static void stop_timeout(void) {
    if (tl_timeout_enabled) {
        tl_timeout_enabled = false;
        tl_timeout_deadline = 0;
        PyEval_SetTrace(NULL, NULL);
    }
}

static bool check_timeout_error(void) {
    if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_TimeoutError)) {
        return true;
    }
    return false;
}

/* ============================================================================
 * Execution mode detection
 * ============================================================================ */

static void detect_execution_mode(void) {
#ifdef HAVE_FREE_THREADED
    /* Python 3.13+ with free-threading enabled */
    g_execution_mode = PY_MODE_FREE_THREADED;
    return;
#endif

#ifdef HAVE_SUBINTERPRETERS
    /* Python 3.12+ supports per-interpreter GIL */
    g_execution_mode = PY_MODE_SUBINTERP;
    return;
#endif

    /* Fallback: multi-executor with shared GIL */
    g_execution_mode = PY_MODE_MULTI_EXECUTOR;
}

/* ============================================================================
 * Request processing
 * ============================================================================ */

/**
 * Initialize a request structure.
 */
static void request_init(py_request_t *req) {
    memset(req, 0, sizeof(py_request_t));
    pthread_mutex_init(&req->mutex, NULL);
    pthread_cond_init(&req->cond, NULL);
    req->completed = false;
}

/**
 * Clean up a request structure.
 */
static void request_cleanup(py_request_t *req) {
    pthread_mutex_destroy(&req->mutex);
    pthread_cond_destroy(&req->cond);
}

/**
 * Process a single request in the executor thread (GIL held).
 */
static void process_request(py_request_t *req) {
    ErlNifEnv *env = req->env;
    py_worker_t *worker = req->worker;

    switch (req->type) {
    case PY_REQ_CALL: {
        /* Set thread-local worker context for callbacks */
        tl_current_worker = worker;
        tl_callback_env = env;
        tl_allow_suspension = true;  /* Allow suspension for direct calls */

        char *module_name = binary_to_string(&req->module_bin);
        char *func_name = binary_to_string(&req->func_bin);
        if (module_name == NULL || func_name == NULL) {
            enif_free(module_name);
            enif_free(func_name);
            req->result = make_error(env, "alloc_failed");
            break;
        }

        PyObject *func = NULL;

        /* Special handling for __main__ - look in worker's namespace */
        if (strcmp(module_name, "__main__") == 0) {
            func = PyDict_GetItemString(worker->locals, func_name);
            if (func == NULL) {
                func = PyDict_GetItemString(worker->globals, func_name);
            }
            if (func != NULL) {
                Py_INCREF(func);
            } else {
                PyErr_Format(PyExc_NameError, "name '%s' is not defined", func_name);
                req->result = make_py_error(env);
                goto call_cleanup;
            }
        } else {
            PyObject *module = PyImport_ImportModule(module_name);
            if (module == NULL) {
                req->result = make_py_error(env);
                goto call_cleanup;
            }
            func = PyObject_GetAttrString(module, func_name);
            Py_DECREF(module);
        }

        if (func == NULL) {
            req->result = make_py_error(env);
            goto call_cleanup;
        }

        /* Convert args list to Python tuple */
        unsigned int args_len;
        if (!enif_get_list_length(env, req->args_term, &args_len)) {
            Py_DECREF(func);
            req->result = make_error(env, "invalid_args");
            goto call_cleanup;
        }

        PyObject *args = PyTuple_New(args_len);
        ERL_NIF_TERM head, tail = req->args_term;
        for (unsigned int i = 0; i < args_len; i++) {
            enif_get_list_cell(env, tail, &head, &tail);
            PyObject *arg = term_to_py(env, head);
            if (arg == NULL) {
                Py_DECREF(args);
                Py_DECREF(func);
                req->result = make_error(env, "arg_conversion_failed");
                goto call_cleanup;
            }
            PyTuple_SET_ITEM(args, i, arg);
        }

        /* Convert kwargs map to Python dict */
        PyObject *kwargs = NULL;
        if (enif_is_map(env, req->kwargs_term)) {
            kwargs = term_to_py(env, req->kwargs_term);
        }

        /* Start timeout if specified */
        start_timeout(req->timeout_ms);

        /* Call the function */
        PyObject *py_result = PyObject_Call(func, args, kwargs);

        /* Stop timeout */
        stop_timeout();

        Py_DECREF(func);
        Py_DECREF(args);
        Py_XDECREF(kwargs);

        if (py_result == NULL) {
            if (check_timeout_error()) {
                PyErr_Clear();
                req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_TIMEOUT);
            } else if (is_suspension_exception()) {
                /*
                 * Python code called erlang.call() which raised SuspensionRequired.
                 * Create a suspended state and return {suspended, CallbackId, StateRef, {Func, Args}}
                 * so Erlang can execute the callback and then resume.
                 */
                PyObject *exc_args = get_suspension_args();  /* Clears exception */
                if (exc_args == NULL) {
                    req->result = make_error(env, "get_suspension_args_failed");
                } else {
                    suspended_state_t *suspended = create_suspended_state(env, exc_args, req);
                    if (suspended == NULL) {
                        Py_DECREF(exc_args);
                        req->result = make_error(env, "create_suspended_state_failed");
                    } else {
                        /* Extract callback info from exception args */
                        PyObject *callback_id_obj = PyTuple_GetItem(exc_args, 0);
                        PyObject *func_name_obj = PyTuple_GetItem(exc_args, 1);
                        PyObject *call_args_obj = PyTuple_GetItem(exc_args, 2);

                        uint64_t callback_id = PyLong_AsUnsignedLongLong(callback_id_obj);
                        Py_ssize_t fn_len;
                        const char *fn = PyUnicode_AsUTF8AndSize(func_name_obj, &fn_len);

                        /* Create Erlang terms */
                        ERL_NIF_TERM state_ref = enif_make_resource(env, suspended);
                        enif_release_resource(suspended);  /* Erlang now holds the reference */

                        ERL_NIF_TERM callback_id_term = enif_make_uint64(env, callback_id);

                        ERL_NIF_TERM func_name_term;
                        unsigned char *fn_buf = enif_make_new_binary(env, fn_len, &func_name_term);
                        memcpy(fn_buf, fn, fn_len);

                        ERL_NIF_TERM args_term = py_to_term(env, call_args_obj);

                        Py_DECREF(exc_args);

                        /* Return {suspended, CallbackId, StateRef, {FuncName, Args}} */
                        req->result = enif_make_tuple4(env,
                            ATOM_SUSPENDED,
                            callback_id_term,
                            state_ref,
                            enif_make_tuple2(env, func_name_term, args_term));
                    }
                }
            } else {
                req->result = make_py_error(env);
            }
        } else if (PyGen_Check(py_result) || PyIter_Check(py_result)) {
            py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
            if (wrapper == NULL) {
                Py_DECREF(py_result);
                req->result = make_error(env, "alloc_failed");
            } else {
                wrapper->obj = py_result;
                ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
                enif_release_resource(wrapper);
                req->result = enif_make_tuple2(env, ATOM_OK,
                    enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
            }
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, py_result);
            Py_DECREF(py_result);
            req->result = enif_make_tuple2(env, ATOM_OK, term_result);
        }

    call_cleanup:
        tl_current_worker = NULL;
        tl_callback_env = NULL;
        tl_allow_suspension = false;
        enif_free(module_name);
        enif_free(func_name);
        break;
    }

    case PY_REQ_EVAL: {
        tl_current_worker = worker;
        tl_callback_env = env;
        tl_allow_suspension = true;  /* Allow suspension - we replay on resume */

        char *code = binary_to_string(&req->code_bin);
        if (code == NULL) {
            req->result = make_error(env, "alloc_failed");
            break;
        }

        /* Update locals if provided */
        if (enif_is_map(env, req->locals_term)) {
            PyObject *new_locals = term_to_py(env, req->locals_term);
            if (new_locals != NULL && PyDict_Check(new_locals)) {
                PyDict_Update(worker->locals, new_locals);
                Py_DECREF(new_locals);
            }
        }

        /* Start timeout if specified */
        start_timeout(req->timeout_ms);

        /* Compile and evaluate */
        PyObject *compiled = Py_CompileString(code, "<erlang>", Py_eval_input);

        if (compiled == NULL) {
            stop_timeout();
            req->result = make_py_error(env);
        } else {
            PyObject *py_result = PyEval_EvalCode(compiled, worker->globals, worker->locals);
            Py_DECREF(compiled);
            stop_timeout();

            if (py_result == NULL) {
                if (check_timeout_error()) {
                    PyErr_Clear();
                    req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_TIMEOUT);
                } else if (is_suspension_exception()) {
                    /* Handle suspension from erlang.call() in eval */
                    PyObject *exc_args = get_suspension_args();  /* Clears exception */
                    if (exc_args == NULL) {
                        req->result = make_error(env, "get_suspension_args_failed");
                    } else {
                        suspended_state_t *suspended = create_suspended_state(env, exc_args, req);
                        if (suspended == NULL) {
                            Py_DECREF(exc_args);
                            req->result = make_error(env, "create_suspended_state_failed");
                        } else {
                            PyObject *callback_id_obj = PyTuple_GetItem(exc_args, 0);
                            PyObject *func_name_obj = PyTuple_GetItem(exc_args, 1);
                            PyObject *call_args_obj = PyTuple_GetItem(exc_args, 2);

                            uint64_t callback_id = PyLong_AsUnsignedLongLong(callback_id_obj);
                            Py_ssize_t fn_len;
                            const char *fn = PyUnicode_AsUTF8AndSize(func_name_obj, &fn_len);

                            ERL_NIF_TERM state_ref = enif_make_resource(env, suspended);
                            enif_release_resource(suspended);

                            ERL_NIF_TERM callback_id_term = enif_make_uint64(env, callback_id);

                            ERL_NIF_TERM func_name_term;
                            unsigned char *fn_buf = enif_make_new_binary(env, fn_len, &func_name_term);
                            memcpy(fn_buf, fn, fn_len);

                            ERL_NIF_TERM args_term = py_to_term(env, call_args_obj);

                            Py_DECREF(exc_args);

                            req->result = enif_make_tuple4(env,
                                ATOM_SUSPENDED,
                                callback_id_term,
                                state_ref,
                                enif_make_tuple2(env, func_name_term, args_term));
                        }
                    }
                } else {
                    req->result = make_py_error(env);
                }
            } else if (PyGen_Check(py_result) || PyIter_Check(py_result)) {
                py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
                if (wrapper == NULL) {
                    Py_DECREF(py_result);
                    req->result = make_error(env, "alloc_failed");
                } else {
                    wrapper->obj = py_result;
                    ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
                    enif_release_resource(wrapper);
                    req->result = enif_make_tuple2(env, ATOM_OK,
                        enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
                }
            } else {
                ERL_NIF_TERM term_result = py_to_term(env, py_result);
                Py_DECREF(py_result);
                req->result = enif_make_tuple2(env, ATOM_OK, term_result);
            }
        }

        tl_current_worker = NULL;
        tl_callback_env = NULL;
        tl_allow_suspension = false;
        enif_free(code);
        break;
    }

    case PY_REQ_EXEC: {
        tl_current_worker = worker;
        tl_callback_env = env;
        /* Note: tl_allow_suspension stays false for exec - suspension not allowed */

        char *code = binary_to_string(&req->code_bin);
        if (code == NULL) {
            req->result = make_error(env, "alloc_failed");
            break;
        }

        PyObject *compiled = Py_CompileString(code, "<erlang>", Py_file_input);

        if (compiled == NULL) {
            req->result = make_py_error(env);
        } else {
            PyObject *py_result = PyEval_EvalCode(compiled, worker->globals, worker->locals);
            Py_DECREF(compiled);

            if (py_result == NULL) {
                req->result = make_py_error(env);
            } else {
                Py_DECREF(py_result);
                req->result = ATOM_OK;
            }
        }

        tl_current_worker = NULL;
        tl_callback_env = NULL;
        enif_free(code);
        break;
    }

    case PY_REQ_NEXT: {
        PyObject *item = PyIter_Next(req->gen_wrapper->obj);

        if (item == NULL) {
            if (PyErr_Occurred()) {
                if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
                    PyErr_Clear();
                    req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_STOP_ITERATION);
                } else {
                    req->result = make_py_error(env);
                }
            } else {
                req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_STOP_ITERATION);
            }
        } else if (PyGen_Check(item) || PyIter_Check(item)) {
            py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
            if (wrapper == NULL) {
                Py_DECREF(item);
                req->result = make_error(env, "alloc_failed");
            } else {
                wrapper->obj = item;
                ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
                enif_release_resource(wrapper);
                req->result = enif_make_tuple2(env, ATOM_OK,
                    enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
            }
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, item);
            Py_DECREF(item);
            req->result = enif_make_tuple2(env, ATOM_OK, term_result);
        }
        break;
    }

    case PY_REQ_IMPORT: {
        char *module_name = binary_to_string(&req->module_bin);
        if (module_name == NULL) {
            req->result = make_error(env, "alloc_failed");
            break;
        }

        PyObject *module = PyImport_ImportModule(module_name);
        enif_free(module_name);

        if (module == NULL) {
            req->result = make_py_error(env);
        } else {
            py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
            if (wrapper == NULL) {
                Py_DECREF(module);
                req->result = make_error(env, "alloc_failed");
            } else {
                wrapper->obj = module;
                ERL_NIF_TERM mod_ref = enif_make_resource(env, wrapper);
                enif_release_resource(wrapper);
                req->result = enif_make_tuple2(env, ATOM_OK, mod_ref);
            }
        }
        break;
    }

    case PY_REQ_GETATTR: {
        char *attr_name = binary_to_string(&req->attr_bin);
        if (attr_name == NULL) {
            req->result = make_error(env, "alloc_failed");
            break;
        }

        PyObject *attr = PyObject_GetAttrString(req->obj_wrapper->obj, attr_name);
        enif_free(attr_name);

        if (attr == NULL) {
            req->result = make_py_error(env);
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, attr);
            Py_DECREF(attr);
            req->result = enif_make_tuple2(env, ATOM_OK, term_result);
        }
        break;
    }

    case PY_REQ_MEMORY_STATS: {
        /* Import gc module */
        PyObject *gc_module = PyImport_ImportModule("gc");
        if (gc_module == NULL) {
            req->result = make_error(env, "gc_import_failed");
            break;
        }

        ERL_NIF_TERM result_map = enif_make_new_map(env);

        PyObject *stats = PyObject_CallMethod(gc_module, "get_stats", NULL);
        if (stats != NULL && PyList_Check(stats)) {
            Py_ssize_t num_gens = PyList_Size(stats);
            if (num_gens > 0) {
                ERL_NIF_TERM *gen_stats = enif_alloc(sizeof(ERL_NIF_TERM) * num_gens);
                if (gen_stats != NULL) {
                    for (Py_ssize_t i = 0; i < num_gens; i++) {
                        PyObject *gen = PyList_GetItem(stats, i);
                        gen_stats[i] = py_to_term(env, gen);
                    }
                    ERL_NIF_TERM gc_stats_list = enif_make_list_from_array(env, gen_stats, num_gens);
                    enif_free(gen_stats);
                    enif_make_map_put(env, result_map,
                        enif_make_atom(env, "gc_stats"), gc_stats_list, &result_map);
                }
                /* If gen_stats alloc failed, we skip gc_stats but continue with other stats */
            }
            Py_DECREF(stats);
        }

        PyObject *counts = PyObject_CallMethod(gc_module, "get_count", NULL);
        if (counts != NULL && PyTuple_Check(counts)) {
            ERL_NIF_TERM count_term = py_to_term(env, counts);
            enif_make_map_put(env, result_map,
                enif_make_atom(env, "gc_count"), count_term, &result_map);
            Py_DECREF(counts);
        }

        PyObject *threshold = PyObject_CallMethod(gc_module, "get_threshold", NULL);
        if (threshold != NULL && PyTuple_Check(threshold)) {
            ERL_NIF_TERM threshold_term = py_to_term(env, threshold);
            enif_make_map_put(env, result_map,
                enif_make_atom(env, "gc_threshold"), threshold_term, &result_map);
            Py_DECREF(threshold);
        }

        Py_DECREF(gc_module);

        /* Try to get tracemalloc stats if available */
        PyObject *tracemalloc = PyImport_ImportModule("tracemalloc");
        if (tracemalloc != NULL) {
            PyObject *is_tracing = PyObject_CallMethod(tracemalloc, "is_tracing", NULL);
            if (is_tracing != NULL && PyObject_IsTrue(is_tracing)) {
                PyObject *current_traced = PyObject_CallMethod(tracemalloc, "get_traced_memory", NULL);
                if (current_traced != NULL && PyTuple_Check(current_traced)) {
                    ERL_NIF_TERM current = py_to_term(env, PyTuple_GetItem(current_traced, 0));
                    ERL_NIF_TERM peak = py_to_term(env, PyTuple_GetItem(current_traced, 1));
                    enif_make_map_put(env, result_map,
                        enif_make_atom(env, "traced_memory_current"), current, &result_map);
                    enif_make_map_put(env, result_map,
                        enif_make_atom(env, "traced_memory_peak"), peak, &result_map);
                    Py_DECREF(current_traced);
                }
            }
            Py_XDECREF(is_tracing);
            Py_DECREF(tracemalloc);
        }
        PyErr_Clear();

        req->result = enif_make_tuple2(env, ATOM_OK, result_map);
        break;
    }

    case PY_REQ_GC: {
        PyObject *gc_module = PyImport_ImportModule("gc");
        if (gc_module == NULL) {
            req->result = make_error(env, "gc_import_failed");
            break;
        }

        PyObject *result = PyObject_CallMethod(gc_module, "collect", "i", req->gc_generation);
        Py_DECREF(gc_module);

        if (result == NULL) {
            req->result = make_py_error(env);
        } else {
            long collected = PyLong_AsLong(result);
            Py_DECREF(result);
            req->result = enif_make_tuple2(env, ATOM_OK, enif_make_long(env, collected));
        }
        break;
    }

    case PY_REQ_SHUTDOWN:
        /* Signal to exit the loop - nothing to do here */
        break;
    }
}

/* ============================================================================
 * Single executor thread implementation
 * ============================================================================ */

/**
 * Main function for the executor thread.
 * Acquires GIL and processes requests until shutdown.
 */
static void *executor_thread_main(void *arg) {
    (void)arg;

    /* Acquire GIL for this thread */
    PyGILState_STATE gstate = PyGILState_Ensure();

    g_executor_running = true;

    /*
     * Main processing loop.
     * We continue processing until we receive a PY_REQ_SHUTDOWN request.
     * The shutdown flag is used to stop waiting when the queue is empty.
     */
    bool should_exit = false;
    while (!should_exit) {
        py_request_t *req = NULL;

        /* Release GIL while waiting for work (like PyO3 allow_threads) */
        Py_BEGIN_ALLOW_THREADS

        pthread_mutex_lock(&g_executor_mutex);
        while (g_executor_queue_head == NULL && !g_executor_shutdown) {
            pthread_cond_wait(&g_executor_cond, &g_executor_mutex);
        }

        /* Dequeue request if available */
        if (g_executor_queue_head != NULL) {
            req = g_executor_queue_head;
            g_executor_queue_head = req->next;
            if (g_executor_queue_head == NULL) {
                g_executor_queue_tail = NULL;
            }
            req->next = NULL;
        } else if (g_executor_shutdown) {
            /* Queue is empty and shutdown requested - exit */
            should_exit = true;
        }
        pthread_mutex_unlock(&g_executor_mutex);

        Py_END_ALLOW_THREADS

        if (req != NULL) {
            if (req->type == PY_REQ_SHUTDOWN) {
                /* Signal completion and exit */
                pthread_mutex_lock(&req->mutex);
                req->completed = true;
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
                should_exit = true;
            } else {
                /* Process the request with GIL held */
                process_request(req);

                /* Signal completion */
                pthread_mutex_lock(&req->mutex);
                req->completed = true;
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
            }
        }
    }

    g_executor_running = false;
    PyGILState_Release(gstate);

    return NULL;
}

/**
 * Enqueue a request to the appropriate executor based on execution mode.
 * Routes to multi-executor pool, single executor, or executes directly.
 */
static void executor_enqueue(py_request_t *req) {
    switch (g_execution_mode) {
#ifdef HAVE_FREE_THREADED
        case PY_MODE_FREE_THREADED:
            /* Execute directly in free-threaded mode - no executor needed */
            {
                PyGILState_STATE gstate = PyGILState_Ensure();
                process_request(req);
                PyGILState_Release(gstate);
                /* Signal completion immediately */
                pthread_mutex_lock(&req->mutex);
                req->completed = true;
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
            }
            return;
#endif

        case PY_MODE_MULTI_EXECUTOR:
            if (g_multi_executor_initialized) {
                /* Route to multi-executor pool */
                int exec_id = select_executor();
                multi_executor_enqueue(exec_id, req);
                return;
            }
            /* Fall through to single executor if multi not initialized */
            break;

        case PY_MODE_SUBINTERP:
        default:
            /* Use single executor */
            break;
    }

    /* Single executor queue */
    pthread_mutex_lock(&g_executor_mutex);
    req->next = NULL;
    if (g_executor_queue_tail == NULL) {
        g_executor_queue_head = req;
        g_executor_queue_tail = req;
    } else {
        g_executor_queue_tail->next = req;
        g_executor_queue_tail = req;
    }
    pthread_cond_signal(&g_executor_cond);
    pthread_mutex_unlock(&g_executor_mutex);
}

/**
 * Wait for a request to complete.
 */
static void executor_wait(py_request_t *req) {
    pthread_mutex_lock(&req->mutex);
    while (!req->completed) {
        pthread_cond_wait(&req->cond, &req->mutex);
    }
    pthread_mutex_unlock(&req->mutex);
}

/**
 * Start the executor thread.
 * Called during Python initialization.
 */
static int executor_start(void) {
    g_executor_shutdown = false;
    g_executor_queue_head = NULL;
    g_executor_queue_tail = NULL;

    if (pthread_create(&g_executor_thread, NULL, executor_thread_main, NULL) != 0) {
        return -1;
    }

    /* Wait for executor to be ready */
    int max_wait = 100;  /* 1 second max */
    while (!g_executor_running && max_wait-- > 0) {
        usleep(10000);  /* 10ms */
    }

    return g_executor_running ? 0 : -1;
}

/**
 * Stop the executor thread.
 * Called during Python finalization.
 */
static void executor_stop(void) {
    if (!g_executor_running) {
        return;
    }

    /* Send shutdown request */
    py_request_t shutdown_req;
    request_init(&shutdown_req);
    shutdown_req.type = PY_REQ_SHUTDOWN;

    g_executor_shutdown = true;
    executor_enqueue(&shutdown_req);
    executor_wait(&shutdown_req);
    request_cleanup(&shutdown_req);

    /* Wait for thread to finish */
    pthread_join(g_executor_thread, NULL);
}

/* ============================================================================
 * Multi-executor pool implementation
 *
 * For MULTI_EXECUTOR mode (traditional Python), we run N executor threads
 * that each hold the GIL in turn. This allows GIL contention-based parallelism
 * similar to PyO3's multi-executor pattern.
 * ============================================================================ */

/**
 * Main function for a multi-executor thread.
 * Each executor has its own queue and processes requests independently.
 */
static void *multi_executor_thread_main(void *arg) {
    executor_t *exec = (executor_t *)arg;

    /* Acquire GIL for this thread */
    PyGILState_STATE gstate = PyGILState_Ensure();

    exec->running = true;

    while (!exec->shutdown) {
        py_request_t *req = NULL;

        /* Release GIL while waiting for work */
        Py_BEGIN_ALLOW_THREADS

        pthread_mutex_lock(&exec->mutex);
        while (exec->queue_head == NULL && !exec->shutdown) {
            pthread_cond_wait(&exec->cond, &exec->mutex);
        }

        /* Dequeue request if available */
        if (exec->queue_head != NULL) {
            req = exec->queue_head;
            exec->queue_head = req->next;
            if (exec->queue_head == NULL) {
                exec->queue_tail = NULL;
            }
            req->next = NULL;
        }
        pthread_mutex_unlock(&exec->mutex);

        Py_END_ALLOW_THREADS

        if (req != NULL) {
            if (req->type == PY_REQ_SHUTDOWN) {
                pthread_mutex_lock(&req->mutex);
                req->completed = true;
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
                break;
            } else {
                /* Process the request with GIL held */
                process_request(req);

                /* Signal completion */
                pthread_mutex_lock(&req->mutex);
                req->completed = true;
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
            }
        }
    }

    exec->running = false;
    PyGILState_Release(gstate);

    return NULL;
}

/**
 * Select an executor using round-robin.
 */
static int select_executor(void) {
    int idx = atomic_fetch_add(&g_next_executor, 1) % g_num_executors;
    return idx;
}

/**
 * Enqueue a request to a specific executor.
 */
static void multi_executor_enqueue(int exec_id, py_request_t *req) {
    executor_t *exec = &g_executors[exec_id];

    pthread_mutex_lock(&exec->mutex);
    req->next = NULL;
    if (exec->queue_tail == NULL) {
        exec->queue_head = req;
        exec->queue_tail = req;
    } else {
        exec->queue_tail->next = req;
        exec->queue_tail = req;
    }
    pthread_cond_signal(&exec->cond);
    pthread_mutex_unlock(&exec->mutex);
}

/**
 * Start the multi-executor pool.
 */
static int multi_executor_start(int num_executors) {
    if (g_multi_executor_initialized) {
        return 0;
    }

    if (num_executors <= 0) {
        num_executors = 4;
    }
    if (num_executors > MAX_EXECUTORS) {
        num_executors = MAX_EXECUTORS;
    }

    g_num_executors = num_executors;

    for (int i = 0; i < num_executors; i++) {
        executor_t *exec = &g_executors[i];
        exec->id = i;
        exec->queue_head = NULL;
        exec->queue_tail = NULL;
        exec->running = false;
        exec->shutdown = false;
        pthread_mutex_init(&exec->mutex, NULL);
        pthread_cond_init(&exec->cond, NULL);

        if (pthread_create(&exec->thread, NULL, multi_executor_thread_main, exec) != 0) {
            /* Cleanup already created threads */
            for (int j = 0; j < i; j++) {
                g_executors[j].shutdown = true;
                pthread_cond_signal(&g_executors[j].cond);
                pthread_join(g_executors[j].thread, NULL);
                pthread_mutex_destroy(&g_executors[j].mutex);
                pthread_cond_destroy(&g_executors[j].cond);
            }
            return -1;
        }
    }

    /* Wait for all executors to be ready */
    int max_wait = 100;
    bool all_ready = false;
    while (!all_ready && max_wait-- > 0) {
        all_ready = true;
        for (int i = 0; i < num_executors; i++) {
            if (!g_executors[i].running) {
                all_ready = false;
                break;
            }
        }
        if (!all_ready) {
            usleep(10000);
        }
    }

    g_multi_executor_initialized = all_ready;
    return all_ready ? 0 : -1;
}

/**
 * Stop the multi-executor pool.
 */
static void multi_executor_stop(void) {
    if (!g_multi_executor_initialized) {
        return;
    }

    /* Allocate shutdown requests for all executors */
    py_request_t *shutdown_reqs[MAX_EXECUTORS] = {0};

    /* Signal shutdown and send shutdown requests to all executors */
    for (int i = 0; i < g_num_executors; i++) {
        executor_t *exec = &g_executors[i];
        exec->shutdown = true;

        py_request_t *shutdown_req = enif_alloc(sizeof(py_request_t));
        if (shutdown_req != NULL) {
            request_init(shutdown_req);
            shutdown_req->type = PY_REQ_SHUTDOWN;
            shutdown_reqs[i] = shutdown_req;
            multi_executor_enqueue(i, shutdown_req);
        }
        /* If alloc fails, the shutdown flag is already set, so executor
         * will exit when it checks the flag */
    }

    /* Wait for all executors to finish and clean up shutdown requests */
    for (int i = 0; i < g_num_executors; i++) {
        executor_t *exec = &g_executors[i];
        pthread_join(exec->thread, NULL);
        pthread_mutex_destroy(&exec->mutex);
        pthread_cond_destroy(&exec->cond);

        /* Clean up the shutdown request */
        if (shutdown_reqs[i] != NULL) {
            request_cleanup(shutdown_reqs[i]);
            enif_free(shutdown_reqs[i]);
        }
    }

    g_multi_executor_initialized = false;
}

/* ============================================================================
 * Free-threaded execution (Python 3.13+ nogil)
 * ============================================================================ */

#ifdef HAVE_FREE_THREADED
/**
 * Execute a request directly in free-threaded mode.
 * No GIL management needed - Python handles synchronization internally.
 */
static ERL_NIF_TERM execute_direct(ErlNifEnv *env, py_request_t *req) {
    /* In free-threaded mode, PyGILState functions still work but are essentially no-ops */
    PyGILState_STATE gstate = PyGILState_Ensure();

    process_request(req);

    PyGILState_Release(gstate);
    return req->result;
}
#endif
