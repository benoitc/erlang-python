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
 * @file py_worker_pool.c
 * @brief Worker thread pool implementation
 *
 * Implements a pool of worker threads for processing Python operations.
 * Each worker can have its own subinterpreter (Python 3.12+) for true
 * parallelism, or share the GIL with batching for older Python versions.
 */

/* ============================================================================
 * Global Pool Instance
 * ============================================================================ */

py_worker_pool_t g_pool = {
    .num_workers = 0,
    .initialized = false,
    .shutting_down = false,
    .request_id_counter = 1,
    .use_subinterpreters = false,
    .free_threaded = false
};

/* Atom for response message */
static ERL_NIF_TERM ATOM_PY_RESPONSE;

/* ============================================================================
 * Queue Operations
 * ============================================================================ */

static void queue_init(py_pool_queue_t *queue) {
    queue->head = NULL;
    queue->tail = NULL;
    atomic_store(&queue->pending_count, 0);
    atomic_store(&queue->total_enqueued, 0);
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

static void queue_destroy(py_pool_queue_t *queue) {
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
}

static void queue_enqueue(py_pool_queue_t *queue, py_pool_request_t *req) {
    pthread_mutex_lock(&queue->mutex);

    req->next = NULL;
    if (queue->tail == NULL) {
        queue->head = req;
        queue->tail = req;
    } else {
        queue->tail->next = req;
        queue->tail = req;
    }

    atomic_fetch_add(&queue->pending_count, 1);
    atomic_fetch_add(&queue->total_enqueued, 1);

    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

static py_pool_request_t *queue_dequeue(py_pool_queue_t *queue, bool wait) {
    pthread_mutex_lock(&queue->mutex);

    while (queue->head == NULL) {
        if (!wait) {
            pthread_mutex_unlock(&queue->mutex);
            return NULL;
        }
        pthread_cond_wait(&queue->cond, &queue->mutex);

        /* Check for shutdown after wakeup */
        if (g_pool.shutting_down) {
            pthread_mutex_unlock(&queue->mutex);
            return NULL;
        }
    }

    py_pool_request_t *req = queue->head;
    queue->head = req->next;
    if (queue->head == NULL) {
        queue->tail = NULL;
    }
    req->next = NULL;

    atomic_fetch_sub(&queue->pending_count, 1);
    pthread_mutex_unlock(&queue->mutex);

    return req;
}

/* Wake up all workers waiting on the queue */
static void queue_broadcast(py_pool_queue_t *queue) {
    pthread_mutex_lock(&queue->mutex);
    pthread_cond_broadcast(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

/* ============================================================================
 * Request Management
 * ============================================================================ */

static py_pool_request_t *py_pool_request_new(py_pool_request_type_t type,
                                               ErlNifPid caller_pid) {
    py_pool_request_t *req = enif_alloc(sizeof(py_pool_request_t));
    if (req == NULL) {
        return NULL;
    }

    memset(req, 0, sizeof(py_pool_request_t));
    req->type = type;
    req->caller_pid = caller_pid;
    req->request_id = atomic_fetch_add(&g_pool.request_id_counter, 1);
    req->msg_env = enif_alloc_env();

    if (req->msg_env == NULL) {
        enif_free(req);
        return NULL;
    }

    return req;
}

static void py_pool_request_free(py_pool_request_t *req) {
    if (req == NULL) {
        return;
    }

    if (req->module_name) {
        enif_free(req->module_name);
    }
    if (req->func_name) {
        enif_free(req->func_name);
    }
    if (req->code) {
        enif_free(req->code);
    }
    if (req->runner_name) {
        enif_free(req->runner_name);
    }
    if (req->callable_name) {
        enif_free(req->callable_name);
    }
    if (req->body_data) {
        enif_free(req->body_data);
    }
    if (req->msg_env) {
        enif_free_env(req->msg_env);
    }

    enif_free(req);
}

/* ============================================================================
 * Module Caching
 * ============================================================================ */

static PyObject *py_pool_get_module(py_pool_worker_t *worker,
                                     const char *module_name) {
    /* Check cache first */
    if (worker->module_cache != NULL) {
        PyObject *key = PyUnicode_FromString(module_name);
        if (key != NULL) {
            PyObject *module = PyDict_GetItem(worker->module_cache, key);
            Py_DECREF(key);
            if (module != NULL) {
                return module;  /* Borrowed reference */
            }
        }
    }

    /* Import module */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        return NULL;
    }

    /* Cache it */
    if (worker->module_cache != NULL) {
        PyObject *key = PyUnicode_FromString(module_name);
        if (key != NULL) {
            PyDict_SetItem(worker->module_cache, key, module);
            Py_DECREF(key);
        }
    }

    Py_DECREF(module);  /* Dict now owns it, return borrowed ref */
    return PyDict_GetItemString(worker->module_cache, module_name);
}

static void py_pool_clear_module_cache(py_pool_worker_t *worker) {
    if (worker->module_cache != NULL) {
        PyDict_Clear(worker->module_cache);
    }
}

/* ============================================================================
 * Response Sending
 * ============================================================================ */

static void py_pool_send_response(py_pool_request_t *req, ERL_NIF_TERM result) {
    /* Build message: {py_response, RequestId, Result} */
    ERL_NIF_TERM request_id_term = enif_make_uint64(req->msg_env, req->request_id);
    ERL_NIF_TERM msg = enif_make_tuple3(req->msg_env,
                                         ATOM_PY_RESPONSE,
                                         request_id_term,
                                         result);

    enif_send(NULL, &req->caller_pid, req->msg_env, msg);
}

/* ============================================================================
 * Request Processing - CALL/APPLY
 * ============================================================================ */

static ERL_NIF_TERM py_pool_process_call(py_pool_worker_t *worker,
                                          py_pool_request_t *req) {
    ErlNifEnv *env = req->msg_env;

    /* Get module */
    PyObject *module = py_pool_get_module(worker, req->module_name);
    if (module == NULL) {
        ERL_NIF_TERM err = make_py_error(env);
        return err;
    }

    /* Get function */
    PyObject *func = PyObject_GetAttrString(module, req->func_name);
    if (func == NULL) {
        ERL_NIF_TERM err = make_py_error(env);
        return err;
    }

    /* Convert args to Python */
    PyObject *args = term_to_py(env, req->args_term);
    if (args == NULL) {
        Py_DECREF(func);
        return make_error(env, "args_conversion_failed");
    }

    /* Ensure args is a tuple */
    if (!PyTuple_Check(args)) {
        if (PyList_Check(args)) {
            PyObject *tuple = PyList_AsTuple(args);
            Py_DECREF(args);
            args = tuple;
        } else {
            PyObject *tuple = PyTuple_Pack(1, args);
            Py_DECREF(args);
            args = tuple;
        }
    }

    /* Call function */
    PyObject *result = PyObject_Call(func, args, NULL);
    Py_DECREF(func);
    Py_DECREF(args);

    if (result == NULL) {
        return make_py_error(env);
    }

    /* Convert result to Erlang */
    ERL_NIF_TERM result_term = py_to_term(env, result);
    Py_DECREF(result);

    return enif_make_tuple2(env, ATOM_OK, result_term);
}

static ERL_NIF_TERM py_pool_process_apply(py_pool_worker_t *worker,
                                           py_pool_request_t *req) {
    ErlNifEnv *env = req->msg_env;

    /* Get module */
    PyObject *module = py_pool_get_module(worker, req->module_name);
    if (module == NULL) {
        return make_py_error(env);
    }

    /* Get function */
    PyObject *func = PyObject_GetAttrString(module, req->func_name);
    if (func == NULL) {
        return make_py_error(env);
    }

    /* Convert args to Python */
    PyObject *args = term_to_py(env, req->args_term);
    if (args == NULL) {
        Py_DECREF(func);
        return make_error(env, "args_conversion_failed");
    }

    /* Ensure args is a tuple */
    if (!PyTuple_Check(args)) {
        if (PyList_Check(args)) {
            PyObject *tuple = PyList_AsTuple(args);
            Py_DECREF(args);
            args = tuple;
        } else {
            PyObject *tuple = PyTuple_Pack(1, args);
            Py_DECREF(args);
            args = tuple;
        }
    }

    /* Convert kwargs to Python dict */
    PyObject *kwargs = NULL;
    if (enif_is_map(env, req->kwargs_term)) {
        kwargs = term_to_py(env, req->kwargs_term);
        if (kwargs != NULL && !PyDict_Check(kwargs)) {
            Py_DECREF(kwargs);
            kwargs = NULL;
        }
    }

    /* Call function with kwargs */
    PyObject *result = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (result == NULL) {
        return make_py_error(env);
    }

    /* Convert result to Erlang */
    ERL_NIF_TERM result_term = py_to_term(env, result);
    Py_DECREF(result);

    return enif_make_tuple2(env, ATOM_OK, result_term);
}

/* ============================================================================
 * Request Processing - EVAL/EXEC
 * ============================================================================ */

static ERL_NIF_TERM py_pool_process_eval(py_pool_worker_t *worker,
                                          py_pool_request_t *req) {
    ErlNifEnv *env = req->msg_env;

    /* Compile code as expression */
    PyObject *code = Py_CompileString(req->code, "<eval>", Py_eval_input);
    if (code == NULL) {
        return make_py_error(env);
    }

    /* Prepare locals if provided */
    PyObject *locals = worker->locals;
    /* Check if locals_term was set (non-zero) before checking if it's a map */
    if (req->locals_term != 0 && enif_is_map(env, req->locals_term)) {
        PyObject *new_locals = term_to_py(env, req->locals_term);
        if (new_locals != NULL && PyDict_Check(new_locals)) {
            /* Merge with existing locals */
            PyDict_Update(locals, new_locals);
            Py_DECREF(new_locals);
        }
    }

    /* Evaluate */
    PyObject *result = PyEval_EvalCode(code, worker->globals, locals);
    Py_DECREF(code);

    if (result == NULL) {
        return make_py_error(env);
    }

    /* Convert result to Erlang */
    ERL_NIF_TERM result_term = py_to_term(env, result);
    Py_DECREF(result);

    return enif_make_tuple2(env, ATOM_OK, result_term);
}

static ERL_NIF_TERM py_pool_process_exec(py_pool_worker_t *worker,
                                          py_pool_request_t *req) {
    ErlNifEnv *env = req->msg_env;

    /* Compile code as statements */
    PyObject *code = Py_CompileString(req->code, "<exec>", Py_file_input);
    if (code == NULL) {
        return make_py_error(env);
    }

    /* Execute */
    PyObject *result = PyEval_EvalCode(code, worker->globals, worker->locals);
    Py_DECREF(code);

    if (result == NULL) {
        return make_py_error(env);
    }

    Py_DECREF(result);
    return enif_make_tuple2(env, ATOM_OK, ATOM_NONE);
}

/* ============================================================================
 * Request Processing - ASGI
 * ============================================================================ */

static ERL_NIF_TERM py_pool_process_asgi(py_pool_worker_t *worker,
                                          py_pool_request_t *req) {
    ErlNifEnv *env = req->msg_env;

    /* Get runner module */
    PyObject *runner_module = py_pool_get_module(worker, req->runner_name);
    if (runner_module == NULL) {
        return make_py_error(env);
    }

    /* Get 'run' function from runner */
    PyObject *run_func = PyObject_GetAttrString(runner_module, "run");
    if (run_func == NULL) {
        return make_py_error(env);
    }

    /* Get ASGI app module */
    PyObject *app_module = py_pool_get_module(worker, req->module_name);
    if (app_module == NULL) {
        Py_DECREF(run_func);
        return make_py_error(env);
    }

    /* Get ASGI callable */
    PyObject *app_callable = PyObject_GetAttrString(app_module, req->callable_name);
    if (app_callable == NULL) {
        Py_DECREF(run_func);
        return make_py_error(env);
    }

    /* Build scope dict from Erlang term using optimized ASGI conversion */
    PyObject *scope = asgi_scope_from_map(env, req->scope_term);
    if (scope == NULL) {
        Py_DECREF(run_func);
        Py_DECREF(app_callable);
        return make_py_error(env);
    }

    /* Create body bytes from binary */
    PyObject *body = PyBytes_FromStringAndSize((const char *)req->body_data,
                                                req->body_len);
    if (body == NULL) {
        Py_DECREF(run_func);
        Py_DECREF(app_callable);
        Py_DECREF(scope);
        return make_py_error(env);
    }

    /* Call runner.run(app, scope, body) */
    PyObject *args = PyTuple_Pack(3, app_callable, scope, body);
    Py_DECREF(app_callable);
    Py_DECREF(scope);
    Py_DECREF(body);

    if (args == NULL) {
        Py_DECREF(run_func);
        return make_py_error(env);
    }

    PyObject *result = PyObject_Call(run_func, args, NULL);
    Py_DECREF(run_func);
    Py_DECREF(args);

    if (result == NULL) {
        return make_py_error(env);
    }

    /* Extract ASGI response using optimized extraction */
    ERL_NIF_TERM response = extract_asgi_response(env, result);
    Py_DECREF(result);

    return enif_make_tuple2(env, ATOM_OK, response);
}

/* ============================================================================
 * Request Processing - WSGI
 * ============================================================================ */

static ERL_NIF_TERM py_pool_process_wsgi(py_pool_worker_t *worker,
                                          py_pool_request_t *req) {
    ErlNifEnv *env = req->msg_env;

    /* Get app module */
    PyObject *app_module = py_pool_get_module(worker, req->module_name);
    if (app_module == NULL) {
        return make_py_error(env);
    }

    /* Get WSGI callable */
    PyObject *app_callable = PyObject_GetAttrString(app_module, req->callable_name);
    if (app_callable == NULL) {
        return make_py_error(env);
    }

    /* Build environ dict */
    PyObject *environ = term_to_py(env, req->environ_term);
    if (environ == NULL || !PyDict_Check(environ)) {
        Py_DECREF(app_callable);
        Py_XDECREF(environ);
        return make_error(env, "invalid_environ");
    }

    /* Create start_response callable */
    /* For simplicity, use a list to collect status/headers */
    PyObject *response_started = PyList_New(0);
    if (response_started == NULL) {
        Py_DECREF(app_callable);
        Py_DECREF(environ);
        return make_py_error(env);
    }

    /* For now, return error asking to use ASGI instead */
    /* Full WSGI implementation would need a proper start_response callable */
    Py_DECREF(app_callable);
    Py_DECREF(environ);
    Py_DECREF(response_started);

    return make_error(env, "wsgi_not_fully_implemented_use_asgi");
}

/* ============================================================================
 * Request Processing Dispatcher
 * ============================================================================ */

static void py_pool_process_request(py_pool_worker_t *worker,
                                     py_pool_request_t *req) {
    uint64_t start_ns = get_monotonic_ns();
    ERL_NIF_TERM result;

    switch (req->type) {
        case PY_POOL_REQ_CALL:
            result = py_pool_process_call(worker, req);
            break;
        case PY_POOL_REQ_APPLY:
            result = py_pool_process_apply(worker, req);
            break;
        case PY_POOL_REQ_EVAL:
            result = py_pool_process_eval(worker, req);
            break;
        case PY_POOL_REQ_EXEC:
            result = py_pool_process_exec(worker, req);
            break;
        case PY_POOL_REQ_ASGI:
            result = py_pool_process_asgi(worker, req);
            break;
        case PY_POOL_REQ_WSGI:
            result = py_pool_process_wsgi(worker, req);
            break;
        case PY_POOL_REQ_SHUTDOWN:
            /* Shutdown handled by worker thread */
            return;
        default:
            result = make_error(req->msg_env, "unknown_request_type");
            break;
    }

    /* Send response */
    py_pool_send_response(req, result);

    /* Update stats */
    uint64_t elapsed_ns = get_monotonic_ns() - start_ns;
    atomic_fetch_add(&worker->requests_processed, 1);
    atomic_fetch_add(&worker->total_processing_ns, elapsed_ns);
}

/* ============================================================================
 * Worker Thread
 * ============================================================================ */

static int worker_init_python_state(py_pool_worker_t *worker) {
#ifdef HAVE_SUBINTERPRETERS
    if (g_pool.use_subinterpreters) {
        /* Create sub-interpreter with its own GIL */
        PyInterpreterConfig config = {
            .use_main_obmalloc = 0,
            .allow_fork = 0,
            .allow_exec = 0,
            .allow_threads = 1,
            .allow_daemon_threads = 0,
            .check_multi_interp_extensions = 1,
            .gil = PyInterpreterConfig_OWN_GIL,
        };

        PyStatus status = Py_NewInterpreterFromConfig(&worker->tstate, &config);
        if (PyStatus_Exception(status)) {
            return -1;
        }

        worker->interp = PyThreadState_GetInterpreter(worker->tstate);
    } else
#endif
    {
        /* Non-subinterpreter mode: acquire GIL */
        gil_guard_t guard = gil_acquire();
        (void)guard;  /* Will be released when worker exits */
    }

    /* Create per-worker state */
    worker->module_cache = PyDict_New();
    worker->globals = PyDict_New();
    worker->locals = PyDict_New();

    if (worker->module_cache == NULL ||
        worker->globals == NULL ||
        worker->locals == NULL) {
        Py_XDECREF(worker->module_cache);
        Py_XDECREF(worker->globals);
        Py_XDECREF(worker->locals);
        return -1;
    }

    /* Add builtins to globals */
    PyObject *builtins = PyEval_GetBuiltins();
    if (builtins != NULL) {
        PyDict_SetItemString(worker->globals, "__builtins__", builtins);
    }

    /* Initialize ASGI state for this interpreter */
    worker->asgi_state = get_asgi_interp_state();

    return 0;
}

static void worker_cleanup_python_state(py_pool_worker_t *worker) {
    Py_XDECREF(worker->module_cache);
    Py_XDECREF(worker->globals);
    Py_XDECREF(worker->locals);
    worker->module_cache = NULL;
    worker->globals = NULL;
    worker->locals = NULL;

#ifdef HAVE_SUBINTERPRETERS
    if (g_pool.use_subinterpreters && worker->tstate != NULL) {
        Py_EndInterpreter(worker->tstate);
        worker->tstate = NULL;
        worker->interp = NULL;
    }
#endif
}

static void *py_pool_worker_thread(void *arg) {
    py_pool_worker_t *worker = (py_pool_worker_t *)arg;

    /* Initialize Python state */
    gil_guard_t guard = {0};

#ifdef HAVE_SUBINTERPRETERS
    if (g_pool.use_subinterpreters) {
        /* Acquire GIL in main interpreter first */
        guard = gil_acquire();

        /* Create sub-interpreter */
        PyInterpreterConfig config = {
            .use_main_obmalloc = 0,
            .allow_fork = 0,
            .allow_exec = 0,
            .allow_threads = 1,
            .allow_daemon_threads = 0,
            .check_multi_interp_extensions = 1,
            .gil = PyInterpreterConfig_OWN_GIL,
        };

        PyStatus status = Py_NewInterpreterFromConfig(&worker->tstate, &config);
        if (PyStatus_Exception(status)) {
            gil_release(guard);
            worker->running = false;
            return NULL;
        }

        worker->interp = PyThreadState_GetInterpreter(worker->tstate);

        /* Release main GIL - we now have our own */
        gil_release(guard);

        /* We're now attached to our sub-interpreter */
    } else
#endif
    {
        /* Non-subinterpreter mode: acquire the shared GIL */
        guard = gil_acquire();
    }

    /* Create per-worker state */
    worker->module_cache = PyDict_New();
    worker->globals = PyDict_New();
    worker->locals = PyDict_New();

    if (worker->module_cache == NULL ||
        worker->globals == NULL ||
        worker->locals == NULL) {
        goto cleanup;
    }

    /* Add builtins to globals */
    PyObject *builtins = PyEval_GetBuiltins();
    if (builtins != NULL) {
        PyDict_SetItemString(worker->globals, "__builtins__", builtins);
    }

    /* Initialize ASGI state for this interpreter */
    worker->asgi_state = get_asgi_interp_state();

    worker->running = true;

    /* Main processing loop */
    while (!worker->shutdown) {
        py_pool_request_t *req = NULL;

#ifdef HAVE_SUBINTERPRETERS
        if (g_pool.use_subinterpreters) {
            /* Subinterpreter mode: we own our GIL, just dequeue and process */
            req = queue_dequeue(&g_pool.queue, true);
        } else
#endif
        {
            /* Release GIL while waiting for work */
            Py_BEGIN_ALLOW_THREADS
            req = queue_dequeue(&g_pool.queue, true);
            Py_END_ALLOW_THREADS
        }

        if (req == NULL || req->type == PY_POOL_REQ_SHUTDOWN) {
            if (req != NULL) {
                py_pool_request_free(req);
            }
            break;
        }

        /* Process with GIL held (or in subinterpreter with own GIL) */
        py_pool_process_request(worker, req);
        py_pool_request_free(req);
    }

cleanup:
    /* Clean up Python state */
    Py_XDECREF(worker->module_cache);
    Py_XDECREF(worker->globals);
    Py_XDECREF(worker->locals);
    worker->module_cache = NULL;
    worker->globals = NULL;
    worker->locals = NULL;

#ifdef HAVE_SUBINTERPRETERS
    if (g_pool.use_subinterpreters && worker->tstate != NULL) {
        Py_EndInterpreter(worker->tstate);
        worker->tstate = NULL;
        worker->interp = NULL;
    } else
#endif
    {
        gil_release(guard);
    }

    worker->running = false;
    return NULL;
}

/* ============================================================================
 * Pool Lifecycle
 * ============================================================================ */

static int py_pool_init(int num_workers) {
    if (g_pool.initialized) {
        return 0;  /* Already initialized */
    }

    /* Determine number of workers */
    if (num_workers <= 0) {
        /* Auto-detect: use number of CPUs */
        long ncpus = sysconf(_SC_NPROCESSORS_ONLN);
        num_workers = (ncpus > 0) ? (int)ncpus : 4;
    }
    if (num_workers > POOL_MAX_WORKERS) {
        num_workers = POOL_MAX_WORKERS;
    }

    /* Detect execution mode */
#ifdef HAVE_FREE_THREADED
    g_pool.free_threaded = true;
    g_pool.use_subinterpreters = false;
#elif defined(HAVE_SUBINTERPRETERS)
    g_pool.free_threaded = false;
    g_pool.use_subinterpreters = true;
#else
    g_pool.free_threaded = false;
    g_pool.use_subinterpreters = false;
#endif

    /* Initialize queue */
    queue_init(&g_pool.queue);

    /* Initialize workers */
    g_pool.num_workers = num_workers;
    for (int i = 0; i < num_workers; i++) {
        py_pool_worker_t *worker = &g_pool.workers[i];
        memset(worker, 0, sizeof(py_pool_worker_t));
        worker->worker_id = i;
        worker->shutdown = false;
        atomic_store(&worker->requests_processed, 0);
        atomic_store(&worker->total_processing_ns, 0);
    }

    /* Start worker threads */
    for (int i = 0; i < num_workers; i++) {
        py_pool_worker_t *worker = &g_pool.workers[i];
        int rc = pthread_create(&worker->thread, NULL,
                                py_pool_worker_thread, worker);
        if (rc != 0) {
            /* Failed to create thread - shut down already created ones */
            g_pool.shutting_down = true;
            queue_broadcast(&g_pool.queue);
            for (int j = 0; j < i; j++) {
                pthread_join(g_pool.workers[j].thread, NULL);
            }
            queue_destroy(&g_pool.queue);
            return -1;
        }
    }

    /* Wait for workers to start */
    for (int i = 0; i < num_workers; i++) {
        while (!g_pool.workers[i].running && !g_pool.workers[i].shutdown) {
            usleep(1000);  /* 1ms */
        }
    }

    g_pool.initialized = true;
    return 0;
}

static void py_pool_shutdown(void) {
    if (!g_pool.initialized) {
        return;
    }

    g_pool.shutting_down = true;

    /* Send shutdown requests to all workers */
    for (int i = 0; i < g_pool.num_workers; i++) {
        g_pool.workers[i].shutdown = true;

        /* Enqueue shutdown request to wake up workers */
        py_pool_request_t *shutdown_req = py_pool_request_new(
            PY_POOL_REQ_SHUTDOWN, (ErlNifPid){0});
        if (shutdown_req != NULL) {
            queue_enqueue(&g_pool.queue, shutdown_req);
        }
    }

    /* Wake up all waiting workers */
    queue_broadcast(&g_pool.queue);

    /* Wait for workers to terminate */
    for (int i = 0; i < g_pool.num_workers; i++) {
        pthread_join(g_pool.workers[i].thread, NULL);
    }

    /* Drain and free remaining requests */
    py_pool_request_t *req;
    while ((req = queue_dequeue(&g_pool.queue, false)) != NULL) {
        /* Send error response for abandoned requests */
        if (req->type != PY_POOL_REQ_SHUTDOWN && req->msg_env != NULL) {
            ERL_NIF_TERM error = make_error(req->msg_env, "pool_shutdown");
            py_pool_send_response(req, error);
        }
        py_pool_request_free(req);
    }

    queue_destroy(&g_pool.queue);
    g_pool.initialized = false;
    g_pool.shutting_down = false;
}

static bool py_pool_is_initialized(void) {
    return g_pool.initialized;
}

static int py_pool_enqueue(py_pool_request_t *req) {
    if (!g_pool.initialized || g_pool.shutting_down) {
        return -1;
    }

    queue_enqueue(&g_pool.queue, req);
    return 0;
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

static void py_pool_get_stats(py_pool_stats_t *stats) {
    memset(stats, 0, sizeof(py_pool_stats_t));

    stats->num_workers = g_pool.num_workers;
    stats->initialized = g_pool.initialized;
    stats->use_subinterpreters = g_pool.use_subinterpreters;
    stats->free_threaded = g_pool.free_threaded;
    stats->pending_count = atomic_load(&g_pool.queue.pending_count);
    stats->total_enqueued = atomic_load(&g_pool.queue.total_enqueued);

    for (int i = 0; i < g_pool.num_workers && i < POOL_MAX_WORKERS; i++) {
        stats->worker_stats[i].requests_processed =
            atomic_load(&g_pool.workers[i].requests_processed);
        stats->worker_stats[i].total_processing_ns =
            atomic_load(&g_pool.workers[i].total_processing_ns);
    }
}

/* ============================================================================
 * NIF Functions
 * ============================================================================ */

static ERL_NIF_TERM nif_pool_start(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    if (argc != 1) {
        return enif_make_badarg(env);
    }

    int num_workers;
    if (!enif_get_int(env, argv[0], &num_workers)) {
        return enif_make_badarg(env);
    }

    if (py_pool_init(num_workers) != 0) {
        return make_error(env, "failed_to_start_pool");
    }

    return ATOM_OK;
}

static ERL_NIF_TERM nif_pool_stop(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    py_pool_shutdown();
    return ATOM_OK;
}

static ERL_NIF_TERM nif_pool_submit(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {
    if (argc != 5) {
        return enif_make_badarg(env);
    }

    if (!g_pool.initialized) {
        return make_error(env, "pool_not_started");
    }

    /* Get request type atom */
    char type_buf[32];
    if (!enif_get_atom(env, argv[0], type_buf, sizeof(type_buf), ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    py_pool_request_type_t type;
    if (strcmp(type_buf, "call") == 0) {
        type = PY_POOL_REQ_CALL;
    } else if (strcmp(type_buf, "apply") == 0) {
        type = PY_POOL_REQ_APPLY;
    } else if (strcmp(type_buf, "eval") == 0) {
        type = PY_POOL_REQ_EVAL;
    } else if (strcmp(type_buf, "exec") == 0) {
        type = PY_POOL_REQ_EXEC;
    } else if (strcmp(type_buf, "asgi") == 0) {
        type = PY_POOL_REQ_ASGI;
    } else if (strcmp(type_buf, "wsgi") == 0) {
        type = PY_POOL_REQ_WSGI;
    } else {
        return make_error(env, "unknown_request_type");
    }

    /* Get caller PID */
    ErlNifPid caller_pid;
    if (!enif_self(env, &caller_pid)) {
        return make_error(env, "cannot_get_self_pid");
    }

    /* Create request */
    py_pool_request_t *req = py_pool_request_new(type, caller_pid);
    if (req == NULL) {
        return make_error(env, "request_allocation_failed");
    }

    /* Parse arguments based on type */
    switch (type) {
        case PY_POOL_REQ_CALL:
        case PY_POOL_REQ_APPLY: {
            /* argv[1] = Module, argv[2] = Func, argv[3] = Args, argv[4] = Kwargs/undefined */
            ErlNifBinary module_bin, func_bin;
            if (!enif_inspect_binary(env, argv[1], &module_bin) ||
                !enif_inspect_binary(env, argv[2], &func_bin)) {
                py_pool_request_free(req);
                return enif_make_badarg(env);
            }

            req->module_name = enif_alloc(module_bin.size + 1);
            req->func_name = enif_alloc(func_bin.size + 1);
            if (req->module_name == NULL || req->func_name == NULL) {
                py_pool_request_free(req);
                return make_error(env, "allocation_failed");
            }

            memcpy(req->module_name, module_bin.data, module_bin.size);
            req->module_name[module_bin.size] = '\0';
            memcpy(req->func_name, func_bin.data, func_bin.size);
            req->func_name[func_bin.size] = '\0';

            req->args_term = enif_make_copy(req->msg_env, argv[3]);

            if (type == PY_POOL_REQ_APPLY && !enif_is_atom(env, argv[4])) {
                req->kwargs_term = enif_make_copy(req->msg_env, argv[4]);
            }
            break;
        }

        case PY_POOL_REQ_EVAL:
        case PY_POOL_REQ_EXEC: {
            /* argv[1] = Code, argv[2-4] = unused */
            ErlNifBinary code_bin;
            if (!enif_inspect_binary(env, argv[1], &code_bin)) {
                py_pool_request_free(req);
                return enif_make_badarg(env);
            }

            req->code = enif_alloc(code_bin.size + 1);
            if (req->code == NULL) {
                py_pool_request_free(req);
                return make_error(env, "allocation_failed");
            }

            memcpy(req->code, code_bin.data, code_bin.size);
            req->code[code_bin.size] = '\0';

            if (!enif_is_atom(env, argv[2])) {
                req->locals_term = enif_make_copy(req->msg_env, argv[2]);
            }
            break;
        }

        case PY_POOL_REQ_ASGI: {
            /* argv[1] = Runner, argv[2] = Module, argv[3] = Callable, argv[4] = {Scope, Body} */
            ErlNifBinary runner_bin, module_bin, callable_bin;
            if (!enif_inspect_binary(env, argv[1], &runner_bin) ||
                !enif_inspect_binary(env, argv[2], &module_bin) ||
                !enif_inspect_binary(env, argv[3], &callable_bin)) {
                py_pool_request_free(req);
                return enif_make_badarg(env);
            }

            req->runner_name = enif_alloc(runner_bin.size + 1);
            req->module_name = enif_alloc(module_bin.size + 1);
            req->callable_name = enif_alloc(callable_bin.size + 1);
            if (req->runner_name == NULL || req->module_name == NULL ||
                req->callable_name == NULL) {
                py_pool_request_free(req);
                return make_error(env, "allocation_failed");
            }

            memcpy(req->runner_name, runner_bin.data, runner_bin.size);
            req->runner_name[runner_bin.size] = '\0';
            memcpy(req->module_name, module_bin.data, module_bin.size);
            req->module_name[module_bin.size] = '\0';
            memcpy(req->callable_name, callable_bin.data, callable_bin.size);
            req->callable_name[callable_bin.size] = '\0';

            /* Parse {Scope, Body} tuple */
            int arity;
            const ERL_NIF_TERM *tuple;
            if (!enif_get_tuple(env, argv[4], &arity, &tuple) || arity != 2) {
                py_pool_request_free(req);
                return enif_make_badarg(env);
            }

            req->scope_term = enif_make_copy(req->msg_env, tuple[0]);

            ErlNifBinary body_bin;
            if (enif_inspect_binary(env, tuple[1], &body_bin)) {
                req->body_data = enif_alloc(body_bin.size);
                if (req->body_data == NULL) {
                    py_pool_request_free(req);
                    return make_error(env, "allocation_failed");
                }
                memcpy(req->body_data, body_bin.data, body_bin.size);
                req->body_len = body_bin.size;
            } else {
                req->body_data = NULL;
                req->body_len = 0;
            }
            break;
        }

        case PY_POOL_REQ_WSGI: {
            /* argv[1] = Module, argv[2] = Callable, argv[3] = Environ, argv[4] = unused */
            ErlNifBinary module_bin, callable_bin;
            if (!enif_inspect_binary(env, argv[1], &module_bin) ||
                !enif_inspect_binary(env, argv[2], &callable_bin)) {
                py_pool_request_free(req);
                return enif_make_badarg(env);
            }

            req->module_name = enif_alloc(module_bin.size + 1);
            req->callable_name = enif_alloc(callable_bin.size + 1);
            if (req->module_name == NULL || req->callable_name == NULL) {
                py_pool_request_free(req);
                return make_error(env, "allocation_failed");
            }

            memcpy(req->module_name, module_bin.data, module_bin.size);
            req->module_name[module_bin.size] = '\0';
            memcpy(req->callable_name, callable_bin.data, callable_bin.size);
            req->callable_name[callable_bin.size] = '\0';

            req->environ_term = enif_make_copy(req->msg_env, argv[3]);
            break;
        }

        default:
            py_pool_request_free(req);
            return make_error(env, "unknown_request_type");
    }

    /* Enqueue request */
    if (py_pool_enqueue(req) != 0) {
        py_pool_request_free(req);
        return make_error(env, "enqueue_failed");
    }

    /* Return {ok, RequestId} */
    return enif_make_tuple2(env, ATOM_OK,
                            enif_make_uint64(env, req->request_id));
}

static ERL_NIF_TERM nif_pool_stats(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    py_pool_stats_t stats;
    py_pool_get_stats(&stats);

    /* Build result map */
    ERL_NIF_TERM keys[6], values[6];

    keys[0] = enif_make_atom(env, "num_workers");
    values[0] = enif_make_int(env, stats.num_workers);

    keys[1] = enif_make_atom(env, "initialized");
    values[1] = stats.initialized ? ATOM_TRUE : ATOM_FALSE;

    keys[2] = enif_make_atom(env, "use_subinterpreters");
    values[2] = stats.use_subinterpreters ? ATOM_TRUE : ATOM_FALSE;

    keys[3] = enif_make_atom(env, "free_threaded");
    values[3] = stats.free_threaded ? ATOM_TRUE : ATOM_FALSE;

    keys[4] = enif_make_atom(env, "pending_count");
    values[4] = enif_make_uint64(env, stats.pending_count);

    keys[5] = enif_make_atom(env, "total_enqueued");
    values[5] = enif_make_uint64(env, stats.total_enqueued);

    ERL_NIF_TERM result;
    enif_make_map_from_arrays(env, keys, values, 6, &result);

    return result;
}

/* Initialize pool-specific atoms */
static int pool_atoms_init(ErlNifEnv *env) {
    ATOM_PY_RESPONSE = enif_make_atom(env, "py_response");
    return 0;
}
