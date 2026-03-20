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
 * @file py_subinterp_thread.c
 * @brief OWN_GIL subinterpreter thread pool implementation
 * @author Benoit Chesneau
 *
 * Implements a pthread pool where each thread owns a Python subinterpreter
 * with OWN_GIL for true parallelism.
 */

#include "py_subinterp_thread.h"
#include "py_nif.h"
#include "py_buffer.h"
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#ifdef HAVE_SUBINTERPRETERS

/* ============================================================================
 * Global State
 * ============================================================================ */

/** @brief Global thread pool instance */
subinterp_thread_pool_t g_thread_pool = {0};

/** @brief Resource type for handles (set by NIF load) */
ErlNifResourceType *PY_SUBINTERP_HANDLE_RESOURCE_TYPE = NULL;

/* Forward declarations */
static void *worker_thread_main(void *arg);
static int worker_create_namespace(subinterp_thread_worker_t *w, uint64_t handle_id);
static void worker_destroy_namespace(subinterp_thread_worker_t *w, uint64_t handle_id);
static subinterp_namespace_t *worker_find_namespace(subinterp_thread_worker_t *w, uint64_t handle_id);
static int write_full(int fd, const void *buf, size_t count);
static int read_full(int fd, void *buf, size_t count);

/* Defined in py_callback.c */
extern int create_erlang_module(void);

/* ============================================================================
 * Pool Management
 * ============================================================================ */

int subinterp_thread_pool_init(int num_workers) {
    if (atomic_load(&g_thread_pool.initialized)) {
        return 0;  /* Already initialized */
    }

    /* Set default/cap worker count */
    if (num_workers <= 0) {
        num_workers = SUBINTERP_THREAD_POOL_DEFAULT;
    }
    if (num_workers > SUBINTERP_THREAD_POOL_MAX) {
        num_workers = SUBINTERP_THREAD_POOL_MAX;
    }

    /* Initialize pool state */
    memset(&g_thread_pool, 0, sizeof(g_thread_pool));
    g_thread_pool.num_workers = num_workers;
    atomic_store(&g_thread_pool.next_worker, 0);
    atomic_store(&g_thread_pool.next_handle_id, 1);
    atomic_store(&g_thread_pool.next_request_id, 1);

    /* Create workers - declare i outside loop for cleanup_workers label */
    int i;
    for (i = 0; i < num_workers; i++) {
        subinterp_thread_worker_t *w = &g_thread_pool.workers[i];

        w->worker_id = i;
        atomic_store(&w->running, false);
        atomic_store(&w->shutdown_requested, false);
        atomic_store(&w->initialized, false);
        atomic_store(&w->requests_processed, 0);
        atomic_store(&w->errors_count, 0);
        w->num_namespaces = 0;

        /* Initialize mutexes */
        if (pthread_mutex_init(&w->dispatch_mutex, NULL) != 0) {
            fprintf(stderr, "subinterp_thread_pool_init: failed to init dispatch_mutex for worker %d\n", i);
            goto cleanup_workers;
        }
        if (pthread_mutex_init(&w->ns_mutex, NULL) != 0) {
            fprintf(stderr, "subinterp_thread_pool_init: failed to init ns_mutex for worker %d\n", i);
            pthread_mutex_destroy(&w->dispatch_mutex);
            goto cleanup_workers;
        }

        /* Create pipes */
        if (pipe(w->cmd_pipe) < 0) {
            fprintf(stderr, "subinterp_thread_pool_init: failed to create cmd_pipe for worker %d: %s\n",
                    i, strerror(errno));
            pthread_mutex_destroy(&w->dispatch_mutex);
            pthread_mutex_destroy(&w->ns_mutex);
            goto cleanup_workers;
        }
        if (pipe(w->result_pipe) < 0) {
            fprintf(stderr, "subinterp_thread_pool_init: failed to create result_pipe for worker %d: %s\n",
                    i, strerror(errno));
            close(w->cmd_pipe[0]);
            close(w->cmd_pipe[1]);
            pthread_mutex_destroy(&w->dispatch_mutex);
            pthread_mutex_destroy(&w->ns_mutex);
            goto cleanup_workers;
        }

        /* Set non-blocking on read ends for proper timeout handling */
        /* Actually, keep blocking for simplicity - we control timeouts via protocol */

        /* Start worker thread */
        if (pthread_create(&w->thread, NULL, worker_thread_main, w) != 0) {
            fprintf(stderr, "subinterp_thread_pool_init: failed to create thread for worker %d: %s\n",
                    i, strerror(errno));
            close(w->cmd_pipe[0]);
            close(w->cmd_pipe[1]);
            close(w->result_pipe[0]);
            close(w->result_pipe[1]);
            pthread_mutex_destroy(&w->dispatch_mutex);
            pthread_mutex_destroy(&w->ns_mutex);
            goto cleanup_workers;
        }

        /* Wait for worker to initialize */
        int timeout_ms = 5000;  /* 5 second timeout */
        int waited = 0;
        while (!atomic_load(&w->initialized) && waited < timeout_ms) {
            usleep(10000);  /* 10ms */
            waited += 10;
        }

        if (!atomic_load(&w->initialized)) {
            fprintf(stderr, "subinterp_thread_pool_init: worker %d failed to initialize\n", i);
            atomic_store(&w->shutdown_requested, true);
            /* Write shutdown message to unblock worker if stuck */
            owngil_header_t shutdown = {
                .magic = OWNGIL_MAGIC,
                .version = OWNGIL_PROTOCOL_VERSION,
                .msg_type = MSG_REQUEST,
                .req_type = REQ_SHUTDOWN,
                .payload_len = 0,
            };
            write(w->cmd_pipe[1], &shutdown, sizeof(shutdown));
            pthread_join(w->thread, NULL);
            close(w->cmd_pipe[0]);
            close(w->cmd_pipe[1]);
            close(w->result_pipe[0]);
            close(w->result_pipe[1]);
            pthread_mutex_destroy(&w->dispatch_mutex);
            pthread_mutex_destroy(&w->ns_mutex);
            goto cleanup_workers;
        }
    }

    atomic_store(&g_thread_pool.initialized, true);

#ifdef DEBUG
    fprintf(stderr, "subinterp_thread_pool_init: created %d OWN_GIL workers\n", num_workers);
#endif

    return 0;

cleanup_workers:
    /* Clean up already created workers */
    for (int j = 0; j < i; j++) {
        subinterp_thread_worker_t *w = &g_thread_pool.workers[j];
        atomic_store(&w->shutdown_requested, true);
        owngil_header_t shutdown = {
            .magic = OWNGIL_MAGIC,
            .version = OWNGIL_PROTOCOL_VERSION,
            .msg_type = MSG_REQUEST,
            .req_type = REQ_SHUTDOWN,
            .payload_len = 0,
        };
        write(w->cmd_pipe[1], &shutdown, sizeof(shutdown));
        pthread_join(w->thread, NULL);
        close(w->cmd_pipe[0]);
        close(w->cmd_pipe[1]);
        close(w->result_pipe[0]);
        close(w->result_pipe[1]);
        pthread_mutex_destroy(&w->dispatch_mutex);
        pthread_mutex_destroy(&w->ns_mutex);
    }
    return -1;
}

void subinterp_thread_pool_shutdown(void) {
    if (!atomic_load(&g_thread_pool.initialized)) {
        return;
    }

    /* Mark as not initialized to prevent new work */
    atomic_store(&g_thread_pool.initialized, false);

    /* Signal all workers to shut down */
    for (int i = 0; i < g_thread_pool.num_workers; i++) {
        subinterp_thread_worker_t *w = &g_thread_pool.workers[i];

        if (!atomic_load(&w->running)) {
            continue;
        }

        atomic_store(&w->shutdown_requested, true);

        /* Send shutdown message */
        owngil_header_t shutdown = {
            .magic = OWNGIL_MAGIC,
            .version = OWNGIL_PROTOCOL_VERSION,
            .msg_type = MSG_REQUEST,
            .req_type = REQ_SHUTDOWN,
            .payload_len = 0,
        };
        write_full(w->cmd_pipe[1], &shutdown, sizeof(shutdown));
    }

    /* Wait for all workers to exit */
    for (int i = 0; i < g_thread_pool.num_workers; i++) {
        subinterp_thread_worker_t *w = &g_thread_pool.workers[i];

        if (w->thread != 0) {
            pthread_join(w->thread, NULL);
        }

        /* Close pipes */
        if (w->cmd_pipe[0] >= 0) close(w->cmd_pipe[0]);
        if (w->cmd_pipe[1] >= 0) close(w->cmd_pipe[1]);
        if (w->result_pipe[0] >= 0) close(w->result_pipe[0]);
        if (w->result_pipe[1] >= 0) close(w->result_pipe[1]);

        /* Destroy mutexes */
        pthread_mutex_destroy(&w->dispatch_mutex);
        pthread_mutex_destroy(&w->ns_mutex);
    }

    g_thread_pool.num_workers = 0;

#ifdef DEBUG
    fprintf(stderr, "subinterp_thread_pool_shutdown: complete\n");
#endif
}

bool subinterp_thread_pool_is_ready(void) {
    return atomic_load(&g_thread_pool.initialized);
}

void subinterp_thread_pool_stats(int *num_workers, uint64_t *total_requests,
                                  uint64_t *total_errors) {
    if (num_workers) *num_workers = g_thread_pool.num_workers;

    uint64_t reqs = 0, errs = 0;
    for (int i = 0; i < g_thread_pool.num_workers; i++) {
        reqs += atomic_load(&g_thread_pool.workers[i].requests_processed);
        errs += atomic_load(&g_thread_pool.workers[i].errors_count);
    }
    if (total_requests) *total_requests = reqs;
    if (total_errors) *total_errors = errs;
}

/* ============================================================================
 * Worker Thread Main Loop
 * ============================================================================ */

static void *worker_thread_main(void *arg) {
    subinterp_thread_worker_t *w = (subinterp_thread_worker_t *)arg;

    /* Create OWN_GIL subinterpreter.
     * For OWN_GIL, we need the main GIL to create the subinterpreter,
     * then the subinterpreter gets its own GIL. After creation,
     * we're switched to the new subinterpreter's thread state. */
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 1,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_OWN_GIL,
    };

    /* Acquire main GIL to create subinterpreter */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Save main thread state before creating subinterpreter */
    PyThreadState *main_tstate = PyThreadState_Get();

    PyStatus status = Py_NewInterpreterFromConfig(&w->tstate, &config);
    if (PyStatus_Exception(status) || w->tstate == NULL) {
        fprintf(stderr, "worker %d: failed to create OWN_GIL subinterpreter\n", w->worker_id);
        /* Restore main thread state and release */
        PyThreadState_Swap(main_tstate);
        PyGILState_Release(gstate);
        return NULL;
    }

    /* Now we're in the new subinterpreter's thread state with its own GIL.
     * The main GIL was released when Py_NewInterpreterFromConfig switched to OWN_GIL. */
    w->interp = PyThreadState_GetInterpreter(w->tstate);

    /* Create erlang module in this subinterpreter */
    if (create_erlang_module() < 0) {
        fprintf(stderr, "worker %d: failed to create erlang module\n", w->worker_id);
        PyErr_Clear();
        /* Continue without erlang module - callbacks won't work */
    } else {
        /* Register PyBuffer with erlang module in this subinterpreter */
        if (PyBuffer_register_with_module() < 0) {
            PyErr_Clear();
            /* Non-fatal - PyBuffer just won't be available */
        }
    }

    /* Initialize asyncio for this worker */
    w->asyncio_module = PyImport_ImportModule("asyncio");
    if (w->asyncio_module == NULL) {
        fprintf(stderr, "worker %d: failed to import asyncio\n", w->worker_id);
        PyErr_Clear();
    } else {
        /* Create a new event loop for this worker */
        PyObject *new_event_loop = PyObject_CallMethod(w->asyncio_module,
            "new_event_loop", NULL);
        if (new_event_loop == NULL) {
            fprintf(stderr, "worker %d: failed to create asyncio event loop\n", w->worker_id);
            PyErr_Clear();
        } else {
            w->asyncio_loop = new_event_loop;
            /* Set as the running event loop for this thread */
            PyObject *result = PyObject_CallMethod(w->asyncio_module,
                "set_event_loop", "O", w->asyncio_loop);
            Py_XDECREF(result);
            PyErr_Clear();
        }
    }

    /* Release the subinterpreter's GIL (we'll acquire it per-request) */
    PyEval_SaveThread();

    /* Signal that we're initialized */
    atomic_store(&w->running, true);
    atomic_store(&w->initialized, true);

    /* Main command loop */
    while (!atomic_load(&w->shutdown_requested)) {
        owngil_header_t header;
        int n = read_full(w->cmd_pipe[0], &header, sizeof(header));
        if (n <= 0) {
            if (errno == EINTR) continue;
            break;  /* Pipe closed or error */
        }

        /* Validate header */
        if (header.magic != OWNGIL_MAGIC || header.version != OWNGIL_PROTOCOL_VERSION) {
            fprintf(stderr, "worker %d: invalid protocol header\n", w->worker_id);
            continue;
        }

        /* Handle shutdown */
        if (header.req_type == REQ_SHUTDOWN) {
            break;
        }

        /* Read payload if present */
        unsigned char *payload = NULL;
        if (header.payload_len > 0) {
            payload = malloc(header.payload_len);
            if (payload == NULL) {
                fprintf(stderr, "worker %d: failed to allocate payload\n", w->worker_id);
                continue;
            }
            n = read_full(w->cmd_pipe[0], payload, header.payload_len);
            if (n != (int)header.payload_len) {
                fprintf(stderr, "worker %d: failed to read payload\n", w->worker_id);
                free(payload);
                continue;
            }
        }

        /* Acquire our GIL for Python execution */
        PyEval_RestoreThread(w->tstate);

        /* Handle namespace management (needs GIL for Python dict operations) */
        if (header.req_type == REQ_CREATE_NS) {
            worker_create_namespace(w, header.handle_id);
            PyEval_SaveThread();
            /* Send simple OK response */
            owngil_header_t resp = {
                .magic = OWNGIL_MAGIC,
                .version = OWNGIL_PROTOCOL_VERSION,
                .msg_type = MSG_RESPONSE,
                .request_id = header.request_id,
                .payload_len = 0,
            };
            write_full(w->result_pipe[1], &resp, sizeof(resp));
            free(payload);
            continue;
        }

        if (header.req_type == REQ_DESTROY_NS) {
            worker_destroy_namespace(w, header.handle_id);
            PyEval_SaveThread();
            /* Send simple OK response */
            owngil_header_t resp = {
                .magic = OWNGIL_MAGIC,
                .version = OWNGIL_PROTOCOL_VERSION,
                .msg_type = MSG_RESPONSE,
                .request_id = header.request_id,
                .payload_len = 0,
            };
            write_full(w->result_pipe[1], &resp, sizeof(resp));
            free(payload);
            continue;
        }

        /* Find namespace for this handle */
        subinterp_namespace_t *ns = worker_find_namespace(w, header.handle_id);
        if (ns == NULL) {
            /* Namespace not found - create default one on the fly */
            worker_create_namespace(w, header.handle_id);
            ns = worker_find_namespace(w, header.handle_id);
        }

        /* Process the request */
        owngil_header_t resp_header = {
            .magic = OWNGIL_MAGIC,
            .version = OWNGIL_PROTOCOL_VERSION,
            .msg_type = MSG_RESPONSE,
            .request_id = header.request_id,
            .payload_len = 0,
        };
        unsigned char *resp_payload = NULL;
        size_t resp_payload_len = 0;

        /* Decode payload using temporary env */
        ErlNifEnv *tmp_env = enif_alloc_env();
        ERL_NIF_TERM payload_term;
        int arity;
        const ERL_NIF_TERM *elements;
        bool success = false;

        if (tmp_env != NULL && header.payload_len > 0) {
            if (enif_binary_to_term(tmp_env, payload, header.payload_len,
                                     &payload_term, 0) != 0) {
                if (enif_get_tuple(tmp_env, payload_term, &arity, &elements)) {
                    /* Execute based on request type */
                    PyObject *result = NULL;
                    PyObject *globals = ns ? ns->globals : PyDict_New();
                    PyObject *locals = ns ? ns->locals : PyDict_New();
                    bool owns_globals = (ns == NULL);
                    bool owns_locals = (ns == NULL);

                    /* Check allocation if we own the dicts */
                    if ((owns_globals && globals == NULL) || (owns_locals && locals == NULL)) {
                        if (owns_globals) Py_XDECREF(globals);
                        if (owns_locals) Py_XDECREF(locals);
                        break;
                    }

                    switch (header.req_type) {
                        case REQ_CALL:
                        case REQ_CAST: {
                            /* Payload: {Module, Func, Args, Kwargs} */
                            if (arity >= 3) {
                                ErlNifBinary mod_bin, func_bin;
                                char mod_str[256], func_str[256];

                                /* Get module name */
                                if (enif_inspect_binary(tmp_env, elements[0], &mod_bin)) {
                                    size_t len = mod_bin.size < 255 ? mod_bin.size : 255;
                                    memcpy(mod_str, mod_bin.data, len);
                                    mod_str[len] = '\0';
                                } else if (enif_get_atom(tmp_env, elements[0], mod_str, 256, ERL_NIF_LATIN1)) {
                                    /* Already filled */
                                } else {
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Get function name */
                                if (enif_inspect_binary(tmp_env, elements[1], &func_bin)) {
                                    size_t len = func_bin.size < 255 ? func_bin.size : 255;
                                    memcpy(func_str, func_bin.data, len);
                                    func_str[len] = '\0';
                                } else if (enif_get_atom(tmp_env, elements[1], func_str, 256, ERL_NIF_LATIN1)) {
                                    /* Already filled */
                                } else {
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Import module */
                                PyObject *module = NULL;
                                if (ns && ns->module_cache) {
                                    PyObject *key = PyUnicode_FromString(mod_str);
                                    module = PyDict_GetItem(ns->module_cache, key);
                                    if (module == NULL) {
                                        module = PyImport_ImportModule(mod_str);
                                        if (module) {
                                            PyDict_SetItem(ns->module_cache, key, module);
                                        }
                                    } else {
                                        Py_INCREF(module);
                                    }
                                    Py_DECREF(key);
                                } else {
                                    module = PyImport_ImportModule(mod_str);
                                }

                                if (module == NULL) {
                                    PyErr_Clear();
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Get function */
                                PyObject *func = PyObject_GetAttrString(module, func_str);
                                Py_DECREF(module);

                                if (func == NULL) {
                                    PyErr_Clear();
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Convert args list to Python tuple */
                                ERL_NIF_TERM args_list = elements[2];
                                unsigned int args_len;
                                PyObject *py_args = NULL;

                                if (enif_get_list_length(tmp_env, args_list, &args_len)) {
                                    py_args = PyTuple_New(args_len);
                                    if (py_args) {
                                        ERL_NIF_TERM head, tail = args_list;
                                        for (unsigned int idx = 0; idx < args_len; idx++) {
                                            if (!enif_get_list_cell(tmp_env, tail, &head, &tail)) {
                                                Py_DECREF(py_args);
                                                py_args = NULL;
                                                break;
                                            }
                                            PyObject *py_arg = term_to_py(tmp_env, head);
                                            if (py_arg == NULL) {
                                                Py_DECREF(py_args);
                                                py_args = NULL;
                                                break;
                                            }
                                            PyTuple_SET_ITEM(py_args, idx, py_arg);
                                        }
                                    }
                                }

                                if (py_args == NULL) {
                                    py_args = PyTuple_New(0);
                                }

                                /* Call function */
                                result = PyObject_Call(func, py_args, NULL);
                                Py_DECREF(py_args);
                                Py_DECREF(func);

                                if (result == NULL) {
                                    PyErr_Clear();
                                } else {
                                    success = true;
                                }
                            }
                            break;
                        }

                        case REQ_ASYNC_CALL: {
                            /* Payload: {Module, Func, Args, Kwargs, CallerPid, Ref} */
                            /* For async calls, we run the coroutine and send result via erlang.send() */
                            if (arity >= 6) {
                                ErlNifBinary mod_bin, func_bin;
                                char mod_str[256], func_str[256];

                                /* Get module name */
                                if (enif_inspect_binary(tmp_env, elements[0], &mod_bin)) {
                                    size_t len = mod_bin.size < 255 ? mod_bin.size : 255;
                                    memcpy(mod_str, mod_bin.data, len);
                                    mod_str[len] = '\0';
                                } else if (enif_get_atom(tmp_env, elements[0], mod_str, 256, ERL_NIF_LATIN1)) {
                                    /* Already filled */
                                } else {
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Get function name */
                                if (enif_inspect_binary(tmp_env, elements[1], &func_bin)) {
                                    size_t len = func_bin.size < 255 ? func_bin.size : 255;
                                    memcpy(func_str, func_bin.data, len);
                                    func_str[len] = '\0';
                                } else if (enif_get_atom(tmp_env, elements[1], func_str, 256, ERL_NIF_LATIN1)) {
                                    /* Already filled */
                                } else {
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Import module */
                                PyObject *module = NULL;
                                if (ns && ns->module_cache) {
                                    PyObject *key = PyUnicode_FromString(mod_str);
                                    module = PyDict_GetItem(ns->module_cache, key);
                                    if (module == NULL) {
                                        module = PyImport_ImportModule(mod_str);
                                        if (module) {
                                            PyDict_SetItem(ns->module_cache, key, module);
                                        }
                                    } else {
                                        Py_INCREF(module);
                                    }
                                    Py_DECREF(key);
                                } else {
                                    module = PyImport_ImportModule(mod_str);
                                }

                                if (module == NULL) {
                                    PyErr_Clear();
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Get function */
                                PyObject *func = PyObject_GetAttrString(module, func_str);
                                Py_DECREF(module);

                                if (func == NULL) {
                                    PyErr_Clear();
                                    if (owns_globals) Py_DECREF(globals);
                                    if (owns_locals) Py_DECREF(locals);
                                    break;
                                }

                                /* Convert args list to Python tuple */
                                ERL_NIF_TERM args_list = elements[2];
                                unsigned int args_len;
                                PyObject *py_args = NULL;

                                if (enif_get_list_length(tmp_env, args_list, &args_len)) {
                                    py_args = PyTuple_New(args_len);
                                    if (py_args) {
                                        ERL_NIF_TERM head, tail = args_list;
                                        for (unsigned int idx = 0; idx < args_len; idx++) {
                                            if (!enif_get_list_cell(tmp_env, tail, &head, &tail)) {
                                                Py_DECREF(py_args);
                                                py_args = NULL;
                                                break;
                                            }
                                            PyObject *py_arg = term_to_py(tmp_env, head);
                                            if (py_arg == NULL) {
                                                Py_DECREF(py_args);
                                                py_args = NULL;
                                                break;
                                            }
                                            PyTuple_SET_ITEM(py_args, idx, py_arg);
                                        }
                                    }
                                }

                                if (py_args == NULL) {
                                    py_args = PyTuple_New(0);
                                }

                                /* Call function */
                                result = PyObject_Call(func, py_args, NULL);
                                Py_DECREF(py_args);
                                Py_DECREF(func);

                                if (result == NULL) {
                                    PyErr_Clear();
                                } else {
                                    /* Check if result is a coroutine and run it */
                                    if (w->asyncio_loop != NULL && PyCoro_CheckExact(result)) {
                                        PyObject *final_result = PyObject_CallMethod(
                                            w->asyncio_loop, "run_until_complete", "O", result);
                                        Py_DECREF(result);
                                        result = final_result;
                                        if (result == NULL) {
                                            PyErr_Clear();
                                        }
                                    }
                                    if (result != NULL) {
                                        success = true;
                                    }
                                }

                                /* Send result via erlang.send() to CallerPid */
                                /* elements[4] = CallerPid, elements[5] = Ref */
                                ERL_NIF_TERM result_term;
                                if (success && result != NULL) {
                                    ERL_NIF_TERM py_result = py_to_term(tmp_env, result);
                                    result_term = enif_make_tuple2(tmp_env,
                                        enif_make_atom(tmp_env, "ok"), py_result);
                                } else {
                                    result_term = enif_make_tuple2(tmp_env,
                                        enif_make_atom(tmp_env, "error"),
                                        enif_make_atom(tmp_env, "execution_failed"));
                                }

                                /* Build {async_result, Ref, Result} message */
                                ERL_NIF_TERM msg = enif_make_tuple3(tmp_env,
                                    enif_make_atom(tmp_env, "async_result"),
                                    elements[5],  /* Ref */
                                    result_term);

                                /* Get CallerPid and send */
                                ErlNifPid caller_pid;
                                if (enif_get_local_pid(tmp_env, elements[4], &caller_pid)) {
                                    enif_send(NULL, &caller_pid, tmp_env, msg);
                                }

                                Py_XDECREF(result);
                                result = NULL;  /* Don't process result in normal path */
                                success = false;  /* Already handled */
                            }
                            if (owns_globals) Py_DECREF(globals);
                            if (owns_locals) Py_DECREF(locals);
                            break;
                        }

                        case REQ_EVAL: {
                            /* Payload: {Code, Locals} */
                            if (arity >= 1) {
                                ErlNifBinary code_bin;
                                if (enif_inspect_binary(tmp_env, elements[0], &code_bin)) {
                                    char *code_str = malloc(code_bin.size + 1);
                                    if (code_str) {
                                        memcpy(code_str, code_bin.data, code_bin.size);
                                        code_str[code_bin.size] = '\0';

                                        result = PyRun_String(code_str, Py_eval_input, globals, locals);
                                        free(code_str);

                                        if (result == NULL) {
                                            PyErr_Clear();
                                        } else {
                                            success = true;
                                        }
                                    }
                                }
                            }
                            break;
                        }

                        case REQ_EXEC: {
                            /* Payload: {Code} */
                            if (arity >= 1) {
                                ErlNifBinary code_bin;
                                if (enif_inspect_binary(tmp_env, elements[0], &code_bin)) {
                                    char *code_str = malloc(code_bin.size + 1);
                                    if (code_str) {
                                        memcpy(code_str, code_bin.data, code_bin.size);
                                        code_str[code_bin.size] = '\0';

                                        result = PyRun_String(code_str, Py_file_input, globals, locals);
                                        free(code_str);

                                        if (result == NULL) {
                                            PyErr_Clear();
                                        } else {
                                            Py_DECREF(result);
                                            result = Py_None;
                                            Py_INCREF(result);
                                            success = true;
                                        }
                                    }
                                }
                            }
                            break;
                        }

                        default:
                            break;
                    }

                    /* Clean up owned dicts after switch completes */
                    if (owns_globals) Py_DECREF(globals);
                    if (owns_locals) Py_DECREF(locals);

                    /* Serialize result using py_to_term for full type support */
                    if (success && result != NULL) {
                        ERL_NIF_TERM result_term = py_to_term(tmp_env, result);
                        Py_XDECREF(result);

                        /* Wrap in {ok, Result} */
                        ERL_NIF_TERM ok_tuple = enif_make_tuple2(tmp_env,
                            enif_make_atom(tmp_env, "ok"), result_term);

                        /* Serialize to ETF */
                        ErlNifBinary etf_bin;
                        if (enif_term_to_binary(tmp_env, ok_tuple, &etf_bin)) {
                            resp_payload = malloc(etf_bin.size);
                            if (resp_payload) {
                                memcpy(resp_payload, etf_bin.data, etf_bin.size);
                                resp_payload_len = etf_bin.size;
                            }
                            enif_release_binary(&etf_bin);
                        }
                    } else {
                        resp_header.msg_type = MSG_ERROR;
                        /* Serialize error */
                        ERL_NIF_TERM err_tuple = enif_make_tuple2(tmp_env,
                            enif_make_atom(tmp_env, "error"),
                            enif_make_atom(tmp_env, "execution_failed"));
                        ErlNifBinary etf_bin;
                        if (enif_term_to_binary(tmp_env, err_tuple, &etf_bin)) {
                            resp_payload = malloc(etf_bin.size);
                            if (resp_payload) {
                                memcpy(resp_payload, etf_bin.data, etf_bin.size);
                                resp_payload_len = etf_bin.size;
                            }
                            enif_release_binary(&etf_bin);
                        }
                    }
                }
            }
        }

        if (tmp_env) {
            enif_free_env(tmp_env);
        }

        /* Release GIL */
        PyEval_SaveThread();

        /* Send response (except for cast) */
        if (header.req_type != REQ_CAST) {
            resp_header.payload_len = resp_payload_len;
            write_full(w->result_pipe[1], &resp_header, sizeof(resp_header));
            if (resp_payload_len > 0) {
                write_full(w->result_pipe[1], resp_payload, resp_payload_len);
            }
        }

        free(payload);
        free(resp_payload);

        atomic_fetch_add(&w->requests_processed, 1);
    }

    /* Cleanup */
    PyEval_RestoreThread(w->tstate);

    /* Clean up all namespaces */
    pthread_mutex_lock(&w->ns_mutex);
    for (int i = 0; i < w->num_namespaces; i++) {
        subinterp_namespace_t *ns = &w->namespaces[i];
        if (ns->initialized) {
            Py_XDECREF(ns->asyncio_loop);
            Py_XDECREF(ns->module_cache);
            Py_XDECREF(ns->globals);
            Py_XDECREF(ns->locals);
        }
    }
    w->num_namespaces = 0;
    pthread_mutex_unlock(&w->ns_mutex);

    /* Clean up worker asyncio resources */
    Py_XDECREF(w->asyncio_loop);
    w->asyncio_loop = NULL;
    Py_XDECREF(w->asyncio_module);
    w->asyncio_module = NULL;

    /* End interpreter */
    Py_EndInterpreter(w->tstate);
    w->tstate = NULL;
    w->interp = NULL;

    atomic_store(&w->running, false);

    return NULL;
}

/* ============================================================================
 * Namespace Management
 * ============================================================================ */

static int worker_create_namespace(subinterp_thread_worker_t *w, uint64_t handle_id) {
    pthread_mutex_lock(&w->ns_mutex);

    /* Check if namespace already exists */
    for (int i = 0; i < w->num_namespaces; i++) {
        if (w->namespaces[i].handle_id == handle_id && w->namespaces[i].initialized) {
            pthread_mutex_unlock(&w->ns_mutex);
            return 0;  /* Already exists */
        }
    }

    /* Find empty slot or add new */
    int slot = -1;
    for (int i = 0; i < w->num_namespaces; i++) {
        if (!w->namespaces[i].initialized) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        if (w->num_namespaces >= MAX_NAMESPACES_PER_WORKER) {
            pthread_mutex_unlock(&w->ns_mutex);
            return -1;  /* Full */
        }
        slot = w->num_namespaces++;
    }

    subinterp_namespace_t *ns = &w->namespaces[slot];
    ns->handle_id = handle_id;
    ns->globals = PyDict_New();
    ns->locals = PyDict_New();
    ns->module_cache = PyDict_New();
    ns->asyncio_loop = NULL;  /* Uses worker's shared event loop */
    memset(&ns->owner_pid, 0, sizeof(ns->owner_pid));

    if (ns->globals && ns->locals && ns->module_cache) {
        /* Import __builtins__ */
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(ns->globals, "__builtins__", builtins);
        ns->initialized = true;
    } else {
        Py_XDECREF(ns->globals);
        Py_XDECREF(ns->locals);
        Py_XDECREF(ns->module_cache);
        ns->globals = NULL;
        ns->locals = NULL;
        ns->module_cache = NULL;
        pthread_mutex_unlock(&w->ns_mutex);
        return -1;
    }

    pthread_mutex_unlock(&w->ns_mutex);
    return 0;
}

static void worker_destroy_namespace(subinterp_thread_worker_t *w, uint64_t handle_id) {
    /* Note: Caller must hold the GIL */
    pthread_mutex_lock(&w->ns_mutex);

    for (int i = 0; i < w->num_namespaces; i++) {
        if (w->namespaces[i].handle_id == handle_id && w->namespaces[i].initialized) {
            subinterp_namespace_t *ns = &w->namespaces[i];

            Py_XDECREF(ns->module_cache);
            Py_XDECREF(ns->globals);
            Py_XDECREF(ns->locals);

            ns->initialized = false;
            ns->globals = NULL;
            ns->locals = NULL;
            ns->module_cache = NULL;
            break;
        }
    }

    pthread_mutex_unlock(&w->ns_mutex);
}

static subinterp_namespace_t *worker_find_namespace(subinterp_thread_worker_t *w, uint64_t handle_id) {
    pthread_mutex_lock(&w->ns_mutex);

    for (int i = 0; i < w->num_namespaces; i++) {
        if (w->namespaces[i].handle_id == handle_id && w->namespaces[i].initialized) {
            pthread_mutex_unlock(&w->ns_mutex);
            return &w->namespaces[i];
        }
    }

    pthread_mutex_unlock(&w->ns_mutex);
    return NULL;
}

/* ============================================================================
 * Handle Management
 * ============================================================================ */

int subinterp_thread_handle_create(py_subinterp_handle_t *handle) {
    if (!subinterp_thread_pool_is_ready()) {
        return -1;
    }

    /* Select worker round-robin */
    uint64_t worker_idx = atomic_fetch_add(&g_thread_pool.next_worker, 1);
    int worker_id = worker_idx % g_thread_pool.num_workers;

    /* Generate unique handle ID */
    uint64_t handle_id = atomic_fetch_add(&g_thread_pool.next_handle_id, 1);

    handle->worker_id = worker_id;
    handle->handle_id = handle_id;
    atomic_store(&handle->destroyed, false);

    /* Create namespace in worker */
    subinterp_thread_worker_t *w = &g_thread_pool.workers[worker_id];

    /* Lock dispatch to ensure exclusive access */
    pthread_mutex_lock(&w->dispatch_mutex);

    /* Send create namespace request */
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

    write_full(w->cmd_pipe[1], &header, sizeof(header));

    /* Wait for response */
    owngil_header_t resp;
    read_full(w->result_pipe[0], &resp, sizeof(resp));

    pthread_mutex_unlock(&w->dispatch_mutex);

    return (resp.msg_type == MSG_RESPONSE) ? 0 : -1;
}

void subinterp_thread_handle_destroy(py_subinterp_handle_t *handle) {
    if (atomic_exchange(&handle->destroyed, true)) {
        return;  /* Already destroyed */
    }

    if (!subinterp_thread_pool_is_ready()) {
        return;
    }

    if (handle->worker_id < 0 || handle->worker_id >= g_thread_pool.num_workers) {
        return;
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[handle->worker_id];

    pthread_mutex_lock(&w->dispatch_mutex);

    /* Send destroy namespace request */
    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_DESTROY_NS,
        .request_id = request_id,
        .handle_id = handle->handle_id,
        .payload_len = 0,
    };

    write_full(w->cmd_pipe[1], &header, sizeof(header));

    /* Wait for response */
    owngil_header_t resp;
    read_full(w->result_pipe[0], &resp, sizeof(resp));

    pthread_mutex_unlock(&w->dispatch_mutex);
}

/* ============================================================================
 * Execution API
 * ============================================================================ */

ERL_NIF_TERM subinterp_thread_call(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM module, ERL_NIF_TERM func,
                                    ERL_NIF_TERM args, ERL_NIF_TERM kwargs) {
    if (atomic_load(&handle->destroyed)) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "handle_destroyed"));
    }

    if (!subinterp_thread_pool_is_ready()) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "pool_not_ready"));
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[handle->worker_id];

    /* Build payload tuple: {Module, Func, Args, Kwargs} */
    ERL_NIF_TERM payload_tuple = enif_make_tuple4(env, module, func, args, kwargs);

    /* Serialize to ETF */
    ErlNifBinary payload_bin;
    if (!enif_term_to_binary(env, payload_tuple, &payload_bin)) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "serialization_failed"));
    }

    /* Lock dispatch */
    pthread_mutex_lock(&w->dispatch_mutex);

    /* Send request */
    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_CALL,
        .request_id = request_id,
        .handle_id = handle->handle_id,
        .payload_len = payload_bin.size,
    };

    write_full(w->cmd_pipe[1], &header, sizeof(header));
    write_full(w->cmd_pipe[1], payload_bin.data, payload_bin.size);
    enif_release_binary(&payload_bin);

    /* Read response */
    owngil_header_t resp_header;
    if (read_full(w->result_pipe[0], &resp_header, sizeof(resp_header)) != sizeof(resp_header)) {
        pthread_mutex_unlock(&w->dispatch_mutex);
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "read_failed"));
    }

    ERL_NIF_TERM result;
    if (resp_header.payload_len > 0) {
        unsigned char *resp_payload = enif_alloc(resp_header.payload_len);
        if (resp_payload == NULL) {
            pthread_mutex_unlock(&w->dispatch_mutex);
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "alloc_failed"));
        }

        if (read_full(w->result_pipe[0], resp_payload, resp_header.payload_len)
                != (int)resp_header.payload_len) {
            enif_free(resp_payload);
            pthread_mutex_unlock(&w->dispatch_mutex);
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "read_failed"));
        }

        /* Deserialize response */
        if (enif_binary_to_term(env, resp_payload, resp_header.payload_len,
                                 &result, 0) == 0) {
            result = enif_make_tuple2(env, enif_make_atom(env, "error"),
                                      enif_make_atom(env, "deserialize_failed"));
        }

        enif_free(resp_payload);
    } else {
        result = enif_make_atom(env, "ok");
    }

    pthread_mutex_unlock(&w->dispatch_mutex);
    return result;
}

ERL_NIF_TERM subinterp_thread_eval(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM code, ERL_NIF_TERM locals) {
    if (atomic_load(&handle->destroyed)) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "handle_destroyed"));
    }

    if (!subinterp_thread_pool_is_ready()) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "pool_not_ready"));
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[handle->worker_id];

    /* Build payload tuple: {Code, Locals} */
    ERL_NIF_TERM payload_tuple = enif_make_tuple2(env, code, locals);

    ErlNifBinary payload_bin;
    if (!enif_term_to_binary(env, payload_tuple, &payload_bin)) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "serialization_failed"));
    }

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_EVAL,
        .request_id = request_id,
        .handle_id = handle->handle_id,
        .payload_len = payload_bin.size,
    };

    write_full(w->cmd_pipe[1], &header, sizeof(header));
    write_full(w->cmd_pipe[1], payload_bin.data, payload_bin.size);
    enif_release_binary(&payload_bin);

    owngil_header_t resp_header;
    if (read_full(w->result_pipe[0], &resp_header, sizeof(resp_header)) != sizeof(resp_header)) {
        pthread_mutex_unlock(&w->dispatch_mutex);
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "read_failed"));
    }

    ERL_NIF_TERM result;
    if (resp_header.payload_len > 0) {
        unsigned char *resp_payload = enif_alloc(resp_header.payload_len);
        if (resp_payload == NULL) {
            pthread_mutex_unlock(&w->dispatch_mutex);
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "alloc_failed"));
        }

        if (read_full(w->result_pipe[0], resp_payload, resp_header.payload_len)
                != (int)resp_header.payload_len) {
            enif_free(resp_payload);
            pthread_mutex_unlock(&w->dispatch_mutex);
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "read_failed"));
        }

        if (enif_binary_to_term(env, resp_payload, resp_header.payload_len,
                                 &result, 0) == 0) {
            result = enif_make_tuple2(env, enif_make_atom(env, "error"),
                                      enif_make_atom(env, "deserialize_failed"));
        }

        enif_free(resp_payload);
    } else {
        result = enif_make_atom(env, "ok");
    }

    pthread_mutex_unlock(&w->dispatch_mutex);
    return result;
}

ERL_NIF_TERM subinterp_thread_exec(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM code) {
    if (atomic_load(&handle->destroyed)) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "handle_destroyed"));
    }

    if (!subinterp_thread_pool_is_ready()) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "pool_not_ready"));
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[handle->worker_id];

    /* Build payload tuple: {Code} */
    ERL_NIF_TERM payload_tuple = enif_make_tuple1(env, code);

    ErlNifBinary payload_bin;
    if (!enif_term_to_binary(env, payload_tuple, &payload_bin)) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "serialization_failed"));
    }

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_EXEC,
        .request_id = request_id,
        .handle_id = handle->handle_id,
        .payload_len = payload_bin.size,
    };

    write_full(w->cmd_pipe[1], &header, sizeof(header));
    write_full(w->cmd_pipe[1], payload_bin.data, payload_bin.size);
    enif_release_binary(&payload_bin);

    owngil_header_t resp_header;
    if (read_full(w->result_pipe[0], &resp_header, sizeof(resp_header)) != sizeof(resp_header)) {
        pthread_mutex_unlock(&w->dispatch_mutex);
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                enif_make_atom(env, "read_failed"));
    }

    ERL_NIF_TERM result;
    if (resp_header.payload_len > 0) {
        unsigned char *resp_payload = enif_alloc(resp_header.payload_len);
        if (resp_payload == NULL) {
            pthread_mutex_unlock(&w->dispatch_mutex);
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "alloc_failed"));
        }

        if (read_full(w->result_pipe[0], resp_payload, resp_header.payload_len)
                != (int)resp_header.payload_len) {
            enif_free(resp_payload);
            pthread_mutex_unlock(&w->dispatch_mutex);
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "read_failed"));
        }

        if (enif_binary_to_term(env, resp_payload, resp_header.payload_len,
                                 &result, 0) == 0) {
            result = enif_make_tuple2(env, enif_make_atom(env, "error"),
                                      enif_make_atom(env, "deserialize_failed"));
        }

        enif_free(resp_payload);
    } else {
        result = enif_make_atom(env, "ok");
    }

    pthread_mutex_unlock(&w->dispatch_mutex);
    return result;
}

ERL_NIF_TERM subinterp_thread_cast(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                    ERL_NIF_TERM module, ERL_NIF_TERM func,
                                    ERL_NIF_TERM args) {
    if (atomic_load(&handle->destroyed)) {
        return enif_make_atom(env, "ok");  /* Silently ignore for cast */
    }

    if (!subinterp_thread_pool_is_ready()) {
        return enif_make_atom(env, "ok");  /* Silently ignore for cast */
    }

    subinterp_thread_worker_t *w = &g_thread_pool.workers[handle->worker_id];

    /* Build payload tuple: {Module, Func, Args} */
    ERL_NIF_TERM payload_tuple = enif_make_tuple3(env, module, func, args);

    ErlNifBinary payload_bin;
    if (!enif_term_to_binary(env, payload_tuple, &payload_bin)) {
        return enif_make_atom(env, "ok");  /* Silently fail for cast */
    }

    pthread_mutex_lock(&w->dispatch_mutex);

    uint64_t request_id = atomic_fetch_add(&g_thread_pool.next_request_id, 1);
    owngil_header_t header = {
        .magic = OWNGIL_MAGIC,
        .version = OWNGIL_PROTOCOL_VERSION,
        .msg_type = MSG_REQUEST,
        .req_type = REQ_CAST,
        .request_id = request_id,
        .handle_id = handle->handle_id,
        .payload_len = payload_bin.size,
    };

    write_full(w->cmd_pipe[1], &header, sizeof(header));
    write_full(w->cmd_pipe[1], payload_bin.data, payload_bin.size);
    enif_release_binary(&payload_bin);

    pthread_mutex_unlock(&w->dispatch_mutex);

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM subinterp_thread_async_call(ErlNifEnv *env, py_subinterp_handle_t *handle,
                                          ERL_NIF_TERM module, ERL_NIF_TERM func,
                                          ERL_NIF_TERM args, ErlNifPid *caller_pid,
                                          ERL_NIF_TERM ref) {
    /* For async, we send the request but don't wait for response.
     * The worker thread uses erlang.send() to deliver result. */
    (void)caller_pid;
    (void)ref;

    /* For now, implement as sync call - async requires erlang.send support */
    ERL_NIF_TERM kwargs = enif_make_new_map(env);
    return subinterp_thread_call(env, handle, module, func, args, kwargs);
}

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

static int write_full(int fd, const void *buf, size_t count) {
    const unsigned char *p = buf;
    size_t remaining = count;

    while (remaining > 0) {
        ssize_t n = write(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += n;
        remaining -= n;
    }

    return count;
}

static int read_full(int fd, void *buf, size_t count) {
    unsigned char *p = buf;
    size_t remaining = count;

    while (remaining > 0) {
        ssize_t n = read(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            return count - remaining;  /* EOF */
        }
        p += n;
        remaining -= n;
    }

    return count;
}

#endif /* HAVE_SUBINTERPRETERS */
