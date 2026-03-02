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
 * @file py_wsgi.c
 * @brief WSGI marshalling optimizations implementation
 * @author Benoit Chesneau
 *
 * This file implements optimized WSGI request/response handling:
 * - Interned Python string keys for environ dicts
 * - Pre-built constant values reused across requests
 * - Direct WSGI NIF for maximum performance
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Per-Interpreter State Management
 * ============================================================================ */

/* Storage for per-interpreter states */
static wsgi_interp_state_t *g_wsgi_interp_states[WSGI_MAX_INTERPRETERS];
static int g_wsgi_interp_state_count = 0;
static pthread_mutex_t g_wsgi_interp_state_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Flag: WSGI subsystem is initialized (not per-interpreter) */
static bool g_wsgi_initialized = false;

/**
 * @brief Initialize a single interpreter state for WSGI
 */
static int init_wsgi_interp_state(wsgi_interp_state_t *state) {
    /* CGI/1.1 environ keys */
    state->key_request_method = PyUnicode_InternFromString("REQUEST_METHOD");
    if (!state->key_request_method) return -1;
    state->key_script_name = PyUnicode_InternFromString("SCRIPT_NAME");
    if (!state->key_script_name) return -1;
    state->key_path_info = PyUnicode_InternFromString("PATH_INFO");
    if (!state->key_path_info) return -1;
    state->key_query_string = PyUnicode_InternFromString("QUERY_STRING");
    if (!state->key_query_string) return -1;
    state->key_content_type = PyUnicode_InternFromString("CONTENT_TYPE");
    if (!state->key_content_type) return -1;
    state->key_content_length = PyUnicode_InternFromString("CONTENT_LENGTH");
    if (!state->key_content_length) return -1;
    state->key_server_name = PyUnicode_InternFromString("SERVER_NAME");
    if (!state->key_server_name) return -1;
    state->key_server_port = PyUnicode_InternFromString("SERVER_PORT");
    if (!state->key_server_port) return -1;
    state->key_server_protocol = PyUnicode_InternFromString("SERVER_PROTOCOL");
    if (!state->key_server_protocol) return -1;
    state->key_remote_host = PyUnicode_InternFromString("REMOTE_HOST");
    if (!state->key_remote_host) return -1;
    state->key_remote_addr = PyUnicode_InternFromString("REMOTE_ADDR");
    if (!state->key_remote_addr) return -1;

    /* WSGI environ keys */
    state->key_wsgi_version = PyUnicode_InternFromString("wsgi.version");
    if (!state->key_wsgi_version) return -1;
    state->key_wsgi_url_scheme = PyUnicode_InternFromString("wsgi.url_scheme");
    if (!state->key_wsgi_url_scheme) return -1;
    state->key_wsgi_input = PyUnicode_InternFromString("wsgi.input");
    if (!state->key_wsgi_input) return -1;
    state->key_wsgi_errors = PyUnicode_InternFromString("wsgi.errors");
    if (!state->key_wsgi_errors) return -1;
    state->key_wsgi_multithread = PyUnicode_InternFromString("wsgi.multithread");
    if (!state->key_wsgi_multithread) return -1;
    state->key_wsgi_multiprocess = PyUnicode_InternFromString("wsgi.multiprocess");
    if (!state->key_wsgi_multiprocess) return -1;
    state->key_wsgi_run_once = PyUnicode_InternFromString("wsgi.run_once");
    if (!state->key_wsgi_run_once) return -1;

    /* WSGI extensions */
    state->key_wsgi_file_wrapper = PyUnicode_InternFromString("wsgi.file_wrapper");
    if (!state->key_wsgi_file_wrapper) return -1;
    state->key_wsgi_input_terminated = PyUnicode_InternFromString("wsgi.input_terminated");
    if (!state->key_wsgi_input_terminated) return -1;
    state->key_wsgi_early_hints = PyUnicode_InternFromString("wsgi.early_hints");
    if (!state->key_wsgi_early_hints) return -1;

    /* HTTP protocol constants */
    state->http_10 = PyUnicode_InternFromString("HTTP/1.0");
    if (!state->http_10) return -1;
    state->http_11 = PyUnicode_InternFromString("HTTP/1.1");
    if (!state->http_11) return -1;

    /* Scheme constants */
    state->scheme_http = PyUnicode_InternFromString("http");
    if (!state->scheme_http) return -1;
    state->scheme_https = PyUnicode_InternFromString("https");
    if (!state->scheme_https) return -1;

    /* HTTP methods */
    state->method_get = PyUnicode_InternFromString("GET");
    if (!state->method_get) return -1;
    state->method_post = PyUnicode_InternFromString("POST");
    if (!state->method_post) return -1;
    state->method_put = PyUnicode_InternFromString("PUT");
    if (!state->method_put) return -1;
    state->method_delete = PyUnicode_InternFromString("DELETE");
    if (!state->method_delete) return -1;
    state->method_head = PyUnicode_InternFromString("HEAD");
    if (!state->method_head) return -1;
    state->method_options = PyUnicode_InternFromString("OPTIONS");
    if (!state->method_options) return -1;
    state->method_patch = PyUnicode_InternFromString("PATCH");
    if (!state->method_patch) return -1;

    /* WSGI version tuple (1, 0) */
    {
        PyObject *v1 = PyLong_FromLong(1);
        PyObject *v2 = PyLong_FromLong(0);
        if (!v1 || !v2) {
            Py_XDECREF(v1);
            Py_XDECREF(v2);
            return -1;
        }
        state->wsgi_version_tuple = PyTuple_Pack(2, v1, v2);
        Py_DECREF(v1);
        Py_DECREF(v2);
        if (!state->wsgi_version_tuple) return -1;
    }

    /* Boolean constants */
    state->py_true = Py_True;
    Py_INCREF(state->py_true);
    state->py_false = Py_False;
    Py_INCREF(state->py_false);

    /* Empty values */
    state->empty_string = PyUnicode_InternFromString("");
    if (!state->empty_string) return -1;
    state->empty_bytes = PyBytes_FromStringAndSize("", 0);
    if (!state->empty_bytes) return -1;

    /* Import io.BytesIO for wsgi.input */
    PyObject *io_module = PyImport_ImportModule("io");
    if (!io_module) return -1;
    state->bytesio_class = PyObject_GetAttrString(io_module, "BytesIO");
    Py_DECREF(io_module);
    if (!state->bytesio_class) return -1;

    /* HTTP header prefix */
    state->http_prefix = PyUnicode_InternFromString("HTTP_");
    if (!state->http_prefix) return -1;

    state->initialized = true;
    return 0;
}

/**
 * @brief Clean up a single interpreter state for WSGI
 */
static void cleanup_wsgi_interp_state_internal(wsgi_interp_state_t *state) {
    if (!state || !state->initialized) return;

    Py_XDECREF(state->key_request_method);
    Py_XDECREF(state->key_script_name);
    Py_XDECREF(state->key_path_info);
    Py_XDECREF(state->key_query_string);
    Py_XDECREF(state->key_content_type);
    Py_XDECREF(state->key_content_length);
    Py_XDECREF(state->key_server_name);
    Py_XDECREF(state->key_server_port);
    Py_XDECREF(state->key_server_protocol);
    Py_XDECREF(state->key_remote_host);
    Py_XDECREF(state->key_remote_addr);
    Py_XDECREF(state->key_wsgi_version);
    Py_XDECREF(state->key_wsgi_url_scheme);
    Py_XDECREF(state->key_wsgi_input);
    Py_XDECREF(state->key_wsgi_errors);
    Py_XDECREF(state->key_wsgi_multithread);
    Py_XDECREF(state->key_wsgi_multiprocess);
    Py_XDECREF(state->key_wsgi_run_once);
    Py_XDECREF(state->key_wsgi_file_wrapper);
    Py_XDECREF(state->key_wsgi_input_terminated);
    Py_XDECREF(state->key_wsgi_early_hints);
    Py_XDECREF(state->http_10);
    Py_XDECREF(state->http_11);
    Py_XDECREF(state->scheme_http);
    Py_XDECREF(state->scheme_https);
    Py_XDECREF(state->method_get);
    Py_XDECREF(state->method_post);
    Py_XDECREF(state->method_put);
    Py_XDECREF(state->method_delete);
    Py_XDECREF(state->method_head);
    Py_XDECREF(state->method_options);
    Py_XDECREF(state->method_patch);
    Py_XDECREF(state->wsgi_version_tuple);
    Py_XDECREF(state->py_true);
    Py_XDECREF(state->py_false);
    Py_XDECREF(state->empty_string);
    Py_XDECREF(state->empty_bytes);
    Py_XDECREF(state->bytesio_class);
    Py_XDECREF(state->http_prefix);

    state->initialized = false;
}

/**
 * @brief Get or create per-interpreter state for WSGI
 */
wsgi_interp_state_t *get_wsgi_interp_state(void) {
    PyInterpreterState *interp = PyInterpreterState_Get();

    /* Fast path: check existing states without lock */
    for (int i = 0; i < g_wsgi_interp_state_count; i++) {
        if (g_wsgi_interp_states[i] && g_wsgi_interp_states[i]->interp == interp) {
            return g_wsgi_interp_states[i];
        }
    }

    /* Slow path: acquire lock and create new state */
    pthread_mutex_lock(&g_wsgi_interp_state_mutex);

    /* Double-check after acquiring lock */
    for (int i = 0; i < g_wsgi_interp_state_count; i++) {
        if (g_wsgi_interp_states[i] && g_wsgi_interp_states[i]->interp == interp) {
            pthread_mutex_unlock(&g_wsgi_interp_state_mutex);
            return g_wsgi_interp_states[i];
        }
    }

    /* Check capacity */
    if (g_wsgi_interp_state_count >= WSGI_MAX_INTERPRETERS) {
        pthread_mutex_unlock(&g_wsgi_interp_state_mutex);
        PyErr_SetString(PyExc_RuntimeError, "Too many Python interpreters for WSGI");
        return NULL;
    }

    /* Allocate and initialize new state */
    wsgi_interp_state_t *state = enif_alloc(sizeof(wsgi_interp_state_t));
    if (!state) {
        pthread_mutex_unlock(&g_wsgi_interp_state_mutex);
        PyErr_NoMemory();
        return NULL;
    }

    memset(state, 0, sizeof(wsgi_interp_state_t));
    state->interp = interp;

    if (init_wsgi_interp_state(state) < 0) {
        cleanup_wsgi_interp_state_internal(state);
        enif_free(state);
        pthread_mutex_unlock(&g_wsgi_interp_state_mutex);
        return NULL;
    }

    g_wsgi_interp_states[g_wsgi_interp_state_count++] = state;
    pthread_mutex_unlock(&g_wsgi_interp_state_mutex);

    return state;
}

/**
 * @brief Clean up state for a specific interpreter
 */
void cleanup_wsgi_interp_state(PyInterpreterState *interp) {
    if (!interp) {
        interp = PyInterpreterState_Get();
    }

    pthread_mutex_lock(&g_wsgi_interp_state_mutex);

    for (int i = 0; i < g_wsgi_interp_state_count; i++) {
        if (g_wsgi_interp_states[i] && g_wsgi_interp_states[i]->interp == interp) {
            cleanup_wsgi_interp_state_internal(g_wsgi_interp_states[i]);
            enif_free(g_wsgi_interp_states[i]);

            /* Shift remaining states down */
            for (int j = i; j < g_wsgi_interp_state_count - 1; j++) {
                g_wsgi_interp_states[j] = g_wsgi_interp_states[j + 1];
            }
            g_wsgi_interp_states[--g_wsgi_interp_state_count] = NULL;
            break;
        }
    }

    pthread_mutex_unlock(&g_wsgi_interp_state_mutex);
}

/**
 * @brief Clean up all interpreter states
 */
void cleanup_all_wsgi_interp_states(void) {
    pthread_mutex_lock(&g_wsgi_interp_state_mutex);

    for (int i = 0; i < g_wsgi_interp_state_count; i++) {
        if (g_wsgi_interp_states[i]) {
            cleanup_wsgi_interp_state_internal(g_wsgi_interp_states[i]);
            enif_free(g_wsgi_interp_states[i]);
            g_wsgi_interp_states[i] = NULL;
        }
    }
    g_wsgi_interp_state_count = 0;

    pthread_mutex_unlock(&g_wsgi_interp_state_mutex);
}

/* ============================================================================
 * Initialization / Cleanup
 * ============================================================================ */

static int wsgi_scope_init(void) {
    if (g_wsgi_initialized) {
        return 0;
    }

    wsgi_interp_state_t *state = get_wsgi_interp_state();
    if (!state) {
        return -1;
    }

    g_wsgi_initialized = true;
    return 0;
}

static void wsgi_scope_cleanup(void) {
    if (!g_wsgi_initialized) {
        return;
    }

    cleanup_all_wsgi_interp_states();
    g_wsgi_initialized = false;
}

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

static PyObject *wsgi_get_method(const char *method, size_t len) {
    /* Check common methods - return new reference */
    switch (len) {
        case 3:
            if (memcmp(method, "GET", 3) == 0) {
                Py_INCREF(WSGI_METHOD_GET);
                return WSGI_METHOD_GET;
            }
            if (memcmp(method, "PUT", 3) == 0) {
                Py_INCREF(WSGI_METHOD_PUT);
                return WSGI_METHOD_PUT;
            }
            break;
        case 4:
            if (memcmp(method, "POST", 4) == 0) {
                Py_INCREF(WSGI_METHOD_POST);
                return WSGI_METHOD_POST;
            }
            if (memcmp(method, "HEAD", 4) == 0) {
                Py_INCREF(WSGI_METHOD_HEAD);
                return WSGI_METHOD_HEAD;
            }
            break;
        case 5:
            if (memcmp(method, "PATCH", 5) == 0) {
                Py_INCREF(WSGI_METHOD_PATCH);
                return WSGI_METHOD_PATCH;
            }
            break;
        case 6:
            if (memcmp(method, "DELETE", 6) == 0) {
                Py_INCREF(WSGI_METHOD_DELETE);
                return WSGI_METHOD_DELETE;
            }
            break;
        case 7:
            if (memcmp(method, "OPTIONS", 7) == 0) {
                Py_INCREF(WSGI_METHOD_OPTIONS);
                return WSGI_METHOD_OPTIONS;
            }
            break;
    }

    /* Uncommon method - create new string */
    return PyUnicode_FromStringAndSize(method, len);
}

/**
 * @brief Get cached key for an environ key term
 */
static PyObject *wsgi_get_key_for_term(ErlNifEnv *env, ERL_NIF_TERM term) {
    char key_buf[128];
    ErlNifBinary key_bin;

    if (enif_is_atom(env, term)) {
        if (enif_get_atom(env, term, key_buf, sizeof(key_buf), ERL_NIF_LATIN1)) {
            /* Map to cached keys */
            if (strcmp(key_buf, "REQUEST_METHOD") == 0) return WSGI_KEY_REQUEST_METHOD;
            if (strcmp(key_buf, "SCRIPT_NAME") == 0) return WSGI_KEY_SCRIPT_NAME;
            if (strcmp(key_buf, "PATH_INFO") == 0) return WSGI_KEY_PATH_INFO;
            if (strcmp(key_buf, "QUERY_STRING") == 0) return WSGI_KEY_QUERY_STRING;
            if (strcmp(key_buf, "CONTENT_TYPE") == 0) return WSGI_KEY_CONTENT_TYPE;
            if (strcmp(key_buf, "CONTENT_LENGTH") == 0) return WSGI_KEY_CONTENT_LENGTH;
            if (strcmp(key_buf, "SERVER_NAME") == 0) return WSGI_KEY_SERVER_NAME;
            if (strcmp(key_buf, "SERVER_PORT") == 0) return WSGI_KEY_SERVER_PORT;
            if (strcmp(key_buf, "SERVER_PROTOCOL") == 0) return WSGI_KEY_SERVER_PROTOCOL;
            if (strcmp(key_buf, "REMOTE_HOST") == 0) return WSGI_KEY_REMOTE_HOST;
            if (strcmp(key_buf, "REMOTE_ADDR") == 0) return WSGI_KEY_REMOTE_ADDR;
        }
    } else if (enif_inspect_binary(env, term, &key_bin)) {
        /* Check for binary keys */
        if (key_bin.size < sizeof(key_buf)) {
            memcpy(key_buf, key_bin.data, key_bin.size);
            key_buf[key_bin.size] = '\0';

            /* Check common keys */
            if (strcmp(key_buf, "REQUEST_METHOD") == 0) return WSGI_KEY_REQUEST_METHOD;
            if (strcmp(key_buf, "SCRIPT_NAME") == 0) return WSGI_KEY_SCRIPT_NAME;
            if (strcmp(key_buf, "PATH_INFO") == 0) return WSGI_KEY_PATH_INFO;
            if (strcmp(key_buf, "QUERY_STRING") == 0) return WSGI_KEY_QUERY_STRING;
            if (strcmp(key_buf, "CONTENT_TYPE") == 0) return WSGI_KEY_CONTENT_TYPE;
            if (strcmp(key_buf, "CONTENT_LENGTH") == 0) return WSGI_KEY_CONTENT_LENGTH;
            if (strcmp(key_buf, "SERVER_NAME") == 0) return WSGI_KEY_SERVER_NAME;
            if (strcmp(key_buf, "SERVER_PORT") == 0) return WSGI_KEY_SERVER_PORT;
            if (strcmp(key_buf, "SERVER_PROTOCOL") == 0) return WSGI_KEY_SERVER_PROTOCOL;
            if (strcmp(key_buf, "REMOTE_HOST") == 0) return WSGI_KEY_REMOTE_HOST;
            if (strcmp(key_buf, "REMOTE_ADDR") == 0) return WSGI_KEY_REMOTE_ADDR;
            if (strcmp(key_buf, "wsgi.version") == 0) return WSGI_KEY_WSGI_VERSION;
            if (strcmp(key_buf, "wsgi.url_scheme") == 0) return WSGI_KEY_WSGI_URL_SCHEME;
            if (strcmp(key_buf, "wsgi.input") == 0) return WSGI_KEY_WSGI_INPUT;
            if (strcmp(key_buf, "wsgi.errors") == 0) return WSGI_KEY_WSGI_ERRORS;
            if (strcmp(key_buf, "wsgi.multithread") == 0) return WSGI_KEY_WSGI_MULTITHREAD;
            if (strcmp(key_buf, "wsgi.multiprocess") == 0) return WSGI_KEY_WSGI_MULTIPROCESS;
            if (strcmp(key_buf, "wsgi.run_once") == 0) return WSGI_KEY_WSGI_RUN_ONCE;
        }
    }

    return NULL;
}

/**
 * @brief Convert WSGI environ map from Erlang to Python dict
 */
static PyObject *wsgi_environ_from_map(ErlNifEnv *env, ERL_NIF_TERM environ_map) {
    if (!g_wsgi_initialized) {
        PyErr_SetString(PyExc_RuntimeError, "WSGI cache not initialized");
        return NULL;
    }

    if (!enif_is_map(env, environ_map)) {
        PyErr_SetString(PyExc_TypeError, "expected map for WSGI environ");
        return NULL;
    }

    PyObject *environ = PyDict_New();
    if (environ == NULL) {
        return NULL;
    }

    ERL_NIF_TERM key, value;
    ErlNifMapIterator iter;

    enif_map_iterator_create(env, environ_map, &iter, ERL_NIF_MAP_ITERATOR_FIRST);

    while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
        PyObject *py_key = wsgi_get_key_for_term(env, key);
        bool key_borrowed = (py_key != NULL);

        if (py_key == NULL) {
            /* Unknown key - create new Python string */
            py_key = term_to_py(env, key);
            if (py_key == NULL) {
                enif_map_iterator_destroy(env, &iter);
                Py_DECREF(environ);
                return NULL;
            }
        }

        /* Convert value with optimizations for known keys */
        PyObject *py_value = NULL;

        if (py_key == WSGI_KEY_REQUEST_METHOD) {
            ErlNifBinary method_bin;
            if (enif_inspect_binary(env, value, &method_bin)) {
                py_value = wsgi_get_method((char *)method_bin.data, method_bin.size);
            }
        } else if (py_key == WSGI_KEY_SERVER_PROTOCOL) {
            ErlNifBinary proto_bin;
            if (enif_inspect_binary(env, value, &proto_bin)) {
                if (proto_bin.size == 8 && memcmp(proto_bin.data, "HTTP/1.1", 8) == 0) {
                    Py_INCREF(WSGI_HTTP_11);
                    py_value = WSGI_HTTP_11;
                } else if (proto_bin.size == 8 && memcmp(proto_bin.data, "HTTP/1.0", 8) == 0) {
                    Py_INCREF(WSGI_HTTP_10);
                    py_value = WSGI_HTTP_10;
                }
            }
        } else if (py_key == WSGI_KEY_WSGI_URL_SCHEME) {
            ErlNifBinary scheme_bin;
            if (enif_inspect_binary(env, value, &scheme_bin)) {
                if (scheme_bin.size == 4 && memcmp(scheme_bin.data, "http", 4) == 0) {
                    Py_INCREF(WSGI_SCHEME_HTTP);
                    py_value = WSGI_SCHEME_HTTP;
                } else if (scheme_bin.size == 5 && memcmp(scheme_bin.data, "https", 5) == 0) {
                    Py_INCREF(WSGI_SCHEME_HTTPS);
                    py_value = WSGI_SCHEME_HTTPS;
                }
            }
        } else if (py_key == WSGI_KEY_SCRIPT_NAME || py_key == WSGI_KEY_PATH_INFO ||
                   py_key == WSGI_KEY_QUERY_STRING) {
            ErlNifBinary str_bin;
            if (enif_inspect_binary(env, value, &str_bin) && str_bin.size == 0) {
                Py_INCREF(WSGI_EMPTY_STRING);
                py_value = WSGI_EMPTY_STRING;
            }
        } else if (py_key == WSGI_KEY_WSGI_VERSION) {
            /* Always use cached version tuple */
            Py_INCREF(WSGI_VERSION_TUPLE);
            py_value = WSGI_VERSION_TUPLE;
        } else if (py_key == WSGI_KEY_WSGI_MULTITHREAD ||
                   py_key == WSGI_KEY_WSGI_MULTIPROCESS) {
            /* These are typically true */
            if (enif_is_atom(env, value)) {
                char atom_buf[16];
                if (enif_get_atom(env, value, atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
                    if (strcmp(atom_buf, "true") == 0) {
                        Py_INCREF(WSGI_PY_TRUE);
                        py_value = WSGI_PY_TRUE;
                    } else if (strcmp(atom_buf, "false") == 0) {
                        Py_INCREF(WSGI_PY_FALSE);
                        py_value = WSGI_PY_FALSE;
                    }
                }
            }
        } else if (py_key == WSGI_KEY_WSGI_RUN_ONCE) {
            /* Typically false */
            if (enif_is_atom(env, value)) {
                char atom_buf[16];
                if (enif_get_atom(env, value, atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
                    if (strcmp(atom_buf, "false") == 0) {
                        Py_INCREF(WSGI_PY_FALSE);
                        py_value = WSGI_PY_FALSE;
                    } else if (strcmp(atom_buf, "true") == 0) {
                        Py_INCREF(WSGI_PY_TRUE);
                        py_value = WSGI_PY_TRUE;
                    }
                }
            }
        } else if (py_key == WSGI_KEY_WSGI_INPUT) {
            /* Create BytesIO from binary body */
            ErlNifBinary body_bin;
            if (enif_inspect_binary(env, value, &body_bin)) {
                PyObject *body_bytes = PyBytes_FromStringAndSize(
                    (char *)body_bin.data, body_bin.size);
                if (body_bytes) {
                    py_value = PyObject_CallFunctionObjArgs(
                        WSGI_BYTESIO_CLASS, body_bytes, NULL);
                    Py_DECREF(body_bytes);
                }
            }
        }

        /* Generic conversion if no optimization applied */
        if (py_value == NULL) {
            py_value = term_to_py(env, value);
            if (py_value == NULL) {
                if (!key_borrowed) {
                    Py_DECREF(py_key);
                }
                enif_map_iterator_destroy(env, &iter);
                Py_DECREF(environ);
                return NULL;
            }
        }

        /* Use interned key reference */
        if (key_borrowed) {
            Py_INCREF(py_key);
        }

        int set_result = PyDict_SetItem(environ, py_key, py_value);
        Py_DECREF(py_key);
        Py_DECREF(py_value);

        if (set_result < 0) {
            enif_map_iterator_destroy(env, &iter);
            Py_DECREF(environ);
            return NULL;
        }

        enif_map_iterator_next(env, &iter);
    }

    enif_map_iterator_destroy(env, &iter);
    return environ;
}

/* ============================================================================
 * NIF Function
 * ============================================================================ */

static ERL_NIF_TERM nif_wsgi_run(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc < 4) {
        return make_error(env, "badarg");
    }

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    if (!g_wsgi_initialized) {
        /* Initialize lazily */
        PyGILState_STATE init_gstate = PyGILState_Ensure();
        int init_result = wsgi_scope_init();
        PyGILState_Release(init_gstate);
        if (init_result < 0) {
            return make_error(env, "wsgi_init_failed");
        }
    }

    ErlNifBinary runner_bin, module_bin, callable_bin;

    if (!enif_inspect_binary(env, argv[0], &runner_bin)) {
        return make_error(env, "invalid_runner");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &callable_bin)) {
        return make_error(env, "invalid_callable");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    ERL_NIF_TERM result;

    /* Convert names */
    char *runner_name = binary_to_string(&runner_bin);
    char *module_name = binary_to_string(&module_bin);
    char *callable_name = binary_to_string(&callable_bin);
    if (runner_name == NULL || module_name == NULL || callable_name == NULL) {
        enif_free(runner_name);
        enif_free(module_name);
        enif_free(callable_name);
        PyGILState_Release(gstate);
        return make_error(env, "alloc_failed");
    }

    /* Build optimized environ dict from Erlang map */
    PyObject *environ = wsgi_environ_from_map(env, argv[3]);
    if (environ == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Import the WSGI runner module */
    PyObject *runner_module = PyImport_ImportModule(runner_name);
    if (runner_module == NULL) {
        Py_DECREF(environ);
        result = make_py_error(env);
        goto cleanup;
    }

    /* Call _run_wsgi_sync(module_name, callable_name, environ) */
    PyObject *run_result = PyObject_CallMethod(
        runner_module, "_run_wsgi_sync", "ssO",
        module_name, callable_name, environ);

    Py_DECREF(runner_module);
    Py_DECREF(environ);

    if (run_result == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert result to Erlang term */
    ERL_NIF_TERM term_result = py_to_term(env, run_result);
    Py_DECREF(run_result);

    result = enif_make_tuple2(env, ATOM_OK, term_result);

cleanup:
    enif_free(runner_name);
    enif_free(module_name);
    enif_free(callable_name);
    PyGILState_Release(gstate);

    return result;
}
