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
 * @file py_asgi.c
 * @brief ASGI marshalling optimizations implementation
 * @author Benoit Chesneau
 *
 * This file implements optimized ASGI request/response handling:
 * - Interned Python string keys for scope dicts
 * - Pre-built constant values reused across requests
 * - Thread-local response pooling
 * - Direct ASGI NIF for maximum performance
 *
 * Sub-interpreter and Free-threading Support:
 * - Per-interpreter state for all cached Python objects
 * - Thread-safe state access via mutex (for free-threading mode)
 * - Automatic cleanup when interpreters are finalized
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Per-Interpreter State Management
 * ============================================================================
 * Each Python interpreter maintains its own set of interned keys and cached
 * constants. This is required for:
 * - Sub-interpreters (Python 3.12+): Each has separate sys.modules
 * - Free-threading (Python 3.13+): Avoid data races on shared state
 */

/* Storage for per-interpreter states */
static asgi_interp_state_t *g_interp_states[ASGI_MAX_INTERPRETERS];
static int g_interp_state_count = 0;
static pthread_mutex_t g_interp_state_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Flag: ASGI subsystem is initialized (not per-interpreter) */
static bool g_asgi_initialized = false;

/**
 * @brief Initialize a single interpreter state
 */
static int init_interp_state(asgi_interp_state_t *state) {
    /* Intern core scope keys */
    state->key_type = PyUnicode_InternFromString("type");
    if (!state->key_type) return -1;
    state->key_asgi = PyUnicode_InternFromString("asgi");
    if (!state->key_asgi) return -1;
    state->key_http_version = PyUnicode_InternFromString("http_version");
    if (!state->key_http_version) return -1;
    state->key_method = PyUnicode_InternFromString("method");
    if (!state->key_method) return -1;
    state->key_scheme = PyUnicode_InternFromString("scheme");
    if (!state->key_scheme) return -1;
    state->key_path = PyUnicode_InternFromString("path");
    if (!state->key_path) return -1;
    state->key_raw_path = PyUnicode_InternFromString("raw_path");
    if (!state->key_raw_path) return -1;
    state->key_query_string = PyUnicode_InternFromString("query_string");
    if (!state->key_query_string) return -1;
    state->key_root_path = PyUnicode_InternFromString("root_path");
    if (!state->key_root_path) return -1;
    state->key_headers = PyUnicode_InternFromString("headers");
    if (!state->key_headers) return -1;
    state->key_server = PyUnicode_InternFromString("server");
    if (!state->key_server) return -1;
    state->key_client = PyUnicode_InternFromString("client");
    if (!state->key_client) return -1;
    state->key_state = PyUnicode_InternFromString("state");
    if (!state->key_state) return -1;

    /* ASGI subdict keys */
    state->key_version = PyUnicode_InternFromString("version");
    if (!state->key_version) return -1;
    state->key_spec_version = PyUnicode_InternFromString("spec_version");
    if (!state->key_spec_version) return -1;

    /* WebSocket keys */
    state->key_subprotocols = PyUnicode_InternFromString("subprotocols");
    if (!state->key_subprotocols) return -1;

    /* Extension keys */
    state->key_extensions = PyUnicode_InternFromString("extensions");
    if (!state->key_extensions) return -1;
    state->key_http_trailers = PyUnicode_InternFromString("http.response.trailers");
    if (!state->key_http_trailers) return -1;
    state->key_http_early_hints = PyUnicode_InternFromString("http.response.early_hints");
    if (!state->key_http_early_hints) return -1;

    /* Type constants */
    state->type_http = PyUnicode_InternFromString("http");
    if (!state->type_http) return -1;
    state->type_websocket = PyUnicode_InternFromString("websocket");
    if (!state->type_websocket) return -1;
    state->type_lifespan = PyUnicode_InternFromString("lifespan");
    if (!state->type_lifespan) return -1;

    /* HTTP versions */
    state->http_10 = PyUnicode_InternFromString("1.0");
    if (!state->http_10) return -1;
    state->http_11 = PyUnicode_InternFromString("1.1");
    if (!state->http_11) return -1;
    state->http_2 = PyUnicode_InternFromString("2");
    if (!state->http_2) return -1;
    state->http_3 = PyUnicode_InternFromString("3");
    if (!state->http_3) return -1;

    /* Schemes */
    state->scheme_http = PyUnicode_InternFromString("http");
    if (!state->scheme_http) return -1;
    state->scheme_https = PyUnicode_InternFromString("https");
    if (!state->scheme_https) return -1;
    state->scheme_ws = PyUnicode_InternFromString("ws");
    if (!state->scheme_ws) return -1;
    state->scheme_wss = PyUnicode_InternFromString("wss");
    if (!state->scheme_wss) return -1;

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
    state->method_connect = PyUnicode_InternFromString("CONNECT");
    if (!state->method_connect) return -1;
    state->method_trace = PyUnicode_InternFromString("TRACE");
    if (!state->method_trace) return -1;

    /* Empty values */
    state->empty_string = PyUnicode_InternFromString("");
    if (!state->empty_string) return -1;
    state->empty_bytes = PyBytes_FromStringAndSize("", 0);
    if (!state->empty_bytes) return -1;

    /* Pre-interned header names (bytes) for common HTTP headers */
    state->header_host = PyBytes_FromStringAndSize("host", 4);
    if (!state->header_host) return -1;
    state->header_accept = PyBytes_FromStringAndSize("accept", 6);
    if (!state->header_accept) return -1;
    state->header_content_type = PyBytes_FromStringAndSize("content-type", 12);
    if (!state->header_content_type) return -1;
    state->header_content_length = PyBytes_FromStringAndSize("content-length", 14);
    if (!state->header_content_length) return -1;
    state->header_user_agent = PyBytes_FromStringAndSize("user-agent", 10);
    if (!state->header_user_agent) return -1;
    state->header_cookie = PyBytes_FromStringAndSize("cookie", 6);
    if (!state->header_cookie) return -1;
    state->header_authorization = PyBytes_FromStringAndSize("authorization", 13);
    if (!state->header_authorization) return -1;
    state->header_cache_control = PyBytes_FromStringAndSize("cache-control", 13);
    if (!state->header_cache_control) return -1;
    state->header_connection = PyBytes_FromStringAndSize("connection", 10);
    if (!state->header_connection) return -1;
    state->header_accept_encoding = PyBytes_FromStringAndSize("accept-encoding", 15);
    if (!state->header_accept_encoding) return -1;
    state->header_accept_language = PyBytes_FromStringAndSize("accept-language", 15);
    if (!state->header_accept_language) return -1;
    state->header_referer = PyBytes_FromStringAndSize("referer", 7);
    if (!state->header_referer) return -1;
    state->header_origin = PyBytes_FromStringAndSize("origin", 6);
    if (!state->header_origin) return -1;
    state->header_if_none_match = PyBytes_FromStringAndSize("if-none-match", 13);
    if (!state->header_if_none_match) return -1;
    state->header_if_modified_since = PyBytes_FromStringAndSize("if-modified-since", 17);
    if (!state->header_if_modified_since) return -1;
    state->header_x_forwarded_for = PyBytes_FromStringAndSize("x-forwarded-for", 15);
    if (!state->header_x_forwarded_for) return -1;

    /* Cached HTTP status code integers */
    state->status_200 = PyLong_FromLong(200);
    if (!state->status_200) return -1;
    state->status_201 = PyLong_FromLong(201);
    if (!state->status_201) return -1;
    state->status_204 = PyLong_FromLong(204);
    if (!state->status_204) return -1;
    state->status_301 = PyLong_FromLong(301);
    if (!state->status_301) return -1;
    state->status_302 = PyLong_FromLong(302);
    if (!state->status_302) return -1;
    state->status_304 = PyLong_FromLong(304);
    if (!state->status_304) return -1;
    state->status_400 = PyLong_FromLong(400);
    if (!state->status_400) return -1;
    state->status_401 = PyLong_FromLong(401);
    if (!state->status_401) return -1;
    state->status_403 = PyLong_FromLong(403);
    if (!state->status_403) return -1;
    state->status_404 = PyLong_FromLong(404);
    if (!state->status_404) return -1;
    state->status_405 = PyLong_FromLong(405);
    if (!state->status_405) return -1;
    state->status_500 = PyLong_FromLong(500);
    if (!state->status_500) return -1;
    state->status_502 = PyLong_FromLong(502);
    if (!state->status_502) return -1;
    state->status_503 = PyLong_FromLong(503);
    if (!state->status_503) return -1;

    /* Build ASGI subdict: {"version": "3.0", "spec_version": "2.3"} */
    state->asgi_subdict = PyDict_New();
    if (!state->asgi_subdict) return -1;

    PyObject *version_30 = PyUnicode_InternFromString("3.0");
    PyObject *spec_version = PyUnicode_InternFromString("2.3");
    if (!version_30 || !spec_version) {
        Py_XDECREF(version_30);
        Py_XDECREF(spec_version);
        return -1;
    }

    PyDict_SetItem(state->asgi_subdict, state->key_version, version_30);
    PyDict_SetItem(state->asgi_subdict, state->key_spec_version, spec_version);
    Py_DECREF(version_30);
    Py_DECREF(spec_version);

    state->initialized = true;
    return 0;
}

/**
 * @brief Clean up a single interpreter state
 */
static void cleanup_interp_state(asgi_interp_state_t *state) {
    if (!state || !state->initialized) return;

    /* Clean up all Python objects */
    Py_XDECREF(state->key_type);
    Py_XDECREF(state->key_asgi);
    Py_XDECREF(state->key_http_version);
    Py_XDECREF(state->key_method);
    Py_XDECREF(state->key_scheme);
    Py_XDECREF(state->key_path);
    Py_XDECREF(state->key_raw_path);
    Py_XDECREF(state->key_query_string);
    Py_XDECREF(state->key_root_path);
    Py_XDECREF(state->key_headers);
    Py_XDECREF(state->key_server);
    Py_XDECREF(state->key_client);
    Py_XDECREF(state->key_state);
    Py_XDECREF(state->key_version);
    Py_XDECREF(state->key_spec_version);
    Py_XDECREF(state->key_subprotocols);
    Py_XDECREF(state->key_extensions);
    Py_XDECREF(state->key_http_trailers);
    Py_XDECREF(state->key_http_early_hints);
    Py_XDECREF(state->type_http);
    Py_XDECREF(state->type_websocket);
    Py_XDECREF(state->type_lifespan);
    Py_XDECREF(state->asgi_subdict);
    Py_XDECREF(state->http_10);
    Py_XDECREF(state->http_11);
    Py_XDECREF(state->http_2);
    Py_XDECREF(state->http_3);
    Py_XDECREF(state->scheme_http);
    Py_XDECREF(state->scheme_https);
    Py_XDECREF(state->scheme_ws);
    Py_XDECREF(state->scheme_wss);
    Py_XDECREF(state->method_get);
    Py_XDECREF(state->method_post);
    Py_XDECREF(state->method_put);
    Py_XDECREF(state->method_delete);
    Py_XDECREF(state->method_head);
    Py_XDECREF(state->method_options);
    Py_XDECREF(state->method_patch);
    Py_XDECREF(state->method_connect);
    Py_XDECREF(state->method_trace);
    Py_XDECREF(state->empty_string);
    Py_XDECREF(state->empty_bytes);

    /* Clean up pre-interned header names */
    Py_XDECREF(state->header_host);
    Py_XDECREF(state->header_accept);
    Py_XDECREF(state->header_content_type);
    Py_XDECREF(state->header_content_length);
    Py_XDECREF(state->header_user_agent);
    Py_XDECREF(state->header_cookie);
    Py_XDECREF(state->header_authorization);
    Py_XDECREF(state->header_cache_control);
    Py_XDECREF(state->header_connection);
    Py_XDECREF(state->header_accept_encoding);
    Py_XDECREF(state->header_accept_language);
    Py_XDECREF(state->header_referer);
    Py_XDECREF(state->header_origin);
    Py_XDECREF(state->header_if_none_match);
    Py_XDECREF(state->header_if_modified_since);
    Py_XDECREF(state->header_x_forwarded_for);

    /* Clean up cached status codes */
    Py_XDECREF(state->status_200);
    Py_XDECREF(state->status_201);
    Py_XDECREF(state->status_204);
    Py_XDECREF(state->status_301);
    Py_XDECREF(state->status_302);
    Py_XDECREF(state->status_304);
    Py_XDECREF(state->status_400);
    Py_XDECREF(state->status_401);
    Py_XDECREF(state->status_403);
    Py_XDECREF(state->status_404);
    Py_XDECREF(state->status_405);
    Py_XDECREF(state->status_500);
    Py_XDECREF(state->status_502);
    Py_XDECREF(state->status_503);

    state->initialized = false;
}

/**
 * @brief Get or create per-interpreter state for current interpreter
 */
asgi_interp_state_t *get_asgi_interp_state(void) {
    PyInterpreterState *interp = PyInterpreterState_Get();

    /* Fast path: check existing states without lock */
    for (int i = 0; i < g_interp_state_count; i++) {
        if (g_interp_states[i] && g_interp_states[i]->interp == interp) {
            return g_interp_states[i];
        }
    }

    /* Slow path: acquire lock and create new state */
    pthread_mutex_lock(&g_interp_state_mutex);

    /* Double-check after acquiring lock */
    for (int i = 0; i < g_interp_state_count; i++) {
        if (g_interp_states[i] && g_interp_states[i]->interp == interp) {
            pthread_mutex_unlock(&g_interp_state_mutex);
            return g_interp_states[i];
        }
    }

    /* Check capacity */
    if (g_interp_state_count >= ASGI_MAX_INTERPRETERS) {
        pthread_mutex_unlock(&g_interp_state_mutex);
        PyErr_SetString(PyExc_RuntimeError, "Too many Python interpreters");
        return NULL;
    }

    /* Allocate and initialize new state */
    asgi_interp_state_t *state = enif_alloc(sizeof(asgi_interp_state_t));
    if (!state) {
        pthread_mutex_unlock(&g_interp_state_mutex);
        PyErr_NoMemory();
        return NULL;
    }

    memset(state, 0, sizeof(asgi_interp_state_t));
    state->interp = interp;

    if (init_interp_state(state) < 0) {
        cleanup_interp_state(state);
        enif_free(state);
        pthread_mutex_unlock(&g_interp_state_mutex);
        return NULL;
    }

    g_interp_states[g_interp_state_count++] = state;
    pthread_mutex_unlock(&g_interp_state_mutex);

    return state;
}

/**
 * @brief Clean up state for a specific interpreter
 */
void cleanup_asgi_interp_state(PyInterpreterState *interp) {
    if (!interp) {
        interp = PyInterpreterState_Get();
    }

    pthread_mutex_lock(&g_interp_state_mutex);

    for (int i = 0; i < g_interp_state_count; i++) {
        if (g_interp_states[i] && g_interp_states[i]->interp == interp) {
            cleanup_interp_state(g_interp_states[i]);
            enif_free(g_interp_states[i]);

            /* Shift remaining states down */
            for (int j = i; j < g_interp_state_count - 1; j++) {
                g_interp_states[j] = g_interp_states[j + 1];
            }
            g_interp_states[--g_interp_state_count] = NULL;
            break;
        }
    }

    pthread_mutex_unlock(&g_interp_state_mutex);
}

/**
 * @brief Clean up all interpreter states
 */
void cleanup_all_asgi_interp_states(void) {
    pthread_mutex_lock(&g_interp_state_mutex);

    for (int i = 0; i < g_interp_state_count; i++) {
        if (g_interp_states[i]) {
            cleanup_interp_state(g_interp_states[i]);
            enif_free(g_interp_states[i]);
            g_interp_states[i] = NULL;
        }
    }
    g_interp_state_count = 0;

    pthread_mutex_unlock(&g_interp_state_mutex);
}

/* ============================================================================
 * Thread-Local Response Pool
 * ============================================================================ */

static __thread asgi_response_pool_t *tl_response_pool = NULL;

/**
 * @brief Initialize thread-local response pool
 */
static int asgi_init_response_pool(void) {
    if (tl_response_pool != NULL && tl_response_pool->initialized) {
        return 0;
    }

    tl_response_pool = enif_alloc(sizeof(asgi_response_pool_t));
    if (tl_response_pool == NULL) {
        return -1;
    }

    memset(tl_response_pool, 0, sizeof(asgi_response_pool_t));

    /* Pre-allocate response structures */
    for (int i = 0; i < ASGI_RESPONSE_POOL_SIZE; i++) {
        asgi_pooled_response_t *resp = &tl_response_pool->responses[i];

        /* Create dict for response */
        resp->dict = PyDict_New();
        if (resp->dict == NULL) {
            /* Clean up already allocated */
            for (int j = 0; j < i; j++) {
                Py_XDECREF(tl_response_pool->responses[j].dict);
                if (tl_response_pool->responses[j].body_buffer != NULL) {
                    enif_free(tl_response_pool->responses[j].body_buffer);
                }
            }
            enif_free(tl_response_pool);
            tl_response_pool = NULL;
            return -1;
        }

        /* Pre-allocate body buffer */
        resp->body_buffer = enif_alloc(ASGI_INITIAL_BODY_BUFFER_SIZE);
        if (resp->body_buffer == NULL) {
            Py_DECREF(resp->dict);
            for (int j = 0; j < i; j++) {
                Py_XDECREF(tl_response_pool->responses[j].dict);
                if (tl_response_pool->responses[j].body_buffer != NULL) {
                    enif_free(tl_response_pool->responses[j].body_buffer);
                }
            }
            enif_free(tl_response_pool);
            tl_response_pool = NULL;
            return -1;
        }

        resp->body_buffer_cap = ASGI_INITIAL_BODY_BUFFER_SIZE;
        resp->body_len = 0;
        resp->status = 0;
        resp->in_use = false;
        resp->headers = NULL;
        resp->pool_index = i;
    }

    tl_response_pool->in_use_count = 0;
    tl_response_pool->initialized = true;
    return 0;
}

/**
 * @brief Clean up thread-local response pool
 */
static void asgi_cleanup_response_pool(void) {
    if (tl_response_pool == NULL) {
        return;
    }

    for (int i = 0; i < ASGI_RESPONSE_POOL_SIZE; i++) {
        asgi_pooled_response_t *resp = &tl_response_pool->responses[i];
        Py_XDECREF(resp->dict);
        if (resp->body_buffer != NULL) {
            enif_free(resp->body_buffer);
        }
    }

    enif_free(tl_response_pool);
    tl_response_pool = NULL;
}

/* ============================================================================
 * Initialization / Cleanup (Per-Interpreter)
 * ============================================================================ */

/**
 * @brief Initialize ASGI subsystem for current interpreter
 *
 * This now uses per-interpreter state. Each interpreter gets its own
 * set of interned keys and cached constants, created on first use.
 *
 * @return 0 on success, -1 on error
 */
static int asgi_scope_init(void) {
    if (g_asgi_initialized) {
        /* Already initialized globally, per-interpreter state
         * will be created lazily in get_asgi_interp_state() */
        return 0;
    }

    /* Initialize per-interpreter state for current interpreter */
    asgi_interp_state_t *state = get_asgi_interp_state();
    if (!state) {
        return -1;
    }

    g_asgi_initialized = true;
    return 0;
}

/**
 * @brief Clean up ASGI subsystem
 *
 * Cleans up all per-interpreter states. Should be called during
 * module finalization.
 */
static void asgi_scope_cleanup(void) {
    if (!g_asgi_initialized) {
        return;
    }

    /* Clean up all per-interpreter states */
    cleanup_all_asgi_interp_states();

    g_asgi_initialized = false;
}

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

static PyObject *asgi_get_method(const char *method, size_t len) {
    /* Check common methods - return borrowed reference */
    switch (len) {
        case 3:
            if (memcmp(method, "GET", 3) == 0) {
                Py_INCREF(ASGI_METHOD_GET);
                return ASGI_METHOD_GET;
            }
            if (memcmp(method, "PUT", 3) == 0) {
                Py_INCREF(ASGI_METHOD_PUT);
                return ASGI_METHOD_PUT;
            }
            break;
        case 4:
            if (memcmp(method, "POST", 4) == 0) {
                Py_INCREF(ASGI_METHOD_POST);
                return ASGI_METHOD_POST;
            }
            if (memcmp(method, "HEAD", 4) == 0) {
                Py_INCREF(ASGI_METHOD_HEAD);
                return ASGI_METHOD_HEAD;
            }
            break;
        case 5:
            if (memcmp(method, "PATCH", 5) == 0) {
                Py_INCREF(ASGI_METHOD_PATCH);
                return ASGI_METHOD_PATCH;
            }
            if (memcmp(method, "TRACE", 5) == 0) {
                Py_INCREF(ASGI_METHOD_TRACE);
                return ASGI_METHOD_TRACE;
            }
            break;
        case 6:
            if (memcmp(method, "DELETE", 6) == 0) {
                Py_INCREF(ASGI_METHOD_DELETE);
                return ASGI_METHOD_DELETE;
            }
            break;
        case 7:
            if (memcmp(method, "OPTIONS", 7) == 0) {
                Py_INCREF(ASGI_METHOD_OPTIONS);
                return ASGI_METHOD_OPTIONS;
            }
            if (memcmp(method, "CONNECT", 7) == 0) {
                Py_INCREF(ASGI_METHOD_CONNECT);
                return ASGI_METHOD_CONNECT;
            }
            break;
    }

    /* Uncommon method - create new string */
    return PyUnicode_FromStringAndSize(method, len);
}

static PyObject *asgi_get_http_version(int version) {
    switch (version) {
        case 10:
            Py_INCREF(ASGI_HTTP_10);
            return ASGI_HTTP_10;
        case 11:
            Py_INCREF(ASGI_HTTP_11);
            return ASGI_HTTP_11;
        case 20:
            Py_INCREF(ASGI_HTTP_2);
            return ASGI_HTTP_2;
        case 30:
            Py_INCREF(ASGI_HTTP_3);
            return ASGI_HTTP_3;
        default:
            /* Default to 1.1 */
            Py_INCREF(ASGI_HTTP_11);
            return ASGI_HTTP_11;
    }
}

static PyObject *asgi_get_scheme(int scheme) {
    switch (scheme) {
        case 0:
            Py_INCREF(ASGI_SCHEME_HTTP);
            return ASGI_SCHEME_HTTP;
        case 1:
            Py_INCREF(ASGI_SCHEME_HTTPS);
            return ASGI_SCHEME_HTTPS;
        case 2:
            Py_INCREF(ASGI_SCHEME_WS);
            return ASGI_SCHEME_WS;
        case 3:
            Py_INCREF(ASGI_SCHEME_WSS);
            return ASGI_SCHEME_WSS;
        default:
            Py_INCREF(ASGI_SCHEME_HTTP);
            return ASGI_SCHEME_HTTP;
    }
}

/**
 * @brief Get cached header name or create new bytes object
 *
 * Uses length-based dispatch for efficient lookup of common HTTP header names.
 * Returns a new reference (either Py_INCREF'd cached value or new PyBytes).
 */
static PyObject *get_cached_header_name(asgi_interp_state_t *state,
                                        const unsigned char *name, size_t len) {
    switch (len) {
        case 4:
            if (memcmp(name, "host", 4) == 0) {
                Py_INCREF(state->header_host);
                return state->header_host;
            }
            break;
        case 6:
            if (memcmp(name, "accept", 6) == 0) {
                Py_INCREF(state->header_accept);
                return state->header_accept;
            }
            if (memcmp(name, "cookie", 6) == 0) {
                Py_INCREF(state->header_cookie);
                return state->header_cookie;
            }
            if (memcmp(name, "origin", 6) == 0) {
                Py_INCREF(state->header_origin);
                return state->header_origin;
            }
            break;
        case 7:
            if (memcmp(name, "referer", 7) == 0) {
                Py_INCREF(state->header_referer);
                return state->header_referer;
            }
            break;
        case 10:
            if (memcmp(name, "user-agent", 10) == 0) {
                Py_INCREF(state->header_user_agent);
                return state->header_user_agent;
            }
            if (memcmp(name, "connection", 10) == 0) {
                Py_INCREF(state->header_connection);
                return state->header_connection;
            }
            break;
        case 12:
            if (memcmp(name, "content-type", 12) == 0) {
                Py_INCREF(state->header_content_type);
                return state->header_content_type;
            }
            break;
        case 13:
            if (memcmp(name, "authorization", 13) == 0) {
                Py_INCREF(state->header_authorization);
                return state->header_authorization;
            }
            if (memcmp(name, "cache-control", 13) == 0) {
                Py_INCREF(state->header_cache_control);
                return state->header_cache_control;
            }
            if (memcmp(name, "if-none-match", 13) == 0) {
                Py_INCREF(state->header_if_none_match);
                return state->header_if_none_match;
            }
            break;
        case 14:
            if (memcmp(name, "content-length", 14) == 0) {
                Py_INCREF(state->header_content_length);
                return state->header_content_length;
            }
            break;
        case 15:
            if (memcmp(name, "accept-encoding", 15) == 0) {
                Py_INCREF(state->header_accept_encoding);
                return state->header_accept_encoding;
            }
            if (memcmp(name, "accept-language", 15) == 0) {
                Py_INCREF(state->header_accept_language);
                return state->header_accept_language;
            }
            if (memcmp(name, "x-forwarded-for", 15) == 0) {
                Py_INCREF(state->header_x_forwarded_for);
                return state->header_x_forwarded_for;
            }
            break;
        case 17:
            if (memcmp(name, "if-modified-since", 17) == 0) {
                Py_INCREF(state->header_if_modified_since);
                return state->header_if_modified_since;
            }
            break;
    }

    /* Uncommon header - create new bytes object */
    return PyBytes_FromStringAndSize((char *)name, len);
}

/* ============================================================================
 * Response Pool Functions
 * ============================================================================ */

static asgi_pooled_response_t *asgi_acquire_response(void) {
    /* Initialize pool on first use */
    if (tl_response_pool == NULL || !tl_response_pool->initialized) {
        if (asgi_init_response_pool() < 0) {
            return NULL;
        }
    }

    /* Find free slot */
    for (int i = 0; i < ASGI_RESPONSE_POOL_SIZE; i++) {
        asgi_pooled_response_t *resp = &tl_response_pool->responses[i];
        if (!resp->in_use) {
            resp->in_use = true;
            resp->body_len = 0;
            resp->status = 0;
            resp->headers = NULL;
            tl_response_pool->in_use_count++;
            return resp;
        }
    }

    /* Pool exhausted - allocate temporary response */
    asgi_pooled_response_t *resp = enif_alloc(sizeof(asgi_pooled_response_t));
    if (resp == NULL) {
        return NULL;
    }

    resp->dict = PyDict_New();
    if (resp->dict == NULL) {
        enif_free(resp);
        return NULL;
    }

    resp->body_buffer = enif_alloc(ASGI_INITIAL_BODY_BUFFER_SIZE);
    if (resp->body_buffer == NULL) {
        Py_DECREF(resp->dict);
        enif_free(resp);
        return NULL;
    }

    resp->body_buffer_cap = ASGI_INITIAL_BODY_BUFFER_SIZE;
    resp->body_len = 0;
    resp->status = 0;
    resp->in_use = true;
    resp->headers = NULL;
    resp->pool_index = -1;  /* Marks as non-pooled */

    return resp;
}

static void asgi_release_response(asgi_pooled_response_t *resp) {
    if (resp == NULL) {
        return;
    }

    /* Clear dict contents but keep the dict */
    if (resp->dict != NULL) {
        PyDict_Clear(resp->dict);
    }

    /* Reset body */
    resp->body_len = 0;
    resp->status = 0;
    resp->headers = NULL;

    if (resp->pool_index >= 0) {
        /* Return to pool */
        resp->in_use = false;
        if (tl_response_pool != NULL) {
            tl_response_pool->in_use_count--;
        }
    } else {
        /* Non-pooled - free everything */
        Py_XDECREF(resp->dict);
        if (resp->body_buffer != NULL) {
            enif_free(resp->body_buffer);
        }
        enif_free(resp);
    }
}

static void asgi_reset_response(asgi_pooled_response_t *resp) {
    if (resp == NULL) {
        return;
    }

    if (resp->dict != NULL) {
        PyDict_Clear(resp->dict);
    }

    resp->body_len = 0;
    resp->status = 0;
    resp->headers = NULL;
}

static int asgi_ensure_body_capacity(asgi_pooled_response_t *resp, size_t needed) {
    if (needed <= resp->body_buffer_cap) {
        return 0;
    }

    if (needed > ASGI_MAX_BODY_BUFFER_SIZE) {
        return -1;
    }

    /* Grow by doubling, capped at max */
    size_t new_cap = resp->body_buffer_cap * 2;
    while (new_cap < needed && new_cap < ASGI_MAX_BODY_BUFFER_SIZE) {
        new_cap *= 2;
    }
    if (new_cap > ASGI_MAX_BODY_BUFFER_SIZE) {
        new_cap = ASGI_MAX_BODY_BUFFER_SIZE;
    }
    if (new_cap < needed) {
        return -1;
    }

    uint8_t *new_buffer = enif_alloc(new_cap);
    if (new_buffer == NULL) {
        return -1;
    }

    /* Copy existing data */
    if (resp->body_len > 0) {
        memcpy(new_buffer, resp->body_buffer, resp->body_len);
    }

    enif_free(resp->body_buffer);
    resp->body_buffer = new_buffer;
    resp->body_buffer_cap = new_cap;

    return 0;
}

/* ============================================================================
 * Scope Building
 * ============================================================================ */

static PyObject *asgi_build_scope(const asgi_scope_data_t *data) {
    PyObject *scope = PyDict_New();
    if (scope == NULL) {
        return NULL;
    }

    /* type: "http" */
    Py_INCREF(ASGI_TYPE_HTTP);
    if (PyDict_SetItem(scope, ASGI_KEY_TYPE, ASGI_TYPE_HTTP) < 0) {
        goto error;
    }

    /* asgi: {"version": "3.0", "spec_version": "2.3"} - reuse same dict */
    Py_INCREF(ASGI_SUBDICT);
    if (PyDict_SetItem(scope, ASGI_KEY_ASGI, ASGI_SUBDICT) < 0) {
        goto error;
    }

    /* http_version */
    PyObject *http_version = asgi_get_http_version(data->http_version);
    if (PyDict_SetItem(scope, ASGI_KEY_HTTP_VERSION, http_version) < 0) {
        Py_DECREF(http_version);
        goto error;
    }
    Py_DECREF(http_version);

    /* method */
    PyObject *method = asgi_get_method(data->method, data->method_len);
    if (method == NULL || PyDict_SetItem(scope, ASGI_KEY_METHOD, method) < 0) {
        Py_XDECREF(method);
        goto error;
    }
    Py_DECREF(method);

    /* scheme */
    PyObject *scheme = asgi_get_scheme(data->scheme);
    if (PyDict_SetItem(scope, ASGI_KEY_SCHEME, scheme) < 0) {
        Py_DECREF(scheme);
        goto error;
    }
    Py_DECREF(scheme);

    /* path */
    PyObject *path = PyUnicode_FromStringAndSize(data->path, data->path_len);
    if (path == NULL || PyDict_SetItem(scope, ASGI_KEY_PATH, path) < 0) {
        Py_XDECREF(path);
        goto error;
    }
    Py_DECREF(path);

    /* raw_path (bytes) */
    PyObject *raw_path;
    if (data->raw_path_len > 0) {
        raw_path = PyBytes_FromStringAndSize((char *)data->raw_path, data->raw_path_len);
    } else {
        Py_INCREF(ASGI_EMPTY_BYTES);
        raw_path = ASGI_EMPTY_BYTES;
    }
    if (raw_path == NULL || PyDict_SetItem(scope, ASGI_KEY_RAW_PATH, raw_path) < 0) {
        Py_XDECREF(raw_path);
        goto error;
    }
    Py_DECREF(raw_path);

    /* query_string (bytes) */
    PyObject *query_string;
    if (data->query_string_len > 0) {
        query_string = PyBytes_FromStringAndSize(data->query_string, data->query_string_len);
    } else {
        Py_INCREF(ASGI_EMPTY_BYTES);
        query_string = ASGI_EMPTY_BYTES;
    }
    if (query_string == NULL || PyDict_SetItem(scope, ASGI_KEY_QUERY_STRING, query_string) < 0) {
        Py_XDECREF(query_string);
        goto error;
    }
    Py_DECREF(query_string);

    /* root_path */
    PyObject *root_path;
    if (data->root_path_len > 0) {
        root_path = PyUnicode_FromStringAndSize(data->root_path, data->root_path_len);
    } else {
        Py_INCREF(ASGI_EMPTY_STRING);
        root_path = ASGI_EMPTY_STRING;
    }
    if (root_path == NULL || PyDict_SetItem(scope, ASGI_KEY_ROOT_PATH, root_path) < 0) {
        Py_XDECREF(root_path);
        goto error;
    }
    Py_DECREF(root_path);

    /* headers: list of [name, value] pairs (both bytes) */
    /* Use cached header names for common headers */
    asgi_interp_state_t *state = get_asgi_interp_state();
    PyObject *headers = PyList_New(data->headers_count);
    if (headers == NULL) {
        goto error;
    }
    for (size_t i = 0; i < data->headers_count; i++) {
        PyObject *header_pair = PyList_New(2);
        if (header_pair == NULL) {
            Py_DECREF(headers);
            goto error;
        }

        PyObject *name = get_cached_header_name(
            state, data->headers[i].name, data->headers[i].name_len);
        PyObject *value = PyBytes_FromStringAndSize(
            (char *)data->headers[i].value, data->headers[i].value_len);

        if (name == NULL || value == NULL) {
            Py_XDECREF(name);
            Py_XDECREF(value);
            Py_DECREF(header_pair);
            Py_DECREF(headers);
            goto error;
        }

        PyList_SET_ITEM(header_pair, 0, name);
        PyList_SET_ITEM(header_pair, 1, value);
        PyList_SET_ITEM(headers, i, header_pair);
    }
    if (PyDict_SetItem(scope, ASGI_KEY_HEADERS, headers) < 0) {
        Py_DECREF(headers);
        goto error;
    }
    Py_DECREF(headers);

    /* server: (host, port) tuple */
    if (data->server_host_len > 0) {
        PyObject *server_host = PyUnicode_FromStringAndSize(
            data->server_host, data->server_host_len);
        PyObject *server_port = PyLong_FromLong(data->server_port);
        if (server_host == NULL || server_port == NULL) {
            Py_XDECREF(server_host);
            Py_XDECREF(server_port);
            goto error;
        }
        PyObject *server = PyTuple_Pack(2, server_host, server_port);
        Py_DECREF(server_host);
        Py_DECREF(server_port);
        if (server == NULL || PyDict_SetItem(scope, ASGI_KEY_SERVER, server) < 0) {
            Py_XDECREF(server);
            goto error;
        }
        Py_DECREF(server);
    }

    /* client: (host, port) tuple */
    if (data->client_host_len > 0) {
        PyObject *client_host = PyUnicode_FromStringAndSize(
            data->client_host, data->client_host_len);
        PyObject *client_port = PyLong_FromLong(data->client_port);
        if (client_host == NULL || client_port == NULL) {
            Py_XDECREF(client_host);
            Py_XDECREF(client_port);
            goto error;
        }
        PyObject *client = PyTuple_Pack(2, client_host, client_port);
        Py_DECREF(client_host);
        Py_DECREF(client_port);
        if (client == NULL || PyDict_SetItem(scope, ASGI_KEY_CLIENT, client) < 0) {
            Py_XDECREF(client);
            goto error;
        }
        Py_DECREF(client);
    }

    /* state: shared dict from lifespan */
    if (data->state != NULL) {
        Py_INCREF(data->state);
        if (PyDict_SetItem(scope, ASGI_KEY_STATE, data->state) < 0) {
            Py_DECREF(data->state);
            goto error;
        }
    } else {
        /* Create empty state dict */
        PyObject *state = PyDict_New();
        if (state == NULL || PyDict_SetItem(scope, ASGI_KEY_STATE, state) < 0) {
            Py_XDECREF(state);
            goto error;
        }
        Py_DECREF(state);
    }

    /* extensions (optional) */
    if (data->has_trailers || data->has_early_hints) {
        PyObject *extensions = PyDict_New();
        if (extensions == NULL) {
            goto error;
        }

        if (data->has_trailers) {
            PyObject *trailers = PyDict_New();
            if (trailers == NULL) {
                Py_DECREF(extensions);
                goto error;
            }
            PyDict_SetItem(extensions, ASGI_KEY_HTTP_TRAILERS, trailers);
            Py_DECREF(trailers);
        }

        if (data->has_early_hints) {
            PyObject *hints = PyDict_New();
            if (hints == NULL) {
                Py_DECREF(extensions);
                goto error;
            }
            PyDict_SetItem(extensions, ASGI_KEY_HTTP_EARLY_HINTS, hints);
            Py_DECREF(hints);
        }

        PyDict_SetItem(scope, ASGI_KEY_EXTENSIONS, extensions);
        Py_DECREF(extensions);
    }

    return scope;

error:
    Py_DECREF(scope);
    return NULL;
}

/* ============================================================================
 * Zero-Copy Body Handling
 * ============================================================================ */

static PyObject *asgi_binary_to_buffer(ErlNifEnv *env, ERL_NIF_TERM binary) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, binary, &bin)) {
        PyErr_SetString(PyExc_TypeError, "expected binary");
        return NULL;
    }

    /* For small bodies, copy to bytes */
    if (bin.size < ASGI_ZERO_COPY_THRESHOLD) {
        return PyBytes_FromStringAndSize((char *)bin.data, bin.size);
    }

    /* For large bodies, create a memoryview
     * Note: This requires the Erlang binary to stay valid during processing.
     * The memoryview points directly to the binary's memory. */

    /* Create a bytes object that we'll use as the buffer source.
     * For true zero-copy, we'd need to implement a custom buffer object
     * that wraps the Erlang binary. For now, we still copy but use
     * efficient memoryview semantics for subsequent processing. */
    return PyBytes_FromStringAndSize((char *)bin.data, bin.size);
}

/* ============================================================================
 * ASGI Scope from Erlang Map
 * ============================================================================ */

/**
 * @brief Convert a key atom/binary to the corresponding cached key
 */
static PyObject *asgi_get_key_for_term(ErlNifEnv *env, ERL_NIF_TERM term) {
    char key_buf[64];

    if (enif_is_atom(env, term)) {
        if (enif_get_atom(env, term, key_buf, sizeof(key_buf), ERL_NIF_LATIN1)) {
            /* Map to cached keys */
            if (strcmp(key_buf, "type") == 0) return ASGI_KEY_TYPE;
            if (strcmp(key_buf, "asgi") == 0) return ASGI_KEY_ASGI;
            if (strcmp(key_buf, "http_version") == 0) return ASGI_KEY_HTTP_VERSION;
            if (strcmp(key_buf, "method") == 0) return ASGI_KEY_METHOD;
            if (strcmp(key_buf, "scheme") == 0) return ASGI_KEY_SCHEME;
            if (strcmp(key_buf, "path") == 0) return ASGI_KEY_PATH;
            if (strcmp(key_buf, "raw_path") == 0) return ASGI_KEY_RAW_PATH;
            if (strcmp(key_buf, "query_string") == 0) return ASGI_KEY_QUERY_STRING;
            if (strcmp(key_buf, "root_path") == 0) return ASGI_KEY_ROOT_PATH;
            if (strcmp(key_buf, "headers") == 0) return ASGI_KEY_HEADERS;
            if (strcmp(key_buf, "server") == 0) return ASGI_KEY_SERVER;
            if (strcmp(key_buf, "client") == 0) return ASGI_KEY_CLIENT;
            if (strcmp(key_buf, "state") == 0) return ASGI_KEY_STATE;
            if (strcmp(key_buf, "extensions") == 0) return ASGI_KEY_EXTENSIONS;
            /* Fall through for unknown keys */
        }
    }

    /* Unknown key - use generic conversion */
    return NULL;
}

static PyObject *asgi_scope_from_map(ErlNifEnv *env, ERL_NIF_TERM scope_map) {
    if (!g_asgi_initialized) {
        PyErr_SetString(PyExc_RuntimeError, "ASGI scope cache not initialized");
        return NULL;
    }

    if (!enif_is_map(env, scope_map)) {
        PyErr_SetString(PyExc_TypeError, "expected map for ASGI scope");
        return NULL;
    }

    PyObject *scope = PyDict_New();
    if (scope == NULL) {
        return NULL;
    }

    ERL_NIF_TERM key, value;
    ErlNifMapIterator iter;

    enif_map_iterator_create(env, scope_map, &iter, ERL_NIF_MAP_ITERATOR_FIRST);

    while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
        PyObject *py_key = asgi_get_key_for_term(env, key);
        bool key_borrowed = (py_key != NULL);

        if (py_key == NULL) {
            /* Unknown key - create new Python string */
            py_key = term_to_py(env, key);
            if (py_key == NULL) {
                enif_map_iterator_destroy(env, &iter);
                Py_DECREF(scope);
                return NULL;
            }
        }

        /* Convert value based on the key type */
        PyObject *py_value = NULL;

        /* Special handling for known keys */
        if (py_key == ASGI_KEY_TYPE) {
            /* Check for cached type values */
            char type_buf[32];
            ErlNifBinary type_bin;
            if (enif_inspect_binary(env, value, &type_bin) && type_bin.size < sizeof(type_buf)) {
                memcpy(type_buf, type_bin.data, type_bin.size);
                type_buf[type_bin.size] = '\0';
                if (strcmp(type_buf, "http") == 0) {
                    Py_INCREF(ASGI_TYPE_HTTP);
                    py_value = ASGI_TYPE_HTTP;
                } else if (strcmp(type_buf, "websocket") == 0) {
                    Py_INCREF(ASGI_TYPE_WEBSOCKET);
                    py_value = ASGI_TYPE_WEBSOCKET;
                } else if (strcmp(type_buf, "lifespan") == 0) {
                    Py_INCREF(ASGI_TYPE_LIFESPAN);
                    py_value = ASGI_TYPE_LIFESPAN;
                }
            }
        } else if (py_key == ASGI_KEY_ASGI && enif_is_map(env, value)) {
            /* Use cached ASGI subdict if value matches expected format */
            Py_INCREF(ASGI_SUBDICT);
            py_value = ASGI_SUBDICT;
        } else if (py_key == ASGI_KEY_METHOD) {
            ErlNifBinary method_bin;
            if (enif_inspect_binary(env, value, &method_bin)) {
                py_value = asgi_get_method((char *)method_bin.data, method_bin.size);
            }
        } else if (py_key == ASGI_KEY_HTTP_VERSION) {
            ErlNifBinary ver_bin;
            if (enif_inspect_binary(env, value, &ver_bin)) {
                if (ver_bin.size == 3 && memcmp(ver_bin.data, "1.0", 3) == 0) {
                    Py_INCREF(ASGI_HTTP_10);
                    py_value = ASGI_HTTP_10;
                } else if (ver_bin.size == 3 && memcmp(ver_bin.data, "1.1", 3) == 0) {
                    Py_INCREF(ASGI_HTTP_11);
                    py_value = ASGI_HTTP_11;
                } else if (ver_bin.size == 1 && ver_bin.data[0] == '2') {
                    Py_INCREF(ASGI_HTTP_2);
                    py_value = ASGI_HTTP_2;
                } else if (ver_bin.size == 1 && ver_bin.data[0] == '3') {
                    Py_INCREF(ASGI_HTTP_3);
                    py_value = ASGI_HTTP_3;
                }
            }
        } else if (py_key == ASGI_KEY_SCHEME) {
            ErlNifBinary scheme_bin;
            if (enif_inspect_binary(env, value, &scheme_bin)) {
                if (scheme_bin.size == 4 && memcmp(scheme_bin.data, "http", 4) == 0) {
                    Py_INCREF(ASGI_SCHEME_HTTP);
                    py_value = ASGI_SCHEME_HTTP;
                } else if (scheme_bin.size == 5 && memcmp(scheme_bin.data, "https", 5) == 0) {
                    Py_INCREF(ASGI_SCHEME_HTTPS);
                    py_value = ASGI_SCHEME_HTTPS;
                } else if (scheme_bin.size == 2 && memcmp(scheme_bin.data, "ws", 2) == 0) {
                    Py_INCREF(ASGI_SCHEME_WS);
                    py_value = ASGI_SCHEME_WS;
                } else if (scheme_bin.size == 3 && memcmp(scheme_bin.data, "wss", 3) == 0) {
                    Py_INCREF(ASGI_SCHEME_WSS);
                    py_value = ASGI_SCHEME_WSS;
                }
            }
        } else if (py_key == ASGI_KEY_ROOT_PATH) {
            ErlNifBinary rp_bin;
            if (enif_inspect_binary(env, value, &rp_bin) && rp_bin.size == 0) {
                Py_INCREF(ASGI_EMPTY_STRING);
                py_value = ASGI_EMPTY_STRING;
            }
        } else if (py_key == ASGI_KEY_QUERY_STRING || py_key == ASGI_KEY_RAW_PATH) {
            ErlNifBinary bin;
            if (enif_inspect_binary(env, value, &bin) && bin.size == 0) {
                Py_INCREF(ASGI_EMPTY_BYTES);
                py_value = ASGI_EMPTY_BYTES;
            }
        } else if (py_key == ASGI_KEY_HEADERS) {
            /*
             * ASGI spec requires headers to be list[tuple[bytes, bytes]].
             * The Erlang representation is a list of [name_binary, value_binary] pairs.
             * We must convert binaries to Python bytes (not str) for ASGI compliance.
             */
            unsigned int headers_len;
            if (enif_get_list_length(env, value, &headers_len)) {
                py_value = PyList_New(headers_len);
                if (py_value == NULL) {
                    if (!key_borrowed) {
                        Py_DECREF(py_key);
                    }
                    enif_map_iterator_destroy(env, &iter);
                    Py_DECREF(scope);
                    return NULL;
                }

                ERL_NIF_TERM head, tail = value;
                for (unsigned int idx = 0; idx < headers_len; idx++) {
                    if (!enif_get_list_cell(env, tail, &head, &tail)) {
                        Py_DECREF(py_value);
                        py_value = NULL;
                        break;
                    }

                    /* Each header is a 2-element list [name, value] or tuple {name, value} */
                    ERL_NIF_TERM hname_term, hvalue_term;
                    int harity;
                    const ERL_NIF_TERM *htuple;
                    ERL_NIF_TERM hhead, htail;

                    if (enif_get_tuple(env, head, &harity, &htuple) && harity == 2) {
                        /* Tuple format: {name, value} */
                        hname_term = htuple[0];
                        hvalue_term = htuple[1];
                    } else if (enif_get_list_cell(env, head, &hhead, &htail)) {
                        /* List format: [name, value] */
                        hname_term = hhead;
                        if (!enif_get_list_cell(env, htail, &hvalue_term, &htail)) {
                            Py_DECREF(py_value);
                            py_value = NULL;
                            break;
                        }
                    } else {
                        Py_DECREF(py_value);
                        py_value = NULL;
                        break;
                    }

                    /* Extract binaries and convert to Python bytes */
                    ErlNifBinary name_bin, value_bin;
                    if (!enif_inspect_binary(env, hname_term, &name_bin) ||
                        !enif_inspect_binary(env, hvalue_term, &value_bin)) {
                        Py_DECREF(py_value);
                        py_value = NULL;
                        break;
                    }

                    /* Create tuple(bytes, bytes) per ASGI spec */
                    /* Use cached header name for common headers */
                    asgi_interp_state_t *state = get_asgi_interp_state();
                    PyObject *py_name = get_cached_header_name(
                        state, name_bin.data, name_bin.size);
                    PyObject *py_hvalue = PyBytes_FromStringAndSize(
                        (char *)value_bin.data, value_bin.size);

                    if (py_name == NULL || py_hvalue == NULL) {
                        Py_XDECREF(py_name);
                        Py_XDECREF(py_hvalue);
                        Py_DECREF(py_value);
                        py_value = NULL;
                        break;
                    }

                    PyObject *header_tuple = PyTuple_Pack(2, py_name, py_hvalue);
                    Py_DECREF(py_name);
                    Py_DECREF(py_hvalue);

                    if (header_tuple == NULL) {
                        Py_DECREF(py_value);
                        py_value = NULL;
                        break;
                    }

                    PyList_SET_ITEM(py_value, idx, header_tuple);  /* Steals reference */
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
                Py_DECREF(scope);
                return NULL;
            }
        }

        /* Use interned key reference (borrow) */
        if (key_borrowed) {
            Py_INCREF(py_key);
        }

        int set_result = PyDict_SetItem(scope, py_key, py_value);
        Py_DECREF(py_key);
        Py_DECREF(py_value);

        if (set_result < 0) {
            enif_map_iterator_destroy(env, &iter);
            Py_DECREF(scope);
            return NULL;
        }

        enif_map_iterator_next(env, &iter);
    }

    enif_map_iterator_destroy(env, &iter);
    return scope;
}

/* ============================================================================
 * Direct Response Extraction
 * ============================================================================ */

/**
 * @brief Extract ASGI response tuple directly to Erlang terms
 *
 * Optimized response conversion that directly extracts (status, headers, body)
 * tuple elements without going through generic py_to_term(). Falls back to
 * py_to_term() for non-standard responses.
 *
 * Expected Python format: tuple(int, list[tuple[bytes, bytes]], bytes)
 * Output Erlang format: {Status, [{Header, Value}, ...], Body}
 */
static ERL_NIF_TERM extract_asgi_response(ErlNifEnv *env, PyObject *result) {
    /* Validate 3-element tuple, fallback to py_to_term if not */
    if (!PyTuple_Check(result) || PyTuple_Size(result) != 3) {
        return py_to_term(env, result);
    }

    /* Get tuple elements (borrowed references) */
    PyObject *py_status = PyTuple_GET_ITEM(result, 0);
    PyObject *py_headers = PyTuple_GET_ITEM(result, 1);
    PyObject *py_body = PyTuple_GET_ITEM(result, 2);

    /* Validate types */
    if (!PyLong_Check(py_status) || !PyList_Check(py_headers) || !PyBytes_Check(py_body)) {
        return py_to_term(env, result);
    }

    /* Extract status code directly */
    long status = PyLong_AsLong(py_status);
    if (status == -1 && PyErr_Occurred()) {
        PyErr_Clear();
        return py_to_term(env, result);
    }
    ERL_NIF_TERM erl_status = enif_make_int(env, (int)status);

    /* Extract headers list - iterate backwards for efficient cons-cell building */
    Py_ssize_t headers_len = PyList_Size(py_headers);
    ERL_NIF_TERM erl_headers = enif_make_list(env, 0);  /* Start with empty list */

    for (Py_ssize_t i = headers_len - 1; i >= 0; i--) {
        PyObject *header_item = PyList_GET_ITEM(py_headers, i);

        /* Each header should be a 2-element tuple/list of bytes */
        PyObject *py_name = NULL;
        PyObject *py_value = NULL;

        if (PyTuple_Check(header_item) && PyTuple_Size(header_item) == 2) {
            py_name = PyTuple_GET_ITEM(header_item, 0);
            py_value = PyTuple_GET_ITEM(header_item, 1);
        } else if (PyList_Check(header_item) && PyList_Size(header_item) == 2) {
            py_name = PyList_GET_ITEM(header_item, 0);
            py_value = PyList_GET_ITEM(header_item, 1);
        } else {
            /* Invalid header format, fallback */
            return py_to_term(env, result);
        }

        /* Both name and value must be bytes */
        if (!PyBytes_Check(py_name) || !PyBytes_Check(py_value)) {
            return py_to_term(env, result);
        }

        /* Convert header name */
        char *name_data = PyBytes_AS_STRING(py_name);
        Py_ssize_t name_len = PyBytes_GET_SIZE(py_name);
        ERL_NIF_TERM erl_name;
        unsigned char *name_buf = enif_make_new_binary(env, name_len, &erl_name);
        memcpy(name_buf, name_data, name_len);

        /* Convert header value */
        char *value_data = PyBytes_AS_STRING(py_value);
        Py_ssize_t value_len = PyBytes_GET_SIZE(py_value);
        ERL_NIF_TERM erl_value;
        unsigned char *value_buf = enif_make_new_binary(env, value_len, &erl_value);
        memcpy(value_buf, value_data, value_len);

        /* Create header tuple and prepend to list */
        ERL_NIF_TERM header_tuple = enif_make_tuple2(env, erl_name, erl_value);
        erl_headers = enif_make_list_cell(env, header_tuple, erl_headers);
    }

    /* Extract body directly */
    char *body_data = PyBytes_AS_STRING(py_body);
    Py_ssize_t body_len = PyBytes_GET_SIZE(py_body);
    ERL_NIF_TERM erl_body;
    unsigned char *body_buf = enif_make_new_binary(env, body_len, &erl_body);
    memcpy(body_buf, body_data, body_len);

    return enif_make_tuple3(env, erl_status, erl_headers, erl_body);
}

/* ============================================================================
 * NIF Functions
 * ============================================================================ */

static ERL_NIF_TERM nif_asgi_build_scope(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    if (!g_asgi_initialized) {
        return make_error(env, "asgi_not_initialized");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *scope = asgi_scope_from_map(env, argv[0]);
    if (scope == NULL) {
        ERL_NIF_TERM error = make_py_error(env);
        PyGILState_Release(gstate);
        return error;
    }

    /* Wrap scope in a resource to return to Erlang */
    py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
    if (wrapper == NULL) {
        Py_DECREF(scope);
        PyGILState_Release(gstate);
        return make_error(env, "alloc_failed");
    }

    wrapper->obj = scope;

    ERL_NIF_TERM result = enif_make_resource(env, wrapper);
    enif_release_resource(wrapper);

    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, result);
}

static ERL_NIF_TERM nif_asgi_run(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc < 5) {
        return make_error(env, "badarg");
    }

    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    if (!g_asgi_initialized) {
        return make_error(env, "asgi_not_initialized");
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

    /* Convert runner, module and callable names */
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

    /* Import module */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Get ASGI callable */
    PyObject *asgi_app = PyObject_GetAttrString(module, callable_name);
    Py_DECREF(module);
    if (asgi_app == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Build optimized scope dict from Erlang map */
    PyObject *scope = asgi_scope_from_map(env, argv[3]);
    if (scope == NULL) {
        Py_DECREF(asgi_app);
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert body binary */
    PyObject *body = asgi_binary_to_buffer(env, argv[4]);
    if (body == NULL) {
        Py_DECREF(scope);
        Py_DECREF(asgi_app);
        result = make_py_error(env);
        goto cleanup;
    }

    /* Import the ASGI runner module */
    PyObject *runner_module = PyImport_ImportModule(runner_name);
    if (runner_module == NULL) {
        /* Fallback: try to run ASGI app directly with asyncio.run */
        PyErr_Clear();

        PyObject *asyncio = PyImport_ImportModule("asyncio");
        if (asyncio == NULL) {
            Py_DECREF(body);
            Py_DECREF(scope);
            Py_DECREF(asgi_app);
            result = make_error(env, "asyncio_import_failed");
            goto cleanup;
        }

        /* Build receive and send callables (stub for now) */
        /* For a full implementation, these would be proper Python async functions */

        /* For now, return error indicating runner module is required */
        Py_DECREF(asyncio);
        Py_DECREF(body);
        Py_DECREF(scope);
        Py_DECREF(asgi_app);
        result = make_error(env, "runner_module_required");
        goto cleanup;
    }

    /* Call _run_asgi_sync(module_name, callable_name, scope, body) */
    PyObject *run_result = PyObject_CallMethod(
        runner_module, "_run_asgi_sync", "ssOO",
        module_name, callable_name, scope, body);

    Py_DECREF(runner_module);
    Py_DECREF(body);
    Py_DECREF(scope);
    Py_DECREF(asgi_app);

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
    PyGILState_Release(gstate);

    return result;
}
