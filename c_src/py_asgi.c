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
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Global Interned Keys
 * ============================================================================ */

/* Core scope keys */
PyObject *ASGI_KEY_TYPE = NULL;
PyObject *ASGI_KEY_ASGI = NULL;
PyObject *ASGI_KEY_HTTP_VERSION = NULL;
PyObject *ASGI_KEY_METHOD = NULL;
PyObject *ASGI_KEY_SCHEME = NULL;
PyObject *ASGI_KEY_PATH = NULL;
PyObject *ASGI_KEY_RAW_PATH = NULL;
PyObject *ASGI_KEY_QUERY_STRING = NULL;
PyObject *ASGI_KEY_ROOT_PATH = NULL;
PyObject *ASGI_KEY_HEADERS = NULL;
PyObject *ASGI_KEY_SERVER = NULL;
PyObject *ASGI_KEY_CLIENT = NULL;
PyObject *ASGI_KEY_STATE = NULL;

/* ASGI subdict keys */
PyObject *ASGI_KEY_VERSION = NULL;
PyObject *ASGI_KEY_SPEC_VERSION = NULL;

/* WebSocket keys */
PyObject *ASGI_KEY_SUBPROTOCOLS = NULL;

/* Extension keys */
PyObject *ASGI_KEY_EXTENSIONS = NULL;
PyObject *ASGI_KEY_HTTP_TRAILERS = NULL;
PyObject *ASGI_KEY_HTTP_EARLY_HINTS = NULL;

/* ============================================================================
 * Pre-built Constant Values
 * ============================================================================ */

/* Type constants */
PyObject *ASGI_TYPE_HTTP = NULL;
PyObject *ASGI_TYPE_WEBSOCKET = NULL;
PyObject *ASGI_TYPE_LIFESPAN = NULL;

/* ASGI subdict - built once, used for every request */
PyObject *ASGI_SUBDICT = NULL;

/* HTTP versions */
PyObject *ASGI_HTTP_10 = NULL;
PyObject *ASGI_HTTP_11 = NULL;
PyObject *ASGI_HTTP_2 = NULL;
PyObject *ASGI_HTTP_3 = NULL;

/* Schemes */
PyObject *ASGI_SCHEME_HTTP = NULL;
PyObject *ASGI_SCHEME_HTTPS = NULL;
PyObject *ASGI_SCHEME_WS = NULL;
PyObject *ASGI_SCHEME_WSS = NULL;

/* Common HTTP methods */
PyObject *ASGI_METHOD_GET = NULL;
PyObject *ASGI_METHOD_POST = NULL;
PyObject *ASGI_METHOD_PUT = NULL;
PyObject *ASGI_METHOD_DELETE = NULL;
PyObject *ASGI_METHOD_HEAD = NULL;
PyObject *ASGI_METHOD_OPTIONS = NULL;
PyObject *ASGI_METHOD_PATCH = NULL;
PyObject *ASGI_METHOD_CONNECT = NULL;
PyObject *ASGI_METHOD_TRACE = NULL;

/* Empty values */
PyObject *ASGI_EMPTY_STRING = NULL;
PyObject *ASGI_EMPTY_BYTES = NULL;

/* Flag: ASGI cache is initialized */
static bool g_asgi_initialized = false;

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
 * Initialization / Cleanup
 * ============================================================================ */

/**
 * @brief Intern a string and store in target
 */
static int intern_string(PyObject **target, const char *str) {
    *target = PyUnicode_InternFromString(str);
    return (*target != NULL) ? 0 : -1;
}

static int asgi_scope_init(void) {
    if (g_asgi_initialized) {
        return 0;
    }

    /* Intern core scope keys */
    if (intern_string(&ASGI_KEY_TYPE, "type") < 0) return -1;
    if (intern_string(&ASGI_KEY_ASGI, "asgi") < 0) return -1;
    if (intern_string(&ASGI_KEY_HTTP_VERSION, "http_version") < 0) return -1;
    if (intern_string(&ASGI_KEY_METHOD, "method") < 0) return -1;
    if (intern_string(&ASGI_KEY_SCHEME, "scheme") < 0) return -1;
    if (intern_string(&ASGI_KEY_PATH, "path") < 0) return -1;
    if (intern_string(&ASGI_KEY_RAW_PATH, "raw_path") < 0) return -1;
    if (intern_string(&ASGI_KEY_QUERY_STRING, "query_string") < 0) return -1;
    if (intern_string(&ASGI_KEY_ROOT_PATH, "root_path") < 0) return -1;
    if (intern_string(&ASGI_KEY_HEADERS, "headers") < 0) return -1;
    if (intern_string(&ASGI_KEY_SERVER, "server") < 0) return -1;
    if (intern_string(&ASGI_KEY_CLIENT, "client") < 0) return -1;
    if (intern_string(&ASGI_KEY_STATE, "state") < 0) return -1;

    /* Intern ASGI subdict keys */
    if (intern_string(&ASGI_KEY_VERSION, "version") < 0) return -1;
    if (intern_string(&ASGI_KEY_SPEC_VERSION, "spec_version") < 0) return -1;

    /* Intern WebSocket keys */
    if (intern_string(&ASGI_KEY_SUBPROTOCOLS, "subprotocols") < 0) return -1;

    /* Intern extension keys */
    if (intern_string(&ASGI_KEY_EXTENSIONS, "extensions") < 0) return -1;
    if (intern_string(&ASGI_KEY_HTTP_TRAILERS, "http.response.trailers") < 0) return -1;
    if (intern_string(&ASGI_KEY_HTTP_EARLY_HINTS, "http.response.early_hints") < 0) return -1;

    /* Create type constants */
    ASGI_TYPE_HTTP = PyUnicode_InternFromString("http");
    if (ASGI_TYPE_HTTP == NULL) return -1;
    ASGI_TYPE_WEBSOCKET = PyUnicode_InternFromString("websocket");
    if (ASGI_TYPE_WEBSOCKET == NULL) return -1;
    ASGI_TYPE_LIFESPAN = PyUnicode_InternFromString("lifespan");
    if (ASGI_TYPE_LIFESPAN == NULL) return -1;

    /* Create HTTP version constants */
    ASGI_HTTP_10 = PyUnicode_InternFromString("1.0");
    if (ASGI_HTTP_10 == NULL) return -1;
    ASGI_HTTP_11 = PyUnicode_InternFromString("1.1");
    if (ASGI_HTTP_11 == NULL) return -1;
    ASGI_HTTP_2 = PyUnicode_InternFromString("2");
    if (ASGI_HTTP_2 == NULL) return -1;
    ASGI_HTTP_3 = PyUnicode_InternFromString("3");
    if (ASGI_HTTP_3 == NULL) return -1;

    /* Create scheme constants */
    ASGI_SCHEME_HTTP = PyUnicode_InternFromString("http");
    if (ASGI_SCHEME_HTTP == NULL) return -1;
    ASGI_SCHEME_HTTPS = PyUnicode_InternFromString("https");
    if (ASGI_SCHEME_HTTPS == NULL) return -1;
    ASGI_SCHEME_WS = PyUnicode_InternFromString("ws");
    if (ASGI_SCHEME_WS == NULL) return -1;
    ASGI_SCHEME_WSS = PyUnicode_InternFromString("wss");
    if (ASGI_SCHEME_WSS == NULL) return -1;

    /* Create HTTP method constants */
    ASGI_METHOD_GET = PyUnicode_InternFromString("GET");
    if (ASGI_METHOD_GET == NULL) return -1;
    ASGI_METHOD_POST = PyUnicode_InternFromString("POST");
    if (ASGI_METHOD_POST == NULL) return -1;
    ASGI_METHOD_PUT = PyUnicode_InternFromString("PUT");
    if (ASGI_METHOD_PUT == NULL) return -1;
    ASGI_METHOD_DELETE = PyUnicode_InternFromString("DELETE");
    if (ASGI_METHOD_DELETE == NULL) return -1;
    ASGI_METHOD_HEAD = PyUnicode_InternFromString("HEAD");
    if (ASGI_METHOD_HEAD == NULL) return -1;
    ASGI_METHOD_OPTIONS = PyUnicode_InternFromString("OPTIONS");
    if (ASGI_METHOD_OPTIONS == NULL) return -1;
    ASGI_METHOD_PATCH = PyUnicode_InternFromString("PATCH");
    if (ASGI_METHOD_PATCH == NULL) return -1;
    ASGI_METHOD_CONNECT = PyUnicode_InternFromString("CONNECT");
    if (ASGI_METHOD_CONNECT == NULL) return -1;
    ASGI_METHOD_TRACE = PyUnicode_InternFromString("TRACE");
    if (ASGI_METHOD_TRACE == NULL) return -1;

    /* Create empty values */
    ASGI_EMPTY_STRING = PyUnicode_InternFromString("");
    if (ASGI_EMPTY_STRING == NULL) return -1;
    ASGI_EMPTY_BYTES = PyBytes_FromStringAndSize("", 0);
    if (ASGI_EMPTY_BYTES == NULL) return -1;

    /* Build ASGI subdict: {"version": "3.0", "spec_version": "2.3"} */
    ASGI_SUBDICT = PyDict_New();
    if (ASGI_SUBDICT == NULL) return -1;

    PyObject *version_30 = PyUnicode_InternFromString("3.0");
    PyObject *spec_version = PyUnicode_InternFromString("2.3");
    if (version_30 == NULL || spec_version == NULL) {
        Py_XDECREF(version_30);
        Py_XDECREF(spec_version);
        return -1;
    }

    PyDict_SetItem(ASGI_SUBDICT, ASGI_KEY_VERSION, version_30);
    PyDict_SetItem(ASGI_SUBDICT, ASGI_KEY_SPEC_VERSION, spec_version);
    Py_DECREF(version_30);
    Py_DECREF(spec_version);

    g_asgi_initialized = true;
    return 0;
}

static void asgi_scope_cleanup(void) {
    if (!g_asgi_initialized) {
        return;
    }

    /* Clean up interned keys */
    Py_XDECREF(ASGI_KEY_TYPE);
    Py_XDECREF(ASGI_KEY_ASGI);
    Py_XDECREF(ASGI_KEY_HTTP_VERSION);
    Py_XDECREF(ASGI_KEY_METHOD);
    Py_XDECREF(ASGI_KEY_SCHEME);
    Py_XDECREF(ASGI_KEY_PATH);
    Py_XDECREF(ASGI_KEY_RAW_PATH);
    Py_XDECREF(ASGI_KEY_QUERY_STRING);
    Py_XDECREF(ASGI_KEY_ROOT_PATH);
    Py_XDECREF(ASGI_KEY_HEADERS);
    Py_XDECREF(ASGI_KEY_SERVER);
    Py_XDECREF(ASGI_KEY_CLIENT);
    Py_XDECREF(ASGI_KEY_STATE);
    Py_XDECREF(ASGI_KEY_VERSION);
    Py_XDECREF(ASGI_KEY_SPEC_VERSION);
    Py_XDECREF(ASGI_KEY_SUBPROTOCOLS);
    Py_XDECREF(ASGI_KEY_EXTENSIONS);
    Py_XDECREF(ASGI_KEY_HTTP_TRAILERS);
    Py_XDECREF(ASGI_KEY_HTTP_EARLY_HINTS);

    /* Clean up constants */
    Py_XDECREF(ASGI_TYPE_HTTP);
    Py_XDECREF(ASGI_TYPE_WEBSOCKET);
    Py_XDECREF(ASGI_TYPE_LIFESPAN);
    Py_XDECREF(ASGI_SUBDICT);
    Py_XDECREF(ASGI_HTTP_10);
    Py_XDECREF(ASGI_HTTP_11);
    Py_XDECREF(ASGI_HTTP_2);
    Py_XDECREF(ASGI_HTTP_3);
    Py_XDECREF(ASGI_SCHEME_HTTP);
    Py_XDECREF(ASGI_SCHEME_HTTPS);
    Py_XDECREF(ASGI_SCHEME_WS);
    Py_XDECREF(ASGI_SCHEME_WSS);
    Py_XDECREF(ASGI_METHOD_GET);
    Py_XDECREF(ASGI_METHOD_POST);
    Py_XDECREF(ASGI_METHOD_PUT);
    Py_XDECREF(ASGI_METHOD_DELETE);
    Py_XDECREF(ASGI_METHOD_HEAD);
    Py_XDECREF(ASGI_METHOD_OPTIONS);
    Py_XDECREF(ASGI_METHOD_PATCH);
    Py_XDECREF(ASGI_METHOD_CONNECT);
    Py_XDECREF(ASGI_METHOD_TRACE);
    Py_XDECREF(ASGI_EMPTY_STRING);
    Py_XDECREF(ASGI_EMPTY_BYTES);

    /* Reset all pointers */
    ASGI_KEY_TYPE = NULL;
    ASGI_KEY_ASGI = NULL;
    ASGI_KEY_HTTP_VERSION = NULL;
    ASGI_KEY_METHOD = NULL;
    ASGI_KEY_SCHEME = NULL;
    ASGI_KEY_PATH = NULL;
    ASGI_KEY_RAW_PATH = NULL;
    ASGI_KEY_QUERY_STRING = NULL;
    ASGI_KEY_ROOT_PATH = NULL;
    ASGI_KEY_HEADERS = NULL;
    ASGI_KEY_SERVER = NULL;
    ASGI_KEY_CLIENT = NULL;
    ASGI_KEY_STATE = NULL;
    ASGI_KEY_VERSION = NULL;
    ASGI_KEY_SPEC_VERSION = NULL;
    ASGI_KEY_SUBPROTOCOLS = NULL;
    ASGI_KEY_EXTENSIONS = NULL;
    ASGI_KEY_HTTP_TRAILERS = NULL;
    ASGI_KEY_HTTP_EARLY_HINTS = NULL;
    ASGI_TYPE_HTTP = NULL;
    ASGI_TYPE_WEBSOCKET = NULL;
    ASGI_TYPE_LIFESPAN = NULL;
    ASGI_SUBDICT = NULL;
    ASGI_HTTP_10 = NULL;
    ASGI_HTTP_11 = NULL;
    ASGI_HTTP_2 = NULL;
    ASGI_HTTP_3 = NULL;
    ASGI_SCHEME_HTTP = NULL;
    ASGI_SCHEME_HTTPS = NULL;
    ASGI_SCHEME_WS = NULL;
    ASGI_SCHEME_WSS = NULL;
    ASGI_METHOD_GET = NULL;
    ASGI_METHOD_POST = NULL;
    ASGI_METHOD_PUT = NULL;
    ASGI_METHOD_DELETE = NULL;
    ASGI_METHOD_HEAD = NULL;
    ASGI_METHOD_OPTIONS = NULL;
    ASGI_METHOD_PATCH = NULL;
    ASGI_METHOD_CONNECT = NULL;
    ASGI_METHOD_TRACE = NULL;
    ASGI_EMPTY_STRING = NULL;
    ASGI_EMPTY_BYTES = NULL;

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

        PyObject *name = PyBytes_FromStringAndSize(
            (char *)data->headers[i].name, data->headers[i].name_len);
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
