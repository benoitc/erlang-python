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

/* ============================================================================
 * Internal Profiling Support
 * ============================================================================
 * When ASGI_PROFILING is defined, detailed timing of each phase is collected.
 * Enable with: -DASGI_PROFILING during compilation
 */
#ifdef ASGI_PROFILING
#include <sys/time.h>

typedef struct {
    uint64_t count;
    uint64_t gil_acquire_us;
    uint64_t string_conv_us;
    uint64_t module_import_us;
    uint64_t get_callable_us;
    uint64_t scope_build_us;
    uint64_t body_conv_us;
    uint64_t runner_import_us;
    uint64_t runner_call_us;
    uint64_t response_extract_us;
    uint64_t gil_release_us;
    uint64_t total_us;
} asgi_profile_stats_t;

static asgi_profile_stats_t g_asgi_profile = {0};
static pthread_mutex_t g_asgi_profile_mutex = PTHREAD_MUTEX_INITIALIZER;

static inline uint64_t get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

#define PROF_START() uint64_t _prof_start = get_time_us(), _prof_prev = _prof_start, _prof_now
#define PROF_MARK(field) do { \
    _prof_now = get_time_us(); \
    pthread_mutex_lock(&g_asgi_profile_mutex); \
    g_asgi_profile.field += (_prof_now - _prof_prev); \
    pthread_mutex_unlock(&g_asgi_profile_mutex); \
    _prof_prev = _prof_now; \
} while(0)
#define PROF_END() do { \
    _prof_now = get_time_us(); \
    pthread_mutex_lock(&g_asgi_profile_mutex); \
    g_asgi_profile.count++; \
    g_asgi_profile.total_us += (_prof_now - _prof_start); \
    pthread_mutex_unlock(&g_asgi_profile_mutex); \
} while(0)

#else
#define PROF_START()
#define PROF_MARK(field)
#define PROF_END()
#endif /* ASGI_PROFILING */

/* ASGI-specific Erlang atoms for scope map keys */
ERL_NIF_TERM ATOM_ASGI_PATH;
ERL_NIF_TERM ATOM_ASGI_HEADERS;
ERL_NIF_TERM ATOM_ASGI_CLIENT;
ERL_NIF_TERM ATOM_ASGI_QUERY_STRING;
ERL_NIF_TERM ATOM_ASGI_METHOD;

/* Resource type for zero-copy body buffers */
ErlNifResourceType *ASGI_BUFFER_RESOURCE_TYPE = NULL;

/* ============================================================================
 * Zero-Copy Buffer Resource
 * ============================================================================
 * A NIF resource that holds binary data and can be exposed to Python via
 * the buffer protocol. This enables zero-copy access within Python while
 * ensuring the data stays valid as long as Python holds references.
 */

typedef struct {
    unsigned char *data;    /* Binary data */
    size_t size;            /* Data size */
    int ref_count;          /* Python reference count for buffer views */
} asgi_buffer_resource_t;

/**
 * @brief Destructor for buffer resources
 */
static void asgi_buffer_resource_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    asgi_buffer_resource_t *buf = (asgi_buffer_resource_t *)obj;
    if (buf->data != NULL) {
        enif_free(buf->data);
        buf->data = NULL;
    }
}

/* ============================================================================
 * Python Buffer Object
 * ============================================================================
 * A Python object that wraps an ASGI buffer resource and exposes it via
 * the buffer protocol for zero-copy access.
 */

typedef struct {
    PyObject_HEAD
    asgi_buffer_resource_t *resource;  /* NIF resource (we hold a reference) */
    void *resource_ref;                /* For releasing the resource */
} AsgiBufferObject;

static PyTypeObject AsgiBufferType;  /* Forward declaration */

/**
 * @brief Release buffer callback for Python buffer protocol
 */
static void AsgiBuffer_releasebuffer(PyObject *obj, Py_buffer *view) {
    (void)view;
    AsgiBufferObject *self = (AsgiBufferObject *)obj;
    if (self->resource != NULL) {
        self->resource->ref_count--;
    }
}

/**
 * @brief Get buffer callback for Python buffer protocol
 */
static int AsgiBuffer_getbuffer(PyObject *obj, Py_buffer *view, int flags) {
    AsgiBufferObject *self = (AsgiBufferObject *)obj;

    if (self->resource == NULL || self->resource->data == NULL) {
        PyErr_SetString(PyExc_BufferError, "Buffer has been released");
        return -1;
    }

    /* Fill in the buffer structure */
    view->obj = obj;
    view->buf = self->resource->data;
    view->len = self->resource->size;
    view->readonly = 1;
    view->itemsize = 1;
    view->format = (flags & PyBUF_FORMAT) ? "B" : NULL;
    view->ndim = 1;
    view->shape = (flags & PyBUF_ND) ? &view->len : NULL;
    view->strides = (flags & PyBUF_STRIDES) ? &view->itemsize : NULL;
    view->suboffsets = NULL;
    view->internal = NULL;

    self->resource->ref_count++;
    Py_INCREF(obj);

    return 0;
}

static PyBufferProcs AsgiBuffer_as_buffer = {
    .bf_getbuffer = AsgiBuffer_getbuffer,
    .bf_releasebuffer = AsgiBuffer_releasebuffer,
};

/**
 * @brief Deallocate buffer object
 */
static void AsgiBuffer_dealloc(AsgiBufferObject *self) {
    if (self->resource_ref != NULL) {
        enif_release_resource(self->resource_ref);
        self->resource_ref = NULL;
        self->resource = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/**
 * @brief Get length of buffer
 */
static Py_ssize_t AsgiBuffer_length(AsgiBufferObject *self) {
    if (self->resource == NULL) {
        return 0;
    }
    return (Py_ssize_t)self->resource->size;
}

/**
 * @brief Get bytes representation
 */
static PyObject *AsgiBuffer_bytes(AsgiBufferObject *self) {
    if (self->resource == NULL || self->resource->data == NULL) {
        return PyBytes_FromStringAndSize("", 0);
    }
    return PyBytes_FromStringAndSize((char *)self->resource->data,
                                      self->resource->size);
}

static PyMethodDef AsgiBuffer_methods[] = {
    {"__bytes__", (PyCFunction)AsgiBuffer_bytes, METH_NOARGS,
     "Return bytes copy of buffer"},
    {NULL}
};

static PySequenceMethods AsgiBuffer_as_sequence = {
    .sq_length = (lenfunc)AsgiBuffer_length,
};

static PyTypeObject AsgiBufferType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "erlang_python.AsgiBuffer",
    .tp_doc = "Zero-copy ASGI body buffer",
    .tp_basicsize = sizeof(AsgiBufferObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)AsgiBuffer_dealloc,
    .tp_as_buffer = &AsgiBuffer_as_buffer,
    .tp_as_sequence = &AsgiBuffer_as_sequence,
    .tp_methods = AsgiBuffer_methods,
};

/**
 * @brief Create an AsgiBuffer from a NIF resource
 */
static PyObject *AsgiBuffer_from_resource(asgi_buffer_resource_t *resource,
                                           void *resource_ref) {
    AsgiBufferObject *obj = PyObject_New(AsgiBufferObject, &AsgiBufferType);
    if (obj == NULL) {
        return NULL;
    }

    obj->resource = resource;
    obj->resource_ref = resource_ref;
    /* Keep the resource alive */
    enif_keep_resource(resource_ref);

    return (PyObject *)obj;
}

/**
 * @brief Initialize the AsgiBuffer type (call during module init)
 */
static int AsgiBuffer_init_type(void) {
    if (PyType_Ready(&AsgiBufferType) < 0) {
        return -1;
    }
    return 0;
}

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

    /* Clean up callable cache */
    if (state->cached_module_name) {
        enif_free(state->cached_module_name);
        state->cached_module_name = NULL;
    }
    if (state->cached_callable_name) {
        enif_free(state->cached_callable_name);
        state->cached_callable_name = NULL;
    }
    Py_XDECREF(state->cached_callable);
    state->cached_callable = NULL;

    if (state->cached_runner_name) {
        enif_free(state->cached_runner_name);
        state->cached_runner_name = NULL;
    }
    Py_XDECREF(state->cached_runner);
    state->cached_runner = NULL;
    Py_XDECREF(state->cached_run_func);
    state->cached_run_func = NULL;

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
 * Callable Caching
 * ============================================================================
 * Cache module imports and callable lookups to avoid per-request overhead.
 * Most ASGI apps use a single module/callable, so a simple last-used cache
 * provides excellent hit rates.
 */

/**
 * @brief Get cached ASGI callable or import and cache it
 *
 * Returns a borrowed reference to the cached callable.
 */
static PyObject *get_cached_asgi_callable(asgi_interp_state_t *state,
                                          const char *module_name,
                                          const char *callable_name) {
    /* Check if cached */
    if (state->cached_module_name &&
        state->cached_callable_name &&
        strcmp(state->cached_module_name, module_name) == 0 &&
        strcmp(state->cached_callable_name, callable_name) == 0 &&
        state->cached_callable != NULL) {
        return state->cached_callable;  /* Cache hit - borrowed reference */
    }

    /* Cache miss - import and cache */
    PyObject *module = PyImport_ImportModule(module_name);
    if (!module) return NULL;

    PyObject *callable = PyObject_GetAttrString(module, callable_name);
    Py_DECREF(module);
    if (!callable) return NULL;

    /* Update cache */
    if (state->cached_module_name) {
        enif_free(state->cached_module_name);
    }
    if (state->cached_callable_name) {
        enif_free(state->cached_callable_name);
    }
    Py_XDECREF(state->cached_callable);

    state->cached_module_name = enif_alloc(strlen(module_name) + 1);
    state->cached_callable_name = enif_alloc(strlen(callable_name) + 1);
    if (!state->cached_module_name || !state->cached_callable_name) {
        Py_DECREF(callable);
        return NULL;
    }
    strcpy(state->cached_module_name, module_name);
    strcpy(state->cached_callable_name, callable_name);
    state->cached_callable = callable;  /* Takes ownership */

    return callable;  /* Borrowed reference */
}

/**
 * @brief Get cached ASGI runner function or import and cache it
 *
 * Returns a borrowed reference to the cached _run_asgi_sync function.
 */
static PyObject *get_cached_asgi_runner(asgi_interp_state_t *state,
                                        const char *runner_name) {
    /* Check if cached */
    if (state->cached_runner_name &&
        strcmp(state->cached_runner_name, runner_name) == 0 &&
        state->cached_run_func != NULL) {
        return state->cached_run_func;  /* Cache hit - borrowed reference */
    }

    /* Cache miss - import and cache */
    PyObject *runner = PyImport_ImportModule(runner_name);
    if (!runner) return NULL;

    PyObject *run_func = PyObject_GetAttrString(runner, "_run_asgi_sync");
    if (!run_func) {
        Py_DECREF(runner);
        return NULL;
    }

    /* Update cache */
    if (state->cached_runner_name) {
        enif_free(state->cached_runner_name);
    }
    Py_XDECREF(state->cached_runner);
    Py_XDECREF(state->cached_run_func);

    state->cached_runner_name = enif_alloc(strlen(runner_name) + 1);
    if (!state->cached_runner_name) {
        Py_DECREF(run_func);
        Py_DECREF(runner);
        return NULL;
    }
    strcpy(state->cached_runner_name, runner_name);
    state->cached_runner = runner;      /* Takes ownership */
    state->cached_run_func = run_func;  /* Takes ownership */

    return run_func;  /* Borrowed reference */
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

    /* Initialize the AsgiBuffer Python type for zero-copy body handling */
    if (AsgiBuffer_init_type() < 0) {
        return -1;
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
 * Zero-Copy Body Handling
 * ============================================================================ */

static PyObject *asgi_binary_to_buffer(ErlNifEnv *env, ERL_NIF_TERM binary) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, binary, &bin)) {
        PyErr_SetString(PyExc_TypeError, "expected binary");
        return NULL;
    }

    /* For small bodies, copy to bytes - overhead of resource not worth it */
    if (bin.size < ASGI_ZERO_COPY_THRESHOLD) {
        return PyBytes_FromStringAndSize((char *)bin.data, bin.size);
    }

    /* For large bodies, use resource-backed buffer for zero-copy Python access.
     *
     * This approach:
     * 1. Copies data once into a NIF resource
     * 2. Resource stays alive as long as Python holds references
     * 3. Python can slice/view the buffer without additional copies
     * 4. Works safely with async code since resource lifetime is managed
     */
    if (ASGI_BUFFER_RESOURCE_TYPE == NULL) {
        /* Fallback if resource type not initialized */
        return PyBytes_FromStringAndSize((char *)bin.data, bin.size);
    }

    /* Allocate resource */
    asgi_buffer_resource_t *resource = enif_alloc_resource(
        ASGI_BUFFER_RESOURCE_TYPE, sizeof(asgi_buffer_resource_t));
    if (resource == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    /* Allocate and copy data */
    resource->data = enif_alloc(bin.size);
    if (resource->data == NULL) {
        enif_release_resource(resource);
        PyErr_NoMemory();
        return NULL;
    }
    memcpy(resource->data, bin.data, bin.size);
    resource->size = bin.size;
    resource->ref_count = 0;

    /* Create Python buffer object wrapping the resource */
    PyObject *buffer = AsgiBuffer_from_resource(resource, resource);
    /* Release our reference - Python now owns it */
    enif_release_resource(resource);

    if (buffer == NULL) {
        return NULL;
    }

    return buffer;
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
    PyObject *py_status = NULL;
    PyObject *py_headers = NULL;
    PyObject *py_body = NULL;

    /* Handle both dict format (hornbeam) and tuple format */
    if (PyDict_Check(result)) {
        /* Dict format: {'status': int, 'headers': list, 'body': bytes, ...} */
        py_status = PyDict_GetItemString(result, "status");
        py_headers = PyDict_GetItemString(result, "headers");
        py_body = PyDict_GetItemString(result, "body");

        if (py_status == NULL || py_headers == NULL || py_body == NULL) {
            return py_to_term(env, result);
        }
    } else if (PyTuple_Check(result) && PyTuple_Size(result) == 3) {
        /* Tuple format: (status, headers, body) */
        py_status = PyTuple_GET_ITEM(result, 0);
        py_headers = PyTuple_GET_ITEM(result, 1);
        py_body = PyTuple_GET_ITEM(result, 2);
    } else {
        return py_to_term(env, result);
    }

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

    if (!runtime_is_running()) {
        return make_error(env, "python_not_initialized");
    }

    if (!g_asgi_initialized) {
        return make_error(env, "asgi_not_initialized");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Build scope dict from Erlang map */
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
    wrapper->interp_id = 0;  /* Main interpreter */
    atomic_fetch_add(&g_counters.pyobj_created, 1);

    ERL_NIF_TERM result = enif_make_resource(env, wrapper);
    enif_release_resource(wrapper);

    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, result);
}

static ERL_NIF_TERM nif_asgi_run(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    PROF_START();

    if (argc < 5) {
        return make_error(env, "badarg");
    }

    if (!runtime_is_running()) {
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
    PROF_MARK(gil_acquire_us);

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
    PROF_MARK(string_conv_us);

    /* Get per-interpreter state for callable caching */
    asgi_interp_state_t *state = get_asgi_interp_state();
    if (state == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Get cached ASGI callable (avoids per-request import) */
    PyObject *asgi_app = get_cached_asgi_callable(state, module_name, callable_name);
    if (asgi_app == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }
    PROF_MARK(module_import_us);
    PROF_MARK(get_callable_us);

    /* Build optimized scope dict from Erlang map */
    PyObject *scope = asgi_scope_from_map(env, argv[3]);
    if (scope == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }
    PROF_MARK(scope_build_us);

    /* Convert body binary */
    PyObject *body = asgi_binary_to_buffer(env, argv[4]);
    if (body == NULL) {
        Py_DECREF(scope);
        result = make_py_error(env);
        goto cleanup;
    }
    PROF_MARK(body_conv_us);

    /* Get cached runner function (avoids per-request import) */
    PyObject *run_func = get_cached_asgi_runner(state, runner_name);
    if (run_func == NULL) {
        Py_DECREF(body);
        Py_DECREF(scope);
        result = make_py_error(env);
        goto cleanup;
    }
    PROF_MARK(runner_import_us);

    /* Create Python strings for module/callable names (for runner call) */
    PyObject *py_module_name = PyUnicode_FromString(module_name);
    PyObject *py_callable_name = PyUnicode_FromString(callable_name);
    if (py_module_name == NULL || py_callable_name == NULL) {
        Py_XDECREF(py_module_name);
        Py_XDECREF(py_callable_name);
        Py_DECREF(body);
        Py_DECREF(scope);
        result = make_py_error(env);
        goto cleanup;
    }

    /* Call _run_asgi_sync(module_name, callable_name, scope, body) using cached function */
    PyObject *run_result = PyObject_CallFunctionObjArgs(
        run_func, py_module_name, py_callable_name, scope, body, NULL);

    Py_DECREF(py_module_name);
    Py_DECREF(py_callable_name);
    Py_DECREF(body);
    Py_DECREF(scope);
    /* Note: asgi_app and run_func are borrowed references from cache - don't decref */
    PROF_MARK(runner_call_us);

    if (run_result == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert result to Erlang term using optimized extraction */
    ERL_NIF_TERM term_result = extract_asgi_response(env, run_result);
    Py_DECREF(run_result);
    PROF_MARK(response_extract_us);

    result = enif_make_tuple2(env, ATOM_OK, term_result);

cleanup:
    enif_free(runner_name);
    enif_free(module_name);
    enif_free(callable_name);
    PyGILState_Release(gstate);
    PROF_MARK(gil_release_us);
    PROF_END();

    return result;
}

#ifdef ASGI_PROFILING
/**
 * @brief Get ASGI profiling statistics
 * @return Map with timing breakdown
 */
static ERL_NIF_TERM nif_asgi_profile_stats(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    pthread_mutex_lock(&g_asgi_profile_mutex);
    asgi_profile_stats_t stats = g_asgi_profile;
    pthread_mutex_unlock(&g_asgi_profile_mutex);

    if (stats.count == 0) {
        return enif_make_tuple2(env, ATOM_OK,
            enif_make_new_map(env));
    }

    ERL_NIF_TERM keys[12], values[12];
    int i = 0;

    keys[i] = enif_make_atom(env, "count");
    values[i++] = enif_make_uint64(env, stats.count);

    keys[i] = enif_make_atom(env, "gil_acquire_us");
    values[i++] = enif_make_uint64(env, stats.gil_acquire_us);

    keys[i] = enif_make_atom(env, "string_conv_us");
    values[i++] = enif_make_uint64(env, stats.string_conv_us);

    keys[i] = enif_make_atom(env, "module_import_us");
    values[i++] = enif_make_uint64(env, stats.module_import_us);

    keys[i] = enif_make_atom(env, "get_callable_us");
    values[i++] = enif_make_uint64(env, stats.get_callable_us);

    keys[i] = enif_make_atom(env, "scope_build_us");
    values[i++] = enif_make_uint64(env, stats.scope_build_us);

    keys[i] = enif_make_atom(env, "body_conv_us");
    values[i++] = enif_make_uint64(env, stats.body_conv_us);

    keys[i] = enif_make_atom(env, "runner_import_us");
    values[i++] = enif_make_uint64(env, stats.runner_import_us);

    keys[i] = enif_make_atom(env, "runner_call_us");
    values[i++] = enif_make_uint64(env, stats.runner_call_us);

    keys[i] = enif_make_atom(env, "response_extract_us");
    values[i++] = enif_make_uint64(env, stats.response_extract_us);

    keys[i] = enif_make_atom(env, "gil_release_us");
    values[i++] = enif_make_uint64(env, stats.gil_release_us);

    keys[i] = enif_make_atom(env, "total_us");
    values[i++] = enif_make_uint64(env, stats.total_us);

    ERL_NIF_TERM map;
    enif_make_map_from_arrays(env, keys, values, i, &map);

    return enif_make_tuple2(env, ATOM_OK, map);
}

/**
 * @brief Reset ASGI profiling statistics
 */
static ERL_NIF_TERM nif_asgi_profile_reset(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    pthread_mutex_lock(&g_asgi_profile_mutex);
    memset(&g_asgi_profile, 0, sizeof(g_asgi_profile));
    pthread_mutex_unlock(&g_asgi_profile_mutex);

    return ATOM_OK;
}
#endif /* ASGI_PROFILING */
