# ASGI NIF Marshalling Optimizations

## Context

Performance analysis of hornbeam ASGI requests shows:
- Base HTTP overhead: ~1.5ms
- Pure Python runner: ~1.4ms per request (including 1ms sleep)
- NIF path: ~1.29ms per request
- Full HTTP stack with 100 connections: ~16ms latency

The marshalling between Erlang and Python is well-optimized but there are opportunities for further improvement, especially for high-throughput ASGI workloads.

## Current Optimizations (py_asgi.c, py_convert.c)

- Interned Python keys for ASGI scope dict
- Cached HTTP constants (methods, versions, schemes)
- Thread-local response pooling with pre-allocated buffers
- Stack allocation for small containers (<16 items)
- Type-check ordering optimized for web workloads
- Direct atom comparison with `enif_is_identical`

## Proposed Optimizations

### Priority 1: Zero-Copy Request Body

**File:** `c_src/py_asgi.c`

**Current Implementation:**
```c
// Line ~932 in asgi_binary_to_buffer()
return PyBytes_FromStringAndSize((char *)bin.data, bin.size);
```

**Proposed:**
```c
// For bodies larger than threshold, use memoryview
#define ZERO_COPY_THRESHOLD 4096

static PyObject *asgi_binary_to_buffer(ErlNifEnv *env, ERL_NIF_TERM binary) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, binary, &bin)) {
        PyErr_SetString(PyExc_TypeError, "expected binary");
        return NULL;
    }

    if (bin.size < ZERO_COPY_THRESHOLD) {
        return PyBytes_FromStringAndSize((char *)bin.data, bin.size);
    }

    // Create a memoryview that references the Erlang binary directly
    // Note: Requires ensuring binary lifetime via enif_keep_resource
    Py_buffer pybuf = {
        .buf = bin.data,
        .len = bin.size,
        .readonly = 1,
        .itemsize = 1,
        .format = "B",
        .ndim = 1,
        .shape = NULL,
        .strides = NULL,
        .suboffsets = NULL,
        .obj = NULL
    };
    return PyMemoryView_FromBuffer(&pybuf);
}
```

**Considerations:**
- Must ensure Erlang binary stays alive during Python execution
- Use `enif_make_resource_binary` or ref-counting mechanism
- Fallback to copy if memoryview creation fails

**Expected Impact:** 10-15% improvement for large request bodies (>4KB)

---

### Priority 2: Direct Response Tuple Extraction

**File:** `c_src/py_asgi.c`

**Current Implementation:**
```c
// Line ~1383 in nif_asgi_run()
ERL_NIF_TERM term_result = py_to_term(env, run_result);
```

The Python runner returns a tuple `(status, headers, body)` but we convert the whole thing generically.

**Proposed:**
```c
static ERL_NIF_TERM extract_asgi_response(ErlNifEnv *env, PyObject *result) {
    if (!PyTuple_Check(result) || PyTuple_Size(result) != 3) {
        return py_to_term(env, result);  // Fallback
    }

    // Direct extraction - no dict iteration needed
    PyObject *py_status = PyTuple_GET_ITEM(result, 0);
    PyObject *py_headers = PyTuple_GET_ITEM(result, 1);
    PyObject *py_body = PyTuple_GET_ITEM(result, 2);

    // Convert status directly
    int status = PyLong_AsLong(py_status);
    ERL_NIF_TERM erl_status = enif_make_int(env, status);

    // Convert headers list directly
    Py_ssize_t num_headers = PyList_Size(py_headers);
    ERL_NIF_TERM erl_headers = enif_make_list(env, 0);

    for (Py_ssize_t i = num_headers - 1; i >= 0; i--) {
        PyObject *header = PyList_GET_ITEM(py_headers, i);
        // Extract header tuple [name, value]
        PyObject *name = PyTuple_GET_ITEM(header, 0);
        PyObject *value = PyTuple_GET_ITEM(header, 1);

        ERL_NIF_TERM erl_name = py_bytes_to_binary(env, name);
        ERL_NIF_TERM erl_value = py_bytes_to_binary(env, value);
        ERL_NIF_TERM header_pair = enif_make_list2(env, erl_name, erl_value);
        erl_headers = enif_make_list_cell(env, header_pair, erl_headers);
    }

    // Convert body directly
    ERL_NIF_TERM erl_body = py_bytes_to_binary(env, py_body);

    return enif_make_tuple3(env, erl_status, erl_headers, erl_body);
}

// Helper for direct bytes->binary without py_to_term overhead
static inline ERL_NIF_TERM py_bytes_to_binary(ErlNifEnv *env, PyObject *obj) {
    Py_ssize_t size = PyBytes_Size(obj);
    char *data = PyBytes_AsString(obj);
    ERL_NIF_TERM bin;
    unsigned char *buf = enif_make_new_binary(env, size, &bin);
    memcpy(buf, data, size);
    return bin;
}
```

**Expected Impact:** 5-10% improvement

---

### Priority 3: Scope Template Cloning

**File:** `c_src/py_asgi.c`

For repeated requests to the same path, most scope values are identical. Cache a template and clone.

**Proposed:**
```c
#define SCOPE_CACHE_SIZE 64

typedef struct {
    uint64_t path_hash;
    size_t path_len;
    PyObject *scope_template;  // Pre-built scope with static fields
} scope_cache_entry_t;

static __thread scope_cache_entry_t scope_cache[SCOPE_CACHE_SIZE];
static __thread int scope_cache_initialized = 0;

static uint64_t hash_path(const char *path, size_t len) {
    // FNV-1a hash
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)path[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static PyObject *get_or_create_scope(ErlNifEnv *env, ERL_NIF_TERM scope_map) {
    // Extract path for cache lookup
    ERL_NIF_TERM path_term;
    if (!enif_get_map_value(env, scope_map, ATOM_PATH, &path_term)) {
        return asgi_scope_from_map(env, scope_map);  // Fallback
    }

    ErlNifBinary path_bin;
    if (!enif_inspect_binary(env, path_term, &path_bin)) {
        return asgi_scope_from_map(env, scope_map);
    }

    uint64_t path_hash = hash_path((char *)path_bin.data, path_bin.size);
    int idx = path_hash % SCOPE_CACHE_SIZE;

    scope_cache_entry_t *entry = &scope_cache[idx];

    if (entry->path_hash == path_hash && entry->scope_template != NULL) {
        // Cache hit - clone template and update dynamic fields
        PyObject *scope = PyDict_Copy(entry->scope_template);

        // Update only dynamic fields: client, headers, query_string
        update_dynamic_scope_fields(env, scope, scope_map);
        return scope;
    }

    // Cache miss - build full scope and cache template
    PyObject *scope = asgi_scope_from_map(env, scope_map);

    // Create template (without client/headers)
    PyObject *template = PyDict_Copy(scope);
    PyDict_DelItem(template, ASGI_KEY_CLIENT);
    PyDict_DelItem(template, ASGI_KEY_HEADERS);
    PyDict_DelItem(template, ASGI_KEY_QUERY_STRING);

    // Update cache
    Py_XDECREF(entry->scope_template);
    entry->path_hash = path_hash;
    entry->path_len = path_bin.size;
    entry->scope_template = template;

    return scope;
}
```

**Expected Impact:** 15-20% for applications with repeated path patterns

---

### Priority 4: Pre-Interned Header Names

**File:** `c_src/py_asgi.c`

**Add to `asgi_interp_state_t`:**
```c
typedef struct {
    // ... existing fields ...

    // Common header names (as bytes)
    PyObject *header_content_type;
    PyObject *header_content_length;
    PyObject *header_cache_control;
    PyObject *header_accept;
    PyObject *header_accept_encoding;
    PyObject *header_host;
    PyObject *header_user_agent;
    PyObject *header_authorization;
    PyObject *header_cookie;
    PyObject *header_set_cookie;
    PyObject *header_location;
    PyObject *header_etag;
    PyObject *header_last_modified;
    PyObject *header_if_none_match;
    PyObject *header_if_modified_since;
} asgi_interp_state_t;
```

**Initialize:**
```c
static int init_interp_state(asgi_interp_state_t *state) {
    // ... existing code ...

    // Pre-intern common header names as bytes
    state->header_content_type = PyBytes_FromString("content-type");
    state->header_content_length = PyBytes_FromString("content-length");
    state->header_cache_control = PyBytes_FromString("cache-control");
    // ... etc ...
}
```

**Use in header conversion:**
```c
static PyObject *get_header_name(asgi_interp_state_t *state,
                                  const char *name, size_t len) {
    // Fast path for common headers
    switch (len) {
        case 4:
            if (memcmp(name, "host", 4) == 0) {
                Py_INCREF(state->header_host);
                return state->header_host;
            }
            if (memcmp(name, "etag", 4) == 0) {
                Py_INCREF(state->header_etag);
                return state->header_etag;
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
            break;
        case 12:
            if (memcmp(name, "content-type", 12) == 0) {
                Py_INCREF(state->header_content_type);
                return state->header_content_type;
            }
            break;
        case 14:
            if (memcmp(name, "content-length", 14) == 0) {
                Py_INCREF(state->header_content_length);
                return state->header_content_length;
            }
            break;
        // ... more cases ...
    }

    // Fallback: create new bytes object
    return PyBytes_FromStringAndSize(name, len);
}
```

**Expected Impact:** 3-5% improvement

---

### Priority 5: Lazy Header Conversion

**File:** `c_src/py_asgi.c`

Most ASGI apps only access 2-3 headers. Convert on-demand.

**Proposed:**
```c
// Custom Python type that wraps Erlang header list
typedef struct {
    PyObject_HEAD
    ErlNifEnv *env;
    ERL_NIF_TERM headers_term;
    PyObject *converted;  // Cache of converted headers
    int fully_converted;
} LazyHeaderList;

static PyObject *LazyHeaderList_getitem(LazyHeaderList *self, Py_ssize_t idx) {
    // Convert single header on access
    if (self->converted != NULL) {
        PyObject *cached = PyList_GetItem(self->converted, idx);
        if (cached != Py_None) {
            Py_INCREF(cached);
            return cached;
        }
    }

    // Convert this specific header
    ERL_NIF_TERM header = get_header_at_index(self->env, self->headers_term, idx);
    PyObject *result = convert_header(self->env, header);

    // Cache it
    if (self->converted != NULL) {
        PyList_SetItem(self->converted, idx, result);
        Py_INCREF(result);
    }

    return result;
}

// Only convert all when iterated or len() called
static Py_ssize_t LazyHeaderList_length(LazyHeaderList *self) {
    if (!self->fully_converted) {
        convert_all_headers(self);
    }
    return PyList_Size(self->converted);
}
```

**Expected Impact:** 5-10% for apps that check few headers

---

### Priority 6: Cached Status Code Integers

**File:** `c_src/py_asgi.c`

**Add to interp state:**
```c
// Common HTTP status codes
PyObject *status_200;
PyObject *status_201;
PyObject *status_204;
PyObject *status_301;
PyObject *status_302;
PyObject *status_304;
PyObject *status_400;
PyObject *status_401;
PyObject *status_403;
PyObject *status_404;
PyObject *status_500;
PyObject *status_502;
PyObject *status_503;
```

**Helper:**
```c
static PyObject *get_status_int(asgi_interp_state_t *state, int status) {
    switch (status) {
        case 200: Py_INCREF(state->status_200); return state->status_200;
        case 201: Py_INCREF(state->status_201); return state->status_201;
        case 204: Py_INCREF(state->status_204); return state->status_204;
        case 301: Py_INCREF(state->status_301); return state->status_301;
        case 302: Py_INCREF(state->status_302); return state->status_302;
        case 304: Py_INCREF(state->status_304); return state->status_304;
        case 400: Py_INCREF(state->status_400); return state->status_400;
        case 401: Py_INCREF(state->status_401); return state->status_401;
        case 403: Py_INCREF(state->status_403); return state->status_403;
        case 404: Py_INCREF(state->status_404); return state->status_404;
        case 500: Py_INCREF(state->status_500); return state->status_500;
        case 502: Py_INCREF(state->status_502); return state->status_502;
        case 503: Py_INCREF(state->status_503); return state->status_503;
        default: return PyLong_FromLong(status);
    }
}
```

**Expected Impact:** 1-2% improvement

---

## Testing

After implementing, benchmark with:

```bash
# Simple endpoint (no async)
wrk -t4 -c100 -d10s http://127.0.0.1:8765/

# With 1ms sleep
wrk -t4 -c100 -d10s "http://127.0.0.1:8765/sleep?ms=1"

# Large body
wrk -t4 -c100 -d10s -s post_body.lua http://127.0.0.1:8765/upload
```

Compare before/after:
- Requests per second
- Average latency
- P99 latency

## Implementation Order

1. **Direct Response Tuple Extraction** - ✅ DONE (commit 54b063e)
2. **Pre-Interned Header Names** - ✅ DONE (commit 54b063e)
3. **Cached Status Codes** - ✅ DONE (commit 54b063e)
4. **Zero-Copy Request Body** - ✅ DONE (commit 19b28fc)
5. **Scope Template Cloning** - ✅ DONE (commit 2448882)
6. **Lazy Header Conversion** - ✅ DONE (latest)

## Implementation Status

All 6 optimizations have been implemented:

| Optimization | Commit | Expected Improvement |
|--------------|--------|----------------------|
| Direct Response Tuple Extraction | 54b063e | 5-10% |
| Pre-Interned Header Names | 54b063e | 3-5% |
| Cached Status Codes | 54b063e | 1-2% |
| Zero-Copy Request Body (≥1KB) | 19b28fc | 10-15% for large bodies |
| Scope Template Caching | 2448882 | 15-20% for repeated paths |
| Lazy Header Conversion (≥4 headers) | latest | 5-10% for few header accesses |

**Total expected improvement: 40-60%** for typical ASGI workloads.

## Notes

- All optimizations should be backwards compatible
- Add feature flags if needed for gradual rollout
- Profile with `perf` or `dtrace` to validate improvements
- Consider Python 3.13 free-threading implications
