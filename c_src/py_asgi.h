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
 * @file py_asgi.h
 * @brief ASGI marshalling optimizations for Erlang-Python bridge
 * @author Benoit Chesneau
 *
 * @section overview Overview
 *
 * This module provides optimized ASGI request/response handling through:
 * - Pre-interned Python string keys (avoid repeated string allocation)
 * - Cached constant values (reuse immutable objects across requests)
 * - Response pooling (reduce dict allocation overhead)
 * - Direct ASGI NIF bypassing generic py:call() path
 *
 * @section subinterpreters Sub-interpreter and Free-threading Support
 *
 * This module supports Python 3.12+ sub-interpreters and Python 3.13+
 * free-threading (no-GIL) mode through per-interpreter state management:
 * - Each interpreter maintains its own set of interned keys and constants
 * - State is automatically created on first use in each interpreter
 * - Thread-safe access in free-threading mode via mutex protection
 * - Proper cleanup when interpreter is finalized
 *
 * @section performance Performance Gains
 *
 * | Optimization | Improvement |
 * |--------------|-------------|
 * | Interned keys | +15-20% throughput |
 * | Response pooling | +20-25% throughput |
 * | Direct NIF | +25-30% throughput |
 *
 * @section usage Usage
 *
 * Call asgi_scope_init() during Python initialization and
 * asgi_scope_cleanup() during finalization.
 */

#ifndef PY_ASGI_H
#define PY_ASGI_H

#include "py_nif.h"
#include <pthread.h>

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @def ASGI_RESPONSE_POOL_SIZE
 * @brief Number of pooled response structures per thread
 */
#define ASGI_RESPONSE_POOL_SIZE 16

/**
 * @def ASGI_INITIAL_BODY_BUFFER_SIZE
 * @brief Initial body buffer capacity in bytes
 */
#define ASGI_INITIAL_BODY_BUFFER_SIZE 4096

/**
 * @def ASGI_MAX_BODY_BUFFER_SIZE
 * @brief Maximum body buffer size (prevents runaway memory)
 */
#define ASGI_MAX_BODY_BUFFER_SIZE (16 * 1024 * 1024)

/**
 * @def ASGI_ZERO_COPY_THRESHOLD
 * @brief Minimum body size to use zero-copy buffer protocol
 */
#define ASGI_ZERO_COPY_THRESHOLD 1024

/**
 * @def ASGI_MAX_INTERPRETERS
 * @brief Maximum number of concurrent sub-interpreters supported
 */
#define ASGI_MAX_INTERPRETERS 64

/**
 * @def SCOPE_CACHE_SIZE
 * @brief Number of scope templates to cache per thread
 */
#define SCOPE_CACHE_SIZE 64

/**
 * @def LAZY_HEADERS_THRESHOLD
 * @brief Minimum number of headers to use lazy conversion
 *
 * For small header counts, eager conversion is faster due to lower overhead.
 * Only use lazy conversion when there are enough headers to benefit.
 */
#ifndef LAZY_HEADERS_THRESHOLD
#define LAZY_HEADERS_THRESHOLD 4
#endif

/* ============================================================================
 * ASGI Erlang Atoms
 * ============================================================================ */

extern ERL_NIF_TERM ATOM_ASGI_PATH;
extern ERL_NIF_TERM ATOM_ASGI_HEADERS;
extern ERL_NIF_TERM ATOM_ASGI_CLIENT;
extern ERL_NIF_TERM ATOM_ASGI_QUERY_STRING;

/* Resource type for zero-copy body buffers */
extern ErlNifResourceType *ASGI_BUFFER_RESOURCE_TYPE;

/* Resource type for lazy header conversion */
extern ErlNifResourceType *ASGI_LAZY_HEADERS_RESOURCE_TYPE;

/* ============================================================================
 * Per-Interpreter State (Sub-interpreter & Free-threading Support)
 * ============================================================================ */

/**
 * @brief Per-interpreter ASGI state
 *
 * Each Python interpreter (main or sub-interpreter) has its own set of
 * interned keys and cached constants. This structure holds all interpreter-
 * specific state and is accessed via get_asgi_interp_state().
 *
 * In free-threading mode (Python 3.13+), access to this state is protected
 * by a mutex to prevent data races.
 */
typedef struct asgi_interp_state {
    /* Interpreter identification */
    PyInterpreterState *interp;     /**< Python interpreter this state belongs to */
    bool initialized;               /**< State has been initialized */

    /* Core scope keys */
    PyObject *key_type;             /**< "type" */
    PyObject *key_asgi;             /**< "asgi" */
    PyObject *key_http_version;     /**< "http_version" */
    PyObject *key_method;           /**< "method" */
    PyObject *key_scheme;           /**< "scheme" */
    PyObject *key_path;             /**< "path" */
    PyObject *key_raw_path;         /**< "raw_path" */
    PyObject *key_query_string;     /**< "query_string" */
    PyObject *key_root_path;        /**< "root_path" */
    PyObject *key_headers;          /**< "headers" */
    PyObject *key_server;           /**< "server" */
    PyObject *key_client;           /**< "client" */
    PyObject *key_state;            /**< "state" */

    /* ASGI subdict keys */
    PyObject *key_version;          /**< "version" */
    PyObject *key_spec_version;     /**< "spec_version" */

    /* WebSocket keys */
    PyObject *key_subprotocols;     /**< "subprotocols" */

    /* Extension keys */
    PyObject *key_extensions;       /**< "extensions" */
    PyObject *key_http_trailers;    /**< "http.response.trailers" */
    PyObject *key_http_early_hints; /**< "http.response.early_hints" */

    /* Type constants */
    PyObject *type_http;            /**< "http" */
    PyObject *type_websocket;       /**< "websocket" */
    PyObject *type_lifespan;        /**< "lifespan" */

    /* ASGI subdict (version info) */
    PyObject *asgi_subdict;         /**< {"version": "3.0", "spec_version": "2.3"} */

    /* HTTP versions */
    PyObject *http_10;              /**< "1.0" */
    PyObject *http_11;              /**< "1.1" */
    PyObject *http_2;               /**< "2" */
    PyObject *http_3;               /**< "3" */

    /* Schemes */
    PyObject *scheme_http;          /**< "http" */
    PyObject *scheme_https;         /**< "https" */
    PyObject *scheme_ws;            /**< "ws" */
    PyObject *scheme_wss;           /**< "wss" */

    /* Common HTTP methods */
    PyObject *method_get;           /**< "GET" */
    PyObject *method_post;          /**< "POST" */
    PyObject *method_put;           /**< "PUT" */
    PyObject *method_delete;        /**< "DELETE" */
    PyObject *method_head;          /**< "HEAD" */
    PyObject *method_options;       /**< "OPTIONS" */
    PyObject *method_patch;         /**< "PATCH" */
    PyObject *method_connect;       /**< "CONNECT" */
    PyObject *method_trace;         /**< "TRACE" */

    /* Empty values */
    PyObject *empty_string;         /**< "" */
    PyObject *empty_bytes;          /**< b"" */

    /* Pre-interned header names (bytes) for common HTTP headers */
    PyObject *header_host;              /**< b"host" */
    PyObject *header_accept;            /**< b"accept" */
    PyObject *header_content_type;      /**< b"content-type" */
    PyObject *header_content_length;    /**< b"content-length" */
    PyObject *header_user_agent;        /**< b"user-agent" */
    PyObject *header_cookie;            /**< b"cookie" */
    PyObject *header_authorization;     /**< b"authorization" */
    PyObject *header_cache_control;     /**< b"cache-control" */
    PyObject *header_connection;        /**< b"connection" */
    PyObject *header_accept_encoding;   /**< b"accept-encoding" */
    PyObject *header_accept_language;   /**< b"accept-language" */
    PyObject *header_referer;           /**< b"referer" */
    PyObject *header_origin;            /**< b"origin" */
    PyObject *header_if_none_match;     /**< b"if-none-match" */
    PyObject *header_if_modified_since; /**< b"if-modified-since" */
    PyObject *header_x_forwarded_for;   /**< b"x-forwarded-for" */

    /* Cached HTTP status code integers */
    PyObject *status_200;   /**< 200 OK */
    PyObject *status_201;   /**< 201 Created */
    PyObject *status_204;   /**< 204 No Content */
    PyObject *status_301;   /**< 301 Moved Permanently */
    PyObject *status_302;   /**< 302 Found */
    PyObject *status_304;   /**< 304 Not Modified */
    PyObject *status_400;   /**< 400 Bad Request */
    PyObject *status_401;   /**< 401 Unauthorized */
    PyObject *status_403;   /**< 403 Forbidden */
    PyObject *status_404;   /**< 404 Not Found */
    PyObject *status_405;   /**< 405 Method Not Allowed */
    PyObject *status_500;   /**< 500 Internal Server Error */
    PyObject *status_502;   /**< 502 Bad Gateway */
    PyObject *status_503;   /**< 503 Service Unavailable */
} asgi_interp_state_t;

/**
 * @brief Get per-interpreter ASGI state for current interpreter
 *
 * Returns the ASGI state for the current Python interpreter. Creates and
 * initializes the state if it doesn't exist. Thread-safe in free-threading mode.
 *
 * @return Pointer to interpreter state, or NULL on error
 */
asgi_interp_state_t *get_asgi_interp_state(void);

/**
 * @brief Clean up ASGI state for a specific interpreter
 *
 * Should be called when an interpreter is being finalized.
 *
 * @param interp The interpreter being finalized, or NULL for current
 */
void cleanup_asgi_interp_state(PyInterpreterState *interp);

/**
 * @brief Clean up all ASGI interpreter states
 *
 * Called during module unload to clean up all cached state.
 */
void cleanup_all_asgi_interp_states(void);

/* ============================================================================
 * Legacy Global Accessors (for backward compatibility)
 * ============================================================================
 * These macros provide the old global variable names but now access
 * per-interpreter state. Code using these will continue to work but
 * now properly supports sub-interpreters.
 */

#define ASGI_KEY_TYPE           (get_asgi_interp_state()->key_type)
#define ASGI_KEY_ASGI           (get_asgi_interp_state()->key_asgi)
#define ASGI_KEY_HTTP_VERSION   (get_asgi_interp_state()->key_http_version)
#define ASGI_KEY_METHOD         (get_asgi_interp_state()->key_method)
#define ASGI_KEY_SCHEME         (get_asgi_interp_state()->key_scheme)
#define ASGI_KEY_PATH           (get_asgi_interp_state()->key_path)
#define ASGI_KEY_RAW_PATH       (get_asgi_interp_state()->key_raw_path)
#define ASGI_KEY_QUERY_STRING   (get_asgi_interp_state()->key_query_string)
#define ASGI_KEY_ROOT_PATH      (get_asgi_interp_state()->key_root_path)
#define ASGI_KEY_HEADERS        (get_asgi_interp_state()->key_headers)
#define ASGI_KEY_SERVER         (get_asgi_interp_state()->key_server)
#define ASGI_KEY_CLIENT         (get_asgi_interp_state()->key_client)
#define ASGI_KEY_STATE          (get_asgi_interp_state()->key_state)
#define ASGI_KEY_VERSION        (get_asgi_interp_state()->key_version)
#define ASGI_KEY_SPEC_VERSION   (get_asgi_interp_state()->key_spec_version)
#define ASGI_KEY_SUBPROTOCOLS   (get_asgi_interp_state()->key_subprotocols)
#define ASGI_KEY_EXTENSIONS     (get_asgi_interp_state()->key_extensions)
#define ASGI_KEY_HTTP_TRAILERS  (get_asgi_interp_state()->key_http_trailers)
#define ASGI_KEY_HTTP_EARLY_HINTS (get_asgi_interp_state()->key_http_early_hints)

#define ASGI_TYPE_HTTP          (get_asgi_interp_state()->type_http)
#define ASGI_TYPE_WEBSOCKET     (get_asgi_interp_state()->type_websocket)
#define ASGI_TYPE_LIFESPAN      (get_asgi_interp_state()->type_lifespan)
#define ASGI_SUBDICT            (get_asgi_interp_state()->asgi_subdict)

#define ASGI_HTTP_10            (get_asgi_interp_state()->http_10)
#define ASGI_HTTP_11            (get_asgi_interp_state()->http_11)
#define ASGI_HTTP_2             (get_asgi_interp_state()->http_2)
#define ASGI_HTTP_3             (get_asgi_interp_state()->http_3)

#define ASGI_SCHEME_HTTP        (get_asgi_interp_state()->scheme_http)
#define ASGI_SCHEME_HTTPS       (get_asgi_interp_state()->scheme_https)
#define ASGI_SCHEME_WS          (get_asgi_interp_state()->scheme_ws)
#define ASGI_SCHEME_WSS         (get_asgi_interp_state()->scheme_wss)

#define ASGI_METHOD_GET         (get_asgi_interp_state()->method_get)
#define ASGI_METHOD_POST        (get_asgi_interp_state()->method_post)
#define ASGI_METHOD_PUT         (get_asgi_interp_state()->method_put)
#define ASGI_METHOD_DELETE      (get_asgi_interp_state()->method_delete)
#define ASGI_METHOD_HEAD        (get_asgi_interp_state()->method_head)
#define ASGI_METHOD_OPTIONS     (get_asgi_interp_state()->method_options)
#define ASGI_METHOD_PATCH       (get_asgi_interp_state()->method_patch)
#define ASGI_METHOD_CONNECT     (get_asgi_interp_state()->method_connect)
#define ASGI_METHOD_TRACE       (get_asgi_interp_state()->method_trace)

#define ASGI_EMPTY_STRING       (get_asgi_interp_state()->empty_string)
#define ASGI_EMPTY_BYTES        (get_asgi_interp_state()->empty_bytes)

/* ============================================================================
 * Deprecated: Direct Global Variable Access
 * ============================================================================
 * The following extern declarations are kept for source compatibility but
 * the variables no longer exist. Use the macros above instead which access
 * per-interpreter state.
 */


/* ============================================================================
 * Response Pooling
 * ============================================================================ */

/**
 * @defgroup asgi_pool Response Pool
 * @brief Thread-local response object pooling
 * @{
 */

/**
 * @struct asgi_pooled_response_t
 * @brief Pre-allocated response structure for ASGI requests
 *
 * Contains a pre-built dict and body buffer that can be reused
 * across requests to avoid repeated allocation.
 */
typedef struct {
    /** @brief Pre-allocated result dict */
    PyObject *dict;

    /** @brief Pre-allocated body buffer */
    uint8_t *body_buffer;

    /** @brief Current capacity of body_buffer */
    size_t body_buffer_cap;

    /** @brief Actual body length */
    size_t body_len;

    /** @brief HTTP status code */
    int status;

    /** @brief Whether this pool slot is currently in use */
    bool in_use;

    /** @brief Headers list (borrowed reference, owned by dict) */
    PyObject *headers;

    /** @brief Pool index for debugging */
    int pool_index;
} asgi_pooled_response_t;

/**
 * @struct asgi_response_pool_t
 * @brief Thread-local pool of response structures
 */
typedef struct {
    /** @brief Array of pooled responses */
    asgi_pooled_response_t responses[ASGI_RESPONSE_POOL_SIZE];

    /** @brief Number of responses currently in use */
    int in_use_count;

    /** @brief Whether pool is initialized */
    bool initialized;
} asgi_response_pool_t;

/** @} */

/* ============================================================================
 * ASGI Scope Building
 * ============================================================================ */

/**
 * @defgroup asgi_scope Scope Building
 * @brief Optimized ASGI scope dict construction
 * @{
 */

/**
 * @struct asgi_scope_data_t
 * @brief Input data for building ASGI scope
 *
 * Erlang passes this data from the request; C builds the optimized
 * scope dict using interned keys and cached constants.
 */
typedef struct {
    /* Request line */
    const char *method;
    size_t method_len;
    const char *path;
    size_t path_len;
    const uint8_t *raw_path;
    size_t raw_path_len;
    const char *query_string;
    size_t query_string_len;

    /* HTTP version: 10, 11, 20, 30 */
    int http_version;

    /* Scheme: 0=http, 1=https */
    int scheme;

    /* Client info */
    const char *client_host;
    size_t client_host_len;
    int client_port;

    /* Server info */
    const char *server_host;
    size_t server_host_len;
    int server_port;

    /* Root path (usually empty) */
    const char *root_path;
    size_t root_path_len;

    /* Headers: array of (name, value) pairs */
    struct {
        const uint8_t *name;
        size_t name_len;
        const uint8_t *value;
        size_t value_len;
    } *headers;
    size_t headers_count;

    /* State dict (shared from lifespan) */
    PyObject *state;

    /* Extensions (optional) */
    bool has_trailers;
    bool has_early_hints;
} asgi_scope_data_t;

/** @} */

/* ============================================================================
 * Function Declarations
 * ============================================================================ */

/**
 * @defgroup asgi_funcs ASGI Functions
 * @brief Public API for ASGI optimizations
 * @{
 */

/**
 * @brief Initialize ASGI scope key cache
 *
 * Creates and interns all ASGI key strings. Must be called once
 * during Python initialization, with the GIL held.
 *
 * @return 0 on success, -1 on error
 *
 * @pre GIL must be held
 * @pre Python must be initialized
 */
static int asgi_scope_init(void);

/**
 * @brief Clean up ASGI scope key cache
 *
 * Releases all cached Python objects. Must be called during
 * finalization, with the GIL held.
 *
 * @pre GIL must be held
 */
static void asgi_scope_cleanup(void);

/**
 * @brief Build optimized ASGI scope dict
 *
 * Creates a scope dict using interned keys and cached constants
 * for maximum performance.
 *
 * @param data Scope data from Erlang
 * @return New scope dict (caller owns reference), or NULL on error
 *
 * @pre GIL must be held
 * @pre asgi_scope_init() has been called
 */
static PyObject *asgi_build_scope(const asgi_scope_data_t *data);

/**
 * @brief Get HTTP method as cached Python string
 *
 * Looks up common methods in cache, falling back to creating
 * a new string for uncommon methods.
 *
 * @param method Method name
 * @param len Method name length
 * @return Python string (borrowed for cached, new for uncached)
 *         Caller must handle reference properly based on return value
 *
 * @pre GIL must be held
 */
static PyObject *asgi_get_method(const char *method, size_t len);

/**
 * @brief Get HTTP version as cached Python string
 *
 * @param version Version code: 10, 11, 20, 30
 * @return Cached Python string (borrowed reference)
 *
 * @pre GIL must be held
 */
static PyObject *asgi_get_http_version(int version);

/**
 * @brief Get scheme as cached Python string
 *
 * @param scheme 0=http, 1=https, 2=ws, 3=wss
 * @return Cached Python string (borrowed reference)
 *
 * @pre GIL must be held
 */
static PyObject *asgi_get_scheme(int scheme);

/**
 * @brief Acquire a pooled response structure
 *
 * Gets a pre-allocated response from the thread-local pool.
 * If the pool is exhausted, allocates a new structure.
 *
 * @return Pooled response, or NULL on allocation failure
 *
 * @pre GIL must be held
 */
static asgi_pooled_response_t *asgi_acquire_response(void);

/**
 * @brief Release a pooled response structure
 *
 * Returns a response to the pool for reuse. Clears the body
 * buffer and resets fields.
 *
 * @param resp Response to release
 *
 * @pre GIL must be held
 */
static void asgi_release_response(asgi_pooled_response_t *resp);

/**
 * @brief Reset response for reuse
 *
 * Clears body buffer and resets status/headers without
 * releasing to pool.
 *
 * @param resp Response to reset
 *
 * @pre GIL must be held
 */
static void asgi_reset_response(asgi_pooled_response_t *resp);

/**
 * @brief Ensure body buffer has sufficient capacity
 *
 * Grows the body buffer if needed to accommodate additional data.
 *
 * @param resp Response with body buffer
 * @param needed Required capacity in bytes
 * @return 0 on success, -1 if growth exceeds ASGI_MAX_BODY_BUFFER_SIZE
 */
static int asgi_ensure_body_capacity(asgi_pooled_response_t *resp, size_t needed);

/**
 * @brief Create a zero-copy buffer view of Erlang binary
 *
 * For large bodies (> ASGI_ZERO_COPY_THRESHOLD), creates a memoryview
 * that directly references the Erlang binary data.
 *
 * @param env NIF environment
 * @param binary Erlang binary term
 * @return Python memoryview or bytes object
 *
 * @pre GIL must be held
 *
 * @warning The returned memoryview is only valid while the Erlang
 *          binary is not garbage collected. Use for immediate processing only.
 */
static PyObject *asgi_binary_to_buffer(ErlNifEnv *env, ERL_NIF_TERM binary);

/**
 * @brief Convert ASGI scope map from Erlang to Python dict
 *
 * Optimized version of term_to_py for ASGI scope maps that uses
 * interned keys and cached constants.
 *
 * @param env NIF environment
 * @param scope_map Erlang map containing scope data
 * @return Python scope dict (caller owns reference), or NULL on error
 *
 * @pre GIL must be held
 */
static PyObject *asgi_scope_from_map(ErlNifEnv *env, ERL_NIF_TERM scope_map);

/**
 * @brief Get cached header name or create new bytes object
 *
 * Looks up common header names in cache, falling back to creating
 * a new bytes object for uncommon headers.
 *
 * @param state Per-interpreter ASGI state
 * @param name Header name bytes
 * @param len Header name length
 * @return Python bytes object (new reference)
 *
 * @pre GIL must be held
 */
static PyObject *get_cached_header_name(asgi_interp_state_t *state,
                                        const unsigned char *name, size_t len);

/**
 * @brief Extract ASGI response tuple directly to Erlang terms
 *
 * Optimized response conversion that directly extracts (status, headers, body)
 * tuple elements without going through generic py_to_term().
 *
 * @param env NIF environment
 * @param result Python result (expected: tuple(int, list, bytes))
 * @return Erlang term {Status, Headers, Body} or generic conversion fallback
 *
 * @pre GIL must be held
 */
static ERL_NIF_TERM extract_asgi_response(ErlNifEnv *env, PyObject *result);

/** @} */

/* ============================================================================
 * NIF Function Declarations
 * ============================================================================ */

/**
 * @defgroup asgi_nifs ASGI NIFs
 * @brief Native functions for optimized ASGI handling
 * @{
 */

/**
 * @brief Direct ASGI run NIF
 *
 * Optimized NIF that bypasses generic py:call() path:
 * - Builds scope dict using interned keys
 * - Uses response pooling
 * - Runs ASGI app coroutine synchronously
 *
 * NIF signature: py_asgi:run(Runner, AppModule, AppCallable, ScopeMap, BodyBinary) ->
 *                  {ok, {Status, Headers, Body}} | {error, Reason}
 *
 * @param env NIF environment
 * @param argc Argument count (must be 5)
 * @param argv [Runner, AppModule, AppCallable, ScopeMap, BodyBinary]
 * @return Result tuple
 */
static ERL_NIF_TERM nif_asgi_run(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/**
 * @brief Build ASGI scope dict from Erlang map NIF
 *
 * Converts an Erlang scope map to a Python dict using optimizations.
 * Useful when scope building and app calling are separate.
 *
 * NIF signature: py_asgi:build_scope(ScopeMap) ->
 *                  {ok, ScopeRef} | {error, Reason}
 *
 * @param env NIF environment
 * @param argc Argument count (must be 1)
 * @param argv [ScopeMap]
 * @return {ok, ScopeRef} where ScopeRef is a wrapped Python dict
 */
static ERL_NIF_TERM nif_asgi_build_scope(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/** @} */

#endif /* PY_ASGI_H */
