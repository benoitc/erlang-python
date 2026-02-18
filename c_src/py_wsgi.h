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
 * @file py_wsgi.h
 * @brief WSGI marshalling optimizations for Erlang-Python bridge
 * @author Benoit Chesneau
 *
 * @section overview Overview
 *
 * This module provides optimized WSGI request/response handling through:
 * - Pre-interned Python string keys (avoid repeated string allocation)
 * - Cached constant values (reuse immutable objects across requests)
 * - Direct WSGI NIF bypassing generic py:call() path
 *
 * @section subinterpreters Sub-interpreter and Free-threading Support
 *
 * This module supports Python 3.12+ sub-interpreters and Python 3.13+
 * free-threading (no-GIL) mode through per-interpreter state management.
 *
 * @section performance Performance Gains
 *
 * Similar to ASGI optimizations, expected ~60-80% throughput improvement
 * compared to generic py:call() path.
 */

#ifndef PY_WSGI_H
#define PY_WSGI_H

#include "py_nif.h"
#include <pthread.h>

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @def WSGI_MAX_INTERPRETERS
 * @brief Maximum number of concurrent sub-interpreters supported
 */
#define WSGI_MAX_INTERPRETERS 64

/* ============================================================================
 * Per-Interpreter State (Sub-interpreter & Free-threading Support)
 * ============================================================================ */

/**
 * @brief Per-interpreter WSGI state
 *
 * Each Python interpreter has its own set of interned keys and cached
 * constants for WSGI environ dicts.
 */
typedef struct wsgi_interp_state {
    /* Interpreter identification */
    PyInterpreterState *interp;     /**< Python interpreter this state belongs to */
    bool initialized;               /**< State has been initialized */

    /* CGI/1.1 environ keys */
    PyObject *key_request_method;   /**< "REQUEST_METHOD" */
    PyObject *key_script_name;      /**< "SCRIPT_NAME" */
    PyObject *key_path_info;        /**< "PATH_INFO" */
    PyObject *key_query_string;     /**< "QUERY_STRING" */
    PyObject *key_content_type;     /**< "CONTENT_TYPE" */
    PyObject *key_content_length;   /**< "CONTENT_LENGTH" */
    PyObject *key_server_name;      /**< "SERVER_NAME" */
    PyObject *key_server_port;      /**< "SERVER_PORT" */
    PyObject *key_server_protocol;  /**< "SERVER_PROTOCOL" */
    PyObject *key_remote_host;      /**< "REMOTE_HOST" */
    PyObject *key_remote_addr;      /**< "REMOTE_ADDR" */

    /* WSGI environ keys (PEP 3333) */
    PyObject *key_wsgi_version;     /**< "wsgi.version" */
    PyObject *key_wsgi_url_scheme;  /**< "wsgi.url_scheme" */
    PyObject *key_wsgi_input;       /**< "wsgi.input" */
    PyObject *key_wsgi_errors;      /**< "wsgi.errors" */
    PyObject *key_wsgi_multithread; /**< "wsgi.multithread" */
    PyObject *key_wsgi_multiprocess;/**< "wsgi.multiprocess" */
    PyObject *key_wsgi_run_once;    /**< "wsgi.run_once" */

    /* WSGI extensions */
    PyObject *key_wsgi_file_wrapper;     /**< "wsgi.file_wrapper" */
    PyObject *key_wsgi_input_terminated; /**< "wsgi.input_terminated" */
    PyObject *key_wsgi_early_hints;      /**< "wsgi.early_hints" */

    /* HTTP protocol constants */
    PyObject *http_10;              /**< "HTTP/1.0" */
    PyObject *http_11;              /**< "HTTP/1.1" */

    /* Scheme constants */
    PyObject *scheme_http;          /**< "http" */
    PyObject *scheme_https;         /**< "https" */

    /* Common HTTP methods */
    PyObject *method_get;           /**< "GET" */
    PyObject *method_post;          /**< "POST" */
    PyObject *method_put;           /**< "PUT" */
    PyObject *method_delete;        /**< "DELETE" */
    PyObject *method_head;          /**< "HEAD" */
    PyObject *method_options;       /**< "OPTIONS" */
    PyObject *method_patch;         /**< "PATCH" */

    /* WSGI version tuple (1, 0) */
    PyObject *wsgi_version_tuple;   /**< (1, 0) */

    /* Boolean constants */
    PyObject *py_true;              /**< Py_True cached */
    PyObject *py_false;             /**< Py_False cached */

    /* Empty values */
    PyObject *empty_string;         /**< "" */
    PyObject *empty_bytes;          /**< b"" */

    /* BytesIO class for wsgi.input */
    PyObject *bytesio_class;        /**< io.BytesIO */

    /* HTTP header prefix */
    PyObject *http_prefix;          /**< "HTTP_" */
} wsgi_interp_state_t;

/**
 * @brief Get per-interpreter WSGI state for current interpreter
 *
 * Returns the WSGI state for the current Python interpreter. Creates and
 * initializes the state if it doesn't exist. Thread-safe in free-threading mode.
 *
 * @return Pointer to interpreter state, or NULL on error
 */
wsgi_interp_state_t *get_wsgi_interp_state(void);

/**
 * @brief Clean up WSGI state for a specific interpreter
 *
 * Should be called when an interpreter is being finalized.
 *
 * @param interp The interpreter being finalized, or NULL for current
 */
void cleanup_wsgi_interp_state(PyInterpreterState *interp);

/**
 * @brief Clean up all WSGI interpreter states
 *
 * Called during module unload to clean up all cached state.
 */
void cleanup_all_wsgi_interp_states(void);

/* ============================================================================
 * Legacy Global Accessors (for backward compatibility)
 * ============================================================================ */

#define WSGI_KEY_REQUEST_METHOD    (get_wsgi_interp_state()->key_request_method)
#define WSGI_KEY_SCRIPT_NAME       (get_wsgi_interp_state()->key_script_name)
#define WSGI_KEY_PATH_INFO         (get_wsgi_interp_state()->key_path_info)
#define WSGI_KEY_QUERY_STRING      (get_wsgi_interp_state()->key_query_string)
#define WSGI_KEY_CONTENT_TYPE      (get_wsgi_interp_state()->key_content_type)
#define WSGI_KEY_CONTENT_LENGTH    (get_wsgi_interp_state()->key_content_length)
#define WSGI_KEY_SERVER_NAME       (get_wsgi_interp_state()->key_server_name)
#define WSGI_KEY_SERVER_PORT       (get_wsgi_interp_state()->key_server_port)
#define WSGI_KEY_SERVER_PROTOCOL   (get_wsgi_interp_state()->key_server_protocol)
#define WSGI_KEY_REMOTE_HOST       (get_wsgi_interp_state()->key_remote_host)
#define WSGI_KEY_REMOTE_ADDR       (get_wsgi_interp_state()->key_remote_addr)

#define WSGI_KEY_WSGI_VERSION      (get_wsgi_interp_state()->key_wsgi_version)
#define WSGI_KEY_WSGI_URL_SCHEME   (get_wsgi_interp_state()->key_wsgi_url_scheme)
#define WSGI_KEY_WSGI_INPUT        (get_wsgi_interp_state()->key_wsgi_input)
#define WSGI_KEY_WSGI_ERRORS       (get_wsgi_interp_state()->key_wsgi_errors)
#define WSGI_KEY_WSGI_MULTITHREAD  (get_wsgi_interp_state()->key_wsgi_multithread)
#define WSGI_KEY_WSGI_MULTIPROCESS (get_wsgi_interp_state()->key_wsgi_multiprocess)
#define WSGI_KEY_WSGI_RUN_ONCE     (get_wsgi_interp_state()->key_wsgi_run_once)
#define WSGI_KEY_WSGI_FILE_WRAPPER (get_wsgi_interp_state()->key_wsgi_file_wrapper)
#define WSGI_KEY_WSGI_INPUT_TERMINATED (get_wsgi_interp_state()->key_wsgi_input_terminated)
#define WSGI_KEY_WSGI_EARLY_HINTS  (get_wsgi_interp_state()->key_wsgi_early_hints)

#define WSGI_HTTP_10               (get_wsgi_interp_state()->http_10)
#define WSGI_HTTP_11               (get_wsgi_interp_state()->http_11)
#define WSGI_SCHEME_HTTP           (get_wsgi_interp_state()->scheme_http)
#define WSGI_SCHEME_HTTPS          (get_wsgi_interp_state()->scheme_https)

#define WSGI_METHOD_GET            (get_wsgi_interp_state()->method_get)
#define WSGI_METHOD_POST           (get_wsgi_interp_state()->method_post)
#define WSGI_METHOD_PUT            (get_wsgi_interp_state()->method_put)
#define WSGI_METHOD_DELETE         (get_wsgi_interp_state()->method_delete)
#define WSGI_METHOD_HEAD           (get_wsgi_interp_state()->method_head)
#define WSGI_METHOD_OPTIONS        (get_wsgi_interp_state()->method_options)
#define WSGI_METHOD_PATCH          (get_wsgi_interp_state()->method_patch)

#define WSGI_VERSION_TUPLE         (get_wsgi_interp_state()->wsgi_version_tuple)
#define WSGI_PY_TRUE               (get_wsgi_interp_state()->py_true)
#define WSGI_PY_FALSE              (get_wsgi_interp_state()->py_false)
#define WSGI_EMPTY_STRING          (get_wsgi_interp_state()->empty_string)
#define WSGI_EMPTY_BYTES           (get_wsgi_interp_state()->empty_bytes)
#define WSGI_BYTESIO_CLASS         (get_wsgi_interp_state()->bytesio_class)
#define WSGI_HTTP_PREFIX           (get_wsgi_interp_state()->http_prefix)

/* ============================================================================
 * Function Declarations
 * ============================================================================ */

/**
 * @brief Initialize WSGI subsystem for current interpreter
 *
 * @return 0 on success, -1 on error
 */
static int wsgi_scope_init(void);

/**
 * @brief Clean up WSGI subsystem
 */
static void wsgi_scope_cleanup(void);

/**
 * @brief Get HTTP method as cached Python string
 *
 * @param method Method name
 * @param len Method name length
 * @return Python string (new reference)
 */
static PyObject *wsgi_get_method(const char *method, size_t len);

/**
 * @brief Convert WSGI environ map from Erlang to Python dict
 *
 * @param env NIF environment
 * @param environ_map Erlang map containing environ data
 * @return Python environ dict (new reference), or NULL on error
 */
static PyObject *wsgi_environ_from_map(ErlNifEnv *env, ERL_NIF_TERM environ_map);

/**
 * @brief Direct WSGI run NIF
 *
 * NIF signature: py_wsgi:run(Runner, AppModule, AppCallable, EnvironMap) ->
 *                  {ok, {Status, Headers, Body}} | {error, Reason}
 *
 * @param env NIF environment
 * @param argc Argument count (must be 4)
 * @param argv [Runner, AppModule, AppCallable, EnvironMap]
 * @return Result tuple
 */
static ERL_NIF_TERM nif_wsgi_run(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

#endif /* PY_WSGI_H */
