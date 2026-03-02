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
 * @file py_logging.c
 * @brief NIF-based logging and distributed tracing for Python-Erlang integration
 * @author Benoit Chesneau
 *
 * @ingroup logging
 *
 * This module provides fire-and-forget logging and tracing from Python to Erlang:
 *
 * @par Logging
 * - Python's `logging` module integrated with Erlang's `logger`
 * - Fire-and-forget: `erlang._log()` never blocks Python execution
 * - Level filtering at NIF level to skip unnecessary message creation
 *
 * @par Tracing
 * - Span-based distributed tracing from Python
 * - Context propagation via thread-local storage
 * - Events within spans for milestones
 *
 * @par Architecture
 * ```
 * Python logging.info()
 *        |
 *        v
 * ErlangHandler.emit()
 *        |
 *        v
 * erlang._log()  ----NIF---->  enif_send()  ---->  py_logger (gen_server)
 *        |                                                   |
 *        | (returns immediately)                       logger:log()
 *        v
 * continue execution
 * ```
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Global state for logging and tracing
 * ============================================================================ */

/** @brief PID of the Erlang process receiving log messages */
ErlNifPid g_log_receiver_pid;

/** @brief Flag: log receiver is registered */
volatile bool g_has_log_receiver = false;

/** @brief Minimum log level threshold (Python levelno) */
volatile int g_log_level_threshold = 0;

/** @brief PID of the Erlang process receiving trace spans */
ErlNifPid g_trace_receiver_pid;

/** @brief Flag: trace receiver is registered */
volatile bool g_has_trace_receiver = false;

/* ============================================================================
 * Level mapping
 * ============================================================================ */

/**
 * @brief Convert Python log level (levelno) to Erlang logger level atom
 *
 * Python levels:
 *   DEBUG    = 10
 *   INFO     = 20
 *   WARNING  = 30
 *   ERROR    = 40
 *   CRITICAL = 50
 *
 * @param env NIF environment for atom creation
 * @param level Python levelno
 * @return Erlang atom: debug | info | warning | error | critical
 */
static ERL_NIF_TERM level_to_atom(ErlNifEnv *env, int level) {
    if (level <= 10) return enif_make_atom(env, "debug");
    if (level <= 20) return enif_make_atom(env, "info");
    if (level <= 30) return enif_make_atom(env, "warning");
    if (level <= 40) return enif_make_atom(env, "error");
    return enif_make_atom(env, "critical");
}

/* ============================================================================
 * Python-callable functions
 * ============================================================================ */

/**
 * @brief Python: erlang._log(level, logger_name, message, metadata)
 *
 * Fire-and-forget log message to Erlang. Never blocks Python.
 * If no receiver is registered or level is below threshold, returns immediately.
 *
 * @param self Module reference (unused)
 * @param args Tuple: (level:int, logger_name:str, message:str, metadata:dict)
 * @return None
 */
static PyObject *erlang_log_impl(PyObject *self, PyObject *args) {
    (void)self;

    /* Quick exit if no receiver */
    if (!g_has_log_receiver) {
        Py_RETURN_NONE;
    }

    int level;
    const char *logger_name;
    Py_ssize_t logger_name_len;
    const char *message;
    Py_ssize_t message_len;
    PyObject *metadata;

    if (!PyArg_ParseTuple(args, "is#s#O",
            &level, &logger_name, &logger_name_len, &message, &message_len, &metadata)) {
        return NULL;
    }

    /* Level filtering */
    if (level < g_log_level_threshold) {
        Py_RETURN_NONE;
    }

    /* Create message environment (will be freed after send) */
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        /* Non-fatal: just skip this log message */
        Py_RETURN_NONE;
    }

    /* Build the message: {py_log, Level, LoggerName, Message, Metadata, Timestamp} */
    ERL_NIF_TERM level_term = level_to_atom(msg_env, level);

    ERL_NIF_TERM logger_term;
    unsigned char *logger_buf = enif_make_new_binary(msg_env, logger_name_len, &logger_term);
    if (logger_buf != NULL) {
        memcpy(logger_buf, logger_name, logger_name_len);
    }

    ERL_NIF_TERM message_term;
    unsigned char *msg_buf = enif_make_new_binary(msg_env, message_len, &message_term);
    if (msg_buf != NULL) {
        memcpy(msg_buf, message, message_len);
    }

    /* Convert metadata dict to Erlang map */
    ERL_NIF_TERM meta_term = py_to_term(msg_env, metadata);

    /* Timestamp in microseconds */
    uint64_t ts = get_monotonic_ns() / 1000;
    ERL_NIF_TERM ts_term = enif_make_uint64(msg_env, ts);

    ERL_NIF_TERM msg = enif_make_tuple6(msg_env,
        ATOM_PY_LOG,
        level_term,
        logger_term,
        message_term,
        meta_term,
        ts_term);

    /* Fire-and-forget send */
    enif_send(NULL, &g_log_receiver_pid, msg_env, msg);
    enif_free_env(msg_env);

    Py_RETURN_NONE;
}

/**
 * @brief Python: erlang._trace_start(name, span_id, parent_id, attrs)
 *
 * Signal start of a trace span to Erlang.
 *
 * @param self Module reference (unused)
 * @param args Tuple: (name:str, span_id:int, parent_id:int|None, attrs:dict)
 * @return None
 */
static PyObject *erlang_trace_start_impl(PyObject *self, PyObject *args) {
    (void)self;

    if (!g_has_trace_receiver) {
        Py_RETURN_NONE;
    }

    const char *name;
    Py_ssize_t name_len;
    unsigned long long span_id;
    PyObject *parent_id_obj;
    PyObject *attrs;

    if (!PyArg_ParseTuple(args, "s#KOO",
            &name, &name_len, &span_id, &parent_id_obj, &attrs)) {
        return NULL;
    }

    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        Py_RETURN_NONE;
    }

    ERL_NIF_TERM span_id_term = enif_make_uint64(msg_env, span_id);

    ERL_NIF_TERM parent_id_term;
    if (parent_id_obj == Py_None) {
        parent_id_term = enif_make_atom(msg_env, "undefined");
    } else {
        unsigned long long parent_id = PyLong_AsUnsignedLongLong(parent_id_obj);
        parent_id_term = enif_make_uint64(msg_env, parent_id);
    }

    ERL_NIF_TERM name_term;
    unsigned char *name_buf = enif_make_new_binary(msg_env, name_len, &name_term);
    if (name_buf != NULL) {
        memcpy(name_buf, name, name_len);
    }

    ERL_NIF_TERM attrs_term = py_to_term(msg_env, attrs);

    uint64_t ts = get_monotonic_ns() / 1000;
    ERL_NIF_TERM ts_term = enif_make_uint64(msg_env, ts);

    /* {span_start, SpanId, ParentId, Name, Attrs, StartTime} */
    ERL_NIF_TERM msg = enif_make_tuple6(msg_env,
        ATOM_SPAN_START,
        span_id_term,
        parent_id_term,
        name_term,
        attrs_term,
        ts_term);

    enif_send(NULL, &g_trace_receiver_pid, msg_env, msg);
    enif_free_env(msg_env);

    Py_RETURN_NONE;
}

/**
 * @brief Python: erlang._trace_end(span_id, status, attrs)
 *
 * Signal end of a trace span to Erlang.
 *
 * @param self Module reference (unused)
 * @param args Tuple: (span_id:int, status:str, attrs:dict)
 * @return None
 */
static PyObject *erlang_trace_end_impl(PyObject *self, PyObject *args) {
    (void)self;

    if (!g_has_trace_receiver) {
        Py_RETURN_NONE;
    }

    unsigned long long span_id;
    const char *status;
    PyObject *attrs;

    if (!PyArg_ParseTuple(args, "KsO", &span_id, &status, &attrs)) {
        return NULL;
    }

    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        Py_RETURN_NONE;
    }

    ERL_NIF_TERM span_id_term = enif_make_uint64(msg_env, span_id);
    /* Use string instead of atom to prevent atom table exhaustion from
     * arbitrary Python trace status strings */
    ERL_NIF_TERM status_term = enif_make_string(msg_env, status, ERL_NIF_LATIN1);
    ERL_NIF_TERM attrs_term = py_to_term(msg_env, attrs);

    uint64_t ts = get_monotonic_ns() / 1000;
    ERL_NIF_TERM ts_term = enif_make_uint64(msg_env, ts);

    /* {span_end, SpanId, Status, Attrs, EndTime} */
    ERL_NIF_TERM msg = enif_make_tuple5(msg_env,
        ATOM_SPAN_END,
        span_id_term,
        status_term,
        attrs_term,
        ts_term);

    enif_send(NULL, &g_trace_receiver_pid, msg_env, msg);
    enif_free_env(msg_env);

    Py_RETURN_NONE;
}

/**
 * @brief Python: erlang._trace_event(span_id, name, attrs)
 *
 * Add an event to a trace span.
 *
 * @param self Module reference (unused)
 * @param args Tuple: (span_id:int, name:str, attrs:dict)
 * @return None
 */
static PyObject *erlang_trace_event_impl(PyObject *self, PyObject *args) {
    (void)self;

    if (!g_has_trace_receiver) {
        Py_RETURN_NONE;
    }

    unsigned long long span_id;
    const char *name;
    Py_ssize_t name_len;
    PyObject *attrs;

    if (!PyArg_ParseTuple(args, "Ks#O", &span_id, &name, &name_len, &attrs)) {
        return NULL;
    }

    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        Py_RETURN_NONE;
    }

    ERL_NIF_TERM span_id_term = enif_make_uint64(msg_env, span_id);

    ERL_NIF_TERM name_term;
    unsigned char *name_buf = enif_make_new_binary(msg_env, name_len, &name_term);
    if (name_buf != NULL) {
        memcpy(name_buf, name, name_len);
    }

    ERL_NIF_TERM attrs_term = py_to_term(msg_env, attrs);

    uint64_t ts = get_monotonic_ns() / 1000;
    ERL_NIF_TERM ts_term = enif_make_uint64(msg_env, ts);

    /* {span_event, SpanId, Name, Attrs, Time} */
    ERL_NIF_TERM msg = enif_make_tuple5(msg_env,
        ATOM_SPAN_EVENT,
        span_id_term,
        name_term,
        attrs_term,
        ts_term);

    enif_send(NULL, &g_trace_receiver_pid, msg_env, msg);
    enif_free_env(msg_env);

    Py_RETURN_NONE;
}

/* ============================================================================
 * NIF functions for receiver management
 * ============================================================================ */

/**
 * @brief NIF: set_log_receiver(Pid, Level) -> ok
 *
 * Register the Erlang process that will receive log messages.
 *
 * @param env NIF environment
 * @param argc Argument count (2)
 * @param argv [Pid, Level]
 * @return ok atom
 */
static ERL_NIF_TERM nif_set_log_receiver(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    ErlNifPid pid;
    int level;

    if (!enif_get_local_pid(env, argv[0], &pid)) {
        return make_error(env, "invalid_pid");
    }

    if (!enif_get_int(env, argv[1], &level)) {
        return make_error(env, "invalid_level");
    }

    g_log_receiver_pid = pid;
    g_log_level_threshold = level;
    g_has_log_receiver = true;

    return ATOM_OK;
}

/**
 * @brief NIF: clear_log_receiver() -> ok
 *
 * Unregister the log receiver. Log messages will be dropped.
 *
 * @param env NIF environment
 * @param argc Argument count (0)
 * @param argv (unused)
 * @return ok atom
 */
static ERL_NIF_TERM nif_clear_log_receiver(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)env;
    (void)argc;
    (void)argv;

    g_has_log_receiver = false;

    return ATOM_OK;
}

/**
 * @brief NIF: set_trace_receiver(Pid) -> ok
 *
 * Register the Erlang process that will receive trace spans.
 *
 * @param env NIF environment
 * @param argc Argument count (1)
 * @param argv [Pid]
 * @return ok atom
 */
static ERL_NIF_TERM nif_set_trace_receiver(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;

    ErlNifPid pid;

    if (!enif_get_local_pid(env, argv[0], &pid)) {
        return make_error(env, "invalid_pid");
    }

    g_trace_receiver_pid = pid;
    g_has_trace_receiver = true;

    return ATOM_OK;
}

/**
 * @brief NIF: clear_trace_receiver() -> ok
 *
 * Unregister the trace receiver. Trace events will be dropped.
 *
 * @param env NIF environment
 * @param argc Argument count (0)
 * @param argv (unused)
 * @return ok atom
 */
static ERL_NIF_TERM nif_clear_trace_receiver(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)env;
    (void)argc;
    (void)argv;

    g_has_trace_receiver = false;

    return ATOM_OK;
}
