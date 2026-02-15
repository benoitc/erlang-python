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
 * py_nif.h - Shared header for Python integration NIF for Erlang
 *
 * This header contains type definitions, macros, and declarations shared
 * across the NIF implementation modules.
 */

#ifndef PY_NIF_H
#define PY_NIF_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <erl_nif.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <pthread.h>
#if defined(__linux__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#include <dlfcn.h>
#define NEED_DLOPEN_GLOBAL 1
#endif

/* ============================================================================
 * Sub-interpreter support (Python 3.12+)
 * ============================================================================ */

/* Check for Python 3.12+ for per-interpreter GIL */
#if PY_VERSION_HEX >= 0x030C0000
#define HAVE_SUBINTERPRETERS 1
#endif

/* Check for Python 3.13+ free-threaded build (no GIL) */
#if PY_VERSION_HEX >= 0x030D0000
#ifdef Py_GIL_DISABLED
#define HAVE_FREE_THREADED 1
#endif
#endif

/* ============================================================================
 * Execution mode support
 * ============================================================================ */

typedef enum {
    PY_MODE_FREE_THREADED,   /* Python 3.13+ free-threaded build (no GIL) */
    PY_MODE_SUBINTERP,       /* Python 3.12+ with per-interpreter GIL */
    PY_MODE_MULTI_EXECUTOR   /* Traditional Python with N executors */
} py_execution_mode_t;

/* ============================================================================
 * Type definitions
 * ============================================================================ */

typedef struct {
    PyThreadState *thread_state;
    PyObject *globals;      /* Global namespace for this worker */
    PyObject *locals;       /* Local namespace */
    bool owns_gil;
    /* Callback support */
    int callback_pipe[2];   /* Pipe for callback IPC: [read, write] */
    ErlNifPid callback_handler;
    bool has_callback_handler;
    ErlNifEnv *callback_env;  /* Env for building callback messages */
} py_worker_t;

/* Pending async request */
typedef struct async_pending {
    uint64_t id;
    PyObject *future;       /* concurrent.futures.Future */
    ErlNifPid caller;
    ERL_NIF_TERM ref;
    struct async_pending *next;
} async_pending_t;

typedef struct {
    pthread_t loop_thread;
    PyObject *event_loop;       /* asyncio event loop */
    int notify_pipe[2];         /* Pipe for notifications: [read, write] */
    volatile bool loop_running;
    volatile bool shutdown;
    pthread_mutex_t queue_mutex;
    async_pending_t *pending_head;
    async_pending_t *pending_tail;
    ErlNifEnv *msg_env;         /* Env for sending messages */
} py_async_worker_t;

/* Python object wrapper for preventing GC */
typedef struct {
    PyObject *obj;
} py_object_t;

/* Request types for the executor */
typedef enum {
    PY_REQ_CALL,
    PY_REQ_EVAL,
    PY_REQ_EXEC,
    PY_REQ_NEXT,
    PY_REQ_IMPORT,
    PY_REQ_GETATTR,
    PY_REQ_MEMORY_STATS,
    PY_REQ_GC,
    PY_REQ_SHUTDOWN
} py_request_type_t;

/* Request structure for executor */
typedef struct py_request {
    py_request_type_t type;

    /* Synchronization - caller waits on this */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    volatile bool completed;

    /* Request parameters */
    py_worker_t *worker;           /* Worker context (optional, for worker ops) */
    ErlNifEnv *env;                /* Caller's env for term conversion */

    /* For call/eval/exec */
    ErlNifBinary module_bin;       /* Module name (for call/import) */
    ErlNifBinary func_bin;         /* Function name (for call) */
    ErlNifBinary code_bin;         /* Code string (for eval/exec) */
    ERL_NIF_TERM args_term;        /* Arguments list */
    ERL_NIF_TERM kwargs_term;      /* Keyword arguments map */
    ERL_NIF_TERM locals_term;      /* Locals map (for eval) */
    unsigned long timeout_ms;       /* Timeout in milliseconds */

    /* For next */
    py_object_t *gen_wrapper;      /* Generator wrapper */

    /* For getattr */
    py_object_t *obj_wrapper;      /* Object wrapper */
    ErlNifBinary attr_bin;         /* Attribute name */

    /* For gc */
    int gc_generation;

    /* Result */
    ERL_NIF_TERM result;           /* Result term */

    /* Queue linkage */
    struct py_request *next;
} py_request_t;

/* Suspended state for reentrant callbacks */
typedef struct {
    py_worker_t *worker;        /* Worker context */
    uint64_t callback_id;       /* Unique callback ID */

    /* Callback function info (what erlang.call() is calling) */
    char *callback_func_name;   /* Function name being called via erlang.call */
    size_t callback_func_len;   /* Length of callback function name */
    PyObject *callback_args;    /* Arguments for the callback */

    /* Original Python call context (for replay) */
    ErlNifBinary orig_module;   /* Original module name */
    ErlNifBinary orig_func;     /* Original function name */
    ERL_NIF_TERM orig_args;     /* Original args (copied to result_env) */
    ERL_NIF_TERM orig_kwargs;   /* Original kwargs (copied to result_env) */
    ErlNifEnv *orig_env;        /* Environment holding original terms */
    int orig_timeout_ms;        /* Original timeout */
    int request_type;           /* PY_REQ_CALL, PY_REQ_EVAL, etc. */
    ErlNifBinary orig_code;     /* For eval/exec: the code string */
    ERL_NIF_TERM orig_locals;   /* For eval: locals map */

    /* Callback result */
    unsigned char *result_data; /* Raw result data from callback */
    size_t result_len;          /* Length of result data */
    volatile bool has_result;   /* Whether result is available */
    volatile bool is_error;     /* Whether result is an error */

    pthread_mutex_t mutex;      /* Synchronization */
    pthread_cond_t cond;        /* Signal when result is available */
} suspended_state_t;

#ifdef HAVE_SUBINTERPRETERS
typedef struct {
    PyInterpreterState *interp;
    PyThreadState *tstate;
    PyObject *globals;
    PyObject *locals;
} py_subinterp_worker_t;
#endif

/* Multi-executor pool for MULTI_EXECUTOR mode */
#define MAX_EXECUTORS 16

typedef struct {
    pthread_t thread;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    struct py_request *queue_head;
    struct py_request *queue_tail;
    volatile bool running;
    volatile bool shutdown;
    int id;
} executor_t;

/* ============================================================================
 * Global state externs
 * ============================================================================ */

extern ErlNifResourceType *WORKER_RESOURCE_TYPE;
extern ErlNifResourceType *PYOBJ_RESOURCE_TYPE;
extern ErlNifResourceType *ASYNC_WORKER_RESOURCE_TYPE;
extern ErlNifResourceType *SUSPENDED_STATE_RESOURCE_TYPE;
#ifdef HAVE_SUBINTERPRETERS
extern ErlNifResourceType *SUBINTERP_WORKER_RESOURCE_TYPE;
#endif

extern bool g_python_initialized;
extern PyThreadState *g_main_thread_state;

/* Execution mode */
extern py_execution_mode_t g_execution_mode;
extern int g_num_executors;

/* Multi-executor pool */
extern executor_t g_executors[MAX_EXECUTORS];
extern _Atomic int g_next_executor;
extern bool g_multi_executor_initialized;

/* Single executor state */
extern pthread_t g_executor_thread;
extern pthread_mutex_t g_executor_mutex;
extern pthread_cond_t g_executor_cond;
extern py_request_t *g_executor_queue_head;
extern py_request_t *g_executor_queue_tail;
extern volatile bool g_executor_running;
extern volatile bool g_executor_shutdown;

/* Global counter for callback IDs */
extern _Atomic uint64_t g_callback_id_counter;

/* Custom exception for suspension */
extern PyObject *SuspensionRequiredException;

/* Thread-local callback context */
extern __thread py_worker_t *tl_current_worker;
extern __thread ErlNifEnv *tl_callback_env;
extern __thread suspended_state_t *tl_current_suspended;
extern __thread bool tl_allow_suspension;

/* Thread-local timeout state */
extern __thread uint64_t tl_timeout_deadline;
extern __thread bool tl_timeout_enabled;

/* Atoms */
extern ERL_NIF_TERM ATOM_OK;
extern ERL_NIF_TERM ATOM_ERROR;
extern ERL_NIF_TERM ATOM_TRUE;
extern ERL_NIF_TERM ATOM_FALSE;
extern ERL_NIF_TERM ATOM_NONE;
extern ERL_NIF_TERM ATOM_UNDEFINED;
extern ERL_NIF_TERM ATOM_NIF_NOT_LOADED;
extern ERL_NIF_TERM ATOM_GENERATOR;
extern ERL_NIF_TERM ATOM_STOP_ITERATION;
extern ERL_NIF_TERM ATOM_TIMEOUT;
extern ERL_NIF_TERM ATOM_NAN;
extern ERL_NIF_TERM ATOM_INFINITY;
extern ERL_NIF_TERM ATOM_NEG_INFINITY;
extern ERL_NIF_TERM ATOM_ERLANG_CALLBACK;
extern ERL_NIF_TERM ATOM_ASYNC_RESULT;
extern ERL_NIF_TERM ATOM_ASYNC_ERROR;
extern ERL_NIF_TERM ATOM_SUSPENDED;

/* ============================================================================
 * Helper function declarations (py_convert.c)
 * ============================================================================ */

/**
 * Convert Python object to Erlang term.
 */
static ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj);

/**
 * Convert Erlang term to Python object.
 */
static PyObject *term_to_py(ErlNifEnv *env, ERL_NIF_TERM term);

/**
 * Create an error tuple {error, Reason}.
 */
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *reason);

/**
 * Create an error tuple from Python exception.
 */
static ERL_NIF_TERM make_py_error(ErlNifEnv *env);

/**
 * Helper to convert ErlNifBinary to null-terminated C string.
 * Caller must call enif_free() on the result.
 * Returns NULL on allocation failure.
 */
static char *binary_to_string(const ErlNifBinary *bin) {
    char *str = enif_alloc(bin->size + 1);
    if (str != NULL) {
        memcpy(str, bin->data, bin->size);
        str[bin->size] = '\0';
    }
    return str;
}

/* ============================================================================
 * Timeout support declarations (py_exec.c)
 * ============================================================================ */

static inline uint64_t get_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void start_timeout(unsigned long timeout_ms);
static void stop_timeout(void);
static bool check_timeout_error(void);

/* ============================================================================
 * Executor declarations (py_exec.c)
 * ============================================================================ */

static void process_request(py_request_t *req);
static void executor_enqueue(py_request_t *req);
static void executor_wait(py_request_t *req);
static void request_init(py_request_t *req);
static void request_cleanup(py_request_t *req);
static int executor_start(void);
static void executor_stop(void);

/* Multi-executor */
static void *multi_executor_thread_main(void *arg);
static int multi_executor_start(int num_executors);
static void multi_executor_stop(void);
static int select_executor(void);
static void multi_executor_enqueue(int exec_id, struct py_request *req);

/* ============================================================================
 * Callback declarations (py_callback.c)
 * ============================================================================ */

static int create_erlang_module(void);
static PyObject *erlang_call_impl(PyObject *self, PyObject *args);
static PyObject *erlang_module_getattr(PyObject *module, PyObject *name);
static void *async_event_loop_thread(void *arg);

/* Suspension handling */
static bool is_suspension_exception(void);
static PyObject *get_suspension_args(void);
static suspended_state_t *create_suspended_state(ErlNifEnv *env, PyObject *exc_args,
                                                  py_request_t *req);
static PyObject *parse_callback_response(unsigned char *response_data, size_t response_len);

/* Execution mode detection */
static void detect_execution_mode(void);

#endif /* PY_NIF_H */
