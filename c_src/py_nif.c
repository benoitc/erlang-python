/**
 * py_nif.c - Python integration NIF for Erlang
 *
 * This NIF embeds Python and allows Erlang processes to execute Python code
 * using dirty I/O schedulers. The design follows patterns from Granian:
 *
 * - GIL is released while waiting for Erlang messages
 * - Workers run on dirty I/O schedulers
 * - Type conversion between Erlang terms and Python objects
 *
 * Key patterns:
 * - Py_BEGIN_ALLOW_THREADS / Py_END_ALLOW_THREADS around blocking ops
 * - Resource types for Python objects to ensure proper cleanup
 * - Dirty NIF flags for GIL-holding operations
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <erl_nif.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <pthread.h>

/* ============================================================================
 * Timeout support
 * ============================================================================ */

static inline uint64_t get_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Thread-local timeout state */
static __thread uint64_t tl_timeout_deadline = 0;
static __thread bool tl_timeout_enabled = false;

static int python_trace_callback(PyObject *obj, PyFrameObject *frame, int what, PyObject *arg) {
    if (tl_timeout_enabled && tl_timeout_deadline > 0) {
        if (get_monotonic_ns() > tl_timeout_deadline) {
            PyErr_SetString(PyExc_TimeoutError, "execution timeout");
            return -1;  /* Abort execution */
        }
    }
    return 0;
}

static void start_timeout(unsigned long timeout_ms) {
    if (timeout_ms > 0) {
        tl_timeout_deadline = get_monotonic_ns() + (timeout_ms * 1000000ULL);
        tl_timeout_enabled = true;
        PyEval_SetTrace(python_trace_callback, NULL);
    }
}

static void stop_timeout(void) {
    if (tl_timeout_enabled) {
        tl_timeout_enabled = false;
        tl_timeout_deadline = 0;
        PyEval_SetTrace(NULL, NULL);
    }
}

static bool check_timeout_error(void) {
    if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_TimeoutError)) {
        return true;
    }
    return false;
}

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

/* ============================================================================
 * Async worker for asyncio support
 * ============================================================================ */

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

/* ============================================================================
 * Sub-interpreter support (Python 3.12+)
 * ============================================================================ */

/* Check for Python 3.12+ for per-interpreter GIL */
#if PY_VERSION_HEX >= 0x030C0000
#define HAVE_SUBINTERPRETERS 1
#endif

#ifdef HAVE_SUBINTERPRETERS
typedef struct {
    PyInterpreterState *interp;
    PyThreadState *tstate;
    PyObject *globals;
    PyObject *locals;
} py_subinterp_worker_t;
#endif

/* Python object wrapper for preventing GC - forward declaration needed for py_request_t */
typedef struct {
    PyObject *obj;
} py_object_t;

/* ============================================================================
 * Executor thread for GIL management (PyO3-style)
 *
 * All Python operations are routed through a single executor thread to ensure
 * consistent thread state. This prevents TLS corruption when C extensions like
 * PyTorch maintain thread-local state.
 * ============================================================================ */

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

/* Global executor state */
static pthread_t g_executor_thread;
static pthread_mutex_t g_executor_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_executor_cond = PTHREAD_COND_INITIALIZER;
static py_request_t *g_executor_queue_head = NULL;
static py_request_t *g_executor_queue_tail = NULL;
static volatile bool g_executor_running = false;
static volatile bool g_executor_shutdown = false;

/* Forward declarations for executor */
static void *executor_thread_main(void *arg);
static void executor_enqueue(py_request_t *req);
static void executor_wait(py_request_t *req);
static void process_request(py_request_t *req);
static void request_init(py_request_t *req);
static void request_cleanup(py_request_t *req);
static int executor_start(void);
static void executor_stop(void);

static ErlNifResourceType *WORKER_RESOURCE_TYPE = NULL;
static ErlNifResourceType *PYOBJ_RESOURCE_TYPE = NULL;
static ErlNifResourceType *ASYNC_WORKER_RESOURCE_TYPE = NULL;
#ifdef HAVE_SUBINTERPRETERS
static ErlNifResourceType *SUBINTERP_WORKER_RESOURCE_TYPE = NULL;
#endif

/* Global state */
static bool g_python_initialized = false;
static PyThreadState *g_main_thread_state = NULL;

/* Thread-local callback context (set before Python execution) */
static __thread py_worker_t *tl_current_worker = NULL;
static __thread ErlNifEnv *tl_callback_env = NULL;

/* Atoms */
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_NONE;
static ERL_NIF_TERM ATOM_UNDEFINED;
static ERL_NIF_TERM ATOM_NIF_NOT_LOADED;
static ERL_NIF_TERM ATOM_GENERATOR;
static ERL_NIF_TERM ATOM_STOP_ITERATION;
static ERL_NIF_TERM ATOM_TIMEOUT;
static ERL_NIF_TERM ATOM_NAN;
static ERL_NIF_TERM ATOM_INFINITY;
static ERL_NIF_TERM ATOM_NEG_INFINITY;
static ERL_NIF_TERM ATOM_ERLANG_CALLBACK;
static ERL_NIF_TERM ATOM_ASYNC_RESULT;
static ERL_NIF_TERM ATOM_ASYNC_ERROR;

/* ============================================================================
 * Forward declarations
 * ============================================================================ */

static ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj);
static PyObject *term_to_py(ErlNifEnv *env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *reason);
static ERL_NIF_TERM make_py_error(ErlNifEnv *env);
static int create_erlang_module(void);
static PyObject *erlang_call_impl(PyObject *self, PyObject *args);
static void *async_event_loop_thread(void *arg);

/* ============================================================================
 * Resource callbacks
 * ============================================================================ */

static void worker_destructor(ErlNifEnv *env, void *obj) {
    py_worker_t *worker = (py_worker_t *)obj;

    /* Close callback pipes */
    if (worker->callback_pipe[0] >= 0) {
        close(worker->callback_pipe[0]);
    }
    if (worker->callback_pipe[1] >= 0) {
        close(worker->callback_pipe[1]);
    }

    /* Only clean up Python state if Python is still initialized */
    if (worker->thread_state != NULL && g_python_initialized) {
        PyEval_RestoreThread(worker->thread_state);
        Py_XDECREF(worker->globals);
        Py_XDECREF(worker->locals);
        PyThreadState_Clear(worker->thread_state);
        PyThreadState_DeleteCurrent();
    }
}

static void pyobj_destructor(ErlNifEnv *env, void *obj) {
    py_object_t *wrapper = (py_object_t *)obj;

    if (wrapper->obj != NULL && g_python_initialized) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_DECREF(wrapper->obj);
        PyGILState_Release(gstate);
    }
}

static void async_worker_destructor(ErlNifEnv *env, void *obj) {
    py_async_worker_t *worker = (py_async_worker_t *)obj;

    /* Signal shutdown */
    worker->shutdown = true;

    /* Write to pipe to wake up event loop */
    if (worker->notify_pipe[1] >= 0) {
        char c = 'q';
        (void)write(worker->notify_pipe[1], &c, 1);
    }

    /* Wait for thread to finish */
    if (worker->loop_running) {
        pthread_join(worker->loop_thread, NULL);
    }

    /* Clean up pending requests */
    pthread_mutex_lock(&worker->queue_mutex);
    async_pending_t *p = worker->pending_head;
    while (p != NULL) {
        async_pending_t *next = p->next;
        if (g_python_initialized && p->future != NULL) {
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_DECREF(p->future);
            PyGILState_Release(gstate);
        }
        enif_free(p);
        p = next;
    }
    pthread_mutex_unlock(&worker->queue_mutex);

    pthread_mutex_destroy(&worker->queue_mutex);

    /* Close pipes */
    if (worker->notify_pipe[0] >= 0) close(worker->notify_pipe[0]);
    if (worker->notify_pipe[1] >= 0) close(worker->notify_pipe[1]);

    if (worker->msg_env != NULL) {
        enif_free_env(worker->msg_env);
    }

    /* Clean up event loop */
    if (g_python_initialized && worker->event_loop != NULL) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_DECREF(worker->event_loop);
        PyGILState_Release(gstate);
    }
}

#ifdef HAVE_SUBINTERPRETERS
static void subinterp_worker_destructor(ErlNifEnv *env, void *obj) {
    py_subinterp_worker_t *worker = (py_subinterp_worker_t *)obj;

    if (worker->tstate != NULL && g_python_initialized) {
        /* Switch to this interpreter's thread state */
        PyThreadState *old_tstate = PyThreadState_Swap(worker->tstate);

        Py_XDECREF(worker->globals);
        Py_XDECREF(worker->locals);

        /* End the interpreter */
        Py_EndInterpreter(worker->tstate);

        /* Restore previous thread state */
        PyThreadState_Swap(old_tstate);
    }
}
#endif

/* ============================================================================
 * Initialization
 * ============================================================================ */

static ERL_NIF_TERM nif_py_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (g_python_initialized) {
        return ATOM_OK;
    }

    /* Initialize Python with thread support */
    PyConfig config;
    PyConfig_InitPythonConfig(&config);

    /* Parse options from argv[0] if provided */
    if (argc > 0 && enif_is_map(env, argv[0])) {
        ERL_NIF_TERM key, value;
        ErlNifMapIterator iter;

        enif_map_iterator_create(env, argv[0], &iter, ERL_NIF_MAP_ITERATOR_FIRST);
        while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
            /* Handle python_home, python_path, etc. */
            enif_map_iterator_next(env, &iter);
        }
        enif_map_iterator_destroy(env, &iter);
    }

    PyStatus status = Py_InitializeFromConfig(&config);
    PyConfig_Clear(&config);

    if (PyStatus_Exception(status)) {
        return make_error(env, "python_init_failed");
    }

    g_python_initialized = true;

    /* Create the 'erlang' module for callbacks */
    if (create_erlang_module() < 0) {
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "erlang_module_creation_failed");
    }

    /* Save main thread state and release GIL for other threads */
    g_main_thread_state = PyEval_SaveThread();

    /* Start the executor thread for GIL management */
    if (executor_start() < 0) {
        PyEval_RestoreThread(g_main_thread_state);
        g_main_thread_state = NULL;
        Py_Finalize();
        g_python_initialized = false;
        return make_error(env, "executor_start_failed");
    }

    return ATOM_OK;
}

static ERL_NIF_TERM nif_finalize(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return ATOM_OK;
    }

    /* Stop the executor thread first */
    executor_stop();

    /* Restore main thread state before finalizing */
    if (g_main_thread_state != NULL) {
        PyEval_RestoreThread(g_main_thread_state);
        g_main_thread_state = NULL;
    }

    Py_Finalize();
    g_python_initialized = false;

    return ATOM_OK;
}

/* ============================================================================
 * Worker management
 * ============================================================================ */

static ERL_NIF_TERM nif_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_worker_t *worker = enif_alloc_resource(WORKER_RESOURCE_TYPE, sizeof(py_worker_t));
    if (worker == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Acquire GIL to create thread state */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Create a new thread state for this worker */
    PyInterpreterState *interp = PyInterpreterState_Get();
    worker->thread_state = PyThreadState_New(interp);

    /* Create global/local namespaces */
    worker->globals = PyDict_New();
    worker->locals = PyDict_New();

    /* Import __builtins__ into globals */
    PyObject *builtins = PyEval_GetBuiltins();
    PyDict_SetItemString(worker->globals, "__builtins__", builtins);

    /* Import erlang module into worker's namespace for callbacks */
    PyObject *erlang_module = PyImport_ImportModule("erlang");
    if (erlang_module != NULL) {
        PyDict_SetItemString(worker->globals, "erlang", erlang_module);
        Py_DECREF(erlang_module);
    }

    worker->owns_gil = false;

    /* Initialize callback state */
    worker->callback_pipe[0] = -1;
    worker->callback_pipe[1] = -1;
    worker->has_callback_handler = false;
    worker->callback_env = NULL;

    PyGILState_Release(gstate);

    ERL_NIF_TERM result = enif_make_resource(env, worker);
    enif_release_resource(worker);

    return enif_make_tuple2(env, ATOM_OK, result);
}

static ERL_NIF_TERM nif_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Resource destructor will handle cleanup */
    return ATOM_OK;
}

/* ============================================================================
 * Python execution (dirty NIFs)
 * ============================================================================ */

/**
 * Execute a Python function call.
 *
 * Args: WorkerRef, Module (binary), Func (binary), Args (list), Kwargs (map), [Timeout (ms)]
 *
 * This is a dirty I/O NIF because it holds the GIL during execution.
 */
static ERL_NIF_TERM nif_worker_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Build request and route to executor */
    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_CALL;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.module_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &req.func_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_func");
    }

    req.args_term = argv[3];
    req.kwargs_term = (argc > 4) ? argv[4] : 0;
    req.timeout_ms = 0;

    if (argc > 5) {
        enif_get_ulong(env, argv[5], &req.timeout_ms);
    }

    /* Submit to executor and wait */
    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Evaluate a Python expression.
 * Args: WorkerRef, Code (binary), Locals (map), [Timeout (ms)]
 */
static ERL_NIF_TERM nif_worker_eval(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_EVAL;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.code_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_code");
    }

    req.locals_term = (argc > 2) ? argv[2] : 0;
    req.timeout_ms = 0;
    if (argc > 3) {
        enif_get_ulong(env, argv[3], &req.timeout_ms);
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Execute Python statements.
 */
static ERL_NIF_TERM nif_worker_exec(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_EXEC;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.code_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_code");
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Get next item from a generator/iterator.
 * Returns {ok, Value} | {error, stop_iteration} | {error, Error}
 */
static ERL_NIF_TERM nif_worker_next(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;
    py_object_t *gen_wrapper;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_get_resource(env, argv[1], PYOBJ_RESOURCE_TYPE, (void **)&gen_wrapper)) {
        return make_error(env, "invalid_generator");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_NEXT;
    req.worker = worker;
    req.env = env;
    req.gen_wrapper = gen_wrapper;

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Import a Python module.
 */
static ERL_NIF_TERM nif_import_module(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_IMPORT;
    req.worker = worker;
    req.env = env;

    if (!enif_inspect_binary(env, argv[1], &req.module_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_module");
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Get attribute from Python object.
 */
static ERL_NIF_TERM nif_get_attr(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;
    py_object_t *obj_wrapper;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_get_resource(env, argv[1], PYOBJ_RESOURCE_TYPE, (void **)&obj_wrapper)) {
        return make_error(env, "invalid_object");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_GETATTR;
    req.worker = worker;
    req.env = env;
    req.obj_wrapper = obj_wrapper;

    if (!enif_inspect_binary(env, argv[2], &req.attr_bin)) {
        request_cleanup(&req);
        return make_error(env, "invalid_attr");
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Get Python version.
 */
static ERL_NIF_TERM nif_version(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    const char *version = Py_GetVersion();
    ERL_NIF_TERM version_bin;

    unsigned char *buf = enif_make_new_binary(env, strlen(version), &version_bin);
    memcpy(buf, version, strlen(version));

    return enif_make_tuple2(env, ATOM_OK, version_bin);
}

/**
 * Get Python memory statistics.
 * Returns a map with memory stats.
 */
static ERL_NIF_TERM nif_memory_stats(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_MEMORY_STATS;
    req.env = env;

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Force Python garbage collection.
 * Returns the number of unreachable objects collected.
 */
static ERL_NIF_TERM nif_gc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_request_t req;
    request_init(&req);
    req.type = PY_REQ_GC;
    req.env = env;
    req.gc_generation = 2;  /* Full collection by default */
    if (argc > 0) {
        enif_get_int(env, argv[0], &req.gc_generation);
    }

    executor_enqueue(&req);
    executor_wait(&req);

    ERL_NIF_TERM result = req.result;
    request_cleanup(&req);
    return result;
}

/**
 * Start memory tracing.
 */
static ERL_NIF_TERM nif_tracemalloc_start(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *tracemalloc = PyImport_ImportModule("tracemalloc");
    if (tracemalloc == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "tracemalloc_import_failed");
    }

    int nframe = 1;
    if (argc > 0) {
        enif_get_int(env, argv[0], &nframe);
    }

    PyObject *result = PyObject_CallMethod(tracemalloc, "start", "i", nframe);
    Py_DECREF(tracemalloc);

    ERL_NIF_TERM ret;
    if (result == NULL) {
        ret = make_py_error(env);
    } else {
        Py_DECREF(result);
        ret = ATOM_OK;
    }

    PyGILState_Release(gstate);
    return ret;
}

/**
 * Stop memory tracing.
 */
static ERL_NIF_TERM nif_tracemalloc_stop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *tracemalloc = PyImport_ImportModule("tracemalloc");
    if (tracemalloc == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "tracemalloc_import_failed");
    }

    PyObject *result = PyObject_CallMethod(tracemalloc, "stop", NULL);
    Py_DECREF(tracemalloc);

    ERL_NIF_TERM ret;
    if (result == NULL) {
        ret = make_py_error(env);
    } else {
        Py_DECREF(result);
        ret = ATOM_OK;
    }

    PyGILState_Release(gstate);
    return ret;
}

/* ============================================================================
 * Asyncio support
 * ============================================================================ */

/**
 * Callback function that gets invoked when a future completes.
 * This is called from within the event loop thread.
 */
static void async_future_callback(py_async_worker_t *worker, async_pending_t *pending) {
    ErlNifEnv *msg_env = enif_alloc_env();
    PyObject *py_result = PyObject_CallMethod(pending->future, "result", NULL);

    ERL_NIF_TERM result_term;
    if (py_result == NULL) {
        /* Exception occurred */
        PyObject *exc = PyObject_CallMethod(pending->future, "exception", NULL);
        if (exc != NULL && exc != Py_None) {
            PyObject *str = PyObject_Str(exc);
            const char *err_msg = str ? PyUnicode_AsUTF8(str) : "unknown";
            result_term = enif_make_tuple2(msg_env, ATOM_ERROR,
                enif_make_string(msg_env, err_msg, ERL_NIF_LATIN1));
            Py_XDECREF(str);
        } else {
            result_term = enif_make_tuple2(msg_env, ATOM_ERROR,
                enif_make_atom(msg_env, "unknown"));
        }
        Py_XDECREF(exc);
        PyErr_Clear();
    } else {
        result_term = enif_make_tuple2(msg_env, ATOM_OK,
            py_to_term(msg_env, py_result));
        Py_DECREF(py_result);
    }

    /* Send message: {async_result, Id, Result} */
    ERL_NIF_TERM msg = enif_make_tuple3(msg_env,
        ATOM_ASYNC_RESULT,
        enif_make_uint64(msg_env, pending->id),
        result_term);
    enif_send(NULL, &pending->caller, msg_env, msg);
    enif_free_env(msg_env);
}

/**
 * Background thread running the asyncio event loop.
 * This thread owns the event loop and processes coroutines.
 */
static void *async_event_loop_thread(void *arg) {
    py_async_worker_t *worker = (py_async_worker_t *)arg;

    /* Acquire GIL for this thread */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Import asyncio */
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (asyncio == NULL) {
        PyErr_Print();
        PyGILState_Release(gstate);
        worker->loop_running = false;
        return NULL;
    }

    /* Create new event loop */
    PyObject *loop = PyObject_CallMethod(asyncio, "new_event_loop", NULL);
    if (loop == NULL) {
        PyErr_Print();
        Py_DECREF(asyncio);
        PyGILState_Release(gstate);
        worker->loop_running = false;
        return NULL;
    }

    /* Set as current loop */
    PyObject *set_result = PyObject_CallMethod(asyncio, "set_event_loop", "O", loop);
    Py_XDECREF(set_result);

    worker->event_loop = loop;
    Py_INCREF(loop);  /* Keep extra ref for worker struct */

    Py_DECREF(asyncio);

    worker->loop_running = true;

    /* Run the event loop with proper GIL management */
    while (!worker->shutdown) {
        /* Release GIL while sleeping (allow other Python threads to run) */
        Py_BEGIN_ALLOW_THREADS
        usleep(10000);  /* 10ms sleep without holding GIL */
        Py_END_ALLOW_THREADS

        /* Run one iteration of the event loop with GIL held */
        PyObject *asyncio_mod = PyImport_ImportModule("asyncio");
        if (asyncio_mod != NULL) {
            PyObject *sleep_coro = PyObject_CallMethod(asyncio_mod, "sleep", "d", 0.0);
            if (sleep_coro != NULL) {
                PyObject *task = PyObject_CallMethod(loop, "create_task", "O", sleep_coro);
                Py_DECREF(sleep_coro);
                if (task != NULL) {
                    PyObject *run_result = PyObject_CallMethod(loop, "run_until_complete", "O", task);
                    Py_DECREF(task);
                    Py_XDECREF(run_result);
                }
            }
            Py_DECREF(asyncio_mod);
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }

        /* Check for completed futures (GIL held) */
        pthread_mutex_lock(&worker->queue_mutex);
        async_pending_t *prev = NULL;
        async_pending_t *p = worker->pending_head;
        while (p != NULL) {
            if (p->future != NULL) {
                PyObject *done = PyObject_CallMethod(p->future, "done", NULL);
                if (done != NULL && PyObject_IsTrue(done)) {
                    Py_DECREF(done);

                    /* Future is complete - process it */
                    async_future_callback(worker, p);

                    /* Remove from list */
                    Py_DECREF(p->future);
                    if (prev == NULL) {
                        worker->pending_head = p->next;
                    } else {
                        prev->next = p->next;
                    }
                    if (p == worker->pending_tail) {
                        worker->pending_tail = prev;
                    }
                    async_pending_t *to_free = p;
                    p = p->next;
                    enif_free(to_free);
                    continue;
                }
                Py_XDECREF(done);
            }
            prev = p;
            p = p->next;
        }
        pthread_mutex_unlock(&worker->queue_mutex);
    }

    /* Stop and close the event loop */
    PyObject_CallMethod(loop, "stop", NULL);
    PyObject_CallMethod(loop, "close", NULL);
    Py_DECREF(loop);

    worker->loop_running = false;
    PyGILState_Release(gstate);

    return NULL;
}

/**
 * Create a new async worker with background event loop.
 */
static ERL_NIF_TERM nif_async_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_async_worker_t *worker = enif_alloc_resource(ASYNC_WORKER_RESOURCE_TYPE, sizeof(py_async_worker_t));
    if (worker == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Initialize fields */
    worker->event_loop = NULL;
    worker->loop_running = false;
    worker->shutdown = false;
    worker->pending_head = NULL;
    worker->pending_tail = NULL;
    worker->msg_env = enif_alloc_env();

    /* Create notification pipe */
    if (pipe(worker->notify_pipe) < 0) {
        enif_release_resource(worker);
        return make_error(env, "pipe_failed");
    }

    /* Initialize mutex */
    pthread_mutex_init(&worker->queue_mutex, NULL);

    /* Start the event loop thread */
    if (pthread_create(&worker->loop_thread, NULL, async_event_loop_thread, worker) != 0) {
        close(worker->notify_pipe[0]);
        close(worker->notify_pipe[1]);
        pthread_mutex_destroy(&worker->queue_mutex);
        enif_release_resource(worker);
        return make_error(env, "thread_create_failed");
    }

    /* Wait for event loop to be ready */
    int max_wait = 100;  /* 1 second max */
    while (!worker->loop_running && max_wait-- > 0) {
        usleep(10000);  /* 10ms */
    }

    if (!worker->loop_running) {
        worker->shutdown = true;
        pthread_join(worker->loop_thread, NULL);
        close(worker->notify_pipe[0]);
        close(worker->notify_pipe[1]);
        pthread_mutex_destroy(&worker->queue_mutex);
        enif_release_resource(worker);
        return make_error(env, "event_loop_start_failed");
    }

    ERL_NIF_TERM result = enif_make_resource(env, worker);
    enif_release_resource(worker);

    return enif_make_tuple2(env, ATOM_OK, result);
}

/**
 * Destroy an async worker.
 */
static ERL_NIF_TERM nif_async_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_async_worker_t *worker;

    if (!enif_get_resource(env, argv[0], ASYNC_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Resource destructor will handle cleanup */
    return ATOM_OK;
}

/* Counter for unique async call IDs */
static uint64_t g_async_id_counter = 0;

/**
 * Submit an async call to the event loop.
 * Args: AsyncWorkerRef, Module (binary), Func (binary), Args (list), Kwargs (map), CallerPid
 * Returns: {ok, AsyncId}
 */
static ERL_NIF_TERM nif_async_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_async_worker_t *worker;
    ErlNifBinary module_bin, func_bin;
    ErlNifPid caller;

    if (!enif_get_resource(env, argv[0], ASYNC_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!worker->loop_running) {
        return make_error(env, "event_loop_not_running");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &func_bin)) {
        return make_error(env, "invalid_func");
    }
    if (!enif_get_local_pid(env, argv[5], &caller)) {
        return make_error(env, "invalid_caller");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Convert module/func names */
    char *module_name = enif_alloc(module_bin.size + 1);
    memcpy(module_name, module_bin.data, module_bin.size);
    module_name[module_bin.size] = '\0';

    char *func_name = enif_alloc(func_bin.size + 1);
    memcpy(func_name, func_bin.data, func_bin.size);
    func_name[func_bin.size] = '\0';

    ERL_NIF_TERM result;

    /* Import module and get function */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    PyObject *func = PyObject_GetAttrString(module, func_name);
    Py_DECREF(module);
    if (func == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert args list to Python tuple */
    unsigned int args_len;
    if (!enif_get_list_length(env, argv[3], &args_len)) {
        Py_DECREF(func);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[3];
    for (unsigned int i = 0; i < args_len; i++) {
        enif_get_list_cell(env, tail, &head, &tail);
        PyObject *arg = term_to_py(env, head);
        if (arg == NULL) {
            Py_DECREF(args);
            Py_DECREF(func);
            result = make_error(env, "arg_conversion_failed");
            goto cleanup;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }

    /* Convert kwargs */
    PyObject *kwargs = NULL;
    if (argc > 4 && enif_is_map(env, argv[4])) {
        kwargs = term_to_py(env, argv[4]);
    }

    /* Call the function to get coroutine */
    PyObject *coro = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (coro == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Check if result is a coroutine */
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (asyncio == NULL) {
        Py_DECREF(coro);
        result = make_error(env, "asyncio_import_failed");
        goto cleanup;
    }

    PyObject *iscoroutine = PyObject_CallMethod(asyncio, "iscoroutine", "O", coro);
    bool is_coro = iscoroutine != NULL && PyObject_IsTrue(iscoroutine);
    Py_XDECREF(iscoroutine);

    if (!is_coro) {
        Py_DECREF(asyncio);
        /* Not a coroutine - return result directly */
        ERL_NIF_TERM term_result = py_to_term(env, coro);
        Py_DECREF(coro);
        result = enif_make_tuple2(env, ATOM_OK,
            enif_make_tuple2(env, enif_make_atom(env, "immediate"), term_result));
        goto cleanup;
    }

    /* Submit coroutine to event loop using run_coroutine_threadsafe */
    PyObject *future = PyObject_CallMethod(asyncio, "run_coroutine_threadsafe",
        "OO", coro, worker->event_loop);
    Py_DECREF(coro);
    Py_DECREF(asyncio);

    if (future == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Create pending entry */
    uint64_t async_id = __sync_fetch_and_add(&g_async_id_counter, 1);

    async_pending_t *pending = enif_alloc(sizeof(async_pending_t));
    pending->id = async_id;
    pending->future = future;
    pending->caller = caller;
    pending->next = NULL;

    /* Add to pending list */
    pthread_mutex_lock(&worker->queue_mutex);
    if (worker->pending_tail == NULL) {
        worker->pending_head = pending;
        worker->pending_tail = pending;
    } else {
        worker->pending_tail->next = pending;
        worker->pending_tail = pending;
    }
    pthread_mutex_unlock(&worker->queue_mutex);

    result = enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, async_id));

cleanup:
    enif_free(module_name);
    enif_free(func_name);
    PyGILState_Release(gstate);

    return result;
}

/**
 * Execute multiple async calls concurrently using asyncio.gather.
 * Args: AsyncWorkerRef, CallsList (list of {Module, Func, Args}), CallerPid
 * Returns: {ok, AsyncId}
 */
static ERL_NIF_TERM nif_async_gather(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_async_worker_t *worker;
    ErlNifPid caller;

    if (!enif_get_resource(env, argv[0], ASYNC_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!worker->loop_running) {
        return make_error(env, "event_loop_not_running");
    }
    if (!enif_get_local_pid(env, argv[2], &caller)) {
        return make_error(env, "invalid_caller");
    }

    unsigned int calls_len;
    if (!enif_get_list_length(env, argv[1], &calls_len)) {
        return make_error(env, "invalid_calls_list");
    }

    if (calls_len == 0) {
        return enif_make_tuple2(env, ATOM_OK,
            enif_make_tuple2(env, enif_make_atom(env, "immediate"), enif_make_list(env, 0)));
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Import asyncio */
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (asyncio == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "asyncio_import_failed");
    }

    /* Build list of coroutines */
    PyObject *coros = PyList_New(calls_len);
    ERL_NIF_TERM head, tail = argv[1];

    for (unsigned int i = 0; i < calls_len; i++) {
        enif_get_list_cell(env, tail, &head, &tail);

        int arity;
        const ERL_NIF_TERM *tuple;
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity < 3) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "invalid_call_tuple");
        }

        ErlNifBinary module_bin, func_bin;
        if (!enif_inspect_binary(env, tuple[0], &module_bin) ||
            !enif_inspect_binary(env, tuple[1], &func_bin)) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "invalid_module_or_func");
        }

        char module_name[256], func_name[256];
        if (module_bin.size >= 256 || func_bin.size >= 256) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "name_too_long");
        }
        memcpy(module_name, module_bin.data, module_bin.size);
        module_name[module_bin.size] = '\0';
        memcpy(func_name, func_bin.data, func_bin.size);
        func_name[func_bin.size] = '\0';

        /* Import module and get function */
        PyObject *module = PyImport_ImportModule(module_name);
        if (module == NULL) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            ERL_NIF_TERM err = make_py_error(env);
            PyGILState_Release(gstate);
            return err;
        }

        PyObject *func = PyObject_GetAttrString(module, func_name);
        Py_DECREF(module);
        if (func == NULL) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            ERL_NIF_TERM err = make_py_error(env);
            PyGILState_Release(gstate);
            return err;
        }

        /* Convert args */
        unsigned int args_len;
        if (!enif_get_list_length(env, tuple[2], &args_len)) {
            Py_DECREF(func);
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            PyGILState_Release(gstate);
            return make_error(env, "invalid_args");
        }

        PyObject *args = PyTuple_New(args_len);
        ERL_NIF_TERM arg_head, arg_tail = tuple[2];
        for (unsigned int j = 0; j < args_len; j++) {
            enif_get_list_cell(env, arg_tail, &arg_head, &arg_tail);
            PyObject *arg = term_to_py(env, arg_head);
            if (arg == NULL) {
                Py_DECREF(args);
                Py_DECREF(func);
                Py_DECREF(coros);
                Py_DECREF(asyncio);
                PyGILState_Release(gstate);
                return make_error(env, "arg_conversion_failed");
            }
            PyTuple_SET_ITEM(args, j, arg);
        }

        /* Call function to get coroutine */
        PyObject *coro = PyObject_Call(func, args, NULL);
        Py_DECREF(func);
        Py_DECREF(args);

        if (coro == NULL) {
            Py_DECREF(coros);
            Py_DECREF(asyncio);
            ERL_NIF_TERM err = make_py_error(env);
            PyGILState_Release(gstate);
            return err;
        }

        PyList_SET_ITEM(coros, i, coro);
    }

    /* Create asyncio.gather(*coros) */
    PyObject *gather_args = PyTuple_New(calls_len);
    for (unsigned int i = 0; i < calls_len; i++) {
        PyObject *coro = PyList_GetItem(coros, i);
        Py_INCREF(coro);
        PyTuple_SET_ITEM(gather_args, i, coro);
    }

    PyObject *gather_func = PyObject_GetAttrString(asyncio, "gather");
    PyObject *gather_coro = PyObject_Call(gather_func, gather_args, NULL);
    Py_DECREF(gather_func);
    Py_DECREF(gather_args);
    Py_DECREF(coros);

    if (gather_coro == NULL) {
        Py_DECREF(asyncio);
        ERL_NIF_TERM err = make_py_error(env);
        PyGILState_Release(gstate);
        return err;
    }

    /* Submit to event loop */
    PyObject *future = PyObject_CallMethod(asyncio, "run_coroutine_threadsafe",
        "OO", gather_coro, worker->event_loop);
    Py_DECREF(gather_coro);
    Py_DECREF(asyncio);

    if (future == NULL) {
        ERL_NIF_TERM err = make_py_error(env);
        PyGILState_Release(gstate);
        return err;
    }

    /* Create pending entry */
    uint64_t async_id = __sync_fetch_and_add(&g_async_id_counter, 1);

    async_pending_t *pending = enif_alloc(sizeof(async_pending_t));
    pending->id = async_id;
    pending->future = future;
    pending->caller = caller;
    pending->next = NULL;

    /* Add to pending list */
    pthread_mutex_lock(&worker->queue_mutex);
    if (worker->pending_tail == NULL) {
        worker->pending_head = pending;
        worker->pending_tail = pending;
    } else {
        worker->pending_tail->next = pending;
        worker->pending_tail = pending;
    }
    pthread_mutex_unlock(&worker->queue_mutex);

    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, async_id));
}

/**
 * Iterate an async generator.
 * Args: AsyncWorkerRef, Module, Func, Args, CallerPid
 * Returns: {ok, AsyncId} - results will be streamed as messages
 */
static ERL_NIF_TERM nif_async_stream(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* For now, delegate to async_call - async generators will be handled
     * in the Erlang layer by collecting results */
    return nif_async_call(env, argc, argv);
}

/* ============================================================================
 * Sub-interpreter support (Python 3.12+)
 * ============================================================================ */

/**
 * Check if sub-interpreters with per-interpreter GIL are supported.
 */
static ERL_NIF_TERM nif_subinterp_supported(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
#ifdef HAVE_SUBINTERPRETERS
    return ATOM_TRUE;
#else
    return ATOM_FALSE;
#endif
}

#ifdef HAVE_SUBINTERPRETERS

/**
 * Create a new sub-interpreter with its own GIL.
 *
 * For sub-interpreters with per-interpreter GIL (Python 3.12+), we need
 * careful thread state management since Py_NewInterpreterFromConfig
 * automatically switches to the new interpreter's thread state.
 */
static ERL_NIF_TERM nif_subinterp_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    py_subinterp_worker_t *worker = enif_alloc_resource(SUBINTERP_WORKER_RESOURCE_TYPE,
                                                         sizeof(py_subinterp_worker_t));
    if (worker == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Need the main GIL to create sub-interpreter */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Save current thread state so we can restore it after creating sub-interp */
    PyThreadState *main_tstate = PyThreadState_Get();

    /* Configure sub-interpreter with its own GIL */
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 1,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_OWN_GIL,  /* This is the key - own GIL! */
    };

    PyThreadState *tstate = NULL;
    PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);

    if (PyStatus_Exception(status) || tstate == NULL) {
        /* We're still in main interpreter on error */
        PyGILState_Release(gstate);
        enif_release_resource(worker);
        return make_error(env, "subinterp_create_failed");
    }

    /*
     * Py_NewInterpreterFromConfig has switched us to the new sub-interpreter.
     * We're now holding the sub-interpreter's GIL (since it has OWN_GIL).
     */
    worker->interp = PyThreadState_GetInterpreter(tstate);
    worker->tstate = tstate;

    /* Create global/local namespaces in the new interpreter */
    worker->globals = PyDict_New();
    worker->locals = PyDict_New();

    /* Import __builtins__ */
    PyObject *builtins = PyEval_GetBuiltins();
    PyDict_SetItemString(worker->globals, "__builtins__", builtins);

    /*
     * Switch back to main interpreter:
     * 1. Detach from sub-interpreter (swap to NULL)
     * 2. Re-attach to main interpreter (swap to main_tstate)
     * This properly releases the sub-interpreter's GIL and reacquires main GIL.
     */
    PyThreadState_Swap(NULL);
    PyThreadState_Swap(main_tstate);

    /* Now we're back in main interpreter, release main GIL properly */
    PyGILState_Release(gstate);

    ERL_NIF_TERM result = enif_make_resource(env, worker);
    enif_release_resource(worker);

    return enif_make_tuple2(env, ATOM_OK, result);
}

/**
 * Destroy a sub-interpreter worker.
 */
static ERL_NIF_TERM nif_subinterp_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_subinterp_worker_t *worker;

    if (!enif_get_resource(env, argv[0], SUBINTERP_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    /* Resource destructor will handle cleanup */
    return ATOM_OK;
}

/**
 * Call a function in a sub-interpreter.
 * Args: WorkerRef, Module (binary), Func (binary), Args (list), Kwargs (map)
 *
 * Sub-interpreters with per-interpreter GIL (Python 3.12+) require proper
 * thread state management. We don't mix PyGILState with PyThreadState_Swap.
 */
static ERL_NIF_TERM nif_subinterp_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_subinterp_worker_t *worker;
    ErlNifBinary module_bin, func_bin;

    if (!enif_get_resource(env, argv[0], SUBINTERP_WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &func_bin)) {
        return make_error(env, "invalid_func");
    }

    /*
     * Properly enter the sub-interpreter:
     * 1. Save current thread state (may be NULL if no current interpreter)
     * 2. Swap to NULL (detach from current interpreter)
     * 3. Swap to sub-interpreter's thread state (attach to sub-interpreter)
     *
     * For sub-interpreters with own GIL, we acquire that GIL by swapping
     * to its thread state.
     */
    PyThreadState *saved_tstate = PyThreadState_Swap(NULL);
    PyThreadState_Swap(worker->tstate);

    char *module_name = enif_alloc(module_bin.size + 1);
    memcpy(module_name, module_bin.data, module_bin.size);
    module_name[module_bin.size] = '\0';

    char *func_name = enif_alloc(func_bin.size + 1);
    memcpy(func_name, func_bin.data, func_bin.size);
    func_name[func_bin.size] = '\0';

    ERL_NIF_TERM result;

    /* Import module */
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Get function */
    PyObject *func = PyObject_GetAttrString(module, func_name);
    Py_DECREF(module);
    if (func == NULL) {
        result = make_py_error(env);
        goto cleanup;
    }

    /* Convert args */
    unsigned int args_len;
    if (!enif_get_list_length(env, argv[3], &args_len)) {
        Py_DECREF(func);
        result = make_error(env, "invalid_args");
        goto cleanup;
    }

    PyObject *args = PyTuple_New(args_len);
    ERL_NIF_TERM head, tail = argv[3];
    for (unsigned int i = 0; i < args_len; i++) {
        enif_get_list_cell(env, tail, &head, &tail);
        PyObject *arg = term_to_py(env, head);
        if (arg == NULL) {
            Py_DECREF(args);
            Py_DECREF(func);
            result = make_error(env, "arg_conversion_failed");
            goto cleanup;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }

    /* Convert kwargs */
    PyObject *kwargs = NULL;
    if (argc > 4 && enif_is_map(env, argv[4])) {
        kwargs = term_to_py(env, argv[4]);
    }

    /* Call the function */
    PyObject *py_result = PyObject_Call(func, args, kwargs);
    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (py_result == NULL) {
        result = make_py_error(env);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    enif_free(module_name);
    enif_free(func_name);

    /*
     * Exit the sub-interpreter:
     * 1. Swap to NULL (detach from sub-interpreter)
     * 2. Swap back to saved thread state (may be NULL)
     */
    PyThreadState_Swap(NULL);
    if (saved_tstate != NULL) {
        PyThreadState_Swap(saved_tstate);
    }

    return result;
}

/**
 * Execute multiple calls in parallel across sub-interpreters.
 * Each call runs in its own sub-interpreter with its own GIL.
 * Args: WorkerRefs (list of sub-interpreter refs), Calls (list of {Module, Func, Args})
 * Returns: List of results
 */
static ERL_NIF_TERM nif_parallel_execute(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    unsigned int workers_len, calls_len;

    if (!enif_get_list_length(env, argv[0], &workers_len)) {
        return make_error(env, "invalid_workers_list");
    }
    if (!enif_get_list_length(env, argv[1], &calls_len)) {
        return make_error(env, "invalid_calls_list");
    }
    if (workers_len == 0 || calls_len == 0) {
        return enif_make_tuple2(env, ATOM_OK, enif_make_list(env, 0));
    }
    if (workers_len < calls_len) {
        return make_error(env, "not_enough_workers");
    }

    /* For true parallelism, we would spawn threads here.
     * For simplicity in this initial implementation, we execute sequentially
     * but each call uses its own sub-interpreter with its own GIL,
     * so they don't block each other on the GIL. */

    ERL_NIF_TERM *results = enif_alloc(sizeof(ERL_NIF_TERM) * calls_len);
    ERL_NIF_TERM worker_head, worker_tail = argv[0];
    ERL_NIF_TERM call_head, call_tail = argv[1];

    for (unsigned int i = 0; i < calls_len; i++) {
        enif_get_list_cell(env, worker_tail, &worker_head, &worker_tail);
        enif_get_list_cell(env, call_tail, &call_head, &call_tail);

        int arity;
        const ERL_NIF_TERM *tuple;
        if (!enif_get_tuple(env, call_head, &arity, &tuple) || arity < 3) {
            enif_free(results);
            return make_error(env, "invalid_call_tuple");
        }

        /* Build args array for subinterp_call */
        ERL_NIF_TERM call_args[5] = {worker_head, tuple[0], tuple[1], tuple[2],
                                      (arity > 3) ? tuple[3] : enif_make_new_map(env)};

        results[i] = nif_subinterp_call(env, 5, call_args);
    }

    ERL_NIF_TERM result_list = enif_make_list_from_array(env, results, calls_len);
    enif_free(results);

    return enif_make_tuple2(env, ATOM_OK, result_list);
}

#else /* !HAVE_SUBINTERPRETERS */

/* Stub implementations for older Python versions */
static ERL_NIF_TERM nif_subinterp_worker_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return make_error(env, "subinterpreters_not_supported");
}

static ERL_NIF_TERM nif_subinterp_worker_destroy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return make_error(env, "subinterpreters_not_supported");
}

static ERL_NIF_TERM nif_subinterp_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return make_error(env, "subinterpreters_not_supported");
}

static ERL_NIF_TERM nif_parallel_execute(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return make_error(env, "subinterpreters_not_supported");
}

#endif /* HAVE_SUBINTERPRETERS */

/* ============================================================================
 * Type conversion: Python -> Erlang
 * ============================================================================ */

static ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj) {
    if (obj == Py_None) {
        return ATOM_NONE;
    }

    if (obj == Py_True) {
        return ATOM_TRUE;
    }

    if (obj == Py_False) {
        return ATOM_FALSE;
    }

    if (PyLong_Check(obj)) {
        long long val = PyLong_AsLongLong(obj);
        if (PyErr_Occurred()) {
            PyErr_Clear();
            /* Try as unsigned */
            unsigned long long uval = PyLong_AsUnsignedLongLong(obj);
            if (PyErr_Occurred()) {
                PyErr_Clear();
                /* Fall back to string representation for big integers */
                PyObject *str = PyObject_Str(obj);
                if (str != NULL) {
                    const char *s = PyUnicode_AsUTF8(str);
                    ERL_NIF_TERM result = enif_make_string(env, s, ERL_NIF_LATIN1);
                    Py_DECREF(str);
                    return result;
                }
            }
            return enif_make_uint64(env, uval);
        }
        return enif_make_int64(env, val);
    }

    if (PyFloat_Check(obj)) {
        double val = PyFloat_AsDouble(obj);
        /* Handle special float values */
        if (val != val) {  /* NaN check */
            return ATOM_NAN;
        } else if (val == HUGE_VAL) {
            return ATOM_INFINITY;
        } else if (val == -HUGE_VAL) {
            return ATOM_NEG_INFINITY;
        }
        return enif_make_double(env, val);
    }

    if (PyUnicode_Check(obj)) {
        Py_ssize_t size;
        const char *data = PyUnicode_AsUTF8AndSize(obj, &size);
        if (data == NULL) {
            return ATOM_ERROR;
        }
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, size, &bin);
        memcpy(buf, data, size);
        return bin;
    }

    if (PyBytes_Check(obj)) {
        Py_ssize_t size = PyBytes_Size(obj);
        char *data = PyBytes_AsString(obj);
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, size, &bin);
        memcpy(buf, data, size);
        return bin;
    }

    if (PyList_Check(obj)) {
        Py_ssize_t len = PyList_Size(obj);
        ERL_NIF_TERM *items = enif_alloc(sizeof(ERL_NIF_TERM) * len);
        for (Py_ssize_t i = 0; i < len; i++) {
            PyObject *item = PyList_GetItem(obj, i);  /* Borrowed ref */
            items[i] = py_to_term(env, item);
        }
        ERL_NIF_TERM result = enif_make_list_from_array(env, items, len);
        enif_free(items);
        return result;
    }

    if (PyTuple_Check(obj)) {
        Py_ssize_t len = PyTuple_Size(obj);
        ERL_NIF_TERM *items = enif_alloc(sizeof(ERL_NIF_TERM) * len);
        for (Py_ssize_t i = 0; i < len; i++) {
            PyObject *item = PyTuple_GetItem(obj, i);  /* Borrowed ref */
            items[i] = py_to_term(env, item);
        }
        ERL_NIF_TERM result = enif_make_tuple_from_array(env, items, len);
        enif_free(items);
        return result;
    }

    if (PyDict_Check(obj)) {
        ERL_NIF_TERM map = enif_make_new_map(env);
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(obj, &pos, &key, &value)) {
            ERL_NIF_TERM k = py_to_term(env, key);
            ERL_NIF_TERM v = py_to_term(env, value);
            enif_make_map_put(env, map, k, v, &map);
        }
        return map;
    }

    /* For other objects, convert to string representation */
    PyObject *str = PyObject_Str(obj);
    if (str != NULL) {
        const char *s = PyUnicode_AsUTF8(str);
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, strlen(s), &bin);
        memcpy(buf, s, strlen(s));
        Py_DECREF(str);
        return bin;
    }

    return ATOM_UNDEFINED;
}

/* ============================================================================
 * Type conversion: Erlang -> Python
 * ============================================================================ */

static PyObject *term_to_py(ErlNifEnv *env, ERL_NIF_TERM term) {
    int i_val;
    long l_val;
    ErlNifSInt64 i64_val;
    ErlNifUInt64 u64_val;
    double d_val;
    ErlNifBinary bin;
    unsigned int list_len;
    int arity;
    const ERL_NIF_TERM *tuple;

    /* Check for atoms first */
    if (enif_is_atom(env, term)) {
        char atom_buf[256];
        if (enif_get_atom(env, term, atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
            if (strcmp(atom_buf, "true") == 0) {
                Py_RETURN_TRUE;
            } else if (strcmp(atom_buf, "false") == 0) {
                Py_RETURN_FALSE;
            } else if (strcmp(atom_buf, "nil") == 0 ||
                       strcmp(atom_buf, "none") == 0 ||
                       strcmp(atom_buf, "undefined") == 0) {
                Py_RETURN_NONE;
            } else {
                /* Convert atom to string */
                return PyUnicode_FromString(atom_buf);
            }
        }
    }

    /* Integer */
    if (enif_get_int(env, term, &i_val)) {
        return PyLong_FromLong(i_val);
    }
    if (enif_get_long(env, term, &l_val)) {
        return PyLong_FromLong(l_val);
    }
    if (enif_get_int64(env, term, &i64_val)) {
        return PyLong_FromLongLong(i64_val);
    }
    if (enif_get_uint64(env, term, &u64_val)) {
        return PyLong_FromUnsignedLongLong(u64_val);
    }

    /* Float */
    if (enif_get_double(env, term, &d_val)) {
        return PyFloat_FromDouble(d_val);
    }

    /* Binary/String */
    if (enif_inspect_binary(env, term, &bin)) {
        return PyUnicode_FromStringAndSize((char *)bin.data, bin.size);
    }
    if (enif_inspect_iolist_as_binary(env, term, &bin)) {
        return PyUnicode_FromStringAndSize((char *)bin.data, bin.size);
    }

    /* List */
    if (enif_get_list_length(env, term, &list_len)) {
        PyObject *list = PyList_New(list_len);
        ERL_NIF_TERM head, tail = term;
        for (unsigned int i = 0; i < list_len; i++) {
            enif_get_list_cell(env, tail, &head, &tail);
            PyObject *item = term_to_py(env, head);
            if (item == NULL) {
                Py_DECREF(list);
                return NULL;
            }
            PyList_SET_ITEM(list, i, item);  /* Steals reference */
        }
        return list;
    }

    /* Tuple */
    if (enif_get_tuple(env, term, &arity, &tuple)) {
        PyObject *py_tuple = PyTuple_New(arity);
        for (int i = 0; i < arity; i++) {
            PyObject *item = term_to_py(env, tuple[i]);
            if (item == NULL) {
                Py_DECREF(py_tuple);
                return NULL;
            }
            PyTuple_SET_ITEM(py_tuple, i, item);  /* Steals reference */
        }
        return py_tuple;
    }

    /* Map */
    if (enif_is_map(env, term)) {
        PyObject *dict = PyDict_New();
        ERL_NIF_TERM key, value;
        ErlNifMapIterator iter;

        enif_map_iterator_create(env, term, &iter, ERL_NIF_MAP_ITERATOR_FIRST);
        while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
            PyObject *py_key = term_to_py(env, key);
            PyObject *py_value = term_to_py(env, value);
            if (py_key == NULL || py_value == NULL) {
                Py_XDECREF(py_key);
                Py_XDECREF(py_value);
                Py_DECREF(dict);
                enif_map_iterator_destroy(env, &iter);
                return NULL;
            }
            PyDict_SetItem(dict, py_key, py_value);
            Py_DECREF(py_key);
            Py_DECREF(py_value);
            enif_map_iterator_next(env, &iter);
        }
        enif_map_iterator_destroy(env, &iter);
        return dict;
    }

    /* Check for resource (wrapped Python object) */
    py_object_t *wrapper;
    if (enif_get_resource(env, term, PYOBJ_RESOURCE_TYPE, (void **)&wrapper)) {
        Py_INCREF(wrapper->obj);
        return wrapper->obj;
    }

    /* Unknown type - convert to string */
    Py_RETURN_NONE;
}

/* ============================================================================
 * Error handling
 * ============================================================================ */

static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *reason) {
    return enif_make_tuple2(env, ATOM_ERROR,
        enif_make_atom(env, reason));
}

static ERL_NIF_TERM make_py_error(ErlNifEnv *env) {
    PyObject *type, *value, *traceback;
    PyErr_Fetch(&type, &value, &traceback);

    if (value == NULL) {
        return make_error(env, "unknown_python_error");
    }

    /* Check for StopIteration */
    if (PyErr_GivenExceptionMatches(type, PyExc_StopIteration)) {
        PyErr_Clear();
        Py_XDECREF(type);
        Py_XDECREF(value);
        Py_XDECREF(traceback);
        return enif_make_tuple2(env, ATOM_ERROR,
            enif_make_tuple2(env, ATOM_STOP_ITERATION, ATOM_NONE));
    }

    PyObject *str = PyObject_Str(value);
    const char *err_msg = str ? PyUnicode_AsUTF8(str) : "unknown";

    PyObject *type_name = PyObject_GetAttrString(type, "__name__");
    const char *type_str = type_name ? PyUnicode_AsUTF8(type_name) : "Exception";

    ERL_NIF_TERM error_tuple = enif_make_tuple2(env,
        enif_make_atom(env, type_str),
        enif_make_string(env, err_msg, ERL_NIF_LATIN1));

    Py_XDECREF(str);
    Py_XDECREF(type_name);
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(traceback);

    PyErr_Clear();

    return enif_make_tuple2(env, ATOM_ERROR, error_tuple);
}

/* ============================================================================
 * Erlang callback module for Python
 * ============================================================================ */

/**
 * Python implementation of erlang.call(name, *args)
 *
 * This function allows Python code to call registered Erlang functions.
 * It sends a message to the callback handler and blocks until it receives
 * a response via the callback pipe.
 */
static PyObject *erlang_call_impl(PyObject *self, PyObject *args) {
    if (tl_current_worker == NULL || !tl_current_worker->has_callback_handler) {
        PyErr_SetString(PyExc_RuntimeError, "No callback handler registered");
        return NULL;
    }

    Py_ssize_t nargs = PyTuple_Size(args);
    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "erlang.call requires at least a function name");
        return NULL;
    }

    /* Get function name (first arg) */
    PyObject *name_obj = PyTuple_GetItem(args, 0);
    if (!PyUnicode_Check(name_obj)) {
        PyErr_SetString(PyExc_TypeError, "Function name must be a string");
        return NULL;
    }
    const char *func_name = PyUnicode_AsUTF8(name_obj);
    if (func_name == NULL) {
        return NULL;
    }

    /* Build args list (remaining args) */
    PyObject *call_args = PyTuple_GetSlice(args, 1, nargs);
    if (call_args == NULL) {
        return NULL;
    }

    /* Convert args to Erlang term */
    ErlNifEnv *msg_env = enif_alloc_env();
    ERL_NIF_TERM func_term;
    {
        size_t len = strlen(func_name);
        unsigned char *buf = enif_make_new_binary(msg_env, len, &func_term);
        memcpy(buf, func_name, len);
    }

    ERL_NIF_TERM args_term = py_to_term(msg_env, call_args);
    Py_DECREF(call_args);

    /* Create callback reference */
    uint64_t callback_id = (uint64_t)pthread_self();  /* Use thread ID as callback ID */
    ERL_NIF_TERM id_term = enif_make_uint64(msg_env, callback_id);

    /* Send message: {erlang_callback, CallbackId, FuncName, Args} */
    ERL_NIF_TERM msg = enif_make_tuple4(msg_env,
        ATOM_ERLANG_CALLBACK,
        id_term,
        func_term,
        args_term);

    /* Variables for response */
    uint32_t response_len = 0;
    ssize_t n;
    char *response_data = NULL;

    /* Release GIL before sending and waiting */
    Py_BEGIN_ALLOW_THREADS

    enif_send(NULL, &tl_current_worker->callback_handler, msg_env, msg);
    enif_free_env(msg_env);

    /* Wait for response by reading from pipe */
    /* Response format: 4-byte length + msgpack/term data */
    n = read(tl_current_worker->callback_pipe[0], &response_len, sizeof(response_len));

    Py_END_ALLOW_THREADS

    if (n != sizeof(response_len)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to read callback response length");
        return NULL;
    }

    /* Read response data */
    response_data = enif_alloc(response_len);
    if (response_data == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate response buffer");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    n = read(tl_current_worker->callback_pipe[0], response_data, response_len);
    Py_END_ALLOW_THREADS

    if (n != (ssize_t)response_len) {
        enif_free(response_data);
        PyErr_SetString(PyExc_RuntimeError, "Failed to read callback response data");
        return NULL;
    }

    /* Parse response: first byte is status (0=ok, 1=error), rest is the value */
    uint8_t status = response_data[0];

    /* Decode the binary response into an Erlang term, then to Python */
    /* For simplicity, we use a simple encoding: status byte + string representation */
    /* TODO: Use a more efficient binary encoding */

    if (response_len < 2) {
        enif_free(response_data);
        if (status == 0) {
            Py_RETURN_NONE;
        } else {
            PyErr_SetString(PyExc_RuntimeError, "Erlang callback failed");
            return NULL;
        }
    }

    /* Parse the result as a Python expression (simple approach) */
    /* The response is: status_byte + python_repr_string */
    char *result_str = response_data + 1;
    size_t result_len = response_len - 1;

    PyObject *result = NULL;
    if (status == 0) {
        /* Try to evaluate the result string as Python literal */
        PyObject *ast_module = PyImport_ImportModule("ast");
        if (ast_module != NULL) {
            PyObject *literal_eval = PyObject_GetAttrString(ast_module, "literal_eval");
            if (literal_eval != NULL) {
                PyObject *arg = PyUnicode_FromStringAndSize(result_str, result_len);
                if (arg != NULL) {
                    result = PyObject_CallFunctionObjArgs(literal_eval, arg, NULL);
                    Py_DECREF(arg);
                    if (result == NULL) {
                        /* If literal_eval fails, return as string */
                        PyErr_Clear();
                        result = PyUnicode_FromStringAndSize(result_str, result_len);
                    }
                }
                Py_DECREF(literal_eval);
            }
            Py_DECREF(ast_module);
        }
        if (result == NULL) {
            result = PyUnicode_FromStringAndSize(result_str, result_len);
        }
    } else {
        /* Error case */
        PyErr_SetString(PyExc_RuntimeError, result_str);
    }

    enif_free(response_data);
    return result;
}

/* Python method definitions for erlang module */
static PyMethodDef ErlangModuleMethods[] = {
    {"call", erlang_call_impl, METH_VARARGS,
     "Call a registered Erlang function.\n\n"
     "Usage: erlang.call('func_name', arg1, arg2, ...)\n"
     "Returns: The result from the Erlang function."},
    {NULL, NULL, 0, NULL}
};

/* Module definition */
static struct PyModuleDef ErlangModuleDef = {
    PyModuleDef_HEAD_INIT,
    "erlang",                           /* Module name */
    "Interface for calling Erlang functions from Python.",  /* Docstring */
    -1,                                 /* Size of per-interpreter state (-1 = global) */
    ErlangModuleMethods                 /* Methods */
};

/**
 * Create and register the 'erlang' module in Python.
 * Called during Python initialization.
 */
static int create_erlang_module(void) {
    PyObject *module = PyModule_Create(&ErlangModuleDef);
    if (module == NULL) {
        return -1;
    }

    /* Add module to sys.modules */
    PyObject *sys_modules = PyImport_GetModuleDict();
    if (PyDict_SetItemString(sys_modules, "erlang", module) < 0) {
        Py_DECREF(module);
        return -1;
    }

    return 0;
}

/**
 * NIF: Set callback handler for a worker.
 * Args: WorkerRef, CallbackHandlerPid
 */
static ERL_NIF_TERM nif_set_callback_handler(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }

    if (!enif_get_local_pid(env, argv[1], &worker->callback_handler)) {
        return make_error(env, "invalid_pid");
    }

    /* Create pipe for callback responses */
    if (pipe(worker->callback_pipe) < 0) {
        return make_error(env, "pipe_failed");
    }

    worker->has_callback_handler = true;

    /* Return the write end of the pipe as a file descriptor for Erlang to use */
    return enif_make_tuple2(env, ATOM_OK,
        enif_make_int(env, worker->callback_pipe[1]));
}

/**
 * NIF: Send callback response to a worker.
 * This is called by Erlang after executing the callback.
 * Args: Fd, ResponseBinary
 */
static ERL_NIF_TERM nif_send_callback_response(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int fd;
    ErlNifBinary response;

    if (!enif_get_int(env, argv[0], &fd)) {
        return make_error(env, "invalid_fd");
    }

    if (!enif_inspect_binary(env, argv[1], &response)) {
        return make_error(env, "invalid_response");
    }

    /* Write length then data */
    uint32_t len = (uint32_t)response.size;
    ssize_t n = write(fd, &len, sizeof(len));
    if (n != sizeof(len)) {
        return make_error(env, "write_length_failed");
    }

    n = write(fd, response.data, response.size);
    if (n != (ssize_t)response.size) {
        return make_error(env, "write_data_failed");
    }

    return ATOM_OK;
}

/* ============================================================================
 * Executor thread implementation
 *
 * The executor thread owns the Python GIL and processes all Python operations.
 * This ensures consistent thread state for C extensions with TLS.
 * ============================================================================ */

/**
 * Process a single request in the executor thread (GIL held).
 */
static void process_request(py_request_t *req) {
    ErlNifEnv *env = req->env;
    py_worker_t *worker = req->worker;

    switch (req->type) {
    case PY_REQ_CALL: {
        /* Set thread-local worker context for callbacks */
        tl_current_worker = worker;
        tl_callback_env = env;

        char *module_name = enif_alloc(req->module_bin.size + 1);
        memcpy(module_name, req->module_bin.data, req->module_bin.size);
        module_name[req->module_bin.size] = '\0';

        char *func_name = enif_alloc(req->func_bin.size + 1);
        memcpy(func_name, req->func_bin.data, req->func_bin.size);
        func_name[req->func_bin.size] = '\0';

        PyObject *func = NULL;

        /* Special handling for __main__ - look in worker's namespace */
        if (strcmp(module_name, "__main__") == 0) {
            func = PyDict_GetItemString(worker->locals, func_name);
            if (func == NULL) {
                func = PyDict_GetItemString(worker->globals, func_name);
            }
            if (func != NULL) {
                Py_INCREF(func);
            } else {
                PyErr_Format(PyExc_NameError, "name '%s' is not defined", func_name);
                req->result = make_py_error(env);
                goto call_cleanup;
            }
        } else {
            PyObject *module = PyImport_ImportModule(module_name);
            if (module == NULL) {
                req->result = make_py_error(env);
                goto call_cleanup;
            }
            func = PyObject_GetAttrString(module, func_name);
            Py_DECREF(module);
        }

        if (func == NULL) {
            req->result = make_py_error(env);
            goto call_cleanup;
        }

        /* Convert args list to Python tuple */
        unsigned int args_len;
        if (!enif_get_list_length(env, req->args_term, &args_len)) {
            Py_DECREF(func);
            req->result = make_error(env, "invalid_args");
            goto call_cleanup;
        }

        PyObject *args = PyTuple_New(args_len);
        ERL_NIF_TERM head, tail = req->args_term;
        for (unsigned int i = 0; i < args_len; i++) {
            enif_get_list_cell(env, tail, &head, &tail);
            PyObject *arg = term_to_py(env, head);
            if (arg == NULL) {
                Py_DECREF(args);
                Py_DECREF(func);
                req->result = make_error(env, "arg_conversion_failed");
                goto call_cleanup;
            }
            PyTuple_SET_ITEM(args, i, arg);
        }

        /* Convert kwargs map to Python dict */
        PyObject *kwargs = NULL;
        if (enif_is_map(env, req->kwargs_term)) {
            kwargs = term_to_py(env, req->kwargs_term);
        }

        /* Start timeout if specified */
        start_timeout(req->timeout_ms);

        /* Call the function */
        PyObject *py_result = PyObject_Call(func, args, kwargs);

        /* Stop timeout */
        stop_timeout();

        Py_DECREF(func);
        Py_DECREF(args);
        Py_XDECREF(kwargs);

        if (py_result == NULL) {
            if (check_timeout_error()) {
                PyErr_Clear();
                req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_TIMEOUT);
            } else {
                req->result = make_py_error(env);
            }
        } else if (PyGen_Check(py_result) || PyIter_Check(py_result)) {
            py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
            wrapper->obj = py_result;
            ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
            enif_release_resource(wrapper);
            req->result = enif_make_tuple2(env, ATOM_OK,
                enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, py_result);
            Py_DECREF(py_result);
            req->result = enif_make_tuple2(env, ATOM_OK, term_result);
        }

    call_cleanup:
        tl_current_worker = NULL;
        tl_callback_env = NULL;
        enif_free(module_name);
        enif_free(func_name);
        break;
    }

    case PY_REQ_EVAL: {
        tl_current_worker = worker;
        tl_callback_env = env;

        char *code = enif_alloc(req->code_bin.size + 1);
        memcpy(code, req->code_bin.data, req->code_bin.size);
        code[req->code_bin.size] = '\0';

        /* Update locals if provided */
        if (enif_is_map(env, req->locals_term)) {
            PyObject *new_locals = term_to_py(env, req->locals_term);
            if (new_locals != NULL && PyDict_Check(new_locals)) {
                PyDict_Update(worker->locals, new_locals);
                Py_DECREF(new_locals);
            }
        }

        /* Start timeout if specified */
        start_timeout(req->timeout_ms);

        /* Compile and evaluate */
        PyObject *compiled = Py_CompileString(code, "<erlang>", Py_eval_input);

        if (compiled == NULL) {
            stop_timeout();
            req->result = make_py_error(env);
        } else {
            PyObject *py_result = PyEval_EvalCode(compiled, worker->globals, worker->locals);
            Py_DECREF(compiled);
            stop_timeout();

            if (py_result == NULL) {
                if (check_timeout_error()) {
                    PyErr_Clear();
                    req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_TIMEOUT);
                } else {
                    req->result = make_py_error(env);
                }
            } else if (PyGen_Check(py_result) || PyIter_Check(py_result)) {
                py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
                wrapper->obj = py_result;
                ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
                enif_release_resource(wrapper);
                req->result = enif_make_tuple2(env, ATOM_OK,
                    enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
            } else {
                ERL_NIF_TERM term_result = py_to_term(env, py_result);
                Py_DECREF(py_result);
                req->result = enif_make_tuple2(env, ATOM_OK, term_result);
            }
        }

        tl_current_worker = NULL;
        tl_callback_env = NULL;
        enif_free(code);
        break;
    }

    case PY_REQ_EXEC: {
        tl_current_worker = worker;
        tl_callback_env = env;

        char *code = enif_alloc(req->code_bin.size + 1);
        memcpy(code, req->code_bin.data, req->code_bin.size);
        code[req->code_bin.size] = '\0';

        PyObject *compiled = Py_CompileString(code, "<erlang>", Py_file_input);

        if (compiled == NULL) {
            req->result = make_py_error(env);
        } else {
            PyObject *py_result = PyEval_EvalCode(compiled, worker->globals, worker->locals);
            Py_DECREF(compiled);

            if (py_result == NULL) {
                req->result = make_py_error(env);
            } else {
                Py_DECREF(py_result);
                req->result = ATOM_OK;
            }
        }

        tl_current_worker = NULL;
        tl_callback_env = NULL;
        enif_free(code);
        break;
    }

    case PY_REQ_NEXT: {
        PyObject *item = PyIter_Next(req->gen_wrapper->obj);

        if (item == NULL) {
            if (PyErr_Occurred()) {
                if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
                    PyErr_Clear();
                    req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_STOP_ITERATION);
                } else {
                    req->result = make_py_error(env);
                }
            } else {
                req->result = enif_make_tuple2(env, ATOM_ERROR, ATOM_STOP_ITERATION);
            }
        } else if (PyGen_Check(item) || PyIter_Check(item)) {
            py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
            wrapper->obj = item;
            ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
            enif_release_resource(wrapper);
            req->result = enif_make_tuple2(env, ATOM_OK,
                enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, item);
            Py_DECREF(item);
            req->result = enif_make_tuple2(env, ATOM_OK, term_result);
        }
        break;
    }

    case PY_REQ_IMPORT: {
        char *module_name = enif_alloc(req->module_bin.size + 1);
        memcpy(module_name, req->module_bin.data, req->module_bin.size);
        module_name[req->module_bin.size] = '\0';

        PyObject *module = PyImport_ImportModule(module_name);
        enif_free(module_name);

        if (module == NULL) {
            req->result = make_py_error(env);
        } else {
            py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
            wrapper->obj = module;
            ERL_NIF_TERM mod_ref = enif_make_resource(env, wrapper);
            enif_release_resource(wrapper);
            req->result = enif_make_tuple2(env, ATOM_OK, mod_ref);
        }
        break;
    }

    case PY_REQ_GETATTR: {
        char *attr_name = enif_alloc(req->attr_bin.size + 1);
        memcpy(attr_name, req->attr_bin.data, req->attr_bin.size);
        attr_name[req->attr_bin.size] = '\0';

        PyObject *attr = PyObject_GetAttrString(req->obj_wrapper->obj, attr_name);
        enif_free(attr_name);

        if (attr == NULL) {
            req->result = make_py_error(env);
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, attr);
            Py_DECREF(attr);
            req->result = enif_make_tuple2(env, ATOM_OK, term_result);
        }
        break;
    }

    case PY_REQ_MEMORY_STATS: {
        /* Import gc module */
        PyObject *gc_module = PyImport_ImportModule("gc");
        if (gc_module == NULL) {
            req->result = make_error(env, "gc_import_failed");
            break;
        }

        ERL_NIF_TERM result_map = enif_make_new_map(env);

        PyObject *stats = PyObject_CallMethod(gc_module, "get_stats", NULL);
        if (stats != NULL && PyList_Check(stats)) {
            Py_ssize_t num_gens = PyList_Size(stats);
            ERL_NIF_TERM *gen_stats = enif_alloc(sizeof(ERL_NIF_TERM) * num_gens);
            for (Py_ssize_t i = 0; i < num_gens; i++) {
                PyObject *gen = PyList_GetItem(stats, i);
                gen_stats[i] = py_to_term(env, gen);
            }
            ERL_NIF_TERM gc_stats_list = enif_make_list_from_array(env, gen_stats, num_gens);
            enif_free(gen_stats);
            enif_make_map_put(env, result_map,
                enif_make_atom(env, "gc_stats"), gc_stats_list, &result_map);
            Py_DECREF(stats);
        }

        PyObject *counts = PyObject_CallMethod(gc_module, "get_count", NULL);
        if (counts != NULL && PyTuple_Check(counts)) {
            ERL_NIF_TERM count_term = py_to_term(env, counts);
            enif_make_map_put(env, result_map,
                enif_make_atom(env, "gc_count"), count_term, &result_map);
            Py_DECREF(counts);
        }

        PyObject *threshold = PyObject_CallMethod(gc_module, "get_threshold", NULL);
        if (threshold != NULL && PyTuple_Check(threshold)) {
            ERL_NIF_TERM threshold_term = py_to_term(env, threshold);
            enif_make_map_put(env, result_map,
                enif_make_atom(env, "gc_threshold"), threshold_term, &result_map);
            Py_DECREF(threshold);
        }

        Py_DECREF(gc_module);

        /* Try to get tracemalloc stats if available */
        PyObject *tracemalloc = PyImport_ImportModule("tracemalloc");
        if (tracemalloc != NULL) {
            PyObject *is_tracing = PyObject_CallMethod(tracemalloc, "is_tracing", NULL);
            if (is_tracing != NULL && PyObject_IsTrue(is_tracing)) {
                PyObject *current_traced = PyObject_CallMethod(tracemalloc, "get_traced_memory", NULL);
                if (current_traced != NULL && PyTuple_Check(current_traced)) {
                    ERL_NIF_TERM current = py_to_term(env, PyTuple_GetItem(current_traced, 0));
                    ERL_NIF_TERM peak = py_to_term(env, PyTuple_GetItem(current_traced, 1));
                    enif_make_map_put(env, result_map,
                        enif_make_atom(env, "traced_memory_current"), current, &result_map);
                    enif_make_map_put(env, result_map,
                        enif_make_atom(env, "traced_memory_peak"), peak, &result_map);
                    Py_DECREF(current_traced);
                }
            }
            Py_XDECREF(is_tracing);
            Py_DECREF(tracemalloc);
        }
        PyErr_Clear();

        req->result = enif_make_tuple2(env, ATOM_OK, result_map);
        break;
    }

    case PY_REQ_GC: {
        PyObject *gc_module = PyImport_ImportModule("gc");
        if (gc_module == NULL) {
            req->result = make_error(env, "gc_import_failed");
            break;
        }

        PyObject *result = PyObject_CallMethod(gc_module, "collect", "i", req->gc_generation);
        Py_DECREF(gc_module);

        if (result == NULL) {
            req->result = make_py_error(env);
        } else {
            long collected = PyLong_AsLong(result);
            Py_DECREF(result);
            req->result = enif_make_tuple2(env, ATOM_OK, enif_make_long(env, collected));
        }
        break;
    }

    case PY_REQ_SHUTDOWN:
        /* Signal to exit the loop - nothing to do here */
        break;
    }
}

/**
 * Main function for the executor thread.
 * Acquires GIL and processes requests until shutdown.
 */
static void *executor_thread_main(void *arg) {
    (void)arg;

    /* Acquire GIL for this thread */
    PyGILState_STATE gstate = PyGILState_Ensure();

    g_executor_running = true;

    /*
     * Main processing loop.
     * We continue processing until we receive a PY_REQ_SHUTDOWN request.
     * The shutdown flag is used to stop waiting when the queue is empty.
     */
    bool should_exit = false;
    while (!should_exit) {
        py_request_t *req = NULL;

        /* Release GIL while waiting for work (like PyO3 allow_threads) */
        Py_BEGIN_ALLOW_THREADS

        pthread_mutex_lock(&g_executor_mutex);
        while (g_executor_queue_head == NULL && !g_executor_shutdown) {
            pthread_cond_wait(&g_executor_cond, &g_executor_mutex);
        }

        /* Dequeue request if available */
        if (g_executor_queue_head != NULL) {
            req = g_executor_queue_head;
            g_executor_queue_head = req->next;
            if (g_executor_queue_head == NULL) {
                g_executor_queue_tail = NULL;
            }
            req->next = NULL;
        } else if (g_executor_shutdown) {
            /* Queue is empty and shutdown requested - exit */
            should_exit = true;
        }
        pthread_mutex_unlock(&g_executor_mutex);

        Py_END_ALLOW_THREADS

        if (req != NULL) {
            if (req->type == PY_REQ_SHUTDOWN) {
                /* Signal completion and exit */
                pthread_mutex_lock(&req->mutex);
                req->completed = true;
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
                should_exit = true;
            } else {
                /* Process the request with GIL held */
                process_request(req);

                /* Signal completion */
                pthread_mutex_lock(&req->mutex);
                req->completed = true;
                pthread_cond_signal(&req->cond);
                pthread_mutex_unlock(&req->mutex);
            }
        }
    }

    g_executor_running = false;
    PyGILState_Release(gstate);

    return NULL;
}

/**
 * Enqueue a request to the executor thread.
 */
static void executor_enqueue(py_request_t *req) {
    pthread_mutex_lock(&g_executor_mutex);
    req->next = NULL;
    if (g_executor_queue_tail == NULL) {
        g_executor_queue_head = req;
        g_executor_queue_tail = req;
    } else {
        g_executor_queue_tail->next = req;
        g_executor_queue_tail = req;
    }
    pthread_cond_signal(&g_executor_cond);
    pthread_mutex_unlock(&g_executor_mutex);
}

/**
 * Wait for a request to complete.
 */
static void executor_wait(py_request_t *req) {
    pthread_mutex_lock(&req->mutex);
    while (!req->completed) {
        pthread_cond_wait(&req->cond, &req->mutex);
    }
    pthread_mutex_unlock(&req->mutex);
}

/**
 * Initialize a request structure.
 */
static void request_init(py_request_t *req) {
    memset(req, 0, sizeof(py_request_t));
    pthread_mutex_init(&req->mutex, NULL);
    pthread_cond_init(&req->cond, NULL);
    req->completed = false;
}

/**
 * Clean up a request structure.
 */
static void request_cleanup(py_request_t *req) {
    pthread_mutex_destroy(&req->mutex);
    pthread_cond_destroy(&req->cond);
}

/**
 * Start the executor thread.
 * Called during Python initialization.
 */
static int executor_start(void) {
    g_executor_shutdown = false;
    g_executor_queue_head = NULL;
    g_executor_queue_tail = NULL;

    if (pthread_create(&g_executor_thread, NULL, executor_thread_main, NULL) != 0) {
        return -1;
    }

    /* Wait for executor to be ready */
    int max_wait = 100;  /* 1 second max */
    while (!g_executor_running && max_wait-- > 0) {
        usleep(10000);  /* 10ms */
    }

    return g_executor_running ? 0 : -1;
}

/**
 * Stop the executor thread.
 * Called during Python finalization.
 */
static void executor_stop(void) {
    if (!g_executor_running) {
        return;
    }

    /* Send shutdown request */
    py_request_t shutdown_req;
    request_init(&shutdown_req);
    shutdown_req.type = PY_REQ_SHUTDOWN;

    g_executor_shutdown = true;
    executor_enqueue(&shutdown_req);
    executor_wait(&shutdown_req);
    request_cleanup(&shutdown_req);

    /* Wait for thread to finish */
    pthread_join(g_executor_thread, NULL);
}

/* ============================================================================
 * NIF setup
 * ============================================================================ */

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info) {
    /* Create resource types */
    WORKER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_worker", worker_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    PYOBJ_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_object", pyobj_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    ASYNC_WORKER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_async_worker", async_worker_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

#ifdef HAVE_SUBINTERPRETERS
    SUBINTERP_WORKER_RESOURCE_TYPE = enif_open_resource_type(
        env, NULL, "py_subinterp_worker", subinterp_worker_destructor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if (WORKER_RESOURCE_TYPE == NULL || PYOBJ_RESOURCE_TYPE == NULL ||
        ASYNC_WORKER_RESOURCE_TYPE == NULL || SUBINTERP_WORKER_RESOURCE_TYPE == NULL) {
        return -1;
    }
#else
    if (WORKER_RESOURCE_TYPE == NULL || PYOBJ_RESOURCE_TYPE == NULL ||
        ASYNC_WORKER_RESOURCE_TYPE == NULL) {
        return -1;
    }
#endif

    /* Initialize atoms */
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_NONE = enif_make_atom(env, "none");
    ATOM_UNDEFINED = enif_make_atom(env, "undefined");
    ATOM_NIF_NOT_LOADED = enif_make_atom(env, "nif_not_loaded");
    ATOM_GENERATOR = enif_make_atom(env, "generator");
    ATOM_STOP_ITERATION = enif_make_atom(env, "stop_iteration");
    ATOM_TIMEOUT = enif_make_atom(env, "timeout");
    ATOM_NAN = enif_make_atom(env, "nan");
    ATOM_INFINITY = enif_make_atom(env, "infinity");
    ATOM_NEG_INFINITY = enif_make_atom(env, "neg_infinity");
    ATOM_ERLANG_CALLBACK = enif_make_atom(env, "erlang_callback");
    ATOM_ASYNC_RESULT = enif_make_atom(env, "async_result");
    ATOM_ASYNC_ERROR = enif_make_atom(env, "async_error");

    return 0;
}

static int upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data,
                   ERL_NIF_TERM load_info) {
    return load(env, priv_data, load_info);
}

static void unload(ErlNifEnv *env, void *priv_data) {
    /* Cleanup handled by finalize */
}

static ErlNifFunc nif_funcs[] = {
    /* Initialization */
    {"init", 0, nif_py_init, 0},
    {"init", 1, nif_py_init, 0},
    {"finalize", 0, nif_finalize, 0},

    /* Worker management */
    {"worker_new", 0, nif_worker_new, 0},
    {"worker_new", 1, nif_worker_new, 0},
    {"worker_destroy", 1, nif_worker_destroy, 0},

    /* Python execution - dirty I/O NIFs */
    {"worker_call", 5, nif_worker_call, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_call", 6, nif_worker_call, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_eval", 3, nif_worker_eval, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_eval", 4, nif_worker_eval, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_exec", 2, nif_worker_exec, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"worker_next", 2, nif_worker_next, ERL_NIF_DIRTY_JOB_IO_BOUND},

    /* Module operations */
    {"import_module", 2, nif_import_module, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"get_attr", 3, nif_get_attr, ERL_NIF_DIRTY_JOB_IO_BOUND},

    /* Info */
    {"version", 0, nif_version, 0},

    /* Memory and GC */
    {"memory_stats", 0, nif_memory_stats, 0},
    {"gc", 0, nif_gc, 0},
    {"gc", 1, nif_gc, 0},
    {"tracemalloc_start", 0, nif_tracemalloc_start, 0},
    {"tracemalloc_start", 1, nif_tracemalloc_start, 0},
    {"tracemalloc_stop", 0, nif_tracemalloc_stop, 0},

    /* Callback support */
    {"set_callback_handler", 2, nif_set_callback_handler, 0},
    {"send_callback_response", 2, nif_send_callback_response, 0},

    /* Async worker management */
    {"async_worker_new", 0, nif_async_worker_new, 0},
    {"async_worker_destroy", 1, nif_async_worker_destroy, 0},

    /* Async execution - dirty I/O NIFs */
    {"async_call", 6, nif_async_call, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"async_gather", 3, nif_async_gather, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"async_stream", 6, nif_async_stream, ERL_NIF_DIRTY_JOB_IO_BOUND},

    /* Sub-interpreter support */
    {"subinterp_supported", 0, nif_subinterp_supported, 0},
    {"subinterp_worker_new", 0, nif_subinterp_worker_new, 0},
    {"subinterp_worker_destroy", 1, nif_subinterp_worker_destroy, 0},
    {"subinterp_call", 5, nif_subinterp_call, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"parallel_execute", 2, nif_parallel_execute, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(py_nif, nif_funcs, load, NULL, upgrade, unload)
