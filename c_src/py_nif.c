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
} py_worker_t;

static ErlNifResourceType *WORKER_RESOURCE_TYPE = NULL;
static ErlNifResourceType *PYOBJ_RESOURCE_TYPE = NULL;

/* Python object wrapper for preventing GC */
typedef struct {
    PyObject *obj;
} py_object_t;

/* Global state */
static bool g_python_initialized = false;
static PyThreadState *g_main_thread_state = NULL;

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

/* ============================================================================
 * Forward declarations
 * ============================================================================ */

static ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj);
static PyObject *term_to_py(ErlNifEnv *env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *reason);
static ERL_NIF_TERM make_py_error(ErlNifEnv *env);

/* ============================================================================
 * Resource callbacks
 * ============================================================================ */

static void worker_destructor(ErlNifEnv *env, void *obj) {
    py_worker_t *worker = (py_worker_t *)obj;

    if (worker->thread_state != NULL) {
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

    /* Save main thread state and release GIL for other threads */
    g_main_thread_state = PyEval_SaveThread();

    return ATOM_OK;
}

static ERL_NIF_TERM nif_finalize(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return ATOM_OK;
    }

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

    worker->owns_gil = false;

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
    ErlNifBinary module_bin, func_bin;
    unsigned long timeout_ms = 0;  /* 0 = no timeout */

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }
    if (!enif_inspect_binary(env, argv[2], &func_bin)) {
        return make_error(env, "invalid_func");
    }

    /* Get timeout from argv[5] if provided (6-arity call) */
    if (argc > 5) {
        enif_get_ulong(env, argv[5], &timeout_ms);
    }

    /* Acquire GIL */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Convert module name to C string */
    char *module_name = enif_alloc(module_bin.size + 1);
    memcpy(module_name, module_bin.data, module_bin.size);
    module_name[module_bin.size] = '\0';

    char *func_name = enif_alloc(func_bin.size + 1);
    memcpy(func_name, func_bin.data, func_bin.size);
    func_name[func_bin.size] = '\0';

    ERL_NIF_TERM result;
    PyObject *func = NULL;

    /* Special handling for __main__ - look in worker's namespace */
    if (strcmp(module_name, "__main__") == 0) {
        /* First check locals, then globals */
        func = PyDict_GetItemString(worker->locals, func_name);
        if (func == NULL) {
            func = PyDict_GetItemString(worker->globals, func_name);
        }
        if (func != NULL) {
            Py_INCREF(func);  /* GetItemString returns borrowed ref */
        } else {
            PyErr_Format(PyExc_NameError, "name '%s' is not defined", func_name);
            result = make_py_error(env);
            goto cleanup;
        }
    } else {
        /* Import module */
        PyObject *module = PyImport_ImportModule(module_name);
        if (module == NULL) {
            result = make_py_error(env);
            goto cleanup;
        }

        /* Get function */
        func = PyObject_GetAttrString(module, func_name);
        Py_DECREF(module);
    }

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
        PyTuple_SET_ITEM(args, i, arg);  /* Steals reference */
    }

    /* Convert kwargs map to Python dict */
    PyObject *kwargs = NULL;
    if (argc > 4 && enif_is_map(env, argv[4])) {
        kwargs = term_to_py(env, argv[4]);
    }

    /* Start timeout if specified */
    start_timeout(timeout_ms);

    /* Call the function */
    PyObject *py_result = PyObject_Call(func, args, kwargs);

    /* Stop timeout */
    stop_timeout();

    Py_DECREF(func);
    Py_DECREF(args);
    Py_XDECREF(kwargs);

    if (py_result == NULL) {
        /* Check for timeout */
        if (check_timeout_error()) {
            PyErr_Clear();
            result = enif_make_tuple2(env, ATOM_ERROR, ATOM_TIMEOUT);
        } else {
            result = make_py_error(env);
        }
        goto cleanup;
    }

    /* Check if result is a generator */
    if (PyGen_Check(py_result) || PyIter_Check(py_result)) {
        /* Wrap generator in a resource so it persists */
        py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
        wrapper->obj = py_result;  /* Takes ownership */
        ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
        enif_release_resource(wrapper);

        result = enif_make_tuple2(env, ATOM_OK,
            enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
    } else {
        /* Convert result to Erlang term */
        ERL_NIF_TERM term_result = py_to_term(env, py_result);
        Py_DECREF(py_result);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

cleanup:
    enif_free(module_name);
    enif_free(func_name);
    PyGILState_Release(gstate);

    return result;
}

/**
 * Evaluate a Python expression.
 * Args: WorkerRef, Code (binary), Locals (map), [Timeout (ms)]
 */
static ERL_NIF_TERM nif_worker_eval(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;
    ErlNifBinary code_bin;
    unsigned long timeout_ms = 0;  /* 0 = no timeout */

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Convert code to C string */
    char *code = enif_alloc(code_bin.size + 1);
    memcpy(code, code_bin.data, code_bin.size);
    code[code_bin.size] = '\0';

    /* Update locals from argv[2] if provided */
    if (argc > 2 && enif_is_map(env, argv[2])) {
        PyObject *new_locals = term_to_py(env, argv[2]);
        if (new_locals != NULL && PyDict_Check(new_locals)) {
            PyDict_Update(worker->locals, new_locals);
            Py_DECREF(new_locals);
        }
    }

    /* Get timeout from argv[3] if provided */
    if (argc > 3) {
        enif_get_ulong(env, argv[3], &timeout_ms);
    }

    /* Start timeout if specified */
    start_timeout(timeout_ms);

    /* Compile and evaluate */
    PyObject *compiled = Py_CompileString(code, "<erlang>", Py_eval_input);
    ERL_NIF_TERM result;

    if (compiled == NULL) {
        stop_timeout();
        result = make_py_error(env);
    } else {
        PyObject *py_result = PyEval_EvalCode(compiled, worker->globals, worker->locals);
        Py_DECREF(compiled);

        stop_timeout();

        if (py_result == NULL) {
            /* Check for timeout */
            if (check_timeout_error()) {
                PyErr_Clear();
                result = enif_make_tuple2(env, ATOM_ERROR, ATOM_TIMEOUT);
            } else {
                result = make_py_error(env);
            }
        } else {
            /* Check if result is a generator */
            if (PyGen_Check(py_result) || PyIter_Check(py_result)) {
                /* Wrap generator in a resource so it persists */
                py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
                wrapper->obj = py_result;  /* Takes ownership */
                ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
                enif_release_resource(wrapper);

                result = enif_make_tuple2(env, ATOM_OK,
                    enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
            } else {
                ERL_NIF_TERM term_result = py_to_term(env, py_result);
                Py_DECREF(py_result);
                result = enif_make_tuple2(env, ATOM_OK, term_result);
            }
        }
    }

    enif_free(code);
    PyGILState_Release(gstate);

    return result;
}

/**
 * Execute Python statements.
 */
static ERL_NIF_TERM nif_worker_exec(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;
    ErlNifBinary code_bin;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_inspect_binary(env, argv[1], &code_bin)) {
        return make_error(env, "invalid_code");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    char *code = enif_alloc(code_bin.size + 1);
    memcpy(code, code_bin.data, code_bin.size);
    code[code_bin.size] = '\0';

    PyObject *compiled = Py_CompileString(code, "<erlang>", Py_file_input);
    ERL_NIF_TERM result;

    if (compiled == NULL) {
        result = make_py_error(env);
    } else {
        PyObject *py_result = PyEval_EvalCode(compiled, worker->globals, worker->locals);
        Py_DECREF(compiled);

        if (py_result == NULL) {
            result = make_py_error(env);
        } else {
            Py_DECREF(py_result);
            result = ATOM_OK;
        }
    }

    enif_free(code);
    PyGILState_Release(gstate);

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

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *item = PyIter_Next(gen_wrapper->obj);
    ERL_NIF_TERM result;

    if (item == NULL) {
        if (PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
                PyErr_Clear();
                result = enif_make_tuple2(env, ATOM_ERROR, ATOM_STOP_ITERATION);
            } else {
                result = make_py_error(env);
            }
        } else {
            /* No error, just end of iteration */
            result = enif_make_tuple2(env, ATOM_ERROR, ATOM_STOP_ITERATION);
        }
    } else {
        /* Check if item is itself a generator */
        if (PyGen_Check(item) || PyIter_Check(item)) {
            py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
            wrapper->obj = item;
            ERL_NIF_TERM gen_ref = enif_make_resource(env, wrapper);
            enif_release_resource(wrapper);
            result = enif_make_tuple2(env, ATOM_OK,
                enif_make_tuple2(env, ATOM_GENERATOR, gen_ref));
        } else {
            ERL_NIF_TERM term_result = py_to_term(env, item);
            Py_DECREF(item);
            result = enif_make_tuple2(env, ATOM_OK, term_result);
        }
    }

    PyGILState_Release(gstate);
    return result;
}

/**
 * Import a Python module.
 */
static ERL_NIF_TERM nif_import_module(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;
    ErlNifBinary module_bin;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_inspect_binary(env, argv[1], &module_bin)) {
        return make_error(env, "invalid_module");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    char *module_name = enif_alloc(module_bin.size + 1);
    memcpy(module_name, module_bin.data, module_bin.size);
    module_name[module_bin.size] = '\0';

    PyObject *module = PyImport_ImportModule(module_name);
    enif_free(module_name);

    ERL_NIF_TERM result;
    if (module == NULL) {
        result = make_py_error(env);
    } else {
        py_object_t *wrapper = enif_alloc_resource(PYOBJ_RESOURCE_TYPE, sizeof(py_object_t));
        wrapper->obj = module;
        ERL_NIF_TERM mod_ref = enif_make_resource(env, wrapper);
        enif_release_resource(wrapper);
        result = enif_make_tuple2(env, ATOM_OK, mod_ref);
    }

    PyGILState_Release(gstate);
    return result;
}

/**
 * Get attribute from Python object.
 */
static ERL_NIF_TERM nif_get_attr(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    py_worker_t *worker;
    py_object_t *obj_wrapper;
    ErlNifBinary attr_bin;

    if (!enif_get_resource(env, argv[0], WORKER_RESOURCE_TYPE, (void **)&worker)) {
        return make_error(env, "invalid_worker");
    }
    if (!enif_get_resource(env, argv[1], PYOBJ_RESOURCE_TYPE, (void **)&obj_wrapper)) {
        return make_error(env, "invalid_object");
    }
    if (!enif_inspect_binary(env, argv[2], &attr_bin)) {
        return make_error(env, "invalid_attr");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    char *attr_name = enif_alloc(attr_bin.size + 1);
    memcpy(attr_name, attr_bin.data, attr_bin.size);
    attr_name[attr_bin.size] = '\0';

    PyObject *attr = PyObject_GetAttrString(obj_wrapper->obj, attr_name);
    enif_free(attr_name);

    ERL_NIF_TERM result;
    if (attr == NULL) {
        result = make_py_error(env);
    } else {
        ERL_NIF_TERM term_result = py_to_term(env, attr);
        Py_DECREF(attr);
        result = enif_make_tuple2(env, ATOM_OK, term_result);
    }

    PyGILState_Release(gstate);
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

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Import gc module */
    PyObject *gc_module = PyImport_ImportModule("gc");
    if (gc_module == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "gc_import_failed");
    }

    /* Get gc.get_stats() */
    PyObject *stats = PyObject_CallMethod(gc_module, "get_stats", NULL);

    ERL_NIF_TERM result_map = enif_make_new_map(env);

    if (stats != NULL && PyList_Check(stats)) {
        /* gc.get_stats() returns a list of dicts for each generation */
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

    /* Get gc.get_count() */
    PyObject *counts = PyObject_CallMethod(gc_module, "get_count", NULL);
    if (counts != NULL && PyTuple_Check(counts)) {
        ERL_NIF_TERM count_term = py_to_term(env, counts);
        enif_make_map_put(env, result_map,
            enif_make_atom(env, "gc_count"), count_term, &result_map);
        Py_DECREF(counts);
    }

    /* Get gc.get_threshold() */
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
        /* Check if tracing is enabled */
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
    PyErr_Clear();  /* Clear any import errors */

    PyGILState_Release(gstate);

    return enif_make_tuple2(env, ATOM_OK, result_map);
}

/**
 * Force Python garbage collection.
 * Returns the number of unreachable objects collected.
 */
static ERL_NIF_TERM nif_gc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    if (!g_python_initialized) {
        return make_error(env, "python_not_initialized");
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Import gc module and call gc.collect() */
    PyObject *gc_module = PyImport_ImportModule("gc");
    if (gc_module == NULL) {
        PyGILState_Release(gstate);
        return make_error(env, "gc_import_failed");
    }

    /* Optional generation argument */
    int generation = 2;  /* Full collection by default */
    if (argc > 0) {
        enif_get_int(env, argv[0], &generation);
    }

    PyObject *result = PyObject_CallMethod(gc_module, "collect", "i", generation);
    Py_DECREF(gc_module);

    ERL_NIF_TERM ret;
    if (result == NULL) {
        ret = make_py_error(env);
    } else {
        long collected = PyLong_AsLong(result);
        Py_DECREF(result);
        ret = enif_make_tuple2(env, ATOM_OK, enif_make_long(env, collected));
    }

    PyGILState_Release(gstate);
    return ret;
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

    if (WORKER_RESOURCE_TYPE == NULL || PYOBJ_RESOURCE_TYPE == NULL) {
        return -1;
    }

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
    {"tracemalloc_stop", 0, nif_tracemalloc_stop, 0}
};

ERL_NIF_INIT(py_nif, nif_funcs, load, NULL, upgrade, unload)
