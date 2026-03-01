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
 * @file py_convert.c
 * @brief Bidirectional type conversion between Python and Erlang
 * @author Benoit Chesneau
 *
 * @ingroup convert
 *
 * This module provides functions for converting data between Python objects
 * and Erlang terms. It handles the complete type mapping in both directions,
 * including special cases like NumPy arrays and IEEE floating point specials.
 *
 * @par Conversion Strategy
 *
 * The conversion follows these principles:
 * - Preserve semantics where possible (Python dict → Erlang map)
 * - Use binaries for strings (UTF-8 encoded)
 * - Handle Python's rich numeric types gracefully
 * - Fall back to string representation for unknown types
 *
 * @par Memory Management
 *
 * - `py_to_term()`: Does NOT consume Python references
 * - `term_to_py()`: Caller MUST Py_DECREF returned objects
 * - Uses enif_alloc/enif_free for temporary buffers
 *
 * @par Thread Safety
 *
 * All functions require the GIL to be held by the calling thread.
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* Stack allocation threshold for small tuples/maps to avoid heap allocation */
#define SMALL_CONTAINER_THRESHOLD 16

/* ============================================================================
 * Python to Erlang Conversion
 * ============================================================================ */

/**
 * @brief Check if object is a numpy ndarray
 *
 * Uses cached type when available (main interpreter), falls back to
 * attribute detection for subinterpreters where the cached type is invalid.
 *
 * @param obj Python object to check
 * @return true if obj is a numpy ndarray, false otherwise
 */
static inline bool is_numpy_ndarray(PyObject *obj) {
    /* Use cached type for fast isinstance check when available.
     * The cache is only valid in the main interpreter - subinterpreters
     * have their own object space, so we fall back to attribute detection. */
    if (g_numpy_ndarray_type != NULL && g_execution_mode != PY_MODE_SUBINTERP) {
        return PyObject_IsInstance(obj, g_numpy_ndarray_type) == 1;
    }

    /* Fallback: duck typing via attribute detection.
     * Check for both 'tolist' method and 'ndim' attribute. */
    return PyObject_HasAttrString(obj, "tolist") &&
           PyObject_HasAttrString(obj, "ndim");
}

/**
 * @brief Convert a Python object to an Erlang term
 *
 * Recursively converts Python objects to their Erlang equivalents.
 * The function handles all common Python types and provides fallback
 * behavior for custom objects.
 *
 * @par Type Mapping
 *
 * | Python Type       | Erlang Type           | Notes                    |
 * |-------------------|-----------------------|--------------------------|
 * | `None`            | `none` atom           |                          |
 * | `True`            | `true` atom           |                          |
 * | `False`           | `false` atom          |                          |
 * | `int`             | integer               | Falls back to string for big ints |
 * | `float`           | float                 | NaN/Inf become atoms     |
 * | `str`             | binary                | UTF-8 encoded            |
 * | `bytes`           | binary                | Raw bytes                |
 * | `list`            | list                  | Recursive conversion     |
 * | `tuple`           | tuple                 | Recursive conversion     |
 * | `dict`            | map                   | Recursive conversion     |
 * | `numpy.ndarray`   | nested list           | Via `.tolist()` method   |
 * | Other             | binary                | String representation    |
 *
 * @par Special Float Handling
 *
 * IEEE 754 special values are converted to atoms:
 * - NaN → `nan`
 * - +Inf → `infinity`
 * - -Inf → `neg_infinity`
 *
 * @param env  NIF environment for term allocation
 * @param obj  Python object to convert (borrowed reference, not consumed)
 *
 * @return Erlang term representing the Python object
 *
 * @retval ATOM_ERROR  On allocation failure
 * @retval ATOM_UNDEFINED  On conversion failure
 *
 * @pre GIL must be held
 * @pre env must be valid
 * @pre obj must be a valid Python object (may be Py_None)
 *
 * @note Does NOT consume a reference to obj
 * @note Recursively converts container types
 * @note Type checks are ordered by expected frequency for web workloads
 *
 * @see term_to_py() for the reverse conversion
 */
static ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj) {
    /*
     * Type check ordering optimized for web/ASGI workloads:
     * 1. Strings (most common in HTTP headers, bodies, JSON)
     * 2. Dicts (headers, JSON objects, scope)
     * 3. Integers (status codes, content-length)
     * 4. Lists (header lists, JSON arrays)
     * 5. Booleans, None
     * 6. Floats, bytes, tuples
     * 7. NumPy arrays, fallback
     */

    /* Handle Unicode strings - most common in web workloads */
    if (PyUnicode_Check(obj)) {
        Py_ssize_t size;
        const char *data = PyUnicode_AsUTF8AndSize(obj, &size);
        if (data == NULL) {
            return ATOM_ERROR;
        }
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, size, &bin);
        if (buf == NULL) {
            return ATOM_ERROR;
        }
        memcpy(buf, data, size);
        return bin;
    }

    /* Handle dictionaries - very common (headers, JSON, scope) */
    if (PyDict_Check(obj)) {
        Py_ssize_t size = PyDict_Size(obj);
        if (size == 0) {
            return enif_make_new_map(env);
        }

        /* Use stack allocation for small maps, heap for large */
        ERL_NIF_TERM stack_keys[SMALL_CONTAINER_THRESHOLD];
        ERL_NIF_TERM stack_values[SMALL_CONTAINER_THRESHOLD];
        ERL_NIF_TERM *keys = stack_keys;
        ERL_NIF_TERM *values = stack_values;

        if (size > SMALL_CONTAINER_THRESHOLD) {
            keys = enif_alloc(sizeof(ERL_NIF_TERM) * size);
            values = enif_alloc(sizeof(ERL_NIF_TERM) * size);
            if (keys == NULL || values == NULL) {
                if (keys != stack_keys) enif_free(keys);
                if (values != stack_values) enif_free(values);
                return ATOM_ERROR;
            }
        }

        /* Collect all key-value pairs */
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        Py_ssize_t i = 0;
        while (PyDict_Next(obj, &pos, &key, &value)) {
            keys[i] = py_to_term(env, key);
            values[i] = py_to_term(env, value);
            i++;
        }

        /* Build map from arrays - more efficient than iterative puts */
        ERL_NIF_TERM map;
        int result = enif_make_map_from_arrays(env, keys, values, size, &map);

        if (keys != stack_keys) enif_free(keys);
        if (values != stack_values) enif_free(values);

        if (!result) {
            return ATOM_ERROR;
        }
        return map;
    }

    /* Handle integers - use overflow-aware API for efficiency */
    if (PyLong_Check(obj)) {
        /* Check for boolean first (bool subclasses int in Python) */
        if (obj == Py_True) {
            return ATOM_TRUE;
        }
        if (obj == Py_False) {
            return ATOM_FALSE;
        }

        int overflow;
        long long val = PyLong_AsLongLongAndOverflow(obj, &overflow);

        if (overflow == 0) {
            /* Fits in signed 64-bit */
            return enif_make_int64(env, val);
        } else if (overflow == 1) {
            /* Positive overflow - try unsigned */
            unsigned long long uval = PyLong_AsUnsignedLongLong(obj);
            if (!PyErr_Occurred()) {
                return enif_make_uint64(env, uval);
            }
            PyErr_Clear();
        }
        /* overflow == -1 or unsigned also overflowed: fall back to string */
        PyObject *str = PyObject_Str(obj);
        if (str != NULL) {
            Py_ssize_t len;
            const char *s = PyUnicode_AsUTF8AndSize(str, &len);
            if (s != NULL) {
                ERL_NIF_TERM result = enif_make_string_len(env, s, len, ERL_NIF_LATIN1);
                Py_DECREF(str);
                return result;
            }
            Py_DECREF(str);
        }
        PyErr_Clear();
        return ATOM_ERROR;
    }

    /*
     * Handle lists - build directly without temporary array allocation.
     * We iterate in reverse and prepend each element, which is O(1) per element
     * and avoids the overhead of allocating/freeing a temporary array.
     */
    if (PyList_Check(obj)) {
        Py_ssize_t len = PyList_Size(obj);
        ERL_NIF_TERM list = enif_make_list(env, 0);  /* Start with empty list */
        for (Py_ssize_t i = len - 1; i >= 0; i--) {
            PyObject *item = PyList_GetItem(obj, i);  /* Borrowed ref */
            ERL_NIF_TERM term = py_to_term(env, item);
            list = enif_make_list_cell(env, term, list);
        }
        return list;
    }

    /* Handle None singleton */
    if (obj == Py_None) {
        return ATOM_NONE;
    }

    /* Handle floats with special value handling */
    if (PyFloat_Check(obj)) {
        double val = PyFloat_AsDouble(obj);
        /* Check for NaN (NaN != NaN is always true) */
        if (val != val) {
            return ATOM_NAN;
        } else if (val == HUGE_VAL) {
            return ATOM_INFINITY;
        } else if (val == -HUGE_VAL) {
            return ATOM_NEG_INFINITY;
        }
        return enif_make_double(env, val);
    }

    /* Handle byte strings */
    if (PyBytes_Check(obj)) {
        Py_ssize_t size = PyBytes_Size(obj);
        char *data = PyBytes_AsString(obj);
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, size, &bin);
        if (buf == NULL) {
            return ATOM_ERROR;
        }
        memcpy(buf, data, size);
        return bin;
    }

    /* Handle tuples - use stack allocation for small tuples */
    if (PyTuple_Check(obj)) {
        Py_ssize_t len = PyTuple_Size(obj);
        if (len == 0) {
            return enif_make_tuple(env, 0);
        }

        ERL_NIF_TERM stack_items[SMALL_CONTAINER_THRESHOLD];
        ERL_NIF_TERM *items = stack_items;

        if (len > SMALL_CONTAINER_THRESHOLD) {
            items = enif_alloc(sizeof(ERL_NIF_TERM) * len);
            if (items == NULL) {
                return ATOM_ERROR;
            }
        }

        for (Py_ssize_t i = 0; i < len; i++) {
            PyObject *item = PyTuple_GetItem(obj, i);  /* Borrowed ref */
            items[i] = py_to_term(env, item);
        }

        ERL_NIF_TERM result = enif_make_tuple_from_array(env, items, len);

        if (items != stack_items) {
            enif_free(items);
        }
        return result;
    }

    /* Handle ErlangPid → Erlang PID */
    if (Py_IS_TYPE(obj, &ErlangPidType)) {
        ErlangPidObject *pid_obj = (ErlangPidObject *)obj;
        return enif_make_pid(env, &pid_obj->pid);
    }

    /* Handle NumPy arrays by converting to Python list first */
    if (is_numpy_ndarray(obj)) {
        PyObject *tolist = PyObject_CallMethod(obj, "tolist", NULL);
        if (tolist != NULL) {
            ERL_NIF_TERM result = py_to_term(env, tolist);
            Py_DECREF(tolist);
            return result;
        }
        PyErr_Clear();
    }

    /*
     * Fallback: convert any other object to its string representation.
     * This handles custom classes, functions, modules, etc.
     */
    PyObject *str = PyObject_Str(obj);
    if (str != NULL) {
        Py_ssize_t len;
        const char *s = PyUnicode_AsUTF8AndSize(str, &len);
        if (s != NULL) {
            ERL_NIF_TERM bin;
            unsigned char *buf = enif_make_new_binary(env, len, &bin);
            if (buf != NULL) {
                memcpy(buf, s, len);
                Py_DECREF(str);
                return bin;
            }
        }
        Py_DECREF(str);
    }

    return ATOM_UNDEFINED;
}

/* ============================================================================
 * Erlang to Python Conversion
 * ============================================================================ */

/**
 * @brief Convert an Erlang term to a Python object
 *
 * Recursively converts Erlang terms to their Python equivalents.
 * The function handles all common Erlang types and provides sensible
 * defaults for edge cases.
 *
 * @par Type Mapping
 *
 * | Erlang Type       | Python Type           | Notes                    |
 * |-------------------|-----------------------|--------------------------|
 * | `true` atom       | `True`                |                          |
 * | `false` atom      | `False`               |                          |
 * | `nil/none/undefined` | `None`             |                          |
 * | Other atoms       | `str`                 | Atom name as string      |
 * | integer           | `int`                 | All integer sizes        |
 * | float             | `float`               |                          |
 * | binary            | `str`                 | UTF-8 decoded, falls back to bytes |
 * | list              | `list`                | Recursive conversion     |
 * | tuple             | `tuple`               | Recursive conversion     |
 * | map               | `dict`                | Recursive conversion     |
 * | py_object resource | unwrapped object     | Direct unwrap            |
 * | Other             | `None`                | Fallback                 |
 *
 * @param env   NIF environment containing the term
 * @param term  Erlang term to convert
 *
 * @return New Python object (caller owns reference), or NULL on error
 *
 * @retval NULL  On conversion error (with Python exception set)
 *
 * @pre GIL must be held
 * @pre env must be valid
 * @pre term must be a valid Erlang term
 *
 * @warning Caller MUST call Py_DECREF on the returned object
 * @warning On NULL return, check PyErr_Occurred() for exception
 *
 * @note Creates new references for all returned objects
 * @note Recursively converts container types
 *
 * @see py_to_term() for the reverse conversion
 */
static PyObject *term_to_py(ErlNifEnv *env, ERL_NIF_TERM term) {
    double d_val;
    ErlNifBinary bin;
    unsigned int list_len;
    int arity;
    const ERL_NIF_TERM *tuple;

    /*
     * Check atoms first using direct term comparison (enif_is_identical)
     * instead of string comparison (strcmp). This is faster because
     * atoms are interned - we just compare pointers.
     */
    if (enif_is_atom(env, term)) {
        if (enif_is_identical(term, ATOM_TRUE)) {
            Py_RETURN_TRUE;
        }
        if (enif_is_identical(term, ATOM_FALSE)) {
            Py_RETURN_FALSE;
        }
        if (enif_is_identical(term, ATOM_NIL) ||
            enif_is_identical(term, ATOM_NONE) ||
            enif_is_identical(term, ATOM_UNDEFINED)) {
            Py_RETURN_NONE;
        }
        /* Convert other atoms to Python string */
        char atom_buf[256];
        if (enif_get_atom(env, term, atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
            return PyUnicode_FromString(atom_buf);
        }
        Py_RETURN_NONE;
    }

    /*
     * Try integer types - use a single int64 check first since it covers
     * most practical cases, then fall back to larger types.
     */
    ErlNifSInt64 i64_val;
    if (enif_get_int64(env, term, &i64_val)) {
        return PyLong_FromLongLong(i64_val);
    }

    ErlNifUInt64 u64_val;
    if (enif_get_uint64(env, term, &u64_val)) {
        return PyLong_FromUnsignedLongLong(u64_val);
    }

    /* Try float */
    if (enif_get_double(env, term, &d_val)) {
        return PyFloat_FromDouble(d_val);
    }

    /*
     * Check binary BEFORE list to avoid flattening iolists.
     * Try UTF-8 decode first, fall back to bytes for invalid UTF-8.
     */
    if (enif_inspect_binary(env, term, &bin)) {
        PyObject *str = PyUnicode_DecodeUTF8((char *)bin.data, bin.size, "strict");
        if (str != NULL) {
            return str;
        }
        /* Not valid UTF-8 - return as bytes instead */
        PyErr_Clear();
        return PyBytes_FromStringAndSize((char *)bin.data, bin.size);
    }

    /* Check list (must come after binary to preserve structure) */
    if (enif_get_list_length(env, term, &list_len)) {
        PyObject *list = PyList_New(list_len);
        if (list == NULL) {
            return NULL;
        }
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

    /* Check tuple */
    if (enif_get_tuple(env, term, &arity, &tuple)) {
        PyObject *py_tuple = PyTuple_New(arity);
        if (py_tuple == NULL) {
            return NULL;
        }
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

    /* Check map */
    if (enif_is_map(env, term)) {
        PyObject *dict = PyDict_New();
        if (dict == NULL) {
            return NULL;
        }
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

    /* Check for PID */
    {
        ErlNifPid pid;
        if (enif_get_local_pid(env, term, &pid)) {
            ErlangPidObject *obj = PyObject_New(ErlangPidObject, &ErlangPidType);
            if (obj == NULL) return NULL;
            obj->pid = pid;
            return (PyObject *)obj;
        }
    }

    /* Check for wrapped Python object resource */
    py_object_t *wrapper;
    if (enif_get_resource(env, term, PYOBJ_RESOURCE_TYPE, (void **)&wrapper)) {
        Py_INCREF(wrapper->obj);
        return wrapper->obj;
    }

    /* Fallback: return None for unknown types */
    Py_RETURN_NONE;
}

/* ============================================================================
 * Error Handling Utilities
 * ============================================================================ */

/**
 * @brief Create a simple error tuple `{error, Reason}`
 *
 * Creates an Erlang error tuple with an atom reason. This is used for
 * internal errors that don't originate from Python exceptions.
 *
 * @param env     NIF environment for term allocation
 * @param reason  Error reason as C string (converted to atom)
 *
 * @return `{error, reason}` tuple
 *
 * @par Example
 * @code
 * // Returns {error, alloc_failed}
 * return make_error(env, "alloc_failed");
 * @endcode
 */
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *reason) {
    return enif_make_tuple2(env, ATOM_ERROR,
        enif_make_atom(env, reason));
}

/**
 * @brief Create an error tuple from the current Python exception
 *
 * Fetches the current Python exception state, formats it as an Erlang
 * error tuple, and clears the Python error indicator.
 *
 * @par Return Format
 *
 * On success: `{error, {ExceptionType, Message}}`
 *
 * Where:
 * - `ExceptionType` is an atom (e.g., `'ValueError'`)
 * - `Message` is a string with the exception message
 *
 * @par Special Cases
 *
 * - `StopIteration` → `{error, {stop_iteration, none}}`
 * - No exception set → `{error, unknown_python_error}`
 *
 * @param env  NIF environment for term allocation
 *
 * @return `{error, {ExceptionType, Message}}` tuple
 *
 * @pre GIL must be held
 *
 * @note Always clears the Python exception state
 * @note Safe to call even if no exception is set
 *
 * @see make_error() for simple string errors
 */
static ERL_NIF_TERM make_py_error(ErlNifEnv *env) {
    PyObject *type, *value, *traceback;
    PyErr_Fetch(&type, &value, &traceback);

    if (value == NULL) {
        return make_error(env, "unknown_python_error");
    }

    /* Special handling for StopIteration (common in iterators) */
    if (PyErr_GivenExceptionMatches(type, PyExc_StopIteration)) {
        PyErr_Clear();
        Py_XDECREF(type);
        Py_XDECREF(value);
        Py_XDECREF(traceback);
        return enif_make_tuple2(env, ATOM_ERROR,
            enif_make_tuple2(env, ATOM_STOP_ITERATION, ATOM_NONE));
    }

    /* Get exception message */
    PyObject *str = PyObject_Str(value);
    const char *err_msg = str ? PyUnicode_AsUTF8(str) : "unknown";

    /* Get exception type name */
    PyObject *type_name = PyObject_GetAttrString(type, "__name__");
    const char *type_str = type_name ? PyUnicode_AsUTF8(type_name) : "Exception";

    /* Build error tuple: {error, {TypeAtom, MessageString}} */
    ERL_NIF_TERM error_tuple = enif_make_tuple2(env,
        enif_make_atom(env, type_str),
        enif_make_string(env, err_msg, ERL_NIF_LATIN1));

    /* Clean up Python objects */
    Py_XDECREF(str);
    Py_XDECREF(type_name);
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(traceback);

    PyErr_Clear();

    return enif_make_tuple2(env, ATOM_ERROR, error_tuple);
}
