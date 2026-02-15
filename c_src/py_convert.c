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

/* ============================================================================
 * Python to Erlang Conversion
 * ============================================================================ */

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
 *
 * @see term_to_py() for the reverse conversion
 */
static ERL_NIF_TERM py_to_term(ErlNifEnv *env, PyObject *obj) {
    /* Handle None singleton */
    if (obj == Py_None) {
        return ATOM_NONE;
    }

    /* Handle boolean singletons (must check before int, as bool subclasses int) */
    if (obj == Py_True) {
        return ATOM_TRUE;
    }

    if (obj == Py_False) {
        return ATOM_FALSE;
    }

    /* Handle integers */
    if (PyLong_Check(obj)) {
        long long val = PyLong_AsLongLong(obj);
        if (PyErr_Occurred()) {
            PyErr_Clear();
            /* Value too large for signed - try unsigned */
            unsigned long long uval = PyLong_AsUnsignedLongLong(obj);
            if (PyErr_Occurred()) {
                PyErr_Clear();
                /* Value too large for 64-bit - fall back to string representation */
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

    /* Handle Unicode strings */
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

    /* Handle byte strings */
    if (PyBytes_Check(obj)) {
        Py_ssize_t size = PyBytes_Size(obj);
        char *data = PyBytes_AsString(obj);
        ERL_NIF_TERM bin;
        unsigned char *buf = enif_make_new_binary(env, size, &bin);
        memcpy(buf, data, size);
        return bin;
    }

    /* Handle lists */
    if (PyList_Check(obj)) {
        Py_ssize_t len = PyList_Size(obj);
        if (len == 0) {
            return enif_make_list(env, 0);
        }
        ERL_NIF_TERM *items = enif_alloc(sizeof(ERL_NIF_TERM) * len);
        if (items == NULL) {
            return ATOM_ERROR;
        }
        for (Py_ssize_t i = 0; i < len; i++) {
            PyObject *item = PyList_GetItem(obj, i);  /* Borrowed ref */
            items[i] = py_to_term(env, item);
        }
        ERL_NIF_TERM result = enif_make_list_from_array(env, items, len);
        enif_free(items);
        return result;
    }

    /*
     * Handle NumPy arrays by converting to Python list first.
     * Detection: has both 'tolist' method and 'ndim' attribute.
     */
    if (PyObject_HasAttrString(obj, "tolist") && PyObject_HasAttrString(obj, "ndim")) {
        PyObject *tolist = PyObject_CallMethod(obj, "tolist", NULL);
        if (tolist != NULL) {
            ERL_NIF_TERM result = py_to_term(env, tolist);
            Py_DECREF(tolist);
            return result;
        }
        PyErr_Clear();
    }

    /* Handle tuples */
    if (PyTuple_Check(obj)) {
        Py_ssize_t len = PyTuple_Size(obj);
        if (len == 0) {
            return enif_make_tuple(env, 0);
        }
        ERL_NIF_TERM *items = enif_alloc(sizeof(ERL_NIF_TERM) * len);
        if (items == NULL) {
            return ATOM_ERROR;
        }
        for (Py_ssize_t i = 0; i < len; i++) {
            PyObject *item = PyTuple_GetItem(obj, i);  /* Borrowed ref */
            items[i] = py_to_term(env, item);
        }
        ERL_NIF_TERM result = enif_make_tuple_from_array(env, items, len);
        enif_free(items);
        return result;
    }

    /* Handle dictionaries */
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

    /*
     * Fallback: convert any other object to its string representation.
     * This handles custom classes, functions, modules, etc.
     */
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
 * | binary            | `str`                 | UTF-8 decoded            |
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
    int i_val;
    long l_val;
    ErlNifSInt64 i64_val;
    ErlNifUInt64 u64_val;
    double d_val;
    ErlNifBinary bin;
    unsigned int list_len;
    int arity;
    const ERL_NIF_TERM *tuple;

    /* Check for atoms first (most specific checks) */
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
                /* Convert atom name to Python string */
                return PyUnicode_FromString(atom_buf);
            }
        }
    }

    /* Try integer types in order of size */
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

    /* Try float */
    if (enif_get_double(env, term, &d_val)) {
        return PyFloat_FromDouble(d_val);
    }

    /*
     * Check binary BEFORE list to avoid flattening iolists.
     * This preserves the distinction between binaries and charlists.
     */
    if (enif_inspect_binary(env, term, &bin)) {
        return PyUnicode_FromStringAndSize((char *)bin.data, bin.size);
    }

    /* Check list (must come after binary to preserve structure) */
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

    /* Check tuple */
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

    /* Check map */
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
