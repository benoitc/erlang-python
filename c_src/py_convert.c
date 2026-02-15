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
 * py_convert.c - Type conversion between Python and Erlang
 *
 * This module handles bidirectional type conversion:
 * - py_to_term(): Python objects -> Erlang terms
 * - term_to_py(): Erlang terms -> Python objects
 * - Error handling utilities
 */

/* Note: This file is included from py_nif.c after py_nif.h */

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

    /* Handle numpy arrays by converting to list first */
    if (PyObject_HasAttrString(obj, "tolist") && PyObject_HasAttrString(obj, "ndim")) {
        PyObject *tolist = PyObject_CallMethod(obj, "tolist", NULL);
        if (tolist != NULL) {
            ERL_NIF_TERM result = py_to_term(env, tolist);
            Py_DECREF(tolist);
            return result;
        }
        PyErr_Clear();
    }

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

    /* Binary/String - check binary first, but NOT iolist (which would flatten lists) */
    if (enif_inspect_binary(env, term, &bin)) {
        return PyUnicode_FromStringAndSize((char *)bin.data, bin.size);
    }

    /* List - must check before iolist to preserve list structure */
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
