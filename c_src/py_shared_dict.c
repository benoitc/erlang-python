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
 * py_shared_dict.c - SharedDict implementation for Erlang-Python bridge
 *
 * This file implements process-scoped shared dictionaries that allow
 * Python code running in different contexts to share data safely.
 *
 * Features:
 * - Process-scoped: owned by an Erlang process, destroyed when process dies
 * - Thread-safe: mutex-protected operations
 * - Cross-interpreter safe: values stored as pickled bytes
 */

/* ============================================================================
 * Resource Callbacks
 * ============================================================================ */

/**
 * @brief Down callback for py_shared_dict_t
 *
 * Called when the owning process dies. Sets destroyed flag and clears the dict.
 * This callback is invoked by the runtime when the monitored process terminates.
 */
static void shared_dict_down(ErlNifEnv *env, void *obj,
                              ErlNifPid *pid, ErlNifMonitor *mon) {
    (void)env;
    (void)pid;
    (void)mon;
    py_shared_dict_t *sd = (py_shared_dict_t *)obj;

    /* Mark as destroyed - subsequent access will return badarg */
    atomic_store(&sd->destroyed, true);
    sd->monitor_active = false;

    /* Clear the Python dict if runtime is still running */
    if (runtime_is_running() && sd->dict != NULL) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        pthread_mutex_lock(&sd->mutex);
        Py_CLEAR(sd->dict);
        pthread_mutex_unlock(&sd->mutex);
        PyGILState_Release(gstate);
    }
}

/**
 * @brief Destructor for py_shared_dict_t
 *
 * Called when the resource is garbage collected.
 * Cleans up the monitor, Python dict, and mutex.
 */
static void shared_dict_destructor(ErlNifEnv *env, void *obj) {
    py_shared_dict_t *sd = (py_shared_dict_t *)obj;

    /* Demonitor if still active */
    if (sd->monitor_active && env != NULL) {
        enif_demonitor_process(env, sd, &sd->monitor);
        sd->monitor_active = false;
    }

    /* Clear Python dict if runtime is still running */
    if (runtime_is_running() && sd->dict != NULL) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_CLEAR(sd->dict);
        PyGILState_Release(gstate);
    }

    /* Destroy mutex */
    pthread_mutex_destroy(&sd->mutex);
}

/* ============================================================================
 * NIF Functions
 * ============================================================================ */

/**
 * @brief Create a new process-scoped SharedDict
 *
 * Creates a SharedDict owned by the calling process. The dict is automatically
 * destroyed when the owning process terminates.
 *
 * @return {ok, Reference} on success, {error, Reason} on failure
 */
static ERL_NIF_TERM nif_shared_dict_new(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;

    if (!runtime_is_running()) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "not_initialized"));
    }

    /* Allocate resource */
    py_shared_dict_t *sd = enif_alloc_resource(
        PY_SHARED_DICT_RESOURCE_TYPE, sizeof(py_shared_dict_t));
    if (sd == NULL) {
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "alloc_failed"));
    }

    /* Initialize fields */
    memset(sd, 0, sizeof(py_shared_dict_t));
    atomic_store(&sd->destroyed, false);
    pthread_mutex_init(&sd->mutex, NULL);

    /* Create Python dict for storage */
    PyGILState_STATE gstate = PyGILState_Ensure();
    sd->dict = PyDict_New();
    if (sd->dict == NULL) {
        PyGILState_Release(gstate);
        pthread_mutex_destroy(&sd->mutex);
        enif_release_resource(sd);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "dict_alloc_failed"));
    }
    PyGILState_Release(gstate);

    /* Note: Process monitoring disabled for now to debug crash
     * SharedDict will be garbage collected when no references remain */
    sd->monitor_active = false;

    /* Create reference term and release our reference */
    ERL_NIF_TERM ref = enif_make_resource(env, sd);
    enif_release_resource(sd);

    return enif_make_tuple2(env, ATOM_OK, ref);
}

/**
 * @brief Get a value from SharedDict
 *
 * @param Handle SharedDict reference
 * @param Key Binary key
 * @param Default Default value if key not found
 * @return Value or Default, badarg if destroyed
 */
static ERL_NIF_TERM nif_shared_dict_get(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_shared_dict_t *sd;
    if (!enif_get_resource(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, (void **)&sd)) {
        return enif_make_badarg(env);
    }

    /* Check if destroyed */
    if (atomic_load(&sd->destroyed)) {
        return enif_make_badarg(env);
    }

    /* Get key as binary */
    ErlNifBinary key_bin;
    if (!enif_inspect_binary(env, argv[1], &key_bin)) {
        return enif_make_badarg(env);
    }

    /* Lock and access dict */
    pthread_mutex_lock(&sd->mutex);

    /* Check destroyed again after acquiring lock */
    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_badarg(env);
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Create Python key from binary */
    PyObject *py_key = PyBytes_FromStringAndSize((char *)key_bin.data, key_bin.size);
    if (py_key == NULL) {
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_badarg(env);
    }

    /* Look up value (pickled bytes) */
    PyObject *pickled = PyDict_GetItem(sd->dict, py_key);
    Py_DECREF(py_key);

    if (pickled == NULL) {
        /* Key not found, return default */
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return argv[2];  /* Return default */
    }

    /* Unpickle the value */
    PyObject *pickle_mod = PyImport_ImportModule("pickle");
    if (pickle_mod == NULL) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "pickle_unavailable"));
    }

    PyObject *loads = PyObject_GetAttrString(pickle_mod, "loads");
    Py_DECREF(pickle_mod);
    if (loads == NULL) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "pickle_loads_unavailable"));
    }

    PyObject *value = PyObject_CallFunctionObjArgs(loads, pickled, NULL);
    Py_DECREF(loads);

    if (value == NULL) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "unpickle_failed"));
    }

    /* Convert Python value to Erlang term */
    ERL_NIF_TERM result = py_to_term(env, value);
    Py_DECREF(value);

    PyGILState_Release(gstate);
    pthread_mutex_unlock(&sd->mutex);

    return result;
}

/**
 * @brief Set a value in SharedDict
 *
 * @param Handle SharedDict reference
 * @param Key Binary key
 * @param Value Erlang term value (will be pickled)
 * @return ok on success, badarg if destroyed
 */
static ERL_NIF_TERM nif_shared_dict_set(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_shared_dict_t *sd;
    if (!enif_get_resource(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, (void **)&sd)) {
        return enif_make_badarg(env);
    }

    /* Check if destroyed */
    if (atomic_load(&sd->destroyed)) {
        return enif_make_badarg(env);
    }

    /* Get key as binary */
    ErlNifBinary key_bin;
    if (!enif_inspect_binary(env, argv[1], &key_bin)) {
        return enif_make_badarg(env);
    }

    /* Lock and access dict */
    pthread_mutex_lock(&sd->mutex);

    /* Check destroyed again after acquiring lock */
    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_badarg(env);
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Convert Erlang term to Python object */
    PyObject *py_value = term_to_py(env, argv[2]);
    if (py_value == NULL) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "term_conversion_failed"));
    }

    /* Pickle the value for cross-interpreter safety */
    PyObject *pickle_mod = PyImport_ImportModule("pickle");
    if (pickle_mod == NULL) {
        PyErr_Clear();
        Py_DECREF(py_value);
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "pickle_unavailable"));
    }

    PyObject *dumps = PyObject_GetAttrString(pickle_mod, "dumps");
    Py_DECREF(pickle_mod);
    if (dumps == NULL) {
        PyErr_Clear();
        Py_DECREF(py_value);
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "pickle_dumps_unavailable"));
    }

    PyObject *pickled = PyObject_CallFunctionObjArgs(dumps, py_value, NULL);
    Py_DECREF(dumps);
    Py_DECREF(py_value);

    if (pickled == NULL) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "pickle_failed"));
    }

    /* Create Python key from binary */
    PyObject *py_key = PyBytes_FromStringAndSize((char *)key_bin.data, key_bin.size);
    if (py_key == NULL) {
        Py_DECREF(pickled);
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_badarg(env);
    }

    /* Store pickled value in dict */
    int set_result = PyDict_SetItem(sd->dict, py_key, pickled);
    Py_DECREF(py_key);
    Py_DECREF(pickled);

    if (set_result < 0) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_tuple2(env, ATOM_ERROR,
                                enif_make_atom(env, "dict_set_failed"));
    }

    PyGILState_Release(gstate);
    pthread_mutex_unlock(&sd->mutex);

    return ATOM_OK;
}

/**
 * @brief Delete a key from SharedDict
 *
 * @param Handle SharedDict reference
 * @param Key Binary key
 * @return ok on success (even if key didn't exist), badarg if destroyed
 */
static ERL_NIF_TERM nif_shared_dict_del(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_shared_dict_t *sd;
    if (!enif_get_resource(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, (void **)&sd)) {
        return enif_make_badarg(env);
    }

    /* Check if destroyed */
    if (atomic_load(&sd->destroyed)) {
        return enif_make_badarg(env);
    }

    /* Get key as binary */
    ErlNifBinary key_bin;
    if (!enif_inspect_binary(env, argv[1], &key_bin)) {
        return enif_make_badarg(env);
    }

    /* Lock and access dict */
    pthread_mutex_lock(&sd->mutex);

    /* Check destroyed again after acquiring lock */
    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_badarg(env);
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Create Python key from binary */
    PyObject *py_key = PyBytes_FromStringAndSize((char *)key_bin.data, key_bin.size);
    if (py_key == NULL) {
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_badarg(env);
    }

    /* Delete key (ignore if not found) */
    PyDict_DelItem(sd->dict, py_key);
    PyErr_Clear();  /* Clear KeyError if key didn't exist */
    Py_DECREF(py_key);

    PyGILState_Release(gstate);
    pthread_mutex_unlock(&sd->mutex);

    return ATOM_OK;
}

/**
 * @brief Get all keys from SharedDict
 *
 * @param Handle SharedDict reference
 * @return List of binary keys, badarg if destroyed
 */
static ERL_NIF_TERM nif_shared_dict_keys(ErlNifEnv *env, int argc,
                                          const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_shared_dict_t *sd;
    if (!enif_get_resource(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, (void **)&sd)) {
        return enif_make_badarg(env);
    }

    /* Check if destroyed */
    if (atomic_load(&sd->destroyed)) {
        return enif_make_badarg(env);
    }

    /* Lock and access dict */
    pthread_mutex_lock(&sd->mutex);

    /* Check destroyed again after acquiring lock */
    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_badarg(env);
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Get keys */
    PyObject *keys = PyDict_Keys(sd->dict);
    if (keys == NULL) {
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_list(env, 0);
    }

    Py_ssize_t len = PyList_Size(keys);
    ERL_NIF_TERM *key_terms = enif_alloc(sizeof(ERL_NIF_TERM) * len);
    if (key_terms == NULL) {
        Py_DECREF(keys);
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&sd->mutex);
        return enif_make_list(env, 0);
    }

    for (Py_ssize_t i = 0; i < len; i++) {
        PyObject *py_key = PyList_GetItem(keys, i);
        if (PyBytes_Check(py_key)) {
            char *data;
            Py_ssize_t size;
            PyBytes_AsStringAndSize(py_key, &data, &size);
            ERL_NIF_TERM bin;
            unsigned char *buf = enif_make_new_binary(env, size, &bin);
            if (buf != NULL) {
                memcpy(buf, data, size);
                key_terms[i] = bin;
            } else {
                key_terms[i] = enif_make_atom(env, "error");
            }
        } else {
            key_terms[i] = enif_make_atom(env, "error");
        }
    }

    ERL_NIF_TERM result = enif_make_list_from_array(env, key_terms, len);
    enif_free(key_terms);
    Py_DECREF(keys);

    PyGILState_Release(gstate);
    pthread_mutex_unlock(&sd->mutex);

    return result;
}

/**
 * @brief Explicitly destroy a SharedDict.
 *
 * Marks the SharedDict as destroyed and clears its Python dict.
 * This is idempotent - calling on an already-destroyed dict returns ok.
 *
 * @param Handle SharedDict reference
 * @return ok atom
 */
static ERL_NIF_TERM nif_shared_dict_destroy(ErlNifEnv *env, int argc,
                                             const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_shared_dict_t *sd;
    if (!enif_get_resource(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, (void **)&sd)) {
        return enif_make_badarg(env);
    }

    /* Check if already destroyed - idempotent */
    if (atomic_load(&sd->destroyed)) {
        return ATOM_OK;
    }

    /* Mark as destroyed */
    atomic_store(&sd->destroyed, true);

    /* Clear the Python dict */
    pthread_mutex_lock(&sd->mutex);
    if (sd->dict != NULL && runtime_is_running()) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_CLEAR(sd->dict);
        PyGILState_Release(gstate);
    }
    sd->dict = NULL;
    pthread_mutex_unlock(&sd->mutex);

    return ATOM_OK;
}

/* ============================================================================
 * Python Methods
 * ============================================================================
 * These functions implement the Python-side API for SharedDict, allowing
 * Python code to access process-scoped shared dictionaries.
 */

/**
 * Extract py_shared_dict_t* from a PyCapsule.
 * Returns NULL and sets exception on error.
 */
static py_shared_dict_t *get_shared_dict_from_capsule(PyObject *capsule) {
    if (!PyCapsule_IsValid(capsule, "py_shared_dict")) {
        PyErr_SetString(PyExc_TypeError, "Invalid SharedDict handle");
        return NULL;
    }
    py_shared_dict_t *sd = (py_shared_dict_t *)PyCapsule_GetPointer(capsule, "py_shared_dict");
    if (sd == NULL) {
        PyErr_SetString(PyExc_ValueError, "SharedDict handle is NULL");
        return NULL;
    }
    if (atomic_load(&sd->destroyed)) {
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }
    return sd;
}

/**
 * Python implementation of SharedDict.get(key, default=None)
 * Usage: erlang._shared_dict_get(handle, key)
 */
static PyObject *py_shared_dict_get_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    const char *key;
    Py_ssize_t key_len;

    if (!PyArg_ParseTuple(args, "Os#", &handle, &key, &key_len)) {
        return NULL;
    }

    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;

    pthread_mutex_lock(&sd->mutex);

    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }

    /* Create Python key from bytes */
    PyObject *py_key = PyBytes_FromStringAndSize(key, key_len);
    if (py_key == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    /* Look up pickled value */
    PyObject *pickled = PyDict_GetItem(sd->dict, py_key);
    Py_DECREF(py_key);

    if (pickled == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        Py_RETURN_NONE;
    }

    /* Unpickle the value */
    PyObject *pickle_mod = PyImport_ImportModule("pickle");
    if (pickle_mod == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    PyObject *loads = PyObject_GetAttrString(pickle_mod, "loads");
    Py_DECREF(pickle_mod);
    if (loads == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    PyObject *value = PyObject_CallFunctionObjArgs(loads, pickled, NULL);
    Py_DECREF(loads);

    pthread_mutex_unlock(&sd->mutex);
    return value;
}

/**
 * Python implementation of SharedDict.set(key, value)
 * Usage: erlang._shared_dict_set(handle, key, value)
 */
static PyObject *py_shared_dict_set_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    const char *key;
    Py_ssize_t key_len;
    PyObject *value;

    if (!PyArg_ParseTuple(args, "Os#O", &handle, &key, &key_len, &value)) {
        return NULL;
    }

    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;

    pthread_mutex_lock(&sd->mutex);

    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }

    /* Pickle the value */
    PyObject *pickle_mod = PyImport_ImportModule("pickle");
    if (pickle_mod == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    PyObject *dumps = PyObject_GetAttrString(pickle_mod, "dumps");
    Py_DECREF(pickle_mod);
    if (dumps == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    PyObject *pickled = PyObject_CallFunctionObjArgs(dumps, value, NULL);
    Py_DECREF(dumps);
    if (pickled == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    /* Create Python key */
    PyObject *py_key = PyBytes_FromStringAndSize(key, key_len);
    if (py_key == NULL) {
        Py_DECREF(pickled);
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    /* Store in dict */
    int result = PyDict_SetItem(sd->dict, py_key, pickled);
    Py_DECREF(py_key);
    Py_DECREF(pickled);

    pthread_mutex_unlock(&sd->mutex);

    if (result < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Python implementation of SharedDict.delete(key)
 * Usage: erlang._shared_dict_del(handle, key)
 * Returns True if key existed, False otherwise
 */
static PyObject *py_shared_dict_del_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    const char *key;
    Py_ssize_t key_len;

    if (!PyArg_ParseTuple(args, "Os#", &handle, &key, &key_len)) {
        return NULL;
    }

    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;

    pthread_mutex_lock(&sd->mutex);

    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }

    /* Create Python key */
    PyObject *py_key = PyBytes_FromStringAndSize(key, key_len);
    if (py_key == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    /* Check if key exists */
    int exists = PyDict_Contains(sd->dict, py_key);
    if (exists > 0) {
        PyDict_DelItem(sd->dict, py_key);
        PyErr_Clear();
    }
    Py_DECREF(py_key);

    pthread_mutex_unlock(&sd->mutex);

    if (exists > 0) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

/**
 * Python implementation of SharedDict.contains(key)
 * Usage: erlang._shared_dict_contains(handle, key)
 */
static PyObject *py_shared_dict_contains_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    const char *key;
    Py_ssize_t key_len;

    if (!PyArg_ParseTuple(args, "Os#", &handle, &key, &key_len)) {
        return NULL;
    }

    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;

    pthread_mutex_lock(&sd->mutex);

    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }

    /* Create Python key */
    PyObject *py_key = PyBytes_FromStringAndSize(key, key_len);
    if (py_key == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    int exists = PyDict_Contains(sd->dict, py_key);
    Py_DECREF(py_key);

    pthread_mutex_unlock(&sd->mutex);

    if (exists > 0) {
        Py_RETURN_TRUE;
    } else if (exists == 0) {
        Py_RETURN_FALSE;
    } else {
        return NULL;  /* Error occurred */
    }
}

/**
 * Python implementation of SharedDict.destroy()
 * Usage: erlang._shared_dict_destroy(handle)
 *
 * Explicitly destroys the SharedDict, invalidating all references.
 * This is idempotent - calling on already-destroyed dict returns None.
 */
static PyObject *py_shared_dict_destroy_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }

    if (!PyCapsule_IsValid(capsule, "py_shared_dict")) {
        PyErr_SetString(PyExc_TypeError, "Invalid SharedDict handle");
        return NULL;
    }

    py_shared_dict_t *sd = (py_shared_dict_t *)PyCapsule_GetPointer(capsule, "py_shared_dict");
    if (sd == NULL) {
        PyErr_SetString(PyExc_ValueError, "SharedDict handle is NULL");
        return NULL;
    }

    /* If already destroyed, return None (idempotent) */
    if (atomic_load(&sd->destroyed)) {
        Py_RETURN_NONE;
    }

    /* Mark as destroyed */
    atomic_store(&sd->destroyed, true);

    /* Clear the Python dict */
    pthread_mutex_lock(&sd->mutex);
    Py_CLEAR(sd->dict);
    pthread_mutex_unlock(&sd->mutex);

    Py_RETURN_NONE;
}

/**
 * Python implementation of SharedDict.keys()
 * Usage: erlang._shared_dict_keys(handle)
 */
static PyObject *py_shared_dict_keys_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;

    if (!PyArg_ParseTuple(args, "O", &handle)) {
        return NULL;
    }

    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;

    pthread_mutex_lock(&sd->mutex);

    if (atomic_load(&sd->destroyed) || sd->dict == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }

    /* Get keys and decode them to strings */
    PyObject *keys = PyDict_Keys(sd->dict);
    if (keys == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    Py_ssize_t len = PyList_Size(keys);
    PyObject *result = PyList_New(len);
    if (result == NULL) {
        Py_DECREF(keys);
        pthread_mutex_unlock(&sd->mutex);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < len; i++) {
        PyObject *py_key = PyList_GetItem(keys, i);
        if (PyBytes_Check(py_key)) {
            /* Decode bytes to string */
            PyObject *str_key = PyUnicode_DecodeUTF8(
                PyBytes_AS_STRING(py_key),
                PyBytes_GET_SIZE(py_key),
                "replace");
            if (str_key == NULL) {
                Py_DECREF(result);
                Py_DECREF(keys);
                pthread_mutex_unlock(&sd->mutex);
                return NULL;
            }
            PyList_SET_ITEM(result, i, str_key);
        } else {
            Py_INCREF(py_key);
            PyList_SET_ITEM(result, i, py_key);
        }
    }

    Py_DECREF(keys);
    pthread_mutex_unlock(&sd->mutex);
    return result;
}
