/* Copyright 2026 Benoit Chesneau. Licensed under the Apache License, Version 2.0. */

/**
 * Native storage for SharedDict.
 *
 * Only byte strings cross interpreters. Pickling and unpickling happen before
 * or after the table lock, never while it is held. Consequently Python code
 * runs under its own interpreter's GIL and cannot invert the GIL/table-lock
 * order or re-enter a locked dictionary from pickle callbacks.
 */

typedef struct { unsigned char *data; size_t len; } shared_dict_bytes_t;
typedef struct { unsigned char *data; size_t len; } shared_dict_key_t;

static uint64_t shared_dict_hash(const unsigned char *key, size_t len) {
    uint64_t hash = UINT64_C(1469598103934665603);
    for (size_t i = 0; i < len; i++) {
        hash ^= key[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static unsigned char *shared_dict_value(py_shared_dict_entry_t *entry) {
    return entry->data + entry->key_len;
}

static py_shared_dict_entry_t *shared_dict_find_locked(
        py_shared_dict_t *sd, const unsigned char *key, size_t key_len,
        py_shared_dict_entry_t ***link_out) {
    size_t bucket = (size_t)(shared_dict_hash(key, key_len) % PY_SHARED_DICT_BUCKETS);
    py_shared_dict_entry_t **link = &sd->buckets[bucket];
    while (*link != NULL) {
        py_shared_dict_entry_t *entry = *link;
        if (entry->key_len == key_len &&
            (key_len == 0 || memcmp(entry->data, key, key_len) == 0)) {
            if (link_out != NULL) *link_out = link;
            return entry;
        }
        link = &entry->next;
    }
    if (link_out != NULL) *link_out = link;
    return NULL;
}

static py_shared_dict_entry_t *shared_dict_entry_new(
        const unsigned char *key, size_t key_len,
        const unsigned char *value, size_t value_len) {
    if (key_len > SIZE_MAX - value_len ||
        sizeof(py_shared_dict_entry_t) > SIZE_MAX - key_len - value_len) return NULL;
    py_shared_dict_entry_t *entry = enif_alloc(
        sizeof(*entry) + key_len + value_len);
    if (entry == NULL) return NULL;
    entry->next = NULL;
    entry->key_len = key_len;
    entry->value_len = value_len;
    if (key_len > 0) memcpy(entry->data, key, key_len);
    if (value_len > 0) memcpy(entry->data + key_len, value, value_len);
    return entry;
}

static void shared_dict_free_entries(py_shared_dict_entry_t *entry) {
    while (entry != NULL) {
        py_shared_dict_entry_t *next = entry->next;
        enif_free(entry);
        entry = next;
    }
}

static py_shared_dict_entry_t *shared_dict_detach_all_locked(py_shared_dict_t *sd) {
    py_shared_dict_entry_t *head = NULL;
    for (size_t i = 0; i < PY_SHARED_DICT_BUCKETS; i++) {
        py_shared_dict_entry_t *entry = sd->buckets[i];
        while (entry != NULL) {
            py_shared_dict_entry_t *next = entry->next;
            entry->next = head;
            head = entry;
            entry = next;
        }
        sd->buckets[i] = NULL;
    }
    sd->size = 0;
    return head;
}

static void shared_dict_destroy_contents(py_shared_dict_t *sd) {
    if (!sd->mutex_initialized) return;
    pthread_mutex_lock(&sd->mutex);
    atomic_store(&sd->destroyed, true);
    py_shared_dict_entry_t *entries = shared_dict_detach_all_locked(sd);
    pthread_mutex_unlock(&sd->mutex);
    shared_dict_free_entries(entries);
}

static int shared_dict_store(py_shared_dict_t *sd,
                             const unsigned char *key, size_t key_len,
                             const unsigned char *value, size_t value_len) {
    py_shared_dict_entry_t *new_entry = shared_dict_entry_new(
        key, key_len, value, value_len);
    if (new_entry == NULL) return -1;
    pthread_mutex_lock(&sd->mutex);
    if (atomic_load(&sd->destroyed)) {
        pthread_mutex_unlock(&sd->mutex);
        enif_free(new_entry);
        return 1;
    }
    py_shared_dict_entry_t **link;
    py_shared_dict_entry_t *old = shared_dict_find_locked(sd, key, key_len, &link);
    new_entry->next = old != NULL ? old->next : NULL;
    *link = new_entry;
    if (old == NULL) sd->size++;
    pthread_mutex_unlock(&sd->mutex);
    if (old != NULL) enif_free(old);
    return 0;
}

/* 1 found, 0 absent, -1 destroyed, -2 allocation failure. */
static int shared_dict_copy_value(py_shared_dict_t *sd,
                                  const unsigned char *key, size_t key_len,
                                  shared_dict_bytes_t *out) {
    out->data = NULL;
    out->len = 0;
    pthread_mutex_lock(&sd->mutex);
    if (atomic_load(&sd->destroyed)) {
        pthread_mutex_unlock(&sd->mutex);
        return -1;
    }
    py_shared_dict_entry_t *entry = shared_dict_find_locked(sd, key, key_len, NULL);
    if (entry == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return 0;
    }
    unsigned char *copy = enif_alloc(entry->value_len > 0 ? entry->value_len : 1);
    if (copy == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return -2;
    }
    if (entry->value_len > 0) memcpy(copy, shared_dict_value(entry), entry->value_len);
    out->data = copy;
    out->len = entry->value_len;
    pthread_mutex_unlock(&sd->mutex);
    return 1;
}

static int shared_dict_delete(py_shared_dict_t *sd,
                              const unsigned char *key, size_t key_len) {
    pthread_mutex_lock(&sd->mutex);
    if (atomic_load(&sd->destroyed)) {
        pthread_mutex_unlock(&sd->mutex);
        return -1;
    }
    py_shared_dict_entry_t **link;
    py_shared_dict_entry_t *entry = shared_dict_find_locked(sd, key, key_len, &link);
    if (entry != NULL) {
        *link = entry->next;
        sd->size--;
    }
    pthread_mutex_unlock(&sd->mutex);
    if (entry != NULL) enif_free(entry);
    return entry != NULL ? 1 : 0;
}

static PyObject *shared_dict_pickle_dumps(PyObject *value) {
    PyObject *pickle = PyImport_ImportModule("pickle");
    if (pickle == NULL) return NULL;
    PyObject *dumps = PyObject_GetAttrString(pickle, "dumps");
    Py_DECREF(pickle);
    if (dumps == NULL) return NULL;
    PyObject *result = PyObject_CallFunctionObjArgs(dumps, value, NULL);
    Py_DECREF(dumps);
    return result;
}

static PyObject *shared_dict_pickle_loads(const unsigned char *data, size_t len) {
    if (len > (size_t)PY_SSIZE_T_MAX) {
        PyErr_SetString(PyExc_OverflowError, "stored SharedDict value is too large");
        return NULL;
    }
    PyObject *bytes = PyBytes_FromStringAndSize((const char *)data, (Py_ssize_t)len);
    if (bytes == NULL) return NULL;
    PyObject *pickle = PyImport_ImportModule("pickle");
    if (pickle == NULL) { Py_DECREF(bytes); return NULL; }
    PyObject *loads = PyObject_GetAttrString(pickle, "loads");
    Py_DECREF(pickle);
    if (loads == NULL) { Py_DECREF(bytes); return NULL; }
    PyObject *result = PyObject_CallFunctionObjArgs(loads, bytes, NULL);
    Py_DECREF(loads);
    Py_DECREF(bytes);
    return result;
}

static int shared_dict_snapshot_keys(py_shared_dict_t *sd,
                                     shared_dict_key_t **keys_out,
                                     size_t *count_out) {
    *keys_out = NULL;
    *count_out = 0;
    pthread_mutex_lock(&sd->mutex);
    if (atomic_load(&sd->destroyed)) {
        pthread_mutex_unlock(&sd->mutex);
        return -1;
    }
    if (sd->size > SIZE_MAX / sizeof(shared_dict_key_t)) {
        pthread_mutex_unlock(&sd->mutex);
        return -2;
    }
    shared_dict_key_t *keys = sd->size > 0
        ? enif_alloc(sd->size * sizeof(*keys)) : NULL;
    if (sd->size > 0 && keys == NULL) {
        pthread_mutex_unlock(&sd->mutex);
        return -2;
    }
    size_t count = 0;
    for (size_t i = 0; i < PY_SHARED_DICT_BUCKETS; i++) {
        for (py_shared_dict_entry_t *entry = sd->buckets[i];
             entry != NULL; entry = entry->next) {
            unsigned char *copy = enif_alloc(entry->key_len > 0 ? entry->key_len : 1);
            if (copy == NULL) {
                for (size_t j = 0; j < count; j++) enif_free(keys[j].data);
                enif_free(keys);
                pthread_mutex_unlock(&sd->mutex);
                return -2;
            }
            if (entry->key_len > 0) memcpy(copy, entry->data, entry->key_len);
            keys[count].data = copy;
            keys[count].len = entry->key_len;
            count++;
        }
    }
    pthread_mutex_unlock(&sd->mutex);
    *keys_out = keys;
    *count_out = count;
    return 0;
}

static void shared_dict_free_keys(shared_dict_key_t *keys, size_t count) {
    for (size_t i = 0; i < count; i++) enif_free(keys[i].data);
    enif_free(keys);
}

/* Resource callback. */
static void shared_dict_destructor(ErlNifEnv *env, void *obj) {
    py_shared_dict_t *sd = obj;
    if (sd->monitor_active && env != NULL) {
        enif_demonitor_process(env, sd, &sd->monitor);
        sd->monitor_active = false;
    }
    shared_dict_destroy_contents(sd);
    if (sd->mutex_initialized) {
        pthread_mutex_destroy(&sd->mutex);
        sd->mutex_initialized = false;
    }
}

/* Erlang NIFs. */
static ERL_NIF_TERM nif_shared_dict_new(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    RUNTIME_CHECK_ERROR(env);
    py_shared_dict_t *sd = enif_alloc_resource(
        PY_SHARED_DICT_RESOURCE_TYPE, sizeof(*sd));
    if (sd == NULL) return make_error_atom(env, "alloc_failed");
    memset(sd, 0, sizeof(*sd));
    atomic_store(&sd->destroyed, false);
    if (pthread_mutex_init(&sd->mutex, NULL) != 0) {
        enif_release_resource(sd);
        return make_error_atom(env, "mutex_init_failed");
    }
    sd->mutex_initialized = true;
    ERL_NIF_TERM ref = enif_make_resource(env, sd);
    enif_release_resource(sd);
    return enif_make_tuple2(env, ATOM_OK, ref);
}

static ERL_NIF_TERM nif_shared_dict_get(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_shared_dict_t *sd;
    GET_RESOURCE_OR_BADARG(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, &sd);
    CHECK_NOT_DESTROYED(env, sd);
    ErlNifBinary key;
    INSPECT_BINARY_OR_BADARG(env, argv[1], key);
    shared_dict_bytes_t stored;
    int found = shared_dict_copy_value(sd, key.data, key.size, &stored);
    if (found == 0) return argv[2];
    if (found == -1) return enif_make_badarg(env);
    if (found == -2) return make_error_atom(env, "alloc_failed");
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyObject *value = shared_dict_pickle_loads(stored.data, stored.len);
    enif_free(stored.data);
    if (value == NULL) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        return make_error_atom(env, "unpickle_failed");
    }
    ERL_NIF_TERM result = py_to_term(env, value);
    Py_DECREF(value);
    PyGILState_Release(gstate);
    return result;
}

static ERL_NIF_TERM nif_shared_dict_set(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_shared_dict_t *sd;
    GET_RESOURCE_OR_BADARG(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, &sd);
    CHECK_NOT_DESTROYED(env, sd);
    ErlNifBinary key;
    INSPECT_BINARY_OR_BADARG(env, argv[1], key);
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyObject *value = term_to_py(env, argv[2]);
    PyObject *pickled = value != NULL ? shared_dict_pickle_dumps(value) : NULL;
    Py_XDECREF(value);
    if (pickled == NULL) {
        PyErr_Clear();
        PyGILState_Release(gstate);
        return make_error_atom(env, "pickle_failed");
    }
    char *data;
    Py_ssize_t len;
    if (PyBytes_AsStringAndSize(pickled, &data, &len) < 0) {
        Py_DECREF(pickled);
        PyErr_Clear();
        PyGILState_Release(gstate);
        return make_error_atom(env, "pickle_failed");
    }
    int result = shared_dict_store(sd, key.data, key.size,
                                   (unsigned char *)data, (size_t)len);
    Py_DECREF(pickled);
    PyGILState_Release(gstate);
    if (result == 1) return enif_make_badarg(env);
    if (result < 0) return make_error_atom(env, "alloc_failed");
    return ATOM_OK;
}

static ERL_NIF_TERM nif_shared_dict_del(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_shared_dict_t *sd;
    GET_RESOURCE_OR_BADARG(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, &sd);
    CHECK_NOT_DESTROYED(env, sd);
    ErlNifBinary key;
    INSPECT_BINARY_OR_BADARG(env, argv[1], key);
    return shared_dict_delete(sd, key.data, key.size) < 0
        ? enif_make_badarg(env) : ATOM_OK;
}

static ERL_NIF_TERM nif_shared_dict_keys(ErlNifEnv *env, int argc,
                                          const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_shared_dict_t *sd;
    GET_RESOURCE_OR_BADARG(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, &sd);
    CHECK_NOT_DESTROYED(env, sd);
    shared_dict_key_t *keys;
    size_t count;
    int snapshot = shared_dict_snapshot_keys(sd, &keys, &count);
    if (snapshot == -1) return enif_make_badarg(env);
    if (snapshot == -2) return make_error_atom(env, "alloc_failed");
    if (count > UINT_MAX || count > SIZE_MAX / sizeof(ERL_NIF_TERM)) {
        shared_dict_free_keys(keys, count);
        return make_error_atom(env, "too_many_keys");
    }
    ERL_NIF_TERM *terms = count > 0 ? enif_alloc(count * sizeof(*terms)) : NULL;
    if (count > 0 && terms == NULL) {
        shared_dict_free_keys(keys, count);
        return make_error_atom(env, "alloc_failed");
    }
    for (size_t i = 0; i < count; i++) {
        unsigned char *buf = enif_make_new_binary(env, keys[i].len, &terms[i]);
        if (buf == NULL && keys[i].len > 0) {
            enif_free(terms);
            shared_dict_free_keys(keys, count);
            return make_error_atom(env, "alloc_failed");
        }
        if (keys[i].len > 0) memcpy(buf, keys[i].data, keys[i].len);
    }
    ERL_NIF_TERM result = enif_make_list_from_array(env, terms, (unsigned)count);
    enif_free(terms);
    shared_dict_free_keys(keys, count);
    return result;
}

static ERL_NIF_TERM nif_shared_dict_destroy(ErlNifEnv *env, int argc,
                                             const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_shared_dict_t *sd;
    GET_RESOURCE_OR_BADARG(env, argv[0], PY_SHARED_DICT_RESOURCE_TYPE, &sd);
    shared_dict_destroy_contents(sd);
    return ATOM_OK;
}

/* Python methods. The caller already holds its own interpreter's GIL. */
static py_shared_dict_t *get_shared_dict_from_capsule(PyObject *capsule) {
    if (!PyCapsule_IsValid(capsule, "py_shared_dict")) {
        PyErr_SetString(PyExc_TypeError, "Invalid SharedDict handle");
        return NULL;
    }
    py_shared_dict_t *sd = PyCapsule_GetPointer(capsule, "py_shared_dict");
    if (sd != NULL && atomic_load(&sd->destroyed)) {
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }
    return sd;
}

static PyObject *py_shared_dict_get_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    const char *key;
    Py_ssize_t key_len;
    if (!PyArg_ParseTuple(args, "Os#", &handle, &key, &key_len)) return NULL;
    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;
    shared_dict_bytes_t stored;
    int found = shared_dict_copy_value(sd, (unsigned char *)key, (size_t)key_len, &stored);
    if (found == 0) Py_RETURN_NONE;
    if (found == -1) {
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }
    if (found == -2) return PyErr_NoMemory();
    PyObject *value = shared_dict_pickle_loads(stored.data, stored.len);
    enif_free(stored.data);
    return value;
}

static PyObject *py_shared_dict_set_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle, *value;
    const char *key;
    Py_ssize_t key_len;
    if (!PyArg_ParseTuple(args, "Os#O", &handle, &key, &key_len, &value)) return NULL;
    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;
    PyObject *pickled = shared_dict_pickle_dumps(value);
    if (pickled == NULL) return NULL;
    char *data;
    Py_ssize_t len;
    if (PyBytes_AsStringAndSize(pickled, &data, &len) < 0) {
        Py_DECREF(pickled);
        return NULL;
    }
    int result = shared_dict_store(sd, (unsigned char *)key, (size_t)key_len,
                                   (unsigned char *)data, (size_t)len);
    Py_DECREF(pickled);
    if (result == 1) {
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }
    if (result < 0) return PyErr_NoMemory();
    Py_RETURN_NONE;
}

static PyObject *py_shared_dict_del_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    const char *key;
    Py_ssize_t key_len;
    if (!PyArg_ParseTuple(args, "Os#", &handle, &key, &key_len)) return NULL;
    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;
    int result = shared_dict_delete(sd, (unsigned char *)key, (size_t)key_len);
    if (result < 0) {
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }
    return PyBool_FromLong(result);
}

static PyObject *py_shared_dict_contains_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    const char *key;
    Py_ssize_t key_len;
    if (!PyArg_ParseTuple(args, "Os#", &handle, &key, &key_len)) return NULL;
    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;
    pthread_mutex_lock(&sd->mutex);
    bool destroyed = atomic_load(&sd->destroyed);
    bool found = !destroyed && shared_dict_find_locked(
        sd, (unsigned char *)key, (size_t)key_len, NULL) != NULL;
    pthread_mutex_unlock(&sd->mutex);
    if (destroyed) {
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }
    return PyBool_FromLong(found);
}

static PyObject *py_shared_dict_destroy_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *capsule;
    if (!PyArg_ParseTuple(args, "O", &capsule)) return NULL;
    if (!PyCapsule_IsValid(capsule, "py_shared_dict")) {
        PyErr_SetString(PyExc_TypeError, "Invalid SharedDict handle");
        return NULL;
    }
    py_shared_dict_t *sd = PyCapsule_GetPointer(capsule, "py_shared_dict");
    if (sd == NULL) return NULL;
    shared_dict_destroy_contents(sd);
    Py_RETURN_NONE;
}

static PyObject *py_shared_dict_keys_impl(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *handle;
    if (!PyArg_ParseTuple(args, "O", &handle)) return NULL;
    py_shared_dict_t *sd = get_shared_dict_from_capsule(handle);
    if (sd == NULL) return NULL;
    shared_dict_key_t *keys;
    size_t count;
    int snapshot = shared_dict_snapshot_keys(sd, &keys, &count);
    if (snapshot == -1) {
        PyErr_SetString(PyExc_RuntimeError, "SharedDict has been destroyed");
        return NULL;
    }
    if (snapshot == -2) return PyErr_NoMemory();
    if (count > (size_t)PY_SSIZE_T_MAX) {
        shared_dict_free_keys(keys, count);
        return PyErr_NoMemory();
    }
    PyObject *result = PyList_New((Py_ssize_t)count);
    if (result == NULL) {
        shared_dict_free_keys(keys, count);
        return NULL;
    }
    for (size_t i = 0; i < count; i++) {
        PyObject *key = PyUnicode_DecodeUTF8(
            (const char *)keys[i].data, (Py_ssize_t)keys[i].len, "replace");
        if (key == NULL) {
            Py_DECREF(result);
            shared_dict_free_keys(keys, count);
            return NULL;
        }
        PyList_SET_ITEM(result, (Py_ssize_t)i, key);
    }
    shared_dict_free_keys(keys, count);
    return result;
}

#define PY_SHARED_DICT_NIFS \
    {"shared_dict_new", 0, nif_shared_dict_new, 0}, \
    {"shared_dict_get", 3, nif_shared_dict_get, ERL_NIF_DIRTY_JOB_IO_BOUND}, \
    {"shared_dict_set", 3, nif_shared_dict_set, ERL_NIF_DIRTY_JOB_IO_BOUND}, \
    {"shared_dict_del", 2, nif_shared_dict_del, ERL_NIF_DIRTY_JOB_IO_BOUND}, \
    {"shared_dict_keys", 1, nif_shared_dict_keys, ERL_NIF_DIRTY_JOB_IO_BOUND}, \
    {"shared_dict_destroy", 1, nif_shared_dict_destroy, ERL_NIF_DIRTY_JOB_IO_BOUND}
