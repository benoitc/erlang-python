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
 * @file py_reactor_buffer.c
 * @brief Zero-copy buffer implementation for reactor protocol layer
 */

#include "py_reactor_buffer.h"
#include <unistd.h>
#include <errno.h>
#include <string.h>

/* Resource type - initialized in py_nif.c */
ErlNifResourceType *REACTOR_BUFFER_RESOURCE_TYPE = NULL;

/* ============================================================================
 * Resource Management
 * ============================================================================ */

void reactor_buffer_resource_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    reactor_buffer_resource_t *buf = (reactor_buffer_resource_t *)obj;
    if (buf->data != NULL) {
        enif_free(buf->data);
        buf->data = NULL;
    }
}

reactor_buffer_resource_t *reactor_buffer_alloc(size_t capacity) {
    if (REACTOR_BUFFER_RESOURCE_TYPE == NULL) {
        return NULL;
    }

    reactor_buffer_resource_t *resource = enif_alloc_resource(
        REACTOR_BUFFER_RESOURCE_TYPE, sizeof(reactor_buffer_resource_t));
    if (resource == NULL) {
        return NULL;
    }

    resource->data = enif_alloc(capacity);
    if (resource->data == NULL) {
        enif_release_resource(resource);
        return NULL;
    }

    resource->size = 0;
    resource->capacity = capacity;
    resource->ref_count = 0;

    return resource;
}

int reactor_buffer_read_fd(int fd, size_t max_size,
                           reactor_buffer_resource_t **out_resource,
                           size_t *out_size) {
    if (max_size > REACTOR_MAX_READ_SIZE) {
        max_size = REACTOR_MAX_READ_SIZE;
    }

    reactor_buffer_resource_t *resource = reactor_buffer_alloc(max_size);
    if (resource == NULL) {
        return -1;
    }

    ssize_t n = read(fd, resource->data, max_size);
    if (n < 0) {
        enif_release_resource(resource);
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            *out_resource = NULL;
            *out_size = 0;
            return 0;  /* Would block - not an error */
        }
        return -1;  /* Error */
    }

    if (n == 0) {
        enif_release_resource(resource);
        *out_resource = NULL;
        *out_size = 0;
        return 1;  /* EOF */
    }

    resource->size = (size_t)n;
    *out_resource = resource;
    *out_size = (size_t)n;
    return 0;
}

/* ============================================================================
 * Python Buffer Protocol
 * ============================================================================ */

static void ReactorBuffer_releasebuffer(PyObject *obj, Py_buffer *view) {
    (void)view;
    ReactorBufferObject *self = (ReactorBufferObject *)obj;
    if (self->resource != NULL) {
        self->resource->ref_count--;
    }
}

static int ReactorBuffer_getbuffer(PyObject *obj, Py_buffer *view, int flags) {
    ReactorBufferObject *self = (ReactorBufferObject *)obj;

    if (self->resource == NULL || self->resource->data == NULL) {
        PyErr_SetString(PyExc_BufferError, "Buffer has been released");
        return -1;
    }

    view->obj = obj;
    view->buf = self->resource->data;
    view->len = self->resource->size;
    view->readonly = 1;
    view->itemsize = 1;
    view->format = (flags & PyBUF_FORMAT) ? "B" : NULL;
    view->ndim = 1;
    view->shape = (flags & PyBUF_ND) ? &view->len : NULL;
    view->strides = (flags & PyBUF_STRIDES) ? &view->itemsize : NULL;
    view->suboffsets = NULL;
    view->internal = NULL;

    self->resource->ref_count++;
    Py_INCREF(obj);

    return 0;
}

static PyBufferProcs ReactorBuffer_as_buffer = {
    .bf_getbuffer = ReactorBuffer_getbuffer,
    .bf_releasebuffer = ReactorBuffer_releasebuffer,
};

/* ============================================================================
 * Cached Memoryview Helper
 * ============================================================================ */

/**
 * Get or create the cached memoryview for fast buffer operations.
 * The memoryview is created lazily and cached for subsequent accesses.
 */
static PyObject *ReactorBuffer_get_memoryview(ReactorBufferObject *self) {
    if (self->cached_memoryview == NULL) {
        if (self->resource == NULL || self->resource->data == NULL) {
            PyErr_SetString(PyExc_BufferError, "Buffer has been released");
            return NULL;
        }
        /* Create memoryview from self (uses our buffer protocol) */
        self->cached_memoryview = PyMemoryView_FromObject((PyObject *)self);
        if (self->cached_memoryview == NULL) {
            return NULL;
        }
    }
    Py_INCREF(self->cached_memoryview);
    return self->cached_memoryview;
}

/* ============================================================================
 * Python Sequence Protocol
 * ============================================================================ */

static Py_ssize_t ReactorBuffer_length(ReactorBufferObject *self) {
    if (self->resource == NULL) {
        return 0;
    }
    return (Py_ssize_t)self->resource->size;
}

static PyObject *ReactorBuffer_item(ReactorBufferObject *self, Py_ssize_t i) {
    if (self->resource == NULL || self->resource->data == NULL) {
        PyErr_SetString(PyExc_IndexError, "Buffer has been released");
        return NULL;
    }

    if (i < 0) {
        i += self->resource->size;
    }

    if (i < 0 || (size_t)i >= self->resource->size) {
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }

    return PyLong_FromLong(self->resource->data[i]);
}

static PyObject *ReactorBuffer_subscript(ReactorBufferObject *self, PyObject *key) {
    if (self->resource == NULL || self->resource->data == NULL) {
        PyErr_SetString(PyExc_IndexError, "Buffer has been released");
        return NULL;
    }

    if (PyLong_Check(key)) {
        Py_ssize_t i = PyLong_AsSsize_t(key);
        if (i == -1 && PyErr_Occurred()) {
            return NULL;
        }
        return ReactorBuffer_item(self, i);
    }

    if (PySlice_Check(key)) {
        Py_ssize_t start, stop, step, slicelength;
        if (PySlice_GetIndicesEx(key, self->resource->size,
                                  &start, &stop, &step, &slicelength) < 0) {
            return NULL;
        }

        if (step == 1) {
            /* Contiguous slice - return bytes directly */
            return PyBytes_FromStringAndSize(
                (char *)self->resource->data + start, slicelength);
        }

        /* Non-contiguous slice - build bytes */
        PyObject *result = PyBytes_FromStringAndSize(NULL, slicelength);
        if (result == NULL) {
            return NULL;
        }
        char *dest = PyBytes_AS_STRING(result);
        for (Py_ssize_t i = 0, j = start; i < slicelength; i++, j += step) {
            dest[i] = self->resource->data[j];
        }
        return result;
    }

    PyErr_SetString(PyExc_TypeError, "indices must be integers or slices");
    return NULL;
}

static PySequenceMethods ReactorBuffer_as_sequence = {
    .sq_length = (lenfunc)ReactorBuffer_length,
    .sq_item = (ssizeargfunc)ReactorBuffer_item,
};

static PyMappingMethods ReactorBuffer_as_mapping = {
    .mp_length = (lenfunc)ReactorBuffer_length,
    .mp_subscript = (binaryfunc)ReactorBuffer_subscript,
};

/* ============================================================================
 * Python Methods
 * ============================================================================ */

static void ReactorBuffer_dealloc(ReactorBufferObject *self) {
    Py_CLEAR(self->cached_memoryview);
    if (self->resource_ref != NULL) {
        enif_release_resource(self->resource_ref);
        self->resource_ref = NULL;
        self->resource = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *ReactorBuffer_bytes(ReactorBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->resource == NULL || self->resource->data == NULL) {
        return PyBytes_FromStringAndSize("", 0);
    }
    return PyBytes_FromStringAndSize((char *)self->resource->data,
                                      self->resource->size);
}

/* Return memoryview for zero-copy access */
static PyObject *ReactorBuffer_memoryview(ReactorBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    return ReactorBuffer_get_memoryview(self);
}

static PyObject *ReactorBuffer_repr(ReactorBufferObject *self) {
    if (self->resource == NULL) {
        return PyUnicode_FromString("<ReactorBuffer (released)>");
    }
    return PyUnicode_FromFormat("<ReactorBuffer size=%zu>", self->resource->size);
}

/* Bytes-like method: startswith */
static PyObject *ReactorBuffer_startswith(ReactorBufferObject *self, PyObject *args) {
    PyObject *prefix;
    Py_ssize_t start = 0;
    Py_ssize_t end = PY_SSIZE_T_MAX;

    if (!PyArg_ParseTuple(args, "O|nn:startswith", &prefix, &start, &end)) {
        return NULL;
    }

    if (self->resource == NULL || self->resource->data == NULL) {
        Py_RETURN_FALSE;
    }

    Py_buffer prefix_buf;
    if (PyObject_GetBuffer(prefix, &prefix_buf, PyBUF_SIMPLE) < 0) {
        return NULL;
    }

    Py_ssize_t size = self->resource->size;
    if (start < 0) start += size;
    if (start < 0) start = 0;
    if (end < 0) end += size;
    if (end > size) end = size;
    if (start > end) {
        PyBuffer_Release(&prefix_buf);
        Py_RETURN_FALSE;
    }

    Py_ssize_t available = end - start;
    int result = 0;
    if (prefix_buf.len <= available) {
        result = memcmp(self->resource->data + start, prefix_buf.buf,
                        prefix_buf.len) == 0;
    }

    PyBuffer_Release(&prefix_buf);
    return PyBool_FromLong(result);
}

/* Bytes-like method: endswith */
static PyObject *ReactorBuffer_endswith(ReactorBufferObject *self, PyObject *args) {
    PyObject *suffix;
    Py_ssize_t start = 0;
    Py_ssize_t end = PY_SSIZE_T_MAX;

    if (!PyArg_ParseTuple(args, "O|nn:endswith", &suffix, &start, &end)) {
        return NULL;
    }

    if (self->resource == NULL || self->resource->data == NULL) {
        Py_RETURN_FALSE;
    }

    Py_buffer suffix_buf;
    if (PyObject_GetBuffer(suffix, &suffix_buf, PyBUF_SIMPLE) < 0) {
        return NULL;
    }

    Py_ssize_t size = self->resource->size;
    if (start < 0) start += size;
    if (start < 0) start = 0;
    if (end < 0) end += size;
    if (end > size) end = size;
    if (start > end) {
        PyBuffer_Release(&suffix_buf);
        Py_RETURN_FALSE;
    }

    Py_ssize_t available = end - start;
    int result = 0;
    if (suffix_buf.len <= available) {
        Py_ssize_t offset = end - suffix_buf.len;
        if (offset >= start) {
            result = memcmp(self->resource->data + offset, suffix_buf.buf,
                            suffix_buf.len) == 0;
        }
    }

    PyBuffer_Release(&suffix_buf);
    return PyBool_FromLong(result);
}

/* Bytes-like method: find */
static PyObject *ReactorBuffer_find(ReactorBufferObject *self, PyObject *args) {
    PyObject *sub;
    Py_ssize_t start = 0;
    Py_ssize_t end = PY_SSIZE_T_MAX;

    if (!PyArg_ParseTuple(args, "O|nn:find", &sub, &start, &end)) {
        return NULL;
    }

    if (self->resource == NULL || self->resource->data == NULL) {
        return PyLong_FromLong(-1);
    }

    Py_buffer sub_buf;
    if (PyObject_GetBuffer(sub, &sub_buf, PyBUF_SIMPLE) < 0) {
        return NULL;
    }

    Py_ssize_t size = self->resource->size;
    if (start < 0) start += size;
    if (start < 0) start = 0;
    if (end < 0) end += size;
    if (end > size) end = size;

    Py_ssize_t result = -1;
    if (start <= end && sub_buf.len <= (end - start)) {
        /* Simple search - for small patterns memmem isn't always available */
        const unsigned char *haystack = self->resource->data + start;
        Py_ssize_t haystack_len = end - start;
        const unsigned char *needle = sub_buf.buf;
        Py_ssize_t needle_len = sub_buf.len;

        if (needle_len == 0) {
            result = start;
        } else {
            for (Py_ssize_t i = 0; i <= haystack_len - needle_len; i++) {
                if (memcmp(haystack + i, needle, needle_len) == 0) {
                    result = start + i;
                    break;
                }
            }
        }
    }

    PyBuffer_Release(&sub_buf);
    return PyLong_FromSsize_t(result);
}

/* Bytes-like method: rfind */
static PyObject *ReactorBuffer_rfind(ReactorBufferObject *self, PyObject *args) {
    PyObject *sub;
    Py_ssize_t start = 0;
    Py_ssize_t end = PY_SSIZE_T_MAX;

    if (!PyArg_ParseTuple(args, "O|nn:rfind", &sub, &start, &end)) {
        return NULL;
    }

    if (self->resource == NULL || self->resource->data == NULL) {
        return PyLong_FromLong(-1);
    }

    Py_buffer sub_buf;
    if (PyObject_GetBuffer(sub, &sub_buf, PyBUF_SIMPLE) < 0) {
        return NULL;
    }

    Py_ssize_t size = self->resource->size;
    if (start < 0) start += size;
    if (start < 0) start = 0;
    if (end < 0) end += size;
    if (end > size) end = size;

    Py_ssize_t result = -1;
    if (start <= end && sub_buf.len <= (end - start)) {
        const unsigned char *haystack = self->resource->data + start;
        Py_ssize_t haystack_len = end - start;
        const unsigned char *needle = sub_buf.buf;
        Py_ssize_t needle_len = sub_buf.len;

        if (needle_len == 0) {
            result = end;
        } else {
            for (Py_ssize_t i = haystack_len - needle_len; i >= 0; i--) {
                if (memcmp(haystack + i, needle, needle_len) == 0) {
                    result = start + i;
                    break;
                }
            }
        }
    }

    PyBuffer_Release(&sub_buf);
    return PyLong_FromSsize_t(result);
}

/* Bytes-like method: index */
static PyObject *ReactorBuffer_index(ReactorBufferObject *self, PyObject *args) {
    PyObject *result = ReactorBuffer_find(self, args);
    if (result == NULL) {
        return NULL;
    }

    Py_ssize_t pos = PyLong_AsSsize_t(result);
    if (pos == -1 && !PyErr_Occurred()) {
        Py_DECREF(result);
        PyErr_SetString(PyExc_ValueError, "subsection not found");
        return NULL;
    }

    return result;
}

/* Bytes-like method: count */
static PyObject *ReactorBuffer_count(ReactorBufferObject *self, PyObject *args) {
    PyObject *sub;
    Py_ssize_t start = 0;
    Py_ssize_t end = PY_SSIZE_T_MAX;

    if (!PyArg_ParseTuple(args, "O|nn:count", &sub, &start, &end)) {
        return NULL;
    }

    if (self->resource == NULL || self->resource->data == NULL) {
        return PyLong_FromLong(0);
    }

    Py_buffer sub_buf;
    if (PyObject_GetBuffer(sub, &sub_buf, PyBUF_SIMPLE) < 0) {
        return NULL;
    }

    Py_ssize_t size = self->resource->size;
    if (start < 0) start += size;
    if (start < 0) start = 0;
    if (end < 0) end += size;
    if (end > size) end = size;

    Py_ssize_t count = 0;
    if (start <= end && sub_buf.len > 0 && sub_buf.len <= (end - start)) {
        const unsigned char *haystack = self->resource->data + start;
        Py_ssize_t haystack_len = end - start;
        const unsigned char *needle = sub_buf.buf;
        Py_ssize_t needle_len = sub_buf.len;

        for (Py_ssize_t i = 0; i <= haystack_len - needle_len; i++) {
            if (memcmp(haystack + i, needle, needle_len) == 0) {
                count++;
                i += needle_len - 1;  /* Non-overlapping */
            }
        }
    } else if (sub_buf.len == 0 && start <= end) {
        /* Empty pattern matches between every character + at start/end */
        count = (end - start) + 1;
    }

    PyBuffer_Release(&sub_buf);
    return PyLong_FromSsize_t(count);
}

/* Bytes-like method: decode */
static PyObject *ReactorBuffer_decode(ReactorBufferObject *self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"encoding", "errors", NULL};
    const char *encoding = "utf-8";
    const char *errors = "strict";

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ss:decode", kwlist,
                                      &encoding, &errors)) {
        return NULL;
    }

    if (self->resource == NULL || self->resource->data == NULL) {
        return PyUnicode_FromStringAndSize("", 0);
    }

    return PyUnicode_Decode((char *)self->resource->data, self->resource->size,
                            encoding, errors);
}

/* Bytes-like method: split */
static PyObject *ReactorBuffer_split(ReactorBufferObject *self, PyObject *args, PyObject *kwargs) {
    /* Delegate to bytes.split by creating a bytes object */
    PyObject *bytes_obj = ReactorBuffer_bytes(self, NULL);
    if (bytes_obj == NULL) {
        return NULL;
    }

    PyObject *result = PyObject_Call(
        PyObject_GetAttrString(bytes_obj, "split"), args, kwargs);
    Py_DECREF(bytes_obj);
    return result;
}

/* Bytes-like method: strip */
static PyObject *ReactorBuffer_strip(ReactorBufferObject *self, PyObject *args) {
    PyObject *bytes_obj = ReactorBuffer_bytes(self, NULL);
    if (bytes_obj == NULL) {
        return NULL;
    }

    PyObject *result = PyObject_CallMethod(bytes_obj, "strip", "O", args);
    Py_DECREF(bytes_obj);
    return result;
}

/* Rich comparison (for == and other comparisons with bytes) */
static PyObject *ReactorBuffer_richcompare(PyObject *self, PyObject *other, int op) {
    ReactorBufferObject *buf = (ReactorBufferObject *)self;

    if (buf->resource == NULL || buf->resource->data == NULL) {
        if (op == Py_EQ) {
            Py_RETURN_FALSE;
        } else if (op == Py_NE) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_NOTIMPLEMENTED;
    }

    Py_buffer other_buf;
    if (PyObject_GetBuffer(other, &other_buf, PyBUF_SIMPLE) < 0) {
        PyErr_Clear();
        if (op == Py_EQ) {
            Py_RETURN_FALSE;
        } else if (op == Py_NE) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_NOTIMPLEMENTED;
    }

    int cmp;
    if (buf->resource->size != (size_t)other_buf.len) {
        cmp = (buf->resource->size < (size_t)other_buf.len) ? -1 : 1;
    } else {
        cmp = memcmp(buf->resource->data, other_buf.buf, buf->resource->size);
    }

    PyBuffer_Release(&other_buf);

    switch (op) {
        case Py_LT: return PyBool_FromLong(cmp < 0);
        case Py_LE: return PyBool_FromLong(cmp <= 0);
        case Py_EQ: return PyBool_FromLong(cmp == 0);
        case Py_NE: return PyBool_FromLong(cmp != 0);
        case Py_GT: return PyBool_FromLong(cmp > 0);
        case Py_GE: return PyBool_FromLong(cmp >= 0);
        default: Py_RETURN_NOTIMPLEMENTED;
    }
}

/* Hash function (compatible with bytes) */
static Py_hash_t ReactorBuffer_hash(ReactorBufferObject *self) {
    if (self->resource == NULL || self->resource->data == NULL) {
        return 0;
    }
    /* Use the same hash as bytes */
    PyObject *bytes = ReactorBuffer_bytes(self, NULL);
    if (bytes == NULL) {
        return -1;
    }
    Py_hash_t hash = PyObject_Hash(bytes);
    Py_DECREF(bytes);
    return hash;
}

/* Contains check for 'in' operator */
static int ReactorBuffer_contains(ReactorBufferObject *self, PyObject *arg) {
    if (self->resource == NULL || self->resource->data == NULL) {
        return 0;
    }

    Py_buffer sub_buf;
    if (PyObject_GetBuffer(arg, &sub_buf, PyBUF_SIMPLE) < 0) {
        return -1;
    }

    int result = 0;
    if (sub_buf.len == 0) {
        result = 1;
    } else if ((size_t)sub_buf.len <= self->resource->size) {
        const unsigned char *haystack = self->resource->data;
        size_t haystack_len = self->resource->size;
        const unsigned char *needle = sub_buf.buf;
        size_t needle_len = sub_buf.len;

        for (size_t i = 0; i <= haystack_len - needle_len; i++) {
            if (memcmp(haystack + i, needle, needle_len) == 0) {
                result = 1;
                break;
            }
        }
    }

    PyBuffer_Release(&sub_buf);
    return result;
}

/* Test helper to create a buffer for testing */
static PyObject *ReactorBuffer_test_create(PyTypeObject *type, PyObject *args) {
    (void)type;
    Py_buffer data_buf;

    if (!PyArg_ParseTuple(args, "y*:_test_create", &data_buf)) {
        return NULL;
    }

    reactor_buffer_resource_t *resource = reactor_buffer_alloc(data_buf.len);
    if (resource == NULL) {
        PyBuffer_Release(&data_buf);
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate buffer");
        return NULL;
    }

    memcpy(resource->data, data_buf.buf, data_buf.len);
    resource->size = data_buf.len;
    PyBuffer_Release(&data_buf);

    PyObject *result = ReactorBuffer_from_resource(resource, resource);
    /* from_resource does enif_keep_resource, so we release our reference */
    enif_release_resource(resource);

    return result;
}

static PyMethodDef ReactorBuffer_methods[] = {
    {"__bytes__", (PyCFunction)ReactorBuffer_bytes, METH_NOARGS,
     "Return bytes copy of buffer"},
    {"memoryview", (PyCFunction)ReactorBuffer_memoryview, METH_NOARGS,
     "Return a memoryview for zero-copy access"},
    {"startswith", (PyCFunction)ReactorBuffer_startswith, METH_VARARGS,
     "Return True if buffer starts with prefix"},
    {"endswith", (PyCFunction)ReactorBuffer_endswith, METH_VARARGS,
     "Return True if buffer ends with suffix"},
    {"find", (PyCFunction)ReactorBuffer_find, METH_VARARGS,
     "Return lowest index of substring, or -1 if not found"},
    {"rfind", (PyCFunction)ReactorBuffer_rfind, METH_VARARGS,
     "Return highest index of substring, or -1 if not found"},
    {"index", (PyCFunction)ReactorBuffer_index, METH_VARARGS,
     "Return lowest index of substring; raise ValueError if not found"},
    {"count", (PyCFunction)ReactorBuffer_count, METH_VARARGS,
     "Return number of non-overlapping occurrences"},
    {"decode", (PyCFunction)ReactorBuffer_decode, METH_VARARGS | METH_KEYWORDS,
     "Decode the buffer using the specified encoding"},
    {"split", (PyCFunction)ReactorBuffer_split, METH_VARARGS | METH_KEYWORDS,
     "Split the buffer"},
    {"strip", (PyCFunction)ReactorBuffer_strip, METH_VARARGS,
     "Strip whitespace or specified bytes"},
    {"_test_create", (PyCFunction)ReactorBuffer_test_create,
     METH_VARARGS | METH_CLASS,
     "Create a ReactorBuffer for testing (internal use)"},
    {NULL}
};

/* Extend sequence methods with contains */
static PySequenceMethods ReactorBuffer_as_sequence_full = {
    .sq_length = (lenfunc)ReactorBuffer_length,
    .sq_item = (ssizeargfunc)ReactorBuffer_item,
    .sq_contains = (objobjproc)ReactorBuffer_contains,
};

PyTypeObject ReactorBufferType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "erlang.reactor.ReactorBuffer",
    .tp_doc = "Zero-copy reactor read buffer",
    .tp_basicsize = sizeof(ReactorBufferObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)ReactorBuffer_dealloc,
    .tp_repr = (reprfunc)ReactorBuffer_repr,
    .tp_as_buffer = &ReactorBuffer_as_buffer,
    .tp_as_sequence = &ReactorBuffer_as_sequence_full,
    .tp_as_mapping = &ReactorBuffer_as_mapping,
    .tp_hash = (hashfunc)ReactorBuffer_hash,
    .tp_richcompare = ReactorBuffer_richcompare,
    .tp_methods = ReactorBuffer_methods,
};

/* ============================================================================
 * Initialization
 * ============================================================================ */

PyObject *ReactorBuffer_from_resource(reactor_buffer_resource_t *resource,
                                       void *resource_ref) {
    ReactorBufferObject *obj = PyObject_New(ReactorBufferObject, &ReactorBufferType);
    if (obj == NULL) {
        return NULL;
    }

    obj->resource = resource;
    obj->resource_ref = resource_ref;
    obj->cached_memoryview = NULL;  /* Created lazily on first use */
    enif_keep_resource(resource_ref);

    return (PyObject *)obj;
}

int ReactorBuffer_init_type(void) {
    if (PyType_Ready(&ReactorBufferType) < 0) {
        return -1;
    }
    return 0;
}

int ReactorBuffer_register_with_reactor(void) {
    /* Import erlang module (created in py_callback.c) */
    PyObject *erlang_module = PyImport_ImportModule("erlang");
    if (erlang_module == NULL) {
        /* Module doesn't exist yet - this shouldn't happen */
        PyErr_Clear();
        return -1;
    }

    /* Add ReactorBuffer type to the erlang module */
    Py_INCREF(&ReactorBufferType);
    if (PyModule_AddObject(erlang_module, "ReactorBuffer",
                           (PyObject *)&ReactorBufferType) < 0) {
        Py_DECREF(&ReactorBufferType);
        Py_DECREF(erlang_module);
        return -1;
    }

    Py_DECREF(erlang_module);
    return 0;
}
