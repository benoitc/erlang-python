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
 * @file py_buffer.c
 * @brief Zero-copy WSGI input buffer implementation
 *
 * This module provides a PyBuffer Python type for zero-copy access to HTTP
 * request bodies in WSGI applications. Data is written by Erlang and read
 * by Python with file-like semantics.
 *
 * Key optimization techniques:
 * - Zero-copy via buffer protocol (memoryview access)
 * - memmem/memchr for efficient line scanning
 * - GIL release during blocking waits
 * - No data copying for readline when possible
 */

/* Enable memmem on Linux (it's a GNU extension) */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "py_buffer.h"
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>

/* Portable memmem fallback for systems that don't have it.
 * memmem is available on: Linux (glibc), macOS, FreeBSD >= 13
 * Not available on: older BSDs, some embedded systems */
#if !defined(__GLIBC__) && !defined(__APPLE__) && !defined(__FreeBSD__)
static void *portable_memmem(const void *haystack, size_t haystacklen,
                              const void *needle, size_t needlelen) {
    if (needlelen == 0) return (void *)haystack;
    if (needlelen > haystacklen) return NULL;

    const unsigned char *h = haystack;
    const unsigned char *n = needle;
    const unsigned char *end = h + haystacklen - needlelen + 1;

    while (h < end) {
        h = memchr(h, n[0], end - h);
        if (h == NULL) return NULL;
        if (memcmp(h, n, needlelen) == 0) return (void *)h;
        h++;
    }
    return NULL;
}
#define memmem portable_memmem
#endif

/* Resource type - initialized in py_nif.c */
ErlNifResourceType *PY_BUFFER_RESOURCE_TYPE = NULL;

/* ============================================================================
 * Resource Management
 * ============================================================================ */

void py_buffer_resource_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_buffer_resource_t *buf = (py_buffer_resource_t *)obj;

    pthread_mutex_destroy(&buf->mutex);
    pthread_cond_destroy(&buf->data_ready);

    if (buf->data != NULL) {
        enif_free(buf->data);
        buf->data = NULL;
    }
}

py_buffer_resource_t *py_buffer_alloc(ssize_t content_length) {
    if (PY_BUFFER_RESOURCE_TYPE == NULL) {
        return NULL;
    }

    py_buffer_resource_t *resource = enif_alloc_resource(
        PY_BUFFER_RESOURCE_TYPE, sizeof(py_buffer_resource_t));
    if (resource == NULL) {
        return NULL;
    }

    /* Determine initial capacity */
    size_t capacity;
    if (content_length > 0) {
        capacity = (size_t)content_length;
    } else {
        capacity = PY_BUFFER_DEFAULT_CAPACITY;
    }

    resource->data = enif_alloc(capacity);
    if (resource->data == NULL) {
        enif_release_resource(resource);
        return NULL;
    }

    resource->capacity = capacity;
    resource->write_pos = 0;
    resource->read_pos = 0;
    resource->content_length = content_length;
    resource->eof = false;
    resource->closed = false;
    resource->view_count = 0;

    if (pthread_mutex_init(&resource->mutex, NULL) != 0) {
        enif_free(resource->data);
        enif_release_resource(resource);
        return NULL;
    }

    if (pthread_cond_init(&resource->data_ready, NULL) != 0) {
        pthread_mutex_destroy(&resource->mutex);
        enif_free(resource->data);
        enif_release_resource(resource);
        return NULL;
    }

    return resource;
}

int py_buffer_write(py_buffer_resource_t *buf, const unsigned char *data, size_t size) {
    if (size == 0) return 0;

    pthread_mutex_lock(&buf->mutex);

    if (buf->closed) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }

    /* Check for overflow */
    if (size > SIZE_MAX - buf->write_pos) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }

    /* Check if we need to grow the buffer */
    size_t required = buf->write_pos + size;
    if (required > buf->capacity) {
        /* Calculate new capacity */
        size_t new_capacity = buf->capacity;
        while (new_capacity < required) {
            new_capacity *= PY_BUFFER_GROW_FACTOR;
        }

        unsigned char *new_data = enif_alloc(new_capacity);
        if (new_data == NULL) {
            pthread_mutex_unlock(&buf->mutex);
            return -1;
        }

        /* Copy existing data */
        if (buf->write_pos > 0) {
            memcpy(new_data, buf->data, buf->write_pos);
        }

        enif_free(buf->data);
        buf->data = new_data;
        buf->capacity = new_capacity;
    }

    /* Append new data */
    memcpy(buf->data + buf->write_pos, data, size);
    buf->write_pos += size;

    /* Signal waiting readers */
    pthread_cond_broadcast(&buf->data_ready);

    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

void py_buffer_close(py_buffer_resource_t *buf) {
    pthread_mutex_lock(&buf->mutex);
    buf->eof = true;
    /* Wake up all waiting readers */
    pthread_cond_broadcast(&buf->data_ready);
    pthread_mutex_unlock(&buf->mutex);
}

/* ============================================================================
 * Internal Helper Functions
 * ============================================================================ */

/**
 * @brief Find newline in buffer using memchr (fast single-byte search)
 *
 * @param data Buffer data
 * @param size Buffer size
 * @return Pointer to newline, or NULL if not found
 */
static inline const unsigned char *find_newline(const unsigned char *data, size_t size) {
    return memchr(data, '\n', size);
}


/* ============================================================================
 * Python Buffer Protocol
 * ============================================================================ */

static void PyBuffer_releasebuffer(PyObject *obj, Py_buffer *view) {
    (void)view;
    PyBufferObject *self = (PyBufferObject *)obj;
    if (self->resource != NULL) {
        pthread_mutex_lock(&self->resource->mutex);
        self->resource->view_count--;
        pthread_mutex_unlock(&self->resource->mutex);
    }
}

static int PyBuffer_getbuffer(PyObject *obj, Py_buffer *view, int flags) {
    PyBufferObject *self = (PyBufferObject *)obj;

    if (self->resource == NULL || self->resource->data == NULL) {
        PyErr_SetString(PyExc_BufferError, "Buffer has been released");
        return -1;
    }

    py_buffer_resource_t *buf = self->resource;

    pthread_mutex_lock(&buf->mutex);

    /* Expose only written-but-not-read data */
    size_t available = buf->write_pos - buf->read_pos;

    view->obj = obj;
    view->buf = buf->data + buf->read_pos;
    view->len = available;
    view->readonly = 1;
    view->itemsize = 1;
    view->format = (flags & PyBUF_FORMAT) ? "B" : NULL;
    view->ndim = 1;
    view->shape = (flags & PyBUF_ND) ? &view->len : NULL;
    view->strides = (flags & PyBUF_STRIDES) ? &view->itemsize : NULL;
    view->suboffsets = NULL;
    view->internal = NULL;

    buf->view_count++;

    pthread_mutex_unlock(&buf->mutex);

    Py_INCREF(obj);
    return 0;
}

static PyBufferProcs PyBuffer_as_buffer = {
    .bf_getbuffer = PyBuffer_getbuffer,
    .bf_releasebuffer = PyBuffer_releasebuffer,
};

/* ============================================================================
 * Python Methods
 * ============================================================================ */

static void PyBuffer_dealloc(PyBufferObject *self) {
    if (self->resource_ref != NULL) {
        enif_release_resource(self->resource_ref);
        self->resource_ref = NULL;
        self->resource = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/**
 * @brief read(size=-1) - Read up to size bytes, blocking if needed
 *
 * If size is -1, read all available data (until EOF).
 * Returns bytes object. Returns empty bytes at EOF.
 *
 * GIL Optimization: All blocking waits and data copying happen in a single
 * GIL-released block to avoid unnecessary GIL roundtrips.
 */
static PyObject *PyBuffer_read(PyBufferObject *self, PyObject *args) {
    Py_ssize_t size = -1;
    if (!PyArg_ParseTuple(args, "|n", &size)) return NULL;

    if (self->resource == NULL) {
        PyErr_SetString(PyExc_ValueError, "I/O operation on closed buffer");
        return NULL;
    }

    py_buffer_resource_t *buf = self->resource;

    /* Local variables for results - set inside GIL-released block */
    unsigned char *local_copy = NULL;
    size_t to_read = 0;
    int closed_empty = 0;
    int oom_error = 0;

    /* Release GIL for all blocking waits and data copying */
    Py_BEGIN_ALLOW_THREADS
    pthread_mutex_lock(&buf->mutex);

    if (size < 0) {
        /* Read all: wait for all content or EOF */
        if (buf->content_length > 0) {
            /* Known content length - wait for complete data */
            while (buf->write_pos < (size_t)buf->content_length &&
                   !buf->eof && !buf->closed) {
                pthread_cond_wait(&buf->data_ready, &buf->mutex);
            }
        } else {
            /* Unknown length - wait for any data or EOF */
            while (buf->read_pos >= buf->write_pos && !buf->eof && !buf->closed) {
                pthread_cond_wait(&buf->data_ready, &buf->mutex);
            }
        }
    } else {
        /* Read specific amount: wait for any data */
        while (buf->read_pos >= buf->write_pos && !buf->eof && !buf->closed) {
            pthread_cond_wait(&buf->data_ready, &buf->mutex);
        }
    }

    /* Calculate result while holding mutex */
    if (buf->closed && buf->read_pos >= buf->write_pos) {
        closed_empty = 1;
    } else {
        size_t available = buf->write_pos - buf->read_pos;
        to_read = (size < 0) ? available :
                  ((available < (size_t)size) ? available : (size_t)size);

        /* Copy data while holding mutex */
        if (to_read > 0) {
            local_copy = (unsigned char *)malloc(to_read);
            if (local_copy == NULL) {
                oom_error = 1;
            } else {
                memcpy(local_copy, buf->data + buf->read_pos, to_read);
                buf->read_pos += to_read;
            }
        }
    }

    pthread_mutex_unlock(&buf->mutex);
    Py_END_ALLOW_THREADS

    /* Create Python objects with GIL held */
    if (oom_error) {
        PyErr_NoMemory();
        return NULL;
    }

    if (closed_empty || to_read == 0) {
        return PyBytes_FromStringAndSize("", 0);
    }

    PyObject *result = PyBytes_FromStringAndSize((char *)local_copy, to_read);
    free(local_copy);
    return result;
}

/**
 * @brief read_nonblock(size=-1) - Read available bytes without blocking
 *
 * Returns immediately with whatever data is available.
 * If no data available and not at EOF, returns empty bytes (not None).
 * Use readable_amount() to check if data is available first.
 */
static PyObject *PyBuffer_read_nonblock(PyBufferObject *self, PyObject *args) {
    Py_ssize_t size = -1;
    if (!PyArg_ParseTuple(args, "|n", &size)) return NULL;

    if (self->resource == NULL) {
        PyErr_SetString(PyExc_ValueError, "I/O operation on closed buffer");
        return NULL;
    }

    py_buffer_resource_t *buf = self->resource;
    unsigned char *local_copy = NULL;
    size_t to_read = 0;

    pthread_mutex_lock(&buf->mutex);

    /* Calculate available data */
    size_t available = buf->write_pos - buf->read_pos;

    /* Determine how much to read */
    if (size < 0) {
        to_read = available;
    } else {
        to_read = (available < (size_t)size) ? available : (size_t)size;
    }

    /* Copy data while holding mutex */
    if (to_read > 0) {
        local_copy = (unsigned char *)malloc(to_read);
        if (local_copy != NULL) {
            memcpy(local_copy, buf->data + buf->read_pos, to_read);
            buf->read_pos += to_read;
        }
    }

    pthread_mutex_unlock(&buf->mutex);

    /* Create PyBytes without holding mutex */
    if (local_copy == NULL && to_read > 0) {
        PyErr_NoMemory();
        return NULL;
    }

    PyObject *result;
    if (to_read > 0) {
        result = PyBytes_FromStringAndSize((char *)local_copy, to_read);
        free(local_copy);
    } else {
        result = PyBytes_FromStringAndSize("", 0);
    }

    return result;
}

/**
 * @brief readable_amount() - Return number of bytes available without blocking
 *
 * Returns the number of bytes that can be read immediately without blocking.
 * Useful for async I/O to check before calling read_nonblock().
 */
static PyObject *PyBuffer_readable_amount(PyBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->resource == NULL) {
        return PyLong_FromLong(0);
    }

    pthread_mutex_lock(&self->resource->mutex);
    size_t available = self->resource->write_pos - self->resource->read_pos;
    pthread_mutex_unlock(&self->resource->mutex);

    return PyLong_FromSize_t(available);
}

/**
 * @brief at_eof() - Check if buffer is at EOF
 *
 * Returns True if EOF has been signaled and all data has been read.
 * Useful for async I/O loops to know when to stop.
 */
static PyObject *PyBuffer_at_eof(PyBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->resource == NULL) {
        Py_RETURN_TRUE;
    }

    pthread_mutex_lock(&self->resource->mutex);
    bool at_eof = (self->resource->eof || self->resource->closed) &&
                  (self->resource->read_pos >= self->resource->write_pos);
    pthread_mutex_unlock(&self->resource->mutex);

    return PyBool_FromLong(at_eof);
}

/**
 * @brief readline(size=-1) - Read one line, blocking if needed
 *
 * Uses memchr for fast newline scanning (zero-copy search).
 * Returns bytes including the newline, or empty bytes at EOF.
 */
static PyObject *PyBuffer_readline(PyBufferObject *self, PyObject *args) {
    Py_ssize_t size = -1;
    if (!PyArg_ParseTuple(args, "|n", &size)) return NULL;

    if (self->resource == NULL) {
        PyErr_SetString(PyExc_ValueError, "I/O operation on closed buffer");
        return NULL;
    }

    py_buffer_resource_t *buf = self->resource;
    unsigned char *local_copy = NULL;
    size_t copy_len = 0;

    Py_BEGIN_ALLOW_THREADS
    pthread_mutex_lock(&buf->mutex);

    /* Search for newline in available data */
    while (true) {
        size_t available = buf->write_pos - buf->read_pos;

        if (available > 0) {
            const unsigned char *start = buf->data + buf->read_pos;
            size_t search_len = available;

            /* Limit search if size specified */
            if (size > 0 && (size_t)size < search_len) {
                search_len = (size_t)size;
            }

            /* Fast newline search using memchr */
            const unsigned char *newline = find_newline(start, search_len);

            if (newline != NULL) {
                /* Found newline - copy line including newline */
                copy_len = (newline - start) + 1;
                local_copy = (unsigned char *)malloc(copy_len);
                if (local_copy != NULL) {
                    memcpy(local_copy, start, copy_len);
                    buf->read_pos += copy_len;
                }
                break;
            }

            /* No newline found - check if we should return what we have */
            if (buf->eof || (size > 0 && available >= (size_t)size)) {
                copy_len = (size > 0 && (size_t)size < available)
                           ? (size_t)size : available;
                local_copy = (unsigned char *)malloc(copy_len);
                if (local_copy != NULL) {
                    memcpy(local_copy, start, copy_len);
                    buf->read_pos += copy_len;
                }
                break;
            }
        } else if (buf->eof || buf->closed) {
            /* No more data coming */
            break;
        }

        /* Wait for more data */
        pthread_cond_wait(&buf->data_ready, &buf->mutex);
    }

    pthread_mutex_unlock(&buf->mutex);
    Py_END_ALLOW_THREADS

    /* Create PyBytes without holding mutex */
    if (local_copy == NULL && copy_len > 0) {
        PyErr_NoMemory();
        return NULL;
    }

    PyObject *result;
    if (copy_len > 0) {
        result = PyBytes_FromStringAndSize((char *)local_copy, copy_len);
        free(local_copy);
    } else {
        result = PyBytes_FromStringAndSize("", 0);
    }

    return result;
}

/**
 * @brief readlines(hint=-1) - Read all lines
 *
 * Returns list of bytes objects, each including their newline.
 */
static PyObject *PyBuffer_readlines(PyBufferObject *self, PyObject *args) {
    Py_ssize_t hint = -1;
    if (!PyArg_ParseTuple(args, "|n", &hint)) return NULL;

    PyObject *lines = PyList_New(0);
    if (lines == NULL) return NULL;

    Py_ssize_t total_size = 0;

    PyObject *empty_args = PyTuple_New(0);
    if (empty_args == NULL) {
        Py_DECREF(lines);
        return NULL;
    }

    while (true) {
        PyObject *line = PyBuffer_readline(self, empty_args);
        if (line == NULL) {
            Py_DECREF(empty_args);
            Py_DECREF(lines);
            return NULL;
        }

        Py_ssize_t line_len = PyBytes_Size(line);
        if (line_len == 0) {
            Py_DECREF(line);
            break;  /* EOF reached */
        }

        if (PyList_Append(lines, line) < 0) {
            Py_DECREF(line);
            Py_DECREF(empty_args);
            Py_DECREF(lines);
            return NULL;
        }
        Py_DECREF(line);

        total_size += line_len;

        /* Check hint */
        if (hint > 0 && total_size >= hint) {
            break;
        }
    }

    Py_DECREF(empty_args);
    return lines;
}

/**
 * @brief seek(offset, whence=0) - Seek to position
 *
 * Only supports seeking within already-read data (whence=0, 1).
 * Cannot seek forward past write_pos.
 */
static PyObject *PyBuffer_seek(PyBufferObject *self, PyObject *args) {
    Py_ssize_t offset;
    int whence = 0;
    if (!PyArg_ParseTuple(args, "n|i", &offset, &whence)) return NULL;

    if (self->resource == NULL) {
        PyErr_SetString(PyExc_ValueError, "I/O operation on closed buffer");
        return NULL;
    }

    py_buffer_resource_t *buf = self->resource;

    pthread_mutex_lock(&buf->mutex);

    size_t new_pos;
    switch (whence) {
        case 0:  /* SEEK_SET */
            if (offset < 0) {
                pthread_mutex_unlock(&buf->mutex);
                PyErr_SetString(PyExc_ValueError, "Negative seek position");
                return NULL;
            }
            new_pos = (size_t)offset;
            break;

        case 1:  /* SEEK_CUR */
            if (offset < 0 && (size_t)(-offset) > buf->read_pos) {
                pthread_mutex_unlock(&buf->mutex);
                PyErr_SetString(PyExc_ValueError, "Seek would go before start");
                return NULL;
            }
            new_pos = buf->read_pos + offset;
            break;

        case 2:  /* SEEK_END */
            if (!buf->eof) {
                pthread_mutex_unlock(&buf->mutex);
                PyErr_SetString(PyExc_ValueError,
                    "Cannot seek from end before EOF");
                return NULL;
            }
            if (offset > 0) {
                pthread_mutex_unlock(&buf->mutex);
                PyErr_SetString(PyExc_ValueError,
                    "Cannot seek past end");
                return NULL;
            }
            new_pos = buf->write_pos + offset;
            break;

        default:
            pthread_mutex_unlock(&buf->mutex);
            PyErr_SetString(PyExc_ValueError, "Invalid whence value");
            return NULL;
    }

    /* Cannot seek past written data */
    if (new_pos > buf->write_pos) {
        pthread_mutex_unlock(&buf->mutex);
        PyErr_SetString(PyExc_ValueError, "Cannot seek past available data");
        return NULL;
    }

    buf->read_pos = new_pos;

    pthread_mutex_unlock(&buf->mutex);

    return PyLong_FromSize_t(new_pos);
}

/**
 * @brief tell() - Return current read position
 */
static PyObject *PyBuffer_tell(PyBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->resource == NULL) {
        PyErr_SetString(PyExc_ValueError, "I/O operation on closed buffer");
        return NULL;
    }

    pthread_mutex_lock(&self->resource->mutex);
    size_t pos = self->resource->read_pos;
    pthread_mutex_unlock(&self->resource->mutex);

    return PyLong_FromSize_t(pos);
}

/**
 * @brief readable() - Always returns True
 */
static PyObject *PyBuffer_readable(PyBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    (void)self;
    Py_RETURN_TRUE;
}

/**
 * @brief writable() - Always returns False
 */
static PyObject *PyBuffer_writable(PyBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    (void)self;
    Py_RETURN_FALSE;
}

/**
 * @brief seekable() - Returns True (limited seeking supported)
 */
static PyObject *PyBuffer_seekable(PyBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    (void)self;
    Py_RETURN_TRUE;
}

/**
 * @brief close() - Mark buffer as closed
 */
static PyObject *PyBuffer_close_method(PyBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->resource != NULL) {
        pthread_mutex_lock(&self->resource->mutex);
        self->resource->closed = true;
        pthread_cond_broadcast(&self->resource->data_ready);
        pthread_mutex_unlock(&self->resource->mutex);
    }
    Py_RETURN_NONE;
}

/**
 * @brief closed property getter
 */
static PyObject *PyBuffer_closed_get(PyBufferObject *self, void *closure) {
    (void)closure;
    if (self->resource == NULL) {
        Py_RETURN_TRUE;
    }
    pthread_mutex_lock(&self->resource->mutex);
    bool closed = self->resource->closed;
    pthread_mutex_unlock(&self->resource->mutex);
    return PyBool_FromLong(closed);
}

/**
 * @brief __repr__
 */
static PyObject *PyBuffer_repr(PyBufferObject *self) {
    if (self->resource == NULL) {
        return PyUnicode_FromString("<PyBuffer (released)>");
    }

    pthread_mutex_lock(&self->resource->mutex);
    size_t available = self->resource->write_pos - self->resource->read_pos;
    size_t total = self->resource->write_pos;
    bool eof = self->resource->eof;
    pthread_mutex_unlock(&self->resource->mutex);

    return PyUnicode_FromFormat(
        "<PyBuffer available=%zu total=%zu eof=%s>",
        available, total, eof ? "True" : "False");
}

/**
 * @brief __len__ - Return available bytes
 */
static Py_ssize_t PyBuffer_length(PyBufferObject *self) {
    if (self->resource == NULL) {
        return 0;
    }
    pthread_mutex_lock(&self->resource->mutex);
    Py_ssize_t len = self->resource->write_pos - self->resource->read_pos;
    pthread_mutex_unlock(&self->resource->mutex);
    return len;
}

/* Iterator support */

static PyObject *PyBuffer_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}

static PyObject *PyBuffer_iternext(PyBufferObject *self) {
    PyObject *empty_args = PyTuple_New(0);
    if (empty_args == NULL) {
        return NULL;
    }

    PyObject *line = PyBuffer_readline(self, empty_args);
    Py_DECREF(empty_args);

    if (line == NULL) {
        return NULL;
    }

    if (PyBytes_Size(line) == 0) {
        Py_DECREF(line);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    return line;
}

/* Bytes-like method: find - uses memchr/memmem for speed */
static PyObject *PyBuffer_find(PyBufferObject *self, PyObject *args) {
    PyObject *sub;
    Py_ssize_t start = 0;
    Py_ssize_t end = PY_SSIZE_T_MAX;

    if (!PyArg_ParseTuple(args, "O|nn:find", &sub, &start, &end)) {
        return NULL;
    }

    if (self->resource == NULL) {
        return PyLong_FromLong(-1);
    }

    Py_buffer sub_buf;
    if (PyObject_GetBuffer(sub, &sub_buf, PyBUF_SIMPLE) < 0) {
        return NULL;
    }

    pthread_mutex_lock(&self->resource->mutex);

    size_t available = self->resource->write_pos - self->resource->read_pos;
    const unsigned char *data = self->resource->data + self->resource->read_pos;

    /* Adjust start/end */
    if (start < 0) start += available;
    if (start < 0) start = 0;
    if (end < 0) end += available;
    if (end > (Py_ssize_t)available) end = available;

    Py_ssize_t result = -1;

    if (start <= end && sub_buf.len <= (end - start)) {
        const unsigned char *haystack = data + start;
        Py_ssize_t haystack_len = end - start;
        const unsigned char *needle = sub_buf.buf;
        Py_ssize_t needle_len = sub_buf.len;

        if (needle_len == 0) {
            result = start;
        } else if (needle_len == 1) {
            /* Single byte: use memchr (very fast) */
            void *found = memchr(haystack, needle[0], haystack_len);
            if (found != NULL) {
                result = start + ((const unsigned char *)found - haystack);
            }
        } else {
            /* Multi-byte: use memmem */
            void *found = memmem(haystack, haystack_len, needle, needle_len);
            if (found != NULL) {
                result = start + ((const unsigned char *)found - haystack);
            }
        }
    }

    pthread_mutex_unlock(&self->resource->mutex);
    PyBuffer_Release(&sub_buf);

    return PyLong_FromSsize_t(result);
}

/* Test helper to create a buffer for testing */
static PyObject *PyBuffer_test_create(PyTypeObject *type, PyObject *args) {
    (void)type;
    Py_buffer data_buf;
    Py_ssize_t content_length = -1;

    if (!PyArg_ParseTuple(args, "|y*n:_test_create", &data_buf, &content_length)) {
        return NULL;
    }

    py_buffer_resource_t *resource = py_buffer_alloc(content_length);
    if (resource == NULL) {
        if (data_buf.buf != NULL) PyBuffer_Release(&data_buf);
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate buffer");
        return NULL;
    }

    if (data_buf.buf != NULL && data_buf.len > 0) {
        if (py_buffer_write(resource, data_buf.buf, data_buf.len) < 0) {
            PyBuffer_Release(&data_buf);
            enif_release_resource(resource);
            PyErr_SetString(PyExc_MemoryError, "Failed to write initial data");
            return NULL;
        }
        PyBuffer_Release(&data_buf);
    }

    PyObject *result = PyBuffer_from_resource(resource, resource);
    /* from_resource does enif_keep_resource, so we release our reference */
    enif_release_resource(resource);

    return result;
}

/* Method definitions */
static PyMethodDef PyBuffer_methods[] = {
    {"read", (PyCFunction)PyBuffer_read, METH_VARARGS,
     "Read up to size bytes, blocking if needed"},
    {"read_nonblock", (PyCFunction)PyBuffer_read_nonblock, METH_VARARGS,
     "Read available bytes without blocking"},
    {"readline", (PyCFunction)PyBuffer_readline, METH_VARARGS,
     "Read one line, blocking if needed"},
    {"readlines", (PyCFunction)PyBuffer_readlines, METH_VARARGS,
     "Read all lines"},
    {"seek", (PyCFunction)PyBuffer_seek, METH_VARARGS,
     "Seek to position"},
    {"tell", (PyCFunction)PyBuffer_tell, METH_NOARGS,
     "Return current read position"},
    {"readable", (PyCFunction)PyBuffer_readable, METH_NOARGS,
     "Return True (always readable)"},
    {"readable_amount", (PyCFunction)PyBuffer_readable_amount, METH_NOARGS,
     "Return number of bytes available without blocking"},
    {"at_eof", (PyCFunction)PyBuffer_at_eof, METH_NOARGS,
     "Return True if at EOF with no more data"},
    {"writable", (PyCFunction)PyBuffer_writable, METH_NOARGS,
     "Return False (not writable from Python)"},
    {"seekable", (PyCFunction)PyBuffer_seekable, METH_NOARGS,
     "Return True (limited seeking supported)"},
    {"close", (PyCFunction)PyBuffer_close_method, METH_NOARGS,
     "Close the buffer"},
    {"find", (PyCFunction)PyBuffer_find, METH_VARARGS,
     "Return lowest index of substring, or -1 if not found"},
    {"_test_create", (PyCFunction)PyBuffer_test_create,
     METH_VARARGS | METH_CLASS,
     "Create a PyBuffer for testing (internal use)"},
    {NULL}
};

/* Getset definitions */
static PyGetSetDef PyBuffer_getset[] = {
    {"closed", (getter)PyBuffer_closed_get, NULL, "True if buffer is closed", NULL},
    {NULL}
};

/* Sequence methods for len() */
static PySequenceMethods PyBuffer_as_sequence = {
    .sq_length = (lenfunc)PyBuffer_length,
};

/* Type definition */
PyTypeObject PyBufferType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "erlang.PyBuffer",
    .tp_doc = "Zero-copy WSGI input buffer with file-like interface",
    .tp_basicsize = sizeof(PyBufferObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)PyBuffer_dealloc,
    .tp_repr = (reprfunc)PyBuffer_repr,
    .tp_as_buffer = &PyBuffer_as_buffer,
    .tp_as_sequence = &PyBuffer_as_sequence,
    .tp_iter = PyBuffer_iter,
    .tp_iternext = (iternextfunc)PyBuffer_iternext,
    .tp_methods = PyBuffer_methods,
    .tp_getset = PyBuffer_getset,
};

/* ============================================================================
 * Initialization
 * ============================================================================ */

PyObject *PyBuffer_from_resource(py_buffer_resource_t *resource,
                                  void *resource_ref) {
    PyBufferObject *obj = PyObject_New(PyBufferObject, &PyBufferType);
    if (obj == NULL) {
        return NULL;
    }

    obj->resource = resource;
    obj->resource_ref = resource_ref;
    enif_keep_resource(resource_ref);

    return (PyObject *)obj;
}

int PyBuffer_init_type(void) {
    if (PyType_Ready(&PyBufferType) < 0) {
        return -1;
    }
    return 0;
}

int PyBuffer_register_with_module(void) {
    /* Import erlang module (created in py_callback.c) */
    PyObject *erlang_module = PyImport_ImportModule("erlang");
    if (erlang_module == NULL) {
        /* Module doesn't exist yet - this shouldn't happen */
        PyErr_Clear();
        return -1;
    }

    /* Add PyBuffer type to the erlang module */
    Py_INCREF(&PyBufferType);
    if (PyModule_AddObject(erlang_module, "PyBuffer",
                           (PyObject *)&PyBufferType) < 0) {
        Py_DECREF(&PyBufferType);
        Py_DECREF(erlang_module);
        return -1;
    }

    Py_DECREF(erlang_module);
    return 0;
}

/* ============================================================================
 * NIF Functions
 * ============================================================================ */

/**
 * @brief NIF: py_buffer_create(ContentLength | undefined) -> {ok, Ref}
 *
 * Create a new PyBuffer resource.
 */
static ERL_NIF_TERM nif_py_buffer_create(ErlNifEnv *env, int argc,
                                          const ERL_NIF_TERM argv[]) {
    (void)argc;

    ssize_t content_length = -1;

    /* Check if content_length is provided */
    if (!enif_is_atom(env, argv[0])) {
        /* It's a number */
        ErlNifSInt64 len;
        if (!enif_get_int64(env, argv[0], &len)) {
            return make_error(env, "invalid_content_length");
        }
        content_length = (ssize_t)len;
    }
    /* If it's an atom (undefined), content_length stays -1 */

    py_buffer_resource_t *resource = py_buffer_alloc(content_length);
    if (resource == NULL) {
        return make_error(env, "alloc_failed");
    }

    ERL_NIF_TERM ref = enif_make_resource(env, resource);
    enif_release_resource(resource);

    return enif_make_tuple2(env, ATOM_OK, ref);
}

/**
 * @brief NIF: py_buffer_write(Ref, Data) -> ok | {error, Reason}
 *
 * Write binary data to the buffer.
 */
static ERL_NIF_TERM nif_py_buffer_write(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_buffer_resource_t *resource;
    if (!enif_get_resource(env, argv[0], PY_BUFFER_RESOURCE_TYPE, (void **)&resource)) {
        return make_error(env, "invalid_buffer");
    }

    ErlNifBinary data;
    if (!enif_inspect_binary(env, argv[1], &data)) {
        return make_error(env, "invalid_data");
    }

    if (py_buffer_write(resource, data.data, data.size) < 0) {
        return make_error(env, "write_failed");
    }

    return ATOM_OK;
}

/**
 * @brief NIF: py_buffer_close(Ref) -> ok
 *
 * Close the buffer (signal EOF).
 */
static ERL_NIF_TERM nif_py_buffer_close(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]) {
    (void)argc;

    py_buffer_resource_t *resource;
    if (!enif_get_resource(env, argv[0], PY_BUFFER_RESOURCE_TYPE, (void **)&resource)) {
        return make_error(env, "invalid_buffer");
    }

    py_buffer_close(resource);

    return ATOM_OK;
}
