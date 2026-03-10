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
 * @file py_channel.c
 * @brief Channel API implementation for bidirectional message passing
 *
 * This module provides efficient bidirectional communication between
 * Erlang and Python using enif_ioq for zero-copy message buffering.
 */

#include "py_channel.h"
#include "py_event_loop.h"

/* Resource type - initialized in channel_init */
ErlNifResourceType *CHANNEL_RESOURCE_TYPE = NULL;

/* Global channel ID counter */
static _Atomic uint64_t g_channel_id_counter = 1;

/* Atom declarations - reuse from py_nif.c */
static ERL_NIF_TERM ATOM_BUSY;
static ERL_NIF_TERM ATOM_CLOSED;
static ERL_NIF_TERM ATOM_EMPTY;

/* ============================================================================
 * Channel Resource Management
 * ============================================================================ */

void channel_resource_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    py_channel_t *channel = (py_channel_t *)obj;

    /* Release waiter loop reference if held */
    if (channel->waiter_loop != NULL) {
        enif_release_resource(channel->waiter_loop);
        channel->waiter_loop = NULL;
    }

    if (channel->queue != NULL) {
        enif_ioq_destroy(channel->queue);
        channel->queue = NULL;
    }

    pthread_mutex_destroy(&channel->mutex);
}

py_channel_t *channel_alloc(size_t max_size) {
    if (CHANNEL_RESOURCE_TYPE == NULL) {
        return NULL;
    }

    py_channel_t *channel = enif_alloc_resource(
        CHANNEL_RESOURCE_TYPE, sizeof(py_channel_t));
    if (channel == NULL) {
        return NULL;
    }

    channel->queue = enif_ioq_create(ERL_NIF_IOQ_NORMAL);
    if (channel->queue == NULL) {
        enif_release_resource(channel);
        return NULL;
    }

    channel->max_size = max_size;
    channel->current_size = 0;
    channel->waiting = NULL;
    channel->waiting_callback_id = 0;
    channel->waiter_loop = NULL;
    channel->waiter_callback_id = 0;
    channel->has_waiter = false;
    channel->has_sync_waiter = false;
    memset(&channel->sync_waiter_pid, 0, sizeof(ErlNifPid));
    channel->closed = false;
    channel->channel_id = atomic_fetch_add(&g_channel_id_counter, 1);

    if (pthread_mutex_init(&channel->mutex, NULL) != 0) {
        enif_ioq_destroy(channel->queue);
        enif_release_resource(channel);
        return NULL;
    }

    return channel;
}

int channel_send(py_channel_t *channel, const unsigned char *data, size_t size) {
    pthread_mutex_lock(&channel->mutex);

    if (channel->closed) {
        pthread_mutex_unlock(&channel->mutex);
        return -1;
    }

    /* Check backpressure */
    if (channel->max_size > 0 && channel->current_size + size > channel->max_size) {
        pthread_mutex_unlock(&channel->mutex);
        return 1;  /* Busy - backpressure */
    }

    /* Create binary and enqueue */
    ErlNifBinary bin;
    if (!enif_alloc_binary(size, &bin)) {
        pthread_mutex_unlock(&channel->mutex);
        return -1;
    }
    memcpy(bin.data, data, size);

    /* enif_ioq_enq_binary takes ownership of the binary */
    if (!enif_ioq_enq_binary(channel->queue, &bin, 0)) {
        enif_release_binary(&bin);
        pthread_mutex_unlock(&channel->mutex);
        return -1;
    }

    channel->current_size += size;

    /* Check if there's a waiting context to resume */
    bool should_resume = (channel->waiting != NULL);

    /* Check if there's an async waiter to dispatch */
    erlang_event_loop_t *loop_to_wake = NULL;
    uint64_t callback_id = 0;

    if (channel->has_waiter) {
        loop_to_wake = channel->waiter_loop;
        callback_id = channel->waiter_callback_id;
        channel->has_waiter = false;
        channel->waiter_loop = NULL;
        /* Note: Keep the reference until after dispatch */
    }

    /* Check if there's a sync waiter to notify */
    ErlNifPid sync_waiter;
    bool notify_sync = false;

    if (channel->has_sync_waiter) {
        sync_waiter = channel->sync_waiter_pid;
        notify_sync = true;
        channel->has_sync_waiter = false;
    }

    pthread_mutex_unlock(&channel->mutex);

    /* Resume happens outside the lock to avoid deadlocks */
    if (should_resume) {
        channel_resume_waiting(channel);
    }

    /* Dispatch async waiter via timer dispatch (same path as timers) */
    if (loop_to_wake != NULL) {
        event_loop_add_pending(loop_to_wake, EVENT_TYPE_TIMER, callback_id, -1);
        /* Release the reference we kept in channel_wait */
        enif_release_resource(loop_to_wake);
    }

    /* Notify sync waiter via Erlang message */
    if (notify_sync) {
        ErlNifEnv *msg_env = enif_alloc_env();
        enif_send(NULL, &sync_waiter, msg_env,
                  enif_make_atom(msg_env, "channel_data_ready"));
        enif_free_env(msg_env);
    }

    return 0;
}

int channel_send_owned_binary(py_channel_t *channel, ErlNifBinary *bin) {
    pthread_mutex_lock(&channel->mutex);

    if (channel->closed) {
        enif_release_binary(bin);
        pthread_mutex_unlock(&channel->mutex);
        return -1;
    }

    /* Check backpressure */
    if (channel->max_size > 0 && channel->current_size + bin->size > channel->max_size) {
        enif_release_binary(bin);
        pthread_mutex_unlock(&channel->mutex);
        return 1;  /* Busy - backpressure */
    }

    size_t msg_size = bin->size;

    /* Directly enqueue - transfers ownership to IOQueue */
    if (!enif_ioq_enq_binary(channel->queue, bin, 0)) {
        enif_release_binary(bin);
        pthread_mutex_unlock(&channel->mutex);
        return -1;
    }

    channel->current_size += msg_size;

    /* Check if there's a waiting context to resume */
    bool should_resume = (channel->waiting != NULL);

    /* Check if there's an async waiter to dispatch */
    erlang_event_loop_t *loop_to_wake = NULL;
    uint64_t callback_id = 0;

    if (channel->has_waiter) {
        loop_to_wake = channel->waiter_loop;
        callback_id = channel->waiter_callback_id;
        channel->has_waiter = false;
        channel->waiter_loop = NULL;
    }

    /* Check if there's a sync waiter to notify */
    ErlNifPid sync_waiter;
    bool notify_sync = false;

    if (channel->has_sync_waiter) {
        sync_waiter = channel->sync_waiter_pid;
        notify_sync = true;
        channel->has_sync_waiter = false;
    }

    pthread_mutex_unlock(&channel->mutex);

    if (should_resume) {
        channel_resume_waiting(channel);
    }

    if (loop_to_wake != NULL) {
        event_loop_add_pending(loop_to_wake, EVENT_TYPE_TIMER, callback_id, -1);
        enif_release_resource(loop_to_wake);
    }

    /* Notify sync waiter via Erlang message */
    if (notify_sync) {
        ErlNifEnv *msg_env = enif_alloc_env();
        enif_send(NULL, &sync_waiter, msg_env,
                  enif_make_atom(msg_env, "channel_data_ready"));
        enif_free_env(msg_env);
    }

    return 0;
}

int channel_try_receive(py_channel_t *channel, unsigned char **out_data, size_t *out_size) {
    pthread_mutex_lock(&channel->mutex);

    size_t queue_size = enif_ioq_size(channel->queue);

    if (queue_size == 0) {
        if (channel->closed) {
            pthread_mutex_unlock(&channel->mutex);
            return -1;  /* Closed */
        }
        pthread_mutex_unlock(&channel->mutex);
        return 1;  /* Empty */
    }

    /* Peek at the first iovec entry to get its size */
    SysIOVec *iov;
    int iovcnt;
    iov = enif_ioq_peek(channel->queue, &iovcnt);

    if (iovcnt == 0 || iov == NULL || iov[0].iov_len == 0) {
        pthread_mutex_unlock(&channel->mutex);
        return 1;  /* Empty */
    }

    size_t msg_size = iov[0].iov_len;

    /* Allocate output buffer */
    *out_data = enif_alloc(msg_size);
    if (*out_data == NULL) {
        pthread_mutex_unlock(&channel->mutex);
        return -1;
    }

    /* Copy data and dequeue */
    memcpy(*out_data, iov[0].iov_base, msg_size);
    *out_size = msg_size;

    /* Dequeue the message */
    enif_ioq_deq(channel->queue, msg_size, NULL);
    channel->current_size -= msg_size;

    pthread_mutex_unlock(&channel->mutex);
    return 0;
}

void channel_close(py_channel_t *channel) {
    pthread_mutex_lock(&channel->mutex);
    channel->closed = true;
    bool should_resume = (channel->waiting != NULL);

    /* Check if there's an async waiter to dispatch */
    erlang_event_loop_t *loop_to_wake = NULL;
    uint64_t callback_id = 0;

    if (channel->has_waiter) {
        loop_to_wake = channel->waiter_loop;
        callback_id = channel->waiter_callback_id;
        channel->has_waiter = false;
        channel->waiter_loop = NULL;
    }

    /* Check if there's a sync waiter to notify */
    ErlNifPid sync_waiter;
    bool notify_sync = false;

    if (channel->has_sync_waiter) {
        sync_waiter = channel->sync_waiter_pid;
        notify_sync = true;
        channel->has_sync_waiter = false;
    }

    pthread_mutex_unlock(&channel->mutex);

    if (should_resume) {
        channel_resume_waiting(channel);
    }

    /* Dispatch async waiter to signal closure */
    if (loop_to_wake != NULL) {
        event_loop_add_pending(loop_to_wake, EVENT_TYPE_TIMER, callback_id, -1);
        enif_release_resource(loop_to_wake);
    }

    /* Notify sync waiter that channel is closed */
    if (notify_sync) {
        ErlNifEnv *msg_env = enif_alloc_env();
        enif_send(NULL, &sync_waiter, msg_env,
                  enif_make_atom(msg_env, "channel_closed"));
        enif_free_env(msg_env);
    }
}

void channel_resume_waiting(py_channel_t *channel) {
    /* This function would trigger resume of the suspended context.
     * For now, the actual resume logic is handled in the NIF receive function
     * by checking if data is available before suspending. */
    (void)channel;
}

int channel_init(ErlNifEnv *env) {
    /* Initialize channel-specific atoms */
    ATOM_BUSY = enif_make_atom(env, "busy");
    ATOM_CLOSED = enif_make_atom(env, "closed");
    ATOM_EMPTY = enif_make_atom(env, "empty");

    return 0;
}

/* ============================================================================
 * ChannelBuffer Python Type
 * ============================================================================ */

static void ChannelBuffer_dealloc(ChannelBufferObject *self) {
    Py_CLEAR(self->cached_memoryview);
    if (self->data != NULL) {
        enif_free(self->data);
        self->data = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int ChannelBuffer_getbuffer(PyObject *obj, Py_buffer *view, int flags) {
    ChannelBufferObject *self = (ChannelBufferObject *)obj;

    if (self->data == NULL) {
        PyErr_SetString(PyExc_BufferError, "Buffer has been released");
        return -1;
    }

    view->obj = obj;
    view->buf = self->data;
    view->len = self->size;
    view->readonly = 1;
    view->itemsize = 1;
    view->format = (flags & PyBUF_FORMAT) ? "B" : NULL;
    view->ndim = 1;
    view->shape = (flags & PyBUF_ND) ? &view->len : NULL;
    view->strides = (flags & PyBUF_STRIDES) ? &view->itemsize : NULL;
    view->suboffsets = NULL;
    view->internal = NULL;

    Py_INCREF(obj);
    return 0;
}

static void ChannelBuffer_releasebuffer(PyObject *obj, Py_buffer *view) {
    (void)obj;
    (void)view;
    /* No cleanup needed - data lifetime managed by ChannelBufferObject */
}

static PyBufferProcs ChannelBuffer_as_buffer = {
    .bf_getbuffer = ChannelBuffer_getbuffer,
    .bf_releasebuffer = ChannelBuffer_releasebuffer,
};

static Py_ssize_t ChannelBuffer_length(ChannelBufferObject *self) {
    return (Py_ssize_t)self->size;
}

static PyObject *ChannelBuffer_item(ChannelBufferObject *self, Py_ssize_t i) {
    if (self->data == NULL) {
        PyErr_SetString(PyExc_IndexError, "Buffer has been released");
        return NULL;
    }

    if (i < 0) {
        i += self->size;
    }

    if (i < 0 || (size_t)i >= self->size) {
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }

    return PyLong_FromLong(self->data[i]);
}

static PyObject *ChannelBuffer_subscript(ChannelBufferObject *self, PyObject *key) {
    if (self->data == NULL) {
        PyErr_SetString(PyExc_IndexError, "Buffer has been released");
        return NULL;
    }

    if (PyLong_Check(key)) {
        Py_ssize_t i = PyLong_AsSsize_t(key);
        if (i == -1 && PyErr_Occurred()) {
            return NULL;
        }
        return ChannelBuffer_item(self, i);
    }

    if (PySlice_Check(key)) {
        Py_ssize_t start, stop, step, slicelength;
        if (PySlice_GetIndicesEx(key, self->size, &start, &stop, &step, &slicelength) < 0) {
            return NULL;
        }

        if (step == 1) {
            /* Contiguous slice - return bytes directly */
            return PyBytes_FromStringAndSize((char *)self->data + start, slicelength);
        }

        /* Non-contiguous slice */
        PyObject *result = PyBytes_FromStringAndSize(NULL, slicelength);
        if (result == NULL) {
            return NULL;
        }
        char *dest = PyBytes_AS_STRING(result);
        for (Py_ssize_t i = 0, j = start; i < slicelength; i++, j += step) {
            dest[i] = self->data[j];
        }
        return result;
    }

    PyErr_SetString(PyExc_TypeError, "indices must be integers or slices");
    return NULL;
}

static PySequenceMethods ChannelBuffer_as_sequence = {
    .sq_length = (lenfunc)ChannelBuffer_length,
    .sq_item = (ssizeargfunc)ChannelBuffer_item,
};

static PyMappingMethods ChannelBuffer_as_mapping = {
    .mp_length = (lenfunc)ChannelBuffer_length,
    .mp_subscript = (binaryfunc)ChannelBuffer_subscript,
};

static PyObject *ChannelBuffer_bytes(ChannelBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->data == NULL) {
        return PyBytes_FromStringAndSize("", 0);
    }
    return PyBytes_FromStringAndSize((char *)self->data, self->size);
}

static PyObject *ChannelBuffer_memoryview(ChannelBufferObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->cached_memoryview == NULL) {
        if (self->data == NULL) {
            PyErr_SetString(PyExc_BufferError, "Buffer has been released");
            return NULL;
        }
        self->cached_memoryview = PyMemoryView_FromObject((PyObject *)self);
        if (self->cached_memoryview == NULL) {
            return NULL;
        }
    }
    Py_INCREF(self->cached_memoryview);
    return self->cached_memoryview;
}

static PyObject *ChannelBuffer_repr(ChannelBufferObject *self) {
    if (self->data == NULL) {
        return PyUnicode_FromString("<ChannelBuffer (released)>");
    }
    return PyUnicode_FromFormat("<ChannelBuffer size=%zu>", self->size);
}

static PyObject *ChannelBuffer_decode(ChannelBufferObject *self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"encoding", "errors", NULL};
    const char *encoding = "utf-8";
    const char *errors = "strict";

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ss:decode", kwlist, &encoding, &errors)) {
        return NULL;
    }

    if (self->data == NULL) {
        return PyUnicode_FromStringAndSize("", 0);
    }

    return PyUnicode_Decode((char *)self->data, self->size, encoding, errors);
}

static PyMethodDef ChannelBuffer_methods[] = {
    {"__bytes__", (PyCFunction)ChannelBuffer_bytes, METH_NOARGS,
     "Return bytes copy of buffer"},
    {"memoryview", (PyCFunction)ChannelBuffer_memoryview, METH_NOARGS,
     "Return a memoryview for zero-copy access"},
    {"decode", (PyCFunction)ChannelBuffer_decode, METH_VARARGS | METH_KEYWORDS,
     "Decode the buffer using the specified encoding"},
    {NULL}
};

PyTypeObject ChannelBufferType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "erlang.channel.ChannelBuffer",
    .tp_doc = "Zero-copy channel message buffer",
    .tp_basicsize = sizeof(ChannelBufferObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)ChannelBuffer_dealloc,
    .tp_repr = (reprfunc)ChannelBuffer_repr,
    .tp_as_buffer = &ChannelBuffer_as_buffer,
    .tp_as_sequence = &ChannelBuffer_as_sequence,
    .tp_as_mapping = &ChannelBuffer_as_mapping,
    .tp_methods = ChannelBuffer_methods,
};

int ChannelBuffer_init_type(void) {
    if (PyType_Ready(&ChannelBufferType) < 0) {
        return -1;
    }
    return 0;
}

PyObject *ChannelBuffer_from_data(unsigned char *data, size_t size) {
    ChannelBufferObject *obj = PyObject_New(ChannelBufferObject, &ChannelBufferType);
    if (obj == NULL) {
        enif_free(data);
        return NULL;
    }

    obj->data = data;
    obj->size = size;
    obj->cached_memoryview = NULL;

    return (PyObject *)obj;
}

int ChannelBuffer_register(void) {
    /* Import erlang module */
    PyObject *erlang_module = PyImport_ImportModule("erlang");
    if (erlang_module == NULL) {
        PyErr_Clear();
        return -1;
    }

    /* Add ChannelBuffer type to the erlang module */
    Py_INCREF(&ChannelBufferType);
    if (PyModule_AddObject(erlang_module, "ChannelBuffer", (PyObject *)&ChannelBufferType) < 0) {
        Py_DECREF(&ChannelBufferType);
        Py_DECREF(erlang_module);
        return -1;
    }

    Py_DECREF(erlang_module);
    return 0;
}

/* ============================================================================
 * Channel NIF Functions
 * ============================================================================ */

/**
 * @brief Create a new channel
 *
 * nif_channel_create(MaxSize) -> {ok, ChannelRef}
 */
static ERL_NIF_TERM nif_channel_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    size_t max_size = 0;

    if (argc >= 1) {
        ErlNifUInt64 size_val;
        if (enif_get_uint64(env, argv[0], &size_val)) {
            max_size = (size_t)size_val;
        }
    }

    py_channel_t *channel = channel_alloc(max_size);
    if (channel == NULL) {
        return make_error(env, "alloc_failed");
    }

    /* Set owner to caller */
    enif_self(env, &channel->owner);

    ERL_NIF_TERM ref = enif_make_resource(env, channel);
    enif_release_resource(channel);

    return enif_make_tuple2(env, ATOM_OK, ref);
}

/**
 * @brief Send a message to a channel
 *
 * nif_channel_send(ChannelRef, Term) -> ok | busy | {error, closed}
 *
 * Optimized: serializes the term once and passes the binary directly to
 * channel_send_owned_binary, eliminating the extra allocation and copy
 * that would occur in channel_send.
 */
static ERL_NIF_TERM nif_channel_send(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    /*
     * Serialize term to binary using external term format.
     * term_to_binary allocates and we own the binary, so pass it
     * directly to channel_send_owned_binary to avoid double copy.
     */
    ErlNifBinary bin;
    if (!enif_term_to_binary(env, argv[1], &bin)) {
        return make_error(env, "term_to_binary_failed");
    }

    int result = channel_send_owned_binary(channel, &bin);

    /* bin is now owned by IOQueue or released on error */
    switch (result) {
        case 0:
            return ATOM_OK;
        case 1:
            return ATOM_BUSY;
        default:
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_CLOSED);
    }
}

/**
 * @brief Receive from a channel (may suspend if empty)
 *
 * nif_channel_receive(ContextRef, ChannelRef) -> {ok, Data} | {suspended, ...} | {error, closed}
 *
 * When the queue is empty and suspension is allowed, this triggers suspension
 * so Python can yield to Erlang while waiting for data.
 */
static ERL_NIF_TERM nif_channel_receive(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    py_channel_t *channel;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    if (!enif_get_resource(env, argv[1], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    unsigned char *data;
    size_t size;
    int result = channel_try_receive(channel, &data, &size);

    if (result == 0) {
        /* Data available - convert back to term */
        ERL_NIF_TERM term;
        if (enif_binary_to_term(env, data, size, &term, 0) == 0) {
            enif_free(data);
            return make_error(env, "binary_to_term_failed");
        }
        enif_free(data);
        return enif_make_tuple2(env, ATOM_OK, term);
    } else if (result == 1) {
        /* Empty - return empty indicator; Python side will handle suspension */
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_EMPTY);
    } else {
        /* Closed */
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_CLOSED);
    }
}

/**
 * @brief Non-blocking receive from a channel
 *
 * nif_channel_try_receive(ChannelRef) -> {ok, Data} | {error, empty} | {error, closed}
 */
static ERL_NIF_TERM nif_channel_try_receive(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    unsigned char *data;
    size_t size;
    int result = channel_try_receive(channel, &data, &size);

    if (result == 0) {
        /* Data available - convert back to term */
        ERL_NIF_TERM term;
        if (enif_binary_to_term(env, data, size, &term, 0) == 0) {
            enif_free(data);
            return make_error(env, "binary_to_term_failed");
        }
        enif_free(data);
        return enif_make_tuple2(env, ATOM_OK, term);
    } else if (result == 1) {
        /* Empty */
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_EMPTY);
    } else {
        /* Closed */
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_CLOSED);
    }
}

/**
 * @brief Send a reply to an Erlang process
 *
 * nif_channel_reply(ContextRef, Pid, Term) -> ok | {error, Reason}
 *
 * Allows Python code to send messages to arbitrary Erlang processes.
 */
static ERL_NIF_TERM nif_channel_reply(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_context_t *ctx;
    ErlNifPid pid;

    if (!enif_get_resource(env, argv[0], PY_CONTEXT_RESOURCE_TYPE, (void **)&ctx)) {
        return make_error(env, "invalid_context");
    }

    if (!enif_get_local_pid(env, argv[1], &pid)) {
        return make_error(env, "invalid_pid");
    }

    /* Create a message environment and copy the term */
    ErlNifEnv *msg_env = enif_alloc_env();
    if (msg_env == NULL) {
        return make_error(env, "alloc_env_failed");
    }

    ERL_NIF_TERM msg = enif_make_copy(msg_env, argv[2]);

    /* Send the message */
    if (!enif_send(env, &pid, msg_env, msg)) {
        enif_free_env(msg_env);
        return make_error(env, "send_failed");
    }

    enif_free_env(msg_env);
    return ATOM_OK;
}

/**
 * @brief Close a channel
 *
 * nif_channel_close(ChannelRef) -> ok
 */
static ERL_NIF_TERM nif_channel_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    channel_close(channel);
    return ATOM_OK;
}

/**
 * @brief Get channel info
 *
 * nif_channel_info(ChannelRef) -> #{size => N, max_size => M, closed => Bool}
 */
static ERL_NIF_TERM nif_channel_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    pthread_mutex_lock(&channel->mutex);
    size_t current_size = channel->current_size;
    size_t max_size = channel->max_size;
    bool closed = channel->closed;
    pthread_mutex_unlock(&channel->mutex);

    ERL_NIF_TERM keys[] = {
        enif_make_atom(env, "size"),
        enif_make_atom(env, "max_size"),
        enif_make_atom(env, "closed")
    };
    ERL_NIF_TERM values[] = {
        enif_make_uint64(env, current_size),
        enif_make_uint64(env, max_size),
        closed ? ATOM_TRUE : ATOM_FALSE
    };

    ERL_NIF_TERM map;
    enif_make_map_from_arrays(env, keys, values, 3, &map);
    return map;
}

/**
 * @brief Register an async waiter for a channel
 *
 * nif_channel_wait(ChannelRef, CallbackId, LoopRef) -> ok | {ok, Data}
 *
 * If data is available, returns immediately with the data.
 * If empty, registers the callback_id and loop for dispatch when data arrives.
 * Python awaits a Future that will be resolved when the callback fires.
 */
ERL_NIF_TERM nif_channel_wait(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;
    erlang_event_loop_t *loop;
    ErlNifUInt64 callback_id;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }

    if (!enif_get_resource(env, argv[2], EVENT_LOOP_RESOURCE_TYPE, (void **)&loop)) {
        return make_error(env, "invalid_loop");
    }

    pthread_mutex_lock(&channel->mutex);

    /* Check if channel is closed */
    if (channel->closed) {
        pthread_mutex_unlock(&channel->mutex);
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, "closed"));
    }

    /* Check if data already available */
    size_t queue_size = enif_ioq_size(channel->queue);
    if (queue_size > 0) {
        /* Data available - dequeue and return immediately */
        SysIOVec *iov;
        int iovcnt;
        iov = enif_ioq_peek(channel->queue, &iovcnt);

        if (iovcnt > 0 && iov != NULL && iov[0].iov_len > 0) {
            size_t msg_size = iov[0].iov_len;
            unsigned char *data = enif_alloc(msg_size);
            if (data == NULL) {
                pthread_mutex_unlock(&channel->mutex);
                return make_error(env, "alloc_failed");
            }

            memcpy(data, iov[0].iov_base, msg_size);
            enif_ioq_deq(channel->queue, msg_size, NULL);
            channel->current_size -= msg_size;

            pthread_mutex_unlock(&channel->mutex);

            /* Convert back to term */
            ERL_NIF_TERM term;
            if (enif_binary_to_term(env, data, msg_size, &term, 0) == 0) {
                enif_free(data);
                return make_error(env, "binary_to_term_failed");
            }
            enif_free(data);
            return enif_make_tuple2(env, ATOM_OK, term);
        }
    }

    /* No data - register waiter */
    /* Keep reference to event loop to prevent destruction while waiting */
    enif_keep_resource(loop);

    /* Clear any previous waiter (should not happen, but be safe) */
    if (channel->waiter_loop != NULL) {
        enif_release_resource(channel->waiter_loop);
    }

    channel->waiter_loop = loop;
    channel->waiter_callback_id = callback_id;
    channel->has_waiter = true;

    pthread_mutex_unlock(&channel->mutex);

    /* Return ok - Python will await Future */
    return ATOM_OK;
}

/**
 * @brief Cancel an async waiter for a channel
 *
 * nif_channel_cancel_wait(ChannelRef, CallbackId) -> ok
 *
 * Called when the Python Future is cancelled or times out.
 */
ERL_NIF_TERM nif_channel_cancel_wait(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;
    ErlNifUInt64 callback_id;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    if (!enif_get_uint64(env, argv[1], &callback_id)) {
        return make_error(env, "invalid_callback_id");
    }

    pthread_mutex_lock(&channel->mutex);

    /* Only cancel if the callback_id matches */
    if (channel->has_waiter && channel->waiter_callback_id == callback_id) {
        if (channel->waiter_loop != NULL) {
            enif_release_resource(channel->waiter_loop);
            channel->waiter_loop = NULL;
        }
        channel->has_waiter = false;
        channel->waiter_callback_id = 0;
    }

    pthread_mutex_unlock(&channel->mutex);
    return ATOM_OK;
}

/**
 * @brief Register a sync waiter for blocking receive
 *
 * nif_channel_register_sync_waiter(ChannelRef) -> ok | {error, Reason}
 *
 * Registers the calling process as a sync waiter. When data arrives,
 * the waiter receives a 'channel_data_ready' message. When the channel
 * closes, receives 'channel_closed'.
 */
ERL_NIF_TERM nif_channel_register_sync_waiter(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    pthread_mutex_lock(&channel->mutex);

    /* Check if channel is closed */
    if (channel->closed) {
        pthread_mutex_unlock(&channel->mutex);
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, "closed"));
    }

    /* Check if another sync waiter is already registered */
    if (channel->has_sync_waiter) {
        pthread_mutex_unlock(&channel->mutex);
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, "waiter_exists"));
    }

    /* Get calling process PID */
    if (!enif_self(env, &channel->sync_waiter_pid)) {
        pthread_mutex_unlock(&channel->mutex);
        return make_error(env, "no_calling_process");
    }

    channel->has_sync_waiter = true;

    pthread_mutex_unlock(&channel->mutex);
    return ATOM_OK;
}
