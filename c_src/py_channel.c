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

    /* Notify sync waiter that channel is being destroyed.
     * This is safe because enif_send just puts a message in the process mailbox. */
    if (channel->has_sync_waiter) {
        ErlNifEnv *msg_env = enif_alloc_env();
        if (msg_env != NULL) {
            enif_send(NULL, &channel->sync_waiter_pid, msg_env,
                      enif_make_atom(msg_env, "channel_closed"));
            enif_free_env(msg_env);
        }
    }

    /* Note: We do NOT notify async waiter here because the event loop
     * may already be destroyed (resources can be GC'd in any order).
     * The async waiter will timeout or be cancelled when the event loop
     * is destroyed. We just release our reference to the loop. */
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

    /* Check if there's an async waiter to dispatch.
     * IMPORTANT: Clear waiter state BEFORE releasing mutex to avoid race condition.
     * With task_ready notification, the callback can fire before we re-acquire the mutex.
     * If dispatch fails (rare), data is still in channel for next receive. */
    erlang_event_loop_t *loop_to_wake = NULL;
    uint64_t callback_id = 0;
    bool has_async_waiter = channel->has_waiter;

    if (has_async_waiter) {
        loop_to_wake = channel->waiter_loop;
        callback_id = channel->waiter_callback_id;
        /* Clear waiter state now to avoid race with fast callback */
        channel->has_waiter = false;
        channel->waiter_loop = NULL;
        channel->waiter_callback_id = 0;
    }

    /* Check if there's a sync waiter to notify */
    ErlNifPid sync_waiter;
    bool has_sync_waiter = channel->has_sync_waiter;

    if (has_sync_waiter) {
        sync_waiter = channel->sync_waiter_pid;
        /* Clear waiter state now to avoid race */
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
    if (has_sync_waiter) {
        ErlNifEnv *msg_env = enif_alloc_env();
        if (msg_env != NULL) {
            enif_send(NULL, &sync_waiter, msg_env,
                      enif_make_atom(msg_env, "channel_data_ready"));
            enif_free_env(msg_env);
        }
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

    /* Check if there's an async waiter to dispatch.
     * IMPORTANT: Clear waiter state BEFORE releasing mutex to avoid race condition.
     * With task_ready notification, the callback can fire before we re-acquire the mutex.
     * If dispatch fails (rare), data is still in channel for next receive. */
    erlang_event_loop_t *loop_to_wake = NULL;
    uint64_t callback_id = 0;
    bool has_async_waiter = channel->has_waiter;

    if (has_async_waiter) {
        loop_to_wake = channel->waiter_loop;
        callback_id = channel->waiter_callback_id;
        /* Clear waiter state now to avoid race with fast callback */
        channel->has_waiter = false;
        channel->waiter_loop = NULL;
        channel->waiter_callback_id = 0;
    }

    /* Check if there's a sync waiter to notify */
    ErlNifPid sync_waiter;
    bool has_sync_waiter = channel->has_sync_waiter;

    if (has_sync_waiter) {
        sync_waiter = channel->sync_waiter_pid;
        /* Clear waiter state now to avoid race */
        channel->has_sync_waiter = false;
    }

    pthread_mutex_unlock(&channel->mutex);

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
    if (has_sync_waiter) {
        ErlNifEnv *msg_env = enif_alloc_env();
        if (msg_env != NULL) {
            enif_send(NULL, &sync_waiter, msg_env,
                      enif_make_atom(msg_env, "channel_data_ready"));
            enif_free_env(msg_env);
        }
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

    /* Check if there's an async waiter to dispatch.
     * For close, we unconditionally clear the waiter since the channel
     * is now closed - any future receive will see the closed state. */
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
        if (msg_env != NULL) {
            enif_send(NULL, &sync_waiter, msg_env,
                      enif_make_atom(msg_env, "channel_closed"));
            enif_free_env(msg_env);
        }
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

    /* Reject if any waiter already exists (no mixed or duplicate waiters) */
    if (channel->has_waiter || channel->has_sync_waiter) {
        pthread_mutex_unlock(&channel->mutex);
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, "waiter_exists"));
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
 * nif_channel_register_sync_waiter(ChannelRef) -> ok | has_data | {error, Reason}
 *
 * Registers the calling process as a sync waiter. When data arrives,
 * the waiter receives a 'channel_data_ready' message. When the channel
 * closes, receives 'channel_closed'.
 *
 * Returns 'has_data' if data is already available (race condition fix).
 * The caller should retry the receive in this case.
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

    /* Check if data is already available (race condition fix).
     * If data arrived between try_receive and register_sync_waiter,
     * return has_data so the caller can retry the receive. */
    if (enif_ioq_size(channel->queue) > 0) {
        pthread_mutex_unlock(&channel->mutex);
        return enif_make_atom(env, "has_data");
    }

    /* Reject if any waiter already exists (no mixed or duplicate waiters) */
    if (channel->has_sync_waiter || channel->has_waiter) {
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

/* ============================================================================
 * ByteChannel NIF Functions (raw bytes, no term conversion)
 * ============================================================================ */

/**
 * @brief Send raw bytes to a channel (no term_to_binary)
 *
 * nif_byte_channel_send_bytes(ChannelRef, Binary) -> ok | busy | {error, closed}
 *
 * Sends raw binary data directly to the channel without term serialization.
 * Used for ByteChannel API where raw byte streams are desired.
 */
ERL_NIF_TERM nif_byte_channel_send_bytes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;
    ErlNifBinary bin;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    if (!enif_inspect_binary(env, argv[1], &bin)) {
        return make_error(env, "invalid_binary");
    }

    int result = channel_send(channel, bin.data, bin.size);

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
 * @brief Non-blocking receive raw bytes from a channel (no binary_to_term)
 *
 * nif_byte_channel_try_receive_bytes(ChannelRef) -> {ok, Binary} | {error, empty} | {error, closed}
 *
 * Receives raw binary data directly from the channel without term deserialization.
 * Used for ByteChannel API where raw byte streams are desired.
 */
ERL_NIF_TERM nif_byte_channel_try_receive_bytes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    py_channel_t *channel;

    if (!enif_get_resource(env, argv[0], CHANNEL_RESOURCE_TYPE, (void **)&channel)) {
        return make_error(env, "invalid_channel");
    }

    unsigned char *data;
    size_t size;
    int result = channel_try_receive(channel, &data, &size);

    if (result == 0) {
        /* Data available - return raw binary (NO enif_binary_to_term) */
        ERL_NIF_TERM bin_term;
        unsigned char *bin_data = enif_make_new_binary(env, size, &bin_term);
        if (bin_data == NULL) {
            enif_free(data);
            return make_error(env, "alloc_failed");
        }
        memcpy(bin_data, data, size);
        enif_free(data);
        return enif_make_tuple2(env, ATOM_OK, bin_term);
    } else if (result == 1) {
        /* Empty */
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_EMPTY);
    } else {
        /* Closed */
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_CLOSED);
    }
}

/**
 * @brief Register an async waiter for raw bytes from a channel
 *
 * nif_byte_channel_wait_bytes(ChannelRef, CallbackId, LoopRef) -> ok | {ok, Binary} | {error, ...}
 *
 * If data is available, returns immediately with raw binary data.
 * If empty, registers the callback_id and loop for dispatch when data arrives.
 * Same as nif_channel_wait but returns raw bytes instead of terms.
 */
ERL_NIF_TERM nif_byte_channel_wait_bytes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
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

    /* Reject if any waiter already exists */
    if (channel->has_waiter || channel->has_sync_waiter) {
        pthread_mutex_unlock(&channel->mutex);
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, "waiter_exists"));
    }

    /* Check if data already available */
    size_t queue_size = enif_ioq_size(channel->queue);
    if (queue_size > 0) {
        /* Data available - dequeue and return immediately as raw binary */
        SysIOVec *iov;
        int iovcnt;
        iov = enif_ioq_peek(channel->queue, &iovcnt);

        if (iovcnt > 0 && iov != NULL && iov[0].iov_len > 0) {
            size_t msg_size = iov[0].iov_len;

            /* Create result binary before dequeuing */
            ERL_NIF_TERM bin_term;
            unsigned char *bin_data = enif_make_new_binary(env, msg_size, &bin_term);
            if (bin_data == NULL) {
                pthread_mutex_unlock(&channel->mutex);
                return make_error(env, "alloc_failed");
            }

            memcpy(bin_data, iov[0].iov_base, msg_size);
            enif_ioq_deq(channel->queue, msg_size, NULL);
            channel->current_size -= msg_size;

            pthread_mutex_unlock(&channel->mutex);

            /* Return raw binary (NO enif_binary_to_term) */
            return enif_make_tuple2(env, ATOM_OK, bin_term);
        }
    }

    /* No data - register waiter */
    enif_keep_resource(loop);

    channel->waiter_loop = loop;
    channel->waiter_callback_id = callback_id;
    channel->has_waiter = true;

    pthread_mutex_unlock(&channel->mutex);

    /* Return ok - Python will await Future */
    return ATOM_OK;
}
