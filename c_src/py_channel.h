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
 * @file py_channel.h
 * @brief Channel API for bidirectional Erlang-Python message passing
 * @author Benoit Chesneau
 *
 * This module provides a Channel abstraction for efficient bidirectional
 * communication between Erlang processes and Python code. Channels use
 * enif_ioq for zero-copy message buffering and integrate with the existing
 * suspension/resume mechanism for non-blocking receives.
 *
 * @par Architecture
 *
 * ```
 * Erlang -> Python (via IOQueue):
 *   py_channel:send(Ch, Term)
 *   -> NIF serializes to binary
 *   -> enif_ioq_enq_binary()
 *   -> Resume suspended Python (if waiting)
 *
 * Python -> Erlang (via enif_send):
 *   channel.reply(pid, term)
 *   -> NIF builds Erlang term
 *   -> enif_send() to process mailbox
 * ```
 *
 * @par Zero-Copy Design
 *
 * ChannelBuffer wraps dequeued binaries without copying, exposing them
 * through Python's buffer protocol. This follows the ReactorBuffer pattern.
 */

#ifndef PY_CHANNEL_H
#define PY_CHANNEL_H

#include <Python.h>
#include <erl_nif.h>
#include <stdbool.h>
#include <pthread.h>

/* Forward declarations */
struct suspended_context_state;
struct erlang_event_loop;

/* ============================================================================
 * Channel Resource Type
 * ============================================================================ */

/**
 * @brief Resource type for channel handles
 */
extern ErlNifResourceType *CHANNEL_RESOURCE_TYPE;

/**
 * @struct py_channel_t
 * @brief Channel resource for bidirectional message passing
 *
 * Channels provide a message queue backed by enif_ioq for efficient
 * buffering. When Python tries to receive from an empty queue, execution
 * suspends and resumes when Erlang sends a message.
 */
typedef struct {
    /** @brief Message queue (enif_ioq for zero-copy buffering) */
    ErlNifIOQueue *queue;

    /** @brief Maximum queue size in bytes for backpressure */
    size_t max_size;

    /** @brief Current total size of queued data */
    size_t current_size;

    /** @brief Owner process PID */
    ErlNifPid owner;

    /** @brief Mutex for thread-safe queue access */
    pthread_mutex_t mutex;

    /** @brief Suspended Python context waiting on this channel */
    struct suspended_context_state *waiting;

    /** @brief Callback ID for the waiting context */
    uint64_t waiting_callback_id;

    /** @brief Event loop for async waiter (ref-counted via enif_keep_resource) */
    struct erlang_event_loop *waiter_loop;

    /** @brief Callback ID for async waiter dispatch */
    uint64_t waiter_callback_id;

    /** @brief Flag: async waiter is registered */
    bool has_waiter;

    /** @brief Flag: channel is closed */
    bool closed;

    /** @brief Unique channel ID for debugging */
    uint64_t channel_id;
} py_channel_t;

/* ============================================================================
 * ChannelBuffer Python Type
 * ============================================================================ */

/**
 * @brief The ChannelBuffer Python type object
 */
extern PyTypeObject ChannelBufferType;

/**
 * @struct ChannelBufferObject
 * @brief Python object wrapping channel message data
 *
 * ChannelBuffer exposes dequeued binary data via the buffer protocol,
 * enabling zero-copy access to channel messages in Python.
 */
typedef struct {
    PyObject_HEAD
    unsigned char *data;           /**< Message data (owned) */
    size_t size;                   /**< Data size */
    PyObject *cached_memoryview;   /**< Cached memoryview for fast access */
} ChannelBufferObject;

/* ============================================================================
 * Function Declarations
 * ============================================================================ */

/**
 * @brief Initialize channel module
 *
 * Called during NIF load to set up channel resource type.
 *
 * @param env NIF environment
 * @return 0 on success, -1 on error
 */
int channel_init(ErlNifEnv *env);

/**
 * @brief Initialize the ChannelBuffer Python type
 *
 * Must be called during Python initialization with the GIL held.
 *
 * @return 0 on success, -1 on error
 */
int ChannelBuffer_init_type(void);

/**
 * @brief Register ChannelBuffer with erlang.channel module
 *
 * Makes ChannelBuffer accessible from Python.
 *
 * @return 0 on success, -1 on error
 *
 * @pre GIL must be held
 * @pre ChannelBuffer_init_type() must have been called
 */
int ChannelBuffer_register(void);

/**
 * @brief Create a ChannelBuffer from raw data
 *
 * @param data Data to wrap (ownership transferred to buffer)
 * @param size Size of data
 * @return New ChannelBuffer object, or NULL on error
 *
 * @pre GIL must be held
 */
PyObject *ChannelBuffer_from_data(unsigned char *data, size_t size);

/**
 * @brief Resource destructor for channels
 */
void channel_resource_dtor(ErlNifEnv *env, void *obj);

/**
 * @brief Allocate a new channel resource
 *
 * @param max_size Maximum queue size for backpressure (0 = unlimited)
 * @return New channel resource, or NULL on error
 */
py_channel_t *channel_alloc(size_t max_size);

/**
 * @brief Send a binary message to a channel
 *
 * @param channel Channel to send to
 * @param data Binary data
 * @param size Data size
 * @return 0 on success, 1 if busy (backpressure), -1 on error
 */
int channel_send(py_channel_t *channel, const unsigned char *data, size_t size);

/**
 * @brief Try to receive a message from a channel (non-blocking)
 *
 * @param channel Channel to receive from
 * @param out_data Output: message data (caller must free with enif_free)
 * @param out_size Output: message size
 * @return 0 on success, 1 if empty, -1 if closed
 */
int channel_try_receive(py_channel_t *channel, unsigned char **out_data, size_t *out_size);

/**
 * @brief Close a channel
 *
 * Closes the channel and wakes any waiting receivers with StopIteration.
 *
 * @param channel Channel to close
 */
void channel_close(py_channel_t *channel);

/**
 * @brief Resume a waiting Python context with channel data
 *
 * Called when data becomes available for a suspended receive.
 *
 * @param channel Channel with waiting context
 */
void channel_resume_waiting(py_channel_t *channel);

/* ============================================================================
 * Channel NIF Declarations
 * ============================================================================ */

/**
 * @brief Register an async waiter for a channel
 *
 * NIF: channel_wait(ChannelRef, CallbackId, LoopRef) -> ok | {ok, Data}
 */
ERL_NIF_TERM nif_channel_wait(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]);

/**
 * @brief Cancel an async waiter for a channel
 *
 * NIF: channel_cancel_wait(ChannelRef, CallbackId) -> ok
 */
ERL_NIF_TERM nif_channel_cancel_wait(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]);

#endif /* PY_CHANNEL_H */
