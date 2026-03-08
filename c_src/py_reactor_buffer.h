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
 * @file py_reactor_buffer.h
 * @brief Zero-copy buffer support for reactor protocol layer
 * @author Benoit Chesneau
 *
 * This module provides a ReactorBuffer Python type that wraps NIF-allocated
 * memory and exposes it via the buffer protocol. This enables zero-copy
 * data passing from the reactor read path to Python protocol handlers.
 *
 * The ReactorBuffer type behaves like bytes:
 * - Buffer protocol (memoryview(buf) works)
 * - __bytes__() method (bytes(buf) works)
 * - Sequence protocol (len(buf), buf[0], buf[0:10] work)
 * - String methods via delegation (buf.startswith(), buf.find() work)
 */

#ifndef PY_REACTOR_BUFFER_H
#define PY_REACTOR_BUFFER_H

#include <Python.h>
#include <erl_nif.h>
#include <stdbool.h>

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @def REACTOR_ZERO_COPY_THRESHOLD
 * @brief Minimum read size to use zero-copy buffer
 *
 * For small reads, the overhead of creating a buffer resource may exceed
 * the benefit of zero-copy. Below this threshold, we use regular bytes.
 */
#define REACTOR_ZERO_COPY_THRESHOLD 1024

/**
 * @def REACTOR_MAX_READ_SIZE
 * @brief Maximum single read size
 */
#define REACTOR_MAX_READ_SIZE 65536

/* ============================================================================
 * Buffer Resource Type
 * ============================================================================ */

/**
 * @brief Resource type for zero-copy read buffers
 */
extern ErlNifResourceType *REACTOR_BUFFER_RESOURCE_TYPE;

/**
 * @struct reactor_buffer_resource_t
 * @brief NIF resource that holds read buffer data
 *
 * The data is allocated via enif_alloc and freed when all Python
 * references are released.
 */
typedef struct {
    unsigned char *data;    /**< Buffer data */
    size_t size;            /**< Actual data size */
    size_t capacity;        /**< Allocated capacity */
    int ref_count;          /**< Python buffer view reference count */
} reactor_buffer_resource_t;

/* ============================================================================
 * Python Type
 * ============================================================================ */

/**
 * @brief The ReactorBuffer Python type object
 */
extern PyTypeObject ReactorBufferType;

/**
 * @struct ReactorBufferObject
 * @brief Python object wrapping a reactor buffer resource
 */
typedef struct {
    PyObject_HEAD
    reactor_buffer_resource_t *resource;  /**< NIF resource (we hold a reference) */
    void *resource_ref;                   /**< For releasing the resource */
} ReactorBufferObject;

/* ============================================================================
 * Function Declarations
 * ============================================================================ */

/**
 * @brief Initialize the ReactorBuffer type
 *
 * Must be called during Python initialization with the GIL held.
 *
 * @return 0 on success, -1 on error
 */
int ReactorBuffer_init_type(void);

/**
 * @brief Register ReactorBuffer with erlang.reactor module
 *
 * Makes ReactorBuffer accessible from Python for testing via
 * erlang.reactor.ReactorBuffer._test_create()
 *
 * @return 0 on success, -1 on error
 *
 * @pre GIL must be held
 * @pre ReactorBuffer_init_type() must have been called
 * @pre erlang.reactor module must exist
 */
int ReactorBuffer_register_with_reactor(void);

/**
 * @brief Create a ReactorBuffer from a NIF resource
 *
 * @param resource The buffer resource
 * @param resource_ref Resource reference (for enif_release_resource)
 * @return New ReactorBuffer object, or NULL on error
 *
 * @pre GIL must be held
 */
PyObject *ReactorBuffer_from_resource(reactor_buffer_resource_t *resource,
                                       void *resource_ref);

/**
 * @brief Allocate a new buffer resource
 *
 * @param capacity Initial capacity
 * @return New resource, or NULL on error
 */
reactor_buffer_resource_t *reactor_buffer_alloc(size_t capacity);

/**
 * @brief Resource destructor
 */
void reactor_buffer_resource_dtor(ErlNifEnv *env, void *obj);

/**
 * @brief Read from fd into a buffer resource
 *
 * Reads up to max_size bytes from fd into a newly allocated buffer resource.
 *
 * @param fd File descriptor to read from
 * @param max_size Maximum bytes to read
 * @param out_resource Output: the buffer resource
 * @param out_size Output: actual bytes read
 * @return 0 on success, -1 on error, 1 on EOF
 */
int reactor_buffer_read_fd(int fd, size_t max_size,
                           reactor_buffer_resource_t **out_resource,
                           size_t *out_size);

#endif /* PY_REACTOR_BUFFER_H */
