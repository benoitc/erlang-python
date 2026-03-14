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
 * @file py_buffer.h
 * @brief Zero-copy WSGI input buffer support
 * @author Benoit Chesneau
 *
 * This module provides a PyBuffer Python type that wraps a NIF-allocated
 * buffer resource and exposes it via the buffer protocol. Erlang can write
 * HTTP request body chunks to the buffer while Python reads them with
 * file-like methods (read, readline, readlines) or direct buffer access.
 *
 * The buffer supports blocking reads that release the GIL while waiting
 * for data from Erlang.
 *
 * Key features:
 * - Buffer protocol (memoryview(buf) works for zero-copy access)
 * - File-like interface (read, readline, readlines, seek, tell)
 * - Blocking reads with GIL released (uses pthread_cond)
 * - Iterator protocol for line-by-line reading
 */

#ifndef PY_BUFFER_H
#define PY_BUFFER_H

#include <Python.h>
#include <erl_nif.h>
#include <stdbool.h>
#include <pthread.h>

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @def PY_BUFFER_DEFAULT_CAPACITY
 * @brief Default buffer capacity when content_length is unknown (chunked)
 */
#define PY_BUFFER_DEFAULT_CAPACITY 65536

/**
 * @def PY_BUFFER_GROW_FACTOR
 * @brief Growth factor when buffer needs to expand
 */
#define PY_BUFFER_GROW_FACTOR 2

/* ============================================================================
 * Buffer Resource Type
 * ============================================================================ */

/**
 * @brief Resource type for zero-copy input buffers
 */
extern ErlNifResourceType *PY_BUFFER_RESOURCE_TYPE;

/**
 * @struct py_buffer_resource_t
 * @brief NIF resource that holds streaming input buffer data
 *
 * The buffer is written by Erlang (producer) and read by Python (consumer).
 * Uses pthread mutex/cond for thread-safe blocking reads.
 */
typedef struct {
    unsigned char *data;      /**< Buffer data */
    size_t capacity;          /**< Allocated capacity */
    size_t write_pos;         /**< Current write position (producer) */
    size_t read_pos;          /**< Current read position (consumer) */
    ssize_t content_length;   /**< Expected total size, -1 for chunked */
    pthread_mutex_t mutex;    /**< Mutex for thread-safe access */
    pthread_cond_t data_ready; /**< Condition for blocking reads */
    bool eof;                 /**< End of data flag (close called) */
    bool closed;              /**< Buffer closed flag */
    int view_count;           /**< Active Python buffer view count */
} py_buffer_resource_t;

/* ============================================================================
 * Python Type
 * ============================================================================ */

/**
 * @brief The PyBuffer Python type object
 */
extern PyTypeObject PyBufferType;

/**
 * @struct PyBufferObject
 * @brief Python object wrapping a py_buffer resource
 *
 * Provides file-like interface and buffer protocol for zero-copy access.
 */
typedef struct {
    PyObject_HEAD
    py_buffer_resource_t *resource;  /**< NIF resource (we hold a reference) */
    void *resource_ref;              /**< For releasing the resource */
} PyBufferObject;

/* ============================================================================
 * Function Declarations - NIF Resource Management
 * ============================================================================ */

/**
 * @brief Allocate a new buffer resource
 *
 * @param content_length Expected size, or -1 for chunked encoding
 * @return New resource, or NULL on error
 */
py_buffer_resource_t *py_buffer_alloc(ssize_t content_length);

/**
 * @brief Resource destructor
 */
void py_buffer_resource_dtor(ErlNifEnv *env, void *obj);

/**
 * @brief Write data to the buffer (Erlang producer side)
 *
 * Appends data to the buffer, expanding if necessary.
 * Signals waiting readers when data is available.
 *
 * @param buf Buffer resource
 * @param data Data to write
 * @param size Size of data
 * @return 0 on success, -1 on error (buffer closed or alloc failure)
 */
int py_buffer_write(py_buffer_resource_t *buf, const unsigned char *data, size_t size);

/**
 * @brief Close the buffer (Erlang producer side)
 *
 * Sets EOF flag and wakes up any waiting readers.
 *
 * @param buf Buffer resource
 */
void py_buffer_close(py_buffer_resource_t *buf);

/* ============================================================================
 * Function Declarations - Python Type
 * ============================================================================ */

/**
 * @brief Initialize the PyBuffer type
 *
 * Must be called during Python initialization with the GIL held.
 *
 * @return 0 on success, -1 on error
 */
int PyBuffer_init_type(void);

/**
 * @brief Register PyBuffer with erlang module
 *
 * Makes PyBuffer accessible from Python.
 *
 * @return 0 on success, -1 on error
 *
 * @pre GIL must be held
 * @pre PyBuffer_init_type() must have been called
 * @pre erlang module must exist
 */
int PyBuffer_register_with_module(void);

/**
 * @brief Create a PyBuffer from a NIF resource
 *
 * @param resource The buffer resource
 * @param resource_ref Resource reference (for enif_release_resource)
 * @return New PyBuffer object, or NULL on error
 *
 * @pre GIL must be held
 */
PyObject *PyBuffer_from_resource(py_buffer_resource_t *resource,
                                  void *resource_ref);

#endif /* PY_BUFFER_H */
