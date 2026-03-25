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
 * @file py_parallel_pool.h
 * @brief OWN_GIL parallel Python execution pool
 * @author Benoit Chesneau
 *
 * This module implements transparent parallel Python execution via OWN_GIL
 * subinterpreters. When enabled via build flag (-DENABLE_PARALLEL_PYTHON),
 * contexts are automatically assigned to subinterpreters that each have their
 * own GIL, enabling true parallel execution.
 *
 * Architecture:
 *   - N subinterpreters created at startup (N = number of schedulers)
 *   - Each subinterpreter has its own GIL (OWN_GIL mode)
 *   - Contexts are assigned to slots via round-robin
 *   - User API is unchanged - parallelism is transparent
 *
 * Build:
 *   Default:  rebar3 compile                          (worker mode)
 *   Parallel: CMAKE_OPTIONS="-DENABLE_PARALLEL_PYTHON=ON" rebar3 compile
 */

#ifndef PY_PARALLEL_POOL_H
#define PY_PARALLEL_POOL_H

#ifdef ENABLE_PARALLEL_PYTHON

#include <Python.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

/**
 * @def MAX_PARALLEL_SLOTS
 * @brief Maximum number of parallel slots (subinterpreters)
 */
#define MAX_PARALLEL_SLOTS 64

/**
 * @def DEFAULT_PARALLEL_SLOTS
 * @brief Default number of parallel slots (matches typical scheduler count)
 */
#define DEFAULT_PARALLEL_SLOTS 8

/* ============================================================================
 * Types
 * ============================================================================ */

/**
 * @struct parallel_slot_t
 * @brief A single OWN_GIL subinterpreter slot
 *
 * Each slot represents an isolated Python subinterpreter with its own GIL.
 * Multiple contexts may share a slot (serialized by the slot's GIL).
 */
typedef struct {
    /** @brief Slot ID (0 to N-1) */
    int slot_id;

    /** @brief Python interpreter state */
    PyInterpreterState *interp;

    /** @brief Thread state for this interpreter */
    PyThreadState *tstate;

    /** @brief Global namespace dictionary */
    PyObject *globals;

    /** @brief Local namespace dictionary */
    PyObject *locals;

    /** @brief Module cache (Dict: module_name -> PyModule) */
    PyObject *module_cache;

    /** @brief Whether this slot is initialized and ready */
    bool initialized;
} parallel_slot_t;

/* ============================================================================
 * Pool Management API
 * ============================================================================ */

/**
 * @brief Initialize the parallel pool
 *
 * Creates `size` OWN_GIL subinterpreters. Must be called after Python is
 * initialized and with the main GIL held.
 *
 * @param size Number of subinterpreters to create (0 = default, capped at MAX)
 * @return 0 on success, -1 on failure
 */
int parallel_pool_init(int size);

/**
 * @brief Shutdown the parallel pool
 *
 * Destroys all subinterpreters and cleans up resources.
 * Must be called with main GIL held during Python finalization.
 */
void parallel_pool_shutdown(void);

/**
 * @brief Check if the parallel pool is initialized
 *
 * @return true if pool is ready for use
 */
bool parallel_pool_is_initialized(void);

/**
 * @brief Get pool size
 *
 * @return Number of slots in the pool
 */
int parallel_pool_size(void);

/* ============================================================================
 * Slot Assignment API
 * ============================================================================ */

/**
 * @brief Assign a slot to a new context (round-robin)
 *
 * Atomically selects the next slot for a context. Thread-safe.
 * Contexts share slots - serialization happens via the slot's GIL.
 *
 * @return Slot ID (0 to pool_size-1)
 */
int parallel_pool_assign_slot(void);

/**
 * @brief Get a slot by ID
 *
 * @param slot_id Slot index
 * @return Pointer to the slot, or NULL if invalid
 */
parallel_slot_t *parallel_pool_get(int slot_id);

/* ============================================================================
 * Execution API
 * ============================================================================ */

/**
 * @brief Acquire a slot's GIL for Python execution
 *
 * Must be called before any Python operations in the slot's interpreter.
 * This acquires the slot's own GIL (not the main interpreter's GIL).
 *
 * @param slot Slot to acquire
 *
 * @note Caller MUST call parallel_slot_release() when done
 */
void parallel_slot_acquire(parallel_slot_t *slot);

/**
 * @brief Release a slot's GIL after Python execution
 *
 * Must be called after Python operations are complete.
 *
 * @param slot Slot to release
 */
void parallel_slot_release(parallel_slot_t *slot);

/**
 * @brief Get the globals dict for a slot
 *
 * @param slot Slot to query
 * @return Borrowed reference to globals dict
 */
static inline PyObject *parallel_slot_globals(parallel_slot_t *slot) {
    return slot->globals;
}

/**
 * @brief Get the locals dict for a slot
 *
 * @param slot Slot to query
 * @return Borrowed reference to locals dict
 */
static inline PyObject *parallel_slot_locals(parallel_slot_t *slot) {
    return slot->locals;
}

/**
 * @brief Get the module cache for a slot
 *
 * @param slot Slot to query
 * @return Borrowed reference to module cache dict
 */
static inline PyObject *parallel_slot_module_cache(parallel_slot_t *slot) {
    return slot->module_cache;
}

#endif /* ENABLE_PARALLEL_PYTHON */

#endif /* PY_PARALLEL_POOL_H */
