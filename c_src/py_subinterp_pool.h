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
 * @file py_subinterp_pool.h
 * @brief Subinterpreter pool for shared-GIL execution model
 * @author Benoit Chesneau
 *
 * This module implements a pool of pre-created subinterpreters with shared GIL.
 * Based on PEP 554/734's InterpreterPoolExecutor pattern: pool of subinterpreters
 * executed by a thread pool (dirty schedulers in our case).
 *
 * The key insight is that subinterpreters provide namespace isolation without
 * requiring OWN_GIL. With shared GIL, we can use PyThreadState_Swap() on dirty
 * schedulers to switch between interpreters efficiently.
 *
 * Benefits over OWN_GIL thread-per-context model:
 * - No dedicated pthread per context (saves resources)
 * - No mutex/condvar dispatch overhead (direct execution)
 * - No term copying between envs (safe enif_make_* on dirty scheduler)
 * - ~4x better performance (400K vs 100K calls/sec)
 */

#ifndef PY_SUBINTERP_POOL_H
#define PY_SUBINTERP_POOL_H

#include <Python.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

#ifdef HAVE_SUBINTERPRETERS

/**
 * @def MAX_SUBINTERPRETERS
 * @brief Maximum number of subinterpreters in the pool
 */
#define MAX_SUBINTERPRETERS 64

/**
 * @def DEFAULT_POOL_SIZE
 * @brief Default number of subinterpreters to create at startup
 *
 * This should be large enough to cover the number of schedulers on the
 * machine, since each context typically gets one subinterpreter.
 */
#define DEFAULT_POOL_SIZE 32

/**
 * @struct subinterp_slot_t
 * @brief A single slot in the subinterpreter pool
 *
 * Each slot represents one subinterpreter with its associated state.
 * Slots are pre-created at pool initialization and reused.
 */
typedef struct {
    /** @brief Python interpreter state */
    PyInterpreterState *interp;

    /** @brief Thread state for this interpreter (used with PyThreadState_Swap) */
    PyThreadState *tstate;

    /** @brief Global namespace dictionary (__globals__) */
    PyObject *globals;

    /** @brief Local namespace dictionary (__locals__) */
    PyObject *locals;

    /** @brief Module cache (Dict: module_name -> PyModule) */
    PyObject *module_cache;

    /** @brief Whether this slot is initialized and ready for use */
    bool initialized;
} subinterp_slot_t;

/**
 * @brief Initialize the subinterpreter pool
 *
 * Creates `size` subinterpreters with shared GIL. Must be called
 * with the main GIL held (during Python initialization).
 *
 * @param size Number of subinterpreters to create (capped at MAX_SUBINTERPRETERS)
 * @return 0 on success, -1 on failure
 *
 * @note Call subinterp_pool_shutdown() to clean up
 */
int subinterp_pool_init(int size);

/**
 * @brief Allocate a slot from the pool
 *
 * Finds an available slot and marks it as in-use atomically.
 * Thread-safe.
 *
 * @return Slot index (0 to pool_size-1), or -1 if pool is exhausted
 */
int subinterp_pool_alloc(void);

/**
 * @brief Release a slot back to the pool
 *
 * Marks the slot as available. Thread-safe.
 *
 * @param slot Slot index to release
 */
void subinterp_pool_free(int slot);

/**
 * @brief Get a slot by index
 *
 * @param slot Slot index
 * @return Pointer to the slot, or NULL if invalid index
 */
subinterp_slot_t *subinterp_pool_get(int slot);

/**
 * @brief Check if a slot is currently allocated
 *
 * @param slot Slot index
 * @return true if allocated, false if free or invalid
 */
bool subinterp_pool_is_allocated(int slot);

/**
 * @brief Get current pool size
 *
 * @return Number of slots in the pool
 */
int subinterp_pool_size(void);

/**
 * @brief Get number of currently allocated slots
 *
 * @return Count of slots currently in use
 */
int subinterp_pool_allocated_count(void);

/**
 * @brief Shutdown the pool and clean up all subinterpreters
 *
 * Must be called with the main GIL held (during Python finalization).
 * All slots must be released before calling this.
 */
void subinterp_pool_shutdown(void);

/**
 * @brief Check if the pool is initialized
 *
 * @return true if pool is initialized and ready for use
 */
bool subinterp_pool_is_initialized(void);

#endif /* HAVE_SUBINTERPRETERS */

#endif /* PY_SUBINTERP_POOL_H */
