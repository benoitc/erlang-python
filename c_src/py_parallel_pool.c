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
 * @file py_parallel_pool.c
 * @brief OWN_GIL parallel Python execution pool implementation
 * @author Benoit Chesneau
 *
 * Implements a pool of OWN_GIL subinterpreters for true parallel Python
 * execution. Each subinterpreter has its own GIL, allowing concurrent
 * Python execution across multiple Erlang schedulers.
 */

#include "py_parallel_pool.h"

#ifdef ENABLE_PARALLEL_PYTHON

#include "py_nif.h"
#include "py_buffer.h"
#include "py_reactor_buffer.h"
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ============================================================================
 * Pool State
 * ============================================================================ */

/** @brief Pool of OWN_GIL subinterpreters */
static parallel_slot_t g_parallel_pool[MAX_PARALLEL_SLOTS];

/** @brief Current parallel pool size */
static int g_parallel_pool_size = 0;

/** @brief Parallel pool initialized flag */
static _Atomic bool g_parallel_pool_initialized = false;

/** @brief Round-robin counter for slot assignment */
static _Atomic uint64_t g_parallel_next_slot = 0;

/* ============================================================================
 * Forward declarations for functions defined in other modules
 * ============================================================================ */

/* Defined in py_callback.c, included from py_nif.c */
extern int create_erlang_module(void);

/* Defined in py_event_loop.c, included from py_nif.c */
extern int init_subinterpreter_event_loop(ErlNifEnv *env);

/* ============================================================================
 * Pool Implementation
 * ============================================================================ */

int parallel_pool_init(int size) {
    if (atomic_load(&g_parallel_pool_initialized)) {
        return 0;  /* Already initialized */
    }

    /* Set default/cap size */
    if (size <= 0) {
        size = DEFAULT_PARALLEL_SLOTS;
    }
    if (size > MAX_PARALLEL_SLOTS) {
        size = MAX_PARALLEL_SLOTS;
    }

    /* Clear pool state */
    memset(g_parallel_pool, 0, sizeof(g_parallel_pool));
    atomic_store(&g_parallel_next_slot, 0);

    /* Note: When creating OWN_GIL subinterpreters, the main GIL is released
     * after each Py_NewInterpreterFromConfig call and we need to re-acquire
     * it with PyGILState_Ensure() for the next iteration. */

    for (int i = 0; i < size; i++) {
        parallel_slot_t *slot = &g_parallel_pool[i];
        slot->slot_id = i;

        /* Configure subinterpreter with OWN_GIL for true parallelism */
        PyInterpreterConfig config = {
            .use_main_obmalloc = 0,
            .allow_fork = 0,
            .allow_exec = 0,
            .allow_threads = 1,
            .allow_daemon_threads = 0,
            .check_multi_interp_extensions = 1,
            .gil = PyInterpreterConfig_OWN_GIL,  /* Key: each has its own GIL */
        };

        PyThreadState *tstate = NULL;
        PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);

        if (PyStatus_Exception(status) || tstate == NULL) {
            fprintf(stderr, "parallel_pool_init: failed to create OWN_GIL subinterp %d\n", i);

            /* When OWN_GIL creation fails, we may be in an undefined state.
             * We need to restore to main interpreter before cleanup. */

            /* Clean up already created interpreters */
            for (int j = 0; j < i; j++) {
                if (g_parallel_pool[j].initialized) {
                    /* Acquire the slot's GIL to clean it up */
                    PyEval_RestoreThread(g_parallel_pool[j].tstate);
                    Py_XDECREF(g_parallel_pool[j].module_cache);
                    Py_XDECREF(g_parallel_pool[j].globals);
                    Py_XDECREF(g_parallel_pool[j].locals);
                    Py_EndInterpreter(g_parallel_pool[j].tstate);
                }
            }

            /* Try to restore main thread state */
            PyGILState_Ensure();
            return -1;
        }

        /* Now we're in the new subinterpreter's context with its own GIL.
         * The main GIL was released when OWN_GIL subinterpreter was created. */
        slot->interp = PyThreadState_GetInterpreter(tstate);
        slot->tstate = tstate;

        /* Initialize globals/locals/module_cache */
        slot->globals = PyDict_New();
        slot->locals = PyDict_New();
        slot->module_cache = PyDict_New();

        if (slot->globals == NULL || slot->locals == NULL || slot->module_cache == NULL) {
            /* Cleanup on failure */
            Py_XDECREF(slot->module_cache);
            Py_XDECREF(slot->globals);
            Py_XDECREF(slot->locals);
            Py_EndInterpreter(tstate);

            /* Clean up already created interpreters */
            for (int j = 0; j < i; j++) {
                if (g_parallel_pool[j].initialized) {
                    PyEval_RestoreThread(g_parallel_pool[j].tstate);
                    Py_XDECREF(g_parallel_pool[j].module_cache);
                    Py_XDECREF(g_parallel_pool[j].globals);
                    Py_XDECREF(g_parallel_pool[j].locals);
                    Py_EndInterpreter(g_parallel_pool[j].tstate);
                }
            }

            PyGILState_Ensure();
            return -1;
        }

        /* Import __builtins__ into globals */
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(slot->globals, "__builtins__", builtins);

        /* Create erlang module in this subinterpreter */
        if (create_erlang_module() < 0) {
            fprintf(stderr, "parallel_pool_init: failed to create erlang module in slot %d\n", i);
            log_and_clear_python_error("parallel_pool create_erlang_module");
            /* Non-fatal - continue without erlang module */
        } else {
            /* Register ReactorBuffer with erlang module */
            if (ReactorBuffer_register_with_reactor() < 0) {
                log_and_clear_python_error("parallel_pool ReactorBuffer_register");
            }

            /* Register PyBuffer with erlang module */
            if (PyBuffer_register_with_module() < 0) {
                log_and_clear_python_error("parallel_pool PyBuffer_register");
            }

            /* Import erlang module into globals */
            PyObject *erlang_module = PyImport_ImportModule("erlang");
            if (erlang_module != NULL) {
                PyDict_SetItemString(slot->globals, "erlang", erlang_module);
                Py_DECREF(erlang_module);
            } else {
                log_and_clear_python_error("parallel_pool erlang import");
            }
        }

        /* Initialize event loop for this subinterpreter */
        if (init_subinterpreter_event_loop(NULL) < 0) {
            fprintf(stderr, "parallel_pool_init: failed to init event loop in slot %d\n", i);
            log_and_clear_python_error("parallel_pool event_loop_init");
            /* Non-fatal - async features won't work */
        }

        slot->initialized = true;
        atomic_store(&slot->active_count, 0);
        atomic_store(&slot->shutdown_requested, false);

        /* Release this slot's GIL (save thread state).
         * After this, we need to re-acquire main GIL to create next subinterpreter. */
        PyEval_SaveThread();

        /* Re-acquire main GIL for next iteration */
        PyGILState_Ensure();
    }

    g_parallel_pool_size = size;
    atomic_store(&g_parallel_pool_initialized, true);

#ifdef DEBUG
    fprintf(stderr, "parallel_pool_init: created %d OWN_GIL subinterpreters\n", size);
#endif

    return 0;
}

/** @brief Maximum time to wait for active slots during shutdown (seconds) */
#define SHUTDOWN_TIMEOUT_SECS 5

void parallel_pool_shutdown(void) {
    if (!atomic_load(&g_parallel_pool_initialized)) {
        return;
    }

    /* Mark as not initialized to prevent new assignments */
    atomic_store(&g_parallel_pool_initialized, false);

    /* Phase 1: Request shutdown on all slots to prevent new acquisitions */
    for (int i = 0; i < g_parallel_pool_size; i++) {
        parallel_slot_t *slot = &g_parallel_pool[i];
        if (slot->initialized) {
            atomic_store(&slot->shutdown_requested, true);
        }
    }

    /* Phase 2: Wait for all active operations to complete */
    struct timespec start_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (int i = 0; i < g_parallel_pool_size; i++) {
        parallel_slot_t *slot = &g_parallel_pool[i];
        if (!slot->initialized) {
            continue;
        }

        /* Wait for slot to become idle */
        while (atomic_load(&slot->active_count) > 0) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            double elapsed = (now.tv_sec - start_time.tv_sec) +
                           (now.tv_nsec - start_time.tv_nsec) / 1e9;

            if (elapsed > SHUTDOWN_TIMEOUT_SECS) {
                /* Timeout waiting for slot - force shutdown */
                break;
            }

            /* Brief sleep to avoid busy-waiting */
            usleep(1000);  /* 1ms */
        }
    }

    /* Phase 3: Clean up each subinterpreter */
    for (int i = 0; i < g_parallel_pool_size; i++) {
        parallel_slot_t *slot = &g_parallel_pool[i];

        if (!slot->initialized) {
            continue;
        }

        /* Acquire this slot's GIL.
         * At this point, no other thread should be using it (we waited above). */
        PyEval_RestoreThread(slot->tstate);

        /* Clean up Python objects */
        Py_XDECREF(slot->module_cache);
        Py_XDECREF(slot->globals);
        Py_XDECREF(slot->locals);

        /* End the interpreter (releases its GIL) */
        Py_EndInterpreter(slot->tstate);

        slot->interp = NULL;
        slot->tstate = NULL;
        slot->globals = NULL;
        slot->locals = NULL;
        slot->module_cache = NULL;
        slot->initialized = false;
    }

    /* Reset pool state */
    g_parallel_pool_size = 0;
    atomic_store(&g_parallel_next_slot, 0);
}

bool parallel_pool_is_initialized(void) {
    return atomic_load(&g_parallel_pool_initialized);
}

int parallel_pool_size(void) {
    return g_parallel_pool_size;
}

/* ============================================================================
 * Slot Assignment
 * ============================================================================ */

int parallel_pool_assign_slot(void) {
    if (!atomic_load(&g_parallel_pool_initialized) || g_parallel_pool_size == 0) {
        return 0;  /* Fallback to first slot */
    }

    /* Round-robin assignment */
    uint64_t slot = atomic_fetch_add(&g_parallel_next_slot, 1);
    return (int)(slot % g_parallel_pool_size);
}

parallel_slot_t *parallel_pool_get(int slot_id) {
    if (slot_id < 0 || slot_id >= g_parallel_pool_size) {
        return NULL;
    }

    if (!g_parallel_pool[slot_id].initialized) {
        return NULL;
    }

    return &g_parallel_pool[slot_id];
}

/* ============================================================================
 * Execution API
 * ============================================================================ */

bool parallel_slot_acquire(parallel_slot_t *slot) {
    if (slot == NULL || !slot->initialized) {
        return false;
    }

    /* Check if shutdown is in progress - reject new acquisitions */
    if (atomic_load(&slot->shutdown_requested)) {
        return false;
    }

    /* Increment active count BEFORE acquiring GIL.
     * This ensures shutdown waits for us. */
    atomic_fetch_add(&slot->active_count, 1);

    /* Double-check shutdown wasn't requested while we incremented */
    if (atomic_load(&slot->shutdown_requested)) {
        atomic_fetch_sub(&slot->active_count, 1);
        return false;
    }

    /* Acquire this slot's GIL by restoring its thread state.
     * This will block if another thread holds this slot's GIL. */
    PyEval_RestoreThread(slot->tstate);
    return true;
}

void parallel_slot_release(parallel_slot_t *slot) {
    if (slot == NULL || !slot->initialized) {
        return;
    }

    /* Release this slot's GIL by saving the thread state */
    slot->tstate = PyEval_SaveThread();

    /* Decrement active count AFTER releasing GIL.
     * This signals to shutdown that we're done. */
    atomic_fetch_sub(&slot->active_count, 1);
}

#endif /* ENABLE_PARALLEL_PYTHON */
