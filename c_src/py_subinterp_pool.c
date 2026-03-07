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
 * @file py_subinterp_pool.c
 * @brief Subinterpreter pool implementation
 * @author Benoit Chesneau
 *
 * Implements a pool of pre-created subinterpreters with shared GIL.
 * The pool is initialized at Python startup and provides fast allocation
 * of isolated Python namespaces for contexts.
 */

#include "py_subinterp_pool.h"
#include <string.h>

#ifdef HAVE_SUBINTERPRETERS

/* ============================================================================
 * Pool State
 * ============================================================================ */

/** @brief Pool of subinterpreters */
static subinterp_slot_t g_subinterp_pool[MAX_SUBINTERPRETERS];

/** @brief Bitmap of allocated slots (1 = in-use, 0 = free) */
static _Atomic uint64_t g_pool_allocation = 0;

/** @brief Current pool size */
static int g_pool_size = 0;

/** @brief Pool initialized flag */
static _Atomic bool g_pool_initialized = false;

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

int subinterp_pool_init(int size) {
    if (atomic_load(&g_pool_initialized)) {
        return 0;  /* Already initialized */
    }

    /* Cap size to maximum */
    if (size <= 0) {
        size = DEFAULT_POOL_SIZE;
    }
    if (size > MAX_SUBINTERPRETERS) {
        size = MAX_SUBINTERPRETERS;
    }

    /* Clear pool state */
    memset(g_subinterp_pool, 0, sizeof(g_subinterp_pool));
    atomic_store(&g_pool_allocation, 0);

    /* Create subinterpreters.
     * We need to be careful here: Py_NewInterpreterFromConfig changes
     * the current thread state. We save the main state and restore
     * after creating each subinterpreter. */
    PyThreadState *main_tstate = PyThreadState_Get();

    for (int i = 0; i < size; i++) {
        subinterp_slot_t *slot = &g_subinterp_pool[i];

        /* Configure subinterpreter with SHARED GIL (default).
         * PyInterpreterConfig_SHARED_GIL (0) shares the GIL with the main interpreter.
         * This is the key difference from OWN_GIL which gives each interpreter its own GIL. */
        PyInterpreterConfig config = {
            .use_main_obmalloc = 0,
            .allow_fork = 0,
            .allow_exec = 0,
            .allow_threads = 1,
            .allow_daemon_threads = 0,
            .check_multi_interp_extensions = 1,
            .gil = PyInterpreterConfig_SHARED_GIL,  /* SHARED GIL for pool model */
        };

        PyThreadState *tstate = NULL;
        PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);

        if (PyStatus_Exception(status) || tstate == NULL) {
            /* Failed to create subinterpreter - clean up and fail */
            fprintf(stderr, "subinterp_pool_init: failed to create subinterp %d\n", i);

            /* Restore main thread state before cleanup */
            PyThreadState_Swap(main_tstate);

            /* Clean up already created interpreters */
            for (int j = 0; j < i; j++) {
                if (g_subinterp_pool[j].initialized) {
                    PyThreadState_Swap(g_subinterp_pool[j].tstate);
                    Py_XDECREF(g_subinterp_pool[j].module_cache);
                    Py_XDECREF(g_subinterp_pool[j].globals);
                    Py_XDECREF(g_subinterp_pool[j].locals);
                    Py_EndInterpreter(g_subinterp_pool[j].tstate);
                }
            }
            PyThreadState_Swap(main_tstate);
            return -1;
        }

        /* tstate is now the current thread state for the new interpreter */
        slot->interp = PyThreadState_GetInterpreter(tstate);
        slot->tstate = tstate;

        /* Initialize globals/locals */
        slot->globals = PyDict_New();
        slot->locals = PyDict_New();
        slot->module_cache = PyDict_New();

        if (slot->globals == NULL || slot->locals == NULL || slot->module_cache == NULL) {
            /* Cleanup on failure */
            Py_XDECREF(slot->module_cache);
            Py_XDECREF(slot->globals);
            Py_XDECREF(slot->locals);
            Py_EndInterpreter(tstate);
            PyThreadState_Swap(main_tstate);

            /* Clean up already created interpreters */
            for (int j = 0; j < i; j++) {
                if (g_subinterp_pool[j].initialized) {
                    PyThreadState_Swap(g_subinterp_pool[j].tstate);
                    Py_XDECREF(g_subinterp_pool[j].module_cache);
                    Py_XDECREF(g_subinterp_pool[j].globals);
                    Py_XDECREF(g_subinterp_pool[j].locals);
                    Py_EndInterpreter(g_subinterp_pool[j].tstate);
                }
            }
            PyThreadState_Swap(main_tstate);
            return -1;
        }

        /* Import __builtins__ into globals */
        PyObject *builtins = PyEval_GetBuiltins();
        PyDict_SetItemString(slot->globals, "__builtins__", builtins);

        /* Create erlang module in this subinterpreter */
        if (create_erlang_module() < 0) {
            fprintf(stderr, "subinterp_pool_init: failed to create erlang module in subinterp %d\n", i);
            PyErr_Clear();
            /* Non-fatal - continue without erlang module */
        } else {
            /* Import erlang module into globals */
            PyObject *erlang_module = PyImport_ImportModule("erlang");
            if (erlang_module != NULL) {
                PyDict_SetItemString(slot->globals, "erlang", erlang_module);
                Py_DECREF(erlang_module);
            } else {
                PyErr_Clear();
            }
        }

        /* Initialize event loop for this subinterpreter.
         * This enables asyncio support (sleep, timers, etc.) */
        if (init_subinterpreter_event_loop(NULL) < 0) {
            fprintf(stderr, "subinterp_pool_init: failed to init event loop in subinterp %d\n", i);
            PyErr_Clear();
            /* Non-fatal - async features just won't work */
        }

        slot->initialized = true;

        /* Swap back to main thread state before creating next subinterpreter */
        PyThreadState_Swap(main_tstate);
    }

    g_pool_size = size;
    atomic_store(&g_pool_initialized, true);

#ifdef DEBUG
    fprintf(stderr, "subinterp_pool_init: created %d subinterpreters with shared GIL\n", size);
#endif

    return 0;
}

int subinterp_pool_alloc(void) {
    if (!atomic_load(&g_pool_initialized)) {
        return -1;
    }

    /* Try to find and allocate a free slot using CAS */
    while (1) {
        uint64_t current = atomic_load(&g_pool_allocation);

        /* Find first free bit (0 bit) */
        int slot = -1;
        for (int i = 0; i < g_pool_size; i++) {
            if ((current & (1ULL << i)) == 0) {
                slot = i;
                break;
            }
        }

        if (slot < 0) {
            /* Pool exhausted */
            return -1;
        }

        /* Try to set the bit atomically */
        uint64_t new_val = current | (1ULL << slot);
        if (atomic_compare_exchange_weak(&g_pool_allocation, &current, new_val)) {
            return slot;
        }
        /* CAS failed, retry */
    }
}

void subinterp_pool_free(int slot) {
    if (slot < 0 || slot >= g_pool_size) {
        return;
    }

    /* Clear the allocation bit atomically */
    uint64_t mask = ~(1ULL << slot);
    atomic_fetch_and(&g_pool_allocation, mask);
}

subinterp_slot_t *subinterp_pool_get(int slot) {
    if (slot < 0 || slot >= g_pool_size) {
        return NULL;
    }

    if (!g_subinterp_pool[slot].initialized) {
        return NULL;
    }

    return &g_subinterp_pool[slot];
}

bool subinterp_pool_is_allocated(int slot) {
    if (slot < 0 || slot >= g_pool_size) {
        return false;
    }

    uint64_t current = atomic_load(&g_pool_allocation);
    return (current & (1ULL << slot)) != 0;
}

int subinterp_pool_size(void) {
    return g_pool_size;
}

int subinterp_pool_allocated_count(void) {
    uint64_t current = atomic_load(&g_pool_allocation);
    int count = 0;

    /* Count set bits (popcount) */
    while (current) {
        count += current & 1;
        current >>= 1;
    }

    return count;
}

void subinterp_pool_shutdown(void) {
    if (!atomic_load(&g_pool_initialized)) {
        return;
    }

    /* Mark as not initialized first to prevent new allocations */
    atomic_store(&g_pool_initialized, false);

    /* Get current thread state (should be main interpreter) */
    PyThreadState *main_tstate = PyThreadState_Get();

    /* Clean up each subinterpreter */
    for (int i = 0; i < g_pool_size; i++) {
        subinterp_slot_t *slot = &g_subinterp_pool[i];

        if (!slot->initialized) {
            continue;
        }

        /* Swap to the subinterpreter's thread state */
        PyThreadState_Swap(slot->tstate);

        /* Clean up Python objects */
        Py_XDECREF(slot->module_cache);
        Py_XDECREF(slot->globals);
        Py_XDECREF(slot->locals);

        /* End the interpreter */
        Py_EndInterpreter(slot->tstate);

        slot->interp = NULL;
        slot->tstate = NULL;
        slot->globals = NULL;
        slot->locals = NULL;
        slot->module_cache = NULL;
        slot->initialized = false;
    }

    /* Restore main thread state */
    PyThreadState_Swap(main_tstate);

    /* Reset pool state */
    g_pool_size = 0;
    atomic_store(&g_pool_allocation, 0);

#ifdef DEBUG
    fprintf(stderr, "subinterp_pool_shutdown: cleaned up all subinterpreters\n");
#endif
}

bool subinterp_pool_is_initialized(void) {
    return atomic_load(&g_pool_initialized);
}

#endif /* HAVE_SUBINTERPRETERS */
