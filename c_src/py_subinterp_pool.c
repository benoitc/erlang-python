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
#include "py_reactor_buffer.h"
#include "py_buffer.h"
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

/** @brief Global import generation counter */
static _Atomic uint64_t g_import_generation = 0;

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
            log_and_clear_python_error("subinterp create_erlang_module");
            /* Non-fatal - continue without erlang module */
        } else {
            /* Register ReactorBuffer with erlang module in this subinterpreter */
            if (ReactorBuffer_register_with_reactor() < 0) {
                log_and_clear_python_error("subinterp ReactorBuffer_register");
                /* Non-fatal - ReactorBuffer just won't be available */
            }

            /* Register PyBuffer with erlang module in this subinterpreter */
            if (PyBuffer_register_with_module() < 0) {
                log_and_clear_python_error("subinterp PyBuffer_register");
                /* Non-fatal - PyBuffer just won't be available */
            }

            /* Import erlang module into globals */
            PyObject *erlang_module = PyImport_ImportModule("erlang");
            if (erlang_module != NULL) {
                PyDict_SetItemString(slot->globals, "erlang", erlang_module);
                Py_DECREF(erlang_module);
            } else {
                log_and_clear_python_error("subinterp erlang import");
            }
        }

        /* Initialize event loop for this subinterpreter.
         * This enables asyncio support (sleep, timers, etc.) */
        if (init_subinterpreter_event_loop(NULL) < 0) {
            fprintf(stderr, "subinterp_pool_init: failed to init event loop in subinterp %d\n", i);
            log_and_clear_python_error("subinterp event_loop_init");
            /* Non-fatal - async features just won't work */
        }

        slot->initialized = true;
        atomic_store(&slot->usage_count, 0);
        atomic_store(&slot->marked_stale, false);
        slot->generation = atomic_load(&g_import_generation);

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

void subinterp_pool_acquire(int slot) {
    if (slot < 0 || slot >= g_pool_size) {
        return;
    }

    subinterp_slot_t *s = &g_subinterp_pool[slot];
    if (!s->initialized) {
        return;
    }

    atomic_fetch_add(&s->usage_count, 1);
}

/**
 * @brief Reinitialize a pool slot with a fresh subinterpreter
 *
 * Called after destroying a stale subinterpreter to create a fresh one.
 * Assumes GIL is already held.
 *
 * @param slot_idx Slot index
 * @return 0 on success, -1 on failure
 */
static int subinterp_pool_reinit_slot(int slot_idx) {
    subinterp_slot_t *slot = &g_subinterp_pool[slot_idx];

    /* Save main thread state */
    PyThreadState *main_tstate = PyThreadState_Get();

    /* Configure subinterpreter with SHARED GIL */
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 1,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_SHARED_GIL,
    };

    PyThreadState *tstate = NULL;
    PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);

    if (PyStatus_Exception(status) || tstate == NULL) {
        fprintf(stderr, "subinterp_pool_reinit_slot: failed to create subinterp for slot %d\n", slot_idx);
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
        Py_XDECREF(slot->module_cache);
        Py_XDECREF(slot->globals);
        Py_XDECREF(slot->locals);
        Py_EndInterpreter(tstate);
        PyThreadState_Swap(main_tstate);
        return -1;
    }

    /* Import __builtins__ into globals */
    PyObject *builtins = PyEval_GetBuiltins();
    PyDict_SetItemString(slot->globals, "__builtins__", builtins);

    /* Create erlang module in this subinterpreter */
    if (create_erlang_module() >= 0) {
        /* Register ReactorBuffer and PyBuffer */
        ReactorBuffer_register_with_reactor();
        PyBuffer_register_with_module();

        /* Import erlang module into globals */
        PyObject *erlang_module = PyImport_ImportModule("erlang");
        if (erlang_module != NULL) {
            PyDict_SetItemString(slot->globals, "erlang", erlang_module);
            Py_DECREF(erlang_module);
        }
    }

    /* Initialize event loop for this subinterpreter */
    init_subinterpreter_event_loop(NULL);

    slot->initialized = true;
    atomic_store(&slot->usage_count, 0);
    atomic_store(&slot->marked_stale, false);
    slot->generation = atomic_load(&g_import_generation);

    /* Swap back to main thread state */
    PyThreadState_Swap(main_tstate);

#ifdef DEBUG
    fprintf(stderr, "subinterp_pool_reinit_slot: reinitialized slot %d\n", slot_idx);
#endif

    return 0;
}

void subinterp_pool_release(int slot) {
    if (slot < 0 || slot >= g_pool_size) {
        return;
    }

    subinterp_slot_t *s = &g_subinterp_pool[slot];
    if (!s->initialized) {
        return;
    }

    int prev_count = atomic_fetch_sub(&s->usage_count, 1);

    /* If usage drops to 0 and slot is marked stale, destroy and reinitialize
     * the subinterpreter. We need the GIL to safely do Python operations. */
    if (prev_count == 1 && atomic_load(&s->marked_stale)) {
        /* Acquire the GIL */
        PyGILState_STATE gstate = PyGILState_Ensure();

        /* Save current thread state and swap to the subinterpreter */
        PyThreadState *saved = PyThreadState_Swap(s->tstate);

        /* Clean up Python objects */
        Py_XDECREF(s->module_cache);
        Py_XDECREF(s->globals);
        Py_XDECREF(s->locals);

        /* End the interpreter - this frees s->tstate */
        Py_EndInterpreter(s->tstate);

        /* Clear slot state */
        s->interp = NULL;
        s->tstate = NULL;
        s->globals = NULL;
        s->locals = NULL;
        s->module_cache = NULL;
        s->initialized = false;
        atomic_store(&s->marked_stale, false);

        /* Swap back to saved thread state (may be NULL if we didn't have one) */
        PyThreadState_Swap(saved);

        /* Reinitialize the slot with a fresh subinterpreter */
        if (subinterp_pool_reinit_slot(slot) < 0) {
            /* Reinit failed - clear allocation bit so slot is skipped */
            uint64_t mask = ~(1ULL << slot);
            atomic_fetch_and(&g_pool_allocation, mask);
            fprintf(stderr, "subinterp_pool_release: failed to reinit slot %d\n", slot);
        }
        /* If reinit succeeded, slot stays allocated but now has fresh interp */

        /* Release the GIL */
        PyGILState_Release(gstate);

#ifdef DEBUG
        fprintf(stderr, "subinterp_pool_release: recycled stale slot %d\n", slot);
#endif
    }
}

uint64_t subinterp_pool_flush_generation(void) {
    if (!atomic_load(&g_pool_initialized)) {
        return 0;
    }

    /* Increment generation */
    uint64_t new_gen = atomic_fetch_add(&g_import_generation, 1) + 1;

    /* Mark all initialized slots as stale */
    for (int i = 0; i < g_pool_size; i++) {
        subinterp_slot_t *slot = &g_subinterp_pool[i];
        if (slot->initialized) {
            atomic_store(&slot->marked_stale, true);
        }
    }

#ifdef DEBUG
    fprintf(stderr, "subinterp_pool_flush_generation: incremented to %llu, marked all slots stale\n",
            (unsigned long long)new_gen);
#endif

    return new_gen;
}

uint64_t subinterp_pool_get_generation(void) {
    return atomic_load(&g_import_generation);
}

#else /* !HAVE_SUBINTERPRETERS */

/* For systems without subinterpreter support, we still need the generation
 * counter for main interpreter context cache invalidation */
static _Atomic uint64_t g_import_generation_no_subinterp = 0;

#endif /* HAVE_SUBINTERPRETERS */

/* ============================================================================
 * Unconditional generation functions (work with or without subinterpreters)
 * ============================================================================ */

uint64_t import_cache_get_generation(void) {
#ifdef HAVE_SUBINTERPRETERS
    return atomic_load(&g_import_generation);
#else
    return atomic_load(&g_import_generation_no_subinterp);
#endif
}

uint64_t import_cache_flush_generation(void) {
#ifdef HAVE_SUBINTERPRETERS
    return subinterp_pool_flush_generation();
#else
    return atomic_fetch_add(&g_import_generation_no_subinterp, 1) + 1;
#endif
}
