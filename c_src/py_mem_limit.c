/* Copyright 2026 Benoit Chesneau
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
 * @file py_mem_limit.c
 * @brief Optional per-interpreter memory caps via obmalloc arena accounting.
 *
 * Why arenas: PyObjectArenaAllocator's free hook receives the block size,
 * so allocations and frees can be attributed exactly without adding a size
 * header to every object. PyMem_SetAllocator's free hook does not, which is
 * why object-level accounting would require growing every allocation.
 *
 * Enforcement raises MemoryError asynchronously in the thread that crossed the
 * cap, the same mechanism py_nif:context_interrupt/1 uses. Returning NULL from
 * the arena allocator does NOT work: obmalloc treats a failed arena as a reason
 * to fall back to PyMem_RawMalloc (Objects/obmalloc.c, _PyObject_Malloc), so
 * the allocation would silently succeed off-arena and the cap would stop
 * counting instead of stopping the code.
 *
 * Because an async exception lands at the next bytecode boundary, usage can
 * overshoot the cap slightly before the code stops. The cap re-arms once usage
 * drops back below it.
 *
 * Scope and limits (documented in doc/features.md):
 * - Attribution is per *interpreter*. All worker-mode contexts share the main
 *   interpreter, so a per-context cap is only meaningful in owngil mode.
 * - Allocations larger than obmalloc's small-object threshold (512 bytes) go
 *   straight to malloc and are NOT counted: large bytes objects, numpy
 *   buffers, and anything using its own allocator.
 * - Granularity is one arena (1 MB on current CPython).
 *
 * The hooks are only installed when the application asks for them, so the
 * default build path is untouched.
 */

#ifdef HAVE_OWNGIL

/** @brief Maximum number of interpreters tracked at once */
#define PY_MEM_LIMIT_SLOTS 64

typedef struct {
    /** @brief Interpreter this slot accounts for (NULL = free slot) */
    PyInterpreterState *interp;

    /** @brief Arena bytes currently allocated to this interpreter */
    size_t used;

    /** @brief Cap in bytes (0 = unlimited, accounting only) */
    size_t limit;

    /** @brief True once the cap was hit and MemoryError was injected */
    bool tripped;
} py_mem_slot_t;

static py_mem_slot_t g_mem_slots[PY_MEM_LIMIT_SLOTS];
static pthread_mutex_t g_mem_slots_mutex = PTHREAD_MUTEX_INITIALIZER;
static PyObjectArenaAllocator g_base_arena;
static bool g_mem_limits_enabled = false;

/**
 * Find the slot for @p interp, optionally creating it.
 *
 * Caller must hold g_mem_slots_mutex. Never acquires the GIL, so the
 * allocator (which runs holding a GIL) cannot deadlock against registration.
 */
static py_mem_slot_t *mem_slot_find(PyInterpreterState *interp, bool create) {
    py_mem_slot_t *free_slot = NULL;

    for (int i = 0; i < PY_MEM_LIMIT_SLOTS; i++) {
        if (g_mem_slots[i].interp == interp) {
            return &g_mem_slots[i];
        }
        if (free_slot == NULL && g_mem_slots[i].interp == NULL) {
            free_slot = &g_mem_slots[i];
        }
    }

    if (!create || free_slot == NULL) {
        return NULL;
    }

    free_slot->interp = interp;
    free_slot->used = 0;
    free_slot->limit = 0;
    free_slot->tripped = false;
    return free_slot;
}

/** @brief Current interpreter, or NULL when no thread state is attached */
static PyInterpreterState *mem_current_interp(void) {
    PyThreadState *tstate = PyThreadState_GetUnchecked();
    return (tstate != NULL) ? PyThreadState_GetInterpreter(tstate) : NULL;
}

static void *py_mem_arena_alloc(void *ctx, size_t size) {
    PyInterpreterState *interp = mem_current_interp();
    py_mem_slot_t *slot = NULL;
    bool inject = false;

    if (interp != NULL) {
        pthread_mutex_lock(&g_mem_slots_mutex);
        slot = mem_slot_find(interp, true);
        if (slot != NULL) {
            slot->used += size;
            if (slot->limit != 0 && slot->used > slot->limit && !slot->tripped) {
                slot->tripped = true;
                inject = true;
            }
        }
        pthread_mutex_unlock(&g_mem_slots_mutex);
    }

    if (inject) {
        /* Raise MemoryError at the next bytecode boundary in this thread. We
         * hold this interpreter's GIL (obmalloc runs under it), and
         * SetAsyncExc neither allocates nor re-enters the allocator.
         * Done outside g_mem_slots_mutex to keep that lock leaf-level. */
        PyThreadState_SetAsyncExc(PyThread_get_thread_ident(), PyExc_MemoryError);
    }

    void *ptr = g_base_arena.alloc(ctx, size);

    if (ptr == NULL && slot != NULL) {
        /* Roll back the reservation on a genuine allocation failure */
        pthread_mutex_lock(&g_mem_slots_mutex);
        slot = mem_slot_find(interp, false);
        if (slot != NULL) {
            slot->used = (slot->used > size) ? slot->used - size : 0;
        }
        pthread_mutex_unlock(&g_mem_slots_mutex);
    }

    return ptr;
}

static void py_mem_arena_free(void *ctx, void *ptr, size_t size) {
    PyInterpreterState *interp = mem_current_interp();

    if (interp != NULL) {
        pthread_mutex_lock(&g_mem_slots_mutex);
        py_mem_slot_t *slot = mem_slot_find(interp, false);
        if (slot != NULL) {
            slot->used = (slot->used > size) ? slot->used - size : 0;
            if (slot->tripped && slot->limit != 0 && slot->used < slot->limit) {
                /* Back under the cap, re-arm enforcement */
                slot->tripped = false;
            }
        }
        pthread_mutex_unlock(&g_mem_slots_mutex);
    }

    g_base_arena.free(ctx, ptr, size);
}

/**
 * @brief Install the accounting arena allocator
 *
 * Must be called before Py_Initialize, since arenas are allocated during
 * interpreter startup and the base allocator is captured here.
 */
static void py_mem_limit_install(void) {
    if (g_mem_limits_enabled) {
        return;
    }

    PyObject_GetArenaAllocator(&g_base_arena);
    if (g_base_arena.alloc == NULL || g_base_arena.free == NULL) {
        return;
    }

    PyObjectArenaAllocator wrapper = g_base_arena;
    wrapper.alloc = py_mem_arena_alloc;
    wrapper.free = py_mem_arena_free;
    PyObject_SetArenaAllocator(&wrapper);

    g_mem_limits_enabled = true;
}

static bool py_mem_limit_enabled(void) {
    return g_mem_limits_enabled;
}

/**
 * @brief Set the cap for an interpreter
 * @param limit Bytes, or 0 to remove the cap (accounting continues)
 * @return 0 on success, -1 if no slot is available
 */
static int py_mem_limit_set(PyInterpreterState *interp, size_t limit) {
    if (!g_mem_limits_enabled || interp == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_mem_slots_mutex);
    py_mem_slot_t *slot = mem_slot_find(interp, true);
    if (slot != NULL) {
        slot->limit = limit;
        slot->tripped = false;
    }
    pthread_mutex_unlock(&g_mem_slots_mutex);

    return (slot != NULL) ? 0 : -1;
}

/**
 * @brief Read current usage and cap for an interpreter
 * @return 0 on success, -1 if the interpreter is not tracked
 */
static int py_mem_limit_get(PyInterpreterState *interp, size_t *used, size_t *limit) {
    if (!g_mem_limits_enabled || interp == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_mem_slots_mutex);
    py_mem_slot_t *slot = mem_slot_find(interp, false);
    if (slot != NULL) {
        *used = slot->used;
        *limit = slot->limit;
    }
    pthread_mutex_unlock(&g_mem_slots_mutex);

    return (slot != NULL) ? 0 : -1;
}

/**
 * @brief Release the slot for a destroyed interpreter
 *
 * Must be called after Py_EndInterpreter, so the arenas freed during teardown
 * are still accounted. Without this a recycled PyInterpreterState pointer
 * would inherit the dead interpreter's usage.
 */
static void py_mem_limit_forget(PyInterpreterState *interp) {
    if (!g_mem_limits_enabled || interp == NULL) {
        return;
    }

    pthread_mutex_lock(&g_mem_slots_mutex);
    py_mem_slot_t *slot = mem_slot_find(interp, false);
    if (slot != NULL) {
        slot->interp = NULL;
        slot->used = 0;
        slot->limit = 0;
        slot->tripped = false;
    }
    pthread_mutex_unlock(&g_mem_slots_mutex);
}

#else  /* !HAVE_OWNGIL */

/* Memory limits require OWN_GIL subinterpreters (Python 3.14+): without them
 * every context shares the main interpreter, so a per-context cap has no
 * meaning. These stubs keep the call sites free of #ifdef. */

static void py_mem_limit_install(void) {}
static bool py_mem_limit_enabled(void) { return false; }

#ifdef HAVE_SUBINTERPRETERS
static int py_mem_limit_set(PyInterpreterState *interp, size_t limit) {
    (void)interp; (void)limit;
    return -1;
}
static int py_mem_limit_get(PyInterpreterState *interp, size_t *used, size_t *limit) {
    (void)interp; (void)used; (void)limit;
    return -1;
}
static void py_mem_limit_forget(PyInterpreterState *interp) { (void)interp; }
#endif

#endif /* HAVE_OWNGIL */
