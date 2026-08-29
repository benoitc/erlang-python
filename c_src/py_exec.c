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
 * @file py_exec.c
 * @brief Python execution engine and GIL management
 * @author Benoit Chesneau
 *
 * @ingroup exec
 *
 * This module implements the core Python execution engine, handling:
 *
 * - **Timeout support**: Trace-based execution timeout monitoring
 * - **Single coordinator executor thread**: serializes legacy worker API and
 *   coordinator tasks behind one GIL-holding thread.
 * - **Request processing**: Dispatch for call/eval/exec/import operations
 * - **Free-threaded mode**: Support for Python 3.13+ no-GIL builds
 *
 * @par Architecture
 *
 * The execution model uses a request/response pattern:
 *
 * ```
 * ┌─────────────┐    enqueue     ┌──────────────┐    process    ┌────────────┐
 * │  NIF Call   │ ────────────>  │   Executor   │ ────────────> │   Python   │
 * │  (Erlang)   │                │    Thread    │               │    Code    │
 * └─────────────┘                └──────────────┘               └────────────┘
 *       │                              │                              │
 *       │         wait(cond)           │                              │
 *       │<─────────────────────────────│<─────────────────────────────│
 *       │                          completed                       result
 * ```
 *
 * Per-context worker threads (see py_nif.c) handle the public worker / owngil
 * APIs directly; the single executor here only backs the legacy worker pool
 * and a few coordinator paths.
 *
 * @par GIL Management Patterns
 *
 * Following PyO3/Granian best practices:
 *
 * - **Py_BEGIN_ALLOW_THREADS**: Release GIL during blocking waits
 * - **Py_END_ALLOW_THREADS**: Re-acquire GIL before Python calls
 * - **PyGILState_Ensure/Release**: For callbacks from non-Python threads
 *
 * @par Thread Safety
 *
 * - Executor queues protected by pthread mutexes
 * - Request completion signaled via condition variables
 * - Thread-local storage for timeout and callback state
 *
 * @note This file is included from py_nif.c (single compilation unit)
 */

/* ============================================================================
 * Execution mode detection
 * ============================================================================ */

static void detect_execution_mode(void) {
#ifdef HAVE_FREE_THREADED
    g_execution_mode = PY_MODE_FREE_THREADED;
#else
    g_execution_mode = PY_MODE_GIL;
#endif
}

