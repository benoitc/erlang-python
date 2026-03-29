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
 * py_util.h - Common utility macros and inline functions
 *
 * This header provides utility macros and functions used across multiple
 * NIF modules to reduce code duplication and standardize error handling.
 */

#ifndef PY_UTIL_H
#define PY_UTIL_H

#include <unistd.h>
#include <stdatomic.h>
#include "erl_nif.h"

/* ============================================================================
 * Pipe Helpers
 * ============================================================================ */

/**
 * @brief Close both ends of a pipe pair safely
 *
 * Closes pipe file descriptors if they are open (>= 0) and sets them to -1.
 *
 * @param pipes Array of 2 file descriptors [read_fd, write_fd]
 */
static inline void close_pipe_pair(int pipes[2]) {
    if (pipes[0] >= 0) {
        close(pipes[0]);
        pipes[0] = -1;
    }
    if (pipes[1] >= 0) {
        close(pipes[1]);
        pipes[1] = -1;
    }
}

/* ============================================================================
 * NIF Error Handling Macros
 * ============================================================================ */

/**
 * @brief Check runtime is running, return error tuple if not
 *
 * Usage:
 *   RUNTIME_CHECK_ERROR(env);
 */
#define RUNTIME_CHECK_ERROR(env) \
    do { \
        if (!runtime_is_running()) { \
            return enif_make_tuple2(env, ATOM_ERROR, \
                                    enif_make_atom(env, "not_initialized")); \
        } \
    } while (0)

/**
 * @brief Get resource from term or return badarg
 *
 * Usage:
 *   GET_RESOURCE_OR_BADARG(env, argv[0], MY_RESOURCE_TYPE, &ptr);
 */
#define GET_RESOURCE_OR_BADARG(env, term, type, out_ptr) \
    do { \
        if (!enif_get_resource(env, term, type, (void **)(out_ptr))) { \
            return enif_make_badarg(env); \
        } \
    } while (0)

/**
 * @brief Check if object is destroyed (atomic flag), return badarg if so
 *
 * Usage:
 *   CHECK_NOT_DESTROYED(env, obj);
 */
#define CHECK_NOT_DESTROYED(env, obj) \
    do { \
        if (atomic_load(&(obj)->destroyed)) { \
            return enif_make_badarg(env); \
        } \
    } while (0)

/**
 * @brief Inspect binary from term or return badarg
 *
 * Usage:
 *   INSPECT_BINARY_OR_BADARG(env, argv[1], key_bin);
 */
#define INSPECT_BINARY_OR_BADARG(env, term, bin) \
    do { \
        if (!enif_inspect_binary(env, term, &(bin))) { \
            return enif_make_badarg(env); \
        } \
    } while (0)

/* ============================================================================
 * Error Tuple Helpers
 * ============================================================================ */

/**
 * @brief Create an {error, reason_atom} tuple
 *
 * @param env NIF environment
 * @param reason Error reason as string (will be converted to atom)
 * @return ERL_NIF_TERM {error, reason}
 */
static inline ERL_NIF_TERM make_error_atom(ErlNifEnv *env, const char *reason) {
    return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, reason));
}

#endif /* PY_UTIL_H */
