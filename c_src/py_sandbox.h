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
 * @file py_sandbox.h
 * @brief Python worker sandboxing declarations
 */

#ifndef PY_SANDBOX_H
#define PY_SANDBOX_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <erl_nif.h>

/* ============================================================================
 * Block Flag Constants
 * ============================================================================ */

/**
 * @defgroup sandbox_flags Sandbox Block Flags
 * @brief Flags for configuring what operations are blocked
 * @{
 */

/** @brief Block file write operations (open with w/a/x/+ modes) */
#define SANDBOX_BLOCK_FILE_WRITE    (1 << 0)

/** @brief Block all file operations */
#define SANDBOX_BLOCK_FILE_READ     (1 << 1)

/** @brief Block subprocess creation (subprocess.*, os.exec*, os.spawn*, os.popen) */
#define SANDBOX_BLOCK_SUBPROCESS    (1 << 2)

/** @brief Block network operations (socket.*) */
#define SANDBOX_BLOCK_NETWORK       (1 << 3)

/** @brief Block ctypes usage (direct memory access) */
#define SANDBOX_BLOCK_CTYPES        (1 << 4)

/** @brief Block non-whitelisted imports */
#define SANDBOX_BLOCK_IMPORT        (1 << 5)

/** @brief Block dynamic code execution (compile, exec, eval) */
#define SANDBOX_BLOCK_EXEC          (1 << 6)

/** @} */

/* ============================================================================
 * Preset Definitions
 * ============================================================================ */

/**
 * @defgroup sandbox_presets Sandbox Presets
 * @brief Pre-defined combinations of block flags
 * @{
 */

/**
 * @brief Strict preset: blocks subprocess, network, ctypes, file_write
 *
 * Use this for untrusted code that should not be able to:
 * - Spawn processes or execute shell commands
 * - Make network connections
 * - Use ctypes for memory manipulation
 * - Write to the filesystem
 */
#define SANDBOX_PRESET_STRICT (SANDBOX_BLOCK_SUBPROCESS | SANDBOX_BLOCK_NETWORK | \
                               SANDBOX_BLOCK_CTYPES | SANDBOX_BLOCK_FILE_WRITE)

/** @} */

/* ============================================================================
 * Type Definitions
 * ============================================================================ */

/**
 * @struct sandbox_policy_t
 * @brief Per-worker sandbox policy configuration
 *
 * This structure holds the sandbox configuration for a single Python worker.
 * The block_flags field uses atomic operations for fast path checking.
 */
struct sandbox_policy_t {
    /** @brief Whether sandbox is enabled for this worker */
    bool enabled;

    /**
     * @brief Bitmask of blocked operation categories
     *
     * Uses atomic operations for lock-free reads in the fast path.
     * Combination of SANDBOX_BLOCK_* flags.
     */
    _Atomic uint32_t block_flags;

    /**
     * @brief Array of allowed import module names
     *
     * When SANDBOX_BLOCK_IMPORT is set, only modules in this list
     * (and their submodules) can be imported.
     */
    char **allowed_imports;

    /** @brief Number of entries in allowed_imports */
    size_t allowed_imports_count;

    /**
     * @brief Whether to log audit events (reserved for future use)
     *
     * When true, blocked events will be logged. Event forwarding to Erlang
     * is planned for a future version.
     */
    bool log_events;

    /**
     * @brief Whether to disable dangerous builtins
     *
     * When true, dangerous builtins (exec, eval, compile, __import__, open)
     * are removed from the builtins module after worker initialization.
     * This provides defense-in-depth beyond audit hook blocking.
     */
    bool disable_builtins;

    /** @brief Mutex for protecting policy updates */
    pthread_mutex_t mutex;
};

/* ============================================================================
 * Function Declarations
 * ============================================================================ */

/**
 * @defgroup sandbox_api Sandbox API
 * @brief Functions for sandbox management
 * @{
 */

/**
 * @brief Initialize the sandbox system
 *
 * Registers the global C audit hook. Must be called once at Python init,
 * before any workers are created.
 *
 * @return 0 on success, -1 on failure
 */
int init_sandbox_system(void);

/**
 * @brief Create a new sandbox policy
 *
 * Creates a new policy with default values (disabled, no blocks).
 *
 * @return New policy (caller owns), or NULL on allocation failure
 */
sandbox_policy_t *sandbox_policy_new(void);

/**
 * @brief Destroy a sandbox policy
 *
 * Frees all resources associated with the policy.
 *
 * @param policy Policy to destroy (may be NULL)
 */
void sandbox_policy_destroy(sandbox_policy_t *policy);

/**
 * @brief Parse sandbox options from Erlang map
 *
 * Parses an Erlang map with sandbox configuration and populates
 * the policy structure.
 *
 * Expected map format:
 * @code{.erlang}
 * #{
 *     preset => strict,              % Optional: use preset
 *     enabled => true,               % Optional: enable/disable
 *     block => [file_write, subprocess, network], % Optional: block list
 *     allow_imports => [<<"json">>, <<"math">>], % Optional: import whitelist
 *     log_events => true             % Optional: enable audit logging
 * }
 * @endcode
 *
 * @param env NIF environment
 * @param opts Options map
 * @param policy Policy to populate
 * @return 0 on success, -1 on failure
 */
int parse_sandbox_options(ErlNifEnv *env, ERL_NIF_TERM opts, sandbox_policy_t *policy);

/**
 * @brief Enable or disable a sandbox policy
 *
 * @param policy Policy to modify
 * @param enabled New enabled state
 */
void sandbox_policy_set_enabled(sandbox_policy_t *policy, bool enabled);

/**
 * @brief Update a sandbox policy with new options
 *
 * Atomically updates the policy with new configuration.
 *
 * @param env NIF environment
 * @param policy Policy to update
 * @param opts New options map
 * @return 0 on success, -1 on failure
 */
int sandbox_policy_update(ErlNifEnv *env, sandbox_policy_t *policy, ERL_NIF_TERM opts);

/**
 * @brief Get policy as Erlang map
 *
 * Converts the policy configuration to an Erlang map for inspection.
 *
 * @param env NIF environment
 * @param policy Policy to convert
 * @return Erlang map term
 */
ERL_NIF_TERM sandbox_policy_to_term(ErlNifEnv *env, sandbox_policy_t *policy);

/**
 * @brief Apply builtin restrictions to a Python worker
 *
 * Removes dangerous builtins (exec, eval, compile, __import__, open)
 * from the worker's builtins module. Must be called with GIL held
 * and worker's thread state active.
 *
 * @param globals The worker's globals dict
 * @return 0 on success, -1 on failure
 */
int sandbox_apply_builtin_restrictions(PyObject *globals);

/** @} */

#endif /* PY_SANDBOX_H */
