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
 * @file py_sandbox.c
 * @brief Python worker sandboxing via audit hooks
 *
 * This module implements sandboxing for Python workers using Python's
 * audit hook mechanism (PEP 578). It allows per-worker configuration of
 * blocked operations such as:
 * - File writes
 * - File reads
 * - Subprocess creation
 * - Network operations
 * - ctypes usage
 * - Dynamic code execution (exec/eval/compile)
 * - Non-whitelisted imports
 *
 * The implementation uses a global C audit hook that checks thread-local
 * worker state for policy enforcement.
 */

#include "py_nif.h"
#include "py_sandbox.h"

/* ============================================================================
 * Block Flag Definitions
 * ============================================================================ */

/**
 * @brief Block flags for sandbox policy
 */
#define SANDBOX_BLOCK_FILE_WRITE    (1 << 0)
#define SANDBOX_BLOCK_FILE_READ     (1 << 1)
#define SANDBOX_BLOCK_SUBPROCESS    (1 << 2)
#define SANDBOX_BLOCK_NETWORK       (1 << 3)
#define SANDBOX_BLOCK_CTYPES        (1 << 4)
#define SANDBOX_BLOCK_IMPORT        (1 << 5)
#define SANDBOX_BLOCK_EXEC          (1 << 6)

/**
 * @brief Strict preset: blocks subprocess, network, ctypes, file_write
 */
#define SANDBOX_PRESET_STRICT (SANDBOX_BLOCK_SUBPROCESS | SANDBOX_BLOCK_NETWORK | \
                               SANDBOX_BLOCK_CTYPES | SANDBOX_BLOCK_FILE_WRITE)

/* ============================================================================
 * Global State
 * ============================================================================ */

/** @brief Flag indicating sandbox system is initialized */
static bool g_sandbox_initialized = false;

/** @brief Mutex for sandbox system initialization */
static pthread_mutex_t g_sandbox_init_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================================
 * Audit Event Classification
 * ============================================================================ */

/**
 * @enum sandbox_event_category_t
 * @brief Categories of audit events
 */
typedef enum {
    SANDBOX_EVENT_NONE = 0,
    SANDBOX_EVENT_FILE_WRITE,
    SANDBOX_EVENT_FILE_READ,
    SANDBOX_EVENT_SUBPROCESS,
    SANDBOX_EVENT_NETWORK,
    SANDBOX_EVENT_CTYPES,
    SANDBOX_EVENT_IMPORT,
    SANDBOX_EVENT_EXEC
} sandbox_event_category_t;

/**
 * @brief Classify an audit event into a sandbox category
 *
 * @param event The audit event name
 * @param args The audit event arguments
 * @return The event category, or SANDBOX_EVENT_NONE if not relevant
 */
static sandbox_event_category_t classify_event(const char *event, PyObject *args) {
    /* File operations */
    if (strcmp(event, "open") == 0) {
        /* Check if write mode */
        if (args != NULL && PyTuple_Check(args) && PyTuple_Size(args) >= 2) {
            PyObject *mode_obj = PyTuple_GetItem(args, 1);
            if (mode_obj != NULL && PyUnicode_Check(mode_obj)) {
                const char *mode = PyUnicode_AsUTF8(mode_obj);
                if (mode != NULL) {
                    /* Check for write/append modes */
                    if (strchr(mode, 'w') != NULL || strchr(mode, 'a') != NULL ||
                        strchr(mode, 'x') != NULL || strchr(mode, '+') != NULL) {
                        return SANDBOX_EVENT_FILE_WRITE;
                    }
                }
            }
        }
        return SANDBOX_EVENT_FILE_READ;
    }

    /* Subprocess events */
    if (strcmp(event, "subprocess.Popen") == 0 ||
        strcmp(event, "os.system") == 0 ||
        strcmp(event, "os.popen") == 0 ||
        strncmp(event, "os.exec", 7) == 0 ||
        strncmp(event, "os.spawn", 8) == 0 ||
        strcmp(event, "os.posix_spawn") == 0 ||
        strcmp(event, "os.fork") == 0) {
        return SANDBOX_EVENT_SUBPROCESS;
    }

    /* Network events */
    if (strncmp(event, "socket.", 7) == 0) {
        return SANDBOX_EVENT_NETWORK;
    }

    /* ctypes events */
    if (strcmp(event, "ctypes.dlopen") == 0 ||
        strcmp(event, "ctypes.dlsym") == 0 ||
        strcmp(event, "ctypes.addressof") == 0 ||
        strcmp(event, "ctypes.create_string_buffer") == 0 ||
        strcmp(event, "ctypes.create_unicode_buffer") == 0 ||
        strncmp(event, "ctypes.", 7) == 0) {
        return SANDBOX_EVENT_CTYPES;
    }

    /* Import events */
    if (strcmp(event, "import") == 0) {
        return SANDBOX_EVENT_IMPORT;
    }

    /* Code execution events */
    if (strcmp(event, "compile") == 0 ||
        strcmp(event, "exec") == 0 ||
        strcmp(event, "builtins.eval") == 0 ||
        strcmp(event, "builtins.exec") == 0 ||
        strcmp(event, "builtins.compile") == 0) {
        return SANDBOX_EVENT_EXEC;
    }

    return SANDBOX_EVENT_NONE;
}

/**
 * @brief Convert event category to block flag
 */
static uint32_t category_to_block_flag(sandbox_event_category_t category) {
    switch (category) {
        case SANDBOX_EVENT_FILE_WRITE:
            return SANDBOX_BLOCK_FILE_WRITE;
        case SANDBOX_EVENT_FILE_READ:
            return SANDBOX_BLOCK_FILE_READ;
        case SANDBOX_EVENT_SUBPROCESS:
            return SANDBOX_BLOCK_SUBPROCESS;
        case SANDBOX_EVENT_NETWORK:
            return SANDBOX_BLOCK_NETWORK;
        case SANDBOX_EVENT_CTYPES:
            return SANDBOX_BLOCK_CTYPES;
        case SANDBOX_EVENT_IMPORT:
            return SANDBOX_BLOCK_IMPORT;
        case SANDBOX_EVENT_EXEC:
            return SANDBOX_BLOCK_EXEC;
        default:
            return 0;
    }
}

/* ============================================================================
 * Import Whitelist Checking
 * ============================================================================ */

/**
 * @brief Check if a module is in the allowed imports whitelist
 *
 * @param policy The sandbox policy
 * @param module_name The module being imported
 * @return true if allowed, false if blocked
 */
static bool is_import_allowed(sandbox_policy_t *policy, const char *module_name) {
    if (policy->allowed_imports == NULL || policy->allowed_imports_count == 0) {
        /* No whitelist - block all imports when BLOCK_IMPORT is set */
        return false;
    }

    /* Check whitelist */
    for (size_t i = 0; i < policy->allowed_imports_count; i++) {
        const char *allowed = policy->allowed_imports[i];
        size_t allowed_len = strlen(allowed);

        /* Exact match */
        if (strcmp(module_name, allowed) == 0) {
            return true;
        }

        /* Check if it's a submodule of an allowed package */
        if (strncmp(module_name, allowed, allowed_len) == 0 &&
            module_name[allowed_len] == '.') {
            return true;
        }
    }

    return false;
}

/* ============================================================================
 * Policy Enforcement
 * ============================================================================ */

/**
 * @brief Check if an event should be blocked based on policy
 *
 * @param policy The sandbox policy (may be NULL for no sandboxing)
 * @param category The event category
 * @param event The raw event name
 * @param args The event arguments
 * @return true if blocked, false if allowed
 */
static bool should_block(sandbox_policy_t *policy, sandbox_event_category_t category,
                         const char *event, PyObject *args) {
    if (policy == NULL || !policy->enabled) {
        return false;
    }

    uint32_t block_flag = category_to_block_flag(category);
    if (block_flag == 0) {
        return false;
    }

    /* Check if this category is blocked */
    uint32_t block_flags = atomic_load(&policy->block_flags);
    if ((block_flags & block_flag) == 0) {
        return false;
    }

    /* Special handling for imports - check whitelist */
    if (category == SANDBOX_EVENT_IMPORT) {
        if (args != NULL && PyTuple_Check(args) && PyTuple_Size(args) >= 1) {
            PyObject *module_obj = PyTuple_GetItem(args, 0);
            if (module_obj != NULL && PyUnicode_Check(module_obj)) {
                const char *module_name = PyUnicode_AsUTF8(module_obj);
                if (module_name != NULL) {
                    pthread_mutex_lock(&policy->mutex);
                    bool allowed = is_import_allowed(policy, module_name);
                    pthread_mutex_unlock(&policy->mutex);
                    return !allowed;
                }
            }
        }
        /* If we can't determine module name, block it */
        return true;
    }

    return true;
}

/* ============================================================================
 * Audit Hook Implementation
 * ============================================================================ */

/**
 * @brief Global C audit hook for sandbox enforcement
 *
 * This hook is registered once at Python init and checks the thread-local
 * worker's sandbox policy for each audit event.
 *
 * @param event The audit event name
 * @param args The audit event arguments
 * @param userData User data (unused)
 * @return 0 to allow, -1 to abort (raises exception)
 */
static int py_audit_hook(const char *event, PyObject *args, void *userData) {
    (void)userData;

    /* Get current worker from thread-local storage */
    py_worker_t *worker = tl_current_worker;
    if (worker == NULL) {
        /* No worker context - allow */
        return 0;
    }

    sandbox_policy_t *policy = worker->sandbox;
    if (policy == NULL || !policy->enabled) {
        /* No sandbox or disabled - allow */
        return 0;
    }

    /* Classify the event */
    sandbox_event_category_t category = classify_event(event, args);
    if (category == SANDBOX_EVENT_NONE) {
        return 0;
    }

    /* Check if should block */
    if (should_block(policy, category, event, args)) {
        /* Log event if enabled */
        if (policy->log_events && worker->has_callback_handler) {
            /* TODO: Send audit event to Erlang handler */
        }

        /* Set PermissionError */
        const char *category_name = "";
        switch (category) {
            case SANDBOX_EVENT_FILE_WRITE:
                category_name = "file_write";
                break;
            case SANDBOX_EVENT_FILE_READ:
                category_name = "file_read";
                break;
            case SANDBOX_EVENT_SUBPROCESS:
                category_name = "subprocess";
                break;
            case SANDBOX_EVENT_NETWORK:
                category_name = "network";
                break;
            case SANDBOX_EVENT_CTYPES:
                category_name = "ctypes";
                break;
            case SANDBOX_EVENT_IMPORT:
                category_name = "import";
                break;
            case SANDBOX_EVENT_EXEC:
                category_name = "exec";
                break;
            default:
                category_name = "unknown";
                break;
        }

        char error_msg[256];
        snprintf(error_msg, sizeof(error_msg),
                 "Sandbox: %s operations are blocked (event: %s)", category_name, event);
        PyErr_SetString(PyExc_PermissionError, error_msg);
        return -1;
    }

    return 0;
}

/* ============================================================================
 * Sandbox System Initialization
 * ============================================================================ */

/**
 * @brief Initialize the sandbox system
 *
 * Registers the global C audit hook. Should be called once at Python init.
 *
 * @return 0 on success, -1 on failure
 */
int init_sandbox_system(void) {
    pthread_mutex_lock(&g_sandbox_init_mutex);

    if (g_sandbox_initialized) {
        pthread_mutex_unlock(&g_sandbox_init_mutex);
        return 0;
    }

    /* Register global audit hook */
    if (PySys_AddAuditHook(py_audit_hook, NULL) != 0) {
        pthread_mutex_unlock(&g_sandbox_init_mutex);
        return -1;
    }

    g_sandbox_initialized = true;
    pthread_mutex_unlock(&g_sandbox_init_mutex);
    return 0;
}

/**
 * @brief Reset sandbox system state
 *
 * Should be called before Py_Finalize() to ensure the audit hook
 * will be re-registered on the next Python init.
 */
void cleanup_sandbox_system(void) {
    pthread_mutex_lock(&g_sandbox_init_mutex);
    g_sandbox_initialized = false;
    pthread_mutex_unlock(&g_sandbox_init_mutex);
}

/* ============================================================================
 * Policy Management
 * ============================================================================ */

/**
 * @brief Create a new sandbox policy
 *
 * @return New policy (caller must free), or NULL on failure
 */
sandbox_policy_t *sandbox_policy_new(void) {
    sandbox_policy_t *policy = enif_alloc(sizeof(sandbox_policy_t));
    if (policy == NULL) {
        return NULL;
    }

    memset(policy, 0, sizeof(sandbox_policy_t));
    pthread_mutex_init(&policy->mutex, NULL);
    policy->enabled = false;
    atomic_init(&policy->block_flags, 0);
    policy->allowed_imports = NULL;
    policy->allowed_imports_count = 0;
    policy->log_events = false;
    policy->disable_builtins = false;

    return policy;
}

/**
 * @brief Destroy a sandbox policy
 *
 * @param policy Policy to destroy
 */
void sandbox_policy_destroy(sandbox_policy_t *policy) {
    if (policy == NULL) {
        return;
    }

    pthread_mutex_lock(&policy->mutex);

    /* Free import whitelist */
    if (policy->allowed_imports != NULL) {
        for (size_t i = 0; i < policy->allowed_imports_count; i++) {
            enif_free(policy->allowed_imports[i]);
        }
        enif_free(policy->allowed_imports);
    }

    pthread_mutex_unlock(&policy->mutex);
    pthread_mutex_destroy(&policy->mutex);
    enif_free(policy);
}

/**
 * @brief Parse sandbox options from Erlang map
 *
 * Expected map format:
 * #{
 *     preset => strict,              % Optional: use preset
 *     enabled => true,               % Optional: enable/disable
 *     block => [file_write, subprocess, network], % Optional: block list
 *     allow_imports => [json, math], % Optional: import whitelist
 *     log_events => true             % Optional: enable audit logging
 * }
 *
 * @param env NIF environment
 * @param opts Options map
 * @param policy Policy to populate
 * @return 0 on success, -1 on failure
 */
int parse_sandbox_options(ErlNifEnv *env, ERL_NIF_TERM opts, sandbox_policy_t *policy) {
    if (!enif_is_map(env, opts)) {
        return -1;
    }

    ERL_NIF_TERM key, value;

    /* Check for preset first */
    key = enif_make_atom(env, "preset");
    if (enif_get_map_value(env, opts, key, &value)) {
        char preset[32];
        if (enif_get_atom(env, value, preset, sizeof(preset), ERL_NIF_LATIN1)) {
            if (strcmp(preset, "strict") == 0) {
                policy->enabled = true;
                policy->block_flags = SANDBOX_PRESET_STRICT;
            }
        }
    }

    /* Check for explicit enabled flag */
    key = enif_make_atom(env, "enabled");
    if (enif_get_map_value(env, opts, key, &value)) {
        char enabled[16];
        if (enif_get_atom(env, value, enabled, sizeof(enabled), ERL_NIF_LATIN1)) {
            policy->enabled = (strcmp(enabled, "true") == 0);
        }
    }

    /* Check for block list
     * Note: An explicit block list is combined with any preset flags using OR.
     * This allows extending a preset with additional blocked operations. */
    key = enif_make_atom(env, "block");
    if (enif_get_map_value(env, opts, key, &value)) {
        unsigned int list_len;
        if (enif_get_list_length(env, value, &list_len)) {
            ERL_NIF_TERM head, tail = value;
            /* Don't reset block_flags - preserve preset flags and merge with block list */
            policy->enabled = true;  /* Enable if block list provided */

            while (enif_get_list_cell(env, tail, &head, &tail)) {
                char block_name[32];
                if (enif_get_atom(env, head, block_name, sizeof(block_name), ERL_NIF_LATIN1)) {
                    if (strcmp(block_name, "file_write") == 0) {
                        policy->block_flags |= SANDBOX_BLOCK_FILE_WRITE;
                    } else if (strcmp(block_name, "file_read") == 0) {
                        policy->block_flags |= SANDBOX_BLOCK_FILE_READ;
                    } else if (strcmp(block_name, "subprocess") == 0) {
                        policy->block_flags |= SANDBOX_BLOCK_SUBPROCESS;
                    } else if (strcmp(block_name, "network") == 0) {
                        policy->block_flags |= SANDBOX_BLOCK_NETWORK;
                    } else if (strcmp(block_name, "ctypes") == 0) {
                        policy->block_flags |= SANDBOX_BLOCK_CTYPES;
                    } else if (strcmp(block_name, "import") == 0) {
                        policy->block_flags |= SANDBOX_BLOCK_IMPORT;
                    } else if (strcmp(block_name, "exec") == 0) {
                        policy->block_flags |= SANDBOX_BLOCK_EXEC;
                    }
                }
            }
        }
    }

    /* Check for import whitelist */
    key = enif_make_atom(env, "allow_imports");
    if (enif_get_map_value(env, opts, key, &value)) {
        unsigned int list_len;
        if (enif_get_list_length(env, value, &list_len) && list_len > 0) {
            policy->allowed_imports = enif_alloc(sizeof(char *) * list_len);
            if (policy->allowed_imports == NULL) {
                return -1;
            }
            policy->allowed_imports_count = 0;

            ERL_NIF_TERM head, tail = value;
            while (enif_get_list_cell(env, tail, &head, &tail)) {
                ErlNifBinary bin;
                if (enif_inspect_binary(env, head, &bin)) {
                    char *import_name = enif_alloc(bin.size + 1);
                    if (import_name == NULL) {
                        /* Cleanup previously allocated imports on failure */
                        for (size_t i = 0; i < policy->allowed_imports_count; i++) {
                            enif_free(policy->allowed_imports[i]);
                        }
                        enif_free(policy->allowed_imports);
                        policy->allowed_imports = NULL;
                        policy->allowed_imports_count = 0;
                        return -1;
                    }
                    memcpy(import_name, bin.data, bin.size);
                    import_name[bin.size] = '\0';
                    policy->allowed_imports[policy->allowed_imports_count++] = import_name;
                } else {
                    /* Try as atom */
                    char atom_buf[256];
                    if (enif_get_atom(env, head, atom_buf, sizeof(atom_buf), ERL_NIF_LATIN1)) {
                        char *import_name = enif_alloc(strlen(atom_buf) + 1);
                        if (import_name == NULL) {
                            /* Cleanup previously allocated imports on failure */
                            for (size_t i = 0; i < policy->allowed_imports_count; i++) {
                                enif_free(policy->allowed_imports[i]);
                            }
                            enif_free(policy->allowed_imports);
                            policy->allowed_imports = NULL;
                            policy->allowed_imports_count = 0;
                            return -1;
                        }
                        strcpy(import_name, atom_buf);
                        policy->allowed_imports[policy->allowed_imports_count++] = import_name;
                    }
                }
            }
        }
    }

    /* Check for log_events flag */
    key = enif_make_atom(env, "log_events");
    if (enif_get_map_value(env, opts, key, &value)) {
        char log_events[16];
        if (enif_get_atom(env, value, log_events, sizeof(log_events), ERL_NIF_LATIN1)) {
            policy->log_events = (strcmp(log_events, "true") == 0);
        }
    }

    /* Check for disable_builtins flag */
    key = enif_make_atom(env, "disable_builtins");
    if (enif_get_map_value(env, opts, key, &value)) {
        char disable_builtins[16];
        if (enif_get_atom(env, value, disable_builtins, sizeof(disable_builtins), ERL_NIF_LATIN1)) {
            policy->disable_builtins = (strcmp(disable_builtins, "true") == 0);
        }
    }

    return 0;
}

/**
 * @brief Enable or disable a sandbox policy
 *
 * @param policy Policy to modify
 * @param enabled New enabled state
 */
void sandbox_policy_set_enabled(sandbox_policy_t *policy, bool enabled) {
    if (policy != NULL) {
        pthread_mutex_lock(&policy->mutex);
        policy->enabled = enabled;
        pthread_mutex_unlock(&policy->mutex);
    }
}

/**
 * @brief Update a sandbox policy with new options
 *
 * @param env NIF environment
 * @param policy Policy to update
 * @param opts New options map
 * @return 0 on success, -1 on failure
 */
int sandbox_policy_update(ErlNifEnv *env, sandbox_policy_t *policy, ERL_NIF_TERM opts) {
    if (policy == NULL) {
        return -1;
    }

    pthread_mutex_lock(&policy->mutex);

    /* Create temporary policy to parse into */
    sandbox_policy_t temp;
    memset(&temp, 0, sizeof(temp));
    temp.block_flags = policy->block_flags;
    temp.enabled = policy->enabled;
    temp.log_events = policy->log_events;

    if (parse_sandbox_options(env, opts, &temp) < 0) {
        /* Clean up any imports allocated in temp before returning */
        if (temp.allowed_imports != NULL) {
            for (size_t i = 0; i < temp.allowed_imports_count; i++) {
                enif_free(temp.allowed_imports[i]);
            }
            enif_free(temp.allowed_imports);
        }
        pthread_mutex_unlock(&policy->mutex);
        return -1;
    }

    /* Update atomic flags */
    atomic_store(&policy->block_flags, temp.block_flags);
    policy->enabled = temp.enabled;
    policy->log_events = temp.log_events;

    /* Update import whitelist - always sync from temp so omitting
     * allow_imports in an update clears any existing whitelist */
    if (policy->allowed_imports != NULL) {
        for (size_t i = 0; i < policy->allowed_imports_count; i++) {
            enif_free(policy->allowed_imports[i]);
        }
        enif_free(policy->allowed_imports);
    }
    policy->allowed_imports = temp.allowed_imports;
    policy->allowed_imports_count = temp.allowed_imports_count;

    pthread_mutex_unlock(&policy->mutex);
    return 0;
}

/**
 * @brief Get policy as Erlang map
 *
 * @param env NIF environment
 * @param policy Policy to convert
 * @return Erlang map term
 */
ERL_NIF_TERM sandbox_policy_to_term(ErlNifEnv *env, sandbox_policy_t *policy) {
    if (policy == NULL) {
        return enif_make_new_map(env);
    }

    pthread_mutex_lock(&policy->mutex);

    ERL_NIF_TERM map = enif_make_new_map(env);

    /* Add enabled */
    ERL_NIF_TERM key = enif_make_atom(env, "enabled");
    ERL_NIF_TERM value = policy->enabled ? enif_make_atom(env, "true") : enif_make_atom(env, "false");
    enif_make_map_put(env, map, key, value, &map);

    /* Add block flags as list */
    ERL_NIF_TERM block_list[7];
    int block_count = 0;
    uint32_t flags = atomic_load(&policy->block_flags);

    if (flags & SANDBOX_BLOCK_FILE_WRITE) {
        block_list[block_count++] = enif_make_atom(env, "file_write");
    }
    if (flags & SANDBOX_BLOCK_FILE_READ) {
        block_list[block_count++] = enif_make_atom(env, "file_read");
    }
    if (flags & SANDBOX_BLOCK_SUBPROCESS) {
        block_list[block_count++] = enif_make_atom(env, "subprocess");
    }
    if (flags & SANDBOX_BLOCK_NETWORK) {
        block_list[block_count++] = enif_make_atom(env, "network");
    }
    if (flags & SANDBOX_BLOCK_CTYPES) {
        block_list[block_count++] = enif_make_atom(env, "ctypes");
    }
    if (flags & SANDBOX_BLOCK_IMPORT) {
        block_list[block_count++] = enif_make_atom(env, "import");
    }
    if (flags & SANDBOX_BLOCK_EXEC) {
        block_list[block_count++] = enif_make_atom(env, "exec");
    }

    key = enif_make_atom(env, "block");
    value = enif_make_list_from_array(env, block_list, block_count);
    enif_make_map_put(env, map, key, value, &map);

    /* Add import whitelist */
    if (policy->allowed_imports != NULL && policy->allowed_imports_count > 0) {
        ERL_NIF_TERM *imports = enif_alloc(sizeof(ERL_NIF_TERM) * policy->allowed_imports_count);
        if (imports != NULL) {
            for (size_t i = 0; i < policy->allowed_imports_count; i++) {
                size_t len = strlen(policy->allowed_imports[i]);
                ERL_NIF_TERM bin;
                unsigned char *buf = enif_make_new_binary(env, len, &bin);
                memcpy(buf, policy->allowed_imports[i], len);
                imports[i] = bin;
            }
            key = enif_make_atom(env, "allow_imports");
            value = enif_make_list_from_array(env, imports, policy->allowed_imports_count);
            enif_make_map_put(env, map, key, value, &map);
            enif_free(imports);
        }
    }

    /* Add log_events */
    key = enif_make_atom(env, "log_events");
    value = policy->log_events ? enif_make_atom(env, "true") : enif_make_atom(env, "false");
    enif_make_map_put(env, map, key, value, &map);

    /* Add disable_builtins */
    key = enif_make_atom(env, "disable_builtins");
    value = policy->disable_builtins ? enif_make_atom(env, "true") : enif_make_atom(env, "false");
    enif_make_map_put(env, map, key, value, &map);

    pthread_mutex_unlock(&policy->mutex);
    return map;
}

/* ============================================================================
 * Builtin Restrictions
 * ============================================================================ */

/**
 * @brief List of dangerous builtins to remove
 *
 * NOTE: __import__ is intentionally NOT included here because removing it
 * breaks Python's import machinery globally (affects PyImport_ImportModule()
 * C API calls). Use the audit hook mechanism with block => [import] instead
 * if you need to control imports.
 */
static const char *DANGEROUS_BUILTINS[] = {
    "exec",
    "eval",
    "compile",
    "open",
    NULL
};

/* Number of dangerous builtins (excluding NULL terminator) */
#define NUM_DANGEROUS_BUILTINS 4

/* Saved original builtin function objects for restoration */
static PyObject *g_saved_builtins[NUM_DANGEROUS_BUILTINS] = {NULL};
static bool g_builtins_saved = false;
static pthread_mutex_t g_builtins_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief Get the builtins dict from globals
 */
static PyObject *get_builtins_dict(PyObject *globals) {
    if (globals == NULL) {
        return NULL;
    }

    /* Get the __builtins__ from globals */
    PyObject *builtins = PyDict_GetItemString(globals, "__builtins__");
    if (builtins == NULL) {
        return NULL;
    }

    /* __builtins__ can be either a module or a dict depending on context */
    if (PyModule_Check(builtins)) {
        return PyModule_GetDict(builtins);
    } else if (PyDict_Check(builtins)) {
        return builtins;
    }
    return NULL;
}

/**
 * @brief Apply builtin restrictions to a Python worker
 *
 * Removes dangerous builtins from the worker's builtins module.
 * The original builtin values are saved for later restoration.
 * Must be called with GIL held and worker's thread state active.
 *
 * @param globals The worker's globals dict
 * @return 0 on success, -1 on failure
 */
int sandbox_apply_builtin_restrictions(PyObject *globals) {
    PyObject *builtins_dict = get_builtins_dict(globals);
    if (builtins_dict == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_builtins_mutex);

    /* Save original builtins on first call (thread-safe) */
    if (!g_builtins_saved) {
        for (int i = 0; DANGEROUS_BUILTINS[i] != NULL; i++) {
            PyObject *obj = PyDict_GetItemString(builtins_dict, DANGEROUS_BUILTINS[i]);
            if (obj != NULL) {
                Py_INCREF(obj);
                g_saved_builtins[i] = obj;
            }
        }
        g_builtins_saved = true;
    }

    pthread_mutex_unlock(&g_builtins_mutex);

    /* Remove each dangerous builtin */
    for (int i = 0; DANGEROUS_BUILTINS[i] != NULL; i++) {
        /* Use PyDict_DelItemString - it's OK if key doesn't exist */
        if (PyDict_GetItemString(builtins_dict, DANGEROUS_BUILTINS[i]) != NULL) {
            PyDict_DelItemString(builtins_dict, DANGEROUS_BUILTINS[i]);
        }
        PyErr_Clear(); /* Clear any KeyError */
    }

    return 0;
}

/**
 * @brief Restore dangerous builtins that were removed
 *
 * Called when a worker with disable_builtins is destroyed, to restore
 * the global Python state for other workers.
 * Must be called with GIL held.
 *
 * @param globals The worker's globals dict (or any valid globals with __builtins__)
 * @return 0 on success, -1 on failure
 */
int sandbox_restore_builtins(PyObject *globals) {
    PyObject *builtins_dict = get_builtins_dict(globals);
    if (builtins_dict == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_builtins_mutex);

    if (!g_builtins_saved) {
        pthread_mutex_unlock(&g_builtins_mutex);
        return 0; /* Nothing to restore */
    }

    /* Restore each saved builtin */
    for (int i = 0; DANGEROUS_BUILTINS[i] != NULL; i++) {
        if (g_saved_builtins[i] != NULL) {
            /* Only restore if not already present */
            if (PyDict_GetItemString(builtins_dict, DANGEROUS_BUILTINS[i]) == NULL) {
                PyDict_SetItemString(builtins_dict, DANGEROUS_BUILTINS[i], g_saved_builtins[i]);
            }
            PyErr_Clear();
        }
    }

    pthread_mutex_unlock(&g_builtins_mutex);
    return 0;
}
