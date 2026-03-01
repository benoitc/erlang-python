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
 * @file py_event_loop.h
 * @brief Erlang-native asyncio event loop using enif_select
 * @author Benoit Chesneau
 *
 * This module provides an asyncio event loop implementation backed by
 * Erlang's scheduler using enif_select for I/O multiplexing. This replaces
 * the polling-based approach with true event-driven callbacks.
 *
 * Architecture:
 * - Python asyncio code calls add_reader/add_writer/call_later
 * - These register with enif_select or erlang:send_after
 * - Erlang sends messages when events occur
 * - Python callbacks are dispatched with GIL released during waits
 */

#ifndef PY_EVENT_LOOP_H
#define PY_EVENT_LOOP_H

#include <erl_nif.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

/** @brief Maximum pending events before processing */
#define MAX_PENDING_EVENTS 256

/** @brief Maximum events to keep in freelist (Phase 7 optimization) */
#define EVENT_FREELIST_SIZE 256

/** @brief Size of pending event hash set for O(1) duplicate detection */
#define PENDING_HASH_SIZE 128

/** @brief Event types for pending callbacks */
typedef enum {
    EVENT_TYPE_READ = 1,
    EVENT_TYPE_WRITE = 2,
    EVENT_TYPE_TIMER = 3
} event_type_t;

/* ============================================================================
 * File Descriptor Resource
 * ============================================================================ */

/* Closing state values for fd lifecycle management */
#define FD_STATE_OPEN    0
#define FD_STATE_CLOSING 1
#define FD_STATE_CLOSED  2

/**
 * @struct fd_resource_t
 * @brief Resource for tracking file descriptors registered with enif_select
 *
 * Each file descriptor monitored for I/O readiness has an associated
 * fd_resource_t that tracks the callback to invoke when the fd is ready.
 */
typedef struct {
    /** @brief The file descriptor being monitored */
    int fd;

    /** @brief Callback ID for the read handler */
    uint64_t read_callback_id;

    /** @brief Callback ID for the write handler */
    uint64_t write_callback_id;

    /** @brief PID of the owning event loop's router */
    ErlNifPid owner_pid;

    /** @brief Whether read monitoring is active */
    bool reader_active;

    /** @brief Whether write monitoring is active */
    bool writer_active;

    /** @brief Reference to the parent event loop */
    struct erlang_event_loop *loop;

    /* Lifecycle management fields */

    /** @brief Closing state: FD_STATE_OPEN, FD_STATE_CLOSING, or FD_STATE_CLOSED */
    _Atomic int closing_state;

    /** @brief Monitor for owner process */
    ErlNifMonitor owner_monitor;

    /** @brief Whether owner_monitor is valid/active */
    bool monitor_active;

    /** @brief Whether to close FD on stop (ownership flag) */
    bool owns_fd;
} fd_resource_t;

/* ============================================================================
 * Pending Event
 * ============================================================================ */

/**
 * @struct pending_event_t
 * @brief Represents a pending event ready to be dispatched to Python
 */
typedef struct pending_event {
    /** @brief Type of event (read, write, timer) */
    event_type_t type;

    /** @brief Callback ID to dispatch */
    uint64_t callback_id;

    /** @brief File descriptor (for read/write events) */
    int fd;

    /** @brief Next pending event in the list */
    struct pending_event *next;
} pending_event_t;

/* ============================================================================
 * Timer Resource
 * ============================================================================ */

/**
 * @struct timer_resource_t
 * @brief Resource for tracking timers scheduled via erlang:send_after
 */
typedef struct {
    /** @brief Unique timer reference from Erlang */
    ERL_NIF_TERM timer_ref;

    /** @brief Callback ID to invoke when timer fires */
    uint64_t callback_id;

    /** @brief Whether this timer is still active */
    bool active;

    /** @brief Reference to the parent event loop */
    struct erlang_event_loop *loop;
} timer_resource_t;

/* ============================================================================
 * Event Loop State
 * ============================================================================ */

/**
 * @struct erlang_event_loop_t
 * @brief Main state for the Erlang-backed asyncio event loop
 *
 * This structure maintains all state needed for the event loop:
 * - Reference to the Erlang worker process (scalable I/O model)
 * - Reference to the Erlang router process (legacy)
 * - Pending events queue
 * - Synchronization primitives
 */
typedef struct erlang_event_loop {
    /** @brief PID of the py_event_router gen_server (legacy) */
    ErlNifPid router_pid;

    /** @brief Whether router_pid has been set */
    bool has_router;

    /** @brief PID of the py_event_worker gen_server (scalable I/O model) */
    ErlNifPid worker_pid;

    /** @brief Whether worker_pid has been set */
    bool has_worker;

    /** @brief Loop identifier for routing */
    char loop_id[64];

    /** @brief Mutex protecting the event loop state */
    pthread_mutex_t mutex;

    /** @brief Condition variable for event notification */
    pthread_cond_t event_cond;

    /** @brief Counter for generating unique callback IDs */
    _Atomic uint64_t next_callback_id;

    /** @brief Head of pending events queue */
    pending_event_t *pending_head;

    /** @brief Tail of pending events queue */
    pending_event_t *pending_tail;

    /** @brief Number of pending events */
    _Atomic int pending_count;

    /** @brief Flag indicating shutdown requested */
    volatile bool shutdown;

    /** @brief Environment for building messages to router */
    ErlNifEnv *msg_env;

    /** @brief Self PID for receiving messages */
    ErlNifPid self_pid;

    /** @brief Whether self_pid has been set */
    bool has_self;

    /* ========== Phase 7 Optimization: Pending Event Freelist ========== */

    /** @brief Head of freelist for recycling pending_event_t structures */
    pending_event_t *event_freelist;

    /** @brief Number of events currently in freelist */
    int freelist_count;

    /* ========== O(1) Duplicate Detection Hash Set ========== */

    /**
     * @brief Hash set for O(1) duplicate pending event detection
     *
     * Key: (callback_id, type) combined into a single uint64_t
     * Uses open addressing with linear probing.
     */
    uint64_t pending_hash_keys[PENDING_HASH_SIZE];

    /** @brief Occupancy flags for hash set slots */
    bool pending_hash_occupied[PENDING_HASH_SIZE];

    /** @brief Count of occupied slots in hash set */
    int pending_hash_count;

    /* ========== Synchronous Sleep Support ========== */

    /** @brief Current synchronous sleep ID being waited on */
    _Atomic uint64_t sync_sleep_id;

    /** @brief Flag indicating sleep has completed */
    _Atomic bool sync_sleep_complete;

    /** @brief Condition variable for sleep completion notification */
    pthread_cond_t sync_sleep_cond;

    /** @brief Whether sync_sleep_cond has been initialized */
    bool sync_sleep_cond_initialized;
} erlang_event_loop_t;

/* ============================================================================
 * Resource Type Declarations
 * ============================================================================ */

/** @brief Resource type for erlang_event_loop_t */
extern ErlNifResourceType *EVENT_LOOP_RESOURCE_TYPE;

/** @brief Resource type for fd_resource_t */
extern ErlNifResourceType *FD_RESOURCE_TYPE;

/** @brief Resource type for timer_resource_t */
extern ErlNifResourceType *TIMER_RESOURCE_TYPE;

/* ============================================================================
 * Atom Declarations
 * ============================================================================ */

extern ERL_NIF_TERM ATOM_SELECT;
extern ERL_NIF_TERM ATOM_READY_INPUT;
extern ERL_NIF_TERM ATOM_READY_OUTPUT;
extern ERL_NIF_TERM ATOM_READ;
extern ERL_NIF_TERM ATOM_WRITE;
extern ERL_NIF_TERM ATOM_TIMER;
extern ERL_NIF_TERM ATOM_START_TIMER;
extern ERL_NIF_TERM ATOM_CANCEL_TIMER;
extern ERL_NIF_TERM ATOM_EVENT_LOOP;
extern ERL_NIF_TERM ATOM_DISPATCH;

/* ============================================================================
 * Initialization Functions
 * ============================================================================ */

/**
 * @brief Initialize event loop module
 *
 * Creates resource types and atoms. Called from NIF load.
 *
 * @param env NIF environment
 * @return 0 on success, -1 on failure
 */
int event_loop_init(ErlNifEnv *env);

/**
 * @brief Clean up event loop module
 *
 * Called from NIF unload.
 */
void event_loop_cleanup(void);

/* ============================================================================
 * Event Loop NIF Functions
 * ============================================================================ */

/**
 * @brief Create a new event loop resource
 *
 * NIF: event_loop_new() -> {ok, LoopRef} | {error, Reason}
 */
ERL_NIF_TERM nif_event_loop_new(ErlNifEnv *env, int argc,
                                 const ERL_NIF_TERM argv[]);

/**
 * @brief Destroy an event loop resource
 *
 * NIF: event_loop_destroy(LoopRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_event_loop_destroy(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]);

/**
 * @brief Set the router PID for the event loop (legacy)
 *
 * NIF: event_loop_set_router(LoopRef, RouterPid) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_event_loop_set_router(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]);

/**
 * @brief Set the worker PID for the event loop (scalable I/O model)
 *
 * NIF: event_loop_set_worker(LoopRef, WorkerPid) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_event_loop_set_worker(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]);

/**
 * @brief Set the loop identifier
 *
 * NIF: event_loop_set_id(LoopRef, LoopId) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_event_loop_set_id(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]);

/**
 * @brief Register a file descriptor for read monitoring
 *
 * Uses enif_select to register the fd with the Erlang scheduler.
 *
 * NIF: add_reader(LoopRef, Fd, CallbackId) -> {ok, FdRef} | {error, Reason}
 */
ERL_NIF_TERM nif_add_reader(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]);

/**
 * @brief Stop monitoring a file descriptor for reads
 *
 * NIF: remove_reader(LoopRef, FdRef) -> ok | {error, Reason}
 * FdRef must be the same resource returned by add_reader.
 */
ERL_NIF_TERM nif_remove_reader(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]);

/**
 * @brief Register a file descriptor for write monitoring
 *
 * NIF: add_writer(LoopRef, Fd, CallbackId) -> {ok, FdRef} | {error, Reason}
 */
ERL_NIF_TERM nif_add_writer(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]);

/**
 * @brief Stop monitoring a file descriptor for writes
 *
 * NIF: remove_writer(LoopRef, FdRef) -> ok | {error, Reason}
 * FdRef must be the same resource returned by add_writer.
 */
ERL_NIF_TERM nif_remove_writer(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]);

/**
 * @brief Schedule a timer callback
 *
 * Sends a message to the router to create an erlang:send_after timer.
 *
 * NIF: call_later(LoopRef, DelayMs, CallbackId) -> {ok, TimerRef} | {error, Reason}
 */
ERL_NIF_TERM nif_call_later(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]);

/**
 * @brief Cancel a pending timer
 *
 * NIF: cancel_timer(LoopRef, TimerRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_cancel_timer(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]);

/**
 * @brief Wait for events with timeout
 *
 * Blocks waiting for events from the Erlang scheduler.
 * IMPORTANT: Releases the GIL while waiting.
 *
 * NIF: poll_events(LoopRef, TimeoutMs) -> {ok, NumEvents} | {error, Reason}
 *
 * Marked as ERL_NIF_DIRTY_JOB_IO_BOUND.
 */
ERL_NIF_TERM nif_poll_events(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[]);

/**
 * @brief Get list of pending events
 *
 * Returns events that have been signaled and are ready for dispatch.
 *
 * NIF: get_pending(LoopRef) -> [{CallbackId, Type}]
 */
ERL_NIF_TERM nif_get_pending(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[]);

/**
 * @brief Dispatch a callback from the router
 *
 * Called by py_event_router when an event occurs.
 *
 * NIF: dispatch_callback(LoopRef, CallbackId, Type) -> ok
 */
ERL_NIF_TERM nif_dispatch_callback(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]);

/**
 * @brief Dispatch a timer callback
 *
 * Called when a timer expires.
 *
 * NIF: dispatch_timer(LoopRef, CallbackId) -> ok
 */
ERL_NIF_TERM nif_dispatch_timer(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]);

/**
 * @brief Wake up the event loop
 *
 * Signals the event loop to check for pending events.
 *
 * NIF: event_loop_wakeup(LoopRef) -> ok
 */
ERL_NIF_TERM nif_event_loop_wakeup(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]);

/**
 * @brief Submit an async coroutine to run on the event loop
 *
 * The coroutine result is sent to CallerPid via erlang.send().
 * This replaces the pthread+usleep polling model with direct message passing.
 *
 * NIF: event_loop_run_async(LoopRef, CallerPid, Ref, Module, Func, Args, Kwargs) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_event_loop_run_async(ErlNifEnv *env, int argc,
                                       const ERL_NIF_TERM argv[]);

/**
 * @brief Signal that a synchronous sleep has completed
 *
 * Called from Erlang when a sleep timer expires.
 *
 * NIF: dispatch_sleep_complete(LoopRef, SleepId) -> ok
 */
ERL_NIF_TERM nif_dispatch_sleep_complete(ErlNifEnv *env, int argc,
                                          const ERL_NIF_TERM argv[]);

/* ============================================================================
 * Internal Helper Functions
 * ============================================================================ */

/**
 * @brief Add an event to the pending queue
 *
 * Thread-safe addition to the pending events list.
 *
 * @param loop Event loop to add event to
 * @param type Event type (read, write, timer)
 * @param callback_id Callback ID to dispatch
 * @param fd File descriptor (for read/write), -1 for timers
 */
void event_loop_add_pending(erlang_event_loop_t *loop, event_type_t type,
                            uint64_t callback_id, int fd);

/**
 * @brief Clear all pending events
 *
 * @param loop Event loop to clear
 */
void event_loop_clear_pending(erlang_event_loop_t *loop);

/**
 * @brief Resource destructor for event loop
 */
void event_loop_destructor(ErlNifEnv *env, void *obj);

/**
 * @brief Resource destructor for fd_resource
 */
void fd_resource_destructor(ErlNifEnv *env, void *obj);

/**
 * @brief Resource destructor for timer_resource
 */
void timer_resource_destructor(ErlNifEnv *env, void *obj);

/**
 * @brief Resource stop callback for fd_resource (called on enif_select stop)
 */
void fd_resource_stop(ErlNifEnv *env, void *obj, ErlNifEvent event,
                      int is_direct_call);

/**
 * @brief Resource down callback for fd_resource (called when owner process dies)
 */
void fd_resource_down(ErlNifEnv *env, void *obj, ErlNifPid *pid,
                      ErlNifMonitor *mon);

/**
 * @brief Get callback ID from an fd resource
 *
 * NIF: get_fd_callback_id(FdRes, Type) -> CallbackId | undefined
 */
ERL_NIF_TERM nif_get_fd_callback_id(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]);

/**
 * @brief Re-register an fd for read monitoring
 *
 * Called after an event is delivered since enif_select is one-shot.
 *
 * NIF: reselect_reader(LoopRef, FdRes) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_reselect_reader(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]);

/**
 * @brief Re-register an fd for write monitoring
 *
 * Called after an event is delivered since enif_select is one-shot.
 *
 * NIF: reselect_writer(LoopRef, FdRes) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_reselect_writer(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]);

/**
 * @brief Handle a select event (dispatch + auto-reselect)
 *
 * Combined function that gets callback ID, dispatches to pending queue,
 * and auto-reselects for persistent watcher behavior.
 *
 * NIF: handle_fd_event(FdRef, Type) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_handle_fd_event(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]);

/**
 * @brief Handle FD event and immediately reselect for next event
 *
 * Combined operation that eliminates one roundtrip.
 *
 * NIF: handle_fd_event_and_reselect(FdRef, Type) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_handle_fd_event_and_reselect(ErlNifEnv *env, int argc,
                                               const ERL_NIF_TERM argv[]);

/**
 * @brief Stop read monitoring without closing the FD
 *
 * Pauses monitoring. Can be resumed with start_reader.
 *
 * NIF: stop_reader(FdRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_stop_reader(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]);

/**
 * @brief Start/resume read monitoring on an existing watcher
 *
 * NIF: start_reader(FdRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_start_reader(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]);

/**
 * @brief Stop write monitoring without closing the FD
 *
 * Pauses monitoring. Can be resumed with start_writer.
 *
 * NIF: stop_writer(FdRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_stop_writer(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]);

/**
 * @brief Start/resume write monitoring on an existing watcher
 *
 * NIF: start_writer(FdRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_start_writer(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]);

/**
 * @brief Cancel read monitoring (legacy alias for stop_reader)
 *
 * NIF: cancel_reader(LoopRef, FdRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_cancel_reader(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]);

/**
 * @brief Cancel write monitoring (legacy alias for stop_writer)
 *
 * NIF: cancel_writer(LoopRef, FdRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_cancel_writer(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]);

/**
 * @brief Explicitly close an FD with proper lifecycle cleanup
 *
 * Transfers ownership and triggers proper cleanup via ERL_NIF_SELECT_STOP.
 *
 * NIF: close_fd(FdRef) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_close_fd(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[]);

/* ============================================================================
 * Test Helper Functions
 * ============================================================================ */

/**
 * @brief Create a pipe for testing fd monitoring
 *
 * NIF: create_test_pipe() -> {ok, {ReadFd, WriteFd}} | {error, Reason}
 */
ERL_NIF_TERM nif_create_test_pipe(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]);

/**
 * @brief Close a test file descriptor
 *
 * NIF: close_test_fd(Fd) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_close_test_fd(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]);

/**
 * @brief Write data to a test file descriptor
 *
 * NIF: write_test_fd(Fd, Data) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_write_test_fd(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]);

/**
 * @brief Read data from a test file descriptor
 *
 * NIF: read_test_fd(Fd, MaxSize) -> {ok, Data} | {error, Reason}
 */
ERL_NIF_TERM nif_read_test_fd(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]);

/**
 * @brief Create a TCP listener socket for testing
 *
 * NIF: create_test_tcp_listener(Port) -> {ok, {ListenFd, ActualPort}} | {error, Reason}
 */
ERL_NIF_TERM nif_create_test_tcp_listener(ErlNifEnv *env, int argc,
                                           const ERL_NIF_TERM argv[]);

/**
 * @brief Accept a connection on a TCP listener socket
 *
 * NIF: accept_test_tcp(ListenFd) -> {ok, ClientFd} | {error, Reason}
 */
ERL_NIF_TERM nif_accept_test_tcp(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]);

/**
 * @brief Connect to a TCP server for testing
 *
 * NIF: connect_test_tcp(Host, Port) -> {ok, Fd} | {error, Reason}
 */
ERL_NIF_TERM nif_connect_test_tcp(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]);

/* ============================================================================
 * UDP Test Helper Functions
 * ============================================================================ */

/**
 * @brief Create a UDP socket for testing
 *
 * NIF: create_test_udp_socket(Port) -> {ok, {Fd, ActualPort}} | {error, Reason}
 */
ERL_NIF_TERM nif_create_test_udp_socket(ErlNifEnv *env, int argc,
                                         const ERL_NIF_TERM argv[]);

/**
 * @brief Receive data from a UDP socket with source address
 *
 * NIF: recvfrom_test_udp(Fd, MaxSize) -> {ok, {Data, {Host, Port}}} | {error, Reason}
 */
ERL_NIF_TERM nif_recvfrom_test_udp(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]);

/**
 * @brief Send data to a UDP destination address
 *
 * NIF: sendto_test_udp(Fd, Data, Host, Port) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_sendto_test_udp(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]);

/**
 * @brief Enable or disable SO_BROADCAST on a UDP socket
 *
 * NIF: set_udp_broadcast(Fd, Enable) -> ok | {error, Reason}
 */
ERL_NIF_TERM nif_set_udp_broadcast(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]);

/* ============================================================================
 * Python Module Functions
 * ============================================================================ */

/**
 * @brief Initialize the global Python event loop
 *
 * Sets the event loop that Python code will use.
 *
 * @param env NIF environment
 * @param loop Event loop to use
 * @return 0 on success, -1 on failure
 */
int py_event_loop_init_python(ErlNifEnv *env, erlang_event_loop_t *loop);

/**
 * @brief NIF to set the global Python event loop
 */
ERL_NIF_TERM nif_set_python_event_loop(ErlNifEnv *env, int argc,
                                        const ERL_NIF_TERM argv[]);

/**
 * @brief Create and register the py_event_loop Python module
 *
 * Called during Python initialization.
 *
 * @return 0 on success, -1 on failure
 */
int create_py_event_loop_module(void);

/**
 * @brief Create a default event loop
 *
 * Creates a default event loop and sets it as g_python_event_loop.
 * This ensures Python asyncio always has an event loop available.
 * Called after NIF is fully loaded.
 *
 * @param env NIF environment
 * @return 0 on success, -1 on failure
 */
int create_default_event_loop(ErlNifEnv *env);

#endif /* PY_EVENT_LOOP_H */
