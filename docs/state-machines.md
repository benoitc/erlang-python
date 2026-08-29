# State machines

The states each long-lived thing in erlang_python moves through, what moves
it, and what is allowed in each state. Read this before changing a loop,
a shutdown path or a restart policy; the invariants at the end are the ones
a change must keep. Messages and frames are in [protocols](protocols.md).

## The Python runtime (C)

`g_runtime_state` in `c_src/py_nif.h`, moved with compare-and-swap so only
one thread wins each transition.

```
UNINIT --init--> INITING --ok--> RUNNING --finalize--> SHUTTING_DOWN --> STOPPED
                    |                                                      ^
                    +--------------------- failure ------------------------+
```

- `runtime_is_running()` gates every NIF that touches Python; a NIF that
  finds `SHUTTING_DOWN` or `STOPPED` returns `not_running` or an error.
- `STOPPED` may re-enter `INITING`: the runtime can be finalized and
  initialized again in one VM (the suites do this).

## An embedded context (`py_context` process + context thread)

Two cooperating machines: the Erlang process and the pthread in C.

Context thread (`worker_context_thread_main`, `owngil_context_thread_main`
in `c_src/py_nif.c`):

```
starting --namespaces created--> waiting --dequeue--> executing --reply--> waiting
    |                                |                                       
    +-- init_error -----> exited     +-- shutdown_requested ----> exited
```

- `waiting` blocks on `queue_not_empty` under `queue_mutex`.
- `executing` is bracketed by `py_context_exec_enter` / `exec_leave`
  (interrupt bookkeeping) around the GIL; the request mirror on
  `py_context_t` is valid only here.
- `exited` sets `worker_running = false`; `nif_context_destroy` joins with a
  timeout and, if the join fails, marks the context `leaked` and pins the
  resource instead of freeing it.

Erlang process (`loop/1` in `py_context`):

```
idle --{call|eval|exec|submit}--> in_request --{py_result}--> idle
 |                                   |
 |                                   +--{suspended, ...}--> in_callback --resume--> in_request
 |
 +--{start_loop}--> loop_running --{py_result, LoopReq}--> idle
                        |
                        +--{stop_loop, GraceMs}--> stopping --grace--> interrupt --deadline--> idle
```

- `in_request` is a blocking receive for the reply; nested callbacks
  arrive as `{erlang_callback, ...}` (pipe) or `{suspended, ...}` and are
  served inline, so the process never deadlocks with its own thread.
- In `loop_running` the loop request `LoopReq` occupies the thread: `call`,
  `eval`, `exec` and `call_method` answer `{error, loop_running}`; `stop`
  first stops the loop. The owner is monitored and its `DOWN` stops the loop.
- `stopping` arms `loop_stop_deadline` (cooperative) then
  `loop_interrupt_deadline`; the loop exit is the `py_result` for `LoopReq`,
  and the owner gets `{py_loop_exit, Ctx, Result}`.

## A request (`ctx_request_t`)

```
created (refcount 1) --enqueue--> queued (2) --dequeue--> running --done--> completed
                                      |                                        |
                                      +-- cancelled (destroy, timeout) -------+
                                                                               v
                                                              freed when refcount hits 0
```

The queue and the caller each hold one reference; whoever releases last
frees. `cancelled` is checked by the thread before running, so a request
cancelled while queued is answered `{error, cancelled}` without touching
Python.

## An isolated context (`py_isolated`, `gen_statem`)

States are the `state()` type in `src/py_isolated.erl`; `sys:get_state/1`
shows the current one and `sys:trace/2` prints transitions.

```
            start_child + handshake
                     |
                     v
   +--------------> idle <----------------------------------+
   |                 |                                      |
   |  main request   |  start_loop                          |
   |                 v                                      |
   |           {busy, Id} ---reply Id---> idle              |
   |                 |                                      |
   |                 |          looping --stop_loop-----> stopping_loop
   |                 |             |         (grace: interrupt, then kill)
   |                 |             +--loop_exit event--------+
   |                 |
   +-- child exit / kill / socket error --> {restarting, Reason} --new child--> idle
                                                     |
                                                     +-- budget exhausted --> stop
```

Per state:

| State | Main requests (`call`, `eval`, `exec`, `start_loop`) | Other requests | Timers armed |
|---|---|---|---|
| `idle` | dispatched, go to `{busy, Id}` (`start_loop` goes to `looping`) | dispatched | none |
| `{busy, Id}` | postponed, unless from a process running a callback for this context (nested, dispatched) | dispatched | `{timeout, kill}` bound to `Id` once an interrupt was sent |
| `looping` | `{error, loop_running}` | dispatched (`submit`, `pass_fd`, ...) | none |
| `stopping_loop` | postponed | dispatched | `state_timeout` for the interrupt, then `{timeout, kill}` bound to `loop` |
| `{restarting, R}` | postponed | postponed | `state_timeout` waiting for the port's `exit_status` |

Transitions and their triggers:

- `{busy, Id}` to `idle`: a status-1/2 frame with `Id`. Frames for other
  ids (nested requests) do not change state.
- Anything to `{restarting, Reason}`: `{Port, {exit_status, S}}`, a socket
  `abort`, a `{memory_limit, Rss}` event, `kill/1`, or a request the state
  machine cannot deliver. In-flight requests, submitted tasks and a running
  loop fail with `Reason` (`fail_pending/2`); postponed requests are kept
  and served by the next child.
- `{restarting, _}` to `idle`: the port reported the exit and a new child
  passed the handshake. `restart_allowed/1` counts restarts in
  `restart_period`; over `max_restarts` (or with `restart => false`) the
  process stops with `{child_exited, Reason}`.
- `looping` to `stopping_loop`: `stop_loop/2` or the owner's `DOWN`.
  `stopping_loop` to `idle`: the `{loop_exit, R}` event. The interrupt
  `state_timeout` and the kill backstop escalate if the loop does not exit.

Interrupt timing: `interrupt/1` in `{busy, Id}` sends `{interrupt, Id}` and
arms `{{timeout, kill}, KillAfter, Id}`. The reply for `Id` cancels the
timer; if it fires, the child gets SIGKILL and the machine goes to
`{restarting, killed}`. An interrupt in any other state answers
`not_running`.

## The child (`_isolated.py`)

The main thread runs one request at a time but can nest:

```
idle --request--> executing [stack: Id1]
                     |
                     +-- erlang.call --> waiting for reply, serving nested requests [Id1, Id2] ...
                     |
                     +-- SIGUSR1 while running --> _Interrupted raised in the request on top
```

- `_exec_stack` holds the ids being executed, innermost last. An
  `{interrupt, Target}` control is honoured only if `Target` is the top of
  the stack; otherwise it is stale and dropped. The signal handler raises
  only while `running` is true, so an interrupt between requests cannot
  leak into the next one.
- The reader thread never runs Python code: it parses frames, resolves
  waiters, and pushes requests and interrupts to the main thread's inbox.
- `broken` (EOF or a hard error on the socket) is terminal: the reader
  calls `os._exit`, since nothing useful can happen in the process any more
  and a main thread stuck in a C call must not keep it alive. Erlang sees
  the port's `exit_status` and runs the restart policy above.
- With a loop: `start_loop` runs `run_forever` on the main thread; requests
  that need the main thread are refused with `loop_running`, `submit` goes
  through `call_soon_threadsafe`, and `stop_loop` calls `loop.stop()` from
  the reader thread.

## A shared region (`py_shm`)

```
new --> open --close/1 or owner DOWN--> closed (file unlinked, handle closed)
```

Mappings in Python outlive `closed` until the wrapper is closed or
collected; a later access raises `ValueError`, never a fault. A shared
buffer adds `closed = true` in its header at `py_buffer:close/1`, and
readers waiting in `_py_buffer_wait` are answered with the closed flag.

## Invariants

- A context executes one top-level request at a time, in every mode.
  Embedded: one thread, one dequeue. Isolated: `{busy, Id}` plus `postpone`.
- Nested requests only come from a process serving a callback of the same
  context. Anything else waits.
- A restart never loses a queued request, only the ones in flight, and
  callers of those get an error naming the cause.
- Interrupts target the request executing now; a stale interrupt is
  dropped on both sides (kill timer bound to the id in Erlang, stack check
  in the child, `interrupt_pending` cleared in `exec_leave` for embedded
  contexts).
- Shutdown never frees memory a thread may still use: embedded contexts
  leak on a failed join, the child is reaped through the port.
