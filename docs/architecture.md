# Architecture

This page is the map of erlang_python. It
says which processes and threads exist, how a call travels from `py:call/3`
to Python and back in each context mode, how Python calls Erlang, and which
code paths are live. Read it before opening `c_src/` or `src/py_context.erl`.
The [code map](code-map.md) lists every file; the [glossary](glossary.md)
defines the overloaded words (worker, context, pool).

## One picture

```
Erlang VM                                              child OS process
+--------------------------------------------------+   (isolated mode only)
| erlang_python_sup                                |
|   py_callback   registry of Erlang funs Python   |
|                 may call                         |
|   py_shm        shared memory regions            |
|   py_thread_handler  spawns a handler process    |
|                 per Python thread that calls     |
|                 Erlang                           |
|   py_logger / py_tracer  Python logging, tracing |
|   py_context_sup ---- py_context (one per        |
|      context; mode worker | owngil | isolated)   |
|   py_context_init  starts the default pool       |
|   py_event_worker_sup / _registry  loop drivers  |
|   py_event_loop, py_event_loop_pool  main-       |
|                 interpreter asyncio loops        |
+--------------------------------------------------+
        |  NIF calls (dirty schedulers)     |  Unix socket, ETF frames
        v                                   v
+------------------------+          +-------------------------+
| libpython in the VM    |          | python3 py_isolated_    |
|  main interpreter      |          |   child.py              |
|  worker contexts: one  |          |  reader thread + main   |
|   pthread each, shared |          |   thread, own asyncio   |
|   GIL                  |          |   loop                  |
|  owngil contexts: one  |          +-------------------------+
|   pthread + own sub-   |
|   interpreter each     |
+------------------------+
```

A **context** is the unit of work: an Erlang process (`py_context`) that owns
one Python execution environment and serves calls in order. Pools
(`py_context_router`) route `py:call/3` to a context by scheduler affinity.

## The three context modes

| | `worker` | `owngil` | `isolated` |
|---|---|---|---|
| Python runs in | the VM, main interpreter | the VM, a sub-interpreter with its own GIL | a child process |
| Thread | one pthread per context (`worker_context_thread_main`) | one pthread per context (`owngil_context_thread_main`) | the child's main thread |
| Erlang process loop | the receive loop in `py_context` | the receive loop in `py_context` | `py_isolated` (`gen_statem`) |
| Transport | NIF request queue on `py_context_t` | same | Unix socket, frames of the callback pipe format |
| Python -> Erlang | suspension protocol | blocking callback pipe | socket frames |
| Interrupt | `PyThreadState_SetAsyncExc`, next bytecode | same | signal in the child, `SIGKILL` backstop |
| Guide | [context-affinity](context-affinity.md), [pools](pools.md) | [owngil_internals](owngil_internals.md) | [isolated](isolated.md) |

## Life of a call, per mode

### worker and owngil (embedded)

1. `py:call(M, F, A)` picks a context through `py_context_router` and sends
   `{call, From, MRef, M, F, Args, Kwargs}` to the `py_context` process
   (`src/py.erl`, `src/py_context.erl:call/6`). The caller waits in
   `await_reply/3`; a timeout there calls `interrupt/1`.
2. The `py_context` receive loop takes it and calls `handle_call_with_suspension/5`,
   which calls the `context_call_async` NIF
   (`c_src/py_nif.c`, `nif_context_call_async`). The NIF converts the
   arguments (`term_to_py`, `c_src/py_convert.c`) into a request, enqueues it
   on the context's queue (`ctx_queue_enqueue`) and returns `{enqueued, Ref}`
   at once. The Erlang process is now free to serve callbacks.
3. The context's pthread (`worker_context_thread_main` or
   `owngil_context_thread_main`, `c_src/py_nif.c`) dequeues the request and
   runs it through `owngil_execute_request` (despite its name it serves both
   modes), which calls into Python with the GIL held.
4. The thread converts the result (`py_to_term`) and sends
   `{py_result, Ref, Result}` to the `py_context` process, which replies
   `From ! {MRef, Result}`.

The older paths in `nif_context_call` (a blocking variant with an inline
"legacy" executor) are kept for the fallback
`{error, async_requires_worker_thread}` and are not taken by contexts
created today; see [code map](code-map.md) for the list of legacy code.

### isolated

1. Same first step: the message reaches the `py_context` pid, which in this
   mode runs `py_isolated` (a `gen_statem` entered with `enter_loop`).
2. In `idle` the request is encoded with `term_to_binary` into a frame
   `<<Id:64/native, Len:32/native, Status:8, ETF/binary>>` and written to
   the Unix socket; the state becomes `{busy, Id}` and other callers'
   requests are postponed (served in order when the child is free).
3. In the child, the reader thread parses frames and queues the request;
   the main thread runs it (`_erlang_impl/_isolated.py`, `Runtime._dispatch`)
   and writes the reply frame.
4. Back in `py_isolated`, the reply for the busy id moves the state to
   `idle` and answers the caller. A crash of the child is seen as the port's
   `exit_status`; the state goes through `{restarting, Reason}` and a new
   child is started within the restart budget.

Protocol details: the module header of `src/py_isolated.erl` and the
docstring of `priv/_erlang_impl/_isolated.py`.

## Python calling Erlang (`erlang.call`)

`erlang_call_impl` in `c_src/py_callback.c` chooses one of these paths, in
this order (the comment above it is the authoritative version):

1. **Suspension** (worker contexts). The Python call raises
   `SuspensionRequired`; the context thread returns
   `{suspended, CallbackId, State, {Name, Args}}` to `py_context`, which runs
   the registered fun (`execute/2` in `py_callback`), possibly serving nested calls
   meanwhile (`wait_for_callback/2`), and resumes with
   the `resume_callback` NIF.
2. **Blocking callback pipe** (owngil contexts). The context thread writes
   a request on a pipe and blocks; the `py_context` process has a dedicated
   handler (`callback_handler_loop/1`) that runs the fun and writes the
   response frame back with `context_write_callback_response`.
3. **Legacy worker handler** (`worker_*` NIFs): only used by
   `examples/gen_test.erl`.
4. **Thread worker** (`c_src/py_thread_worker.c`): any Python thread that is
   not a context thread (`threading.Thread`, executors) asks the
   `py_thread_handler` coordinator for a handler process and talks to it
   over a pipe. There is also an async variant (`erlang.async_call`) using a
   per-interpreter async pipe.

In isolated mode there is one path: a status-3 frame on the socket, answered
by a process the `py_isolated` state machine spawns; nested calls into the
same context are dispatched immediately because they come from that process.

The frame format shared by the pipe and the socket, and the ETF conventions,
will get their own page (protocols); until then `c_src/py_convert.c` (type
mapping) and `priv/_erlang_impl/_etf.py` are the reference.

## asyncio

Three different machineries, on purpose:

- Embedded contexts share an `ErlangEventLoop` (`priv/_erlang_impl/_loop.py`)
  whose `add_reader`/`add_writer` map onto `enif_select` and `call_later` onto
  `erlang:send_after`; readiness is delivered to a `py_event_worker` process
  per loop (`c_src/py_event_loop.c`, `src/py_event_worker.erl`). Worker loops
  (`py_context:start_loop/1`) run such a loop on the context thread.
- `py_event_loop`/`py_event_loop_pool` expose the main-interpreter loops for
  `py:async_call/3` and friends.
- An isolated child runs a plain asyncio loop; the only integration is
  delivering results over the socket. Nothing of the reactor is ported.

See [event_loop_architecture](event_loop_architecture.md) for the embedded
loop and [asyncio](asyncio.md) for the API.

## Data paths that avoid copies

- `py_buffer` (native): a NIF resource Erlang writes into and Python reads
  through the buffer protocol; embedded modes only.
- `py_shm` and `py_buffer:new(#{shared => true})`: a file mapped
  `MAP_SHARED` by the VM (through iommap) and by any interpreter, embedded or
  child, with flow control through callbacks (`_py_buffer_wait`,
  `_py_buffer_consumed`). See [isolated](isolated.md#bulk-data-with-shared-memory).
- `py_channel`, `py_byte_channel`: message queues between Erlang and Python
  coroutines, embedded modes only.

## Where state lives

| State | Owner | Notes |
|---|---|---|
| Registered callbacks | `py_callback` ETS `py_callbacks` | also mirrored in a C name registry so `erlang.<name>` resolves; `py_state` exposes its store the same way (`erlang.state_get`) |
| Import and path registry | `py_import` ETS | applied to every new interpreter and to isolated children at start |
| Preload code | `py_preload` persistent_term | run once per interpreter |
| Context pid -> NIF ref | `py_context` ETS `py_context_refs` | lets `interrupt/1` reach a context blocked in a NIF; isolated contexts store the atom `isolated` |
| Per-Erlang-process Python env | `py` process dictionary + NIF env resource | `py:call(Ctx, ...)`; not applicable to isolated contexts |
| Shared regions | `py_shm` ETS `py_shm_regions` | closed on owner death |
| Python-side state | the interpreter | lost when an isolated child restarts |

## Invariants worth knowing before editing

- A context serves one request at a time. Embedded: the thread dequeues one
  request; nested callbacks are served by the `py_context` process while the
  thread waits. Isolated: enforced by the `{busy, Id}` state and `postpone`.
- Only the context's own thread touches its Python objects; NIF callers only
  enqueue. `py_context_t` fields are documented in `c_src/py_nif.h`.
- Interrupts target the request executing now. Isolated mode also cancels a
  queued request by id; embedded modes cannot.
- Everything that crosses the socket is a term; NIF resources (channels,
  native buffers, object references) do not cross, and the API says so with
  `{error, not_supported_in_isolated}`.
- `binary_to_term` on child data is not `safe`: the child can create atoms.

## What is live and what is not

Kept for now, not used by current contexts: the `worker_*` NIF API and its
single executor thread (`c_src/py_exec.c`), the `async_worker_*` NIFs (they
return `deprecated`), `c_src/py_worker_pool.c` (no caller), the inline
"legacy" executor branches in `nif_context_*`, and the test-only fd NIFs in
`c_src/py_event_loop.c`. They are listed in the [code map](code-map.md) so
nobody debugs them by mistake; removing them is planned.
