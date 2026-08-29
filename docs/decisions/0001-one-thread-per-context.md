# 0001: One pthread per context, NIFs only enqueue

Since 3.0.0. Code: `ctx_thread_main_worker`, `ctx_thread_main_owngil`,
the request queue on `py_context_t` (`c_src/py_nif.c`, `c_src/py_nif.h`).

## Situation

Before 3.0.0 Python ran on dirty schedulers: a NIF took the GIL, swapped
in the context's thread state and executed the call. That pinned a dirty
scheduler for the whole call with a 30 s cap, gave numpy, torch and
tensorflow a different OS thread on every call (they keep per-thread
state and some refuse to work across threads), and needed a single-slot
request buffer that raced with concurrent callers.

## Decision

Every context owns one pthread that runs all of its Python. NIFs called
from Erlang processes allocate a `ctx_request_t`, append it to the
context's queue and return at once; the thread dequeues, executes with
the GIL, and sends `{py_result, Ref, Result}` back. The same thread
serves worker mode (main interpreter, shared GIL) and owngil mode (its
own interpreter and GIL).

## Consequences

- Stable thread affinity: a context's Python always runs on the same OS
  thread, and no dirty scheduler is held during a call.
- Erlang processes never touch a context's Python objects; the lock and
  ownership contract on `py_context_t` follows from this.
- Shutdown must join the thread. A thread stuck in a C call cannot be
  joined, so the context is leaked on purpose rather than freed under a
  running thread.
- Interrupts have to reach the thread from outside: `interrupt_mutex`,
  `exec_thread_id` and `PyThreadState_SetAsyncExc`.
- One request at a time per context is a property of the design, not a
  limitation to work around; parallelism comes from more contexts.
