# 0002: Three callback delivery paths, chosen by the calling thread

Since 3.0.0. Code: `erlang_call_impl` in `c_src/py_callback.c`,
`c_src/py_thread_worker.c`, `src/py_thread_handler.erl`, the callback
handling in `src/py_context.erl`.

## Situation

`erlang.call` must work from the context thread, from a Python thread the
user started (executors, `threading.Thread`), and while the context
thread is already waiting for Erlang (nested calls). A single blocking
handler process deadlocks as soon as the callback calls Python again on
the same context.

## Decision

The path is chosen by who is calling:

1. Suspension, on a context thread with suspension enabled: the call
   raises `SuspensionRequired`, the context returns
   `{suspended, ...}` to its process, which runs the function, serves
   nested requests meanwhile, and resumes. Inspired by PyO3's model.
2. The context callback pipe, on a context thread with a handler
   registered: the thread blocks on a pipe while the handler process
   runs the function.
3. The thread worker, for every other Python thread: a per-thread worker
   with its own pipe and handler process, coordinated by
   `py_thread_handler`.

All three answer with the same response body (`<<2, ETF>>` or
`<<1, Message>>`).

## Consequences

- Nested callbacks of any depth work on the suspension path because the
  Erlang process is never blocked in a NIF while it waits.
- Three writers and two parsers of the response body must stay in step;
  the protocols page lists them.
- A Python thread that calls Erlang is not a context thread and cannot
  reach the calling context's Python objects; re-entrant calls into the
  same owngil context from a spawned thread are not supported.
- Adding a mode means adding a path (isolated mode has a fourth: the
  socket frame), not extending one of these.
