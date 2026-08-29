# c_src

The NIF that embeds CPython in the VM. One translation unit: `py_nif.c`
`#include`s every other `.c` file (see the "Include module implementations"
section near line 265), so build with `rebar3 compile` (CMake through
`do_cmake.sh` / `do_build.sh`), never a single file. Headers declare what
the included files share.

Read `docs/architecture.md` first for how a call travels; this file says
where things are.

## Files

| File | What it owns | Notes |
|---|---|---|
| `py_nif.h` | All shared types: `py_context_t` and its request queue, request types, callback and suspension state, runtime state machine, atoms, globals, declarations | 2.4k lines. The struct comments carry the locking rules; read `py_context_t` before touching threads |
| `py_nif.c` | Runtime init and finalize, resource types, context create/destroy, the request queue, `ctx_thread_main_worker` and `ctx_thread_main_owngil`, `ctx_execute_*` (one set for both thread kinds), `ctx_dispatch` / `ctx_dispatch_async` (the only way a NIF reaches a context thread), the `nif_context_*` NIFs, process-local envs, `py_ref`, the NIF function table at the end, assembled from the `PY_*_NIFS` macros of the other files | Sections are banner-separated; `grep -n '^ \* ===\|^/\* ==='` lists them |
| `py_convert.c` | `py_to_term`, `term_to_py`, depth limits, tagged tuples (`{bytes, B}`, `{'$py_shm', ...}`), error tuples `{error, {Type, Msg}}` | The type mapping tables in the comments are the reference for `_etf.py` |
| `py_exec.c` | Execution mode detection (free-threaded or GIL build) | |
| `py_callback.c` | The `erlang` Python module: `call`, `send`, `whereis`, `schedule*`, `Atom`/`Pid`/`Ref` types, callback delivery paths (suspension, blocking pipe, async pipe), channel and shared-dict methods, callback name registry | `erlang_call_impl` documents the path precedence |
| `py_thread_worker.c` | Python threads calling Erlang through `py_thread_handler` | |
| `py_subinterp_thread.c/.h` | Thread pool of sub-interpreters (owngil contexts, loop pools) | |
| `py_event_loop.c/.h` | `ErlangEventLoop` support: `enif_select` readers and writers, timers, task injection into loops, reactor dispatch, fd registry, Python module `py_event_loop`; plus test-only fd/TCP/UDP NIFs (section "Test Helper Functions") | Largest file |
| `py_channel.c/.h`, `py_buffer.c/.h`, `py_reactor_buffer.c/.h`, `py_shared_dict.c` | Resources with a Python-facing object each | |
| `py_logging.c` | Logging and tracing NIFs | |
| `py_mem_limit.c` | obmalloc arena accounting for owngil memory caps | |
| `py_util.c/.h` | Macros, small helpers | |

## Where the live paths are

- `py:call/3` in worker or owngil mode: `nif_context_call_async` (`py_nif.c`)
  enqueues; `ctx_thread_main_worker` or `ctx_thread_main_owngil`
  dequeues and calls `ctx_execute_request`; the reply goes out as
  `{py_result, Ref, Result}`.
- `erlang.call` from Python: `erlang_call_impl` (`py_callback.c`).
- Interrupt: `nif_context_interrupt` (`py_nif.c`), `interrupt_mutex` rules on
  `py_context_t`.
- Type conversion: `py_to_term` / `term_to_py` (`py_convert.c`).
- Isolated mode has no C code of its own: `os_kill` is the only NIF it uses.

## Rules that are easy to break

- Only the context's thread touches the context's Python objects. NIFs
  called from Erlang processes enqueue requests and return; they do not run
  Python for a context that has a thread.
- `Py_BEGIN_ALLOW_THREADS` around every blocking wait; never block on an
  Erlang-side resource with the GIL held.
- Never call a Future method while holding `async_futures_mutex`
  (`py_callback.c`, comment above the struct explains the pattern).
- `interrupt_mutex` is taken only by threads that do not hold the GIL.
- `queue_mutex` protects the request queue of a context and is taken before
  a request's own mutex (`ctx_queue_cancel_all`), never the other way.
- A NIF that can block or run Python is registered with a dirty scheduler
  flag in the table at the end of `py_nif.c`.

## Adding a NIF

1. Implement `static ERL_NIF_TERM nif_x(ErlNifEnv*, int, const ERL_NIF_TERM[])`
   next to related code.
2. Add `{"x", Arity, nif_x, Flags}` to the `PY_*_NIFS` macro at the end of
   that file (or to the `py_nif.c` block of `nif_funcs[]` for NIFs that
   live there).
3. Add the stub and its `-spec` and doc to `src/py_nif.erl`.
4. Cover it in a suite; `rebar3 dialyzer` and `rebar3 xref` must stay clean.

