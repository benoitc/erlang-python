# Glossary

The same words mean different things in different files of this project.
This page fixes one meaning per term and says where the other uses come
from, so a reader can translate as they go.

## Context

**Context**: one Python execution environment served by one Erlang process
(`py_context`), in order, one request at a time. The unit of `py:context/0`,
pools and modes. In C it is `py_context_t` (`c_src/py_nif.h`); in
`py_context.erl` it is the process; for isolated mode the process runs
`py_isolated` and the environment is a child OS process.

Other uses: `py_reactor_context` is a context that also owns file
descriptors for the reactor; "coordinator context" in C comments means the
`py_thread_handler` side of the thread-worker channel.

## Mode

**Mode**: how a context runs Python. `worker` (main interpreter, one pthread
per context, shared GIL), `owngil` (a sub-interpreter with its own GIL per
context, one pthread), `isolated` (a child process). `py_context:new(#{mode => ...})`.

Related flags on `py_context_t`: `uses_worker_thread` (has its own pthread;
true for worker and owngil contexts created today), `is_subinterp` (has its
own sub-interpreter), `uses_own_gil` (that sub-interpreter has its own GIL).
`subinterp` in file and NIF names (`py_subinterp_thread.c`,
`subinterp_supported/0`) refers to the machinery owngil mode is built on;
there is no separate "subinterp mode" any more.

The runtime-wide `PY_MODE_FREE_THREADED` / `PY_MODE_GIL` in `py_nif.h` is
about the Python build (free-threaded or not), not about contexts.

## Worker

The most overloaded word. Meanings, by file:

| Where | Meaning | Prefer to say |
|---|---|---|
| `py_context:new(#{mode => worker})` | the context mode above | worker mode |
| `worker_context_thread_main`, `uses_worker_thread` (`py_nif.c`) | the pthread that serves a context's queue | context thread |
| `worker_new/call/eval/exec` NIFs, `py_worker_t` | the legacy per-worker API, before contexts | legacy worker API |
| `py_worker_pool.c`, `py_pool_worker_t` | an older pool, no caller | legacy pool |
| `thread_worker`, `thread_worker_call` (`py_thread_worker.c`), `py_thread_handler` | the channel a Python thread uses to call Erlang | thread callback bridge |
| `py_event_worker` | the Erlang process that drives one asyncio loop (readiness, timers) | loop driver |
| `docs/workers.md`, "worker loop" | a long-running asyncio loop on a context thread, gunicorn-style | worker loop |

## Pool

`py_context_router` pools (`py:call(Pool, M, F, A)`): named sets of contexts
routed by scheduler. `py_event_loop_pool`: main-interpreter asyncio loops
with process affinity. `g_thread_pool` in `py_subinterp_thread.c`: the
threads behind owngil contexts. `g_pool` in `py_worker_pool.c`: legacy.

## Callback

An Erlang function registered with `py:register_function/2` or
`register/2` in `py_callback` and called from Python as `erlang.call('name', ...)`
or `erlang.name(...)`. Four delivery paths exist (suspension, blocking pipe,
thread worker, socket); see [architecture](architecture.md#python-calling-erlang-erlangcall).

**Suspension**: worker-mode delivery where the Python call raises
`SuspensionRequired`, the context thread hands control to the Erlang process,
and execution resumes with the result (`resume_callback/2`).

**Callback pipe**: owngil-mode delivery where the context thread blocks on a
pipe until the `py_context` handler process writes the response frame.

## Frame

The wire unit of the callback pipe and of the isolated socket:
`<<Id:64/native, Len:32/native, Body:Len/binary>>`, body `<<Status:8, ETF>>`.
Status 0 request, 1 error reply, 2 ok reply, 3 request from Python,
4 event, 5 control.

## Loop

`ErlangEventLoop`: the asyncio loop implementation backed by `enif_select`
(`_erlang_impl/_loop.py`), used by embedded modes. "Loop ref": the NIF
handle of such a loop (`py_context:loop_ref/1`, the `submit_task` NIF).
An isolated child uses the standard asyncio loop and has no loop ref.

## Environment, process-local env

The Python namespace a call runs in. Every context has globals; in embedded
modes each Erlang process can additionally get its own env inside a context
(`py:call(Ctx, ...)`, [process-bound-envs](process-bound-envs.md)); isolated
contexts have one namespace, the child's `__main__`.

## Handle

A term that stands for something living elsewhere: `{'$py_shm', Id, Path, Size}`
(shared region), `{'$py_buffer', Id, Path, Ring}` (shared buffer), NIF
resource references (native buffers, channels, `py_ref` object references).
Only the first two cross a process boundary.

## Child

The OS process an isolated context runs Python in, started from
`priv/py_isolated_child.py`. It is restarted on crash within the context's
restart budget; its state does not survive a restart.

## Interrupt, kill

`py_context:interrupt/1` stops the request executing now (at the next
bytecode in embedded modes, immediately in the child through a signal).
`py_context:kill/1` sends `SIGKILL` to an isolated child; there is no
equivalent for embedded contexts.
