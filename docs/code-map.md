# Code map

Every source file, what it owns, and where to look for its behaviour. Status
is `live` (on the path of a context created today), `legacy` (kept for
compatibility, no current caller in `src/`), or `test` (only exercised by
suites). Guides are in `docs/`, suites in `test/`. Start with
[architecture](architecture.md).

## Erlang (`src/`)

| Module | Owns | Status | Guide | Suites |
|---|---|---|---|---|
| `py` | Public API facade: call/eval/exec, streams, async helpers, venvs, memory, function registration | live | README, getting-started | `py_SUITE`, `py_api_SUITE`, `py_stream_SUITE`, `py_venv_SUITE` |
| `py_context` | The context process for embedded modes and the API every mode answers (`call/eval/exec`, `interrupt`, `kill`, loops, `pass_fd`); dispatch to `py_isolated` for isolated mode | live | context-affinity, workers, interrupts | `py_context_SUITE`, `py_context_process_SUITE`, `py_interrupt_SUITE`, `py_worker_loop_SUITE` |
| `py_isolated` | `gen_statem` driving a child process over the socket; restart policy | live | isolated | `py_isolated_*_SUITE` |
| `py_context_router` | Pools and scheduler-affinity routing | live | pools, context-affinity | `py_context_router_SUITE`, `py_pool_SUITE` |
| `py_context_sup`, `py_context_init` | Supervisor of contexts; starts the default pool at boot | live | pools | (through the above) |
| `py_nif` | Erlang stubs and docs for every NIF | live | api-reference | all |
| `py_callback` | Registry of Erlang funs callable as `erlang.call('name', ...)` | live | README (callbacks) | `py_callback_encoding_SUITE`, `py_thread_callback_SUITE` |
| `py_thread_handler` | Coordinator that gives each Python thread calling Erlang a handler process and a pipe | live | threading | `py_thread_callback_SUITE`, `py_reentrant_SUITE` |
| `py_event_loop` | Main-interpreter asyncio loop: `run`, `create_task`, `await`, and the loop callbacks Python needs | live | asyncio | `py_event_loop_SUITE`, `py_async_task_SUITE` |
| `py_event_loop_pool` | Several main-interpreter loops with process affinity | live | asyncio | `py_event_loop_pool_SUITE` |
| `py_event_worker`, `_sup`, `_registry` | One process per running loop receiving `enif_select` readiness and timers | live | event_loop_architecture | `py_event_loop_SUITE`, `py_fd_ops_SUITE` |
| `py_reactor_context` | FD-owning context for the protocol-based reactor | live | reactor | `py_reactor_SUITE` |
| `py_channel`, `py_byte_channel` | Term and byte queues between Erlang and Python coroutines (NIF resources) | live | channel | `py_channel_SUITE`, `py_byte_channel_SUITE` |
| `py_buffer` | Native streaming input buffer; shared variant delegates to `py_shm` | live | buffer, isolated | `py_buffer_SUITE`, `py_isolated_buffer_SUITE` |
| `py_shm` | Shared memory regions over iommap and the ring behind shared buffers | live | isolated | `py_isolated_shm_SUITE` |
| `py_import` | Registry of imports and `sys.path` entries applied to every interpreter | live | imports | `py_import_SUITE` |
| `py_preload` | Code run once per interpreter at start | live | preload | `py_preload_SUITE` |
| `py_state` | Shared key/value store visible from Python as `erlang.state_get/set/delete/keys` | live | README (shared state) | `py_state_SUITE` |
| `py_semaphore` | ETS counting semaphore for rate limiting | live | scalability | (through `py_SUITE`) |
| `py_logger`, `py_tracer` | Python `logging` into Erlang logger; tracing hooks | live | logging | `py_logging_SUITE` |
| `erlang_python_app`, `erlang_python_sup` | Application start and the supervision tree | live | architecture | all |
| `py_util` | Small helpers | live | | |

## C (`c_src/`)

`py_nif.c` is the only translation unit: it `#include`s the other `.c`
files. Editing `py_convert.c` alone does not compile it alone; build with
`rebar3 compile`. See `c_src/README.md`.

| File | Owns | Status |
|---|---|---|
| `py_nif.h` | Every shared type: `py_context_t`, request types, runtime state machine, atoms, globals | live |
| `py_nif.c` | Runtime init, context creation and destruction, the request queue and the two context thread mains, the process-per-context NIFs (`nif_context_*`), process-local envs, `py_ref`, the NIF table | live, with legacy branches |
| `py_convert.c` | `py_to_term` / `term_to_py`, the type mapping, tagged tuples (`{bytes, B}`, shared handles) | live |
| `py_exec.c` | Execution with suspension support; the legacy single executor thread | live (suspension), legacy (executor) |
| `py_callback.c` | The `erlang` Python module: `call`, `send`, `whereis`, `Atom`/`Pid`/`Ref` types, schedule markers, callback pipes, channel and shared dict methods | live |
| `py_thread_worker.c` | Python threads calling Erlang through `py_thread_handler` | live |
| `py_subinterp_thread.c` | Sub-interpreter thread pool used by owngil contexts and loop pools | live |
| `py_event_loop.c` | `ErlangEventLoop` support: `enif_select` readers/writers, timers, task injection, reactor dispatch, fd registry; also ~570 lines of test-only fd/TCP/UDP NIFs | live; test section |
| `py_channel.c`, `py_buffer.c`, `py_reactor_buffer.c`, `py_shared_dict.c` | The corresponding resources and their Python-facing methods | live |
| `py_logging.c` | Logging and tracing NIFs | live |
| `py_mem_limit.c` | Per-interpreter memory caps (owngil) | live |
| `py_worker_pool.c/.h` | An older worker pool | legacy, no caller |
| `py_util.c/.h` | Macros and helpers | live |

Inside `py_nif.c`, these are legacy: the `worker_*` NIFs and the "Worker
management" section, the `async_worker_*` NIFs (return `deprecated`), the
inline executor branches marked "Legacy mode" in `nif_context_call`,
`nif_context_eval`, `nif_context_exec`, and the `cancel_reader/writer`
aliases.

## Python (`priv/`)

`priv/` is on `sys.path` of every interpreter. `_erlang_impl` is the Python
half of the `erlang` module; the embedded C module delegates to it for the
loop, channels and servers.

| File | Owns | Used by |
|---|---|---|
| `_erlang_impl/__init__.py` | Public surface of `erlang` in embedded modes: `run`, `sleep`, `spawn_task`, loop policy, `atom`, channels, `server` | embedded |
| `_erlang_impl/_loop.py`, `_policy.py`, `_transport.py` | `ErlangEventLoop` (uvloop-compatible) over `enif_select` | embedded |
| `_erlang_impl/_reactor.py` | Protocol-based reactor over fds Erlang owns | embedded |
| `_erlang_impl/_channel.py`, `_byte_channel.py` | Python side of channels | embedded |
| `_erlang_impl/_server.py` | `serve`, `adopt`, `stop_serving` on fds handed over by Erlang; plain asyncio, works in every mode | all |
| `_erlang_impl/_sandbox.py`, `_subprocess.py` | Audit hook blocking fork/exec inside the VM | embedded |
| `_erlang_impl/_mode.py` | Detects how Python is running (embedded, free-threaded, child) | all |
| `_erlang_impl/_etf.py` | Pure-Python ETF codec with the `py_convert.c` mapping | isolated child |
| `_erlang_impl/_isolated.py` | Child runtime: socket frames, reader thread, re-entrant main loop, interrupt signal, asyncio loop, the `erlang` shim | isolated child |
| `_erlang_impl/_shm.py` | `SharedMemory` and `SharedBuffer` wrappers over mmap | all |
| `py_isolated_child.py` | Child launcher: rlimits, parent-death signal, cgroup join, connect | isolated child |
| `test_erlang_loop.py`, `tests/` | Python-side tests of the loop | test |

## Tests (`test/`)

Suites named `py_<area>_SUITE`. Cross-mode suites run the same cases in
`worker` and `isolated` groups (`py_isolated_SUITE`, `py_isolated_vm_SUITE`,
`py_isolated_shm_SUITE`, `py_isolated_buffer_SUITE`). Python helpers used by
suites are `test/py_test_*.py`. `test/coverage_audit.md` maps public APIs to
cases. `test/test.config` holds node-wide settings (memory limits flag).

## Build and docs

`rebar.config` runs `do_cmake.sh` / `do_build.sh` (CMake in `c_src/`) as
compile hooks; the NIF lands in `priv/py_nif.so`. `make lint-docs` checks
that Erlang snippets in the guides call real exports and that Python
snippets parse. `rebar3 ex_doc` builds the guides listed in `rebar.config`.
