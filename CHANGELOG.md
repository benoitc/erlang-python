# Changelog

## 1.2.0 (unreleased)

### Added

- **Context Affinity** - Bind Erlang processes to dedicated Python workers for state persistence
  - `py:bind()` / `py:unbind()` - Bind current process to a worker, preserving Python state
  - `py:bind(new)` - Create explicit context handles for multiple contexts per process
  - `py:with_context(Fun)` - Scoped helper with automatic bind/unbind
  - Context-aware functions: `py:ctx_call/4-6`, `py:ctx_eval/2-4`, `py:ctx_exec/2`
  - Automatic cleanup via process monitors when bound processes die
  - O(1) ETS-based binding lookup for minimal overhead
  - New test suite: `test/py_context_SUITE.erl`

- **Python Thread Support** - Any spawned Python thread can now call `erlang.call()` without blocking
  - Supports `threading.Thread`, `concurrent.futures.ThreadPoolExecutor`, and any other Python threads
  - Each spawned thread lazily acquires a dedicated "thread worker" channel
  - One lightweight Erlang process per Python thread handles callbacks
  - Automatic cleanup when Python thread exits via `pthread_key_t` destructor
  - New module: `py_thread_handler.erl` - Coordinator and per-thread handlers
  - New C file: `py_thread_worker.c` - Thread worker pool management
  - New test suite: `test/py_thread_callback_SUITE.erl`
  - New documentation: `docs/threading.md` - Threading support guide

- **Reentrant Callbacks** - Python→Erlang→Python callback chains without deadlocks
  - Exception-based suspension mechanism interrupts Python execution cleanly
  - Callbacks execute in separate processes to prevent worker pool exhaustion
  - Supports arbitrarily deep nesting (tested up to 10+ levels)
  - Transparent to users - `erlang.call()` works the same, just without deadlocks
  - New test suite: `test/py_reentrant_SUITE.erl`
  - New examples: `examples/reentrant_demo.erl` and `examples/reentrant_demo.py`

### Changed

- Callback handlers now spawn separate processes for execution, allowing workers
  to remain available for nested `py:eval`/`py:call` operations
- **Modular C code structure** - Split monolithic `py_nif.c` (4,335 lines) into
  logical modules for better maintainability:
  - `py_nif.h` - Shared header with types, macros, and declarations
  - `py_convert.c` - Bidirectional type conversion (Python ↔ Erlang)
  - `py_exec.c` - Python execution engine and GIL management
  - `py_callback.c` - Erlang callback support and asyncio integration
  - Uses `#include` approach for single compilation unit (no build changes needed)

### Fixed

- **Multiple sequential erlang.call()** - Fixed infinite loop when Python code makes
  multiple sequential `erlang.call()` invocations in the same function. The replay
  mechanism now falls back to blocking pipe behavior for subsequent calls after the
  first suspension, preventing the infinite replay loop.
- **Memory safety in C NIF** - Fixed memory leaks and added NULL checks
  - `nif_async_worker_new`: msg_env now freed on pipe/thread creation failure
  - `multi_executor_stop`: shutdown requests now properly freed after join
  - `create_suspended_state`: binary allocations cleaned up on failure paths
  - Added NULL checks on all `enif_alloc_resource` and `enif_alloc_env` calls
- **Dialyzer warnings** - Added `{suspended, ...}` return type to NIF specs for
  `worker_call`, `worker_eval`, and `resume_callback` functions
- **Dead code removal** - Cleaned up unused code discovered during code review:
  - Removed `execute_direct()` function in `py_exec.c` (duplicated inline logic)
  - Removed unused `ref` field from `async_pending_t` struct in `py_nif.h`
  - Removed `worker_recv/2` from `py_nif.erl` (declared but never implemented in C)

### Documentation

- **Doxygen-style C documentation** - Added documentation to all C source files:
  - Architecture overview with execution mode diagrams
  - Type mapping tables for conversions
  - GIL management patterns and best practices
  - Suspension/resume flow diagrams for callbacks
  - Function-level `@param`, `@return`, `@pre`, `@warning`, `@see` annotations

## 1.1.0 (2026-02-15)

### Added

- **Shared State API** - ETS-backed storage for sharing data between Python workers
  - `state_set/get/delete/keys/clear` accessible from Python via `from erlang import ...`
  - `py:state_store/fetch/remove/keys/clear` from Erlang
  - Atomic counters with `state_incr/decr` (Python) and `py:state_incr/decr` (Erlang)
  - New example: `examples/shared_state_example.erl`

- **Native Python Import Syntax** for Erlang callbacks
  - `from erlang import my_func; my_func(args)` - most Pythonic
  - `erlang.my_func(args)` - attribute-style access
  - `erlang.call('my_func', args)` - legacy syntax still works

- **Module Reload** - Reload Python modules across all workers during development
  - `py:reload(module)` uses `importlib.reload()` to refresh modules from disk
  - `py_pool:broadcast/1` for sending requests to all workers

- **Documentation improvements**
  - Added shared state section to getting-started, scalability, and ai-integration guides
  - Added embedding caching example using shared state
  - Added hex.pm badges to README

### Fixed

- **Memory safety** - Added NULL checks to all `enif_alloc()` calls in NIF code
- **Worker resilience** - Fixed crash in `py_subinterp_pool:terminate/2` when workers undefined
- **Streaming example** - Fixed to work with worker pool design (workers don't share namespace)
- **ETS table ownership** - Moved `py_callbacks` table creation to supervisor for resilience

### Changed

- Created `py_util` module to consolidate duplicate code (`to_binary/1`, `send_response/3`, `normalize_timeout/1-2`)
- Consolidated `async_await/2` to call `await/2` reducing duplication

## 1.0.0 (2026-02-14)

Initial release of erlang_python - Execute Python from Erlang/Elixir using dirty NIFs.

### Features

- **Python Integration**
  - Call Python functions with `py:call/3-5`
  - Evaluate expressions with `py:eval/1-3`
  - Execute statements with `py:exec/1-2`
  - Stream from Python generators with `py:stream/3-4`

- **Multiple Execution Modes** (auto-detected)
  - Free-threaded Python 3.13+ (no GIL, true parallelism)
  - Sub-interpreters Python 3.12+ (per-interpreter GIL)
  - Multi-executor for older Python versions

- **Worker Pools**
  - Main worker pool for synchronous calls
  - Async worker pool for asyncio coroutines
  - Sub-interpreter pool for parallel execution

- **Erlang/Elixir Callbacks**
  - Register functions callable from Python via `py:register_function/2-3`
  - Python code calls back with `erlang.call('name', args...)`

- **Virtual Environment Support**
  - Activate venvs with `py:activate_venv/1`
  - Use isolated package dependencies

- **Rate Limiting**
  - ETS-based semaphore prevents overload
  - Configurable max concurrent operations

- **Type Conversion**
  - Automatic conversion between Erlang and Python types
  - Integers, floats, strings, lists, tuples, maps/dicts, booleans

- **Memory Management**
  - Access Python GC stats with `py:memory_stats/0`
  - Force garbage collection with `py:gc/0-1`
  - Memory tracing with `py:tracemalloc_start/stop`

### Examples

- `semantic_search.erl` - Text embeddings and similarity search
- `rag_example.erl` - Retrieval-Augmented Generation with Ollama
- `ai_chat.erl` - Interactive LLM chat
- `erlang_concurrency.erl` - 10x speedup with BEAM processes
- `elixir_example.exs` - Full Elixir integration demo

### Documentation

- Getting Started guide
- AI Integration guide
- Type Conversion reference
- Scalability and performance tuning
- Streaming with generators
