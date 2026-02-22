# Changelog

## 1.6.0 (2026-02-22)

### Added

- **Python Logging Integration** - Forward Python's `logging` module to Erlang's `logger`
  - `py:configure_logging/0,1` - Setup Python logging to forward to Erlang
  - `erlang.ErlangHandler` - Python logging handler that sends to Erlang
  - `erlang.setup_logging(level, format)` - Configure logging from Python
  - Fire-and-forget architecture using `enif_send()` for non-blocking messaging
  - Level filtering at NIF level for performance (skip message creation for filtered logs)
  - Log metadata includes module, line number, and function name
  - Thread-safe - works from any Python thread

- **Distributed Tracing** - Collect trace spans from Python code
  - `py:enable_tracing/0`, `py:disable_tracing/0` - Enable/disable span collection
  - `py:get_traces/0` - Retrieve collected spans
  - `py:clear_traces/0` - Clear collected spans
  - `erlang.Span(name, **attrs)` - Context manager for creating spans
  - `erlang.trace(name)` - Decorator for tracing functions
  - Span events via `span.event(name, **attrs)`
  - Automatic parent/child span linking via thread-local storage
  - Error status capture with exception details
  - Duration tracking in microseconds

- **New Erlang modules**
  - `py_logger` - gen_server receiving log messages from Python workers
  - `py_tracer` - gen_server collecting and managing trace spans

- **New C source**
  - `c_src/py_logging.c` - NIF implementations for logging and tracing

- **Documentation and examples**
  - `docs/logging.md` - Logging and tracing documentation
  - `examples/logging_example.erl` - Working escript example
  - Updated `docs/getting-started.md` with logging/tracing section

- **New test suite**
  - `test/py_logging_SUITE.erl` - 9 tests for logging and tracing

- `ATOM_NIL` for Elixir `nil` compatibility in type conversions

### Performance

- **Type conversion optimizations** - Faster Python ↔ Erlang marshalling
  - Use `enif_is_identical` for atom comparison instead of `strcmp`
  - Use `PyLong_AsLongLongAndOverflow` to avoid exception machinery
  - Cache `numpy.ndarray` type at init for fast isinstance checks
  - Stack allocate small tuples/maps (≤16 elements) to avoid heap allocation
  - Use `enif_make_map_from_arrays` for O(n) map building vs O(n²) puts
  - Reorder type checks for web workloads (strings/dicts first)
  - UTF-8 decode with bytes fallback for invalid sequences

- **Fire-and-forget NIF architecture** - Log and trace calls never block Python execution
  - Uses `enif_send()` to dispatch messages asynchronously to Erlang processes
  - Python code continues immediately after sending, no round-trip wait
- **NIF-level log filtering** - Messages below threshold are discarded before term creation
  - Volatile bool flags for O(1) receiver availability checks
  - Level threshold stored in C global, no Erlang callback needed
- **Minimal term allocation** - Direct Erlang term building without intermediate structures
  - Timestamps captured at NIF level using `enif_monotonic_time()`

### Fixed

- **Python 3.12+ event loop thread isolation** - Fixed asyncio timeouts on Python 3.12+
  - `ErlangEventLoop` now only used for main thread; worker threads get `SelectorEventLoop`
  - Async worker threads bypass the policy to create `SelectorEventLoop` directly
  - Per-call `ErlNifEnv` for thread-safe timer scheduling in free-threaded mode
  - Fail-fast error handling in `erlang_loop.py` instead of silent hangs
  - Added `gil_acquire()`/`gil_release()` helpers to avoid GIL double-acquisition

## 1.5.0 (2026-02-18)

### Added

- **`py_asgi` module** - Optimized ASGI request handling with:
  - Pre-interned Python string keys (15+ ASGI scope keys)
  - Cached constant values (http type, HTTP versions, methods, schemes)
  - Thread-local response pooling (16 slots per thread, 4KB initial buffer)
  - Direct NIF path bypassing generic py:call()
  - ~60-80% throughput improvement over py:call()
  - Configurable runner module via `runner` option
  - Sub-interpreter and free-threading (Python 3.13+) support

- **`py_wsgi` module** - Optimized WSGI request handling with:
  - Pre-interned WSGI environ keys
  - Direct NIF path for marshalling
  - ~60-80% throughput improvement over py:call()
  - Sub-interpreter and free-threading support

- **Web frameworks documentation** - New documentation at `docs/web-frameworks.md`

## 1.4.0 (2026-02-18)

### Added

- **Erlang-native asyncio event loop** - Custom asyncio event loop backed by Erlang's scheduler
  - `ErlangEventLoop` class in `priv/erlang_loop.py`
  - Sub-millisecond latency via Erlang's `enif_select` (vs 10ms polling)
  - Zero CPU usage when idle - no busy-waiting or polling overhead
  - Full GIL release during waits for better concurrency
  - Native Erlang scheduler integration for I/O events
  - Event loop policy via `get_event_loop_policy()`

- **TCP support for asyncio event loop**
  - `create_connection()` - TCP client connections
  - `create_server()` - TCP server with accept loop
  - `_ErlangSocketTransport` - Non-blocking socket transport with write buffering
  - `_ErlangServer` - TCP server with `serve_forever()` support

- **UDP/datagram support for asyncio event loop**
  - `create_datagram_endpoint()` - Create UDP endpoints with full parameter support
  - `_ErlangDatagramTransport` - Datagram transport implementation
  - Parameters: `local_addr`, `remote_addr`, `reuse_address`, `reuse_port`, `allow_broadcast`
  - `DatagramProtocol` callbacks: `datagram_received()`, `error_received()`
  - Support for both connected and unconnected UDP
  - New NIF helpers: `create_test_udp_socket`, `sendto_test_udp`, `recvfrom_test_udp`, `set_udp_broadcast`
  - New test suite: `test/py_udp_e2e_SUITE.erl`

- **Asyncio event loop documentation**
  - New documentation: `docs/asyncio.md`
  - Updated `docs/getting-started.md` with link to asyncio documentation

### Performance

- **Event loop optimizations**
  - Fixed `run_until_complete` callback removal bug (was using two different lambda references)
  - Cached `ast.literal_eval` lookup at module initialization (avoids import per callback)
  - O(1) timer cancellation via handle-to-callback_id reverse map (was O(n) iteration)
  - Detach pending queue under mutex, build Erlang terms outside lock (reduced contention)
  - O(1) duplicate event detection using hash set (was O(n) linear scan)
  - Added `PERF_BUILD` cmake option for aggressive optimizations (-O3, LTO, -march=native)

## 1.3.2 (2026-02-17)

### Fixed

- **torch/PyTorch introspection compatibility** - Fixed `AttributeError: 'erlang.Function'
  object has no attribute 'endswith'` when importing torch or sentence_transformers in
  contexts where erlang_python callbacks are registered.
  - Root cause: torch does dynamic introspection during import, iterating through Python's
    namespace and calling `.endswith()` on objects. The `erlang` module's `__getattr__` was
    returning `ErlangFunction` wrappers for *any* attribute access.
  - Solution: Added C-side callback name registry. Now `__getattr__` only returns
    `ErlangFunction` wrappers for actually registered callbacks. Unregistered attributes
    raise `AttributeError` (normal Python behavior).
  - New test: `test_callback_name_registry` in `py_reentrant_SUITE.erl`

## 1.3.1 (2026-02-16)

### Fixed

- **Hex.pm packaging** - Added `files` section to app.src to include build scripts
  (`do_cmake.sh`, `do_build.sh`) and other necessary files in the hex.pm package

## 1.3.0 (2026-02-16)

### Added

- **Asyncio Support** - New `erlang.async_call()` for asyncio-compatible callbacks
  - `await erlang.async_call('func', arg1, arg2)` - Call Erlang from async Python code
  - Integrates with asyncio event loop via `add_reader()`
  - No exceptions raised for control flow (unlike `erlang.call()`)
  - Releases dirty NIF thread while waiting (non-blocking)
  - Works with FastAPI, Starlette, aiohttp, and other ASGI frameworks
  - Supports concurrent calls via `asyncio.gather()`
  - New test: `test_async_call` in `py_reentrant_SUITE.erl`
  - New test module: `test/py_test_async.py`
  - Updated documentation: `docs/threading.md` - Added Asyncio Support section

### Fixed

- **Flag-based callback detection in replay path** - Fixed SuspensionRequired exceptions
  leaking when ASGI middleware catches and re-raises exceptions. The replay path in
  `nif_resume_callback_dirty` now uses flag-based detection (checking `tl_pending_callback`)
  instead of exception-type detection.

### Changed

- **C code optimizations and refactoring**
  - **Thread safety fixes**: Used `pthread_once` for async callback initialization,
    fixed mutex held during Python calls in async event loop thread
  - **Timeout handling**: Added `read_with_timeout()` and `read_length_prefixed_data()`
    helpers with proper timeouts on all blocking pipe reads (30s for callbacks, 10s for spawns)
  - **Code deduplication**: Merged `create_suspended_state()` and
    `create_suspended_state_from_existing()` into unified `create_suspended_state_ex()`,
    extracted `build_pending_callback_exc_args()` and `build_suspended_result()` helpers
  - **Performance**: Optimized list conversion using `enif_make_list_cell()` to build
    lists directly without temporary array allocation
  - Removed unused `make_suspended_term()` function

## 1.2.0 (2026-02-15)

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
  - `py_pool:broadcast` for sending requests to all workers

- **Documentation improvements**
  - Added shared state section to getting-started, scalability, and ai-integration guides
  - Added embedding caching example using shared state
  - Added hex.pm badges to README

### Fixed

- **Memory safety** - Added NULL checks to all `enif_alloc()` calls in NIF code
- **Worker resilience** - Fixed crash in `py_subinterp_pool:terminate` when workers undefined
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
