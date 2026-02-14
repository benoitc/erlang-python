# Changelog

## 1.1.0 (Unreleased)

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
