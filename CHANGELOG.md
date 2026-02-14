# Changelog

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
