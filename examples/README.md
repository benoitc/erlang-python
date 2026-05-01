# Examples

This directory contains runnable examples demonstrating erlang_python features.

## Prerequisites

```bash
rebar3 compile
```

## Basic Examples

### basic_example.erl
Basic Python calls, eval, and async operations.
```bash
escript examples/basic_example.erl
```

### streaming_example.erl
Streaming from Python generators.
```bash
escript examples/streaming_example.erl
```

### shared_state_example.erl
Shared state between Erlang and Python workers.
```bash
escript examples/shared_state_example.erl
```

### logging_example.erl
Python logging forwarded to Erlang logger, plus distributed tracing.
```bash
escript examples/logging_example.erl
```

### erlang_concurrency.erl
Demonstrates parallel execution using BEAM processes from Python.
```bash
escript examples/erlang_concurrency.erl
```

### reentrant_callback.erl / reentrant_demo.erl
Python calling Erlang calling Python (reentrant callbacks).
```bash
escript examples/reentrant_callback.erl
escript examples/reentrant_demo.erl
```

## AI/ML Examples

These require additional Python packages.

### semantic_search.erl
Semantic search with sentence-transformers.
```bash
python3 -m venv /tmp/ai-venv
/tmp/ai-venv/bin/pip install sentence-transformers numpy
escript examples/semantic_search.erl
```

### rag_example.erl
Retrieval-Augmented Generation with Ollama.
```bash
/tmp/ai-venv/bin/pip install sentence-transformers numpy requests
ollama pull llama3.2
escript examples/rag_example.erl
```

### ai_chat.erl
Interactive AI chat.
```bash
escript examples/ai_chat.erl
```

### embedder_example.erl
Text embeddings generation.
```bash
escript examples/embedder_example.erl
```

## Reactor Examples

### reactor_echo.erl
Simple echo server using Reactor API.
```bash
escript examples/reactor_echo.erl
```

### reactor_owngil_example.erl
Reactor with OWN_GIL subinterpreters (Python 3.14+).
```bash
escript examples/reactor_owngil_example.erl
```

## Benchmarks

### benchmark.erl
General performance benchmark with options.
```bash
escript examples/benchmark.erl --quick    # Quick run
escript examples/benchmark.erl --full     # Full benchmark
escript examples/benchmark.erl --concurrent  # Concurrency focus
```

### benchmark_compare.erl
Version comparison benchmark with summary table.
```bash
escript examples/benchmark_compare.erl
```

### bench_channel.erl
Channel API throughput benchmark.
```bash
escript examples/bench_channel.erl
```

### bench_reactor_modes.erl
Reactor worker vs OWN_GIL benchmark.
```bash
escript examples/bench_reactor_modes.erl
```

### bench_event_loop.erl
Event loop performance comparison.
```bash
escript examples/bench_event_loop.erl
```

### bench_reactor_buffer.erl
Reactor buffer performance.
```bash
escript examples/bench_reactor_buffer.erl
```

### bench_resource_pool.erl
Resource pool benchmark.
```bash
escript examples/bench_resource_pool.erl
```

### run_benchmark.erl
Event loop benchmark (run in shell).
```bash
rebar3 shell
> run_benchmark:run().
```

## Elixir

### elixir_example.exs
Elixir integration example.
```bash
elixir --erl "-pa _build/default/lib/erlang_python/ebin" examples/elixir_example.exs
```

## Other

### gen_test.erl
Test generator for development.
```bash
escript examples/gen_test.erl
```
