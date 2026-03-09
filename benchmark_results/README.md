# Benchmark Results

This directory stores benchmark results for tracking performance across versions.

## Running Benchmarks

All benchmark scripts are in the `examples/` directory. Compile first with `rebar3 compile`.

### Quick Benchmark

```bash
escript examples/benchmark.erl --quick
```

### Full Benchmark

```bash
escript examples/benchmark.erl --full
```

### Version Comparison Benchmark

```bash
escript examples/benchmark_compare.erl
```

This outputs a summary table useful for comparing performance between versions.

### Concurrency Benchmark

```bash
escript examples/benchmark.erl --concurrent
```

### Channel API Benchmark

```bash
escript examples/bench_channel.erl
```

Compares Channel API throughput at various message sizes.

### Reactor Modes Benchmark

```bash
escript examples/bench_reactor_modes.erl
```

Compares Reactor performance in worker mode vs subinterpreter mode (Python 3.12+).

### Event Loop Benchmark

```bash
rebar3 shell
> run_benchmark:run().
```

Compares Erlang event loop vs standard asyncio event loop performance.

## Saving Results

To save benchmark results for comparison:

```bash
# Run and save to timestamped file
escript examples/benchmark_compare.erl > benchmark_results/$(date +%Y%m%d_%H%M%S).txt
```

## Result Files

Result files are named with timestamps: `YYYYMMDD_HHMMSS.txt`

Example files:
- `reactor_modes_20260309_093558.txt` - Reactor modes comparison results

## Key Metrics

When comparing versions, focus on:

1. **Sync call latency** - Time per `py:call()` operation
2. **Cast throughput** - Non-blocking `py:cast()` operations per second
3. **Concurrent throughput** - Performance under parallel load
4. **Channel throughput** - Messages per second at various sizes
5. **Reactor modes** - Worker vs subinterpreter performance

## System Information

Benchmarks automatically print system info including:
- Erlang/OTP version
- Number of schedulers
- Python version
- Execution mode (free_threaded, subinterp, multi_executor)
- Max concurrent operations
