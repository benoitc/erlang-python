#!/bin/bash
# Benchmark regression test for asyncio performance
#
# Compares commit 9150564e (per-loop isolation) against baseline 73267864.
#
# Usage:
#   ./scripts/bench_regression.sh
#   ./scripts/bench_regression.sh --quick    # Fewer iterations
#   ./scripts/bench_regression.sh --full     # More iterations

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_results"

BASELINE_COMMIT="73267864"
REGRESSION_COMMIT="9150564e"

# Default benchmark options
BENCH_OPTS="#{}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            BENCH_OPTS="#{timer_iterations => 1000, tcp_messages => 1000, concurrent_timers => 500, asgi_requests => 500}"
            shift
            ;;
        --full)
            BENCH_OPTS="#{timer_iterations => 50000, tcp_messages => 20000, concurrent_timers => 5000, asgi_requests => 5000}"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

mkdir -p "$RESULTS_DIR"

cd "$PROJECT_DIR"

# Save current state
ORIGINAL_BRANCH=$(git branch --show-current 2>/dev/null || git rev-parse --short HEAD)
STASH_NEEDED=false

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Stashing uncommitted changes..."
    git stash push -m "bench_regression temporary stash"
    STASH_NEEDED=true
fi

cleanup() {
    echo ""
    echo "Cleaning up..."
    git checkout "$ORIGINAL_BRANCH" 2>/dev/null || git checkout -
    if [ "$STASH_NEEDED" = true ]; then
        echo "Restoring stashed changes..."
        git stash pop || true
    fi
}

trap cleanup EXIT

run_benchmark() {
    local commit=$1
    local label=$2
    local output_file="$RESULTS_DIR/${label}_$(date +%Y%m%d_%H%M%S).txt"

    echo ""
    echo "========================================"
    echo "Benchmarking: $label ($commit)"
    echo "========================================"

    # Checkout commit
    git checkout "$commit" --quiet

    # Copy benchmark file if it doesn't exist in this commit
    if [ ! -f "test/py_asyncio_bench.erl" ]; then
        echo "Copying benchmark module to this commit..."
        git show "$ORIGINAL_BRANCH:test/py_asyncio_bench.erl" > test/py_asyncio_bench.erl 2>/dev/null || \
        git show HEAD:test/py_asyncio_bench.erl > test/py_asyncio_bench.erl 2>/dev/null || true
    fi

    # Clean and compile
    echo "Compiling..."
    rm -rf _build/default/lib/erlang_python/ebin/*.beam 2>/dev/null || true
    rebar3 compile

    # Run benchmark
    echo "Running benchmark..."
    erl -pa _build/default/lib/*/ebin \
        -noshell \
        -eval "
            application:ensure_all_started(erlang_python),
            Results = py_asyncio_bench:run_all($BENCH_OPTS),
            file:write_file(\"$output_file\", io_lib:format(\"~p.~n\", [Results])),
            init:stop()
        " 2>&1 | tee -a "$output_file"

    echo ""
    echo "Results saved to: $output_file"
}

# Run benchmarks
echo "Starting regression benchmark..."
echo "Baseline: $BASELINE_COMMIT"
echo "Regression: $REGRESSION_COMMIT"
echo ""

# First run on current (regression) commit since we have the benchmark file
run_benchmark "$REGRESSION_COMMIT" "regression"
REGRESSION_FILE=$(ls -t "$RESULTS_DIR"/regression_*.txt 2>/dev/null | head -1)

# Then run on baseline
run_benchmark "$BASELINE_COMMIT" "baseline"
BASELINE_FILE=$(ls -t "$RESULTS_DIR"/baseline_*.txt 2>/dev/null | head -1)

# Compare results
echo ""
echo "========================================"
echo "COMPARISON"
echo "========================================"
echo ""
echo "Baseline ($BASELINE_COMMIT):"
if [ -f "$BASELINE_FILE" ]; then
    grep -E "(timers/sec|msg/sec|req/sec|latency|MB/sec)" "$BASELINE_FILE" | head -10
fi

echo ""
echo "Regression ($REGRESSION_COMMIT):"
if [ -f "$REGRESSION_FILE" ]; then
    grep -E "(timers/sec|msg/sec|req/sec|latency|MB/sec)" "$REGRESSION_FILE" | head -10
fi

echo ""
echo "========================================"
echo "Benchmark complete. Results in: $RESULTS_DIR"
echo "========================================"
