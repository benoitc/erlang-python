#!/bin/bash
# Benchmark comparison script for scalable I/O model
#
# Compares benchmark results between baseline and current implementation.
#
# Usage:
#   ./scripts/bench_compare.sh                      # Run baseline vs current
#   ./scripts/bench_compare.sh baseline_v1.7.1     # Compare with specific baseline
#   ./scripts/bench_compare.sh baseline current     # Compare two result files

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_results"

mkdir -p "$RESULTS_DIR"

cd "$PROJECT_DIR"

run_benchmark() {
    local label=$1
    local output_file="$RESULTS_DIR/${label}_$(date +%Y%m%d_%H%M%S).txt"

    echo ""
    echo "========================================"
    echo "Running benchmark: $label"
    echo "========================================"

    # Compile first
    echo "Compiling..."
    rebar3 compile

    # Run benchmark
    echo "Running benchmark..."
    erl -pa _build/default/lib/*/ebin \
        -noshell \
        -eval "
            application:ensure_all_started(erlang_python),
            Results = py_scalable_io_bench:run_all(),
            py_scalable_io_bench:save_results(Results, \"$output_file\"),
            init:stop()
        " 2>&1 | tee "$output_file.log"

    echo ""
    echo "Results saved to: $output_file"
    echo "$output_file"
}

compare_results() {
    local baseline_file=$1
    local current_file=$2

    echo ""
    echo "========================================"
    echo "COMPARISON"
    echo "========================================"

    if [ -f "$baseline_file" ] && [ -f "$current_file" ]; then
        echo ""
        echo "Baseline: $baseline_file"
        echo "Current:  $current_file"
        echo ""

        # Extract key metrics using Erlang
        erl -pa _build/default/lib/*/ebin \
            -noshell \
            -eval "
                {ok, [Baseline]} = file:consult(\"$baseline_file\"),
                {ok, [Current]} = file:consult(\"$current_file\"),

                CompareMetric = fun(Name, BMap, CMap, Key) ->
                    case {maps:get(Key, maps:get(Name, BMap, #{}), undefined),
                          maps:get(Key, maps:get(Name, CMap, #{}), undefined)} of
                        {undefined, _} -> skip;
                        {_, undefined} -> skip;
                        {B, C} when is_number(B), is_number(C) ->
                            Diff = ((C - B) / B) * 100,
                            Sign = if Diff >= 0 -> \"+\"; true -> \"\" end,
                            io:format(\"~-35s: ~10.1f -> ~10.1f (~s~.1f%)~n\",
                                     [atom_to_list(Name) ++ \"/\" ++ atom_to_list(Key),
                                      B, C, Sign, Diff])
                    end
                end,

                io:format(\"~nKey Metrics Comparison:~n\"),
                io:format(\"~s~n\", [string:copies(\"-\", 70)]),

                CompareMetric(timer_throughput_single, Baseline, Current, timers_per_sec),
                CompareMetric(timer_latency, Baseline, Current, p95_latency_ms),
                CompareMetric(timer_latency, Baseline, Current, p99_latency_ms),
                CompareMetric(tcp_echo_single, Baseline, Current, messages_per_sec),
                CompareMetric(timer_throughput_concurrent, Baseline, Current, timers_per_sec),
                CompareMetric(tcp_echo_concurrent, Baseline, Current, messages_per_sec),

                init:stop()
            " 2>/dev/null || echo "Could not parse result files"
    else
        echo "One or both result files not found"
    fi
}

# Main logic
case $# in
    0)
        # Run baseline benchmark on v1.7.1 tag and current
        BASELINE_FILE=$(run_benchmark "baseline")
        CURRENT_FILE=$(run_benchmark "current")
        compare_results "$BASELINE_FILE" "$CURRENT_FILE"
        ;;
    1)
        # Compare with specified baseline
        BASELINE_FILE="$RESULTS_DIR/$1.txt"
        if [ ! -f "$BASELINE_FILE" ]; then
            BASELINE_FILE=$(ls -t "$RESULTS_DIR/$1"*.txt 2>/dev/null | head -1)
        fi
        CURRENT_FILE=$(run_benchmark "current")
        compare_results "$BASELINE_FILE" "$CURRENT_FILE"
        ;;
    2)
        # Compare two specific files
        BASELINE_FILE="$1"
        CURRENT_FILE="$2"
        if [ ! -f "$BASELINE_FILE" ]; then
            BASELINE_FILE="$RESULTS_DIR/$1"
        fi
        if [ ! -f "$CURRENT_FILE" ]; then
            CURRENT_FILE="$RESULTS_DIR/$2"
        fi
        compare_results "$BASELINE_FILE" "$CURRENT_FILE"
        ;;
    *)
        echo "Usage: $0 [baseline_label] [current_label]"
        exit 1
        ;;
esac

echo ""
echo "========================================"
echo "Benchmark complete"
echo "========================================"
