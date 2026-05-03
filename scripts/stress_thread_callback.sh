#!/usr/bin/env bash
# Stress harness for py_thread_callback_SUITE. Runs the suite repeatedly
# and aborts on the first failure. Use under CPU pressure to widen race
# windows:
#
#   for i in 1 2 3 4; do yes > /dev/null & done
#   ENABLE_PY_THREAD_CB_TRACE=ON ./scripts/stress_thread_callback.sh 50
#   kill %1 %2 %3 %4
#
# A clean run prints "PASS <iter>" on every iteration and exits 0.

set -euo pipefail

ITERATIONS="${1:-50}"

cd "$(dirname "$0")/.."

if [[ "${ENABLE_PY_THREAD_CB_TRACE:-OFF}" == "ON" ]]; then
    echo "Rebuilding with PY_THREAD_CB_TRACE on..."
    rm -rf _build/cmake _build/default/lib/erlang_python/priv/erlang_python_nif*
    mkdir -p _build/cmake
    (cd _build/cmake && cmake ../../c_src -DENABLE_PY_THREAD_CB_TRACE=ON \
        && cmake --build . -- -j "$(nproc 2>/dev/null || sysctl -n hw.ncpu)")
fi

rebar3 compile

for i in $(seq 1 "$ITERATIONS"); do
    if ! rebar3 ct --suite=test/py_thread_callback_SUITE --readable=compact; then
        echo "FAIL on iteration $i"
        exit 1
    fi
    echo "PASS $i"
done

echo "All $ITERATIONS iterations passed."
