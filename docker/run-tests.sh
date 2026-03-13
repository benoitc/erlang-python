#!/bin/bash
# Run tests in Docker with different Python configurations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$SCRIPT_DIR"

usage() {
    echo "Usage: $0 [python314|asan|all]"
    echo ""
    echo "Options:"
    echo "  python314  - Run tests with Python 3.14 (free-threading)"
    echo "  asan       - Run tests with Python + Address Sanitizer"
    echo "  all        - Run both test configurations"
    echo ""
    echo "Examples:"
    echo "  $0 python314"
    echo "  $0 asan"
    echo "  $0 all"
}

build_and_run() {
    local target=$1
    echo "========================================"
    echo "Building and running: $target"
    echo "========================================"

    docker compose build "$target"
    docker compose run --rm "$target"
}

case "${1:-all}" in
    python314)
        build_and_run python314
        ;;
    asan)
        build_and_run asan
        ;;
    all)
        build_and_run python314
        echo ""
        build_and_run asan
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        echo "Error: Unknown option '$1'"
        usage
        exit 1
        ;;
esac

echo ""
echo "========================================"
echo "All tests completed successfully!"
echo "========================================"
