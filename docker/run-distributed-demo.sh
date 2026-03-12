#!/bin/bash
# Run the distributed Python demo with Docker
#
# This script starts a 3-node Erlang cluster and runs the distributed demo.
#
# Usage:
#   ./docker/run-distributed-demo.sh          # Run demo automatically
#   ./docker/run-distributed-demo.sh shell    # Start interactive coordinator shell
#   ./docker/run-distributed-demo.sh clean    # Clean up containers

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.distributed.yml"

cd "$PROJECT_DIR"

case "${1:-demo}" in
    demo)
        echo "Starting distributed Python demo..."
        echo "This will start 3 Erlang nodes and run the demo."
        echo ""

        # Start workers in background
        docker compose -f "$COMPOSE_FILE" up -d worker1 worker2

        echo "Waiting for workers to start..."
        sleep 5

        # Run demo
        docker compose -f "$COMPOSE_FILE" run --rm demo

        # Clean up
        docker compose -f "$COMPOSE_FILE" down
        ;;

    shell)
        echo "Starting distributed cluster with interactive coordinator..."
        echo ""
        echo "Once in the shell, run:"
        echo "  net_adm:ping('worker1@worker1')."
        echo "  net_adm:ping('worker2@worker2')."
        echo "  nodes()."
        echo "  distributed_python:demo()."
        echo ""

        # Start workers in background
        docker compose -f "$COMPOSE_FILE" up -d worker1 worker2

        echo "Waiting for workers to start..."
        sleep 5

        # Start interactive coordinator
        docker compose -f "$COMPOSE_FILE" run --rm coordinator

        # Clean up
        docker compose -f "$COMPOSE_FILE" down
        ;;

    clean)
        echo "Cleaning up containers..."
        docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans
        ;;

    logs)
        docker compose -f "$COMPOSE_FILE" logs -f
        ;;

    *)
        echo "Usage: $0 [demo|shell|clean|logs]"
        echo ""
        echo "Commands:"
        echo "  demo   - Run the distributed demo automatically (default)"
        echo "  shell  - Start interactive coordinator shell"
        echo "  clean  - Clean up containers"
        echo "  logs   - Show logs from all containers"
        exit 1
        ;;
esac
