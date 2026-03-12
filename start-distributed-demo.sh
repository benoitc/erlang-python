#!/bin/sh
# Start the distributed demo
set -e

echo "Waiting for workers to be ready..."
sleep 5

echo "Starting demo..."
cd /app

erl -sname demo \
    -setcookie distributed_demo \
    -pa _build/default/lib/erlang_python/ebin \
    -noshell \
    -eval "
        application:ensure_all_started(erlang_python),
        timer:sleep(2000),
        io:format(\"Connecting to workers...~n\"),
        pong = net_adm:ping(worker1@worker1),
        pong = net_adm:ping(worker2@worker2),
        timer:sleep(500),
        io:format(\"Connected nodes: ~p~n\", [nodes()]),
        distributed_python:demo(),
        init:stop()
    "
