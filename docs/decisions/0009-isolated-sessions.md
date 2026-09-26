# 0009: Sessions fork from a prepared zygote

Since 5.1.0. Code: `src/py_session.erl`, `src/py_session_template.erl`,
`priv/py_zygote.py`, `src/py_isolated.erl` (session origin), `src/py_child.erl`.

## Situation

Callers such as a durable-execution engine need each run of a Python
function to start from the same state and to leave nothing behind for the
next run. An isolated context keeps its interpreter between calls, and a
new one costs a cold interpreter start plus every import (about 50 ms
before the first import). Temporal's Python SDK isolates a workflow run by
re-importing its module in a fresh `sys.modules` inside a shared process;
Restate's does not isolate invocations at all and relies on the platform
(a container, a Lambda microVM). Neither gives a fresh process per run.

## Decision

A template prepares an interpreter once, in a zygote: a single-threaded
child that runs the imports and preload, builds the child runtime (the
`_isolated.Runtime` and the `erlang` module) without a socket, then forks
one child per session. The forked child connects that runtime to its own
socket and continues as a normal isolated child, so a session is an
isolated context (`py_isolated` with `session => true`). The zygote reports
each child's exit on its control socket; the template forwards it to the
session's context. `start => spawn` starts a normal child per session
instead, with an optional warm pool, for templates that cannot be forked.

A session is never restarted: after its child dies it answers every
request with the reason until it is closed. It runs in its own scratch
directory, its stdio is detached from the zygote's port, and it sees only
the template's environment.

Not chosen: a subinterpreter per session (about 13 ms, shares the process
environment, working directory, hash seed and C-extension state, and PyO3
extensions refuse to load), Temporal-style re-import in one process (state
leaks through passthrough and C modules), CRIU (Linux only, needs
privileges and PID namespaces), and Wasm images (no native C extensions).

## Consequences

- A session costs a fork and a connect (a few milliseconds) instead of an
  interpreter start and the imports.
- The zygote must stay single threaded: a template whose imports start a
  thread is refused. On macOS, modules that load Objective-C cannot be
  forked safely; `start => spawn` is the way out.
- All sessions of a template share its hash seed and its prepared state;
  per-session randomness relies on Python's at-fork reseeding.
- `erlang` functions fail during preload: the runtime is not connected
  until a session exists.
- A zygote that dies is rebuilt; its orphaned sessions are watched by pid
  (`py_isolated` probes `kill(pid, 0)`), since nobody reports their exit.
