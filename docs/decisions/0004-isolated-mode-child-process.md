# 0004: Isolation is a child OS process over a Unix socket

Since 5.0.0. Code: `src/py_isolated.erl`, `priv/py_isolated_child.py`,
`priv/_erlang_impl/_isolated.py`.

## Situation

Embedded modes cannot stop a Python call stuck in C, cannot bound its
memory or CPU, and a segfault in an extension kills the node. Running
untrusted or unknown Python code needs those guarantees, with the public
API unchanged so a pool can mix modes.

## Decision

An isolated context is one child process per context, started with
`open_port` so the VM reaps it, talking to its `py_context` process over
a Unix socket with the frame format of the callback pipe
(`<<Id:64, Len:32, Body>>`, body `<<Status, ETF>>`). Interrupts are a
signal to the child's main thread with a `SIGKILL` backstop; limits are
rlimits, cgroups v2 on Linux and an RSS watchdog on macOS; a crash
restarts the child within a budget. The child uses the standard asyncio
loop; there is no C code of its own in the VM beyond `os_kill`.

Not chosen: a seccomp or Capsicum sandbox (a later hardening step, the
process boundary is the first one), a NIF-side sub-process pool, and a
new wire protocol (the existing frame and ETF conventions are enough).

## Consequences

- Everything crossing the socket is a term. NIF resources (channels,
  native buffers, object references) do not cross; the API says so with
  `{error, not_supported_in_isolated}`.
- The child can create atoms (`binary_to_term` is not called with
  `safe` because handles and control terms need atoms); untrusted code
  must not mint unbounded distinct atoms.
- Each call copies arguments and results through the socket; bulk data
  needs shared memory (0006).
- Python state is lost on restart; the context stays usable.
- Platform code lives in the child launcher: parent-death signal
  (`prctl` on Linux, `procctl` on FreeBSD), memory limits per OS.
