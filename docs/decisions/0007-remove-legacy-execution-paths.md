# 0007: One execution path per mode; the legacy API is removed

Since 5.0.0. Code: the removal in `c_src/py_nif.c`, `c_src/py_exec.c`,
`c_src/py_callback.c`, `src/py_nif.erl` (PR #75).

## Situation

After 0001 every context had a thread, but the code still carried the
paths from before: an executor thread with a worker pool, blocking NIF
variants that ran Python on dirty schedulers, suspended-state resources
for a resume protocol nothing used, and `py_nif` stubs for all of it.
Roughly 4 500 lines were reachable only from suites or from nothing, and
every reader had to work out which of two paths was live.

## Decision

Remove them. A context created today has exactly one path per mode:
the queue and context thread for worker and owngil, the socket for
isolated. NIFs that needed a thread now answer
`{error, context_has_no_thread}` instead of falling back to a scheduler.
Because public functions disappeared, the release that carries this is a
major version (5.0.0), not a minor one.

## Consequences

- `docs/code-map.md` can say "live" for everything in `src/` and
  `c_src/` except the test helpers, and mean it.
- There is no fallback when a context has no thread; that state is a
  bug, and it is reported as one.
- Anyone on the removed functions upgrades through the changelog's
  Removed section.
