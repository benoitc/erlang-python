# Isolated Sessions

A session is a fresh Python process for one piece of work: a request, a
workflow step, a user's job. You prepare a template once (interpreter,
paths, imports, preload code, environment, limits), then every
`py_session:new/1` gives you a new process that starts from that template
and shares no state with any other session. Use sessions when one run must
never see what another run left behind: module globals, `sys.modules`,
environment changes, threads, files in its working directory.

## Build a template

```erlang
{ok, T} = py_session:template(#{
    paths => ["/srv/app"],
    imports => [orders],
    preload => <<"import orders\norders.load_rules()">>,
    env => #{"TZ" => "UTC"},
    hash_seed => 0
}).
```

The imports and preload run once, now. With the default `start => fork`
they run in a zygote process that every session is forked from, so a
session starts with them already done.

## Open a session and use it

```erlang
{ok, S} = py_session:new(T),
{ok, Result} = py_context:call(S, orders, handle, [Event, State]),
ok = py_session:close(S).
```

A session is an isolated context: `py_context:call/eval/exec`, callbacks
with `erlang.call`, calls back into the same session from a callback,
`py_context:interrupt/1`, `kill/1`, worker loops and `pass_fd/2` all work
on it. It is linked to the process that created it. `close/1` kills its
process; a session is never reused.

## Run one call

```erlang
{ok, Result} = py_session:run(T, orders, handle, [Event, State],
                              #{timeout => 5000}).
```

`run/5` opens a session, makes the call and closes it, also when the call
fails.

## Choose how sessions start

| `start` | How a session starts | Use it when |
|---|---|---|
| `fork` (default) | forked from the template's zygote: imports and preload are already done | the template can be forked |
| `spawn` | a new interpreter runs the imports and preload | a module starts a thread when imported, or loads Objective-C on macOS |
| `reimport` | no new process: the function's module is imported again in a fresh module dictionary, in a worker or owngil context | you only need fresh module state per run, and each run is one call (see below) |

A fork copies only the thread that calls it, so a template whose imports
start a thread is refused:

```erlang
{error, {template_failed, {init_failed, {threads, [<<"import-time-thread">>]}}}} =
    py_session:template(#{imports => [module_with_a_thread]}).
```

With `start => spawn`, keep sessions started ahead of time so `new/1` does
not wait for an interpreter:

```erlang
{ok, T} = py_session:template(#{start => spawn, warm => 4, imports => [orders]}).
```

With `start => fork`, `zygotes => N` runs N zygotes that fork in turn when
one is not enough.

## Re-import runs in a worker or owngil context

When a fresh module state per run is enough, and you do not need a process
boundary, run in an embedded context instead. This is how Temporal's Python
SDK isolates a workflow run:

```erlang
{ok, T} = py_session:template(#{
    start => reimport,
    mode => worker,                 %% or owngil
    contexts => 4,                  %% runs are spread over these
    paths => ["/srv/app"],
    imports => [orders_models],     %% imported once, shared by every run
    passthrough => [pydantic]       %% shared too, imported when first used
}),
{ok, Result} = py_session:run(T, orders, handle, [Event, State]).
```

Each `run/5` imports `orders` again in a new module dictionary, so its
globals start fresh and nothing a run leaves in them reaches the next one.
The standard library, `erlang`, `imports` and `passthrough` modules are
shared with the context. A run is one call: `py_session:new/1` answers
`{error, {not_supported, reimport}}`. Calls back into the same context from
a callback work.

| | `fork` / `spawn` | `reimport` |
|---|---|---|
| Module globals of the function's module and what it imports | fresh | fresh |
| Standard library, `imports`, `passthrough` | fresh (`fork`: as prepared) | shared, their state persists |
| C extension state, environment, working directory, threads, hash seed | fresh (hash seed per template) | shared with the context |
| A call stuck in C | killed | runs on; interrupts land at the next bytecode |
| A segfault in a C extension | kills the session | kills the node |
| Memory and CPU limits | yes | no |
| Cost per run | a fork, or a warm child | an import of the function's module |
| Cost per call inside the run | a local socket round trip | none |

Two things to know about re-import runs:

- `sys.modules` in that interpreter becomes a mapping that shows each thread
  its run's modules, and `builtins.__import__` is replaced, from the first
  re-import template on. Code that checks `type(sys.modules) is dict` sees
  the difference.
- C code that looks modules up in the interpreter's own table does not see a
  run's modules. The C `pickle` is one: pickling an instance of a class
  defined in a re-imported module fails (`KeyError` or `PicklingError`,
  depending on the Python version). Put such classes in
  a module listed in `imports`, or use `start => fork`.

## What a session sees

| | Sessions of one template |
|---|---|
| Module globals, `sys.modules`, `__main__` | fresh per session |
| `os.environ` | the template's `env` only; nothing from the VM unless `clear_env => false` |
| Working directory | an empty directory per session, removed on close |
| Threads, open files, sockets | none from another session |
| `random` state | reseeded per session |
| Hash seed (`set` and `dict` of `str` order) | the same for every session: `hash_seed`, or one chosen at build time |
| Imports and preload globals | the same starting state for every session |

`print` in a session goes to the Erlang logger, tagged with the session's
context.

## Bound each session

```erlang
{ok, T} = py_session:template(#{
    imports => [orders],
    rlimits => #{as => 512 * 1024 * 1024, cpu => 10, nofile => 256},
    kill_after => 1000
}).
```

The limits apply to every session, as for isolated contexts (see
[Isolated Contexts](isolated.md)). A timeout on a call interrupts it;
`py_context:kill/1` ends the session's process at once.

## When a session dies

```erlang
{error, {child_exited, {signal, 6}}} = py_context:call(S, orders, crash, []),
{error, {child_exited, {signal, 6}}} = py_context:eval(S, <<"1">>),
ok = py_session:close(S).
```

A session whose process dies is not restarted: every request answers with
the reason until you close it. Other sessions and the template are not
affected. If a zygote dies, the template starts a new one; the sessions it
had forked keep running.

## Refresh after a deploy

```erlang
ok = py_session:refresh(T).
```

New sessions start from the new code; sessions already open keep what they
started with.

## Inspect a template

```erlang
#{start := fork, zygotes := [#{os_pid := _}], sessions := Live, forks := Total} =
    py_session:info(T).
```

## What it costs

Measured with `examples/bench_sessions.erl` on Apple silicon (macOS 27,
Python 3.14, 14 cores), p50 of new + first call + close. Run it on your
machine for your own figures: the cost of a fork grows with the size of
the prepared process.

| How the session starts | Time to a used and closed session |
|---|---|
| `reimport` (one run, worker or owngil context) | 0.25 ms |
| `spawn` with a `warm` pool that keeps up | 0.6 ms |
| `fork` | 3.5 ms |
| `spawn` | 65 ms |
| a plain isolated context, for reference | 60 ms |

| Throughput, 8 callers | |
|---|---|
| `fork`, one zygote | about 650 sessions a second (Linux: about 1,000) |
| `reimport` on four owngil contexts | about 13,500 runs a second |
| `reimport` on four worker contexts (one GIL) | about 3,500 runs a second |

One zygote forks one session at a time; add `zygotes` when sessions are
opened faster than one zygote forks them. A call inside a session costs
what it costs in any isolated context (30 us p50, 70 us for a call that
calls back into the session), since it crosses the same socket. A call in
a re-import run stays in the process.

`examples/bench_sessions_sdks.py` measures the isolation step of Temporal's
workflow sandbox (0.5 ms for a standard-library workflow, 4.6 ms when it
defines a pydantic model, since the module is imported again each run) and
of Restate's SDK (under a microsecond: it does not isolate invocations) on
the same workflow module.

## Limits

- A session is a process boundary, not a security boundary: it runs as the
  node's user and can reach what that user can. See [Security](security.md).
- `erlang.call` and the other `erlang` functions work in a session, not
  while the template prepares: preload code that calls Erlang fails with
  "erlang is not connected".
- On macOS a module that loads Objective-C frameworks cannot be forked
  safely; use `start => spawn` for it.
- All sessions of a template share one hash seed, so they order sets the
  same way. `refresh/1` picks a new one when `hash_seed` is not set.

## See also

- [Isolated Contexts](isolated.md): the options sessions share
- [Security](security.md): what a process boundary does and does not bound
- [Decision 0009](decisions/0009-isolated-sessions.md): why sessions fork from a zygote
