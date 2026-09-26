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

Measured with `examples/bench_sessions.erl`, new + first call + close,
p50. Run it on your machine for your own figures: the cost of a fork grows
with the size of the prepared process.

| How the session starts | macOS 27, Python 3.14 | Linux (container), Python 3.11 |
|---|---|---|
| `fork` | ~4 ms | ~4 ms |
| `spawn` with a `warm` pool that keeps up | ~2 ms | ~2 ms |
| `spawn` | ~60 ms | ~45 ms |
| a plain isolated context, for reference | ~60 ms | ~40 ms |

One zygote forks one session at a time. On Linux it served about 1,000
sessions a second with 8 or more callers; add `zygotes` when sessions are
opened faster than one zygote forks them.

A call inside a session costs what it costs in any isolated context, since
it crosses the same socket. `examples/bench_sessions_sdks.py` measures the
isolation step of Temporal's workflow sandbox and Restate's SDK on the same
workflow module.

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
