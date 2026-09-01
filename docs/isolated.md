# Isolated Contexts

This guide covers `isolated` mode, where a context's CPython interpreter runs
in a child OS process instead of inside the BEAM. You need it when Python
code must not be able to take the node down or run forever: user-supplied
scripts, C extensions you do not control, work that needs a hard time or
memory bound. The public API is the one you already use with `worker` and
`owngil`; you switch by configuration.

## What the three modes guarantee

| | `worker` | `owngil` | `isolated` |
|---|---|---|---|
| Where Python runs | BEAM process, one pthread per context | BEAM process, one pthread and one interpreter per context | Child OS process per context |
| Interrupt a Python loop | yes (`KeyboardInterrupt` at the next bytecode) | yes | yes |
| Interrupt a blocking C call (`time.sleep`, socket read, numpy kernel) | no, only when the call returns | no | yes (signal), then `SIGKILL` |
| Hard bound on a call | no: a stuck call keeps its thread until it returns | no | yes: `kill_after` then `SIGKILL` |
| Memory cap | no | obmalloc accounting, C extensions not counted | `RLIMIT_AS` (Linux, FreeBSD), RSS watchdog (macOS), cgroups v2 (Linux) |
| CPU bound | no | no | `RLIMIT_CPU` |
| Segfault in a C extension | kills the node | kills the node | kills the child, caller gets `{error, {child_exited, {signal, 11}}}` |
| Python state after a crash | n/a | n/a | lost; the child restarts and the context stays usable |
| Call latency (`eval("1+1")`, p50, same machine) | 16 us | ~20 us | 25 us |
| Memory per context | shared interpreter | one interpreter | one process, ~16 MB RSS bare |
| Startup | microseconds | milliseconds | ~40 ms |
| Zero-copy `py_buffer`, channels, `erlang.schedule`, object refs | yes | yes | no (see Limits) |

## Start a context

```erlang
{ok, Ctx} = py_context:new(#{mode => isolated}),
{ok, 4} = py_context:eval(Ctx, <<"2+2">>),
{ok, 4.0} = py_context:call(Ctx, math, sqrt, [16]),
ok = py_context:exec(Ctx, <<"x = 41">>),
{ok, 42} = py_context:eval(Ctx, <<"x + 1">>),
ok = py_context:stop(Ctx).
```

A pool works the same way, and `py:call/4` routes to it:

```erlang
{ok, _} = py_context_router:start_pool(sandbox, 4, isolated),
{ok, 4.0} = py:call(sandbox, math, sqrt, [16]).
```

Options of `py_context:new/1` specific to this mode:

| Option | Default | Meaning |
|---|---|---|
| `python` | interpreter matching the embedded runtime (`py:python_executable/0`), or `isolated_python` app env | Executable to run |
| `rlimits` | `#{}` | `#{as => Bytes, cpu => Seconds, nofile => N}`, applied with `setrlimit` before any user code |
| `cgroup` | none | Path of a cgroup v2 directory the child joins (limits written by you: `memory.max`, `cpu.max`, `pids.max`) |
| `env` | `#{}` | Extra environment variables for the child |
| `paths` | `[]` | Extra `sys.path` entries (registered `py_import` paths and imports are applied too) |
| `preload` | none | Code run once in the child before anything else |
| `kill_after` | `1000` | Milliseconds between a soft interrupt and `SIGKILL` |
| `restart` | `true` | Start a fresh child when the current one dies |
| `max_restarts`, `restart_period` | `5`, `10000` | Restart budget; past it the context process exits with the child's reason |
| `start_timeout` | `10000` | Milliseconds allowed for the child to connect |

## Cancel work that ignores the embedded modes

`py_context:interrupt/1` sends the child a signal that raises
`KeyboardInterrupt` in the running request, inside a blocking C call too. If
the request has not returned after `kill_after`, the child is killed:

```erlang
{ok, Ctx} = py_context:new(#{mode => isolated, kill_after => 500}),
Self = self(),
spawn(fun() -> Self ! {done, py_context:eval(Ctx, <<"__import__('time').sleep(60)">>)} end),
timer:sleep(100),
ok = py_context:interrupt(Ctx),
receive {done, {error, interrupted}} -> ok end.
```

A timeout targets only the request that timed out: if the child is
executing it, the child is interrupted (and killed after `kill_after` if the
interrupt is not honoured); if it is still queued behind other callers'
requests, it is dropped from the queue and nobody else is interrupted. This
differs from the embedded modes, where an interrupt can only hit whatever
runs. `py_context:interrupt/1` remains context-wide: it interrupts the
request executing now. `py_context:kill/1` skips the soft step:

```erlang
ok = py_context:kill(Ctx),
%% A fresh child is already serving; the Python state is gone
{ok, 4} = py_context:eval(Ctx, <<"2+2">>).
```

In-flight calls return `{error, interrupted}` (soft) or `{error, killed}`
(hard).

## Bound memory and CPU

```erlang
{ok, Ctx} = py_context:new(#{mode => isolated,
                             rlimits => #{as => 512 * 1024 * 1024,
                                          cpu => 30,
                                          nofile => 256}}).
```

Past `as`, allocations fail with `MemoryError` in the child (or the child
dies if it cannot cope); past `cpu`, the child dies with `SIGXCPU` and the
caller gets `{error, {child_exited, {signal, 24}}}`. rlimits are POSIX and
are the portable bound: Linux and FreeBSD enforce all three in the kernel.
macOS ignores `RLIMIT_AS`, so there the child enforces `as` itself: a
watchdog thread polls its resident set every 50 ms and exits when it passes
the limit, and the caller gets `{error, {child_exited, {memory_limit, Bytes}}}`.
`cpu` and `nofile` are kernel-enforced on all three. A free-threaded
CPython build reserves a large virtual range at startup, so its `as` limit
must be well above what a regular build needs (several GB).

With cgroups v2 (Linux only), create the group and write the limits, then
hand the directory to the context. On any other platform the option is
refused before a child is spawned, with `{error, {cgroup_unsupported, Os}}`:

```sh
mkdir /sys/fs/cgroup/py_sandbox
echo 268435456 > /sys/fs/cgroup/py_sandbox/memory.max
echo "50000 100000" > /sys/fs/cgroup/py_sandbox/cpu.max
echo 64 > /sys/fs/cgroup/py_sandbox/pids.max
```

```erlang
{ok, Ctx} = py_context:new(#{mode => isolated, cgroup => "/sys/fs/cgroup/py_sandbox"}).
```

The child joins the group before running any user code; if it cannot, the
start fails with `{error, {startup_error, [{cgroup, Reason}]}}`.

## Survive a crash

```erlang
{ok, Ctx} = py_context:new(#{mode => isolated}),
{error, {child_exited, {signal, 11}}} =
    py_context:eval(Ctx, <<"__import__('ctypes').memset(0, 0, 1)">>),
{ok, 4} = py_context:eval(Ctx, <<"2+2">>).
```

The node, every other context and the context process itself are unaffected.
With `restart => false` the context process exits with
`{child_exited, Reason}` instead, so a supervisor of yours decides.

## Call Erlang from the child

`erlang.call`, `erlang.send`, `erlang.whereis`, `erlang.Pid`, `erlang.Atom`
work as in the embedded modes, from any Python thread, and callbacks nest: an
Erlang function called from Python may call back into the same context, from
the process running the callback. The context serves one caller at a time,
in order; a nested call from the callback process goes through at once,
while a call from any other process waits for the current request to finish
(so a callback that hands the nested call to another process and waits for
it would wait until its own timeout).

```erlang
py:register_function(double, fun([X]) -> X * 2 end),
{ok, 84} = py_context:eval(Ctx, <<"__import__('erlang').call('double', 42)">>).
```

```python
import erlang

def notify(pid):
    erlang.send(pid, ('progress', 50))   # raises erlang.ProcessError if pid is dead
```

Round trips cross a Unix socket as external term format; the type mapping is
the one in [Type Conversion](type-conversion.md). One difference: integers
beyond 64 bits arrive intact (the NIF converter has no bignum path).

## asyncio and worker loops

The child runs a plain `asyncio` loop. A call that returns a coroutine is
awaited and its value returned:

```python
async def fetch(n):
    await asyncio.sleep(0.1)
    return n * 2
```

```erlang
{ok, 4} = py_context:call(Ctx, myapp, fetch, [2]).
```

The worker loop API of [Worker Loops](workers.md) works unchanged
(`start_loop`, `submit`, `submit_await`, `stop_loop`), and a wedged loop is
killed by `stop_loop/2` after its grace period. To serve on a socket Erlang
owns, hand the fd over with `py_context:pass_fd/2`; it crosses the control
socket with `SCM_RIGHTS`:

```erlang
{ok, LSock} = gen_tcp:listen(8080, [binary, {active, false}]),
{ok, Fd} = inet:getfd(LSock),
[begin
     {ok, Ctx} = py_context:new(#{mode => isolated}),
     ok = py_context:start_loop(Ctx),
     {ok, ChildFd} = py_context:pass_fd(Ctx, Fd),
     {ok, _} = py_context:submit_await(Ctx, myapp, serve, [ChildFd])
 end || _ <- lists:seq(1, 4)].
```

Inside a coroutine, `await erlang.async_call(name, *args)` keeps the loop
running while Erlang answers.

## Bulk data with shared memory

Arguments and results cross the socket as a copy. For large payloads use a
shared region: a file mapped `MAP_SHARED` on both sides through
[iommap](https://hex.pm/packages/iommap). Add it to your deps:

```erlang
{deps, [{iommap, "1.1.3"}]}.
```

A region is a fixed-size handle you pass like any other argument, in any
context mode:

```erlang
{ok, Shm} = py_shm:new(64 * 1024 * 1024),
ok = py_shm:write(Shm, 0, Floats),                       %% one copy
{ok, Sum} = py_context:call(Ctx, myapp, sum_floats, [Shm]),
Out = py_shm:binary(Shm, 0, 1024),                       %% no copy
ok = py_shm:close(Shm).
```

```python
import numpy

def sum_floats(shm):                        # erlang.SharedMemory
    a = numpy.frombuffer(shm.buffer, dtype=numpy.float32)   # no copy
    a[:1024] = 0                            # Erlang sees it
    return float(a.sum())
```

Python-produced data is zero-copy in both directions (write into the
region, read it in Erlang with `py_shm:binary/3`); Erlang-produced data
costs one `write/3` copy instead of encode, socket, decode and copy. A
region is mapped once per interpreter and reused across calls; it is
closed by `close/1` or when its owner process exits.

Streaming bodies use the same mechanism through `py_buffer`:

```erlang
{ok, Buf} = py_buffer:new(#{shared => true}),            %% 4 MB ring
ok = py_buffer:write(Buf, Chunk),                        %% blocks when full
ok = py_buffer:close(Buf),
py_context:call(Ctx, myapp, handle, [#{<<"wsgi.input">> => Buf}]).
```

The Python side gets `erlang.SharedBuffer` with the `read`, `readline`,
`readlines`, iteration and `read_nonblock` of the native buffer. Flow
control is a callback round trip per blocking read, so in an embedded
context the native `py_buffer:new/0,1` is still the cheaper choice; use
`shared => true` when the buffer may reach an isolated context or a pool
mixing modes. A native buffer cannot cross into a child.

Rules: regions are never resized (a truncated file would be a `SIGBUS`, so
the size is checked when mapping); mapped pages count against the child's
`as` limit and its resident set; `/dev/shm` is used when present, else a
private directory under `TMPDIR`; while a call holds a handle the child
owns the region, and a concurrent `write/3` is a caller error.

What sharing changes about isolation: the child can still only crash or
exhaust itself, but it can write anything into a region it holds, at any
time, and `py_shm:binary/3` sees those bytes (a binary that changes under
you; take it once the callee is done, or copy with `read/3`). Hand the child
a read-only handle when it only needs to read: `py_shm:new(Size, #{writable => false})`
or `py_shm:read_only(Shm)` map it `PROT_READ` in Python, and Erlang keeps
writing. A child that runs as your user could still truncate the region
file on purpose; sealing and syscall filtering are separate hardening work.

## Process model

- One child per context, started with `open_port` so the VM reaps it and
  reports its exit status: no zombies, and a child that dies before
  connecting is reported with its output (`{error, {child_exited_at_start, Reason, Output}}`).
- The context process is a `gen_statem` (`py_isolated`) with states
  `idle`, `{busy, RequestId}`, `looping`, `stopping_loop` and
  `{restarting, Reason}`; `sys:get_state(Ctx)` shows what it is doing and
  `sys:trace(Ctx, true)` prints its events. It serves one caller at a time,
  in order; a request that arrives while the child restarts waits for the
  new child instead of failing. It outlives the process that created it,
  and stops if that process crashes.
- The child and the context process talk over a Unix socket in a private
  directory, framed exactly like the embedded callback pipe
  (`<<Id:64/native, Len:32/native, Body>>`, body `<<Status, ETF>>`).
- A reader thread in the child owns the socket. It delivers requests to the
  main thread, routes replies to whichever thread is waiting, handles
  `interrupt` by signalling the main thread, and exits the process on EOF.
  So when the BEAM dies, every child exits, even one stuck in a C call; on
  Linux (`prctl(PR_SET_PDEATHSIG)`) and FreeBSD (`procctl(PROC_PDEATHSIG_CTL)`)
  the kernel delivers `SIGKILL` for the same case.
- When the socket breaks or the child exits, pending calls fail with
  `{error, {child_exited, Reason}}`, new calls fail the same way until the
  restart has happened (which takes about 100 ms), and nothing hangs.
- Child stdout and stderr are forwarded to the Erlang logger, one line per
  message, tagged with the context id and OS pid. `py_context:child_info/1`
  returns `os_pid`, `python_version`, `executable` and `platform`.

## Limits

- Python object references cannot cross a process boundary:
  `py_context:call_method/4` returns `{error, not_supported_in_isolated}`,
  results are always converted to terms, and process-local environments
  (`py:call(Ctx, ...)` per Erlang process) map to the child's single
  namespace.
- `erlang.schedule*`, channels (`py_channel`, `py_byte_channel`),
  `py_buffer`, shared dicts and the reactor need the embedded interpreter and
  raise `RuntimeError("... not available in isolated mode")`.
- `py_context:loop_ref/1` returns `{error, not_supported_in_isolated}`;
  `submit/4` without a running loop returns `{error, no_loop}` (there is no
  event worker to step an idle loop).
- `erlang.call` from inside a coroutine blocks the loop, as in the embedded
  modes; use `erlang.async_call`.
- Without `caps` the child holds every authority the user running the node
  holds: it reads and writes what that user can, dials anywhere and can
  spawn processes.
- The child decodes terms with the same rules as the NIF, so atoms sent from
  Python are created in the VM's atom table. Do not let untrusted code mint
  unbounded distinct atoms.
- No syscall filtering: process isolation plus rlimits is the boundary. What
  the child may *reach* (files, addresses, environment) is named with the
  `caps` option, which is a cooperative policy over Python rather than a
  kernel boundary: see [capabilities](capabilities.md).
- Shared memory and `caps` do not combine: a region reaches the child as a
  path, and granting it would grant every region the node owns. A seccomp (Linux) or Capsicum (FreeBSD)
  sandbox is a separate hardening step.
- Each call copies its arguments and result through the socket: a 1 MB
  binary round-trips in about 1.3 ms, 16 MB in about 27 ms (worker mode:
  0.2 ms and 3 ms). For bulk data use shared memory (below).

## See also

- [Interrupts](interrupts.md) for the embedded-mode interrupt semantics
- [Worker Loops](workers.md) for the loop API and serving on Erlang sockets
- [Memory](memory.md) for the owngil memory caps
- [Security](security.md) for the audit-hook sandbox of the embedded modes
