# Worker Loops

This guide covers running a long-lived asyncio event loop inside a Python
context and driving it from Erlang: starting and stopping the loop, injecting
coroutines into it, and serving TCP or UDP on sockets that Erlang owns. You
need it when you want Python servers or background async work to run as
supervised workers inside the VM, the way gunicorn runs worker processes, with
Erlang as the arbiter.

## What a worker loop is

An owngil `py_context` has its own interpreter, its own GIL, its own thread and
its own `ErlangEventLoop`. `py_context:start_loop/1` runs that loop forever on
the context thread. From then on:

- `py_context:submit/4,5` schedules a coroutine or a plain function on the loop
  and returns a task reference; the result arrives as `{async_result, TaskRef,
  {ok, Value} | {error, Reason}}` (use `py_event_loop:await/1,2` or
  `submit_await/4,5,6`).
- fds registered by the loop (servers, connections, channels) are served by
  the loop thread; readiness comes through the context's own `py_event_worker`
  process.
- `py_context:stop_loop/1,2` stops it from inside, or interrupts the thread if
  it does not exit within the grace period. The owner receives
  `{py_loop_exit, Ctx, Result}` when the loop ends, for any reason.

Worker contexts get the same API on the shared main interpreter loop, which
allows one running `ErlangEventLoop` per interpreter: use owngil (Python
3.14+) for several workers.

## Serve TCP on a socket Erlang owns

Bind once in Erlang, duplicate the listen fd for each worker with
`py:dup_fd/1`, and let each worker accept on its copy.

```python
# myapp.py, importable by the workers
import asyncio
import erlang

class Echo(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)

async def serve(listen_fd):
    server = await erlang.server.serve(listen_fd, Echo)
    return 'serving'
```

```erlang
{ok, LSock} = gen_tcp:listen(8000, [binary, {reuseaddr, true}, {backlog, 1024}]),
{ok, LFd} = inet:getfd(LSock),

Workers = [begin
    {ok, W} = py_context:new(#{mode => owngil, preload => <<"import myapp">>}),
    ok = py_context:start_loop(W),
    {ok, Dup} = py:dup_fd(LFd),
    {ok, <<"serving">>} = py_context:submit_await(W, myapp, serve, [Dup]),
    W
end || _ <- lists:seq(1, 4)].
```

Every worker now accepts on the same socket; the kernel hands each connection
to one of them. Erlang keeps the listen socket open across worker restarts, so
replacing a worker never closes the port.

For UDP, open the socket with `gen_udp`, dup its fd the same way and call
`erlang.server.serve(fd, MyDatagramProtocol, udp=True)`; on Linux use one
`SO_REUSEPORT` socket per worker instead of one shared fd, so the kernel
spreads flows.

## Hand over one connection at a time

When Erlang wants to decide per connection (routing, tenancy, or keeping some
connections in Erlang), accept in Erlang and adopt the fd in the worker.

```python
async def adopt(fd):
    await erlang.server.adopt(fd, Echo)
    return 'adopted'
```

```erlang
{ok, Conn} = gen_tcp:accept(LSock),
{ok, Fd} = inet:getfd(Conn),
{ok, Dup} = py:dup_fd(Fd),
{ok, <<"adopted">>} = py_context:submit_await(Worker, myapp, adopt, [Dup]),
gen_tcp:close(Conn).   %% Python owns the dup, Erlang drops its copy
```

## Inject work into a running loop

`submit` targets module level functions (`Module:Func`), imported in the
worker (put entry points in a module, or register one in `sys.modules` from
`preload`). Coroutine functions are awaited, plain functions are called.

```erlang
{ok, Ref} = py_context:submit(W, myapp, refresh_cache, [Key]),
%% ... other work ...
{ok, Result} = py_event_loop:await(Ref, 5000).

%% or in one step
{ok, Result} = py_context:submit_await(W, myapp, refresh_cache, [Key], #{}, 5000).
```

Task start failures come back as errors, not silence: `{error,
function_not_found}` for a missing module or function, `{error,
args_conversion_failed}`, or the Python exception if the call itself raised.

## Control from Erlang without submit

A `py_channel` awaited inside the loop delivers Erlang messages to the running
loop with no polling; use it as the control plane of a worker (adopt this fd,
drain, report stats):

```python
async def control(channel_ref):
    ch = erlang.Channel(channel_ref)
    async for msg in ch:
        match msg:
            case ('adopt', fd):
                await erlang.server.adopt(fd, Echo)
            case ('stop',):
                return 'stopped'
```

```erlang
{ok, Ch} = py_channel:new(),
{ok, _} = py_context:submit(W, myapp, control, [Ch]),
ok = py_channel:send(Ch, {adopt, Dup}).
```

## Stop, restart, supervise

```erlang
ok = py_context:stop_loop(W, 5000),           %% cooperative, interrupt after 5 s
receive {py_loop_exit, W, Result} -> Result end,
ok = py_context:start_loop(W).                %% same context, fresh loop
```

- `stop_loop` returns `ok` once the loop has exited, `{error, no_loop}` when
  none runs, `{error, timeout}` if it survived the interrupt.
- `py_context:interrupt/1` ends the loop at once with `{py_loop_exit, W,
  {error, interrupted}}`; a loop blocked in a C call (`time.sleep`, a numpy
  kernel) exits when that call returns.
- `py_context:stop/1` on a looping context interrupts the loop first, then
  destroys the context.
- If the owner process (the caller of `start_loop`, or `#{owner => Pid}`) dies,
  the loop is stopped.
- Put workers under your own supervisor. `py_context` processes are
  `temporary` under `py_context_sup`; a restart is `py_context:new/1` again,
  `start_loop`, and a new dup of the listen fd. Use `memory_limit` in
  `py_context:new/1` to cap a worker, and `py_nif:context_memory_usage/1` to
  watch it.

## Rules

- While a loop runs, `py_context:call/eval/exec/call_method` on that context
  return `{error, loop_running}`. The thread is busy in the loop, and a call
  that timed out would interrupt it. Use `submit`.
- Pass fds you may close: `py:dup_fd/1` copies, `serve` and `adopt` wrap the
  fd in a socket that owns it and close it when done. Never hand the original
  fd of a live `gen_tcp` socket.
- One running `ErlangEventLoop` per interpreter: worker mode supports one
  worker loop, owngil one per context.
- Modules used by `submit` must be importable in the worker; the `exec`
  namespace of the context is not searched.

## Numbers

`examples/bench_worker_loop.erl` (Apple M4 Pro, OTP 29, Python 3.14, gen_tcp
clients in the same VM):

| Measure | Result |
|---|---|
| connect + echo + close, one worker | about 15 000 conn/s |
| keep-alive echo, one worker, 50 connections | about 70 000 req/s |
| connect + echo + close, 2 to 8 workers on one listen fd | about 23 000 conn/s (client bound) |
| `submit_await` coroutine, one caller | about 25 us round trip |
| `submit_await` coroutine, 100 callers | about 4 us per op, 250 000 ops/s |
| `py_context:call` on an idle owngil context | about 12 us |
| Erlang accept + adopt vs Python accept | 13 500 vs 14 500 conn/s |

## See also

- [Asyncio](asyncio.md) for the ErlangEventLoop itself
- [Channels](channel.md) for the control plane
- [Interrupts](interrupts.md) for what an interrupt does to a loop
- [OWN_GIL Internals](owngil_internals.md) for the thread and interpreter model
