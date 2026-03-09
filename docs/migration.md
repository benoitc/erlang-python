# Migration Guide: v1.8.x to v2.0

This guide covers breaking changes and migration steps when upgrading from erlang_python v1.8.x to v2.0.

## Quick Checklist

- [ ] Rename `py:call_async` → `py:cast`
- [ ] Replace `py:bind`/`py:unbind` with `py_context_router`
- [ ] Replace `py:ctx_*` functions with `py_context:*`
- [ ] Replace `erlang_asyncio` imports with `erlang`
- [ ] Replace `erlang_asyncio.run()` with `erlang.run()`
- [ ] Replace subprocess calls with Erlang ports
- [ ] Move signal handlers to Erlang level
- [ ] Review any `os.fork`/`os.exec` usage
- [ ] Update code relying on shared state between contexts (now isolated)

## Architecture Changes

### OWN_GIL Subinterpreter Thread Pool (Python 3.12+)

The most significant change in v2.0 is the new execution model. On Python 3.12+, erlang_python now uses **OWN_GIL subinterpreters** for true parallelism:

**v1.8.x Architecture:**
- Single Python interpreter with shared GIL
- Worker pool with round-robin dispatch
- All workers share global state

**v2.0 Architecture:**
- N subinterpreters, each in its own thread with its own GIL
- Each subinterpreter has isolated state (no shared globals)
- True parallel execution without GIL contention
- 25-30% faster cast operations

**Impact on your code:**

1. **Isolated namespaces**: Variables defined in one context are not visible in others
   ```erlang
   %% v1.8.x - this worked (shared namespace)
   py:exec(<<"x = 42">>),
   {ok, 42} = py:eval(<<"x">>).  %% Might go to different worker

   %% v2.0 - use explicit context for shared state
   Ctx = py:context(1),
   py:exec(Ctx, <<"x = 42">>),
   {ok, 42} = py:eval(Ctx, <<"x">>).  %% Same context
   ```

2. **Module imports are per-context**: Each subinterpreter loads modules independently
   ```erlang
   %% Import in one context doesn't affect others
   Ctx1 = py:context(1),
   Ctx2 = py:context(2),
   py:exec(Ctx1, <<"import mymodule">>),
   %% Ctx2 does NOT have mymodule imported
   ```

3. **Use Shared State API for cross-context data**:
   ```python
   from erlang import state_set, state_get
   state_set("config", {"key": "value"})  # Available to all contexts
   ```

### Execution Mode Detection

Check which mode is active:

```erlang
%% Check execution mode
py:execution_mode().
%% => subinterp     (Python 3.12+ with OWN_GIL)
%% => free_threaded (Python 3.13t with --disable-gil)
%% => multi_executor (Python < 3.12)

%% Check if subinterpreters are supported
py:subinterp_supported().
%% => true | false
```

## New APIs

### `py:context/1` - Explicit Context Selection

Get a specific context by index for operations that need shared state:

```erlang
%% Get context 1 (1-based indexing)
Ctx = py:context(1),

%% All operations on Ctx share state
ok = py:exec(Ctx, <<"counter = 0">>),
ok = py:exec(Ctx, <<"counter += 1">>),
{ok, 1} = py:eval(Ctx, <<"counter">>).
```

### `py:start_contexts/0` - Initialize Context Pool

Explicitly start the context pool (usually done automatically):

```erlang
{ok, Contexts} = py:start_contexts().
%% Returns list of context PIDs
```

### `py_context_router` - Context Routing

The context router manages context distribution:

```erlang
%% Start with default contexts (one per scheduler)
{ok, Contexts} = py_context_router:start().

%% Start with custom count
{ok, Contexts} = py_context_router:start(#{contexts => 8}).

%% Get context for current scheduler
Ctx = py_context_router:get_context().

%% Get specific context
Ctx = py_context_router:get_context(1).

%% Bind current process to a context
ok = py_context_router:bind_context(Ctx).

%% Unbind
ok = py_context_router:unbind_context().

%% Get pool size
N = py_context_router:num_contexts().
```

## API Changes

### `py:call_async` renamed to `py:cast`

The function for non-blocking Python calls has been renamed to follow gen_server conventions:

**Before (v1.8.x):**
```erlang
Ref = py:call_async(math, factorial, [100]),
{ok, Result} = py:await(Ref).
```

**After (v2.0):**
```erlang
Ref = py:cast(math, factorial, [100]),
{ok, Result} = py:await(Ref).
```

The semantics are identical - only the name changed.

### `erlang_asyncio` module removed

The separate `erlang_asyncio` Python module has been consolidated into the main `erlang` module. Use `erlang.run()` with standard asyncio functions.

**Before (v1.8.x):**
```python
import erlang_asyncio

async def handler():
    await erlang_asyncio.sleep(0.1)
    return "done"

result = erlang_asyncio.run(handler())
```

**After (v2.0):**
```python
import erlang
import asyncio

async def handler():
    await asyncio.sleep(0.1)  # Standard asyncio, uses Erlang timers
    return "done"

result = erlang.run(handler())  # Run with Erlang event loop
```

**Function mapping:**

| v1.8.x | v2.0 |
|--------|------|
| `erlang_asyncio.run(coro)` | `erlang.run(coro)` |
| `erlang_asyncio.sleep(delay)` | `asyncio.sleep(delay)` inside `erlang.run()` |
| `erlang_asyncio.gather(*coros)` | `asyncio.gather(*coros)` inside `erlang.run()` |
| `erlang_asyncio.wait_for(coro, timeout)` | `asyncio.wait_for(coro, timeout)` inside `erlang.run()` |
| `erlang_asyncio.create_task(coro)` | `asyncio.create_task(coro)` inside `erlang.run()` |
| `erlang_asyncio.new_event_loop()` | `erlang.new_event_loop()` |

## Removed Features

### Context Affinity Functions (`bind`/`unbind`)

The process-binding functions have been removed. The new architecture uses `py_context_router` for automatic scheduler-affinity routing.

**Before (v1.8.x):**
```erlang
ok = py:bind(),
ok = py:exec(<<"x = 42">>),
{ok, 42} = py:eval(<<"x">>),
ok = py:unbind().

%% Or with explicit contexts
{ok, Ctx} = py:bind(new),
ok = py:ctx_exec(Ctx, <<"y = 100">>),
{ok, 100} = py:ctx_eval(Ctx, <<"y">>),
ok = py:unbind(Ctx).
```

**After (v2.0) - Use context router:**
```erlang
%% Automatic scheduler-affinity routing (recommended)
{ok, _} = py:call(math, sqrt, [16]).

%% Or explicit context binding via router
Ctx = py_context_router:get_context(),
py_context_router:bind_context(Ctx),
{ok, _} = py:call(math, sqrt, [16]),  %% Uses bound context
py_context_router:unbind_context().

%% For isolated state, use py_context directly
{ok, Contexts} = py_context_router:start(),
Ctx = py_context_router:get_context(1),
ok = py_context:exec(Ctx, <<"x = 42">>),
{ok, 42} = py_context:eval(Ctx, <<"x">>, #{}).
```

**Removed functions:**
- `py:bind/0`, `py:bind/1`
- `py:unbind/0`, `py:unbind/1`
- `py:is_bound/0`
- `py:with_context/1`
- `py:ctx_call/4,5,6`
- `py:ctx_eval/2,3,4`
- `py:ctx_exec/2`

### Subprocess Support

Python subprocess operations (`subprocess.Popen`, `asyncio.create_subprocess_*`, etc.) are no longer available. They are blocked by the audit hook sandbox because `fork()` would corrupt the Erlang VM.

**Before (v1.8.x):**
```python
import subprocess
result = subprocess.run(["ls", "-la"], capture_output=True)
```

**After (v2.0) - Use Erlang ports:**
```erlang
%% Register a shell command helper
py:register_function(run_command, fun([Cmd, Args]) ->
    Port = open_port({spawn_executable, Cmd},
                     [{args, Args}, binary, exit_status, stderr_to_stdout]),
    collect_output(Port, [])
end).

collect_output(Port, Acc) ->
    receive
        {Port, {data, Data}} -> collect_output(Port, [Data | Acc]);
        {Port, {exit_status, Status}} ->
            {Status, iolist_to_binary(lists:reverse(Acc))}
    end.
```

```python
from erlang import run_command
status, output = run_command("/bin/ls", ["-la"])
```

See [Security](security.md) for details on blocked operations.

### Signal Handling

Signal handlers can no longer be registered from Python. The ErlangEventLoop raises `NotImplementedError` for `add_signal_handler` and `remove_signal_handler`.

**Before (v1.8.x):**
```python
import signal
import asyncio

loop = asyncio.get_event_loop()
loop.add_signal_handler(signal.SIGTERM, shutdown_handler)
```

**After (v2.0) - Handle at Erlang level:**
```erlang
%% In your application supervisor or main module
os:set_signal(sigterm, handle),

%% Then in a process that handles system messages
receive
    {signal, sigterm} ->
        %% Graceful shutdown
        application:stop(my_app)
end.
```

## New Features to Consider

### Scheduler-Affinity Context Router

The new `py_context_router` automatically routes Python calls based on scheduler ID, providing better cache locality:

```erlang
%% Automatically uses scheduler-based routing
{ok, Result} = py:call(math, sqrt, [16]).

%% Or explicitly bind a context to a process
Ctx = py_context_router:get_context(),
py_context_router:bind_context(Ctx),
%% All calls from this process now go to Ctx
```

### `erlang.reactor` for Protocol Handling

For building custom servers, the new reactor module provides FD-based protocol handling:

```python
from erlang.reactor import Protocol, serve

class EchoProtocol(Protocol):
    def data_received(self, data):
        self.write(data)
        return "continue"

serve(sock, EchoProtocol)
```

See [Reactor](reactor.md) for full documentation.

### Socket FD Handoff

Pass socket file descriptors directly from Erlang to Python for high-performance I/O.

Use `py:dup_fd/1` to duplicate the fd before handoff. This lets Erlang close its
socket while Python keeps its own copy:

```erlang
%% Erlang: Accept and hand off to reactor
{ok, ClientSock} = gen_tcp:accept(ListenSock),
{ok, Fd} = inet:getfd(ClientSock),

%% Duplicate fd so both sides have independent copies
{ok, DupFd} = py:dup_fd(Fd),
py_reactor_context:handoff(DupFd, #{type => tcp}),

%% Safe to close Erlang's socket
gen_tcp:close(ClientSock).
```

For direct asyncio usage:

```erlang
{ok, Fd} = inet:getfd(ClientSock),
{ok, DupFd} = py:dup_fd(Fd),
Ctx = py:context(1),
py:call(Ctx, my_handler, handle_fd, [DupFd]).
```

```python
# Python: Use fd with asyncio
import socket
sock = socket.socket(fileno=fd)
sock.setblocking(False)
# ... use with asyncio
```

See [Reactor](reactor.md#socket-ownership) for details.

### `erlang.send()` for Fire-and-Forget Messages

Send messages directly to Erlang processes without waiting:

```python
import erlang

# Send to a registered process
erlang.send(("my_server", "node@host"), {"event": "user_login", "user": 123})

# Send to a PID
erlang.send(pid, "hello")
```

## Performance Improvements

The v2.0 release includes significant performance improvements:

| Operation | v1.8.1 | v2.0 | Improvement |
|-----------|--------|------|-------------|
| Sync py:call | 0.011 ms | 0.004 ms | 2.9x faster |
| Sync py:eval | 0.014 ms | 0.007 ms | 2.0x faster |
| Cast (async) | 0.011 ms | 0.004 ms | 2.8x faster |
| Throughput | ~90K/s | ~250K/s | 2.8x higher |

These improvements come from:
- Event-driven async model (no pthread polling)
- Scheduler-affinity routing
- Per-interpreter isolation
- Optimized NIF paths

## Troubleshooting

### "RuntimeError: fork() blocked by sandbox"

You're trying to use subprocess or os.fork(). Use Erlang ports instead. See [Security](security.md).

### "NotImplementedError: Signal handlers not supported"

Signal handling must be done at the Erlang level. See the Signal Handling section above.

### "AttributeError: module 'erlang_asyncio' has no attribute..."

The `erlang_asyncio` module has been removed. Update imports to use `erlang` directly.

### Module not found: `_erlang_impl._loop`

If you see this error with `py:async_call`, ensure the application is fully started:
```erlang
{ok, _} = application:ensure_all_started(erlang_python).
```

### Variables not found across py:exec/py:eval calls

In v2.0 with subinterpreters, each call may go to a different context. Use explicit contexts:

```erlang
%% Wrong - may use different contexts
py:exec(<<"x = 42">>),
{error, _} = py:eval(<<"x">>).  %% x not defined in this context!

%% Right - use explicit context
Ctx = py:context(1),
py:exec(Ctx, <<"x = 42">>),
{ok, 42} = py:eval(Ctx, <<"x">>).
```

### asyncio tests failing with subinterpreters

Some asyncio operations don't work correctly across subinterpreters because each has isolated event loop state. For asyncio-heavy code, either:

1. Use explicit context to keep operations in one subinterpreter
2. Use `erlang.run()` within a single context
3. Check `py:execution_mode()` and adapt accordingly

### C extension not compatible with subinterpreters

Some C extensions don't support subinterpreters. Check for errors like:
```
ImportError: module does not support subinterpreters
```

Options:
1. Use Python < 3.12 (falls back to multi_executor mode)
2. Check if the library has a subinterpreter-compatible version
3. Isolate the library usage to a single context

## Configuration

### Pool Size

Configure the number of contexts in `sys.config`:

```erlang
{erlang_python, [
    {num_contexts, 8}  %% Default: erlang:system_info(schedulers)
]}
```

Or at runtime:
```erlang
py_context_router:start(#{contexts => 8}).
```

The C-level pool supports up to 64 subinterpreters (default pre-allocated: 32).
