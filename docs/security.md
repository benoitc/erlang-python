# Security

This guide covers the security sandbox that protects the Erlang VM when running embedded Python code.

## Overview

When Python runs embedded inside the Erlang VM (BEAM), certain operations must be blocked because they would corrupt or destabilize the runtime. The `erlang_python` library automatically installs a sandbox that blocks these dangerous operations.

### Why Fork/Exec Are Blocked

The Erlang VM is a sophisticated runtime with:
- Multiple scheduler threads managing lightweight processes
- Complex memory management and garbage collection
- Intricate internal state for message passing and I/O

When `fork()` is called, the child process gets a copy of the parent's memory but only the calling thread. This leaves the child with corrupted state - scheduler threads are missing, locks are in inconsistent states, and internal data structures are broken. The child process will crash or behave unpredictably.

Similarly, `exec()` replaces the current process image entirely, terminating the Erlang VM.

## Audit Hook Mechanism

The sandbox uses Python's audit hook system (PEP 578) to intercept dangerous operations at a low level, before they can execute:

```python
# Automatically installed when running inside Erlang
import sys
sys.addaudithook(sandbox_hook)  # Cannot be removed once installed
```

This provides defense-in-depth - even if Python code tries to import `os` or `subprocess` directly, the operations are blocked.

## Blocked Operations

| Operation | Module | Reason |
|-----------|--------|--------|
| `fork()` | `os` | Corrupts Erlang VM state |
| `forkpty()` | `os` | Uses fork internally |
| `system()` | `os` | Executes via shell (uses fork) |
| `popen()` | `os` | Opens pipe to subprocess (uses fork) |
| `exec*()` | `os` | Replaces process image |
| `spawn*()` | `os` | Creates subprocess (uses fork) |
| `posix_spawn*()` | `os` | POSIX subprocess creation |
| `Popen` | `subprocess` | Creates subprocess (uses fork) |
| `run()` | `subprocess` | Wrapper around Popen |
| `call()` | `subprocess` | Wrapper around Popen |

## Error Messages

When blocked operations are attempted, you'll see:

```python
>>> import subprocess
>>> subprocess.run(['ls'])
RuntimeError: subprocess.Popen is blocked in Erlang VM context.
fork()/exec() would corrupt the Erlang runtime.
Use Erlang ports (open_port/2) for subprocess management.
```

```python
>>> import os
>>> os.fork()
RuntimeError: os.fork is blocked in Erlang VM context.
fork()/exec() would corrupt the Erlang runtime.
Use Erlang ports (open_port/2) for subprocess management.
```

## Recommended Alternatives

Instead of using Python's subprocess facilities, use Erlang's port mechanism which properly manages external processes.

### From Erlang: Running Shell Commands

```erlang
%% Run a command and capture output
run_command(Cmd) ->
    Port = open_port({spawn, Cmd}, [exit_status, binary, stderr_to_stdout]),
    collect_output(Port, []).

collect_output(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            collect_output(Port, [Data | Acc]);
        {Port, {exit_status, Status}} ->
            {Status, iolist_to_binary(lists:reverse(Acc))}
    after 30000 ->
        port_close(Port),
        {error, timeout}
    end.

%% Usage
{0, Output} = run_command("ls -la").
```

### From Python: Calling Erlang to Run Commands

Register an Erlang function that runs commands:

```erlang
%% In Erlang
py:register_function(run_shell, fun([Cmd]) ->
    Port = open_port({spawn, binary_to_list(Cmd)},
                     [exit_status, binary, stderr_to_stdout]),
    collect_output(Port, [])
end).
```

```python
# In Python
from erlang import run_shell

# This calls through Erlang, which properly manages the subprocess
result = run_shell("ls -la")
```

### Using Erlang Ports for Long-Running Processes

```erlang
%% Start a long-running process
{ok, Port} = py:call('__main__', start_worker_via_erlang, []),

%% The Python code registers a function:
py:register_function(start_worker_via_erlang, fun([]) ->
    Port = open_port({spawn, "python3 worker.py"},
                     [binary, {line, 1024}, use_stdio]),
    Port  % Return port reference to Python
end).
```

### Alternative: Use `erlang.send()` for Communication

For Python code that needs to trigger external processes, use message passing to coordinate with Erlang supervisors:

```python
import erlang

# Send a request to an Erlang process that manages subprocesses
erlang.send(supervisor_pid, ('spawn_worker', worker_args))
```

## Checking Sandbox Status

From Python, you can check if the sandbox is active:

```python
from erlang._sandbox import is_sandboxed

if is_sandboxed():
    print("Running inside Erlang VM - subprocess operations blocked")
```

## Signal Handling Note

Signal handling is also not supported in the Erlang event loop. The `ErlangEventLoop` raises `NotImplementedError` for `add_signal_handler()` and `remove_signal_handler()`. Signal handling should be done at the Erlang VM level using Erlang's signal handling facilities.

## See Also

- [Getting Started](getting-started.md) - Basic usage guide
- [Asyncio](asyncio.md) - Erlang-native asyncio event loop
- [Threading](threading.md) - Python threading support
