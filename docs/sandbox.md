# Worker Sandbox

Worker sandboxing allows you to create Python workers with restricted capabilities, blocking potentially dangerous operations like file writes, subprocess execution, and network access. This is useful for running untrusted Python code safely.

## Why Sandbox?

When executing Python code from external sources or user input, you may want to restrict what operations the code can perform:

- Prevent file system modifications
- Block subprocess creation and shell commands
- Disable network access
- Restrict dynamic code execution
- Control which modules can be imported

## Quick Start

Create a sandboxed worker using the `strict` preset:

```erlang
%% Create worker with strict sandbox
{ok, Worker} = py_nif:worker_new(#{
    sandbox => #{preset => strict}
}),

%% Safe operations work
ok = py_nif:worker_exec(Worker, <<"result = 1 + 2">>),

%% File writes are blocked
{error, {'PermissionError', _}} =
    py_nif:worker_exec(Worker, <<"open('/tmp/test.txt', 'w')">>),

py_nif:worker_destroy(Worker).
```

## Presets

### Strict Preset

The `strict` preset blocks:
- `subprocess`: `subprocess.Popen`, `os.system`, `os.exec*`, `os.spawn*`, `os.popen`
- `network`: `socket.*` operations
- `ctypes`: ctypes module (memory access)
- `file_write`: `open()` with write/append modes (`w`, `a`, `x`, `+`)

```erlang
{ok, Worker} = py_nif:worker_new(#{
    sandbox => #{preset => strict}
}).
```

## Custom Policies

For fine-grained control, specify exactly which operations to block:

```erlang
{ok, Worker} = py_nif:worker_new(#{
    sandbox => #{
        enabled => true,
        block => [file_write, subprocess, network]
    }
}).
```

### Available Block Flags

| Flag | Blocks |
|------|--------|
| `file_write` | `open()` with write/append modes |
| `file_read` | All file read operations |
| `subprocess` | `subprocess.*`, `os.exec*`, `os.spawn*`, `os.popen` |
| `network` | `socket.*` operations |
| `ctypes` | ctypes module (memory access) |
| `import` | Non-whitelisted imports |
| `exec` | `compile()`, `exec()`, `eval()` |

### Import Whitelist

When blocking imports, you can whitelist specific modules:

```erlang
{ok, Worker} = py_nif:worker_new(#{
    sandbox => #{
        enabled => true,
        block => [import],
        allow_imports => [<<"json">>, <<"math">>, <<"re">>]
    }
}).
```

Note: Import whitelisting is complex because importing a module often triggers additional imports for dependencies. You may need to whitelist more modules than expected.

## Dynamic Policy Control

You can enable, disable, or modify the sandbox policy at runtime:

### Enable/Disable

```erlang
%% Temporarily disable sandbox
ok = py_nif:sandbox_enable(Worker, false),
%% ... run trusted code ...
ok = py_nif:sandbox_enable(Worker, true).
```

### Update Policy

```erlang
%% Change policy at runtime
ok = py_nif:sandbox_set_policy(Worker, #{
    enabled => true,
    block => [file_write, subprocess]
}).
```

### Query Policy

```erlang
{ok, Policy} = py_nif:sandbox_get_policy(Worker).
%% Policy is a map with enabled, block, allow_imports, log_events
```

## High-Level API

For simple one-off sandboxed calls, use `py:call_sandboxed/4,5`:

```erlang
%% Call a Python function with sandbox protection
{ok, Result} = py:call_sandboxed(math, sqrt, [16], #{preset => strict}).

%% With kwargs
{ok, Json} = py:call_sandboxed(json, dumps, [#{a => 1}], #{indent => 2}, #{preset => strict}).
```

This creates a temporary sandboxed worker, executes the call, and destroys the worker.

## Error Handling

When a blocked operation is attempted, Python raises a `PermissionError`:

```erlang
{ok, Worker} = py_nif:worker_new(#{sandbox => #{preset => strict}}),
Result = py_nif:worker_exec(Worker, <<"open('/tmp/test.txt', 'w')">>),
%% Result = {error, {'PermissionError', <<"Sandbox: file_write operations are blocked (event: open)">>}}
```

The error message indicates which operation category was blocked and which audit event triggered it.

## Architecture

The sandbox uses Python's audit hook mechanism (PEP 578):

1. A global C audit hook is registered at Python initialization
2. Each worker has a per-worker sandbox policy
3. The hook checks the current worker's policy for each audit event
4. If the operation is blocked, the hook raises `PermissionError`

This design ensures:
- Zero overhead for workers without sandbox
- Fast path checking using atomic flags
- Thread-safe policy updates

## Limitations

- **Audit event coverage**: Not all Python operations trigger audit events. The sandbox is most reliable for file operations.
- **Python version**: Audit hooks were added in Python 3.8. Some audit events may vary by version.
- **Import dependencies**: Blocking imports can be tricky due to Python's complex import machinery.
- **Not a security boundary**: This sandbox provides defense-in-depth but should not be the only security measure for running untrusted code.

## Examples

### Read-Only Worker

```erlang
{ok, Worker} = py_nif:worker_new(#{
    sandbox => #{
        enabled => true,
        block => [file_write, subprocess, network, ctypes]
    }
}),
%% Can read files
{ok, Data} = py_nif:worker_eval(Worker, <<"open('/etc/hosts').read()">>, #{}),
%% Cannot write files
{error, _} = py_nif:worker_exec(Worker, <<"open('/tmp/out.txt', 'w')">>).
```

### Computation-Only Worker

```erlang
{ok, Worker} = py_nif:worker_new(#{
    sandbox => #{
        enabled => true,
        block => [file_write, file_read, subprocess, network, ctypes]
    }
}),
%% Pure computation works
{ok, Result} = py_nif:worker_eval(Worker, <<"sum([x**2 for x in range(10)])">>, #{}).
```
