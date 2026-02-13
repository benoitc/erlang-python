# erlang_python

Execute Python applications from Erlang using dirty NIFs.

## Overview

This library embeds a Python interpreter into the Erlang VM and provides a
clean API for calling Python functions, evaluating expressions, and streaming
results from generators.

Key features:
- **Dirty NIF execution** - Python code runs on dirty I/O schedulers, not blocking the BEAM
- **GIL-aware** - Releases GIL while waiting for Erlang messages
- **Type conversion** - Automatic conversion between Erlang and Python types
- **Streaming** - Support for Python generators with chunk-by-chunk delivery
- **Worker pool** - Multiple Python workers for concurrent execution

## Requirements

- Erlang/OTP 24+
- Python 3.8+
- C compiler (gcc, clang)

## Building

```bash
rebar3 compile
```

## Quick Start

```erlang
%% Start the application
ok = application:start(erlang_python).

%% Call a Python function
{ok, 4.0} = py:call(math, sqrt, [16]).

%% With keyword arguments
{ok, Json} = py:call(json, dumps, [#{foo => bar}], #{indent => 2}).

%% Evaluate an expression
{ok, 45} = py:eval(<<"sum(range(10))">>).

%% Execute statements
ok = py:exec(<<"
def my_func(x):
    return x * 2
">>).
{ok, 10} = py:call('__main__', my_func, [5]).

%% Async calls
Ref = py:call_async(math, factorial, [100]),
%% ... do other work ...
{ok, Result} = py:await(Ref).

%% Streaming from generators
{ok, Chunks} = py:stream(mymodule, generate_tokens, [Prompt]).
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        BEAM VM                                   │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐             │
│  │ Erlang     │    │ Erlang     │    │ Erlang     │             │
│  │ Process    │    │ Process    │    │ Process    │             │
│  └─────┬──────┘    └─────┬──────┘    └─────┬──────┘             │
│        │                 │                 │                     │
│        └────────────┬────┴────────────────┬┘                     │
│                     ▼                     ▼                      │
│              ┌──────────────────────────────┐                    │
│              │    py_pool (gen_server)      │  Request routing   │
│              └──────────────┬───────────────┘                    │
│                             │                                    │
├─────────────────────────────┼────────────────────────────────────┤
│ Dirty IO Scheduler Pool     │                                    │
│  ┌─────────────────────┐    │    ┌─────────────────────┐         │
│  │ py_worker           │◄───┼───►│ py_worker           │         │
│  │ - GIL acquired      │         │ - GIL acquired      │         │
│  │ - Releases for recv │         │ - Releases for recv │         │
│  └─────────┬───────────┘         └─────────┬───────────┘         │
│            │                               │                     │
│            ▼                               ▼                     │
│     ┌────────────┐                  ┌────────────┐               │
│     │ Python     │                  │ Python     │               │
│     │ Interpreter│                  │ Interpreter│               │
│     └────────────┘                  └────────────┘               │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

In `sys.config`:

```erlang
[
  {erlang_python, [
    {num_workers, 4},           %% Number of Python workers
    {python_path, "/usr/bin/python3"},
    {worker_timeout, 30000}     %% Default timeout in ms
  ]}
].
```

## Type Mappings

| Erlang | Python |
|--------|--------|
| `integer()` | `int` |
| `float()` | `float` |
| `binary()` | `str` |
| `atom()` | `str` |
| `true` / `false` | `True` / `False` |
| `none` / `nil` / `undefined` | `None` |
| `list()` | `list` |
| `tuple()` | `tuple` |
| `map()` | `dict` |

## License

Apache-2.0
