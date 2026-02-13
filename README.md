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
- **Timeout support** - Configurable execution timeouts per call
- **Memory monitoring** - Access to Python GC stats and tracemalloc

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

%% Evaluate with local variables
{ok, 25} = py:eval(<<"x * y">>, #{x => 5, y => 5}).

%% Execute statements (defines persist per-worker)
ok = py:exec(<<"
def my_func(x):
    return x * 2
">>).

%% Async calls
Ref = py:call_async(math, factorial, [100]),
%% ... do other work ...
{ok, Result} = py:await(Ref).

%% Streaming from generators
{ok, [0,1,4,9,16]} = py:stream_eval(<<"(x**2 for x in range(5))">>).
```

## API Reference

### Function Calls

```erlang
%% Basic call
{ok, Result} = py:call(Module, Function, Args).
{ok, Result} = py:call(Module, Function, Args, KwArgs).

%% With timeout (milliseconds)
{ok, Result} = py:call(Module, Function, Args, KwArgs, 5000).
{error, timeout} = py:call(slow_module, slow_func, [], #{}, 100).

%% Async call
Ref = py:call_async(Module, Function, Args).
{ok, Result} = py:await(Ref).
{ok, Result} = py:await(Ref, Timeout).
```

### Expression Evaluation

```erlang
%% Simple eval
{ok, 42} = py:eval(<<"21 * 2">>).

%% With local variables
{ok, 100} = py:eval(<<"x * y">>, #{x => 10, y => 10}).

%% With timeout
{ok, 45} = py:eval(<<"sum(range(10))">>, #{}, 5000).
{error, timeout} = py:eval(<<"sum(range(10**9))">>, #{}, 100).
```

### Statement Execution

```erlang
%% Execute Python statements
ok = py:exec(<<"x = 42">>).
ok = py:exec(<<"
def hello(name):
    return f'Hello, {name}!'
">>).
```

### Streaming

```erlang
%% Stream from generator function
{ok, Chunks} = py:stream(Module, GeneratorFunc, Args).
{ok, Chunks} = py:stream(Module, GeneratorFunc, Args, KwArgs).

%% Stream from generator expression
{ok, [0,1,4,9,16]} = py:stream_eval(<<"(x**2 for x in range(5))">>).
{ok, [<<"A">>,<<"B">>,<<"C">>]} = py:stream_eval(<<"(c.upper() for c in 'abc')">>).
```

### Memory and GC

```erlang
%% Get memory statistics
{ok, Stats} = py:memory_stats().
%% Stats contains: gc_stats, gc_count, gc_threshold
%% And if tracemalloc enabled: traced_memory_current, traced_memory_peak

%% Force garbage collection
{ok, Collected} = py:gc().        %% Full collection
{ok, Collected} = py:gc(0).       %% Generation 0 only
{ok, Collected} = py:gc(1).       %% Generations 0 and 1
{ok, Collected} = py:gc(2).       %% Full collection

%% Memory tracing (for debugging)
ok = py:tracemalloc_start().
%% ... do work ...
{ok, Stats} = py:memory_stats().  %% Now includes traced_memory_*
ok = py:tracemalloc_stop().
```

### Info

```erlang
%% Get Python version
{ok, <<"3.12.1 ...">>} = py:version().
```

## Architecture

```
+-------------------------------------------------------------------+
|                        BEAM VM                                     |
|  +------------+    +------------+    +------------+               |
|  | Erlang     |    | Erlang     |    | Erlang     |               |
|  | Process    |    | Process    |    | Process    |               |
|  +-----+------+    +-----+------+    +-----+------+               |
|        |                 |                 |                       |
|        +--------+--------+--------+--------+                       |
|                 v                 v                                |
|              +---------------------------+                         |
|              |    py_pool (gen_server)   |  Round-robin routing    |
|              +-------------+-------------+                         |
|                            |                                       |
+----------------------------+---------------------------------------+
| Dirty IO Scheduler Pool    |                                       |
|  +---------------------+   |   +---------------------+             |
|  | py_worker           |<--+-->| py_worker           |             |
|  | - GIL acquired      |       | - GIL acquired      |             |
|  | - Timeout support   |       | - Timeout support   |             |
|  +----------+----------+       +----------+----------+             |
|             |                             |                        |
|             v                             v                        |
|     +-------------+               +-------------+                  |
|     | Python      |               | Python      |                  |
|     | Interpreter |               | Interpreter |                  |
|     +-------------+               +-------------+                  |
+--------------------------------------------------------------------+
```

## Type Mappings

### Erlang to Python

| Erlang | Python |
|--------|--------|
| `integer()` | `int` |
| `float()` | `float` |
| `binary()` | `str` |
| `atom()` (except special) | `str` |
| `true` / `false` | `True` / `False` |
| `none` / `nil` / `undefined` | `None` |
| `list()` | `list` |
| `tuple()` | `tuple` |
| `map()` | `dict` |

### Python to Erlang

| Python | Erlang |
|--------|--------|
| `int` | `integer()` |
| `float` | `float()` |
| `float('nan')` | `nan` (atom) |
| `float('inf')` | `infinity` (atom) |
| `float('-inf')` | `neg_infinity` (atom) |
| `str` | `binary()` |
| `bytes` | `binary()` |
| `True` / `False` | `true` / `false` |
| `None` | `none` |
| `list` | `list()` |
| `tuple` | `tuple()` |
| `dict` | `map()` |
| generator | `{generator, Ref}` (internal) |

## Configuration

In `sys.config`:

```erlang
[
  {erlang_python, [
    {num_workers, 4}            %% Number of Python workers (default: 4)
  ]}
].
```

## Error Handling

Python exceptions are converted to Erlang error tuples:

```erlang
{error, {'NameError', "name 'x' is not defined"}} = py:eval(<<"x">>).
{error, {'ZeroDivisionError', "division by zero"}} = py:eval(<<"1/0">>).
{error, {'ModuleNotFoundError', "..."}} = py:call(nonexistent, func, []).
{error, timeout} = py:eval(<<"sum(range(10**9))">>, #{}, 100).
```

## License

Apache-2.0
