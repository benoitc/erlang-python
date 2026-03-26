# Getting Started

This guide walks you through using erlang_python to execute Python code from Erlang.

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {erlang_python, "3.0.0"}
]}.
```

Or from git:

```erlang
{deps, [
    {erlang_python, {git, "https://github.com/benoitc/erlang-python.git", {branch, "main"}}}
]}.
```

## Starting the Application

```erlang
1> application:ensure_all_started(erlang_python).
{ok, [erlang_python]}
```

The application starts a pool of Python worker processes that handle requests.

## Basic Usage

### Calling Python Functions

```erlang
%% Call math.sqrt(16)
{ok, 4.0} = py:call(math, sqrt, [16]).

%% Call json.dumps with keyword arguments
{ok, Json} = py:call(json, dumps, [#{name => <<"Alice">>}], #{indent => 2}).
```

### Evaluating Expressions

```erlang
%% Simple arithmetic
{ok, 42} = py:eval(<<"21 * 2">>).

%% Using Python built-ins
{ok, 45} = py:eval(<<"sum(range(10))">>).

%% With local variables
{ok, 100} = py:eval(<<"x * y">>, #{x => 10, y => 10}).

%% Note: Python locals aren't accessible in nested scopes (lambda/comprehensions).
%% Use default arguments to capture values:
{ok, [2, 4, 6]} = py:eval(<<"list(map(lambda x, m=multiplier: x * m, items))">>,
                          #{items => [1, 2, 3], multiplier => 2}).
```

### Executing Statements

Use `py:exec/1` to execute Python statements:

```erlang
ok = py:exec(<<"
import random

def roll_dice(sides=6):
    return random.randint(1, sides)
">>).
```

Note: Definitions made with `exec` are local to the worker that executes them.
Subsequent calls may go to different workers. Use [Shared State](#shared-state) to
share data between workers, or [Context Affinity](#context-affinity) to bind to a
dedicated worker.

## Working with Timeouts

All operations support optional timeouts:

```erlang
%% 5 second timeout
{ok, Result} = py:call(mymodule, slow_func, [], #{}, 5000).

%% Timeout error
{error, timeout} = py:eval(<<"sum(range(10**9))">>, #{}, 100).
```

## Async Calls

For non-blocking operations:

```erlang
%% Start async call (returns ref for await)
Ref = py:spawn_call(math, factorial, [1000]).

%% Do other work...

%% Wait for result
{ok, HugeNumber} = py:await(Ref).

%% Fire-and-forget (no result)
ok = py:cast(some_module, log_event, [EventData]).
```

## Streaming from Generators

Python generators can be streamed efficiently:

```erlang
%% Stream a generator expression
{ok, [0,1,4,9,16]} = py:stream_eval(<<"(x**2 for x in range(5))">>).

%% Stream from a generator function (if defined)
{ok, Chunks} = py:stream(mymodule, generate_data, [arg1, arg2]).
```

## Shared State

Python workers don't share namespace state, but you can share data via the
built-in state API:

```erlang
%% Store from Erlang
py:state_store(<<"config">>, #{api_key => <<"secret">>, timeout => 5000}).

%% Read from Python
ok = py:exec(<<"
from erlang import state_get
config = state_get('config')
print(config['api_key'])
">>).
```

### From Python

```python
from erlang import state_set, state_get, state_delete, state_keys
from erlang import state_incr, state_decr

# Key-value storage
state_set('my_key', {'data': [1, 2, 3]})
value = state_get('my_key')

# Atomic counters (thread-safe)
state_incr('requests')       # +1, returns new value
state_incr('requests', 10)   # +10
state_decr('requests')       # -1

# Management
keys = state_keys()
state_delete('my_key')
```

### From Erlang

```erlang
py:state_store(Key, Value).
{ok, Value} = py:state_fetch(Key).
py:state_remove(Key).
Keys = py:state_keys().

%% Atomic counters
1 = py:state_incr(<<"hits">>).
11 = py:state_incr(<<"hits">>, 10).
10 = py:state_decr(<<"hits">>).
```

## Type Conversions

Values are automatically converted between Erlang and Python:

```erlang
%% Numbers
{ok, 42} = py:eval(<<"42">>).           %% int -> integer
{ok, 3.14} = py:eval(<<"3.14">>).       %% float -> float

%% Strings
{ok, <<"hello">>} = py:eval(<<"'hello'">>).  %% str -> binary

%% Collections
{ok, [1,2,3]} = py:eval(<<"[1,2,3]">>).      %% list -> list
{ok, {1,2,3}} = py:eval(<<"(1,2,3)">>).      %% tuple -> tuple
{ok, #{<<"a">> := 1}} = py:eval(<<"{'a': 1}">>).  %% dict -> map

%% Booleans and None
{ok, true} = py:eval(<<"True">>).
{ok, false} = py:eval(<<"False">>).
{ok, none} = py:eval(<<"None">>).
```

## Context Affinity

By default, each call may go to a different context. To preserve Python state across
calls (variables, imports, objects), use an explicit context:

```erlang
%% Get a specific context
Ctx = py:context(1),

%% State persists across calls to the same context
ok = py:exec(Ctx, <<"counter = 0">>),
ok = py:exec(Ctx, <<"counter += 1">>),
{ok, 1} = py:eval(Ctx, <<"counter">>),

ok = py:exec(Ctx, <<"counter += 1">>),
{ok, 2} = py:eval(Ctx, <<"counter">>).
```

Or bind a context to the current process for automatic routing:

```erlang
Ctx = py_context_router:get_context(),
ok = py_context_router:bind_context(Ctx),
try
    ok = py:exec(<<"x = 10">>),
    {ok, 20} = py:eval(<<"x * 2">>)
after
    py_context_router:unbind_context()
end.
```

See [Context Affinity](context-affinity.md) for explicit contexts and advanced usage.

## Virtual Environments

### Automatic Virtual Environment Setup

Use `py:ensure_venv/2,3` to automatically create and activate a virtual environment:

```erlang
%% Create venv and install from requirements.txt
ok = py:ensure_venv("/path/to/myapp/venv", "requirements.txt").

%% Install from pyproject.toml (editable install)
ok = py:ensure_venv("/path/to/venv", "pyproject.toml").

%% With options: extras, custom installer, or force recreate
ok = py:ensure_venv("/path/to/venv", "pyproject.toml", [
    {extras, ["dev", "test"]},   %% Install optional dependencies
    {installer, uv},             %% Use uv instead of pip (faster)
    {python, "/usr/bin/python3.12"}  %% Specific Python version
]).

%% Force recreate even if venv exists
ok = py:ensure_venv("/path/to/venv", "requirements.txt", [force]).
```

### Manual Virtual Environment Activation

```erlang
%% Activate an existing venv
ok = py:activate_venv(<<"/path/to/venv">>).

%% Check current venv
{ok, #{<<"active">> := true, <<"venv_path">> := Path}} = py:venv_info().

%% Deactivate when done
ok = py:deactivate_venv().
```

## Execution Modes

By default, erlang_python uses worker mode which works with any Python version.

For true parallel Python execution:
- **Free-threaded Python** (3.13t+): Automatic, no special build needed
- **Parallel pool** (Python 3.14+): Build with `CMAKE_OPTIONS="-DENABLE_PARALLEL_PYTHON=ON"`

### Runtime Detection

Check the current execution mode:

```erlang
%% See how Python is being executed
py:execution_mode().
%% => free_threaded | multi_executor

%% Check rate limiting status
py_semaphore:max_concurrent().  %% Maximum concurrent calls
py_semaphore:current().         %% Currently executing
```

See [Scalability](scalability.md) for details on execution modes and performance tuning.

## Logging and Tracing

### Python Logging to Erlang

Forward Python `logging` messages to Erlang's `logger`:

```erlang
%% Configure Python logging
ok = py:configure_logging(#{level => info}).

%% Now Python logs appear in Erlang logger
ok = py:exec(<<"
import logging
logging.info('Hello from Python!')
logging.warning('Something needs attention')
">>).
```

### Distributed Tracing

Collect trace spans from Python code:

```erlang
%% Enable tracing
ok = py:enable_tracing().

%% Run traced Python code
ok = py:exec(<<"
import erlang
with erlang.Span('my-operation', key='value'):
    pass  # your code here
">>).

%% Retrieve spans
{ok, Spans} = py:get_traces().

%% Clean up
ok = py:clear_traces().
ok = py:disable_tracing().
```

See [Logging and Tracing](logging.md) for details on span events, decorators, and error handling.

## Using from Elixir

erlang_python works seamlessly with Elixir. The `:py` module can be called directly:

```elixir
# Start the application
{:ok, _} = Application.ensure_all_started(:erlang_python)

# Call Python functions
{:ok, 4.0} = :py.call(:math, :sqrt, [16])

# Evaluate expressions
{:ok, result} = :py.eval("2 + 2")

# With variables
{:ok, 100} = :py.eval("x * y", %{x: 10, y: 10})

# Call with keyword arguments
{:ok, json} = :py.call(:json, :dumps, [%{name: "Elixir"}], %{indent: 2})
```

### Register Elixir Functions for Python

```elixir
# Register an Elixir function
:py.register_function(:factorial, fn [n] ->
  Enum.reduce(1..n, 1, &*/2)
end)

# Call from Python
{:ok, 3628800} = :py.eval("__import__('erlang').call('factorial', 10)")

# Cleanup
:py.unregister_function(:factorial)
```

### Parallel Processing with BEAM

```elixir
# Register parallel map using BEAM processes
:py.register_function(:parallel_map, fn [func_name, items] ->
  parent = self()

  refs = Enum.map(items, fn item ->
    ref = make_ref()
    spawn(fn ->
      result = apply_function(func_name, item)
      send(parent, {ref, result})
    end)
    ref
  end)

  Enum.map(refs, fn ref ->
    receive do
      {^ref, result} -> result
    after
      5000 -> {:error, :timeout}
    end
  end)
end)
```

### Running the Elixir Example

A complete working example is available:

```bash
elixir --erl "-pa _build/default/lib/erlang_python/ebin" examples/elixir_example.exs
```

This demonstrates basic calls, data conversion, callbacks, parallel processing (10x speedup), and AI integration.

## Using the Erlang Event Loop

For async Python code, use the `erlang` module which provides an Erlang-backed asyncio event loop for better performance:

```python
import erlang
import asyncio

async def my_handler():
    # Uses Erlang's erlang:send_after/3 - no Python event loop overhead
    await asyncio.sleep(0.1)  # 100ms
    return "done"

# Run a coroutine with the Erlang event loop
result = erlang.run(my_handler())

# Standard asyncio functions work seamlessly
async def main():
    results = await asyncio.gather(task1(), task2(), task3())

erlang.run(main())
```

This is especially useful in async handlers where sleep operations are common. See [Asyncio](asyncio.md) for the full API reference.

## Security Considerations

When Python runs inside the Erlang VM, certain operations are blocked for safety:

- **Subprocess operations blocked** - `subprocess.Popen`, `os.fork()`, `os.system()`, etc. would corrupt the Erlang VM
- **Signal handling not supported** - Signal handling should be done at the Erlang level

If you need to run external commands, use Erlang ports (`open_port/2`) instead:

```erlang
%% From Erlang - run a shell command
Port = open_port({spawn, "ls -la"}, [exit_status, binary]),
receive
    {Port, {data, Data}} -> Data;
    {Port, {exit_status, 0}} -> ok
end.
```

See [Security](security.md) for details on blocked operations and recommended alternatives.

## Custom Pool Support

erlang_python lets you create pools on demand to separate CPU-bound and I/O-bound operations:

```erlang
%% Create io pool for I/O-bound operations
{ok, _} = py_context_router:start_pool(io, 10, worker).

%% Register entire modules to io pool
py:register_pool(io, requests).
py:register_pool(io, psycopg2).

%% Or register specific callables
py:register_pool(io, {db, query}).        %% Only db.query goes to io pool

%% Calls automatically route to the right pool
{ok, 4.0} = py:call(math, sqrt, [16]).       %% -> default pool (fast)
{ok, Resp} = py:call(requests, get, [Url]).  %% -> io pool (module registered)
{ok, Rows} = py:call(db, query, [Sql]).      %% -> io pool (callable registered)
```

This prevents slow HTTP requests from blocking quick math operations. See [Pool Support](pools.md) for configuration and advanced usage.

## Zero-Copy Buffers

For streaming data from Erlang to Python, use `py_buffer`:

```erlang
%% Create buffer for streaming data
{ok, Buf} = py_buffer:new(ContentLength),

%% Write chunks as they arrive
ok = py_buffer:write(Buf, Chunk1),
ok = py_buffer:write(Buf, Chunk2),
ok = py_buffer:close(Buf),

%% Pass to Python handler
py:call(Ctx, myapp, handle, [Buf]).
```

Python sees it as a file-like object with blocking reads:

```python
def handle(buf):
    body = buf.read()  # Blocks until data ready
    # Or iterate lines
    for line in buf:
        process(line)
```

See [Buffer API](buffer.md) for zero-copy memoryview access and fast substring search.

## Next Steps

- See [Pool Support](pools.md) for separating CPU and I/O operations
- See [Type Conversion](type-conversion.md) for detailed type mapping
- See [Context Affinity](context-affinity.md) for preserving Python state
- See [Streaming](streaming.md) for working with generators
- See [Memory Management](memory.md) for GC and debugging
- See [Scalability](scalability.md) for parallelism and performance
- See [Logging and Tracing](logging.md) for Python logging and distributed tracing
- See [AI Integration](ai-integration.md) for ML/AI examples
- See [Asyncio Event Loop](asyncio.md) for the Erlang-native asyncio implementation with TCP and UDP support
- See [Buffer API](buffer.md) for zero-copy streaming buffers
- See [Reactor](reactor.md) for FD-based protocol handling
- See [Security](security.md) for sandbox and blocked operations
- See [Distributed Execution](distributed.md) for running Python across Erlang nodes
