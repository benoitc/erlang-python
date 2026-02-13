# Getting Started

This guide walks you through using erlang_python to execute Python code from Erlang.

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {erlang_python, {git, "https://github.com/yourorg/erlang-python.git", {tag, "v0.1.0"}}}
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
Subsequent calls may go to different workers.

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
%% Start async call
Ref = py:call_async(math, factorial, [1000]).

%% Do other work...

%% Wait for result
{ok, HugeNumber} = py:await(Ref).
```

## Streaming from Generators

Python generators can be streamed efficiently:

```erlang
%% Stream a generator expression
{ok, [0,1,4,9,16]} = py:stream_eval(<<"(x**2 for x in range(5))">>).

%% Stream from a generator function (if defined)
{ok, Chunks} = py:stream(mymodule, generate_data, [arg1, arg2]).
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

## Next Steps

- See [Type Conversion](type-conversion.md) for detailed type mapping
- See [Streaming](streaming.md) for working with generators
- See [Memory Management](memory.md) for GC and debugging
