# Streaming

This guide covers working with Python generators from Erlang.

## Overview

Python generators allow processing large datasets or infinite sequences
efficiently by yielding values one at a time. erlang_python supports
two modes of streaming:

1. **Batch streaming** (`py:stream/3,4`, `py:stream_eval/1,2`) - Collects all values into a list
2. **True streaming** (`py:stream_start/3,4`) - Sends events as values are yielded

## True Streaming (Event-driven)

For real-time processing where you need values as they arrive (e.g., LLM tokens,
live data feeds), use `py:stream_start/3,4`:

```erlang
%% Start streaming from a Python iterator
{ok, Ref} = py:stream_start(builtins, iter, [[1,2,3,4,5]]),

%% Receive events as values are yielded
receive_loop(Ref).

receive_loop(Ref) ->
    receive
        {py_stream, Ref, {data, Value}} ->
            io:format("Got: ~p~n", [Value]),
            receive_loop(Ref);
        {py_stream, Ref, done} ->
            io:format("Complete~n");
        {py_stream, Ref, {error, Reason}} ->
            io:format("Error: ~p~n", [Reason])
    after 30000 ->
        timeout
    end.
```

### Events

The stream sends these messages to the owner process:

- `{py_stream, Ref, {data, Value}}` - Each yielded value
- `{py_stream, Ref, done}` - Stream completed successfully
- `{py_stream, Ref, {error, Reason}}` - Stream error

### Options

```erlang
%% Send events to a different process
{ok, Ref} = py:stream_start(Module, Func, Args, #{owner => OtherPid}).
```

### Cancellation

Cancel an active stream:

```erlang
{ok, Ref} = py:stream_start(my_module, long_generator, []),
%% ... receive some values ...
ok = py:stream_cancel(Ref).
%% Stream will stop on next iteration
```

### Async Generators

`stream_start` supports both sync and async generators:

```erlang
%% Async generator (e.g., streaming from an async API)
ok = py:exec(<<"
async def async_gen():
    for i in range(5):
        await asyncio.sleep(0.1)
        yield i
">>),
{ok, Ref} = py:stream_start('__main__', async_gen, []).
```

## Batch Streaming (Collecting All Values)

For simpler use cases where you want all values at once:

### Generator Expressions

```erlang
%% Stream squares of numbers 0-9
{ok, Squares} = py:stream_eval(<<"(x**2 for x in range(10))">>).
%% Squares = [0,1,4,9,16,25,36,49,64,81]

%% Stream uppercase characters
{ok, Upper} = py:stream_eval(<<"(c.upper() for c in 'hello')">>).
%% Upper = [<<"H">>,<<"E">>,<<"L">>,<<"L">>,<<"O">>]

%% Stream filtered values
{ok, Evens} = py:stream_eval(<<"(x for x in range(20) if x % 2 == 0)">>).
%% Evens = [0,2,4,6,8,10,12,14,16,18]
```

### Iterator Objects

Any Python iterator can be streamed:

```erlang
%% Stream from range
{ok, Numbers} = py:stream_eval(<<"iter(range(5))">>).
%% Numbers = [0,1,2,3,4]

%% Stream dictionary items
{ok, Items} = py:stream_eval(<<"iter({'a': 1, 'b': 2}.items())">>).
%% Items = [{<<"a">>, 1}, {<<"b">>, 2}]
```

### Generator Functions

Define generator functions with `yield`:

```erlang
%% Define a generator function
ok = py:exec(<<"
def fibonacci(n):
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b
">>).

%% Stream from it
%% Note: Functions defined with exec are local to the worker that executes them.
%% Subsequent calls may go to different workers in the pool.
{ok, Fib} = py:stream('__main__', fibonacci, [10]).
%% Fib = [0,1,1,2,3,5,8,13,21,34]
```

For reliable inline generators, use lambda with walrus operator (Python 3.8+):

```erlang
%% Fibonacci using inline lambda - works reliably across workers
{ok, Fib} = py:stream_eval(<<"(lambda: ((fib := [0, 1]), [fib.append(fib[-1] + fib[-2]) for _ in range(8)], iter(fib))[-1])()">>).
%% Fib = [0,1,1,2,3,5,8,13,21,34]
```

## When to Use Each Mode

| Use Case | Recommended API |
|----------|-----------------|
| LLM token streaming | `stream_start/3,4` |
| Real-time data feeds | `stream_start/3,4` |
| Live progress updates | `stream_start/3,4` |
| Batch processing | `stream/3,4` or `stream_eval/1,2` |
| Small datasets | `stream/3,4` or `stream_eval/1,2` |
| One-time collection | `stream/3,4` or `stream_eval/1,2` |

## Memory Considerations

- `stream_start`: Low memory - values processed as they arrive
- `stream/stream_eval`: Values collected into a list - memory grows with output size
- Generators are garbage collected after exhaustion

## Use Cases

### LLM Token Streaming

```erlang
%% Stream tokens from an LLM
{ok, Ref} = py:stream_start(llm_client, generate_tokens, [Prompt]),
stream_to_client(Ref, WebSocket).

stream_to_client(Ref, WS) ->
    receive
        {py_stream, Ref, {data, Token}} ->
            websocket:send(WS, Token),
            stream_to_client(Ref, WS);
        {py_stream, Ref, done} ->
            websocket:send(WS, <<"[DONE]">>);
        {py_stream, Ref, {error, _}} ->
            websocket:send(WS, <<"[ERROR]">>)
    end.
```

### Data Processing Pipelines

```erlang
%% Process file lines (if defined in Python)
{ok, Lines} = py:stream(mymodule, read_lines, [<<"data.txt">>]).

%% Transform each line
Results = [process_line(L) || L <- Lines].
```

### Batch Processing

```erlang
%% Process in batches
ok = py:exec(<<"
def batches(data, size):
    for i in range(0, len(data), size):
        yield data[i:i+size]
">>).
```
