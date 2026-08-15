# Interrupting Python Code

This guide covers stopping Python code that is already running: a call that
overruns its timeout, or work you want to cancel early. Without this, an
Erlang-side timeout only stops *waiting*: the Python thread keeps running and
the context stays busy. You need it whenever you run code you do not fully
control, such as user-supplied scripts.

## Timeouts interrupt automatically

Pass a timeout to any `py_context` call. When it expires the Python code is
interrupted and the context is free again:

```erlang
{ok, Ctx} = py_context:new(#{mode => owngil}),
{error, timeout} = py_context:eval(Ctx, <<"while True: pass">>, #{}, 500),
%% The context is immediately reusable
{ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 5000).
```

The caller still gets `{error, timeout}`. The Python side sees a
`KeyboardInterrupt` at the point it was executing.

## Cancelling explicitly

To stop work before its timeout, or work started with `infinity`, call
`py:interrupt/1` from any process:

```erlang
Ctx = py:context(),
spawn(fun() -> py_context:eval(Ctx, <<"while True: pass">>, #{}, infinity) end),
%% ... later, from anywhere ...
ok = py:interrupt(Ctx).
```

The interrupted call returns `{error, interrupted}`. `py:interrupt/1` returns
`not_running` if the context is idle.

## Catching it in Python

The interrupt arrives as `KeyboardInterrupt`, which derives from
`BaseException`, so ordinary handlers do not swallow it:

```python
try:
    do_work()
except Exception:      # does NOT catch the interrupt
    log_failure()
```

Catch it explicitly to clean up, then re-raise:

```python
try:
    do_work()
except KeyboardInterrupt:
    release_resources()
    raise
```

Code that catches `BaseException` and continues will keep running. Destroy the
context to deal with that:

```erlang
ok = py_context:destroy(Ctx).
```

## Limits

- CPython delivers an async exception at the next bytecode boundary. Code
  blocked inside a C call (`time.sleep`, a numpy kernel, a socket read) is
  not interrupted until that call returns. The call still times out on the
  Erlang side; the context becomes usable once the C call finishes.
- An interrupt targets the context, not an individual request. Interrupting a
  context that just finished one call and started another stops the new one.
- `py:call/3,4` and `py:eval/1,2` use `infinity` by default. Pass an explicit
  timeout, or use `py:interrupt/1`, if you need a bound.
- A context running a worker loop (`py_context:start_loop/1`) refuses
  `call/eval/exec` with `{error, loop_running}` for this reason: a timed-out
  call would interrupt the loop. `py_context:interrupt/1` on such a context
  ends the loop with `{py_loop_exit, Ctx, {error, interrupted}}`; use
  `py_context:stop_loop/1,2` for a cooperative stop. See [Worker
  Loops](workers.md).
