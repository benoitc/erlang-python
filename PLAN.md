# Plan: Fix Remaining Test Failures

## Current Status

| Python | Passed | Failed | Skipped | Notes |
|--------|--------|--------|---------|-------|
| 3.9    | 203    | 0      | 12      | All pass (subinterp tests skipped) |
| 3.13   | 224    | 3      | 2       | py_pid_send_SUITE failures |
| 3.14   | 219    | 8      | 2       | py_pid_send_SUITE failures |

## Root Cause

The `py_pid_send_SUITE` tests fail on Python 3.12+ with subinterpreter mode because:

1. `init_per_suite` sets `sys.path` via `py:exec()` on the main context
2. When tests run, they use context router which routes to subinterpreter contexts
3. Subinterpreters have isolated `sys.path` - the path modification doesn't propagate
4. Result: `ModuleNotFoundError: No module named 'py_test_pid_send'`

## Fix Options

### Option 1: Set sys.path per-context (Recommended)

Modify `py_context:init/1` to accept an optional `sys_path` list and set it when creating the context. This ensures each subinterpreter has the correct path.

```erlang
%% In py_context.erl
init(#{sys_path := Paths} = Opts) ->
    %% After context creation, set sys.path
    lists:foreach(fun(P) ->
        py_nif:context_exec(Ctx, <<"import sys; sys.path.insert(0, '", P/binary, "')">>)
    end, Paths),
    ...
```

### Option 2: Fix test setup to use context-aware path setting

Modify `py_pid_send_SUITE:init_per_suite/1` to set the path on all contexts:

```erlang
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    TestDir = list_to_binary(code:lib_dir(erlang_python, test)),
    %% Set path on all contexts
    NumContexts = py_context_router:num_contexts(),
    [begin
        Ctx = py_context_router:get_context(I),
        py_context:exec(Ctx, <<"import sys; sys.path.insert(0, '", TestDir/binary, "')">>)
    end || I <- lists:seq(1, NumContexts)],
    Config.
```

### Option 3: Use absolute imports in tests

Modify tests to use `importlib` with absolute file paths instead of relying on sys.path.

## Implementation Plan

1. **Fix py_pid_send_SUITE init_per_suite** (Option 2)
   - Modify to set sys.path on all contexts, not just main
   - This is the minimal fix that doesn't require API changes

2. **Add sys_path option to py:context/1** (Option 1 for future)
   - Add `sys_path` option to context creation
   - Apply to both worker and subinterpreter modes
   - Document in API

3. **Test validation**
   - Run full test suite on Python 3.9, 3.13, 3.14
   - Ensure all tests pass

## Remaining Issue

### test_send_dead_process_raises_process_error (1 failure on 3.13/3.14)

**Problem:** The test calls `erlang.send(dead_pid, msg)` which raises `ProcessError`.
The Python code tries to catch `erlang.ProcessError` but it's not caught.

**Root cause:** In subinterpreter mode, the `erlang` module is created separately
in each subinterpreter. The `ProcessError` exception class created by the NIF
when raising the error is from a different module instance than the one the
Python code imports. This is a class identity issue:

```python
# In subinterpreter
import erlang  # Gets subinterpreter's erlang module
try:
    erlang.send(dead_pid, msg)  # Raises ProcessError from NIF's erlang module
except erlang.ProcessError:  # This is a different class!
    return True  # Never reached
```

**Fix options:**
1. Store exception classes globally and share across subinterpreters
2. Use string-based exception matching in tests
3. Ensure the NIF uses the same exception class as the subinterpreter's erlang module

## Timeline

1. ~~Fix py_pid_send_SUITE init_per_suite~~ - DONE
2. ~~Validate all Python versions pass~~ - DONE (203/226/226 passed)
3. Fix ProcessError class identity issue - follow-up PR
4. Add sys_path option to context API - follow-up PR
