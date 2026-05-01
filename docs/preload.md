# Preload Code

This guide covers preloading Python code that executes during interpreter initialization.

## Overview

The `py_preload` module allows you to define Python code that runs once per interpreter at creation time. The resulting globals (imports, functions, variables) become available to all process-local environments.

## Use Cases

- Share common imports across all contexts
- Define utility functions used throughout your application
- Set up configuration or constants
- Avoid repeated initialization overhead

## API

| Function | Description |
|----------|-------------|
| `set_code/1` | Set preload code (binary or iolist) |
| `get_code/0` | Get current preload code or `undefined` |
| `clear_code/0` | Remove preload code |
| `has_preload/0` | Check if preload is configured |

## Basic Usage

```erlang
%% Set preload code at application startup
py_preload:set_code(<<"
import json
import os

def shared_helper(x):
    return x * 2

CONFIG = {'debug': True, 'version': '1.0'}
">>).

%% Create a context - preload is automatically applied
{ok, Ctx} = py_context:new(#{mode => worker}).

%% Use preloaded imports
{ok, <<"{\"a\": 1}">>} = py:eval(Ctx, <<"json.dumps({'a': 1})">>).

%% Use preloaded functions
{ok, 10} = py:eval(Ctx, <<"shared_helper(5)">>).

%% Use preloaded variables
{ok, true} = py:eval(Ctx, <<"CONFIG['debug']">>).
```

## Execution Flow

```
Interpreter/Context Creation
    │
    ▼
apply_registered_paths()     ← sys.path updates
    │
    ▼
apply_registered_imports()   ← module imports
    │
    ▼
apply_preload()              ← preload code execution
    │
    ▼
(interpreter ready)
    │
    ▼
create_local_env()           ← copies from interpreter globals
```

## Process Isolation

Each process-local environment gets an isolated copy of preloaded globals:

```erlang
py_preload:set_code(<<"COUNTER = 0">>).

{ok, Ctx1} = py_context:new(#{mode => worker}).
{ok, Ctx2} = py_context:new(#{mode => worker}).

%% Modify in Ctx1
ok = py:exec(Ctx1, <<"COUNTER = 100">>).
{ok, 100} = py:eval(Ctx1, <<"COUNTER">>).

%% Ctx2 still has original value
{ok, 0} = py:eval(Ctx2, <<"COUNTER">>).
```

## Clearing Preload

Clearing preload only affects new contexts:

```erlang
py_preload:set_code(<<"PRELOADED = 42">>).

{ok, Ctx1} = py_context:new(#{mode => worker}).
{ok, 42} = py:eval(Ctx1, <<"PRELOADED">>).

%% Clear preload
py_preload:clear_code().

%% Existing context still has it
{ok, 42} = py:eval(Ctx1, <<"PRELOADED">>).

%% New context does not
{ok, Ctx2} = py_context:new(#{mode => worker}).
{error, _} = py:eval(Ctx2, <<"PRELOADED">>).
```

## Best Practices

1. **Set preload early** - Configure before creating any contexts
2. **Keep it focused** - Only include truly shared code
3. **Avoid side effects** - Preload runs once per interpreter
4. **Use for imports** - Common imports benefit most from preloading

## Limitations

- Changes to preload code don't affect existing contexts
- Same preload applies to both context modes (worker, owngil)
- Preload errors during context creation will fail the context
