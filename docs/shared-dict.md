# SharedDict API

SharedDict provides process-scoped shared dictionaries for bidirectional state sharing between Erlang and Python. Use it when you need to share configuration, caches, or session state within a single Python context.

## Overview

SharedDict is designed for scenarios where Erlang and Python need to share mutable state:

| Use Case | Description |
|----------|-------------|
| Configuration | Pass runtime config from Erlang, readable by Python |
| Session state | Maintain state across multiple Python calls |
| Caches | Share cached data between Erlang and Python |
| Coordination | Exchange data without serialization overhead |

SharedDict values are pickled for cross-interpreter safety, making them safe to use with Python subinterpreters.

## Quick Start

### Erlang Side

```erlang
%% Create a SharedDict
{ok, SD} = py:shared_dict_new(),

%% Set values
ok = py:shared_dict_set(SD, <<"config">>, #{host => <<"localhost">>, port => 8080}),

%% Get values
Config = py:shared_dict_get(SD, <<"config">>),
%% #{<<"host">> => <<"localhost">>, <<"port">> => 8080}

%% Get with default
Value = py:shared_dict_get(SD, <<"missing">>, default_value),
%% default_value

%% List keys
Keys = py:shared_dict_keys(SD),
%% [<<"config">>]

%% Delete a key
ok = py:shared_dict_del(SD, <<"config">>),

%% Explicit cleanup (optional - GC handles this)
ok = py:shared_dict_destroy(SD).
```

### Python Side

```python
from erlang import SharedDict

def process_with_config(sd_handle):
    # Wrap the handle
    sd = SharedDict(sd_handle)

    # Dict-like access
    config = sd['config']
    host = config.get('host') or config.get(b'host')

    # Set values
    sd['result'] = {'status': 'ok', 'count': 42}

    # Check membership
    if 'config' in sd:
        print("Config found")

    # Iterate keys
    for key in sd.keys():
        print(f"Key: {key}")

    # Delete
    del sd['result']
```

## Erlang API

### `py:shared_dict_new/0`

Create a new SharedDict owned by the calling process.

```erlang
{ok, SD} = py:shared_dict_new().
```

The SharedDict is automatically destroyed when the owning process terminates.

### `py:shared_dict_get/2,3`

Get a value from the SharedDict.

```erlang
Value = py:shared_dict_get(SD, <<"key">>).
%% Returns undefined if key not found

Value = py:shared_dict_get(SD, <<"key">>, default).
%% Returns default if key not found
```

### `py:shared_dict_set/3`

Set a value in the SharedDict.

```erlang
ok = py:shared_dict_set(SD, <<"key">>, Value).
```

Values are pickled internally for cross-interpreter safety.

### `py:shared_dict_del/2`

Delete a key from the SharedDict.

```erlang
ok = py:shared_dict_del(SD, <<"key">>).
```

Returns `ok` even if the key doesn't exist.

### `py:shared_dict_keys/1`

Get all keys from the SharedDict.

```erlang
Keys = py:shared_dict_keys(SD).
%% [<<"key1">>, <<"key2">>]
```

### `py:shared_dict_destroy/1`

Explicitly destroy a SharedDict.

```erlang
ok = py:shared_dict_destroy(SD).
```

After destruction, any operations on the SharedDict raise an error. This is idempotent - calling multiple times is safe.

## Python API

### `SharedDict` class

Dict-like wrapper for SharedDict handles passed from Erlang.

```python
from erlang import SharedDict

# Create from handle (passed via eval/exec locals)
sd = SharedDict(handle)
```

#### Subscript Access

```python
# Get value
value = sd['key']  # Raises KeyError if not found

# Set value
sd['key'] = value

# Delete
del sd['key']  # Raises KeyError if not found
```

#### `get(key, default=None)`

Get value with optional default.

```python
value = sd.get('key')  # Returns None if not found
value = sd.get('key', 'default')
```

#### `keys()`

Get all keys as a list.

```python
for key in sd.keys():
    process(key)
```

#### `__contains__`

Check if key exists.

```python
if 'key' in sd:
    process(sd['key'])
```

#### `destroy()`

Explicitly destroy the SharedDict from Python.

```python
sd.destroy()  # Invalidates all references
```

## Design

### Thread Safety

SharedDict uses a mutex to protect concurrent access. Multiple Python threads or Erlang processes can safely read/write the same SharedDict.

### Value Storage

Values are pickled (serialized) when stored and unpickled when retrieved. This ensures:

1. **Cross-interpreter safety** - Safe to use with Python subinterpreters
2. **Type preservation** - Python types round-trip correctly
3. **Isolation** - Changes to retrieved values don't affect stored values

### Lifecycle

SharedDicts follow two cleanup paths:

1. **Automatic (GC)** - When the owning Erlang process terminates, the SharedDict is marked for garbage collection
2. **Explicit** - Call `py:shared_dict_destroy/1` or `sd.destroy()` for immediate cleanup

Use explicit destroy when you need deterministic cleanup or want to release resources before process termination.

## Examples

### Configuration Passing

```erlang
%% Erlang: Set up config before Python call
{ok, SD} = py:shared_dict_new(),
ok = py:shared_dict_set(SD, <<"db">>, #{
    host => <<"localhost">>,
    port => 5432,
    user => <<"admin">>
}),

%% Pass handle to Python
{ok, _} = py:eval(<<"process_data(handle)">>, #{<<"handle">> => SD}).
```

```python
from erlang import SharedDict

def process_data(sd_handle):
    sd = SharedDict(sd_handle)
    db_config = sd['db']

    # Use config to connect to database
    conn = connect(
        host=db_config.get('host') or db_config.get(b'host'),
        port=db_config.get('port') or db_config.get(b'port')
    )

    # Store results back
    sd['results'] = {'rows_processed': 1000}
```

### Session State

```erlang
%% Create session state for a user
{ok, Session} = py:shared_dict_new(),
ok = py:shared_dict_set(Session, <<"user_id">>, UserId),
ok = py:shared_dict_set(Session, <<"cart">>, []),

%% Multiple Python calls share the session
{ok, _} = py:call(shop, add_to_cart, [Session, ItemId]),
{ok, _} = py:call(shop, add_to_cart, [Session, Item2Id]),

%% Read final state
Cart = py:shared_dict_get(Session, <<"cart">>),

%% Cleanup when done
ok = py:shared_dict_destroy(Session).
```

### Cache Sharing

```erlang
%% Create shared cache
{ok, Cache} = py:shared_dict_new(),

%% Inject the handle into Python globals (py:exec/1 has no locals
%% argument, so we stash it via py:eval with a side effect).
{ok, _} = py:eval(
    <<"(globals().__setitem__('_cache_handle', handle), None)[-1]">>,
    #{handle => Cache}),

%% Python can now populate the cache
ok = py:exec(<<"
from erlang import SharedDict
cache = SharedDict(_cache_handle)
cache['computed'] = 42
">>),

%% Erlang can read cached values
42 = py:shared_dict_get(Cache, <<"computed">>).
```

## See Also

- [State API](state.md) - Global shared state (different scope)
- [Channel](channel.md) - Message passing between Erlang and Python
- [Getting Started](getting-started.md) - Basic usage guide
