# Import and Path Registry

The `py_import` module manages a global registry for Python imports and sys.path
additions that are automatically applied to all Python interpreters.

## Overview

When you call `py:call/3` or similar functions, erlang_python needs to import
the Python module first. The import registry allows you to:

- Pre-register modules that should be imported in all interpreters
- Add paths to `sys.path` in all interpreters
- Configure imports and paths via application environment

Registered imports and paths are applied:
- Immediately to all running interpreters (contexts, event loops, OWN_GIL sessions)
- Automatically to any new interpreters created later

## Configuration

Configure imports and paths in your application environment:

```erlang
%% sys.config or app.src
{erlang_python, [
    {imports, [
        {json, dumps},
        {json, loads},
        {os, getcwd}
    ]},
    {paths, [
        "/path/to/my/modules",
        "/another/path"
    ]}
]}
```

## API

### Import Registry

#### `ensure_imported/1`

Register a module for import in all interpreters.

```erlang
ok = py_import:ensure_imported(json).
```

#### `ensure_imported/2`

Register a module/function pair for import.

```erlang
ok = py_import:ensure_imported(json, dumps).
ok = py_import:ensure_imported(json, loads).
```

#### `is_imported/1,2`

Check if a module or module/function is registered.

```erlang
true = py_import:is_imported(json).
true = py_import:is_imported(json, dumps).
false = py_import:is_imported(unknown_module).
```

#### `all_imports/0`

Get all registered imports.

```erlang
[{<<"json">>, all}, {<<"math">>, <<"sqrt">>}] = py_import:all_imports().
```

#### `import_list/0`

Get imports as a map grouped by module.

```erlang
{ok, #{<<"json">> => [<<"dumps">>, <<"loads">>],
       <<"math">> => []}} = py_import:import_list().
```

#### `clear_imports/0`

Remove all registered imports. Does not affect already-running interpreters.

```erlang
ok = py_import:clear_imports().
```

### Path Registry

#### `add_path/1`

Add a directory to `sys.path` in all interpreters.

```erlang
ok = py_import:add_path("/path/to/my/modules").
```

#### `add_paths/1`

Add multiple directories to `sys.path`.

```erlang
ok = py_import:add_paths(["/path/to/lib1", "/path/to/lib2"]).
```

#### `all_paths/0`

Get all registered paths in insertion order.

```erlang
[<<"/path/to/modules">>] = py_import:all_paths().
```

#### `is_path_added/1`

Check if a path is registered.

```erlang
true = py_import:is_path_added("/path/to/modules").
```

#### `clear_paths/0`

Remove all registered paths. Does not affect already-running interpreters.

```erlang
ok = py_import:clear_paths().
```

## Examples

### Pre-loading Common Modules

```erlang
%% At application startup
ok = py_import:ensure_imported(json),
ok = py_import:ensure_imported(os),
ok = py_import:ensure_imported(datetime).

%% Now all py:call invocations skip the import step for these modules
{ok, Json} = py:call(json, dumps, [[{key, value}]]).
```

### Adding Custom Module Paths

```erlang
%% Add your project's Python modules to sys.path
ok = py_import:add_path("/opt/myapp/python"),
ok = py_import:add_path("/opt/myapp/vendor").

%% Now you can import modules from these directories
{ok, Result} = py:call(mymodule, myfunction, []).
```

### Runtime Configuration

```erlang
%% Check what's registered
Imports = py_import:all_imports(),
Paths = py_import:all_paths(),
io:format("Registered imports: ~p~n", [Imports]),
io:format("Registered paths: ~p~n", [Paths]).
```

## Notes

- The `__main__` module cannot be cached (returns `{error, main_not_cacheable}`)
- Clearing registries does not affect already-running interpreters
- Paths are added in order, maintaining their relative priority in `sys.path`
- All module and path values are normalized to binaries internally
