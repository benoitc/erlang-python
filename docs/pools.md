# Pool Support

This guide covers erlang_python's pool architecture for separating CPU-bound and I/O-bound Python operations.

## Overview

erlang_python provides a `default` pool that starts automatically, and allows you to create additional pools on demand:

| Pool | Purpose | Default Size | Use Case |
|------|---------|--------------|----------|
| `default` | Quick CPU-bound operations | Number of schedulers | Math, string processing, data transformation |
| custom pools | User-defined pools | User-defined | HTTP requests, database queries, GPU work |

Create pools on demand to separate slow I/O operations from blocking quick CPU operations.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         py:call/3,4,5                             │
│                              │                                    │
│                              ▼                                    │
│                    ┌─────────────────┐                           │
│                    │  lookup_pool()  │                           │
│                    │  (registration) │                           │
│                    └────────┬────────┘                           │
│                             │                                    │
│              ┌──────────────┴──────────────┐                     │
│              ▼                              ▼                     │
│     ┌────────────────┐             ┌────────────────┐            │
│     │  default pool  │             │  custom pools  │            │
│     │  (N contexts)  │             │  (on demand)   │            │
│     └────────────────┘             └────────────────┘            │
│              │                              │                     │
│     ┌────────┴────────┐            ┌────────┴────────┐           │
│     ▼        ▼        ▼            ▼        ▼        ▼           │
│   Ctx1    Ctx2    CtxN          Ctx1    Ctx2    CtxN            │
│   (math)  (json)  (...)         (http)  (db)   (...)            │
└──────────────────────────────────────────────────────────────────┘
```

## Basic Usage

### Creating Custom Pools

Create pools on demand for specific workloads:

```erlang
%% Create an io pool for I/O-bound operations
{ok, _Contexts} = py_context_router:start_pool(io, 10, worker).

%% Create a gpu pool for ML workloads
{ok, _} = py_context_router:start_pool(gpu, 2, worker).
```

### Explicit Pool Selection

Specify the pool directly in the call:

```erlang
%% Use default pool (quick operations)
{ok, 4.0} = py:call(default, math, sqrt, [16]).

%% Use io pool (after creating it)
{ok, Response} = py:call(io, requests, get, [Url]).

%% With keyword arguments
{ok, Data} = py:call(io, requests, get, [Url], #{timeout => 30}).
```

### Registration-Based Routing

Register modules or specific functions to automatically route to a specific pool:

```erlang
%% Register entire module to io pool (all functions in module)
ok = py:register_pool(io, requests).
ok = py:register_pool(io, aiohttp).
ok = py:register_pool(io, psycopg2).

%% Register specific module.function to io pool
ok = py:register_pool(io, {urllib, urlopen}).    %% Only urllib.urlopen
ok = py:register_pool(io, {httpx, 'get'}).       %% Only httpx.get
ok = py:register_pool(io, {db, query}).          %% Only db.query

%% Now calls route automatically - no code changes needed
{ok, 4.0} = py:call(math, sqrt, [16]).           %% -> default pool
{ok, Resp} = py:call(requests, get, [Url]).      %% -> io pool (module registered)
{ok, Rows} = py:call(db, query, [Sql]).          %% -> io pool (function registered)
{ok, Data} = py:call(db, connect, [Dsn]).        %% -> default pool (only db.query registered)
```

### Unregistering

```erlang
%% Remove module registration
ok = py:unregister_pool(requests).

%% Remove function registration
ok = py:unregister_pool({urllib, urlopen}).
```

## Registration Priority

Function-specific registrations take priority over module-wide registrations:

```erlang
%% Register all json functions to io pool
ok = py:register_pool(io, json).

%% But keep json.dumps on default pool (it's fast)
ok = py:register_pool(default, {json, dumps}).

%% Results:
io = py_context_router:lookup_pool(json, loads).   %% Module registration
default = py_context_router:lookup_pool(json, dumps).  %% Function override
```

## API Reference

### High-Level API (py module)

```erlang
%% Register entire module to pool (all callables in the module)
-spec register_pool(Pool, Module) -> ok when
    Pool :: default | io | atom(),
    Module :: atom().

%% Register specific callable (module.function) to pool
-spec register_pool(Pool, {Module, Callable}) -> ok when
    Pool :: default | io | atom(),
    Module :: atom(),
    Callable :: atom().

%% Unregister module or specific callable
-spec unregister_pool(Module | {Module, Callable}) -> ok.

%% Call on specific pool
-spec call(Pool, Module, Func, Args) -> {ok, Result} | {error, Reason}.
-spec call(Pool, Module, Func, Args, Kwargs) -> {ok, Result} | {error, Reason}.
```

### Low-Level API (py_context_router module)

```erlang
%% Pool management
-spec start_pool(Pool, Size) -> {ok, [pid()]} | {error, term()}.
-spec start_pool(Pool, Size, Mode) -> {ok, [pid()]} | {error, term()}.
-spec stop_pool(Pool) -> ok.
-spec pool_started(Pool) -> boolean().

%% Context access
-spec get_context(Pool) -> pid().
-spec num_contexts(Pool) -> non_neg_integer().
-spec contexts(Pool) -> [pid()].

%% Registration
-spec register_pool(Pool, Module) -> ok.
-spec register_pool(Pool, Module, Func) -> ok.
-spec unregister_pool(Module) -> ok.
-spec unregister_pool(Module, Func) -> ok.
-spec lookup_pool(Module, Func) -> Pool.
-spec list_pool_registrations() -> [{{Module, Func | '_'}, Pool}].
```

## Configuration

Configure default pool size via application environment:

```erlang
%% sys.config
[
    {erlang_python, [
        %% Default pool size (default: erlang:system_info(schedulers))
        {default_pool_size, 8}
    ]}
].
```

### Runtime Configuration

```erlang
%% Start io pool for I/O-bound operations
{ok, _} = py_context_router:start_pool(io, 10, worker).

%% Start GPU pool for ML operations
{ok, _} = py_context_router:start_pool(gpu, 2, worker).

%% Register operations to route to specific pools
ok = py:register_pool(io, requests).
ok = py:register_pool(io, psycopg2).
ok = py:register_pool(gpu, torch).
ok = py:register_pool(gpu, tensorflow).
```

## Use Cases

### Web Application with Database

```erlang
%% At application startup
init_pools() ->
    %% Create io pool for I/O-bound operations
    {ok, _} = py_context_router:start_pool(io, 10, worker),

    %% Register I/O-heavy modules
    py:register_pool(io, requests),
    py:register_pool(io, httpx),
    py:register_pool(io, psycopg2),
    py:register_pool(io, redis),
    ok.

%% In request handler - no pool awareness needed
handle_request(UserId) ->
    %% Fast: uses default pool
    {ok, Hash} = py:call(hashlib, sha256, [UserId]),

    %% Slow: automatically uses io pool
    {ok, User} = py:call(psycopg2, fetchone, [<<"SELECT * FROM users WHERE id = ?">>, [UserId]]),

    %% Fast: uses default pool
    {ok, Json} = py:call(json, dumps, [User]),
    {ok, Json}.
```

### ML Pipeline with I/O

```erlang
%% Create and configure io pool
{ok, _} = py_context_router:start_pool(io, 10, worker),
py:register_pool(io, boto3),        %% S3 access
py:register_pool(io, requests),      %% API calls

%% ML operations stay on default pool (CPU-intensive)
%% I/O operations go to io pool

process_batch(Items) ->
    %% Parallel fetch from S3 (io pool)
    Futures = [py:spawn_call(boto3, download_file, [Key]) || Key <- Items],
    Files = [py:await(F) || F <- Futures],

    %% Process with ML model (default pool - doesn't block I/O)
    [{ok, _} = py:call(model, predict, [File]) || File <- Files].
```

## Performance Considerations

### Pool Sizing

| Workload | default Pool | io Pool |
|----------|--------------|---------|
| CPU-heavy | Schedulers | Small (5-10) |
| I/O-heavy | Small (2-4) | Large (20-50) |
| Mixed | Schedulers | 10-20 |

### When to Use Registration

**Use registration when:**
- You have clear I/O-bound modules (HTTP clients, database drivers)
- You want transparent routing without changing call sites
- Multiple call sites use the same module

**Use explicit pool selection when:**
- A function's pool depends on arguments
- You need fine-grained control per-call
- Testing or debugging specific pools

## Monitoring

```erlang
%% Check pool status
true = py_context_router:pool_started(default).
false = py_context_router:pool_started(io).  %% Not started yet

%% Start io pool
{ok, _} = py_context_router:start_pool(io, 10, worker).
true = py_context_router:pool_started(io).

%% Check pool sizes
DefaultSize = py_context_router:num_contexts(default).
IoSize = py_context_router:num_contexts(io).

%% List all registrations
Registrations = py_context_router:list_pool_registrations().
%% => [{{requests, '_'}, io}, {{json, dumps}, default}, ...]

%% Check which pool a call would use
io = py_context_router:lookup_pool(requests, get).
default = py_context_router:lookup_pool(math, sqrt).
```

## Backward Compatibility

Existing code using `py:call/3,4,5` without pool registration continues to work unchanged, using the default pool:

```erlang
%% These all use the default pool (backward compatible)
{ok, 4.0} = py:call(math, sqrt, [16]).
{ok, Data} = py:call(json, dumps, [#{a => 1}]).
{ok, 6} = py:eval(<<"2 + 4">>).
```

## See Also

- [Scalability](scalability.md) - Execution modes and parallel execution
- [Getting Started](getting-started.md) - Basic usage
- [Asyncio](asyncio.md) - Async I/O with event loops
