# Context Affinity

Context affinity allows you to bind an Erlang process to a dedicated Python context, preserving Python state (variables, imports, objects) across multiple `py:call/eval/exec` invocations.

## Why Context Affinity?

By default, each call to `py:call`, `py:eval`, or `py:exec` may be handled by a different context from the pool. This means:

- Variables defined in one call are not available in the next
- Imported modules must be re-imported
- Objects created in one call cannot be accessed later

Context affinity solves this by dedicating a context to your process, ensuring all calls go to the same Python interpreter with preserved state.

## Using Explicit Contexts

The simplest approach is to use explicit context handles:

```erlang
%% Get a specific context by index (1-based)
Ctx = py:context(1),

%% Now all calls to this context share state
ok = py:exec(Ctx, <<"counter = 0">>),
ok = py:exec(Ctx, <<"counter += 1">>),
{ok, 1} = py:eval(Ctx, <<"counter">>),

ok = py:exec(Ctx, <<"counter += 1">>),
{ok, 2} = py:eval(Ctx, <<"counter">>).
```

### Multiple Independent Contexts

Use multiple contexts for isolation:

```erlang
%% Get two different contexts
Ctx1 = py:context(1),
Ctx2 = py:context(2),

%% Each context has its own namespace
ok = py:exec(Ctx1, <<"x = 'context one'">>),
ok = py:exec(Ctx2, <<"x = 'context two'">>),

%% Values are isolated
{ok, <<"context one">>} = py:eval(Ctx1, <<"x">>),
{ok, <<"context two">>} = py:eval(Ctx2, <<"x">>).
```

## Binding Contexts to Processes

For automatic context routing, bind a context to the current process:

```erlang
%% Get a context
Ctx = py_context_router:get_context(),

%% Bind it to the current process
ok = py_context_router:bind_context(Ctx),

%% Now all py:call/eval/exec from this process use the bound context
ok = py:exec(<<"x = 42">>),
{ok, 42} = py:eval(<<"x">>),

%% Unbind when done
ok = py_context_router:unbind_context().
```

### Scoped Binding with try/after

Always ensure cleanup with try/after:

```erlang
run_with_context(Fun) ->
    Ctx = py_context_router:get_context(),
    ok = py_context_router:bind_context(Ctx),
    try
        Fun()
    after
        py_context_router:unbind_context()
    end.

%% Usage
Result = run_with_context(fun() ->
    ok = py:exec(<<"total = 0">>),
    ok = py:exec(<<"for i in range(10): total += i">>),
    py:eval(<<"total">>)
end),
{ok, 45} = Result.
```

## Context API Reference

### `py:context/0`

Get the context for the current scheduler (automatic affinity):

```erlang
Ctx = py:context().
```

### `py:context/1`

Get a specific context by index (1-based):

```erlang
Ctx = py:context(1).
```

### `py_context_router:bind_context/1`

Bind a context to the current process:

```erlang
ok = py_context_router:bind_context(Ctx).
```

### `py_context_router:unbind_context/0`

Remove the context binding for the current process:

```erlang
ok = py_context_router:unbind_context().
```

### `py_context_router:get_context/0,1`

Get a context from the pool:

```erlang
%% Get context for current scheduler
Ctx = py_context_router:get_context().

%% Get context from a specific pool
Ctx = py_context_router:get_context(io).
```

## Use Cases

### Stateful Computation

```erlang
Ctx = py:context(1),

%% Load a model once
py:exec(Ctx, <<"
import pickle
with open('model.pkl', 'rb') as f:
    model = pickle.load(f)
">>),

%% Use it multiple times
{ok, Pred1} = py:eval(Ctx, <<"model.predict([[1, 2, 3]])">>),
{ok, Pred2} = py:eval(Ctx, <<"model.predict([[4, 5, 6]])">>).
```

### Database Connections

```erlang
Ctx = py:context(1),

%% Establish connection once
py:exec(Ctx, <<"
import sqlite3
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()
cursor.execute('CREATE TABLE users (id INTEGER, name TEXT)')
">>),

%% Use the connection across multiple calls
py:exec(Ctx, <<"cursor.execute('INSERT INTO users VALUES (1, \"Alice\")')">>),
py:exec(Ctx, <<"cursor.execute('INSERT INTO users VALUES (2, \"Bob\")')">>),
{ok, Users} = py:eval(Ctx, <<"cursor.execute('SELECT * FROM users').fetchall()">>),

%% Clean up
py:exec(Ctx, <<"conn.close()">>).
```

### Incremental Processing

```erlang
Ctx = py:context(1),

%% Initialize accumulator
py:exec(Ctx, <<"results = []">>),

%% Process items one at a time
lists:foreach(fun(Item) ->
    py:eval(Ctx, <<"results.append(item * 2)">>, #{item => Item})
end, [1, 2, 3, 4, 5]),

%% Get final results
{ok, [2, 4, 6, 8, 10]} = py:eval(Ctx, <<"results">>).
```

## Scheduler Affinity (Default Behavior)

By default, without explicit binding, calls are routed based on the current Erlang scheduler. This provides good cache locality while allowing multiple processes to share contexts:

```erlang
%% Processes on the same scheduler share a context
%% Processes on different schedulers use different contexts
{ok, Result} = py:call(math, sqrt, [16]).
```

This is usually what you want for stateless operations where isolation isn't critical.

## Performance Considerations

- **Context binding overhead**: `bind_context()` requires a gen_server call
- **Lookup overhead**: Once bound, routing adds only an O(1) ETS lookup
- **Pool exhaustion**: Each bound context removes it from round-robin rotation
- **Recommendation**: Use explicit `py:context(N)` for stateful operations; let automatic routing handle stateless calls

## Context Pool Statistics

Check pool status:

```erlang
%% Check number of contexts
N = py_context_router:num_contexts().

%% Check if a pool is started
true = py_context_router:pool_started(default).
true = py_context_router:pool_started(io).

%% Get all contexts in a pool
Contexts = py_context_router:contexts(default).
```

## Error Handling

### Context Not Available

```erlang
%% If contexts aren't started
case py:contexts_started() of
    true -> proceed();
    false -> {error, contexts_not_started}
end.
```

## Process-Bound Environments

> **Note:** For a detailed guide on building "Python actors" and the Erlang philosophy behind process-bound environments, see [Process-Bound Environments](process-bound-envs.md).

Process-bound environments provide true process-level isolation for Python state. Each Erlang process automatically gets its own Python namespace that persists across calls.

### How It Works

When you call `py:call()`, `py:eval()`, or `py:exec()`, the library automatically:

1. Looks up or creates a process-local Python environment for your Erlang process
2. Executes the Python code using that environment
3. Stores variables, imports, and objects in that environment
4. Cleans up automatically when your Erlang process exits

This happens transparently - no explicit binding required.

### Basic Usage

```erlang
%% Get a context
Ctx = py:context(1),

%% Define a variable - it persists for THIS Erlang process
ok = py:exec(Ctx, <<"counter = 0">>),
ok = py:exec(Ctx, <<"counter += 1">>),
{ok, 1} = py:eval(Ctx, <<"counter">>).

%% In a different Erlang process, counter is independent:
spawn(fun() ->
    ok = py:exec(Ctx, <<"counter = 100">>),
    {ok, 100} = py:eval(Ctx, <<"counter">>)
end).

%% Back in original process, still 1
{ok, 1} = py:eval(Ctx, <<"counter">>).
```

### Process Affinity for AI Workloads

Process-bound environments are ideal for scenarios where each Erlang process needs isolated Python state:

```erlang
%% Each user session gets its own chat history
handle_user_session(UserId) ->
    Ctx = py:context(),
    %% Initialize conversation for this process
    ok = py:exec(Ctx, <<"
conversation_history = []
def add_message(role, content):
    conversation_history.append({'role': role, 'content': content})
def get_history():
    return conversation_history
">>),
    session_loop(Ctx).

session_loop(Ctx) ->
    receive
        {user_message, Msg} ->
            py:call(Ctx, '__main__', add_message, [<<"user">>, Msg]),
            %% Process with AI...
            session_loop(Ctx);
        get_history ->
            {ok, History} = py:call(Ctx, '__main__', get_history, []),
            History
    end.
```

### Isolation Between Processes

```erlang
%% Process A
spawn(fun() ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"x = 'from process A'">>)
end),

%% Process B - same context, but isolated environment
spawn(fun() ->
    Ctx = py:context(1),  %% Same context!
    ok = py:exec(Ctx, <<"x = 'from process B'">>),
    {ok, <<"from process B">>} = py:eval(Ctx, <<"x">>)  %% Own value
end).
```

### Memory Management

Environments are automatically freed when:
- The Erlang process exits (normal, abnormal, or killed)
- The NIF resource destructor runs during garbage collection

No manual cleanup is needed. The environments use the correct memory allocator for each interpreter (critical for subinterpreters which have isolated allocators).

### When to Use Process-Bound Environments

**Good use cases:**
- Stateful sessions (chat, game state, user preferences)
- Long-running workers that accumulate state
- Process-per-request patterns with state
- AI pipelines with per-request context

**Consider alternatives when:**
- State must be shared between Erlang processes (use shared state API instead)
- State needs to outlive the Erlang process (use explicit storage)
- You need multiple independent namespaces per process (use explicit contexts)

### Technical Details

Process-bound environments work by:

1. Storing a `reference()` in the calling process's dictionary under `py_local_env`
2. The reference points to a Python dict created inside the interpreter
3. Each interpreter ID maps to a separate environment (for subinterpreter support)
4. The NIF uses this dict as `locals` for `exec()` and `eval()` operations

For subinterpreters, environments are created inside the target interpreter to ensure memory safety - Python's subinterpreters have isolated memory allocators.

## Best Practices

1. **Use explicit contexts for stateful operations**: `Ctx = py:context(1)` ensures state persists
2. **Use automatic routing for stateless calls**: Let the router handle distribution
3. **Always unbind in finally blocks**: Prevent context leaks
4. **Minimize binding time**: Don't hold contexts longer than necessary
5. **Monitor pool size**: Check `py_context_router:num_contexts()` to understand capacity
6. **Leverage process-bound environments**: For per-process state, rely on automatic environment isolation rather than manual binding
