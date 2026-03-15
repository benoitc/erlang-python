# Process-Bound Python Environments

*Since version 2.2.0*

## Philosophy

In Erlang, processes are the fundamental unit of isolation. Each process has its own heap, mailbox, and lifecycle. When a process crashes, it takes its state with it and can be restarted clean by a supervisor.

erlang_python extends this philosophy to Python: **each Erlang process gets its own isolated Python environment**. Variables, imports, and objects defined in one process are invisible to others, even when using the same Python context.

This design enables:
- **Clean restarts**: Resetting Python state = terminating the Erlang process
- **Fault isolation**: A corrupted Python state crashes only its owning process
- **Supervision**: Standard OTP supervisors can manage Python-backed actors
- **Actor model**: Build stateful Python services that behave like gen_servers

## How It Works

When you call `py:exec/eval/call`, the library:

1. Looks up a process-local environment keyed by `{ContextPid, InterpreterId}`
2. Creates one if it doesn't exist (a Python `dict` inside the interpreter)
3. Uses that dict as the namespace for execution
4. Cleans up automatically when the Erlang process exits

```erlang
%% Process A
spawn(fun() ->
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"state = 'hello'">>),
    {ok, <<"hello">>} = py:eval(Ctx, <<"state">>)
end).

%% Process B - same context, isolated state
spawn(fun() ->
    Ctx = py:context(1),
    %% state is undefined here - different process
    {error, _} = py:eval(Ctx, <<"state">>)
end).
```

## OWN_GIL Mode

OWN_GIL contexts (Python 3.12+) provide true parallel execution with dedicated pthreads. Process-bound environments work with OWN_GIL, allowing multiple Erlang processes to share a single OWN_GIL context while maintaining isolated Python namespaces.

### Explicit Environment Creation

For OWN_GIL contexts, you can explicitly create and manage environments:

```erlang
%% Create an OWN_GIL context
{ok, Ctx} = py_context:start_link(1, owngil),

%% Create a process-local environment
{ok, Env} = py_context:create_local_env(Ctx),

%% Get the NIF reference for low-level operations
CtxRef = py_context:get_nif_ref(Ctx),

%% Execute code in the isolated environment
ok = py_nif:context_exec(CtxRef, <<"
class MyService:
    def __init__(self):
        self.counter = 0
    def increment(self):
        self.counter += 1
        return self.counter

service = MyService()
">>, Env),

%% Call functions in the environment
{ok, 1} = py_nif:context_eval(CtxRef, <<"service.increment()">>, #{}, Env),
{ok, 2} = py_nif:context_eval(CtxRef, <<"service.increment()">>, #{}, Env).
```

### Sharing Context, Isolating State

Multiple Erlang processes can share an OWN_GIL context while maintaining isolated namespaces:

```erlang
%% Shared OWN_GIL context
{ok, Ctx} = py_context:start_link(1, owngil),
CtxRef = py_context:get_nif_ref(Ctx),

%% Process A - its own namespace
spawn(fun() ->
    {ok, EnvA} = py_context:create_local_env(Ctx),
    ok = py_nif:context_exec(CtxRef, <<"x = 'from A'">>, EnvA),
    {ok, <<"from A">>} = py_nif:context_eval(CtxRef, <<"x">>, #{}, EnvA)
end),

%% Process B - separate namespace, same context
spawn(fun() ->
    {ok, EnvB} = py_context:create_local_env(Ctx),
    ok = py_nif:context_exec(CtxRef, <<"x = 'from B'">>, EnvB),
    {ok, <<"from B">>} = py_nif:context_eval(CtxRef, <<"x">>, #{}, EnvB)
end).
%% Both execute in parallel on the same OWN_GIL thread, but with isolated state
```

### When to Use Explicit vs Implicit Environments

| Approach | API | Use Case |
|----------|-----|----------|
| **Implicit** | `py:exec/eval/call` | Simple cases, automatic management |
| **Explicit** | `create_local_env` + `py_nif:context_*` | OWN_GIL, fine-grained control, multiple envs per process |

**Use implicit (py:exec)** when:
- Using worker or subinterp modes
- One environment per process is sufficient
- You want automatic lifecycle management

**Use explicit (create_local_env)** when:
- Using OWN_GIL mode for parallel execution
- Need multiple environments in a single process
- Want to pass environments between processes
- Need direct NIF-level control

## Event Loop Environments

The event loop API also supports per-process namespaces. Each Erlang process gets an isolated namespace within the event loop, allowing you to define functions and state that persist across async task calls.

### Defining Functions for Async Tasks

```erlang
%% Get the event loop reference
{ok, Loop} = py_event_loop:get_loop(),
LoopRef = py_event_loop:get_nif_ref(Loop),

%% Define a function in this process's namespace
ok = py_nif:event_loop_exec(LoopRef, <<"
import asyncio

async def my_async_function(x):
    await asyncio.sleep(0.1)
    return x * 2

counter = 0

async def increment_and_get():
    global counter
    counter += 1
    return counter
">>),

%% Call the function via create_task - uses __main__ module
{ok, Ref} = py_event_loop:create_task(Loop, '__main__', my_async_function, [21]),
{ok, 42} = py_event_loop:await(Ref),

%% State persists across calls
{ok, Ref1} = py_event_loop:create_task(Loop, '__main__', increment_and_get, []),
{ok, 1} = py_event_loop:await(Ref1),
{ok, Ref2} = py_event_loop:create_task(Loop, '__main__', increment_and_get, []),
{ok, 2} = py_event_loop:await(Ref2).
```

### Evaluating Expressions

```erlang
%% Evaluate expressions in the process's namespace
{ok, 42} = py_nif:event_loop_eval(LoopRef, <<"21 * 2">>),

%% Access variables defined via exec
ok = py_nif:event_loop_exec(LoopRef, <<"result = 'computed'">>),
{ok, <<"computed">>} = py_nif:event_loop_eval(LoopRef, <<"result">>).
```

### Process Isolation

Different Erlang processes have isolated event loop namespaces:

```erlang
{ok, Loop} = py_event_loop:get_loop(),
LoopRef = py_event_loop:get_nif_ref(Loop),

%% Process A defines x
spawn(fun() ->
    ok = py_nif:event_loop_exec(LoopRef, <<"x = 'A'">>),
    {ok, <<"A">>} = py_nif:event_loop_eval(LoopRef, <<"x">>)
end),

%% Process B has its own x
spawn(fun() ->
    ok = py_nif:event_loop_exec(LoopRef, <<"x = 'B'">>),
    {ok, <<"B">>} = py_nif:event_loop_eval(LoopRef, <<"x">>)
end).
```

### Cleanup

Event loop namespaces are automatically cleaned up when the Erlang process exits. The event loop monitors each process that creates a namespace and removes it on process termination.

## Building Python Actors

The process-bound model enables a pattern we call "Python actors" - Erlang processes that encapsulate Python state and expose it through message passing.

### Basic Actor Pattern

```erlang
-module(py_counter).
-behaviour(gen_server).

-export([start_link/0, increment/1, decrement/1, get/1]).
-export([init/1, handle_call/3, handle_cast/2]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

increment(Pid) -> gen_server:call(Pid, increment).
decrement(Pid) -> gen_server:call(Pid, decrement).
get(Pid) -> gen_server:call(Pid, get).

init([]) ->
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
class Counter:
    def __init__(self):
        self.value = 0
    def increment(self):
        self.value += 1
        return self.value
    def decrement(self):
        self.value -= 1
        return self.value
    def get(self):
        return self.value

counter = Counter()
">>),
    {ok, #{ctx => Ctx}}.

handle_call(increment, _From, #{ctx := Ctx} = State) ->
    {ok, Value} = py:eval(Ctx, <<"counter.increment()">>),
    {reply, Value, State};

handle_call(decrement, _From, #{ctx := Ctx} = State) ->
    {ok, Value} = py:eval(Ctx, <<"counter.decrement()">>),
    {reply, Value, State};

handle_call(get, _From, #{ctx := Ctx} = State) ->
    {ok, Value} = py:eval(Ctx, <<"counter.get()">>),
    {reply, Value, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.
```

Usage:
```erlang
{ok, Counter} = py_counter:start_link(),
1 = py_counter:increment(Counter),
2 = py_counter:increment(Counter),
1 = py_counter:decrement(Counter),
1 = py_counter:get(Counter).
```

### Reset via Process Termination

Following Erlang's "let it crash" philosophy, resetting Python state is simple:

```erlang
%% Supervise the Python actor
init([]) ->
    Children = [
        #{id => py_worker,
          start => {py_worker, start_link, []},
          restart => permanent}
    ],
    {ok, {#{strategy => one_for_one}, Children}}.

%% To reset: just terminate and let supervisor restart
reset_worker(Sup) ->
    ok = supervisor:terminate_child(Sup, py_worker),
    {ok, _} = supervisor:restart_child(Sup, py_worker).
```

No need to manually clear variables, reload modules, or reset interpreter state. The new process starts with a fresh Python environment.

### Stateful ML Pipeline

```erlang
-module(ml_predictor).
-behaviour(gen_server).

-export([start_link/1, predict/2]).
-export([init/1, handle_call/3, terminate/2]).

start_link(ModelPath) ->
    gen_server:start_link(?MODULE, ModelPath, []).

predict(Pid, Features) ->
    gen_server:call(Pid, {predict, Features}).

init(ModelPath) ->
    Ctx = py:context(),
    %% Define functions and load model - stored in process-bound environment
    ok = py:exec(Ctx, <<"
import pickle

_model = None

def load_model(path):
    global _model
    with open(path, 'rb') as f:
        _model = pickle.load(f)
    return True

def predict(features):
    return _model.predict([features]).tolist()[0]
">>),
    %% Load model - it's stored in _model global within this process's env
    {ok, true} = py:call(Ctx, '__main__', load_model, [ModelPath]),
    {ok, #{ctx => Ctx}}.

handle_call({predict, Features}, _From, #{ctx := Ctx} = State) ->
    {ok, Result} = py:call(Ctx, '__main__', predict, [Features]),
    {reply, {ok, Result}, State}.

terminate(_Reason, _State) ->
    %% Python environment automatically cleaned up
    ok.
```

## Advantages

| Aspect | Benefit |
|--------|---------|
| **Isolation** | Processes cannot interfere with each other's Python state |
| **Cleanup** | No resource leaks - process death = environment cleanup |
| **Restart** | Fresh state by terminating process (no manual reset logic) |
| **Supervision** | OTP supervisors manage Python actors like any other process |
| **Debugging** | Process dictionary inspection shows environment reference |
| **Memory** | Each process's Python memory counted separately |

## Trade-offs

| Aspect | Consideration |
|--------|---------------|
| **Memory overhead** | Each process has separate Python dict; no sharing |
| **Startup cost** | Environment created on first call per process |
| **No shared state** | State sharing requires explicit message passing or ETS |
| **Module caching** | Imported modules cached at interpreter level, not per-process |

## When to Use

**Good fit:**
- Stateful services (sessions, connections, workflows)
- Actor-style Python components
- Isolated workers that may need reset
- Per-request processing with state accumulation
- Supervised Python services

**Consider alternatives when:**
- Sharing state between many processes (use ETS or message passing)
- State must survive process restarts (use external storage)
- Memory is constrained (many processes = many environments)
- Truly stateless operations (environment overhead unnecessary)

## Comparison with Other Models

### vs. Global Interpreter State

Traditional Python embedding shares state globally. Any code can modify any variable. Isolation requires explicit namespace management.

With process-bound environments:
```erlang
%% Each process is automatically isolated
spawn(fun() -> py:exec(Ctx, <<"x = 1">>) end),
spawn(fun() -> py:exec(Ctx, <<"x = 2">>) end).
%% No conflict - different environments
```

### vs. Multiple Interpreters

Some systems create separate Python interpreters per "session". This provides isolation but:
- High memory cost per interpreter
- GIL contention in multi-interpreter setups
- Complex lifecycle management

Process-bound environments use a single interpreter (or subinterpreter pool) but isolate at the namespace level - lightweight and efficient.

### vs. Stateless Lambda-Style

Some systems treat Python as pure functions with no state between calls:
```erlang
%% Stateless style - no persistence
py:call(math, sqrt, [16]).
```

Process-bound environments allow both stateless and stateful patterns in the same system.

## Technical Details

Environments are stored as NIF resources with the following lifecycle:

1. **Creation**: First `py:exec/eval/call` in a process allocates an environment
2. **Storage**: Reference kept in process dictionary under `py_local_env`
3. **Usage**: Each call uses the environment as local namespace
4. **Cleanup**: NIF resource destructor runs when process terminates

For subinterpreters, environments are created inside the target interpreter using its memory allocator - critical for memory safety.

### Interpreter ID Validation

Each `py_env_resource_t` stores the Python interpreter ID (`interp_id`) when created. For OWN_GIL contexts, before any operation using a process-local env, the system validates that the env belongs to the current interpreter:

```c
PyInterpreterState *current_interp = PyInterpreterState_Get();
if (penv->interp_id != PyInterpreterState_GetID(current_interp)) {
    return {error, env_wrong_interpreter};
}
```

This prevents:
- Using an env from a destroyed interpreter (dangling pointer)
- Using an env created for a different OWN_GIL context
- Memory corruption from cross-interpreter dict access

### Cleanup Safety

For the main interpreter (`interp_id == 0`), the destructor acquires the GIL and decrefs the Python dicts normally.

For subinterpreters, the destructor skips `Py_DECREF` because:
1. `PyGILState_Ensure` cannot safely acquire a subinterpreter's GIL
2. The Python objects will be freed when the subinterpreter is destroyed via `Py_EndInterpreter`

This design prioritizes safety over avoiding minor memory leaks during edge cases.

## See Also

- [OWN_GIL Internals](owngil_internals.md) - Architecture and safety mechanisms for OWN_GIL mode
- [Scalability](scalability.md) - Mode comparison (owngil vs subinterp vs worker)
- [Event Loop Architecture](event_loop_architecture.md) - Per-process namespace management
- [Context Affinity](context-affinity.md) - Context binding and routing
- [Scheduling](asyncio.md) - Cooperative scheduling for long operations
