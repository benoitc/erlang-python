# Protocols

Every message and frame that crosses a boundary in erlang_python: between
an Erlang process and the context process, between the context thread in C
and Erlang, and between the VM and an isolated child. Read this when you
change a message shape, add a request kind, or debug a hang with
`sys:trace/2` or `strace`. The code is the reference; this page tells you
where each piece is and what must stay consistent.

## Boundaries

```
 caller process  --(1) Erlang messages-->  py_context / py_isolated process
                                                  |
                       embedded modes             |          isolated mode
                (2) NIF request queue             |     (4) Unix socket frames
                (3) callback pipe / thread pipe   |
                                                  v
                          context thread in C            child OS process
```

1. Erlang messages: `src/py_context.erl` (API side and embedded loop),
   `src/py_isolated.erl` (isolated loop).
2. Request queue: `ctx_request_t` in `c_src/py_nif.h`, enqueued by
   `nif_context_call_async`, answered with `{py_result, Ref, Result}`.
3. Callbacks: `erlang_call_impl` in `c_src/py_callback.c`,
   `c_src/py_thread_worker.c`, `src/py_thread_handler.erl`.
4. Socket: `src/py_isolated.erl`, `priv/_erlang_impl/_isolated.py`,
   `priv/_erlang_impl/_etf.py`.

## 1. Caller to context process

The public API (`py:call/3`, `py_context:call/4`, ...) is a plain message
plus a monitor. Both context loops accept the same messages, so a caller
never knows the mode.

```erlang
%% py_context:submit/5 and friends
MRef = erlang:monitor(process, Ctx),
Ctx ! {call, self(), MRef, Module, Func, Args, Kwargs},
%% reply
{MRef, Result}          %% {ok, Term} | {error, Reason}
```

| Message | Reply | Notes |
|---|---|---|
| `{call, From, MRef, Module, Func, Args, Kwargs}` | `{ok, R}` / `{error, E}` | 8-tuple variant adds `EnvRef` for process-local envs |
| `{eval, From, MRef, Code, Locals}` | same | 6-tuple variant adds `EnvRef` |
| `{exec, From, MRef, Code}` | `ok` / `{error, E}` | 5-tuple variant adds `EnvRef` |
| `{submit, From, MRef, TaskRef, Module, Func, Args, Kwargs}` | `ok`, then `{async_result, TaskRef, Result}` to `From` | asyncio coroutine on the context loop |
| `{start_loop, From, MRef, Owner}` | `ok` / `{error, already_running}` | runs `run_forever` on the context thread; `Owner` is monitored and gets `{py_loop_exit, Ctx, Result}` when the loop ends |
| `{stop_loop, From, MRef, GraceMs}` | `ok` / `{error, no_loop}` | cooperative stop, interrupt after `GraceMs`, then kill |
| `{loop_ref, From, MRef}` | `{ok, LoopRef}` / `{error, no_loop}` | isolated: `{error, not_supported_in_isolated}` |
| `{interrupt, From, MRef}` | `ok` / `not_running` | |
| `{kill, From, MRef}` | `ok` | isolated only: SIGKILL, answered once the new child is up |
| `{pass_fd, From, MRef, Fd}` | `ok` / `{error, E}` | isolated only: SCM_RIGHTS |
| `{child_info, From, MRef}` | `{ok, Map}` | isolated only |
| `{stop, From, MRef}` | `ok` | |

Timeouts are the caller's business: `await_reply/3` waits `Timeout`, then
sends `{interrupt_request, MRef}` so the request executing now is
interrupted, waits a short grace for the late reply, and returns
`{error, timeout}`. Loop control messages use `await_ctrl_reply/3`, which
must not interrupt (it would stop the loop it manages) and instead sends
`{cancel_ctrl, MRef}` so an isolated context drops the pending entry.

Rules:

- One request at a time per context. The embedded loop blocks in the
  request; `py_isolated` postpones with `gen_statem` `postpone` while in
  `{busy, Id}`.
- A request from a process that is running a callback for this context is
  nested and served at once (both loops track those pids).
- Replies always go to `From`, tagged with `MRef`; a late reply after a
  timeout is flushed by `demonitor(MRef, [flush])`.

## 2. Request queue (embedded modes)

`nif_context_call_async(Ctx, Kind, Data, RequestId)` allocates a
`ctx_request_t`, copies the terms into `request_env`, appends it under
`queue_mutex` and returns. The context thread dequeues, runs the request,
and sends:

```erlang
{py_result, RequestId, Result}
```

to `caller_pid` through `msg_env`. Cancelled requests (context destroyed
while queued) are answered with `{py_result, Id, {error, cancelled}}`.
Request kinds are `ctx_request_type_t` in `c_src/py_nif.h`; the execute
functions read them from the request mirror on `py_context_t`
(`request_type`, `request_term`, ...).

A result that needs a callback served before completion comes back as
`{suspended, CallbackId, StateRef, {Name, Args}}` in place of the final
result (see 3.1); the process resumes with `resume_callback/2` and waits
for the next `py_result`.

## 3. Python calling Erlang

`erlang.call(Name, Args...)` picks a path in `erlang_call_impl`
(`c_src/py_callback.c`); the comment above that function is authoritative.
Precedence: suspension when the calling thread is a context thread with
suspension enabled, then the context's blocking pipe when a handler is set,
then the thread worker for every other Python thread.

### 3.1 Suspension

The Python call raises `SuspensionRequired` and sets thread-local pending
state; the execute function sees the flag before the exception type and
returns the `{suspended, ...}` result above. `py_context` runs the fun
(`execute/2` in `py_callback`), serving nested requests meanwhile
(`wait_for_callback/2`), and calls `resume_callback(StateRef, Response)`
with the response body of 3.4.

### 3.2 Context callback pipe

Set up with `context_set_callback_handler(Ref, Pid)`. The context thread
sends:

```erlang
{erlang_callback, CallbackId, FuncName, Args}
```

to the handler process and blocks (GIL released) reading the pipe with a
30 s timeout. The handler runs the fun and answers with
`context_write_callback_response(Ref, Body)`, which writes
`<<Len:32/native, Body/binary>>` on the pipe. There is no id on this pipe:
one context thread, one outstanding call.

### 3.3 Thread worker pipe

Any other Python thread (a `threading.Thread`, an executor worker) sends:

```erlang
{thread_callback, WorkerId, CallbackId, FuncName, Args}
```

to the `py_thread_handler` coordinator, which gives each `WorkerId` a
handler process (`{thread_worker_spawn, WorkerId, WriteFd}`) and replies
with `thread_worker_write_response(Fd, CallbackId, Body)`. The frame on the
response pipe is:

```
<<CallbackId:64/native, Len:32/native, Body:Len/binary>>
```

The reader discards frames whose id is not the one it waits for; a short
read or a timeout poisons the worker so the pipe is never read out of
phase. `erlang.async_call` uses the same frame on a per-interpreter
non-blocking pipe (`async_callback_pipe`, `{async_callback, CallbackId,
FuncName, Args, WriteFd}`), parsed incrementally by the reader tick.

### 3.4 Response body

Every path answers with the same body:

```
<<2, ETF/binary>>        %% ok, term_to_binary(Result)
<<1, Message/binary>>    %% error, UTF-8 text, raised as RuntimeError
```

Built in `handle_blocking_callback/3` (`py_context`), `py_thread_handler`
and the isolated loop; parsed by `parse_callback_response` in C and
`_isolated.py` in the child. Keep the three writers and two parsers in
step.

## 4. Isolated socket

The child (`priv/py_isolated_child.py`) connects to a Unix socket in the
private directory (`py_isolated:sock_dir/0`) and both sides exchange frames:

```
<<Id:64/native, Len:32/native, Body:Len/binary>>
Body = <<Status:8, ETF/binary>>
```

| Status | Direction | Meaning | Payload |
|---|---|---|---|
| 0 | Erlang to child | request | `{call, M, F, Args, Kwargs}`, `{eval, Code, Locals}`, `{exec, Code}`, `{submit, TaskRef, ...}`, `start_loop`, `{pass_fd, ...}`, `ping`, `shutdown`, `{init, Opts}` |
| 1 | either | error reply | `{Class, Message, Traceback}` or an atom |
| 2 | either | ok reply | the result term |
| 3 | child to Erlang | request from Python | `{call, Name, Args}`, `{send, Pid, Msg}`, `{whereis, Name}` |
| 4 | child to Erlang | event, `Id` = 0 | `{ready, Info}`, `{startup_error, Problems}`, `{memory_limit, Rss}`, `{log, Level, Msg}`, `{async_result, TaskRef, R}`, `{loop_exit, R}` |
| 5 | Erlang to child | control, `Id` = 0 | `{interrupt, Target}`, `{cancel, Id}`, `stop_loop`, `{shm_close, Id}` |

Ids: Erlang numbers its requests from 1 (`next_id`); the child numbers its
status-3 requests independently; replies carry the id of the request they
answer. Control and events use id 0 and never get a reply.

ETF: `_etf.py` encodes what `py_convert.c` would produce for the same
Python value, with two differences worth knowing: `Pid`, `Ref` and `Port`
are opaque and keep their raw bytes, and the child can create atoms
(`binary_to_term/1` is not called with `safe`). Shared handles
(`{'$py_shm', Id, Path, Size}`, `{'$py_buffer', Id, Path, Ring}`) are plain
tuples on the wire and become `SharedMemory` / `SharedBuffer` on arrival
(`_shm.from_term`); NIF resources cannot cross and the API answers
`{error, not_supported_in_isolated}`.

Handshake (`py_isolated:handshake/1`): the child sends `{ready, Info}`,
Erlang sends `{init, Opts}` (status 0, id 1) and the preload `exec`,
serving status-3 requests that arrive meanwhile with a bounded callback
runner. Anything else during the handshake fails the start.

Interrupt: `{interrupt, Target}` is read by the child's reader thread, which
sends SIGUSR1 to the main thread when `Target` is the request executing
now (top of `_exec_stack`); the signal handler raises `_Interrupted`
(a `KeyboardInterrupt` subclass) only while a request runs. `{cancel, Id}`
marks a queued request so it is answered `{error, cancelled}` instead of
run. Erlang arms a SIGKILL backstop bound to the request id when it sends
an interrupt; a reply for that id cancels it.

Flow control: the socket buffers are 1 MB each way; a frame larger than
that is written in pieces by both sides. Bulk data goes through shared
memory, not the socket (see [isolated](isolated.md)).

## 5. Shared buffer ring

`py_buffer:new(#{shared => true})` allocates a `py_shm` region used as a
ring. The first page is a header written by Erlang after each `write/2`
(write position, closed flag, ring size) and read by Python; the
`py_shm` server keeps the consumed position from the callbacks below, not
from the header. Notifications are callbacks, so they work in every mode:

| Callback | Called by | Returns |
|---|---|---|
| `_py_buffer_wait(Id, ReadPos)` | reader, before blocking | `{WPos, Closed}` once `WPos > ReadPos` or the buffer is closed |
| `_py_buffer_consumed(Id, N)` | reader, after a read | `ok`; a writer blocked on space wakes up |
| `_py_buffer_state(Id)` | reader, on (re)map | `{WPos, Closed}` without waiting |

All three answer `{error, closed}` for an unknown id. The header is only
read after a callback returned, so ordering follows the round trip and the
Python side needs no fence.

## Changing a protocol

- Add a request kind: the message in `py_context` (API and embedded loop),
  the `ctx_request_type_t` and execute function in C, the `request/6`
  clause in `py_isolated`, and `_on_request` in `_isolated.py`.
- Add a control: `send_frame(Child, 0, ?STATUS_CONTROL, Term)` in
  `py_isolated` and `_on_control` in `_isolated.py`; controls must be safe
  to handle on the reader thread.
- Add a child event: `self.event(...)` in `_isolated.py` and an
  `{ok, {0, ?STATUS_EVENT, ...}}` clause in `drain_socket` or `handshake`.
- Change the response body: three writers and two parsers listed in 3.4.
- Never change the frame header: the child, the callback pipe reader and
  the async pipe parser share it.
