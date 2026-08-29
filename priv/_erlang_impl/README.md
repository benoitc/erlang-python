# _erlang_impl

The Python half of the `erlang` module. `priv/` is on `sys.path` of every
interpreter erlang_python starts. In embedded modes the C module `erlang`
(`c_src/py_callback.c`) is the primary and delegates to this package for the
asyncio loop, channels, servers and helpers; in an isolated child there is
no C module and `_isolated.py` builds the whole `erlang` module from here.

| Module | What it is | Embedded | Child |
|---|---|---|---|
| `__init__.py` | Public surface: `run`, `sleep`, `spawn_task`, `new_event_loop`, `install`, `atom`, `channel`, `byte_channel`, `server`, `reactor` | yes | partly (the child shim re-exports `server` and reimplements the rest on the stdlib loop) |
| `_loop.py` | `ErlangEventLoop`: asyncio loop whose readiness comes from `enif_select` and timers from `erlang:send_after`, through the `py_event_loop` C module | yes | no |
| `_policy.py`, `_transport.py` | Loop policy and transports for the loop above | yes | no |
| `_reactor.py` | Protocol-based reactor on fds Erlang owns | yes | no |
| `_channel.py`, `_byte_channel.py` | `Channel`, `ByteChannel` over NIF resources | yes | no |
| `_server.py` | `serve(listen_fd, factory)`, `adopt(fd, factory)`, `stop_serving`: plain asyncio on an fd handed over by Erlang | yes | yes |
| `_sandbox.py`, `_subprocess.py` | Audit hook that blocks fork/exec inside the VM | yes | no |
| `_mode.py` | Detects the execution mode | yes | yes |
| `_etf.py` | ETF codec with the `py_convert.c` type mapping; opaque `Pid`, `Ref`, `Port` keep their raw bytes | no | yes |
| `_isolated.py` | Child runtime: frame parser, reader thread, re-entrant main loop, interrupt signal, execution stack, asyncio loop, `SharedMemory` conversion, the `erlang` shim | no | yes |
| `_shm.py` | `SharedMemory` and `SharedBuffer` over `mmap`; `from_term` cache; buffer flow control through Erlang callbacks | yes | yes |

Conventions: modules starting with `_` are internal; user code imports
`erlang`. Anything a mode cannot support raises `RuntimeError` with the
mode in the message rather than degrading silently (`_isolated.py`,
`_subprocess.py`).

Tests for the loop live in `priv/test_erlang_loop.py` and `priv/tests/`;
the Erlang suites drive everything else.
