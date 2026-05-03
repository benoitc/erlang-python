# Documentation Snippet → Test Coverage Audit

This file maps every public `py:*` and `erlang.*` API shown in the
README and `docs/*.md` to at least one `*_SUITE.erl` test that
exercises it. Update this table whenever a documented API is added,
renamed, or removed.

## Erlang public API (`src/py.erl` exports)

| API | Documented in | Test suite | Test case |
|---|---|---|---|
| `py:call/3` | README, getting-started.md, pools.md | `py_SUITE` | `test_call_math`, `test_call_json` |
| `py:call/4` | README, pools.md | `py_SUITE` | `test_call_kwargs` |
| `py:call/5` | pools.md | `py_pool_SUITE` | router-bound call tests |
| `py:cast/3` | README, threading.md | `py_SUITE` | `test_cast` |
| `py:cast/4` | README | `py_SUITE` | `test_cast_kwargs` |
| `py:cast/5` | (internal Ctx variant) | `py_SUITE` (indirect) | covered via `test_cast` |
| `py:eval/1` | README, getting-started.md | `py_SUITE` | `test_eval` |
| `py:eval/2` | README, getting-started.md | `py_SUITE` | `test_eval`, `test_eval_complex_locals` |
| `py:eval/3` | README | `py_SUITE` | `test_timeout` |
| `py:exec/1` | README, getting-started.md | `py_SUITE` | `test_exec` |
| `py:exec/2` | scalability.md (with Ctx) | `py_context_SUITE` | exec tests |
| `py:spawn_call/3` | README | `py_SUITE` | `test_spawn_call` |
| `py:await/1`, `py:await/2` | README | `py_SUITE` | `test_spawn_call` |
| `py:async_call/3` | README, asyncio.md, threading.md | `py_SUITE` | `test_asyncio_call` |
| `py:async_await/1`, `py:async_await/2` | README, asyncio.md | `py_SUITE` | `test_asyncio_call` |
| `py:async_gather/1` | README, asyncio.md | `py_SUITE` | `test_asyncio_gather` |
| `py:async_gather/2` | (timeout variant) | `py_SUITE` | `test_asyncio_gather_timeout` |
| `py:parallel/1` | README, scalability.md | `py_SUITE` | `test_parallel_execution` |
| `py:subinterp_supported/0` | scalability.md | `py_SUITE` | `test_subinterp_supported` |
| `py:execution_mode/0` | README, scalability.md | `py_SUITE` | `test_execution_mode` |
| `py:context/0`, `py:context/1` | README, process-bound-envs.md | `py_context_SUITE` | context creation |
| `py:state_set`, `state_fetch`, `state_incr`, `state_decr`, `state_remove`, `state_keys`, `state_clear` | README, getting-started.md | `py_SUITE` | `test_shared_state` |
| `py:register_function/2`, `register_function/3` | README, threading.md | `py_SUITE` | `test_erlang_callback`, `test_erlang_callback_mfa` |
| `py:unregister_function/1` | README | `py_SUITE` | `test_erlang_callback` |
| `py:stream/3`, `py:stream_eval/1` | README, streaming.md | `py_SUITE` | `test_streaming`, `test_stream_function` |
| `py:dup_fd/1` | reactor.md | `py_fd_ops_SUITE` | `dup_fd_test` |
| `py:configure_logging/0,1` | README, logging.md | `py_logging_SUITE` | logging tests |
| `py:enable_tracing/0`, `disable_tracing/0`, `get_traces/0`, `clear_traces/0` | README, logging.md | `py_logging_SUITE` | tracing tests |
| `py:activate_venv/1`, `deactivate_venv/0`, `venv_info/0` | README | `py_SUITE` | `test_venv` |
| `py:memory_stats/0`, `py:gc/0` | README | `py_SUITE` | `test_memory_stats`, `test_gc` |
| `py:tracemalloc_start/0`, `tracemalloc_stop/0` | README | `py_SUITE` | indirect via memory tests |
| `py:version/0` | (no doc) | not exercised | — |

## Python public API (`erlang.*` and `from erlang import ...`)

| API | Documented in | Test suite |
|---|---|---|
| `erlang.call(name, *args)` | README, threading.md | `py_SUITE` (`test_erlang_callback`) |
| `erlang.async_call(name, *args)` | threading.md, asyncio.md | `py_async_e2e_SUITE`, `py_thread_callback_SUITE` |
| `erlang.send(pid, msg)` | README, threading.md | `py_pid_send_SUITE` |
| `erlang.atom(name)` | (Python-side wrapper) | `py_owngil_features_SUITE`, `py_test_pid_send.py` |
| `erlang.Pid` | type-conversion.md | `py_pid_send_SUITE` |
| `erlang.Span`, `erlang.trace()` | logging.md | `py_logging_SUITE` |
| `erlang.run(main)`, `erlang.new_event_loop()` | asyncio.md | `py_asyncio_compat_SUITE`, `py_event_loop_SUITE` |
| `erlang.install()` | asyncio.md | `py_asyncio_policy_SUITE` |
| `erlang.spawn_task()` | asyncio.md | `py_async_task_SUITE` |
| `erlang.sleep()` | asyncio.md | `py_erlang_sleep_SUITE` |
| `erlang.SharedDict` | shared-dict.md | `py_SUITE` (`test_shared_dict_*`) |
| `erlang.Channel`, `erlang.ByteChannel` | channel.md, buffer.md | `py_channel_SUITE`, `py_byte_channel_SUITE` |
| `erlang.reactor.Protocol` and friends | reactor.md | `py_reactor_SUITE` |
| `erlang.state_set / state_get / state_delete / state_keys / state_incr / state_decr` (registered callbacks) | README, getting-started.md | `py_SUITE` (`test_shared_state`) |

## Notes for future audits

1. The `py:cast/5` (Ctx, Module, Func, Args, Kwargs) variant is not
   directly exercised. It is a one-line wrapper around
   `py_context:call/5` and shares its kwargs marshaling with
   `py:call/5`. If a future change diverges them, add a dedicated test.

2. `py:version/0` is exported but not documented. Either document it
   or drop the export.

3. `from erlang import run_command` (`docs/migration.md:426`) and
   `from erlang import run_shell` (`docs/security.md:103`) refer to
   user-side registrations, not built-ins. The surrounding prose makes
   the example pattern explicit; do not mistake these for documented
   APIs.

4. `priv/_erlang_impl/_ssl.py` was removed in this audit (no importer).
   Do not reintroduce unless an importer is wired up.

5. `priv/_erlang_impl/_subprocess.py` is intentionally a stub that
   raises `NotImplementedError` from `create_subprocess_shell` and
   `create_subprocess_exec`. It is lazy-imported by
   `priv/_erlang_impl/_loop.py:948,958` when an asyncio caller invokes
   `loop.subprocess_shell()` / `loop.subprocess_exec()`, which are
   blocked because `fork()` would corrupt the Erlang VM. Do not delete.
