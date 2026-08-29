# 0005: The isolated context process is a gen_statem

Since 5.0.0. Code: `src/py_isolated.erl`, entered from `init/4` in `py_context`
with `gen_statem:enter_loop/5`.

## Situation

The embedded context process is a hand-written receive loop in
`py_context`. The isolated process has more to track: a request in
flight, requests to hold while the child restarts, a running loop with a
grace period, an interrupt with a kill backstop bound to one request id,
and callback processes whose nested requests must pass while others
wait. A first version as a receive loop mirrored `py_context` but every
wait needed its own selective receive and its own timer bookkeeping.

## Decision

`py_isolated` is a `gen_statem` (`handle_event_function`, state enter
calls) with states `idle`, `{busy, Id}`, `looping`, `stopping_loop` and
`{restarting, Reason}`. Requests that must wait are `postpone`d and
replayed by the behaviour in arrival order; timers are `state_timeout`
and named generic timeouts (`{timeout, kill}`) cancelled by the state
change that makes them moot. It is spawned with `proc_lib` so it keeps
the process identity and message protocol of `py_context`.

Not chosen: sharing the receive loop with `py_context` (the two have
different failure models: a child can die and restart, a thread cannot),
or a `gen_server` with a state field (postpone and state timeouts would
have to be reimplemented).

## Consequences

- `sys:get_state/1` shows what a context is doing and `sys:trace/2`
  prints every event; the state machines page is a transcription of the
  callback module.
- Callers do not see the behaviour: messages and replies are those of
  `py_context`.
- The two context processes are different code. A change to the message
  protocol touches both.
