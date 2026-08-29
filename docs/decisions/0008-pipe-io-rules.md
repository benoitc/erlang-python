# 0008: Pipe I/O is non-blocking, deadlined and waited with poll

Since 3.1.0 (deadlines), 5.0.0 (poll). Code: `read_with_timeout` and
`write_all_with_deadline` in `c_src/py_nif.h`, the pipe setup in
`c_src/py_thread_worker.c` and `c_src/py_callback.c`.

## Situation

Callback responses are written by Erlang processes on dirty I/O
schedulers into pipes read by Python threads. A blocking write to a
stalled reader pinned a dirty scheduler for good; a short read or write
left the framed protocol out of phase with no way back. Later, a CI run
with more than 1024 open files showed that `select()` cannot watch such a
descriptor at all, and the first thread worker created past that point
took every later thread callback down with it.

## Decision

Write ends are `O_NONBLOCK`; every write is `write_all_with_deadline`
and every read `read_with_timeout`, both waiting with `poll()`. A
partial frame is never recovered in band: the thread worker is poisoned
and replaced, and the context pipe is closed only when its thread has
been joined. The coordinator reports a failed ready signal instead of
leaving Python to time out.

## Consequences

- No scheduler thread waits on Python without a bound.
- A desynchronised pipe costs one worker, not a hang.
- `select()` and `<sys/select.h>` do not appear in the NIF; a review
  that sees them come back should ask why.
