# 0009: An isolated child reaches only what it was granted

Since 5.1.0. Code: `src/py_caps.erl`, `priv/_erlang_impl/_caps.py`, the
prologue of `priv/py_isolated_child.py`.

## Situation

Isolated mode bounded what Python could *consume*: memory, CPU, time, and a
crash. It bounded nothing it could *reach*. The child ran as the node's user
with the node's environment, could read and write every file that user
could, dial anywhere, spawn processes, and truncate the shared memory
regions it was handed, which turns the mapping the VM holds into a `SIGBUS`.
The audit hook the embedded modes install was never installed there.

The first sketch was a deny list of dangerous operations. That is the wrong
shape: it enumerates what to stop, so it is wrong the moment something new
appears, and it says nothing about what a job is supposed to touch.

## Decision

Grant capabilities instead. `py_context:new(#{mode => isolated, caps => ...})`
names directories with an access level, environment variables, and network
rules; nothing else is reachable. No `caps` key leaves existing behaviour
alone, so this is additive.

The model, the option shape, the refusal semantics and the test list are
taken from `erlang_wasm`'s WASI preview 1 implementation rather than
invented: `{Proto, Addr, Port}` rules, addresses and never host names,
resolution as its own capability, binding checked against `listen` and not
`connect`, IPv4-mapped folding, a malformed rule raising where the grant is
written. A grant means the same thing in both projects.

Enforcement is an audit hook in the child, installed last in the prologue so
the runtime's own imports are outside the grants and everything after them
is inside. Paths are resolved a component at a time with `openat` and
`O_NOFOLLOW` from the descriptor of the grant, following symlinks by hand:
that is erlang_wasm's `native` backend, which needed a C NIF there because
Erlang has no `openat`, and needs none here because Python has one.

Not chosen: routing every open through Erlang so `wasi_fs` could enforce it
directly. It would share one implementation and close the check-to-use
window, but it makes an `open` a socket round trip from arbitrary code,
including the child's own reader thread, and the deadlock surface is not
worth it at 25 microseconds a call.

## What this is not

Three review rounds all found the same shape of defect: a way for Python to
step around a check written in Python. Each was real and each was closed,
but the pattern is the point. An audit hook is a cooperative policy, and
the honest split is:

* `caps` is for code you partly trust. It stops mistakes and casual misuse,
  and it makes what a job may touch reviewable.
* The process, its rlimits and `kill_after` are what hold against code that
  is trying to get out.
* Kernel enforcement (Landlock, seccomp, or the state and hook moved into
  the NIF) is the point at which `caps` may be described as protection
  against adversarial code. It is not there yet, and the guide says so
  rather than implying otherwise.

## Consequences

- It is a policy over Python, not a boundary. A C extension calling
  `open(2)`, or a thread swapping a path between the check and the kernel's
  resolution, is not stopped. The guide says so in those words.
- The grants have to live in the hook's closure rather than in module
  state, and nothing inside the interpreter may widen them. An earlier
  version kept them in a module attribute and let `_shm` add region paths:
  both were levers any Python could pull. Regions are now granted from
  Erlang, as the directory holding them, opened and nothing more.
- Enforcement can only cover what CPython announces. Calls that create
  something and raise no audit event are removed from `os` and `posix`
  instead, and the ones that only observe are documented as visible.
- Nothing on the decision path may be reachable by name, including the
  re-entrancy guard, the event tables and the `os` functions the check
  itself calls. The guard is set around the containment walk alone, since a
  wider one would leave a user `__fspath__` running with enforcement off.
- Shared memory does not combine with a capability set. A region arrives as
  a path, granting the directory would hand over every region the node
  owns, and an open-only grant cannot stop truncation because
  `file.truncate()` announces nothing. Passing the descriptor, sealed on
  Linux, is the way to make it work.
- `ctypes` must be refused, or every rule is advisory. Libraries that need
  it cannot run under a capability set.
- The interpreter's own `sys.path` is granted automatically, or nothing
  imports. A capability set therefore always grants reading the standard
  library, as WASI's preopened sysroot does.
- An open inside a grant costs about 11 microseconds more, growing with path
  depth, so grants should sit close to what is read.
- Landlock on Linux consumes the same table and would make it a boundary.
  The table is shaped for that.
