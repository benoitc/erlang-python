# Capabilities

This guide covers `caps`, the option that says what an isolated child may
reach: which directories, which environment variables, which addresses.
Python that asks for anything else is refused. It is the WASI model, and
the vocabulary is the same as
[erlang_wasm](https://github.com/benoitc/erlang_wasm)'s, so a grant means
the same thing in both.

**Read this before you rely on it.** `caps` is a cooperative policy over
Python, not a boundary. It stops code that is not trying to get out, and it
makes what a job may touch explicit and reviewable. It does not stop code
that is trying to get out: a C extension calling `open(2)` never reaches
the audit hook it is built on. Use it for code you partly trust; use the
process boundary in [Isolated Contexts](isolated.md) for the rest, and see
[what holds and what does not](#what-holds-and-what-does-not) for the
detail.

Read [Isolated Contexts](isolated.md) first: `caps` only applies there.

## Grant what it needs

```erlang
{ok, Ctx} = py_context:new(#{
    mode => isolated,
    caps => #{
        dirs => [{"/srv/models", read},
                 {"/var/data/job42", write}],
        env  => #{<<"MODEL_DIR">> => <<"/srv/models">>},
        net  => #{connect => [{tcp, <<"10.0.0.0/8">>, {5432, 5432}}],
                  resolve => deny}
    }}),
{ok, _} = py_context:call(Ctx, scorer, run, [<<"job42">>]).
```

Inside the child, ordinary Python works inside the grants and fails outside
them:

```python
open('/srv/models/weights.bin', 'rb')   # granted
open('/var/data/job42/out.csv', 'w')    # granted
open('/etc/passwd')                     # PermissionError
open('/srv/models/w', 'w')              # PermissionError: read grant
```

Leave a key out and there is none of it. `caps => #{}` grants nothing but the
interpreter's own files, and no `caps` key at all is the behaviour you have
today: the child holds every authority the user running the node holds.

## What each key grants

| key | grants | leaving it out means |
| --- | --- | --- |
| `dirs` | directories, `read` or `write` | no filesystem beyond the interpreter's own |
| `env` | environment variables | zero variables, **not** the node's |
| `net` | sockets, to the addresses you name | no network at all |

`read` covers opening, reading and listing. `write` adds creating, renaming,
unlinking and truncating. Rights apply to everything below the directory, so
a `read` grant yields no writable file however the code opens it.

Always granted, because nothing works otherwise: the interpreter's own
`sys.path`, `sys.prefix` and `sys.base_prefix` for reading, so imports work,
and `/dev/null`, `/dev/zero`, `/dev/random`, `/dev/urandom`. WASI preopens
its sysroot for the same reason. Directories you list in the `paths` option
are granted for reading too, since that option tells the child to import
from them.

The `env` option and `caps` cannot be used together. The option adds
variables and a grant says what the whole environment is, so taking both
would let the option quietly win; `caps.env` is the one that names an
environment, and using both is `{error, {bad_caps,
env_option_conflicts_with_caps_env}}`.

## Name a network

```erlang
net => #{connect => [{tcp, <<"10.0.0.0/8">>, {8000, 8099}}],
         listen  => [{tcp, <<"127.0.0.1">>, 8080}],
         resolve => allow}
```

A rule is `{Proto, Addr, Port}`. `Proto` is `tcp` or `udp`, `Addr` is an
address tuple, a binary address or a binary CIDR, and `Port` is an integer, a
`{Lo, Hi}` range, or `any`.

Four things to know before writing your first grant:

- **`connect` and `listen` are separate**, and neither implies the other.
  Binding claims a local address, which is what `listen` grants, so code
  wanting a particular source port needs a `listen` rule for it.
- **You name addresses, never names.** There is no rule that says
  `example.com`: a name would have to be resolved to be checked and resolved
  again to be used, and the two answers can differ. `resolve` is its own
  capability, off unless granted, and what it returns carries no authority.
  Code may learn an address it cannot reach, and the connect is refused then.
- **`::ffff:127.0.0.1` is `127.0.0.1`.** IPv4-mapped addresses are folded
  before matching, so the mapped notation cannot walk past an IPv4 rule.
- **Nothing is denied implicitly.** `<<"0.0.0.0/0">>` really does include
  link-local and cloud metadata addresses. Name what you mean.

A socket Erlang opened and handed over with `py_context:pass_fd/2` needs no
rule: the child was given the descriptor, and the descriptor is the
capability. That is how you serve on a port under a capability set.

```erlang
{ok, LSock} = gen_tcp:listen(8080, [binary, {active, false}]),
{ok, Fd} = inet:getfd(LSock),
{ok, ChildFd} = py_context:pass_fd(Ctx, Fd),
ok = py_context:start_loop(Ctx),
{ok, _} = py_context:submit_await(Ctx, myapp, serve, [ChildFd]).
```

## Shared memory does not combine with this yet

A `py_shm` region reaches the child as a path, so under a capability set it
is refused like any other ungranted path. Granting it would mean granting
the directory the node keeps every region in, which hands over every
region, and an open grant cannot prevent truncation anyway because
`file.truncate()` announces nothing. A truncated region is a `SIGBUS` in
the VM that mapped it, so the half-measure is worse than the refusal.

The fix is to pass the region's descriptor rather than its name, with
`memfd_create` and `F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL` on Linux,
which keeps a writable mapping while making the object unresizable, and
cooperative-only handling where sealing does not exist. Until that lands,
use shared memory or a capability set, not both.

## What is refused

Every route below has its own test case in `py_isolated_caps_SUITE`. All are
refused with a `PermissionError` and never a `FileNotFoundError`, so the
error cannot be used to find out what exists outside a grant.

| the code asks for | it gets |
| --- | --- |
| `note.txt`, `./note.txt`, `sub/deep.txt` | opened |
| `sub/../note.txt` | opened: it never leaves the grant |
| `../secret/key.txt` | refused |
| `/etc/passwd` | refused |
| `escape.txt`, a symlink out of the grant | refused |
| `outdir/key.txt`, through a symlinked directory | refused |
| `sub/../../secret/key.txt` | refused |
| a symlink cycle | refused |
| `missing.txt` inside a grant | `FileNotFoundError` |
| `subprocess.run`, `os.fork`, `os.exec*` | refused |
| `ctypes.CDLL` | refused |
| `os.kill` at another process, `os.killpg` | refused |
| `socket.gethostbyname` and every other resolver | refused unless `resolve` |
| `os.mkfifo`, `os.mknod` | not there: see below |
| a Unix-socket address, connect or bind | refused |

Signalling is refused because the child usually shares the node's user and
its parent is the BEAM, so an unchecked `os.kill` is a way to take the node
down. Signalling itself is allowed. Every resolver is gated, not only
`getaddrinfo`: a name lookup is a message to whoever answers it, so gating
one of them would leave the rest as a way out. A Unix-socket address is
refused rather than checked as a file, because reaching one is talking to
whatever is behind it, which a directory grant says nothing about; a
descriptor Erlang passed over with `py_context:pass_fd/2` is unaffected.

Subprocesses are refused outright: a capability set names what may be
reached, and another process is not something you granted. `ctypes` is
refused because it reaches libc directly, which would make every rule above
advisory. A library that needs `ctypes` cannot run under a capability set.

## What holds and what does not

Enforcement is a CPython audit hook, so the whole of what it can see is
what CPython announces, and everything below follows from that.

**What holds.** Python code that asks for a path, an address or a process
outside the grants is refused, whether it asks through `open`, `os.open`,
`pathlib`, numpy or any other library, because the event is raised by the
interpreter rather than by the caller. Path containment is resolved by the
kernel one component at a time, so `..`, absolute paths, symlinks out of a
grant and symlinked directory prefixes are all refused rather than
lexically guessed at. Nothing on the decision path is reachable by name:
the grants, the tables and even the `os` functions the check uses are bound
into the hook's closure when it is installed, so assigning to this module
changes nothing.

**What does not hold.**

- A C extension calling `open(2)` or `connect(2)` never reaches an audit
  hook. Neither does `file.truncate()` or `mmap.resize()`, which CPython
  does not announce, so a writable descriptor can always shorten its own
  file. That is part of what a `write` grant grants.
- A thread that replaces a path between the check and the kernel's own
  resolution is not stopped. Do not point a `write` grant at a directory
  another party writes to concurrently.
- `os.stat`, `os.access` and the rest of the calls that observe without
  reaching are left alone, so what exists outside a grant stays visible
  even though reading it does not.
- Closure state is a bar, not a wall. Python exposes its own object graph,
  and code that goes looking can reach a hook's cells.

**What would make it hold.** A kernel. Landlock on Linux takes the same
grant table and enforces it below the interpreter, which is the point at
which a C extension stops being an exception; moving the state and the hook
into the NIF would take the rest. Neither is here yet. Until then, the
boundary you have is the process: `rlimits`, `kill_after`, and the
supervision in [Isolated Contexts](isolated.md).

## Cost

The check is an audit hook, so it runs on every open, and paths are resolved
one component at a time against the descriptor of the grant. Measured on
macOS with Python 3.14, an open inside a grant costs about 11 microseconds
more than an unguarded one, and the cost grows with the depth of the path
below its grant. Grant close to what the code reads: `/srv/models` rather
than `/`.

Nothing else changes. Calls, results, shared memory and interrupts are what
they were.

## Check what a child got

```erlang
{ok, Info} = py_context:child_info(Ctx),
maps:get(caps, Info).
```

```python
import erlang
erlang.caps()      # None when no capability set was given
```

Both report the grants as the child holds them, including the automatic
ones, and `strict_paths` tells you whether the platform resolved paths
component by component or fell back to a lexical check.

## See also

- [Isolated Contexts](isolated.md) for the process boundary itself
- [Security](security.md) for what the embedded modes do instead
- [decision 0009](decisions/0009-child-capabilities.md) for why it is shaped
  this way
