# Contributing

This page is for people changing erlang_python itself: how to build and
test it, what a change of each kind touches, and how to send it. For using
the library, start with [Getting Started](docs/getting-started.md). Design
background is in [architecture](docs/architecture.md), file ownership in
[code map](docs/code-map.md), message shapes in
[protocols](docs/protocols.md). AI agents also follow
[AGENTS.md](https://github.com/benoitc/erlang-python/blob/main/AGENTS.md).

## Write for the person who reads it

Every issue, pull request and comment is read by a person. Make it easy for
them.

- Read what you send before you send it. You are responsible for every
  line, whether you wrote it or a tool did.
- Use plain English and short sentences. Say what happens, what you
  expected, and how to reproduce it. Explain any term a reader may not
  know.
- Don't inflate it. No buzzwords, no severity scores, CVSS or CWE labels,
  no "critical" for a bug, no long generated write-ups. If it takes a page
  to explain, cut it down until it doesn't.
- A bug report gives: the erlang_python, OTP and Python versions, the OS,
  the context mode (`worker`, `owngil`, `isolated`, a session), a minimal
  reproduction, what you got and what you expected. For a crash of the
  node, the C stack trace or the ASan report helps most.
- A pull request says what was wrong and why it matters, the fix in a few
  lines, and any breaking change. Link the issue. Don't walk through the
  diff file by file; the reviewer reads the code.
- A fix comes with a test that fails without it.

AI tools are fine to use. Before you open anything, you must have read the
change, run the tests, and be able to answer questions about it yourself.

Maintainers read every issue and pull request before acting on it. We close
without further review:

- text we cannot understand after one request to clarify;
- descriptions that don't match the code or the reproduction;
- reports that cannot be reproduced;
- submissions opened by an automated agent that no person has read.

## Security issues and bugs

Python code that erlang_python runs holds every authority of the user
running the node. The sandbox keeps Python from forking the VM, and
isolated mode bounds what a child may consume and contains its crashes;
neither confines what Python may reach. [Security](docs/security.md) and
the limits in [Isolated Contexts](docs/isolated.md#limits) say what each
one bounds.

So anything Python code can do is a bug, not a security issue. That
includes reports described as a sandbox escape or a denial of service,
when they come from:

- Python getting past the audit hook;
- an isolated child or a session reading files, the network or the
  environment;
- a limit (rlimits, cgroups, `clear_env`) that was not applied;
- a C extension crashing the node, or atoms sent from Python filling the
  atom table.

Report these as a normal bug, in public. The fix goes in a public pull
request and in the CHANGELOG under Fixed. We don't publish advisories or
request CVEs for them, and advisories filed for them are closed with a link
to the fix.

Only problems that give something to someone who cannot already run code
on the node go through [SECURITY.md](https://github.com/benoitc/erlang-python/blob/main/SECURITY.md): memory corruption or
code execution in the VM from data, not code, that passes through the
library (terms, bytes in channels, buffers and shared memory), or the
library loading or running code from a place the configuration did not
name.

## Build

You need OTP 27 or later, Python 3.12 or later with headers, CMake and a C
compiler.

```sh
rebar3 compile              # runs do_cmake.sh / do_build.sh, builds priv/py_nif.so
PYTHON_CONFIG=python3.13-config rebar3 compile   # pick an interpreter
```

Notes:

- `c_src/py_nif.c` includes the other `.c` files; there is no per-file
  compile. A change in any `c_src` file rebuilds the one NIF.
- The interpreter used at build time is the one embedded at run time.
  `python3-config` on `PATH` is the default.
- CMake caches the interpreter and the build tool in `_build/cmake`. When
  either moves, `rebar3 compile` prints a CMake error but exits 0 and keeps
  the old `py_nif.so`. Run `rm -rf _build/cmake` when you switch Python
  versions, build flags or toolchains, and check what the NIF links against
  (`otool -L` on macOS, `ldd` on Linux).

## Test

```sh
rebar3 ct --readable=compact                       # everything
rebar3 ct --suite test/py_isolated_SUITE           # one suite
rebar3 ct --suite test/py_context_SUITE --case test_call   # one case
STRESS=1 rebar3 ct --suite test/py_worker_loop_stress_SUITE  # stress suites
```

Notes:

- `test/test.config` turns memory limits on for the whole run; it is
  applied automatically.
- The `test` profile pulls `iommap` for the shared memory suites; users
  add it to their own deps.
- Free-threaded Python: build and run with `PYTHON_GIL=0` and a `3.13t`
  interpreter; cases that need the GIL skip themselves.
- ASan: configure CMake by hand, then let rebar3 pick the objects up:

```sh
rm -rf _build && mkdir -p _build/cmake && cd _build/cmake
cmake ../../c_src -DENABLE_ASAN=ON -DENABLE_UBSAN=ON && cmake --build . && cd ../..
rebar3 compile
LD_PRELOAD=$(gcc -print-file-name=libasan.so) ASAN_OPTIONS=detect_leaks=0 rebar3 ct
```

A test must fail for one reason. A case that also passes when the call
crashes, or that asserts a value it computed itself, is worse than no test
because it reads as coverage. A feature comes with unit cases, a bench
escript in `examples/` with numbers before and after, and a stress suite
gated by `STRESS=1`.

### FreeBSD by hand

CI covers FreeBSD, but the isolated mode (procctl, no cgroups, no
`/dev/shm`) is easier to debug in a local VM. On an Apple Silicon Mac:

```sh
# 1. image (arm64 on Apple Silicon, amd64 elsewhere)
curl -O https://download.freebsd.org/releases/VM-IMAGES/14.1-RELEASE/aarch64/Latest/FreeBSD-14.1-RELEASE-arm64-aarch64.qcow2.xz
xz -d FreeBSD-14.1-RELEASE-arm64-aarch64.qcow2.xz
qemu-img resize FreeBSD-14.1-RELEASE-arm64-aarch64.qcow2 +20G

# 2. boot headless, ssh on host port 2222
qemu-system-aarch64 -M virt -accel hvf -cpu host -m 4096 -smp 4 \
  -bios /opt/homebrew/share/qemu/edk2-aarch64-code.fd \
  -drive file=FreeBSD-14.1-RELEASE-arm64-aarch64.qcow2,if=virtio,format=qcow2 \
  -netdev user,id=n0,hostfwd=tcp::2222-:22 -device virtio-net-pci,netdev=n0 \
  -nographic -serial mon:stdio
```

On the console, log in as `root` (no password), set one, and enable ssh:

```sh
passwd
sysrc sshd_enable=YES
sed -i '' 's/^#PermitRootLogin no/PermitRootLogin yes/' /etc/ssh/sshd_config
service sshd start
pkg install -y erlang-runtime28 python313 py313-numpy cmake gmake git
fetch -o /root/rebar3 https://github.com/erlang/rebar3/releases/download/3.25.0/rebar3
chmod +x /root/rebar3
```

Then from the host, ship the tree and run:

```sh
git archive --format=tgz -o /tmp/ep.tgz HEAD
scp -P 2222 /tmp/ep.tgz root@127.0.0.1:/root/
ssh -p 2222 root@127.0.0.1 'rm -rf ep && mkdir ep && tar -C ep -xzf ep.tgz && cd ep \
  && export PATH=/usr/local/lib/erlang28/bin:$PATH PYTHON_CONFIG=python3.13-config \
  && /root/rebar3 compile && /root/rebar3 ct --readable=compact'
```

`erlang-runtime28` installs outside the default `PATH`; `pkg info -l
erlang-runtime28 | grep bin/erl` shows where. Use `-accel kvm` and the
amd64 image on Linux hosts.

## Before you commit

Run what CI runs. These are the checks that gate a pull request:

```sh
rebar3 fmt                    # format first, then check the rest
rebar3 compile
rebar3 ct --readable=compact
rebar3 fmt --check
rebar3 lint
rebar3 xref
rebar3 dialyzer
make lint-docs                # snippets in README.md, CONTRIBUTING.md and docs/
make check-code-map           # every file in the code map
rebar3 ex_doc
```

erlfmt formats `src`, `test`, `examples`, `scripts` and `rebar.config`; the
C and Python sources have no formatter, so follow the style around your
change. Elvis reads `elvis.config`. The modules that broke a rule when the
rules were adopted are listed in that rule's `ignore`; a new module follows
every rule, and a module comes off a list when it is reworked, never onto
one to make a change pass.

CI runs the matrix in `.github/workflows/ci.yml`: OTP 27 to 29 with Python
3.12 to 3.14 on Ubuntu and macOS, FreeBSD 14 through `vmactions/freebsd-vm`,
free-threaded 3.13t, ASan, the lint job (formatting, Elvis, xref, dialyzer,
code map) and the documentation build. A pull request is merged when all of
them are green.

## Recipes

Every recipe ends with the same steps: a test case, the checks above, and
an entry in `CHANGELOG.md` under the unreleased version.

### Add a NIF

1. Implement `static ERL_NIF_TERM nif_x(ErlNifEnv*, int, const ERL_NIF_TERM[])`
   in the `c_src` file that owns the area (see `c_src/README.md`).
2. Add `{"x", Arity, nif_x, Flags}` to the `PY_*_NIFS` macro at the end of
   that file (`nif_funcs[]` in `c_src/py_nif.c` concatenates them; NIFs
   that live in `py_nif.c` go in its own block there). `Flags` is `ERL_NIF_DIRTY_JOB_CPU_BOUND` or
   `ERL_NIF_DIRTY_JOB_IO_BOUND` when the NIF can block or run Python, `0`
   otherwise.
3. Add the stub, its `-spec` and a `@doc` to `src/py_nif.erl`, and the
   export.
4. Follow the rules in `c_src/README.md`: only the context thread touches
   a context's Python objects; release the GIL around every blocking
   wait; respect the lock order on `py_context_t`.
5. Test it from a suite through the Erlang API that uses it, not through
   `py_nif` directly, unless it is a test helper.

### Add an `erlang.*` function for Python

The `erlang` module has three implementations that must agree:

1. Embedded modes, C: add `erlang_x_impl` and its entry in the method
   table of `c_src/py_callback.c` (`PyMethodDef erlang_methods`). Names
   starting with `_` are internal helpers for the Python package.
2. Embedded modes, Python: if the function is written in Python, add it to
   `priv/_erlang_impl/__init__.py` and to `__all__`; the C module copies
   it onto `erlang` at import (see the bootstrap code near the end of
   `py_callback.c`).
3. Isolated mode: add it to `install_erlang_module` in
   `priv/_erlang_impl/_isolated.py`. Anything the child cannot support
   goes through `_not_supported(name)` so the user gets a clear
   `RuntimeError`, never a silent difference.
4. Document it in the guide of its area and in `README.md` (API
   reference section), and add the row to `test/coverage_audit.md`.
5. Test it in a cross-mode suite so it runs in `worker` and `isolated`
   groups.

### Add a context option

Options arrive as the map given to `py_context:new/1`. Pool contexts are
started by `py_context_sup:start_context/2` with the mode only, so an
option that must apply pool-wide also needs `py_context_router` and the
supervisor to carry it.

1. Embedded modes: read it where the context is set up in
   `src/py_context.erl` (`init/4` and the helpers it calls; `memory_limit`,
   `preload`, `owner` and `start_timeout` are the existing examples).
   If the C side needs it, pass it to `nif_context_create` and extend the
   options parsing there.
2. Isolated mode: read it in `src/py_isolated.erl` (`start_child/1`
   assembles the child command line and environment; `rlimits`, `cgroup`,
   `python`, `restart`, `max_restarts`, `kill_after` are the examples) and,
   if the child must know, add it to the `{init, Opts}` request handled by
   `_init` in `priv/_erlang_impl/_isolated.py`.
3. If the option makes no sense in one mode, reject it there with an
   error that names the option rather than ignoring it.
4. Document it in the options table of the relevant guide
   (`docs/isolated.md`, `docs/workers.md`, `docs/memory.md`) and test both
   the effect and the rejection.

### Add a test suite

1. Name it `test/py_<area>_SUITE.erl`; Python helpers go in
   `test/py_test_<area>.py` and are imported after
   `sys.path.insert(0, TestDir)` (see `py_reentrant_SUITE` for the
   pattern).
2. Start and stop the application in `init_per_suite` /
   `end_per_suite`; the default pool is `py:start_contexts/0`.
3. Behaviour every mode must share runs in groups: `groups/0` with
   `worker` and `isolated`, contexts created with
   `py_context:new(#{mode => Mode})` from `init_per_group`
   (`py_isolated_SUITE` shows the layout).
4. Skip, do not fail, when a platform or interpreter cannot run a case:
   `{skip, Reason}` with the reason a human can act on.
5. Add the suite to the table in `docs/code-map.md` and the cases that
   cover a documented API to `test/coverage_audit.md`; a new module also
   needs a row in its Modules table. `make check-code-map` verifies both.
6. Cases that measure time or memory print their numbers with `ct:pal`
   and assert only on invariants, never on absolute timings.

### Add or change a guide

1. Create `docs/<name>.md` in the task-oriented form: one paragraph on
   what it is and when you need it, then steps with code, then short
   notes. Second person, Erlang snippets, no hype.
2. Every Erlang snippet must call real exports at the right arity and
   every Python snippet must parse: `make lint-docs` checks both.
   `<!-- skip-lint -->` above a fence exempts it; say why in the prose.
3. Register the page in both `extras` lists of `rebar.config` (the flat
   list and the grouped one) so `rebar3 ex_doc` builds it, and link it
   from the guide or README section that leads to it.
4. If the page documents a new API, add its rows to
   `test/coverage_audit.md`.

## Pull requests

Branch from `main` using the prefix that fits: `fix/`, `feat/`, `perf/`,
`refactor/`, `docs/`, `test/` or `chore/`.

Work on one issue goes in one pull request, with one commit per step a
reviewer has to look at, each with its tests. Docs and the CHANGELOG entry
go in the same pull request. Pull requests are merged with a merge commit,
so the commits stay as you wrote them.

Write the commit subject in the imperative, describing what the code now
does rather than what it used to do, in about 60 characters. Keep the body
to one short paragraph on why and how; the diff lists the files. Do not add
generated-by or co-authored-by trailers.

Say what the change is for and how it works. Skip the test inventory: CI
reports that. If the change touches the public API, show it in a short code
block.

- `CHANGELOG.md` is updated under the unreleased version: `Added`,
  `Changed`, `Removed` or `Fixed`, in plain English, saying what was wrong
  or what is new and what it means for the user. No code snippets or
  implementation details. Removing a public function is a major version.
- A change that reverses or extends a decision in `docs/decisions/` gets a
  new record there; the old one is not edited.

Sync a branch that has fallen behind by merging `main` into it, not by
rebasing.

## Releases

The version lives in `src/erlang_python.app.src`. A release is one commit
on `main` that bumps it and dates the CHANGELOG section, then an annotated
tag `vX.Y.Z` on that commit and a GitHub release whose notes are that
CHANGELOG section as is. Before a release, the documentation is reviewed
from a reader's point of view, and the findings the maintainer picks are
fixed first.
