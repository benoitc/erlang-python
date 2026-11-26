# Agents

Instructions for AI coding agents working on erlang_python. Read this file
before you change anything; it applies to every agent and every change. The
same rules, written for people, are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Project Overview

erlang_python embeds CPython in the Erlang VM through a NIF: Erlang processes
call Python, and Python calls back into Erlang. Python runs in contexts:
`worker` (a thread per context on the main interpreter), `owngil` (a thread
and a subinterpreter with its own GIL, Python 3.14 and later) and `isolated`
(a child OS process). Isolated sessions give each session a fresh child
started from a prepared template. Requires Erlang/OTP 28 or later, Python
3.12 or later with headers, CMake and a C compiler.

## First principle: a NIF bug takes down the node

The C code and the Erlang code around each NIF call run inside the VM. A
crash, a leak or a deadlock there belongs to the whole node, not to one
process. Every change to `c_src`, to `priv/_erlang_impl` or to the Erlang
side of a NIF call must hold three things before it is called done,
committed or opened as a pull request:

1. **The whole defect class is fixed.** A finding, yours or a reviewer's,
   names examples. Grep for every other site of the same pattern (each
   `enif_binary_to_term`, each `Py_BuildValue` with an `O` argument, each
   raw resource cast) and fix or clear each one. Counts and line numbers
   quoted in a report drift: check them against the current tree.
2. **The mechanism is right, not just plausible.** Say which thread owns
   the object, which locks are held and in what order, whether the call can
   block and whether it holds the GIL while it does, and whether escaping
   even applies at that position. The rules are in
   [c_src/README.md](c_src/README.md#rules-that-are-easy-to-break).
3. **A test proves it.** The test fails on main without the change. A leak
   gets a test that measures memory; a race gets a test that forces the
   interleaving instead of waiting on timing.

## Python code is not a security boundary

Python code that erlang_python runs holds every authority of the user
running the node. The audit-hook sandbox keeps Python from forking the VM,
and isolated mode bounds what a child may consume and contains its crashes;
neither confines what Python may reach (see
[docs/security.md](docs/security.md) and the limits in
[docs/isolated.md](docs/isolated.md#limits)).

So anything Python code can do is a bug at most, never a vulnerability:
getting past the audit hook, an isolated child or a session reading files,
the network or the environment, a limit (rlimits, cgroups, `clear_env`)
that was not applied, a C extension crashing the node, atom exhaustion from
atoms Python sends. Treat it as a bug:

- fix it in a public pull request, with a public issue if one is needed;
- list it under **Fixed** in the CHANGELOG, in plain English;
- do not open a GitHub Security Advisory, request a CVE, embargo the fix,
  give it a severity, or describe how to attack it;
- when it arrived as an advisory, close the advisory as not a vulnerability
  and link the pull request that fixes it.

Only a problem that gives something to someone who cannot already run code
on the node goes through [SECURITY.md](SECURITY.md): memory corruption or
code execution in the VM from data, not code, that passes through the
library (terms, bytes in channels, buffers and shared memory), or the
library loading or running code from a place the configuration did not
name. If unsure, ask the maintainer before treating anything as a security
issue.

## Required Checks

Every change must be formatted and pass all checks before committing. This
list matches what CI gates on; see [CONTRIBUTING.md](CONTRIBUTING.md) for
the rest of the workflow.

```bash
rebar3 fmt                    # Auto-format (always run first)
rebar3 compile                # Builds priv/py_nif.so through CMake
rebar3 ct --readable=compact  # Common Test suites
rebar3 fmt --check            # CI fails on unformatted code
rebar3 lint                   # Elvis linter must pass
rebar3 xref                   # Cross-reference analysis must pass
rebar3 dialyzer               # Type checking must pass
make lint-docs                # Snippets in README.md and docs/ must be valid
make check-code-map           # Every module and suite has its row
rebar3 ex_doc                 # Docs must build
```

`rebar3 compile` can succeed without building anything. CMake caches the
interpreter and the build tool in `_build/cmake`; when either moves,
compile prints a CMake error, exits 0 and keeps the old `py_nif.so`. After
a toolchain or Python change, run `rm -rf _build/cmake` before compiling,
pick the interpreter with `PYTHON_CONFIG=...`, and check what the NIF links
against (`otool -L` on macOS, `ldd` on Linux).

## Tests

- Every behaviour change comes with a test in the same change that pins
  it, and the test fails on main. A manual smoke check is not a test.
- Prefer Common Test cases that drive Python through `py:exec` and
  `py:eval` over ad hoc scripts. Gate a version-specific case on the
  runtime and name it after the gate.
- A feature has three kinds of tests: unit cases, a profile (a bench
  escript in `examples/` with numbers before and after), and a stress
  suite gated by `STRESS=1`, run once under ASan.
- A test fails for one reason. A case that measures time or memory prints
  its numbers with `ct:pal` and asserts only on invariants.

## Plans

Read the files a change touches before you change them. For a large
refactor, or a change across many modules, propose a short plan and wait
for the maintainer before you start.

- Lead with what each piece gives the user, in plain English, and ASCII
  call paths (open, call, nested call, close, failure). Files and commits
  come after.
- A plan that adds a primitive also covers the code that uses it (for
  example `py_buffer` on top of `py_shm`). Stage it inside the plan; do not
  leave it for a follow-up.

## Pull Requests

Work on one issue goes in one pull request. Give each change a reviewer has
to look at its own commit, with its tests, rather than opening a pull
request per change. Docs and CHANGELOG entries go in the same pull request.
Pull requests are merged with a merge commit, so the commits stay.

- Commit subject in the imperative, under about 60 characters, saying what
  the code now does. The body is one short paragraph on why and how; no
  list of files.
- Descriptions state what was wrong and why it matters, the fix in a few
  lines, and breaking changes, as prose. Reference issues with `Fixes #N`.
  No test plan, no test counts, no file-by-file walkthrough, no labelled
  sections.
- No `Co-Authored-By`, "Generated with" or similar trailers, in commits or
  pull requests.
- Show the maintainer the full commit message and pull request text before
  you commit, push or open anything, and wait for approval. Ask before
  anything that publishes or cannot be undone: a push, a pull request, a
  merge, a tag, a release, closing an issue.
- CHANGELOG: under the unreleased version, in `Added`, `Changed`, `Removed`
  or `Fixed`. Plain English, what was wrong or what is new and what it
  means for the user. Public API names are fine; code snippets, internal
  function names and implementation details are not.
- Follow "Write for the person who reads it" in
  [CONTRIBUTING.md](CONTRIBUTING.md#write-for-the-person-who-reads-it):
  plain English, no inflated wording, never "comprehensive", no em dashes,
  and a person has read the change before it is opened.

## Releases

A release is one commit on `main` that bumps `vsn` in
`src/erlang_python.app.src` and dates the CHANGELOG section, then an
annotated tag `vX.Y.Z` on that commit and a GitHub release whose notes are
that CHANGELOG section as is. The maintainer tags, releases and
publishes to Hex; do not do any of it unless asked.

Before a release, review the documentation and maintainability of main
from a reader's point of view (the `docs-maintainability-review` skill, if
your agent has it) and give the plan to the maintainer. The review changes
no source code, its documentation work goes on its own branch, and the
release waits until the maintainer has said which findings to fix first.

## Where Things Are

- [docs/architecture.md](docs/architecture.md): how the pieces fit.
- [docs/code-map.md](docs/code-map.md): every module, C file and suite,
  with the guide that covers it.
- [docs/protocols.md](docs/protocols.md) and
  [docs/state-machines.md](docs/state-machines.md): message and frame
  shapes, process states.
- [docs/decisions/](docs/decisions/overview.md): decision records. A change
  that reverses or extends one adds a new record; the old one is not
  edited.
- [c_src/README.md](c_src/README.md): the C files and their rules.
- [test/coverage_audit.md](test/coverage_audit.md): which suite covers each
  documented API.
- [CONTRIBUTING.md](CONTRIBUTING.md#recipes): what a new NIF, `erlang.*`
  function, context option, suite or guide must touch.

## Formatting and Linting Notes

erlfmt formats `src`, `test`, `examples`, `scripts` and `rebar.config`.
The C and Python sources have no formatter; follow the style around the
change.

Elvis rules are configured in `elvis.config`, the only lint configuration.
The modules that broke a rule when the rules were adopted are listed in
that rule's `ignore`. A new module follows every rule. When you rework a
module, take it off the lists it no longer needs; never add a module to a
list to make a change pass.
