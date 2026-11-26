# Security Policy

## Supported Versions

Security fixes are applied to the latest minor release on the `main` branch.

| Version | Supported |
| ------- | --------- |
| 5.0.x   | yes       |
| < 5.0   | no        |

## What is a security issue

Private reports are for problems that give something to someone who cannot
already run code on the node: memory corruption or code execution in the
VM from data, not code, that passes through erlang_python (terms, bytes in
channels, buffers and shared memory), or erlang_python loading or running
code from a place the configuration did not name.

## What is a bug

Python code that erlang_python runs holds every authority of the user
running the node. The sandbox keeps Python from forking the VM, and
isolated mode bounds what a child may consume and contains its crashes;
neither confines what Python may reach. See
[docs/security.md](docs/security.md) and the limits in
[docs/isolated.md](docs/isolated.md#limits).

So anything Python code can do is a bug, even when described as a sandbox
escape or a denial of service: getting past the audit hook, an isolated
child or a session reading files, the network or the environment, a limit
that was not applied, a C extension crashing the node, atoms sent from
Python filling the atom table. Report it as a public bug; see
[Security issues and bugs](CONTRIBUTING.md#security-issues-and-bugs). We
don't publish advisories or request CVEs for these, and advisories filed
for them are closed as bugs with a link to the fix.

Write the report in plain English, with a minimal reproduction, as described
in [CONTRIBUTING.md](CONTRIBUTING.md#write-for-the-person-who-reads-it).

## Reporting a Vulnerability

Please report suspected vulnerabilities privately through GitHub's
[Private Vulnerability Reporting](https://github.com/benoitc/erlang-python/security/advisories/new).

Do not open public issues or pull requests for security problems.

When reporting, include:

- A description of the issue and its impact.
- Steps to reproduce, or a proof of concept.
- Affected versions and platform details.
- Any suggested fix or mitigation.

You can expect an initial acknowledgement within 7 days. We will work with
you on a fix and coordinate disclosure once a patched release is available.
Credit will be given in the advisory unless you prefer to remain anonymous.
