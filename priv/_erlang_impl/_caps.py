# Copyright 2026 Benoit Chesneau
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""What an isolated child may reach, enforced over Python.

Erlang names the directories, environment variables and addresses this
process may reach (`py_caps.erl`); this module refuses the rest. It is
installed once, in the child's prologue, before any user code runs.

**This is a cooperative policy, not a boundary.** The difference decides
what you may use it for:

* It stops code that is not trying to get out. Reading the wrong dataset,
  writing outside a job's directory, calling home: those stop, and what a
  job may touch becomes something you can read off the context options.
* It does not stop code that is trying to get out. A C extension calling
  `open(2)` never reaches an audit hook. Neither does `file.truncate()`,
  which CPython does not announce. And the grants live in a closure rather
  than in a module attribute, which removes the one-line way to switch them
  off but not the last one: Python exposes its own object graph, so code
  that goes looking can reach a hook's cells.

Hard isolation is a kernel's job and is not here yet. Landlock on Linux
takes the same grant table; moving the state and the hook into the NIF
would take the rest. Until then, treat `caps` as a policy for partially
trusted code, and the process boundary in `docs/isolated.md` as the thing
that holds against the rest.

The audit surface, which is the whole of what is enforced:

* **Refused by rule**: `open`, `os.listdir`, `os.scandir`, `os.mkdir`,
  `os.rmdir`, `os.remove`, `os.rename`, `os.link`, `os.symlink`,
  `os.truncate`, `os.chmod`, `os.chown`, `os.utime`, the `shutil.*` events,
  `socket.connect`, `socket.bind`, `socket.sendto`, and every resolver.
* **Refused outright**: process creation, `ctypes`, signals to anything but
  this process, and Unix-socket addresses.
* **Ignored, deliberately**: `os.stat`, `os.access`, `os.statvfs`,
  `os.chdir` and the other calls that observe without reaching. What exists
  outside a grant stays visible; reading it does not.
* **Removed, because CPython announces nothing**: `os.mkfifo` and
  `os.mknod`, which create. `file.truncate()` and `mmap.resize()` announce
  nothing either and cannot be removed, so a writable descriptor can always
  shorten its own file. That is part of what a `write` grant grants, and it
  is why there is no grant that means "open but do not resize".

Path containment is erlang_wasm's native backend (`c_src/wasi_file_nif.c`),
which needs no C here because Python has `openat`: walk a component at a
time with ``O_NOFOLLOW`` from the descriptor of the grant, follow a symlink
by hand so it is worth what the same text written out is worth, and count
depth so ``..`` moves inside a grant but not out of it.

Refusals are `PermissionError`, never `FileNotFoundError`, so a refusal
says nothing about what exists outside a grant.
"""

import errno
import ipaddress
import os
import socket
import sys
import threading

__all__ = ['install', 'installed', 'grants', 'CapabilityError']

# Eight, as Linux allows per path. It has to be a constant a cycle cannot
# outrun; a self-referential link is otherwise not an error but a hang.
_MAX_SYMLINKS = 8

_READ, _WRITE = 'read', 'write'

# Devices that carry nothing about the host and whose absence breaks code in
# ways that are hard to read. Granted for reading with any capability set.
_ALWAYS_READ = ('/dev/null', '/dev/urandom', '/dev/random', '/dev/zero')

# Process creation, always refused: a capability set names what may be
# reached, and another process is not something it was granted.
_SUBPROCESS_EVENTS = frozenset({
    'subprocess.Popen', 'os.system', 'os.popen', 'os.fork', 'os.forkpty',
    'os.posix_spawn', 'os.posix_spawnp',
})
_EXEC_PREFIXES = ('os.exec', 'os.spawn')

# Signalling, refused except towards this process. The child usually shares
# the node's user and its parent is the BEAM, so an unchecked os.kill is a
# way to take the node down.
_SIGNAL_EVENTS = frozenset({'os.kill', 'os.killpg'})

# Resolution, granted by `resolve`. Every one of these reaches a resolver,
# so gating only getaddrinfo would leave the rest as a way out.
_RESOLVE_EVENTS = frozenset({
    'socket.getaddrinfo', 'socket.gethostbyname', 'socket.gethostbyaddr',
    'socket.getnameinfo', 'socket.getservbyname', 'socket.gethostname',
})

# ctypes reaches libc directly, so leaving it open would make every rule
# here advisory. A library that needs it cannot run under a capability set.
_CTYPES_PREFIX = 'ctypes.'

# Calls that create something and announce nothing, so they are taken away
# rather than refused.
_UNAUDITED_CREATORS = ('mkfifo', 'mknod')

# Audit events that name a path, and what they need for it.
_PATH_EVENTS = {
    'os.listdir': _READ,
    'os.scandir': _READ,
    'os.mkdir': _WRITE,
    'os.rmdir': _WRITE,
    'os.remove': _WRITE,
    'os.rename': _WRITE,
    'os.link': _WRITE,
    'os.symlink': _WRITE,
    'os.truncate': _WRITE,
    'os.chmod': _WRITE,
    'os.chown': _WRITE,
    'os.utime': _WRITE,
    'shutil.copyfile': _WRITE,
    'shutil.copymode': _WRITE,
    'shutil.copystat': _WRITE,
    'shutil.copytree': _WRITE,
    'shutil.move': _WRITE,
    'shutil.rmtree': _WRITE,
    'shutil.unpack_archive': _WRITE,
}

# Operations that act on a name and not on what it points at, so the last
# component is not followed: removing a symlink that leads out of a grant
# removes something inside the grant.
_NAME_EVENTS = frozenset({
    'os.remove', 'os.rename', 'os.symlink', 'os.link', 'os.rmdir', 'os.mkdir',
})

# Events that name two paths; both ends are checked.
_TWO_PATH_EVENTS = frozenset({
    'os.rename', 'os.link', 'os.symlink', 'shutil.copyfile', 'shutil.copymode',
    'shutil.copystat', 'shutil.copytree', 'shutil.move',
})

# A summary of what was granted, for `grants()`. It gates nothing: the
# grants themselves are reachable only from the hook's closure.
_summary = None


class CapabilityError(PermissionError):
    """Raised for anything a capability set does not grant.

    A `PermissionError`, so code that already handles one keeps working, and
    never a `FileNotFoundError`: whether a path outside a grant exists is
    not something a refusal should disclose.
    """


class _Grant:
    """One granted directory, held open.

    Opened once and kept: naming the directory by path on every check would
    leave it to be resolved again each time, so replacing it would move the
    grant. Anchored to the descriptor, a swapped *child* is what gets
    refused.

    Both the path as granted and its resolved form are prefixes, because a
    grant is often reached through a symlink (`/tmp` is `/private/tmp` on
    macOS) and code inside the child will name it either way.
    """

    __slots__ = ('path', 'access', 'fd', 'prefixes')

    def __init__(self, path, access):
        self.path = path
        self.access = access
        self.fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        real = os.path.realpath(path)
        self.prefixes = (path,) if real == path else (path, real)

    def writable(self):
        return self.access == _WRITE

    def remainder(self, path):
        """The part of `path` below this grant, or None if it is not under it."""
        for prefix in self.prefixes:
            if path == prefix:
                return ''
            if path.startswith(prefix.rstrip('/') + '/'):
                return path[len(prefix.rstrip('/')) + 1:]
        return None


class _State:
    __slots__ = ('dirs', 'files', 'net', 'lexical')

    def __init__(self):
        self.dirs = []
        # Exact paths that may be opened for reading: the devices above.
        self.files = {}
        self.net = None
        self.lexical = False


class _Enforcer:
    """What `_make_enforcer` returns: the hook, and its parts for tests."""

    __slots__ = ('hook', 'walk', 'contained', 'check_path', 'writes')

    def __init__(self, **parts):
        for name, part in parts.items():
            setattr(self, name, part)


def _make_enforcer(st):
    """Build the audit hook over `st`.

    Everything on the decision path is bound here rather than looked up when
    the hook runs, because a name resolved at call time is a name any Python
    in this process can rebind: `_caps._writes = lambda *_: False` would
    otherwise turn every open into a read. That includes `os` itself, so the
    primitives are bound one by one, and the walk lives here rather than at
    module level so no shared function object is left behind whose defaults
    could be rewritten.
    """
    # syscalls and constants, bound once
    _open, _close, _readlink = os.open, os.close, os.readlink
    _getcwd, _getpid = os.getcwd, os.getpid
    _fspath, _fsdecode = os.fspath, os.fsdecode
    _normpath, _abspath = os.path.normpath, os.path.abspath
    _O_RDONLY, _O_NOFOLLOW = os.O_RDONLY, os.O_NOFOLLOW
    _O_DIRECTORY = getattr(os, 'O_DIRECTORY', 0)
    _O_WRITES = (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC
                 | os.O_APPEND)
    # A symlink met with O_NOFOLLOW: ELOOP on Linux and macOS, EMLINK on
    # FreeBSD.
    _ELOOP, _EMLINK = errno.ELOOP, errno.EMLINK
    _ip_address = ipaddress.ip_address
    _SOCK_DGRAM = socket.SOCK_DGRAM
    _error = CapabilityError
    READ, WRITE = _READ, _WRITE

    # the tables, copied so rebinding a module attribute changes nothing
    path_events = dict(_PATH_EVENTS)
    name_events = frozenset(_NAME_EVENTS)
    two_path_events = frozenset(_TWO_PATH_EVENTS)
    subprocess_events = frozenset(_SUBPROCESS_EVENTS)
    exec_prefixes = tuple(_EXEC_PREFIXES)
    ctypes_prefix = _CTYPES_PREFIX
    signal_events = frozenset(_SIGNAL_EVENTS)
    resolve_events = frozenset(_RESOLVE_EVENTS)
    dirs, files, net, lexical = st.dirs, st.files, st.net, st.lexical
    max_links = _MAX_SYMLINKS

    # The re-entrancy guard, created here so there is no module attribute to
    # assign to. It is set around the walk and nothing else: the walk calls
    # only `open`, `close` and `readlink` on its own account, so no user
    # code can run while enforcement is off.
    busy = threading.local()

    def walk(grant, rel, follow_last):
        """Resolve `rel` beneath `grant`, a component at a time.

        Returns `(dirfd, component, owned)`; the caller closes `dirfd` when
        `owned`. Raises `CapabilityError` if the path leaves the grant.
        """
        dirfd, owned = grant.fd, False
        depth = links = 0
        pending = [p for p in rel.split('/') if p not in ('', '.')]
        busy.on = True
        try:
            while pending:
                comp = pending.pop(0)
                last = not pending
                if comp == '..':
                    if depth == 0:
                        raise _error('path leaves the grant: %s' % rel)
                    depth -= 1
                    nxt = _open('..', _O_RDONLY | _O_DIRECTORY, dir_fd=dirfd)
                    if owned:
                        _close(dirfd)
                    dirfd, owned = nxt, True
                    continue
                if last and not follow_last:
                    return dirfd, comp, owned
                try:
                    nxt = _open(comp, _O_RDONLY | _O_NOFOLLOW, dir_fd=dirfd)
                except OSError as exc:
                    if exc.errno not in (_ELOOP, _EMLINK):
                        if last:
                            # Not there. That is not a containment answer;
                            # let the caller's own call raise its own error.
                            return dirfd, comp, owned
                        raise
                    if links >= max_links:
                        raise _error('too many symlinks: %s' % rel) from None
                    links += 1
                    target = _readlink(comp, dir_fd=dirfd)
                    if target.startswith('/'):
                        # Refused rather than reinterpreted: resolving it
                        # against the grant would silently mean something
                        # other than what it says.
                        raise _error(
                            'symlink leaves the grant: %s' % rel) from None
                    pending = [p for p in target.split('/')
                               if p not in ('', '.')] + pending
                    continue
                if last:
                    _close(nxt)
                    return dirfd, comp, owned
                if owned:
                    _close(dirfd)
                dirfd, owned = nxt, True
                depth += 1
            return dirfd, '.', owned
        except BaseException:
            if owned:
                _close(dirfd)
            raise
        finally:
            busy.on = False

    def contained(grant, path, need, follow):
        """Is `path` inside `grant`, with `need` access?

        `path` keeps its `..` deliberately: collapsing them first is what
        makes a check disagree with the kernel, because `link/..` is the
        directory the link points into and not the one the link sits in.
        """
        if need == WRITE and not grant.writable():
            return False
        rel = grant.remainder(path)
        if rel is None:
            return False
        if lexical:
            depth = 0
            for comp in rel.split('/'):
                if comp in ('', '.'):
                    continue
                depth += -1 if comp == '..' else 1
                if depth < 0:
                    return False
            return True
        try:
            dirfd, _comp, owned = walk(grant, rel, follow_last=follow)
        except _error:
            return False
        except OSError:
            # A component that is not there is not a containment answer: the
            # path was inside the grant, it simply does not exist.
            return True
        if owned:
            _close(dirfd)
        return True

    def check_path(path, need, event, follow=True, opening=False):
        # Conversion first and unguarded, because `__fspath__` is user code
        # and has to run with the hook live, so its own opens are checked.
        if not isinstance(path, (str, bytes)):
            if isinstance(path, int):
                # A descriptor. Reading through one is already granted,
                # since opening it was checked; changing what it names is
                # not, because a descriptor cannot be mapped back to a path
                # portably enough to check.
                if need == WRITE:
                    raise _error(
                        '%s: a capability set grants no change through a '
                        'descriptor' % event)
                return
            if not hasattr(path, '__fspath__'):
                return
            path = _fspath(path)
        if isinstance(path, bytes):
            try:
                path = _fsdecode(path)
            except ValueError:
                raise _error('%s: undecodable path' % event) from None
        absolute = path if path.startswith('/') \
            else _getcwd().rstrip('/') + '/' + path
        if opening and need == READ \
                and files.get(_normpath(_abspath(absolute))) == READ:
            return
        for grant in dirs:
            if contained(grant, absolute, need, follow):
                return
        raise _error('%s: %s is not granted for %s' % (event, path, need))

    def writes(mode, flags):
        """Does this open ask for anything but reading?"""
        if isinstance(mode, str) and mode:
            return any(c in mode for c in 'wax+')
        if isinstance(flags, int):
            return bool(flags & _O_WRITES)
        return True

    def check_net(kind, event, args):
        sock_obj = args[0] if args else None
        address = args[1] if len(args) > 1 else None
        if not isinstance(address, tuple) or len(address) < 2:
            # A Unix socket names a path, but reaching one is talking to
            # whatever is behind it, which is not something a directory
            # grant says anything about. A descriptor Erlang passed over is
            # unaffected: it is connected or listening already.
            raise _error(
                '%s: a capability set grants no unix-socket or unknown '
                'address; a descriptor has to come from Erlang' % event)
        if net is None:
            raise _error('%s: no network was granted' % event)
        host, port = address[0], address[1]
        try:
            addr = _ip_address(host)
        except ValueError:
            # A rule names addresses, so an unresolved name matches none.
            raise _error('%s: %r is not granted' % (event, address)) from None
        mapped = getattr(addr, 'ipv4_mapped', None)
        if mapped is not None:
            addr = mapped
        proto = 'udp' if getattr(sock_obj, 'type', None) == _SOCK_DGRAM \
            else 'tcp'
        for rule_proto, rule_net, lo, hi in net[kind]:
            if rule_proto == proto and lo <= int(port) <= hi \
                    and addr in rule_net:
                return
        raise _error('%s: %r is not granted' % (event, address))

    def hook(event, args):
        if getattr(busy, 'on', False):
            return
        if event in subprocess_events or event.startswith(exec_prefixes):
            raise _error('%s: a capability set grants no subprocess' % event)
        if event.startswith(ctypes_prefix):
            raise _error(
                '%s: a capability set grants no ctypes, which would reach '
                'past every other rule' % event)
        if event in signal_events:
            if event == 'os.killpg' or not args or args[0] != _getpid():
                raise _error(
                    '%s: a capability set grants no signals to other '
                    'processes' % event)
            return
        if event in resolve_events:
            if net is None or not net['resolve']:
                raise _error(
                    '%s: resolution is its own capability and was not '
                    'granted' % event)
            return
        if event == 'open':
            check_path(args[0], WRITE if writes(args[1], args[2]) else READ,
                       event, opening=True)
        elif event in path_events:
            need = path_events[event]
            follow = event not in name_events
            check_path(args[0], need, event, follow)
            if event in two_path_events and len(args) > 1:
                check_path(args[1], WRITE, event, follow)
        elif event in ('socket.connect', 'socket.sendto'):
            check_net('connect', event, args)
        elif event == 'socket.bind':
            check_net('listen', event, args)

    return _Enforcer(hook=hook, walk=walk, contained=contained,
                     check_path=check_path, writes=writes)


def _parse_net(net):
    if not net:
        return None
    out = {'resolve': bool(net.get('resolve')), 'connect': [], 'listen': []}
    for kind in ('connect', 'listen'):
        for rule in net.get(kind) or ():
            lo, hi = rule['ports']
            out[kind].append((rule['proto'],
                              ipaddress.ip_network(rule['cidr']),
                              int(lo), int(hi)))
    return out


def _disarm_unaudited():
    """Take away the calls CPython does not announce.

    `os.mkfifo` and `os.mknod` create something and raise no audit event, so
    a hook cannot refuse them. Removing the names is not a boundary either,
    but it is the difference between a documented gap and an open one.
    """
    import posix
    for name in _UNAUDITED_CREATORS:
        for module in (os, posix):
            if hasattr(module, name):
                try:
                    delattr(module, name)
                except (AttributeError, TypeError):
                    pass


def install(caps):
    """Install the capability set. Called once, before any user code."""
    global _summary
    if _summary is not None:
        return []
    st = _State()
    problems = []
    st.lexical = os.open not in os.supports_dir_fd
    if st.lexical:
        problems.append('this platform has no openat: paths are checked '
                        'lexically and a symlink out of a grant is not seen')

    # Everything the interpreter itself reads. Without these nothing
    # imports, which is why WASI preopens its sysroot too.
    auto = [(p, _READ) for p in [sys.prefix, sys.base_prefix] + list(sys.path)
            if p]
    named = [(d['path'], d['access']) for d in caps.get('dirs') or ()]

    seen = set()
    for path, access in auto + named:
        real = os.path.normpath(os.path.abspath(path))
        if (real, access) in seen:
            continue
        seen.add((real, access))
        try:
            st.dirs.append(_Grant(real, access))
        except OSError as exc:
            if access != _READ:
                problems.append('cannot open granted directory %s: %s'
                                % (path, exc))
    for dev in _ALWAYS_READ:
        st.files[dev] = _READ
    st.net = _parse_net(caps.get('net'))

    _disarm_unaudited()
    _summary = {
        'dirs': tuple((g.path, g.access) for g in st.dirs),
        'net': None if st.net is None else {
            'connect': tuple(_rule_text(r) for r in st.net['connect']),
            'listen': tuple(_rule_text(r) for r in st.net['listen']),
            'resolve': st.net['resolve'],
        },
        'strict_paths': not st.lexical,
    }
    sys.addaudithook(_make_enforcer(st).hook)
    return problems


def installed():
    return _summary is not None


def grants():
    """What was granted, for `child_info` and `erlang.caps()`.

    A fresh copy each time: this is something to look at, never something
    the hook consults.
    """
    if _summary is None:
        return None
    net = _summary['net']
    return {
        'dirs': [tuple(d) for d in _summary['dirs']],
        'net': None if net is None else {
            'connect': list(net['connect']),
            'listen': list(net['listen']),
            'resolve': net['resolve'],
        },
        'strict_paths': _summary['strict_paths'],
    }


def _rule_text(rule):
    proto, net, lo, hi = rule
    return '%s %s %d-%d' % (proto, net, lo, hi)
