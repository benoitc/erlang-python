"""Helpers for py_isolated_caps_SUITE.

Each one is the smallest Python that performs one operation a capability set
either grants or refuses. They return a plain term on success and let the
exception through on refusal, so the suite sees `{error, {'CapabilityError',
Msg}}` and can tell it apart from a missing file.
"""

import asyncio
import os
import socket
import subprocess

_servers = {}


def _bytes(value):
    """Erlang binaries arrive as `str`; `{bytes, B}` is what arrives as bytes."""
    return value.encode() if isinstance(value, str) else value


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


# --- filesystem -------------------------------------------------------------

def read_file(path):
    with open(path, 'rb') as fh:
        return fh.read()


def write_file(path, data):
    with open(path, 'wb') as fh:
        fh.write(_bytes(data))
    return 'ok'


def append_file(path, data):
    with open(path, 'ab') as fh:
        fh.write(_bytes(data))
    return 'ok'


def truncate_file(path):
    os.truncate(path, 0)
    return 'ok'


def remove_file(path):
    os.remove(path)
    return 'ok'


def list_dir(path):
    return sorted(os.listdir(path))


# --- network ----------------------------------------------------------------

def connect(host, port):
    sock = socket.socket()
    try:
        sock.settimeout(5)
        sock.connect((_text(host), port))
        return 'connected'
    finally:
        sock.close()


def bind(host, port):
    sock = socket.socket()
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((_text(host), port))
        return 'bound'
    finally:
        sock.close()


def resolve(name):
    name = _text(name)
    return sorted({info[4][0] for info in socket.getaddrinfo(name, 80)})


class _Echo(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(b'pong')


async def serve(fd):
    """Accept on a descriptor Erlang passed over and answer one request."""
    import erlang
    _servers[fd] = await erlang.server.serve(fd, _Echo)
    return 'serving'


# --- the rest ---------------------------------------------------------------

def getenv(name):
    name = _text(name)
    value = os.environ.get(name)
    return None if value is None else value


def run_subprocess():
    subprocess.run(['true'], check=False)
    return 'ran'


def shm_write(region, data):
    data = _bytes(data)
    region[0:len(data)] = data
    return 'ok'


class _Fspath:
    """A path object whose conversion tries to read outside every grant."""

    def __init__(self, path):
        self.path = path
        self.leaked = None

    def __fspath__(self):
        try:
            with open('/etc/hosts'):
                self.leaked = True
        except Exception:
            self.leaked = False
        return self.path


def read_through_fspath(path):
    """Did the conversion get an unchecked read? It must not."""
    obj = _Fspath(_text(path))
    try:
        with open(obj):
            pass
    except Exception:
        pass
    return obj.leaked


def truncate_by_descriptor(path):
    fd = os.open(_text(path), os.O_RDONLY)
    try:
        os.truncate(fd, 0)
        return 'truncated'
    finally:
        os.close(fd)


def unix_connect(path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(_text(path))
        return 'connected'
    finally:
        sock.close()


def unix_bind(path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(_text(path))
        return 'bound'
    finally:
        sock.close()
