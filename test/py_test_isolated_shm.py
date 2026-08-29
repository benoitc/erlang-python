"""Helpers for py_isolated_shm_SUITE and py_isolated_buffer_SUITE. They run
unchanged in worker and isolated contexts."""

import erlang

maps = 0   # how many times a wrapper was constructed in this interpreter


def _count_map():
    global maps
    maps += 1


def kind(obj):
    return type(obj).__name__


def shm_len(shm):
    return len(shm)


def shm_sum(shm, n):
    """Sum the first n bytes through a memoryview (no copy)."""
    return sum(memoryview(shm.buffer)[:n])


def shm_read(shm, offset, n):
    return bytes(shm[offset:offset + n])


def shm_write(shm, offset, data):
    shm[offset:offset + len(data)] = data
    return len(data)


def shm_fill(shm, byte, n):
    shm[0:n] = bytes([byte]) * n
    return n


def shm_identity(shm):
    return shm


def shm_in_structure(payload):
    """payload = {'regions': [shm, ...], 'label': ...}; returns lengths."""
    return [len(s) for s in payload['regions']]


def shm_numpy_sum(shm, n):
    import numpy
    a = numpy.frombuffer(shm.buffer, dtype=numpy.uint8, count=n)
    return int(a.sum())


def shm_numpy_write(shm, n):
    import numpy
    a = numpy.frombuffer(shm.buffer, dtype=numpy.uint8, count=n)
    a[:] = numpy.arange(n, dtype=numpy.uint8)
    return int(a.sum())


def map_count():
    from _erlang_impl import _shm
    return len(_shm._cache)


def shm_write_readonly(shm):
    try:
        shm[0:3] = b'abc'
        return 'wrote'
    except TypeError:
        return 'read_only'


def shm_closed_access(shm):
    shm.close()
    try:
        shm[0]
        return 'readable'
    except ValueError:
        return 'closed'


# ---- shared buffers ---------------------------------------------------------

def buf_kind(buf):
    return type(buf).__name__


def buf_read_all(buf):
    return buf.read()


def buf_read_n(buf, n):
    return buf.read(n)


def buf_read_chunks(buf, n):
    out = []
    while True:
        chunk = buf.read(n)
        if not chunk:
            return out
        out.append(chunk)


def buf_readline(buf):
    return buf.readline()


def buf_readlines(buf):
    return buf.readlines()


def buf_iter(buf):
    return [line for line in buf]


def buf_read_nonblock(buf):
    return buf.read_nonblock()


def buf_at_eof(buf):
    return buf.at_eof()


def buf_consume_len(buf, chunk):
    """Total bytes read in chunks of `chunk`."""
    total = 0
    while True:
        data = buf.read(chunk)
        if not data:
            return total
        total += len(data)


def buf_consume_checksum(buf, chunk):
    total = 0
    acc = 0
    while True:
        data = buf.read(chunk)
        if not data:
            return (total, acc)
        total += len(data)
        acc = (acc + sum(data)) % 1000003


def buf_from_environ(environ):
    body = environ['wsgi.input'].read()
    return (environ['method'], len(body), body[:8])


def buf_read_with_callback(buf, name):
    """Read while a callback re-enters the context: must not deadlock."""
    head = buf.read(4)
    nested = erlang.call(name, 21)
    rest = buf.read()
    return (head, nested, len(rest))
