# Copyright 2026 Benoit Chesneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Shared memory regions and shared streaming buffers (py_shm, py_buffer with
shared => true), seen from Python.

A region handle arrives from Erlang as the tuple ('$py_shm', id, path, size)
and is turned into a SharedMemory; a shared buffer handle
('$py_buffer', id, path, ring_size) becomes a SharedBuffer. Both map the
region file with mmap (MAP_SHARED), so the memory is the one Erlang wrote
through iommap. The same classes serve the embedded interpreter (conversion
in c_src/py_convert.c) and the isolated child (conversion in _isolated.py).

Flow control for buffers goes through Erlang callbacks:
  erlang.call('_py_buffer_wait', id, read_pos)  -> (write_pos, closed)
  erlang.call('_py_buffer_consumed', id, n)     -> ok
  erlang.call('_py_buffer_state', id)           -> (write_pos, closed)
"""

import collections
import mmap
import os
import struct
import threading

__all__ = ['SharedMemory', 'SharedBuffer', 'from_term', 'is_shared', 'forget']

SHM_TAG = '$py_shm'
SHM_RO_TAG = '$py_shm_ro'
BUFFER_TAG = '$py_buffer'
_HEADER = 4096
_HEADER_FMT = struct.Struct('=QBQ')   # write position, closed flag, ring size (Erlang)
_RPOS_OFFSET = _HEADER_FMT.size
_RPOS_FMT = struct.Struct('=Q')       # read position (written by the reader)

# id -> wrapper, bounded: mapping again is cheap next to the copy it saves,
# and Erlang's close does not reach every interpreter.
_CACHE_MAX = 64
_cache = collections.OrderedDict()
_cache_lock = threading.Lock()


def _erlang():
    import erlang
    return erlang


def _atom(name):
    return _erlang().atom(name)


def _open_mapping(path, size, writable=True):
    if isinstance(path, bytes):
        path = os.fsdecode(path)
    fd = os.open(path, os.O_RDWR if writable else os.O_RDONLY)
    try:
        actual = os.fstat(fd).st_size
        if actual != size:
            raise RuntimeError('shared region %s is %d bytes, handle says %d'
                               % (path, actual, size))
        prot = mmap.PROT_READ | (mmap.PROT_WRITE if writable else 0)
        return mmap.mmap(fd, size, mmap.MAP_SHARED, prot)
    finally:
        os.close(fd)


class SharedMemory:
    """A fixed-size region shared with Erlang.

    Supports the buffer protocol (memoryview, numpy.frombuffer, bytes()),
    len(), slicing for read and write, and `close()`. `buffer` is the
    underlying mmap object, for APIs that want a plain buffer."""

    __slots__ = ('id', 'path', 'size', 'writable', '_mmap', '__weakref__')

    def __init__(self, id, path, size, writable=True):
        self.id = id
        self.path = path
        self.size = size
        self.writable = writable
        self._mmap = _open_mapping(path, size, writable)

    # buffer protocol (Python 3.12+); older versions use .buffer / memoryview
    def __buffer__(self, flags):
        return memoryview(self._mmap)

    def __release_buffer__(self, view):
        view.release()

    @property
    def buffer(self):
        return self._mmap

    @property
    def closed(self):
        return self._mmap.closed

    def __len__(self):
        return self.size

    def __getitem__(self, key):
        return self._mmap[key]

    def __setitem__(self, key, value):
        if not self.writable:
            raise TypeError('read-only shared region')
        self._mmap[key] = value

    def view(self, offset=0, length=None):
        end = self.size if length is None else offset + length
        return memoryview(self._mmap)[offset:end]

    def close(self):
        with _cache_lock:
            for k in [k for k in _cache if k[0] == self.id]:
                _cache.pop(k, None)
        if not self._mmap.closed:
            self._mmap.close()

    def to_term(self):
        return (_atom(SHM_TAG if self.writable else SHM_RO_TAG), self.id, self.path, self.size)

    def __repr__(self):
        return '<erlang.SharedMemory id=%d size=%d%s%s>' % (
            self.id, self.size, '' if self.writable else ' read-only',
            ' closed' if self.closed else '')


class SharedBuffer:
    """Streaming input buffer over a shared ring: the `wsgi.input` shape.

    Erlang appends with py_buffer:write/2 and ends with py_buffer:close/1;
    reads block until data or EOF, like the embedded PyBuffer."""

    __slots__ = ('id', 'path', 'ring', '_mmap', '_rpos', '_wpos', '_closed',
                 '_lock', '__weakref__')

    def __init__(self, id, path, ring):
        self.id = id
        self.path = path
        self.ring = ring
        self._mmap = _open_mapping(path, _HEADER + ring)
        self._wpos = 0
        self._closed = False
        self._lock = threading.Lock()
        # Resume where a previous mapping (a dead child) stopped
        (self._rpos,) = _RPOS_FMT.unpack_from(self._mmap, _RPOS_OFFSET)
        self._refresh_header()

    # -- state -------------------------------------------------------------

    def _refresh_header(self):
        wpos, flag, _ring = _HEADER_FMT.unpack_from(self._mmap, 0)
        self._wpos = wpos
        self._closed = bool(flag)

    def _wait_for_data(self):
        """Block until write position passes our read position or EOF."""
        wpos, closed = _erlang().call('_py_buffer_wait', self.id, self._rpos)
        self._wpos = wpos
        self._closed = bool(closed)

    def _consumed(self, n):
        if n:
            _erlang().call('_py_buffer_consumed', self.id, n)

    def _available(self):
        return self._wpos - self._rpos

    def _take(self, n):
        """Copy n bytes (n <= available) out of the ring and advance."""
        start = self._rpos % self.ring
        end = start + n
        if end <= self.ring:
            data = bytes(self._mmap[_HEADER + start:_HEADER + end])
        else:
            first = self.ring - start
            data = (bytes(self._mmap[_HEADER + start:_HEADER + self.ring]) +
                    bytes(self._mmap[_HEADER:_HEADER + (n - first)]))
        self._rpos += n
        _RPOS_FMT.pack_into(self._mmap, _RPOS_OFFSET, self._rpos)
        self._consumed(n)
        return data

    @property
    def closed(self):
        return self._mmap.closed

    def at_eof(self):
        self._refresh_header()
        return self._closed and self._available() == 0

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def readable_amount(self):
        self._refresh_header()
        return self._available()

    # -- reads -------------------------------------------------------------

    def read(self, size=-1):
        with self._lock:
            if size is None or size < 0:
                chunks = []
                while True:
                    if self._available() == 0:
                        if self._closed:
                            break
                        self._wait_for_data()
                        continue
                    chunks.append(self._take(self._available()))
                return b''.join(chunks)
            if size == 0:
                return b''
            while self._available() == 0:
                if self._closed:
                    return b''
                self._wait_for_data()
            return self._take(min(size, self._available()))

    def read_nonblock(self, size=-1):
        with self._lock:
            self._refresh_header()
            avail = self._available()
            if avail == 0:
                return b''
            n = avail if (size is None or size < 0) else min(size, avail)
            return self._take(n)

    def readline(self, size=-1):
        with self._lock:
            limit = None if (size is None or size < 0) else size
            out = bytearray()
            while True:
                if self._available() == 0:
                    if self._closed:
                        return bytes(out)
                    self._wait_for_data()
                    continue
                # Search the readable range for a newline, handling the wrap
                avail = self._available()
                want = avail if limit is None else min(avail, limit - len(out))
                start = self._rpos % self.ring
                end = start + want
                if end <= self.ring:
                    view = self._mmap[_HEADER + start:_HEADER + end]
                else:
                    view = (self._mmap[_HEADER + start:_HEADER + self.ring] +
                            self._mmap[_HEADER:_HEADER + (end - self.ring)])
                idx = view.find(b'\n')
                take = want if idx < 0 else idx + 1
                out += self._take(take)
                if idx >= 0 or (limit is not None and len(out) >= limit):
                    return bytes(out)

    def readlines(self, hint=-1):
        lines = []
        total = 0
        while True:
            line = self.readline()
            if not line:
                return lines
            lines.append(line)
            total += len(line)
            if hint is not None and hint > 0 and total >= hint:
                return lines

    def __iter__(self):
        return self

    def __next__(self):
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def close(self):
        with _cache_lock:
            _cache.pop((self.id, BUFFER_TAG), None)
        if not self._mmap.closed:
            self._mmap.close()

    def to_term(self):
        return (_atom(BUFFER_TAG), self.id, self.path, self.ring)

    def __repr__(self):
        return '<erlang.SharedBuffer id=%d ring=%d>' % (self.id, self.ring)


# ---------------------------------------------------------------------------
# conversion entry points (used by py_convert.c and _isolated.py)
# ---------------------------------------------------------------------------

def is_shared(obj):
    return isinstance(obj, (SharedMemory, SharedBuffer))


def from_term(tag, id, path, size):
    """Wrapper for a handle tuple, cached per interpreter by id."""
    key = (id, tag)
    with _cache_lock:
        cached = _cache.get(key)
        if cached is not None and not cached.closed:
            _cache.move_to_end(key)
            return cached
    if tag == SHM_TAG:
        obj = SharedMemory(id, path, size)
    elif tag == SHM_RO_TAG:
        obj = SharedMemory(id, path, size, writable=False)
    elif tag == BUFFER_TAG:
        obj = SharedBuffer(id, path, size)
    else:
        raise ValueError('unknown shared handle tag %r' % (tag,))
    evicted = []
    with _cache_lock:
        _cache[key] = obj
        while len(_cache) > _CACHE_MAX:
            evicted.append(_cache.popitem(last=False)[1])
    # Unmap outside the lock: close() takes it too
    for old in evicted:
        try:
            old.close()
        except Exception:
            pass
    return obj


def forget(id):
    """Drop and unmap cached wrappers of a region (Erlang closed it)."""
    with _cache_lock:
        objs = [_cache.pop(k) for k in list(_cache) if k[0] == id]
    for obj in objs:
        try:
            obj.close()
        except Exception:
            pass


def convert_args(value):
    """Replace handle tuples inside a decoded argument, recursively."""
    if isinstance(value, tuple):
        if len(value) == 4 and value[0] in (SHM_TAG, SHM_RO_TAG, BUFFER_TAG) \
                and isinstance(value[1], int):
            return from_term(value[0], value[1], value[2], value[3])
        return tuple(convert_args(v) for v in value)
    if isinstance(value, list):
        return [convert_args(v) for v in value]
    if isinstance(value, dict):
        return {k: convert_args(v) for k, v in value.items()}
    return value
