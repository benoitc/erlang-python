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

"""Unit tests for the transport close path of ErlangEventLoop.

A transport must not close its socket while the fd may still be in the BEAM
poll set: it detaches the fd and hands it to the loop (_close_socket), which
releases the fd resource with ownership so the NIF closes it after the
select stop. These tests drive the loop against a recording NIF stub, so
they check the contract without a running BEAM.
"""

import asyncio
import os
import socket
import unittest

from _erlang_impl import _loop as loop_mod
from _erlang_impl._transport import ErlangSocketTransport, ErlangDatagramTransport


class _RecordingNif(loop_mod._MockNifModule):
    """Mock NIF that records fd resource calls."""

    def __init__(self):
        super().__init__()
        self.calls = []

    def _add_reader_for(self, capsule, fd, callback_id):
        self.calls.append(('add_reader', fd))
        return super()._add_reader_for(capsule, fd, callback_id)

    def _add_writer_for(self, capsule, fd, callback_id):
        self.calls.append(('add_writer', fd))
        return super()._add_writer_for(capsule, fd, callback_id)

    def _clear_fd_read(self, fd_key):
        self.calls.append(('clear_read', self._fd_by_key.get(fd_key)))

    def _clear_fd_write(self, fd_key):
        self.calls.append(('clear_write', self._fd_by_key.get(fd_key)))

    def _release_fd_resource(self, fd_key, take_ownership=False):
        self.calls.append(('release', self._fd_by_key.get(fd_key), take_ownership))
        return super()._release_fd_resource(fd_key, take_ownership)


def _make_loop():
    """ErlangEventLoop over the recording stub (no py_event_loop C module)."""
    loop = loop_mod.ErlangEventLoop.__new__(loop_mod.ErlangEventLoop)
    nif = _RecordingNif()
    # Mirror the parts of __init__ the transports touch
    loop._pel = nif
    loop._loop_capsule = nif._loop_new()
    loop._uses_global_capsule = False
    loop._readers = {}
    loop._writers = {}
    loop._callbacks_by_cid = {}
    loop._fd_resources = {}
    loop._timers = {}
    loop._timer_refs = {}
    loop._handle_to_callback_id = {}
    loop._ready = __import__('collections').deque()
    loop._ready_append = loop._ready.append
    loop._ready_popleft = loop._ready.popleft
    loop._handle_pool = []
    loop._handle_pool_max = 150
    loop._cached_time = 0.0
    loop._wake_pending = False
    loop._running = False
    loop._stopping = False
    loop._closed = False
    loop._thread_id = None
    loop._clock_resolution = 1e-9
    loop._exception_handler = None
    loop._current_handle = None
    loop._debug = False
    loop._task_factory = None
    loop._default_executor = None
    loop._signal_handlers = {}
    loop._execution_mode = None
    loop._callback_id = 0
    return loop, nif


class _Proto(asyncio.Protocol):
    def __init__(self):
        self.lost = []

    def connection_made(self, transport):
        pass

    def connection_lost(self, exc):
        self.lost.append(exc)


class TestTransportClose(unittest.TestCase):

    def setUp(self):
        self.loop, self.nif = _make_loop()
        self.a, self.b = socket.socketpair()
        self.a.setblocking(False)

    def tearDown(self):
        for s in (self.a, self.b):
            try:
                s.close()
            except OSError:
                pass

    def _fd_is_open(self, fd):
        try:
            os.fstat(fd)
            return True
        except OSError:
            return False

    def test_close_hands_fd_to_nif(self):
        fd = self.a.fileno()
        proto = _Proto()
        transport = ErlangSocketTransport(self.loop, self.a, proto)
        self.loop.add_reader(fd, transport._read_ready)
        self.assertEqual(self.nif.calls, [('add_reader', fd)])

        transport.close()
        # No pending writes: connection_lost ran and the socket was detached,
        # the fd itself is closed by the (mock) NIF with ownership
        self.assertEqual(proto.lost, [None])
        self.assertEqual(self.a.fileno(), -1)
        self.assertIn(('release', fd, True), self.nif.calls)
        self.assertFalse(self._fd_is_open(fd))
        # nothing left registered for that fd
        self.assertNotIn(fd, self.loop._fd_resources)
        self.assertNotIn(fd, self.loop._readers)

    def test_stop_reading_keeps_resource(self):
        fd = self.a.fileno()
        transport = ErlangSocketTransport(self.loop, self.a, _Proto())
        self.loop.add_reader(fd, transport._read_ready)
        transport.pause_reading()
        self.assertIn(('clear_read', fd), self.nif.calls)
        # resource kept for resume, no release issued
        self.assertNotIn(('release', fd, False), self.nif.calls)
        self.assertIn(fd, self.loop._fd_resources)
        transport.resume_reading()
        self.assertIn(fd, self.loop._readers)

    def test_abort_closes_once(self):
        fd = self.a.fileno()
        proto = _Proto()
        transport = ErlangSocketTransport(self.loop, self.a, proto)
        self.loop.add_reader(fd, transport._read_ready)
        transport.abort()
        transport.abort()
        transport.close()
        self.assertEqual(proto.lost, [None])
        releases = [c for c in self.nif.calls if c[0] == 'release']
        self.assertEqual(releases, [('release', fd, True)])

    def test_pending_write_defers_close(self):
        fd = self.a.fileno()
        proto = _Proto()
        transport = ErlangSocketTransport(self.loop, self.a, proto)
        self.loop.add_reader(fd, transport._read_ready)
        # Fill the buffer so the write cannot complete synchronously
        transport._buffer = bytearray(b'pending')
        transport._buffer_offset = 0
        transport.close()
        # reading stopped, connection not lost yet, socket still open
        self.assertIn(('clear_read', fd), self.nif.calls)
        self.assertEqual(proto.lost, [])
        self.assertNotEqual(self.a.fileno(), -1)
        # drain: the write callback finishes and closes
        transport._write_ready_cb()
        self.assertEqual(proto.lost, [None])
        self.assertIn(('release', fd, True), self.nif.calls)

    def test_datagram_close(self):
        u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        u.bind(('127.0.0.1', 0))
        u.setblocking(False)
        fd = u.fileno()
        proto = _Proto()
        transport = ErlangDatagramTransport(self.loop, u, proto)
        self.loop.add_reader(fd, transport._read_ready)
        transport.close()
        self.assertEqual(proto.lost, [None])
        self.assertIn(('release', fd, True), self.nif.calls)
        self.assertFalse(self._fd_is_open(fd))

    def test_close_socket_unregistered(self):
        # A socket the loop never registered is closed directly
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        fd = s.fileno()
        self.loop._close_socket(s)
        self.assertFalse(self._fd_is_open(fd))
        self.assertEqual([c for c in self.nif.calls if c[0] == 'release'], [])

    def test_loop_close_releases_kept_resources(self):
        fd = self.a.fileno()
        transport = ErlangSocketTransport(self.loop, self.a, _Proto())
        self.loop.add_reader(fd, transport._read_ready)
        transport.pause_reading()  # resource kept without reader
        self.loop.close()
        self.assertIn(('release', fd, False), self.nif.calls)
        self.assertEqual(self.loop._fd_resources, {})

    def test_del_abandoned_transport(self):
        # A paused transport is not referenced by the loop's readers; when it
        # is dropped its socket still goes through the loop close path
        fd = self.a.fileno()
        transport = ErlangSocketTransport(self.loop, self.a, _Proto())
        self.loop.add_reader(fd, transport._read_ready)
        transport.pause_reading()
        sock = self.a
        self.a = socket.socket()  # keep tearDown happy
        del transport
        import gc
        gc.collect()
        self.assertEqual(sock.fileno(), -1)
        self.assertIn(('release', fd, True), self.nif.calls)


if __name__ == '__main__':
    unittest.main()
