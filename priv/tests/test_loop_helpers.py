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

"""Unit tests for the worker loop entry points behind py_context:start_loop/1.

_run_loop_forever() runs an ErlangEventLoop on the calling thread until
_stop_loop() (a coroutine scheduled on it) stops it. Runs inside the BEAM
through tests.ct_runner in an owngil context (its own interpreter, no other
ErlangEventLoop alive), so it exercises the real py_event_loop module.
"""

import asyncio
import threading
import unittest


def _impl():
    # _run_loop_forever looks up new_event_loop in the _erlang_impl namespace,
    # so that is the module to hook
    import _erlang_impl
    return _erlang_impl


class TestLoopHelpers(unittest.TestCase):

    def test_run_forever_returns_after_stop(self):
        impl = _impl()
        started = threading.Event()
        result = {}

        def runner():
            # The loop must be created on the running thread; grab it once
            # it is running to schedule the stop
            orig = impl.new_event_loop

            def new_loop_hook():
                loop = orig()
                result['loop'] = loop
                started.set()
                return loop
            impl.new_event_loop = new_loop_hook
            try:
                result['ret'] = impl._run_loop_forever()
            finally:
                impl.new_event_loop = orig

        t = threading.Thread(target=runner)
        t.start()
        self.assertTrue(started.wait(5))
        loop = result['loop']
        # give run_forever a moment to start
        for _ in range(100):
            if loop.is_running():
                break
            threading.Event().wait(0.01)
        self.assertTrue(loop.is_running())
        loop.call_soon_threadsafe(loop.create_task, impl._stop_loop())
        t.join(5)
        self.assertFalse(t.is_alive())
        self.assertEqual(result['ret'], 'stopped')
        self.assertTrue(loop.is_closed())
        # the current event loop is cleared, a new one can be created
        with self.assertRaises(RuntimeError):
            asyncio.get_running_loop()
        loop2 = impl.new_event_loop()
        loop2.close()

    def test_stop_loop_outside_loop_raises(self):
        impl = _impl()
        coro = impl._stop_loop()
        with self.assertRaises(RuntimeError):
            coro.send(None)  # no running loop
        coro.close()

    def test_second_loop_while_running_fails_cleanly(self):
        """One running ErlangEventLoop per interpreter: a second creation
        raises while the first runs, and works again once it has stopped."""
        impl = _impl()
        loop = impl.new_event_loop()
        stopped = threading.Event()

        def runner():
            try:
                loop.run_forever()
            finally:
                stopped.set()

        t = threading.Thread(target=runner)
        t.start()
        for _ in range(100):
            if loop.is_running():
                break
            threading.Event().wait(0.01)
        self.assertTrue(loop.is_running())
        try:
            with self.assertRaises(RuntimeError):
                impl.new_event_loop()
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(5)
            loop.close()
        self.assertTrue(stopped.is_set())
        again = impl.new_event_loop()
        again.close()


if __name__ == '__main__':
    unittest.main()
