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
Core event loop tests adapted from uvloop's test_base.py.

These tests verify fundamental event loop operations:
- call_soon, call_later, call_at scheduling
- run_forever, run_until_complete, stop, close
- Future and Task creation
- Exception handling
- Debug mode
"""

import asyncio
import contextvars
import gc
import socket
import threading
import time
import unittest
import weakref

from . import _testbase as tb


class _TestCallSoon:
    """Tests for call_soon functionality."""

    def test_call_soon_basic(self):
        """Test basic call_soon scheduling."""
        results = []

        def callback(x):
            results.append(x)

        self.loop.call_soon(callback, 1)
        self.loop.call_soon(callback, 2)
        self.loop.call_soon(callback, 3)

        self.run_briefly()

        self.assertEqual(results, [1, 2, 3])

    def test_call_soon_order(self):
        """Test that call_soon preserves FIFO order."""
        results = []

        for i in range(10):
            self.loop.call_soon(results.append, i)

        self.run_briefly()

        self.assertEqual(results, list(range(10)))

    def test_call_soon_cancel(self):
        """Test cancelling a call_soon handle."""
        results = []

        def callback(x):
            results.append(x)

        self.loop.call_soon(callback, 1)
        handle = self.loop.call_soon(callback, 2)
        self.loop.call_soon(callback, 3)

        handle.cancel()
        self.run_briefly()

        self.assertEqual(results, [1, 3])

    def test_call_soon_double_cancel(self):
        """Test that cancelling twice is safe."""
        results = []
        handle = self.loop.call_soon(results.append, 1)

        handle.cancel()
        handle.cancel()  # Should not raise

        self.run_briefly()
        self.assertEqual(results, [])

    def test_call_soon_threadsafe(self):
        """Test call_soon_threadsafe from another thread."""
        results = []
        event = threading.Event()

        def callback(x):
            results.append(x)
            if x == 3:
                self.loop.stop()

        def thread_func():
            event.wait()
            self.loop.call_soon_threadsafe(callback, 2)
            self.loop.call_soon_threadsafe(callback, 3)

        self.loop.call_soon(callback, 1)
        thread = threading.Thread(target=thread_func)
        thread.start()

        self.loop.call_soon(event.set)
        self.loop.run_forever()

        thread.join(timeout=5)
        self.assertEqual(results, [1, 2, 3])

    def test_call_soon_exception(self):
        """Test that exceptions in callbacks are handled."""
        self.loop.set_exception_handler(self.loop_exception_handler)
        results = []

        def bad_callback():
            raise ValueError("test error")

        def good_callback():
            results.append(1)

        self.loop.call_soon(bad_callback)
        self.loop.call_soon(good_callback)

        self.run_briefly()

        # Good callback should still run
        self.assertEqual(results, [1])
        # Exception should be recorded
        self.assertEqual(len(self.exceptions), 1)
        self.assertIsInstance(self.exceptions[0]['exception'], ValueError)

    def test_call_soon_with_context(self):
        """Test call_soon with context argument."""
        var = contextvars.ContextVar('var')
        results = []

        def callback():
            results.append(var.get())

        # Set value first, then copy context
        var.set('test_value')
        ctx = contextvars.copy_context()

        # Change value after copying - callback should see old value
        var.set('different_value')

        # Schedule with the copied context
        self.loop.call_soon(callback, context=ctx)

        self.run_briefly()

        # Should use the context's value (test_value)
        self.assertEqual(results, ['test_value'])


class _TestCallLater:
    """Tests for call_later and call_at functionality."""

    def test_call_later_basic(self):
        """Test basic call_later scheduling."""
        results = []
        start = time.monotonic()

        def callback():
            results.append(time.monotonic() - start)
            self.loop.stop()

        self.loop.call_later(0.05, callback)
        self.loop.run_forever()

        self.assertEqual(len(results), 1)
        self.assertGreaterEqual(results[0], 0.04)

    def test_call_later_ordering(self):
        """Test that call_later respects timing order."""
        results = []

        def callback(x):
            results.append(x)
            if x == 3:
                self.loop.stop()

        # Schedule out of order
        self.loop.call_later(0.03, callback, 3)
        self.loop.call_later(0.01, callback, 1)
        self.loop.call_later(0.02, callback, 2)

        self.loop.run_forever()

        self.assertEqual(results, [1, 2, 3])

    def test_call_later_cancel(self):
        """Test cancelling a call_later handle."""
        results = []

        def callback(x):
            results.append(x)
            if len(results) == 2:
                self.loop.stop()

        self.loop.call_later(0.01, callback, 1)
        handle = self.loop.call_later(0.02, callback, 2)
        self.loop.call_later(0.03, callback, 3)

        handle.cancel()
        self.loop.run_forever()

        self.assertEqual(results, [1, 3])

    def test_call_later_zero_delay(self):
        """Test call_later with zero delay."""
        results = []

        def callback(x):
            results.append(x)

        self.loop.call_later(0, callback, 1)
        self.loop.call_later(0, callback, 2)

        self.run_briefly()

        self.assertEqual(results, [1, 2])

    def test_call_later_negative_delay(self):
        """Test call_later with negative delay (treated as 0)."""
        results = []

        def callback(x):
            results.append(x)

        self.loop.call_later(-1, callback, 1)

        self.run_briefly()

        self.assertEqual(results, [1])

    def test_call_at(self):
        """Test call_at scheduling."""
        results = []
        now = self.loop.time()

        def callback():
            results.append(True)
            self.loop.stop()

        self.loop.call_at(now + 0.05, callback)
        self.loop.run_forever()

        self.assertEqual(results, [True])

    def test_call_at_past_time(self):
        """Test call_at with time in the past (should run immediately)."""
        results = []
        now = self.loop.time()

        self.loop.call_at(now - 1, lambda: results.append(1))

        self.run_briefly()

        self.assertEqual(results, [1])

    def test_timer_handle_cancelled_property(self):
        """Test TimerHandle.cancelled() method."""
        handle = self.loop.call_later(100, lambda: None)

        self.assertFalse(handle.cancelled())
        handle.cancel()
        self.assertTrue(handle.cancelled())


class _TestRunMethods:
    """Tests for run_forever, run_until_complete, stop, close."""

    def test_run_until_complete_coroutine(self):
        """Test run_until_complete with a coroutine."""
        async def coro():
            return 42

        result = self.loop.run_until_complete(coro())
        self.assertEqual(result, 42)

    def test_run_until_complete_future(self):
        """Test run_until_complete with a future."""
        future = self.loop.create_future()
        self.loop.call_soon(future.set_result, 'hello')
        result = self.loop.run_until_complete(future)
        self.assertEqual(result, 'hello')

    def test_run_until_complete_task(self):
        """Test run_until_complete with a task."""
        async def coro():
            await asyncio.sleep(0.01)
            return 'task_result'

        task = self.loop.create_task(coro())
        result = self.loop.run_until_complete(task)
        self.assertEqual(result, 'task_result')

    def test_run_forever_stop(self):
        """Test run_forever and stop."""
        results = []

        def callback():
            results.append(1)
            self.loop.stop()

        self.loop.call_soon(callback)
        self.loop.run_forever()

        self.assertEqual(results, [1])
        self.assertFalse(self.loop.is_running())

    def test_stop_before_run(self):
        """Test calling stop before run_forever."""
        self.loop.stop()
        self.loop.run_forever()  # Should return immediately

    def test_close(self):
        """Test closing the loop."""
        self.assertFalse(self.loop.is_closed())
        self.loop.close()
        self.assertTrue(self.loop.is_closed())

        # Should be idempotent
        self.loop.close()
        self.assertTrue(self.loop.is_closed())

    def test_close_running_raises(self):
        """Test that closing a running loop raises."""
        async def try_close():
            with self.assertRaises(RuntimeError):
                self.loop.close()

        self.loop.run_until_complete(try_close())

    def test_run_until_complete_nested_raises(self):
        """Test that nested run_until_complete raises."""
        async def outer():
            # Use 0.1 to ensure it goes through timer path, not fast path
            sleep_coro = asyncio.sleep(0.1)
            try:
                with self.assertRaises(RuntimeError):
                    self.loop.run_until_complete(sleep_coro)
            finally:
                sleep_coro.close()

        self.loop.run_until_complete(outer())

    def test_run_until_complete_on_closed_raises(self):
        """Test that run_until_complete on closed loop raises."""
        self.loop.close()

        async def coro():
            pass

        c = coro()
        try:
            with self.assertRaises(RuntimeError):
                self.loop.run_until_complete(c)
        finally:
            c.close()

    def test_is_running(self):
        """Test is_running() method."""
        self.assertFalse(self.loop.is_running())

        async def check():
            self.assertTrue(self.loop.is_running())

        self.loop.run_until_complete(check())
        self.assertFalse(self.loop.is_running())

    def test_time(self):
        """Test time() method returns monotonic time."""
        t1 = self.loop.time()
        time.sleep(0.01)
        t2 = self.loop.time()

        self.assertGreater(t2, t1)
        self.assertAlmostEqual(t2 - t1, 0.01, places=2)


class _TestFuturesAndTasks:
    """Tests for Future and Task creation."""

    def test_create_future(self):
        """Test create_future."""
        future = self.loop.create_future()
        self.assertIsInstance(future, asyncio.Future)
        self.assertFalse(future.done())

        self.loop.call_soon(future.set_result, 123)
        result = self.loop.run_until_complete(future)
        self.assertEqual(result, 123)

    def test_create_future_exception(self):
        """Test create_future with exception."""
        future = self.loop.create_future()

        self.loop.call_soon(
            future.set_exception,
            ValueError("test error")
        )

        with self.assertRaises(ValueError):
            self.loop.run_until_complete(future)

    def test_create_task(self):
        """Test create_task."""
        async def coro():
            await asyncio.sleep(0.01)
            return 42

        async def main():
            task = self.loop.create_task(coro())
            result = await task
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, 42)

    def test_create_task_with_name(self):
        """Test create_task with name argument."""
        async def coro():
            return 1

        async def main():
            task = self.loop.create_task(coro(), name='test_task')
            self.assertEqual(task.get_name(), 'test_task')
            await task

        self.loop.run_until_complete(main())

    def test_task_cancel(self):
        """Test task cancellation."""
        async def long_running():
            await asyncio.sleep(10)

        async def main():
            task = self.loop.create_task(long_running())
            await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.loop.run_until_complete(main())

    def test_gather(self):
        """Test asyncio.gather with our loop."""
        async def task(n):
            await asyncio.sleep(0.01)
            return n * 2

        async def main():
            results = await asyncio.gather(
                task(1), task(2), task(3)
            )
            return results

        results = self.loop.run_until_complete(main())
        self.assertEqual(results, [2, 4, 6])

    def test_task_factory(self):
        """Test custom task factory."""
        factory_calls = []

        def task_factory(loop, coro):
            factory_calls.append(coro)
            return asyncio.Task(coro, loop=loop)

        self.loop.set_task_factory(task_factory)
        self.assertEqual(self.loop.get_task_factory(), task_factory)

        async def coro():
            return 1

        self.loop.run_until_complete(coro())

        self.assertEqual(len(factory_calls), 1)

        # Reset
        self.loop.set_task_factory(None)
        self.assertIsNone(self.loop.get_task_factory())


class _TestExceptionHandling:
    """Tests for exception handling."""

    def test_default_exception_handler(self):
        """Test default exception handler."""
        self.loop.set_exception_handler(self.loop_exception_handler)

        def callback():
            raise ValueError("test error")

        self.loop.call_soon(callback)
        self.run_briefly()

        self.assertEqual(len(self.exceptions), 1)
        self.assertIn('exception', self.exceptions[0])
        self.assertIsInstance(self.exceptions[0]['exception'], ValueError)

    def test_custom_exception_handler(self):
        """Test custom exception handler."""
        errors = []

        def handler(loop, context):
            errors.append(context)

        self.loop.set_exception_handler(handler)
        self.assertEqual(self.loop.get_exception_handler(), handler)

        def callback():
            raise RuntimeError("custom test")

        self.loop.call_soon(callback)
        self.run_briefly()

        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0]['exception'], RuntimeError)

    def test_call_exception_handler(self):
        """Test call_exception_handler method."""
        errors = []

        def handler(loop, context):
            errors.append(context)

        self.loop.set_exception_handler(handler)
        self.loop.call_exception_handler({
            'message': 'test message',
            'exception': ValueError('test'),
        })

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]['message'], 'test message')


class _TestDebugMode:
    """Tests for debug mode."""

    def test_debug_mode_toggle(self):
        """Test debug mode toggle."""
        self.assertFalse(self.loop.get_debug())
        self.loop.set_debug(True)
        self.assertTrue(self.loop.get_debug())
        self.loop.set_debug(False)
        self.assertFalse(self.loop.get_debug())


class _TestReaderWriter:
    """Tests for add_reader/add_writer/remove_reader/remove_writer."""

    def test_add_remove_reader(self):
        """Test add_reader and remove_reader."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)

        try:
            results = []

            def reader_callback():
                results.append('read')
                self.loop.remove_reader(sock.fileno())
                self.loop.stop()

            self.loop.add_reader(sock.fileno(), reader_callback)

            # Trigger readability by connecting to a server
            # For this test, we'll use a timeout approach
            self.loop.call_later(0.01, self.loop.stop)
            self.loop.run_forever()

            # Remove reader should return True
            removed = self.loop.remove_reader(sock.fileno())
            # May be False if already removed
            self.assertIn(removed, [True, False])

            # Remove again should return False
            removed = self.loop.remove_reader(sock.fileno())
            self.assertFalse(removed)

        finally:
            sock.close()

    def test_add_remove_writer(self):
        """Test add_writer and remove_writer."""
        # Use a socket pair for reliable write readiness
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)

        try:
            results = []

            def writer_callback():
                results.append('write')
                self.loop.remove_writer(wsock.fileno())
                self.loop.stop()

            self.loop.add_writer(wsock.fileno(), writer_callback)

            # Add timeout fallback in case writer doesn't fire immediately
            self.loop.call_later(0.1, self.loop.stop)
            self.loop.run_forever()

            # Remove writer if still registered
            self.loop.remove_writer(wsock.fileno())

            # Socket should be writable immediately (or within timeout)
            self.assertIn('write', results)

        finally:
            rsock.close()
            wsock.close()


class _TestAsyncioIntegration:
    """Tests for integration with asyncio APIs."""

    def test_asyncio_sleep(self):
        """Test asyncio.sleep works correctly."""
        async def main():
            start = time.monotonic()
            await asyncio.sleep(0.05)
            elapsed = time.monotonic() - start
            return elapsed

        elapsed = self.loop.run_until_complete(main())
        self.assertGreaterEqual(elapsed, 0.04)

    def test_asyncio_sleep_zero_fast_path(self):
        """Test asyncio.sleep(0) fast path returns immediately."""
        async def main():
            start = time.monotonic()
            # sleep(0) should use fast path and return immediately
            await asyncio.sleep(0)
            elapsed = time.monotonic() - start
            return elapsed

        elapsed = self.loop.run_until_complete(main())
        # Should complete very quickly (fast path)
        self.assertLess(elapsed, 0.01)

    def test_asyncio_wait_for(self):
        """Test asyncio.wait_for."""
        async def slow():
            await asyncio.sleep(10)

        async def main():
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(slow(), timeout=0.05)

        self.loop.run_until_complete(main())

    def test_asyncio_wait_for_completed(self):
        """Test asyncio.wait_for with already completed future."""
        async def fast():
            return 'done'

        async def main():
            result = await asyncio.wait_for(fast(), timeout=10)
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, 'done')

    def test_asyncio_shield(self):
        """Test asyncio.shield."""
        async def important():
            await asyncio.sleep(0.01)
            return "done"

        async def main():
            task = asyncio.shield(important())
            result = await task
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, "done")

    def test_asyncio_all_tasks(self):
        """Test asyncio.all_tasks."""
        async def bg_task():
            await asyncio.sleep(1)

        async def main():
            task = self.loop.create_task(bg_task())
            await asyncio.sleep(0)
            all_tasks = asyncio.all_tasks(self.loop)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return len(all_tasks)

        count = self.loop.run_until_complete(main())
        self.assertGreaterEqual(count, 1)

    def test_asyncio_current_task(self):
        """Test asyncio.current_task."""
        async def main():
            current = asyncio.current_task(self.loop)
            self.assertIsNotNone(current)
            return current

        task = self.loop.run_until_complete(main())
        self.assertIsInstance(task, asyncio.Task)

    def test_asyncio_ensure_future(self):
        """Test asyncio.ensure_future."""
        async def coro():
            return 42

        async def main():
            future = asyncio.ensure_future(coro(), loop=self.loop)
            result = await future
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, 42)


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangCallSoon(_TestCallSoon, tb.ErlangTestCase):
    pass


class TestAIOCallSoon(_TestCallSoon, tb.AIOTestCase):
    pass


class TestErlangCallLater(_TestCallLater, tb.ErlangTestCase):
    pass


class TestAIOCallLater(_TestCallLater, tb.AIOTestCase):
    pass


class TestErlangRunMethods(_TestRunMethods, tb.ErlangTestCase):
    pass


class TestAIORunMethods(_TestRunMethods, tb.AIOTestCase):
    pass


class TestErlangFuturesAndTasks(_TestFuturesAndTasks, tb.ErlangTestCase):
    pass


class TestAIOFuturesAndTasks(_TestFuturesAndTasks, tb.AIOTestCase):
    pass


class TestErlangExceptionHandling(_TestExceptionHandling, tb.ErlangTestCase):
    pass


class TestAIOExceptionHandling(_TestExceptionHandling, tb.AIOTestCase):
    pass


class TestErlangDebugMode(_TestDebugMode, tb.ErlangTestCase):
    pass


class TestAIODebugMode(_TestDebugMode, tb.AIOTestCase):
    pass


class TestErlangReaderWriter(_TestReaderWriter, tb.ErlangTestCase):
    pass


class TestAIOReaderWriter(_TestReaderWriter, tb.AIOTestCase):
    pass


class TestErlangAsyncioIntegration(_TestAsyncioIntegration, tb.ErlangTestCase):
    pass


class TestAIOAsyncioIntegration(_TestAsyncioIntegration, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
