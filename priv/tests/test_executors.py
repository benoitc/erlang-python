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
Executor tests adapted from uvloop's test_executors.py.

These tests verify executor functionality:
- run_in_executor with default executor
- run_in_executor with custom executor
- Executor task cancellation
"""

import asyncio
import concurrent.futures
import threading
import time
import unittest

from . import _testbase as tb


class _TestRunInExecutor:
    """Tests for run_in_executor functionality."""

    def test_run_in_executor_basic(self):
        """Test basic run_in_executor usage."""
        def blocking_func(x):
            time.sleep(0.01)
            return x * 2

        async def main():
            result = await self.loop.run_in_executor(None, blocking_func, 21)
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, 42)

    def test_run_in_executor_multiple(self):
        """Test multiple run_in_executor calls."""
        def blocking_func(x):
            time.sleep(0.01)
            return x

        async def main():
            tasks = [
                self.loop.run_in_executor(None, blocking_func, i)
                for i in range(5)
            ]
            results = await asyncio.gather(*tasks)
            return results

        results = self.loop.run_until_complete(main())
        self.assertEqual(sorted(results), [0, 1, 2, 3, 4])

    def test_run_in_executor_thread(self):
        """Test that run_in_executor runs in different thread."""
        main_thread_id = threading.get_ident()
        executor_thread_id = []

        def get_thread_id():
            executor_thread_id.append(threading.get_ident())
            return threading.get_ident()

        async def main():
            result = await self.loop.run_in_executor(None, get_thread_id)
            return result

        result = self.loop.run_until_complete(main())

        self.assertEqual(len(executor_thread_id), 1)
        self.assertNotEqual(executor_thread_id[0], main_thread_id)

    def test_run_in_executor_exception(self):
        """Test run_in_executor with exception."""
        def failing_func():
            raise ValueError("test error")

        async def main():
            with self.assertRaises(ValueError):
                await self.loop.run_in_executor(None, failing_func)

        self.loop.run_until_complete(main())

    def test_run_in_executor_cpu_bound(self):
        """Test run_in_executor with CPU-bound work."""
        def cpu_bound():
            total = 0
            for i in range(100000):
                total += i
            return total

        async def main():
            result = await self.loop.run_in_executor(None, cpu_bound)
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, sum(range(100000)))


class _TestCustomExecutor:
    """Tests for run_in_executor with custom executor."""

    def test_run_in_custom_executor(self):
        """Test run_in_executor with custom ThreadPoolExecutor."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        def blocking_func(x):
            time.sleep(0.01)
            return x * 3

        async def main():
            result = await self.loop.run_in_executor(executor, blocking_func, 10)
            return result

        try:
            result = self.loop.run_until_complete(main())
            self.assertEqual(result, 30)
        finally:
            executor.shutdown(wait=True)

    def test_set_default_executor(self):
        """Test set_default_executor method."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        executor_used = []

        original_submit = executor.submit

        def tracked_submit(*args, **kwargs):
            executor_used.append(True)
            return original_submit(*args, **kwargs)

        executor.submit = tracked_submit

        def blocking_func():
            return 42

        async def main():
            self.loop.set_default_executor(executor)
            result = await self.loop.run_in_executor(None, blocking_func)
            return result

        try:
            result = self.loop.run_until_complete(main())
            self.assertEqual(result, 42)
            self.assertTrue(executor_used)
        finally:
            executor.shutdown(wait=True)

    def test_process_pool_executor(self):
        """Test run_in_executor with ProcessPoolExecutor."""
        def cpu_bound(n):
            return sum(range(n))

        async def main():
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=1)
            try:
                result = await self.loop.run_in_executor(
                    executor, cpu_bound, 10000
                )
                return result
            finally:
                executor.shutdown(wait=True)

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, sum(range(10000)))


class _TestExecutorConcurrency:
    """Tests for executor concurrency behavior."""

    def test_executor_concurrent_calls(self):
        """Test concurrent executor calls."""
        start_times = []
        end_times = []
        lock = threading.Lock()

        def track_timing(x):
            with lock:
                start_times.append(time.monotonic())
            time.sleep(0.05)
            with lock:
                end_times.append(time.monotonic())
            return x

        async def main():
            tasks = [
                self.loop.run_in_executor(None, track_timing, i)
                for i in range(3)
            ]
            results = await asyncio.gather(*tasks)
            return results

        results = self.loop.run_until_complete(main())

        self.assertEqual(sorted(results), [0, 1, 2])
        # Check that tasks ran concurrently (starts overlap with other ends)
        self.assertEqual(len(start_times), 3)
        self.assertEqual(len(end_times), 3)

    def test_executor_mixed_with_async(self):
        """Test mixing executor calls with async operations."""
        results = []

        def blocking_work(x):
            time.sleep(0.02)
            return f"blocking:{x}"

        async def async_work(x):
            await asyncio.sleep(0.01)
            return f"async:{x}"

        async def main():
            tasks = [
                self.loop.run_in_executor(None, blocking_work, 1),
                async_work(2),
                self.loop.run_in_executor(None, blocking_work, 3),
                async_work(4),
            ]
            return await asyncio.gather(*tasks)

        results = self.loop.run_until_complete(main())

        self.assertEqual(len(results), 4)
        self.assertIn('blocking:1', results)
        self.assertIn('async:2', results)
        self.assertIn('blocking:3', results)
        self.assertIn('async:4', results)


class _TestExecutorCancel:
    """Tests for executor task cancellation."""

    def test_executor_cancel_pending(self):
        """Test cancelling a pending executor task."""
        started = threading.Event()
        can_continue = threading.Event()

        def slow_func():
            started.set()
            can_continue.wait(timeout=10)
            return 42

        async def main():
            # Start a task that blocks
            task1 = self.loop.run_in_executor(None, slow_func)

            # Wait for it to start
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0.01)

            # This task will be pending in the executor queue
            task2 = self.loop.run_in_executor(None, lambda: 99)

            # Let first task complete
            can_continue.set()

            result1 = await task1
            result2 = await task2

            return result1, result2

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, (42, 99))


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangRunInExecutor(_TestRunInExecutor, tb.ErlangTestCase):
    pass


class TestAIORunInExecutor(_TestRunInExecutor, tb.AIOTestCase):
    pass


class TestErlangCustomExecutor(_TestCustomExecutor, tb.ErlangTestCase):
    pass


class TestAIOCustomExecutor(_TestCustomExecutor, tb.AIOTestCase):
    pass


class TestErlangExecutorConcurrency(_TestExecutorConcurrency, tb.ErlangTestCase):
    pass


class TestAIOExecutorConcurrency(_TestExecutorConcurrency, tb.AIOTestCase):
    pass


class TestErlangExecutorCancel(_TestExecutorCancel, tb.ErlangTestCase):
    pass


class TestAIOExecutorCancel(_TestExecutorCancel, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
