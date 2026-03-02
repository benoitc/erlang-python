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
Erlang-specific API tests.

These tests verify the Erlang-specific extensions and compatibility APIs:
- ErlangEventLoop
- ErlangEventLoopPolicy
- erlang.run()
- erlang.install()
- erlang.new_event_loop()

The unified 'erlang' module provides both:
- Callback support: erlang.call(), erlang.async_call(), dynamic function access
- Event loop API: erlang.run(), erlang.new_event_loop(), erlang.ErlangEventLoop
"""

import asyncio
import sys
import unittest
import warnings

from . import _testbase as tb


def _get_erlang_event_loop():
    """Get ErlangEventLoop class from available sources."""
    # Try unified erlang module first
    try:
        import erlang
        if hasattr(erlang, 'ErlangEventLoop'):
            return erlang.ErlangEventLoop
    except ImportError:
        pass

    # Try _erlang_impl package
    try:
        from _erlang_impl import ErlangEventLoop
        return ErlangEventLoop
    except ImportError:
        pass

    # Add parent directory to path and try again
    import os
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from _erlang_impl import ErlangEventLoop
    return ErlangEventLoop


def _get_erlang_module():
    """Get the erlang module with event loop API."""
    import erlang
    if hasattr(erlang, 'run'):
        return erlang

    # Extension not loaded - try to load _erlang_impl manually
    import os
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    import _erlang_impl
    # Manually extend
    erlang.run = _erlang_impl.run
    erlang.new_event_loop = _erlang_impl.new_event_loop
    erlang.install = _erlang_impl.install
    erlang.EventLoopPolicy = _erlang_impl.EventLoopPolicy
    erlang.ErlangEventLoop = _erlang_impl.ErlangEventLoop
    return erlang


class TestErlangEventLoopCreation(unittest.TestCase):
    """Tests for ErlangEventLoop creation and basic properties."""

    def test_create_event_loop(self):
        """Test creating an ErlangEventLoop."""
        ErlangEventLoop = _get_erlang_event_loop()

        loop = ErlangEventLoop()
        self.assertIsInstance(loop, asyncio.AbstractEventLoop)
        self.assertFalse(loop.is_running())
        self.assertFalse(loop.is_closed())
        loop.close()
        self.assertTrue(loop.is_closed())

    def test_event_loop_implements_interface(self):
        """Test that ErlangEventLoop implements AbstractEventLoop interface."""
        ErlangEventLoop = _get_erlang_event_loop()

        loop = ErlangEventLoop()
        try:
            # Check required methods exist
            methods = [
                'run_forever',
                'run_until_complete',
                'stop',
                'close',
                'is_running',
                'is_closed',
                'call_soon',
                'call_later',
                'call_at',
                'time',
                'create_future',
                'create_task',
                'add_reader',
                'remove_reader',
                'add_writer',
                'remove_writer',
                'sock_recv',
                'sock_sendall',
                'sock_connect',
                'sock_accept',
                'create_server',
                'create_connection',
                'create_datagram_endpoint',
                'getaddrinfo',
                'getnameinfo',
                'run_in_executor',
                'set_exception_handler',
                'get_exception_handler',
                'get_debug',
                'set_debug',
            ]

            for method in methods:
                self.assertTrue(
                    hasattr(loop, method),
                    f"ErlangEventLoop missing method: {method}"
                )
                self.assertTrue(
                    callable(getattr(loop, method)),
                    f"ErlangEventLoop.{method} is not callable"
                )
        finally:
            loop.close()


def _get_event_loop_policy():
    """Get ErlangEventLoopPolicy class from available sources."""
    # Try unified erlang module first
    try:
        import erlang
        if hasattr(erlang, 'EventLoopPolicy'):
            return erlang.EventLoopPolicy
    except ImportError:
        pass

    # Try _erlang_impl package
    try:
        from _erlang_impl._policy import ErlangEventLoopPolicy
        return ErlangEventLoopPolicy
    except ImportError:
        pass

    # Add parent directory to path and try again
    import os
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from _erlang_impl._policy import ErlangEventLoopPolicy
    return ErlangEventLoopPolicy


class TestErlangEventLoopPolicy(unittest.TestCase):
    """Tests for ErlangEventLoopPolicy."""

    def test_policy_creates_erlang_loop(self):
        """Test that policy creates ErlangEventLoop instances."""
        ErlangEventLoop = _get_erlang_event_loop()
        ErlangEventLoopPolicy = _get_event_loop_policy()

        policy = ErlangEventLoopPolicy()
        loop = policy.new_event_loop()

        try:
            self.assertIsInstance(loop, ErlangEventLoop)
        finally:
            loop.close()

    def test_policy_get_event_loop(self):
        """Test policy.get_event_loop() method."""
        ErlangEventLoopPolicy = _get_event_loop_policy()

        policy = ErlangEventLoopPolicy()
        old_policy = asyncio.get_event_loop_policy()

        try:
            asyncio.set_event_loop_policy(policy)
            loop = policy.new_event_loop()
            policy.set_event_loop(loop)

            # get_event_loop should return the set loop
            retrieved = policy.get_event_loop()
            self.assertIs(retrieved, loop)

            loop.close()
        finally:
            asyncio.set_event_loop_policy(old_policy)


class TestErlangModuleFunctions(unittest.TestCase):
    """Tests for erlang module functions."""

    def test_new_event_loop(self):
        """Test erlang.new_event_loop() function."""
        erlang = _get_erlang_module()
        ErlangEventLoop = _get_erlang_event_loop()

        loop = erlang.new_event_loop()
        try:
            self.assertIsInstance(loop, ErlangEventLoop)
        finally:
            loop.close()

    def test_run_function(self):
        """Test erlang.run() function."""
        erlang = _get_erlang_module()

        async def main():
            return 42

        result = erlang.run(main())
        self.assertEqual(result, 42)

    def test_run_with_debug(self):
        """Test erlang.run() with debug flag."""
        erlang = _get_erlang_module()

        async def main():
            return 'debug_test'

        result = erlang.run(main(), debug=True)
        self.assertEqual(result, 'debug_test')

    def test_install_function(self):
        """Test erlang.install() function."""
        erlang = _get_erlang_module()

        old_policy = asyncio.get_event_loop_policy()

        try:
            if sys.version_info >= (3, 12):
                # Should emit deprecation warning
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    erlang.install()
                    self.assertTrue(len(w) >= 1)
                    self.assertTrue(
                        any(issubclass(warning.category, DeprecationWarning)
                            for warning in w)
                    )
            else:
                erlang.install()

            # Policy should be ErlangEventLoopPolicy
            policy = asyncio.get_event_loop_policy()
            self.assertIsInstance(policy, erlang.EventLoopPolicy)

        finally:
            asyncio.set_event_loop_policy(old_policy)


class TestErlangLoopSpecificFeatures(tb.ErlangTestCase):
    """Tests for Erlang-specific event loop features."""

    def test_time_resolution(self):
        """Test time resolution."""
        t1 = self.loop.time()
        t2 = self.loop.time()

        # Times should be monotonic
        self.assertGreaterEqual(t2, t1)

    def test_shutdown_asyncgens(self):
        """Test shutdown_asyncgens method."""
        async def main():
            await self.loop.shutdown_asyncgens()

        self.loop.run_until_complete(main())

    def test_shutdown_default_executor(self):
        """Test shutdown_default_executor method."""
        # First use the executor
        def blocking():
            return 42

        async def main():
            result = await self.loop.run_in_executor(None, blocking)
            await self.loop.shutdown_default_executor()
            return result

        result = self.loop.run_until_complete(main())
        self.assertEqual(result, 42)


class TestUVLoopCompatibility(tb.ErlangTestCase):
    """Tests for uvloop API compatibility."""

    def test_uvloop_like_api(self):
        """Test that erlang module provides uvloop-like API."""
        erlang = _get_erlang_module()

        # uvloop provides these
        self.assertTrue(hasattr(erlang, 'run'))
        self.assertTrue(hasattr(erlang, 'new_event_loop'))
        self.assertTrue(hasattr(erlang, 'install'))
        self.assertTrue(hasattr(erlang, 'EventLoopPolicy'))

    def test_event_loop_is_asyncio_compatible(self):
        """Test that ErlangEventLoop works with asyncio functions."""
        async def main():
            # Test asyncio.sleep
            await asyncio.sleep(0.01)

            # Test asyncio.gather
            results = await asyncio.gather(
                asyncio.sleep(0.01, result=1),
                asyncio.sleep(0.01, result=2),
            )
            return results

        results = self.loop.run_until_complete(main())
        self.assertEqual(results, [1, 2])

    def test_drop_in_replacement(self):
        """Test that ErlangEventLoop can be used as drop-in replacement."""
        # Create a function that uses asyncio APIs
        async def typical_async_code():
            # Task creation
            task = asyncio.create_task(asyncio.sleep(0.01, result='task'))

            # Future
            future = self.loop.create_future()
            self.loop.call_soon(future.set_result, 'future')

            # Gather results
            task_result = await task
            future_result = await future

            return task_result, future_result

        results = self.loop.run_until_complete(typical_async_code())
        self.assertEqual(results, ('task', 'future'))


def _get_mode_module():
    """Get the mode module for execution mode detection."""
    # Try unified erlang module first
    try:
        import erlang
        if hasattr(erlang, 'detect_mode') and hasattr(erlang, 'ExecutionMode'):
            return erlang
    except ImportError:
        pass

    # Try _erlang_impl package
    try:
        from _erlang_impl._mode import detect_mode, ExecutionMode
        class _ModeModule:
            detect_mode = detect_mode
            ExecutionMode = ExecutionMode
        return _ModeModule()
    except ImportError:
        pass

    # Add parent directory to path and try again
    import os
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from _erlang_impl._mode import detect_mode, ExecutionMode
    class _ModeModule:
        detect_mode = detect_mode
        ExecutionMode = ExecutionMode
    return _ModeModule()


class TestExecutionMode(unittest.TestCase):
    """Tests for execution mode detection."""

    def test_detect_mode(self):
        """Test execution mode detection."""
        mode_module = _get_mode_module()
        mode = mode_module.detect_mode()
        self.assertIsInstance(mode, mode_module.ExecutionMode)

    def test_execution_modes_defined(self):
        """Test that execution modes are defined."""
        mode_module = _get_mode_module()
        ExecutionMode = mode_module.ExecutionMode

        # Check that expected modes exist
        self.assertTrue(hasattr(ExecutionMode, 'SHARED_GIL'))
        self.assertTrue(hasattr(ExecutionMode, 'SUBINTERP'))
        self.assertTrue(hasattr(ExecutionMode, 'FREE_THREADED'))


class TestEventLoopPolicy(unittest.TestCase):
    """Test event loop policy installation."""

    def setUp(self):
        # Save original policy
        self._original_policy = asyncio.get_event_loop_policy()

    def tearDown(self):
        # Restore original policy
        asyncio.set_event_loop_policy(self._original_policy)

    def test_set_event_loop_policy(self):
        """Test: asyncio.set_event_loop_policy(erlang.EventLoopPolicy())"""
        erlang = _get_erlang_module()

        # Install erlang event loop policy
        asyncio.set_event_loop_policy(erlang.EventLoopPolicy())

        # Verify policy is installed
        policy = asyncio.get_event_loop_policy()
        self.assertIsInstance(policy, erlang.EventLoopPolicy)

        # Verify new loops use Erlang implementation
        loop = asyncio.new_event_loop()
        try:
            # Run a simple coroutine
            async def simple():
                await asyncio.sleep(0.01)
                return 42

            result = loop.run_until_complete(simple())
            self.assertEqual(result, 42)
        finally:
            loop.close()

    def test_asyncio_run_with_policy(self):
        """Test: asyncio.run() with erlang policy installed."""
        erlang = _get_erlang_module()
        asyncio.set_event_loop_policy(erlang.EventLoopPolicy())

        async def main():
            await asyncio.sleep(0.01)
            return "done"

        result = asyncio.run(main())
        self.assertEqual(result, "done")


class TestManualLoopSetup(unittest.TestCase):
    """Test manual event loop setup."""

    def test_new_event_loop(self):
        """Test: erlang.new_event_loop() creates working loop."""
        erlang = _get_erlang_module()

        loop = erlang.new_event_loop()
        try:
            self.assertFalse(loop.is_closed())
            self.assertFalse(loop.is_running())

            async def simple():
                return 123

            result = loop.run_until_complete(simple())
            self.assertEqual(result, 123)
        finally:
            loop.close()
            self.assertTrue(loop.is_closed())

    def test_set_event_loop_manual(self):
        """Test: asyncio.set_event_loop(erlang.new_event_loop())"""
        erlang = _get_erlang_module()

        loop = erlang.new_event_loop()
        try:
            asyncio.set_event_loop(loop)

            # Verify it's the current loop (in Python 3.10+, this may raise a warning)
            try:
                current = asyncio.get_event_loop()
                self.assertIs(current, loop)
            except DeprecationWarning:
                pass  # Python 3.12+ deprecates get_event_loop without running loop

            async def with_sleep():
                await asyncio.sleep(0.01)
                return "slept"

            result = loop.run_until_complete(with_sleep())
            self.assertEqual(result, "slept")
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    def test_timers_work(self):
        """Test that call_later/asyncio.sleep use Erlang timers."""
        erlang = _get_erlang_module()

        loop = erlang.new_event_loop()
        try:
            results = []

            def callback(x):
                results.append(x)
                if x == 3:
                    loop.stop()

            # Schedule out of order - should execute in time order
            loop.call_later(0.03, callback, 3)
            loop.call_later(0.01, callback, 1)
            loop.call_later(0.02, callback, 2)

            loop.run_forever()
            self.assertEqual(results, [1, 2, 3])
        finally:
            loop.close()


class TestErlangRun(unittest.TestCase):
    """Test erlang.run() function."""

    def test_run_simple(self):
        """Test: erlang.run(coro)"""
        erlang = _get_erlang_module()

        async def simple():
            return 42

        result = erlang.run(simple())
        self.assertEqual(result, 42)

    def test_run_with_sleep(self):
        """Test: erlang.run() with asyncio.sleep()"""
        erlang = _get_erlang_module()

        async def with_sleep():
            await asyncio.sleep(0.05)
            return "done"

        result = erlang.run(with_sleep())
        self.assertEqual(result, "done")

    def test_run_concurrent_tasks(self):
        """Test: erlang.run() with concurrent tasks."""
        erlang = _get_erlang_module()

        async def task(n):
            await asyncio.sleep(0.01)
            return n * 2

        async def main():
            results = await asyncio.gather(
                task(1), task(2), task(3)
            )
            return results

        result = erlang.run(main())
        self.assertEqual(result, [2, 4, 6])


if __name__ == '__main__':
    unittest.main()
