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
Context variable tests adapted from uvloop's test_context.py.

These tests verify context variable functionality:
- Context propagation in tasks
- Context isolation between tasks
- Context in callbacks
"""

import asyncio
import contextvars
import unittest

from . import _testbase as tb


# Context variables for testing
request_id = contextvars.ContextVar('request_id', default=None)
user_name = contextvars.ContextVar('user_name', default='anonymous')


class _TestContextBasic:
    """Tests for basic context variable functionality."""

    def test_context_in_task(self):
        """Test context variable in a task."""
        results = []

        async def task_func():
            results.append(request_id.get())
            request_id.set('task_request')
            results.append(request_id.get())

        async def main():
            request_id.set('main_request')
            results.append(request_id.get())
            await task_func()
            results.append(request_id.get())

        self.loop.run_until_complete(main())

        self.assertEqual(results, [
            'main_request',  # Before task
            'main_request',  # In task, inherited
            'task_request',  # In task, after set
            'task_request',  # Back in main, context shared with await
        ])

    def test_context_in_create_task(self):
        """Test context variable with create_task."""
        results = []

        async def task_func():
            results.append(('task', request_id.get()))

        async def main():
            request_id.set('main')
            task = self.loop.create_task(task_func())
            await task
            results.append(('main', request_id.get()))

        self.loop.run_until_complete(main())

        self.assertEqual(results, [
            ('task', 'main'),  # Task inherits context
            ('main', 'main'),
        ])

    def test_context_isolation_between_tasks(self):
        """Test context isolation between concurrent tasks."""
        results = []

        async def task_func(name, value):
            request_id.set(value)
            await asyncio.sleep(0.01)  # Yield to other tasks
            results.append((name, request_id.get()))

        async def main():
            await asyncio.gather(
                task_func('task1', 'value1'),
                task_func('task2', 'value2'),
                task_func('task3', 'value3'),
            )

        self.loop.run_until_complete(main())

        # Each task should see its own value
        task1_results = [r for r in results if r[0] == 'task1']
        task2_results = [r for r in results if r[0] == 'task2']
        task3_results = [r for r in results if r[0] == 'task3']

        self.assertEqual(task1_results, [('task1', 'value1')])
        self.assertEqual(task2_results, [('task2', 'value2')])
        self.assertEqual(task3_results, [('task3', 'value3')])


class _TestContextCallbacks:
    """Tests for context in callbacks."""

    def test_context_in_call_soon(self):
        """Test context variable in call_soon callback."""
        results = []

        def callback():
            results.append(request_id.get())
            self.loop.stop()

        request_id.set('callback_context')
        ctx = contextvars.copy_context()

        # Clear context
        request_id.set(None)

        # Schedule with context
        self.loop.call_soon(callback, context=ctx)
        self.loop.run_forever()

        self.assertEqual(results, ['callback_context'])

    def test_context_in_call_later(self):
        """Test context variable in call_later callback."""
        results = []

        def callback():
            results.append(request_id.get())
            self.loop.stop()

        request_id.set('later_context')
        ctx = contextvars.copy_context()

        request_id.set('different')

        self.loop.call_later(0.01, callback, context=ctx)
        self.loop.run_forever()

        self.assertEqual(results, ['later_context'])

    def test_context_default_without_context_arg(self):
        """Test that callbacks use current context when no context arg."""
        results = []

        def callback():
            results.append(request_id.get())
            self.loop.stop()

        request_id.set('current_context')

        self.loop.call_soon(callback)
        self.loop.run_forever()

        # Should use the current context
        self.assertIn(results[0], ['current_context', None])


class _TestContextCopy:
    """Tests for context copying behavior."""

    def test_context_copy(self):
        """Test contextvars.copy_context()."""
        request_id.set('original')

        ctx = contextvars.copy_context()

        request_id.set('modified')

        # Context copy should have original value
        self.assertEqual(ctx.get(request_id), 'original')
        self.assertEqual(request_id.get(), 'modified')

    def test_context_run(self):
        """Test context.run() method."""
        results = []

        def func():
            results.append(request_id.get())
            request_id.set('inside_run')
            results.append(request_id.get())

        request_id.set('before_run')
        ctx = contextvars.copy_context()

        ctx.run(func)

        results.append(request_id.get())  # Should still be 'before_run'

        self.assertEqual(results, [
            'before_run',   # Inside run, inherited
            'inside_run',   # Inside run, after set
            'before_run',   # Outside run, unchanged
        ])


class _TestContextTaskSpecific:
    """Tests for task-specific context behavior."""

    def test_task_context_argument(self):
        """Test create_task with context argument (Python 3.11+)."""
        import sys
        if sys.version_info < (3, 11):
            self.skipTest("context argument requires Python 3.11+")

        results = []

        async def task_func():
            results.append(request_id.get())

        async def main():
            request_id.set('main')
            ctx = contextvars.copy_context()

            request_id.set('different')

            task = self.loop.create_task(task_func(), context=ctx)
            await task

        self.loop.run_until_complete(main())

        self.assertEqual(results, ['main'])

    def test_current_task_context(self):
        """Test that current_task sees correct context."""
        results = []

        async def check_context():
            current = asyncio.current_task()
            self.assertIsNotNone(current)
            results.append(request_id.get())

        async def main():
            request_id.set('task_context')
            await check_context()

        self.loop.run_until_complete(main())

        self.assertEqual(results, ['task_context'])


class _TestContextMultipleVars:
    """Tests for multiple context variables."""

    def test_multiple_context_vars(self):
        """Test multiple context variables in same task."""
        results = []

        async def task_func():
            results.append((request_id.get(), user_name.get()))
            request_id.set('new_request')
            user_name.set('new_user')
            results.append((request_id.get(), user_name.get()))

        async def main():
            request_id.set('req1')
            user_name.set('user1')
            await task_func()
            results.append((request_id.get(), user_name.get()))

        self.loop.run_until_complete(main())

        self.assertEqual(results, [
            ('req1', 'user1'),           # Inherited
            ('new_request', 'new_user'),  # After modification
            ('new_request', 'new_user'),  # Back in main, context shared
        ])

    def test_context_vars_parallel_tasks(self):
        """Test multiple context vars in parallel tasks."""
        results = {}

        async def task_func(task_id, req_val, user_val):
            request_id.set(req_val)
            user_name.set(user_val)
            await asyncio.sleep(0.01)
            results[task_id] = (request_id.get(), user_name.get())

        async def main():
            await asyncio.gather(
                task_func('t1', 'req1', 'user1'),
                task_func('t2', 'req2', 'user2'),
                task_func('t3', 'req3', 'user3'),
            )

        self.loop.run_until_complete(main())

        self.assertEqual(results['t1'], ('req1', 'user1'))
        self.assertEqual(results['t2'], ('req2', 'user2'))
        self.assertEqual(results['t3'], ('req3', 'user3'))


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangContextBasic(_TestContextBasic, tb.ErlangTestCase):
    pass


class TestAIOContextBasic(_TestContextBasic, tb.AIOTestCase):
    pass


class TestErlangContextCallbacks(_TestContextCallbacks, tb.ErlangTestCase):
    pass


class TestAIOContextCallbacks(_TestContextCallbacks, tb.AIOTestCase):
    pass


class TestErlangContextCopy(_TestContextCopy, tb.ErlangTestCase):
    pass


class TestAIOContextCopy(_TestContextCopy, tb.AIOTestCase):
    pass


class TestErlangContextTaskSpecific(_TestContextTaskSpecific, tb.ErlangTestCase):
    pass


class TestAIOContextTaskSpecific(_TestContextTaskSpecific, tb.AIOTestCase):
    pass


class TestErlangContextMultipleVars(_TestContextMultipleVars, tb.ErlangTestCase):
    pass


class TestAIOContextMultipleVars(_TestContextMultipleVars, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
