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
Tests for erlang.spawn_task() function.

Tests spawning async tasks from both sync and async contexts.
"""

import asyncio
import os
import sys
import unittest

from . import _testbase as tb


def _get_spawn_task():
    """Get spawn_task function, handling import path setup."""
    try:
        import erlang
        return erlang.spawn_task
    except ImportError:
        pass

    # Add priv directory to path for _erlang_impl
    priv_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if priv_dir not in sys.path:
        sys.path.insert(0, priv_dir)

    from _erlang_impl import spawn_task
    return spawn_task


spawn_task = _get_spawn_task()


class _TestSpawnTask:
    """Tests for spawn_task functionality."""

    def test_spawn_task_from_async_context(self):
        """Test spawning a task from async context."""
        results = []

        async def background():
            results.append('background')

        async def main():
            task = spawn_task(background())
            self.assertIsInstance(task, asyncio.Task)
            await asyncio.sleep(0.01)
            results.append('main')

        self.loop.run_until_complete(main())
        self.assertIn('background', results)
        self.assertIn('main', results)

    def test_spawn_task_returns_awaitable(self):
        """Test that spawn_task returns an awaitable Task."""
        async def compute():
            return 42

        async def main():
            task = spawn_task(compute())
            result = await task
            self.assertEqual(result, 42)

        self.loop.run_until_complete(main())

    def test_spawn_task_with_name(self):
        """Test spawning a named task."""
        async def noop():
            pass

        async def main():
            task = spawn_task(noop(), name='test-task')
            self.assertEqual(task.get_name(), 'test-task')
            await task

        self.loop.run_until_complete(main())

    def test_spawn_task_exception_handling(self):
        """Test that exceptions in spawned tasks are captured."""
        async def failing():
            raise ValueError("test error")

        async def main():
            task = spawn_task(failing())
            await asyncio.sleep(0.01)
            self.assertTrue(task.done())
            with self.assertRaises(ValueError):
                task.result()

        self.loop.run_until_complete(main())


class TestErlangSpawnTask(_TestSpawnTask, tb.ErlangTestCase):
    pass


class TestAIOSpawnTask(_TestSpawnTask, tb.AIOTestCase):
    pass
