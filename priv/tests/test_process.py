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
Subprocess/process tests for ErlangEventLoop.

These tests verify that dangerous subprocess operations are blocked
when running inside the Erlang VM via audit hooks.
"""

import asyncio
import os
import subprocess
import sys
import unittest

from . import _testbase as tb


class TestErlangSubprocessBlocked(tb.ErlangTestCase):
    """Verify subprocess is blocked via event loop or audit hooks."""

    def test_asyncio_subprocess_shell_blocked(self):
        """Test asyncio.create_subprocess_shell is blocked."""
        async def main():
            await asyncio.create_subprocess_shell('echo hello')

        # NotImplementedError from ErlangEventLoop._subprocess, or RuntimeError from audit hook
        with self.assertRaises((NotImplementedError, RuntimeError)):
            self.loop.run_until_complete(main())

    def test_asyncio_subprocess_exec_blocked(self):
        """Test asyncio.create_subprocess_exec is blocked."""
        async def main():
            await asyncio.create_subprocess_exec('echo', 'hello')

        # NotImplementedError from ErlangEventLoop._subprocess, or RuntimeError from audit hook
        with self.assertRaises((NotImplementedError, RuntimeError)):
            self.loop.run_until_complete(main())


class TestErlangOsBlocked(tb.ErlangTestCase):
    """Verify os.* process functions are blocked."""

    def _assert_blocked_or_not_supported(self, msg):
        """Check that error message indicates operation was blocked/not supported."""
        msg_lower = msg.lower()
        self.assertTrue(
            'blocked' in msg_lower or 'not supported' in msg_lower,
            f"Expected 'blocked' or 'not supported' in: {msg}"
        )

    @unittest.skipUnless(hasattr(os, 'fork'), "fork not available")
    def test_os_fork_blocked(self):
        """Test os.fork is blocked."""
        with self.assertRaises(RuntimeError) as cm:
            os.fork()
        self._assert_blocked_or_not_supported(str(cm.exception))

    def test_os_system_blocked(self):
        """Test os.system is blocked."""
        with self.assertRaises(RuntimeError) as cm:
            os.system('echo hello')
        self._assert_blocked_or_not_supported(str(cm.exception))

    def test_os_popen_blocked(self):
        """Test os.popen is blocked."""
        with self.assertRaises(RuntimeError) as cm:
            os.popen('echo hello')
        self._assert_blocked_or_not_supported(str(cm.exception))

    @unittest.skipUnless(hasattr(os, 'execv'), "execv not available")
    def test_os_execv_blocked(self):
        """Test os.execv is blocked."""
        with self.assertRaises(RuntimeError) as cm:
            os.execv('/bin/echo', ['echo', 'hello'])
        self._assert_blocked_or_not_supported(str(cm.exception))

    @unittest.skipUnless(hasattr(os, 'spawnl'), "spawnl not available")
    def test_os_spawnl_blocked(self):
        """Test os.spawnl is blocked."""
        with self.assertRaises(RuntimeError) as cm:
            os.spawnl(os.P_WAIT, '/bin/echo', 'echo', 'hello')
        self._assert_blocked_or_not_supported(str(cm.exception))


if __name__ == '__main__':
    unittest.main()
