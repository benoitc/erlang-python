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
Subprocess tests for ErlangEventLoop.

Subprocess is NOT supported in ErlangEventLoop because Python's subprocess
module uses fork() which corrupts the Erlang VM.

These tests verify that:
1. ErlangEventLoop raises NotImplementedError for subprocess operations
2. Standard asyncio subprocess works outside the Erlang NIF environment
"""

import asyncio
import subprocess
import sys
import unittest

from . import _testbase as tb


class TestErlangSubprocessNotSupported(tb.ErlangTestCase):
    """Verify that subprocess raises NotImplementedError in ErlangEventLoop."""

    def test_subprocess_shell_not_supported(self):
        """Test that create_subprocess_shell raises NotImplementedError."""
        async def main():
            await asyncio.create_subprocess_shell(
                'echo hello',
                stdout=subprocess.PIPE,
            )

        with self.assertRaises(NotImplementedError) as cm:
            self.loop.run_until_complete(main())

        self.assertIn('not supported', str(cm.exception).lower())

    def test_subprocess_exec_not_supported(self):
        """Test that create_subprocess_exec raises NotImplementedError."""
        async def main():
            await asyncio.create_subprocess_exec(
                sys.executable, '-c', 'print("hello")',
                stdout=subprocess.PIPE,
            )

        with self.assertRaises(NotImplementedError) as cm:
            self.loop.run_until_complete(main())

        self.assertIn('not supported', str(cm.exception).lower())


# =============================================================================
# Standard asyncio tests (outside Erlang NIF)
# =============================================================================

@unittest.skipIf(
    tb.INSIDE_ERLANG_NIF,
    "asyncio subprocess uses fork() which corrupts Erlang VM"
)
class TestAIOSubprocessShell(tb.AIOTestCase):
    """Test asyncio subprocess_shell outside Erlang (for comparison)."""

    def test_subprocess_shell_echo(self):
        """Test subprocess_shell with echo command."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'echo "hello world"',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            return stdout.decode().strip(), proc.returncode

        stdout, returncode = self.loop.run_until_complete(main())
        self.assertEqual(stdout, 'hello world')
        self.assertEqual(returncode, 0)


@unittest.skipIf(
    tb.INSIDE_ERLANG_NIF,
    "asyncio subprocess uses fork() which corrupts Erlang VM"
)
class TestAIOSubprocessExec(tb.AIOTestCase):
    """Test asyncio subprocess_exec outside Erlang (for comparison)."""

    def test_subprocess_exec_basic(self):
        """Test basic subprocess_exec."""
        async def main():
            proc = await asyncio.create_subprocess_exec(
                sys.executable, '-c', 'print("hello")',
                stdout=subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            return stdout.decode().strip(), proc.returncode

        stdout, returncode = self.loop.run_until_complete(main())
        self.assertEqual(stdout, 'hello')
        self.assertEqual(returncode, 0)


if __name__ == '__main__':
    unittest.main()
