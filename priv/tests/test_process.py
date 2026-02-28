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
Subprocess tests adapted from uvloop's test_process.py.

These tests verify subprocess functionality:
- subprocess_shell
- subprocess_exec
- Subprocess I/O
- Process termination
"""

import asyncio
import os
import signal
import subprocess
import sys
import unittest

from . import _testbase as tb


class _TestSubprocessShell:
    """Tests for subprocess_shell functionality."""

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

    def test_subprocess_shell_exit_code(self):
        """Test subprocess_shell exit code."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'exit 42',
                stdout=subprocess.PIPE,
            )
            await proc.wait()
            return proc.returncode

        returncode = self.loop.run_until_complete(main())
        self.assertEqual(returncode, 42)

    def test_subprocess_shell_stdin(self):
        """Test subprocess_shell with stdin."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'cat',
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            stdout, _ = await proc.communicate(input=b'test input')
            return stdout

        output = self.loop.run_until_complete(main())
        self.assertEqual(output, b'test input')

    def test_subprocess_shell_stderr(self):
        """Test subprocess_shell stderr capture."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'echo "error" >&2',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            return stdout, stderr.decode().strip()

        stdout, stderr = self.loop.run_until_complete(main())
        self.assertEqual(stderr, 'error')


class _TestSubprocessExec:
    """Tests for subprocess_exec functionality."""

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

    def test_subprocess_exec_with_args(self):
        """Test subprocess_exec with arguments."""
        async def main():
            proc = await asyncio.create_subprocess_exec(
                sys.executable, '-c',
                'import sys; print(sys.argv[1:])',
                'arg1', 'arg2',
                stdout=subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            return stdout.decode().strip()

        output = self.loop.run_until_complete(main())
        self.assertIn('arg1', output)
        self.assertIn('arg2', output)

    def test_subprocess_exec_stdin_stdout(self):
        """Test subprocess_exec with stdin and stdout pipes."""
        code = '''
import sys
data = sys.stdin.read()
print(f"received: {data}", end="")
'''
        async def main():
            proc = await asyncio.create_subprocess_exec(
                sys.executable, '-c', code,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            stdout, _ = await proc.communicate(input=b'test data')
            return stdout.decode()

        output = self.loop.run_until_complete(main())
        self.assertEqual(output, 'received: test data')


class _TestSubprocessIO:
    """Tests for subprocess I/O operations."""

    def test_subprocess_write_stdin(self):
        """Test writing to subprocess stdin."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'cat',
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )

            proc.stdin.write(b'line1\n')
            proc.stdin.write(b'line2\n')
            await proc.stdin.drain()
            proc.stdin.close()
            await proc.stdin.wait_closed()

            stdout = await proc.stdout.read()
            await proc.wait()
            return stdout

        output = self.loop.run_until_complete(main())
        self.assertEqual(output, b'line1\nline2\n')

    def test_subprocess_readline(self):
        """Test reading lines from subprocess."""
        code = '''
import sys
for i in range(3):
    print(f"line{i}")
    sys.stdout.flush()
'''
        async def main():
            proc = await asyncio.create_subprocess_exec(
                sys.executable, '-c', code,
                stdout=subprocess.PIPE,
            )

            lines = []
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                lines.append(line.decode().strip())

            await proc.wait()
            return lines

        lines = self.loop.run_until_complete(main())
        self.assertEqual(lines, ['line0', 'line1', 'line2'])


class _TestSubprocessTerminate:
    """Tests for subprocess termination."""

    @unittest.skipIf(sys.platform == 'win32', "Signals not available on Windows")
    def test_subprocess_terminate(self):
        """Test terminating a subprocess."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'sleep 60',
                stdout=subprocess.PIPE,
            )

            # Give it time to start
            await asyncio.sleep(0.1)

            proc.terminate()
            returncode = await proc.wait()

            return returncode

        returncode = self.loop.run_until_complete(main())
        # SIGTERM typically gives -15 on Unix
        self.assertIn(returncode, [-15, -signal.SIGTERM, 1])

    @unittest.skipIf(sys.platform == 'win32', "Signals not available on Windows")
    def test_subprocess_kill(self):
        """Test killing a subprocess."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'sleep 60',
                stdout=subprocess.PIPE,
            )

            await asyncio.sleep(0.1)

            proc.kill()
            returncode = await proc.wait()

            return returncode

        returncode = self.loop.run_until_complete(main())
        # SIGKILL typically gives -9 on Unix
        self.assertIn(returncode, [-9, -signal.SIGKILL, 1])

    @unittest.skipIf(sys.platform == 'win32', "Signals not available on Windows")
    def test_subprocess_send_signal(self):
        """Test sending signal to subprocess."""
        code = '''
import signal
import sys

def handler(sig, frame):
    print("received signal", flush=True)
    sys.exit(0)

signal.signal(signal.SIGUSR1, handler)
print("ready", flush=True)
signal.pause()
'''
        async def main():
            proc = await asyncio.create_subprocess_exec(
                sys.executable, '-c', code,
                stdout=subprocess.PIPE,
            )

            # Wait for "ready"
            line = await proc.stdout.readline()
            self.assertEqual(line.decode().strip(), 'ready')

            # Send signal
            proc.send_signal(signal.SIGUSR1)

            # Wait for response
            line = await proc.stdout.readline()
            await proc.wait()

            return line.decode().strip()

        output = self.loop.run_until_complete(main())
        self.assertEqual(output, 'received signal')


class _TestSubprocessTimeout:
    """Tests for subprocess with timeouts."""

    def test_subprocess_communicate_timeout(self):
        """Test communicate with timeout."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'sleep 60',
                stdout=subprocess.PIPE,
            )

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(proc.communicate(), timeout=0.1)

            proc.kill()
            await proc.wait()

        self.loop.run_until_complete(main())

    def test_subprocess_wait_timeout(self):
        """Test wait with timeout."""
        async def main():
            proc = await asyncio.create_subprocess_shell(
                'sleep 60',
                stdout=subprocess.PIPE,
            )

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=0.1)

            proc.kill()
            await proc.wait()

        self.loop.run_until_complete(main())


class _TestSubprocessConcurrent:
    """Tests for concurrent subprocess operations."""

    def test_subprocess_concurrent(self):
        """Test running multiple subprocesses concurrently."""
        async def run_proc(n):
            proc = await asyncio.create_subprocess_exec(
                sys.executable, '-c', f'print({n})',
                stdout=subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            return int(stdout.decode().strip())

        async def main():
            results = await asyncio.gather(
                run_proc(1),
                run_proc(2),
                run_proc(3),
            )
            return results

        results = self.loop.run_until_complete(main())
        self.assertEqual(sorted(results), [1, 2, 3])


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangSubprocessShell(_TestSubprocessShell, tb.ErlangTestCase):
    pass


class TestAIOSubprocessShell(_TestSubprocessShell, tb.AIOTestCase):
    pass


class TestErlangSubprocessExec(_TestSubprocessExec, tb.ErlangTestCase):
    pass


class TestAIOSubprocessExec(_TestSubprocessExec, tb.AIOTestCase):
    pass


class TestErlangSubprocessIO(_TestSubprocessIO, tb.ErlangTestCase):
    pass


class TestAIOSubprocessIO(_TestSubprocessIO, tb.AIOTestCase):
    pass


class TestErlangSubprocessTerminate(_TestSubprocessTerminate, tb.ErlangTestCase):
    pass


class TestAIOSubprocessTerminate(_TestSubprocessTerminate, tb.AIOTestCase):
    pass


class TestErlangSubprocessTimeout(_TestSubprocessTimeout, tb.ErlangTestCase):
    pass


class TestAIOSubprocessTimeout(_TestSubprocessTimeout, tb.AIOTestCase):
    pass


class TestErlangSubprocessConcurrent(_TestSubprocessConcurrent, tb.ErlangTestCase):
    pass


class TestAIOSubprocessConcurrent(_TestSubprocessConcurrent, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
