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
Subprocess support via Erlang ports.

This module provides subprocess management that uses Erlang's open_port
instead of os.fork(), making it compatible with subinterpreters and
free-threaded Python where fork() is problematic.

Architecture:
- Erlang creates subprocess via open_port({spawn_executable, Cmd}, ...)
- Port messages are routed to Python callbacks
- stdin/stdout/stderr are handled via port I/O
- Process monitoring uses Erlang's built-in port monitoring
"""

import asyncio
import os
import signal
import subprocess
from asyncio import transports, protocols
from typing import Any, Callable, Optional, Tuple, Union, List

__all__ = [
    'SubprocessTransport',
    'create_subprocess_shell',
    'create_subprocess_exec',
]


class SubprocessTransport(transports.SubprocessTransport):
    """Subprocess transport backed by Erlang ports.

    Uses Erlang's open_port for subprocess management instead of
    Python's os.fork(), which doesn't work well with subinterpreters
    and free-threaded Python.
    """

    def __init__(self, loop, protocol, program, args, shell,
                 stdin, stdout, stderr, **kwargs):
        self._loop = loop
        self._protocol = protocol
        self._program = program
        self._args = args
        self._shell = shell
        self._stdin = stdin
        self._stdout = stdout
        self._stderr = stderr
        self._pid = None
        self._returncode = None
        self._closed = False
        self._port_ref = None
        self._pel = None

        # Pipe transports
        self._stdin_transport = None
        self._stdout_transport = None
        self._stderr_transport = None

        try:
            import py_event_loop as pel
            self._pel = pel
        except ImportError:
            pass

        self._extra = kwargs.get('extra', {})

    async def _start(self):
        """Start the subprocess."""
        if self._pel is not None:
            # Use Erlang port for subprocess
            try:
                self._port_ref = await self._spawn_via_erlang()
            except Exception:
                # Fall back to Python subprocess
                await self._spawn_via_python()
        else:
            await self._spawn_via_python()

        # Notify protocol
        self._loop.call_soon(self._protocol.connection_made, self)

        # Start reading stdout/stderr if available
        if self._stdout_transport is not None:
            self._loop.call_soon(self._protocol.pipe_data_received, 1, b'')
        if self._stderr_transport is not None:
            self._loop.call_soon(self._protocol.pipe_data_received, 2, b'')

    async def _spawn_via_erlang(self):
        """Spawn subprocess via Erlang port.

        Uses open_port({spawn_executable, ...}, [...]) for subprocess creation.
        """
        callback_id = self._loop._next_id()

        if self._shell:
            # Shell command
            if os.name == 'nt':
                cmd = os.environ.get('COMSPEC', 'cmd.exe')
                args = ['/c', self._program]
            else:
                cmd = '/bin/sh'
                args = ['-c', self._program]
        else:
            cmd = self._program
            args = list(self._args) if self._args else []

        # Spawn via Erlang NIF
        port_ref = self._pel._subprocess_spawn(cmd, args, {
            'stdin': self._stdin is not None,
            'stdout': self._stdout is not None,
            'stderr': self._stderr is not None,
            'callback_id': callback_id,
        })

        self._pid = self._pel._subprocess_get_pid(port_ref)
        return port_ref

    async def _spawn_via_python(self):
        """Fall back to Python's subprocess module."""
        proc = await asyncio.create_subprocess_exec(
            self._program,
            *(self._args or []),
            stdin=self._stdin,
            stdout=self._stdout,
            stderr=self._stderr,
        )

        self._pid = proc.pid
        self._proc = proc

        # Wrap process pipes as transports
        if proc.stdin is not None:
            self._stdin_transport = _PipeWriteTransport(
                self._loop, proc.stdin, self._protocol, 0
            )
        if proc.stdout is not None:
            self._stdout_transport = _PipeReadTransport(
                self._loop, proc.stdout, self._protocol, 1
            )
        if proc.stderr is not None:
            self._stderr_transport = _PipeReadTransport(
                self._loop, proc.stderr, self._protocol, 2
            )

    def get_pid(self) -> Optional[int]:
        """Return the subprocess process ID."""
        return self._pid

    def get_returncode(self) -> Optional[int]:
        """Return the subprocess return code."""
        return self._returncode

    def get_pipe_transport(self, fd: int) -> Optional[transports.Transport]:
        """Return the transport for a pipe.

        Args:
            fd: 0 for stdin, 1 for stdout, 2 for stderr.

        Returns:
            Transport for the pipe or None if not connected.
        """
        if fd == 0:
            return self._stdin_transport
        elif fd == 1:
            return self._stdout_transport
        elif fd == 2:
            return self._stderr_transport
        return None

    def send_signal(self, sig: int) -> None:
        """Send a signal to the subprocess.

        Args:
            sig: Signal number to send.
        """
        if self._pid is None:
            raise ProcessLookupError("Process not started")

        if self._port_ref is not None and self._pel is not None:
            self._pel._subprocess_signal(self._port_ref, sig)
        elif hasattr(self, '_proc'):
            self._proc.send_signal(sig)
        else:
            os.kill(self._pid, sig)

    def terminate(self) -> None:
        """Terminate the subprocess with SIGTERM."""
        self.send_signal(signal.SIGTERM)

    def kill(self) -> None:
        """Kill the subprocess with SIGKILL."""
        if os.name == 'nt':
            self.send_signal(signal.SIGTERM)
        else:
            self.send_signal(signal.SIGKILL)

    def close(self) -> None:
        """Close the transport."""
        if self._closed:
            return
        self._closed = True

        # Close pipe transports
        if self._stdin_transport is not None:
            self._stdin_transport.close()
        if self._stdout_transport is not None:
            self._stdout_transport.close()
        if self._stderr_transport is not None:
            self._stderr_transport.close()

        # Terminate process if still running
        if self._returncode is None:
            try:
                self.terminate()
            except ProcessLookupError:
                pass

    def get_extra_info(self, name: str, default=None):
        """Get extra info about the transport."""
        return self._extra.get(name, default)

    def is_closing(self) -> bool:
        """Return True if the transport is closing."""
        return self._closed

    def _on_process_exit(self, returncode: int) -> None:
        """Called when the subprocess exits.

        Args:
            returncode: The process exit code.
        """
        self._returncode = returncode
        self._loop.call_soon(self._protocol.process_exited)

    def _on_stdout_data(self, data: bytes) -> None:
        """Called when data is received on stdout."""
        self._loop.call_soon(self._protocol.pipe_data_received, 1, data)

    def _on_stderr_data(self, data: bytes) -> None:
        """Called when data is received on stderr."""
        self._loop.call_soon(self._protocol.pipe_data_received, 2, data)


class _PipeReadTransport(transports.ReadTransport):
    """Read transport for subprocess pipes."""

    def __init__(self, loop, pipe, protocol, fd):
        self._loop = loop
        self._pipe = pipe
        self._protocol = protocol
        self._fd = fd
        self._paused = False
        self._closing = False

    def pause_reading(self):
        self._paused = True

    def resume_reading(self):
        self._paused = False

    def close(self):
        if self._closing:
            return
        self._closing = True
        self._pipe.close()

    def is_closing(self):
        return self._closing

    def get_extra_info(self, name, default=None):
        if name == 'pipe':
            return self._pipe
        return default


class _PipeWriteTransport(transports.WriteTransport):
    """Write transport for subprocess stdin."""

    def __init__(self, loop, pipe, protocol, fd):
        self._loop = loop
        self._pipe = pipe
        self._protocol = protocol
        self._fd = fd
        self._closing = False

    def write(self, data):
        if self._closing:
            return
        self._pipe.write(data)

    def writelines(self, list_of_data):
        for data in list_of_data:
            self.write(data)

    def write_eof(self):
        self._pipe.close()

    def can_write_eof(self):
        return True

    def close(self):
        if self._closing:
            return
        self._closing = True
        self._pipe.close()

    def is_closing(self):
        return self._closing

    def abort(self):
        self.close()

    def get_extra_info(self, name, default=None):
        if name == 'pipe':
            return self._pipe
        return default

    def get_write_buffer_size(self):
        return 0

    def get_write_buffer_limits(self):
        return (0, 0)

    def set_write_buffer_limits(self, high=None, low=None):
        pass


async def create_subprocess_shell(
        loop, protocol_factory, cmd, *,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs) -> Tuple[SubprocessTransport, protocols.Protocol]:
    """Create a subprocess running a shell command.

    Args:
        loop: The event loop.
        protocol_factory: Factory for the subprocess protocol.
        cmd: Shell command to run.
        stdin: stdin handling (PIPE, DEVNULL, or None).
        stdout: stdout handling.
        stderr: stderr handling.
        **kwargs: Additional arguments.

    Returns:
        Tuple of (transport, protocol).
    """
    protocol = protocol_factory()
    transport = SubprocessTransport(
        loop, protocol, cmd, None, shell=True,
        stdin=stdin, stdout=stdout, stderr=stderr, **kwargs
    )
    await transport._start()
    return transport, protocol


async def create_subprocess_exec(
        loop, protocol_factory, program, *args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs) -> Tuple[SubprocessTransport, protocols.Protocol]:
    """Create a subprocess executing a program.

    Args:
        loop: The event loop.
        protocol_factory: Factory for the subprocess protocol.
        program: Program to execute.
        *args: Program arguments.
        stdin: stdin handling (PIPE, DEVNULL, or None).
        stdout: stdout handling.
        stderr: stderr handling.
        **kwargs: Additional arguments.

    Returns:
        Tuple of (transport, protocol).
    """
    protocol = protocol_factory()
    transport = SubprocessTransport(
        loop, protocol, program, args, shell=False,
        stdin=stdin, stdout=stdout, stderr=stderr, **kwargs
    )
    await transport._start()
    return transport, protocol
