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
Subprocess support is not available in ErlangEventLoop.

Rationale:
    Erlang's port system provides superior subprocess management with
    built-in supervision, monitoring, and fault tolerance. Additionally,
    Python's subprocess module uses fork() which corrupts the Erlang VM
    when called from within the NIF.

Alternative:
    Use Erlang ports directly via erlang.call() for subprocess needs.

Example:
    # In Python:
    result = erlang.call('my_module', 'run_shell', [b'echo hello'])

    # In Erlang (my_module.erl):
    run_shell(Cmd) ->
        Port = open_port({spawn, binary_to_list(Cmd)},
                         [binary, exit_status, stderr_to_stdout]),
        collect_output(Port, []).

    collect_output(Port, Acc) ->
        receive
            {Port, {data, Data}} ->
                collect_output(Port, [Data | Acc]);
            {Port, {exit_status, 0}} ->
                {ok, iolist_to_binary(lists:reverse(Acc))};
            {Port, {exit_status, N}} ->
                {error, {exit_status, N, iolist_to_binary(lists:reverse(Acc))}}
        after 30000 ->
            port_close(Port),
            {error, timeout}
        end.
"""

__all__ = [
    'create_subprocess_shell',
    'create_subprocess_exec',
]


_NOT_SUPPORTED_MSG = """\
Subprocess is not supported in ErlangEventLoop.

Python's subprocess module uses fork() which corrupts the Erlang VM.
Use Erlang ports directly via erlang.call() instead.

Example:
    result = erlang.call('my_module', 'run_shell', [b'echo hello'])

See the module docstring for a complete Erlang implementation example.
"""


async def create_subprocess_shell(loop, protocol_factory, cmd, **kwargs):
    """Not supported - raises NotImplementedError."""
    raise NotImplementedError(_NOT_SUPPORTED_MSG)


async def create_subprocess_exec(loop, protocol_factory, program, *args, **kwargs):
    """Not supported - raises NotImplementedError."""
    raise NotImplementedError(_NOT_SUPPORTED_MSG)
