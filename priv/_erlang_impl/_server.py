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

"""Serve on fds handed over by Erlang.

Erlang owns the listen socket (gen_tcp or the socket module), duplicates its
fd with py:dup_fd/1 for each worker context, and each worker calls serve()
on its copy from a coroutine scheduled with py_context:submit/4. Accepted
connections can also be handed over one by one with adopt().

    async def main(listen_fd):
        server = await erlang.server.serve(listen_fd, EchoProtocol)
        await server.serve_forever()

The fd passed in must be one this interpreter may close: serve() and adopt()
wrap it in a socket object that owns it.
"""

import asyncio
import socket

__all__ = ['serve', 'adopt', 'stop_serving']


def _socket_from_fd(fd, *, udp=False):
    """Wrap an fd owned by Python in a non-blocking socket object."""
    if not isinstance(fd, int) or fd < 0:
        raise ValueError(f"invalid fd: {fd!r}")
    kind = socket.SOCK_DGRAM if udp else socket.SOCK_STREAM
    try:
        sock = socket.socket(fileno=fd)
    except OSError as exc:
        raise OSError(exc.errno, f"cannot adopt fd {fd}: {exc.strerror}") from exc
    if sock.type != kind:
        sock.detach()
        raise ValueError(f"fd {fd} is not a {'datagram' if udp else 'stream'} socket")
    sock.setblocking(False)
    return sock


async def serve(listen_fd, protocol_factory, *, udp=False, backlog=100):
    """Serve on a listen fd (TCP/Unix) or a bound datagram fd (UDP).

    Returns an asyncio Server for stream sockets, or the datagram transport
    for udp=True. The caller keeps the loop alive (serve_forever, or the
    loop started with py_context:start_loop/1).
    """
    loop = asyncio.get_running_loop()
    sock = _socket_from_fd(listen_fd, udp=udp)
    if udp:
        transport, _protocol = await loop.create_datagram_endpoint(
            protocol_factory, sock=sock)
        return transport
    return await loop.create_server(protocol_factory, sock=sock, backlog=backlog)


async def adopt(fd, protocol_factory):
    """Take over an accepted connection whose fd Erlang handed to us.

    Returns (transport, protocol) like loop.create_connection.
    """
    loop = asyncio.get_running_loop()
    sock = _socket_from_fd(fd)
    return await loop.create_connection(protocol_factory, sock=sock)


async def stop_serving(server, *, wait_closed=True):
    """Stop accepting on a Server or close a datagram transport."""
    server.close()
    if wait_closed and hasattr(server, 'wait_closed'):
        await server.wait_closed()
