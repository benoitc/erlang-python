#!/usr/bin/env python3
"""I/O focused event loop benchmark."""

import asyncio
import time
import socket

class EchoServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport
    def data_received(self, data):
        self.transport.write(data)

class EchoClientProtocol(asyncio.Protocol):
    def __init__(self, messages, message_size, on_complete):
        self.messages = messages
        self.message_size = message_size
        self.on_complete = on_complete
        self.sent = 0
        self.received = 0
        self.buffer = b''
        self.start_time = None

    def connection_made(self, transport):
        self.transport = transport
        self.start_time = time.perf_counter()
        self._send_next()

    def _send_next(self):
        if self.sent < self.messages:
            self.transport.write(b'x' * self.message_size)
            self.sent += 1

    def data_received(self, data):
        self.buffer += data
        while len(self.buffer) >= self.message_size:
            self.buffer = self.buffer[self.message_size:]
            self.received += 1
            if self.received < self.messages:
                self._send_next()
            elif self.received == self.messages:
                elapsed = time.perf_counter() - self.start_time
                self.on_complete.set_result(elapsed)
                self.transport.close()

async def benchmark_tcp(messages=2000, message_size=64):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        EchoServerProtocol, '127.0.0.1', 0, reuse_address=True)
    port = server.sockets[0].getsockname()[1]

    complete_future = loop.create_future()
    await loop.create_connection(
        lambda: EchoClientProtocol(messages, message_size, complete_future),
        '127.0.0.1', port)

    elapsed = await complete_future
    server.close()
    await server.wait_closed()

    return messages / elapsed, (messages * message_size * 2) / elapsed / 1024 / 1024

def run_io_benchmark():
    results = {}

    # Erlang loop
    try:
        from _erlang_impl import ErlangEventLoop
        erl_loop = ErlangEventLoop()
        asyncio.set_event_loop(erl_loop)
        erl_msg_sec, erl_mb_sec = erl_loop.run_until_complete(benchmark_tcp())
        erl_loop.close()
        results['erlang'] = {'msg_per_sec': erl_msg_sec, 'mb_per_sec': erl_mb_sec}
    except Exception as e:
        results['erlang_error'] = str(e)

    # Standard loop
    std_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(std_loop)
    std_msg_sec, std_mb_sec = std_loop.run_until_complete(benchmark_tcp())
    std_loop.close()
    results['standard'] = {'msg_per_sec': std_msg_sec, 'mb_per_sec': std_mb_sec}

    return results

if __name__ == '__main__':
    print(run_io_benchmark())
else:
    io_benchmark_results = run_io_benchmark()
