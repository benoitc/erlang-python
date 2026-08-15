# Helpers for py_worker_loop_SUITE: protocols and coroutines that
# py_context:submit/4 schedules on a context's worker loop.
import asyncio
import erlang

served = 0
_servers = {}
_datagram = {}


class Echo(asyncio.Protocol):
    """Reply with 'ok:' + data, tagged with the worker id, then close."""

    tag = b''

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        global served
        served += 1
        self.transport.write(self.tag + b'ok:' + data)
        self.transport.close()


class KeepAlive(asyncio.Protocol):
    """Answer every 'ping' with 'pong', keep the connection open."""

    def connection_made(self, transport):
        self.transport = transport
        self.buf = b''

    def data_received(self, data):
        global served
        self.buf += data
        while len(self.buf) >= 4:
            self.buf = self.buf[4:]
            served += 1
            self.transport.write(b'pong')


class Greedy(asyncio.Protocol):
    """'hog' allocates past any sane memory cap; anything else echoes."""

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        if data == b'hog':
            try:
                hog = [[] for _ in range(3000000)]  # about 170 MB of empty lists
                self.transport.write(b'no-cap:%d' % len(hog))
            except MemoryError:
                self.transport.write(b'memoryerror')
        else:
            self.transport.write(b'ok:' + data)
        self.transport.close()


class EchoUDP(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        global served
        served += 1
        self.transport.sendto(b'udp:' + data, addr)


async def serve(fd, tag=b''):
    """Serve TCP on a listen fd handed over by Erlang."""
    if isinstance(tag, str):
        tag = tag.encode()
    proto = type('TaggedEcho', (Echo,), {'tag': tag})
    server = await erlang.server.serve(fd, proto)
    _servers[fd] = server
    return 'serving'


async def serve_keepalive(fd):
    server = await erlang.server.serve(fd, KeepAlive)
    _servers[fd] = server
    return 'serving'


async def serve_greedy(fd):
    server = await erlang.server.serve(fd, Greedy)
    _servers[fd] = server
    return 'serving'


async def block_loop(seconds):
    """Wedge the loop in a blocking C call (time.sleep)."""
    import time
    time.sleep(seconds)
    return 'unblocked'


async def serve_udp(fd):
    transport = await erlang.server.serve(fd, EchoUDP, udp=True)
    _datagram[fd] = transport
    return 'serving'


async def stop(fd):
    server = _servers.pop(fd, None)
    if server is not None:
        await erlang.server.stop_serving(server)
    transport = _datagram.pop(fd, None)
    if transport is not None:
        transport.close()
    return 'stopped'


async def adopt(fd):
    """Take over an accepted connection fd."""
    await erlang.server.adopt(fd, Echo)
    return 'adopted'


async def add(a, b):
    await asyncio.sleep(0.001)
    return a + b


def sync_add(a, b):
    return a + b


async def sleep_then(value, seconds):
    await asyncio.sleep(seconds)
    return value


async def raise_error():
    raise ValueError('boom')


async def wait_channel(ref):
    """Await one message on a py_channel from inside the loop."""
    ch = erlang.Channel(ref)
    msg = await ch.async_receive()
    return msg


def served_count():
    return served
