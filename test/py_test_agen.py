# Async generators used by py_stream_SUITE to exercise py:stream_start/3,4.
import asyncio


async def counter(n):
    """Yield 0..n-1, awaiting between values so the loop really suspends."""
    for i in range(n):
        await asyncio.sleep(0)
        yield i


async def scaled(n, factor):
    for i in range(n):
        await asyncio.sleep(0)
        yield i * factor


async def empty():
    """An async generator that yields nothing."""
    return
    yield  # pragma: no cover - makes this an async generator


async def failing(n):
    for i in range(n):
        await asyncio.sleep(0)
        if i == 2:
            raise ValueError("agen boom")
        yield i


async def slow(n):
    """Yield slowly enough that a cancel can land mid-stream."""
    for i in range(n):
        await asyncio.sleep(0.05)
        yield i
