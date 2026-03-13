"""Test async task for uvloop-inspired API."""
import asyncio

async def simple_task():
    """A simple async task that returns a value."""
    await asyncio.sleep(0.01)
    return "hello from async"

async def task_with_args(x, y):
    """An async task that takes arguments."""
    await asyncio.sleep(0.01)
    return x + y

async def failing_task():
    """An async task that raises an exception."""
    await asyncio.sleep(0.01)
    raise ValueError("intentional error")

def sync_func():
    """A regular non-async function."""
    return "sync result"
