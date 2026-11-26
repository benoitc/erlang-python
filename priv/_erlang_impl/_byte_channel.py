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
Raw byte channel for Erlang-Python communication.

ByteChannel provides raw byte streaming without term serialization,
suitable for HTTP bodies, file transfers, and binary protocols.

Unlike Channel which serializes Erlang terms, ByteChannel passes
raw binaries directly without any encoding/decoding overhead.

Usage:

    # Receiving bytes from Erlang (sync)
    def process_bytes(channel_ref):
        from erlang import ByteChannel
        ch = ByteChannel(channel_ref)

        # Blocking receive (releases GIL while waiting)
        data = ch.receive_bytes()

        # Non-blocking receive
        data = ch.try_receive_bytes()  # Returns None if empty

        # Iterate over bytes
        for chunk in ch:
            process(chunk)

    # Async receiving (asyncio compatible)
    async def process_bytes_async(channel_ref):
        from erlang import ByteChannel
        ch = ByteChannel(channel_ref)

        # Async receive (yields to other coroutines while waiting)
        data = await ch.async_receive_bytes()

        # Async iteration
        async for chunk in ch:
            process(chunk)

    # Sending bytes back to Erlang
    ch.send_bytes(b"HTTP/1.1 200 OK\\r\\n")
"""

from typing import Optional

__all__ = ['ByteChannel', 'ByteChannelClosed']


class ByteChannelClosed(Exception):
    """Raised when attempting to receive from a closed byte channel."""
    pass


class ByteChannel:
    """Raw byte channel for Erlang-Python communication.

    ByteChannel wraps an Erlang channel reference and provides a Pythonic
    interface for sending and receiving raw bytes without term serialization.

    Attributes:
        _ref: The underlying Erlang channel reference.
    """

    __slots__ = ('_ref',)

    def __init__(self, channel_ref):
        """Initialize a byte channel wrapper.

        Args:
            channel_ref: The Erlang channel reference from py_byte_channel:new/0.
        """
        self._ref = channel_ref

    def send_bytes(self, data: bytes) -> bool:
        """Send raw bytes to the channel.

        Args:
            data: Binary data to send.

        Returns:
            True on success.

        Raises:
            RuntimeError: If the channel is closed or busy (backpressure).
            TypeError: If data is not bytes.
        """
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data must be bytes, bytearray, or memoryview")

        import erlang
        # Use _channel_send which sends raw bytes
        return erlang._channel_send(self._ref, bytes(data))

    def try_receive_bytes(self) -> Optional[bytes]:
        """Try to receive bytes without blocking.

        Returns:
            The received bytes, or None if the channel is empty.

        Raises:
            ByteChannelClosed: If the channel has been closed.
        """
        import erlang

        try:
            return erlang._byte_channel_try_receive_bytes(self._ref)
        except RuntimeError as e:
            if "closed" in str(e):
                raise ByteChannelClosed("Channel has been closed")
            raise

    def receive_bytes(self, timeout_ms: int = -1) -> bytes:
        """Receive the next bytes from the channel.

        This is a blocking receive that waits for data. The GIL is released
        while waiting, allowing other Python threads to run.

        Args:
            timeout_ms: Timeout in milliseconds (-1 = infinite, default)

        Returns:
            The received bytes.

        Raises:
            ByteChannelClosed: If the channel has been closed.
            TimeoutError: If timeout expires before data arrives.
        """
        import erlang

        try:
            return erlang._byte_channel_receive_bytes(self._ref, timeout_ms)
        except RuntimeError as e:
            if "closed" in str(e):
                raise ByteChannelClosed("Channel has been closed")
            raise

    def __iter__(self):
        """Iterate over bytes until the channel is closed.

        Yields:
            Each chunk of bytes received from the channel.

        Example:
            for chunk in byte_channel:
                process(chunk)
        """
        while True:
            try:
                yield self.receive_bytes()
            except ByteChannelClosed:
                break

    # ========================================================================
    # Async methods (asyncio compatible)
    # ========================================================================

    async def async_receive_bytes(self) -> bytes:
        """Async receive - yields to other coroutines while waiting.

        This method uses event-driven notification when running on ErlangEventLoop.
        When data arrives, the channel notifies the event loop via timer dispatch,
        avoiding polling overhead.

        Falls back to polling on non-Erlang event loops.

        Returns:
            The received bytes.

        Raises:
            ByteChannelClosed: If the channel has been closed.

        Example:
            data = await byte_channel.async_receive_bytes()
        """
        import asyncio

        # Non-blocking first (direct NIF - fast path). Received bytes are
        # never None, so None here always means the channel is empty.
        result = self.try_receive_bytes()
        if result is not None:
            return result

        loop = asyncio.get_running_loop()
        if hasattr(loop, '_loop_capsule') and hasattr(loop, '_timers'):
            return await self._async_receive_event_driven(loop)
        return await self._async_receive_polling()

    async def _async_receive_event_driven(self, loop) -> bytes:
        """Event-driven async receive using channel waiter mechanism.

        Registers with the channel and waits for EVENT_TYPE_TIMER dispatch
        when data arrives. The callback only wakes the task, which then
        takes the data itself, so a task cancelled after the wake-up leaves
        the data in the channel.
        """
        from asyncio import events
        import erlang

        while True:
            future = loop.create_future()
            callback_id = id(future)

            def on_channel_ready(future=future):
                if not future.done():
                    future.set_result(None)

            loop._timers[callback_id] = events.Handle(on_channel_ready, (), loop)
            try:
                result = erlang._byte_channel_wait(self._ref, callback_id, loop._loop_capsule)
                if isinstance(result, tuple):
                    if result[0] == 'ok':
                        return result[1]
                    if result[1] == 'closed':
                        raise ByteChannelClosed("Channel closed")
                    raise RuntimeError(f"Channel wait failed: {result[1]}")
                await future
            finally:
                loop._timers.pop(callback_id, None)
                erlang._byte_channel_cancel_wait(self._ref, callback_id)

    async def _async_receive_polling(self) -> bytes:
        """Fallback async receive for non-Erlang event loops.

        Uses run_in_executor to run blocking receive in a thread pool,
        avoiding busy-polling while still releasing the event loop.
        """
        import asyncio
        import erlang

        loop = asyncio.get_running_loop()

        # Run blocking receive in thread pool (releases GIL, waits on condition var)
        # Use 100ms timeout chunks to allow cancellation checks
        while True:
            try:
                result = await loop.run_in_executor(
                    None,
                    lambda: erlang._byte_channel_receive_bytes(self._ref, 100)  # 100ms timeout
                )
                return result
            except TimeoutError:
                # Check for cancellation, then retry
                await asyncio.sleep(0)
                continue
            except RuntimeError as e:
                if "channel closed" in str(e).lower():
                    raise ByteChannelClosed("Channel closed")
                raise

    def __aiter__(self):
        """Return async iterator for the byte channel.

        Example:
            async for chunk in byte_channel:
                process(chunk)
        """
        return self

    async def __anext__(self) -> bytes:
        """Get next bytes asynchronously.

        Raises:
            StopAsyncIteration: When the channel is closed.
        """
        try:
            return await self.async_receive_bytes()
        except ByteChannelClosed:
            raise StopAsyncIteration

    def _is_closed(self) -> bool:
        """Check if the channel is closed."""
        import erlang
        try:
            return erlang._channel_is_closed(self._ref)
        except Exception:
            return True

    def close(self):
        """Close the byte channel.

        Closes the channel and wakes any waiting receivers with ByteChannelClosed.
        Safe to call multiple times - subsequent calls have no effect.

        Example:
            byte_channel.close()  # Signal no more data
        """
        import erlang
        try:
            erlang._channel_close(self._ref)
        except Exception:
            pass  # Ignore errors on close (may already be closed)

    def __enter__(self):
        """Context manager entry - returns self."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - closes the channel."""
        self.close()
        return False

    def __repr__(self):
        return f"<ByteChannel ref={self._ref!r}>"
