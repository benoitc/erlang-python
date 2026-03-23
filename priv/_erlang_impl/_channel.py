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
Bidirectional channel for Erlang-Python communication.

Channels provide efficient streaming message passing without syscall overhead.

Usage:

    # Receiving messages from Erlang (sync)
    def process_channel(channel_ref):
        from erlang.channel import Channel
        ch = Channel(channel_ref)

        # Blocking receive (suspends Python, yields to Erlang)
        msg = ch.receive()

        # Non-blocking receive
        msg = ch.try_receive()  # Returns None if empty

        # Iterate over messages
        for msg in ch:
            process(msg)

    # Async receiving (asyncio compatible)
    async def process_channel_async(channel_ref):
        from erlang.channel import Channel
        ch = Channel(channel_ref)

        # Async receive (yields to other coroutines while waiting)
        msg = await ch.async_receive()

        # Async iteration
        async for msg in ch:
            process(msg)

    # Sending replies to Erlang processes
    from erlang.channel import reply
    reply(pid, {"status": "ok", "data": result})
"""

__all__ = ['Channel', 'reply', 'ChannelClosed']


class ChannelClosed(Exception):
    """Raised when attempting to receive from a closed channel."""
    pass


class Channel:
    """Bidirectional channel for Erlang-Python communication.

    Channels wrap an Erlang channel reference and provide a Pythonic
    interface for receiving messages and iterating over them.

    Attributes:
        _ref: The underlying Erlang channel reference.
    """

    __slots__ = ('_ref',)

    def __init__(self, channel_ref):
        """Initialize a channel wrapper.

        Args:
            channel_ref: The Erlang channel reference from py_channel:new/0.
        """
        self._ref = channel_ref

    def receive(self, timeout_ms=-1):
        """Receive the next message from the channel.

        This is a blocking receive that waits for data. The GIL is released
        while waiting, allowing other Python threads to run.

        Args:
            timeout_ms: Timeout in milliseconds (-1 = infinite, default)

        Returns:
            The received Erlang term, converted to Python.

        Raises:
            ChannelClosed: If the channel has been closed.
            TimeoutError: If timeout expires before data arrives.
        """
        import erlang

        # Direct NIF call - no erlang.call() roundtrip
        try:
            return erlang._channel_receive(self._ref, timeout_ms)
        except RuntimeError as e:
            if "closed" in str(e):
                raise ChannelClosed("Channel has been closed")
            raise

    def try_receive(self):
        """Try to receive a message without blocking.

        Returns:
            The received message, or None if the channel is empty.

        Raises:
            ChannelClosed: If the channel has been closed.
        """
        import erlang

        # Direct NIF call - no erlang.call() roundtrip
        try:
            return erlang._channel_try_receive(self._ref)
        except RuntimeError as e:
            if "closed" in str(e):
                raise ChannelClosed("Channel has been closed")
            raise

    def __iter__(self):
        """Iterate over messages until the channel is closed.

        Yields:
            Each message received from the channel.

        Example:
            for msg in channel:
                process(msg)
        """
        while True:
            try:
                yield self.receive()
            except ChannelClosed:
                break

    # ========================================================================
    # Async methods (asyncio compatible)
    # ========================================================================

    async def async_receive(self):
        """Async receive - yields to other coroutines while waiting.

        This method uses event-driven notification when running on ErlangEventLoop.
        When data arrives, the channel notifies the event loop via timer dispatch,
        avoiding polling overhead.

        Falls back to polling on non-Erlang event loops.

        Returns:
            The received Erlang term, converted to Python.

        Raises:
            ChannelClosed: If the channel has been closed.

        Example:
            msg = await channel.async_receive()
        """
        import asyncio

        # Try non-blocking first (direct NIF - fast)
        result = self.try_receive()
        if result is not None:
            return result

        # Check if closed
        if self._is_closed():
            raise ChannelClosed("Channel has been closed")

        # Get the running event loop
        loop = asyncio.get_running_loop()

        # Check if this is an ErlangEventLoop with native dispatch support
        if hasattr(loop, '_loop_capsule') and hasattr(loop, '_timers'):
            return await self._async_receive_event_driven(loop)
        else:
            # Fallback for non-Erlang event loops: use polling
            return await self._async_receive_polling()

    async def _async_receive_event_driven(self, loop):
        """Event-driven async receive using channel waiter mechanism.

        Registers with the channel and waits for EVENT_TYPE_TIMER dispatch
        when data arrives. No polling required.
        """
        import asyncio
        from asyncio import events
        import erlang

        future = loop.create_future()
        callback_id = id(future)

        # Callback that fires when channel has data
        def on_channel_ready():
            if future.done():
                return
            try:
                data = self.try_receive()
                if data is not None:
                    future.set_result(data)
                elif self._is_closed():
                    future.set_exception(ChannelClosed("Channel closed"))
                else:
                    # Data consumed by race - set None to signal retry
                    future.set_result(None)
            except Exception as e:
                future.set_exception(e)

        # Create handle and register in timer dispatch system
        # Only use _timers dict - don't touch _handle_to_callback_id (for timer cancellation only)
        handle = events.Handle(on_channel_ready, (), loop)
        loop._timers[callback_id] = handle

        try:
            # Register waiter with channel (direct C call, no Erlang overhead)
            result = erlang._channel_wait(self._ref, callback_id, loop._loop_capsule)

            if isinstance(result, tuple):
                if result[0] == 'ok':
                    # Data already available - clean up and return
                    loop._timers.pop(callback_id, None)
                    return result[1]
                elif result[0] == 'error':
                    loop._timers.pop(callback_id, None)
                    if result[1] == 'closed':
                        raise ChannelClosed("Channel closed")
                    raise RuntimeError(f"Channel wait failed: {result[1]}")

            # Waiter registered - await notification
            try:
                data = await future
                # Handle race condition (data was None)
                if data is None:
                    # Retry with polling fallback for this edge case
                    return await self._async_receive_polling()
                return data
            except asyncio.CancelledError:
                erlang._channel_cancel_wait(self._ref, callback_id)
                raise
        finally:
            loop._timers.pop(callback_id, None)

    async def _async_receive_polling(self):
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
                    lambda: erlang._channel_receive(self._ref, 100)  # 100ms timeout
                )
                return result
            except TimeoutError:
                # Check for cancellation, then retry
                await asyncio.sleep(0)
                continue
            except RuntimeError as e:
                if "channel closed" in str(e).lower():
                    raise ChannelClosed("Channel closed")
                raise

    def __aiter__(self):
        """Return async iterator for the channel.

        Example:
            async for msg in channel:
                process(msg)
        """
        return self

    async def __anext__(self):
        """Get next message asynchronously.

        Raises:
            StopAsyncIteration: When the channel is closed.
        """
        try:
            return await self.async_receive()
        except ChannelClosed:
            raise StopAsyncIteration

    def _is_closed(self):
        """Check if the channel is closed."""
        import erlang
        try:
            return erlang._channel_is_closed(self._ref)
        except Exception:
            return True

    def close(self):
        """Close the channel.

        Closes the channel and wakes any waiting receivers with ChannelClosed.
        Safe to call multiple times - subsequent calls have no effect.

        Example:
            channel.close()  # Signal no more data
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
        return f"<Channel ref={self._ref!r}>"


def reply(pid, term):
    """Send a reply to an Erlang process.

    This allows Python code to send messages back to Erlang processes
    via enif_send, enabling bidirectional communication.

    Args:
        pid: The Erlang PID to send to (from erlang.self() or passed in).
        term: The Python value to send (will be converted to Erlang term).

    Returns:
        True on success.

    Raises:
        RuntimeError: If the send fails.

    Example:
        # Python handler receives a request with the sender's PID
        def handle_request(request):
            sender_pid = request['reply_to']
            result = compute_something()
            reply(sender_pid, {'status': 'ok', 'result': result})
    """
    import erlang

    # Use erlang.send for fire-and-forget message sending
    # This directly calls enif_send without requiring callback handling
    result = erlang.send(pid, term)

    if result == 'ok' or result is True:
        return True
    elif isinstance(result, tuple) and result[0] == 'error':
        raise RuntimeError(f"Reply failed: {result[1]}")
    else:
        return True
