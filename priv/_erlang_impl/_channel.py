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

        This method integrates with the asyncio event loop, allowing other
        coroutines to run while waiting for data from Erlang.

        Returns:
            The received Erlang term, converted to Python.

        Raises:
            ChannelClosed: If the channel has been closed.

        Example:
            msg = await channel.async_receive()
        """
        import asyncio
        import erlang

        # Try non-blocking first
        result = self.try_receive()
        if result is not None:
            return result

        # Check if closed
        if self._is_closed():
            raise ChannelClosed("Channel has been closed")

        # Get the running event loop
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        # Get callback_id from the event loop
        callback_id = loop._next_id()

        # Get the loop capsule for registration with Erlang
        loop_capsule = loop._loop_capsule

        # Track this channel receive in the timers dict (reuses timer dispatch)
        # We store a tuple (channel, future) so we can resolve it
        self._pending_async = (callback_id, future)

        # Create a handle that will resolve the future when dispatched
        from asyncio import events

        def _resolve_callback():
            if not future.done():
                # Data should be available now, fetch it
                try:
                    result = self.try_receive()
                    if result is not None:
                        future.set_result(result)
                    elif self._is_closed():
                        future.set_exception(ChannelClosed("Channel has been closed"))
                    else:
                        # Rare edge case: woken but no data. Re-register.
                        future.set_exception(ChannelClosed("Channel state error"))
                except Exception as e:
                    future.set_exception(e)

        handle = events.Handle(_resolve_callback, (), loop)
        loop._timers[callback_id] = handle
        loop._handle_to_callback_id[id(handle)] = callback_id

        # Tell Erlang to dispatch callback_id when data arrives
        try:
            wait_result = erlang.call('_py_channel_wait', self._ref, callback_id, loop_capsule)

            # If data was immediately available, Erlang returns {ok, Data}
            if isinstance(wait_result, tuple) and len(wait_result) == 2:
                status, value = wait_result
                if status == 'ok':
                    # Data returned immediately - cancel the waiter and return
                    loop._timers.pop(callback_id, None)
                    loop._handle_to_callback_id.pop(id(handle), None)
                    return value
                elif status == 'error':
                    loop._timers.pop(callback_id, None)
                    loop._handle_to_callback_id.pop(id(handle), None)
                    if value == 'closed':
                        raise ChannelClosed("Channel has been closed")
                    else:
                        raise RuntimeError(f"Channel wait error: {value}")
        except Exception as e:
            # Cleanup on error
            loop._timers.pop(callback_id, None)
            loop._handle_to_callback_id.pop(id(handle), None)
            raise

        try:
            return await future
        finally:
            # Cleanup on cancel or completion
            loop._timers.pop(callback_id, None)
            loop._handle_to_callback_id.pop(id(handle), None)
            # Cancel the waiter in Erlang if still registered
            try:
                erlang.call('_py_channel_cancel_wait', self._ref, callback_id)
            except Exception:
                pass

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
