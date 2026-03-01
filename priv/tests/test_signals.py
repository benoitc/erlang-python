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
Signal handling tests adapted from uvloop's test_signals.py.

These tests verify signal handler functionality:
- add_signal_handler
- remove_signal_handler
- Signal delivery
"""

import asyncio
import os
import signal
import sys
import threading
import unittest

from . import _testbase as tb


def _signals_available():
    """Check if signals are available on this platform."""
    return sys.platform != 'win32'


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class _TestSignalHandler:
    """Tests for signal handler functionality."""

    def test_add_signal_handler(self):
        """Test adding a signal handler."""
        results = []

        def handler():
            results.append('signal')
            self.loop.stop()

        # Use SIGUSR1 to avoid interfering with test runner
        self.loop.add_signal_handler(signal.SIGUSR1, handler)

        # Send signal
        self.loop.call_soon(lambda: os.kill(os.getpid(), signal.SIGUSR1))
        self.loop.run_forever()

        self.assertEqual(results, ['signal'])

        # Cleanup
        self.loop.remove_signal_handler(signal.SIGUSR1)

    def test_add_signal_handler_with_args(self):
        """Test signal handler with arguments."""
        results = []

        def handler(x, y):
            results.append((x, y))
            self.loop.stop()

        self.loop.add_signal_handler(signal.SIGUSR1, handler, 'a', 'b')

        self.loop.call_soon(lambda: os.kill(os.getpid(), signal.SIGUSR1))
        self.loop.run_forever()

        self.assertEqual(results, [('a', 'b')])

        self.loop.remove_signal_handler(signal.SIGUSR1)

    def test_remove_signal_handler(self):
        """Test removing a signal handler."""
        results = []

        def handler():
            results.append('signal')

        self.loop.add_signal_handler(signal.SIGUSR1, handler)
        removed = self.loop.remove_signal_handler(signal.SIGUSR1)

        self.assertTrue(removed)

        # Remove again should return False
        removed = self.loop.remove_signal_handler(signal.SIGUSR1)
        self.assertFalse(removed)

    def test_remove_nonexistent_handler(self):
        """Test removing a handler that doesn't exist."""
        removed = self.loop.remove_signal_handler(signal.SIGUSR2)
        self.assertFalse(removed)

    def test_replace_signal_handler(self):
        """Test replacing an existing signal handler."""
        results = []

        def handler1():
            results.append('handler1')
            self.loop.stop()

        def handler2():
            results.append('handler2')
            self.loop.stop()

        self.loop.add_signal_handler(signal.SIGUSR1, handler1)
        self.loop.add_signal_handler(signal.SIGUSR1, handler2)  # Replaces

        self.loop.call_soon(lambda: os.kill(os.getpid(), signal.SIGUSR1))
        self.loop.run_forever()

        # Should only have handler2's result
        self.assertEqual(results, ['handler2'])

        self.loop.remove_signal_handler(signal.SIGUSR1)


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class _TestSignalMultiple:
    """Tests for multiple signal handlers."""

    def test_multiple_signals(self):
        """Test handling multiple different signals."""
        results = []
        count = [0]

        def handler(sig_name):
            results.append(sig_name)
            count[0] += 1
            if count[0] >= 2:
                self.loop.stop()

        self.loop.add_signal_handler(signal.SIGUSR1, handler, 'SIGUSR1')
        self.loop.add_signal_handler(signal.SIGUSR2, handler, 'SIGUSR2')

        def send_signals():
            os.kill(os.getpid(), signal.SIGUSR1)
            os.kill(os.getpid(), signal.SIGUSR2)

        self.loop.call_soon(send_signals)
        self.loop.run_forever()

        self.assertEqual(sorted(results), ['SIGUSR1', 'SIGUSR2'])

        self.loop.remove_signal_handler(signal.SIGUSR1)
        self.loop.remove_signal_handler(signal.SIGUSR2)


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class _TestSignalRestrictions:
    """Tests for signal handler restrictions."""

    def test_signal_handler_on_closed_loop(self):
        """Test adding signal handler on closed loop."""
        self.loop.close()

        def handler():
            pass

        with self.assertRaises(RuntimeError):
            self.loop.add_signal_handler(signal.SIGUSR1, handler)

        # Recreate loop for teardown
        self.loop = self.new_loop()


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class _TestSignalDelivery:
    """Tests for signal delivery behavior."""

    def test_signal_delivery_during_io(self):
        """Test signal delivery during I/O wait."""
        results = []

        def handler():
            results.append('signal')

        self.loop.add_signal_handler(signal.SIGUSR1, handler)

        async def main():
            # Start an I/O wait
            await asyncio.sleep(0.1)
            return True

        # Send signal during sleep
        def send_signal():
            os.kill(os.getpid(), signal.SIGUSR1)

        self.loop.call_later(0.05, send_signal)

        result = self.loop.run_until_complete(main())

        self.assertTrue(result)
        self.assertEqual(results, ['signal'])

        self.loop.remove_signal_handler(signal.SIGUSR1)

    def test_signal_handler_stops_loop(self):
        """Test that signal handler can stop the loop."""
        def handler():
            self.loop.stop()

        self.loop.add_signal_handler(signal.SIGUSR1, handler)

        # Send signal after a delay
        self.loop.call_later(0.05, lambda: os.kill(os.getpid(), signal.SIGUSR1))

        # This should stop because of the signal
        self.loop.run_forever()

        self.assertFalse(self.loop.is_running())

        self.loop.remove_signal_handler(signal.SIGUSR1)


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

# -----------------------------------------------------------------------------
# Erlang tests: ErlangEventLoop has limited signal support (SIGINT, SIGTERM,
# SIGHUP only). Other signals raise ValueError. These tests verify that
# unsupported signals are handled correctly.
# -----------------------------------------------------------------------------


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class TestErlangSignalLimitedSupport(tb.ErlangTestCase):
    """Test ErlangEventLoop's limited signal handling support.

    ErlangEventLoop only supports SIGINT, SIGTERM, and SIGHUP.
    Other signals like SIGUSR1/SIGUSR2 raise ValueError.
    """

    def test_add_unsupported_signal_raises_valueerror(self):
        """add_signal_handler for unsupported signals should raise ValueError."""
        with self.assertRaises(ValueError):
            self.loop.add_signal_handler(signal.SIGUSR1, lambda: None)

    def test_add_unsupported_signal_with_args_raises_valueerror(self):
        """add_signal_handler with args for unsupported signal raises ValueError."""
        with self.assertRaises(ValueError):
            self.loop.add_signal_handler(signal.SIGUSR1, lambda x, y: None, 'a', 'b')

    def test_remove_nonexistent_handler_returns_false(self):
        """remove_signal_handler for non-existent handler returns False."""
        result = self.loop.remove_signal_handler(signal.SIGUSR1)
        self.assertFalse(result)

    def test_remove_different_nonexistent_handler_returns_false(self):
        """remove_signal_handler for SIGUSR2 returns False when not registered."""
        result = self.loop.remove_signal_handler(signal.SIGUSR2)
        self.assertFalse(result)


# -----------------------------------------------------------------------------
# AIO tests: Standard asyncio does support signal handling, so these tests
# verify normal signal functionality works with the asyncio event loop.
# -----------------------------------------------------------------------------


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class TestAIOSignalHandler(_TestSignalHandler, tb.AIOTestCase):
    pass


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class TestAIOSignalMultiple(_TestSignalMultiple, tb.AIOTestCase):
    pass


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class TestAIOSignalRestrictions(_TestSignalRestrictions, tb.AIOTestCase):
    pass


@unittest.skipUnless(_signals_available(), "Signals not available on this platform")
class TestAIOSignalDelivery(_TestSignalDelivery, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
