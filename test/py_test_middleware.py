"""Test module for middleware-style exception handling.

This module tests that erlang.call() works correctly even when wrapped
in try/except blocks that catch and re-raise exceptions, simulating
ASGI/WSGI middleware behavior.
"""

import erlang


def call_with_try_except(key):
    """Wraps erlang.call in try/except that catches and re-raises."""
    try:
        return erlang.call('get_value', key)
    except Exception as e:
        # This simulates middleware logging
        raise


def call_with_base_exception(key):
    """Wraps erlang.call in try/except that catches BaseException."""
    try:
        return erlang.call('get_value', key)
    except BaseException as e:
        # Catches everything including SuspensionRequired
        raise


def call_with_nested_try(key):
    """Multiple nested try/except blocks."""
    try:
        try:
            return erlang.call('get_value', key)
        except Exception:
            raise
    except Exception:
        raise


def call_with_finally(key):
    """Try/finally pattern common in cleanup code."""
    cleanup_ran = False
    try:
        return erlang.call('get_value', key)
    finally:
        cleanup_ran = True


def call_through_layers(key):
    """Simulates calling through multiple middleware layers."""
    def inner():
        try:
            return erlang.call('get_value', key)
        except Exception:
            raise

    def outer():
        try:
            return inner()
        except Exception:
            raise

    return outer()
