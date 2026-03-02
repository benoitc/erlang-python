"""Test module for erlang.reactor functionality.

This module provides Python-side tests for the reactor API.
These can be called from Erlang tests or run standalone.
"""

import socket


def test_protocol_creation():
    """Test Protocol class can be instantiated and subclassed."""
    import sys
    sys.path.insert(0, 'priv')
    from _erlang_impl import reactor

    # Test base protocol
    proto = reactor.Protocol()
    assert proto.fd == -1
    assert proto.client_info == {}
    assert proto.write_buffer == bytearray()
    assert proto.closed is False

    # Test connection_made
    proto.connection_made(42, {'addr': '127.0.0.1', 'port': 8080})
    assert proto.fd == 42
    assert proto.client_info == {'addr': '127.0.0.1', 'port': 8080}

    return True


def test_echo_protocol():
    """Test a simple echo protocol with socketpair."""
    import sys
    sys.path.insert(0, 'priv')
    from _erlang_impl import reactor

    class EchoProtocol(reactor.Protocol):
        def data_received(self, data):
            self.write_buffer.extend(data)
            return "write_pending"

        def write_ready(self):
            if not self.write_buffer:
                return "read_pending"
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            return "continue" if self.write_buffer else "read_pending"

    # Create socketpair for testing
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    try:
        # Set up protocol
        reactor.set_protocol_factory(EchoProtocol)
        reactor.init_connection(s1.fileno(), {'type': 'test'})

        # Send data through s2
        s2.send(b'hello world')

        # Trigger read on protocol side
        action = reactor.on_read_ready(s1.fileno())
        assert action == "write_pending"

        # Check write buffer
        proto = reactor.get_protocol(s1.fileno())
        assert bytes(proto.write_buffer) == b'hello world'

        # Trigger write
        action = reactor.on_write_ready(s1.fileno())
        # Should be read_pending since buffer was flushed
        assert action == "read_pending"

        # Read the echoed data
        echoed = s2.recv(1024)
        assert echoed == b'hello world'

        # Clean up
        reactor.close_connection(s1.fileno())
        assert proto.closed is True

    finally:
        s1.close()
        s2.close()

    return True


def test_multiple_protocols():
    """Test multiple protocols can be registered simultaneously."""
    import sys
    sys.path.insert(0, 'priv')
    from _erlang_impl import reactor

    class SimpleProtocol(reactor.Protocol):
        instances = []

        def __init__(self):
            super().__init__()
            SimpleProtocol.instances.append(self)

        def data_received(self, data):
            return "continue"

        def write_ready(self):
            return "close"

    # Reset instances
    SimpleProtocol.instances = []

    # Create multiple socketpairs
    pairs = []
    for _ in range(5):
        s1, s2 = socket.socketpair()
        s1.setblocking(False)
        pairs.append((s1, s2))

    try:
        # Set factory and init connections
        reactor.set_protocol_factory(SimpleProtocol)
        for s1, s2 in pairs:
            reactor.init_connection(s1.fileno(), {})

        # Verify all instances created
        assert len(SimpleProtocol.instances) == 5

        # Verify each has correct fd
        for (s1, s2), proto in zip(pairs, SimpleProtocol.instances):
            assert proto.fd == s1.fileno()

        # Clean up
        for s1, s2 in pairs:
            reactor.close_connection(s1.fileno())

    finally:
        for s1, s2 in pairs:
            s1.close()
            s2.close()

    return True


def run_all_tests():
    """Run all reactor tests."""
    tests = [
        test_protocol_creation,
        test_echo_protocol,
        test_multiple_protocols,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, 'PASS' if result else 'FAIL'))
        except Exception as e:
            results.append((test.__name__, f'ERROR: {e}'))

    return results


if __name__ == '__main__':
    results = run_all_tests()
    for name, status in results:
        print(f'{name}: {status}')
