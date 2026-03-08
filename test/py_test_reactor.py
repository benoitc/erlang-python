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


def test_reactor_buffer():
    """Test ReactorBuffer behaves like bytes."""
    import erlang

    # Get ReactorBuffer type from erlang module
    ReactorBuffer = erlang.ReactorBuffer

    # Create test buffer using the _test_create class method
    test_data = b'GET / HTTP/1.1\r\n'
    buf = ReactorBuffer._test_create(test_data)

    # Test len()
    assert len(buf) == len(test_data), f"len mismatch: {len(buf)} != {len(test_data)}"

    # Test startswith()
    assert buf.startswith(b'GET'), "startswith(b'GET') failed"
    assert buf.startswith(b'GET /'), "startswith(b'GET /') failed"
    assert not buf.startswith(b'POST'), "startswith(b'POST') should be False"

    # Test endswith()
    assert buf.endswith(b'\r\n'), "endswith(b'\\r\\n') failed"
    assert not buf.endswith(b'END'), "endswith(b'END') should be False"

    # Test indexing
    assert buf[0] == ord('G'), f"buf[0] should be ord('G'), got {buf[0]}"
    assert buf[-1] == ord('\n'), f"buf[-1] should be ord('\\n'), got {buf[-1]}"

    # Test slicing
    assert buf[0:3] == b'GET', f"buf[0:3] should be b'GET', got {buf[0:3]}"
    assert buf[-2:] == b'\r\n', f"buf[-2:] should be b'\\r\\n', got {buf[-2:]}"

    # Test bytes() conversion
    assert bytes(buf) == test_data, f"bytes(buf) mismatch"

    # Test memoryview
    mv = memoryview(buf)
    assert mv[0] == ord('G'), f"memoryview[0] should be ord('G')"
    assert bytes(mv) == test_data, "memoryview bytes mismatch"

    # Test find()
    assert buf.find(b'HTTP') == 6, f"find(b'HTTP') should be 6, got {buf.find(b'HTTP')}"
    assert buf.find(b'HTTPS') == -1, "find(b'HTTPS') should be -1"

    # Test rfind()
    assert buf.rfind(b'/') == 4, f"rfind(b'/') should be 4, got {buf.rfind(b'/')}"

    # Test count()
    assert buf.count(b'/') == 1, f"count(b'/') should be 1, got {buf.count(b'/')}"

    # Test 'in' operator
    assert b'HTTP' in buf, "b'HTTP' in buf should be True"
    assert b'HTTPS' not in buf, "b'HTTPS' not in buf should be True"

    # Test decode()
    decoded = buf.decode('utf-8')
    assert decoded == 'GET / HTTP/1.1\r\n', f"decode mismatch: {decoded}"

    # Test comparison with bytes
    assert buf == test_data, "buf == test_data should be True"
    assert buf != b'other', "buf != b'other' should be True"

    return True


def test_reactor_buffer_with_protocol():
    """Test ReactorBuffer works transparently in protocol data_received."""
    import sys
    sys.path.insert(0, 'priv')
    from _erlang_impl import reactor
    import erlang

    # Track what was received
    received_data = []
    received_types = []

    class TestProtocol(reactor.Protocol):
        def data_received(self, data):
            received_data.append(bytes(data))  # Convert to bytes for comparison
            received_types.append(type(data).__name__)
            # Test that bytes-like operations work
            if data.startswith(b'PING'):
                self.write_buffer.extend(b'PONG')
                return "write_pending"
            return "continue"

        def write_ready(self):
            if not self.write_buffer:
                return "read_pending"
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            return "continue" if self.write_buffer else "read_pending"

    import socket
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    try:
        reactor.set_protocol_factory(TestProtocol)
        reactor.init_connection(s1.fileno(), {'type': 'test'})

        # Send test data
        s2.send(b'PING')

        # Call on_read_ready - this will use ReactorBuffer internally
        action = reactor.on_read_ready(s1.fileno())

        # Verify the protocol received the data
        assert len(received_data) == 1, f"Expected 1 received, got {len(received_data)}"
        assert received_data[0] == b'PING', f"Expected b'PING', got {received_data[0]}"
        assert action == "write_pending", f"Expected 'write_pending', got {action}"

        # The type should be ReactorBuffer (not bytes) when using NIF
        # But if running without NIF initialization, it might be bytes
        # We accept both for compatibility
        assert received_types[0] in ('ReactorBuffer', 'bytes'), \
            f"Expected ReactorBuffer or bytes, got {received_types[0]}"

        reactor.close_connection(s1.fileno())

    finally:
        s1.close()
        s2.close()

    return True


def run_all_tests():
    """Run all reactor tests."""
    tests = [
        test_protocol_creation,
        test_echo_protocol,
        test_multiple_protocols,
        test_reactor_buffer,
        test_reactor_buffer_with_protocol,
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
