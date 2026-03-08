#!/usr/bin/env python3
"""Benchmark for ReactorBuffer zero-copy performance.

Compares:
1. ReactorBuffer with zero-copy access (buffer protocol)
2. Regular bytes with data copying

Run: python3 examples/bench_reactor_buffer.py
"""

import time
import socket
import statistics
import sys

sys.path.insert(0, 'priv')


def bench_buffer_operations(iterations=10000):
    """Benchmark common buffer operations."""
    import erlang

    results = {}

    # Test data sizes
    sizes = [64, 256, 1024, 4096, 16384, 65536]

    for size in sizes:
        test_data = b'X' * size

        # Create ReactorBuffer
        buf = erlang.ReactorBuffer._test_create(test_data)
        regular_bytes = bytes(test_data)

        # Benchmark: memoryview access (zero-copy)
        start = time.perf_counter()
        for _ in range(iterations):
            mv = memoryview(buf)
            _ = mv[0]
        mv_time = time.perf_counter() - start

        # Benchmark: memoryview on regular bytes
        start = time.perf_counter()
        for _ in range(iterations):
            mv = memoryview(regular_bytes)
            _ = mv[0]
        mv_bytes_time = time.perf_counter() - start

        # Benchmark: extend bytearray (uses buffer protocol)
        start = time.perf_counter()
        for _ in range(iterations):
            ba = bytearray()
            ba.extend(buf)
        extend_buf_time = time.perf_counter() - start

        # Benchmark: extend bytearray from bytes
        start = time.perf_counter()
        for _ in range(iterations):
            ba = bytearray()
            ba.extend(regular_bytes)
        extend_bytes_time = time.perf_counter() - start

        # Benchmark: slice operation
        slice_len = min(100, size)
        start = time.perf_counter()
        for _ in range(iterations):
            _ = buf[0:slice_len]
        slice_buf_time = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(iterations):
            _ = regular_bytes[0:slice_len]
        slice_bytes_time = time.perf_counter() - start

        # Benchmark: startswith
        prefix = test_data[:10]
        start = time.perf_counter()
        for _ in range(iterations):
            _ = buf.startswith(prefix)
        startswith_buf_time = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(iterations):
            _ = regular_bytes.startswith(prefix)
        startswith_bytes_time = time.perf_counter() - start

        results[size] = {
            'memoryview': {'buffer': mv_time, 'bytes': mv_bytes_time},
            'extend': {'buffer': extend_buf_time, 'bytes': extend_bytes_time},
            'slice': {'buffer': slice_buf_time, 'bytes': slice_bytes_time},
            'startswith': {'buffer': startswith_buf_time, 'bytes': startswith_bytes_time},
        }

    return results


def bench_protocol_simulation(iterations=1000):
    """Simulate protocol data_received with different payload sizes."""
    import erlang

    results = {}
    sizes = [64, 256, 1024, 4096, 16384, 65536]

    for size in sizes:
        test_data = b'GET / HTTP/1.1\r\nHost: example.com\r\n\r\n' + b'X' * (size - 40)
        test_data = test_data[:size]  # Ensure exact size

        buf = erlang.ReactorBuffer._test_create(test_data)
        regular_bytes = bytes(test_data)

        # Simulate typical protocol parsing with ReactorBuffer
        def parse_with_buffer(data):
            # Check method
            if data.startswith(b'GET'):
                method = 'GET'
            elif data.startswith(b'POST'):
                method = 'POST'
            else:
                method = 'OTHER'

            # Find header end
            pos = data.find(b'\r\n\r\n')

            # Buffer to write buffer
            write_buf = bytearray()
            write_buf.extend(data)

            return len(write_buf)

        # Benchmark with ReactorBuffer
        start = time.perf_counter()
        for _ in range(iterations):
            _ = parse_with_buffer(buf)
        buf_time = time.perf_counter() - start

        # Benchmark with regular bytes
        start = time.perf_counter()
        for _ in range(iterations):
            _ = parse_with_buffer(regular_bytes)
        bytes_time = time.perf_counter() - start

        results[size] = {
            'buffer_time': buf_time,
            'bytes_time': bytes_time,
            'ops_per_sec_buffer': iterations / buf_time,
            'ops_per_sec_bytes': iterations / bytes_time,
        }

    return results


def bench_echo_protocol(iterations=500):
    """Benchmark echo protocol pattern with socketpair."""
    import erlang.reactor as reactor

    results = {}
    sizes = [64, 256, 1024, 4096, 16384]

    class EchoProtocol(reactor.Protocol):
        def data_received(self, data):
            self.write_buffer.extend(data)
            return 'write_pending'

        def write_ready(self):
            if not self.write_buffer:
                return 'read_pending'
            written = self.write(bytes(self.write_buffer))
            del self.write_buffer[:written]
            return 'continue' if self.write_buffer else 'read_pending'

    for size in sizes:
        test_data = b'X' * size
        times = []

        for _ in range(iterations):
            s1, s2 = socket.socketpair()
            s1.setblocking(False)
            s2.setblocking(False)

            try:
                reactor.set_protocol_factory(EchoProtocol)
                reactor.init_connection(s1.fileno(), {'type': 'test'})

                s2.send(test_data)

                start = time.perf_counter()
                action = reactor.on_read_ready(s1.fileno())
                elapsed = time.perf_counter() - start
                times.append(elapsed)

                reactor.close_connection(s1.fileno())
            finally:
                s1.close()
                s2.close()

        avg_time = statistics.mean(times)
        results[size] = {
            'avg_time_ms': avg_time * 1000,
            'ops_per_sec': 1.0 / avg_time,
            'p50_ms': statistics.median(times) * 1000,
            'p95_ms': sorted(times)[int(len(times) * 0.95)] * 1000,
        }

    return results


def format_results(name, results):
    """Format benchmark results."""
    print(f"\n{'='*60}")
    print(f" {name}")
    print(f"{'='*60}")

    if 'memoryview' in list(results.values())[0]:
        # Buffer operations format
        print(f"{'Size':>8} | {'Operation':>12} | {'Buffer (ms)':>12} | {'Bytes (ms)':>12} | {'Ratio':>8}")
        print("-" * 60)
        for size, ops in sorted(results.items()):
            for op_name, times in ops.items():
                buf_ms = times['buffer'] * 1000
                bytes_ms = times['bytes'] * 1000
                ratio = bytes_ms / buf_ms if buf_ms > 0 else 0
                print(f"{size:>8} | {op_name:>12} | {buf_ms:>12.3f} | {bytes_ms:>12.3f} | {ratio:>7.2f}x")
    elif 'buffer_time' in list(results.values())[0]:
        # Protocol simulation format
        print(f"{'Size':>8} | {'Buffer (ops/s)':>14} | {'Bytes (ops/s)':>14} | {'Speedup':>8}")
        print("-" * 60)
        for size, data in sorted(results.items()):
            buf_ops = data['ops_per_sec_buffer']
            bytes_ops = data['ops_per_sec_bytes']
            speedup = buf_ops / bytes_ops if bytes_ops > 0 else 0
            print(f"{size:>8} | {buf_ops:>14.0f} | {bytes_ops:>14.0f} | {speedup:>7.2f}x")
    else:
        # Echo protocol format
        print(f"{'Size':>8} | {'Avg (ms)':>10} | {'P50 (ms)':>10} | {'P95 (ms)':>10} | {'Ops/sec':>10}")
        print("-" * 60)
        for size, data in sorted(results.items()):
            print(f"{size:>8} | {data['avg_time_ms']:>10.3f} | {data['p50_ms']:>10.3f} | {data['p95_ms']:>10.3f} | {data['ops_per_sec']:>10.0f}")


def main():
    print("\n" + "=" * 60)
    print(" ReactorBuffer Zero-Copy Benchmark")
    print("=" * 60)

    # Check if ReactorBuffer is available
    try:
        import erlang
        buf = erlang.ReactorBuffer._test_create(b'test')
        print(f"ReactorBuffer available: {type(buf).__name__}")
    except Exception as e:
        print(f"ERROR: ReactorBuffer not available: {e}")
        sys.exit(1)

    print("\nRunning benchmarks...")

    # Run benchmarks
    print("\n[1/3] Buffer operations benchmark...")
    buffer_ops = bench_buffer_operations(iterations=10000)
    format_results("Buffer Operations (10000 iterations)", buffer_ops)

    print("\n[2/3] Protocol simulation benchmark...")
    protocol_sim = bench_protocol_simulation(iterations=5000)
    format_results("Protocol Simulation (5000 iterations)", protocol_sim)

    print("\n[3/3] Echo protocol benchmark...")
    echo_proto = bench_echo_protocol(iterations=200)
    format_results("Echo Protocol (200 iterations)", echo_proto)

    # Summary
    print("\n" + "=" * 60)
    print(" Summary")
    print("=" * 60)

    # Calculate average speedup for large payloads (>= 1KB)
    large_payload_speedups = []
    for size, data in protocol_sim.items():
        if size >= 1024:
            speedup = data['ops_per_sec_buffer'] / data['ops_per_sec_bytes']
            large_payload_speedups.append(speedup)

    if large_payload_speedups:
        avg_speedup = statistics.mean(large_payload_speedups)
        print(f"Average speedup for payloads >= 1KB: {avg_speedup:.2f}x")
        improvement = (avg_speedup - 1.0) * 100
        print(f"Performance improvement: {improvement:+.1f}%")

    print("\nDone.")


if __name__ == '__main__':
    main()
