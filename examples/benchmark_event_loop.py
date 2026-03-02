# Benchmark: Standard asyncio vs Erlang Event Loop
#
# This script compares the performance of Python's standard asyncio
# event loop against the Erlang-native event loop implementation.
#
# Run from Erlang shell:
#   rebar3 compile && rebar3 shell
#   py:exec(<<"exec(open('examples/benchmark_event_loop.py').read())">>).

import asyncio
import time
import socket
import statistics
import sys


# =============================================================================
# Benchmark Configuration
# =============================================================================

TIMER_ITERATIONS = 100
TIMER_DELAY_MS = 1  # Target delay in milliseconds

TCP_MESSAGES = 1000
TCP_MESSAGE_SIZE = 64

UDP_MESSAGES = 500
UDP_MESSAGE_SIZE = 64

IDLE_DURATION_SEC = 1.0


# =============================================================================
# Timer Latency Benchmark
# =============================================================================

async def benchmark_timer_latency(iterations=TIMER_ITERATIONS, delay_ms=TIMER_DELAY_MS):
    """Measure asyncio.sleep() precision.

    Target: 1ms delay, measure actual latency vs expected.
    Returns dict with latency statistics.
    """
    delay_sec = delay_ms / 1000.0
    latencies = []

    for _ in range(iterations):
        start = time.perf_counter()
        await asyncio.sleep(delay_sec)
        elapsed = time.perf_counter() - start
        latency_ms = (elapsed - delay_sec) * 1000
        latencies.append(latency_ms)

    return {
        'target_ms': delay_ms,
        'mean_latency_ms': statistics.mean(latencies),
        'median_latency_ms': statistics.median(latencies),
        'stdev_latency_ms': statistics.stdev(latencies) if len(latencies) > 1 else 0,
        'min_latency_ms': min(latencies),
        'max_latency_ms': max(latencies),
        'p95_latency_ms': sorted(latencies)[int(len(latencies) * 0.95)],
        'iterations': iterations,
    }


# =============================================================================
# Idle CPU Usage Benchmark
# =============================================================================

async def benchmark_idle(duration_sec=IDLE_DURATION_SEC):
    """Run idle loop and report approximate overhead.

    Measures how many event loop iterations occur when idle.
    Lower is better (less busy-waiting).
    """
    iterations = 0
    start = time.perf_counter()
    end_time = start + duration_sec

    while time.perf_counter() < end_time:
        await asyncio.sleep(0.01)  # 10ms idle periods
        iterations += 1

    actual_duration = time.perf_counter() - start

    return {
        'duration_sec': actual_duration,
        'iterations': iterations,
        'iterations_per_sec': iterations / actual_duration,
    }


# =============================================================================
# TCP Echo Throughput Benchmark
# =============================================================================

class EchoServerProtocol(asyncio.Protocol):
    """TCP echo server protocol."""

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)


class EchoClientProtocol(asyncio.Protocol):
    """TCP echo client protocol for benchmarking."""

    def __init__(self, messages, message_size, on_complete):
        self.messages = messages
        self.message_size = message_size
        self.on_complete = on_complete
        self.sent = 0
        self.received = 0
        self.buffer = b''
        self.start_time = None

    def connection_made(self, transport):
        self.transport = transport
        self.start_time = time.perf_counter()
        self._send_next()

    def _send_next(self):
        if self.sent < self.messages:
            data = b'x' * self.message_size
            self.transport.write(data)
            self.sent += 1

    def data_received(self, data):
        self.buffer += data
        while len(self.buffer) >= self.message_size:
            self.buffer = self.buffer[self.message_size:]
            self.received += 1
            if self.received < self.messages:
                self._send_next()
            elif self.received == self.messages:
                elapsed = time.perf_counter() - self.start_time
                self.on_complete.set_result(elapsed)
                self.transport.close()


async def benchmark_tcp_echo(messages=TCP_MESSAGES, message_size=TCP_MESSAGE_SIZE):
    """Run TCP echo server/client and measure messages/sec.

    Returns throughput statistics.
    """
    loop = asyncio.get_running_loop()

    # Start server
    server = await loop.create_server(
        EchoServerProtocol,
        '127.0.0.1', 0,
        reuse_address=True
    )
    port = server.sockets[0].getsockname()[1]

    # Run client
    complete_future = loop.create_future()
    transport, protocol = await loop.create_connection(
        lambda: EchoClientProtocol(messages, message_size, complete_future),
        '127.0.0.1', port
    )

    elapsed = await complete_future
    server.close()
    await server.wait_closed()

    messages_per_sec = messages / elapsed
    bytes_per_sec = (messages * message_size * 2) / elapsed  # *2 for echo

    return {
        'messages': messages,
        'message_size': message_size,
        'elapsed_sec': elapsed,
        'messages_per_sec': messages_per_sec,
        'throughput_mb_sec': bytes_per_sec / (1024 * 1024),
    }


# =============================================================================
# UDP Echo Latency Benchmark
# =============================================================================

class UDPEchoServerProtocol(asyncio.DatagramProtocol):
    """UDP echo server protocol."""

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.transport.sendto(data, addr)


class UDPEchoClientProtocol(asyncio.DatagramProtocol):
    """UDP echo client protocol for benchmarking."""

    def __init__(self, messages, message_size, on_complete):
        self.messages = messages
        self.message_size = message_size
        self.on_complete = on_complete
        self.sent = 0
        self.received = 0
        self.latencies = []
        self.send_times = {}
        self.start_time = None
        self.seq = 0

    def connection_made(self, transport):
        self.transport = transport
        self.start_time = time.perf_counter()
        self._send_next()

    def _send_next(self):
        if self.sent < self.messages:
            # Include sequence number in message for matching
            seq_bytes = self.seq.to_bytes(4, 'big')
            data = seq_bytes + b'x' * (self.message_size - 4)
            self.send_times[self.seq] = time.perf_counter()
            self.transport.sendto(data)
            self.seq += 1
            self.sent += 1

    def datagram_received(self, data, addr):
        recv_time = time.perf_counter()
        seq = int.from_bytes(data[:4], 'big')
        if seq in self.send_times:
            latency = (recv_time - self.send_times[seq]) * 1000  # ms
            self.latencies.append(latency)
            del self.send_times[seq]

        self.received += 1
        if self.received < self.messages:
            self._send_next()
        elif self.received == self.messages:
            elapsed = time.perf_counter() - self.start_time
            self.on_complete.set_result((elapsed, self.latencies))
            self.transport.close()


async def benchmark_udp_echo(messages=UDP_MESSAGES, message_size=UDP_MESSAGE_SIZE):
    """Run UDP echo and measure round-trip latency.

    Returns latency statistics.
    """
    loop = asyncio.get_running_loop()

    # Start server
    server_transport, _ = await loop.create_datagram_endpoint(
        UDPEchoServerProtocol,
        local_addr=('127.0.0.1', 0)
    )
    server_port = server_transport.get_extra_info('sockname')[1]

    # Run client
    complete_future = loop.create_future()
    client_transport, client_protocol = await loop.create_datagram_endpoint(
        lambda: UDPEchoClientProtocol(messages, message_size, complete_future),
        remote_addr=('127.0.0.1', server_port)
    )

    elapsed, latencies = await complete_future
    server_transport.close()

    return {
        'messages': messages,
        'message_size': message_size,
        'elapsed_sec': elapsed,
        'messages_per_sec': messages / elapsed,
        'mean_latency_ms': statistics.mean(latencies),
        'median_latency_ms': statistics.median(latencies),
        'stdev_latency_ms': statistics.stdev(latencies) if len(latencies) > 1 else 0,
        'min_latency_ms': min(latencies),
        'max_latency_ms': max(latencies),
        'p95_latency_ms': sorted(latencies)[int(len(latencies) * 0.95)],
    }


# =============================================================================
# Benchmark Runner
# =============================================================================

def run_benchmark_suite(loop_name, loop):
    """Run all benchmarks with the given event loop."""
    results = {'loop_name': loop_name}

    print(f"\n{'='*60}")
    print(f"Running benchmarks with: {loop_name}")
    print('='*60)

    asyncio.set_event_loop(loop)

    # Timer latency
    print(f"\n[{loop_name}] Timer Latency (target: {TIMER_DELAY_MS}ms)...")
    try:
        timer_result = loop.run_until_complete(benchmark_timer_latency())
        results['timer'] = timer_result
        print(f"  Mean latency:   {timer_result['mean_latency_ms']:.3f} ms")
        print(f"  Median latency: {timer_result['median_latency_ms']:.3f} ms")
        print(f"  P95 latency:    {timer_result['p95_latency_ms']:.3f} ms")
    except Exception as e:
        print(f"  ERROR: {e}")
        results['timer'] = {'error': str(e)}

    # Idle CPU
    print(f"\n[{loop_name}] Idle CPU ({IDLE_DURATION_SEC}s)...")
    try:
        idle_result = loop.run_until_complete(benchmark_idle())
        results['idle'] = idle_result
        print(f"  Iterations:     {idle_result['iterations']}")
        print(f"  Rate:           {idle_result['iterations_per_sec']:.1f} iter/sec")
    except Exception as e:
        print(f"  ERROR: {e}")
        results['idle'] = {'error': str(e)}

    # TCP throughput
    print(f"\n[{loop_name}] TCP Echo ({TCP_MESSAGES} messages, {TCP_MESSAGE_SIZE}B)...")
    try:
        tcp_result = loop.run_until_complete(benchmark_tcp_echo())
        results['tcp'] = tcp_result
        print(f"  Elapsed:        {tcp_result['elapsed_sec']:.3f} sec")
        print(f"  Throughput:     {tcp_result['messages_per_sec']:.0f} msg/sec")
        print(f"  Bandwidth:      {tcp_result['throughput_mb_sec']:.2f} MB/sec")
    except Exception as e:
        print(f"  ERROR: {e}")
        results['tcp'] = {'error': str(e)}

    # UDP latency
    print(f"\n[{loop_name}] UDP Echo ({UDP_MESSAGES} messages, {UDP_MESSAGE_SIZE}B)...")
    try:
        udp_result = loop.run_until_complete(benchmark_udp_echo())
        results['udp'] = udp_result
        print(f"  Elapsed:        {udp_result['elapsed_sec']:.3f} sec")
        print(f"  Throughput:     {udp_result['messages_per_sec']:.0f} msg/sec")
        print(f"  Mean RTT:       {udp_result['mean_latency_ms']:.3f} ms")
        print(f"  P95 RTT:        {udp_result['p95_latency_ms']:.3f} ms")
    except Exception as e:
        print(f"  ERROR: {e}")
        results['udp'] = {'error': str(e)}

    return results


def print_comparison(std_results, erlang_results):
    """Print comparison table of results."""
    print("\n")
    print("="*70)
    print("COMPARISON: Standard asyncio vs Erlang Event Loop")
    print("="*70)

    def compare_metric(name, std_val, erl_val, lower_better=True):
        """Compare two metrics and show improvement."""
        if std_val is None or erl_val is None:
            return
        if lower_better:
            if std_val > 0:
                improvement = ((std_val - erl_val) / std_val) * 100
                better = "Erlang" if improvement > 0 else "Standard"
            else:
                improvement = 0
                better = "N/A"
        else:
            if erl_val > 0:
                improvement = ((erl_val - std_val) / std_val) * 100 if std_val > 0 else 0
                better = "Erlang" if improvement > 0 else "Standard"
            else:
                improvement = 0
                better = "N/A"

        arrow = "↓" if (lower_better and improvement > 0) or (not lower_better and improvement > 0) else "↑"
        print(f"  {name:25} {std_val:>12.3f}  {erl_val:>12.3f}  {abs(improvement):>6.1f}% {arrow} ({better})")

    # Timer comparison
    print(f"\n{'Timer Latency':-^70}")
    print(f"  {'Metric':25} {'Standard':>12}  {'Erlang':>12}  {'Change':>14}")
    print(f"  {'-'*25} {'-'*12}  {'-'*12}  {'-'*14}")
    if 'error' not in std_results.get('timer', {}) and 'error' not in erlang_results.get('timer', {}):
        compare_metric("Mean latency (ms)",
                      std_results['timer']['mean_latency_ms'],
                      erlang_results['timer']['mean_latency_ms'], True)
        compare_metric("P95 latency (ms)",
                      std_results['timer']['p95_latency_ms'],
                      erlang_results['timer']['p95_latency_ms'], True)
    else:
        print("  (skipped due to errors)")

    # TCP comparison
    print(f"\n{'TCP Echo Throughput':-^70}")
    print(f"  {'Metric':25} {'Standard':>12}  {'Erlang':>12}  {'Change':>14}")
    print(f"  {'-'*25} {'-'*12}  {'-'*12}  {'-'*14}")
    if 'error' not in std_results.get('tcp', {}) and 'error' not in erlang_results.get('tcp', {}):
        compare_metric("Messages/sec",
                      std_results['tcp']['messages_per_sec'],
                      erlang_results['tcp']['messages_per_sec'], False)
        compare_metric("Throughput (MB/sec)",
                      std_results['tcp']['throughput_mb_sec'],
                      erlang_results['tcp']['throughput_mb_sec'], False)
    else:
        print("  (skipped due to errors)")

    # UDP comparison
    print(f"\n{'UDP Echo Latency':-^70}")
    print(f"  {'Metric':25} {'Standard':>12}  {'Erlang':>12}  {'Change':>14}")
    print(f"  {'-'*25} {'-'*12}  {'-'*12}  {'-'*14}")
    if 'error' not in std_results.get('udp', {}) and 'error' not in erlang_results.get('udp', {}):
        compare_metric("Messages/sec",
                      std_results['udp']['messages_per_sec'],
                      erlang_results['udp']['messages_per_sec'], False)
        compare_metric("Mean RTT (ms)",
                      std_results['udp']['mean_latency_ms'],
                      erlang_results['udp']['mean_latency_ms'], True)
        compare_metric("P95 RTT (ms)",
                      std_results['udp']['p95_latency_ms'],
                      erlang_results['udp']['p95_latency_ms'], True)
    else:
        print("  (skipped due to errors)")

    print("\n" + "="*70)
    print("Note: Lower latency and higher throughput are better.")
    print("="*70)


def is_erlang_nif_available():
    """Check if the real Erlang NIF module is available."""
    try:
        import py_event_loop
        return py_event_loop._is_initialized()
    except (ImportError, AttributeError):
        return False


def main():
    """Run benchmarks with both event loops and compare."""
    print("\n" + "="*70)
    print("Event Loop Benchmark: Standard asyncio vs Erlang-native")
    print("="*70)
    print(f"Python version: {sys.version}")
    print(f"Timer iterations: {TIMER_ITERATIONS}")
    print(f"TCP messages: {TCP_MESSAGES} x {TCP_MESSAGE_SIZE}B")
    print(f"UDP messages: {UDP_MESSAGES} x {UDP_MESSAGE_SIZE}B")

    # Test with standard asyncio
    std_loop = asyncio.new_event_loop()
    std_results = run_benchmark_suite("Standard asyncio", std_loop)
    std_loop.close()

    # Check if we're running inside Erlang with the real NIF
    if not is_erlang_nif_available():
        print("\n" + "="*70)
        print("NOTICE: Erlang NIF not available")
        print("="*70)
        print("The Erlang event loop benchmarks require running inside Erlang.")
        print("To run the full benchmark comparison:")
        print("")
        print("  rebar3 compile && rebar3 shell")
        print("  py:exec(<<\"exec(open('examples/benchmark_event_loop.py').read())\">>).")
        print("")
        print("Standard asyncio results shown above.")
        return std_results

    # Test with Erlang event loop
    try:
        # Try to import from erlang module (primary API)
        try:
            from erlang import ErlangEventLoop
        except ImportError:
            # Fallback to _erlang_impl if erlang module not extended
            try:
                from _erlang_impl import ErlangEventLoop
            except ImportError:
                import os
                priv_path = os.path.join(os.path.dirname(__file__), '..', 'priv')
                if priv_path not in sys.path:
                    sys.path.insert(0, priv_path)
                from _erlang_impl import ErlangEventLoop

        erlang_loop = ErlangEventLoop()
        erlang_results = run_benchmark_suite("Erlang Event Loop", erlang_loop)
        erlang_loop.close()

        # Print comparison
        print_comparison(std_results, erlang_results)
        return {'standard': std_results, 'erlang': erlang_results}

    except ImportError as e:
        print(f"\nCould not import ErlangEventLoop: {e}")
        print("Make sure to run this from the Erlang shell:")
        print("  rebar3 shell")
        print("  py:exec(<<\"exec(open('examples/benchmark_event_loop.py').read())\").")
    except Exception as e:
        print(f"\nError running Erlang loop benchmarks: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
else:
    # When exec()'d from Erlang, run automatically
    main()
