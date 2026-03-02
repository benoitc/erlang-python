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
DNS resolution tests adapted from uvloop's test_dns.py.

These tests verify DNS operations:
- getaddrinfo
- getnameinfo
"""

import asyncio
import socket
import unittest

from . import _testbase as tb


class _TestGetaddrinfo:
    """Tests for getaddrinfo functionality."""

    def test_getaddrinfo_localhost(self):
        """Test getaddrinfo for localhost."""
        async def main():
            result = await self.loop.getaddrinfo(
                'localhost', 80,
                family=socket.AF_INET,
                type=socket.SOCK_STREAM
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        # Check structure: (family, type, proto, canonname, sockaddr)
        family, type_, proto, canonname, sockaddr = result[0]
        self.assertEqual(family, socket.AF_INET)
        self.assertEqual(type_, socket.SOCK_STREAM)
        self.assertIsInstance(sockaddr, tuple)
        self.assertEqual(len(sockaddr), 2)  # (host, port)
        self.assertEqual(sockaddr[1], 80)

    def test_getaddrinfo_127_0_0_1(self):
        """Test getaddrinfo for IP address."""
        async def main():
            result = await self.loop.getaddrinfo(
                '127.0.0.1', 8080,
                family=socket.AF_INET,
                type=socket.SOCK_STREAM
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        family, type_, proto, canonname, sockaddr = result[0]
        self.assertEqual(family, socket.AF_INET)
        self.assertEqual(sockaddr[0], '127.0.0.1')
        self.assertEqual(sockaddr[1], 8080)

    def test_getaddrinfo_no_port(self):
        """Test getaddrinfo without port."""
        async def main():
            result = await self.loop.getaddrinfo(
                'localhost', None,
                family=socket.AF_INET,
                type=socket.SOCK_STREAM
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_getaddrinfo_service_name(self):
        """Test getaddrinfo with service name."""
        async def main():
            result = await self.loop.getaddrinfo(
                'localhost', 'http',
                family=socket.AF_INET,
                type=socket.SOCK_STREAM
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        family, type_, proto, canonname, sockaddr = result[0]
        self.assertEqual(sockaddr[1], 80)  # HTTP port

    def test_getaddrinfo_udp(self):
        """Test getaddrinfo for UDP."""
        async def main():
            result = await self.loop.getaddrinfo(
                'localhost', 53,
                family=socket.AF_INET,
                type=socket.SOCK_DGRAM
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        family, type_, proto, canonname, sockaddr = result[0]
        self.assertEqual(type_, socket.SOCK_DGRAM)

    def test_getaddrinfo_any_family(self):
        """Test getaddrinfo with any address family."""
        async def main():
            result = await self.loop.getaddrinfo(
                'localhost', 80,
                family=socket.AF_UNSPEC,
                type=socket.SOCK_STREAM
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_getaddrinfo_flags(self):
        """Test getaddrinfo with flags."""
        async def main():
            result = await self.loop.getaddrinfo(
                'localhost', 80,
                family=socket.AF_INET,
                type=socket.SOCK_STREAM,
                flags=socket.AI_PASSIVE
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, list)

    def test_getaddrinfo_parallel(self):
        """Test multiple parallel getaddrinfo calls."""
        async def main():
            results = await asyncio.gather(
                self.loop.getaddrinfo('localhost', 80),
                self.loop.getaddrinfo('localhost', 443),
                self.loop.getaddrinfo('127.0.0.1', 8080),
            )
            return results

        results = self.loop.run_until_complete(main())

        self.assertEqual(len(results), 3)
        for result in results:
            self.assertIsInstance(result, list)
            self.assertGreater(len(result), 0)

    def test_getaddrinfo_bad_host(self):
        """Test getaddrinfo with non-existent host."""
        async def main():
            with self.assertRaises(socket.gaierror):
                await self.loop.getaddrinfo(
                    'invalid.host.that.does.not.exist.example',
                    80
                )

        self.loop.run_until_complete(main())


class _TestGetnameinfo:
    """Tests for getnameinfo functionality."""

    def test_getnameinfo_basic(self):
        """Test getnameinfo for localhost address."""
        async def main():
            result = await self.loop.getnameinfo(
                ('127.0.0.1', 80)
            )
            return result

        result = self.loop.run_until_complete(main())

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        host, port = result
        self.assertIsInstance(host, str)
        self.assertIsInstance(port, str)

    def test_getnameinfo_numeric(self):
        """Test getnameinfo with numeric flags."""
        async def main():
            result = await self.loop.getnameinfo(
                ('127.0.0.1', 80),
                socket.NI_NUMERICHOST | socket.NI_NUMERICSERV
            )
            return result

        result = self.loop.run_until_complete(main())

        host, port = result
        self.assertEqual(host, '127.0.0.1')
        self.assertEqual(port, '80')

    def test_getnameinfo_ipv6(self):
        """Test getnameinfo with IPv6 address."""
        async def main():
            result = await self.loop.getnameinfo(
                ('::1', 80),
                socket.NI_NUMERICHOST | socket.NI_NUMERICSERV
            )
            return result

        try:
            result = self.loop.run_until_complete(main())
            host, port = result
            self.assertIn(':', host)  # IPv6 contains colons
            self.assertEqual(port, '80')
        except socket.gaierror:
            # IPv6 may not be available
            pass


class _TestDNSConcurrent:
    """Tests for concurrent DNS operations."""

    def test_concurrent_getaddrinfo(self):
        """Test many concurrent getaddrinfo operations."""
        async def main():
            tasks = [
                self.loop.getaddrinfo('localhost', port)
                for port in range(8000, 8010)
            ]
            results = await asyncio.gather(*tasks)
            return results

        results = self.loop.run_until_complete(main())

        self.assertEqual(len(results), 10)
        for result in results:
            self.assertIsInstance(result, list)
            self.assertGreater(len(result), 0)


# =============================================================================
# Test classes that combine mixins with test cases
# =============================================================================

class TestErlangGetaddrinfo(_TestGetaddrinfo, tb.ErlangTestCase):
    pass


class TestAIOGetaddrinfo(_TestGetaddrinfo, tb.AIOTestCase):
    pass


class TestErlangGetnameinfo(_TestGetnameinfo, tb.ErlangTestCase):
    pass


class TestAIOGetnameinfo(_TestGetnameinfo, tb.AIOTestCase):
    pass


class TestErlangDNSConcurrent(_TestDNSConcurrent, tb.ErlangTestCase):
    pass


class TestAIODNSConcurrent(_TestDNSConcurrent, tb.AIOTestCase):
    pass


if __name__ == '__main__':
    unittest.main()
