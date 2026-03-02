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
Test suite for ErlangEventLoop asyncio compatibility.

This test suite is adapted from uvloop's test suite to verify that
ErlangEventLoop is a full drop-in replacement for asyncio's default
event loop.

Test Architecture:
- Uses a mixin pattern for test reuse (like uvloop)
- Tests run against both ErlangEventLoop and asyncio for comparison
- Supports pytest for test discovery and execution

Run tests:
    cd priv && python -m pytest tests/ -v

Run against ErlangEventLoop only:
    cd priv && python -m pytest tests/ -v -k "Erlang"

Run comparison tests:
    cd priv && python -m pytest tests/ -v
"""

from ._testbase import (
    BaseTestCase,
    ErlangTestCase,
    AIOTestCase,
    find_free_port,
    HAVE_SSL,
    ONLYUV,
    ONLYERL,
)

__all__ = [
    'BaseTestCase',
    'ErlangTestCase',
    'AIOTestCase',
    'find_free_port',
    'HAVE_SSL',
    'ONLYUV',
    'ONLYERL',
]
