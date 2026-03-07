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
Pytest configuration for ErlangEventLoop tests.

This file configures pytest for running the asyncio compatibility tests.
"""

import os
import sys

# Add priv directory to path for imports
priv_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if priv_dir not in sys.path:
    sys.path.insert(0, priv_dir)


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "erlang: marks tests for ErlangEventLoop only"
    )
    config.addinivalue_line(
        "markers", "asyncio: marks tests for asyncio event loop only"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers and keywords."""
    # Add skip markers based on test class names
    for item in items:
        if 'Erlang' in item.nodeid:
            item.add_marker('erlang')
        elif 'AIO' in item.nodeid:
            item.add_marker('asyncio')
