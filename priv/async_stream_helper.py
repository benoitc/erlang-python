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
Async stream helper for collecting values from async generators.

This module provides a helper function that consumes an async generator
and returns all values as a list.
"""

import importlib
from typing import Any, List


async def collect_async_gen(
    module_name: str,
    func_name: str,
    args: List[Any],
    kwargs: dict
) -> List[Any]:
    """
    Collect all values from an async generator.

    Args:
        module_name: Name of the Python module containing the async generator
        func_name: Name of the async generator function
        args: Positional arguments for the function
        kwargs: Keyword arguments for the function

    Returns:
        List of all values yielded by the async generator
    """
    # Import the module and get the async generator function
    module = importlib.import_module(module_name)
    func = getattr(module, func_name)

    # Call the function to get the async generator
    async_gen = func(*args, **kwargs)

    # Collect all values
    results = []
    async for value in async_gen:
        results.append(value)

    return results
