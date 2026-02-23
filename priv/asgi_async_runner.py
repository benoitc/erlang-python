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
Async ASGI runner for the unified event-driven architecture.

This module provides an async function that runs an ASGI application
and collects the response. It's designed to be called via py_async_driver:submit.
"""

import importlib
import asyncio
from typing import Dict, List, Tuple, Any, Optional


async def run_asgi(
    module_name: str,
    callable_name: str,
    scope: Dict[str, Any],
    body: bytes
) -> Tuple[int, List[Tuple[bytes, bytes]], bytes]:
    """
    Run an ASGI application and return the response.

    Args:
        module_name: Name of the Python module containing the ASGI app
        callable_name: Name of the ASGI callable (e.g., 'app', 'application')
        scope: ASGI scope dictionary
        body: Request body as bytes

    Returns:
        Tuple of (status_code, headers, response_body)
        where headers is a list of (name, value) byte tuples
    """
    # Import the module and get the ASGI app
    module = importlib.import_module(module_name)
    app = getattr(module, callable_name)

    # Response collector
    status: Optional[int] = None
    headers: List[Tuple[bytes, bytes]] = []
    body_parts: List[bytes] = []

    # Track body consumption
    body_consumed = False

    async def receive():
        """ASGI receive callable - provides request body."""
        nonlocal body_consumed
        if not body_consumed:
            body_consumed = True
            # Ensure body is bytes
            body_bytes = body if isinstance(body, bytes) else body.encode('utf-8') if isinstance(body, str) else bytes(body)
            return {
                'type': 'http.request',
                'body': body_bytes,
                'more_body': False
            }
        # Subsequent calls indicate disconnect
        return {'type': 'http.disconnect'}

    async def send(message: Dict[str, Any]):
        """ASGI send callable - collects response."""
        nonlocal status, headers

        msg_type = message.get('type')

        if msg_type == 'http.response.start':
            status = message.get('status', 500)
            raw_headers = message.get('headers', [])
            # Ensure headers are bytes tuples
            headers = [
                (
                    h[0] if isinstance(h[0], bytes) else h[0].encode('latin-1'),
                    h[1] if isinstance(h[1], bytes) else h[1].encode('latin-1')
                )
                for h in raw_headers
            ]
        elif msg_type == 'http.response.body':
            body_chunk = message.get('body', b'')
            if body_chunk:
                body_parts.append(body_chunk)

    # Run the ASGI app
    await app(scope, receive, send)

    # Combine body parts (ensure all are bytes)
    byte_parts = []
    for part in body_parts:
        if isinstance(part, bytes):
            byte_parts.append(part)
        elif isinstance(part, str):
            byte_parts.append(part.encode('utf-8'))
        else:
            byte_parts.append(bytes(part))
    response_body = b''.join(byte_parts)

    return (status or 500, headers, response_body)
