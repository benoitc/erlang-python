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
Test ASGI applications for py_asgi_async_test.
"""


async def test_asgi_app(scope, receive, send):
    """Simple ASGI app that returns Hello World"""
    await send({
        'type': 'http.response.start',
        'status': 200,
        'headers': [
            [b'content-type', b'text/plain'],
        ],
    })
    await send({
        'type': 'http.response.body',
        'body': b'Hello, World!',
    })


async def echo_body_app(scope, receive, send):
    """ASGI app that echoes the request body"""
    message = await receive()
    body = message.get('body', b'')

    await send({
        'type': 'http.response.start',
        'status': 200,
        'headers': [
            [b'content-type', b'application/octet-stream'],
        ],
    })
    await send({
        'type': 'http.response.body',
        'body': body,
    })
