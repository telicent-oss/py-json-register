# Copyright TELICENT LTD
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
json-register: A JSON registration cache for Python with PostgreSQL backend.

This package provides high-performance JSON object registration with PostgreSQL JSONB storage.
Objects are automatically deduplicated and assigned unique integer IDs.

Classes:
    JsonRegisterCache: Synchronous cache using psycopg3
    JsonRegisterCacheAsync: Asynchronous cache using asyncpg

Exceptions:
    JsonRegisterError: Base exception
    ConfigurationError: Invalid configuration
    ConnectionError: Database connection failure
    InvalidResponseError: Unexpected database response
    CanonicalisationError: JSON canonicalisation failure
"""

from .async_ import JsonRegisterCacheAsync
from .exceptions import (
    CanonicalisationError,
    ConfigurationError,
    ConnectionError,
    InvalidResponseError,
    JsonRegisterError,
)
from .sync import JsonRegisterCache

__version__ = "0.1.0"

__all__ = [
    "JsonRegisterCache",
    "JsonRegisterCacheAsync",
    "JsonRegisterError",
    "ConfigurationError",
    "ConnectionError",
    "InvalidResponseError",
    "CanonicalisationError",
]
