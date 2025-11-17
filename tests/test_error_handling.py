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

"""Unit tests for error handling and edge cases using mocks."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import psycopg
import pytest

from json_register import (
    ConnectionError,
    InvalidResponseError,
    JsonRegisterCache,
    JsonRegisterCacheAsync,
)


class TestSyncErrorHandling:
    """Test error handling in synchronous JsonRegisterCache."""

    def test_connection_pool_creation_failure(self):
        """Test that connection pool creation failure raises ConnectionError."""
        with patch("json_register.sync.ConnectionPool") as mock_pool:
            mock_pool.side_effect = psycopg.OperationalError("Connection failed")

            with pytest.raises(ConnectionError, match="Failed to create connection pool"):
                JsonRegisterCache(
                    database_name="testdb",
                    database_host="localhost",
                    database_port=5432,
                    database_user="user",
                    database_password="pass",
                    lru_cache_size=100,
                )

    def test_register_object_database_error(self):
        """Test that database errors during register_object raise ConnectionError."""
        with patch("json_register.sync.ConnectionPool") as mock_pool_class:
            # Mock the connection pool and connection
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            cache = JsonRegisterCache(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to raise error during execute
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = psycopg.OperationalError("Database error")
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pool.connection.return_value.__enter__.return_value = mock_conn

            with pytest.raises(ConnectionError, match="Database error"):
                cache.register_object({"test": "value"})

    def test_register_object_no_results(self):
        """Test that no results from query raises InvalidResponseError."""
        with patch("json_register.sync.ConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            cache = JsonRegisterCache(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to return no results
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = None
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pool.connection.return_value.__enter__.return_value = mock_conn

            with pytest.raises(InvalidResponseError, match="Query returned no results"):
                cache.register_object({"test": "value"})

    def test_register_batch_objects_database_error(self):
        """Test that database errors during batch registration raise ConnectionError."""
        with patch("json_register.sync.ConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            cache = JsonRegisterCache(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to raise error during execute
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = psycopg.OperationalError("Database error")
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pool.connection.return_value.__enter__.return_value = mock_conn

            with pytest.raises(ConnectionError, match="Database error"):
                cache.register_batch_objects([{"test": "value1"}, {"test": "value2"}])

    def test_register_batch_objects_wrong_result_count(self):
        """Test that mismatched result count raises InvalidResponseError."""
        with patch("json_register.sync.ConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            cache = JsonRegisterCache(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to return wrong number of results
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [(1, 0)]  # Only 1 result for 2 objects
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pool.connection.return_value.__enter__.return_value = mock_conn

            with pytest.raises(InvalidResponseError, match="Expected 2 IDs but got 1"):
                cache.register_batch_objects([{"test": "value1"}, {"test": "value2"}])

    def test_context_manager(self):
        """Test that context manager properly closes connections."""
        with patch("json_register.sync.ConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            with JsonRegisterCache(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            ) as cache:
                assert cache is not None

            # Verify close was called
            mock_pool.close.assert_called_once()

    def test_close_idempotent(self):
        """Test that close can be called multiple times safely."""
        with patch("json_register.sync.ConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            cache = JsonRegisterCache(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            cache.close()
            cache.close()  # Should not raise error

            # Close should be called at least once
            assert mock_pool.close.call_count >= 1


class TestAsyncErrorHandling:
    """Test error handling in asynchronous JsonRegisterCacheAsync."""

    @pytest.mark.asyncio
    async def test_connection_pool_creation_failure(self):
        """Test that connection pool creation failure raises ConnectionError."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            mock_create_pool.side_effect = asyncpg.PostgresError("Connection failed")

            with pytest.raises(ConnectionError, match="Failed to create connection pool"):
                await JsonRegisterCacheAsync.create(
                    database_name="testdb",
                    database_host="localhost",
                    database_port=5432,
                    database_user="user",
                    database_password="pass",
                    lru_cache_size=100,
                )

    @pytest.mark.asyncio
    async def test_connection_pool_returns_none(self):
        """Test that None pool raises ConnectionError."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            mock_create_pool.return_value = None

            with pytest.raises(ConnectionError, match="Failed to create connection pool"):
                await JsonRegisterCacheAsync.create(
                    database_name="testdb",
                    database_host="localhost",
                    database_port=5432,
                    database_user="user",
                    database_password="pass",
                    lru_cache_size=100,
                )

    @pytest.mark.asyncio
    async def test_register_object_database_error(self):
        """Test that database errors during register_object raise ConnectionError."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            # Mock the connection pool
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            cache = await JsonRegisterCacheAsync.create(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to raise error during fetchrow
            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(side_effect=asyncpg.PostgresError("Database error"))
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with pytest.raises(ConnectionError, match="Database error"):
                await cache.register_object({"test": "value"})

    @pytest.mark.asyncio
    async def test_register_object_no_results(self):
        """Test that no results from query raises InvalidResponseError."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            cache = await JsonRegisterCacheAsync.create(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to return no results
            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with pytest.raises(InvalidResponseError, match="Query returned no results"):
                await cache.register_object({"test": "value"})

    @pytest.mark.asyncio
    async def test_register_batch_objects_database_error(self):
        """Test that database errors during batch registration raise ConnectionError."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            cache = await JsonRegisterCacheAsync.create(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to raise error during fetch
            mock_conn = MagicMock()
            mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Database error"))
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with pytest.raises(ConnectionError, match="Database error"):
                await cache.register_batch_objects([{"test": "value1"}, {"test": "value2"}])

    @pytest.mark.asyncio
    async def test_register_batch_objects_wrong_result_count(self):
        """Test that mismatched result count raises InvalidResponseError."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            cache = await JsonRegisterCacheAsync.create(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            # Mock connection to return wrong number of results
            mock_conn = MagicMock()
            mock_conn.fetch = AsyncMock(return_value=[(1, 0)])  # Only 1 result for 2 objects
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with pytest.raises(InvalidResponseError, match="Expected 2 IDs but got 1"):
                await cache.register_batch_objects([{"test": "value1"}, {"test": "value2"}])

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test that async context manager properly closes connections."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            mock_pool = MagicMock()
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            async with await JsonRegisterCacheAsync.create(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            ) as cache:
                assert cache is not None

            # Verify close was called
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        """Test that close can be called multiple times safely."""
        with patch(
            "json_register.async_.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create_pool:
            mock_pool = MagicMock()
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            cache = await JsonRegisterCacheAsync.create(
                database_name="testdb",
                database_host="localhost",
                database_port=5432,
                database_user="user",
                database_password="pass",
                lru_cache_size=100,
            )

            await cache.close()
            await cache.close()  # Should not raise error

            # Close should be called at least once
            assert mock_pool.close.call_count >= 1
