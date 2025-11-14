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

"""Asynchronous JSON registration cache implementation."""

import json
from typing import Any, List

import asyncpg
from lru import LRU

from ._canonicalise import JsonType, canonicalise_json
from .exceptions import ConfigurationError, ConnectionError as ConnError, InvalidResponseError


class JsonRegisterCacheAsync:
    """
    Asynchronous JSON registration cache with PostgreSQL backend.

    This class provides async methods for registering JSON objects and retrieving
    their integer IDs. It uses asyncpg for database connectivity and an LRU cache for
    performance.

    Note:
        Use the `create` class method to instantiate this class, as the constructor
        must be async.

    Example:
        >>> cache = await JsonRegisterCacheAsync.create(
        ...     database_name="mydb",
        ...     database_host="localhost",
        ...     database_port=5432,
        ...     database_user="user",
        ...     database_password="pass",
        ...     lru_cache_size=1000,
        ...     table_name="json_objects",
        ...     id_column="id",
        ...     jsonb_column="json_object",
        ...     pool_size=10
        ... )
        >>> id1 = await cache.register_object({"name": "Alice", "age": 30})
        >>> id2 = await cache.register_object({"name": "Alice", "age": 30})
        >>> assert id1 == id2  # Same object returns same ID
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        table_name: str,
        id_column: str,
        jsonb_column: str,
        lru_cache_size: int,
    ):
        """
        Internal constructor. Use `create()` class method instead.

        Args:
            pool: asyncpg connection pool
            table_name: Name of the table to store JSON objects
            id_column: Name of the ID column
            jsonb_column: Name of the JSONB column
            lru_cache_size: Size of the LRU cache
        """
        self._pool = pool
        self._table_name = table_name
        self._id_column = id_column
        self._jsonb_column = jsonb_column

        # Initialise LRU cache
        self._cache: LRU[str, int] = LRU(lru_cache_size)

        # Pre-build SQL queries
        self._register_query = self._build_register_query()
        self._register_batch_query = self._build_register_batch_query()

    @classmethod
    async def create(
        cls,
        database_name: str,
        database_host: str,
        database_port: int,
        database_user: str,
        database_password: str,
        lru_cache_size: int,
        table_name: str = "json_objects",
        id_column: str = "id",
        jsonb_column: str = "json_object",
        pool_size: int = 10,
    ) -> "JsonRegisterCacheAsync":
        """
        Create a new asynchronous JSON registration cache.

        This is an async factory method that must be awaited.

        Args:
            database_name: PostgreSQL database name
            database_host: PostgreSQL host
            database_port: PostgreSQL port
            database_user: PostgreSQL user
            database_password: PostgreSQL password
            lru_cache_size: Size of the LRU cache
            table_name: Name of the table to store JSON objects (default: "json_objects")
            id_column: Name of the ID column (default: "id")
            jsonb_column: Name of the JSONB column (default: "json_object")
            pool_size: Connection pool size (default: 10)

        Returns:
            JsonRegisterCacheAsync: A new async cache instance

        Raises:
            ConfigurationError: If configuration is invalid
            ConnectionError: If database connection fails
        """
        # Validate configuration
        cls._validate_config(
            database_name,
            database_host,
            database_port,
            database_user,
            table_name,
            id_column,
            jsonb_column,
            lru_cache_size,
            pool_size,
        )

        # Create connection pool
        try:
            pool = await asyncpg.create_pool(
                host=database_host,
                port=database_port,
                database=database_name,
                user=database_user,
                password=database_password,
                min_size=1,
                max_size=pool_size,
            )

            if pool is None:
                raise ConnError("Failed to create connection pool")

        except (asyncpg.PostgresError, OSError) as e:
            raise ConnError(f"Failed to create connection pool: {e}") from e

        return cls(
            pool=pool,
            table_name=table_name,
            id_column=id_column,
            jsonb_column=jsonb_column,
            lru_cache_size=lru_cache_size,
        )

    @staticmethod
    def _validate_config(
        database_name: str,
        database_host: str,
        database_port: int,
        database_user: str,
        table_name: str,
        id_column: str,
        jsonb_column: str,
        lru_cache_size: int,
        pool_size: int,
    ) -> None:
        """Validate configuration parameters."""
        if not database_name:
            raise ConfigurationError("database_name cannot be empty")
        if not database_host:
            raise ConfigurationError("database_host cannot be empty")
        if not (1 <= database_port <= 65535):
            raise ConfigurationError("database_port must be between 1 and 65535")
        if not database_user:
            raise ConfigurationError("database_user cannot be empty")
        if not table_name or not table_name.replace("_", "").isalnum():
            raise ConfigurationError("table_name must be alphanumeric (with underscores)")
        if not id_column or not id_column.replace("_", "").isalnum():
            raise ConfigurationError("id_column must be alphanumeric (with underscores)")
        if not jsonb_column or not jsonb_column.replace("_", "").isalnum():
            raise ConfigurationError("jsonb_column must be alphanumeric (with underscores)")
        if lru_cache_size < 1:
            raise ConfigurationError("lru_cache_size must be at least 1")
        if pool_size < 1:
            raise ConfigurationError("pool_size must be at least 1")

    def _build_register_query(self) -> str:
        """Build SQL query for registering a single object."""
        return f"""
            WITH inserted AS (
                INSERT INTO {self._table_name} ({self._jsonb_column})
                VALUES ($1)
                ON CONFLICT ({self._jsonb_column}) DO NOTHING
                RETURNING {self._id_column}
            )
            SELECT {self._id_column} FROM inserted
            UNION ALL
            SELECT {self._id_column} FROM {self._table_name}
            WHERE {self._jsonb_column} = $1
              AND NOT EXISTS (SELECT 1 FROM inserted)
            LIMIT 1
        """

    def _build_register_batch_query(self) -> str:
        """Build SQL query for registering multiple objects in batch."""
        return f"""
            WITH input_objects AS (
                SELECT
                    row_number() OVER () as original_order,
                    value as json_value
                FROM unnest($1::jsonb[]) WITH ORDINALITY AS t(value, ord)
            ),
            inserted AS (
                INSERT INTO {self._table_name} ({self._jsonb_column})
                SELECT json_value FROM input_objects
                ON CONFLICT ({self._jsonb_column}) DO NOTHING
                RETURNING {self._id_column}, {self._jsonb_column}
            ),
            existing AS (
                SELECT t.{self._id_column}, t.{self._jsonb_column}
                FROM {self._table_name} t
                JOIN input_objects io ON t.{self._jsonb_column} = io.json_value
            )
            SELECT COALESCE(i.{self._id_column}, e.{self._id_column}) as {self._id_column}, io.original_order
            FROM input_objects io
            LEFT JOIN inserted i ON io.json_value = i.{self._jsonb_column}
            LEFT JOIN existing e ON io.json_value = e.{self._jsonb_column}
            ORDER BY io.original_order
        """

    async def register_object(self, json_obj: JsonType) -> int:
        """
        Register a single JSON object and return its ID.

        This is an async method that must be awaited.

        This method first checks the LRU cache. If the object is not cached, it queries
        the database and inserts the object if it doesn't exist.

        Args:
            json_obj: A JSON-serialisable object (dict, list, str, int, float, bool, None)

        Returns:
            int: The ID of the registered object

        Raises:
            CanonicalisationError: If JSON canonicalisation fails
            ConnectionError: If database operation fails

        Example:
            >>> await cache.register_object({"name": "Bob"})
            42
        """
        # Canonicalise JSON for consistent caching
        canonical = canonicalise_json(json_obj)

        # Check cache first
        if canonical in self._cache:
            return self._cache[canonical]

        # Not in cache, query database
        try:
            async with self._pool.acquire() as conn:
                # Convert to JSON string for PostgreSQL
                json_str = json.dumps(json_obj, ensure_ascii=False)

                # Execute query (asyncpg automatically handles parameterisation)
                row = await conn.fetchrow(self._register_query, json_str)

                if row is None:
                    raise InvalidResponseError("Query returned no results")

                obj_id: int = row[0]

        except asyncpg.PostgresError as e:
            raise ConnError(f"Database error: {e}") from e

        # Update cache
        self._cache[canonical] = obj_id

        return obj_id

    async def register_batch_objects(self, json_objects: List[JsonType]) -> List[int]:
        """
        Register multiple JSON objects in batch and return their IDs in the same order.

        This is an async method that must be awaited.

        This method only uses the cache if ALL objects are already cached. Otherwise,
        it performs a batch database operation for efficiency.

        Args:
            json_objects: List of JSON-serialisable objects

        Returns:
            List[int]: List of IDs in the same order as input

        Raises:
            CanonicalisationError: If any JSON canonicalisation fails
            ConnectionError: If database operation fails
            InvalidResponseError: If database returns unexpected number of results

        Example:
            >>> await cache.register_batch_objects([{"a": 1}, {"b": 2}, {"c": 3}])
            [1, 2, 3]
        """
        if not json_objects:
            return []

        # Canonicalise all objects
        canonicals = [canonicalise_json(obj) for obj in json_objects]

        # Check if all objects are in cache
        if all(canonical in self._cache for canonical in canonicals):
            return [self._cache[canonical] for canonical in canonicals]

        # Not all cached, query database in batch
        try:
            async with self._pool.acquire() as conn:
                # Convert objects to JSON strings
                json_strs = [json.dumps(obj, ensure_ascii=False) for obj in json_objects]

                # Execute batch query
                rows = await conn.fetch(self._register_batch_query, json_strs)

                # Validate response
                if len(rows) != len(json_objects):
                    raise InvalidResponseError(
                        f"Expected {len(json_objects)} IDs but got {len(rows)}. "
                        "This indicates a database constraint violation or query error."
                    )

                # Extract IDs (rows are already ordered by original_order)
                ids = [row[0] for row in rows]

        except asyncpg.PostgresError as e:
            raise ConnError(f"Database error: {e}") from e

        # Update cache with all entries
        for canonical, obj_id in zip(canonicals, ids):
            self._cache[canonical] = obj_id

        return ids

    async def close(self) -> None:
        """Close the connection pool and release resources."""
        if hasattr(self, "_pool") and self._pool is not None:
            await self._pool.close()

    async def __aenter__(self) -> "JsonRegisterCacheAsync":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
