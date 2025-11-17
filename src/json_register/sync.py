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

"""Synchronous JSON registration cache implementation."""

import json
from typing import Any, List

import psycopg
from lru import LRU
from psycopg.rows import tuple_row
from psycopg_pool import ConnectionPool

from ._canonicalise import JsonType, canonicalise_json
from ._utils import build_register_batch_query, build_register_query, validate_config
from .exceptions import ConnectionError as ConnError, InvalidResponseError


class JsonRegisterCache:
    """
    Synchronous JSON registration cache with PostgreSQL backend.

    This class provides synchronous methods for registering JSON objects and retrieving
    their integer IDs. It uses psycopg3 for database connectivity and an LRU cache for
    performance.

    Example:
        >>> cache = JsonRegisterCache(
        ...     database_name="mydb",
        ...     database_host="localhost",
        ...     database_port=5432,
        ...     database_user="user",
        ...     database_password="pass"
        ... )
        >>> id1 = cache.register_object({"name": "Alice", "age": 30})
        >>> id2 = cache.register_object({"name": "Alice", "age": 30})
        >>> assert id1 == id2  # Same object returns same ID
    """

    def __init__(
        self,
        database_name: str,
        database_host: str,
        database_port: int,
        database_user: str,
        database_password: str,
        lru_cache_size: int = 1000,
        table_name: str = "json_objects",
        id_column: str = "id",
        jsonb_column: str = "json_object",
        pool_size: int = 10,
    ):
        """
        Initialise the synchronous JSON registration cache.

        Args:
            database_name: PostgreSQL database name
            database_host: PostgreSQL host
            database_port: PostgreSQL port
            database_user: PostgreSQL user
            database_password: PostgreSQL password
            lru_cache_size: Size of the LRU cache (default: 1000)
            table_name: Name of the table to store JSON objects (default: "json_objects")
            id_column: Name of the ID column (default: "id")
            jsonb_column: Name of the JSONB column (default: "json_object")
            pool_size: Connection pool size (default: 10)

        Raises:
            ConfigurationError: If configuration is invalid
            ConnectionError: If database connection fails
        """
        # Validate configuration
        validate_config(
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

        # Store configuration
        self._table_name = table_name
        self._id_column = id_column
        self._jsonb_column = jsonb_column

        # Initialise LRU cache
        self._cache: LRU[str, int] = LRU(lru_cache_size)

        # Build connection string (password won't be logged)
        conninfo = (
            f"host={database_host} "
            f"port={database_port} "
            f"dbname={database_name} "
            f"user={database_user} "
            f"password={database_password}"
        )

        # Create connection pool
        try:
            self._pool = ConnectionPool(
                conninfo=conninfo,
                min_size=1,
                max_size=pool_size,
                open=True,
            )
        except psycopg.Error as e:
            raise ConnError(f"Failed to create connection pool: {e}") from e

        # Pre-build SQL queries using psycopg3 placeholder style
        self._register_query = build_register_query(
            self._table_name, self._id_column, self._jsonb_column, "%s"
        )
        self._register_batch_query = build_register_batch_query(
            self._table_name, self._id_column, self._jsonb_column, "%s"
        )

    def register_object(self, json_obj: JsonType) -> int:
        """
        Register a single JSON object and return its ID.

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
            >>> cache.register_object({"name": "Bob"})
            42
        """
        # Canonicalise JSON for consistent caching
        canonical = canonicalise_json(json_obj)

        # Check cache first
        if canonical in self._cache:
            return self._cache[canonical]

        # Not in cache, query database
        try:
            with self._pool.connection() as conn:
                with conn.cursor(row_factory=tuple_row) as cur:
                    # Convert to JSON string for PostgreSQL
                    json_str = json.dumps(json_obj, ensure_ascii=False)

                    # Execute query (same value passed twice for the UNION query)
                    cur.execute(self._register_query, (json_str, json_str))
                    result = cur.fetchone()

                    if result is None:
                        raise InvalidResponseError("Query returned no results")

                    obj_id: int = result[0]

        except psycopg.Error as e:
            raise ConnError(f"Database error: {e}") from e

        # Update cache
        self._cache[canonical] = obj_id

        return obj_id

    def register_batch_objects(self, json_objects: List[JsonType]) -> List[int]:
        """
        Register multiple JSON objects in batch and return their IDs in the same order.

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
            >>> cache.register_batch_objects([{"a": 1}, {"b": 2}, {"c": 3}])
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
            with self._pool.connection() as conn:
                with conn.cursor(row_factory=tuple_row) as cur:
                    # Convert objects to JSON strings
                    json_strs = [json.dumps(obj, ensure_ascii=False) for obj in json_objects]

                    # Execute batch query
                    cur.execute(self._register_batch_query, (json_strs,))
                    results = cur.fetchall()

                    # Validate response
                    if len(results) != len(json_objects):
                        raise InvalidResponseError(
                            f"Expected {len(json_objects)} IDs but got {len(results)}. "
                            "This indicates a database constraint violation or query error."
                        )

                    # Extract IDs (results are already ordered by original_order)
                    ids = [row[0] for row in results]

        except psycopg.Error as e:
            raise ConnError(f"Database error: {e}") from e

        # Update cache with all entries
        for canonical, obj_id in zip(canonicals, ids):
            self._cache[canonical] = obj_id

        return ids

    def close(self) -> None:
        """Close the connection pool and release resources."""
        if hasattr(self, "_pool"):
            self._pool.close()

    def __enter__(self) -> "JsonRegisterCache":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def __del__(self) -> None:
        """Destructor to ensure resources are cleaned up."""
        self.close()
