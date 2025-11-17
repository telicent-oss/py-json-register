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

"""Shared utility functions for json-register."""

from .exceptions import ConfigurationError


def build_register_query(
    table_name: str,
    id_column: str,
    jsonb_column: str,
    placeholder: str,
) -> str:
    """
    Build SQL query for registering a single object.

    Args:
        table_name: Name of the table
        id_column: Name of the ID column
        jsonb_column: Name of the JSONB column
        placeholder: Parameter placeholder ("%s" for psycopg3, "$1" for asyncpg)

    Returns:
        str: SQL query string
    """
    return f"""
        WITH inserted AS (
            INSERT INTO {table_name} ({jsonb_column})
            VALUES ({placeholder})
            ON CONFLICT ({jsonb_column}) DO NOTHING
            RETURNING {id_column}
        )
        SELECT {id_column} FROM inserted
        UNION ALL
        SELECT {id_column} FROM {table_name}
        WHERE {jsonb_column} = {placeholder}
          AND NOT EXISTS (SELECT 1 FROM inserted)
        LIMIT 1
    """


def build_register_batch_query(
    table_name: str,
    id_column: str,
    jsonb_column: str,
    placeholder: str,
) -> str:
    """
    Build SQL query for registering multiple objects in batch.

    Args:
        table_name: Name of the table
        id_column: Name of the ID column
        jsonb_column: Name of the JSONB column
        placeholder: Parameter placeholder ("%s" for psycopg3, "$1" for asyncpg)

    Returns:
        str: SQL query string
    """
    return f"""
        WITH input_objects AS (
            SELECT
                ord as original_order,
                value as json_value
            FROM unnest({placeholder}::jsonb[]) WITH ORDINALITY AS t(value, ord)
        ),
        inserted AS (
            INSERT INTO {table_name} ({jsonb_column})
            SELECT json_value FROM input_objects
            ON CONFLICT ({jsonb_column}) DO NOTHING
            RETURNING {id_column}, {jsonb_column}
        ),
        existing AS (
            SELECT t.{id_column}, t.{jsonb_column}
            FROM {table_name} t
            JOIN input_objects io ON t.{jsonb_column} = io.json_value
        )
        SELECT COALESCE(i.{id_column}, e.{id_column}) as {id_column}, io.original_order
        FROM input_objects io
        LEFT JOIN inserted i ON io.json_value = i.{jsonb_column}
        LEFT JOIN existing e ON io.json_value = e.{jsonb_column}
        ORDER BY io.original_order
    """


def validate_config(
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
    """
    Validate configuration parameters.

    Args:
        database_name: PostgreSQL database name
        database_host: PostgreSQL host
        database_port: PostgreSQL port
        database_user: PostgreSQL user
        table_name: Name of the table
        id_column: Name of the ID column
        jsonb_column: Name of the JSONB column
        lru_cache_size: Size of the LRU cache
        pool_size: Connection pool size

    Raises:
        ConfigurationError: If any parameter is invalid
    """
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
