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

"""Tests for configuration validation."""

import pytest

from json_register import JsonRegisterCache, JsonRegisterCacheAsync
from json_register.exceptions import ConfigurationError


# Valid base configuration for testing
VALID_CONFIG = {
    "database_name": "testdb",
    "database_host": "localhost",
    "database_port": 5432,
    "database_user": "testuser",
    "database_password": "testpass",
    "lru_cache_size": 100,
}


class TestSyncConfigValidation:
    """Test configuration validation for synchronous cache."""

    def test_empty_database_name(self):
        """Test that empty database name raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_name": ""}
        with pytest.raises(ConfigurationError, match="database_name cannot be empty"):
            JsonRegisterCache(**config)

    def test_empty_database_host(self):
        """Test that empty database host raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_host": ""}
        with pytest.raises(ConfigurationError, match="database_host cannot be empty"):
            JsonRegisterCache(**config)

    def test_invalid_port_low(self):
        """Test that port below 1 raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_port": 0}
        with pytest.raises(ConfigurationError, match="database_port must be between 1 and 65535"):
            JsonRegisterCache(**config)

    def test_invalid_port_high(self):
        """Test that port above 65535 raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_port": 65536}
        with pytest.raises(ConfigurationError, match="database_port must be between 1 and 65535"):
            JsonRegisterCache(**config)

    def test_empty_database_user(self):
        """Test that empty database user raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_user": ""}
        with pytest.raises(ConfigurationError, match="database_user cannot be empty"):
            JsonRegisterCache(**config)

    def test_invalid_table_name_empty(self):
        """Test that empty table name raises ConfigurationError."""
        config = {**VALID_CONFIG, "table_name": ""}
        with pytest.raises(ConfigurationError, match="table_name must be alphanumeric"):
            JsonRegisterCache(**config)

    def test_invalid_table_name_special_chars(self):
        """Test that table name with special characters raises ConfigurationError."""
        config = {**VALID_CONFIG, "table_name": "table-name"}
        with pytest.raises(ConfigurationError, match="table_name must be alphanumeric"):
            JsonRegisterCache(**config)

    def test_valid_table_name_with_underscores(self):
        """Test that table name with underscores is valid (but fails on connection)."""
        config = {**VALID_CONFIG, "table_name": "my_table_name"}
        # This should pass validation but fail on connection
        # We're just testing that the validation doesn't reject underscores
        try:
            JsonRegisterCache(**config)
        except Exception as e:
            # Connection will fail, but we just want to ensure it's not a ConfigurationError
            assert not isinstance(e, ConfigurationError)

    def test_invalid_id_column_empty(self):
        """Test that empty id column raises ConfigurationError."""
        config = {**VALID_CONFIG, "id_column": ""}
        with pytest.raises(ConfigurationError, match="id_column must be alphanumeric"):
            JsonRegisterCache(**config)

    def test_invalid_id_column_special_chars(self):
        """Test that id column with special characters raises ConfigurationError."""
        config = {**VALID_CONFIG, "id_column": "id-column"}
        with pytest.raises(ConfigurationError, match="id_column must be alphanumeric"):
            JsonRegisterCache(**config)

    def test_invalid_jsonb_column_empty(self):
        """Test that empty jsonb column raises ConfigurationError."""
        config = {**VALID_CONFIG, "jsonb_column": ""}
        with pytest.raises(ConfigurationError, match="jsonb_column must be alphanumeric"):
            JsonRegisterCache(**config)

    def test_invalid_jsonb_column_special_chars(self):
        """Test that jsonb column with special characters raises ConfigurationError."""
        config = {**VALID_CONFIG, "jsonb_column": "json@column"}
        with pytest.raises(ConfigurationError, match="jsonb_column must be alphanumeric"):
            JsonRegisterCache(**config)

    def test_invalid_lru_cache_size_zero(self):
        """Test that LRU cache size of 0 raises ConfigurationError."""
        config = {**VALID_CONFIG, "lru_cache_size": 0}
        with pytest.raises(ConfigurationError, match="lru_cache_size must be at least 1"):
            JsonRegisterCache(**config)

    def test_invalid_lru_cache_size_negative(self):
        """Test that negative LRU cache size raises ConfigurationError."""
        config = {**VALID_CONFIG, "lru_cache_size": -1}
        with pytest.raises(ConfigurationError, match="lru_cache_size must be at least 1"):
            JsonRegisterCache(**config)

    def test_invalid_pool_size_zero(self):
        """Test that pool size of 0 raises ConfigurationError."""
        config = {**VALID_CONFIG, "pool_size": 0}
        with pytest.raises(ConfigurationError, match="pool_size must be at least 1"):
            JsonRegisterCache(**config)

    def test_invalid_pool_size_negative(self):
        """Test that negative pool size raises ConfigurationError."""
        config = {**VALID_CONFIG, "pool_size": -1}
        with pytest.raises(ConfigurationError, match="pool_size must be at least 1"):
            JsonRegisterCache(**config)


@pytest.mark.asyncio
class TestAsyncConfigValidation:
    """Test configuration validation for asynchronous cache."""

    async def test_empty_database_name(self):
        """Test that empty database name raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_name": ""}
        with pytest.raises(ConfigurationError, match="database_name cannot be empty"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_empty_database_host(self):
        """Test that empty database host raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_host": ""}
        with pytest.raises(ConfigurationError, match="database_host cannot be empty"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_port_low(self):
        """Test that port below 1 raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_port": 0}
        with pytest.raises(ConfigurationError, match="database_port must be between 1 and 65535"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_port_high(self):
        """Test that port above 65535 raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_port": 65536}
        with pytest.raises(ConfigurationError, match="database_port must be between 1 and 65535"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_empty_database_user(self):
        """Test that empty database user raises ConfigurationError."""
        config = {**VALID_CONFIG, "database_user": ""}
        with pytest.raises(ConfigurationError, match="database_user cannot be empty"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_table_name_empty(self):
        """Test that empty table name raises ConfigurationError."""
        config = {**VALID_CONFIG, "table_name": ""}
        with pytest.raises(ConfigurationError, match="table_name must be alphanumeric"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_table_name_special_chars(self):
        """Test that table name with special characters raises ConfigurationError."""
        config = {**VALID_CONFIG, "table_name": "table-name"}
        with pytest.raises(ConfigurationError, match="table_name must be alphanumeric"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_id_column_empty(self):
        """Test that empty id column raises ConfigurationError."""
        config = {**VALID_CONFIG, "id_column": ""}
        with pytest.raises(ConfigurationError, match="id_column must be alphanumeric"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_jsonb_column_empty(self):
        """Test that empty jsonb column raises ConfigurationError."""
        config = {**VALID_CONFIG, "jsonb_column": ""}
        with pytest.raises(ConfigurationError, match="jsonb_column must be alphanumeric"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_lru_cache_size_zero(self):
        """Test that LRU cache size of 0 raises ConfigurationError."""
        config = {**VALID_CONFIG, "lru_cache_size": 0}
        with pytest.raises(ConfigurationError, match="lru_cache_size must be at least 1"):
            await JsonRegisterCacheAsync.create(**config)

    async def test_invalid_pool_size_zero(self):
        """Test that pool size of 0 raises ConfigurationError."""
        config = {**VALID_CONFIG, "pool_size": 0}
        with pytest.raises(ConfigurationError, match="pool_size must be at least 1"):
            await JsonRegisterCacheAsync.create(**config)
