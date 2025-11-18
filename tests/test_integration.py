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

"""Integration tests with PostgreSQL.

These tests mirror the integration tests from the Rust implementation.
They require a running PostgreSQL database with the schema set up.

Run with: pytest tests/test_integration.py -m integration

Setup:
    CREATE TABLE json_objects (
        id SERIAL PRIMARY KEY,
        json_object JSONB UNIQUE NOT NULL
    );
    CREATE INDEX idx_json_objects_json_object ON json_objects USING gin(json_object);
"""

import os
import time

import pytest

from json_register import JsonRegisterCache, JsonRegisterCacheAsync

# Test configuration - uses environment variables in CI, falls back to local config
TEST_CONFIG = {
    "database_name": os.getenv("TEST_DB_NAME", "access"),
    "database_host": os.getenv("TEST_DB_HOST", "localhost"),
    "database_port": int(os.getenv("TEST_DB_PORT", "5432")),
    "database_user": os.getenv("TEST_DB_USER", "postgres"),
    "database_password": os.getenv("TEST_DB_PASSWORD", ""),
    "lru_cache_size": 100,
    "table_name": os.getenv("TEST_DB_TABLE", "labels"),
    "id_column": os.getenv("TEST_DB_ID_COLUMN", "id"),
    "jsonb_column": os.getenv("TEST_DB_JSONB_COLUMN", "label"),
    "pool_size": 5,
}


@pytest.mark.integration
class TestJsonRegisterCacheIntegration:
    """Integration tests for synchronous JsonRegisterCache."""

    def test_register_object(self):
        """Test registering a single object."""
        cache = JsonRegisterCache(**TEST_CONFIG)

        obj = {"name": "Alice", "age": 30}
        id1 = cache.register_object(obj)
        id2 = cache.register_object(obj)

        # Same object should return same ID
        assert id1 == id2

        cache.close()

    def test_register_batch_objects(self):
        """Test registering multiple objects in batch."""
        cache = JsonRegisterCache(**TEST_CONFIG)

        objects = [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Carol"},
        ]

        ids = cache.register_batch_objects(objects)

        assert len(ids) == 3
        # All IDs should be unique
        assert len(set(ids)) == 3

        cache.close()

    def test_batch_order_preserved_all_new(self):
        """Test that batch registration preserves order for all new objects."""
        cache = JsonRegisterCache(**TEST_CONFIG)

        # Use timestamp to ensure unique objects
        timestamp = int(time.time() * 1000000)

        objects = [
            {"test": "batch_order_1", "timestamp": timestamp, "index": 0},
            {"test": "batch_order_2", "timestamp": timestamp, "index": 1},
            {"test": "batch_order_3", "timestamp": timestamp, "index": 2},
            {"test": "batch_order_4", "timestamp": timestamp, "index": 3},
        ]

        batch_ids = cache.register_batch_objects(objects)
        assert len(batch_ids) == 4, "Should return 4 IDs"

        # Verify each object gets the same ID when registered individually
        for i, obj in enumerate(objects):
            individual_id = cache.register_object(obj)
            assert batch_ids[i] == individual_id, f"Object at index {i} should have matching ID"

        cache.close()

    def test_batch_order_preserved_mixed_existing(self):
        """Test batch order preservation with mix of existing and new objects."""
        cache = JsonRegisterCache(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Pre-register some objects
        obj1 = {"test": "mixed_1", "timestamp": timestamp}
        obj3 = {"test": "mixed_3", "timestamp": timestamp}

        id1 = cache.register_object(obj1)
        id3 = cache.register_object(obj3)

        # Now register batch containing pre-registered + new objects
        obj2 = {"test": "mixed_2", "timestamp": timestamp}
        obj4 = {"test": "mixed_4", "timestamp": timestamp}

        batch = [obj1, obj2, obj3, obj4]
        batch_ids = cache.register_batch_objects(batch)

        assert len(batch_ids) == 4, "Should return 4 IDs"

        # Verify pre-registered objects have their original IDs
        assert batch_ids[0] == id1, "First object should have pre-registered ID"
        assert batch_ids[2] == id3, "Third object should have pre-registered ID"

        # All IDs should be unique
        assert len(set(batch_ids)) == 4, "All IDs should be unique"

        # Verify new objects get consistent IDs
        id2 = cache.register_object(obj2)
        id4 = cache.register_object(obj4)

        assert batch_ids[1] == id2, "Second object should have consistent ID"
        assert batch_ids[3] == id4, "Fourth object should have consistent ID"

        cache.close()

    def test_batch_different_key_orders_same_ids(self):
        """Test that objects with different key orders get same IDs."""
        cache = JsonRegisterCache(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Register batch with specific key order
        batch1 = [
            {"name": "Alice", "age": 30, "timestamp": timestamp},
            {"name": "Bob", "age": 25, "timestamp": timestamp},
        ]

        ids1 = cache.register_batch_objects(batch1)

        # Register same objects with different key order
        batch2 = [
            {"age": 30, "timestamp": timestamp, "name": "Alice"},
            {"timestamp": timestamp, "age": 25, "name": "Bob"},
        ]

        ids2 = cache.register_batch_objects(batch2)

        # Should get same IDs despite different key ordering
        assert ids1 == ids2, "Same objects with different key orders should get same IDs"

        cache.close()

    def test_batch_large_order_preservation(self):
        """Test order preservation with larger batch."""
        cache = JsonRegisterCache(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Create larger batch
        objects = [{"test": "large_batch", "timestamp": timestamp, "index": i, "data": f"item_{i}"} for i in range(20)]

        batch_ids = cache.register_batch_objects(objects)
        assert len(batch_ids) == 20, "Should return 20 IDs"

        # Verify order by checking each object individually
        for i, obj in enumerate(objects):
            individual_id = cache.register_object(obj)
            assert batch_ids[i] == individual_id, f"Object at index {i} should maintain order"

        # Re-register same batch - should get identical IDs in same order
        batch_ids_repeat = cache.register_batch_objects(objects)
        assert batch_ids == batch_ids_repeat, "Re-registering same batch should return same IDs in same order"

        cache.close()

    def test_batch_order_preservation_stress(self):
        """Stress test order preservation with complex mix of existing and new objects."""
        cache = JsonRegisterCache(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Pre-register a set of objects at known positions
        pre_registered = [
            {"type": "pre", "id": 0, "timestamp": timestamp},
            {"type": "pre", "id": 2, "timestamp": timestamp},
            {"type": "pre", "id": 5, "timestamp": timestamp},
            {"type": "pre", "id": 7, "timestamp": timestamp},
            {"type": "pre", "id": 9, "timestamp": timestamp},
        ]

        pre_registered_ids = {}
        for obj in pre_registered:
            obj_id = cache.register_object(obj)
            pre_registered_ids[obj["id"]] = obj_id

        # Create a batch with interleaved existing and new objects
        batch = [
            pre_registered[0],  # index 0: existing
            {"type": "new", "id": 1, "timestamp": timestamp},  # index 1: new
            pre_registered[1],  # index 2: existing
            {"type": "new", "id": 3, "timestamp": timestamp},  # index 3: new
            {"type": "new", "id": 4, "timestamp": timestamp},  # index 4: new
            pre_registered[2],  # index 5: existing
            {"type": "new", "id": 6, "timestamp": timestamp},  # index 6: new
            pre_registered[3],  # index 7: existing
            {"type": "new", "id": 8, "timestamp": timestamp},  # index 8: new
            pre_registered[4],  # index 9: existing
        ]

        # Register batch and get IDs
        batch_ids = cache.register_batch_objects(batch)

        assert len(batch_ids) == 10, "Should return 10 IDs"

        # Verify existing objects have their pre-registered IDs
        assert batch_ids[0] == pre_registered_ids[0], "Index 0 should have pre-registered ID"
        assert batch_ids[2] == pre_registered_ids[2], "Index 2 should have pre-registered ID"
        assert batch_ids[5] == pre_registered_ids[5], "Index 5 should have pre-registered ID"
        assert batch_ids[7] == pre_registered_ids[7], "Index 7 should have pre-registered ID"
        assert batch_ids[9] == pre_registered_ids[9], "Index 9 should have pre-registered ID"

        # Verify new objects got new IDs
        new_ids = [batch_ids[1], batch_ids[3], batch_ids[4], batch_ids[6], batch_ids[8]]
        assert len(set(new_ids)) == 5, "All new objects should have unique IDs"

        # Verify all IDs are unique
        assert len(set(batch_ids)) == 10, "All IDs in batch should be unique"

        # Verify order is preserved by re-registering each object individually
        for i, obj in enumerate(batch):
            individual_id = cache.register_object(obj)
            assert batch_ids[i] == individual_id, (
                f"Object at index {i} should have consistent ID: batch={batch_ids[i]}, individual={individual_id}"
            )

        # Test with duplicates in the same batch
        batch_with_dupes = [
            {"type": "dupe_test", "value": "A", "timestamp": timestamp},
            {"type": "dupe_test", "value": "B", "timestamp": timestamp},
            {"type": "dupe_test", "value": "A", "timestamp": timestamp},  # duplicate of index 0
            {"type": "dupe_test", "value": "C", "timestamp": timestamp},
            {"type": "dupe_test", "value": "B", "timestamp": timestamp},  # duplicate of index 1
        ]

        dupe_ids = cache.register_batch_objects(batch_with_dupes)
        assert len(dupe_ids) == 5, "Should return 5 IDs even with duplicates"
        assert dupe_ids[0] == dupe_ids[2], "Duplicate objects should have same ID"
        assert dupe_ids[1] == dupe_ids[4], "Duplicate objects should have same ID"
        assert len(set(dupe_ids)) == 3, "Should have 3 unique IDs (A, B, C)"

        cache.close()


@pytest.mark.integration
@pytest.mark.asyncio
class TestJsonRegisterCacheAsyncIntegration:
    """Integration tests for asynchronous JsonRegisterCacheAsync."""

    async def test_register_object(self):
        """Test registering a single object (async)."""
        cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)

        obj = {"name": "Alice", "age": 30}
        id1 = await cache.register_object(obj)
        id2 = await cache.register_object(obj)

        # Same object should return same ID
        assert id1 == id2

        await cache.close()

    async def test_register_batch_objects(self):
        """Test registering multiple objects in batch (async)."""
        cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)

        objects = [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Carol"},
        ]

        ids = await cache.register_batch_objects(objects)

        assert len(ids) == 3
        # All IDs should be unique
        assert len(set(ids)) == 3

        await cache.close()

    async def test_batch_order_preserved_all_new(self):
        """Test that batch registration preserves order for all new objects (async)."""
        cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        objects = [
            {"test": "async_batch_order_1", "timestamp": timestamp, "index": 0},
            {"test": "async_batch_order_2", "timestamp": timestamp, "index": 1},
            {"test": "async_batch_order_3", "timestamp": timestamp, "index": 2},
            {"test": "async_batch_order_4", "timestamp": timestamp, "index": 3},
        ]

        batch_ids = await cache.register_batch_objects(objects)
        assert len(batch_ids) == 4, "Should return 4 IDs"

        # Verify each object gets the same ID when registered individually
        for i, obj in enumerate(objects):
            individual_id = await cache.register_object(obj)
            assert batch_ids[i] == individual_id, f"Object at index {i} should have matching ID"

        await cache.close()

    async def test_batch_order_preserved_mixed_existing(self):
        """Test batch order preservation with mix of existing and new objects (async)."""
        cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Pre-register some objects
        obj1 = {"test": "async_mixed_1", "timestamp": timestamp}
        obj3 = {"test": "async_mixed_3", "timestamp": timestamp}

        id1 = await cache.register_object(obj1)
        id3 = await cache.register_object(obj3)

        # Now register batch containing pre-registered + new objects
        obj2 = {"test": "async_mixed_2", "timestamp": timestamp}
        obj4 = {"test": "async_mixed_4", "timestamp": timestamp}

        batch = [obj1, obj2, obj3, obj4]
        batch_ids = await cache.register_batch_objects(batch)

        assert len(batch_ids) == 4, "Should return 4 IDs"

        # Verify pre-registered objects have their original IDs
        assert batch_ids[0] == id1, "First object should have pre-registered ID"
        assert batch_ids[2] == id3, "Third object should have pre-registered ID"

        # All IDs should be unique
        assert len(set(batch_ids)) == 4, "All IDs should be unique"

        # Verify new objects get consistent IDs
        id2 = await cache.register_object(obj2)
        id4 = await cache.register_object(obj4)

        assert batch_ids[1] == id2, "Second object should have consistent ID"
        assert batch_ids[3] == id4, "Fourth object should have consistent ID"

        await cache.close()

    async def test_batch_different_key_orders_same_ids(self):
        """Test that objects with different key orders get same IDs (async)."""
        cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Register batch with specific key order
        batch1 = [
            {"name": "Alice", "age": 30, "timestamp": timestamp},
            {"name": "Bob", "age": 25, "timestamp": timestamp},
        ]

        ids1 = await cache.register_batch_objects(batch1)

        # Register same objects with different key order
        batch2 = [
            {"age": 30, "timestamp": timestamp, "name": "Alice"},
            {"timestamp": timestamp, "age": 25, "name": "Bob"},
        ]

        ids2 = await cache.register_batch_objects(batch2)

        # Should get same IDs despite different key ordering
        assert ids1 == ids2, "Same objects with different key orders should get same IDs"

        await cache.close()

    async def test_batch_large_order_preservation(self):
        """Test order preservation with larger batch (async)."""
        cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Create larger batch
        objects = [
            {"test": "async_large_batch", "timestamp": timestamp, "index": i, "data": f"item_{i}"} for i in range(20)
        ]

        batch_ids = await cache.register_batch_objects(objects)
        assert len(batch_ids) == 20, "Should return 20 IDs"

        # Verify order by checking each object individually
        for i, obj in enumerate(objects):
            individual_id = await cache.register_object(obj)
            assert batch_ids[i] == individual_id, f"Object at index {i} should maintain order"

        # Re-register same batch - should get identical IDs in same order
        batch_ids_repeat = await cache.register_batch_objects(objects)
        assert batch_ids == batch_ids_repeat, "Re-registering same batch should return same IDs in same order"

        await cache.close()

    async def test_batch_order_preservation_stress(self):
        """Stress test order preservation with complex mix of existing and new objects (async)."""
        cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)

        timestamp = int(time.time() * 1000000)

        # Pre-register a set of objects at known positions
        pre_registered = [
            {"type": "async_pre", "id": 0, "timestamp": timestamp},
            {"type": "async_pre", "id": 2, "timestamp": timestamp},
            {"type": "async_pre", "id": 5, "timestamp": timestamp},
            {"type": "async_pre", "id": 7, "timestamp": timestamp},
            {"type": "async_pre", "id": 9, "timestamp": timestamp},
        ]

        pre_registered_ids = {}
        for obj in pre_registered:
            obj_id = await cache.register_object(obj)
            pre_registered_ids[obj["id"]] = obj_id

        # Create a batch with interleaved existing and new objects
        batch = [
            pre_registered[0],  # index 0: existing
            {"type": "async_new", "id": 1, "timestamp": timestamp},  # index 1: new
            pre_registered[1],  # index 2: existing
            {"type": "async_new", "id": 3, "timestamp": timestamp},  # index 3: new
            {"type": "async_new", "id": 4, "timestamp": timestamp},  # index 4: new
            pre_registered[2],  # index 5: existing
            {"type": "async_new", "id": 6, "timestamp": timestamp},  # index 6: new
            pre_registered[3],  # index 7: existing
            {"type": "async_new", "id": 8, "timestamp": timestamp},  # index 8: new
            pre_registered[4],  # index 9: existing
        ]

        # Register batch and get IDs
        batch_ids = await cache.register_batch_objects(batch)

        assert len(batch_ids) == 10, "Should return 10 IDs"

        # Verify existing objects have their pre-registered IDs
        assert batch_ids[0] == pre_registered_ids[0], "Index 0 should have pre-registered ID"
        assert batch_ids[2] == pre_registered_ids[2], "Index 2 should have pre-registered ID"
        assert batch_ids[5] == pre_registered_ids[5], "Index 5 should have pre-registered ID"
        assert batch_ids[7] == pre_registered_ids[7], "Index 7 should have pre-registered ID"
        assert batch_ids[9] == pre_registered_ids[9], "Index 9 should have pre-registered ID"

        # Verify new objects got new IDs
        new_ids = [batch_ids[1], batch_ids[3], batch_ids[4], batch_ids[6], batch_ids[8]]
        assert len(set(new_ids)) == 5, "All new objects should have unique IDs"

        # Verify all IDs are unique
        assert len(set(batch_ids)) == 10, "All IDs in batch should be unique"

        # Verify order is preserved by re-registering each object individually
        for i, obj in enumerate(batch):
            individual_id = await cache.register_object(obj)
            assert batch_ids[i] == individual_id, (
                f"Object at index {i} should have consistent ID: batch={batch_ids[i]}, individual={individual_id}"
            )

        # Test with duplicates in the same batch
        batch_with_dupes = [
            {"type": "async_dupe_test", "value": "A", "timestamp": timestamp},
            {"type": "async_dupe_test", "value": "B", "timestamp": timestamp},
            {
                "type": "async_dupe_test",
                "value": "A",
                "timestamp": timestamp,
            },  # duplicate of index 0
            {"type": "async_dupe_test", "value": "C", "timestamp": timestamp},
            {
                "type": "async_dupe_test",
                "value": "B",
                "timestamp": timestamp,
            },  # duplicate of index 1
        ]

        dupe_ids = await cache.register_batch_objects(batch_with_dupes)
        assert len(dupe_ids) == 5, "Should return 5 IDs even with duplicates"
        assert dupe_ids[0] == dupe_ids[2], "Duplicate objects should have same ID"
        assert dupe_ids[1] == dupe_ids[4], "Duplicate objects should have same ID"
        assert len(set(dupe_ids)) == 3, "Should have 3 unique IDs (A, B, C)"

        await cache.close()
