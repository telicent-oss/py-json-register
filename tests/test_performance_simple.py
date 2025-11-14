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
Simplified performance tests for json-register.

These tests use pytest-benchmark to measure performance without hitting
PostgreSQL size limits.

Run with: pytest tests/test_performance_simple.py -v --benchmark-only
"""

import os
import time

import pytest

from json_register import JsonRegisterCache
from tests.fixtures.json_generator import RandomJsonGenerator

# Test configuration - uses dedicated test table
TEST_CONFIG = {
    "database_name": os.getenv("TEST_DB_NAME", "access"),
    "database_host": os.getenv("TEST_DB_HOST", "localhost"),
    "database_port": int(os.getenv("TEST_DB_PORT", "5432")),
    "database_user": os.getenv("TEST_DB_USER", "postgres"),
    "database_password": os.getenv("TEST_DB_PASSWORD", ""),
    "lru_cache_size": 1000,
    "table_name": os.getenv("TEST_DB_TABLE", "json_register_perf_test"),
    "id_column": os.getenv("TEST_DB_ID_COLUMN", "id"),
    "jsonb_column": os.getenv("TEST_DB_JSONB_COLUMN", "json_data"),
    "pool_size": 10,
}


@pytest.fixture(scope="module")
def json_generator():
    """Provide a RandomJsonGenerator instance."""
    return RandomJsonGenerator(seed=42)


@pytest.fixture(scope="module")
def sync_cache():
    """Provide a synchronous cache instance."""
    cache = JsonRegisterCache(**TEST_CONFIG)
    yield cache
    cache.close()


@pytest.mark.benchmark
class TestPerformance:
    """Core performance benchmarks."""

    def test_single_small_cache_miss(self, benchmark, sync_cache, json_generator):
        """Single small object - cache miss."""
        obj = json_generator.generate_profile("small")
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_single_medium_cache_miss(self, benchmark, sync_cache, json_generator):
        """Single medium object - cache miss."""
        obj = json_generator.generate_profile("medium")
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_single_cache_hit(self, benchmark, sync_cache, json_generator):
        """Single object - cache hit."""
        obj = json_generator.generate_profile("medium")
        sync_cache.register_object(obj)  # Pre-register
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_batch_10_new(self, benchmark, sync_cache, json_generator):
        """Batch of 10 new objects."""
        objects = json_generator.generate_batch(10, min_keys=3, max_keys=10, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 10

    def test_batch_50_new(self, benchmark, sync_cache, json_generator):
        """Batch of 50 new objects."""
        objects = json_generator.generate_batch(50, min_keys=3, max_keys=10, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 50

    def test_batch_100_new(self, benchmark, sync_cache, json_generator):
        """Batch of 100 new objects."""
        objects = json_generator.generate_batch(100, min_keys=3, max_keys=10, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 100

    def test_batch_10_cached(self, benchmark, sync_cache, json_generator):
        """Batch of 10 cached objects."""
        objects = json_generator.generate_batch(10, min_keys=3, max_keys=10, max_depth=2)
        sync_cache.register_batch_objects(objects)  # Pre-register
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 10

    def test_write_cache_db_comparison(self, sync_cache, json_generator):
        """
        Compare all three access patterns: DB WRITE, DB READ, CACHE READ.

        Scenarios:
        1. Cache MISS + DB MISS → DB WRITE (new objects)
        2. Cache MISS + DB HIT  → DB READ (existing objects, cold cache)
        3. Cache HIT            → CACHE READ (warm cache)
        """
        # Generate objects (smaller to avoid PostgreSQL index size limits)
        objects = json_generator.generate_batch(200, min_keys=3, max_keys=8, max_depth=2)

        # Scenario 1: Cache MISS + DB MISS = DB WRITE
        # Objects don't exist anywhere
        start = time.perf_counter()
        ids_write = sync_cache.register_batch_objects(objects)
        write_time = time.perf_counter() - start

        # Scenario 2: Cache HIT (warm cache)
        # Objects are in cache from scenario 1
        start = time.perf_counter()
        ids_cache_warm = sync_cache.register_batch_objects(objects)
        cache_warm_time = time.perf_counter() - start

        # Scenario 3: Cache MISS + DB HIT = DB READ
        # Clear cache by creating new instance, but objects exist in DB
        fresh_cache = JsonRegisterCache(**TEST_CONFIG)
        start = time.perf_counter()
        ids_db_read = fresh_cache.register_batch_objects(objects)
        db_read_time = time.perf_counter() - start
        fresh_cache.close()

        # Verify all return same IDs
        assert ids_write == ids_cache_warm == ids_db_read

        # Print results with clear labels
        print("\n=== 200 Objects: Three Access Patterns ===")
        print(f"1. DB WRITE (cache miss, DB miss):   {write_time * 1000:8.2f}ms")
        print(f"2. CACHE HIT (warm cache):            {cache_warm_time * 1000:8.2f}ms ({write_time / cache_warm_time:5.1f}x faster)")
        print(f"3. DB READ (cache miss, DB hit):      {db_read_time * 1000:8.2f}ms ({write_time / db_read_time:5.1f}x vs write)")
        print(f"\nCache speedup over DB read: {db_read_time / cache_warm_time:.1f}x")

        # Verify expected performance characteristics
        assert cache_warm_time < write_time, "Cache hit should be faster than DB write"
        assert cache_warm_time < db_read_time, "Cache hit should be faster than DB read"
        # DB read should be faster than DB write (no insert, just select)
        assert db_read_time < write_time, "DB read should be faster than DB write"

    def test_sequential_vs_batch(self, sync_cache, json_generator):
        """Compare sequential single calls vs batch registration."""
        objects = json_generator.generate_batch(100, min_keys=3, max_keys=8, max_depth=2)

        # Sequential
        start = time.perf_counter()
        for obj in objects:
            sync_cache.register_object(obj)
        sequential_time = time.perf_counter() - start

        # Batch (use new objects to avoid cache)
        new_objects = json_generator.generate_batch(100, min_keys=3, max_keys=8, max_depth=2)
        start = time.perf_counter()
        sync_cache.register_batch_objects(new_objects)
        batch_time = time.perf_counter() - start

        print("\n=== 100 Objects: Sequential vs Batch ===")
        print(f"Sequential: {sequential_time * 1000:.2f}ms")
        print(f"Batch:      {batch_time * 1000:.2f}ms ({sequential_time / batch_time:.1f}x faster)")

        # Batch should be significantly faster
        assert batch_time < sequential_time

    def test_batch_scaling(self, sync_cache, json_generator):
        """Test how batch performance scales with size."""
        sizes = [10, 50, 100, 200, 500]
        results = []

        for size in sizes:
            objects = json_generator.generate_batch(size, min_keys=3, max_keys=8, max_depth=2)
            start = time.perf_counter()
            sync_cache.register_batch_objects(objects)
            elapsed = time.perf_counter() - start
            results.append((size, elapsed, elapsed / size))

        print("\n=== Batch Scaling ===")
        print("Size | Total (ms) | Per Object (ms)")
        for size, total, per_obj in results:
            print(f"{size:4d} | {total * 1000:10.2f} | {per_obj * 1000:15.4f}")
