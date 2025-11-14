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

"""Performance tests for json-register.

These tests use pytest-benchmark to measure performance of various operations.

Run with: pytest tests/test_performance.py -v --benchmark-only
"""

import os

import pytest

from json_register import JsonRegisterCache, JsonRegisterCacheAsync
from tests.fixtures.json_generator import RandomJsonGenerator

# Test configuration - uses environment variables in CI, falls back to local config
TEST_CONFIG = {
    "database_name": os.getenv("TEST_DB_NAME", "access"),
    "database_host": os.getenv("TEST_DB_HOST", "localhost"),
    "database_port": int(os.getenv("TEST_DB_PORT", "5432")),
    "database_user": os.getenv("TEST_DB_USER", "postgres"),
    "database_password": os.getenv("TEST_DB_PASSWORD", ""),
    "lru_cache_size": 1000,
    "table_name": os.getenv("TEST_DB_TABLE", "labels"),
    "id_column": os.getenv("TEST_DB_ID_COLUMN", "id"),
    "jsonb_column": os.getenv("TEST_DB_JSONB_COLUMN", "label"),
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


@pytest.fixture(scope="module")
async def async_cache():
    """Provide an asynchronous cache instance."""
    cache = await JsonRegisterCacheAsync.create(**TEST_CONFIG)
    yield cache
    await cache.close()


@pytest.mark.benchmark
class TestSyncPerformance:
    """Performance tests for synchronous cache."""

    def test_single_object_cache_miss_small(self, benchmark, sync_cache, json_generator):
        """Benchmark single object registration (cache miss) - small object."""
        obj = json_generator.generate_profile("small")
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_single_object_cache_miss_medium(self, benchmark, sync_cache, json_generator):
        """Benchmark single object registration (cache miss) - medium object."""
        obj = json_generator.generate_profile("medium")
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_single_object_cache_miss_large(self, benchmark, sync_cache, json_generator):
        """Benchmark single object registration (cache miss) - large object."""
        obj = json_generator.generate_profile("large")
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_single_object_cache_hit(self, benchmark, sync_cache, json_generator):
        """Benchmark single object registration (cache hit)."""
        obj = json_generator.generate_profile("medium")
        # Pre-register to ensure cache hit
        sync_cache.register_object(obj)
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_batch_10_objects_all_new(self, benchmark, sync_cache, json_generator):
        """Benchmark batch registration - 10 new objects."""
        objects = json_generator.generate_batch(10, min_keys=5, max_keys=15, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 10

    def test_batch_50_objects_all_new(self, benchmark, sync_cache, json_generator):
        """Benchmark batch registration - 50 new objects."""
        objects = json_generator.generate_batch(50, min_keys=5, max_keys=15, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 50

    def test_batch_100_objects_all_new(self, benchmark, sync_cache, json_generator):
        """Benchmark batch registration - 100 new objects."""
        objects = json_generator.generate_batch(100, min_keys=5, max_keys=15, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 100

    def test_batch_10_objects_all_cached(self, benchmark, sync_cache, json_generator):
        """Benchmark batch registration - 10 cached objects."""
        objects = json_generator.generate_batch(10, min_keys=5, max_keys=15, max_depth=2)
        # Pre-register all objects
        sync_cache.register_batch_objects(objects)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 10

    def test_batch_100_objects_mixed(self, benchmark, sync_cache, json_generator):
        """Benchmark batch registration - 100 mixed (50% cached, 50% new)."""
        cached_objects = json_generator.generate_batch(50, min_keys=5, max_keys=15, max_depth=2)
        new_objects = json_generator.generate_batch(50, min_keys=5, max_keys=15, max_depth=2)

        # Pre-register half
        sync_cache.register_batch_objects(cached_objects)

        # Interleave cached and new objects
        mixed_objects = []
        for i in range(50):
            mixed_objects.append(cached_objects[i])
            mixed_objects.append(new_objects[i])

        result = benchmark(sync_cache.register_batch_objects, mixed_objects)
        assert len(result) == 100

    def test_write_vs_cache_vs_db_read_300_objects(self, sync_cache, json_generator):
        """
        Comprehensive test: DB WRITE vs CACHE READ vs DB READ.

        Tests 300 objects in three scenarios:
        1. All new (database WRITE)
        2. All cached (CACHE READ)
        3. Cache cleared (database READ)
        """
        from pytest_benchmark.fixture import BenchmarkFixture

        # Generate 300 unique objects
        objects = json_generator.generate_batch(300, min_keys=5, max_keys=15, max_depth=2)

        # Scenario 1: Database WRITE (all new objects)
        benchmark_write = BenchmarkFixture.FixtureValue(sync_cache.register_batch_objects)
        result_write = benchmark_write(objects)
        assert len(result_write) == 300
        write_time = benchmark_write.stats.mean

        # Scenario 2: CACHE READ (all objects already registered)
        benchmark_cache = BenchmarkFixture.FixtureValue(sync_cache.register_batch_objects)
        result_cache = benchmark_cache(objects)
        assert len(result_cache) == 300
        cache_time = benchmark_cache.stats.mean

        # Scenario 3: Database READ (clear cache first)
        # Clear the cache by creating a new instance
        from json_register import JsonRegisterCache

        fresh_cache = JsonRegisterCache(
            database_name=sync_cache._table_name,  # Using existing test config
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="",
            lru_cache_size=1000,
        )

        benchmark_db_read = BenchmarkFixture.FixtureValue(fresh_cache.register_batch_objects)
        result_db = benchmark_db_read(objects)
        assert len(result_db) == 300
        db_read_time = benchmark_db_read.stats.mean

        fresh_cache.close()

        # Print comparison
        print("\n300 Objects Performance Comparison:")
        print(f"  DB WRITE:    {write_time * 1000:.2f}ms")
        print(f"  CACHE READ:  {cache_time * 1000:.2f}ms (speedup: {write_time / cache_time:.1f}x)")
        print(
            f"  DB READ:     {db_read_time * 1000:.2f}ms (speedup: {write_time / db_read_time:.1f}x)"
        )

    # Edge case tests
    def test_edge_case_very_small_objects(self, benchmark, sync_cache, json_generator):
        """Benchmark with minimal objects (1-2 keys, no nesting)."""
        objects = json_generator.generate_batch(100, min_keys=1, max_keys=2, max_depth=0)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 100

    def test_edge_case_very_large_objects(self, benchmark, sync_cache, json_generator):
        """Benchmark with very large objects (50-100 keys, deep nesting)."""
        obj = json_generator.generate_object(min_keys=50, max_keys=100, max_depth=4)
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_edge_case_deep_nesting(self, benchmark, sync_cache, json_generator):
        """Benchmark with deeply nested objects (depth 5)."""
        obj = json_generator.generate_profile("deep")
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_edge_case_wide_objects(self, benchmark, sync_cache, json_generator):
        """Benchmark with wide objects (many keys, no nesting)."""
        obj = json_generator.generate_profile("wide")
        result = benchmark(sync_cache.register_object, obj)
        assert isinstance(result, int)

    def test_batch_size_scaling_10(self, benchmark, sync_cache, json_generator):
        """Benchmark batch size scaling - 10 objects."""
        objects = json_generator.generate_batch(10, min_keys=5, max_keys=15, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 10

    def test_batch_size_scaling_100(self, benchmark, sync_cache, json_generator):
        """Benchmark batch size scaling - 100 objects."""
        objects = json_generator.generate_batch(100, min_keys=5, max_keys=15, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 100

    def test_batch_size_scaling_500(self, benchmark, sync_cache, json_generator):
        """Benchmark batch size scaling - 500 objects."""
        objects = json_generator.generate_batch(500, min_keys=5, max_keys=15, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 500

    def test_batch_size_scaling_1000(self, benchmark, sync_cache, json_generator):
        """Benchmark batch size scaling - 1000 objects."""
        objects = json_generator.generate_batch(1000, min_keys=5, max_keys=15, max_depth=2)
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 1000

    def test_duplicate_handling_in_batch(self, benchmark, sync_cache, json_generator):
        """Benchmark batch with duplicate objects."""
        obj = json_generator.generate_profile("medium")
        # Create batch with 50% duplicates
        objects = [
            obj if i % 2 == 0 else json_generator.generate_profile("medium") for i in range(100)
        ]
        result = benchmark(sync_cache.register_batch_objects, objects)
        assert len(result) == 100

    def test_sequential_single_vs_batch(self, sync_cache, json_generator):
        """Compare sequential single registrations vs batch registration."""
        import time

        objects = json_generator.generate_batch(100, min_keys=5, max_keys=15, max_depth=2)

        # Sequential single registrations
        start = time.perf_counter()
        for obj in objects:
            sync_cache.register_object(obj)
        sequential_time = time.perf_counter() - start

        # Batch registration (new objects to avoid cache hits)
        new_objects = json_generator.generate_batch(100, min_keys=5, max_keys=15, max_depth=2)
        start = time.perf_counter()
        sync_cache.register_batch_objects(new_objects)
        batch_time = time.perf_counter() - start

        print("\n100 Objects Sequential vs Batch:")
        print(f"  Sequential: {sequential_time * 1000:.2f}ms")
        print(f"  Batch:      {batch_time * 1000:.2f}ms")
        print(f"  Speedup:    {sequential_time / batch_time:.1f}x")

        # Batch should be faster
        assert batch_time < sequential_time


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestAsyncPerformance:
    """Performance tests for asynchronous cache."""

    async def test_single_object_cache_miss_small(self, benchmark, async_cache, json_generator):
        """Benchmark single object registration (cache miss) - small object."""
        obj = json_generator.generate_profile("small")

        async def register():
            return await async_cache.register_object(obj)

        result = benchmark(register)
        assert isinstance(result, int)

    async def test_single_object_cache_miss_medium(self, benchmark, async_cache, json_generator):
        """Benchmark single object registration (cache miss) - medium object."""
        obj = json_generator.generate_profile("medium")

        async def register():
            return await async_cache.register_object(obj)

        result = benchmark(register)
        assert isinstance(result, int)

    async def test_single_object_cache_miss_large(self, benchmark, async_cache, json_generator):
        """Benchmark single object registration (cache miss) - large object."""
        obj = json_generator.generate_profile("large")

        async def register():
            return await async_cache.register_object(obj)

        result = benchmark(register)
        assert isinstance(result, int)

    async def test_single_object_cache_hit(self, benchmark, async_cache, json_generator):
        """Benchmark single object registration (cache hit)."""
        obj = json_generator.generate_profile("medium")
        # Pre-register to ensure cache hit
        await async_cache.register_object(obj)

        async def register():
            return await async_cache.register_object(obj)

        result = benchmark(register)
        assert isinstance(result, int)

    async def test_batch_10_objects_all_new(self, benchmark, async_cache, json_generator):
        """Benchmark batch registration - 10 new objects."""
        objects = json_generator.generate_batch(10, min_keys=5, max_keys=15, max_depth=2)

        async def register():
            return await async_cache.register_batch_objects(objects)

        result = benchmark(register)
        assert len(result) == 10

    async def test_batch_50_objects_all_new(self, benchmark, async_cache, json_generator):
        """Benchmark batch registration - 50 new objects."""
        objects = json_generator.generate_batch(50, min_keys=5, max_keys=15, max_depth=2)

        async def register():
            return await async_cache.register_batch_objects(objects)

        result = benchmark(register)
        assert len(result) == 50

    async def test_batch_100_objects_all_new(self, benchmark, async_cache, json_generator):
        """Benchmark batch registration - 100 new objects."""
        objects = json_generator.generate_batch(100, min_keys=5, max_keys=15, max_depth=2)

        async def register():
            return await async_cache.register_batch_objects(objects)

        result = benchmark(register)
        assert len(result) == 100

    async def test_batch_10_objects_all_cached(self, benchmark, async_cache, json_generator):
        """Benchmark batch registration - 10 cached objects."""
        objects = json_generator.generate_batch(10, min_keys=5, max_keys=15, max_depth=2)
        # Pre-register all objects
        await async_cache.register_batch_objects(objects)

        async def register():
            return await async_cache.register_batch_objects(objects)

        result = benchmark(register)
        assert len(result) == 10

    async def test_batch_100_objects_mixed(self, benchmark, async_cache, json_generator):
        """Benchmark batch registration - 100 mixed (50% cached, 50% new)."""
        cached_objects = json_generator.generate_batch(50, min_keys=5, max_keys=15, max_depth=2)
        new_objects = json_generator.generate_batch(50, min_keys=5, max_keys=15, max_depth=2)

        # Pre-register half
        await async_cache.register_batch_objects(cached_objects)

        # Interleave cached and new objects
        mixed_objects = []
        for i in range(50):
            mixed_objects.append(cached_objects[i])
            mixed_objects.append(new_objects[i])

        async def register():
            return await async_cache.register_batch_objects(mixed_objects)

        result = benchmark(register)
        assert len(result) == 100
