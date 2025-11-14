# py-json-register

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org)

A high-performance JSON registration cache for Python with PostgreSQL backend.

## Purpose

This library provides a method `register_object` where JSON is provided as the input parameter, and an integer is returned. It also provides a batch registration function `register_batch_objects`. The store for this data is a PostgreSQL table with at least these two columns:

* An ID column (name configurable) - SERIAL PRIMARY KEY
* A JSONB column (name configurable) - JSONB UNIQUE NOT NULL

## Installation

```bash
pip install json-register
```

Or install from source:

```bash
git clone https://github.com/telicent-oss/py-json-register.git
cd py-json-register
pip install -e .
```

## Current Status

**✅ Implemented:**
- Package structure
- JSON canonicalisation helper
- `JsonRegisterCache` (synchronous implementation)
- `JsonRegisterCacheAsync` (asynchronous implementation)
- Connection pooling for both sync and async
- LRU caching
- Exception hierarchy
- Type hints throughout
- Basic unit tests for canonicalisation

**🔄 In Progress:**
- Integration tests with PostgreSQL
- Additional unit tests

**📝 TODO:**
- GitHub Actions CI/CD
- PyPI publishing
- Comprehensive documentation

## Implementation Details

### Core Components

1. **Two Classes:**
   - `JsonRegisterCache` - Synchronous version using `psycopg` (psycopg3)
   - `JsonRegisterCacheAsync` - Asynchronous version using `asyncpg`

2. **Dependencies:**
   - `asyncpg` - High-performance async PostgreSQL driver
   - `psycopg[pool]` - Synchronous PostgreSQL driver with connection pooling (psycopg3)
   - `lru-dict` - Fast C-based LRU cache (or fallback to `cachetools` if pure Python preferred)
   - Standard library `json` for canonicalisation

3. **JSON Canonicalisation:**
   - Currently uses `json.dumps(obj, sort_keys=True, separators=(',', ':'))` for consistent string representation
   - This ensures objects with different key orders produce the same cache key
   - Cache key is the full canonical JSON string (zero collision guarantee)
   - **Alternative:** Can optionally use [`canonicaljson`](https://pypi.org/project/canonicaljson/) for RFC 8785 compliance
     - Add `canonicaljson` as dependency if strict RFC 8785 compliance is required
     - Standard library approach is sufficient for most use cases

### Configuration Parameters

Both classes will accept the following parameters on initialisation:

- `database_name` (str): PostgreSQL database name
- `database_host` (str): PostgreSQL host
- `database_port` (int): PostgreSQL port (default: 5432)
- `database_user` (str): PostgreSQL user
- `database_password` (str): PostgreSQL password
- `lru_cache_size` (int): Size of the LRU cache
- `table_name` (str): Name of the table to store JSON objects (default: "json_objects")
- `id_column` (str): Name of the ID column (default: "id")
- `jsonb_column` (str): Name of the JSONB column (default: "json_object")
- `pool_size` (int): Connection pool size (default: 10)

### API Methods

#### `register_object(json_obj: dict | list | str) -> int`

Registers a single JSON object and returns its ID.

**Behaviour:**
1. Accepts dict, list, or JSON string
2. Canonicalises the JSON
3. Checks LRU cache first
4. If not cached, queries database using upsert pattern
5. Updates cache with result
6. Returns integer ID

**SQL Query:**
```sql
WITH inserted AS (
    INSERT INTO <table_name> (<jsonb_column>)
    VALUES ($1)
    ON CONFLICT (<jsonb_column>) DO NOTHING
    RETURNING <id_column>
)
SELECT <id_column> FROM inserted
UNION ALL
SELECT <id_column> FROM <table_name>
WHERE <jsonb_column> = $1
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1
```

#### `register_batch_objects(json_objects: list) -> list[int]`

Registers multiple JSON objects in batch and returns their IDs in the same order.

**Behaviour:**
1. If all objects are in cache, return cached IDs immediately
2. Otherwise, use database batch operation
3. Use transaction to ensure atomicity
4. Update cache with all results
5. Return list of IDs in same order as input

**SQL Query:**
```sql
WITH input_objects AS (
    SELECT
        row_number() OVER () as original_order,
        value as json_value
    FROM unnest($1::jsonb[]) WITH ORDINALITY AS t(value, ord)
),
inserted AS (
    INSERT INTO <table_name> (<jsonb_column>)
    SELECT json_value FROM input_objects
    ON CONFLICT (<jsonb_column>) DO NOTHING
    RETURNING <id_column>, <jsonb_column>
),
existing AS (
    SELECT t.<id_column>, t.<jsonb_column>
    FROM <table_name> t
    JOIN input_objects io ON t.<jsonb_column> = io.json_value
)
SELECT COALESCE(i.<id_column>, e.<id_column>) as <id_column>, io.original_order
FROM input_objects io
LEFT JOIN inserted i ON io.json_value = i.<jsonb_column>
LEFT JOIN existing e ON io.json_value = e.<jsonb_column>
ORDER BY io.original_order
```

### LRU Cache Behaviour

- **Single object registration:** Always checks cache first
- **Batch registration:** Only uses cache if ALL objects are already cached
  - If even one object is missing, goes to database for entire batch
  - This simplifies the implementation and ensures consistent performance

### Database Schema

```sql
CREATE TABLE json_objects (
    id SERIAL PRIMARY KEY,
    json_object JSONB UNIQUE NOT NULL
);

-- Create GIN index for better JSONB query performance
CREATE INDEX idx_json_objects_json_object ON json_objects USING gin(json_object);
```

### Implementation Checklist

- [x] Create package structure (`json_register/`)
- [x] Implement JSON canonicalisation helper
- [x] Implement `JsonRegisterCache` (sync version)
  - [x] Connection pooling with psycopg3
  - [x] LRU cache integration
  - [x] `register_object` method
  - [x] `register_batch_objects` method
- [x] Implement `JsonRegisterCacheAsync` (async version)
  - [x] Connection pooling with asyncpg
  - [x] LRU cache integration
  - [x] `register_object` async method
  - [x] `register_batch_objects` async method
- [x] Write unit tests
  - [x] Test JSON canonicalisation
  - [ ] Test cache hits/misses
  - [ ] Test single object registration
  - [ ] Test batch registration
  - [ ] Test order preservation
  - [ ] Test different key orders produce same IDs
- [ ] Write integration tests (require PostgreSQL)
  - [ ] Test with real database
  - [ ] Test batch order preservation
  - [ ] Test mixed existing/new objects
- [ ] Documentation
  - [ ] API reference
  - [x] Usage examples
  - [ ] Database setup guide
- [x] Packaging
  - [x] pyproject.toml with dependencies
  - [x] Type hints throughout
  - [x] py.typed marker for type checking

## Example Usage

#### Synchronous

```python
from json_register import JsonRegisterCache

cache = JsonRegisterCache(
    database_name="mydb",
    database_host="localhost",
    database_port=5432,
    database_user="user",
    database_password="pass",
    lru_cache_size=1000,
    table_name="json_objects",
    id_column="id",
    jsonb_column="json_object",
    pool_size=10
)

# Register single object
obj = {"name": "Alice", "age": 30}
id1 = cache.register_object(obj)
id2 = cache.register_object(obj)  # Returns same ID (cached)
assert id1 == id2

# Register batch
objects = [
    {"name": "Bob"},
    {"name": "Carol"},
    {"name": "Dave"}
]
ids = cache.register_batch_objects(objects)
assert len(ids) == 3
```

#### Asynchronous

```python
from json_register import JsonRegisterCacheAsync
import asyncio

async def main():
    cache = await JsonRegisterCacheAsync.create(
        database_name="mydb",
        database_host="localhost",
        database_port=5432,
        database_user="user",
        database_password="pass",
        lru_cache_size=1000,
        table_name="json_objects",
        id_column="id",
        jsonb_column="json_object",
        pool_size=10
    )

    # Register single object
    obj = {"name": "Alice", "age": 30}
    id1 = await cache.register_object(obj)
    id2 = await cache.register_object(obj)  # Returns same ID (cached)
    assert id1 == id2

    # Register batch
    objects = [
        {"name": "Bob"},
        {"name": "Carol"},
        {"name": "Dave"}
    ]
    ids = await cache.register_batch_objects(objects)
    assert len(ids) == 3

asyncio.run(main())
```

### Design Decisions

1. **Native Python over PyO3:**
   - Simpler implementation
   - Easier to maintain and debug
   - Fast PostgreSQL drivers (asyncpg, psycopg3)
   - LRU cache performance is sufficient for this use case

2. **Connection Pooling:**
   - Always enabled for both sync and async
   - Improves performance under concurrent load

3. **Cache Strategy:**
   - Full canonical JSON string as key (not hash)
   - Zero collision guarantee
   - Simple "all or nothing" for batch operations

4. **JSON Canonicalisation:**
   - Standard library `json.dumps` with `sort_keys=True`
   - Sufficient for key normalisation
   - If RFC 8785 compliance needed later, can use `canonicaljson` package

### Testing Requirements

- Python 3.8+
- PostgreSQL 9.4+ with JSONB support
- Unit tests should mock database connections
- Integration tests require running PostgreSQL instance

### Security Considerations

- SQL injection prevention via parameterised queries
- Password handling (never logged)
- Table/column name validation
- Connection string security

## License

Apache License 2.0

Copyright TELICENT LTD
