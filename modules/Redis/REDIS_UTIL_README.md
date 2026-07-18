# Redis Utility Suite — Production-Ready Redis/Valkey for Python

Two complementary classes for using Redis (Valkey) as a full-time database **and** caching layer. One file, zero boilerplate, full CRUD, async, pipelines, TTL, indexing, locking, import/export, and more.

## Table of Contents

- [Which Class Do I Need?](#which-class-do-i-need)
- [Installation](#installation)
- [RedisHashUtil — Hash-Based Persistent Storage](#redishashutil--hash-based-persistent-storage)
  - [Constructor](#redishashutil-constructor)
  - [CRUD Operations](#crud-operations)
  - [Atomic Operations](#atomic-operations)
  - [Bulk Operations](#bulk-operations)
  - [Get All / Delete All / List IDs](#get-all--delete-all--list-ids)
  - [Copy / Rename](#copy--rename)
  - [TTL Operations](#hash-ttl-operations)
  - [Search](#search)
  - [Secondary Indexes](#secondary-indexes)
  - [Distributed Locks](#distributed-locks)
  - [Import / Export](#hash-import--export)
  - [Secure Hashing](#secure-hashing-static-methods)
  - [ID Generation](#id-generation-static-methods)
  - [Context Manager & Async](#hash-context-manager--async)
- [RedisCacheManager — String-Based Caching Layer](#rediscachemanager--string-based-caching-layer)
  - [Constructor](#rediscachemanager-constructor)
  - [CRUD Operations](#cache-crud-operations)
  - [Cache-Aside Pattern](#cache-aside-pattern)
  - [TTL Operations](#cache-ttl-operations)
  - [Bulk Operations](#cache-bulk-operations)
  - [Pattern Invalidation](#pattern-invalidation)
  - [Inspection & Stats](#inspection--stats)
  - [Import / Export](#cache-import--export)
  - [Decorator — Function Result Caching](#decorator--function-result-caching)
  - [Context Manager & Async](#cache-context-manager--async)
- [Use Cases](#use-cases)
- [Docker Compose — Valkey with Persistence](#docker-compose--valkey-with-persistence)
- [Production Notes](#production-notes)

---

## Which Class Do I Need?

| Need | Class | Redis Type | Why |
|------|-------|-----------|-----|
| Store structured data permanently | `RedisHashUtil` | HASH | Fields per entry, secondary indexes, field-level CRUD |
| Cache API responses / computed values | `RedisCacheManager` | STRING | Simple key-value, JSON-serialized, TTL-first design |
| Both | Use them together | — | Hash for DB, Cache for caching |

**Rule of thumb:** If you need to query/filter by individual fields → `RedisHashUtil`. If you just need to store/retrieve whole objects with TTL → `RedisCacheManager`.

---

## Installation

```bash
pip install redis bcrypt
```

> For Valkey, the `redis` Python client works natively — Valkey is API-compatible with Redis.

---

## RedisHashUtil — Hash-Based Persistent Storage

A comprehensive utility for Redis **hash** operations. Each entry is a Redis HASH with multiple fields, namespaced under a prefix. Designed as a full database replacement for entity storage.

### RedisHashUtil Constructor

```python
from redis_hash_util import RedisHashUtil

workers = RedisHashUtil(
    url="redis://localhost:6379/0",   # Redis/Valkey connection URL
    prefix="USERS:WORKERS",           # Key namespace (all keys: USERS:WORKERS:{id})
    index_key="IDX",                  # Segment for secondary indexes
    lock_key="LOCK",                  # Segment for distributed locks
    default_ttl=None,                 # None = permanent, 3600 = 1 hour auto-expire
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | `"redis://localhost:6379/0"` | Redis/Valkey connection URL |
| `prefix` | `str` | `"DEFAULT"` | Hash key prefix for namespacing |
| `index_key` | `str` | `"IDX"` | Key segment for secondary indexes |
| `lock_key` | `str` | `"LOCK"` | Key segment for distributed locks |
| `default_ttl` | `Optional[int]` | `None` | Default TTL in seconds (`None` = permanent) |

### CRUD Operations

```python
# Create — auto-generates UUID4 if no id provided
user_id = workers.create({"username": "johndoe", "email": "john@example.com"})

# Create with explicit id
workers.create({"username": "alice"}, id="u-001")

# Create with TTL override (bypasses default_ttl)
workers.create({"username": "temp"}, id="u-temp", ttl=3600)

# Read — full dict or single field
data = workers.read("u-001")                    # {"username": "alice", ...}
email = workers.read("u-001", field="email")    # "alice@example.com"

# Update — merge fields into existing entry
workers.update("u-001", {"role": "superadmin", "last_login": "2026-01-15"})

# Delete — entire entry or specific fields
workers.delete("u-001")                         # delete entire hash
workers.delete_fields("u-001", "last_login")    # delete specific fields only

# Existence checks
workers.exists("u-001")                         # True/False
workers.field_exists("u-001", "email")          # True/False

# Field inspection
workers.keys("u-001")        # ["username", "email", "role"]
workers.values("u-001")      # ["alice", "alice@example.com", "admin"]
workers.length("u-001")      # 3

# Atomic increment
workers.increment("u-001", "login_count", amount=1)
workers.increment_float("u-001", "balance", amount=99.99)
```

### Atomic Operations

```python
# Set field only if it doesn't exist (HSETNX)
workers.set_if_not_exists("u-001", "created_at", "2026-01-01")  # True (first time)
workers.set_if_not_exists("u-001", "created_at", "2026-06-01")  # False (already exists)

# Get existing entry or create new one (cache-aside pattern for hashes)
data = workers.get_or_create("u-001", {"username": "alice", "status": "pending"})
```

### Bulk Operations

All bulk operations use **pipelines** — no N+1 round trips.

```python
# Bulk create
entries = {
    "u-001": {"name": "Alice", "role": "admin"},
    "u-002": {"name": "Bob", "role": "user"},
    "u-003": {"name": "Charlie", "role": "user"},
}
created = workers.bulk_create(entries, overwrite=True)

# Bulk read
data = workers.bulk_read(["u-001", "u-002", "u-003"])
# {"u-001": {"name": "Alice", ...}, "u-002": {...}, ...}

# Bulk update
workers.bulk_update({
    "u-001": {"status": "inactive"},
    "u-002": {"status": "active"},
})

# Bulk delete
workers.bulk_delete(["u-001", "u-002"])
```

### Get All / Delete All / List IDs

All use **SCAN** — never blocks Redis (safe for production).

```python
# Get all entries under this prefix
all_data = workers.get_all()

# With filtering, sorting, pagination
results = workers.get_all(
    filter_by={"role": "admin", "status": "active"},
    sort_by="username",
    sort_order="asc",       # or "desc"
    offset=0,               # skip first N entries
    limit=10,               # max entries to return (0 = no limit)
)

# Count entries
count = workers.count_all()

# List IDs with pagination
ids = workers.list_ids(offset=0, limit=100)

# Delete all (or by pattern)
workers.delete_all()
workers.delete_all(pattern="u-00")  # delete only matching entries
```

### Copy / Rename

```python
# Copy — auto-generates UUID4 or explicit id
new_id = workers.copy("u-001")
workers.copy("u-001", "u-001-backup")

# Bulk copy
workers.bulk_copy({"u-001": "u-001-v2", "u-002": "u-002-v2"})

# Rename
workers.rename("u-001", "u-001-renamed")
workers.rename("u-001", "u-001-v2", overwrite=True)  # overwrite if target exists
```

### Hash TTL Operations

```python
# Set TTL on an entry
workers.expire("u-001", 3600)   # expire in 1 hour

# Check remaining TTL
remaining = workers.ttl("u-001")  # seconds, -1 = permanent, -2 = missing

# Remove TTL (make permanent)
workers.persist("u-001")

# Bulk expire
workers.bulk_expire(["u-001", "u-002"], 3600)
```

### Search

Linear SCAN-based search. For indexed lookups, use [Secondary Indexes](#secondary-indexes).

```python
# Exact match
admins = workers.search("role", "admin")  # ["u-001", "u-003"]

# Substring match
ali = workers.search("username", "ali", exact=False)  # ["u-001"]

# Search with full data
admins_data = workers.search_with_data("role", "admin")
# {"u-001": {"name": "Alice", ...}, "u-003": {"name": "Charlie", ...}}
```

### Secondary Indexes

Fast SET-based lookups for frequently queried fields.

```python
# Create index entries for a field
workers.create_index("u-001", "role")
workers.create_index("u-002", "role")
workers.create_index("u-003", "role")

# Find by index (fast SET lookup, no scan needed)
admin_ids = workers.find_by_index("role", "admin")  # ["u-001", "u-003"]

# Find with full data
admins = workers.find_by_index_with_data("role", "admin")

# Remove a single index entry
workers.remove_index("u-001", "role")

# Delete ALL indexes for a field
workers.delete_index_field("role")
```

### Distributed Locks

Mutual exclusion across processes/containers.

```python
lock = workers.acquire_lock("u-001", timeout=10.0, blocking_timeout=5.0)
if lock:
    try:
        # Critical section — only one process runs this
        workers.update("u-001", {"status": "processing"})
    finally:
        workers.release_lock(lock)
```

### Hash Import / Export

```python
# JSON
workers.export_json("/tmp/workers.json")
workers.import_json("/tmp/workers.json", overwrite=True)
json_str = workers.export_json_string()

# CSV
workers.export_csv("/tmp/workers.csv")
workers.import_csv("/tmp/workers.csv", id_column="_id")
csv_str = workers.export_csv_string()
```

### Secure Hashing (Static Methods)

```python
# Password hashing (bcrypt, 12 rounds)
hashed = RedisHashUtil.hash_password("SuperSecret123!")
valid = RedisHashUtil.verify_password("SuperSecret123!", hashed)  # True

# One-way SHA-256 for sensitive data (emails, PII)
email_hash = RedisHashUtil.hash_sensitive_data("user@example.com", pepper="myapp")

# HMAC-based hashing
token_hash = RedisHashUtil.hash_sensitive_data_hmac("sensitive-token", secret="my-secret")
```

### ID Generation (Static Methods)

```python
RedisHashUtil.generate_random_string(32)              # "aB3kQ9mN2xR7pL4w..."
RedisHashUtil.generate_random_number(6)               # "482917" (OTP)
RedisHashUtil.generate_random_number(4)               # "0372" (PIN)
RedisHashUtil.generate_token(64)                       # URL-safe token
RedisHashUtil.generate_uuid4()                         # random UUID
RedisHashUtil.generate_uuid5("myapp.users", "john@x") # deterministic UUID
RedisHashUtil.generate_hash_id({"email": "a@b.com"})  # 16-char dedup hash
```

### Hash Context Manager & Async

```python
# Context manager — auto-closes connection
with RedisHashUtil(prefix="TEMP") as temp:
    temp.create({"data": "value"}, id="t-001")
    # connection closed on exit

# Async
import asyncio

async def main():
    users = RedisHashUtil(prefix="USERS")
    uid = await users.async_create({"name": "Alice"}, id="u-001")
    data = await users.async_read("u-001")
    await users.async_close()

asyncio.run(main())
```

---

## RedisCacheManager — String-Based Caching Layer

A production-ready caching utility using Redis **STRING** type. Every entry is a single JSON-serialized value with automatic TTL. Designed for caching API responses, computed results, sessions, and ephemeral data.

**Key difference from RedisHashUtil:** Stores whole objects as JSON strings (not hash fields). TTL is core to the design. No secondary indexes or locks — caching is stateless.

### RedisCacheManager Constructor

```python
from redis_hash_util import RedisCacheManager

# Ephemeral cache — entries auto-expire
cache = RedisCacheManager(
    url="redis://localhost:6379/0",
    prefix="API:USERS",
    default_ttl=600,       # 10 min default TTL (optional)
)

# Persistent cache — entries live forever (like RedisHashUtil)
permanent = RedisCacheManager(
    prefix="CONFIG",
    default_ttl=None,      # no expiry by default
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | `"redis://localhost:6379/0"` | Redis/Valkey connection URL |
| `prefix` | `str` | `"CACHE"` | Key prefix for namespace isolation |
| `default_ttl` | `Optional[int]` | `None` | Default TTL in seconds (`None` = permanent) |

### Cache CRUD Operations

```python
# Store — raises ValueError if key exists (and overwrite=False)
cache.store("user:123", {"name": "Alice", "role": "admin"}, overwrite=True)

# Store with TTL override
cache.store("session:abc", {"token": "xyz"}, ttl=300)

# Retrieve — returns None if missing (or your default)
user = cache.retrieve("user:123")                       # {"name": "Alice", ...}
user = cache.retrieve("user:999", default={"name": "Nobody"})

# Upsert — silent overwrite (never raises)
cache.upsert("user:123", {"name": "Alice", "role": "superadmin"})

# Delete one or more keys
cache.delete("user:123")
cache.delete("key1", "key2", "key3")

# Check existence
cache.exists("user:123")   # True
cache.exists("user:999")   # False
```

### Cache-Aside Pattern

The `get_or_set` method implements the standard **cache-aside (lazy-loading)** pattern.

```python
call_count = 0

def expensive_db_query(user_id: str) -> dict:
    """Simulate slow DB call."""
    global call_count
    call_count += 1
    return {"name": f"User_{user_id}", "computed_at": time.time()}

# First call — cache miss → calls factory → stores result → returns it
result = cache.get_or_set("user:456", lambda: expensive_db_query("456"), ttl=300)
# call_count = 1

# Second call — cache hit → returns stored value, factory NOT called
result = cache.get_or_set("user:456", lambda: expensive_db_query("456"), ttl=300)
# call_count = 1 (unchanged)

# With a static default value (no callable)
config = cache.get_or_set("config:features", {"dark_mode": True, "beta": False})
```

### Atomic Operations

```python
# Store only if key does not exist (SET NX)
cache.store_if_not_exists("lock:job:123", "worker-1")   # True
cache.store_if_not_exists("lock:job:123", "worker-2")   # False (already exists)
```

### Cache TTL Operations

```python
# Set TTL on existing entry
cache.expire("user:123", 7200)   # 2 hours

# Check remaining TTL
cache.ttl("user:123")   # seconds remaining (-1 = permanent, -2 = missing)

# Remove TTL (make permanent)
cache.persist("user:123")

# Bulk expire
cache.bulk_expire(["user:123", "user:456"], 3600)
```

### Cache Bulk Operations

All use **pipelines** for performance.

```python
# Bulk store
cache.bulk_store({
    "item:1": {"name": "apple", "qty": 5},
    "item:2": {"name": "banana", "qty": 3},
    "item:3": {"name": "cherry", "qty": 8},
}, ttl=120)

# Bulk retrieve (missing keys return None or your default)
data = cache.bulk_retrieve(["item:1", "item:2", "item:3", "item:missing"])
# {"item:1": {...}, "item:2": {...}, "item:3": {...}, "item:missing": None}

# Bulk delete
cache.bulk_delete(["item:1", "item:2", "item:3"])
```

### Pattern Invalidation

Invalidate groups of cache entries using glob patterns. All use **SCAN** (non-blocking).

```python
# Delete all entries matching a pattern
cache.invalidate_pattern("user:*")          # delete CACHE:user:123, CACHE:user:456, ...

# Delete by sub-namespace
cache.invalidate_namespace("session")       # delete all CACHE:session:* entries

# Delete ALL entries under this prefix (dangerous!)
cache.flush_all()
```

### Inspection & Stats

```python
# Count entries
cache.count()                           # total under this prefix
cache.count(pattern="user:*")           # count matching pattern

# List keys with pagination
keys = cache.list_keys(limit=10)
keys = cache.list_keys(pattern="user:*", offset=0, limit=20)

# Cache statistics (from Redis INFO)
stats = cache.stats()
# {
#     "used_memory": 1048576,
#     "used_memory_human": "1.00M",
#     "keyspace_hits": 9500,
#     "keyspace_misses": 500,
#     "hit_rate": 0.95,
#     "total_keys": 256,
# }
```

### Cache Import / Export

```python
# JSON file
cache.export_json("/tmp/cache_backup.json")
cache.import_json("/tmp/cache_backup.json", overwrite=True)

# JSON string
json_str = cache.export_json_string()
cache.import_json_string(json_str, overwrite=True)
```

### Decorator — Function Result Caching

Cache any function's return value automatically.

```python
@cache.cache_result(ttl=300)
def get_user(user_id: str) -> dict:
    return db.query_user(user_id)   # only called on cache miss

@cache.cache_result(ttl=60, key_prefix="api")
def fetch_products(category: str) -> list:
    return api.get_products(category)

# With fallback on Redis errors
@cache.cache_result(ttl=300, fallback=lambda func, *a, **kw: func(*a, **kw))
def critical_query(id: str) -> dict:
    return db.query(id)

# Manual cache invalidation
get_user.cache_clear()
```

### Cache Context Manager & Async

```python
# Context manager — auto-closes connection
with RedisCacheManager(prefix="TEMP") as temp:
    temp.store("key", "value")
    # connection closed on exit

# Async
import asyncio

async def main():
    cache = RedisCacheManager(prefix="API")
    await cache.async_store("user:123", {"name": "Alice"})
    data = await cache.async_retrieve("user:123")
    await cache.async_close()

asyncio.run(main())
```

**Available async methods:** `async_store`, `async_retrieve`, `async_upsert`, `async_delete`, `async_exists`, `async_get_or_set`, `async_bulk_store`, `async_bulk_retrieve`, `async_invalidate_pattern`, `async_count`, `async_flush_all`, `async_close`.

---

## Use Cases

| Use Case | Class | Prefix | TTL |
|----------|-------|--------|-----|
| User profiles (permanent DB) | `RedisHashUtil` | `USERS:PROFILES` | `None` |
| Session tokens | `RedisCacheManager` | `SESSIONS` | `86400` (24h) |
| OTP codes | `RedisHashUtil` | `USERS:OTP` | `300` (5 min) |
| Password reset tokens | `RedisCacheManager` | `RESET_TOKENS` | `900` (15 min) |
| API response cache | `RedisCacheManager` | `CACHE:API` | `600` (10 min) |
| Rate limiting counters | `RedisHashUtil` | `RATELIMIT:API` | — |
| Feature flags (permanent) | `RedisHashUtil` | `FEATURES` | `None` |
| Job queues | `RedisHashUtil` | `JOBS:PENDING` | — |
| Computed / expensive results | `RedisCacheManager` | `COMPUTED` | `300` (5 min) |
| Config cache (long-lived) | `RedisCacheManager` | `CONFIG` | `None` |

---

## Docker Compose — Valkey with Persistence

```yaml
version: "3.8"

services:
  valkey:
    image: valkey/valkey:8-alpine
    container_name: valkey
    restart: always
    ports:
      - "6379:6379"
    volumes:
      - valkey_data:/data
    command: >
      valkey-server
      --appendonly yes
      --appendfsync everysec
      --maxmemory 512mb
      --maxmemory-policy allkeys-lru
      --requirepass ${VALKEY_PASSWORD:-supersecretpassword}
      --save 60 1000
      --save 300 100
      --save 900 1
      --tcp-keepalive 300
      --timeout 0
      --databases 16
      --loglevel notice
    healthcheck:
      test: ["CMD", "valkey-cli", "-a", "${VALKEY_PASSWORD:-supersecretpassword}", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - app_network

volumes:
  valkey_data:
    driver: local

networks:
  app_network:
    driver: bridge
```

### Connecting with Password

```python
from redis_hash_util import RedisHashUtil, RedisCacheManager

# Hash-based storage
workers = RedisHashUtil(
    url="redis://:supersecretpassword@localhost:6379/0",
    prefix="USERS:WORKERS",
)

# String-based cache
cache = RedisCacheManager(
    url="redis://:supersecretpassword@localhost:6379/0",
    prefix="API:CACHE",
    default_ttl=600,
)
```

---

## Production Notes

### Shared Across Both Classes

- All bulk operations use **pipelines** — no N+1 round trips
- `get_all` / `delete_all` / `list_ids` / `invalidate_pattern` use **SCAN**, never KEYS (non-blocking)
- `default_ttl` applies automatically — per-call `ttl` parameter overrides it
- Both classes support **context manager** pattern for safe connection cleanup
- Both classes support full **async** with `asyncio`
- All methods have **type hints** and **docstrings**
- Prefix is **uppercased** automatically for consistency

### RedisHashUtil Specific

- Password hashing uses **bcrypt** with 12 rounds
- Random generation uses **`secrets`** module (cryptographically secure)
- Secondary indexes use Redis **SET** for O(1) membership checks
- Distributed locks use Redis **LOCK** with configurable timeout/blocking
- Supports both **JSON** and **CSV** import/export

### RedisCacheManager Specific

- All values are **JSON-serialized** on write, **deserialized** on read
- `default_ttl=None` means permanent — same behavior as `RedisHashUtil`
- `store_if_not_exists` uses atomic **SET NX** — safe for distributed claim patterns
- `get_or_set` implements **cache-aside (lazy-loading)** pattern
- `@cache_result` decorator caches function return values with automatic key generation
- Long cache keys (>128 chars) are **SHA-256 hashed** to stay within Redis limits
- `stats()` returns **hit rate**, memory usage, and key counts from Redis `INFO`
