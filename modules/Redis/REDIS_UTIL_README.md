# Redis Utility Suite — Production-Ready Redis/Valkey for Python

Three complementary classes for using Redis (Valkey) as a full-time database, a general-purpose key-value store, **and** a caching layer. One file, zero boilerplate, full CRUD, async, pipelines, TTL, indexing, locking, import/export, and more.

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
- [RedisStringUtil — String-Based Key-Value Store](#redisstringutil--string-based-key-value-store)
  - [Constructor](#redisstringutil-constructor)
  - [CRUD Operations](#string-crud-operations)
  - [Atomic Operations](#string-atomic-operations)
  - [TTL Operations](#string-ttl-operations)
  - [Bulk Operations](#string-bulk-operations)
  - [Pattern Deletion](#string-pattern-deletion)
  - [Inspection & Stats](#string-inspection--stats)
  - [Import / Export](#string-import--export)
  - [Decorator — Function Result Memoization](#decorator--function-result-memoization)
  - [Context Manager & Async](#string-context-manager--async)
- [RedisCache — Caching Layer on RedisStringUtil](#rediscache--caching-layer-on-redisstringutil)
  - [Constructor](#rediscache-constructor)
  - [Core Operations](#cache-core-operations)
  - [TTL Operations](#cache-ttl-operations)
  - [Cache-Aside & Atomic Claims](#cache-aside--atomic-claims)
  - [Bulk Operations](#cache-bulk-operations)
  - [Inspection & Invalidation](#cache-inspection--invalidation)
  - [Context Manager](#cache-context-manager)
- [Use Cases](#use-cases)
- [Docker Compose — Valkey with Persistence](#docker-compose--valkey-with-persistence)
- [Production Notes](#production-notes)

---

## Which Class Do I Need?

| Need | Class | Redis Type | Why |
|------|-------|-----------|-----|
| Store structured data (multiple fields per entity) | `RedisHashUtil` | HASH | Fields per entry, secondary indexes, field-level CRUD |
| Store whole objects under a flat key | `RedisStringUtil` | STRING | Simple key-value, JSON-serialized, optional TTL |
| Cache API responses / computed values | `RedisCache` | STRING | Dedicated `CACHE:` namespace, TTL-first design |
| Both | Use them together | — | Hash for DB, String for KV, Cache for caching |

**Rule of thumb:** If you need to query/filter by individual fields → `RedisHashUtil`. If you just need to store/retrieve whole objects → `RedisStringUtil`. If the data is meant to be short-lived/expiring → `RedisCache`.

---

## Installation

```bash
pip install redis bcrypt
```

> For Valkey, the `redis` Python client works natively — Valkey is API-compatible with Redis.

---

## RedisHashUtil — Hash-Based Persistent Storage

A comprehensive utility for Redis **hash** operations. Each entry is a Redis HASH with multiple fields, namespaced under a prefix. Designed as a full database replacement for entity storage.

> **Type preservation:** All hash values are JSON-serialized on write and deserialized on read. `int`, `float`, `bool`, `None`, `str`, `list`, and `dict` values round-trip with exact Python types. Atomic increment (`HINCRBY`/`HINCRBYFLOAT`) works seamlessly since numeric JSON strings are valid Redis counters.

### RedisHashUtil Constructor

```python
from redis_core_util import RedisHashUtil

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

# All values preserve their Python types (bool, int, float, None, list, dict)
workers.create({"name": "Alice", "age": 33, "active": False, "tags": ["a", "b"]})
data = workers.read(id)  # age → 33 (int), active → False (bool), tags → ["a", "b"] (list)

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
# Bulk create — dict form (explicit ids)
entries = {
    "u-001": {"name": "Alice", "role": "admin"},
    "u-002": {"name": "Bob", "role": "user"},
}
created = workers.bulk_create(entries, overwrite=True)
# {"u-001": "u-001", "u-002": "u-002"}

# Bulk create — list form (auto-generated UUIDs)
ids = workers.bulk_create([
    {"name": "Alice", "role": "admin"},
    {"name": "Bob", "role": "user"},
], overwrite=True)
# ["f68fab41-...", "db4b9b62-..."]

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
# Exact match (supports any type — str, int, bool, etc.)
admins = workers.search("role", "admin")       # ["u-001", "u-003"]
admins = workers.search("active", True)         # find active=True entries
admins = workers.search("age", 33)              # find age = 33 entries

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

## RedisStringUtil — String-Based Key-Value Store

A general-purpose utility for Redis **STRING** operations. Each entry is a single JSON-serialized value stored under a flat key. This is the STRING counterpart to `RedisHashUtil` — use it whenever you need to store whole objects without field-level queries.

> **Type preservation:** Values are JSON-serialized on write and deserialized on read. `int`, `float`, `bool`, `None`, `str`, `list`, and `dict` values round-trip with exact Python types.

### RedisStringUtil Constructor

```python
from redis_core_util import RedisStringUtil

# Ephemeral store — entries auto-expire
store = RedisStringUtil(
    url="redis://localhost:6379/0",
    prefix="API:USERS",
    default_ttl=600,       # 10 min default TTL (optional)
)

# Persistent store — entries live forever
permanent = RedisStringUtil(
    prefix="CONFIG",
    default_ttl=None,      # no expiry by default
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | `"redis://localhost:6379/0"` | Redis/Valkey connection URL |
| `prefix` | `str` | `"STRING"` | Key prefix for namespace isolation |
| `default_ttl` | `Optional[int]` | `None` | Default TTL in seconds (`None` = permanent) |

### String CRUD Operations

```python
# Set — raises ValueError if key exists (and overwrite=False)
store.set("user:123", {"name": "Alice", "role": "admin"}, overwrite=True)

# Set with TTL override
store.set("session:abc", {"token": "xyz"}, ttl=300)

# Get — returns None if missing (or your default)
user = store.get("user:123")                       # {"name": "Alice", ...}
user = store.get("user:999", default={"name": "Nobody"})

# Upsert — silent overwrite (never raises)
store.upsert("user:123", {"name": "Alice", "role": "superadmin"})

# with_ttl=True — returns (data, remaining_ttl) alongside the value
value, remaining = store.get("user:123", with_ttl=True)
stored, ttl = store.set("user:456", {"x": 1}, ttl=120, with_ttl=True)
exists, ttl = store.exists("user:123", with_ttl=True)

# Delete one or more keys
store.delete("user:123")
store.delete("key1", "key2", "key3")

# Check existence
store.exists("user:123")   # True
store.exists("user:999")   # False
```

### `with_ttl=True` — Get Data and TTL in One Call

On any non-bulk sync method (`set`, `get`, `upsert`, `exists`, `set_if_not_exists`, `get_or_set`, `expire`, `persist`), pass `with_ttl=True` to receive a `(data, seconds_to_live)` tuple instead of just the data.

```python
value, ttl = store.get("user:123", with_ttl=True)
ok, ttl = store.set("user:456", data, ttl=300, with_ttl=True)
ok, ttl = store.exists("user:123", with_ttl=True)
```

### String Atomic Operations

```python
# Store only if key does not exist (SET NX)
store.set_if_not_exists("lock:job:123", "worker-1")   # True
store.set_if_not_exists("lock:job:123", "worker-2")   # False (already exists)

# Get or compute — lazy-loading pattern
result = store.get_or_set("user:456", lambda: expensive_db_query("456"), ttl=300)

# With a static default value (no callable)
config = store.get_or_set("config:features", {"dark_mode": True, "beta": False})

# Non-atomic increment / decrement (GET+SET, approximate counters)
store.increment("page_views:homepage")             # 1
store.increment("page_views:homepage", 5)           # 6
store.decrement("rate_limit:user:42")              # -1
store.decrement("rate_limit:user:42", amount=2)     # -3
```

### String TTL Operations

```python
# Set TTL on existing entry
store.expire("user:123", 7200)   # 2 hours

# Check remaining TTL
store.ttl("user:123")   # seconds remaining (-1 = permanent, -2 = missing)

# Remove TTL (make permanent)
store.persist("user:123")

# with_ttl on TTL methods — returns (bool, seconds) alongside the result
ok, remaining = store.expire("user:123", 3600, with_ttl=True)
ok, ttl = store.persist("user:123", with_ttl=True)

# Bulk expire
store.bulk_expire(["user:123", "user:456"], 3600)
```

### String Bulk Operations

All use **pipelines** for performance.

```python
# Bulk set
store.bulk_set({
    "item:1": {"name": "apple", "qty": 5},
    "item:2": {"name": "banana", "qty": 3},
    "item:3": {"name": "cherry", "qty": 8},
}, ttl=120)

# Bulk get (missing keys return None or your default)
data = store.bulk_get(["item:1", "item:2", "item:3", "item:missing"])
# {"item:1": {...}, "item:2": {...}, "item:3": {...}, "item:missing": None}

# Bulk delete
store.bulk_delete(["item:1", "item:2", "item:3"])
```

### String Pattern Deletion

Delete groups of keys using glob patterns. All use **SCAN** (non-blocking).

```python
# Delete all entries matching a pattern
store.delete_pattern("user:*")          # delete STRING:user:123, STRING:user:456, ...

# Delete by sub-namespace
store.delete_namespace("session")       # delete all STRING:session:* entries

# Delete ALL entries under this prefix (dangerous!)
store.delete_all()
```

### String Inspection & Stats

```python
# Count entries
store.count()                           # total under this prefix
store.count(pattern="user:*")           # count matching pattern

# List keys with pagination
keys = store.list_keys(limit=10)
keys = store.list_keys(pattern="user:*", offset=0, limit=20)

# Redis statistics (from Redis INFO)
stats = store.stats()
# {
#     "used_memory": 1048576,
#     "used_memory_human": "1.00M",
#     "keyspace_hits": 9500,
#     "keyspace_misses": 500,
#     "hit_rate": 0.95,
#     "total_keys": 256,
# }
```

### String Import / Export

```python
# JSON file
store.export_json("/tmp/backup.json")
store.import_json("/tmp/backup.json", overwrite=True)

# JSON string
json_str = store.export_json_string()
store.import_json_string(json_str, overwrite=True)
```

### Decorator — Function Result Memoization

Store any function's return value automatically.

```python
@store.memoize(ttl=300)
def get_user(user_id: str) -> dict:
    return db.query_user(user_id)   # only called on a miss

@store.memoize(ttl=60, key_prefix="api")
def fetch_products(category: str) -> list:
    return api.get_products(category)

# With fallback on Redis errors
@store.memoize(ttl=300, fallback=lambda func, *a, **kw: func(*a, **kw))
def critical_query(id: str) -> dict:
    return db.query(id)

# Manual invalidation
get_user.clear()
```

### String Context Manager & Async

```python
# Context manager — auto-closes connection
with RedisStringUtil(prefix="TEMP") as temp:
    temp.set("key", "value")
    # connection closed on exit

# Async
import asyncio

async def main():
    store = RedisStringUtil(prefix="API")
    await store.async_set("user:123", {"name": "Alice"})
    data = await store.async_get("user:123")
    await store.async_close()

asyncio.run(main())
```

**Available async methods:** `async_set`, `async_get`, `async_upsert`, `async_increment`, `async_decrement`, `async_delete`, `async_exists`, `async_set_if_not_exists`, `async_get_or_set`, `async_bulk_set`, `async_bulk_get`, `async_delete_pattern`, `async_count`, `async_delete_all`, `async_close`.

---

## RedisCache — Caching Layer on RedisStringUtil

A production-ready caching class that wraps `RedisStringUtil`. It instantiates the string util with a dedicated `CACHE` namespace so every cached entry lives under `CACHE:<key>`. All methods accept keys **with or without** the `CACHE` prefix — both resolve to the same entry.

```python
from redis_core_util import RedisCache

cache = RedisCache(
    url="redis://localhost:6379/0",   # Redis/Valkey connection URL
    prefix="CACHE",                    # namespace (defaults to "CACHE")
    default_ttl=None,                  # None = permanent, 600 = auto-expire
)
```

### RedisCache Constructor

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | `"redis://localhost:6379/0"` | Redis/Valkey connection URL |
| `prefix` | `str` | `"CACHE"` | Cache namespace. Keys stored as `{prefix}:{key}` |
| `default_ttl` | `Optional[int]` | `None` | Default TTL in seconds (`None` = permanent) |

### Cache Core Operations

```python
# Set — stored under CACHE:<key>. overwrite=True by default (cache semantics)
cache.set("user:123", {"name": "Alice", "role": "admin"})
cache.set("user:123", {"name": "Alice", "role": "admin"}, ttl=300)

# Get — key may be bare or fully qualified (with or without CACHE prefix)
user = cache.get("user:123")            # {"name": "Alice", ...}
same = cache.get("CACHE:user:123")      # same entry
user = cache.get("user:999", default=None)

# Delete — accepts bare or prefixed keys
cache.delete("user:123")
cache.delete("CACHE:user:123", "user:456")

# Existence check
cache.exists("user:123")   # True
```

### Cache TTL Operations

```python
# Update TTL on an existing entry
cache.expire("user:123", 7200)   # 2 hours

# Check remaining TTL
cache.ttl("user:123")   # seconds remaining (-1 = permanent, -2 = missing)

# Remove TTL (make permanent)
cache.persist("user:123")
```

### Cache-Aside & Atomic Claims

```python
# Cache-aside (lazy-loading) pattern
def expensive_db_query(user_id: str) -> dict:
    """Simulate slow DB call."""
    return {"name": f"User_{user_id}", "computed_at": time.time()}

# First call — cache miss → calls factory → stores result → returns it
result = cache.get_or_set("user:456", lambda: expensive_db_query("456"), ttl=300)

# Second call — cache hit → returns stored value, factory NOT called
result = cache.get_or_set("user:456", lambda: expensive_db_query("456"), ttl=300)

# Atomic claim (SET NX) — first-write-wins
cache.set_if_not_exists("lock:job:123", "worker-1")   # True
cache.set_if_not_exists("lock:job:123", "worker-2")   # False
```

### Cache Bulk Operations

All use **pipelines** for performance.

```python
# Bulk set
cache.bulk_set({
    "item:1": {"name": "apple", "qty": 5},
    "item:2": {"name": "banana", "qty": 3},
    "item:3": {"name": "cherry", "qty": 8},
}, ttl=120)

# Bulk get (missing keys return None or your default)
data = cache.bulk_get(["item:1", "item:2", "item:3", "item:missing"])
# {"item:1": {...}, "item:2": {...}, "item:3": {...}, "item:missing": None}

# Bulk delete
cache.bulk_delete(["item:1", "item:2", "item:3"])
```

### Cache Inspection & Invalidation

```python
# Count entries under the CACHE namespace
cache.count()                           # total
cache.count(pattern="user:*")           # count matching pattern

# List keys (returned without the CACHE prefix)
keys = cache.list_keys(pattern="user:*", limit=10)

# Invalidate groups of entries (SCAN-based, non-blocking)
cache.invalidate("user:*")              # delete CACHE:user:123, CACHE:user:456, ...
cache.invalidate_namespace("session")   # delete all CACHE:session:* entries

# Delete ALL entries under this namespace (dangerous!)
cache.flush()
```

### Cache Context Manager

`RedisCache` is **synchronous only** — all caching operations are blocking and sync.

```python
# Context manager — auto-closes connection
with RedisCache(prefix="TEMP") as temp:
    temp.set("key", "value")
    # connection closed on exit
```


## Use Cases

| Use Case | Class | Prefix | TTL |
|----------|-------|--------|-----|
| User profiles (permanent DB) | `RedisHashUtil` | `USERS:PROFILES` | `None` |
| Session tokens | `RedisCache` | `SESSIONS` | `86400` (24h) |
| OTP codes | `RedisHashUtil` | `USERS:OTP` | `300` (5 min) |
| Password reset tokens | `RedisCache` | `RESET_TOKENS` | `900` (15 min) |
| API response cache | `RedisCache` | `CACHE:API` | `600` (10 min) |
| Rate limiting counters | `RedisHashUtil` | `RATELIMIT:API` | — |
| Feature flags (permanent) | `RedisHashUtil` | `FEATURES` | `None` |
| Job queues | `RedisHashUtil` | `JOBS:PENDING` | — |
| Computed / expensive results | `RedisCache` | `COMPUTED` | `300` (5 min) |
| Config store (long-lived) | `RedisStringUtil` | `CONFIG` | `None` |

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
from redis_core_util import RedisHashUtil, RedisStringUtil, RedisCache

# Hash-based storage
workers = RedisHashUtil(
    url="redis://:supersecretpassword@localhost:6379/0",
    prefix="USERS:WORKERS",
)

# String-based key-value store
store = RedisStringUtil(
    url="redis://:supersecretpassword@localhost:6379/0",
    prefix="API:CONFIG",
    default_ttl=600,
)

# Cache layer
cache = RedisCache(
    url="redis://:supersecretpassword@localhost:6379/0",
    prefix="CACHE",
    default_ttl=600,
)
```

---

## Production Notes

### Shared Across All Classes

- All bulk operations use **pipelines** — no N+1 round trips
- `get_all` / `delete_all` / `list_ids` / `delete_pattern` use **SCAN**, never KEYS (non-blocking)
- `default_ttl` applies automatically — per-call `ttl` parameter overrides it
- All classes support **context manager** pattern for safe connection cleanup
- `RedisHashUtil` and `RedisStringUtil` support full **async** with `asyncio`; `RedisCache` is synchronous-only
- All methods have **type hints** and **docstrings**
- Prefix is **uppercased** automatically for consistency

### RedisHashUtil Specific

- Password hashing uses **bcrypt** with 12 rounds
- Random generation uses **`secrets`** module (cryptographically secure)
- Secondary indexes use Redis **SET** for O(1) membership checks
- Distributed locks use Redis **LOCK** with configurable timeout/blocking
- Supports both **JSON** and **CSV** import/export

### RedisStringUtil Specific

- All values are **JSON-serialized** on write, **deserialized** on read
- `default_ttl=None` means permanent — same behavior as `RedisHashUtil`
- `set_if_not_exists` uses atomic **SET NX** — safe for distributed claim patterns
- `get_or_set` implements lazy-loading of computed values
- `@memoize` decorator stores function return values with automatic key generation
- Long keys (>128 chars) are **SHA-256 hashed** to stay within Redis limits
- `stats()` returns **hit rate**, memory usage, and key counts from Redis `INFO`

### RedisCache Specific

- Thin wrapper over `RedisStringUtil` — every entry lives under a dedicated `CACHE` namespace
- Keys are accepted **with or without** the namespace prefix (both resolve to the same entry)
- `set` defaults to silent overwrite (`overwrite=True`) — natural cache semantics
- `get_or_set` implements the **cache-aside (lazy-loading)** pattern
- `set_if_not_exists` uses atomic **SET NX** for distributed claim patterns
- `invalidate` / `invalidate_namespace` / `flush` provide SCAN-based group invalidation
- Synchronous-only (no async methods) — cache operations are intentionally simple
