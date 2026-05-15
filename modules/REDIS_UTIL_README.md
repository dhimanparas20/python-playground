# RedisHashUtil — Production-Ready Redis/Valkey Hash Utility

A comprehensive, all-in-one Python utility class for Redis (Valkey) hash operations. Eliminates repetitive entity class boilerplate by providing a single, prefix-namespaced interface for CRUD, bulk operations, TTL, indexing, locking, import/export, and secure data hashing.

## Why Use This?

Every project using Redis ends up writing the same patterns: connect, prefix keys, handle bulk ops with pipelines, manage TTLs, build secondary indexes. This class wraps all of that into a clean, production-ready API so you focus on business logic, not Redis plumbing.

**Before:**
```python
r = redis.Redis()
r.hset("USERS:WORKERS:abc-123", mapping={"name": "John", "role": "admin"})
r.hset("USERS:WORKERS:abc-456", mapping={"name": "Jane", "role": "user"})
# ... repeat for every operation, every project
```

**After:**
```python
workers = RedisHashUtil(prefix="USERS:WORKERS")
workers.create({"name": "John", "role": "admin"}, id="abc-123")
workers.create({"name": "Jane", "role": "user"}, id="abc-456")
```

## Installation

```bash
pip install redis bcrypt
```

> For Valkey, the `redis` Python client works natively — Valkey is API-compatible with Redis.

## Quick Start

```python
from redis_hash_util import RedisHashUtil

# Persistent data — no TTL
workers = RedisHashUtil(
    url="redis://localhost:6379/0",
    prefix="USERS:WORKERS",
    index_key="IDX",
    lock_key="LOCK",
    default_ttl=None,  # lives forever
)

# Ephemeral data — auto-expire
otps = RedisHashUtil(prefix="USERS:OTP", default_ttl=300)  # 5 min TTL
sessions = RedisHashUtil(prefix="USERS:SESSIONS", default_ttl=86400)  # 24h TTL
cache = RedisHashUtil(prefix="CACHE:API", default_ttl=600)  # 10 min TTL
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | `"redis://localhost:6379/0"` | Redis/Valkey connection URL |
| `prefix` | `str` | `"DEFAULT"` | Hash key prefix for namespacing |
| `index_key` | `str` | `"IDX"` | Key segment for secondary indexes |
| `lock_key` | `str` | `"LOCK"` | Key segment for distributed locks |
| `default_ttl` | `Optional[int]` | `None` | Default TTL in seconds (applied to all creates/updates) |

## Features

### CRUD Operations

```python
# Create (auto-generates UUID4 if no id)
user_id = workers.create({"username": "johndoe", "email": "john@example.com"})

# Create with explicit id
workers.create({"username": "alice"}, id="u-001")

# Create with TTL override
workers.create({"username": "temp"}, id="u-temp", ttl=3600)

# Read
data = workers.read("u-001")                    # full dict
email = workers.read("u-001", field="email")    # single field

# Update
workers.update("u-001", {"role": "superadmin", "last_login": "2026-01-15"})

# Delete
workers.delete("u-001")                         # delete entire entry
workers.delete_fields("u-001", "last_login")    # delete specific fields

# Check existence
workers.exists("u-001")                         # entry exists?
workers.field_exists("u-001", "email")          # field exists?

# Field info
workers.keys("u-001")                           # list field names
workers.values("u-001")                         # list values
workers.length("u-001")                         # field count

# Atomic increment
workers.increment("u-001", "login_count", amount=1)
workers.increment_float("u-001", "balance", amount=99.99)
```

### Atomic Operations

```python
# Set field only if it doesn't exist (HSETNX)
workers.set_if_not_exists("u-001", "created_at", "2026-01-01")

# Get existing or create new
data = workers.get_or_create("u-001", {"username": "alice", "status": "pending"})
```

### Bulk Operations (Pipeline-based)

```python
# Bulk create
entries = {
    "u-001": {"name": "Alice", "role": "admin"},
    "u-002": {"name": "Bob", "role": "user"},
    "u-003": {"name": "Charlie", "role": "user"},
}
created_ids = workers.bulk_create(entries, overwrite=True)

# Bulk read
data = workers.bulk_read(["u-001", "u-002", "u-003"])

# Bulk update
workers.bulk_update({"u-001": {"status": "inactive"}, "u-002": {"status": "active"}})

# Bulk delete
workers.bulk_delete(["u-001", "u-002"])
```

### Get All / Delete All (SCAN-based, non-blocking)

```python
# Get all entries
all_data = workers.get_all()

# With filtering, sorting, pagination
active_admins = workers.get_all(
    filter_by={"role": "admin", "status": "active"},
    sort_by="username",
    sort_order="asc",
    offset=0,
    limit=10,
)

# Count entries
count = workers.count_all()

# List IDs
ids = workers.list_ids(offset=0, limit=100)

# Delete all (careful!)
workers.delete_all()
workers.delete_all(pattern="u-00")  # delete matching pattern
```

### Copy / Rename

```python
# Copy (auto-id or explicit)
new_id = workers.copy("u-001")                          # auto UUID4
workers.copy("u-001", "u-001-backup")                   # explicit id

# Bulk copy
workers.bulk_copy({"u-001": "u-001-v2", "u-002": "u-002-v2"})

# Rename
workers.rename("u-001", "u-001-renamed")
```

### TTL Operations

```python
# Set TTL
workers.expire("u-001", 3600)  # expire in 1 hour

# Check TTL
remaining = workers.ttl("u-001")  # seconds remaining (-1=permanent, -2=missing)

# Remove TTL (make permanent)
workers.persist("u-001")

# Bulk expire
workers.bulk_expire(["u-001", "u-002"], 3600)
```

### Search

```python
# Exact match (linear scan)
admins = workers.search("role", "admin")

# Substring match
users_with_ali = workers.search("username", "ali", exact=False)

# With full data
admins_data = workers.search_with_data("role", "admin")
```

### Secondary Indexes

```python
# Create index
workers.create_index("u-001", "role")

# Find by index (fast SET lookup)
admin_ids = workers.find_by_index("role", "admin")

# Find with data
admins = workers.find_by_index_with_data("role", "admin")

# Remove index
workers.remove_index("u-001", "role")

# Delete all indexes for a field
workers.delete_index_field("role")
```

### Distributed Locks

```python
lock = workers.acquire_lock("u-001", timeout=10.0, blocking_timeout=5.0)
if lock:
    try:
        # Critical section — only one process can run this
        workers.update("u-001", {"status": "processing"})
    finally:
        workers.release_lock(lock)
```

### Import / Export

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
# Password hashing (bcrypt)
hashed = RedisHashUtil.hash_password("SuperSecret123!")
valid = RedisHashUtil.verify_password("SuperSecret123!", hashed)

# Sensitive data (one-way SHA-256)
email_hash = RedisHashUtil.hash_sensitive_data("user@example.com", pepper="myapp")

# HMAC-based hashing
token_hash = RedisHashUtil.hash_sensitive_data_hmac("sensitive-token", secret="my-secret")
```

### ID Generation (Static Methods)

```python
RedisHashUtil.generate_random_string(32)              # alphanumeric
RedisHashUtil.generate_random_number(6)               # OTP: "482917"
RedisHashUtil.generate_token(64)                       # URL-safe token
RedisHashUtil.generate_uuid4()                         # random UUID
RedisHashUtil.generate_uuid5("myapp.users", "john@x") # deterministic UUID
RedisHashUtil.generate_hash_id({"email": "a@b.com"})   # 16-char dedup hash
```

### Context Manager

```python
with RedisHashUtil(prefix="TEMP") as temp:
    temp.create({"data": "value"}, id="t-001")
    # connection auto-closed on exit
```

### Async Support

```python
import asyncio

async def main():
    users = RedisHashUtil(prefix="USERS")
    uid = await users.async_create({"name": "Alice"}, id="u-001")
    data = await users.async_read("u-001")
    await users.async_close()

asyncio.run(main())
```

## Use Cases

| Use Case | Config |
|----------|--------|
| User profiles | `prefix="USERS:PROFILES", default_ttl=None` |
| Session tokens | `prefix="USERS:SESSIONS", default_ttl=86400` |
| OTP codes | `prefix="USERS:OTP", default_ttl=300` |
| Password reset tokens | `prefix="USERS:RESET_TOKENS", default_ttl=900` |
| API response cache | `prefix="CACHE:API", default_ttl=600` |
| Rate limiting counters | `prefix="RATELIMIT:API"` + `increment()` |
| Feature flags | `prefix="FEATURES", default_ttl=None` |
| Job queues | `prefix="JOBS:PENDING"` |

## Docker Compose — Valkey (Redis) with Persistence

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

### Using with Valkey

```python
from redis_hash_util import RedisHashUtil

workers = RedisHashUtil(
    url="redis://:supersecretpassword@localhost:6379/0",
    prefix="USERS:WORKERS",
)
```

## Production Notes

- All bulk operations use **pipelines** — no N+1 queries
- `get_all` / `delete_all` / `list_ids` use **SCAN**, never KEYS (non-blocking)
- `default_ttl` applies automatically — method-level `ttl` overrides it
- Password hashing uses **bcrypt** with 12 rounds
- Random generation uses **`secrets`** module (cryptographically secure)
- All methods have full **type hints** and **docstrings**
- Context manager ensures connections are closed properly
