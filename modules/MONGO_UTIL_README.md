# MongoUtil — Production-Ready MongoDB Utility

A comprehensive, all-in-one Python utility class for MongoDB CRUD operations. Eliminates repetitive collection boilerplate by providing a clean interface for CRUD, bulk operations, TTL, native indexing, search, import/export, secure data hashing, and database/collection switching.

## Why Use This?

Every project using MongoDB ends up writing the same patterns: connect, pick database, pick collection, handle bulk writes, create indexes, manage TTL indexes. This class wraps all of that into a clean, production-ready API so you focus on business logic, not MongoDB plumbing.

**Before:**
```python
client = MongoClient("mongodb://localhost:27017")
db = client["myapp"]
col = db["users"]
col.insert_one({"_id": "abc-123", "name": "John", "role": "admin"})
col.create_index("role")
col.create_index("_createdAt", expireAfterSeconds=3600)
# ... repeat for every operation, every project
```

**After:**
```python
users = MongoUtil(database="myapp", collection="users")
users.create({"name": "John", "role": "admin"}, id="abc-123")
users.create_index("role")
```

## Installation

```bash
pip install pymongo bcrypt
```

## Quick Start

```python
from mongo_util import MongoUtil

# Persistent data — no TTL
users = MongoUtil(
    connection_string="mongodb://localhost:27017",
    database="myapp",
    collection="users",
    index_key="IDX",
    default_ttl=None,  # lives forever
)

# Ephemeral data — auto-expire
otps = MongoUtil(database="myapp", collection="otps", default_ttl=300)  # 5 min TTL
sessions = MongoUtil(database="myapp", collection="sessions", default_ttl=86400)  # 24h TTL
cache = MongoUtil(database="myapp", collection="cache", default_ttl=600)  # 10 min TTL
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | *required* | Database name |
| `collection` | `str` | *required* | Collection name |
| `connection_string` | `str` | `"mongodb://localhost:27017"` | MongoDB connection URI |
| `index_key` | `str` | `"IDX"` | Prefix for index names |
| `default_ttl` | `Optional[int]` | `None` | Default TTL in seconds (opt-in, adds `_createdAt` + TTL index) |

## Features

### CRUD Operations

```python
# Create (auto-generates ObjectId if no id)
user_id = users.create({"username": "johndoe", "email": "john@example.com"})
# returns: "68271a3f2e8b4c1234567890"

# Create with explicit id
users.create({"username": "alice"}, id="u-001")

# Read
data = users.read("u-001")                                          # full dict
email = users.read("u-001", projection={"email": 1})                # specific fields
no_password = users.read("u-001", projection={"password": 0})       # exclude field

# Update
users.update("u-001", {"role": "superadmin", "last_login": "2026-01-15"})

# Delete
users.delete("u-001")                                               # delete entire document
users.delete_fields("u-001", "last_login", "temp_field")            # delete specific fields

# Check existence
users.exists("u-001")
users.field_exists("u-001", "email")

# Field info
users.keys("u-001")           # list field names (excluding _id)
users.values("u-001")         # list values
users.length("u-001")         # field count

# Atomic increment (MongoDB $inc)
users.increment("u-001", "login_count", amount=1)
users.increment_float("u-001", "balance", amount=99.99)
```

### Atomic Operations

```python
# Set field only if it doesn't exist ($setOnInsert)
users.set_if_not_exists("u-001", "created_at", "2026-01-01")

# Get existing or create new (atomic upsert)
data = users.get_or_create("u-001", {"username": "alice", "status": "pending"})
```

### Bulk Operations

```python
# Bulk create with explicit ids
entries = {
    "u-001": {"name": "Alice", "role": "admin"},
    "u-002": {"name": "Bob", "role": "user"},
}
created_ids = users.bulk_create(entries, overwrite=True)

# Bulk create with auto-generated ObjectIds (list input)
users.bulk_create([
    {"name": "Alice", "role": "admin"},
    {"name": "Bob", "role": "user"},
])

# Bulk read
data = users.bulk_read(["u-001", "u-002", "u-003"])

# Bulk update
users.bulk_update({"u-001": {"status": "inactive"}, "u-002": {"status": "active"}})

# Bulk delete
users.bulk_delete(["u-001", "u-002"])
```

### Get All with Pagination, Sort, Filter

```python
# Simple get all
all_data = users.get_all()

# With MongoDB query filter
active_users = users.get_all(query={"status": "active", "age": {"$gte": 18}})

# With sorting
sorted_users = users.get_all(sort_by="username", sort_order=1)    # 1=ASC, -1=DESC

# With pagination
page1 = users.get_all(sort_by="username", sort_order=1, skip=0, limit=20)
page2 = users.get_all(sort_by="username", sort_order=1, skip=20, limit=20)

# Combined
results = users.get_all(
    query={"role": "admin", "age": {"$gte": 25}},
    sort_by="username",
    sort_order=-1,
    skip=0,
    limit=10,
)

# Count
count = users.count_all()
admin_count = users.count_all(query={"role": "admin"})

# List IDs with pagination
ids = users.list_ids(skip=0, limit=100)
admin_ids = users.list_ids(query={"role": "admin"})

# Delete with filter
users.delete_all(query={"status": "inactive"})
users.delete_all()  # delete everything
```

### Copy / Rename

```python
# Copy (auto-id or explicit)
new_id = users.copy("u-001")                                      # auto ObjectId
users.copy("u-001", "u-001-backup")                               # explicit id

# Bulk copy
users.bulk_copy({"u-001": "u-001-v2", "u-002": "u-002-v2"})

# Rename
users.rename("u-001", "u-001-renamed")
```

### TTL Operations (opt-in via `default_ttl`)

```python
# TTL is automatic when default_ttl is set
otps = MongoUtil(database="myapp", collection="otps", default_ttl=300)
otps.create({"code": "482917", "user_id": "u-001"}, id="otp-001")
# Document auto-deletes after 5 minutes via MongoDB TTL index

# Manually adjust TTL
otps.expire("otp-001", 600)      # extend to 10 minutes
otps.bulk_expire(["otp-001", "otp-002"], 600)

# Remove TTL (make permanent)
otps.persist("otp-001")          # removes _createdAt field
```

### Search

```python
# Exact match
admins = users.search("role", "admin")

# Substring match (case-insensitive regex)
users_with_ali = users.search("username", "ali", exact=False)

# With full data
admins_data = users.search_with_data("role", "admin")

# MongoDB query (powerful)
young_admins = users.search_many({"role": "admin", "age": {"$lt": 35}})
active_admins = users.search_many({"role": "admin", "status": {"$in": ["active", "pending"]}})

# With full data
results = users.search_many_with_data({"role": "admin", "age": {"$gte": 25}})
```

### Native MongoDB Indexes

```python
# Create index
users.create_index("role")                          # ascending
users.create_index("username", unique=True)         # unique
users.create_index("age", descending=True)          # descending

# Find by indexed field
admin_ids = users.find_by_index("role", "admin")
admins = users.find_by_index_with_data("role", "admin")

# List / drop indexes
print(users.list_indexes())
users.drop_index("role")
```

### Database & Collection Switching

All switch methods return a **new instance** — no shared state mutation.

```python
# Switch collection (same database, same connection)
sessions = users.switch_collection("sessions")

# Switch database (same collection, same connection)
other_users = users.switch_database("otherdb")

# Switch both
logs = users.switch(database="analytics", collection="logs")

# All original config (index_key, default_ttl, connection_string) is preserved
```

### Admin Operations

```python
# List all databases on the server
dbs = MongoUtil.list_databases("mongodb://localhost:27017")
# ['admin', 'local', 'myapp', 'analytics']

# Count databases
db_count = MongoUtil.count_databases("mongodb://localhost:27017")

# List collections in current database
collections = users.list_collections()
# ['users', 'sessions', 'otps', 'logs']

# Count collections
col_count = users.count_collections()

# Count documents in current collection
doc_count = users.count_documents_in_collection()
```

### Import / Export

```python
# JSON
users.export_json("/tmp/users.json")
users.import_json("/tmp/users.json", overwrite=True)
json_str = users.export_json_string()

# CSV
users.export_csv("/tmp/users.csv")
users.import_csv("/tmp/users.csv", id_column="_id")
csv_str = users.export_csv_string()
```

### Secure Hashing (Static Methods)

```python
# Password hashing (bcrypt)
hashed = MongoUtil.hash_password("SuperSecret123!")
valid = MongoUtil.verify_password("SuperSecret123!", hashed)

# Sensitive data (one-way SHA-256)
email_hash = MongoUtil.hash_sensitive_data("user@example.com", pepper="myapp")

# HMAC-based hashing
token_hash = MongoUtil.hash_sensitive_data_hmac("sensitive-token", secret="my-secret")
```

### ID Generation (Static Methods)

```python
MongoUtil.generate_random_string(32)                # alphanumeric
MongoUtil.generate_random_number(6)                 # OTP: "482917"
MongoUtil.generate_token(64)                         # URL-safe token
MongoUtil.generate_uuid4()                           # random UUID
MongoUtil.generate_uuid5("myapp.users", "john@x")   # deterministic UUID
MongoUtil.generate_hash_id({"email": "a@b.com"})     # 16-char dedup hash
```

### Context Manager

```python
with MongoUtil(database="myapp", collection="users") as users:
    users.create({"name": "Alice"}, id="u-001")
    # connection auto-closed on exit
```

## Use Cases

| Use Case | Config |
|----------|--------|
| User accounts | `database="myapp", collection="users", default_ttl=None` |
| Session tokens | `database="myapp", collection="sessions", default_ttl=86400` |
| OTP codes | `database="myapp", collection="otps", default_ttl=300` |
| Password reset tokens | `database="myapp", collection="reset_tokens", default_ttl=900` |
| API response cache | `database="myapp", collection="cache", default_ttl=600` |
| Audit logs | `database="logs", collection="audit", default_ttl=2592000` |
| Feature flags | `database="myapp", collection="features", default_ttl=None` |
| Rate limit tracking | `database="myapp", collection="ratelimits", default_ttl=3600` |

## Docker Compose — MongoDB with Persistence

```yaml
version: "3.8"

services:
  mongodb:
    image: mongo:7
    container_name: mongodb
    restart: always
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: ${MONGO_USER:-admin}
      MONGO_INITDB_ROOT_PASSWORD: ${MONGO_PASSWORD:-supersecretpassword}
      MONGO_INITDB_DATABASE: ${MONGO_DATABASE:-myapp}
    volumes:
      - mongodb_data:/data/db
      - mongodb_config:/data/configdb
      - ./mongo-init:/docker-entrypoint-initdb.d  # optional init scripts
    command: >
      mongod
      --auth
      --wiredTigerCacheSizeGB 1
      --wiredTigerJournalCompressor zstd
      --wiredTigerCollectionBlockCompressor zstd
      --oplogSize 512
      --setParameter diagnosticDataCollectionEnabled=false
      --slowOpThresholdMs 200
      --profile 1
      --maxConns 500
      --setParameter maxSessions=1000
      --setParameter transactionLifetimeLimitSeconds=60
      --setParameter cursorTimeoutMillis=300000
      --setParameter maxTransactionLockRequestTimeoutMillis=5000
      --setParameter maxTimeMSForHedgedReads=100
      --setParameter readHedgingMode=on
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 512M
    networks:
      - app_network

  # Optional: MongoDB Express for web-based admin UI
  mongo-express:
    image: mongo-express:1
    container_name: mongo-express
    restart: unless-stopped
    ports:
      - "8081:8081"
    environment:
      ME_CONFIG_MONGODB_ADMINUSERNAME: ${MONGO_USER:-admin}
      ME_CONFIG_MONGODB_ADMINPASSWORD: ${MONGO_PASSWORD:-supersecretpassword}
      ME_CONFIG_MONGODB_URL: mongodb://${MONGO_USER:-admin}:${MONGO_PASSWORD:-supersecretpassword}@mongodb:27017/
      ME_CONFIG_BASICAUTH: "false"
    depends_on:
      mongodb:
        condition: service_healthy
    networks:
      - app_network

volumes:
  mongodb_data:
    driver: local
  mongodb_config:
    driver: local

networks:
  app_network:
    driver: bridge
```

### Using with Docker MongoDB

```python
from mongo_util import MongoUtil

users = MongoUtil(
    connection_string="mongodb://admin:supersecretpassword@localhost:27017",
    database="myapp",
    collection="users",
)
```

## Full Stack Docker Compose (MongoDB + Valkey + App)

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
    healthcheck:
      test: ["CMD", "valkey-cli", "-a", "${VALKEY_PASSWORD:-supersecretpassword}", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - app_network

  mongodb:
    image: mongo:7
    container_name: mongodb
    restart: always
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: ${MONGO_USER:-admin}
      MONGO_INITDB_ROOT_PASSWORD: ${MONGO_PASSWORD:-supersecretpassword}
    volumes:
      - mongodb_data:/data/db
    command: >
      mongod
      --auth
      --wiredTigerCacheSizeGB 1
      --oplogSize 512
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - app_network

  # Your application
  app:
    build: .
    container_name: app
    restart: always
    ports:
      - "8000:8000"
    environment:
      REDIS_URL: "redis://:${VALKEY_PASSWORD:-supersecretpassword}@valkey:6379/0"
      MONGO_URL: "mongodb://${MONGO_USER:-admin}:${MONGO_PASSWORD:-supersecretpassword}@mongodb:27017"
    depends_on:
      valkey:
        condition: service_healthy
      mongodb:
        condition: service_healthy
    networks:
      - app_network

volumes:
  valkey_data:
    driver: local
  mongodb_data:
    driver: local

networks:
  app_network:
    driver: bridge
```

### Using Both Utils Together

```python
from redis_hash_util import RedisHashUtil
from mongo_util import MongoUtil

import os

# MongoDB — persistent storage
users = MongoUtil(
    connection_string=os.getenv("MONGO_URL", "mongodb://localhost:27017"),
    database="myapp",
    collection="users",
)

# Redis — fast cache layer
user_cache = RedisHashUtil(
    url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    prefix="CACHE:USERS",
    default_ttl=600,
)

# Cache-aside pattern
def get_user(user_id: str):
    # Try cache first
    cached = user_cache.read(user_id)
    if cached:
        return cached
    
    # Fall back to MongoDB
    user = users.read(user_id)
    if user:
        user_cache.create(user, id=user_id)
    return user
```

## Production Notes

- `bulk_create` uses `insert_many` / `bulk_write` — **no N+1 queries**
- `get_all` uses MongoDB's native `find()` with `skip()`, `limit()`, `sort()` — **server-side pagination**
- `default_ttl` is opt-in — adds `_createdAt` field + TTL index only when configured
- Indexes are **native MongoDB indexes** (no separate collection hack)
- Password hashing uses **bcrypt** with 12 rounds
- Random generation uses **`secrets`** module (cryptographically secure)
- Switch methods return **new instances** — no shared state mutation
- All methods have full **type hints** and **docstrings**
- Context manager ensures connections are closed properly
- `create()` without `id` lets **MongoDB generate ObjectIds** natively
