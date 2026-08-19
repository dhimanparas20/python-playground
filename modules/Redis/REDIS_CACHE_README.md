# RedisCache — Standalone Caching Layer

A self-contained Redis/Valkey cache for Python. Redis **STRING** keys, JSON values, L1 memory, atomic TTL, stampede-safe cache-aside, cacheable `None`, per-key locks on set/delete, and process-local metrics.

This file (`redis_cache.py`) does **not** wrap `RedisStringUtil`. STRING commands live inside each method.

> There is also a `RedisCache` in `redis_core_util.py`. That one delegates to `RedisStringUtil` and does **not** include L1 / stampede / metrics. This README is for the standalone class in `redis_cache.py`.

## Table of Contents

- [When to Use This](#when-to-use-this)
- [Installation](#installation)
- [Constructor](#constructor)
- [How Keys Work](#how-keys-work)
- [L1 In-Process Cache](#l1-in-process-cache)
- [Type Preservation & Caching None](#type-preservation--caching-none)
- [Core Operations](#core-operations)
- [Locks on Set and Delete](#locks-on-set-and-delete)
- [TTL Operations](#ttl-operations)
- [Cache-Aside & Stampede Protection](#cache-aside--stampede-protection)
- [Atomic Claims](#atomic-claims)
- [Bulk Operations](#bulk-operations)
- [Inspection & Invalidation](#inspection--invalidation)
- [Metrics](#metrics)
- [Context Manager](#context-manager)
- [Use Cases](#use-cases)
- [Connecting with Password](#connecting-with-password)
- [Method Reference](#method-reference)
- [Production Notes](#production-notes)

---

## When to Use This

| Need | Use |
|------|-----|
| Cache API responses / computed values | `RedisCache` |
| Session tokens, reset tokens, short-lived blobs | `RedisCache` |
| First-writer-wins job claims (`SET NX`) | `RedisCache.set_if_not_exists` |
| Structured entities with field queries | `RedisHashUtil` (core util) |
| Permanent KV / config store | `RedisStringUtil` (core util) |

**Rule of thumb:** if the data is meant to expire or be invalidated as a group, use this class. If you need field-level CRUD or secondary indexes, use `RedisHashUtil`.

---

## Installation

```bash
pip install redis
```

> The `redis` Python client works with Valkey — Valkey is API-compatible with Redis.

```python
from redis_cache import RedisCache
```

---

## Constructor

```python
from redis_cache import RedisCache

cache = RedisCache(
    url="redis://localhost:6379/0",
    prefix="CACHE",
    default_ttl=600,
    l1_maxsize=1024,          # in-process LRU; 0 disables L1
    l1_ttl=2.0,               # seconds in L1 (keep short)
    stampede_lock_ttl=30,     # rebuild lock must outlast factory()
    stampede_wait_timeout=1.0,
    stampede_wait_interval=0.05,
    ttl_jitter=0.1,           # +0–10% on Redis TTL
    lock_stripes=64,          # in-process locks for set/delete/get_or_set
)
```

Connection is validated at init: URL scheme check (`redis` / `rediss` / `unix`) plus a `PING`. Invalid URLs and unreachable servers fail immediately, not on the first `get`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | `"redis://localhost:6379/0"` | Redis/Valkey connection URL |
| `prefix` | `str` | `"CACHE"` | Cache namespace. Uppercased. Keys stored as `{prefix}:{key}` |
| `default_ttl` | `Optional[int]` | `None` | Default TTL in seconds (`None` = permanent) |
| `l1_maxsize` | `int` | `1024` | Max L1 entries. `0` disables L1 |
| `l1_ttl` | `float` | `2.0` | L1 lifetime in seconds (capped by Redis TTL when known). `0` disables L1 |
| `stampede_lock_ttl` | `int` | `30` | Seconds a rebuild lock may be held |
| `stampede_wait_timeout` | `float` | `1.0` | How long waiters wait for a rebuild |
| `stampede_wait_interval` | `float` | `0.05` | Sleep between waiter GET retries (cross-process) |
| `ttl_jitter` | `float` | `0.1` | Extra Redis TTL as a fraction of the base (`0` = none) |
| `lock_stripes` | `int` | `64` | In-process stripe locks for mutations |

```python
# Ephemeral cache — Redis entries auto-expire after 10 minutes
cache = RedisCache(default_ttl=600)

# Disable L1 (every get hits Redis)
cache = RedisCache(default_ttl=600, l1_maxsize=0)

# Named namespace
sessions = RedisCache(prefix="SESSIONS", default_ttl=86400)

# TLS
cache = RedisCache(url="rediss://user:pass@redis.example.com:6379/0")
```

---

## How Keys Work

Every entry is stored as a Redis STRING under `{PREFIX}:{key}`.

```
set("user:123")        →  CACHE:user:123
get("user:123")        →  CACHE:user:123
get("CACHE:user:123")  →  CACHE:user:123   (same entry)
```

Stampede locks live beside the data, not in the same slot:

```
CACHE:user:123              data
CACHE:__lock__:user:123     rebuild lock (internal)
```

`count` / `list_keys` skip `__lock__` keys. `set` / `delete` / `invalidate` drop them automatically.

Prefix is **uppercased** (`"cache"` becomes `"CACHE"`). Bare and fully-qualified keys always hit the same data slot.

---

## L1 In-Process Cache

Hot keys are served from a thread-safe LRU in this process. A hit does **not** talk to Redis.

```
get("user:123")
    → L1 hit?  return (no network)
    → L1 miss  → Redis GET → fill L1 → return
```

- Default: 1024 entries, **2 second** TTL
- L1 TTL is `min(l1_ttl, redis_ttl)` when Redis TTL is known (on write)
- Keep `l1_ttl` short so a delete from another pod is visible quickly
- `set` / `delete` / `invalidate` / `flush` update or drop L1 immediately in **this** process
- Disable with `l1_maxsize=0` or `l1_ttl=0`

```python
cache = RedisCache(l1_maxsize=4096, l1_ttl=1.0)
```

---

## Type Preservation & Caching None

Values are JSON-serialized on write and deserialized on read. `int`, `float`, `bool`, `None`, `str`, `list`, and `dict` round-trip. Plain strings are stored as-is.

`None` is a **real cached value**. A Redis miss and a stored `None` are not the same thing.

```python
cache.set("user:999", None, ttl=30)

cache.lookup("user:999")    # (True, None)  — hit, value is None
cache.lookup("never-set")   # (False, None) — miss

# get() is convenient but ambiguous when default is None
cache.get("user:999")       # None (cached)
cache.get("never-set")      # None (default)

# Unambiguous get: pass a sentinel
MISSING = object()
value = cache.get("never-set", default=MISSING)
if value is MISSING:
    print("miss")
```

`get_or_set` uses the same `found` flag, so a factory that returns `None` is stored and **not** re-run on the next call.

```python
def find_user(uid: str):
    return db.get(uid)  # may be None

cache.get_or_set("u:42", lambda: find_user("42"), ttl=30)
cache.get_or_set("u:42", lambda: find_user("42"), ttl=30)  # factory not called
```

---

## Core Operations

`set` defaults to **overwrite=True** (cache semantics). Pass `overwrite=False` to fail if the key already exists.

TTL is applied in the **same** Redis `SET` (`SET … EX`). There is no separate `EXPIRE` on the write path.

```python
cache.set("user:123", {"name": "Alice", "role": "admin"})
cache.set("user:123", {"name": "Alice", "role": "admin"}, ttl=300)

user = cache.get("user:123")
same = cache.get("CACHE:user:123")      # same entry
user = cache.get("user:999", default=None)

found, value = cache.lookup("user:123")

cache.delete("user:123")
cache.delete("CACHE:user:123", "user:456")

cache.exists("user:123")   # True / False — cached None counts as existing
```

```python
cache.set("config:theme", "dark", overwrite=False)
cache.set("config:theme", "light", overwrite=False)
# ValueError: Key 'CACHE:config:theme' already exists.
```

---

## Locks on Set and Delete

Mutations take an **in-process stripe lock** (sorted by stripe index so multi-key deletes cannot deadlock) so L1 and Redis stay consistent in this process.

`set` and `delete` also **fence** in-flight `get_or_set` rebuilds:

| Method | What the lock does |
|--------|--------------------|
| `set` | Stripe lock + Lua `SET` (with `EX`) + `DEL` stampede lock. A rebuild that still holds the old lock **cannot** overwrite this write. |
| `delete` | Stripe lock + Lua `DEL` data **and** stampede lock. A rebuild that lost the lock will not resurrect the key. |
| `get_or_set` | Stripe lock only to acquire the Redis rebuild lock; factory runs unlocked. Commit is Lua: write **only if** the lock token still matches. |

They do **not** wait on another process's rebuild lock (that would stall writes for the whole factory). They invalidate it instead.

---

## TTL Operations

Per-call `ttl` overrides `default_ttl`. Writes use `SET EX` in one command. Redis TTL conventions: remaining seconds, `-1` = no expiry, `-2` = key missing.

`set` / `get_or_set` / `bulk_set` add **jitter** (default +0–10%) so a wave of keys does not expire at the same instant. `set_if_not_exists` does **not** jitter — claim leases stay exact.

```python
cache.set("session:abc", {"token": "xyz"}, ttl=300)

cache.ttl("session:abc")          # Redis remaining seconds (not L1)
cache.expire("session:abc", 7200)
cache.persist("session:abc")
cache.ttl("session:abc")          # -1
```

```python
cache = RedisCache(default_ttl=600)
cache.set("api:feed", payload)          # ~10 min + jitter
cache.set("api:feed", payload, ttl=60)  # ~60s + jitter
```

---

## Cache-Aside & Stampede Protection

`get_or_set` is cache-aside **with** stampede control:

1. L1, then Redis. A hit (including cached `None`) returns immediately.
2. In-process stripe lock — only one thread tries to take the rebuild lock.
3. Redis `SET NX` rebuild lock — only one process across pods runs `factory`.
4. Same-process waiters block on a `threading.Event`; other processes poll GET.
5. Commit via Lua **only if** we still own the lock (`set`/`delete` can fence us).
6. If the wait times out, waiters compute locally and **do not** overwrite Redis (avoids fighting the lock holder).

```python
def expensive_db_query(user_id: str) -> dict:
    return {"name": f"User_{user_id}", "source": "db"}

# Miss → one factory call (even under concurrency) → store → return
result = cache.get_or_set("user:456", lambda: expensive_db_query("456"), ttl=300)

# Hit → factory NOT called
result = cache.get_or_set("user:456", lambda: expensive_db_query("456"), ttl=300)

config = cache.get_or_set("flags", {"dark_mode": True, "beta": False}, ttl=3600)
```

Set `stampede_lock_ttl` longer than your slowest `factory`. If the lock expires mid-rebuild, another worker may compute too.

---

## Atomic Claims

`set_if_not_exists` uses atomic `SET NX EX`. Safe for distributed job claims. Lease TTL is not jittered.

```python
cache.set_if_not_exists("lock:job:123", "worker-1")   # True
cache.set_if_not_exists("lock:job:123", "worker-2")   # False — already claimed
```

```python
if cache.set_if_not_exists("job:42", worker_id, ttl=30):
    try:
        process(job)
    finally:
        cache.delete("job:42")
```

---

## Bulk Operations

Bulk writes use **pipelines** with `SET … EX` per key (no second EXPIRE pass). `bulk_get` checks L1 first and pipelines only the misses.

```python
cache.bulk_set({
    "item:1": {"name": "apple", "qty": 5},
    "item:2": {"name": "banana", "qty": 3},
    "item:3": {"name": "cherry", "qty": 8},
}, ttl=120)

data = cache.bulk_get(["item:1", "item:2", "item:3", "item:missing"])
# {"item:1": {...}, "item:2": {...}, "item:3": {...}, "item:missing": None}

deleted = cache.bulk_delete(["item:1", "item:2", "item:3"])
# 3
```

Keys may be bare or prefixed. `bulk_get` returns **bare** keys. `overwrite=False` uses `SET NX` (existing keys are skipped, not raised).

---

## Inspection & Invalidation

Count / list / invalidate use **SCAN** — never `KEYS`. Invalidation also drops matching L1 entries and companion stampede locks.

```python
cache.count()
cache.count(pattern="user:*")

keys = cache.list_keys(limit=10)
keys = cache.list_keys(pattern="user:*", offset=0, limit=20)

cache.invalidate("user:*")
cache.invalidate_namespace("session")
cache.flush()   # entire prefix + L1
```

---

## Metrics

Counters are **per process**, not Redis `INFO`. Use them for dashboards or logs.

```python
stats = cache.metrics()
# {
#   "gets": 1000,
#   "l1_hits": 800,
#   "l1_misses": 200,
#   "redis_hits": 150,
#   "redis_misses": 50,
#   "sets": 40,
#   "deletes": 10,
#   "errors": 0,
#   "rebuilds": 50,
#   "stampede_locks_acquired": 50,
#   "stampede_waited": 12,
#   "stampede_lock_timeouts": 0,
#   "hit_rate": 0.95,        # (l1_hits + redis_hits) / gets
#   "l1_hit_rate": 0.80,     # l1_hits / (l1_hits + l1_misses)
#   "l1_size": 120,
#   "l1_enabled": True,
# }

cache.reset_metrics()
```

`errors` increments on Redis exceptions; the exception is still **raised** (this cache does not fail-open).

---

## Context Manager

`RedisCache` is **synchronous only**. Use a context manager so the connection closes on exit (L1 is cleared too).

```python
with RedisCache(prefix="TEMP", default_ttl=60) as temp:
    temp.set("key", "value")
    print(temp.get("key"))
```

```python
cache = RedisCache()
try:
    cache.set("k", "v")
finally:
    cache.close()
```

---

## Use Cases

| Use case | Prefix | TTL | Pattern |
|----------|--------|-----|---------|
| API response cache | `API` | `600` (10 min) | `get_or_set` |
| Session tokens | `SESSIONS` | `86400` (24h) | `set` + `ttl` |
| Password reset tokens | `RESET_TOKENS` | `900` (15 min) | `set` + `get` + `delete` |
| OTP / magic links | `OTP` | `300` (5 min) | `set` + `get` |
| Computed / expensive results | `COMPUTED` | `300` (5 min) | `get_or_set` |
| Negative lookup ("user not found") | `USERS` | `30` | `get_or_set` returning `None` |
| Feature-flag snapshot | `FLAGS` | `60` | `get_or_set` |
| Job / lock claims | `LOCKS` | `30` | `set_if_not_exists` |
| Per-user fragment cache | `FRAGMENTS` | `120` | `invalidate("user:42:*")` |

### 1. API response cache

```python
from redis_cache import RedisCache

api_cache = RedisCache(prefix="API", default_ttl=600)

def get_feed(user_id: str) -> dict:
    return api_cache.get_or_set(
        f"feed:{user_id}",
        lambda: db.fetch_feed(user_id),
        ttl=600,
    )
```

### 2. Session store

```python
sessions = RedisCache(prefix="SESSIONS", default_ttl=86400)

sessions.set(session_id, {
    "user_id": "u-001",
    "role": "admin",
    "ip": request.ip,
})

found, session = sessions.lookup(session_id)
if not found:
    raise Unauthorized()

sessions.delete(session_id)
```

### 3. Password reset token

```python
import secrets

tokens = RedisCache(prefix="RESET_TOKENS", default_ttl=900)

raw = secrets.token_urlsafe(32)
tokens.set(raw, {"user_id": user.id, "email": user.email}, ttl=900)

found, payload = tokens.lookup(submitted)
if not found:
    raise InvalidOrExpiredToken()
tokens.delete(submitted)
```

### 4. Invalidate a user's cached fragments

```python
fragments = RedisCache(prefix="FRAGMENTS", default_ttl=120)

fragments.set(f"user:{uid}:header", header_html)
fragments.set(f"user:{uid}:sidebar", sidebar_html)

fragments.invalidate(f"user:{uid}:*")
```

### 5. Distributed job claim

```python
locks = RedisCache(prefix="LOCKS")

def run_once(job_id: str, worker_id: str) -> bool:
    claimed = locks.set_if_not_exists(job_id, worker_id, ttl=30)
    if not claimed:
        return False
    try:
        do_work(job_id)
        return True
    finally:
        locks.delete(job_id)
```

### 6. Warm a catalog in bulk

```python
catalog = RedisCache(prefix="CATALOG", default_ttl=3600)

catalog.bulk_set({
    f"product:{p.id}": p.to_dict()
    for p in products
}, ttl=3600)

page = catalog.bulk_get([f"product:{i}" for i in ids], default=None)
```

---

## Connecting with Password

```python
from redis_cache import RedisCache

cache = RedisCache(
    url="redis://:supersecretpassword@localhost:6379/0",
    prefix="CACHE",
    default_ttl=600,
)
```

TLS:

```python
cache = RedisCache(
    url="rediss://:supersecretpassword@redis.example.com:6379/0",
    prefix="CACHE",
)
```

---

## Method Reference

| Method | What it does | Redis |
|--------|----------------|-------|
| `lookup(key)` | `(found, value)` — `None` is a real hit | L1 + `GET` |
| `set(key, value, ttl=None, overwrite=True)` | Store a value (fences rebuilds) | Lua `SET EX` + `DEL` lock |
| `get(key, default=None)` | Read a value | L1 + `GET` |
| `delete(*keys)` | Delete data + stampede lock | Lua `DEL` |
| `exists(key)` | Existence check (cached `None` is True) | L1 + `EXISTS` |
| `ttl(key)` | Remaining Redis TTL (`-1` / `-2`) | `TTL` |
| `expire(key, seconds)` | Set / refresh Redis TTL | `EXPIRE` |
| `persist(key)` | Remove Redis TTL | `PERSIST` |
| `get_or_set(key, factory, ttl=None)` | Stampede-safe cache-aside | `GET` + `SET NX` lock + Lua commit |
| `set_if_not_exists(key, value, ttl=None)` | Atomic claim | `SET NX EX` |
| `bulk_set(...)` | Pipeline write with `EX` | `SET EX` × N |
| `bulk_get(...)` | L1 then pipeline read | `GET` × misses |
| `bulk_delete(keys)` | Same as `delete(*keys)` | Lua `DEL` |
| `metrics()` / `reset_metrics()` | Process-local counters | — |
| `count(pattern=None)` | Count data keys (skips locks) | `SCAN` |
| `list_keys(...)` | List bare keys | `SCAN` |
| `invalidate(pattern)` | Delete by glob + L1 | `SCAN` + `DEL` |
| `invalidate_namespace(namespace)` | Delete a sub-namespace | `SCAN` + `DEL` |
| `flush()` | Delete entire prefix + L1 | `SCAN` + `DEL` |
| `close()` | Close client, clear L1 | — |

---

## Production Notes

- **L1** — hot keys skip Redis; default 2s so remote invalidation is not stale for long
- **Atomic TTL** — `SET EX` in one command (Lua on `set`; pipeline `SET ex=` on `bulk_set`)
- **TTL jitter** — default +0–10% on cache writes so hot keys do not expire together
- **`None` is cacheable** — use `lookup` or `get_or_set`; `get()` stays convenient
- **Stampede** — in-process Event + Redis `SET NX` rebuild lock + Lua token check
- **Set/delete locks** — in-process stripes + fence (`DEL` rebuild lock) so a late rebuild cannot clobber a write or undelete
- **Metrics** — `gets`, L1/Redis hits, sets, deletes, errors, stampede counters, hit rates
- Fail-fast connect: URL validation + `PING`
- Prefix uppercased; bare and prefixed keys resolve to the same data entry
- `count` / `list_keys` / `invalidate` / `flush` use **SCAN**, never `KEYS`
- Redis errors increment `errors` and **propagate** (no fail-open)
- Synchronous only
- Independent of `redis_core_util.py`

### Still not in this class (by design)

- Fail-open when Redis is down (callers must catch and fall back)
- Redis Cluster hash tags (Lua uses two keys; fine on standalone / Sentinel)
- Cross-process L1 invalidation (pub/sub) — keep `l1_ttl` short instead
- Async API

### vs `RedisCache` in `redis_core_util.py`

| | `redis_cache.py` | `redis_core_util.py` |
|--|------------------|----------------------|
| Storage | Redis STRING, inlined | Wraps `RedisStringUtil` |
| L1 / stampede / metrics / `lookup` | Yes | No |
| Extra clients | One sync client | String util creates sync **and** async |
| Import | `from redis_cache import RedisCache` | `from redis_core_util import RedisCache` |
