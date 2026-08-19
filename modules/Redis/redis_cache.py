"""
Standalone Redis/Valkey cache layer.

``RedisCache`` is a self-contained caching class that talks to Redis STRING
keys directly. It does not wrap or depend on ``RedisStringUtil``.

Every cached entry lives under ``{prefix}:{key}`` (default prefix ``CACHE``).
Callers may pass a bare key (``"user:123"``) or a fully-qualified key
(``"CACHE:user:123"``); both resolve to the same entry.

Production behavior:
    - L1 in-process LRU (hot keys avoid a Redis round trip)
    - Atomic ``SET EX`` (TTL applied in the same command as the write)
    - ``None`` is a cacheable value (``lookup`` / ``get_or_set``)
    - Stampede protection (in-process singleflight + Redis SET NX lock)
    - Per-key locks on set/delete so L1 and Redis stay consistent
    - Hit/miss/error metrics
"""

from __future__ import annotations

import fnmatch
import json
import random
import threading
import time
import urllib.parse
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

try:
    import redis
except ImportError:
    raise ImportError("Install redis: pip install redis")


_REDIS_URL_SCHEMES: tuple = ("redis", "rediss", "unix")
_LOCK_SEGMENT = "__lock__"

# SET data + drop stampede lock so an in-flight rebuild cannot overwrite us.
_LUA_SET_FENCE = """
if ARGV[2] ~= '' then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
else
  redis.call('SET', KEYS[1], ARGV[1])
end
redis.call('DEL', KEYS[2])
return 1
"""

_LUA_SET_NX_FENCE = """
local ok
if ARGV[2] ~= '' then
  ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))
else
  ok = redis.call('SET', KEYS[1], ARGV[1], 'NX')
end
if ok then
  redis.call('DEL', KEYS[2])
  return 1
end
return 0
"""

# Rebuild commit: write only if we still own the stampede lock (token match).
_LUA_COMMIT_REBUILD = """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then
  return 0
end
if ARGV[3] ~= '' then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
else
  redis.call('SET', KEYS[1], ARGV[2])
end
redis.call('DEL', KEYS[2])
return 1
"""

_LUA_DELETE_FENCE = """
local n = redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
return n
"""

_LUA_RELEASE_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

_METRIC_KEYS = (
    "gets",
    "l1_hits",
    "l1_misses",
    "redis_hits",
    "redis_misses",
    "sets",
    "deletes",
    "errors",
    "rebuilds",
    "stampede_locks_acquired",
    "stampede_waited",
    "stampede_lock_timeouts",
)


def validate_redis_connection(url: str, timeout: float = 5.0) -> None:
    """
    Validate a Redis/Valkey connection URL and verify connectivity.

    The URL is first checked for structure (supported scheme and a host),
    then a lightweight ``PING`` probe is issued against the server.

    Args:
        url: Redis connection URL, e.g. ``"redis://localhost:6379/0"``.
        timeout: Socket timeout in seconds for the connectivity probe.
                 Defaults to 5.0.

    Raises:
        ValueError: If the URL is empty, malformed, or uses a scheme other
            than ``redis``, ``rediss``, or ``unix``.
        ConnectionError: If the Redis server cannot be reached or does not
            respond to ``PING``.
    """
    if not isinstance(url, str) or not url.strip():
        raise ValueError("Redis connection URL cannot be empty.")
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError as exc:
        raise ValueError(f"Malformed Redis connection URL: {url!r}") from exc
    if parsed.scheme not in _REDIS_URL_SCHEMES:
        raise ValueError(
            f"Unsupported Redis URL scheme {parsed.scheme!r} in {url!r}. "
            f"Expected one of: {', '.join(_REDIS_URL_SCHEMES)}."
        )
    if parsed.scheme != "unix" and not parsed.hostname:
        raise ValueError(f"Redis connection URL is missing a host: {url!r}")

    probe = redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=timeout,
        socket_timeout=timeout,
    )
    try:
        probe.ping()
    except redis.exceptions.RedisError as exc:
        raise ConnectionError(f"Could not connect to Redis at {url!r}: {exc}") from exc
    finally:
        probe.close()


class _L1Cache:
    """Thread-safe in-process LRU with per-entry TTL. Stores ``None`` as a hit."""

    def __init__(self, maxsize: int, ttl: float) -> None:
        self.maxsize = maxsize
        self.ttl = ttl
        self._lock = threading.RLock()
        self._data: OrderedDict[str, Tuple[Any, float]] = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self.maxsize > 0 and self.ttl > 0

    def get(self, key: str) -> Tuple[bool, Any]:
        if not self.enabled:
            return False, None
        now = time.monotonic()
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return False, None
            value, expires_at = item
            if expires_at <= now:
                del self._data[key]
                return False, None
            self._data.move_to_end(key)
            return True, value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        if not self.enabled:
            return
        lifetime = self.ttl if ttl is None else ttl
        if lifetime <= 0:
            return
        expires_at = time.monotonic() + lifetime
        with self._lock:
            if key in self._data:
                del self._data[key]
            while len(self._data) >= self.maxsize:
                self._data.popitem(last=False)
            self._data[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def delete_matching(self, pattern: str) -> None:
        with self._lock:
            for key in [k for k in self._data if fnmatch.fnmatch(k, pattern)]:
                del self._data[key]

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class RedisCache:
    """
    Production-ready caching layer using Redis STRING keys directly.

    L1 memory + Redis L2, atomic TTL, stampede locks, cacheable ``None``,
    and in-process per-key locks on mutations. Synchronous only.

    The namespace is always applied internally — callers may pass a bare
    key (``"user:123"``) or a fully-qualified key (``"CACHE:user:123"``);
    both resolve to the same entry.

    Attributes:
        url (str): Redis connection URL.
        prefix (str): Cache namespace. Defaults to "CACHE".
        default_ttl (Optional[int]): Default TTL in seconds. None = permanent.

    Example:
        >>> cache = RedisCache(default_ttl=600)
        >>> cache.set("user:123", {"name": "Alice"}, ttl=300)
        >>> cache.get("user:123")
        {"name": "Alice"}
        >>> cache.get("CACHE:user:123")
        {"name": "Alice"}
        >>> cache.set("missing_user", None, ttl=30)
        >>> cache.lookup("missing_user")
        (True, None)
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        prefix: str = "CACHE",
        default_ttl: Optional[int] = None,
        *,
        l1_maxsize: int = 1024,
        l1_ttl: float = 2.0,
        stampede_lock_ttl: int = 30,
        stampede_wait_timeout: float = 1.0,
        stampede_wait_interval: float = 0.05,
        ttl_jitter: float = 0.1,
        lock_stripes: int = 64,
    ) -> None:
        """
        Initialize RedisCache instance.

        Args:
            url: Redis connection URL. Defaults to localhost:6379.
            prefix: Cache namespace. Defaults to "CACHE". All keys are
                    stored under ``{prefix}:{key}``.
            default_ttl: Default TTL in seconds applied to entries when no
                         explicit ``ttl`` is passed. Defaults to None (permanent).
            l1_maxsize: Max entries in the in-process LRU. ``0`` disables L1.
            l1_ttl: L1 lifetime in seconds (capped by Redis TTL when known).
                    ``0`` disables L1. Keep this short so other processes'
                    invalidations are visible quickly.
            stampede_lock_ttl: Seconds a rebuild lock may be held. Must exceed
                               the slowest ``factory`` you pass to ``get_or_set``.
            stampede_wait_timeout: How long waiters block for a rebuild.
            stampede_wait_interval: Sleep between waiter GET retries.
            ttl_jitter: Extra Redis TTL as a fraction of the base (0.1 = +0–10%)
                        so hot keys do not expire in a single wave.
            lock_stripes: Number of in-process locks for set/delete/get_or_set.

        Raises:
            ValueError: If the Redis URL is invalid or a parameter is out of range.
            ConnectionError: If the Redis server cannot be reached.
        """
        if l1_maxsize < 0:
            raise ValueError("l1_maxsize must be >= 0")
        if l1_ttl < 0:
            raise ValueError("l1_ttl must be >= 0")
        if stampede_lock_ttl <= 0:
            raise ValueError("stampede_lock_ttl must be > 0")
        if stampede_wait_timeout < 0:
            raise ValueError("stampede_wait_timeout must be >= 0")
        if stampede_wait_interval <= 0:
            raise ValueError("stampede_wait_interval must be > 0")
        if not 0 <= ttl_jitter <= 1:
            raise ValueError("ttl_jitter must be between 0 and 1")
        if lock_stripes < 1:
            raise ValueError("lock_stripes must be >= 1")

        validate_redis_connection(url)
        self.url: str = url
        self.prefix: str = prefix.upper()
        self.default_ttl: Optional[int] = default_ttl
        self._stampede_lock_ttl = int(stampede_lock_ttl)
        self._stampede_wait_timeout = float(stampede_wait_timeout)
        self._stampede_wait_interval = float(stampede_wait_interval)
        self._ttl_jitter = float(ttl_jitter)
        self._client: redis.Redis = redis.Redis.from_url(
            self.url, decode_responses=True
        )
        self._l1 = _L1Cache(maxsize=l1_maxsize, ttl=l1_ttl)
        self._stripes = [threading.RLock() for _ in range(lock_stripes)]
        self._rebuild_events: Dict[str, threading.Event] = {}
        self._rebuild_events_guard = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._metrics: Dict[str, int] = {k: 0 for k in _METRIC_KEYS}
        self._lua_set_fence = self._client.register_script(_LUA_SET_FENCE)
        self._lua_set_nx_fence = self._client.register_script(_LUA_SET_NX_FENCE)
        self._lua_commit_rebuild = self._client.register_script(_LUA_COMMIT_REBUILD)
        self._lua_delete_fence = self._client.register_script(_LUA_DELETE_FENCE)
        self._lua_release_lock = self._client.register_script(_LUA_RELEASE_LOCK)

    # ──────────────────────────────────────────────
    # INTERNAL HELPERS
    # ──────────────────────────────────────────────

    def _normalize_key(self, key: str) -> str:
        """Strip the namespace prefix from a key if already present."""
        namespace = f"{self.prefix}:"
        return key[len(namespace):] if key.startswith(namespace) else key

    def _key(self, key: str) -> str:
        """Build the full Redis data key from a bare (normalized) key."""
        return f"{self.prefix}:{key}"

    def _lock_key(self, key: str) -> str:
        """Stampede / fence lock for a bare key."""
        return f"{self.prefix}:{_LOCK_SEGMENT}:{key}"

    def _is_lock_key(self, full_key: str) -> bool:
        return full_key.startswith(f"{self.prefix}:{_LOCK_SEGMENT}:")

    def _stripe_index(self, bare: str) -> int:
        return hash(bare) % len(self._stripes)

    @contextmanager
    def _locked_keys(self, *bares: str) -> Generator[None, None, None]:
        """Acquire in-process stripe locks in sorted order (deadlock-safe)."""
        indexes = sorted({self._stripe_index(b) for b in bares if b})
        for i in indexes:
            self._stripes[i].acquire()
        try:
            yield
        finally:
            for i in reversed(indexes):
                self._stripes[i].release()

    def _serialize(self, value: Any) -> str:
        """Serialize a value to a JSON string for storage."""
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    def _deserialize(self, raw: Optional[str]) -> Optional[Any]:
        """Deserialize a JSON string back to a Python object."""
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def _effective_ttl(self, ttl: Optional[int], *, jitter: bool = True) -> Optional[int]:
        """Resolve per-call ttl vs default_ttl, optionally adding expiry jitter."""
        base = ttl if ttl is not None else self.default_ttl
        if base is None or base <= 0:
            return None
        if not jitter or self._ttl_jitter <= 0:
            return int(base)
        extra = random.random() * (base * self._ttl_jitter)
        return max(1, int(base + extra))

    def _ttl_arg(self, ttl_secs: Optional[int]) -> str:
        return str(ttl_secs) if ttl_secs is not None else ""

    def _l1_put(self, bare: str, value: Any, redis_ttl: Optional[int]) -> None:
        if not self._l1.enabled:
            return
        lifetime = self._l1.ttl
        if redis_ttl is not None:
            lifetime = min(lifetime, float(redis_ttl))
        self._l1.set(bare, value, ttl=lifetime)

    def _m(self, name: str, n: int = 1) -> None:
        with self._metrics_lock:
            self._metrics[name] = self._metrics.get(name, 0) + n

    def _redis(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except redis.exceptions.RedisError:
            self._m("errors")
            raise

    def _lookup(self, bare: str, *, count_get: bool = False) -> Tuple[bool, Any]:
        """
        Return ``(found, value)``. ``found=True`` and ``value=None`` means
        ``None`` was cached. ``found=False`` means the key is absent.
        """
        if count_get:
            self._m("gets")
        if self._l1.enabled:
            found, value = self._l1.get(bare)
            if found:
                self._m("l1_hits")
                return True, value
            self._m("l1_misses")
        raw = self._redis(self._client.get, self._key(bare))
        if raw is None:
            self._m("redis_misses")
            return False, None
        self._m("redis_hits")
        value = self._deserialize(raw)
        self._l1_put(bare, value, redis_ttl=None)
        return True, value

    # ──────────────────────────────────────────────
    # CORE OPERATIONS
    # ──────────────────────────────────────────────

    def lookup(self, key: str) -> Tuple[bool, Any]:
        """
        Unambiguous read: distinguish a missing key from a cached ``None``.

        Args:
            key: Cache key. Accepts bare or prefixed forms.

        Returns:
            ``(True, value)`` on a hit (value may be ``None``),
            ``(False, None)`` on a miss.

        Example:
            >>> cache.set("user:999", None, ttl=30)
            >>> cache.lookup("user:999")
            (True, None)
            >>> cache.lookup("never-set")
            (False, None)
        """
        return self._lookup(self._normalize_key(key), count_get=True)

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        overwrite: bool = True,
    ) -> bool:
        """
        Store a value in the cache under ``{prefix}:{key}``.

        TTL is applied in the same Redis ``SET`` as the value (atomic).
        An in-process per-key lock serializes this write with ``delete`` /
        ``get_or_set`` on the same key. The stampede lock is dropped so an
        in-flight rebuild cannot overwrite this write.

        Args:
            key: Cache key. May be bare (``"user:123"``) or already
                 prefixed (``"CACHE:user:123"``).
            value: Value to store. JSON-serialized automatically. ``None``
                   is stored and can be read back via ``lookup`` / ``get_or_set``.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
            overwrite: If False, a ``ValueError`` is raised when the key
                       already exists. Defaults to True (cache semantics).

        Returns:
            True if the value was stored.

        Raises:
            ValueError: If the key exists and ``overwrite=False``.
        """
        bare = self._normalize_key(key)
        ttl_secs = self._effective_ttl(ttl)
        serialized = self._serialize(value)
        data_key = self._key(bare)
        lock_key = self._lock_key(bare)
        ttl_arg = self._ttl_arg(ttl_secs)
        with self._locked_keys(bare):
            if overwrite:
                self._redis(
                    self._lua_set_fence, keys=[data_key, lock_key], args=[serialized, ttl_arg]
                )
            else:
                stored = self._redis(
                    self._lua_set_nx_fence,
                    keys=[data_key, lock_key],
                    args=[serialized, ttl_arg],
                )
                if not stored:
                    raise ValueError(
                        f"Key '{data_key}' already exists. Use overwrite=True to replace."
                    )
            self._l1_put(bare, value, ttl_secs)
            self._m("sets")
        return True

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a cached value by key (L1 then Redis).

        ``None`` is a valid cached value. ``get("k")`` returning ``None`` is
        ambiguous (miss vs cached ``None``). Use ``lookup`` when that matters,
        or pass a unique ``default`` sentinel.

        Args:
            key: Cache key. Accepts ``"user:123"`` or ``"CACHE:user:123"``.
            default: Value returned when the key is missing.

        Returns:
            The cached value (deserialized), or ``default`` on a miss.
        """
        found, value = self._lookup(self._normalize_key(key), count_get=True)
        if not found:
            return default
        return value

    def delete(self, *keys: str) -> int:
        """
        Delete one or more cached entries.

        Takes an in-process lock per key and deletes the data key **and**
        the stampede lock so an in-flight ``get_or_set`` rebuild cannot
        resurrect the entry.

        Args:
            *keys: Cache keys. Accepts bare or prefixed forms.

        Returns:
            Number of data keys actually deleted.
        """
        if not keys:
            return 0
        bares = [self._normalize_key(k) for k in keys]
        deleted = 0
        with self._locked_keys(*bares):
            try:
                pipe = self._client.pipeline(transaction=False)
                for bare in bares:
                    self._lua_delete_fence(
                        keys=[self._key(bare), self._lock_key(bare)],
                        args=[],
                        client=pipe,
                    )
                results = pipe.execute()
            except redis.exceptions.RedisError:
                self._m("errors")
                raise
            for bare, result in zip(bares, results):
                if int(result or 0) > 0:
                    deleted += 1
                self._l1.delete(bare)
            self._m("deletes", deleted)
        return deleted

    def exists(self, key: str) -> bool:
        """
        Check whether a cache entry exists.

        A cached ``None`` counts as existing. L1 is checked first.

        Args:
            key: Cache key. Accepts bare or prefixed forms.

        Returns:
            True if the entry exists, False otherwise.
        """
        bare = self._normalize_key(key)
        if self._l1.enabled:
            found, _ = self._l1.get(bare)
            if found:
                return True
        return bool(self._redis(self._client.exists, self._key(bare)))

    # ──────────────────────────────────────────────
    # TTL OPERATIONS
    # ──────────────────────────────────────────────

    def ttl(self, key: str) -> int:
        """
        Get the remaining TTL of a cache entry (Redis, not L1).

        Args:
            key: Cache key. Accepts bare or prefixed forms.

        Returns:
            Remaining seconds, -1 if no expiry, -2 if the key does not exist.
        """
        return int(self._redis(self._client.ttl, self._key(self._normalize_key(key))))

    def expire(self, key: str, seconds: int) -> bool:
        """
        Update the TTL of an existing cache entry.

        Args:
            key: Cache key. Accepts bare or prefixed forms.
            seconds: New TTL in seconds.

        Returns:
            True if the TTL was set, False if the key does not exist.
        """
        bare = self._normalize_key(key)
        with self._locked_keys(bare):
            ok = bool(self._redis(self._client.expire, self._key(bare), seconds))
            if ok:
                found, value = self._l1.get(bare)
                if found:
                    self._l1_put(bare, value, redis_ttl=seconds)
            return ok

    def persist(self, key: str) -> bool:
        """
        Remove the TTL from a cache entry (make it permanent on Redis).

        Args:
            key: Cache key. Accepts bare or prefixed forms.

        Returns:
            True if the TTL was removed, False otherwise.
        """
        bare = self._normalize_key(key)
        with self._locked_keys(bare):
            return bool(self._redis(self._client.persist, self._key(bare)))

    # ──────────────────────────────────────────────
    # CACHE-SPECIFIC PATTERNS
    # ──────────────────────────────────────────────

    def get_or_set(
        self,
        key: str,
        factory: Union[Callable[[], Any], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """
        Cache-aside with stampede protection. ``None`` from ``factory`` is cached.

        1. L1 then Redis. Hit (including cached ``None``) returns immediately.
        2. In-process stripe lock — only one thread per key rebuilds here.
        3. Redis ``SET NX`` rebuild lock — only one process across pods rebuilds.
        4. Commit via Lua only if we still own the lock (set/delete fence us).
        5. Waiters retry GET until the winner writes, or until wait timeout.

        Args:
            key: Cache key. Accepts bare or prefixed forms.
            factory: Callable producing the value on a miss, or a static value.
            ttl: TTL in seconds for the new entry. Overrides ``default_ttl``.
                 Jitter is applied so clustered expiry is less likely.

        Returns:
            The cached or freshly computed value (may be ``None``).
        """
        bare = self._normalize_key(key)
        found, value = self._lookup(bare, count_get=True)
        if found:
            return value

        token = uuid.uuid4().hex
        lock_key = self._lock_key(bare)
        acquired = False
        rebuild_event: Optional[threading.Event] = None
        with self._locked_keys(bare):
            found, value = self._lookup(bare, count_get=False)
            if found:
                return value
            acquired = bool(
                self._redis(
                    self._client.set,
                    lock_key,
                    token,
                    nx=True,
                    ex=self._stampede_lock_ttl,
                )
            )
            if acquired:
                rebuild_event = threading.Event()
                with self._rebuild_events_guard:
                    self._rebuild_events[bare] = rebuild_event

        # Factory / wait run *without* the stripe lock so other keys on the
        # same stripe are not blocked for the duration of a slow rebuild.
        if acquired:
            self._m("stampede_locks_acquired")
            try:
                value = factory() if callable(factory) else factory
                self._m("rebuilds")
                ttl_secs = self._effective_ttl(ttl)
                committed = self._redis(
                    self._lua_commit_rebuild,
                    keys=[self._key(bare), lock_key],
                    args=[token, self._serialize(value), self._ttl_arg(ttl_secs)],
                )
                if committed:
                    with self._locked_keys(bare):
                        self._l1_put(bare, value, ttl_secs)
                        self._m("sets")
                    return value
                found, current = self._lookup(bare, count_get=False)
                if found:
                    return current
                return value
            except Exception:
                try:
                    self._lua_release_lock(keys=[lock_key], args=[token])
                except redis.exceptions.RedisError:
                    self._m("errors")
                raise
            finally:
                if rebuild_event is not None:
                    rebuild_event.set()
                with self._rebuild_events_guard:
                    self._rebuild_events.pop(bare, None)

        self._m("stampede_waited")
        with self._rebuild_events_guard:
            local_event = self._rebuild_events.get(bare)
        if local_event is not None:
            local_event.wait(timeout=self._stampede_wait_timeout)
            found, value = self._lookup(bare, count_get=False)
            if found:
                return value
        deadline = time.monotonic() + self._stampede_wait_timeout
        while time.monotonic() < deadline:
            time.sleep(self._stampede_wait_interval)
            found, value = self._lookup(bare, count_get=False)
            if found:
                return value
        self._m("stampede_lock_timeouts")
        found, value = self._lookup(bare, count_get=False)
        if found:
            return value
        self._m("rebuilds")
        return factory() if callable(factory) else factory

    def set_if_not_exists(
        self, key: str, value: Any, ttl: Optional[int] = None
    ) -> bool:
        """
        Atomically store a value only if the key is absent (SET NX EX).

        Useful for distributed "first-write-wins" claims. Lease TTL is **not**
        jittered — claim duration should be exact.

        Args:
            key: Cache key. Accepts bare or prefixed forms.
            value: Value to store.
            ttl: TTL in seconds. Overrides ``default_ttl``.

        Returns:
            True if the value was stored, False if the key already exists.
        """
        bare = self._normalize_key(key)
        ttl_secs = self._effective_ttl(ttl, jitter=False)
        serialized = self._serialize(value)
        data_key = self._key(bare)
        with self._locked_keys(bare):
            if ttl_secs is not None:
                result = self._redis(
                    self._client.set, data_key, serialized, nx=True, ex=ttl_secs
                )
            else:
                result = self._redis(self._client.set, data_key, serialized, nx=True)
            if result:
                self._redis(self._client.delete, self._lock_key(bare))
                self._l1_put(bare, value, ttl_secs)
                self._m("sets")
            return bool(result)

    # ──────────────────────────────────────────────
    # BULK OPERATIONS
    # ──────────────────────────────────────────────

    def bulk_set(
        self,
        entries: Dict[str, Any],
        ttl: Optional[int] = None,
        overwrite: bool = True,
    ) -> int:
        """
        Store multiple cache entries in a pipeline with atomic per-key TTL.

        Args:
            entries: Dict mapping keys to values. Keys may be bare or prefixed.
            ttl: TTL in seconds. Overrides ``default_ttl``. Jittered.
            overwrite: If False, existing keys are skipped (SET NX).

        Returns:
            Number of entries stored.
        """
        if not entries:
            return 0
        ttl_secs = self._effective_ttl(ttl)
        items = [(self._normalize_key(k), v) for k, v in entries.items()]
        bares = [b for b, _ in items]
        stored = 0
        with self._locked_keys(*bares):
            try:
                pipe = self._client.pipeline(transaction=False)
                for bare, value in items:
                    full_key = self._key(bare)
                    serialized = self._serialize(value)
                    if overwrite:
                        if ttl_secs is not None:
                            pipe.set(full_key, serialized, ex=ttl_secs)
                        else:
                            pipe.set(full_key, serialized)
                    else:
                        if ttl_secs is not None:
                            pipe.set(full_key, serialized, nx=True, ex=ttl_secs)
                        else:
                            pipe.set(full_key, serialized, nx=True)
                    pipe.delete(self._lock_key(bare))
                results = pipe.execute()
            except redis.exceptions.RedisError:
                self._m("errors")
                raise
            # results are [set, del, set, del, ...]
            for i, (bare, value) in enumerate(items):
                set_result = results[i * 2]
                if overwrite or set_result:
                    stored += 1
                    self._l1_put(bare, value, ttl_secs)
                elif not overwrite:
                    pass
            self._m("sets", stored)
        return stored

    def bulk_get(
        self, keys: List[str], default: Any = None
    ) -> Dict[str, Any]:
        """
        Retrieve multiple cache entries (L1 first, Redis pipeline for the rest).

        Missing keys return ``default``. Cached ``None`` is returned as ``None``,
        which is indistinguishable from ``default=None`` on a miss — use
        ``lookup`` per key if you need to tell them apart.

        Args:
            keys: List of cache keys. Accepts bare or prefixed forms.
            default: Value returned for missing keys.

        Returns:
            Dict mapping **bare** keys to their values.
        """
        if not keys:
            return {}
        normalized = [self._normalize_key(k) for k in keys]
        self._m("gets", len(normalized))
        output: Dict[str, Any] = {}
        missing: List[str] = []
        for bare in normalized:
            if self._l1.enabled:
                found, value = self._l1.get(bare)
                if found:
                    self._m("l1_hits")
                    output[bare] = value
                    continue
                self._m("l1_misses")
            missing.append(bare)
        if not missing:
            return output
        try:
            pipe = self._client.pipeline(transaction=False)
            for bare in missing:
                pipe.get(self._key(bare))
            results = pipe.execute()
        except redis.exceptions.RedisError:
            self._m("errors")
            raise
        for bare, raw in zip(missing, results):
            if raw is None:
                self._m("redis_misses")
                output[bare] = default
            else:
                self._m("redis_hits")
                value = self._deserialize(raw)
                self._l1_put(bare, value, redis_ttl=None)
                output[bare] = value
        return output

    def bulk_delete(self, keys: List[str]) -> int:
        """
        Delete multiple cache entries (data + stampede lock) in a pipeline.

        Args:
            keys: List of cache keys. Accepts bare or prefixed forms.

        Returns:
            Number of data keys actually deleted.
        """
        return self.delete(*keys) if keys else 0

    # ──────────────────────────────────────────────
    # INSPECTION, INVALIDATION, METRICS
    # ──────────────────────────────────────────────

    def metrics(self) -> Dict[str, Any]:
        """
        Snapshot of cache counters for this process (not Redis INFO).

        Returns:
            Dict with raw counters plus ``hit_rate``, ``l1_hit_rate``,
            and ``l1_size``.
        """
        with self._metrics_lock:
            snap = dict(self._metrics)
        gets = snap["gets"]
        l1_hits = snap["l1_hits"]
        redis_hits = snap["redis_hits"]
        l1_lookups = l1_hits + snap["l1_misses"]
        snap["hit_rate"] = round((l1_hits + redis_hits) / gets, 4) if gets else 0.0
        snap["l1_hit_rate"] = round(l1_hits / l1_lookups, 4) if l1_lookups else 0.0
        snap["l1_size"] = len(self._l1)
        snap["l1_enabled"] = self._l1.enabled
        return snap

    def reset_metrics(self) -> None:
        """Reset all counters to zero. Does not touch cached data."""
        with self._metrics_lock:
            for key in self._metrics:
                self._metrics[key] = 0

    def count(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """
        Count cache entries under the namespace (excludes stampede lock keys).

        Args:
            pattern: Optional sub-pattern to match.
            batch_size: SCAN batch size.

        Returns:
            Number of matching data entries.
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        total = 0
        cursor = 0
        while True:
            cursor, found = self._redis(
                self._client.scan, cursor=cursor, match=search, count=batch_size
            )
            total += sum(1 for k in found if not self._is_lock_key(k))
            if cursor == 0:
                break
        return total

    def list_keys(
        self,
        pattern: Optional[str] = None,
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        List cache keys under the namespace with pagination.

        Args:
            pattern: Optional sub-pattern to match.
            offset: Number of keys to skip.
            limit: Maximum keys to return. 0 = no limit.
            batch_size: SCAN batch size.

        Returns:
            List of bare keys (without the namespace prefix). Lock keys omitted.
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        keys_list: List[str] = []
        cursor = 0
        while True:
            cursor, found = self._redis(
                self._client.scan, cursor=cursor, match=search, count=batch_size
            )
            for full_key in found:
                if self._is_lock_key(full_key):
                    continue
                keys_list.append(full_key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
        if offset > 0 or limit > 0:
            end = offset + limit if limit > 0 else None
            return keys_list[offset:end]
        return keys_list

    def invalidate(self, pattern: str, batch_size: int = 1000) -> int:
        """
        Delete all cache entries matching a glob pattern (SCAN).

        Also drops matching L1 entries and companion stampede locks.

        Args:
            pattern: Glob pattern (e.g., ``"user:*"``).
            batch_size: SCAN batch size.

        Returns:
            Number of data entries deleted.
        """
        search = f"{self.prefix}:{pattern}"
        deleted = 0
        cursor = 0
        while True:
            cursor, found = self._redis(
                self._client.scan, cursor=cursor, match=search, count=batch_size
            )
            data_keys = [k for k in found if not self._is_lock_key(k)]
            lock_keys = [k for k in found if self._is_lock_key(k)]
            if data_keys or lock_keys:
                try:
                    pipe = self._client.pipeline(transaction=False)
                    for full_key in data_keys:
                        bare = full_key.removeprefix(f"{self.prefix}:")
                        pipe.delete(full_key)
                        pipe.delete(self._lock_key(bare))
                        self._l1.delete(bare)
                    for full_key in lock_keys:
                        pipe.delete(full_key)
                    results = pipe.execute()
                except redis.exceptions.RedisError:
                    self._m("errors")
                    raise
                # data_keys contribute two pipeline results each (data + lock)
                for i in range(len(data_keys)):
                    if int(results[i * 2] or 0) > 0:
                        deleted += 1
            if cursor == 0:
                break
        self._l1.delete_matching(pattern)
        self._m("deletes", deleted)
        return deleted

    def invalidate_namespace(self, namespace: str, batch_size: int = 1000) -> int:
        """
        Delete all cache entries under a sub-namespace.

        Args:
            namespace: Sub-namespace (e.g., "session" clears ``CACHE:session:*``).
            batch_size: SCAN batch size.

        Returns:
            Number of entries deleted.
        """
        return self.invalidate(f"{namespace}*", batch_size=batch_size)

    def flush(self, batch_size: int = 1000) -> int:
        """
        Delete ALL cache entries under this namespace (dangerous).

        Args:
            batch_size: SCAN batch size.

        Returns:
            Number of data entries deleted.
        """
        deleted = self.invalidate("*", batch_size=batch_size)
        self._l1.clear()
        return deleted

    def close(self) -> None:
        """Close the Redis connection and drop L1."""
        self._l1.clear()
        self._client.close()

    # ──────────────────────────────────────────────
    # DUNDER METHODS
    # ──────────────────────────────────────────────

    def __repr__(self) -> str:
        """String representation of RedisCache."""
        return (
            f"RedisCache(url='{self.url}', prefix='{self.prefix}', "
            f"default_ttl={self.default_ttl}, l1_maxsize={self._l1.maxsize}, "
            f"l1_ttl={self._l1.ttl})"
        )

    def __enter__(self) -> "RedisCache":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit — close connections."""
        self.close()
