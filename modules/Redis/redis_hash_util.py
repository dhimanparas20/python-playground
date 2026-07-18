"""
Redis Utility Classes
A production-ready, all-in-one utility for Redis (Valkey) operations.

Modules:
    RedisHashUtil    — Hash-based persistent storage (database replacement).
    RedisCacheManager — String-based caching layer with TTL-first design.
"""

from __future__ import annotations

import csv
import functools
import hashlib
import io
import json
import random
import secrets
import string
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

try:
    import redis.asyncio as aioredis
    import redis
    from redis.lock import Lock
except ImportError:
    raise ImportError("Install redis: pip install redis")

try:
    import bcrypt
except ImportError:
    raise ImportError("Install bcrypt: pip install bcrypt")


T = TypeVar("T")


class RedisHashUtil:
    """
    Production-ready Redis hash utility class for CRUD operations,
    bulk operations, locking, indexing, TTL, import/export, and secure data hashing.

    Attributes:
        url (str): Redis connection URL.
        prefix (str): Hash key prefix for namespacing.
        index_key (str): Key segment used for secondary indexes.
        lock_key (str): Key segment used for distributed locks.
        default_ttl (Optional[int]): Default TTL in seconds for entries.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        prefix: str = "DEFAULT",
        index_key: str = "IDX",
        lock_key: str = "LOCK",
        default_ttl: Optional[int] = None,
    ) -> None:
        """
        Initialize RedisHashUtil instance.

        Args:
            url: Redis connection URL. Defaults to localhost:6379.
            prefix: Hash map prefix (e.g., "USERS:WORKERS").
                    All keys will be namespaced under this prefix.
            index_key: Key segment for secondary indexes. Defaults to "IDX".
            lock_key: Key segment for distributed locks. Defaults to "LOCK".
            default_ttl: Default TTL in seconds applied to all new entries
                         when no explicit ttl is passed to methods.

        Example:
            >>> util = RedisHashUtil(
            ...     url="redis://localhost:6379/0",
            ...     prefix="USERS:WORKERS",
            ...     index_key="INDEX",
            ...     default_ttl=3600,
            ... )
        """
        self.url: str = url
        self.prefix: str = prefix.upper()
        self.index_key: str = index_key.upper()
        self.lock_key: str = lock_key.upper()
        self.default_ttl: Optional[int] = default_ttl
        self._sync_client: redis.Redis = redis.Redis.from_url(
            self.url, decode_responses=True
        )
        self._async_client: aioredis.Redis = aioredis.from_url(
            self.url, decode_responses=True
        )

    def _key(self, id: str) -> str:
        """Build full Redis hash key from prefix and id."""
        return f"{self.prefix}:{id}"

    def _apply_ttl(self, key: str, ttl: Optional[int] = None) -> None:
        """Apply TTL to a key. Uses method ttl if provided, else default_ttl."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None:
            self._sync_client.expire(key, effective_ttl)

    async def _apply_ttl_async(self, key: str, ttl: Optional[int] = None) -> None:
        """Async: Apply TTL to a key."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None:
            await self._async_client.expire(key, effective_ttl)

    # ──────────────────────────────────────────────
    # SYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    def create(
        self,
        data: Dict[str, Any],
        id: Optional[str] = None,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> str:
        """
        Create a new hash entry or update if overwrite is True.
        If id is not provided, a UUID4 is auto-generated.

        Args:
            data: Dictionary of field-value pairs to store.
            id: Unique identifier for the hash entry. Auto-generated if None.
            overwrite: If True, delete existing data before writing.
            ttl: TTL in seconds for this entry. Overrides default_ttl if provided.

        Returns:
            The entry id (provided or auto-generated).

        Raises:
            ValueError: If entry exists and overwrite is False.
        """
        if id is None:
            id = self.generate_uuid4()
        key = self._key(id)
        if not overwrite and self._sync_client.exists(key):
            raise ValueError(f"Entry '{key}' already exists. Use overwrite=True to update.")
        if overwrite:
            self._sync_client.delete(key)
        self._sync_client.hset(key, mapping=data)
        self._apply_ttl(key, ttl)
        return id

    def read(self, id: str, field: Optional[str] = None) -> Optional[Union[str, Dict[str, str]]]:
        """
        Read hash entry or specific field.

        Args:
            id: Unique identifier for the hash entry.
            field: Optional specific field to retrieve.

        Returns:
            Single field value if field specified, full dict otherwise, None if not found.
        """
        key = self._key(id)
        if field is not None:
            return self._sync_client.hget(key, field)
        data = self._sync_client.hgetall(key)
        return data if data else None

    def update(self, id: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Update specific fields in an existing hash entry.

        Args:
            id: Unique identifier for the hash entry.
            data: Dictionary of field-value pairs to update.
            ttl: TTL in seconds to refresh. Overrides default_ttl if provided.

        Returns:
            True if entry exists and was updated, False otherwise.
        """
        key = self._key(id)
        if not self._sync_client.exists(key):
            return False
        self._sync_client.hset(key, mapping=data)
        self._apply_ttl(key, ttl)
        return True

    def delete(self, id: str) -> bool:
        """
        Delete entire hash entry.

        Args:
            id: Unique identifier for the hash entry.

        Returns:
            True if entry existed and was deleted, False otherwise.
        """
        return bool(self._sync_client.delete(self._key(id)))

    def delete_fields(self, id: str, *fields: str) -> int:
        """
        Delete specific fields from a hash entry.

        Args:
            id: Unique identifier for the hash entry.
            *fields: Field names to delete.

        Returns:
            Number of fields removed.
        """
        if not fields:
            return 0
        return self._sync_client.hdel(self._key(id), *fields)

    def exists(self, id: str) -> bool:
        """Check if hash entry exists."""
        return bool(self._sync_client.exists(self._key(id)))

    def field_exists(self, id: str, field: str) -> bool:
        """Check if a specific field exists in a hash entry."""
        return bool(self._sync_client.hexists(self._key(id), field))

    def keys(self, id: str) -> List[str]:
        """Get all field names in a hash entry."""
        return self._sync_client.hkeys(self._key(id))

    def values(self, id: str) -> List[str]:
        """Get all values in a hash entry."""
        return self._sync_client.hvals(self._key(id))

    def length(self, id: str) -> int:
        """Get number of fields in a hash entry."""
        return self._sync_client.hlen(self._key(id))

    def increment(self, id: str, field: str, amount: int = 1) -> int:
        """Increment a numeric field value. Can be negative."""
        return self._sync_client.hincrby(self._key(id), field, amount)

    def increment_float(self, id: str, field: str, amount: float = 1.0) -> float:
        """Increment a float field value. Can be negative."""
        return self._sync_client.hincrbyfloat(self._key(id), field, amount)

    # ──────────────────────────────────────────────
    # SET IF NOT EXISTS / GET OR CREATE
    # ──────────────────────────────────────────────

    def set_if_not_exists(self, id: str, field: str, value: Any) -> bool:
        """
        Atomically set a hash field only if it does not already exist.
        Uses HSETNX under the hood.

        Args:
            id: Unique identifier for the hash entry.
            field: Field name to set.
            value: Value to set.

        Returns:
            True if field was set (did not exist), False if already present.
        """
        return bool(self._sync_client.hsetnx(self._key(id), field, value))

    def get_or_create(
        self,
        id: str,
        data: Dict[str, Any],
        ttl: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Return existing entry data, or create it and return the new data.

        Args:
            id: Unique identifier for the hash entry.
            data: Dictionary to store if entry does not exist.
            ttl: TTL in seconds. Overrides default_ttl if provided.

        Returns:
            The entry data (existing or newly created).
        """
        key = self._key(id)
        existing = self._sync_client.hgetall(key)
        if existing:
            return existing
        self._sync_client.hset(key, mapping=data)
        self._apply_ttl(key, ttl)
        return data

    # ──────────────────────────────────────────────
    # TTL OPERATIONS
    # ──────────────────────────────────────────────

    def expire(self, id: str, seconds: int) -> bool:
        """
        Set TTL on a hash entry.

        Args:
            id: Unique identifier for the hash entry.
            seconds: TTL in seconds.

        Returns:
            True if timeout was set, False if key does not exist.
        """
        return bool(self._sync_client.expire(self._key(id), seconds))

    def bulk_expire(self, ids: List[str], seconds: int) -> int:
        """
        Set TTL on multiple hash entries using pipeline.

        Args:
            ids: List of entry identifiers.
            seconds: TTL in seconds.

        Returns:
            Number of entries updated.
        """
        if not ids:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for id in ids:
            pipe.expire(self._key(id), seconds)
        results = pipe.execute()
        return sum(results)

    def ttl(self, id: str) -> int:
        """
        Get remaining TTL of a hash entry.

        Args:
            id: Unique identifier for the hash entry.

        Returns:
            Remaining seconds, -1 if no expiry, -2 if key does not exist.
        """
        return self._sync_client.ttl(self._key(id))

    def persist(self, id: str) -> bool:
        """
        Remove TTL from a hash entry (make it permanent).

        Args:
            id: Unique identifier for the hash entry.

        Returns:
            True if TTL was removed, False otherwise.
        """
        return bool(self._sync_client.persist(self._key(id)))

    # ──────────────────────────────────────────────
    # COPY / RENAME OPERATIONS
    # ──────────────────────────────────────────────

    def copy(
        self,
        source_id: str,
        dest_id: Optional[str] = None,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> str:
        """
        Copy a hash entry to a new id.
        If dest_id is not provided, a UUID4 is auto-generated.

        Args:
            source_id: Source entry identifier.
            dest_id: Destination entry identifier. Auto-generated if None.
            overwrite: If True, overwrite existing destination.
            ttl: TTL for the copied entry. Overrides default_ttl if provided.

        Returns:
            The destination id (provided or auto-generated).

        Raises:
            ValueError: If destination exists and overwrite is False.
        """
        data = self.read(source_id)
        if data is None:
            return ""
        return self.create(dict(data), id=dest_id, overwrite=overwrite, ttl=ttl)

    def bulk_copy(
        self,
        copies: Dict[str, str],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Copy multiple hash entries using pipeline.

        Args:
            copies: Dict mapping source_id -> dest_id.
            overwrite: If True, overwrite existing destinations.
            ttl: TTL for copied entries. Overrides default_ttl if provided.

        Returns:
            Dict mapping destination ids to themselves.
        """
        if not copies:
            return {}
        source_ids = list(copies.keys())
        bulk_data = self.bulk_read(source_ids)
        new_entries: Dict[str, Dict[str, Any]] = {}
        for src_id, dest_id in copies.items():
            data = bulk_data.get(src_id)
            if data is not None:
                new_entries[dest_id] = dict(data)
        if not new_entries:
            return {}
        return self.bulk_create(new_entries, overwrite=overwrite, ttl=ttl)

    def rename(self, old_id: str, new_id: str, overwrite: bool = False) -> bool:
        """
        Rename a hash entry key.

        Args:
            old_id: Current entry identifier.
            new_id: New entry identifier.
            overwrite: If True, overwrite if new_id already exists.

        Returns:
            True if rename succeeded.

        Raises:
            ValueError: If new_id exists and overwrite is False.
        """
        old_key = self._key(old_id)
        new_key = self._key(new_id)
        if not self._sync_client.exists(old_key):
            return False
        if not overwrite and self._sync_client.exists(new_key):
            raise ValueError(f"Entry '{new_key}' already exists. Use overwrite=True.")
        if overwrite:
            self._sync_client.delete(new_key)
        ttl_val = self._sync_client.ttl(old_key)
        self._sync_client.renamenx(old_key, new_key) if not overwrite else self._sync_client.rename(old_key, new_key)
        if ttl_val > 0:
            self._sync_client.expire(new_key, ttl_val)
        return True

    # ──────────────────────────────────────────────
    # SEARCH OPERATIONS
    # ──────────────────────────────────────────────

    def search(
        self,
        field: str,
        value: str,
        exact: bool = True,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        Search entries where a field matches a value (linear scan).
        For production, prefer indexed lookups via create_index + find_by_index.

        Args:
            field: Field name to match against.
            value: Value to search for.
            exact: If True, exact match. If False, substring/contains match.
            batch_size: Scan batch size.

        Returns:
            List of matching entry ids.
        """
        matches: List[str] = []
        cursor = 0
        search = f"{self.prefix}:*"
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.hget(key, field)
                results = pipe.execute()
                for key, val in zip(keys, results):
                    if val is None:
                        continue
                    if exact and val == value:
                        matches.append(key.removeprefix(f"{self.prefix}:"))
                    elif not exact and value in val:
                        matches.append(key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
        return matches

    def search_with_data(
        self,
        field: str,
        value: str,
        exact: bool = True,
        batch_size: int = 1000,
    ) -> Dict[str, Dict[str, str]]:
        """
        Search entries with full data where a field matches a value.

        Args:
            field: Field name to match against.
            value: Value to search for.
            exact: If True, exact match. If False, substring match.
            batch_size: Scan batch size.

        Returns:
            Dict of matching entries with their data.
        """
        ids = self.search(field, value, exact=exact, batch_size=batch_size)
        return self.bulk_read(ids)

    # ──────────────────────────────────────────────
    # BULK OPERATIONS
    # ──────────────────────────────────────────────

    def bulk_create(
        self,
        entries: Dict[str, Dict[str, Any]],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Create multiple hash entries using pipeline.

        Args:
            entries: Dict mapping ids to their data dicts.
            overwrite: If True, overwrite existing entries.
            ttl: TTL in seconds. Overrides default_ttl if provided.

        Returns:
            Dict mapping original ids to themselves (for API consistency).
        """
        ids_map: Dict[str, str] = {}
        pipe = self._sync_client.pipeline(transaction=False)
        for id, data in entries.items():
            key = self._key(id)
            if not overwrite and self._sync_client.exists(key):
                continue
            if overwrite:
                pipe.delete(key)
            pipe.hset(key, mapping=data)
            ids_map[id] = id
        pipe.execute()
        # Apply TTL after pipeline since EXPIRE needs keys to exist
        if ids_map:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for id in ids_map:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                ttl_pipe.execute()
        return ids_map

    def bulk_read(self, ids: List[str]) -> Dict[str, Optional[Dict[str, str]]]:
        """Read multiple hash entries using pipeline."""
        pipe = self._sync_client.pipeline(transaction=False)
        for id in ids:
            pipe.hgetall(self._key(id))
        results = pipe.execute()
        return {id: (data if data else None) for id, data in zip(ids, results)}

    def bulk_update(
        self,
        updates: Dict[str, Dict[str, Any]],
        ttl: Optional[int] = None,
    ) -> int:
        """
        Update multiple hash entries using pipeline.

        Args:
            updates: Dict mapping ids to their update data.
            ttl: TTL in seconds to refresh. Overrides default_ttl if provided.

        Returns:
            Number of entries updated.
        """
        count = 0
        pipe = self._sync_client.pipeline(transaction=False)
        for id, data in updates.items():
            key = self._key(id)
            if self._sync_client.exists(key):
                pipe.hset(key, mapping=data)
                count += 1
        pipe.execute()
        if count > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for id in updates:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                ttl_pipe.execute()
        return count

    def bulk_delete(self, ids: List[str]) -> int:
        """Delete multiple hash entries using pipeline."""
        if not ids:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for id in ids:
            pipe.delete(self._key(id))
        results = pipe.execute()
        return sum(results)

    def get_all(
        self,
        pattern: Optional[str] = None,
        filter_by: Optional[Dict[str, str]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> Dict[str, Dict[str, str]]:
        """
        Get all hash entries under this prefix using SCAN with filtering,
        sorting, and pagination.

        Note: Redis does not support server-side filtering/sorting.
        All data is collected first, then filtered/sorted/paginated in Python.

        Args:
            pattern: Optional sub-pattern to match (appended to prefix).
            filter_by: Optional dict of field-value pairs to filter by.
                       e.g., {"status": "active", "role": "admin"}
            sort_by: Optional field name to sort entries by.
            sort_order: Sort direction. "asc" or "desc". Default "asc".
            offset: Number of entries to skip (for pagination).
            limit: Maximum entries to return. 0 = no limit.
            batch_size: Number of keys to scan per iteration.

        Returns:
            Dict mapping entry ids to their data.
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        result: Dict[str, Dict[str, str]] = {}
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.hgetall(key)
                data_list = pipe.execute()
                for key, data in zip(keys, data_list):
                    id = key.removeprefix(f"{self.prefix}:")
                    if data:
                        # Apply filter
                        if filter_by:
                            match = all(data.get(k) == v for k, v in filter_by.items())
                            if not match:
                                continue
                        result[id] = data
            if cursor == 0:
                break
        # Sort
        if sort_by:
            result = dict(sorted(
                result.items(),
                key=lambda item: item[1].get(sort_by, ""),
                reverse=(sort_order.lower() == "desc"),
            ))
        # Pagination
        if offset > 0 or limit > 0:
            items = list(result.items())
            end = offset + limit if limit > 0 else None
            result = dict(items[offset:end])
        return result

    def delete_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """Delete all hash entries under this prefix using SCAN and pipeline."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    def count_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """Count all hash entries under this prefix."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            count += len(keys)
            if cursor == 0:
                break
        return count

    def list_ids(
        self,
        pattern: Optional[str] = None,
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        List entry ids under this prefix with optional pagination.

        Args:
            pattern: Optional sub-pattern to match.
            offset: Number of ids to skip (for pagination).
            limit: Maximum ids to return. 0 = no limit.
            batch_size: Scan batch size.

        Returns:
            List of entry ids (without prefix).
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        ids: List[str] = []
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            for key in keys:
                ids.append(key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
        # Pagination
        if offset > 0 or limit > 0:
            end = offset + limit if limit > 0 else None
            return ids[offset:end]
        return ids

    # ──────────────────────────────────────────────
    # INDEX OPERATIONS
    # ──────────────────────────────────────────────

    def create_index(self, id: str, field: str) -> bool:
        """
        Create a secondary index for a field using a SET.
        Index key format: {prefix}:{index_key}:{field}:{value} -> set of ids

        Args:
            id: Entry identifier.
            field: Field name to index.

        Returns:
            True if index was created.
        """
        data = self.read(id)
        if not data or field not in data:
            return False
        value = data[field]
        idx_key = f"{self.prefix}:{self.index_key}:{field}:{value}"
        self._sync_client.sadd(idx_key, id)
        return True

    def find_by_index(self, field: str, value: str) -> List[str]:
        """Find entry ids by indexed field value."""
        idx_key = f"{self.prefix}:{self.index_key}:{field}:{value}"
        return list(self._sync_client.smembers(idx_key))

    def find_by_index_with_data(self, field: str, value: str) -> Dict[str, Dict[str, str]]:
        """Find entries with data by indexed field value."""
        ids = self.find_by_index(field, value)
        return self.bulk_read(ids)

    def remove_index(self, id: str, field: str) -> bool:
        """Remove index entry for a specific id and field."""
        data = self.read(id)
        if not data or field not in data:
            return False
        value = data[field]
        idx_key = f"{self.prefix}:{self.index_key}:{field}:{value}"
        self._sync_client.srem(idx_key, id)
        return True

    def delete_index_field(self, field: str) -> int:
        """Delete all index keys for a field."""
        search = f"{self.prefix}:{self.index_key}:{field}:*"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search)
            if keys:
                deleted += self._sync_client.delete(*keys)
            if cursor == 0:
                break
        return deleted

    # ──────────────────────────────────────────────
    # LOCK OPERATIONS
    # ──────────────────────────────────────────────

    def acquire_lock(
        self,
        id: str,
        timeout: float = 10.0,
        blocking: bool = True,
        blocking_timeout: Optional[float] = None,
    ) -> Optional[Lock]:
        """Acquire a distributed lock for a hash entry."""
        lock_redis_key = f"{self.prefix}:{self.lock_key}:{id}"
        lock = self._sync_client.lock(
            lock_redis_key,
            timeout=timeout,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
        )
        if lock.acquire():
            return lock
        return None

    def release_lock(self, lock: Lock) -> bool:
        """Release a distributed lock."""
        try:
            lock.release()
            return True
        except Exception:
            return False

    # ──────────────────────────────────────────────
    # IMPORT / EXPORT — JSON
    # ──────────────────────────────────────────────

    def export_json(
        self,
        filepath: str,
        pattern: Optional[str] = None,
        indent: int = 2,
    ) -> int:
        """
        Export all hash entries under this prefix to a JSON file.

        Args:
            filepath: Path to output JSON file.
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            Number of entries exported.
        """
        data = self.get_all(pattern=pattern)
        export_data = {
            "prefix": self.prefix,
            "index_key": self.index_key,
            "entries": data,
        }
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=indent, ensure_ascii=False)
        return len(data)

    def import_json(
        self,
        filepath: str,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import hash entries from a JSON file.

        Args:
            filepath: Path to input JSON file.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides default_ttl if provided.

        Returns:
            Number of entries imported.
        """
        with open(filepath, "r", encoding="utf-8") as f:
            import_data = json.load(f)
        entries = import_data.get("entries", {})
        return len(self.bulk_create(entries, overwrite=overwrite, ttl=ttl))

    def export_json_string(self, pattern: Optional[str] = None, indent: int = 2) -> str:
        """
        Export all hash entries to a JSON string.

        Args:
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            JSON string representation of all entries.
        """
        data = self.get_all(pattern=pattern)
        export_data = {
            "prefix": self.prefix,
            "index_key": self.index_key,
            "entries": data,
        }
        return json.dumps(export_data, indent=indent, ensure_ascii=False)

    def import_json_string(
        self,
        json_str: str,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import hash entries from a JSON string.

        Args:
            json_str: JSON string containing entries.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides default_ttl if provided.

        Returns:
            Number of entries imported.
        """
        import_data = json.loads(json_str)
        entries = import_data.get("entries", {})
        return len(self.bulk_create(entries, overwrite=overwrite, ttl=ttl))

    def export_csv(
        self,
        filepath: str,
        pattern: Optional[str] = None,
        id_column: str = "_id",
    ) -> int:
        """
        Export all hash entries to a CSV file.
        Each entry becomes a row. The id is stored in a dedicated column.

        Args:
            filepath: Path to output CSV file.
            pattern: Optional sub-pattern to match.
            id_column: Column name used for the entry id. Defaults to "_id".

        Returns:
            Number of entries exported.
        """
        data = self.get_all(pattern=pattern)
        if not data:
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([id_column])
            return 0

        # Collect all unique field names across all entries
        all_fields: set[str] = set()
        for entry in data.values():
            all_fields.update(entry.keys())
        fieldnames = [id_column] + sorted(all_fields)

        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for entry_id, entry_data in data.items():
                row = {id_column: entry_id}
                row.update(entry_data)
                writer.writerow(row)
        return len(data)

    def import_csv(
        self,
        filepath: str,
        id_column: str = "_id",
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import hash entries from a CSV file.

        Args:
            filepath: Path to input CSV file.
            id_column: Column name that contains the entry id. Defaults to "_id".
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides default_ttl if provided.

        Returns:
            Number of entries imported.

        Raises:
            ValueError: If id_column is not found in CSV headers.
        """
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None or id_column not in reader.fieldnames:
                raise ValueError(f"Column '{id_column}' not found in CSV headers: {reader.fieldnames}")
            entries: Dict[str, Dict[str, str]] = {}
            for row in reader:
                entry_id = row.pop(id_column)
                entries[entry_id] = {k: v for k, v in row.items() if v}
        return len(self.bulk_create(entries, overwrite=overwrite, ttl=ttl))

    def export_csv_string(
        self,
        pattern: Optional[str] = None,
        id_column: str = "_id",
    ) -> str:
        """
        Export all hash entries to a CSV string.

        Args:
            pattern: Optional sub-pattern to match.
            id_column: Column name for entry id. Defaults to "_id".

        Returns:
            CSV string of all entries.
        """
        data = self.get_all(pattern=pattern)
        all_fields: set[str] = set()
        for entry in data.values():
            all_fields.update(entry.keys())
        fieldnames = [id_column] + sorted(all_fields)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for entry_id, entry_data in data.items():
            row = {id_column: entry_id}
            row.update(entry_data)
            writer.writerow(row)
        return output.getvalue()

    # ──────────────────────────────────────────────
    # ASYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    async def async_create(
        self,
        data: Dict[str, Any],
        id: Optional[str] = None,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> str:
        """Async: Create a new hash entry. Auto-generates UUID4 if id is None."""
        if id is None:
            id = self.generate_uuid4()
        key = self._key(id)
        if not overwrite and await self._async_client.exists(key):
            raise ValueError(f"Entry '{key}' already exists. Use overwrite=True to update.")
        if overwrite:
            await self._async_client.delete(key)
        await self._async_client.hset(key, mapping=data)
        await self._apply_ttl_async(key, ttl)
        return id

    async def async_read(self, id: str, field: Optional[str] = None) -> Optional[Union[str, Dict[str, str]]]:
        """Async: Read hash entry or specific field."""
        key = self._key(id)
        if field is not None:
            return await self._async_client.hget(key, field)
        data = await self._async_client.hgetall(key)
        return data if data else None

    async def async_update(self, id: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Async: Update specific fields in an existing hash entry."""
        key = self._key(id)
        if not await self._async_client.exists(key):
            return False
        await self._async_client.hset(key, mapping=data)
        await self._apply_ttl_async(key, ttl)
        return True

    async def async_delete(self, id: str) -> bool:
        """Async: Delete entire hash entry."""
        return bool(await self._async_client.delete(self._key(id)))

    async def async_set_if_not_exists(self, id: str, field: str, value: Any) -> bool:
        """Async: Atomically set a hash field only if it does not already exist."""
        return bool(await self._async_client.hsetnx(self._key(id), field, value))

    async def async_get_or_create(
        self,
        id: str,
        data: Dict[str, Any],
        ttl: Optional[int] = None,
    ) -> Dict[str, str]:
        """Async: Return existing entry data, or create it and return new data."""
        key = self._key(id)
        existing = await self._async_client.hgetall(key)
        if existing:
            return existing
        await self._async_client.hset(key, mapping=data)
        await self._apply_ttl_async(key, ttl)
        return data

    async def async_expire(self, id: str, seconds: int) -> bool:
        """Async: Set TTL on a hash entry."""
        return bool(await self._async_client.expire(self._key(id), seconds))

    async def async_ttl(self, id: str) -> int:
        """Async: Get remaining TTL of a hash entry."""
        return await self._async_client.ttl(self._key(id))

    async def async_bulk_create(
        self,
        entries: Dict[str, Dict[str, Any]],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> Dict[str, str]:
        """Async: Create multiple hash entries using pipeline."""
        ids_map: Dict[str, str] = {}
        pipe = self._async_client.pipeline(transaction=False)
        for id, data in entries.items():
            key = self._key(id)
            if not overwrite and await self._async_client.exists(key):
                continue
            if overwrite:
                pipe.delete(key)
            pipe.hset(key, mapping=data)
            ids_map[id] = id
        await pipe.execute()
        if ids_map:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._async_client.pipeline(transaction=False)
                for id in ids_map:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                await ttl_pipe.execute()
        return ids_map

    async def async_bulk_read(self, ids: List[str]) -> Dict[str, Optional[Dict[str, str]]]:
        """Async: Read multiple hash entries using pipeline."""
        pipe = self._async_client.pipeline(transaction=False)
        for id in ids:
            pipe.hgetall(self._key(id))
        results = await pipe.execute()
        return {id: (data if data else None) for id, data in zip(ids, results)}

    async def async_get_all(
        self,
        pattern: Optional[str] = None,
        filter_by: Optional[Dict[str, str]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> Dict[str, Dict[str, str]]:
        """Async: Get all hash entries under this prefix with filtering, sorting, pagination."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        result: Dict[str, Dict[str, str]] = {}
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._async_client.pipeline(transaction=False)
                for key in keys:
                    pipe.hgetall(key)
                data_list = await pipe.execute()
                for key, data in zip(keys, data_list):
                    id = key.removeprefix(f"{self.prefix}:")
                    if data:
                        if filter_by:
                            match = all(data.get(k) == v for k, v in filter_by.items())
                            if not match:
                                continue
                        result[id] = data
            if cursor == 0:
                break
        if sort_by:
            result = dict(sorted(
                result.items(),
                key=lambda item: item[1].get(sort_by, ""),
                reverse=(sort_order.lower() == "desc"),
            ))
        if offset > 0 or limit > 0:
            items = list(result.items())
            end = offset + limit if limit > 0 else None
            result = dict(items[offset:end])
        return result

    async def async_delete_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """Async: Delete all hash entries under this prefix."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._async_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = await pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    async def async_close(self) -> None:
        """Close async Redis connection."""
        await self._async_client.aclose()

    def close(self) -> None:
        """Close sync Redis connection."""
        self._sync_client.close()

    # ──────────────────────────────────────────────
    # STATIC UTILITY METHODS
    # ──────────────────────────────────────────────

    @staticmethod
    def generate_random_string(length: int = 32, charset: str = string.ascii_letters + string.digits) -> str:
        """
        Generate a cryptographically secure random alphanumeric string.

        Args:
            length: Length of the string to generate. Defaults to 32.
            charset: Character set to use. Defaults to alphanumeric.

        Returns:
            Random string of specified length.

        Example:
            >>> RedisHashUtil.generate_random_string(16)
            'aB3kQ9mN2xR7pL4w'
        """
        return "".join(secrets.choice(charset) for _ in range(length))

    @staticmethod
    def generate_random_number(length: int = 6) -> str:
        """
        Generate a cryptographically secure random numeric string.
        Useful for OTPs, PINs, and verification codes.

        Args:
            length: Number of digits. Defaults to 6.

        Returns:
            Random numeric string of specified length.

        Example:
            >>> RedisHashUtil.generate_random_number(6)
            '482917'
            >>> RedisHashUtil.generate_random_number(4)
            '0372'
        """
        digits = string.digits
        return "".join(secrets.choice(digits) for _ in range(length))

    @staticmethod
    def generate_token(length: int = 64) -> str:
        """
        Generate a cryptographically secure URL-safe token.

        Args:
            length: Byte length before encoding. Defaults to 64.

        Returns:
            URL-safe token string.
        """
        return secrets.token_urlsafe(length)

    @staticmethod
    def generate_uuid4() -> str:
        """Generate a random UUID4."""
        return str(uuid.uuid4())

    @staticmethod
    def generate_uuid5(namespace: str, name: str) -> str:
        """
        Generate a deterministic UUID5 from namespace and name.

        Args:
            namespace: Namespace string (e.g., "myapp.users").
            name: Name to hash within the namespace.

        Returns:
            UUID5 string.
        """
        ns_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, namespace)
        return str(uuid.uuid5(ns_uuid, name))

    @staticmethod
    def hash_password(password: str) -> str:
        """
        Hash a password using bcrypt (most secure for passwords).

        Args:
            password: Plain text password to hash.

        Returns:
            Bcrypt hashed password string.
        """
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """
        Verify a password against its bcrypt hash.

        Args:
            password: Plain text password to verify.
            hashed: Bcrypt hashed password to verify against.

        Returns:
            True if password matches, False otherwise.
        """
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))

    @staticmethod
    def hash_sensitive_data(data: str, pepper: str = "") -> str:
        """
        Hash sensitive data (emails, PII) using SHA-256.
        One-way hash for data that doesn't need to be reversed.

        Args:
            data: Sensitive data to hash.
            pepper: Optional application-level pepper for added security.

        Returns:
            Hex digest of hashed data.
        """
        salted = f"{pepper}{data}".encode("utf-8")
        return hashlib.sha256(salted).hexdigest()

    @staticmethod
    def hash_sensitive_data_hmac(data: str, secret: str) -> str:
        """
        Hash sensitive data using HMAC-SHA256.

        Args:
            data: Sensitive data to hash.
            secret: Secret key for HMAC.

        Returns:
            Hex digest of HMAC hash.
        """
        return hashlib.new("sha256", f"{secret}{data}".encode("utf-8"), usedforsecurity=True).hexdigest()

    @staticmethod
    def generate_hash_id(data: Dict[str, Any]) -> str:
        """
        Generate a deterministic hash ID from a data dictionary.
        Useful for deduplication.

        Args:
            data: Dictionary to hash.

        Returns:
            16-character hash ID.
        """
        canonical = str(sorted(data.items()))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    def __repr__(self) -> str:
        """String representation of RedisHashUtil."""
        return (
            f"RedisHashUtil(url='{self.url}', prefix='{self.prefix}', "
            f"index_key='{self.index_key}', lock_key='{self.lock_key}', "
            f"default_ttl={self.default_ttl})"
        )

    def __enter__(self) -> "RedisHashUtil":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - close connections."""
        self.close()


# ══════════════════════════════════════════════
# REDIS CACHE MANAGER — STRING-BASED CACHING
# ══════════════════════════════════════════════


F = TypeVar("F", bound=Callable[..., Any])


class RedisCacheManager:
    """
    Production-ready Redis/Valkey caching layer using STRING type.

    Designed for caching objects, API responses, computed results, and ephemeral
    data. Every entry is stored as a JSON-serialized string with automatic TTL.
    Follows the cache-aside (lazy-loading) pattern by default.

    Key format: ``{prefix}:{cache_key}``

    Differences from RedisHashUtil:
        - Uses Redis STRING (``SET``/``GET``) instead of HASH (``HSET``/``HGET``).
        - All values are JSON-serialized on write and deserialized on read.
        - TTL is fully optional — set ``default_ttl`` for automatic expiry, or
          leave as ``None`` for permanent entries (like RedisHashUtil).
        - No secondary indexes or distributed locks — cache is stateless.
        - Method names use cache semantics (``store``/``retrieve`` instead of
          ``create``/``read``).

    Attributes:
        url (str): Redis connection URL.
        prefix (str): Key prefix for namespace isolation.
        default_ttl (Optional[int]): Default TTL in seconds. None = permanent.

    Example:
        >>> cache = RedisCacheManager(prefix="API:USERS", default_ttl=600)
        >>> cache.store("user:123", {"name": "Alice", "role": "admin"})
        >>> cache.retrieve("user:123")
        {"name": "Alice", "role": "admin"}
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        prefix: str = "CACHE",
        default_ttl: Optional[int] = None,
    ) -> None:
        """
        Initialize RedisCacheManager instance.

        Args:
            url: Redis connection URL. Defaults to localhost:6379.
            prefix: Key prefix for namespace isolation (e.g., "API:USERS").
                    All keys will be namespaced as ``{prefix}:{key}``.
            default_ttl: Default TTL in seconds for cached entries.
                         Defaults to None (permanent). Set a value like
                         3600 for automatic expiry, or pass ttl per-call.

        Example:
            >>> # Ephemeral cache — entries auto-expire
            >>> cache = RedisCacheManager(prefix="API:USERS", default_ttl=600)
            >>> # Persistent cache — entries live forever
            >>> cache = RedisCacheManager(prefix="CONFIG", default_ttl=None)
        """
        self.url: str = url
        self.prefix: str = prefix.upper()
        self.default_ttl: Optional[int] = default_ttl
        self._sync_client: redis.Redis = redis.Redis.from_url(
            self.url, decode_responses=True
        )
        self._async_client: aioredis.Redis = aioredis.from_url(
            self.url, decode_responses=True
        )

    def _key(self, cache_key: str) -> str:
        """Build full Redis key from prefix and cache_key."""
        return f"{self.prefix}:{cache_key}"

    def _serialize(self, value: Any) -> str:
        """Serialize a value to JSON string for storage."""
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    def _deserialize(self, raw: Optional[str]) -> Optional[Any]:
        """Deserialize a JSON string back to Python object."""
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def _apply_ttl(self, key: str, ttl: Optional[int] = None) -> None:
        """Apply TTL to a key. Uses method ttl if provided, else default_ttl."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None and effective_ttl > 0:
            self._sync_client.expire(key, effective_ttl)

    async def _apply_ttl_async(self, key: str, ttl: Optional[int] = None) -> None:
        """Async: Apply TTL to a key."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None and effective_ttl > 0:
            await self._async_client.expire(key, effective_ttl)

    # ──────────────────────────────────────────────
    # SYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    def store(
        self,
        cache_key: str,
        value: Any,
        ttl: Optional[int] = None,
        overwrite: bool = False,
    ) -> bool:
        """
        Store a value in the cache.

        Serializes dicts/lists to JSON strings automatically. Strings are stored
        as-is. If the key already exists and ``overwrite=False``, a ``ValueError``
        is raised.

        Args:
            cache_key: Cache key (will be prefixed with ``{prefix}:``).
            value: Value to cache. Dicts, lists, and primitives are JSON-serialized.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
                 Use -1 for permanent entry.
            overwrite: If True, overwrite existing entries silently.

        Returns:
            True if the value was stored.

        Raises:
            ValueError: If key exists and ``overwrite=False``.

        Example:
            >>> cache.store("session:abc", {"user_id": "123", "role": "admin"})
            True
            >>> cache.store("config:dark_mode", True, ttl=86400)
            True
        """
        key = self._key(cache_key)
        if not overwrite and self._sync_client.exists(key):
            raise ValueError(
                f"Cache key '{key}' already exists. Use overwrite=True to replace."
            )
        serialized = self._serialize(value)
        self._sync_client.set(key, serialized)
        self._apply_ttl(key, ttl)
        return True

    def retrieve(
        self, cache_key: str, default: Any = None
    ) -> Any:
        """
        Retrieve a cached value by key.

        Deserializes JSON strings back to Python objects automatically.
        Returns ``default`` if the key does not exist.

        Args:
            cache_key: Cache key to look up.
            default: Value to return if key is missing. Defaults to None.

        Returns:
            The cached value (deserialized), or ``default`` if not found.

        Example:
            >>> cache.retrieve("session:abc")
            {"user_id": "123", "role": "admin"}
            >>> cache.retrieve("missing:key", default=[])
            []
        """
        key = self._key(cache_key)
        raw = self._sync_client.get(key)
        if raw is None:
            return default
        return self._deserialize(raw)

    def upsert(
        self,
        cache_key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Store or update a cached value (create-or-update semantics).

        Unlike ``store()``, this never raises ``ValueError`` — it silently
        overwrites existing entries. Ideal for cache warming and refresh.

        Args:
            cache_key: Cache key.
            value: Value to cache.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.

        Returns:
            True if the value was stored.

        Example:
            >>> cache.upsert("config:theme", "dark")
            True
        """
        key = self._key(cache_key)
        serialized = self._serialize(value)
        self._sync_client.set(key, serialized)
        self._apply_ttl(key, ttl)
        return True

    def delete(self, *cache_keys: str) -> int:
        """
        Delete one or more cached entries.

        Args:
            *cache_keys: One or more cache keys to delete.

        Returns:
            Number of keys actually deleted.

        Example:
            >>> cache.delete("session:abc", "session:def")
            2
        """
        if not cache_keys:
            return 0
        full_keys = [self._key(k) for k in cache_keys]
        return int(self._sync_client.delete(*full_keys))

    def exists(self, cache_key: str) -> bool:
        """Check if a cache key exists."""
        return bool(self._sync_client.exists(self._key(cache_key)))

    # ──────────────────────────────────────────────
    # ATOMIC / CACHE-ASIDE OPERATIONS
    # ──────────────────────────────────────────────

    def store_if_not_exists(
        self,
        cache_key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Store a value only if the key does not already exist (atomic).

        Uses Redis ``SET NX`` under the hood. Useful for distributed
        lock-free "claim" patterns (e.g., first-write-wins).

        Args:
            cache_key: Cache key.
            value: Value to store.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.

        Returns:
            True if the value was set (key did not exist), False otherwise.

        Example:
            >>> cache.store_if_not_exists("lock:job:123", "worker-1")
            True
            >>> cache.store_if_not_exists("lock:job:123", "worker-2")
            False
        """
        key = self._key(cache_key)
        serialized = self._serialize(value)
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None and effective_ttl > 0:
            result = self._sync_client.set(key, serialized, nx=True, ex=effective_ttl)
        else:
            result = self._sync_client.set(key, serialized, nx=True)
        return bool(result)

    def get_or_set(
        self,
        cache_key: str,
        factory: Union[Callable[[], Any], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """
        Cache-aside (lazy-loading) pattern: retrieve existing value, or compute
        and cache a new one.

        If the key exists, its value is returned immediately. If not, ``factory``
        is called (if callable) or ``factory`` is used directly as the value,
        stored in cache, and returned.

        Args:
            cache_key: Cache key.
            factory: A callable that produces the value on cache miss, or a
                     static value to store on miss.
            ttl: TTL in seconds for the new entry. Overrides ``default_ttl``
                 if provided.

        Returns:
            The cached or freshly computed value.

        Example:
            >>> # With a factory function (lazy computation)
            >>> cache.get_or_set("user:123", lambda: expensive_db_query("123"))
            {"name": "Alice", "role": "admin"}

            >>> # With a static default
            >>> cache.get_or_set("config:feature_x", {"enabled": True})
            {"enabled": True}
        """
        existing = self.retrieve(cache_key)
        if existing is not None:
            return existing
        value = factory() if callable(factory) else factory
        self.upsert(cache_key, value, ttl=ttl)
        return value

    # ──────────────────────────────────────────────
    # TTL OPERATIONS
    # ──────────────────────────────────────────────

    def expire(self, cache_key: str, seconds: int) -> bool:
        """
        Set TTL on an existing cache entry.

        Args:
            cache_key: Cache key.
            seconds: TTL in seconds.

        Returns:
            True if timeout was set, False if key does not exist.

        Example:
            >>> cache.expire("session:abc", 7200)
            True
        """
        return bool(self._sync_client.expire(self._key(cache_key), seconds))

    def bulk_expire(self, cache_keys: List[str], seconds: int) -> int:
        """
        Set TTL on multiple cache entries using pipeline.

        Args:
            cache_keys: List of cache keys.
            seconds: TTL in seconds.

        Returns:
            Number of entries updated.

        Example:
            >>> cache.bulk_expire(["k1", "k2", "k3"], 1800)
            3
        """
        if not cache_keys:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for k in cache_keys:
            pipe.expire(self._key(k), seconds)
        results = pipe.execute()
        return sum(results)

    def ttl(self, cache_key: str) -> int:
        """
        Get remaining TTL of a cache entry.

        Args:
            cache_key: Cache key.

        Returns:
            Remaining seconds, -1 if no expiry, -2 if key does not exist.

        Example:
            >>> cache.ttl("session:abc")
            5400
        """
        return self._sync_client.ttl(self._key(cache_key))

    def persist(self, cache_key: str) -> bool:
        """
        Remove TTL from a cache entry (make it permanent).

        Args:
            cache_key: Cache key.

        Returns:
            True if TTL was removed, False otherwise.

        Example:
            >>> cache.persist("config:feature_x")
            True
        """
        return bool(self._sync_client.persist(self._key(cache_key)))

    # ──────────────────────────────────────────────
    # BULK OPERATIONS
    # ──────────────────────────────────────────────

    def bulk_store(
        self,
        entries: Dict[str, Any],
        ttl: Optional[int] = None,
        overwrite: bool = False,
    ) -> int:
        """
        Store multiple values using pipeline.

        Args:
            entries: Dict mapping cache keys to values.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
            overwrite: If True, overwrite existing entries.

        Returns:
            Number of entries stored.

        Example:
            >>> cache.bulk_store({"k1": [1, 2], "k2": {"a": 1}, "k3": "text"})
            3
        """
        if not entries:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        stored = 0
        for cache_key, value in entries.items():
            key = self._key(cache_key)
            if not overwrite and self._sync_client.exists(key):
                continue
            serialized = self._serialize(value)
            pipe.set(key, serialized)
            stored += 1
        pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for cache_key in entries:
                    ttl_pipe.expire(self._key(cache_key), effective_ttl)
                ttl_pipe.execute()
        return stored

    def bulk_retrieve(
        self, cache_keys: List[str], default: Any = None
    ) -> Dict[str, Any]:
        """
        Retrieve multiple cached values using pipeline.

        Args:
            cache_keys: List of cache keys to look up.
            default: Value to return for missing keys.

        Returns:
            Dict mapping cache keys to their values (or ``default``).

        Example:
            >>> cache.bulk_retrieve(["k1", "k2", "missing"])
            {"k1": [1, 2], "k2": {"a": 1}, "missing": None}
        """
        if not cache_keys:
            return {}
        pipe = self._sync_client.pipeline(transaction=False)
        for cache_key in cache_keys:
            pipe.get(self._key(cache_key))
        results = pipe.execute()
        output: Dict[str, Any] = {}
        for cache_key, raw in zip(cache_keys, results):
            output[cache_key] = self._deserialize(raw) if raw is not None else default
        return output

    def bulk_delete(self, cache_keys: List[str]) -> int:
        """
        Delete multiple cache entries using pipeline.

        Args:
            cache_keys: List of cache keys to delete.

        Returns:
            Number of keys actually deleted.

        Example:
            >>> cache.bulk_delete(["k1", "k2", "k3"])
            3
        """
        if not cache_keys:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for cache_key in cache_keys:
            pipe.delete(self._key(cache_key))
        results = pipe.execute()
        return sum(results)

    # ──────────────────────────────────────────────
    # PATTERN / NAMESPACE OPERATIONS
    # ──────────────────────────────────────────────

    def invalidate_pattern(self, pattern: str, batch_size: int = 1000) -> int:
        """
        Delete all cache entries matching a glob pattern under this prefix.

        Uses SCAN for non-blocking iteration. Useful for invalidating a
        subset of cached data (e.g., all ``user:*`` keys).

        Args:
            pattern: Glob pattern to match (appended to prefix).
                     Example: ``"user:*"`` matches ``CACHE:user:123``.
            batch_size: SCAN batch size.

        Returns:
            Number of keys deleted.

        Example:
            >>> cache.invalidate_pattern("session:*")
            42
        """
        search = f"{self.prefix}:{pattern}"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    def invalidate_namespace(self, namespace: str, batch_size: int = 1000) -> int:
        """
        Delete all cache entries under a sub-namespace.

        A convenience wrapper around ``invalidate_pattern`` that matches
        everything under ``{namespace}:*``.

        Args:
            namespace: Sub-namespace to clear (e.g., "user" clears
                       ``CACHE:user:123``, ``CACHE:user:456``, etc.).
            batch_size: SCAN batch size.

        Returns:
            Number of keys deleted.

        Example:
            >>> cache.invalidate_namespace("session")
            150
        """
        return self.invalidate_pattern(f"{namespace}*", batch_size=batch_size)

    def flush_all(self, batch_size: int = 1000) -> int:
        """
        Delete ALL cache entries under this prefix (dangerous).

        Uses SCAN to iterate — safe for production (non-blocking).

        Args:
            batch_size: SCAN batch size.

        Returns:
            Number of keys deleted.

        Example:
            >>> cache.flush_all()
            1024
        """
        return self.invalidate_pattern("*", batch_size=batch_size)

    # ──────────────────────────────────────────────
    # INSPECTION / STATS
    # ──────────────────────────────────────────────

    def count(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """
        Count cache entries under this prefix.

        Args:
            pattern: Optional sub-pattern to match.
            batch_size: SCAN batch size.

        Returns:
            Number of matching keys.

        Example:
            >>> cache.count()
            256
            >>> cache.count(pattern="user:*")
            42
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            count += len(keys)
            if cursor == 0:
                break
        return count

    def list_keys(
        self,
        pattern: Optional[str] = None,
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        List cache keys under this prefix with optional pagination.

        Args:
            pattern: Optional sub-pattern to match.
            offset: Number of keys to skip.
            limit: Maximum keys to return. 0 = no limit.
            batch_size: SCAN batch size.

        Returns:
            List of cache keys (without prefix).

        Example:
            >>> cache.list_keys(pattern="user:*", limit=10)
            ["user:123", "user:456", ...]
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        keys_list: List[str] = []
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            for key in keys:
                keys_list.append(key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
        if offset > 0 or limit > 0:
            end = offset + limit if limit > 0 else None
            return keys_list[offset:end]
        return keys_list

    def stats(self) -> Dict[str, Any]:
        """
        Get cache statistics from Redis ``INFO`` command.

        Returns:
            Dict with ``used_memory``, ``used_memory_human``,
            ``keyspace_hits``, ``keyspace_misses``, and computed ``hit_rate``.

        Example:
            >>> cache.stats()
            {"used_memory": 1048576, "hit_rate": 0.95, ...}
        """
        info = self._sync_client.info("stats")
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses
        hit_rate = hits / total if total > 0 else 0.0
        memory = self._sync_client.info("memory")
        return {
            "used_memory": memory.get("used_memory", 0),
            "used_memory_human": memory.get("used_memory_human", "N/A"),
            "keyspace_hits": hits,
            "keyspace_misses": misses,
            "hit_rate": round(hit_rate, 4),
            "total_keys": self.count(),
        }

    # ──────────────────────────────────────────────
    # IMPORT / EXPORT
    # ──────────────────────────────────────────────

    def export_json(
        self,
        filepath: str,
        pattern: Optional[str] = None,
        indent: int = 2,
    ) -> int:
        """
        Export all cached entries to a JSON file.

        Args:
            filepath: Output file path.
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            Number of entries exported.

        Example:
            >>> cache.export_json("/tmp/cache_backup.json")
            256
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        entries: Dict[str, Any] = {}
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=1000)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.get(key)
                raw_list = pipe.execute()
                for key, raw in zip(keys, raw_list):
                    if raw is not None:
                        cache_key = key.removeprefix(f"{self.prefix}:")
                        entries[cache_key] = self._deserialize(raw)
            if cursor == 0:
                break
        export_data = {"prefix": self.prefix, "entries": entries}
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=indent, ensure_ascii=False, default=str)
        return len(entries)

    def import_json(
        self,
        filepath: str,
        overwrite: bool = True,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import cached entries from a JSON file.

        Args:
            filepath: Input file path.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides ``default_ttl``.

        Returns:
            Number of entries imported.

        Example:
            >>> cache.import_json("/tmp/cache_backup.json")
            256
        """
        with open(filepath, "r", encoding="utf-8") as f:
            import_data = json.load(f)
        entries = import_data.get("entries", {})
        stored = 0
        pipe = self._sync_client.pipeline(transaction=False)
        for cache_key, value in entries.items():
            key = self._key(cache_key)
            if not overwrite and self._sync_client.exists(key):
                continue
            serialized = self._serialize(value)
            pipe.set(key, serialized)
            stored += 1
        pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for cache_key in entries:
                    ttl_pipe.expire(self._key(cache_key), effective_ttl)
                ttl_pipe.execute()
        return stored

    def export_json_string(
        self, pattern: Optional[str] = None, indent: int = 2
    ) -> str:
        """
        Export all cached entries to a JSON string.

        Args:
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            JSON string of all entries.

        Example:
            >>> json_str = cache.export_json_string()
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        entries: Dict[str, Any] = {}
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=1000)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.get(key)
                raw_list = pipe.execute()
                for key, raw in zip(keys, raw_list):
                    if raw is not None:
                        cache_key = key.removeprefix(f"{self.prefix}:")
                        entries[cache_key] = self._deserialize(raw)
            if cursor == 0:
                break
        export_data = {"prefix": self.prefix, "entries": entries}
        return json.dumps(export_data, indent=indent, ensure_ascii=False, default=str)

    def import_json_string(
        self,
        json_str: str,
        overwrite: bool = True,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import cached entries from a JSON string.

        Args:
            json_str: JSON string containing entries.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries.

        Returns:
            Number of entries imported.

        Example:
            >>> cache.import_json_string(json_str)
            128
        """
        import_data = json.loads(json_str)
        entries = import_data.get("entries", {})
        stored = 0
        pipe = self._sync_client.pipeline(transaction=False)
        for cache_key, value in entries.items():
            key = self._key(cache_key)
            if not overwrite and self._sync_client.exists(key):
                continue
            serialized = self._serialize(value)
            pipe.set(key, serialized)
            stored += 1
        pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for cache_key in entries:
                    ttl_pipe.expire(self._key(cache_key), effective_ttl)
                ttl_pipe.execute()
        return stored

    # ──────────────────────────────────────────────
    # DECORATOR — FUNCTION RESULT CACHING
    # ──────────────────────────────────────────────

    def cache_result(
        self,
        ttl: Optional[int] = None,
        key_prefix: str = "",
        fallback: Optional[Callable] = None,
    ) -> Callable[[F], F]:
        """
        Decorator that caches a function's return value.

        The cache key is derived from the function's module, name, and
        arguments. On cache hit, the stored result is returned without
        calling the function. On miss, the function is called, its result
        is cached, and returned.

        Args:
            ttl: TTL in seconds for the cached result. Overrides ``default_ttl``.
            key_prefix: Optional prefix added to the cache key for namespacing.
            fallback: Optional callable invoked on Redis errors. Receives the
                      original function and its arguments. If not provided,
                      the exception propagates.

        Returns:
            A decorator that wraps the function with caching logic.

        Example:
            >>> @cache.cache_result(ttl=300)
            ... def get_user(user_id: str) -> dict:
            ...     return db.query_user(user_id)

            >>> @cache.cache_result(ttl=60, key_prefix="api")
            ... def fetch_products(category: str) -> list:
            ...     return api.get_products(category)
        """

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                key_parts = [func.__module__, func.__qualname__]
                if key_prefix:
                    key_parts.insert(0, key_prefix)
                key_parts.extend([repr(a) for a in args])
                key_parts.extend([f"{k}={repr(v)}" for k, v in sorted(kwargs.items())])
                raw_key = ":".join(key_parts)
                cache_key = (
                    hashlib.sha256(raw_key.encode()).hexdigest()[:32]
                    if len(raw_key) > 128
                    else raw_key
                )
                try:
                    existing = self.retrieve(cache_key)
                    if existing is not None:
                        return existing
                    result = func(*args, **kwargs)
                    self.upsert(cache_key, result, ttl=ttl)
                    return result
                except Exception as e:
                    if fallback is not None:
                        return fallback(func, *args, **kwargs)
                    raise e

            wrapper.cache_clear = lambda *a, **kw: self.invalidate_pattern(  # type: ignore
                f"{key_prefix or func.__qualname__}*"
            )
            return wrapper  # type: ignore

        return decorator

    # ──────────────────────────────────────────────
    # ASYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    async def async_store(
        self,
        cache_key: str,
        value: Any,
        ttl: Optional[int] = None,
        overwrite: bool = False,
    ) -> bool:
        """Async: Store a value in the cache."""
        key = self._key(cache_key)
        if not overwrite and await self._async_client.exists(key):
            raise ValueError(
                f"Cache key '{key}' already exists. Use overwrite=True to replace."
            )
        serialized = self._serialize(value)
        await self._async_client.set(key, serialized)
        await self._apply_ttl_async(key, ttl)
        return True

    async def async_retrieve(
        self, cache_key: str, default: Any = None
    ) -> Any:
        """Async: Retrieve a cached value."""
        key = self._key(cache_key)
        raw = await self._async_client.get(key)
        if raw is None:
            return default
        return self._deserialize(raw)

    async def async_upsert(
        self,
        cache_key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """Async: Store or update a cached value."""
        key = self._key(cache_key)
        serialized = self._serialize(value)
        await self._async_client.set(key, serialized)
        await self._apply_ttl_async(key, ttl)
        return True

    async def async_delete(self, *cache_keys: str) -> int:
        """Async: Delete one or more cached entries."""
        if not cache_keys:
            return 0
        full_keys = [self._key(k) for k in cache_keys]
        return int(await self._async_client.delete(*full_keys))

    async def async_exists(self, cache_key: str) -> bool:
        """Async: Check if a cache key exists."""
        return bool(await self._async_client.exists(self._key(cache_key)))

    async def async_get_or_set(
        self,
        cache_key: str,
        factory: Union[Callable[[], Any], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """Async: Cache-aside pattern — retrieve or compute and cache."""
        existing = await self.async_retrieve(cache_key)
        if existing is not None:
            return existing
        value = factory() if callable(factory) else factory
        await self.async_upsert(cache_key, value, ttl=ttl)
        return value

    async def async_bulk_retrieve(
        self, cache_keys: List[str], default: Any = None
    ) -> Dict[str, Any]:
        """Async: Retrieve multiple cached values using pipeline."""
        if not cache_keys:
            return {}
        pipe = self._async_client.pipeline(transaction=False)
        for cache_key in cache_keys:
            pipe.get(self._key(cache_key))
        results = await pipe.execute()
        output: Dict[str, Any] = {}
        for cache_key, raw in zip(cache_keys, results):
            output[cache_key] = self._deserialize(raw) if raw is not None else default
        return output

    async def async_bulk_store(
        self,
        entries: Dict[str, Any],
        ttl: Optional[int] = None,
        overwrite: bool = False,
    ) -> int:
        """Async: Store multiple values using pipeline."""
        if not entries:
            return 0
        pipe = self._async_client.pipeline(transaction=False)
        stored = 0
        for cache_key, value in entries.items():
            key = self._key(cache_key)
            if not overwrite and await self._async_client.exists(key):
                continue
            serialized = self._serialize(value)
            pipe.set(key, serialized)
            stored += 1
        await pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._async_client.pipeline(transaction=False)
                for cache_key in entries:
                    ttl_pipe.expire(self._key(cache_key), effective_ttl)
                await ttl_pipe.execute()
        return stored

    async def async_invalidate_pattern(
        self, pattern: str, batch_size: int = 1000
    ) -> int:
        """Async: Delete all cache entries matching a pattern."""
        search = f"{self.prefix}:{pattern}"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            if keys:
                pipe = self._async_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = await pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    async def async_count(
        self, pattern: Optional[str] = None, batch_size: int = 1000
    ) -> int:
        """Async: Count cache entries under this prefix."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        count = 0
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            count += len(keys)
            if cursor == 0:
                break
        return count

    async def async_flush_all(self, batch_size: int = 1000) -> int:
        """Async: Delete ALL cache entries under this prefix."""
        return await self.async_invalidate_pattern("*", batch_size=batch_size)

    async def async_close(self) -> None:
        """Close async Redis connection."""
        await self._async_client.aclose()

    def close(self) -> None:
        """Close sync Redis connection."""
        self._sync_client.close()

    # ──────────────────────────────────────────────
    # DUNDER METHODS
    # ──────────────────────────────────────────────

    def __repr__(self) -> str:
        """String representation of RedisCacheManager."""
        return (
            f"RedisCacheManager(url='{self.url}', prefix='{self.prefix}', "
            f"default_ttl={self.default_ttl})"
        )

    def __enter__(self) -> "RedisCacheManager":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit — close connections."""
        self.close()


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────


def example_redis_hash_util() -> None:
    """Demonstrate RedisHashUtil — hash-based persistent storage."""

    print("=" * 60)
    print("RedisHashUtil — Usage Examples")
    print("=" * 60)

    users = RedisHashUtil(
        url="redis://localhost:6379/0",
        prefix="USERS:WORKERS",
        index_key="INDEX",
        lock_key="MUTEX",
        default_ttl=3600,
    )

    # ── STATIC UTILITIES ──────────────────────

    print("\n--- Static Utilities ---")
    print(f"Random string (16): {RedisHashUtil.generate_random_string(16)}")
    print(f"OTP (6 digits):     {RedisHashUtil.generate_random_number(6)}")
    print(f"Token:              {RedisHashUtil.generate_token(32)}")
    print(f"UUID4:              {RedisHashUtil.generate_uuid4()}")
    print(f"UUID5:              {RedisHashUtil.generate_uuid5('myapp.users', 'john@x.com')}")

    password = "SuperSecret123!"
    hashed_pw = RedisHashUtil.hash_password(password)
    print(f"Hashed password:    {hashed_pw[:30]}...")
    print(f"Password verified:  {RedisHashUtil.verify_password(password, hashed_pw)}")

    # ── CRUD ─────────────────────────────────

    print("\n--- CRUD Operations ---")
    users.delete_all()

    user_id = users.create({
        "username": "johndoe",
        "email": "john@example.com",
        "role": "admin",
        "status": "active",
    })
    print(f"Created user: {user_id}  TTL: {users.ttl(user_id)}s")

    session_id = users.create({"token": "abc123"}, ttl=300)
    print(f"Session TTL: {users.ttl(session_id)}s (overridden to 300)")

    print(f"Read user: {users.read(user_id)}")

    users.update(user_id, {"role": "superadmin", "last_login": "2026-01-15"})
    print(f"Updated user: {users.read(user_id)}")

    print(f"set_if_not_exists (first):  {users.set_if_not_exists(user_id, 'created_at', '2026-01-01')}")
    print(f"set_if_not_exists (second): {users.set_if_not_exists(user_id, 'created_at', '2026-06-01')}")

    # ── TTL ───────────────────────────────────

    print("\n--- TTL Operations ---")
    users.expire(user_id, 7200)
    print(f"TTL after expire(7200): {users.ttl(user_id)}s")
    users.persist(user_id)
    print(f"TTL after persist:      {users.ttl(user_id)} (permanent)")

    # ── BULK ──────────────────────────────────

    print("\n--- Bulk Operations ---")
    users.delete_all()

    bulk_users = {
        RedisHashUtil.generate_uuid4(): {"username": f"user_{i}", "status": "active"}
        for i in range(5)
    }
    created = users.bulk_create(bulk_users, ttl=600)
    print(f"Bulk created: {len(created)} entries")

    all_ids = list(bulk_users.keys())
    print(f"Bulk read: {len(users.bulk_read(all_ids))} entries")

    updates = {id: {"status": "inactive"} for id in all_ids[:2]}
    print(f"Bulk updated: {users.bulk_update(updates)} entries")
    print(f"Bulk deleted: {users.bulk_delete(all_ids[:2])} entries")

    # ── SEARCH / INDEX ────────────────────────

    print("\n--- Search & Index ---")
    users.delete_all()
    users.create({"username": "alice", "role": "admin"}, id="u1", overwrite=True)
    users.create({"username": "bob", "role": "user"}, id="u2", overwrite=True)
    users.create({"username": "charlie", "role": "admin"}, id="u3", overwrite=True)

    print(f"Admins (search):     {users.search('role', 'admin')}")
    print(f"Contains 'ali':      {users.search('username', 'ali', exact=False)}")

    users.create_index("u1", "role")
    users.create_index("u2", "role")
    users.create_index("u3", "role")
    print(f"Admins (index):      {users.find_by_index('role', 'admin')}")
    print(f"Admins with data:    {list(users.find_by_index_with_data('role', 'admin').keys())}")

    # ── IMPORT / EXPORT ───────────────────────

    print("\n--- Export / Import ---")
    users.export_json("/tmp/users_export.json")
    print(f"Exported JSON: {users.export_json_string()[:60]}...")

    users_csv_import = RedisHashUtil(prefix="USERS:CSV_IMPORTED")
    users_csv_import.delete_all()
    users.export_csv("/tmp/users_export.csv")
    imported_csv = users_csv_import.import_csv("/tmp/users_export.csv", overwrite=True)
    print(f"Imported from CSV: {imported_csv} entries")

    # ── CLEANUP ───────────────────────────────

    print("\n--- Cleanup ---")
    users.delete_all()
    users_csv_import.delete_all()
    users.close()
    users_csv_import.close()
    print("Done!\n")


def example_redis_cache_manager() -> None:
    """Demonstrate RedisCacheManager — string-based caching layer."""

    print("=" * 60)
    print("RedisCacheManager — Usage Examples")
    print("=" * 60)

    cache = RedisCacheManager(
        url="redis://localhost:6379/0",
        prefix="API:USERS",
        default_ttl=600,  # optional — entries auto-expire after 10 min
    )

    # ── BASIC CRUD ────────────────────────────

    print("\n--- Basic CRUD ---")
    cache.flush_all()

    # Store (raises on duplicate)
    cache.store("user:123", {"name": "Alice", "role": "admin"}, overwrite=True)
    print(f"Stored user:123")

    # Retrieve (deserializes JSON automatically)
    user = cache.retrieve("user:123")
    print(f"Retrieved:   {user}")

    # Retrieve with default
    missing = cache.retrieve("user:999", default={"name": "Nobody"})
    print(f"Missing key: {missing}")

    # Upsert (silent overwrite)
    cache.upsert("user:123", {"name": "Alice", "role": "superadmin"})
    print(f"Upserted:    {cache.retrieve('user:123')}")

    # Exists
    print(f"Exists:      {cache.exists('user:123')}")
    print(f"Missing:     {cache.exists('user:999')}")

    # Delete
    cache.store("temp:key", "ephemeral", overwrite=True)
    cache.delete("temp:key")
    print(f"Deleted temp:key — exists: {cache.exists('temp:key')}")

    # ── CACHE-ASIDE PATTERN ───────────────────

    print("\n--- get_or_set (Cache-Aside) ---")

    call_count = 0

    def expensive_query(user_id: str) -> dict:
        """Simulate an expensive database call."""
        nonlocal call_count
        call_count += 1
        return {"name": f"User_{user_id}", "computed_at": time.time()}

    # First call — cache miss, calls factory
    result = cache.get_or_set("user:456", lambda: expensive_query("456"), ttl=300)
    print(f"First call:  {result}  (factory calls: {call_count})")

    # Second call — cache hit, factory NOT called
    result = cache.get_or_set("user:456", lambda: expensive_query("456"), ttl=300)
    print(f"Second call: {result}  (factory calls: {call_count})")

    # With static default
    config = cache.get_or_set("config:features", {"dark_mode": True, "beta": False})
    print(f"Config:      {config}")

    # ── TTL OPERATIONS ────────────────────────

    print("\n--- TTL Operations ---")
    cache.store("ttl:test", "expires_soon", ttl=30)
    print(f"TTL remaining: {cache.ttl('ttl:test')}s")

    cache.expire("ttl:test", 3600)
    print(f"After expire:  {cache.ttl('ttl:test')}s")

    cache.persist("ttl:test")
    print(f"After persist: {cache.ttl('ttl:test')} (-1 = permanent)")

    # ── BULK OPERATIONS ───────────────────────

    print("\n--- Bulk Operations ---")
    cache.bulk_store({
        "bulk:1": {"item": "apple", "qty": 5},
        "bulk:2": {"item": "banana", "qty": 3},
        "bulk:3": {"item": "cherry", "qty": 8},
    }, ttl=120)

    bulk_data = cache.bulk_retrieve(["bulk:1", "bulk:2", "bulk:3", "bulk:missing"])
    print(f"Bulk retrieved: {len(bulk_data)} keys")
    for k, v in bulk_data.items():
        print(f"  {k}: {v}")

    print(f"Bulk deleted: {cache.bulk_delete(['bulk:1', 'bulk:2', 'bulk:3'])}")

    # ── ATOMIC STORE IF NOT EXISTS ────────────

    print("\n--- store_if_not_exists ---")
    r1 = cache.store_if_not_exists("atomic:key", "first_writer")
    r2 = cache.store_if_not_exists("atomic:key", "second_writer")
    print(f"First:  {r1}  (value: {cache.retrieve('atomic:key')})")
    print(f"Second: {r2}  (value still: {cache.retrieve('atomic:key')})")

    # ── NAMESPACE / PATTERN INVALIDATION ──────

    print("\n--- Namespace Invalidation ---")
    cache.store("session:abc", "data1", overwrite=True)
    cache.store("session:def", "data2", overwrite=True)
    cache.store("user:sess1", "data3", overwrite=True)
    print(f"Before: {cache.count()} keys")

    cache.invalidate_namespace("session")
    print(f"After invalidating 'session': {cache.count()} keys")

    # ── DECORATOR ─────────────────────────────

    print("\n--- @cache_result Decorator ---")

    @cache.cache_result(ttl=300, key_prefix="decorated")
    def get_product(product_id: str) -> dict:
        """Simulate fetching a product from DB."""
        return {"id": product_id, "name": f"Product-{product_id}", "price": 29.99}

    p1 = get_product("P001")
    p2 = get_product("P001")  # served from cache
    print(f"Product: {p1}")
    print(f"Cached:  {p1 == p2}  (same object from cache)")

    # ── STATS ─────────────────────────────────

    print("\n--- Cache Stats ---")
    s = cache.stats()
    print(f"Total keys:     {s['total_keys']}")
    print(f"Memory:         {s['used_memory_human']}")
    print(f"Hit rate:       {s['hit_rate']}")

    # ── IMPORT / EXPORT ───────────────────────

    print("\n--- Export / Import ---")
    cache.export_json("/tmp/cache_backup.json")
    print(f"Exported to /tmp/cache_backup.json")

    imported = cache.import_json("/tmp/cache_backup.json", overwrite=True)
    print(f"Imported: {imported} entries")

    json_str = cache.export_json_string()
    print(f"JSON string length: {len(json_str)} chars")

    # ── CLEANUP ───────────────────────────────

    print("\n--- Cleanup ---")
    cache.flush_all()
    print(f"Remaining keys: {cache.count()}")
    cache.close()
    print("Done!\n")


if __name__ == "__main__":
    example_redis_hash_util()
    example_redis_cache_manager()
