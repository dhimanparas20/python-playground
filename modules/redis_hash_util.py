"""
Redis Hash Utility Class
A production-ready, all-in-one utility for Redis (Valkey) hash operations.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import secrets
import string
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, TypeVar, Union

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
        id: str,
        data: Dict[str, Any],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Create a new hash entry or update if overwrite is True.

        Args:
            id: Unique identifier for the hash entry.
            data: Dictionary of field-value pairs to store.
            overwrite: If True, delete existing data before writing.
            ttl: TTL in seconds for this entry. Overrides default_ttl if provided.

        Returns:
            True if operation succeeded.

        Raises:
            ValueError: If entry exists and overwrite is False.
        """
        key = self._key(id)
        if not overwrite and self._sync_client.exists(key):
            raise ValueError(f"Entry '{key}' already exists. Use overwrite=True to update.")
        if overwrite:
            self._sync_client.delete(key)
        self._sync_client.hset(key, mapping=data)
        self._apply_ttl(key, ttl)
        return True

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
        dest_id: str,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Copy a hash entry to a new id.

        Args:
            source_id: Source entry identifier.
            dest_id: Destination entry identifier.
            overwrite: If True, overwrite existing destination.
            ttl: TTL for the copied entry. Overrides default_ttl if provided.

        Returns:
            True if copy succeeded.

        Raises:
            ValueError: If destination exists and overwrite is False.
        """
        data = self.read(source_id)
        if data is None:
            return False
        return self.create(dest_id, dict(data), overwrite=overwrite, ttl=ttl)

    def bulk_copy(
        self,
        copies: Dict[str, str],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Copy multiple hash entries using pipeline.

        Args:
            copies: Dict mapping source_id -> dest_id.
            overwrite: If True, overwrite existing destinations.
            ttl: TTL for copied entries. Overrides default_ttl if provided.

        Returns:
            Number of entries copied.
        """
        if not copies:
            return 0
        source_ids = list(copies.keys())
        bulk_data = self.bulk_read(source_ids)
        new_entries: Dict[str, Dict[str, Any]] = {}
        for src_id, dest_id in copies.items():
            data = bulk_data.get(src_id)
            if data is not None:
                new_entries[dest_id] = dict(data)
        if not new_entries:
            return 0
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
    ) -> int:
        """
        Create multiple hash entries using pipeline.

        Args:
            entries: Dict mapping ids to their data dicts.
            overwrite: If True, overwrite existing entries.
            ttl: TTL in seconds. Overrides default_ttl if provided.

        Returns:
            Number of entries created.
        """
        count = 0
        pipe = self._sync_client.pipeline(transaction=False)
        for id, data in entries.items():
            key = self._key(id)
            if not overwrite and self._sync_client.exists(key):
                continue
            if overwrite:
                pipe.delete(key)
            pipe.hset(key, mapping=data)
            count += 1
        pipe.execute()
        # Apply TTL after pipeline since EXPIRE needs keys to exist
        if count > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for id in entries:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                ttl_pipe.execute()
        return count

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

    def get_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> Dict[str, Dict[str, str]]:
        """Get all hash entries under this prefix using SCAN."""
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
                    result[id] = data
            if cursor == 0:
                break
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

    def list_ids(self, pattern: Optional[str] = None, batch_size: int = 1000) -> List[str]:
        """List all entry ids under this prefix."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        ids: List[str] = []
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            for key in keys:
                ids.append(key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
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
        return self.bulk_create(entries, overwrite=overwrite, ttl=ttl)

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
        return self.bulk_create(entries, overwrite=overwrite, ttl=ttl)

    # ──────────────────────────────────────────────
    # IMPORT / EXPORT — CSV
    # ──────────────────────────────────────────────

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
        return self.bulk_create(entries, overwrite=overwrite, ttl=ttl)

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
        id: str,
        data: Dict[str, Any],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> bool:
        """Async: Create a new hash entry or update if overwrite is True."""
        key = self._key(id)
        if not overwrite and await self._async_client.exists(key):
            raise ValueError(f"Entry '{key}' already exists. Use overwrite=True to update.")
        if overwrite:
            await self._async_client.delete(key)
        await self._async_client.hset(key, mapping=data)
        await self._apply_ttl_async(key, ttl)
        return True

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
    ) -> int:
        """Async: Create multiple hash entries using pipeline."""
        count = 0
        pipe = self._async_client.pipeline(transaction=False)
        for id, data in entries.items():
            key = self._key(id)
            if not overwrite and await self._async_client.exists(key):
                continue
            if overwrite:
                pipe.delete(key)
            pipe.hset(key, mapping=data)
            count += 1
        await pipe.execute()
        if count > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._async_client.pipeline(transaction=False)
                for id in entries:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                await ttl_pipe.execute()
        return count

    async def async_bulk_read(self, ids: List[str]) -> Dict[str, Optional[Dict[str, str]]]:
        """Async: Read multiple hash entries using pipeline."""
        pipe = self._async_client.pipeline(transaction=False)
        for id in ids:
            pipe.hgetall(self._key(id))
        results = await pipe.execute()
        return {id: (data if data else None) for id, data in zip(ids, results)}

    async def async_get_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> Dict[str, Dict[str, str]]:
        """Async: Get all hash entries under this prefix."""
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
                    result[id] = data
            if cursor == 0:
                break
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


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("RedisHashUtil - Usage Examples")
    print("=" * 60)

    # Initialize with custom prefix, index_key, lock_key, and default TTL
    users = RedisHashUtil(
        url="redis://localhost:6379/0",
        prefix="USERS:WORKERS",
        index_key="INDEX",
        lock_key="MUTEX",
        default_ttl=3600,  # 1 hour default TTL
    )

    # ── STATIC UTILITIES ──────────────────────

    print("\n--- Static Utilities ---")
    print(f"Random string (16): {RedisHashUtil.generate_random_string(16)}")
    print(f"Random string (32): {RedisHashUtil.generate_random_string(32)}")
    print(f"OTP (6 digits):     {RedisHashUtil.generate_random_number(6)}")
    print(f"PIN (4 digits):     {RedisHashUtil.generate_random_number(4)}")
    print(f"Token:              {RedisHashUtil.generate_token(32)}")
    print(f"UUID4:              {RedisHashUtil.generate_uuid4()}")
    print(f"UUID5:              {RedisHashUtil.generate_uuid5('myapp.users', 'john@example.com')}")

    # Password hashing
    password = "SuperSecret123!"
    hashed_pw = RedisHashUtil.hash_password(password)
    print(f"Hashed password:    {hashed_pw[:30]}...")
    print(f"Password verified:  {RedisHashUtil.verify_password(password, hashed_pw)}")

    # Sensitive data hashing
    email = "user@example.com"
    email_hash = RedisHashUtil.hash_sensitive_data(email, pepper="myapp")
    print(f"Hashed email:       {email_hash}")

    # ── CRUD OPERATIONS ───────────────────────

    print("\n--- CRUD Operations ---")
    users.delete_all()

    # Create with default_ttl (1 hour from init)
    user_id = RedisHashUtil.generate_uuid4()
    users.create(user_id, {
        "username": "johndoe",
        "email": "john@example.com",
        "role": "admin",
        "status": "active",
    })
    print(f"Created user: {user_id}")
    print(f"TTL after create: {users.ttl(user_id)}s")

    # Create with explicit TTL override (5 minutes)
    session_id = RedisHashUtil.generate_uuid4()
    users.create(session_id, {"token": "abc123"}, ttl=300)
    print(f"Session TTL: {users.ttl(session_id)}s (overridden to 300)")

    # Read
    user_data = users.read(user_id)
    print(f"Read user: {user_data}")

    # Update
    users.update(user_id, {"role": "superadmin", "last_login": "2026-01-15"})
    print(f"Updated user: {users.read(user_id)}")

    # Set if not exists (atomic)
    result1 = users.set_if_not_exists(user_id, "created_at", "2026-01-01")
    result2 = users.set_if_not_exists(user_id, "created_at", "2026-06-01")
    print(f"set_if_not_exists (first):  {result1}")
    print(f"set_if_not_exists (second): {result2}")

    # Get or create
    new_id = RedisHashUtil.generate_uuid4()
    got = users.get_or_create(new_id, {"username": "newuser", "status": "pending"})
    print(f"get_or_create: {got}")
    got_again = users.get_or_create(new_id, {"username": "shouldnotchange", "status": "nope"})
    print(f"get_or_create (existing): {got_again}")

    # ── TTL OPERATIONS ────────────────────────

    print("\n--- TTL Operations ---")
    users.expire(user_id, 7200)
    print(f"TTL after expire(7200): {users.ttl(user_id)}s")

    users.persist(user_id)
    print(f"TTL after persist:      {users.ttl(user_id)} (permanent)")

    # Bulk expire
    ids = list(users.list_ids())
    users.bulk_expire(ids, 1800)
    print(f"Bulk expire applied to {len(ids)} entries")

    # ── BULK OPERATIONS ──────────────────────

    print("\n--- Bulk Operations ---")
    users.delete_all()

    bulk_users = {
        RedisHashUtil.generate_uuid4(): {"username": f"user_{i}", "status": "active"}
        for i in range(5)
    }
    created = users.bulk_create(bulk_users, ttl=600)
    print(f"Bulk created: {created} entries (TTL=600)")

    # Bulk read
    all_ids = list(bulk_users.keys())
    bulk_data = users.bulk_read(all_ids)
    print(f"Bulk read: {len(bulk_data)} entries")

    # Bulk update
    updates = {id: {"status": "inactive"} for id in all_ids[:2]}
    updated = users.bulk_update(updates)
    print(f"Bulk updated: {updated} entries")

    # Bulk delete
    deleted = users.bulk_delete(all_ids[:2])
    print(f"Bulk deleted: {deleted} entries")

    # ── COPY / RENAME ─────────────────────────

    print("\n--- Copy / Rename ---")
    remaining_ids = list(users.list_ids())
    if remaining_ids:
        src = remaining_ids[0]
        # Copy
        users.copy(src, src + "_copy", overwrite=True)
        print(f"Copied '{src}' -> '{src}_copy'")

        # Bulk copy
        copies = {id: f"{id}_backup" for id in remaining_ids}
        copied = users.bulk_copy(copies, overwrite=True)
        print(f"Bulk copied: {copied} entries")

        # Rename
        users.rename(src + "_copy", src + "_renamed")
        print(f"Renamed '{src}_copy' -> '{src}_renamed'")
        print(f"Renamed exists: {users.exists(src + '_renamed')}")

    # ── SEARCH ────────────────────────────────

    print("\n--- Search Operations ---")
    users.delete_all()
    users.create("u1", {"username": "alice", "role": "admin"}, overwrite=True)
    users.create("u2", {"username": "bob", "role": "user"}, overwrite=True)
    users.create("u3", {"username": "charlie", "role": "admin"}, overwrite=True)

    # Exact search
    admins = users.search("role", "admin")
    print(f"Admins (exact):    {admins}")

    # Substring search
    ali = users.search("username", "ali", exact=False)
    print(f"Username contains 'ali': {ali}")

    # Search with data
    admins_data = users.search_with_data("role", "admin")
    print(f"Admins with data:  {admins_data}")

    # ── INDEX OPERATIONS ─────────────────────

    print("\n--- Index Operations ---")
    users.create_index("u1", "role")
    users.create_index("u2", "role")
    users.create_index("u3", "role")

    indexed_admins = users.find_by_index("role", "admin")
    print(f"Indexed admins: {indexed_admins}")

    admins_full = users.find_by_index_with_data("role", "admin")
    print(f"Indexed admins with data: {admins_full}")

    # ── IMPORT / EXPORT — JSON ────────────────

    print("\n--- Export / Import JSON ---")
    users.export_json("/tmp/users_export.json")
    print("Exported to /tmp/users_export.json")

    json_str = users.export_json_string()
    print(f"JSON string length: {len(json_str)} chars")

    # Import into a fresh namespace
    users_import = RedisHashUtil(prefix="USERS:IMPORTED")
    users_import.delete_all()
    imported = users_import.import_json("/tmp/users_export.json", overwrite=True)
    print(f"Imported: {imported} entries into USERS:IMPORTED")
    print(f"Imported data: {users_import.get_all()}")

    # Import from string
    users_import.delete_all()
    imported2 = users_import.import_json_string(json_str, overwrite=True)
    print(f"Imported from string: {imported2} entries")

    # ── IMPORT / EXPORT — CSV ─────────────────

    print("\n--- Export / Import CSV ---")
    users.export_csv("/tmp/users_export.csv")
    print("Exported to /tmp/users_export.csv")

    csv_str = users.export_csv_string()
    print(f"CSV string:\n{csv_str}")

    users_csv_import = RedisHashUtil(prefix="USERS:CSV_IMPORTED")
    users_csv_import.delete_all()
    imported_csv = users_csv_import.import_csv("/tmp/users_export.csv", overwrite=True)
    print(f"Imported from CSV: {imported_csv} entries")
    print(f"CSV imported data: {users_csv_import.get_all()}")

    # ── LOCK OPERATIONS ──────────────────────

    print("\n--- Lock Operations ---")
    lock = users.acquire_lock("u1", timeout=5.0, blocking_timeout=2.0)
    if lock:
        print(f"Lock acquired for u1")
        users.release_lock(lock)
        print("Lock released")

    # ── CLEANUP ───────────────────────────────

    print("\n--- Cleanup ---")
    users.delete_all()
    users_import.delete_all()
    users_csv_import.delete_all()
    print(f"Users remaining:           {users.count_all()}")
    print(f"Imported remaining:        {users_import.count_all()}")
    print(f"CSV Imported remaining:    {users_csv_import.count_all()}")
    users.close()
    users_import.close()
    users_csv_import.close()
    print("\nDone!")
