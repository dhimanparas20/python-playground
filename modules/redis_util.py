"""
Redis HashMap Utility Module

Production-ready Redis utility class for hash map operations using redis-py.
Requirements: pip install redis
"""

from typing import Any, Dict, List, Optional, Union
import redis
import random
import string
from passlib.hash import pbkdf2_sha256

DEFAULT_REDIS_URL = "redis://localhost:6379/0"

class RedisHashMap:
    """
    Redis utility class for hash map operations.
    """

    def __init__(self, hash_name: str, connection_url: str = DEFAULT_REDIS_URL) -> None:
        """
        Initialize Redis client and select hash name.

        Args:
            hash_name (str): Name of the Redis hash.
            connection_url (str): Redis connection URL.
        """
        self.client = redis.Redis.from_url(connection_url, decode_responses=True)
        self.hash_name = hash_name

    @staticmethod
    def hashit(data: str) -> str:
        """
        Hash a string using pbkdf2_sha256.

        Args:
            data (str): Data to hash.

        Returns:
            str: Hashed string.
        """
        return pbkdf2_sha256.hash(data)

    @staticmethod
    def verify_hash(password: str, hashed_password: str) -> bool:
        """
        Verify a password against a hash.

        Args:
            password (str): Plain password.
            hashed_password (str): Hashed password.

        Returns:
            bool: True if verified, False otherwise.
        """
        return pbkdf2_sha256.verify(password, hashed_password)

    @staticmethod
    def gen_string(length: int = 15) -> str:
        """
        Generate a random alphanumeric string.

        Args:
            length (int): Length of the string.

        Returns:
            str: Random string.
        """
        characters = string.ascii_letters + string.digits
        return ''.join(random.choices(characters, k=length))

    def insert(self, field: str, value: Any) -> bool:
        """
        Insert or update a single field in the hash.

        Args:
            field (str): Field name.
            value (Any): Value to set.

        Returns:
            bool: True if field is new in the hash and value was set, False if field existed and value was updated.
        """
        return self.client.hset(self.hash_name, field, value) == 1

    def insert_many(self, mapping: Dict[str, Any]) -> bool:
        """
        Insert or update multiple fields in the hash.

        Args:
            mapping (Dict[str, Any]): Field-value pairs.

        Returns:
            bool: True if operation succeeded.
        """
        return self.client.hset(self.hash_name, mapping=mapping) > 0

    def fetch(self, field: Optional[str] = None) -> Union[Any, Dict[str, Any], None]:
        """
        Fetch a field or all fields from the hash.

        Args:
            field (Optional[str]): Field name. If None, fetches all fields.

        Returns:
            Any: Value of the field, or dict of all fields, or None if not found.
        """
        if field:
            return self.client.hget(self.hash_name, field)
        else:
            return self.client.hgetall(self.hash_name)

    def fetch_many(self, fields: List[str]) -> List[Any]:
        """
        Fetch multiple fields from the hash.

        Args:
            fields (List[str]): List of field names.

        Returns:
            List[Any]: List of values.
        """
        return self.client.hmget(self.hash_name, fields)

    def update(self, field: str, value: Any) -> bool:
        """
        Update a field in the hash (same as insert).

        Args:
            field (str): Field name.
            value (Any): New value.

        Returns:
            bool: True if field is new, False if updated.
        """
        return self.insert(field, value)

    def update_many(self, mapping: Dict[str, Any]) -> bool:
        """
        Update multiple fields in the hash (same as insert_many).

        Args:
            mapping (Dict[str, Any]): Field-value pairs.

        Returns:
            bool: True if operation succeeded.
        """
        return self.insert_many(mapping)

    def delete(self, field: str) -> bool:
        """
        Delete a field from the hash.

        Args:
            field (str): Field name.

        Returns:
            bool: True if field was deleted, False if not found.
        """
        return self.client.hdel(self.hash_name, field) == 1

    def delete_many(self, fields: List[str]) -> int:
        """
        Delete multiple fields from the hash.

        Args:
            fields (List[str]): List of field names.

        Returns:
            int: Number of fields deleted.
        """
        return self.client.hdel(self.hash_name, *fields)

    def exists(self, field: str) -> bool:
        """
        Check if a field exists in the hash.

        Args:
            field (str): Field name.

        Returns:
            bool: True if field exists, False otherwise.
        """
        return self.client.hexists(self.hash_name, field)

    def count(self) -> int:
        """
        Count the number of fields in the hash.

        Returns:
            int: Number of fields.
        """
        return self.client.hlen(self.hash_name)

    def keys(self) -> List[str]:
        """
        Get all field names in the hash.

        Returns:
            List[str]: List of field names.
        """
        return self.client.hkeys(self.hash_name)

    def values(self) -> List[Any]:
        """
        Get all values in the hash.

        Returns:
            List[Any]: List of values.
        """
        return self.client.hvals(self.hash_name)

    def incrby(self, field: str, amount: int = 1) -> int:
        """
        Increment the integer value of a field by a given amount.

        Args:
            field (str): Field name.
            amount (int): Amount to increment.

        Returns:
            int: New value.
        """
        return self.client.hincrby(self.hash_name, field, amount)

    def clear(self) -> None:
        """
        Delete the entire hash.
        """
        self.client.delete(self.hash_name)

    def close(self) -> None:
        """
        Close the Redis client connection.
        """
        self.client.close()

# Usage Example:
# rdb = RedisHashMap("myhash")
# rdb.insert("name", "Alice")
# rdb.insert_many({"age": 30, "city": "Wonderland"})
# print(rdb.fetch())  # {'name': 'Alice', 'age': '30', 'city': 'Wonderland'}
# rdb.delete("age")
# print(rdb.fetch())
# rdb.clear()
# rdb.close()
