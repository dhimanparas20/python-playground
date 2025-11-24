"""
Redis HashMap and JSON Utility Module

Production-ready Redis utility class for hash map operations using redis-py.
Requirements: pip install redis
"""

import random
import string
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import redis
from passlib.hash import pbkdf2_sha256

DEFAULT_REDIS_URL = "redis://localhost:6379/0"


class RedisJSONDB:
    """
    Redis utility class for JSON document operations using RedisJSON.

    This class provides a high-level interface for storing, retrieving, updating,
    and deleting JSON documents in Redis using the RedisJSON module.
    """

    def __init__(
        self, collection_name: str, connection_url: str = DEFAULT_REDIS_URL
    ) -> None:
        """
        Initialize the RedisJSONDB instance.

        Args:
            collection_name (str): The name of the collection (prefix for keys).
            connection_url (str): Redis connection URL.
        """
        self.client = redis.Redis.from_url(connection_url, decode_responses=True)
        self.collection_name = collection_name

    @staticmethod
    def hashit(data: str) -> str:
        """
        Hash a string using PBKDF2-SHA256.

        Args:
            data (str): The string to hash.

        Returns:
            str: The hashed string.
        """
        return pbkdf2_sha256.hash(data)

    @staticmethod
    def verify_hash(password: str, hashed_password: str) -> bool:
        """
        Verify a password against a hashed password.

        Args:
            password (str): The plain password to verify.
            hashed_password (str): The hashed password to compare against.

        Returns:
            bool: True if the password matches the hash, False otherwise.
        """
        return pbkdf2_sha256.verify(password, hashed_password)

    @staticmethod
    def gen_string(length: int = 15) -> str:
        """
        Generate a random alphanumeric string.

        Args:
            length (int): Length of the generated string. Default is 15.

        Returns:
            str: The generated random string.
        """
        characters = string.ascii_letters + string.digits
        return "".join(random.choices(characters, k=length))

    @staticmethod
    def gen_uuid() -> str:
        """
        Generate a new UUID string.

        Returns:
            str: The generated UUID.
        """
        return str(uuid4())

    def add_db(self, collection_name: str):
        """
        Change the collection name (key prefix) for subsequent operations.

        Args:
            collection_name (str): The new collection name.
        """
        self.collection_name = collection_name

    def insert(self, data: Dict[str, Any]) -> str:
        """
        Insert a new JSON document into the collection.

        Args:
            data (Dict[str, Any]): The document data to insert.

        Returns:
            str: The ID of the inserted document.
        """
        doc_id = data.get("id", self.gen_string())
        data["id"] = doc_id
        self.client.json().set(f"{self.collection_name}:{doc_id}", "$", data)
        return doc_id

    def insert_many(self, data_list: List[Dict[str, Any]]) -> List[str]:
        """
        Insert multiple JSON documents into the collection.

        Args:
            data_list (List[Dict[str, Any]]): List of document data to insert.

        Returns:
            List[str]: List of IDs of the inserted documents.
        """
        ids = []
        for data in data_list:
            ids.append(self.insert(data))
        return ids

    def insert_unique(self, filter: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """
        Insert a document only if it does not already exist based on a filter or ID.

        Args:
            filter (Dict[str, Any]): Filter to check for existing documents.
            data (Dict[str, Any]): Document data to insert.

        Returns:
            bool: True if the document was inserted, False if it already exists.
        """
        if self.count(filter) > 0 or "id" in data and self.get_by_id(data["id"]):
            return False
        self.insert(data)
        return True

    def filter(self, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve documents matching the given filter.

        Args:
            filter (Optional[Dict[str, Any]]): Dictionary of field-value pairs to filter by.

        Returns:
            List[Dict[str, Any]]: List of matching documents.
        """
        if "id" in (filter or {}):
            return self.get_by_id(filter["id"])
        keys = list(self.client.scan_iter(f"{self.collection_name}:*"))
        results = []
        for key in keys:
            doc = self.client.json().get(key)
            if not doc:
                continue
            doc = doc[0] if isinstance(doc, list) else doc
            if not filter or all(doc.get(k) == v for k, v in filter.items()):
                results.append(doc)
        return results

    def get(self, filter: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve the first document matching the given filter.

        Args:
            filter (Optional[Dict[str, Any]]): Dictionary of field-value pairs to filter by.

        Returns:
            Optional[Dict[str, Any]]: The first matching document, or None if not found.
        """
        docs = self.filter(filter)
        if isinstance(docs, list):
            return docs[0] if docs else None
        if isinstance(docs, dict) or hasattr(docs, "__dict__"):
            return docs
        return None

    def count(self, filter: Optional[Dict[str, Any]] = None) -> int:
        """
        Count the number of documents matching the given filter.

        Args:
            filter (Optional[Dict[str, Any]]): Dictionary of field-value pairs to filter by.

        Returns:
            int: The number of matching documents.
        """
        return len(self.filter(filter))

    def update(
        self,
        filter: Dict[str, Any],
        update_data: Dict[str, Any],
        multiple: bool = True,
        upsert: bool = False,
    ) -> Tuple[int, Union[List[Dict[str, Any]], Dict[str, Any]]]:
        """
        Update documents matching the filter with the provided data.

        Args:
            filter (Dict[str, Any]): Dictionary of field-value pairs to filter by.
            update_data (Dict[str, Any]): Data to update in matching documents.
            multiple (bool): Whether to update multiple documents. Default is True.
            upsert (bool): If True, insert the document if no match is found.

        Returns:
            Tuple[int, Union[List[Dict[str, Any]], Dict[str, Any]]]:
                Number of documents updated and the updated document(s).
        """
        if "id" in filter:
            doc = self.get_by_id(filter["id"])
            if not doc and upsert:
                doc_id = self.insert({**filter, **update_data})
                doc = self.client.json().get(f"{self.collection_name}:{doc_id}")
                return 1, doc
            elif not doc:
                return 0, None
            doc.update(update_data)
            self.client.json().set(f"{self.collection_name}:{doc['id']}", "$", doc)
            return 1, doc

        docs = self.filter(filter)
        updated_count = 0
        updated_docs = []
        for doc in docs:
            doc_id = doc["id"]
            doc.update(update_data)
            self.client.json().set(f"{self.collection_name}:{doc_id}", "$", doc)
            updated_count += 1
            updated_docs.append(doc)
            if not multiple:
                break
        if updated_count == 0 and upsert:
            doc_id = self.insert({**filter, **update_data})
            doc = self.client.json().get(f"{self.collection_name}:{doc_id}")
            updated_docs = [doc]
            updated_count = 1
        return updated_count, (
            updated_docs if multiple else (updated_docs[0] if updated_docs else None)
        )

    def delete(self, filter: Dict[str, Any]) -> int:
        """
        Delete documents matching the given filter.

        Args:
            filter (Dict[str, Any]): Dictionary of field-value pairs to filter by.

        Returns:
            int: The number of documents deleted.
        """
        if "id" in filter:
            doc = self.get_by_id(filter["id"])
            if not doc:
                return 0
            self.client.delete(f"{self.collection_name}:{filter['id']}")
            return 1
        docs = self.filter(filter)
        deleted = 0
        for doc in docs:
            doc_id = doc["id"]
            self.client.delete(f"{self.collection_name}:{doc_id}")
            deleted += 1
        return deleted

    def drop_db(self):
        """
        Delete all documents in the current collection.
        Returns:
            int: The number of documents deleted.
        """
        keys = list(self.client.scan_iter(f"{self.collection_name}:*"))
        if not keys:
            return 0
        deleted = self.client.delete(*keys)
        return deleted

    def drop_entire_db(self):
        """
        Delete all keys and data in the current Redis database (all collections, all types).
        """
        return self.client.flushdb()

    def get_keys(self) -> List[str]:
        """
        Retrieve all document IDs in the current collection.

        Returns:
            List[str]: List of document IDs.
        """
        keys = list(self.client.scan_iter(f"{self.collection_name}:*"))
        return [key.split(":", 1)[1] for key in keys]

    def close(self):
        """
        Close the Redis connection.
        """
        self.client.close()

    def get_by_id(self, _id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a document by its ID.

        Args:
            _id (str): The document ID.

        Returns:
            Optional[Dict[str, Any]]: The document if found, else None.
        """
        doc = self.client.json().get(f"{self.collection_name}:{_id}")
        return doc if doc else None

    def update_or_create(
        self, filter: Dict[str, Any], data: Dict[str, Any]
    ) -> (Dict[str, Any], bool):
        """
        Update the first document matching the filter, or create it if not found.

        Args:
            filter (Dict[str, Any]): Dictionary of field-value pairs to filter by.
            data (Dict[str, Any]): Data to update or create.

        Returns:
            Tuple[Dict[str, Any], bool]: The updated or created document and a boolean indicating creation (True if created).
        """
        docs = self.filter(filter)
        if docs:
            doc = docs[0]
            doc.update(data)
            self.client.json().set(f"{self.collection_name}:{doc['id']}", "$", doc)
            return doc, False
        else:
            doc_id = self.insert({**filter, **data})
            doc = self.client.json().get(f"{self.collection_name}:{doc_id}")
            return doc, True

    def get_or_create(
        self, filter: Dict[str, Any], data: Optional[Dict[str, Any]] = None
    ) -> (Dict[str, Any], bool):
        """
        Retrieve the first document matching the filter, or create it if not found.

        Args:
            filter (Dict[str, Any]): Dictionary of field-value pairs to filter by.
            data (Optional[Dict[str, Any]]): Data to use for creation if not found.

        Returns:
            Tuple[Dict[str, Any], bool]: The found or created document and a boolean indicating creation (True if created).
        """
        docs = self.filter(filter)
        if docs:
            return docs[0], False
        else:
            doc_id = self.insert({**filter, **(data or {})})
            doc = self.client.json().get(f"{self.collection_name}:{doc_id}")
            return doc, True


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
        return "".join(random.choices(characters, k=length))

    def insert(self, field: str, value: Any) -> bool:
        """
        Insert or update a single field in the hash.

        Args:
            field (str): Field name.
            value (Any): Value to set.

        Returns:
            bool: True if field is new in the hash and value was set, False if field existed and value was updated.
        """
        return self.client.hset(self.hash_name, field, str(value)) == 1

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

    def get_all_db(self) -> list:
        """
        Redis does not support multiple databases in the same way as MongoDB.
        This is a placeholder to match the MongoDB API.
        """
        return ["0"]  # Redis default DB is 0

    def get_all_collections(self, db_name: str = None) -> list:
        """
        Redis does not have collections, but we can list all keys.
        """
        return [key for key in self.client.scan_iter("*")]

    def add_db(self, hash_name: str):
        """
        Switch to a different hash (like switching collection).
        """
        self.hash_name = hash_name

    def insert_unique(self, field: str, value: Any) -> bool:
        """
        Insert a field only if it does not exist.
        """
        if self.exists(field):
            return False
        return self.insert(field, value)

    def filter(self, filter_dict: dict = None) -> dict:
        """
        Return all fields matching filter_dict (simple key-value match).
        """
        all_items = self.fetch()
        if not filter_dict:
            return all_items
        return {
            k: v
            for k, v in all_items.items()
            if all(item in all_items.items() for item in filter_dict.items())
        }

    def get(self, field: str) -> Any:
        """
        Get a single field value.
        """
        return self.fetch(field)

    def update(self, field: str, value: Any) -> bool:
        """
        Update a field (same as insert).
        """
        return self.insert(field, value)

    def update_many(self, mapping: dict) -> bool:
        """
        Update multiple fields (same as insert_many).
        """
        return self.insert_many(mapping)

    def delete(self, field: str) -> bool:
        """
        Delete a field.
        """
        return self.client.hdel(self.hash_name, field) == 1

    def drop_db(self):
        """
        Flush all keys in the current database.
        """
        self.client.flushdb()

    def drop_collection(self, hash_name: str = None):
        """
        Delete a hash (collection).
        """
        self.client.delete(hash_name or self.hash_name)

    def get_keys(self) -> list:
        """
        Get all field names in the hash.
        """
        return self.keys()

    def get_by_id(self, field: str) -> Any:
        """
        Get a field by its name (like id).
        """
        return self.fetch(field)

    def update_or_create(self, field: str, value: Any) -> (Any, bool):
        """
        Update a field if it exists, or create it.
        Returns (value, created: bool)
        """
        created = not self.exists(field)
        self.insert(field, value)
        return value, created

    def get_or_create(self, field: str, value: Any) -> (Any, bool):
        """
        Get a field if it exists, or create it.
        Returns (value, created: bool)
        """
        if self.exists(field):
            return self.fetch(field), False
        self.insert(field, value)
        return value, True

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