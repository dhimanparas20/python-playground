"""
Async MongoDB Utility Module

Production-ready MongoDB utility class using PyMongo's AsyncMongoClient.
Requirements: pymongo>=4.13.0, passlib
"""

from typing import Any, Dict, List, Optional, Union
from bson import ObjectId
from pymongo import ASCENDING
from pymongo import AsyncMongoClient
import random
import string
from passlib.hash import pbkdf2_sha256

DEFAULT_STRING = "mongodb://mongo:27017/"

class AsyncMongoDB:
    """
    Async MongoDB utility class for database and collection operations.
    """

    def __init__(self, db_name: str, collection_name: str, connection_str: str = DEFAULT_STRING) -> None:
        """
        Initialize AsyncMongoDB client and select database and collection.

        Args:
            db_name (str): Name of the database.
            collection_name (str): Name of the collection.
            connection_str (str): MongoDB connection string.
        """
        self.client = AsyncMongoClient(connection_str)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

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

    async def add_db(self, db_name: str, collection_name: str) -> None:
        """
        Switch to a different database and collection.

        Args:
            db_name (str): Database name.
            collection_name (str): Collection name.
        """
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    async def get_all_db(self) -> List[str]:
        """
        List all database names.

        Returns:
            List[str]: List of database names.
        """
        return await self.client.list_database_names()

    async def get_all_collections(self, db_name: Optional[str] = None) -> List[str]:
        """
        List all collection names in a database.

        Args:
            db_name (Optional[str]): Database name. If None, uses current db.

        Returns:
            List[str]: List of collection names.
        """
        db = self.client[db_name] if db_name else self.db
        return await db.list_collection_names()

    async def insert(self, data: Dict[str, Any], *args, **kwargs) -> str:
        """
        Insert a single document.

        Args:
            data (Dict[str, Any]): Document to insert.

        Returns:
            str: Inserted document ID.
        """
        result = await self.collection.insert_one(data, *args, **kwargs)
        return str(result.inserted_id)

    async def insert_many(self, data: List[Dict[str, Any]], *args, **kwargs) -> List[str]:
        """
        Insert multiple documents.

        Args:
            data (List[Dict[str, Any]]): List of documents.

        Returns:
            List[str]: List of inserted document IDs.
        """
        result = await self.collection.insert_many(data, *args, **kwargs)
        return [str(_id) for _id in result.inserted_ids]

    async def fetch(
        self,
        filter: Optional[Dict[str, Any]] = None,
        show_id: bool = False,
        *args, **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Fetch documents from the collection.

        Args:
            filter (Optional[Dict[str, Any]]): Query filter.
            show_id (bool): Whether to include '_id' in results.

        Returns:
            List[Dict[str, Any]]: List of documents.
        """
        projection = None if show_id else {"_id": 0}
        cursor = self.collection.find(filter or {}, projection, *args, **kwargs)
        result = []
        async for item in cursor:
            if show_id and "_id" in item:
                item["_id"] = str(item["_id"])
            result.append(item)
        return result

    async def count(self, filter: Optional[Dict[str, Any]] = None, *args, **kwargs) -> int:
        """
        Count documents matching a filter.

        Args:
            filter (Optional[Dict[str, Any]]): Query filter.

        Returns:
            int: Number of documents.
        """
        return await self.collection.count_documents(filter or {}, *args, **kwargs)

    async def update(self, filter: Dict[str, Any], update_data: Dict[str, Any], *args, **kwargs) -> int:
        """
        Update documents matching a filter.

        Args:
            filter (Dict[str, Any]): Query filter.
            update_data (Dict[str, Any]): Data to update.

        Returns:
            int: Number of documents modified.
        """
        if "_id" in filter and isinstance(filter["_id"], str):
            filter["_id"] = ObjectId(filter["_id"])
        result = await self.collection.update_many(filter, {"$set": update_data}, *args, **kwargs)
        return result.modified_count

    async def delete(self, filter: Dict[str, Any], *args, **kwargs) -> int:
        """
        Delete documents matching a filter.

        Args:
            filter (Dict[str, Any]): Query filter.

        Returns:
            int: Number of documents deleted.
        """
        if "_id" in filter and isinstance(filter["_id"], str):
            filter["_id"] = ObjectId(filter["_id"])
        result = await self.collection.delete_many(filter, *args, **kwargs)
        return result.deleted_count

    async def drop_db(self, db_name: Optional[str] = None) -> None:
        """
        Drop a database.

        Args:
            db_name (Optional[str]): Database name. If None, drops current db.
        """
        db_to_drop = db_name or self.db.name
        await self.client.drop_database(db_to_drop)

    async def drop_collection(self, collection_name: Optional[str] = None, db_name: Optional[str] = None) -> None:
        """
        Drop a collection.

        Args:
            collection_name (Optional[str]): Collection name. If None, uses current collection.
            db_name (Optional[str]): Database name. If None, uses current db.
        """
        db = self.client[db_name] if db_name else self.db
        coll = collection_name or self.collection.name
        await db.drop_collection(coll)

    async def get_keys(self, exclude_id: bool = True) -> List[str]:
        """
        Get list of keys in the first document of the collection.

        Args:
            exclude_id (bool): Whether to exclude '_id'.

        Returns:
            List[str]: List of keys.
        """
        doc = await self.collection.find_one()
        if not doc:
            return []
        keys = list(doc.keys())
        if exclude_id and '_id' in keys:
            keys.remove('_id')
        return keys

    async def close(self) -> None:
        """
        Close the MongoDB client connection.
        """
        await self.client.close()

    async def get_by_id(self, _id: Union[str, ObjectId]) -> Optional[Dict[str, Any]]:
        """
        Get a document by its ObjectId.

        Args:
            _id (Union[str, ObjectId]): Document ID.

        Returns:
            Optional[Dict[str, Any]]: The document, or None if not found.
        """
        if isinstance(_id, str):
            _id = ObjectId(_id)
        return await self.collection.find_one({"_id": _id})

    async def create_index(self, keys, **kwargs) -> str:
        """
        Create an index on the collection.

        Args:
            keys: List of tuples (field, direction) or a single field name.
            **kwargs: Additional index options.

        Returns:
            str: Name of the created index.
        """
        return await self.collection.create_index(keys, **kwargs)

# Usage Example (in an async context):
# db = AsyncMongoDB("AutomationBOT", "bot")
# await db.insert({"foo": "bar"})