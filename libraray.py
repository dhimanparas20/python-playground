from pydantic import BaseModel, Field, ValidationError, SecretStr
from typing import List, Optional
from bson import ObjectId
from pymongo_sync import MongoDB  # Your module

class AuthorModel(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    birth_year: Optional[int] = Field(None, ge=1800, le=2100)
    password: SecretStr = Field(..., min_length=8, max_length=20)

class BookModel(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    author: AuthorModel
    year: int = Field(..., ge=1000, le=2100)
    tags: List[str] = []
    isbn: Optional[str] = Field(None, min_length=10, max_length=17)

# Initialize MongoDB
db = MongoDB("library", "books", "mongodb://localhost:27017/")

raw_data = {
    "title": "The Pragmatic Programmer",
    "author": {"name": "Andrew Hunt", "birth_year": 1964, "password": "12345678"},
    "year": 1999,
    "tags": ["programming", "software", "best practices"],
    "isbn": "978-0201616224"
}

try:
    # Validate and parse data
    book = BookModel(**raw_data)
    book_dict = book.model_dump()
    print("title" in book_dict)
    # Hash the password before inserting
    plain_password = book.author.password.get_secret_value()
    book_dict['author']['password'] = db.hashit(plain_password)
    inserted_id = db.insert(book_dict)
    print(f"Inserted book with id: {inserted_id}")
    db.drop_collection()
    db.close()
except ValidationError as e:
    print("Validation failed:", e)
