from modules.redis_util import RedisHashMap

# 1. BOOKS CRUD
books = RedisHashMap("library:books")
book_id = "book:1"
book_data = {
    "title": "The Pragmatic Programmer",
    "author": "Andrew Hunt",
    "year": "1999",
    "isbn": "978-0201616224"
}

# CREATE
books.insert(book_id, str(book_data))
print(f"Book created: {books.fetch(book_id)}")

# READ
print("All books:", books.fetch())  # fetch all books in the hash

# UPDATE
updated_book_data = {
    "title": "The Pragmatic Programmer",
    "author": "Andrew Hunt",
    "year": "1999",
    "isbn": "978-0201616224",
    "tags": "programming,software,best practices"
}
books.update(book_id, str(updated_book_data))
print(f"Book updated: {books.fetch(book_id)}")

# DELETE
books.delete(book_id)
print(f"Book deleted. All books now: {books.fetch()}")

# 2. AUTHORS CRUD
authors = RedisHashMap("library:authors")
author_id = "author:1"
author_data = {
    "name": "Andrew Hunt",
    "birth_year": "1964"
}

# CREATE
authors.insert(author_id, str(author_data))
print(f"Author created: {authors.fetch(author_id)}")

# READ
print("All authors:", authors.fetch())

# UPDATE
authors.update(author_id, str({"name": "Andrew Hunt", "birth_year": "1964", "country": "USA"}))
print(f"Author updated: {authors.fetch(author_id)}")

# DELETE
authors.delete(author_id)
print(f"Author deleted. All authors now: {authors.fetch()}")

# 3. USERS CRUD
users = RedisHashMap("library:users")
user_id = "user:1"
user_data = {
    "username": "alice",
    "email": "alice@example.com",
    "role": "member"
}

# CREATE
users.insert(user_id, str(user_data))
print(f"User created: {users.fetch(user_id)}")

# READ
print("All users:", users.fetch())

# UPDATE
users.update(user_id, str({"username": "alice", "email": "alice@example.com", "role": "admin"}))
print(f"User updated: {users.fetch(user_id)}")

# DELETE
users.delete(user_id)
print(f"User deleted. All users now: {users.fetch()}")

# --- Clean up ---
books.clear()
authors.clear()
users.clear()
books.close()
authors.close()
users.close()