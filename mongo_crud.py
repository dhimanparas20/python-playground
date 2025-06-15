import asyncio
from pymongo_async import AsyncMongoDB

# --- CRUD Operations Demo ---

async def main():
    # Initialize the async MongoDB client
    db = AsyncMongoDB("testdb", "testcollection", "mongodb://localhost:27017/")

    # --- CREATE ---
    print("\n--- CREATE ---")
    doc = {"name": "Alice", "age": 30, "city": "Wonderland"}
    inserted_id = await db.insert(doc)
    print(f"Inserted document ID: {inserted_id}")

    # Insert many
    docs = [
        {"name": "Bob", "age": 25, "city": "Builderland"},
        {"name": "Charlie", "age": 35, "city": "Chocolate Factory"},
    ]
    inserted_ids = await db.insert_many(docs)
    print(f"Inserted multiple document IDs: {inserted_ids}")

    # --- READ ---
    print("\n--- READ ---")
    all_docs = await db.fetch(show_id=True)
    print("All documents:")
    for d in all_docs:
        print(d)

    # Fetch by filter
    filtered_docs = await db.fetch({"age": {"$gte": 30}}, show_id=True)
    print("Filtered documents (age >= 30):")
    for d in filtered_docs:
        print(d)

    # Fetch by ID
    doc_by_id = await db.get_by_id(inserted_id)
    print(f"Document by ID ({inserted_id}): {doc_by_id}")

    # --- UPDATE ---
    print("\n--- UPDATE ---")
    update_count = await db.update({"name": "Alice"}, {"city": "New Wonderland"})
    print(f"Documents updated: {update_count}")

    updated_doc = await db.get_by_id(inserted_id)
    print(f"Updated document: {updated_doc}")

    # --- COUNT ---
    print("\n--- COUNT ---")
    count = await db.count()
    print(f"Total documents in collection: {count}")

    # --- DELETE ---
    print("\n--- DELETE ---")
    delete_count = await db.delete({"name": "Bob"})
    print(f"Documents deleted: {delete_count}")

    # --- FINAL STATE ---
    print("\n--- FINAL STATE ---")
    all_docs = await db.fetch(show_id=True)
    print("All documents after deletion:")
    for d in all_docs:
        print(d)

    # --- CLEANUP (optional) ---
    await db.drop_collection()
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())
