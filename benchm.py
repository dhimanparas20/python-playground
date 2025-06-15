import threading
import time
import random
import string
import asyncio
from pymongo_async import  AsyncMongoDB# Your sync class
from pymongo_sync import MongoDB


def generate_dummy_data(n):
    """Generate n dummy documents."""
    return [
        {
            "name": ''.join(random.choices(string.ascii_letters, k=8)),
            "age": random.randint(18, 65),
            "city": ''.join(random.choices(string.ascii_letters, k=6))
        }
        for _ in range(n)
    ]

def run_sync_insert(db, data):
    db.drop_collection()
    start = time.perf_counter()
    for doc in data:
        db.insert(doc)
    end = time.perf_counter()
    print(f"[PyMongo Sync] Inserted {len(data)} docs in {end - start:.4f} seconds")

def thread_worker(db, doc):
    db.insert(doc)

def run_threaded_insert(db, data):
    db.drop_collection()
    threads = []
    start = time.perf_counter()
    for doc in data:
        t = threading.Thread(target=thread_worker, args=(db, doc))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    end = time.perf_counter()
    print(f"[PyMongo Threaded] Inserted {len(data)} docs in {end - start:.4f} seconds")

async def run_async_insert(db, data):
    await db.drop_collection()
    start = time.perf_counter()
    for doc in data:
        await db.insert(doc)
    end = time.perf_counter()
    print(f"[PyMongo Async] Inserted {len(data)} docs in {end - start:.4f} seconds")
    await db.close()

if __name__ == "__main__":
    N = 5000
    dummy_data = generate_dummy_data(N)

    # 1. PyMongo Sync
    db_sync = MongoDB("benchdb", "benchcol", "mongodb://localhost:27017/")
    run_sync_insert(db_sync, dummy_data)
    db_sync.close()

    # 2. PyMongo Sync with Threading
    db_thread = MongoDB("benchdb", "benchcol", "mongodb://localhost:27017/")
    run_threaded_insert(db_thread, dummy_data)
    db_thread.close()

    # 3. PyMongo Async
    async def async_main():
        db_async = AsyncMongoDB("benchdb", "benchcol", "mongodb://localhost:27017/")
        await run_async_insert(db_async, dummy_data)
    asyncio.run(async_main())