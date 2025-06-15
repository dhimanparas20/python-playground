import threading
import multiprocessing
import asyncio
from time import perf_counter, sleep
from os import system

system("clear")

def io_bound_task(name, n):
    for i in range(n):
        sleep(0.5)  # Simulate I/O wait (e.g., network, disk)
    print(f"{name} done.")

def show_thread_details():
    print("\n[Threading Details]")
    print(f"Active threads: {threading.active_count()}")
    for thread in threading.enumerate():
        print(f"  Name: {thread.name}, Alive: {thread.is_alive()}, Ident: {thread.ident}")

def show_process_details(processes):
    print("\n[Multiprocessing Details]")
    print(f"Active child processes: {len(multiprocessing.active_children())}")
    for proc in processes:
        print(f"  Name: {proc.name}, Alive: {proc.is_alive()}, PID: {proc.pid}")

async def run_io_bound_asyncio(name, n):
    for i in range(n):
        await asyncio.sleep(0.1)  # Simulate async I/O wait
    print(f"{name} done.")

async def run_io_bound_in_threadpool(name, n, loop):
    # Offload to thread pool for demonstration (not needed for real async I/O)
    await loop.run_in_executor(None, io_bound_task, name, n)

if __name__ == "__main__":
    N = 10  # Number of I/O operations per task

    print("=== NORMAL (SEQUENTIAL) EXECUTION ===")
    start = perf_counter()
    io_bound_task("A", N)
    io_bound_task("B", N)
    io_bound_task("C", N)
    io_bound_task("D", N)
    print(normal_time := f"Time taken (sequential): {perf_counter() - start:.2f} seconds\n")

    print("=== THREADING EXECUTION ===")
    t1 = threading.Thread(target=io_bound_task, args=("A", N), name="Thread-A")
    t2 = threading.Thread(target=io_bound_task, args=("B", N), name="Thread-B")
    t3 = threading.Thread(target=io_bound_task, args=("C", N), name="Thread-C")
    t4 = threading.Thread(target=io_bound_task, args=("D", N), name="Thread-D")
    threads = [t1, t2, t3, t4]

    start = perf_counter()
    for t in threads:
        t.start()
    show_thread_details()
    for t in threads:
        t.join()
    print(thread_time := f"Time taken (threading): {perf_counter() - start:.2f} seconds\n")

    print("=== MULTIPROCESSING EXECUTION ===")
    p1 = multiprocessing.Process(target=io_bound_task, args=("A", N), name="Process-A")
    p2 = multiprocessing.Process(target=io_bound_task, args=("B", N), name="Process-B")
    p3 = multiprocessing.Process(target=io_bound_task, args=("C", N), name="Process-C")
    p4 = multiprocessing.Process(target=io_bound_task, args=("D", N), name="Process-D")
    processes = [p1, p2, p3, p4]

    start = perf_counter()
    for p in processes:
        p.start()
    show_process_details(processes)
    for p in processes:
        p.join()
    print(mp_time := f"Time taken (multiprocessing): {perf_counter() - start:.2f} seconds\n")

    print("=== ASYNCIO (NATIVE, RECOMMENDED FOR I/O-BOUND) ===")
    async def asyncio_native():
        await asyncio.gather(
            run_io_bound_asyncio("A", N),
            run_io_bound_asyncio("B", N),
            run_io_bound_asyncio("C", N),
            run_io_bound_asyncio("D", N),
        )
    start = perf_counter()
    asyncio.run(asyncio_native())
    print(asyncio_native_time := f"Time taken (asyncio native): {perf_counter() - start:.2f} seconds\n")

    print("=== ASYNCIO WITH THREAD POOL (FOR SYNC I/O IN ASYNC CODE) ===")
    async def asyncio_threadpool():
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            run_io_bound_in_threadpool("A", N, loop),
            run_io_bound_in_threadpool("B", N, loop),
            run_io_bound_in_threadpool("C", N, loop),
            run_io_bound_in_threadpool("D", N, loop),
        )
    start = perf_counter()
    asyncio.run(asyncio_threadpool())
    print(asyncio_pool_time := f"Time taken (asyncio + thread pool): {perf_counter() - start:.2f} seconds\n")

    print("=== SUMMARY ===")
    print(f"Sequential: {normal_time}")
    print(f"Threading: {thread_time}")
    print(f"Multiprocessing: {mp_time}")
    print(f"Asyncio (native): {asyncio_native_time} (best for async I/O)")
    print(f"Asyncio + ThreadPool: {asyncio_pool_time} (for running sync I/O in async code)\n")
