import threading
import multiprocessing
from multiprocessing import Pool
import asyncio
from time import perf_counter
from os import system

system("clear")

def cpu_bound_task(name, n):
    total = 0
    for i in range(n):
        total += i * i
    print(f"{name} done. Result: {total}")

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

def cpu_bound_task_pool(n):
    # For Pool, we can't pass a name easily, so just print the PID
    total = 0
    for i in range(n):
        total += i * i
    print(f"PID {multiprocessing.current_process().pid} done. Result: {total}")
    return total

async def run_cpu_bound_asyncio(name, n):
    # This will block the event loop and run sequentially!
    cpu_bound_task(name, n)

async def run_cpu_bound_in_processpool(name, n, loop):
    # This will offload the CPU-bound task to a process pool
    await loop.run_in_executor(None, cpu_bound_task, name, n)

if __name__ == "__main__":
    N = 90_000_000  # Adjust for your CPU

    print("=== NORMAL (SEQUENTIAL) EXECUTION ===")
    start = perf_counter()
    cpu_bound_task("A", N)
    cpu_bound_task("B", N)
    cpu_bound_task("C", N)
    cpu_bound_task("D", N)
    print(normal_time := f"Time taken (sequential): {perf_counter() - start:.2f} seconds\n")

    print("=== THREADING EXECUTION ===")
    t1 = threading.Thread(target=cpu_bound_task, args=("A", N), name="Thread-A")
    t2 = threading.Thread(target=cpu_bound_task, args=("B", N), name="Thread-B")
    t3 = threading.Thread(target=cpu_bound_task, args=("C", N), name="Thread-C")
    t4 = threading.Thread(target=cpu_bound_task, args=("D", N), name="Thread-D")
    threads = [t1, t2, t3, t4]

    start = perf_counter()
    for t in threads:
        t.start()
    show_thread_details()
    for t in threads:
        t.join()
    print(thread_time := f"Time taken (threading): {perf_counter() - start:.2f} seconds\n")

    print("=== MULTIPROCESSING EXECUTION ===")
    p1 = multiprocessing.Process(target=cpu_bound_task, args=("A", N), name="Process-A")
    p2 = multiprocessing.Process(target=cpu_bound_task, args=("B", N), name="Process-B")
    p3 = multiprocessing.Process(target=cpu_bound_task, args=("C", N), name="Process-C")
    p4 = multiprocessing.Process(target=cpu_bound_task, args=("D", N), name="Process-D")
    processes = [p1, p2, p3, p4]

    start = perf_counter()
    for p in processes:
        p.start()
    show_process_details(processes)
    for p in processes:
        p.join()
    print(mp_time := f"Time taken (multiprocessing): {perf_counter() - start:.2f} seconds\n")

    print("=== MULTIPROCESSING POOL EXECUTION ===")
    start = perf_counter()
    with Pool(processes=4) as pool:
        # Pool.map expects a function and an iterable of arguments
        pool.map(cpu_bound_task_pool, [N, N, N, N])
    print(pool_time := f"Time taken (multiprocessing.Pool): {perf_counter() - start:.2f} seconds\n")

    print("=== ASYNCIO (DIRECT, NOT RECOMMENDED FOR CPU-BOUND) ===")
    async def asyncio_direct():
        await asyncio.gather(
            run_cpu_bound_asyncio("A", N),
            run_cpu_bound_asyncio("B", N),
            run_cpu_bound_asyncio("C", N),
            run_cpu_bound_asyncio("D", N),
        )
    start = perf_counter()
    asyncio.run(asyncio_direct())
    print(asyncio_direct_time := f"Time taken (asyncio direct): {perf_counter() - start:.2f} seconds\n")

    print("=== ASYNCIO WITH PROCESS POOL (RECOMMENDED FOR CPU-BOUND) ===")
    async def asyncio_processpool():
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            run_cpu_bound_in_processpool("A", N, loop),
            run_cpu_bound_in_processpool("B", N, loop),
            run_cpu_bound_in_processpool("C", N, loop),
            run_cpu_bound_in_processpool("D", N, loop),
        )
    start = perf_counter()
    asyncio.run(asyncio_processpool())
    print(asyncio_pool_time := f"Time taken (asyncio + process pool): {perf_counter() - start:.2f} seconds\n")

    print("=== SUMMARY ===")
    print(f"Sequential: {normal_time}")
    print(f"Threading: {thread_time}")
    print(f"Multiprocessing: {mp_time}")
    print(f"Multiprocessing.Pool: {pool_time}")
    print(f"Asyncio (direct): {asyncio_direct_time} (runs sequentially, not suitable for CPU-bound)")
    print(f"Asyncio + ProcessPool: {asyncio_pool_time} (uses multiple processes, similar to multiprocessing)\n")
