# Python Concurrency & Parallelism — Complete Learning Guide

A beginner-friendly, hands-on exploration of all major Python concurrency models: **Sequential**, **Threading**, **Multiprocessing**, **ThreadPoolExecutor**, **ProcessPoolExecutor**, and **asyncio** — for both CPU-bound and I/O-bound workloads. Each file demonstrates real-world scenarios with `tqdm` progress bars and timing comparisons.

---

## Table of Contents

1. [Why Concurrency?](#why-concurrency)
2. [CPU-Bound vs I/O-Bound — The Key Distinction](#cpu-bound-vs-io-bound--the-key-distinction)
3. [File-by-File Breakdown](#file-by-file-breakdown)
4. [Comparison Summary Table](#comparison-summary-table)
5. [When to Use What](#when-to-use-what)
6. [Quick Reference](#quick-reference--common-patterns)

---

## Why Concurrency?

Python runs **line by line** by default. If you have 4 tasks that each take 5 seconds, the total is 20 seconds.

**Concurrency** lets you overlap waiting time or run tasks in parallel so the total time is much less — sometimes close to the time of a single task.

There are three main tools in Python:

| Tool | Best For | How It Works |
|------|----------|-------------|
| **Threading** | I/O-bound tasks (network, disk, API calls) | Multiple paths of execution **within one process** — shares memory, limited by GIL |
| **Multiprocessing** | CPU-bound tasks (calculations, data processing) | Separate **processes** with their own memory — bypasses the GIL, true parallelism |
| **asyncio** | I/O-bound tasks with structured cooperative concurrency | Single-threaded, single-process, but **awaits** I/O without blocking anything |

---

## CPU-Bound vs I/O-Bound — The Key Distinction

### CPU-Bound

The program spends most of its time **computing** — doing math, processing data. The CPU is the bottleneck.

```
[CPU][CPU][CPU][CPU][CPU][CPU]   →   CPU busy 100% of the time
```

**Examples:** Image processing, video encoding, data analysis, scientific simulations.

**Best tool: Multiprocessing** — each CPU core runs a separate process doing computation in true parallel.

**Worst tool: Threading** — the GIL prevents more than one thread from executing Python bytecode at a time.

### I/O-Bound

The program spends most of its time **waiting** — for a network response, a file read, a database query. The CPU is idle.

```
[WAIT][WAIT][WAIT][WAIT]   [CPU][WAIT][WAIT][WAIT][WAIT]
```

**Examples:** Downloading files, reading from disk, HTTP requests, database queries.

**Best tool: Threading or asyncio** — while one thread waits for I/O, another can use the CPU. The GIL is released during I/O waits.

### The Decisive Matrix

| Nature | Sequential | Threading | Multiprocessing | asyncio |
|--------|-----------|-----------|----------------|---------|
| CPU-bound (math, loops) | ✅ baseline | ❌ slower (GIL) | ✅ fastest | ❌ blocking |
| I/O-bound (network, disk) | ❌ slow | ✅ fast | ✅ fast (heavy) | ✅ fastest + lightweight |

---

## File-by-File Breakdown

### 1. `learn_threding.py` — Threading Basics

Spawns 4 threads, each counting with a 0.1s sleep per step. Compares sequential vs threaded time.

**Key Concepts:**

| Concept | Code | Explanation |
|---------|------|-------------|
| Create thread | `Thread(target=func, args=("a", 50))` | `target` is the function, `args` is a tuple |
| Start | `t1.start()` | Begins execution — returns immediately |
| Join | `t1.join()` | Block until thread finishes |
| Active count | `active_count()` | Number of currently running threads |
| Enumerate | `enumerate()` | List of all live `Thread` objects |
| Thread ID | `thread.ident` | Unique integer ID |
| Thread name | `thread.name` | Auto: `Thread-1` or custom |

**Why threading helps here:** `sleep(0.1)` represents I/O waiting. During sleep the GIL is released, so other threads run. 4 threads finish in ~the time of the longest single thread.

### 2. `learn_multiprocessing.py` — Multiprocessing Basics

Spawns 4 processes with the same counting task. Shows process inspection.

**Key Concepts:**

| Concept | Code | Explanation |
|---------|------|-------------|
| Create process | `Process(target=func, args=("a", 50))` | Same pattern as Thread |
| Start | `p.start()` | Launches a separate OS process |
| Join | `p.join()` | Wait for process to finish |
| Process ID | `current_process().pid` | OS-level PID (unique per process) |
| Active children | `active_children()` | List of alive child Process objects |

**Important:** The `if __name__ == "__main__"` guard is **required** on Windows to prevent infinite spawning.

**Key difference from threading:** Each `Process` has its **own Python interpreter**, its **own GIL**, its **own memory**. True parallel execution.

### 3. `cpu_bound_comp.py` — CPU-Bound Comparison

**The most important file** for understanding threading vs multiprocessing on CPU work. Runs `sum(i*i)` up to 90 million across 6 approaches.

**Approaches:**

| Approach | Expected | Why |
|----------|----------|-----|
| Sequential | 1× (baseline) | Single-threaded, no overhead |
| Threading | **~1× or slower** | **GIL!** Only one thread runs Python at a time. Can be slower than sequential due to contention. |
| Multiprocessing (manual) | **~0.25×** (4× faster) | Each process has its own GIL + CPU core |
| Multiprocessing (Pool) | **~0.25×** | Same benefit, cleaner syntax |
| asyncio (naive) | 1× (sequential) | Single-threaded, blocking code blocks everything |
| asyncio (with executor) | **~0.25×** | Offloads CPU work to a process pool |

```python
# Multiprocessing Pool — cleanest approach for CPU work
with Pool() as pool:
    pool.map(cpu_bound_task_pool, [N, N, N, N])
# pool.map() distributes inputs across available CPU cores
# On exit from `with`, all processes cleaned up automatically
```

```python
# asyncio + ProcessPoolExecutor — for async apps needing CPU work
loop = asyncio.get_event_loop()
await loop.run_in_executor(None, cpu_bound_task, name, n)
# run_in_executor(None, ...) uses default ThreadPoolExecutor
# Pass ProcessPoolExecutor() to use processes instead
```

### 4. `io_bound_comp.py` — I/O-Bound Comparison

Runs an I/O-bound task (`sleep` to simulate network/disk waits) across 5 approaches.

**Crucial:** `sleep()` releases the GIL, so **threading works well here** — unlike CPU-bound work.

**Approaches:**

| Approach | N=10, 4 tasks | Why |
|----------|---------------|-----|
| Sequential | 20.0s (10 × 0.5s × 4) | No concurrency |
| Threading | ~5.0s | All 4 sleep concurrently; GIL released during sleep |
| Multiprocessing | ~5.0s | True parallelism, but heavy overhead |
| asyncio | **~1.0s** | **Lightest** — single-threaded, no context switching |
| ThreadPoolExecutor | ~5.0s | Same as manual threading, cleaner API |

**Why asyncio is fastest for I/O:**
- Single thread, single process — **minimal overhead**
- No thread context switching
- No GIL contention (no threads at all)
- Everything driven by an event loop switching only at `await` points

### 5. `image_downloader.py` — Real-World Image Downloader

Downloads 10 images from [picsum.photos](https://picsum.photos/) using 5 approaches with **tqdm progress bars**.

**Approaches:**

| Approach | When to Use |
|----------|-------------|
| Sequential | Simple scripts, performance doesn't matter |
| Threading (manual) | Fine control over individual threads |
| **ThreadPoolExecutor** | **Best for I/O** — clean API, balanced work |
| Multiprocessing (manual) | Works but overkill for I/O |
| Multiprocessing Pool | Works but unnecessary overhead |

**Key code:**
```python
def download_image(image_name, url=URL):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    with open(f"{FOLDER}/{image_name}.jpg", "wb") as file:
        with tqdm(desc=image_name, total=total_size, unit='B',
                  unit_scale=True, unit_divisor=1024) as pbar:
            for data in response.iter_content(chunk_size=1024):
                file.write(data)
                pbar.update(len(data))
```

- `stream=True` — don't download entire response into memory
- `iter_content(chunk_size=1024)` — read 1KB at a time
- `tqdm` with `content-length` — real-time progress bar

**ThreadPoolExecutor vs manual Thread:**
```python
# Manual — you manage lifecycle
threads = [Thread(target=download_image, args=(f"t_{i}",)) for i in range(10)]
for t in threads: t.start()
for t in threads: t.join()

# ThreadPoolExecutor — cleaner, auto-manages
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(lambda i: download_image(f"pool_{i}"), range(10))
# Workers pick up tasks as they finish previous ones (load balancing)
```

---

## Comparison Summary Table

| Aspect | Threading | Multiprocessing | asyncio |
|--------|-----------|----------------|---------|
| **Execution** | Concurrent (GIL limits CPU) | Parallel (separate processes) | Concurrent (cooperative) |
| **Memory** | Shared (same process) | Separate (each process) | Shared (same process) |
| **GIL** | Affected (released on I/O) | Not affected (each has own) | Not affected (single thread) |
| **CPU-bound** | ❌ Slower than sequential | ✅ **Best choice** | ❌ Blocks event loop |
| **I/O-bound** | ✅ Good | ✅ Works (heavy) | ✅ **Best choice** |
| **Start overhead** | Low (~µs) | High (~ms, new OS process) | Very low (~µs) |
| **Data sharing** | Automatic (shared memory) | Requires Queue/Pipe/Value | Automatic |
| **Error isolation** | One crash = process crash | One crash = others survive | One exception = others continue |
| **Best for** | Network calls, file I/O | Processing, ML, heavy math | High-concurrency I/O, web servers |

---

## When to Use What — Decision Flowchart

```
What is your task doing most of the time?

├─ COMPUTING (math, loops, data processing):
│   └─ Use MULTIPROCESSING (Process/Pool)

├─ WAITING for I/O (network, disk, DB):
│   ├─ Async libraries available?
│   │   ├─ YES → Use ASYNCIO
│   │   └─ NO  → Use THREADING (Thread/ThreadPoolExecutor)
│   └─ Downloading many files?
│       └─ Use ThreadPoolExecutor (simplest)

└─ Both computing AND waiting:
    └─ Combine: ThreadPoolExecutor for I/O, ProcessPoolExecutor for CPU work
       e.g., asyncio + run_in_executor(ProcessPoolExecutor())
```

---

## Quick Reference — Common Patterns

```python
# ─── THREADING (I/O bound, lightweight) ───
from threading import Thread
threads = [Thread(target=work, args=(i,)) for i in range(4)]
for t in threads: t.start()
for t in threads: t.join()


# ─── THREAD POOL (I/O bound, cleaner) ───
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=4) as ex:
    results = list(ex.map(work, range(4)))


# ─── MULTIPROCESSING (CPU bound) ───
from multiprocessing import Process
procs = [Process(target=work, args=(i,)) for i in range(4)]
for p in procs: p.start()
for p in procs: p.join()


# ─── PROCESS POOL (CPU bound, cleaner) ───
from multiprocessing import Pool
with Pool() as pool:                    # auto-detects CPU count
    results = pool.map(work, [10_000_000] * 4)


# ─── ASYNCIO (I/O bound, high concurrency) ───
import asyncio
async def main():
    results = await asyncio.gather(work("A"), work("B"), work("C"))
asyncio.run(main())
```

---

## Beginner Pitfalls

| Pitfall | Explanation | Fix |
|---------|-------------|-----|
| **GIL surprise** | Threading doesn't speed up CPU work | Use multiprocessing for CPU tasks |
| **No `__main__` guard** | Processes crash on Windows | Wrap in `if __name__ == "__main__":` |
| **Shared data in threads** | Race conditions, corrupted data | Use `threading.Lock()` or `Queue` |
| **Too many threads** | Overhead, context switching | Use `ThreadPoolExecutor` with capped workers |
| **asyncio with blocking code** | Blocks the entire event loop | Use `loop.run_in_executor()` |
| **Forgetting `.join()`** | Main exits before work finishes | Always join threads/processes |
| **`Pool` without context manager** | Processes not cleaned up | Always use `with Pool() as pool:` |
