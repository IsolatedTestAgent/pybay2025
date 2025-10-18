"""
This is a CPU intensive computation, it favors multiprocessing over threading.
"""
# pyre-strict

import math
import multiprocessing as mp
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from ft_utils.local import LocalWrapper

# Get number of CPU cores
CPU_COUNT = mp.cpu_count()
WORK_RANGE = 500000
CHUNK_SIZE = WORK_RANGE // CPU_COUNT
# Large shared data that's expensive to copy between processes
SHARED_DATA = list(range(2000000))


def cpu_intensive_work(chunk_start: int, chunk_end: int, shared_data: list[int]) -> tuple[float, int]:
    def expensive_calculation(n: int) -> float:
        result = 0
        # Nested loops for serious CPU work
        for i in range(500):
            for j in range(5):  # Added another nested loop for more fun
                result += (
                    math.sin(n + i + j)
                    * math.cos(n - i + j)
                    * math.sqrt(abs(n - i * j + 1))
                    * math.log(abs(n + i + j + 1))
                )
        return result

    total = 0
    lookups = 0
    # Process the entire chunk assigned to this worker
    for num in range(chunk_start, chunk_end):
        # Expensive calculation
        calc_result = expensive_calculation(num)
        total += calc_result

        # Frequent access to shared data
        if num % 100 < len(shared_data):
            lookups += shared_data[num % 100]

    return total, lookups


def cpu_intensive_work_no_share(chunk_start: int, chunk_end: int) -> tuple[float, int]:
    def expensive_calculation(n: int) -> float:
        result = 0
        # Nested loops for serious CPU work
        for i in range(500):
            for j in range(5):  # Added another nested loop for more fun
                result += (
                    math.sin(n + i + j)
                    * math.cos(n - i + j)
                    * math.sqrt(abs(n - i * j + 1))
                    * math.log(abs(n + i + j + 1))
                )
        return result

    total = 0
    lookups = 0
    # Process the entire chunk assigned to this worker
    for num in range(chunk_start, chunk_end):
        # Expensive calculation
        calc_result = expensive_calculation(num)
        total += calc_result

    return total, lookups


def benchmarking(threading: bool = True) -> tuple[float, list[tuple[float, int]]]:
    # One chunk per CPU core
    chunks = [(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE) for i in range(CPU_COUNT)]

    results = []
    start_time = time.time()
    executor = ThreadPoolExecutor if threading else ProcessPoolExecutor
    with executor(max_workers=CPU_COUNT) as executor:
        futures = [
            executor.submit(cpu_intensive_work, start, end, SHARED_DATA)
            for start, end in chunks
        ]
        results = [f.result() for f in futures]

    return time.time() - start_time, results


def benchmarking_no_share(threading: bool = True) -> tuple[float, list[tuple[float, int]]]:
    # One chunk per CPU core
    chunks = [(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE) for i in range(CPU_COUNT)]

    results = []
    start_time = time.time()
    executor = ThreadPoolExecutor if threading else ProcessPoolExecutor
    with executor(max_workers=CPU_COUNT) as executor:
        futures = [
            executor.submit(cpu_intensive_work_no_share, start, end)
            for start, end in chunks
        ]
        results = [f.result() for f in futures]

    return time.time() - start_time, results


def main() -> None:
    print(f"CPU-intensive computation with shared data access")
    print(f"System specs:")
    print(f"  - CPU cores: {CPU_COUNT}")
    print(f"  - Shared data size: {len(SHARED_DATA):,} elements")
    print(f"  - Work per core: {CHUNK_SIZE} iterations")
    print(f"  - Data copied per process: {len(SHARED_DATA) * 8 / 1024 / 1024:.1f} MB\n")

    print("Running performance comparison...\n")

    print("No shared data...")
    time_no_share, results_no_share = benchmarking_no_share(False)
    print(f"Multiprocessing no share: {time_no_share:.2f}s")
    time_no_share, results_no_share = benchmarking_no_share(True)
    print(f"Threading no share: {time_no_share:.2f}s")
    return
    # Threading (should be slowest - GIL serializes CPU work)
    print("Testing threading...")
    time_thread, results_thread = benchmarking(True)
    # time_thread, results_thread = threading_approach()
    print(f"Threading:       {time_thread:.2f}s")

    # Multiprocessing (should be faster - true parallelism, but data copying overhead)
    print("Testing multiprocessing...")
    time_mp, results_mp = benchmarking(False)
    # time_mp, results_mp = multiprocessing_approach()
    print(f"Multiprocessing: {time_mp:.2f}s")

    if time_thread > time_mp:
        speedup = time_thread / time_mp
        print(f"\nMultiprocessing beat threading: {speedup:.1f}x faster \n")
    else:
        slowdown = time_mp / time_thread
        print(f"\nThreading beat multiprocessing: {slowdown:.1f}x \n")


if __name__ == "__main__":
    main()
