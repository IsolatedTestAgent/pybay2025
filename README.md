# PyBay 2025 - Python Free-Threading Benchmarks

This folder contains benchmarks demonstrating the performance characteristics of Python's free-threading implementation compared to traditional threading and multiprocessing approaches.

## Prerequisites

- Python 3.13+ with free-threading enabled (`python3.13t`). Prefer 3.14+ for better performance.
- The `ft_utils` library installed

## Setting Up Virtual Environments

To properly compare free-threading vs GIL-enabled Python, you should set up separate virtual environments for each version using Python's built-in `venv` module.

### Installing Python 3.14

First, ensure you have both Python 3.14 variants installed on your system:
- **python3.14t** - Free-threading build (GIL disabled)
- **python3.14** - Standard build (GIL enabled)

You can download these from:
- [python.org downloads](https://www.python.org/downloads/)
- Build from source with the appropriate configure flags

### Creating Virtual Environments

Create two separate virtual environments for comparison using the standard `venv` module:

#### 1. Free-Threading Environment (GIL disabled)

```bash
# Create virtual environment with Python 3.14t
python3.14t -m venv .pybay-ft

# Create virtual environment with Python 3.14
python3.14 -m venv .pybay-gil
```

### What to Expect

- **Free-threading**: Threading will show true parallelism for CPU-bound work, competing with or beating multiprocessing
- **GIL-enabled**: Threading will be serialized by the GIL, making multiprocessing significantly faster for CPU-bound tasks
- **Key difference**: The performance gap between threading and multiprocessing should be much smaller (or reversed) in free-threading mode

## Benchmarks

### 1. CPU-Intensive Benchmark (`benchmark_cpu.py`)

This benchmark demonstrates how the GIL affects CPU-bound workloads and compares threading vs multiprocessing performance.

#### What it demonstrates:
- **CPU-intensive computation**: Performs heavy mathematical operations (trigonometric functions, square roots, logarithms) across multiple nested loops
- **GIL impact**: Shows how the Global Interpreter Lock serializes CPU work in traditional Python threading, making multiprocessing faster for CPU-bound tasks
- **Shared data overhead**: Compares performance with and without shared data access to illustrate the cost of copying large data structures between processes

#### How to run:
```bash
python benchmark_cpu.py
```

#### What to expect:
The benchmark will:
1. Run CPU-intensive calculations using both threading and multiprocessing
2. Report timing results showing multiprocessing typically outperforms threading for CPU-bound work (due to GIL limitations in traditional Python)

---

### 2. Work-Stealing with Shared State Benchmark (`benchmark_rw.py`)

This benchmark showcases the advantages of Python's free-threading with `ft_utils` over traditional multiprocessing when working with shared mutable state.

#### What it demonstrates:
- **Work-stealing**: Uses a shared work queue where threads dynamically pull tasks, providing better load balancing than static chunk assignment (especially with unbalanced workloads)
- **Shared mutable state**: Demonstrates three approaches to managing shared state:
  - `ft_utils` concurrent structures (ConcurrentDict, AtomicInt64) - most efficient
  - Standard library locks (threading.Lock) - explicit locking overhead
  - No locks - shows race conditions and data corruption
- **Multiprocessing overhead**: Shows the massive cost of sharing mutable state across processes using Manager.dict() (IPC, serialization, context switches)
- **Unbalanced workload**: Every 10th task is 10x more expensive, making work-stealing far more efficient than static partitioning

#### How to run:

**Default (ft_utils vs multiprocessing):**
```bash
python benchmark_rw.py
```

**Compare all approaches:**
```bash
python benchmark_rw.py --compare-all
```

**Test standard library locks:**
```bash
python benchmark_rw.py --use-stdlib-locks
```

**Demonstrate race conditions (no locks):**
```bash
python benchmark_rw.py --no-locks
```

**Adjust workload size:**
```bash
python benchmark_rw.py --work-range 100000 --shared-data-size 5000000
```

#### Available flags:
- `--work-range N`: Total number of work items to process (default: 50000)
- `--shared-data-size N`: Size of shared data array (default: 2000000)
- `--compare-all`: Run all benchmarks (ft_utils, stdlib locks, and multiprocessing)
- `--use-stdlib-locks`: Use standard library locks instead of ft_utils
- `--no-locks`: Run without locks to demonstrate race conditions (WARNING: produces incorrect results)

#### What to expect:
- **ft_utils approach**: Fastest for shared state operations, using efficient sharded data structures and lock-free atomic operations
- **stdlib locks approach**: Moderate performance, with explicit locking overhead and higher contention
- **Multiprocessing approach**: Slowest due to Manager.dict() IPC overhead for every shared state access
- **No locks approach**: Fastest execution but with severe data corruption (lost updates, incorrect counts)

## Key Takeaways

1. **CPU-bound work in traditional Python**: Multiprocessing wins over threading due to the GIL
2. **Free-threading with shared state**: Threading with `ft_utils` can outperform multiprocessing when:
   - Workload is unbalanced (work-stealing helps)
   - Frequent shared state access is needed
   - Shared data is large (avoiding process copying overhead)
3. **Lock-free structures matter**: `ft_utils.AtomicInt64` and `ft_utils.ConcurrentDict` provide significant performance improvements over standard library locks
4. **Race conditions are real**: Without proper synchronization, concurrent writes cause data loss and corruption

## More Information

For detailed documentation of `ft_utils` at https://github.com/facebookincubator/ft_utils/blob/main/docs/index.md .

Free-threading information: https://py-free-threading.github.io/

Discord: https://discord.gg/f4TRGraYBZ
