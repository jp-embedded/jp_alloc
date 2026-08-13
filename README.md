# jp_alloc

A lock-free, EBR-protected, thread-caching memory allocator.

## Why?

Lock-free allocators based on Treiber stacks (single-linked free lists with
CAS on the head) suffer from the **ABA problem**: if thread A reads the head
pointer `P`, thenthreads B and C pop and re-push the same block so the head
returns to `P`, thread A's CAS succeeds even though the stack changed
underneath — it may install a stale head, corrupting the free list and causing
duplicate allocations or lost frees.

jp_alloc solves this with **Epoch-Based Reclamation (EBR)**: a popped block
cannot reappear at the free list head until every thread that could have
observed the old head has exited its critical section. This eliminates ABA
without 128-bit tagged pointers, hazard pointers, or platform-specific
intrinsics like `cmpxchg16b`.

## How it works

### Compare-and-Swap (CAS)

CAS is the fundamental atomic operation: it atomically compares a memory
location to an expected value and, if they match, replaces it with a new
value. If another thread changed the location in between, the CAS fails and
the caller retries. jp_alloc uses 64-bit CAS (single-word, available on all
modern CPUs) for all free list operations.

### Epoch-Based Reclamation (EBR)

EBR is a deferred-reclamation scheme. Each thread announces an "epoch" (a
global counter) before entering a critical section where it reads shared
data (the free list head). The global epoch advances only when all threads
have moved past the oldest epoch. Blocks that were "retired" (removed from
the free list) are not reused until two epoch advances have passed —
guaranteeing no thread still holds a dangling reference to them.

jp_alloc uses a **3-epoch ring**: retired blocks are stored in one of three
slots indexed by `retire_epoch % 3`. When the epoch advances from `N` to
`N+1`, the slot for epoch `N-1` becomes safe to drain — every active thread
has announced epoch `≥ N`, so none can still hold a reference to a block
retired at epoch `N-1`.

### Thread-local cache

Each thread has a fixed-array cache (32 slots per size class). Allocations
pull from the cache; frees push to the cache — both with **no atomics**
(plain pointer arithmetic on thread-local memory). The cache intercepts
~100% of hot-path operations in balanced alloc/free workloads.

### Batched refill

When the cache misses (empty), it pops up to 16 blocks from the global free
list in a **single CAS** by walking the linked list and swinging the head
past the batch. One CAS amortizes across ~16 future cache misses.

### Buddy splitting

Memory is organized into power-of-2 size classes (1, 2, 4, ..., 8388608 bytes).
When a pool is empty, a block from the next-larger pool is split in half —
one half is returned to the caller, the other goes into the empty pool.

### Demand paging and pool count

The largest pool (8MB) is populated via a single `mmap`. The buddy-split
cascade from that block only touches ~7 pages (28KB) for headers — the
remaining ~8MB stays untouched and costs **no physical RSS** under demand
paging (the OS only allocates physical memory when a page is actually
read or written).

The cascade frequency is exponential: pool N drains 2× less often than
pool N-1 (each split produces 2 blocks, serving 2 future allocations
before the next drain). So the additional pools (17-23) drain
2^(N-6) ≈ 131K times less often than pool 6 — essentially never for
most workloads. Their 8MB mmap is a one-time event, and future
allocations reuse spares from intermediate pools.

## Performance

Benchmark: mixed small-object alloc/free (80B, 48B, 256B, 128B with memset
initialization) plus large malloc/free churn (1KB–32KB), 50,000 operations
per thread. See `jp_alloc_bench.c`.

Test machine: Intel Core i5-8250U (4 cores / 8 threads), 16 GB RAM,
Linux x86_64. All allocators built with `-O2` and tested via `LD_PRELOAD`.

### 1 thread

| Allocator    | Throughput | p99 latency | Peak RSS |
|--------------|-----------|-------------|----------|
| jp_alloc     | 4.31 Mops/s | 384 ns     | 2.0 MB   |
| mimalloc     | 5.83 Mops/s | 768 ns     | 2.3 MB   |
| jemalloc     | 4.40 Mops/s | 1536 ns    | 3.5 MB   |
| tcmalloc     | 4.09 Mops/s | 768 ns     | 7.1 MB   |
| glibc malloc | 2.94 Mops/s | 768 ns     | 1.7 MB   |

### 8 threads

| Allocator    | Throughput | p99 latency | Peak RSS |
|--------------|-----------|-------------|----------|
| jp_alloc     | 16.75 Mops/s | 768 ns    | 3.7 MB   |
| mimalloc     | 14.56 Mops/s | 768 ns    | 5.9 MB   |
| jemalloc     | 11.95 Mops/s | 768 ns    | 9.0 MB   |
| tcmalloc     | 11.14 Mops/s | 1536 ns   | 9.7 MB   |
| glibc malloc | 10.10 Mops/s | 1536 ns   | 2.8 MB   |

### 64 threads

| Allocator    | Throughput | p99 latency | Peak RSS |
|--------------|-----------|-------------|----------|
| jp_alloc     | 24.93 Mops/s | 768 ns    | 13 MB    |
| tcmalloc     | 24.91 Mops/s | 768 ns    | 31 MB    |
| mimalloc     | 22.19 Mops/s | 768 ns    | 28 MB    |
| jemalloc     | 20.51 Mops/s | 768 ns    | 21 MB    |
| glibc malloc | 13.13 Mops/s | 768 ns    | 11 MB    |

### 300 threads

| Allocator    | Throughput | p99 latency | Peak RSS |
|--------------|-----------|-------------|----------|
| jp_alloc     | 24.90 Mops/s | 384 ns     | 36 MB    |
| mimalloc     | 25.31 Mops/s | 768 ns     | 88 MB    |
| jemalloc     | 21.41 Mops/s | 768 ns     | 46 MB    |
| tcmalloc     | 17.22 Mops/s | 196 µs     | 145 MB   |
| glibc malloc | 12.28 Mops/s | 1536 ns    | 23 MB    |

**Throughput**: jp_alloc beats glibc by 103% and jemalloc by 16% at 300
threads. At 8 threads jp_alloc is the fastest of all allocators (16.75
Mops/s). Only mimalloc edges jp_alloc at 300 threads (by 2%).

**p99 latency**: jp_alloc has the lowest p99 of all tested allocators
at 300 threads (384 ns — 2× better than jemalloc/mimalloc, 500× better
than tcmalloc which spikes to 196 µs under heavy contention).

**RSS**: jp_alloc uses the least physical memory among the fast
allocators at every thread count. At 300 threads: 36 MB vs mimalloc 88 MB
(2.4× less), jemalloc 46 MB (1.3× less), tcmalloc 145 MB (4× less).
Only glibc is lighter (23 MB) but glibc is 2× slower.

**p99 latency** = 99th-percentile per-operation latency. Lower is better.

**RSS** = peak resident set size (physical memory used). Only pages
actually touched by buddy-split headers count toward RSS — untouched
pages in the 8MB pool reserve cost zero physical memory under demand
paging.

## Usage

### As an LD_PRELOAD library

```sh
make
LD_PRELOAD=./jp_alloc.so your_program
```

This overrides `malloc`/`free`/`calloc`/`realloc` globally, so all
allocations in the process route through jp_alloc's pools.

### Linked directly

```c
#define JP_ALLOC_IMPLEMENTATION
#include "jp_alloc.h"

/* Use malloc/free as normal — jp_alloc overrides them */
void *buf = malloc(1024);
free(buf);

/* Or use the jp_alloc_* API directly */
void *aligned = jp_alloc_aligned(64, 1024);
jp_free(aligned);
size_t sz = jp_good_size(100); /* actual block size for a request */
```

### Debug mode

```sh
make jp_allocd.so   # debug build with self-checks
LD_PRELOAD=./jp_allocd.so your_program
```

Under `-DJP_ALLOC_DEBUG`, the allocator aborts on:
- **ABA**: a popped block is still marked as live (another thread is using it)
- **Double-free**: a block is freed twice
- **Corruption**: a block's magic number doesn't match

If the program runs to completion without aborting, no bugs were detected.

## Benchmark

```sh
make bench           # builds jpbench (release) + jpbenchd (debug)
./jpbench            # 300 threads, 50k ops/thread, prints throughput/latency/RSS
JPBENCH_THREADS=8 JPBENCH_OPS=20000 ./jpbench    # custom config
JPBENCH_MODE=alloc-heavy ./jpbench                 # stress the refill path
./jpbenchd           # debug build — aborts on ABA/double-free/corruption
```

To compare against other allocators:

```sh
cc -O2 -DJP_ALLOC_BENCH jp_alloc_bench.c -o jpbench_stock -lpthread -lrt -lm
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so ./jpbench_stock
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libmimalloc.so  ./jpbench_stock
./jpbench_stock   # glibc malloc baseline (no LD_PRELOAD)
```

## Configuration

Compile-time flags (all optional):

| Flag | Default | Description |
|------|---------|-------------|
| `JP_ALLOC_DEBUG` | off | Enable ABA/double-free/corruption self-checks |
| `JP_CACHE_N` | 32 | Per-thread cache slots per size class |
| `JP_REFILL` | 16 | Blocks per global free list refill CAS |
| `JP_ALLOC_POOL_COUNT` | 24 | Power-of-2 pool classes (1B..8M) |
| `JP_CACHELINE` | 64 | Cache-line size for alignment padding |

## Platform support

| Platform | Status |
|----------|--------|
| Linux (x86_64, ARM64, 32-bit) | Tested |
| macOS (Intel, Apple Silicon) | Should work (pthreads + mmap) |
| Windows (MinGW) | Should work (VirtualAlloc + WinPthread) |
| MSVC | Fallbacks provided for `__builtin_clzll`; needs pthread shim |
| 32-bit embedded | `JP_CACHELINE` overridable for 32-byte cache lines |

Requires: GCC 4.7+, Clang 3.0+, or MSVC 2015+. C11 compiler support
(`_Thread_local`, `_Alignas`, `_Atomic`). POSIX (pthreads) or Windows with
MinGW (WinPthread).

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).