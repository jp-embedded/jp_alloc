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

### Fragmentation

The power-of-2 size-class design eliminates **external fragmentation**:
every free block in a pool is exactly the same size, so any allocation
request that maps to pool N can be satisfied by any free block in that
pool — no best-fit scanning, no "holes" that are too small for the
next request. A freed 128B block can serve any future `malloc(65..128)`,
and a freed 256B block can serve any future `malloc(129..256)`.

The trade-off is **internal fragmentation** (waste within a block): a
65-byte allocation gets a 128-byte block, wasting 63 bytes (49%). However,
this waste is **bounded and predictable** — the worst case is always 50%
per allocation. Fine-grained size classes (e.g., jemalloc's 48, 64, 80,
96, 112, 128) reduce internal waste but introduce external fragmentation:
a freed 48-byte block can't serve a 64-byte request, so the 48-byte pool
accumulates free blocks while the 64-byte pool is starved. Power-of-2
pools trade higher per-block waste for zero external fragmentation and
O(1) allocation — no scanning, no searching, just pop from the pool.

Buddy splitting further reduces fragmentation by allowing larger blocks
to be split for smaller needs on demand. When a pool is empty, the next
larger pool donates a block — no separate per-size slabs are held idle.
Magazines recycle blocks within their size class, keeping the flow
efficient without needing coalescing (merging freed buddies back into
larger blocks). The 8MB pool reserve is demand-paged, so uncoalesced
small blocks sitting in magazine lists cost only the pages they touch —
the rest of the reserve stays uncommitted.

### Magazines

The global freelist for each pool is a linked list of **magazines** —
each magazine is a struct holding 16 block pointers in an array. Refill
pops one magazine (1 CAS) and memcpys 16 pointers into the TLS cache.
Flush memcpys 16 pointers from the TLS cache into a magazine and
CAS-pushes it to the global list. No dependent-load walks, no in-band
chain-building loops.

Magazines are allocated from the pool system (pool 8 = 256B blocks,
which fit the ~136-byte magazine struct + 16-byte header). A CAS-based
free-list recycles magazines — no static arrays, no mmap, no mutex.
The first magazine allocation triggers one buddy-split from the 8MB
pool reserve, populating ~16K magazines from a single mmap.

Each thread keeps a unified stack of empty magazines across all pools.
Refills push empty magazines here; flushes pop them. The stack stays
small (~active pool count) because refills and flushes naturally
alternate in balanced workloads.

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
initialization) plus large malloc/free churn (1KB–32KB), 10 second timed runs
(steady state). See `jp_alloc_bench.c`.

Test machine: Intel Core i5-8250U (4 cores / 8 threads), 16 GB RAM,
Linux x86_64. All allocators built with `-O2` and tested via `LD_PRELOAD`.
Results are median of 3 runs.

### 1 thread

| Allocator    | Throughput | Peak RSS |
|--------------|-----------|----------|
| jp_alloc     | 5.13 Mops/s | 2.1 MB   |
| tcmalloc     | 5.33 Mops/s | 7.4 MB   |
| mimalloc     | 4.75 Mops/s | 2.4 MB   |
| jemalloc     | 3.68 Mops/s | 4.0 MB   |
| glibc malloc | 2.27 Mops/s | 1.9 MB   |

### 8 threads

| Allocator    | Throughput | Peak RSS |
|--------------|-----------|----------|
| jp_alloc     | 19.48 Mops/s | 4.0 MB  |
| mimalloc     | 16.44 Mops/s | 6.0 MB  |
| jemalloc     | 15.16 Mops/s | 10.7 MB |
| tcmalloc     | 9.99 Mops/s  | 11.4 MB |
| glibc malloc | 7.99 Mops/s  | 2.9 MB  |

### 64 threads

| Allocator    | Throughput | Peak RSS |
|--------------|-----------|----------|
| jp_alloc     | 22.67 Mops/s | 19.5 MB |
| mimalloc     | 18.86 Mops/s | 35.9 MB |
| jemalloc     | 17.38 Mops/s | 50.1 MB |
| tcmalloc     | 12.72 Mops/s | 45.1 MB |
| glibc malloc | 9.28 Mops/s  | 12.9 MB |

### 300 threads

| Allocator    | Throughput | Peak RSS |
|--------------|-----------|----------|
| jp_alloc     | 31.32 Mops/s | 87 MB    |
| mimalloc     | 19.44 Mops/s | 161 MB   |
| jemalloc     | 17.83 Mops/s | 156 MB   |
| tcmalloc     | 9.82 Mops/s  | 178 MB   |
| glibc malloc | 8.76 Mops/s  | 26 MB    |

**Throughput**: jp_alloc is the fastest allocator at steady state — 61%
faster than mimalloc, 76% faster than jemalloc, 3.6× faster than glibc
at 300 threads. At 8 threads jp_alloc is also the fastest (19.48 Mops/s).

**RSS**: jp_alloc uses the least physical memory among the fast allocators
at every thread count. At 300 threads: 87 MB vs mimalloc 161 MB
(1.8× less), jemalloc 156 MB (1.8× less), tcmalloc 178 MB (2× less).
Only glibc is lighter (26 MB) but glibc is 3.6× slower.

**RSS** = peak resident set size (physical memory used) at steady state.
Measurements use 10-second timed runs to capture steady-state behavior.
Shorter runs (sub-second) underestimate RSS for all allocators because
pool pages are not yet fully committed by demand paging.

The global freelist uses **magazines** (arrays of 16 block pointers)
transferred via memcpy, not linked lists walked via dependent loads.
Refill pops one magazine + memcpy 16 pointers to the TLS cache. Flush
memcpys 16 pointers from the cache into a magazine + one CAS push.
Magazines are allocated from the pool system and recycled via a CAS-based
free-list — no static arrays, no mmap for magazines.

## Usage

### As an LD_PRELOAD library

```sh
make
LD_PRELOAD=./jp_alloc.so your_program
```

This overrides `malloc`/`free`/`calloc`/`realloc` globally, so all
allocations in the process route through jp_alloc's pools. For C++
programs, the Makefile also builds a C++ shim (`jp_alloc_cpp.cpp`) that
overrides global `operator new`/`delete`/`new[]`/`delete[]` (including
the C++14 sized and C++17 aligned variants), so `new`/`delete` goes
directly to jp_alloc instead of indirectly via libc malloc. The shim is
only compiled if a C++ compiler is available.

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

For C++ programs, link `jp_alloc_cpp.o` alongside `jp_alloc.o` to
override `operator new`/`delete` directly:

```sh
g++ -std=c++17 -c jp_alloc_cpp.cpp -o jp_alloc_cpp.o
g++ your_program.cpp jp_alloc.o jp_alloc_cpp.o -lpthread -lm
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
| `JP_MAG_SIZE` | 16 | Block pointers per magazine (flush/refill batch size) |
| `JP_ALLOC_POOL_COUNT` | 24 | Power-of-2 pool classes (1B..8M) |
| `JP_CACHELINE` | 64 | Cache-line size for alignment padding |
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