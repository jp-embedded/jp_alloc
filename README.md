# jp_alloc

A lock-free, EBR-protected, thread-caching memory allocator written in pure C11.

Originally a C++20 single-file allocator (`jp_alloc.cpp`), rewritten in C11
with a 3-epoch-ring EBR (Epoch-Based Reclamation) layer that fixes the classic
ABA problem inherent in lock-free Treiber stacks. The original C++ version had
an ABA bug where a block freed, allocated, and freed again could reappear at
the freelist head with the same `next` pointer, causing a thread to install a
stale head and corrupt the freelist. This version eliminates ABA entirely via
EBR: a popped block cannot reappear at the global freelist head until all
threads that observed the prior head have left their pop critical section.

## Features

- **Lock-free**: all hot-path operations use non-blocking 64-bit CAS
- **ABA-free**: 3-epoch-ring EBR prevents the ABA problem without tagged
  pointers, hazard pointers, or 128-bit CAS (no `-mcx16` required)
- **Thread-caching**: per-thread fixed-array cache (N=32 per size class)
  intercepts the hottest alloc/free paths with no atomics
- **Batched refill**: on cache miss, pops up to 16 blocks from the global
  freelist in one CAS, amortizing contention across future pops
- **Batched retire**: when a cache fills, flushes 16 blocks as a single
  atomic chain to the EBR retire list, amortizing epoch bookkeeping
- **Leak-free teardown**: a `pthread_key` destructor flushes the exiting
  thread's caches and retired batches into a global limbo, drained lazily
  by live threads during epoch advancement
- **Portable**: 32-bit and 64-bit, GCC 4.7+/Clang 3.0+/MSVC 2015+,
  POSIX (pthreads) or Windows with MinGW (WinPthread)
- **Self-checking**: under `-DJP_ALLOC_DEBUG`, aborts on ABA, double-free,
  and freelist corruption — used as a regression gate

## Performance

300-thread benchmark (tup-shaped alloc/free churn, memset-realistic):

| Allocator   | Throughput | p99      | RSS    |
|-------------|-----------|----------|--------|
| jp_alloc    | 23.9 Mbps | 384 ns   | 39 MB  |
| mimalloc    | 24.2 Mbps | 768 ns   | 82 MB  |
| jemalloc    | 23.2 Mbps | 768 ns   | 61 MB  |
| tcmalloc    | 17.8 Mbps | 393 µs   | 135 MB |
| glibc malloc| 13.8 Mbps | 1536 ns  | 22 MB  |

jp_alloc is competitive with mimalloc/jemalloc on throughput while using
significantly less memory, and has the lowest p99 latency of all tested
allocators. It beats glibc malloc by ~73%.

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
#include "jp_alloc.h"

/* Use malloc/free/calloc/realloc as normal — jp_alloc overrides them */
void *buf = malloc(1024);
free(buf);

/* Or use the jp_alloc_* API directly for alignment */
void *aligned = jp_alloc_aligned(64, 1024);
jp_free(aligned);

/* Query the size class for a given request */
size_t sz = jp_good_size(100); /* Returns the actual block size */

/* Optional cleanup hook (currently a no-op) */
jp_alloc_reset();
```

Link `jp_alloc.c` into your binary. Define `JP_ALLOC_IMPLEMENTATION` before
including `jp_alloc.h` in the translation unit that compiles `jp_alloc.c`
to get the real definition of `jp_alloc_reset()`:

```c
#define JP_ALLOC_IMPLEMENTATION
#include "jp_alloc.h"
```

## Configuration

Compile-time flags (all optional):

| Flag | Default | Description |
|------|---------|-------------|
| `JP_ALLOC_DEBUG` | off | Enable ABA/double-free/corruption self-checks |
| `JP_CACHE_N` | 32 | Per-thread TLS cache cap per size class |
| `JP_REFILL` | 16 | Batch size for global freelist refill (blocks per CAS) |
| `JP_ALLOC_POOL_COUNT` | 17 | Number of power-of-2 pool classes (1B..64K) |
| `JP_CACHELINE` | 64 | Cache-line size for alignment padding |

## How it works

### Pool structure

Memory is organized into power-of-2 size classes (1, 2, 4, ..., 65536 bytes).
Each size class has a global lock-free freelist (Treiber stack) protected by
EBR. Allocations larger than 64K bypass pools and use `mmap` directly.

### Buddy splitting

When a pool is empty, a block from the next-larger pool is split in half —
one half is returned to the caller, the other goes into the empty pool. This
recursion can chain up to 16 levels but is bounded by the pool count.

### Thread-local cache

Each thread has a fixed-array cache (32 slots per size class) that intercepts
alloc/free with no atomics. On cache miss, a batched refill pops up to 16
blocks from the global freelist in one CAS. When the cache fills, 16 entries
are flushed as a single chain to the EBR retire list.

### EBR (Epoch-Based Reclamation)

A 3-epoch ring prevents ABA: each thread announces its observed epoch before
entering a pop critical section. The global epoch advances only when all
threads have advanced past the oldest epoch. Retired blocks are stored
per-pool per-epoch-slot, so drainage is an O(1) chain splice onto the global
freelist. The TLS cache delays returns to the global freelist and batches
them, amortizing EBR bookkeeping — but EBR alone (not the cache) provides
ABA-freedom.

### Thread teardown

A `pthread_key` destructor flushes the exiting thread's caches and retired
batches into a global limbo indexed by `retire_epoch % 3`. Live threads
drain this limbo lazily during epoch advancement, keeping teardown
ABA-safe and leak-free without per-thread locks.

## Platform support

| Platform | Status |
|----------|--------|
| Linux (x86_64, ARM64) | Tested |
| macOS (Intel, Apple Silicon) | Should work (pthreads + mmap) |
| Windows (MinGW) | Should work (VirtualAlloc + WinPthread) |
| MSVC | Fallbacks provided for `__builtin_clzll`; needs pthread shim |
| 32-bit (embedded) | Designed for portability; `JP_CACHELINE` overridable |

## Debug mode

Build with `-DJP_ALLOC_DEBUG` to enable runtime self-checks:
- **ABA detection**: aborts if a popped block is still marked LIVE
- **Double-free detection**: aborts if a block is freed twice
- **Corruption detection**: aborts if a block's magic number is wrong

```sh
make jp_allocd.so   # Builds the debug variant
LD_PRELOAD=./jp_allocd.so your_program
```

If the program runs to completion, the allocator prints:
```
ABA self-check  : no ABA / corruption detected
```
Any abort means a bug was caught — check `stderr` for details.

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).

## Acknowledgments

Originally written in C++20 at https://github.com/jp-embedded/jp_alloc.
Rewritten in C11 and integrated with [tup](https://github.com/jp-embedded/tup)
(the file-based build system) with:
- 64-bit CAS replacing the 128-bit tagged pointer (cmpxchg16b)
- 3-epoch-ring EBR for ABA-freedom
- Per-thread TLS cache + batched refill
- Windows (VirtualAlloc) and macOS (mremap-gated) support
- A 300-thread stress benchmark with ABA/double-free self-checks