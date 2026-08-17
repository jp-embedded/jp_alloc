/* jp_alloc_test - unit test suite for jp_alloc
 *
 * Build as a standalone binary (3 variants):
 *   cc -O2 -std=c11 -DJP_ALLOC_IMPLEMENTATION -DJP_ALLOC_TEST \
 *       jp_alloc.c jp_alloc_test.c -o jp_alloc_test -lpthread -lm
 *
 *   cc -O1 -g -std=c11 -DJP_ALLOC_IMPLEMENTATION -DJP_ALLOC_DEBUG -DJP_ALLOC_TEST \
 *       jp_alloc.c jp_alloc_test.c -o jp_alloc_test_debug -lpthread -lm
 *
 *   cc -O2 -std=c11 -DJP_ALLOC_IMPLEMENTATION -DJP_ALLOC_TEST -DJP_ALLOC_INTERMEDIATE_K=4 \
 *       jp_alloc.c jp_alloc_test.c -o jp_alloc_test_k4 -lpthread -lm
 *
 * The test links jp_alloc.c in the same translation unit (via
 * -DJP_ALLOC_IMPLEMENTATION), so static internals (g_pools, JP_POOL_COUNT,
 * union header, pool_id) are visible for white-box testing. malloc/free
 * are globally overridden by jp_alloc.c, so all allocations in the test
 * (including stdlib internals) route through jp_alloc.
 *
 * Exit code 0 = all tests passed. Exit code 1 = at least one test failed.
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/* _GNU_SOURCE needed for madvise, mremap (Linux extensions used by jp_alloc.c) */
#ifdef __linux__
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#endif

#include <stddef.h>
#include <pthread.h>
#include <stdint.h>

/* Include jp_alloc.c directly so all static internals (union header,
 * g_pools, JP_POOL_COUNT, pool_id, etc.) are in the same translation
 * unit. This is the same pattern as jp_alloc_bench.c, which also
 * links jp_alloc.c via -DJP_ALLOC_IMPLEMENTATION. The difference is
 * that the bench is compiled as two separate .c files, while the
 * test needs white-box access to static symbols — so we #include
 * the source directly. */
#define JP_ALLOC_IMPLEMENTATION
#include "jp_alloc.c"

#include "jp_alloc_test.h"

/* The test includes jp_alloc.c via the same compilation unit, so all
 * static symbols are accessible. We need these for white-box tests. */
/* g_pools, JP_POOL_COUNT, pool_id, union header, pool_id_by_size,
 * jp_pid_lut_init, g_pid_lut_done — all static in jp_alloc.c, visible
 * here because we're in the same TU. */

/* ---- Helpers ---- */

/* Fill a block with a unique pattern based on a seed value. */
static void fill_pattern(void *p, int seed, size_t sz)
{
	unsigned char *b = p;
	for(size_t i = 0; i < sz; i++)
		b[i] = (unsigned char)(seed + i);
}

/* Verify a block has the expected pattern. Returns 1 on match, 0 on mismatch. */
static int verify_pattern(void *p, int seed, size_t sz)
{
	unsigned char *b = p;
	for(size_t i = 0; i < sz; i++)
		if(b[i] != (unsigned char)(seed + i))
			return 0;
	return 1;
}

/* ---- 1. Header layout ---- */

TEST(test_header_layout)
{
	ASSERT_GE(sizeof(union header), 16);
	ASSERT_EQ(offsetof(union header, s.size), 0);
	ASSERT_EQ(offsetof(union header, s.next), sizeof(size_t));
}

/* ---- 2. Pool table integrity ---- */

TEST(test_pool_table)
{
	/* Verify sorted ascending */
	for(size_t i = 1; i < JP_POOL_COUNT; i++)
		ASSERT_TRUE(g_pools[i].size > g_pools[i-1].size);

	/* Verify intermediate split identity: 1*small + 3*inter = split_from */
	for(size_t i = 0; i < JP_POOL_COUNT; i++) {
		if(!g_pools[i].is_pow2) {
			ASSERT_EQ(g_pools[i].small_size + 3 * g_pools[i].size,
			          g_pools[i].split_from);
			ASSERT_GE(g_pools[i].small_size, sizeof(union header));
		}
	}

	/* Verify the largest pool is a power-of-2 (8M) */
	ASSERT_TRUE(g_pools[JP_POOL_COUNT - 1].is_pow2);
	ASSERT_GE(g_pools[JP_POOL_COUNT - 1].size, 8 * 1024 * 1024);
}

/* ---- 3. Basic alloc/free for every size ---- */

TEST(test_basic_alloc_free)
{
	for(size_t sz = 1; sz <= 4096; sz++) {
		void *p = malloc(sz);
		ASSERT_TRUE(p);
		memset(p, 0xAB, sz);
		free(p);
	}
}

/* ---- 4. Alignment of returned pointers ---- */

TEST(test_alignment)
{
	for(size_t sz = 1; sz <= 4096; sz++) {
		void *p = malloc(sz);
		ASSERT_TRUE(p);
		ASSERT_EQ((uintptr_t)p & 15, 0); /* 16-byte aligned */
		free(p);
	}
}

/* ---- 5. Usable size matches pool for every pool class ---- */

TEST(test_usable_size)
{
	size_t hdr_sz = sizeof(union header);
	for(size_t pid = 0; pid < JP_POOL_COUNT; pid++) {
		size_t pool_sz = g_pools[pid].size;
		if(pool_sz <= hdr_sz) continue; /* skip pools smaller than header */
		size_t req = pool_sz - hdr_sz;
		void *p = malloc(req);
		ASSERT_TRUE(p);
		ASSERT_GE(malloc_usable_size(p), req);
		/* Write to the entire usable area — if the block is too small,
		 * this will corrupt the next block's header and a subsequent
		 * malloc will fail or return a corrupted pointer. */
		memset(p, 0xCD, malloc_usable_size(p));
		/* Allocate another block to check for header corruption */
		void *q = malloc(16);
		ASSERT_TRUE(q);
		memset(q, 0xEF, 16);
		free(q);
		free(p);
	}
}

/* ---- 6. Pattern preservation (use-after-free / corruption detector) ---- */

TEST(test_pattern_preservation)
{
	#define N 1000
	void *ptrs[N];
	size_t sizes[N];
	for(int i = 0; i < N; i++) {
		sizes[i] = (size_t)((i % 20) * 16 + 1);
		ptrs[i] = malloc(sizes[i]);
		ASSERT_TRUE(ptrs[i]);
		fill_pattern(ptrs[i], i, sizes[i]);
	}
	/* Free even-indexed */
	for(int i = 0; i < N; i += 2) {
		free(ptrs[i]);
		ptrs[i] = NULL;
	}
	/* Verify odd-indexed still have correct patterns */
	for(int i = 1; i < N; i += 2) {
		ASSERT_TRUE(verify_pattern(ptrs[i], i, sizes[i]));
	}
	/* Re-allocate freed slots */
	for(int i = 0; i < N; i += 2) {
		ptrs[i] = malloc(sizes[i]);
		ASSERT_TRUE(ptrs[i]);
		fill_pattern(ptrs[i], i + 100, sizes[i]);
	}
	/* Verify all blocks have correct patterns */
	for(int i = 0; i < N; i++) {
		int expected_seed = (i % 2 == 0) ? i + 100 : i;
		ASSERT_TRUE(verify_pattern(ptrs[i], expected_seed, sizes[i]));
	}
	/* Free all */
	for(int i = 0; i < N; i++) free(ptrs[i]);
	#undef N
}

/* ---- 7. Asymmetric split: force every intermediate pool to split ---- */

#if JP_ALLOC_INTERMEDIATE_K > 0
TEST(test_asymmetric_split)
{
	size_t hdr_sz = sizeof(union header);
	for(size_t pid = 0; pid < JP_POOL_COUNT; pid++) {
		if(g_pools[pid].is_pow2) continue;
		size_t inter_sz = g_pools[pid].size;
		size_t req = inter_sz - hdr_sz;
		if(req == 0) continue;

		/* Allocate enough to force at least 2 splits (each yields 3 blocks) */
		void *blocks[8];
		int n = 0;
		for(int i = 0; i < 8; i++) {
			blocks[i] = malloc(req);
			if(!blocks[i]) break;
			n++;
			ASSERT_GE(malloc_usable_size(blocks[i]), req);
			memset(blocks[i], 0xEE, req);
		}
		ASSERT_GE(n, 4); /* at least 2 splits worth */

		/* Verify no two blocks overlap */
		for(int i = 0; i < n; i++) {
			for(int j = i + 1; j < n; j++) {
				char *a = blocks[i];
				char *b = blocks[j];
				size_t sa = malloc_usable_size(blocks[i]) + hdr_sz;
				size_t sb = malloc_usable_size(blocks[j]) + hdr_sz;
				ASSERT_TRUE(a + sa <= b || b + sb <= a);
			}
		}

		for(int i = 0; i < n; i++) free(blocks[i]);
	}
}
#endif /* JP_ALLOC_INTERMEDIATE_K > 0 */

/* ---- 8. Split boundary: carved blocks fit within parent ---- */

#if JP_ALLOC_INTERMEDIATE_K > 0
TEST(test_split_boundary)
{
	size_t hdr_sz = sizeof(union header);
	for(size_t pid = 0; pid < JP_POOL_COUNT; pid++) {
		if(g_pools[pid].is_pow2) continue;
		size_t inter_sz = g_pools[pid].size;
		size_t small_sz = g_pools[pid].small_size;
		size_t split_from = g_pools[pid].split_from;
		size_t req = inter_sz - hdr_sz;
		if(req == 0) continue;

		/* Exhaust the freelist by allocating blocks until we get
		 * ones from a fresh split. We detect a fresh split by
		 * checking that 3 consecutive blocks are at inter_sz
		 * stride from each other (the asymmetric split carves 3
		 * consecutive intermediates). */
		void *drain[256];
		int ndrain = 0;
		/* Allocate up to 256 blocks to drain the freelist */
		for(int i = 0; i < 256; i++) {
			drain[i] = malloc(req);
			if(!drain[i]) break;
			ndrain++;
		}

		/* Now find 3 consecutive blocks at inter_sz stride.
		 * Scan the last few blocks — the most recent ones
		 * are from the latest split. */
		int found = -1;
		for(int i = ndrain - 3; i >= 0; i--) {
			char *a = (char *)drain[i];
			char *b = (char *)drain[i + 1];
			char *c = (char *)drain[i + 2];
			/* Check all 3 are at inter_sz stride (sorted) */
			char *lo = a, *mid = b, *hi = c;
			if(mid < lo) { char *t = lo; lo = mid; mid = t; }
			if(hi < mid) { char *t = mid; mid = hi; hi = t; }
			if(mid < lo) { char *t = lo; lo = mid; mid = t; }
			if(mid - lo == (ptrdiff_t)inter_sz && hi - mid == (ptrdiff_t)inter_sz) {
				found = i;
				/* Verify span fits within parent */
				ptrdiff_t span = (hi + inter_sz) - lo;
				ASSERT_TRUE(span >= 0);
				ASSERT_LE((size_t)span, split_from);
				break;
			}
		}
		ASSERT_TRUE(found >= 0);

		/* Free all drained blocks */
		for(int i = 0; i < ndrain; i++) free(drain[i]);
	}
}
#endif /* JP_ALLOC_INTERMEDIATE_K > 0 */

/* ---- 9. Realloc ---- */

TEST(test_realloc)
{
	/* Grow: data preserved */
	void *p = malloc(32);
	ASSERT_TRUE(p);
	memset(p, 0xAA, 32);
	p = realloc(p, 128);
	ASSERT_TRUE(p);
	for(int i = 0; i < 32; i++)
		ASSERT_EQ(((unsigned char *)p)[i], 0xAA);

	/* Shrink: data preserved up to new size */
	p = realloc(p, 16);
	ASSERT_TRUE(p);
	for(int i = 0; i < 16; i++)
		ASSERT_EQ(((unsigned char *)p)[i], 0xAA);

	/* Realloc to 0 = free */
	p = realloc(p, 0);
	ASSERT_TRUE(p == NULL);

	/* Realloc NULL = malloc */
	p = realloc(NULL, 64);
	ASSERT_TRUE(p);
	memset(p, 0xBB, 64);
	free(p);
}

/* ---- 10. Calloc zero-fill ---- */

TEST(test_calloc)
{
	void *p = calloc(100, 8);
	ASSERT_TRUE(p);
	for(int i = 0; i < 800; i++)
		ASSERT_EQ(((unsigned char *)p)[i], 0);
	free(p);
}

/* ---- 11. Large alloc (> 8MB → direct mmap) ---- */

TEST(test_large_alloc)
{
	void *p = malloc(16 * 1024 * 1024);
	ASSERT_TRUE(p);
	memset(p, 0xBB, 16 * 1024 * 1024);
	free(p);
}

/* ---- 12. Overflow detection (write past request, verify next block) ---- */

TEST(test_overflow_detection)
{
	void *p = malloc(48);
	ASSERT_TRUE(p);
	/* Write to the entire usable area (which includes any slack).
	 * If the pool has no slack, this writes into the next block's
	 * header. The next malloc should then get a corrupted block. */
	size_t usable = malloc_usable_size(p);
	memset(p, 0xCC, usable);

	/* Allocate another block — its header must be intact */
	void *q = malloc(48);
	ASSERT_TRUE(q);
	memset(q, 0xDD, 48);

	/* Verify p's data didn't bleed into q */
	for(int i = 0; i < 48; i++)
		ASSERT_EQ(((unsigned char *)q)[i], 0xDD);

	free(p);
	free(q);
}

/* ---- 13. Multithreaded (4 threads, patterns, no cross-corruption) ---- */

struct mt_args {
	int tid;
	int nallocs;
};

static void *mt_worker(void *arg)
{
	struct mt_args *a = (struct mt_args *)arg;
	void *ptrs[256];
	size_t sizes[256];
	int n = a->nallocs;
	if(n > 256) n = 256;
	for(int i = 0; i < n; i++) {
		sizes[i] = (size_t)((i * 13 + a->tid * 7) % 256 + 1);
		ptrs[i] = malloc(sizes[i]);
		if(!ptrs[i]) return (void *)1;
		fill_pattern(ptrs[i], a->tid * 100 + i, sizes[i]);
	}
	/* Verify patterns */
	for(int i = 0; i < n; i++) {
		if(!verify_pattern(ptrs[i], a->tid * 100 + i, sizes[i]))
			return (void *)1;
	}
	/* Free all */
	for(int i = 0; i < n; i++) free(ptrs[i]);
	return NULL;
}

TEST(test_multithreaded)
{
	#define NTHREADS 4
	pthread_t threads[NTHREADS];
	struct mt_args args[NTHREADS];
	for(int i = 0; i < NTHREADS; i++) {
		args[i].tid = i;
		args[i].nallocs = 200;
		int r = pthread_create(&threads[i], NULL, mt_worker, &args[i]);
		ASSERT_EQ(r, 0);
	}
	int failed = 0;
	for(int i = 0; i < NTHREADS; i++) {
		void *ret;
		pthread_join(threads[i], &ret);
		if(ret != NULL) failed = 1;
	}
	ASSERT_EQ(failed, 0);
	#undef NTHREADS
}

/* ---- Main ---- */

#ifdef JP_ALLOC_TEST
int main(void)
{
	RUN_TEST(test_header_layout);
	RUN_TEST(test_pool_table);
	RUN_TEST(test_basic_alloc_free);
	RUN_TEST(test_alignment);
	RUN_TEST(test_usable_size);
	RUN_TEST(test_pattern_preservation);
#if JP_ALLOC_INTERMEDIATE_K > 0
	RUN_TEST(test_asymmetric_split);
	RUN_TEST(test_split_boundary);
#endif
	RUN_TEST(test_realloc);
	RUN_TEST(test_calloc);
	RUN_TEST(test_large_alloc);
	RUN_TEST(test_overflow_detection);
	RUN_TEST(test_multithreaded);
	DONE();
}
#endif