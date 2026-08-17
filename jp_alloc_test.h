/* jp_alloc_test.h - minimal C test framework for jp_alloc
 *
 * No external dependencies. Provides TEST(), ASSERT_*, RUN_TEST(), DONE().
 * Designed to be included by jp_alloc_test.c which is compiled with
 * -DJP_ALLOC_TEST (providing main()) and links jp_alloc.c in the same
 * translation unit (so static internals are visible for white-box tests).
 *
 * Usage:
 *   TEST(test_foo) {
 *       ASSERT_TRUE(1 == 1);
 *   }
 *   int main(void) {
 *       RUN_TEST(test_foo);
 *       DONE();
 *   }
 */
#ifndef JP_ALLOC_TEST_H
#define JP_ALLOC_TEST_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_test_count = 0;
static int g_test_fail = 0;

#define TEST(name) \
	static void name(void); \
	static void name(void)

#define ASSERT_TRUE(cond) do { \
	if(!(cond)) { \
		fprintf(stderr, "  FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
		g_test_fail++; \
		return; \
	} \
} while(0)

#define ASSERT_EQ(a, b) ASSERT_TRUE((a) == (b))
#define ASSERT_GE(a, b) ASSERT_TRUE((a) >= (b))
#define ASSERT_LE(a, b) ASSERT_TRUE((a) <= (b))
#define ASSERT_NE(a, b) ASSERT_TRUE((a) != (b))

#define RUN_TEST(name) do { \
	g_test_count++; \
	name(); \
	if(g_test_fail) { \
		fprintf(stderr, "\nTest %d FAILED, stopping.\n", g_test_count); \
		return 1; \
	} \
	printf("  PASS: %s\n", #name); \
} while(0)

#define DONE() do { \
	if(g_test_fail) { \
		fprintf(stderr, "\n%d/%d tests FAILED\n", g_test_fail, g_test_count); \
		return 1; \
	} \
	printf("\nAll %d tests passed\n", g_test_count); \
	return 0; \
} while(0)

#endif /* JP_ALLOC_TEST_H */