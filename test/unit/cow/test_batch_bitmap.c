#include "test_harness.h"
#include "cow/cow-batch-bitmap.h"

static void test_zero_and_fill(void)
{
	cow_batch_bitmap_t bm;

	cow_batch_bitmap_zero(&bm);
	TEST_ASSERT(cow_batch_bitmap_is_empty(&bm), "zero produces empty");
	TEST_ASSERT(!cow_batch_bitmap_is_full(&bm), "zero is not full");

	cow_batch_bitmap_fill(&bm);
	TEST_ASSERT(cow_batch_bitmap_is_full(&bm), "fill produces full");
	TEST_ASSERT(!cow_batch_bitmap_is_empty(&bm), "fill is not empty");
}

static void test_set_clear_test(void)
{
	cow_batch_bitmap_t bm;
	int boundaries[] = {0, 63, 64, 127, 128, 191, 192, 255};

	cow_batch_bitmap_zero(&bm);

	for (int i = 0; i < 8; i++) {
		cow_batch_bitmap_set(&bm, boundaries[i]);
		TEST_ASSERT(cow_batch_bitmap_test(&bm, boundaries[i]), "set bit visible");
	}
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 8, "8 bits set");

	for (int i = 0; i < 8; i++) {
		cow_batch_bitmap_clear(&bm, boundaries[i]);
		TEST_ASSERT(!cow_batch_bitmap_test(&bm, boundaries[i]), "clear bit gone");
	}
	TEST_ASSERT(cow_batch_bitmap_is_empty(&bm), "all cleared = empty");
}

static void test_popcount(void)
{
	cow_batch_bitmap_t bm;

	cow_batch_bitmap_zero(&bm);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 0, "empty popcount=0");

	for (int i = 0; i < 256; i++) {
		cow_batch_bitmap_set(&bm, i);
		TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), i + 1, "popcount increments");
	}
}

static void test_next_set(void)
{
	cow_batch_bitmap_t bm;

	cow_batch_bitmap_zero(&bm);
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 0), -1, "empty has no bits");

	cow_batch_bitmap_set(&bm, 5);
	cow_batch_bitmap_set(&bm, 100);
	cow_batch_bitmap_set(&bm, 200);

	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 0), 5, "first set at 5");
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 6), 100, "next set at 100");
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 101), 200, "next set at 200");
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 201), -1, "no more bits");
}

static void test_set_range(void)
{
	cow_batch_bitmap_t bm;

	/* Range within one word */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 10, 20);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 20, "range of 20");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 10), "range start set");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 29), "range end set");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 9), "before range clear");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 30), "after range clear");

	/* Range spanning word boundaries */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 60, 10);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 10, "cross-word range");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 60), "cross start");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 69), "cross end");

	/* Full range */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 0, 256);
	TEST_ASSERT(cow_batch_bitmap_is_full(&bm), "full range = full");
}

static void test_clear_range(void)
{
	cow_batch_bitmap_t bm;

	cow_batch_bitmap_fill(&bm);
	cow_batch_bitmap_clear_range(&bm, 64, 64);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 192, "cleared 64 bits");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 64), "cleared start");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 127), "cleared end");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 63), "before range untouched");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 128), "after range untouched");
}

static void test_and_or_not(void)
{
	cow_batch_bitmap_t a, b, result;

	cow_batch_bitmap_zero(&a);
	cow_batch_bitmap_zero(&b);
	cow_batch_bitmap_set_range(&a, 0, 128);
	cow_batch_bitmap_set_range(&b, 64, 128);

	cow_batch_bitmap_and(&result, &a, &b);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&result), 64, "AND overlap");
	TEST_ASSERT(cow_batch_bitmap_test(&result, 64), "AND bit 64");
	TEST_ASSERT(cow_batch_bitmap_test(&result, 127), "AND bit 127");
	TEST_ASSERT(!cow_batch_bitmap_test(&result, 63), "AND not bit 63");

	cow_batch_bitmap_or(&result, &a, &b);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&result), 192, "OR union");

	cow_batch_bitmap_not(&result, &a);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&result), 128, "NOT flips");
}

static void test_mask(void)
{
	cow_batch_bitmap_t bm;

	cow_batch_bitmap_fill(&bm);
	cow_batch_bitmap_mask(&bm, 100);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 100, "mask to 100");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 99), "bit 99 still set");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 100), "bit 100 cleared");
}

static void test_is_full_upto(void)
{
	cow_batch_bitmap_t bm;

	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 0, 64);
	TEST_ASSERT(cow_batch_bitmap_is_full_upto(&bm, 64), "full up to 64");
	TEST_ASSERT(!cow_batch_bitmap_is_full_upto(&bm, 65), "not full to 65");
	TEST_ASSERT(cow_batch_bitmap_is_full_upto(&bm, 1), "full up to 1");
}

static void test_for_each_set_macro(void)
{
	cow_batch_bitmap_t bm;
	int idx, count = 0;
	int expected[] = {3, 77, 200};

	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set(&bm, 3);
	cow_batch_bitmap_set(&bm, 77);
	cow_batch_bitmap_set(&bm, 200);

	COW_BATCH_BITMAP_FOR_EACH_SET(&bm, idx) {
		TEST_ASSERT(count < 3, "not too many iterations");
		if (count < 3)
			TEST_ASSERT_EQ(idx, expected[count], "correct iteration order");
		count++;
	}
	TEST_ASSERT_EQ(count, 3, "iterated all set bits");
}

static void test_edge_cases(void)
{
	cow_batch_bitmap_t bm;

	/* set_range with count=0 should be no-op */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 50, 0);
	TEST_ASSERT(cow_batch_bitmap_is_empty(&bm), "zero-count range is nop");

	/* set_range beyond limit - clamped to 256 */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 250, 100);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 6, "clamped to 256");

	/* next_set from 256+ */
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 256), -1, "past end = -1");
}

static void test_copy(void)
{
	cow_batch_bitmap_t src, dst;

	cow_batch_bitmap_zero(&src);
	cow_batch_bitmap_set(&src, 42);
	cow_batch_bitmap_set(&src, 200);

	cow_batch_bitmap_copy(&dst, &src);
	TEST_ASSERT(cow_batch_bitmap_test(&dst, 42), "copy bit 42");
	TEST_ASSERT(cow_batch_bitmap_test(&dst, 200), "copy bit 200");
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&dst), 2, "copy popcount");
}

int main(void)
{
	printf("=== COW Batch Bitmap Tests ===\n");
	RUN_TEST(test_zero_and_fill);
	RUN_TEST(test_set_clear_test);
	RUN_TEST(test_popcount);
	RUN_TEST(test_next_set);
	RUN_TEST(test_set_range);
	RUN_TEST(test_clear_range);
	RUN_TEST(test_and_or_not);
	RUN_TEST(test_mask);
	RUN_TEST(test_is_full_upto);
	RUN_TEST(test_for_each_set_macro);
	RUN_TEST(test_edge_cases);
	RUN_TEST(test_copy);
	TEST_SUMMARY();
}
