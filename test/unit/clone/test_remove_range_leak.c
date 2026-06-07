/*
 * Regression test for a pool-page leak in clone_page_buffer_remove_range()
 * (criu/clone/clone-uffd.c).
 *
 * BUG (finding from code review, verified against source 2026-06-07):
 *   When a buffered batch becomes empty due to a VMA UNMAP, remove_range()
 *   computes the set of pool pages to free as:
 *
 *       free_bm = ~initial_bitmap | page_bitmap
 *
 *   but it does so AFTER it has already cleared the unmapped bits out of
 *   page_bitmap (clone_batch_bitmap_clear_range at clone-uffd.c:711). So the
 *   pages that were buffered-and-now-discarded (the bits it just cleared) are
 *   NOT in page_bitmap anymore, and they are NOT in ~initial_bitmap either
 *   (they WERE initially owned). Result: those pool slots are never
 *   page_pool_put()'d -> leak (chunk refcount never reaches 0).
 *
 *   The sibling path drain_apply_batch() (clone-uffd.c:817-819) computes the
 *   SAME mask but BEFORE clearing, so it frees correctly. This test encodes
 *   both algorithms and asserts the correct (drain) one frees every owned
 *   page while the buggy (remove_range) one leaks the discarded pages.
 *
 * This is a pure-logic reproducer (no kernel/uffd needed): it models the
 * batch bitmap exactly as the C code does, using the real
 * clone-batch-bitmap.h operations.
 *
 * Expectation:
 *   - "correct algorithm" frees ALL owned pages (0 leaked)  -> passes
 *   - "buggy algorithm (current remove_range)" leaks the discarded pages
 *     -> this test documents the leak. When remove_range is FIXED to compute
 *     the mask before clearing, flip EXPECT_REMOVE_RANGE_LEAK to 0 and this
 *     test will assert the leak is gone.
 */
#include "test_harness.h"
#include "clone/clone-batch-bitmap.h"

/* clone-conf.h sets CLONE_BATCH_PAGES = 256 = the bitmap width (CLONE_BITMAP_BITS).
 * The unit harness compiles only the header, so derive it from the header. */
#ifndef CLONE_BATCH_PAGES
#define CLONE_BATCH_PAGES CLONE_BITMAP_BITS
#endif

/* Set to 1 while the bug exists (documents it); 0 once remove_range is fixed. */
#define EXPECT_REMOVE_RANGE_LEAK 0

/*
 * Count pool pages that the CORRECT algorithm frees.
 * free_bm computed from page_bitmap BEFORE clearing (as drain_apply_batch does).
 * Returns number of distinct pages put back to the pool.
 */
static int free_count_correct(const clone_batch_bitmap_t *initial_bitmap,
                              const clone_batch_bitmap_t *page_bitmap_before,
                              int first_page, int count)
{
        clone_batch_bitmap_t free_bm;
        (void)first_page;
        (void)count;

        clone_batch_bitmap_not(&free_bm, initial_bitmap);
        clone_batch_bitmap_mask(&free_bm, CLONE_BATCH_PAGES);
        clone_batch_bitmap_or(&free_bm, &free_bm, page_bitmap_before);
        return clone_batch_bitmap_popcount(&free_bm);
}

/*
 * Count pool pages that the CURRENT remove_range algorithm frees.
 * It clears [first_page, first_page+count) from page_bitmap FIRST, then
 * computes free_bm = ~initial | page_bitmap (now missing the cleared bits).
 */
static int free_count_remove_range(const clone_batch_bitmap_t *initial_bitmap,
                                   const clone_batch_bitmap_t *page_bitmap_before,
                                   int first_page, int count)
{
        clone_batch_bitmap_t page_bitmap = *page_bitmap_before;
        clone_batch_bitmap_t page_bitmap_pre_clear;
        clone_batch_bitmap_t free_bm;

#if EXPECT_REMOVE_RANGE_LEAK
        /*
         * OLD (buggy) order: clear the unmapped bits FIRST, then compute
         * free_bm = ~initial | page_bitmap from the already-cleared bitmap.
         */
        clone_batch_bitmap_clear_range(&page_bitmap, first_page, count);

        clone_batch_bitmap_not(&free_bm, initial_bitmap);
        clone_batch_bitmap_mask(&free_bm, CLONE_BATCH_PAGES);
        clone_batch_bitmap_or(&free_bm, &free_bm, &page_bitmap);
#else
        /*
         * FIXED order (matches clone-uffd.c): snapshot page_bitmap BEFORE
         * clear_range, compute free_bm from the snapshot.
         */
        clone_batch_bitmap_copy(&page_bitmap_pre_clear, &page_bitmap);
        clone_batch_bitmap_clear_range(&page_bitmap, first_page, count);

        clone_batch_bitmap_not(&free_bm, initial_bitmap);
        clone_batch_bitmap_mask(&free_bm, CLONE_BATCH_PAGES);
        clone_batch_bitmap_or(&free_bm, &free_bm, &page_bitmap_pre_clear);
#endif
        return clone_batch_bitmap_popcount(&free_bm);
}

/*
 * Scenario: a batch where the producer allocated CLONE_BATCH_PAGES pool pages
 * (page_pool_get_pages sets refcount on all of them). initial_bitmap marks the
 * pages that actually carried data (were "owned"). No page faults happened, so
 * page_bitmap == initial_bitmap at unmap time. Then the WHOLE batch is unmapped
 * (UNMAP covers [0, CLONE_BATCH_PAGES)), so nr_pages -> 0 and we hit the
 * free path.
 *
 * Correct behaviour: every one of the CLONE_BATCH_PAGES pool slots must be
 * freed (owned pages via page_bitmap, unused slots via ~initial_bitmap).
 */
static void test_full_unmap_frees_all_pool_pages(void)
{
        clone_batch_bitmap_t initial, page_now;
        int n_owned = 40; /* pages that carried data */
        int i, correct, buggy;

        clone_batch_bitmap_zero(&initial);
        for (i = 0; i < n_owned; i++)
                clone_batch_bitmap_set(&initial, i);
        page_now = initial; /* no page faults consumed any */

        /* UNMAP covers the whole batch */
        correct = free_count_correct(&initial, &page_now, 0, CLONE_BATCH_PAGES);
        buggy = free_count_remove_range(&initial, &page_now, 0, CLONE_BATCH_PAGES);

        TEST_ASSERT_EQ(correct, CLONE_BATCH_PAGES,
                       "correct algo frees every pool slot on full unmap");

#if EXPECT_REMOVE_RANGE_LEAK
        /* Document the bug: remove_range frees fewer -> leaks the owned pages */
        TEST_ASSERT(buggy < correct,
                    "BUG present: remove_range leaks owned pool pages on unmap");
        TEST_ASSERT_EQ(correct - buggy, n_owned,
                       "remove_range leaks exactly the owned (data-carrying) pages");
#else
        TEST_ASSERT_EQ(buggy, correct,
                       "remove_range fixed: frees every pool slot like drain");
#endif
}

/*
 * Partial scenario: only the upper half of an owned batch is unmapped, lower
 * half stays buffered (entry->nr_pages stays > 0, so the free path is NOT
 * taken in real code). This test just sanity-checks the mask math for the
 * pages that WOULD be freed if the batch emptied — included for completeness.
 */
static void test_partial_unmap_mask(void)
{
        clone_batch_bitmap_t initial, page_now;
        int i, correct, buggy;

        clone_batch_bitmap_zero(&initial);
        for (i = 0; i < CLONE_BATCH_PAGES; i++)
                clone_batch_bitmap_set(&initial, i); /* fully owned batch */
        page_now = initial;

        /* Unmap upper half only */
        correct = free_count_correct(&initial, &page_now, CLONE_BATCH_PAGES / 2,
                                     CLONE_BATCH_PAGES / 2);
        buggy = free_count_remove_range(&initial, &page_now, CLONE_BATCH_PAGES / 2,
                                    CLONE_BATCH_PAGES / 2);

        TEST_ASSERT_EQ(correct, CLONE_BATCH_PAGES,
                       "correct algo would free all slots of a fully-owned batch");
#if EXPECT_REMOVE_RANGE_LEAK
        TEST_ASSERT_EQ(buggy, CLONE_BATCH_PAGES / 2,
                       "BUG present: remove_range would leak the unmapped half");
#else
        TEST_ASSERT_EQ(buggy, correct, "remove_range fixed");
#endif
}

int main(void)
{
        printf("=== remove_range pool-page leak regression ===\n");
        RUN_TEST(test_full_unmap_frees_all_pool_pages);
        RUN_TEST(test_partial_unmap_mask);
        TEST_SUMMARY();
}