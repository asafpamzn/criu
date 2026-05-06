#include <pthread.h>
#include <string.h>
#include <stdint.h>

#include "test_harness.h"
#include "page.h"
#include "cow/page-pool.h"
#include "cow/cow-conf.h"

static void test_thread_init(void)
{
	int rc = page_pool_thread_init(0);
	TEST_ASSERT_EQ(rc, 0, "thread 0 init succeeds");

	/* Double init is safe */
	rc = page_pool_thread_init(0);
	TEST_ASSERT_EQ(rc, 0, "double init returns 0");
}

static void test_get_returns_aligned(void)
{
	page_pool_thread_init(1);

	void *page = page_pool_get(1);
	TEST_ASSERT(page != NULL, "get returns non-NULL");
	TEST_ASSERT_EQ((unsigned long)page & (PAGE_SIZE - 1), 0, "page is PAGE_SIZE aligned");

	/* Write to it to verify it's usable memory */
	memset(page, 0xAB, PAGE_SIZE);

	page_pool_put(page);
}

static void test_get_pages_contiguous(void)
{
	page_pool_thread_init(2);

	int nr_pages = 16;
	void *pages = page_pool_get_pages(2, nr_pages);
	TEST_ASSERT(pages != NULL, "get_pages returns non-NULL");
	TEST_ASSERT_EQ((unsigned long)pages & (PAGE_SIZE - 1), 0, "pages aligned");

	/* Verify all pages are writable and contiguous */
	for (int i = 0; i < nr_pages; i++) {
		void *p = (char *)pages + i * PAGE_SIZE;
		memset(p, i & 0xFF, PAGE_SIZE);
	}

	/* Free each page individually */
	for (int i = 0; i < nr_pages; i++)
		page_pool_put((char *)pages + i * PAGE_SIZE);
}

static void test_get_chunk(void)
{
	page_pool_thread_init(3);

	int nr_pages = 0;
	void *chunk = page_pool_get_chunk(3, &nr_pages);
	TEST_ASSERT(chunk != NULL, "get_chunk returns non-NULL");
	TEST_ASSERT_EQ(nr_pages, COW_ALLOC_BATCH, "get_chunk returns COW_ALLOC_BATCH pages");

	/* Free them all */
	for (int i = 0; i < nr_pages; i++)
		page_pool_put((char *)chunk + i * PAGE_SIZE);
}

static void test_put_refcount(void)
{
	page_pool_thread_init(4);

	/* Allocate several pages from same chunk, free them all */
	void *pages[64];
	for (int i = 0; i < 64; i++)
		pages[i] = page_pool_get(4);

	/* All from same chunk - verify chunk_id matches */
	int first_id = page_pool_get_chunk_id(pages[0]);
	TEST_ASSERT(first_id >= 0, "chunk_id valid");

	for (int i = 1; i < 64; i++) {
		int id = page_pool_get_chunk_id(pages[i]);
		TEST_ASSERT_EQ(id, first_id, "all pages from same chunk");
	}

	/* Free them all */
	for (int i = 0; i < 64; i++)
		page_pool_put(pages[i]);
}

#define MT_POOL_THREADS 4
#define MT_POOL_ALLOCS  1000

static void *pool_allocator(void *arg)
{
	int tid = (int)(long)arg;
	page_pool_thread_init(tid + 10);

	void *pages[MT_POOL_ALLOCS];
	for (int i = 0; i < MT_POOL_ALLOCS; i++)
		pages[i] = page_pool_get(tid + 10);

	/* Write unique pattern to detect overlaps */
	for (int i = 0; i < MT_POOL_ALLOCS; i++)
		memset(pages[i], tid + 1, PAGE_SIZE);

	/* Verify patterns are intact */
	for (int i = 0; i < MT_POOL_ALLOCS; i++) {
		unsigned char *p = pages[i];
		for (int j = 0; j < 16; j++) {
			if (p[j] != (unsigned char)(tid + 1)) {
				return (void *)1L;
			}
		}
	}

	/* Free */
	for (int i = 0; i < MT_POOL_ALLOCS; i++)
		page_pool_put(pages[i]);

	return NULL;
}

static void test_multi_threaded_no_overlap(void)
{
	pthread_t threads[MT_POOL_THREADS];
	void *ret;

	for (int i = 0; i < MT_POOL_THREADS; i++)
		pthread_create(&threads[i], NULL, pool_allocator, (void *)(long)i);

	int all_ok = 1;
	for (int i = 0; i < MT_POOL_THREADS; i++) {
		pthread_join(threads[i], &ret);
		if (ret != NULL)
			all_ok = 0;
	}
	TEST_ASSERT(all_ok, "no overlap between threads");
}

static void test_nr_chunks(void)
{
	int nr = page_pool_get_nr_chunks();
	TEST_ASSERT(nr > 0, "at least one chunk allocated");
}

int main(void)
{
	printf("=== Page Pool Tests ===\n");
	RUN_TEST(test_thread_init);
	RUN_TEST(test_get_returns_aligned);
	RUN_TEST(test_get_pages_contiguous);
	RUN_TEST(test_get_chunk);
	RUN_TEST(test_put_refcount);
	RUN_TEST(test_multi_threaded_no_overlap);
	RUN_TEST(test_nr_chunks);

	page_pool_destroy_all();
	TEST_SUMMARY();
}
