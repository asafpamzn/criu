#ifndef __CR_PAGE_POOL_H__
#define __CR_PAGE_POOL_H__

/*
 * Per-thread page pool to avoid malloc/mprotect contention.
 * Uses 256MB aligned chunks with reference counting.
 *
 * Allocation: Lock-free bump pointer per thread
 * Deallocation: Atomic refcount decrement, munmap when zero
 *
 * Design:
 *   - Each receiver thread has its own pool (no locks on allocation)
 *   - Pages come from 256MB aligned chunks
 *   - Any thread can free pages (atomic refcount decrement)
 *   - When chunk refcount reaches 0, entire 256MB is munmapped
 */

/* Initialize pool for thread (call once per receiver thread) */
int page_pool_thread_init(int thread_id);

/* Get a page from this thread's pool (lock-free) */
void *page_pool_get(int thread_id);

/* Get contiguous chunk for direct decompression (returns first page after header) */
void *page_pool_get_chunk(int thread_id, int *out_nr_pages);

/* Return a page - any thread can call (atomic refcount) */
void page_pool_put(void *page);

/* Cleanup all pools */
void page_pool_destroy_all(void);

#endif /* __CR_PAGE_POOL_H__ */
