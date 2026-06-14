#ifndef __CR_PAGE_POOL_H__
#define __CR_PAGE_POOL_H__

/*
 * Per-thread page pool to avoid malloc/mprotect contention.
 * Uses CLONE_CHUNK_SIZE-aligned chunks with reference counting.
 *
 * Allocation:   lock-free bump pointer per thread.
 * Deallocation: atomic refcount decrement; munmap when refcount hits zero.
 *
 * Each receiver thread has its own pool (no locks on the allocation path);
 * any thread can free pages (atomic refcount).
 */

/* Initialize pool for thread (call once per receiver thread) */
int page_pool_thread_init(int thread_id);

/* Get nr_pages contiguous pages from this thread's pool (lock-free) */
void *page_pool_get_pages(int thread_id, int nr_pages);

/* Return a page - any thread can call (atomic refcount) */
void page_pool_put(void *page);

/* Debug: print chunk stats */
void page_pool_dump_stats(void);

/* Debug: print chunk utilization (how full each chunk got) */
void page_pool_dump_utilization(void);

/* Debug: mark drain started and report puts before drain */
void page_pool_mark_drain_started(void);

/* Get chunk ID from data pointer (for chunk-ordered drain) */
int page_pool_get_chunk_id(void *page);

/* Get number of allocated chunks */
int page_pool_get_nr_chunks(void);

#endif /* __CR_PAGE_POOL_H__ */
