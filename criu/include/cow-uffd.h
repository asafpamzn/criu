#ifndef __CR_COW_UFFD_H__
#define __CR_COW_UFFD_H__

#include <stdbool.h>

/* Initialize COW page buffer (hash table, locks) */
int cow_page_buffer_init(void);

/* Initialize page pool for a receiver thread (call once per thread) */
int cow_page_buffer_thread_init(int thread_id);

/* Lookup page in buffer (thread-safe) - returns data pointer or NULL */
void *cow_page_buffer_lookup_and_remove(unsigned long vaddr);

/*
 * Add page to buffer (thread-safe).
 * thread_id: receiver thread id for lock-free pool allocation (0-15)
 *            must have called cow_page_buffer_thread_init(thread_id) first
 * nocopy: if true, takes ownership of data pointer (from page_pool_get_chunk)
 *         if false, copies data to new page pool allocation
 */
int cow_page_buffer_add(unsigned long vaddr, void *data, int thread_id, bool nocopy);

/* Get current page count */
unsigned long cow_page_buffer_count(void);

/* Destroy COW page buffer and free all resources */
void cow_page_buffer_destroy(void);

/* Discard dirty pages from buffer (called when dirty bitmap received) */
void cow_page_buffer_discard_dirty(unsigned long *dirty_ranges, unsigned int nr_dirty_ranges);

/* Start background drain thread */
int cow_start_drain_thread(void);

/* Stop background drain thread */
void cow_stop_drain_thread(void);

/* Check if drain thread is running */
bool cow_drain_thread_running(void);

/* Remove all pages in range from buffer (for UNMAP/REMOVE events) */
void cow_page_buffer_remove_range(unsigned long start, unsigned long len);

#endif /* __CR_COW_UFFD_H__ */
