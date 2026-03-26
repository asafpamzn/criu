#ifndef __CR_PAGE_POOL_H__
#define __CR_PAGE_POOL_H__

/*
 * Pre-allocated page pool to avoid malloc/mprotect contention
 * when multiple threads allocate PAGE_SIZE buffers concurrently.
 *
 * Uses mmap to allocate 4MB chunks, avoiding glibc malloc's
 * mprotect calls that serialize on the kernel mmap_sem.
 */

int page_pool_init(void);
void page_pool_destroy(void);

void *page_pool_get(void);
void page_pool_put(void *page);

#endif /* __CR_PAGE_POOL_H__ */
