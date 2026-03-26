/*
 * Page Pool - Pre-allocated page buffers to avoid malloc/mprotect contention
 *
 * When multiple threads allocate PAGE_SIZE buffers via malloc, glibc's heap
 * management triggers mprotect calls that serialize on the kernel's mmap_sem
 * write lock. This pool uses mmap to pre-allocate pages in 4MB chunks,
 * eliminating the contention.
 *
 * Design:
 *   - Chunks: 4MB each (1024 pages), allocated via mmap on demand
 *   - Free stack: LIFO stack of available page pointers
 *   - Growth: New chunk allocated when stack is empty
 *   - Fallback: Returns to malloc if pool hits max capacity
 */

#include <sys/mman.h>
#include <pthread.h>
#include <stddef.h>
#include <stdbool.h>

#include "page.h"
#include "page-pool.h"
#include "xmalloc.h"
#include "criu-log.h"

#undef LOG_PREFIX
#define LOG_PREFIX "page-pool: "

#define PAGES_PER_CHUNK  1024		/* 4MB per chunk */
#define CHUNK_SIZE       (PAGES_PER_CHUNK * PAGE_SIZE)
#define MAX_CHUNKS       64		/* Max 256MB total */

static struct {
	void *chunks[MAX_CHUNKS];	/* Array of mmap'd 4MB chunks */
	int nr_chunks;			/* Number of allocated chunks */
	void **free_stack;		/* Stack of free page pointers */
	int stack_size;			/* Allocated stack capacity */
	int stack_top;			/* Next free slot (0 = empty) */
	pthread_spinlock_t lock;
	bool initialized;
} page_pool;

/* Allocate a new 4MB chunk and add all pages to free stack */
static int page_pool_grow(void)
{
	void *chunk;
	int i;

	if (page_pool.nr_chunks >= MAX_CHUNKS) {
		pr_warn("Page pool at max capacity (%d chunks)\n", MAX_CHUNKS);
		return -1;
	}

	chunk = mmap(NULL, CHUNK_SIZE, PROT_READ | PROT_WRITE,
		     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (chunk == MAP_FAILED) {
		pr_perror("Failed to mmap page pool chunk");
		return -1;
	}

	/* Grow free stack if needed */
	if (page_pool.stack_top + PAGES_PER_CHUNK > page_pool.stack_size) {
		int new_size = page_pool.stack_size + PAGES_PER_CHUNK;
		void **new_stack = xrealloc(page_pool.free_stack,
					    new_size * sizeof(void *));
		if (!new_stack) {
			munmap(chunk, CHUNK_SIZE);
			return -1;
		}
		page_pool.free_stack = new_stack;
		page_pool.stack_size = new_size;
	}

	/* Add all pages from new chunk to free stack */
	for (i = 0; i < PAGES_PER_CHUNK; i++)
		page_pool.free_stack[page_pool.stack_top++] = chunk + i * PAGE_SIZE;

	page_pool.chunks[page_pool.nr_chunks++] = chunk;

	pr_info("Allocated chunk %d: %d pages (total: %d MB)\n",
		page_pool.nr_chunks, PAGES_PER_CHUNK,
		page_pool.nr_chunks * CHUNK_SIZE / (1024 * 1024));
	return 0;
}

int page_pool_init(void)
{
	if (page_pool.initialized)
		return 0;

	page_pool.nr_chunks = 0;
	page_pool.stack_top = 0;
	page_pool.stack_size = PAGES_PER_CHUNK;
	page_pool.free_stack = xmalloc(page_pool.stack_size * sizeof(void *));
	if (!page_pool.free_stack)
		return -1;

	pthread_spin_init(&page_pool.lock, PTHREAD_PROCESS_PRIVATE);
	page_pool.initialized = true;

	/* Allocate first chunk */
	if (page_pool_grow() < 0) {
		xfree(page_pool.free_stack);
		page_pool.initialized = false;
		return -1;
	}

	return 0;
}

void page_pool_destroy(void)
{
	int i;

	if (!page_pool.initialized)
		return;

	for (i = 0; i < page_pool.nr_chunks; i++) {
		if (page_pool.chunks[i])
			munmap(page_pool.chunks[i], CHUNK_SIZE);
	}

	xfree(page_pool.free_stack);
	page_pool.free_stack = NULL;
	page_pool.nr_chunks = 0;
	page_pool.stack_top = 0;
	page_pool.initialized = false;

	pr_info("Page pool destroyed\n");
}

void *page_pool_get(void)
{
	void *page = NULL;

	if (!page_pool.initialized)
		return xmalloc(PAGE_SIZE);

	pthread_spin_lock(&page_pool.lock);

	/* If stack empty, grow pool */
	if (page_pool.stack_top == 0) {
		if (page_pool_grow() < 0) {
			pthread_spin_unlock(&page_pool.lock);
			return xmalloc(PAGE_SIZE);  /* Fallback */
		}
	}

	page_pool.stack_top--;
	page = page_pool.free_stack[page_pool.stack_top];

	pthread_spin_unlock(&page_pool.lock);
	return page;
}

void page_pool_put(void *page)
{
	if (!page)
		return;

	if (!page_pool.initialized) {
		xfree(page);
		return;
	}

	pthread_spin_lock(&page_pool.lock);

	/* Always return to pool - stack was sized for all allocated pages */
	page_pool.free_stack[page_pool.stack_top++] = page;

	pthread_spin_unlock(&page_pool.lock);
}
