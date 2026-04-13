/*
 * Per-Thread Page Pool - Lock-free allocation to avoid malloc/mprotect contention
 *
 * When 10 receiver threads allocate PAGE_SIZE buffers via malloc, glibc's heap
 * management triggers mprotect calls that serialize on the kernel's mmap_sem
 * write lock. This pool eliminates contention via:
 *
 *   - Per-thread bump allocator (zero locks on allocation path)
 *   - 256MB aligned chunks (O(1) chunk lookup from page address)
 *   - Atomic reference counting per chunk
 *   - munmap entire chunk when refcount reaches 0
 *
 * Memory layout per chunk (256MB aligned):
 *   +------------------+  <- 256MB aligned base
 *   | Chunk header     |     (refcount, validation pointer)
 *   | (4KB page 0)     |
 *   +------------------+
 *   | Page 1 (4KB)     |  <- First allocatable page
 *   | Page 2 (4KB)     |
 *   | ...              |
 *   | Page 65535       |
 *   +------------------+
 */

#include <sys/mman.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include <execinfo.h>
#include <stdlib.h>

#include "page.h"
#include "cow/page-pool.h"
#include "criu-log.h"
#include "common/bug.h"

#undef LOG_PREFIX
#define LOG_PREFIX "page-pool: "

#define CHUNK_SIZE       (256UL * 1024 * 1024)     /* 256MB */
#define CHUNK_ALIGN      CHUNK_SIZE
#define CHUNK_ALIGN_MASK (~(CHUNK_ALIGN - 1))
#define ALLOC_BATCH      64                        /* 64 pages = 256KB per allocation */
#define PAGES_PER_CHUNK  (CHUNK_SIZE / PAGE_SIZE)  /* 65536 pages */
#define MAX_THREADS      32
#define MAX_CHUNKS       512  /* 512 * 256MB = 128GB max */

/* Chunk header - stored at start of each 256MB region (uses page 0) */
struct chunk_header {
	atomic_int refcount;     /* Pages still in use */
	void *base;              /* Self-pointer for validation */
};

/* Per-thread pool state */
struct thread_pool {
	void *current_chunk;     /* Current chunk base address */
	int next_page;           /* Next page index to allocate */
	bool initialized;
};

static struct thread_pool pools[MAX_THREADS];
static void *all_chunks[MAX_CHUNKS];
static atomic_int nr_chunks;
static pthread_spinlock_t chunk_list_lock;  /* Only for chunk tracking */
static atomic_bool global_init_done;

/* Allocate a new 256MB aligned chunk */
static void *alloc_chunk(void)
{
	void *chunk;
	void *raw;
	struct chunk_header *hdr;
	size_t front_excess, back_excess;
	int idx;

	/*
	 * mmap with MAP_ANONYMOUS gives page-aligned memory.
	 * To get 256MB alignment, allocate extra and align manually.
	 */
	raw = mmap(NULL, CHUNK_SIZE + CHUNK_ALIGN, PROT_READ | PROT_WRITE,
		   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (raw == MAP_FAILED) {
		pr_perror("Failed to mmap 256MB chunk");
		return NULL;
	}

	/* Align to 256MB boundary */
	chunk = (void *)(((unsigned long)raw + CHUNK_ALIGN - 1) & CHUNK_ALIGN_MASK);

	/* Unmap the excess at front and back */
	front_excess = (size_t)(chunk - raw);
	back_excess = CHUNK_ALIGN - front_excess;
	if (front_excess > 0)
		munmap(raw, front_excess);
	if (back_excess > 0)
		munmap((char *)chunk + CHUNK_SIZE, back_excess);

	/* Initialize header (page 0) */
	hdr = (struct chunk_header *)chunk;
	atomic_init(&hdr->refcount, 0);  /* Incremented on each allocation */
	hdr->base = chunk;

	/* Track for cleanup */
	pthread_spin_lock(&chunk_list_lock);
	idx = atomic_load(&nr_chunks);
	if (idx < MAX_CHUNKS) {
		all_chunks[idx] = chunk;
		atomic_fetch_add(&nr_chunks, 1);
	}
	pthread_spin_unlock(&chunk_list_lock);

	pr_info("Allocated 256MB chunk at %p (total: %d chunks)\n",
		chunk, atomic_load(&nr_chunks));

	return chunk;
}

int page_pool_thread_init(int thread_id)
{
	BUG_ON(thread_id < 0 || thread_id >= MAX_THREADS);
	if (thread_id < 0 || thread_id >= MAX_THREADS) {
		pr_err("Invalid thread_id %d (max %d)\n", thread_id, MAX_THREADS);
		return -1;
	}

	if (pools[thread_id].initialized)
		return 0;

	/* First thread initializes the chunk list lock */
	if (!atomic_exchange(&global_init_done, true)) {
		pthread_spin_init(&chunk_list_lock, PTHREAD_PROCESS_PRIVATE);
		atomic_init(&nr_chunks, 0);
	}

	pools[thread_id].current_chunk = alloc_chunk();
	if (!pools[thread_id].current_chunk)
		return -1;

	pools[thread_id].next_page = 1;  /* Skip header page */
	pools[thread_id].initialized = true;

	pr_info("Thread %d pool initialized\n", thread_id);
	return 0;
}

void *page_pool_get(int thread_id)
{
	struct thread_pool *pool;
	void *page;

	if (thread_id < 0 || thread_id >= MAX_THREADS)
		return NULL;

	pool = &pools[thread_id];

	if (!pool->initialized)
		return NULL;

	/* Need new chunk? */
	if (pool->next_page >= PAGES_PER_CHUNK) {
		pool->current_chunk = alloc_chunk();
		if (!pool->current_chunk)
			return NULL;
		pool->next_page = 1;  /* Skip header */
	}

	/* Lock-free allocation: just bump the pointer */
	page = (char *)pool->current_chunk + (pool->next_page * PAGE_SIZE);
	pool->next_page++;
	atomic_fetch_add(&((struct chunk_header *)pool->current_chunk)->refcount, 1);

	return page;
}

/*
 * Get a contiguous 256KB batch (64 pages) for direct decompression.
 * Returns pointer to first page of the batch.
 * Each page must be freed individually with page_pool_put().
 * Interleaved alloc/free is allowed (unused pages can be freed immediately).
 */
void *page_pool_get_chunk(int thread_id, int *out_nr_pages)
{
	struct thread_pool *pool;
	void *batch_start;

	if (thread_id < 0 || thread_id >= MAX_THREADS)
		return NULL;

	pool = &pools[thread_id];

	if (!pool->initialized)
		return NULL;

	/* Need new chunk if not enough pages left for a batch */
	if (pool->next_page + ALLOC_BATCH > PAGES_PER_CHUNK) {
		pool->current_chunk = alloc_chunk();
		if (!pool->current_chunk)
			return NULL;
		pool->next_page = 1;  /* Skip header page */
	}

	/* Allocate ALLOC_BATCH contiguous pages */
	batch_start = (char *)pool->current_chunk + (pool->next_page * PAGE_SIZE);
	pool->next_page += ALLOC_BATCH;
	atomic_fetch_add(&((struct chunk_header *)pool->current_chunk)->refcount, ALLOC_BATCH);

	*out_nr_pages = ALLOC_BATCH;

	return batch_start;
}

void page_pool_put(void *page)
{
	struct chunk_header *hdr;
	int old_ref;

	if (!page)
		return;

	/* Calculate chunk base from page address (256MB aligned) */
	hdr = (struct chunk_header *)((unsigned long)page & CHUNK_ALIGN_MASK);

	/* Validate - check self-pointer */
	if (hdr->base != hdr) {
		pr_err("BUG: page_pool_put called with invalid page %p\n", page);
		BUG();
	}

	/* Atomic decrement */
	old_ref = atomic_fetch_sub(&hdr->refcount, 1);

	/* Last reference? munmap the entire chunk */
	if (old_ref == 1) {
		pr_err("PAGE_POOL: Freeing 256MB chunk at %p (all pages returned)\n", hdr);

		/* Remove from tracking list */
		pthread_spin_lock(&chunk_list_lock);
		for (int i = 0; i < atomic_load(&nr_chunks); i++) {
			if (all_chunks[i] == hdr) {
				all_chunks[i] = NULL;
				break;
			}
		}
		pthread_spin_unlock(&chunk_list_lock);

		munmap(hdr, CHUNK_SIZE);
	}
}

void page_pool_destroy_all(void)
{
	int i, n;

	if (!atomic_load(&global_init_done))
		return;

	pthread_spin_lock(&chunk_list_lock);
	n = atomic_load(&nr_chunks);
	for (i = 0; i < n; i++) {
		if (all_chunks[i]) {
			munmap(all_chunks[i], CHUNK_SIZE);
			all_chunks[i] = NULL;
		}
	}
	atomic_store(&nr_chunks, 0);
	pthread_spin_unlock(&chunk_list_lock);

	for (i = 0; i < MAX_THREADS; i++) {
		pools[i].initialized = false;
		pools[i].current_chunk = NULL;
		pools[i].next_page = 0;
	}

	pr_warn("All page pools destroyed\n");
}
