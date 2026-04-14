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
#include <limits.h>

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
#define MAX_CHUNKS       2048  /* 2048 * 256MB = 512GB max */

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
static atomic_ulong total_put_count;  /* Debug: total page_pool_put calls */
static atomic_ulong total_alloc_count;  /* Debug: total pages allocated */

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
	} else {
		/*
		 * Hit MAX_CHUNKS limit! Chunk is allocated but NOT tracked.
		 * page_pool_get_chunk_id() will return -1 for pages from this chunk,
		 * causing them to be skipped by chunk-ordered drain (fallback handles them).
		 * Consider increasing MAX_CHUNKS if this happens.
		 */
		pr_err("PAGE_POOL: WARNING: Hit MAX_CHUNKS limit (%d)! "
		       "Chunk at %p NOT TRACKED - will use fallback drain\n",
		       MAX_CHUNKS, chunk);
	}
	pthread_spin_unlock(&chunk_list_lock);

	pr_err("PAGE_POOL: Allocated 256MB chunk at %p (total: %d chunks)\n",
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

	/* Debug: track total allocations */
	{
		unsigned long alloc_cnt = atomic_fetch_add(&total_alloc_count, ALLOC_BATCH) + ALLOC_BATCH;
		unsigned long put_cnt = atomic_load(&total_put_count);
		if (alloc_cnt % 1000000 < ALLOC_BATCH) {
			pr_info("PAGE_POOL_ALLOC: total_alloc=%lu total_put=%lu diff=%lu\n",
			       alloc_cnt, put_cnt, alloc_cnt - put_cnt);
		}
	}

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

	/* Debug: periodically log put progress */
	{
		unsigned long put_cnt = atomic_fetch_add(&total_put_count, 1) + 1;
		if (put_cnt % 1000000 == 0) {
			pr_info("PAGE_POOL_PUT: total=%lu page=%p chunk=%p refcount_was=%d\n",
			       put_cnt, page, hdr, old_ref);
		}
	}

	/* Debug: log when chunk is getting close to being freed */
	if (old_ref <= 1000 && old_ref > 1 && (old_ref == 1000 || old_ref == 500 || old_ref == 100 || old_ref == 10)) {
		pr_info("PAGE_POOL_LOW: chunk=%p refcount_now=%d (close to free!)\n",
		       hdr, old_ref - 1);
	}

	/* Last reference? munmap the entire chunk */
	if (old_ref == 1) {
		pr_info("PAGE_POOL: Freeing 256MB chunk at %p (all pages returned)\n", hdr);

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

/* Debug: print chunk stats to see refcount distribution */
void page_pool_dump_stats(void)
{
	int i, n;
	int low_count = 0, mid_count = 0, high_count = 0;
	int min_ref = INT_MAX, max_ref = 0;
	unsigned long total_outstanding = 0;

	if (!atomic_load(&global_init_done))
		return;

	pthread_spin_lock(&chunk_list_lock);
	n = atomic_load(&nr_chunks);
	for (i = 0; i < n; i++) {
		if (all_chunks[i]) {
			struct chunk_header *hdr = all_chunks[i];
			int ref = atomic_load(&hdr->refcount);
			total_outstanding += ref;
			if (ref < min_ref) min_ref = ref;
			if (ref > max_ref) max_ref = ref;
			if (ref < 1000) low_count++;
			else if (ref < 30000) mid_count++;
			else high_count++;
		}
	}
	pthread_spin_unlock(&chunk_list_lock);

	pr_err("PAGE_POOL_STATS: chunks=%d outstanding=%lu | low(<1k)=%d mid(1k-30k)=%d high(>30k)=%d | min=%d max=%d\n",
	       n, total_outstanding, low_count, mid_count, high_count,
	       min_ref == INT_MAX ? 0 : min_ref, max_ref);
}

/*
 * Get chunk ID from a page data pointer.
 * Returns chunk index (0 to nr_chunks-1) or -1 if not found.
 * Used by drain to group pages by chunk for ordered freeing.
 */
int page_pool_get_chunk_id(void *page)
{
	struct chunk_header *hdr;
	int i, n;

	if (!page || !atomic_load(&global_init_done))
		return -1;

	/* Calculate chunk base from page address */
	hdr = (struct chunk_header *)((unsigned long)page & CHUNK_ALIGN_MASK);

	/* Validate self-pointer */
	if (hdr->base != hdr)
		return -1;

	/* Find chunk index in tracking array */
	pthread_spin_lock(&chunk_list_lock);
	n = atomic_load(&nr_chunks);
	for (i = 0; i < n; i++) {
		if (all_chunks[i] == hdr) {
			pthread_spin_unlock(&chunk_list_lock);
			return i;
		}
	}
	pthread_spin_unlock(&chunk_list_lock);

	return -1;
}

/* Get current number of allocated chunks */
int page_pool_get_nr_chunks(void)
{
	if (!atomic_load(&global_init_done))
		return 0;
	return atomic_load(&nr_chunks);
}
