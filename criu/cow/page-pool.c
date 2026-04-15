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
#include "cow/cow-conf.h"
#include "criu-log.h"
#include "common/bug.h"

#undef LOG_PREFIX
#define LOG_PREFIX "page-pool: "

/* Pool configuration constants now in cow-conf.h */

/* Chunk header - stored at start of each 256MB region (uses page 0) */
struct chunk_header {
	atomic_int refcount;      /* Pages still in use */
	atomic_int max_allocated; /* High-water mark of pages allocated */
	atomic_int total_allocated; /* Debug: total pages ever allocated from this chunk */
	atomic_int total_freed;     /* Debug: total pages ever freed to this chunk */
	int chunk_idx;              /* Debug: index in all_chunks array (-1 if untracked) */
	void *base;               /* Self-pointer for validation */
};

/* Per-thread pool state */
struct thread_pool {
	void *current_chunk;     /* Current chunk base address */
	int next_page;           /* Next page index to allocate */
	bool initialized;
};

static struct thread_pool pools[COW_MAX_THREADS];
static void *all_chunks[COW_MAX_POOL_CHUNKS];
static atomic_int nr_chunks;
static pthread_spinlock_t chunk_list_lock;  /* Only for chunk tracking */
static atomic_bool global_init_done;
static atomic_ulong total_put_count;  /* Debug: total page_pool_put calls */
static atomic_ulong total_alloc_count;  /* Debug: total pages allocated */
static atomic_ulong total_alloc_calls; /* Debug: total page_pool_get_chunk calls */
static atomic_int total_chunks_freed;  /* Debug: total chunks freed (refcount→0) */
static atomic_bool drain_started;     /* Debug: set when drain begins */
static atomic_ulong puts_before_drain; /* Debug: page_pool_put calls before drain */
static atomic_int untracked_chunks;    /* Debug: chunks allocated after hitting limit */

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
	raw = mmap(NULL, COW_CHUNK_SIZE + COW_CHUNK_ALIGN, PROT_READ | PROT_WRITE,
		   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (raw == MAP_FAILED) {
		pr_perror("Failed to mmap 256MB chunk");
		return NULL;
	}

	/* Align to 256MB boundary */
	chunk = (void *)(((unsigned long)raw + COW_CHUNK_ALIGN - 1) & COW_CHUNK_ALIGN_MASK);

	/* Unmap the excess at front and back */
	front_excess = (size_t)(chunk - raw);
	back_excess = COW_CHUNK_ALIGN - front_excess;
	if (front_excess > 0)
		munmap(raw, front_excess);
	if (back_excess > 0)
		munmap((char *)chunk + COW_CHUNK_SIZE, back_excess);

	/* Initialize header (page 0) */
	hdr = (struct chunk_header *)chunk;
	atomic_init(&hdr->refcount, 0);      /* Incremented on each allocation */
	atomic_init(&hdr->max_allocated, 1); /* Start at 1 (header page) */
	atomic_init(&hdr->total_allocated, 0);
	atomic_init(&hdr->total_freed, 0);
	hdr->chunk_idx = -1;  /* Will be set if tracked */
	hdr->base = chunk;

	/* Track for cleanup */
	pthread_spin_lock(&chunk_list_lock);
	idx = atomic_load(&nr_chunks);
	if (idx < COW_MAX_POOL_CHUNKS) {
		all_chunks[idx] = chunk;
		hdr->chunk_idx = idx;
		atomic_fetch_add(&nr_chunks, 1);
	} else {
		/*
		 * Hit limit - try to reuse a NULL slot from a freed chunk.
		 * This fixes the bug where freed chunk slots were never reused.
		 */
		int reused_slot = -1;
		for (int i = 0; i < COW_MAX_POOL_CHUNKS; i++) {
			if (all_chunks[i] == NULL) {
				all_chunks[i] = chunk;
				hdr->chunk_idx = i;
				reused_slot = i;
				pr_debug("PAGE_POOL: Reused NULL slot %d for new chunk %p\n",
				       i, chunk);
				break;
			}
		}
		if (reused_slot < 0) {
			/* No NULL slots available - truly out of space */
			atomic_fetch_add(&untracked_chunks, 1);
			pr_err("PAGE_POOL: WARNING: Hit limit (%d) with no NULL slots! "
			       "total_freed=%d untracked=%d total_alloc=%lu total_put=%lu\n",
			       COW_MAX_POOL_CHUNKS, atomic_load(&total_chunks_freed),
			       atomic_load(&untracked_chunks),
			       atomic_load(&total_alloc_count), atomic_load(&total_put_count));
		}
	}
	pthread_spin_unlock(&chunk_list_lock);

	pr_debug("PAGE_POOL: Allocated 256MB chunk at %p (total: %d chunks)\n",
	       chunk, atomic_load(&nr_chunks));

	return chunk;
}

int page_pool_thread_init(int thread_id)
{
	BUG_ON(thread_id < 0 || thread_id >= COW_MAX_THREADS);
	if (thread_id < 0 || thread_id >= COW_MAX_THREADS) {
		pr_err("Invalid thread_id %d (max %d)\n", thread_id, COW_MAX_THREADS);
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

	if (thread_id < 0 || thread_id >= COW_MAX_THREADS)
		return NULL;

	pool = &pools[thread_id];

	if (!pool->initialized)
		return NULL;

	/* Need new chunk? */
	if (pool->next_page >= COW_PAGES_PER_CHUNK) {
		pool->current_chunk = alloc_chunk();
		if (!pool->current_chunk)
			return NULL;
		pool->next_page = 1;  /* Skip header */
	}

	/* Lock-free allocation: just bump the pointer */
	page = (char *)pool->current_chunk + (pool->next_page * PAGE_SIZE);
	pool->next_page++;

	{
		struct chunk_header *hdr = (struct chunk_header *)pool->current_chunk;
		atomic_fetch_add(&hdr->refcount, 1);
		atomic_fetch_add(&hdr->total_allocated, 1);
	}

	/* Debug: track single-page allocations */
	atomic_fetch_add(&total_alloc_count, 1);

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

	if (thread_id < 0 || thread_id >= COW_MAX_THREADS)
		return NULL;

	pool = &pools[thread_id];

	if (!pool->initialized)
		return NULL;

	/* Need new chunk if not enough pages left for a batch */
	if (pool->next_page + COW_ALLOC_BATCH > COW_PAGES_PER_CHUNK) {
		pool->current_chunk = alloc_chunk();
		if (!pool->current_chunk)
			return NULL;
		pool->next_page = 1;  /* Skip header page */
	}

	/* Allocate COW_ALLOC_BATCH contiguous pages */
	batch_start = (char *)pool->current_chunk + (pool->next_page * PAGE_SIZE);
	pool->next_page += COW_ALLOC_BATCH;

	/* Update refcount and max_allocated */
	{
		struct chunk_header *hdr = (struct chunk_header *)pool->current_chunk;
		int current_alloc = pool->next_page;
		int old_max;

		atomic_fetch_add(&hdr->refcount, COW_ALLOC_BATCH);
		atomic_fetch_add(&hdr->total_allocated, COW_ALLOC_BATCH);

		/* Atomically update max_allocated if we've allocated more */
		do {
			old_max = atomic_load(&hdr->max_allocated);
			if (current_alloc <= old_max)
				break;
		} while (!atomic_compare_exchange_weak(&hdr->max_allocated, &old_max, current_alloc));
	}

	/* Debug: track total allocations */
	{
		unsigned long alloc_cnt = atomic_fetch_add(&total_alloc_count, COW_ALLOC_BATCH) + COW_ALLOC_BATCH;
		unsigned long put_cnt = atomic_load(&total_put_count);
		if (alloc_cnt % COW_LOG_SAMPLE_1M < COW_ALLOC_BATCH) {
			pr_info("PAGE_POOL_ALLOC: total_alloc=%lu total_put=%lu diff=%lu\n",
			       alloc_cnt, put_cnt, alloc_cnt - put_cnt);
		}
	}

	*out_nr_pages = COW_ALLOC_BATCH;

	/* Debug: track allocation calls */
	atomic_fetch_add(&total_alloc_calls, 1);

	return batch_start;
}

/*
 * Get exactly nr_pages contiguous pages for direct decompression.
 * More efficient than page_pool_get_chunk() when exact count is known,
 * as it doesn't waste pages that would need to be freed immediately.
 *
 * Returns pointer to first page of the allocation, or NULL on failure.
 * Each page must be freed individually with page_pool_put().
 */
void *page_pool_get_pages(int thread_id, int nr_pages)
{
	struct thread_pool *pool;
	void *pages_start;

	if (thread_id < 0 || thread_id >= COW_MAX_THREADS)
		return NULL;

	if (nr_pages <= 0 || nr_pages > COW_PAGES_PER_CHUNK - 1)
		return NULL;

	pool = &pools[thread_id];

	if (!pool->initialized)
		return NULL;

	/* Need new chunk if not enough pages left */
	if (pool->next_page + nr_pages > COW_PAGES_PER_CHUNK) {
		pool->current_chunk = alloc_chunk();
		if (!pool->current_chunk)
			return NULL;
		pool->next_page = 1;  /* Skip header page */
	}

	/* Allocate exactly nr_pages contiguous pages */
	pages_start = (char *)pool->current_chunk + (pool->next_page * PAGE_SIZE);
	pool->next_page += nr_pages;

	/* Update refcount and max_allocated */
	{
		struct chunk_header *hdr = (struct chunk_header *)pool->current_chunk;
		int current_alloc = pool->next_page;
		int old_max;

		atomic_fetch_add(&hdr->refcount, nr_pages);
		atomic_fetch_add(&hdr->total_allocated, nr_pages);

		/* Atomically update max_allocated if we've allocated more */
		do {
			old_max = atomic_load(&hdr->max_allocated);
			if (current_alloc <= old_max)
				break;
		} while (!atomic_compare_exchange_weak(&hdr->max_allocated, &old_max, current_alloc));
	}

	/* Debug: track total allocations */
	atomic_fetch_add(&total_alloc_count, nr_pages);

	return pages_start;
}

void page_pool_put(void *page)
{
	struct chunk_header *hdr;
	int old_ref;

	if (!page)
		return;

	/* Track puts before drain started */
	if (!atomic_load(&drain_started))
		atomic_fetch_add(&puts_before_drain, 1);

	/* Calculate chunk base from page address (256MB aligned) */
	hdr = (struct chunk_header *)((unsigned long)page & COW_CHUNK_ALIGN_MASK);

	/* Validate - check self-pointer */
	if (hdr->base != hdr) {
		pr_err("BUG: page_pool_put called with invalid page %p\n", page);
		BUG();
	}

	/* Atomic decrement and track per-chunk frees */
	old_ref = atomic_fetch_sub(&hdr->refcount, 1);
	atomic_fetch_add(&hdr->total_freed, 1);

	/* Debug: periodically log put progress */
	{
		unsigned long put_cnt = atomic_fetch_add(&total_put_count, 1) + 1;
		if (put_cnt % COW_LOG_SAMPLE_1M == 0) {
			pr_err("PAGE_POOL_PUT: total=%lu page=%p chunk=%p[%d] refcount_was=%d "
			       "chunk_alloc=%d chunk_freed=%d\n",
			       put_cnt, page, hdr, hdr->chunk_idx, old_ref,
			       atomic_load(&hdr->total_allocated),
			       atomic_load(&hdr->total_freed));
		}
	}

	/* Debug: log when chunk is getting close to being freed */
	if (old_ref <= COW_REFCOUNT_LOW && old_ref > 1 &&
	    (old_ref == COW_REFCOUNT_LOW || old_ref == 500 || old_ref == 100 || old_ref == 10)) {
		pr_err("PAGE_POOL_LOW: chunk=%p[%d] refcount_now=%d alloc=%d freed=%d\n",
		       hdr, hdr->chunk_idx, old_ref - 1,
		       atomic_load(&hdr->total_allocated),
		       atomic_load(&hdr->total_freed));
	}

	/* Last reference? munmap the entire chunk */
	if (old_ref == 1) {
		int freed_count = atomic_fetch_add(&total_chunks_freed, 1) + 1;
		int chunk_idx = hdr->chunk_idx;
		int total_alloc = atomic_load(&hdr->total_allocated);
		int total_free = atomic_load(&hdr->total_freed);

		pr_err("PAGE_POOL_FREE: chunk=%p[%d] freed_count=%d alloc=%d freed=%d "
		       "(should match: %s)\n",
		       hdr, chunk_idx, freed_count, total_alloc, total_free,
		       total_alloc == total_free ? "YES" : "NO - LEAK!");

		/* Remove from tracking list */
		pthread_spin_lock(&chunk_list_lock);
		if (chunk_idx >= 0 && chunk_idx < COW_MAX_POOL_CHUNKS) {
			if (all_chunks[chunk_idx] == hdr) {
				all_chunks[chunk_idx] = NULL;
			} else {
				pr_err("PAGE_POOL_FREE: WARNING chunk[%d] mismatch! "
				       "expected=%p actual=%p\n",
				       chunk_idx, hdr, all_chunks[chunk_idx]);
			}
		} else {
			pr_err("PAGE_POOL_FREE: chunk=%p was untracked (idx=%d)\n",
			       hdr, chunk_idx);
		}
		pthread_spin_unlock(&chunk_list_lock);

		munmap(hdr, COW_CHUNK_SIZE);
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
			munmap(all_chunks[i], COW_CHUNK_SIZE);
			all_chunks[i] = NULL;
		}
	}
	atomic_store(&nr_chunks, 0);
	pthread_spin_unlock(&chunk_list_lock);

	for (i = 0; i < COW_MAX_THREADS; i++) {
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
	int null_slots = 0;
	unsigned long total_outstanding = 0;
	unsigned long total_ever_allocated = 0;
	unsigned long total_ever_freed = 0;

	if (!atomic_load(&global_init_done))
		return;

	pthread_spin_lock(&chunk_list_lock);
	n = atomic_load(&nr_chunks);
	for (i = 0; i < n; i++) {
		if (all_chunks[i]) {
			struct chunk_header *hdr = all_chunks[i];
			int ref = atomic_load(&hdr->refcount);
			total_outstanding += ref;
			total_ever_allocated += atomic_load(&hdr->total_allocated);
			total_ever_freed += atomic_load(&hdr->total_freed);
			if (ref < min_ref) min_ref = ref;
			if (ref > max_ref) max_ref = ref;
			if (ref < COW_REFCOUNT_LOW) low_count++;
			else if (ref < COW_REFCOUNT_MID) mid_count++;
			else high_count++;
		} else {
			null_slots++;
		}
	}
	pthread_spin_unlock(&chunk_list_lock);

	pr_err("PAGE_POOL_STATS: chunks=%d (null=%d freed=%d untracked=%d) outstanding=%lu | "
	       "low(<1k)=%d mid(1k-30k)=%d high(>30k)=%d | min=%d max=%d\n",
	       n, null_slots, atomic_load(&total_chunks_freed), atomic_load(&untracked_chunks),
	       total_outstanding, low_count, mid_count, high_count,
	       min_ref == INT_MAX ? 0 : min_ref, max_ref);
	pr_err("PAGE_POOL_STATS: global_alloc=%lu global_put=%lu diff=%lu | "
	       "chunk_alloc=%lu chunk_freed=%lu diff=%lu\n",
	       atomic_load(&total_alloc_count), atomic_load(&total_put_count),
	       atomic_load(&total_alloc_count) - atomic_load(&total_put_count),
	       total_ever_allocated, total_ever_freed,
	       total_ever_allocated - total_ever_freed);
}

/* Debug: show chunk utilization (how full each chunk got) */
void page_pool_dump_utilization(void)
{
	int i, n;
	unsigned long total_allocated = 0, total_capacity = 0;
	int full_chunks = 0, partial_chunks = 0, empty_chunks = 0;
	int capacity = COW_PAGES_PER_CHUNK - 1;  /* minus header page */

	if (!atomic_load(&global_init_done))
		return;

	pthread_spin_lock(&chunk_list_lock);
	n = atomic_load(&nr_chunks);
	for (i = 0; i < n; i++) {
		if (all_chunks[i]) {
			struct chunk_header *hdr = all_chunks[i];
			int allocated = atomic_load(&hdr->max_allocated);
			total_allocated += allocated;
			total_capacity += capacity;

			/* Categorize by utilization */
			if (allocated > capacity * 9 / 10)
				full_chunks++;
			else if (allocated > capacity / 10)
				partial_chunks++;
			else
				empty_chunks++;
		}
	}
	pthread_spin_unlock(&chunk_list_lock);

	pr_err("PAGE_POOL_UTILIZATION: chunks=%d allocated=%lu capacity=%lu (%.1f%%) | "
	       "full(>90%%)=%d partial=%d empty(<10%%)=%d\n",
	       n, total_allocated, total_capacity,
	       total_capacity > 0 ? (float)total_allocated / total_capacity * 100 : 0,
	       full_chunks, partial_chunks, empty_chunks);
}

/* Mark drain as started and report puts before drain */
void page_pool_mark_drain_started(void)
{
	atomic_store(&drain_started, true);
	pr_err("PAGE_POOL: Drain started, puts_before_drain=%lu total_alloc=%lu\n",
	       atomic_load(&puts_before_drain), atomic_load(&total_alloc_count));
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
	hdr = (struct chunk_header *)((unsigned long)page & COW_CHUNK_ALIGN_MASK);

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

/*
 * Debug: Dump detailed info for chunks that still have outstanding refs.
 * Shows which chunks are blocking memory release.
 */
void page_pool_dump_blocking_chunks(int max_to_show)
{
	int i, n, shown = 0;
	int total_blocking = 0;
	unsigned long blocking_pages = 0;

	if (!atomic_load(&global_init_done))
		return;

	pr_err("PAGE_POOL_BLOCKING: Scanning for chunks with outstanding refs...\n");

	pthread_spin_lock(&chunk_list_lock);
	n = atomic_load(&nr_chunks);
	for (i = 0; i < n && shown < max_to_show; i++) {
		if (all_chunks[i]) {
			struct chunk_header *hdr = all_chunks[i];
			int ref = atomic_load(&hdr->refcount);
			int alloc = atomic_load(&hdr->total_allocated);
			int freed = atomic_load(&hdr->total_freed);

			if (ref > 0) {
				total_blocking++;
				blocking_pages += ref;

				if (shown < max_to_show) {
					pr_err("  BLOCKING[%d]: chunk=%p idx=%d ref=%d "
					       "alloc=%d freed=%d delta=%d\n",
					       shown, hdr, hdr->chunk_idx, ref,
					       alloc, freed, alloc - freed);
					shown++;
				}
			}
		}
	}
	pthread_spin_unlock(&chunk_list_lock);

	pr_err("PAGE_POOL_BLOCKING: total_blocking=%d blocking_pages=%lu "
	       "(%.1f GB stuck)\n",
	       total_blocking, blocking_pages,
	       (float)(blocking_pages * 4096) / (1024 * 1024 * 1024));
}

/*
 * Debug: Get summary of current memory state.
 * Returns outstanding page count via pointer, returns chunk count.
 */
int page_pool_get_memory_state(unsigned long *outstanding_pages,
			       unsigned long *total_allocated,
			       unsigned long *total_freed)
{
	int i, n;
	int active_chunks = 0;
	unsigned long outstanding = 0, alloc = 0, freed = 0;

	if (!atomic_load(&global_init_done)) {
		*outstanding_pages = 0;
		*total_allocated = 0;
		*total_freed = 0;
		return 0;
	}

	pthread_spin_lock(&chunk_list_lock);
	n = atomic_load(&nr_chunks);
	for (i = 0; i < n; i++) {
		if (all_chunks[i]) {
			struct chunk_header *hdr = all_chunks[i];
			outstanding += atomic_load(&hdr->refcount);
			alloc += atomic_load(&hdr->total_allocated);
			freed += atomic_load(&hdr->total_freed);
			active_chunks++;
		}
	}
	pthread_spin_unlock(&chunk_list_lock);

	*outstanding_pages = outstanding;
	*total_allocated = alloc;
	*total_freed = freed;
	return active_chunks;
}
