#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <linux/userfaultfd.h>

#include "int.h"
#include "page.h"
#include "cow/cow-uffd.h"
#include "cow/cow-bulk-send.h"
#include "cow/cow-bulk-recv.h"
#include "uffd.h"
#include "uffd-internal.h"
#include "page-xfer.h"
#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"
#include "common/bug.h"
#include "cow/pf-tracker.h"
#include "cow/page-pool.h"
#include "cow/cow-batch-bitmap.h"
#include "cow/unmapped-tracker.h"
#include "cow/page-state-tracker.h"
#include "pstree.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-uffd: "

/*
 * Main thread reuses P3 thread 0's pool for Phase 4 dirty pages.
 * P3 receivers are stopped by Phase 4, so no contention.
 */
#define PHASE4_POOL_ID 0

/*
 * 256KB-aligned batch buffer entry.
 * Each entry holds up to COW_BATCH_PAGES (64) contiguous pages.
 * A bitmap tracks which pages within the batch are valid.
 * Drain can issue a single UFFDIO_COPY for the entire batch.
 */
#define BATCH_ENTRY_MAGIC	0xBA7C4E71  /* "BATCH_ENTRY" alive */
#define BATCH_ENTRY_DEAD	0xDEADBEEF  /* freed */

struct batch_buffer_entry {
	unsigned int magic;		/* BATCH_ENTRY_MAGIC or BATCH_ENTRY_DEAD */
	unsigned long base_vaddr;	/* 256KB-aligned start address */
	void *data;			/* Contiguous page pool allocation */
	cow_batch_bitmap_t page_bitmap;	/* 1 = page present, 0 = absent */
	cow_batch_bitmap_t initial_bitmap; /* Bits ever set (for drain free accounting) */
	int nr_pages;			/* popcount(page_bitmap) */
	struct hlist_node hash;
	struct list_head chunk_list;	/* Link in chunk's list for ordered drain */
	int chunk_id;			/* Cached page-pool chunk ID */
};

static struct {
	struct hlist_head *hash_table;
	unsigned long nr_batches;	/* Number of batch entries */
	unsigned long nr_pages;		/* Total individual pages buffered */
	unsigned long nr_applied;
	unsigned long nr_discarded;
	unsigned long nr_eagain;
	bool initialized;
} cow_buffer = { .initialized = false };

/*
 * Chunk-ordered drain index.
 * Allows draining batches grouped by their page pool chunk, so chunks
 * can be freed progressively instead of all at the end.
 */
struct chunk_drain_entry {
	struct list_head batches;	/* List of batch_buffer_entry in this chunk */
	pthread_spinlock_t lock;	/* Per-chunk lock for drain */
	atomic_int batch_count;		/* Number of batches in this chunk's list */
};

static struct chunk_drain_entry chunk_index[COW_MAX_POOL_CHUNKS];
static atomic_bool chunk_index_initialized = false;
static atomic_int nr_active_chunks = 0;

/* Fine-grained locks for batch buffer */
static pthread_spinlock_t hash_locks[COW_BATCH_NUM_HASH_LOCKS];


static inline int lock_index(unsigned int hash)
{
	return hash / COW_BATCH_BUCKETS_PER_LOCK;
}

/*
 * Multithreaded drain configuration.
 * Each thread handles a range of chunks for parallel draining.
 * COW_NUM_DRAIN_THREADS now defined as COW_COW_NUM_DRAIN_THREADS in cow-conf.h
 */

struct drain_thread_args {
	int thread_id;
};

static pthread_t drain_threads[COW_NUM_DRAIN_THREADS];
static struct drain_thread_args drain_args[COW_NUM_DRAIN_THREADS];
static atomic_bool drain_thread_stop = false;
static atomic_int drain_threads_active = 0;
static struct list_head *drain_lpis = NULL;  /* lpis list for EAGAIN handling */
static atomic_ulong total_drained = 0;  /* Total pages drained across all threads */
static atomic_int next_drain_chunk = 0;  /* Work-stealing: next chunk to process */
static int max_drain_chunks = 0;  /* Total chunks to drain */
static struct timespec drain_start_time;  /* For TIMING prefix debug */


static inline unsigned int batch_buffer_hash(unsigned long vaddr)
{
	return (vaddr >> COW_BATCH_SHIFT) & (COW_BATCH_BUFFER_HASH_SIZE - 1);
}

static inline unsigned long batch_align(unsigned long vaddr)
{
	return vaddr & COW_BATCH_ALIGN_MASK;
}

static inline int batch_page_index(unsigned long vaddr)
{
	return (vaddr >> PAGE_SHIFT) & (COW_BATCH_PAGES - 1);
}

/*
 * Result codes for cow_uffd_copy_pages()
 */
enum cow_copy_result {
	COW_COPY_OK = 0,
	COW_COPY_EAGAIN = 1,
	COW_COPY_EEXIST = 2,
	COW_COPY_ENOENT = 3,
	COW_COPY_ERROR = -1,
};

/*
 * Unified UFFDIO_COPY wrapper for COW mode.
 * Performs the ioctl and handles soft errors uniformly.
 *
 * @uffd: userfaultfd file descriptor
 * @dst: destination address in target process
 * @src: source buffer
 * @nr_pages: number of pages to copy
 * @copied_out: if non-NULL, set to number of pages actually copied on success
 *
 * Returns: COW_COPY_OK on success, or appropriate error code
 */
static enum cow_copy_result cow_uffd_copy_pages(int uffd, unsigned long dst,
						void *src, unsigned long nr_pages,
						unsigned long *copied_out)
{
	struct uffdio_copy uffd_copy = {
		.dst = dst,
		.src = (unsigned long)src,
		.len = nr_pages * PAGE_SIZE,
		.mode = 0,
		.copy = 0,
	};

	if (ioctl(uffd, UFFDIO_COPY, &uffd_copy) < 0) {
		switch (errno) {
		case EAGAIN:
			return COW_COPY_EAGAIN;
		case EEXIST:
			return COW_COPY_EEXIST;
		case ENOENT:
			return COW_COPY_ENOENT;
		default:
			return COW_COPY_ERROR;
		}
	}

	/* Check for soft error (error returned in .copy field) */
	if (uffd_copy.copy < 0) {
		errno = -uffd_copy.copy;
		switch (errno) {
		case EAGAIN:
			return COW_COPY_EAGAIN;
		case EEXIST:
			return COW_COPY_EEXIST;
		case ENOENT:
			return COW_COPY_ENOENT;
		default:
			return COW_COPY_ERROR;
		}
	}

	if (copied_out)
		*copied_out = uffd_copy.copy / PAGE_SIZE;

	return COW_COPY_OK;
}

/* COW_TRACK_* flags are defined in cow-uffd.h */

/*
 * Unified UFFDIO_COPY with full tracking.
 * Handles buffer stats, page state, unmapped tracker, and EAGAIN queue.
 *
 * @uffd: userfaultfd file descriptor
 * @vaddr: destination virtual address
 * @data: source data buffer
 * @nr_pages: number of pages to copy
 * @lpi: lazy_pages_info (NULL for drain mode)
 * @lpis: list of lpis for drain EAGAIN queue (NULL if lpi provided)
 * @flags: COW_TRACK_* flags
 * @caller: caller name for debug messages
 *
 * Returns:
 *   1 - success (page copied)
 *   0 - soft handled (ENOENT unmapped, EAGAIN queued, EEXIST already done)
 *  -1 - error
 *  -EAGAIN - kernel busy (only with COW_TRACK_RETRY flag)
 */
int cow_uffd_copy(int uffd, unsigned long vaddr, void *data,
		  unsigned long nr_pages,
		  struct lazy_pages_info *lpi,
		  struct list_head *lpis,
		  unsigned int flags,
		  const char *caller)
{
	enum cow_copy_result res;

	res = cow_uffd_copy_pages(uffd, vaddr, data, nr_pages, NULL);

	switch (res) {
	case COW_COPY_OK: {
		unsigned long i;

		if (!(flags & COW_TRACK_RETRY))
			__sync_fetch_and_add(&cow_buffer.nr_applied, 1);
		pf_tracker_set_state(vaddr, PF_STATE_COMPLETED);
		/*
		 * UFFDIO_COPY installed all nr_pages; transition every page in
		 * the range so the tracker reflects reality (drain fast path
		 * copies 64 pages in one call — marking only the base leaves
		 * the other 63 stuck at DRAIN_PENDING).
		 */
		for (i = 0; i < nr_pages; i++)
			page_state_set(vaddr + i * PAGE_SIZE, PAGE_STATE_COPIED);
		if (lpi)
			lpi->copied_pages += nr_pages;
		return 1;
	}

	case COW_COPY_EEXIST: {
		unsigned long i;

		if (!(flags & COW_TRACK_RETRY))
			__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
		for (i = 0; i < nr_pages; i++) {
			unsigned long addr = vaddr + i * PAGE_SIZE;

			if (!unmapped_tracker_is_unmapped(addr) &&
			    page_state_get(addr) != PAGE_STATE_DIRTY)
				page_state_set(addr, PAGE_STATE_DISCARDED);
		}
		if (flags & COW_TRACK_STRICT) {
			pr_err("BUG: %s EEXIST at 0x%lx - duplicate copy!\n", caller, vaddr);
			page_state_print_history(vaddr);
			BUG();
		}
		return 0;  /* soft handled - drain already did it */
	}

	case COW_COPY_ENOENT: {
		unsigned long i;

		if (!(flags & COW_TRACK_RETRY))
			__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
		if (!unmapped_tracker_is_unmapped(vaddr)) {
			for (i = 0; i < nr_pages; i++)
				page_state_set(vaddr + i * PAGE_SIZE, PAGE_STATE_DISCARDED);
			unmapped_tracker_mark_range(vaddr, nr_pages * PAGE_SIZE);
		}
		return 0;
	}

	case COW_COPY_EAGAIN:
		if (flags & COW_TRACK_RETRY)
			return -EAGAIN;
		__sync_fetch_and_add(&cow_buffer.nr_eagain, 1);
		pr_err("EAGAIN_DEBUG: %s EAGAIN at 0x%lx nr_pages=%lu\n",
		       caller, vaddr, nr_pages);
		if (lpis) {
			/* Drain mode - queue copies data, caller frees original */
			cow_queue_drain_eagain_request(lpis, vaddr, data);
		} else if (lpi) {
			/* Normal mode - queue copies data */
			pf_tracker_set_state(vaddr, PF_STATE_PENDING_EAGAIN);
			cow_queue_eagain_request(lpi, vaddr, nr_pages, data, caller);
		}
		return 0;

	case COW_COPY_ERROR:
		pr_err("%s: 0x%lx FAILED errno=%d\n", caller, vaddr, errno);
		page_state_print_history(vaddr);
		BUG();
	}

	return -1;  /* unreachable */
}

int cow_page_buffer_init(void)
{
	int i;

	if (cow_buffer.initialized)
		return 0;

	cow_buffer.hash_table = xmalloc(COW_BATCH_BUFFER_HASH_SIZE *
					sizeof(struct hlist_head));
	BUG_ON(!cow_buffer.hash_table);

	for (i = 0; i < COW_BATCH_BUFFER_HASH_SIZE; i++)
		INIT_HLIST_HEAD(&cow_buffer.hash_table[i]);

	for (i = 0; i < COW_BATCH_NUM_HASH_LOCKS; i++)
		pthread_spin_init(&hash_locks[i], PTHREAD_PROCESS_PRIVATE);

	/* Initialize chunk drain index */
	for (i = 0; i < COW_MAX_POOL_CHUNKS; i++) {
		INIT_LIST_HEAD(&chunk_index[i].batches);
		pthread_spin_init(&chunk_index[i].lock, PTHREAD_PROCESS_PRIVATE);
		atomic_init(&chunk_index[i].batch_count, 0);
	}
	atomic_store(&chunk_index_initialized, true);

	cow_buffer.nr_batches = 0;
	cow_buffer.nr_pages = 0;
	cow_buffer.nr_applied = 0;
	cow_buffer.nr_discarded = 0;
	cow_buffer.nr_eagain = 0;
	cow_buffer.initialized = true;

	pr_info("COW batch buffer initialized (buckets=%d, locks=%d, chunk_slots=%d)\n",
		COW_BATCH_BUFFER_HASH_SIZE, COW_BATCH_NUM_HASH_LOCKS, COW_MAX_POOL_CHUNKS);
	return 0;
}

int cow_page_buffer_thread_init(int thread_id)
{
	return page_pool_thread_init(thread_id);
}

/*
 * Add a contiguous run of pages to the buffer at a given offset within
 * a 256KB-aligned batch.
 *
 * @base_vaddr: 256KB-aligned start address of the batch
 * @data: page data at data + page_offset * PAGE_SIZE
 * @nr_pages: number of valid pages (1..COW_BATCH_PAGES)
 * @page_offset: index of first valid page within the batch (0..63)
 * @thread_id: pool thread id (used only when creating new entry from temp data)
 * @owns_data: true = data is a COW_BATCH_PAGES pool allocation, ownership
 *             transferred. false = data is a temp buffer, only read from it.
 *
 * Bitmap bits [page_offset .. page_offset+nr_pages) are set.
 */
int cow_page_buffer_add_batch(unsigned long base_vaddr, void *data,
			      int nr_pages, int page_offset,
			      int thread_id, bool owns_data)
{
	struct batch_buffer_entry *entry;
	unsigned int hash;
	int lock_idx;
	cow_batch_bitmap_t new_bitmap;
	int i;

	BUG_ON(!cow_buffer.initialized);
	BUG_ON(base_vaddr != batch_align(base_vaddr));
	BUG_ON(nr_pages <= 0 || nr_pages > COW_BATCH_PAGES);
	if (page_offset < 0 || page_offset + nr_pages > COW_BATCH_PAGES) {
		pr_err("BUG: add_batch overflow: base=0x%lx offset=%d nr_pages=%d sum=%d max=%d\n",
		       base_vaddr, page_offset, nr_pages, page_offset + nr_pages, COW_BATCH_PAGES);
		BUG();
	}

	cow_batch_bitmap_zero(&new_bitmap);
	cow_batch_bitmap_set_range(&new_bitmap, page_offset, nr_pages);

	hash = batch_buffer_hash(base_vaddr);
	lock_idx = lock_index(hash);

	pthread_spin_lock(&hash_locks[lock_idx]);

	/* Check if batch entry already exists (dirty re-send) */
	hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
		if (entry->base_vaddr == base_vaddr) {
			/* Overwrite pages in existing batch */
			for (i = 0; i < nr_pages; i++) {
				int idx = page_offset + i;

				memcpy((char *)entry->data + idx * PAGE_SIZE,
				       (char *)data + idx * PAGE_SIZE, PAGE_SIZE);

				if (!cow_batch_bitmap_test(&entry->page_bitmap, idx)) {
					cow_batch_bitmap_set(&entry->page_bitmap, idx);
					cow_batch_bitmap_set(&entry->initial_bitmap, idx);
					entry->nr_pages++;
					__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
				}
				page_state_set_with_crc(base_vaddr + idx * PAGE_SIZE,
							PAGE_STATE_IN_BUFFER,
							(char *)entry->data + idx * PAGE_SIZE);
			}
			pthread_spin_unlock(&hash_locks[lock_idx]);

			/* Free incoming pool buffer if caller passed ownership */
			if (owns_data) {
				for (i = 0; i < COW_BATCH_PAGES; i++)
					page_pool_put((char *)data + i * PAGE_SIZE);
			}
			return 0;
		}
	}

	/* New entry */
	{
		void *batch_data;

		if (owns_data) {
			/* Take ownership of caller's pool buffer (zero copy) */
			batch_data = data;
		} else {
			/* Allocate pool buffer and copy from temp data */
			batch_data = page_pool_get_pages(thread_id, COW_BATCH_PAGES);
			BUG_ON(!batch_data);
			memcpy((char *)batch_data + page_offset * PAGE_SIZE,
			       (char *)data + page_offset * PAGE_SIZE,
			       nr_pages * PAGE_SIZE);
		}

	entry = xmalloc(sizeof(*entry));
	BUG_ON(!entry);

	entry->magic = BATCH_ENTRY_MAGIC;
	entry->base_vaddr = base_vaddr;
	entry->data = batch_data;
	cow_batch_bitmap_copy(&entry->page_bitmap, &new_bitmap);
	cow_batch_bitmap_copy(&entry->initial_bitmap, &new_bitmap);
	entry->nr_pages = nr_pages;
	INIT_HLIST_NODE(&entry->hash);
	INIT_LIST_HEAD(&entry->chunk_list);
	entry->chunk_id = page_pool_get_chunk_id(batch_data);

	/*
	 * Publish the entry to both indices under the hash lock so a concurrent
	 * lookup_and_remove can never observe it in the hash before it exists
	 * in the chunk list. Lock order: hash_lock -> chunk_index.lock (matches
	 * lookup_and_remove and remove_range).
	 */
	if (entry->chunk_id >= 0 && entry->chunk_id < COW_MAX_POOL_CHUNKS) {
		pthread_spin_lock(&chunk_index[entry->chunk_id].lock);
		list_add_tail(&entry->chunk_list, &chunk_index[entry->chunk_id].batches);
		atomic_fetch_add(&chunk_index[entry->chunk_id].batch_count, 1);
		pthread_spin_unlock(&chunk_index[entry->chunk_id].lock);
	}

	hlist_add_head(&entry->hash, &cow_buffer.hash_table[hash]);

	for (i = 0; i < nr_pages; i++)
		page_state_set_with_crc(base_vaddr + (page_offset + i) * PAGE_SIZE,
					PAGE_STATE_IN_BUFFER,
					(char *)batch_data + (page_offset + i) * PAGE_SIZE);
	pthread_spin_unlock(&hash_locks[lock_idx]);
	} /* end new entry block */

	if (entry->chunk_id >= 0 && entry->chunk_id < COW_MAX_POOL_CHUNKS) {
		int cur_max = atomic_load(&nr_active_chunks);

		while (entry->chunk_id >= cur_max) {
			if (atomic_compare_exchange_weak(&nr_active_chunks, &cur_max, entry->chunk_id + 1))
				break;
		}
	}

	__sync_fetch_and_add(&cow_buffer.nr_batches, 1);
	__sync_fetch_and_add(&cow_buffer.nr_pages, nr_pages);
	return 0;
}

/*
 * Get the data pointer for an existing batch entry.
 * Returns entry->data if an entry exists for this 256KB-aligned base,
 * NULL otherwise. Caller can decompress directly into the returned pointer.
 *
 * LOCKING: On success the hash lock is held on return and must be released
 * by a matching call to cow_page_buffer_mark_pages(). This keeps the entry
 * and its data buffer stable across the caller's decompress/memcpy so
 * concurrent producers cannot clobber bytes mid-write.
 */
void *cow_page_buffer_get_data_ptr(unsigned long base_vaddr,
				   int page_offset, int nr_pages)
{
	struct batch_buffer_entry *entry;
	unsigned int hash;
	int lock_idx;

	if (!cow_buffer.initialized)
		return NULL;

	BUG_ON(base_vaddr != batch_align(base_vaddr));

	hash = batch_buffer_hash(base_vaddr);
	lock_idx = lock_index(hash);

	pthread_spin_lock(&hash_locks[lock_idx]);
	hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
		if (entry->base_vaddr == base_vaddr)
			return entry->data;  /* lock held — released by mark_pages */
	}
	pthread_spin_unlock(&hash_locks[lock_idx]);

	return NULL;
}

/*
 * Mark pages as valid in an existing batch after direct decompress.
 * Called after decompressing directly into entry->data.
 *
 * LOCKING: The hash lock is expected to be held by a prior successful
 * cow_page_buffer_get_data_ptr() on the same base_vaddr. This function
 * releases that lock before returning.
 */
void cow_page_buffer_mark_pages(unsigned long base_vaddr,
				int page_offset, int nr_pages)
{
	struct batch_buffer_entry *entry;
	unsigned int hash;
	int lock_idx;
	int i;

	hash = batch_buffer_hash(base_vaddr);
	lock_idx = lock_index(hash);

	hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
		if (entry->base_vaddr == base_vaddr) {
			for (i = 0; i < nr_pages; i++) {
				int idx = page_offset + i;

				if (!cow_batch_bitmap_test(&entry->page_bitmap, idx)) {
					cow_batch_bitmap_set(&entry->page_bitmap, idx);
					cow_batch_bitmap_set(&entry->initial_bitmap, idx);
					entry->nr_pages++;
					__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
				}
				page_state_set_with_crc(base_vaddr + idx * PAGE_SIZE,
							PAGE_STATE_IN_BUFFER,
							(char *)entry->data + idx * PAGE_SIZE);
			}
			pthread_spin_unlock(&hash_locks[lock_idx]);
			return;
		}
	}
	pthread_spin_unlock(&hash_locks[lock_idx]);

	pr_err("BUG: mark_pages called for non-existing entry base=0x%lx\n", base_vaddr);
	BUG();
}

/*
 * Legacy per-page add wrapper.
 * Groups the page into its 256KB-aligned batch.
 * Used by Phase 4 dirty page path which overwrites individual pages.
 */
int cow_page_buffer_add(unsigned long vaddr, void *data, int thread_id, bool nocopy)
{
	unsigned long base = batch_align(vaddr);
	int page_idx = batch_page_index(vaddr);
	struct batch_buffer_entry *entry;
	unsigned int hash;
	int lock_idx;
	void *batch_data;

	BUG_ON(!cow_buffer.initialized);

	hash = batch_buffer_hash(base);
	lock_idx = lock_index(hash);

	pthread_spin_lock(&hash_locks[lock_idx]);

	/* Look for existing batch */
	hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
		if (entry->base_vaddr == base) {
			/* Copy page into existing batch */
			memcpy((char *)entry->data + page_idx * PAGE_SIZE,
			       data, PAGE_SIZE);

			if (!cow_batch_bitmap_test(&entry->page_bitmap, page_idx)) {
				cow_batch_bitmap_set(&entry->page_bitmap, page_idx);
				cow_batch_bitmap_set(&entry->initial_bitmap, page_idx);
				entry->nr_pages++;
				__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
			}
			page_state_set_with_crc(vaddr, PAGE_STATE_IN_BUFFER,
						(char *)entry->data + page_idx * PAGE_SIZE);
			pthread_spin_unlock(&hash_locks[lock_idx]);

			if (nocopy)
				page_pool_put(data);
			return 0;
		}
	}

	/* New batch - allocate full COW_BATCH_PAGES buffer */
	BUG_ON(thread_id < 0);
	batch_data = page_pool_get_pages(thread_id, COW_BATCH_PAGES);
	BUG_ON(!batch_data);
	memcpy((char *)batch_data + page_idx * PAGE_SIZE, data, PAGE_SIZE);

	if (nocopy)
		page_pool_put(data);

	entry = xmalloc(sizeof(*entry));
	BUG_ON(!entry);

	entry->magic = BATCH_ENTRY_MAGIC;
	entry->base_vaddr = base;
	entry->data = batch_data;
	cow_batch_bitmap_zero(&entry->page_bitmap);
	cow_batch_bitmap_set(&entry->page_bitmap, page_idx);
	cow_batch_bitmap_zero(&entry->initial_bitmap);
	cow_batch_bitmap_set(&entry->initial_bitmap, page_idx);
	entry->nr_pages = 1;
	INIT_HLIST_NODE(&entry->hash);
	INIT_LIST_HEAD(&entry->chunk_list);
	entry->chunk_id = page_pool_get_chunk_id(batch_data);

	/*
	 * Publish to both indices under the hash lock (see add_batch).
	 * Lock order: hash_lock -> chunk_index.lock.
	 */
	if (entry->chunk_id >= 0 && entry->chunk_id < COW_MAX_POOL_CHUNKS) {
		pthread_spin_lock(&chunk_index[entry->chunk_id].lock);
		list_add_tail(&entry->chunk_list, &chunk_index[entry->chunk_id].batches);
		atomic_fetch_add(&chunk_index[entry->chunk_id].batch_count, 1);
		pthread_spin_unlock(&chunk_index[entry->chunk_id].lock);
	}

	hlist_add_head(&entry->hash, &cow_buffer.hash_table[hash]);
	page_state_set_with_crc(vaddr, PAGE_STATE_IN_BUFFER,
				(char *)batch_data + page_idx * PAGE_SIZE);
	pthread_spin_unlock(&hash_locks[lock_idx]);

	if (entry->chunk_id >= 0 && entry->chunk_id < COW_MAX_POOL_CHUNKS) {
		int cur_max = atomic_load(&nr_active_chunks);

		while (entry->chunk_id >= cur_max) {
			if (atomic_compare_exchange_weak(&nr_active_chunks, &cur_max, entry->chunk_id + 1))
				break;
		}
	}

	__sync_fetch_and_add(&cow_buffer.nr_batches, 1);
	__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
	return 0;
}

/*
 * Look up a single page in the batch buffer.
 * Returns a pointer to a PAGE_SIZE buffer that the caller must free
 * via page_pool_put(), or NULL if the page is not in the buffer.
 *
 * The page is cleared from the batch bitmap. If the batch becomes empty,
 * the entry is removed and its data buffer freed.
 */
void *cow_page_buffer_lookup_and_remove(unsigned long vaddr)
{
	struct batch_buffer_entry *entry;
	unsigned long base;
	unsigned int hash;
	int lock_idx, page_idx;
	void *page_ptr;

	if (!cow_buffer.initialized)
		return NULL;

	base = batch_align(vaddr);
	page_idx = batch_page_index(vaddr);
	hash = batch_buffer_hash(base);
	lock_idx = lock_index(hash);

	pthread_spin_lock(&hash_locks[lock_idx]);
	hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
		if (entry->magic != BATCH_ENTRY_MAGIC) {
			pr_err("RACE_DEBUG: lookup_and_remove found DEAD entry in hash! "
			       "vaddr=0x%lx base=0x%lx magic=0x%x\n",
			       vaddr, base, entry->magic);
			BUG();
		}
		if (entry->base_vaddr != base)
			continue;
		if (!cow_batch_bitmap_test(&entry->page_bitmap, page_idx)) {
			pthread_spin_unlock(&hash_locks[lock_idx]);
			return NULL;
		}

		page_ptr = (char *)entry->data + page_idx * PAGE_SIZE;

		/* Clear bit and decrement count */
		cow_batch_bitmap_clear(&entry->page_bitmap, page_idx);
		entry->nr_pages--;
		__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);

		if (entry->nr_pages == 0) {
			/* Batch empty — remove entirely */
			int chunk_id = entry->chunk_id;
			cow_batch_bitmap_t free_bm;
			int j;

			hlist_del(&entry->hash);
			pthread_spin_unlock(&hash_locks[lock_idx]);

			if (chunk_id >= 0 && chunk_id < COW_MAX_POOL_CHUNKS) {
				pthread_spin_lock(&chunk_index[chunk_id].lock);
				list_del(&entry->chunk_list);
				atomic_fetch_sub(&chunk_index[chunk_id].batch_count, 1);
				pthread_spin_unlock(&chunk_index[chunk_id].lock);
			}
			/*
			 * Free pages we still own. Skip page_idx (caller frees)
			 * and pages already freed by earlier page faults
			 * (initial_bitmap bit set, page_bitmap bit clear).
			 * page_bitmap is 0 here (entry empty), so freed_by_pf =
			 * initial_bitmap minus the current page_idx bit.
			 */
			/* Also free unused slots (never had data) */
			/* free_bm has bits set for unused slots */
			/* Don't free page_idx — caller will */
			cow_batch_bitmap_not(&free_bm, &entry->initial_bitmap);
			cow_batch_bitmap_mask(&free_bm, COW_BATCH_PAGES);
			cow_batch_bitmap_clear(&free_bm, page_idx);
			COW_BATCH_BITMAP_FOR_EACH_SET(&free_bm, j) {
				page_pool_put((char *)entry->data + j * PAGE_SIZE);
			}
			entry->magic = BATCH_ENTRY_DEAD;
			xfree(entry);
			__sync_fetch_and_sub(&cow_buffer.nr_batches, 1);
			return page_ptr;
		}

		pthread_spin_unlock(&hash_locks[lock_idx]);
		return page_ptr;
	}
	pthread_spin_unlock(&hash_locks[lock_idx]);

	return NULL;
}

unsigned long cow_page_buffer_count(void)
{
	return cow_buffer.nr_pages;
}

void cow_page_buffer_destroy(void)
{
	struct batch_buffer_entry *entry;
	struct hlist_node *tmp;
	int i, j;

	if (!cow_buffer.initialized)
		return;

	/* Stop drain thread first */
	cow_stop_drain_thread();

	/* Walk all buckets and free batch entries */
	for (i = 0; i < COW_BATCH_BUFFER_HASH_SIZE; i++) {
		int lock_idx = lock_index(i);

		pthread_spin_lock(&hash_locks[lock_idx]);
		hlist_for_each_entry_safe(entry, tmp,
					  &cow_buffer.hash_table[i], hash) {
			/* Free pages still owned (bitmap) + unused slots (~initial) */
			cow_batch_bitmap_t free_bm;
			cow_batch_bitmap_not(&free_bm, &entry->initial_bitmap);
			cow_batch_bitmap_mask(&free_bm, COW_BATCH_PAGES);
			cow_batch_bitmap_or(&free_bm, &free_bm, &entry->page_bitmap);
			COW_BATCH_BITMAP_FOR_EACH_SET(&free_bm, j) {
				page_pool_put((char *)entry->data + j * PAGE_SIZE);
			}
			hlist_del(&entry->hash);
			xfree(entry);
		}
		pthread_spin_unlock(&hash_locks[lock_idx]);
	}

	xfree(cow_buffer.hash_table);
	cow_buffer.hash_table = NULL;
	cow_buffer.initialized = false;

	for (i = 0; i < COW_BATCH_NUM_HASH_LOCKS; i++)
		pthread_spin_destroy(&hash_locks[i]);

	/* Clean up chunk index */
	for (i = 0; i < COW_MAX_POOL_CHUNKS; i++) {
		pthread_spin_destroy(&chunk_index[i].lock);
		INIT_LIST_HEAD(&chunk_index[i].batches);
	}
	atomic_store(&chunk_index_initialized, false);
	atomic_store(&nr_active_chunks, 0);

	pr_info("COW batch buffer destroyed: batches=%lu pages=%lu applied=%lu discarded=%lu\n",
		cow_buffer.nr_batches, cow_buffer.nr_pages,
		cow_buffer.nr_applied, cow_buffer.nr_discarded);

	/* Destroy all page pools last */
	page_pool_destroy_all();

}

/*
 * Remove all pages in a range from the buffer.
 * Called when VMA is unmapped - no point keeping these pages.
 * Operates at batch granularity: clears bitmap bits for affected pages.
 */
void cow_page_buffer_remove_range(unsigned long start, unsigned long len)
{
	struct batch_buffer_entry *entry;
	unsigned long base, end;
	unsigned long removed = 0;

	if (!cow_buffer.initialized)
		return;

	end = start + len;

	/* Iterate over 256KB-aligned batches that overlap the range */
	for (base = batch_align(start); base < end; base += COW_BATCH_SIZE) {
		unsigned int hash = batch_buffer_hash(base);
		int lock_idx = lock_index(hash);
		int first_page, last_page;
		cow_batch_bitmap_t clear_mask;
		cow_batch_bitmap_t masked;
		int cleared;

		/* Which pages within this batch overlap [start, end)? */
		first_page = (base < start) ? batch_page_index(start) : 0;
		last_page = (base + COW_BATCH_SIZE > end)
			    ? batch_page_index(end - 1) : (COW_BATCH_PAGES - 1);

		/* Build mask of pages to clear */
		cow_batch_bitmap_zero(&clear_mask);
		cow_batch_bitmap_set_range(&clear_mask, first_page, last_page - first_page + 1);

		pthread_spin_lock(&hash_locks[lock_idx]);
		hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
			if (entry->base_vaddr != base)
				continue;

			cow_batch_bitmap_and(&masked, &entry->page_bitmap, &clear_mask);
			cleared = cow_batch_bitmap_popcount(&masked);
			if (cleared == 0) {
				pthread_spin_unlock(&hash_locks[lock_idx]);
				goto next_batch;
			}

			cow_batch_bitmap_clear_range(&entry->page_bitmap, first_page,
						     last_page - first_page + 1);
			entry->nr_pages -= cleared;
			removed += cleared;

			if (entry->nr_pages == 0) {
				int chunk_id = entry->chunk_id;
				cow_batch_bitmap_t free_bm;
				int j;

				hlist_del(&entry->hash);
				pthread_spin_unlock(&hash_locks[lock_idx]);

				if (chunk_id >= 0 && chunk_id < COW_MAX_POOL_CHUNKS) {
					pthread_spin_lock(&chunk_index[chunk_id].lock);
					list_del(&entry->chunk_list);
					atomic_fetch_sub(&chunk_index[chunk_id].batch_count, 1);
					pthread_spin_unlock(&chunk_index[chunk_id].lock);
				}
				/* Free owned + unused, skip page-fault-served */
				cow_batch_bitmap_not(&free_bm, &entry->initial_bitmap);
				cow_batch_bitmap_mask(&free_bm, COW_BATCH_PAGES);
				cow_batch_bitmap_or(&free_bm, &free_bm, &entry->page_bitmap);
				COW_BATCH_BITMAP_FOR_EACH_SET(&free_bm, j) {
					page_pool_put((char *)entry->data + j * PAGE_SIZE);
				}
				xfree(entry);
				__sync_fetch_and_sub(&cow_buffer.nr_batches, 1);
				goto next_batch;
			}

			pthread_spin_unlock(&hash_locks[lock_idx]);
			goto next_batch;
		}
		pthread_spin_unlock(&hash_locks[lock_idx]);
next_batch:;
	}

	if (removed > 0) {
		__sync_fetch_and_sub(&cow_buffer.nr_pages, removed);
		__sync_fetch_and_add(&cow_buffer.nr_discarded, removed);
		pr_info("Removed %lu pages from buffer for UNMAP range 0x%lx-0x%lx\n",
			removed, start, end);
	}
}


/*
 * Drain a batch via UFFDIO_COPY(s) and free its data.
 * Performs a single UFFDIO_COPY for full batches (bitmap == all-ones),
 * or falls back to per-page copies for partial batches.
 *
 * Returns number of pages drained.
 */
static unsigned long drain_apply_batch(struct batch_buffer_entry *entry,
				       struct list_head *lpis)
{
	unsigned long base = entry->base_vaddr;
	void *data = entry->data;
	unsigned long applied = 0;
	int uffd, i;

	if (entry->magic != BATCH_ENTRY_MAGIC) {
		pr_err("RACE_DEBUG: drain_apply_batch got DEAD entry! "
		       "base=0x%lx magic=0x%x data=%p\n",
		       base, entry->magic, data);
		BUG();
	}

	/* Fast path: full batch — single UFFDIO_COPY for 256KB */
	if (cow_batch_bitmap_is_full_upto(&entry->page_bitmap, COW_BATCH_PAGES)) {
		for (i = 0; i < COW_BATCH_PAGES; i++)
			page_state_set(base + i * PAGE_SIZE, PAGE_STATE_DRAIN_PENDING);

		uffd = cow_get_uffd_for_vaddr(lpis, base);
		if (uffd >= 0) {
			cow_uffd_copy(uffd, base, data, COW_BATCH_PAGES,
				      NULL, lpis, COW_TRACK_STRICT, "DRAIN_BATCH");
		}
		applied = COW_BATCH_PAGES;
	} else {
		/* Partial batch — per-page copies for set bits */
		COW_BATCH_BITMAP_FOR_EACH_SET(&entry->page_bitmap, i) {
			page_state_set(base + i * PAGE_SIZE, PAGE_STATE_DRAIN_PENDING);
			uffd = cow_get_uffd_for_vaddr(lpis, base + i * PAGE_SIZE);
			if (uffd >= 0) {
				cow_uffd_copy(uffd, base + i * PAGE_SIZE,
					      (char *)data + i * PAGE_SIZE, 1,
					      NULL, lpis, COW_TRACK_STRICT, "DRAIN");
			}
			applied++;
		}
	}

	/*
	 * Free pool pages. page_pool_get_pages(COW_BATCH_PAGES) set refcount.
	 * Page faults may have already freed some (cleared bitmap bits).
	 *
	 * free_bitmap = pages drain owns (bitmap) | unused slots (~initial_bitmap)
	 * Skip: pages served by page fault (initial_bitmap & ~bitmap) — already freed.
	 */
	{
		cow_batch_bitmap_t free_bitmap;
		cow_batch_bitmap_t pf_served_bm;
		int free_count, pf_served;

		cow_batch_bitmap_not(&free_bitmap, &entry->initial_bitmap);
		cow_batch_bitmap_mask(&free_bitmap, COW_BATCH_PAGES);
		cow_batch_bitmap_or(&free_bitmap, &free_bitmap, &entry->page_bitmap);
		free_count = cow_batch_bitmap_popcount(&free_bitmap);

		cow_batch_bitmap_not(&pf_served_bm, &entry->page_bitmap);
		cow_batch_bitmap_mask(&pf_served_bm, COW_BATCH_PAGES);
		cow_batch_bitmap_and(&pf_served_bm, &pf_served_bm, &entry->initial_bitmap);
		pf_served = cow_batch_bitmap_popcount(&pf_served_bm);

		if (pf_served > 0) {
			pr_debug("DRAIN_FREE_DEBUG: base=0x%lx "
			       "pf_served=%d freeing=%d of %d\n",
			       base, pf_served, free_count, COW_BATCH_PAGES);
		}

		COW_BATCH_BITMAP_FOR_EACH_SET(&free_bitmap, i) {
			page_pool_put((char *)data + i * PAGE_SIZE);
		}
	}

	return applied;
}

/*
 * Background drain worker thread - proactively UFFDIO_COPY pages
 * from buffer to reduce future page faults and free memory.
 *
 * Each worker handles a subset of CHUNKS for chunk-ordered draining.
 * By draining all batches from one chunk before moving to the next,
 * chunks can be freed progressively instead of all at the end.
 */
static void *background_drain_worker(void *arg)
{
	struct drain_thread_args *args = (struct drain_thread_args *)arg;
	struct batch_buffer_entry *entry, *tmp_entry;
	unsigned long drained = 0;
	unsigned long last_progress_drained = 0;
	time_t last_progress_time = 0;
	int thread_id = args->thread_id;
	int chunk_id;
	int chunks_empty = 0;
	int chunks_with_batches = 0;
	char thread_name[16];

	snprintf(thread_name, sizeof(thread_name), "cow-drain-%d", thread_id);
	pthread_setname_np(pthread_self(), thread_name);

	pr_info("Drain thread %d started, buffered=%lu pages\n", thread_id, cow_buffer.nr_pages);
	last_progress_time = time(NULL);

	while (!atomic_load(&drain_thread_stop) && cow_buffer.nr_pages > 0) {
		/* Work-stealing: atomically grab next chunk */
		chunk_id = atomic_fetch_add(&next_drain_chunk, 1);
		if (chunk_id >= max_drain_chunks)
			break;

		{
			unsigned long chunk_drained = 0;

			pthread_spin_lock(&chunk_index[chunk_id].lock);
			list_for_each_entry_safe(entry, tmp_entry,
						 &chunk_index[chunk_id].batches, chunk_list) {
				int nr = entry->nr_pages;

				/* Remove from chunk list while holding lock */
				list_del(&entry->chunk_list);
				atomic_fetch_sub(&chunk_index[chunk_id].batch_count, 1);
				pthread_spin_unlock(&chunk_index[chunk_id].lock);

				/* Remove from hash table before draining */
				{
					unsigned int hash = batch_buffer_hash(entry->base_vaddr);
					int lock_idx = lock_index(hash);

					pthread_spin_lock(&hash_locks[lock_idx]);
					hlist_del(&entry->hash);
					pthread_spin_unlock(&hash_locks[lock_idx]);
				}

				__sync_fetch_and_sub(&cow_buffer.nr_pages, nr);
				__sync_fetch_and_sub(&cow_buffer.nr_batches, 1);

				drained += drain_apply_batch(entry, drain_lpis);
				chunk_drained += nr;
				entry->magic = BATCH_ENTRY_DEAD;
				xfree(entry);

				/* Log progress every 100k pages or 10 seconds */
				if (drained - last_progress_drained >= COW_LOG_SAMPLE_1M ||
				    time(NULL) - last_progress_time >= COW_DRAIN_PROGRESS_SEC) {
					pr_err("Drain thread %d: drained=%lu chunk=%d remaining=%lu\n",
					       thread_id, drained, chunk_id, cow_buffer.nr_pages);
					last_progress_drained = drained;
					last_progress_time = time(NULL);
				}

				pthread_spin_lock(&chunk_index[chunk_id].lock);
			}
			pthread_spin_unlock(&chunk_index[chunk_id].lock);

			if (chunk_drained == 0)
				chunks_empty++;
			else
				chunks_with_batches++;
		}
	}

	/* Update global statistics */
	atomic_fetch_add(&total_drained, drained);

	pr_info("Drain thread %d finished: drained=%lu chunks_empty=%d chunks_with_batches=%d\n",
	       thread_id, drained, chunks_empty, chunks_with_batches);

	/* Decrement active thread count */
	if (atomic_fetch_sub(&drain_threads_active, 1) == 1) {
		struct timespec drain_end_time;
		unsigned long elapsed_ms;

		/*
		 * Last thread to exit. Add full memory barrier to ensure all
		 * UFFDIO_COPY writes are visible before signaling drain complete.
		 * This is critical on ARM where memory ordering is weaker.
		 */
		atomic_thread_fence(memory_order_seq_cst);

		/* Calculate and print drain duration */
		clock_gettime(CLOCK_MONOTONIC, &drain_end_time);
		elapsed_ms = (drain_end_time.tv_sec - drain_start_time.tv_sec) * 1000 +
			     (drain_end_time.tv_nsec - drain_start_time.tv_nsec) / 1000000;
		pr_err("TIMING: drain took %lu ms\n", elapsed_ms);

		pr_info("Drain complete: total=%lu applied=%lu discarded=%lu eagain=%lu remaining=%lu\n",
		       atomic_load(&total_drained), cow_buffer.nr_applied,
		       cow_buffer.nr_discarded, cow_buffer.nr_eagain, cow_buffer.nr_pages);

		/* Print pool stats to check for leaks */
		page_pool_dump_stats();

		/* All pages should be drained - orphaned pages are a bug */
		BUG_ON(cow_buffer.nr_pages > 0);
	}

	return NULL;
}

int cow_start_drain_thread(struct list_head *lpis)
{
	int i;
	int chunks_per_thread;
	int total_chunks;
	int created = 0;

	if (atomic_load(&drain_threads_active) > 0)
		return 0;

	if (cow_buffer.nr_pages == 0)
		return 0;

	drain_lpis = lpis;  /* Store for EAGAIN handling */
	atomic_store(&drain_thread_stop, false);
	atomic_store(&total_drained, 0);

	/* Get number of chunks to drain */
	total_chunks = atomic_load(&nr_active_chunks);
	if (total_chunks == 0)
		total_chunks = page_pool_get_nr_chunks();
	if (total_chunks == 0)
		total_chunks = COW_MAX_POOL_CHUNKS;  /* Fallback: scan all slots */

	/* Divide chunks evenly among threads */
	chunks_per_thread = (total_chunks + COW_NUM_DRAIN_THREADS - 1) / COW_NUM_DRAIN_THREADS;
	if (chunks_per_thread < 1)
		chunks_per_thread = 1;

	/* Initialize work-stealing globals */
	atomic_store(&next_drain_chunk, 0);
	max_drain_chunks = total_chunks;

	for (i = 0; i < COW_NUM_DRAIN_THREADS; i++) {
		drain_args[i].thread_id = i;

		BUG_ON(pthread_create(&drain_threads[i], NULL,
				      background_drain_worker, &drain_args[i]));
		atomic_fetch_add(&drain_threads_active, 1);
		created++;
	}

	/* Mark drain started and report any puts that happened before */
	page_pool_mark_drain_started();

	/* Record start time for TIMING debug */
	clock_gettime(CLOCK_MONOTONIC, &drain_start_time);

	pr_info("Started %d drain threads, buffered=%lu total_chunks=%d\n",
	       created, cow_buffer.nr_pages, total_chunks);

	return 0;
}

void cow_stop_drain_thread(void)
{
	int i;

	if (atomic_load(&drain_threads_active) == 0)
		return;

	atomic_store(&drain_thread_stop, true);

	/* Join all threads */
	for (i = 0; i < COW_NUM_DRAIN_THREADS; i++) {
		if (drain_threads[i]) {
			pthread_join(drain_threads[i], NULL);
			drain_threads[i] = 0;
		}
	}

	/* Reset state for potential restart */
	atomic_store(&drain_threads_active, 0);
}

bool cow_drain_thread_running(void)
{
	return atomic_load(&drain_threads_active) > 0;
}

/*
 * Handle COW mode exit conditions.
 *
 * Exit sequence:
 * 1. Wait for all_pages_sent signal (guarantees all pages received from socket)
 * 2. Wait for drain thread to finish (buffer empty)
 * 3. Send ACK to primary
 * 4. Cleanup and exit
 *
 * Returns:
 *   1  - should break the main loop (all done)
 *   0  - should continue the main loop
 */
int cow_handle_exit(struct list_head *lpis)
{
	struct lazy_pages_info *lpi, *n;

	/* Condition 1: Wait for all_pages_sent signal from primary */
	if (!cow_is_all_pages_sent_received())
		return 0;

	/* Condition 2: Wait for drain thread to finish */
	if (cow_drain_thread_running())
		return 0;

	/* Condition 3: Wait for buffer to be empty */
	if (cow_page_buffer_count() > 0)
		return 0;

	/* Condition 4: Wait for EAGAIN requests to be processed */
	if (!cow_is_eagain_queue_empty())
		return 0;

	/* Cleanup all lpis */
	list_for_each_entry_safe(lpi, n, lpis, l) {
		lazy_pages_summary(lpi);
		list_del(&lpi->l);
		lpi_put(lpi);
	}

	return 1;  /* Exit main loop */
}

/*
 * ============================================================================
 * UFFD Statistics and Histogram (COW mode)
 * ============================================================================
 */

/* Histogram statistics structure */
static struct {
	/* Histogram buckets by page count: 1, 16, 32, 64, 128, 256, 512, 1024, >1024 */
	unsigned long pf_hist[9]; /* Page fault histogram */
	unsigned long bg_hist[9]; /* Background transfer histogram */

	unsigned long total_pf_reqs;
	unsigned long total_bg_reqs;
	unsigned long total_pages;

	/* Timing statistics (nanoseconds) */
	unsigned long uffd_copy_total_ns;
	unsigned long uffd_copy_count;
	unsigned long drop_iovs_total_ns;
	unsigned long drop_iovs_count;

	/* EAGAIN retry statistics */
	unsigned long eagain_processed;
	unsigned long eagain_succeeded;
	unsigned long eagain_blocked;
	unsigned long eagain_errors;
	unsigned long eagain_skipped;
	unsigned long eagain_total_ns;
	unsigned long eagain_calls;

	time_t last_print_time;
} uffd_stats = {0};

int cow_get_histogram_bucket(unsigned long nr_pages)
{
	if (nr_pages == 1)
		return 0; /* 4KB */
	if (nr_pages <= 16)
		return 1; /* 64KB */
	if (nr_pages <= 32)
		return 2; /* 128KB */
	if (nr_pages <= 64)
		return 3; /* 256KB */
	if (nr_pages <= 128)
		return 4; /* 512KB */
	if (nr_pages <= 256)
		return 5; /* 1MB */
	if (nr_pages <= 512)
		return 6; /* 2MB */
	if (nr_pages <= 1024)
		return 7; /* 4MB */
	return 8;	  /* >4MB */
}

static const char *get_bucket_label(int bucket)
{
	switch (bucket) {
	case 0:
		return "4K";
	case 1:
		return "64K";
	case 2:
		return "128K";
	case 3:
		return "256K";
	case 4:
		return "512K";
	case 5:
		return "1M";
	case 6:
		return "2M";
	case 7:
		return "4M";
	case 8:
		return ">4M";
	default:
		return "?";
	}
}



void cow_uffd_stats_add_copy(unsigned long ns)
{
	uffd_stats.uffd_copy_total_ns += ns;
	uffd_stats.uffd_copy_count++;
}

void cow_uffd_stats_add_drop(unsigned long ns)
{
	uffd_stats.drop_iovs_total_ns += ns;
	uffd_stats.drop_iovs_count++;
}

void check_and_print_uffd_stats(void)
{
	time_t now = time(NULL);
	int i;

	if (now - uffd_stats.last_print_time >= 30) {
		{
			struct timespec ts;
			struct tm *tm;
			clock_gettime(CLOCK_REALTIME, &ts);
			tm = localtime(&ts.tv_sec);
			pr_err("[UFFD_STATS] [%02d:%02d:%02d.%03ld] reqs=%lu(pf:%lu,bg:%lu) pages=%lu\n",
				tm->tm_hour, tm->tm_min, tm->tm_sec, ts.tv_nsec / 1000000,
				uffd_stats.total_pf_reqs + uffd_stats.total_bg_reqs,
				uffd_stats.total_pf_reqs,
				uffd_stats.total_bg_reqs,
				uffd_stats.total_pages);
		}

		/* Print page fault histogram */
		pr_debug("  PF: ");
		for (i = 0; i < 9; i++) {
			if (uffd_stats.pf_hist[i] > 0)
				pr_debug(" %s=%lu", get_bucket_label(i), uffd_stats.pf_hist[i]);
		}
		pr_debug("\n");

		/* Print background transfer histogram */
		pr_debug("  BG: ");
		for (i = 0; i < 9; i++) {
			if (uffd_stats.bg_hist[i] > 0)
				pr_debug(" %s=%lu", get_bucket_label(i), uffd_stats.bg_hist[i]);
		}
		pr_debug("\n");


		/* Print EAGAIN stats */
		if (uffd_stats.eagain_processed > 0 || uffd_stats.eagain_skipped > 0 || uffd_stats.eagain_calls > 0) {
			pr_info("  EAGAIN: processed=%lu succeeded=%lu blocked=%lu errors=%lu skipped=%lu | time=%lu ns (%lu calls)\n",
				uffd_stats.eagain_processed,
				uffd_stats.eagain_succeeded,
				uffd_stats.eagain_blocked,
				uffd_stats.eagain_errors,
				uffd_stats.eagain_skipped,
				uffd_stats.eagain_calls > 0 ? uffd_stats.eagain_total_ns / uffd_stats.eagain_calls : 0,
				uffd_stats.eagain_calls);
		}

		/* Print page fault tracker stats and clean up completed entries */
		pf_tracker_print_stats();

		/* Reset all counters */
		memset(&uffd_stats, 0, sizeof(uffd_stats));
		uffd_stats.last_print_time = now;
	}
}

/*
 * ============================================================================
 * EAGAIN Request Handling (COW mode)
 * ============================================================================
 */

/* Pending EAGAIN requests list - protected by eagain_mutex */
static LIST_HEAD(eagain_requests);
static pthread_mutex_t eagain_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Queue an EAGAIN request for later retry in COW dump mode.
 * For copy operations, buf should point to the data to copy.
 * For zero operations, buf should be NULL.
 */
int cow_queue_eagain_request(struct lazy_pages_info *lpi, __u64 address,
			     unsigned long nr_pages, void *buf, const char *op_name)
{
	struct uffd_eagain_request *req;
	void *buf_copy = NULL;
	unsigned long len = nr_pages * page_size();

	/* Copy buffer if provided (copy operation) */
	if (buf) {
		buf_copy = xmalloc(len);
		BUG_ON(!buf_copy);
		memcpy(buf_copy, buf, len);
	}

	/* Create request entry */
	req = xmalloc(sizeof(*req));
	BUG_ON(!req);

	req->lpi = lpi;
	req->address = address;
	req->nr_pages = nr_pages;
	req->buf = buf_copy;  /* NULL for zero operations */
	INIT_LIST_HEAD(&req->l);

	pthread_mutex_lock(&eagain_mutex);
	list_add_tail(&req->l, &eagain_requests);
	pthread_mutex_unlock(&eagain_mutex);

	/* Only set page state after successfully queueing */
	page_state_set(address, PAGE_STATE_EAGAIN_QUEUED);

	pr_err("EAGAIN_DEBUG: queued 0x%llx nr_pages=%lu op=%s buf=%p buf_copy=%p\n",
	       address, nr_pages, op_name, buf, buf_copy);
	return 0;
}
/*
 * Find the lpi that owns a given vaddr.
 * Returns NULL if no matching lpi found (page unmapped or process exited).
 */
static struct lazy_pages_info *cow_find_lpi_for_vaddr(struct list_head *lpis,
						      unsigned long vaddr)
{
	struct lazy_pages_info *lpi;

	list_for_each_entry(lpi, lpis, l) {
		if (lpi->exited || lpi->lpfd.fd < 0)
			continue;
		if (cow_find_iov(lpi, vaddr))
			return lpi;
	}

	return NULL;
}

/*
 * Queue an EAGAIN request from drain thread context.
 * Finds the appropriate lpi for the vaddr and queues for retry.
 * Returns 0 on success (ownership of data transferred), -1 on error.
 */
int cow_queue_drain_eagain_request(struct list_head *lpis, unsigned long vaddr, void *data)
{
	struct lazy_pages_info *lpi = cow_find_lpi_for_vaddr(lpis, vaddr);

	if (lpi)
		return cow_queue_eagain_request(lpi, vaddr, 1, data, "drain");

	/* No matching lpi - this is a bug */
	pr_err("BUG: No lpi found for drain EAGAIN at 0x%lx\n", vaddr);
	page_state_print_history(vaddr);
	BUG();
	return -1;
}

/* Check if EAGAIN requests queue is empty */
bool cow_is_eagain_queue_empty(void)
{
	bool empty;

	pthread_mutex_lock(&eagain_mutex);
	empty = list_empty(&eagain_requests);
	pthread_mutex_unlock(&eagain_mutex);

	return empty;
}

/*
 * Retry a copy operation that previously failed with EAGAIN.
 * Returns: 0 on success, -EAGAIN if still blocked, -1 on error
 */
static int retry_uffd_copy(struct uffd_eagain_request *req)
{
	int ret;

	ret = cow_uffd_copy(req->lpi->lpfd.fd, req->address,
			    req->buf, req->nr_pages,
			    req->lpi, NULL,
			    COW_TRACK_RETRY | COW_TRACK_STRICT,
			    "EAGAIN_RETRY");
	if (ret == 1) {
		lp_debug(req->lpi, "EAGAIN copy retry succeeded for 0x%llx\n", req->address);
		return 0;
	}
	if (ret == -EAGAIN)
		return -EAGAIN;

	/* ENOENT or ERROR - unified handler already set page state */
	lp_err(req->lpi, "EAGAIN copy retry failed for 0x%llx\n", req->address);
	return -1;
}

/*
 * Retry a zero operation that previously failed with EAGAIN.
 * Returns: 0 on success, -EAGAIN if still blocked, -1 on error
 */
static int retry_uffd_zero(struct uffd_eagain_request *req)
{
	struct uffdio_zeropage uffdio_zeropage;

	uffdio_zeropage.range.start = req->address;
	uffdio_zeropage.range.len = req->nr_pages * page_size();
	uffdio_zeropage.mode = 0;
	uffdio_zeropage.zeropage = 0;

	if (ioctl(req->lpi->lpfd.fd, UFFDIO_ZEROPAGE, &uffdio_zeropage) == -1) {
		if (errno == EAGAIN)
			return -EAGAIN;

		if (errno == EEXIST) {
			pr_err("BUG: EAGAIN zero retry EEXIST at 0x%llx - duplicate zero!\n",
			       req->address);
			page_state_print_history(req->address);
			BUG();
		}

		lp_err(req->lpi, "EAGAIN zero retry failed for 0x%llx: %d\n",
		       req->address, errno);
		page_state_set(req->address, PAGE_STATE_DISCARDED);
		return -1;
	}

	/* Check for soft error */
	if (uffdio_zeropage.zeropage < 0) {
		errno = -uffdio_zeropage.zeropage;
		if (errno == EAGAIN)
			return -EAGAIN;

		lp_err(req->lpi, "EAGAIN zero retry soft error for 0x%llx: %d\n",
		       req->address, errno);
		page_state_set(req->address, PAGE_STATE_DISCARDED);
		return -1;
	}

	/* Success */
	pf_tracker_set_state(req->address, PF_STATE_COMPLETED);
	page_state_set(req->address, PAGE_STATE_COPIED);
	lp_debug(req->lpi, "EAGAIN zero retry succeeded for 0x%llx\n", req->address);
	return 0;
}

/*
 * Process pending EAGAIN requests.
 * Attempts to retry UFFDIO_COPY or UFFDIO_ZEROPAGE for requests that previously failed with EAGAIN.
 */
int cow_process_eagain_requests(void)
{
	struct uffd_eagain_request *req, *n;
	int ret;
	struct timespec t_start, t_end;

	clock_gettime(CLOCK_MONOTONIC, &t_start);

	pthread_mutex_lock(&eagain_mutex);
	list_for_each_entry_safe(req, n, &eagain_requests, l) {
		/* Skip if process has exited */
		if (req->lpi->exited) {
			uffd_stats.eagain_skipped++;
			pr_err("EAGAIN retry failed lpi unmapped for 0x%llx (op=%s)\n",
				 req->address, req->buf ? "copy" : "zero");
			page_state_set(req->address, PAGE_STATE_DISCARDED);
			list_del(&req->l);
			if (req->buf)
				xfree(req->buf);
			xfree(req);
			continue;
		}

		uffd_stats.eagain_processed++;

		pr_err("EAGAIN_DEBUG: retrying 0x%llx nr_pages=%lu op=%s buf=%p\n",
		       req->address, req->nr_pages, req->buf ? "copy" : "zero", req->buf);

		/* Call appropriate retry function based on operation type */
		if (req->buf)
			ret = retry_uffd_copy(req);
		else
			ret = retry_uffd_zero(req);

		if (ret == -EAGAIN) {
			/* Still blocked - keep in queue for next attempt */
			uffd_stats.eagain_blocked++;
			pr_err("EAGAIN_DEBUG: still blocked 0x%llx\n", req->address);
			continue;
		} else if (ret < 0) {
			/* Error - remove from queue (state already set by retry func) */
			uffd_stats.eagain_errors++;
			pr_err("EAGAIN retry error for 0x%llx, removing from queue\n",
			       req->address);
			BUG();
			list_del(&req->l);
			if (req->buf)
				xfree(req->buf);
			xfree(req);
			continue;
		}

		/* Success! */
		uffd_stats.eagain_succeeded++;
		pr_err("EAGAIN_DEBUG: succeeded 0x%llx nr_pages=%lu\n",
		       req->address, req->nr_pages);

		/* Clean up and remove from queue */
		list_del(&req->l);
		if (req->buf)
			xfree(req->buf);
		xfree(req);
	}	

	pthread_mutex_unlock(&eagain_mutex);

	clock_gettime(CLOCK_MONOTONIC, &t_end);
	uffd_stats.eagain_total_ns += (t_end.tv_sec - t_start.tv_sec) * 1000000000 + (t_end.tv_nsec - t_start.tv_nsec);
	uffd_stats.eagain_calls++;

	return 0;
}

/*
 * ============================================================================
 * COW Restore State Management
 * ============================================================================
 *
 * State variables and accessors for COW phased migration.
 * These track the state of the restore process and communication with primary.
 */

/* State flags for COW restore synchronization */
static bool cow_restore_connected = false;
static bool cow_all_pages_sent_received = false;

/* Check if restore has connected (uffd available) */
bool cow_is_restore_connected(void)
{
	return cow_restore_connected;
}

/* Set restore connected flag */
void cow_set_restore_connected(bool connected)
{
	cow_restore_connected = connected;
}



/* Check if all pages have been sent by primary */
bool cow_is_all_pages_sent_received(void)
{
	return cow_all_pages_sent_received;
}

/* Set all_pages_sent flag (called when PS_IOV_ALL_PAGES_SENT received) */
void cow_set_all_pages_sent_received(void)
{
	pr_info("All pages sent signal received - can zero-fill new VMA pages\n");
	cow_all_pages_sent_received = true;
}



/* Return uffd for a given vaddr (for background drain thread) */
int cow_get_uffd_for_vaddr(struct list_head *lpis, unsigned long vaddr)
{
	struct lazy_pages_info *lpi = cow_find_lpi_for_vaddr(lpis, vaddr);
	return lpi ? lpi->lpfd.fd : -1;
}

/*
 * ============================================================================
 * COW Phase 2/3 Infrastructure
 * ============================================================================
 *
 * Pre-buffer and convergence infrastructure for COW phased migration.
 * Pages arrive before criu restore connects, so we buffer them
 * in the hash table until the uffd is available.
 */

static bool phase3_active_flag = false;

void cow_set_phase3_active(bool active)
{
	phase3_active_flag = active;
}

bool cow_is_phase3_active(void)
{
	return phase3_active_flag;
}

/*
 * Initialize the control message reader on the main page server socket.
 * All page data flows through P3 receiver threads; the main socket only
 * carries control messages (end-of-transfer marker, PS_IOV_ALL_PAGES_SENT).
 */
int cow_setup_prebuffer_reader(void)
{
	return page_server_start_async_read_bulk();
}


/*
 * Handle UNMAP/REMOVE event in COW mode.
 * Marks pages as unmapped in trackers and removes from buffer.
 */
void cow_handle_remove_event(unsigned long start, unsigned long len)
{
	/* Mark all pages in range as unmapped for state tracking */
	page_state_mark_range_unmapped(start, len);

	/* Track unmapped pages for production validation */
	unmapped_tracker_mark_range(start, len);

	/* Remove these pages from buffer - no point draining them */
	cow_page_buffer_remove_range(start, len);
}

/*
 * COW post-connect initialization in handle_lazy_accept.
 * and Phase 3 page requests.
 *
 * Returns: 0 on success, -1 on error
 */
int cow_handle_lazy_accept_post_connect(struct list_head *lpis)
{
	/*
	 * Start drain thread if all pages have been sent.
	 * Skip switch_to_convergence() - the async bulk reader was already
	 * cleaned up when we received all_pages_sent, and we don't need it
	 * anymore since all pages are in the buffer.
	 */
	if (cow_is_all_pages_sent_received())
		cow_start_drain_thread(lpis);

	return 0;
}


