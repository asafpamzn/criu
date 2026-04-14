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
#include "uffd.h"
#include "uffd-internal.h"
#include "page-xfer.h"
#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"
#include "common/bug.h"
#include "cow/pf-tracker.h"
#include "cow/page-pool.h"
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

/* Hash table and locking constants now in cow-conf.h */

struct page_buffer_node {
	struct {
		unsigned long vaddr;
		void *data;
	} entries[COW_PAGE_NODE_ENTRIES];
	int count;			/* Number of valid entries in this node */
	struct hlist_node hash;
	struct list_head chunk_list;	/* Link in chunk's page list for ordered drain */
	int chunk_id;			/* Cached chunk ID for drain ordering */
};

static struct {
	struct hlist_head *hash_table;
	unsigned long max_bucket_depth;	/* Max pages in any bucket */
	unsigned long nr_pages;
	unsigned long nr_applied;
	unsigned long nr_discarded;
	unsigned long nr_eagain;
	bool initialized;
} cow_buffer = { .initialized = false };

/*
 * Chunk-ordered drain index.
 * Allows draining pages grouped by their page pool chunk, so chunks
 * can be freed progressively instead of all at the end.
 */
/* COW_MAX_POOL_CHUNKS now defined as COW_MAX_POOL_CHUNKS in cow-conf.h */

struct chunk_drain_entry {
	struct list_head pages;		/* List of page_buffer_nodes in this chunk */
	pthread_spinlock_t lock;	/* Per-chunk lock for drain */
	atomic_int page_count;		/* Number of pages in this chunk's list */
};

static struct chunk_drain_entry chunk_index[COW_MAX_POOL_CHUNKS];
static atomic_bool chunk_index_initialized = false;
static atomic_int nr_active_chunks = 0;

/* Fine-grained locks: 8K locks for 1M buckets */
static pthread_spinlock_t hash_locks[COW_NUM_HASH_LOCKS];

/* Global lock for counters (nr_pages, nr_applied, etc.) */
static pthread_spinlock_t counter_lock;

static inline int lock_index(unsigned int hash)
{
	return hash / COW_BUCKETS_PER_LOCK;
}

/*
 * Multithreaded drain configuration.
 * Each thread handles a range of chunks for parallel draining.
 * COW_NUM_DRAIN_THREADS now defined as COW_COW_NUM_DRAIN_THREADS in cow-conf.h
 */

struct drain_thread_args {
	int thread_id;
	int start_chunk;
	int end_chunk;
};

static pthread_t drain_threads[COW_NUM_DRAIN_THREADS];
static struct drain_thread_args drain_args[COW_NUM_DRAIN_THREADS];
static atomic_bool drain_thread_stop = false;
static atomic_int drain_threads_active = 0;
static struct list_head *drain_lpis = NULL;  /* lpis list for EAGAIN handling */
static atomic_ulong total_drained = 0;  /* Total pages drained across all threads */
static atomic_int next_drain_chunk = 0;  /* Work-stealing: next chunk to process */
static int max_drain_chunks = 0;  /* Total chunks to drain */

static inline unsigned int page_buffer_hash(unsigned long vaddr)
{
	return (vaddr >> PAGE_SHIFT) & (COW_PAGE_BUFFER_HASH_SIZE - 1);
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

/*
 * Flags for cow_uffd_copy_and_track()
 */
#define COW_TRACK_STRICT    (1 << 0)  /* BUG() on EEXIST/ERROR (drain mode) */
#define COW_TRACK_RETRY     (1 << 1)  /* Retry mode: no buffer stats, return -EAGAIN */

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
 * @data_owned: output - set to true if data ownership transferred (EAGAIN drain mode)
 *
 * Returns:
 *   1 - success (page copied)
 *   0 - soft handled (ENOENT unmapped, EAGAIN queued, EEXIST already done)
 *  -1 - error
 *  -EAGAIN - kernel busy (only with COW_TRACK_RETRY flag)
 */
static int cow_uffd_copy_and_track(int uffd, unsigned long vaddr, void *data,
				   unsigned long nr_pages,
				   struct lazy_pages_info *lpi,
				   struct list_head *lpis,
				   unsigned int flags,
				   const char *caller,
				   bool *data_owned)
{
	static atomic_ulong copy_log_count = 0;
	enum cow_copy_result res;

	if (data_owned)
		*data_owned = false;


	res = cow_uffd_copy_pages(uffd, vaddr, data, nr_pages, NULL);

	switch (res) {
	case COW_COPY_OK:
		if (!(flags & COW_TRACK_RETRY))
			__sync_fetch_and_add(&cow_buffer.nr_applied, 1);
		pf_tracker_set_state(vaddr, PF_STATE_COMPLETED);
		page_state_set(vaddr, PAGE_STATE_COPIED);
		if (lpi)
			lpi->copied_pages += nr_pages;
		return 1;

	case COW_COPY_EEXIST:
		if (!(flags & COW_TRACK_RETRY))
			__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
		pr_err("COW_TRACE %s: 0x%lx EEXIST (already copied)\n", caller, vaddr);
		if (!unmapped_tracker_is_unmapped(vaddr) &&
		    page_state_get(vaddr) != PAGE_STATE_DIRTY)
			page_state_set(vaddr, PAGE_STATE_DISCARDED);
		if (flags & COW_TRACK_STRICT) {
			pr_err("BUG: %s EEXIST at 0x%lx - duplicate copy!\n", caller, vaddr);
			page_state_print_history(vaddr);
			BUG();
		}
		return 0;  /* soft handled - drain already did it */

	case COW_COPY_ENOENT:
		if (!(flags & COW_TRACK_RETRY))
			__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
		pr_err("COW_TRACE %s: 0x%lx ENOENT (VMA unmapped)\n", caller, vaddr);
		if (!unmapped_tracker_is_unmapped(vaddr)) {
			page_state_set(vaddr, PAGE_STATE_DISCARDED);
			unmapped_tracker_mark_range(vaddr, nr_pages * PAGE_SIZE);
		}
		return 0;

	case COW_COPY_EAGAIN:
		if (flags & COW_TRACK_RETRY) {
			/* Retry mode - return -EAGAIN, don't queue */
			pr_err("COW_TRACE %s: 0x%lx EAGAIN (retry mode)\n", caller, vaddr);
			return -EAGAIN;
		}
		__sync_fetch_and_add(&cow_buffer.nr_eagain, 1);
		pr_err("COW_TRACE %s: 0x%lx EAGAIN, queuing for retry\n", caller, vaddr);
		if (lpis) {
			/* Drain mode - use drain EAGAIN queue.
			 * cow_queue_eagain_request makes an xmalloc copy of the data,
			 * so we must NOT set data_owned - the caller should still free
			 * the original page pool buffer.
			 */
			cow_queue_drain_eagain_request(lpis, vaddr, data);
			/* Note: data_owned stays false, so caller will page_pool_put(data) */
		} else if (lpi) {
			/* Normal mode - use regular EAGAIN queue */
			pf_tracker_set_state(vaddr, PF_STATE_PENDING_EAGAIN);
			cow_queue_eagain_request(lpi, vaddr, nr_pages, data, caller);
		}
		return 0;

	case COW_COPY_ERROR:
		pr_err("COW_TRACE %s: 0x%lx FAILED errno=%d\n", caller, vaddr, errno);
		page_state_print_history(vaddr);
		if (!unmapped_tracker_is_unmapped(vaddr) &&
		    page_state_get(vaddr) != PAGE_STATE_DIRTY)
			page_state_set(vaddr, PAGE_STATE_DISCARDED);
		
		BUG();
		return -1;
	}

	return -1;  /* unreachable */
}

int cow_page_buffer_init(void)
{
	int i;

	if (cow_buffer.initialized)
		return 0;

	cow_buffer.hash_table = xmalloc(COW_PAGE_BUFFER_HASH_SIZE *
					sizeof(struct hlist_head));
	if (!cow_buffer.hash_table)
		return -1;

	for (i = 0; i < COW_PAGE_BUFFER_HASH_SIZE; i++)
		INIT_HLIST_HEAD(&cow_buffer.hash_table[i]);

	/* Initialize 8K fine-grained locks */
	for (i = 0; i < COW_NUM_HASH_LOCKS; i++)
		pthread_spin_init(&hash_locks[i], PTHREAD_PROCESS_PRIVATE);

	/* Initialize counter lock */
	pthread_spin_init(&counter_lock, PTHREAD_PROCESS_PRIVATE);

	/* Initialize chunk drain index */
	for (i = 0; i < COW_MAX_POOL_CHUNKS; i++) {
		INIT_LIST_HEAD(&chunk_index[i].pages);
		pthread_spin_init(&chunk_index[i].lock, PTHREAD_PROCESS_PRIVATE);
		atomic_init(&chunk_index[i].page_count, 0);
	}
	atomic_store(&chunk_index_initialized, true);

	cow_buffer.nr_pages = 0;
	cow_buffer.nr_applied = 0;
	cow_buffer.nr_discarded = 0;
	cow_buffer.nr_eagain = 0;
	cow_buffer.max_bucket_depth = 0;
	cow_buffer.initialized = true;

	pr_info("COW page buffer initialized (buckets=%d, locks=%d, chunk_slots=%d)\n",
		COW_PAGE_BUFFER_HASH_SIZE, COW_NUM_HASH_LOCKS, COW_MAX_POOL_CHUNKS);
	return 0;
}

int cow_page_buffer_thread_init(int thread_id)
{
	return page_pool_thread_init(thread_id);
}

int cow_page_buffer_add(unsigned long vaddr, void *data, int thread_id, bool nocopy)
{
	struct page_buffer_node *node;
	unsigned int hash;
	enum page_state state;
	void *page_data;
	int lock_idx;
	int i;

	if (!cow_buffer.initialized)
		return -1;

	/*
	 * Server rule: each page is sent only once, unless dirty (re-sent with
	 * newer data). DIRTY -> IN_BUFFER is valid (dirty page re-sent).
	 * IN_BUFFER -> IN_BUFFER is valid (dirty page overwrites existing).
	 * COPIED/DISCARDED -> IN_BUFFER is a bug - server sent duplicate
	 * non-dirty page.
	 */
	state = page_state_get(vaddr);
	if (state == PAGE_STATE_COPIED || state == PAGE_STATE_DISCARDED) {
		pr_err("COW_TRACE ADD_ERROR: 0x%lx already %s - server sent duplicate!\n",
		       vaddr, page_state_name(state));
		page_state_print_history(vaddr);
		BUG();  /* Protocol violation - stop immediately */
	}
	/* PAGE_STATE_DIRTY and PAGE_STATE_IN_BUFFER are OK */

	hash = page_buffer_hash(vaddr);
	lock_idx = lock_index(hash);

	/*
	 * First pass: check if page already exists.
	 * If so, memcpy directly into existing buffer (no allocation).
	 */
	pthread_spin_lock(&hash_locks[lock_idx]);

	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		for (i = 0; i < node->count; i++) {
			if (node->entries[i].vaddr == vaddr) {
				/*
				 * Page exists - memcpy directly into existing buffer.
				 * No allocation needed, no freeing old data.
				 */
				memcpy(node->entries[i].data, data, PAGE_SIZE);
				page_state_set_with_crc(vaddr, PAGE_STATE_IN_BUFFER, data);
				pthread_spin_unlock(&hash_locks[lock_idx]);
				pr_debug("COW_TRACE OVERWRITE: 0x%lx\n", vaddr);
				return 0;
			}
		}
	}

	pthread_spin_unlock(&hash_locks[lock_idx]);

	/*
	 * Page doesn't exist - now allocate (if not nocopy).
	 * Allocation is done outside lock for better concurrency.
	 */
	if (nocopy) {
		/* Take ownership of data pointer directly (from page_pool_get_chunk) */
		page_data = data;
	} else {
		if (thread_id < 0) {
			pr_err("BUG: cow_page_buffer_add called with invalid thread_id %d\n",
			       thread_id);
			BUG();
		}
		page_data = page_pool_get(thread_id);
		if (!page_data) {
			pr_err("BUG: page_pool_get failed for thread %d\n", thread_id);
			BUG();
		}
		memcpy(page_data, data, PAGE_SIZE);
	}

	/*
	 * Second pass: add to existing node or create new one.
	 * Re-check under lock since state may have changed.
	 */
	pthread_spin_lock(&hash_locks[lock_idx]);

	/* Re-check for duplicate (another thread may have added it) */
	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		for (i = 0; i < node->count; i++) {
			if (node->entries[i].vaddr == vaddr) {
				/* Race: page was added by another thread, overwrite */
				memcpy(node->entries[i].data, data, PAGE_SIZE);
				page_state_set_with_crc(vaddr, PAGE_STATE_IN_BUFFER, data);
				pthread_spin_unlock(&hash_locks[lock_idx]);
				/* Free our allocation since we didn't use it */
				page_pool_put(page_data);
				return 0;
			}
		}
		/* Found node with space - add new entry */
		if (node->count < PAGE_NODE_ENTRIES) {
			node->entries[node->count].vaddr = vaddr;
			node->entries[node->count].data = page_data;
			node->count++;
			page_state_set_with_crc(vaddr, PAGE_STATE_IN_BUFFER, page_data);
			pthread_spin_unlock(&hash_locks[lock_idx]);
			__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
			return 0;
		}
	}

	pthread_spin_unlock(&hash_locks[lock_idx]);

	/* Need new node - allocate outside lock */
	node = xmalloc(sizeof(*node));
	if (!node) {
		page_pool_put(page_data);
		return -1;
	}

	node->entries[0].vaddr = vaddr;
	node->entries[0].data = page_data;
	node->count = 1;
	INIT_HLIST_NODE(&node->hash);
	INIT_LIST_HEAD(&node->chunk_list);

	/* Get chunk ID for this page's data for chunk-ordered drain */
	node->chunk_id = page_pool_get_chunk_id(page_data);

	/* Debug: track pages with missing chunk_id for investigation */
	if (node->chunk_id < 0) {
		static atomic_int bad_chunk_count = 0;
		int count = atomic_fetch_add(&bad_chunk_count, 1);
		if (count < 10 || count % COW_LOG_SAMPLE_100K == 0) {
			pr_err("CHUNK_ID_MISSING: page_data=%p vaddr=0x%lx count=%d "
			       "(page not in page pool tracking)\n",
			       page_data, vaddr, count + 1);
		}
	}

	pthread_spin_lock(&hash_locks[lock_idx]);
	hlist_add_head(&node->hash, &cow_buffer.hash_table[hash]);
	page_state_set_with_crc(vaddr, PAGE_STATE_IN_BUFFER, page_data);
	pthread_spin_unlock(&hash_locks[lock_idx]);

	/* Add to chunk index for chunk-ordered drain */
	if (node->chunk_id >= 0 && node->chunk_id < COW_MAX_POOL_CHUNKS) {
		int cur_max = 0;
		pthread_spin_lock(&chunk_index[node->chunk_id].lock);
		list_add_tail(&node->chunk_list, &chunk_index[node->chunk_id].pages);
		atomic_fetch_add(&chunk_index[node->chunk_id].page_count, 1);
		pthread_spin_unlock(&chunk_index[node->chunk_id].lock);

		/* Track max chunk ID seen for drain distribution */
		cur_max = atomic_load(&nr_active_chunks);
		while (node->chunk_id >= cur_max) {
			if (atomic_compare_exchange_weak(&nr_active_chunks, &cur_max, node->chunk_id + 1))
				break;
		}
	}

	__sync_fetch_and_add(&cow_buffer.nr_pages, 1);

	pr_debug("COW_TRACE ADD: 0x%lx chunk=%d (total=%lu)\n", vaddr, node->chunk_id, cow_buffer.nr_pages);

	return 0;
}

void *cow_page_buffer_lookup_and_remove(unsigned long vaddr)
{
	struct page_buffer_node *node;
	unsigned int hash;
	int lock_idx;
	void *data = NULL;
	int i;

	if (!cow_buffer.initialized)
		return NULL;

	hash = page_buffer_hash(vaddr);
	lock_idx = lock_index(hash);

	pthread_spin_lock(&hash_locks[lock_idx]);
	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		for (i = 0; i < node->count; i++) {
			if (node->entries[i].vaddr == vaddr) {
				data = node->entries[i].data;
				/* Move last entry to fill gap */
				node->count--;
				if (i < node->count) {
					node->entries[i] = node->entries[node->count];
				}
				/* Remove empty nodes */
				if (node->count == 0) {
					int chunk_id = node->chunk_id;
					hlist_del(&node->hash);
					pthread_spin_unlock(&hash_locks[lock_idx]);
					/* Also remove from chunk list */
					if (chunk_id >= 0 && chunk_id < COW_MAX_POOL_CHUNKS) {
						pthread_spin_lock(&chunk_index[chunk_id].lock);
						list_del(&node->chunk_list);
						atomic_fetch_sub(&chunk_index[chunk_id].page_count, 1);
						pthread_spin_unlock(&chunk_index[chunk_id].lock);
					}
					xfree(node);
					__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);
					return data;
				}
				pthread_spin_unlock(&hash_locks[lock_idx]);
				__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);
				return data;
			}
		}
	}
	pthread_spin_unlock(&hash_locks[lock_idx]);

	return data;
}

unsigned long cow_page_buffer_count(void)
{
	return cow_buffer.nr_pages;
}

void cow_page_buffer_destroy(void)
{
	struct page_buffer_node *node;
	struct hlist_node *tmp;
	int i, j;

	if (!cow_buffer.initialized)
		return;

	/* Stop drain thread first */
	cow_stop_drain_thread();
	pr_warn("file = %s, line = %d\n",__FILE__, __LINE__);
	/* Lock all buckets and destroy contents */
	for (i = 0; i < COW_PAGE_BUFFER_HASH_SIZE; i++) {
		int lock_idx = lock_index(i);
		pthread_spin_lock(&hash_locks[lock_idx]);
		hlist_for_each_entry_safe(node, tmp,
					  &cow_buffer.hash_table[i], hash) {
			for (j = 0; j < node->count; j++)
				page_pool_put(node->entries[j].data);
			hlist_del(&node->hash);
			xfree(node);
		}
		pthread_spin_unlock(&hash_locks[lock_idx]);
	}
	pr_warn("file = %s, line = %d\n",__FILE__, __LINE__);
	xfree(cow_buffer.hash_table);
	cow_buffer.hash_table = NULL;
	cow_buffer.initialized = false;
	pr_warn("file = %s, line = %d\n",__FILE__, __LINE__);
	/* Destroy all fine-grained locks */
	for (i = 0; i < COW_NUM_HASH_LOCKS; i++)
		pthread_spin_destroy(&hash_locks[i]);
	pthread_spin_destroy(&counter_lock);

	/* Clean up chunk index */
	for (i = 0; i < COW_MAX_POOL_CHUNKS; i++) {
		pthread_spin_destroy(&chunk_index[i].lock);
		INIT_LIST_HEAD(&chunk_index[i].pages);
	}
	atomic_store(&chunk_index_initialized, false);
	atomic_store(&nr_active_chunks, 0);

	pr_warn("file = %s, line = %d\n",__FILE__, __LINE__);
	pr_warn("COW page buffer destroyed: applied=%lu discarded=%lu max_bucket=%lu\n",
		cow_buffer.nr_applied, cow_buffer.nr_discarded,
		cow_buffer.max_bucket_depth);

	/* Destroy all page pools last */
	page_pool_destroy_all();
}

/*
 * Remove all pages in a range from the buffer.
 * Called when VMA is unmapped - no point keeping these pages.
 */
void cow_page_buffer_remove_range(unsigned long start, unsigned long len)
{
	struct page_buffer_node *node;
	unsigned long vaddr, end;
	unsigned int hash, last_hash = UINT_MAX;
	unsigned long removed = 0;
	int i;

	if (!cow_buffer.initialized)
		return;

	end = start + len;

	for (vaddr = start; vaddr < end; vaddr += PAGE_SIZE) {
		hash = page_buffer_hash(vaddr);

		/* Switch locks when hash changes lock group */
		if (lock_index(hash) != lock_index(last_hash)) {
			if (last_hash != UINT_MAX)
				pthread_spin_unlock(&hash_locks[lock_index(last_hash)]);
			pthread_spin_lock(&hash_locks[lock_index(hash)]);
		}
		last_hash = hash;

		/* Search for page in bucket */
		hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
			for (i = 0; i < node->count; i++) {
				if (node->entries[i].vaddr == vaddr) {
					void *data = node->entries[i].data;

					/* Remove by moving last entry here */
					node->count--;
					if (i < node->count)
						node->entries[i] = node->entries[node->count];

					/* Free page data */
					page_pool_put(data);
					removed++;

					/* Remove empty nodes */
					if (node->count == 0) {
						int chunk_id = node->chunk_id;
						hlist_del(&node->hash);
						pthread_spin_unlock(&hash_locks[lock_index(hash)]);
						/* Also remove from chunk list */
						if (chunk_id >= 0 && chunk_id < COW_MAX_POOL_CHUNKS) {
							pthread_spin_lock(&chunk_index[chunk_id].lock);
							list_del(&node->chunk_list);
							atomic_fetch_sub(&chunk_index[chunk_id].page_count, 1);
							pthread_spin_unlock(&chunk_index[chunk_id].lock);
						}
						xfree(node);
						last_hash = UINT_MAX;  /* Force re-acquire */
					}
					goto next_page;
				}
			}
		}
next_page:;
	}

	if (last_hash != UINT_MAX)
		pthread_spin_unlock(&hash_locks[lock_index(last_hash)]);

	if (removed > 0) {
		__sync_fetch_and_sub(&cow_buffer.nr_pages, removed);
		__sync_fetch_and_add(&cow_buffer.nr_discarded, removed);
		pr_info("Removed %lu pages from buffer for UNMAP range 0x%lx-0x%lx\n",
			removed, start, end);
	}
}

/*
 * Re-add a page to the buffer for EAGAIN retry.
 * Called when UFFDIO_COPY fails with EAGAIN.
 */
void cow_page_buffer_readd(unsigned long vaddr, void *data)
{
	struct page_buffer_node *node;
	unsigned int hash;
	int lock_idx;

	hash = page_buffer_hash(vaddr);
	lock_idx = lock_index(hash);

	pthread_spin_lock(&hash_locks[lock_idx]);

	/* Try to find space in existing node */
	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		if (node->count < PAGE_NODE_ENTRIES) {
			node->entries[node->count].vaddr = vaddr;
			node->entries[node->count].data = data;
			node->count++;
			pthread_spin_unlock(&hash_locks[lock_idx]);
			__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
			page_state_set(vaddr, PAGE_STATE_EAGAIN_QUEUED);
			return;
		}
	}

	pthread_spin_unlock(&hash_locks[lock_idx]);

	/* Need new node */
	node = xmalloc(sizeof(*node));
	if (!node) {
		pr_err("Failed to re-add page 0x%lx on EAGAIN\n", vaddr);
		page_pool_put(data);
		return;
	}

	node->entries[0].vaddr = vaddr;
	node->entries[0].data = data;
	node->count = 1;
	INIT_HLIST_NODE(&node->hash);
	INIT_LIST_HEAD(&node->chunk_list);
	node->chunk_id = page_pool_get_chunk_id(data);

	pthread_spin_lock(&hash_locks[lock_idx]);
	hlist_add_head(&node->hash, &cow_buffer.hash_table[hash]);
	pthread_spin_unlock(&hash_locks[lock_idx]);

	/* Add to chunk index for chunk-ordered drain */
	if (node->chunk_id >= 0 && node->chunk_id < COW_MAX_POOL_CHUNKS) {
		pthread_spin_lock(&chunk_index[node->chunk_id].lock);
		list_add_tail(&node->chunk_list, &chunk_index[node->chunk_id].pages);
		atomic_fetch_add(&chunk_index[node->chunk_id].page_count, 1);
		pthread_spin_unlock(&chunk_index[node->chunk_id].lock);
	}

	__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
	page_state_set(vaddr, PAGE_STATE_EAGAIN_QUEUED);
	pr_debug("COW_TRACE DRAIN_READD: 0x%lx chunk=%d re-buffered for EAGAIN retry\n", vaddr, node->chunk_id);
}

/*
 * Fallback drain for orphaned pages (chunk_id=-1).
 * These pages were added to hash table but NOT to chunk_index because
 * page_pool_get_chunk_id() returned -1. The chunk-ordered drain misses them.
 * This function iterates the hash table directly to drain any remaining pages.
 */
static void drain_orphaned_pages_from_hash(void)
{
	struct page_buffer_node *node;
	struct hlist_node *tmp;
	unsigned long drained = 0, discarded = 0;
	unsigned long last_log_count = 0;
	int bucket;

	pr_err("DRAIN_FALLBACK: Starting hash-table scan for %lu orphaned pages\n",
	       cow_buffer.nr_pages);

	for (bucket = 0; bucket < COW_PAGE_BUFFER_HASH_SIZE && cow_buffer.nr_pages > 0; bucket++) {
		int lock_idx = lock_index(bucket);

		pthread_spin_lock(&hash_locks[lock_idx]);
		hlist_for_each_entry_safe(node, tmp, &cow_buffer.hash_table[bucket], hash) {
			while (node->count > 0) {
				int idx = node->count - 1;
				unsigned long vaddr = node->entries[idx].vaddr;
				void *data = node->entries[idx].data;
				int uffd;
				bool data_owned = false;

				node->count--;
				pthread_spin_unlock(&hash_locks[lock_idx]);
				__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);

				/* Track state change */
				page_state_set(vaddr, PAGE_STATE_DRAIN_PENDING);

				/* Find uffd and copy page */
				uffd = cow_get_uffd_for_vaddr(drain_lpis, vaddr);
				if (uffd >= 0) {
					int ret = cow_uffd_copy_and_track(uffd, vaddr, data, 1,
									 NULL, drain_lpis,
									 COW_TRACK_STRICT,
									 "FALLBACK", &data_owned);
					if (ret > 0)
						drained++;
					else
						discarded++;
				} else {
					__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
					discarded++;
					if (!unmapped_tracker_is_unmapped(vaddr) &&
					    page_state_get(vaddr) != PAGE_STATE_DIRTY)
						page_state_set(vaddr, PAGE_STATE_DISCARDED);
				}

				if (!data_owned)
					page_pool_put(data);

				/* Progress logging every 1M pages */
				if ((drained + discarded) - last_log_count >= 1000000) {
					pr_err("DRAIN_FALLBACK: drained=%lu discarded=%lu remaining=%lu\n",
					       drained, discarded, cow_buffer.nr_pages);
					last_log_count = drained + discarded;
				}

				pthread_spin_lock(&hash_locks[lock_idx]);
			}

			/* Remove empty node from hash table */
			if (node->count == 0) {
				hlist_del(&node->hash);
				/*
				 * Note: chunk_list is self-referential (INIT_LIST_HEAD) for
				 * nodes with chunk_id=-1, so no list_del needed.
				 */
				xfree(node);
			}
		}
		pthread_spin_unlock(&hash_locks[lock_idx]);
	}

	pr_err("DRAIN_FALLBACK: DONE drained=%lu discarded=%lu remaining=%lu\n",
	       drained, discarded, cow_buffer.nr_pages);
}

/*
 * Background drain worker thread - proactively UFFDIO_COPY pages
 * from buffer to reduce future page faults and free memory.
 *
 * Each worker handles a subset of CHUNKS for chunk-ordered draining.
 * By draining all pages from one chunk before moving to the next,
 * chunks can be freed progressively instead of all at the end.
 *
 * Thread safety is ensured by:
 *   - Per-chunk locks (chunk_index[].lock) for list iteration
 *   - Fine-grained hash bucket locks (hash_locks[]) for hash removal
 *   - Atomic counters for shared statistics
 *   - Thread-safe page_state and pf_tracker APIs
 */
static void *background_drain_worker(void *arg)
{
	struct drain_thread_args *args = (struct drain_thread_args *)arg;
	struct page_buffer_node *node, *tmp_node;
	unsigned long drained = 0;
	unsigned long last_progress_drained = 0;
	time_t last_progress_time = 0;
	int thread_id = args->thread_id;
	int chunk_id;
	char thread_name[16];

	/* Set thread name for debugging (max 15 chars + null) */
	snprintf(thread_name, sizeof(thread_name), "cow-drain-%d", thread_id);
	pthread_setname_np(pthread_self(), thread_name);

	pr_err("DRAIN_PROGRESS: thread=%d STARTED (work-stealing mode) buffered=%lu\n",
	       thread_id, cow_buffer.nr_pages);
	last_progress_time = time(NULL);

	while (!atomic_load(&drain_thread_stop) && cow_buffer.nr_pages > 0) {
		/* Work-stealing: atomically grab next chunk */
		chunk_id = atomic_fetch_add(&next_drain_chunk, 1);
		if (chunk_id >= max_drain_chunks) {
			/* No more chunks to process - exit work loop */
			pr_info("DRAIN_PROGRESS: thread=%d no more chunks (chunk_id=%d >= max=%d), drained=%lu\n",
			       thread_id, chunk_id, max_drain_chunks, drained);
			break;
		}

		{
			unsigned long chunk_drained = 0;

			pthread_spin_lock(&chunk_index[chunk_id].lock);
			list_for_each_entry_safe(node, tmp_node,
						 &chunk_index[chunk_id].pages, chunk_list) {
				/* Process all entries in this node */
				while (node->count > 0 && !atomic_load(&drain_thread_stop)) {
					/* Take last entry (avoids moving data) */
					int idx = node->count - 1;
					unsigned long vaddr = node->entries[idx].vaddr;
					void *data = node->entries[idx].data;
					int uffd;
					bool free_data = true;

					node->count--;

					/* Track: removed from buffer, about to copy */
					page_state_set(vaddr, PAGE_STATE_DRAIN_PENDING);

					pr_debug("COW_TRACE DRAIN[%d]: 0x%lx chunk=%d (remaining=%lu)\n",
						 thread_id, vaddr, chunk_id, cow_buffer.nr_pages);

					pthread_spin_unlock(&chunk_index[chunk_id].lock);
					__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);

					/* Find uffd for this address and copy */
					uffd = cow_get_uffd_for_vaddr(drain_lpis, vaddr);
					if (uffd >= 0) {
						bool data_owned = false;
						int ret;
						u32 stored_crc;

						/* Check CRC before copy to detect dirty page races */
						if (!page_state_check_crc(vaddr, data, &stored_crc)) {
							u32 buf_count = page_state_get_buffer_count(vaddr);
							pr_err("DRAIN_CRC_MISMATCH: 0x%lx buffer_count=%u "
							       "- data changed between buffer and copy!\n",
							       vaddr, buf_count);
						}

						ret = cow_uffd_copy_and_track(uffd, vaddr, data, 1,
									     NULL, drain_lpis,
									     COW_TRACK_STRICT,
									     "DRAIN", &data_owned);
						if (ret > 0) {
							drained++;
							chunk_drained++;
							/* Log progress every 100k pages or 10 seconds */
							if (drained - last_progress_drained >= COW_LOG_SAMPLE_100K ||
							    time(NULL) - last_progress_time >= COW_DRAIN_PROGRESS_SEC) {
								pr_info("DRAIN_PROGRESS: thread=%d drained=%lu chunk=%d remaining=%lu\n",
								       thread_id, drained, chunk_id, cow_buffer.nr_pages);
								last_progress_drained = drained;
								last_progress_time = time(NULL);
								/* Thread 0 dumps pool stats every 1M pages */
								if (thread_id == 0 && drained % COW_LOG_SAMPLE_1M < COW_LOG_SAMPLE_100K)
									page_pool_dump_stats();
							}
						}
						if (data_owned)
							free_data = false;
					} else {
						__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
						pr_err("COW_TRACE DRAIN[%d]: 0x%lx no uffd found\n",
						       thread_id, vaddr);
						page_state_print_history(vaddr);
						if (!unmapped_tracker_is_unmapped(vaddr) &&
						    page_state_get(vaddr) != PAGE_STATE_DIRTY)
							page_state_set(vaddr, PAGE_STATE_DISCARDED);
						BUG();
					}

					if (free_data)
						page_pool_put(data);
					pthread_spin_lock(&chunk_index[chunk_id].lock);
				}

				/* Remove empty node from both chunk list and hash table */
				if (node->count == 0) {
					unsigned int hash = page_buffer_hash(node->entries[0].vaddr);
					int lock_idx = lock_index(hash);

					list_del(&node->chunk_list);
					atomic_fetch_sub(&chunk_index[chunk_id].page_count, 1);

					/* Also remove from hash table */
					pthread_spin_unlock(&chunk_index[chunk_id].lock);
					pthread_spin_lock(&hash_locks[lock_idx]);
					hlist_del(&node->hash);
					pthread_spin_unlock(&hash_locks[lock_idx]);
					pthread_spin_lock(&chunk_index[chunk_id].lock);

					xfree(node);
				}
			}
			pthread_spin_unlock(&chunk_index[chunk_id].lock);

			/* Log when we finish draining a chunk */
			if (chunk_drained > 0) {
				pr_err("DRAIN_CHUNK_DONE: thread=%d chunk=%d drained=%lu\n",
				       thread_id, chunk_id, chunk_drained);
			}
		}
	}

	/* Update global statistics */
	atomic_fetch_add(&total_drained, drained);

	pr_err("DRAIN_PROGRESS: thread=%d FINISHED drained=%lu\n", thread_id, drained);

	/* Decrement active thread count */
	if (atomic_fetch_sub(&drain_threads_active, 1) == 1) {
		/*
		 * Last thread to exit. Add full memory barrier to ensure all
		 * UFFDIO_COPY writes are visible before signaling drain complete.
		 * This is critical on ARM where memory ordering is weaker.
		 */
		atomic_thread_fence(memory_order_seq_cst);

		pr_err("DRAIN_PROGRESS: ALL_DONE total=%lu applied=%lu discarded=%lu eagain=%lu remaining=%lu\n",
		       atomic_load(&total_drained), cow_buffer.nr_applied,
		       cow_buffer.nr_discarded, cow_buffer.nr_eagain, cow_buffer.nr_pages);

		/* Debug: check what remains in chunk_index vs hash table (limited scan) */
		if (cow_buffer.nr_pages > 0) {
			unsigned long in_chunks = 0, in_hash = 0;
			int i, samples = 0;
			struct page_buffer_node *node;

			/* Count nodes in chunk_index (fast - only 512 entries) */
			for (i = 0; i < COW_MAX_POOL_CHUNKS; i++) {
				int count = atomic_load(&chunk_index[i].page_count);
				in_chunks += count;
			}

			/* Sample hash table - check first 10000 buckets only */
			for (i = 0; i < 10000 && i < COW_PAGE_BUFFER_HASH_SIZE; i++) {
				hlist_for_each_entry(node, &cow_buffer.hash_table[i], hash) {
					in_hash += node->count;
					if (samples < 5) {
						pr_err("DRAIN_REMAIN_SAMPLE: hash[%d] chunk_id=%d count=%d\n",
						       i, node->chunk_id, node->count);
						samples++;
					}
				}
			}
			/* Extrapolate: hash has 1M buckets, we sampled 10k */
			pr_err("DRAIN_REMAIN: chunk_index_nodes=%lu hash_sample(10k)=%lu (extrapolated=%lu) nr_pages=%lu\n",
			       in_chunks, in_hash, in_hash * 100, cow_buffer.nr_pages);

			/*
			 * Orphaned pages with chunk_id=-1 were never added to chunk_index,
			 * so chunk-ordered drain missed them. Fall back to hash iteration.
			 */
			drain_orphaned_pages_from_hash();
		}
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

	pr_info("DRAIN_PROGRESS: chunk-ordered drain: total_chunks=%d chunks_per_thread=%d\n",
	       total_chunks, chunks_per_thread);

	
	/* Initialize work-stealing globals */
	atomic_store(&next_drain_chunk, 0);
	max_drain_chunks = total_chunks;

	for (i = 0; i < COW_NUM_DRAIN_THREADS; i++) {
		drain_args[i].thread_id = i;
		/* start/end_chunk unused with work-stealing, but set for debug logging */
		drain_args[i].start_chunk = 0;
		drain_args[i].end_chunk = total_chunks;

		if (pthread_create(&drain_threads[i], NULL,
				   background_drain_worker, &drain_args[i])) {
			pr_perror("Failed to create drain thread %d", i);
			continue;
		}
		atomic_fetch_add(&drain_threads_active, 1);
		created++;
	}

	if (created == 0) {
		pr_err("Failed to create any drain threads\n");
		return -1;
	}

	pr_err("DRAIN_PROGRESS: STARTING %d/%d drain threads (work-stealing), buffered=%lu total_chunks=%d\n",
	       created, COW_NUM_DRAIN_THREADS, cow_buffer.nr_pages, total_chunks);

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

	/* Only log when state changes to avoid log spam */
	static int last_signal = -1, last_drain = -1;
	static unsigned long call_count = 0;
	int cur_signal = cow_is_all_pages_sent_received();
	int cur_drain = cow_drain_thread_running();

	call_count++;
	if (cur_signal != last_signal || cur_drain != last_drain || call_count % 100 == 0) {
		pr_err("cow_handle_exit[%lu]: signal=%d drain=%d buffer=%lu\n",
		       call_count, cur_signal, cur_drain, cow_page_buffer_count());
		last_signal = cur_signal;
		last_drain = cur_drain;
	}

	/* Condition 1: Wait for all_pages_sent signal from primary */
	if (!cur_signal) {
		return 0;
	}

	/* Condition 2: Wait for drain thread to finish */
	if (cur_drain) {
		return 0;
	}

	/* Condition 3: Wait for buffer to be empty */
	if (cow_page_buffer_count() > 0) {
		pr_err("cow_handle_exit: waiting for buffer to drain (%lu pages remaining)\n",
		       cow_page_buffer_count());
		return 0;
	}

	/* Condition 4: Wait for EAGAIN requests to be processed */
	if (!cow_is_eagain_queue_empty()) {
		pr_err("cow_handle_exit: waiting for EAGAIN requests to be processed\n");
		return 0;
	}
#if 0
	/* All conditions met - send ACK to primary */
	pr_err("All pages received and drained, sending ACK to primary\n");
	if (send_all_pages_sent_ack() < 0)
		pr_warn("Failed to send all_pages_sent ACK\n");
#endif

	/* Cleanup all lpis */
	pr_err("RACE_DEBUG: [MAIN] cow_handle_exit CLEANUP START - drain_active=%d\n",
	       atomic_load(&drain_threads_active));
	list_for_each_entry_safe(lpi, n, lpis, l) {
		pr_err("RACE_DEBUG: [MAIN] list_del lpi=%p BEFORE\n", lpi);
		lazy_pages_summary(lpi);
		list_del(&lpi->l);
		pr_err("RACE_DEBUG: [MAIN] list_del lpi=%p AFTER - calling lpi_put\n", lpi);
		lpi_put(lpi);
	}
	pr_err("RACE_DEBUG: [MAIN] cow_handle_exit CLEANUP DONE\n");

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
	unsigned long io_complete_bulk_total_ns;
	unsigned long io_complete_bulk_count;
	unsigned long io_complete_bulk_count_start;
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

void cow_uffd_stats_inc_pf(unsigned long nr_pages)
{
	int bucket = cow_get_histogram_bucket(nr_pages);
	uffd_stats.total_pf_reqs++;
	uffd_stats.total_pages += nr_pages;
	uffd_stats.pf_hist[bucket]++;
}

void cow_uffd_stats_inc_bg(unsigned long nr_pages)
{
	int bucket = cow_get_histogram_bucket(nr_pages);
	uffd_stats.total_bg_reqs++;
	uffd_stats.total_pages += nr_pages;
	uffd_stats.bg_hist[bucket]++;
}

void cow_uffd_stats_add_io_bulk(unsigned long ns)
{
	uffd_stats.io_complete_bulk_total_ns += ns;
	uffd_stats.io_complete_bulk_count++;
}

void cow_uffd_stats_inc_io_bulk_start(void)
{
	uffd_stats.io_complete_bulk_count_start++;
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

		/* Print timing stats */
		if (uffd_stats.io_complete_bulk_count_start > 0) {
			pr_err("  TIMING: io_bulk=%lu ns (%lu, %lu ops) copy=%lu ns (%lu ops) drop=%lu ns (%lu ops)\n",
				uffd_stats.io_complete_bulk_total_ns / uffd_stats.io_complete_bulk_count,
				uffd_stats.io_complete_bulk_count,
				uffd_stats.io_complete_bulk_count_start,
				uffd_stats.uffd_copy_count > 0 ? uffd_stats.uffd_copy_total_ns / uffd_stats.uffd_copy_count : 0,
				uffd_stats.uffd_copy_count,
				uffd_stats.drop_iovs_count > 0 ? uffd_stats.drop_iovs_total_ns / uffd_stats.drop_iovs_count : 0,
				uffd_stats.drop_iovs_count);
		}

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

	lp_debug(lpi, "uffd_%s EAGAIN in COW mode: queueing 0x%llx/%ld for later\n",
		 op_name, address, len);

	/* Copy buffer if provided (copy operation) */
	if (buf) {
		buf_copy = xmalloc(len);
		if (!buf_copy) {
			lp_err(lpi, "Failed to allocate buffer for EAGAIN request\n");
			return -1;
		}
		memcpy(buf_copy, buf, len);
	}

	/* Create request entry */
	req = xmalloc(sizeof(*req));
	if (!req) {
		if (buf_copy)
			xfree(buf_copy);
		return -1;
	}

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

	pr_debug("Queued EAGAIN request 0x%llx (op=%s)\n", address, op_name);
	return 0;
}

/*
 * Queue an EAGAIN request from drain thread context.
 * Finds the appropriate lpi for the vaddr and queues for retry.
 * Returns 0 on success (ownership of data transferred), -1 on error.
 */
int cow_queue_drain_eagain_request(struct list_head *lpis, unsigned long vaddr, void *data)
{
	struct lazy_pages_info *lpi;

	list_for_each_entry(lpi, lpis, l) {
		if (lpi->exited || lpi->lpfd.fd < 0)
			continue;
		if (!cow_find_iov(lpi, vaddr))
			continue;

		/* Found the lpi - queue the request (page_state set inside on success) */
		return cow_queue_eagain_request(lpi, vaddr, 1, data, "drain");
	}

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

	ret = cow_uffd_copy_and_track(req->lpi->lpfd.fd, req->address,
				      req->buf, req->nr_pages,
				      req->lpi, NULL,
				      COW_TRACK_RETRY | COW_TRACK_STRICT,
				      "EAGAIN_RETRY", NULL);
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

		/* Call appropriate retry function based on operation type */
		if (req->buf)
			ret = retry_uffd_copy(req);
		else
			ret = retry_uffd_zero(req);

		if (ret == -EAGAIN) {
			/* Still blocked - keep in queue for next attempt */
			uffd_stats.eagain_blocked++;
			pr_debug("EAGAIN retry still blocked for 0x%llx (op=%s)\n",
				 req->address, req->buf ? "copy" : "zero");
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
 * IOV Debugging (COW mode)
 * ============================================================================
 */

void cow_dump_lazy_iov_list(struct lazy_pages_info *lpi, const char *name,
			    struct list_head *iovs, unsigned int max_dump)
{
	struct lazy_iov *iov;
	unsigned long count = 0;
	unsigned long pages = 0;
	unsigned long prev_start = 0;
	bool sorted = true;
	bool first = true;

	list_for_each_entry(iov, iovs, l) {
		unsigned long iov_pages;

		iov_pages = (iov->end - iov->start) / page_size();
		pages += iov_pages;

		if (!first && iov->start < prev_start)
			sorted = false;
		first = false;
		prev_start = iov->start;

		if (count < max_dump)
			lp_err(lpi, "%s[%lu]: 0x%lx-0x%lx img_start=0x%lx pages=%lu\n",
			       name, count, iov->start, iov->end, iov->img_start,
			       iov_pages);
		count++;
	}

	lp_err(lpi, "%s: count=%lu pages=%lu sorted=%s\n", name, count, pages,
	       sorted ? "yes" : "no");
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
static bool cow_inventory_ready_received = false;
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


/* Check if inventory.img is ready on disk */
bool cow_is_inventory_ready_received(void)
{
	return cow_inventory_ready_received;
}

/* Set inventory ready flag (called when PS_IOV_INVENTORY_READY received) */
void cow_set_inventory_ready_received(void)
{
	pr_info("Received inventory ready signal from primary\n");
	cow_inventory_ready_received = true;
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
	struct lazy_pages_info *lpi;

	list_for_each_entry(lpi, lpis, l) {
		
		if (lpi->exited || lpi->lpfd.fd < 0)
			continue;
		if (cow_find_iov(lpi, vaddr)) {
			
			return lpi->lpfd.fd;
		}
	}
	
	return -1;
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

/* Pre-buffer state */
static void *prebuffer_buf = NULL;
static bool phase3_active_flag = false;


/* Forward declarations for page server async reader */
extern int page_server_start_async_read_bulk(void *buf, unsigned long nr_pages,
					     ps_async_read_complete complete, void *priv);
extern int page_server_update_async_callback(ps_async_read_complete complete, void *priv);

void cow_set_phase3_active(bool active)
{
	phase3_active_flag = active;
}

bool cow_is_phase3_active(void)
{
	return phase3_active_flag;
}

void *cow_get_prebuffer_buf(void)
{
	return prebuffer_buf;
}

/*
 * Pre-buffer callback: Phase 4 dirty pages arrive on main socket.
 * P3 receivers handle Phase 2 bulk pages, but Phase 4 pages flow here.
 * These pages overwrite existing buffered pages (dirty page updates).
 * Handles batches of pages (compressed transfers send up to 64 pages).
 */
static int prebuffer_io_complete_internal(unsigned long dst_id, unsigned long vaddr,
					  unsigned long nr_pages, void *priv)
{
	void *data = priv;  /* Points to prebuffer_buf with page data */
	unsigned long i;

	pr_debug("prebuffer_io_complete: buffering %lu Phase 4 dirty pages at vaddr=0x%lx\n",
		 nr_pages, vaddr);

	/* Buffer/overwrite each page using P3 thread 0's pool */
	for (i = 0; i < nr_pages; i++) {
		unsigned long page_vaddr = vaddr + i * PAGE_SIZE;
		void *page_data = (char *)data + i * PAGE_SIZE;

		if (cow_page_buffer_add(page_vaddr, page_data, PHASE4_POOL_ID, false) < 0) {
			pr_err("Failed to buffer dirty page at 0x%lx\n", page_vaddr);
			return -1;
		}
	}
	return 0;
}

int cow_setup_prebuffer_reader(void)
{
	int ret;

	/*
	 * Allocate buffer for batch reception (up to 64 pages = 256KB).
	 * Compressed batches from P3 senders can contain multiple pages.
	 */
	prebuffer_buf = xmalloc(COW_BATCH_SIZE);
	if (!prebuffer_buf)
		return -1;

	/*
	 * Initialize pool 0 for Phase 4 dirty pages. P3 receivers will also
	 * init this pool later, but cow_page_buffer_thread_init is idempotent.
	 */
	ret = cow_page_buffer_thread_init(PHASE4_POOL_ID);
	if (ret < 0) {
		pr_err("Failed to init page pool for Phase 4\n");
		xfree(prebuffer_buf);
		prebuffer_buf = NULL;
		return -1;
	}

	ret = page_server_start_async_read_bulk(
		prebuffer_buf, COW_BATCH_PAGES, prebuffer_io_complete_internal, prebuffer_buf);

	return ret;
}


/*
 * Handle UFFDIO_COPY errors in COW mode.
 * Returns:
 *   1 - error handled (EAGAIN queued, EEXIST ignored), caller should return 0
 *   0 - continue with normal error handling
 *  -1 - fatal error, caller should return -1
 */
int cow_uffd_handle_copy_error(struct lazy_pages_info *lpi,
			       __u64 address, unsigned long nr_pages,
			       void *buf, int saved_errno, long copy_result)
{
	/* EAGAIN: queue for retry instead of blocking */
	if (saved_errno == EAGAIN) {
		pf_tracker_set_state(address, PF_STATE_PENDING_EAGAIN);
		return cow_queue_eagain_request(lpi, address, nr_pages, buf, "copy");
	}

	/* EEXIST: duplicate copy - this is a coordination bug */
	if (saved_errno == EEXIST) {
		lp_err(lpi, "BUG: UFFDIO_COPY EEXIST at 0x%llx - duplicate copy!\n",
		       (unsigned long long)address);
		page_state_print_history(address);
		return -1;
	}

	/* Log errors for debugging */
	lp_err(lpi, "UFFDIO_COPY error at 0x%llx: errno=%d copy=%ld\n",
	       (unsigned long long)address, saved_errno, copy_result);
	page_state_print_history(address);

	/* Mark as discarded unless it's unmapped or dirty */
	if (!unmapped_tracker_is_unmapped(address) &&
	    page_state_get(address) != PAGE_STATE_DIRTY)
		page_state_set(address, PAGE_STATE_DISCARDED);

	return 0;  /* Let caller continue with normal error handling */
}

/*
 * COW mode wrapper for uffd_copy error handling.
 * Combines error check + return logic into single call.
 * Returns: -1 = fatal, 0 = handled (caller returns 0), 1 = not handled
 */
int cow_uffd_check_copy_error(struct lazy_pages_info *lpi,
			      __u64 address, unsigned long nr_pages,
			      void *buf, int saved_errno, long copy_result)
{
	int ret = cow_uffd_handle_copy_error(lpi, address, nr_pages,
					     buf, saved_errno, copy_result);
	if (ret != 0)
		return ret < 0 ? -1 : 0;  /* Fatal or handled */
	return 1;  /* Not handled - continue with normal error path */
}

/*
 * Handle UFFDIO_ZEROPAGE errors in COW mode.
 * Returns:
 *   1 - error handled (EAGAIN queued)
 *   0 - continue with normal error handling
 *  -1 - fatal error
 */
int cow_uffd_handle_zero_error(struct lazy_pages_info *lpi,
			       __u64 address, unsigned long nr_pages,
			       int saved_errno)
{
	/* EAGAIN: queue for retry */
	if (saved_errno == EAGAIN)
		return cow_queue_eagain_request(lpi, address, nr_pages, NULL, "zero");

	return 0;  /* Let caller continue with normal error handling */
}

/*
 * COW mode wrapper for uffd_zero error handling (combines check + return).
 * Returns: -1 = fatal, 0 = handled (caller returns 0), 1 = not handled
 */
int cow_uffd_check_zero_error(struct lazy_pages_info *lpi,
			      __u64 address, unsigned long nr_pages,
			      int saved_errno)
{
	int ret = cow_uffd_handle_zero_error(lpi, address, nr_pages, saved_errno);
	if (ret != 0)
		return ret < 0 ? -1 : 0;  /* Fatal or handled */
	return 1;  /* Not handled - continue with normal error path */
}

/*
 * Track successful UFFDIO_COPY in COW mode.
 */
void cow_uffd_copy_success(unsigned long address)
{
	pf_tracker_set_state(address, PF_STATE_COMPLETED);
	page_state_set(address, PAGE_STATE_COPIED);
}



/*
 * Copy page data for convergence callback.
 * Finds the lpi that owns the vaddr and does UFFDIO_COPY.
 *
 * Returns: 0 on success, -1 on error
 */
int cow_convergence_copy_page(struct list_head *lpis,
			      unsigned long vaddr,
			      unsigned long nr_pages, void *buf)
{
	struct lazy_pages_info *lpi;

	list_for_each_entry(lpi, lpis, l) {
		int ret;

		if (lpi->exited || lpi->lpfd.fd < 0)
			continue;
		if (!cow_find_iov(lpi, vaddr))
			continue;

		/* Found the lpi - copy buffer to lpi->buf and do UFFDIO_COPY */
		memcpy(lpi->buf, buf, nr_pages * PAGE_SIZE);

		ret = cow_uffd_copy_and_track(lpi->lpfd.fd, vaddr, lpi->buf, nr_pages,
					      lpi, NULL, COW_TRACK_STRICT,
					      "CONVERGENCE", NULL);

		lp_debug(lpi, "Convergence copy %lu pages at 0x%lx ret=%d\n", nr_pages, vaddr, ret);

		/* Return 0 for success or soft-handled (ENOENT/EAGAIN), -1 for error */
		return ret >= 0 ? 0 : -1;
	}

	pr_err("Copied to unmap range: Convergence callback with no lpi for vaddr 0x%lx\n", vaddr);
	
	return 1;
}

/*
 * Remove buffered pages before urgent copy.
 * Called from uffd_io_complete to prevent EEXIST when drain thread
 * tries to copy the same page later.
 */
void cow_uffd_remove_buffered_pages(unsigned long addr, unsigned long nr)
{
	unsigned long i;

	for (i = 0; i < nr; i++) {
		unsigned long page_addr = addr + i * PAGE_SIZE;
		void *buffered = cow_page_buffer_lookup_and_remove(page_addr);

		if (buffered) {
			page_state_set(page_addr, PAGE_STATE_URGENT_PENDING);
			page_pool_put(buffered);
		} else {
			page_state_set(page_addr, PAGE_STATE_URGENT_PENDING);
		}
	}
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
 * COW bulk IO complete callback.
 * Called when a bulk page read completes in COW mode (without page server Phase 2/3).
 *
 * NOTE: In COW Phase 2/3 mode (opts.cow_dump && opts.use_page_server),
 * This callback is for COW mode without the page server phased approach.
 */
int cow_uffd_io_complete_bulk(struct lazy_pages_info *lpi,
			      unsigned long vaddr, unsigned long nr_pages)
{
	struct lazy_iov *iov;
	unsigned long pages = nr_pages;
	unsigned long tracked_pages;
	int ret;
	struct timespec t_start, t_end;

	cow_uffd_stats_inc_io_bulk_start();
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* Process may exit while pages are in flight */
	if (lpi->exited) {
		lp_debug(lpi, "Page at 0x%lx no longer needed (exited)\n", vaddr);
		return 0;
	}

	/* Check if address is still tracked */
	iov = cow_find_iov(lpi, vaddr);

	/* Also check requests list */
	if (!iov) {
		struct lazy_iov *req;
		list_for_each_entry(req, &lpi->reqs, l) {
			if (vaddr >= req->start && vaddr < req->end) {
				lp_debug(lpi, "Page at 0x%lx found in requests list\n", vaddr);
				iov = req;
				break;
			}
		}
	}

	if (!iov) {
		lp_debug(lpi, "Page at 0x%lx no longer needed (unmapped), dropping\n", vaddr);
		return 0;
	}

	tracked_pages = (iov->end - vaddr) / PAGE_SIZE;
	pages = min(pages, tracked_pages);
	if (!pages)
		return 0;

	ret = cow_uffd_copy_and_track(lpi->lpfd.fd, vaddr, lpi->buf, pages,
				      lpi, NULL, 0, "BULK_IO", NULL);

	/* Only record timing for successful copies */
	if (ret > 0) {
		clock_gettime(CLOCK_MONOTONIC, &t_end);
		cow_uffd_stats_add_io_bulk((t_end.tv_sec - t_start.tv_sec) * 1000000000 +
					   (t_end.tv_nsec - t_start.tv_nsec));
	}

	/* If process exited during error, treat as success */
	if (ret < 0 && lpi->exited)
		return 0;

	/* Return 0 for success or soft-handled, -1 for error */
	return ret >= 0 ? 0 : -1;
}


/*
 * COW post-connect initialization in handle_lazy_accept.
 * and Phase 3 page requests.
 *
 * Returns: 0 on success, -1 on error
 */
int cow_handle_lazy_accept_post_connect(struct list_head *lpis,
					void (*switch_to_convergence)(void))
{
	/*
	 * Start drain thread if all pages have been sent.
	 * Skip switch_to_convergence() - the async bulk reader was already
	 * cleaned up when we received all_pages_sent, and we don't need it
	 * anymore since all pages are in the buffer.
	 */
	if (cow_is_all_pages_sent_received()) {
		pr_info("All pages sent, starting drain thread\n");
		/* Don't call switch_to_convergence - no page server connection */
		cow_start_drain_thread(lpis);
	}

	return 0;
}



