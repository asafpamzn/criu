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

#define PAGE_BUFFER_HASH_BITS 20
#define PAGE_BUFFER_HASH_SIZE (1 << PAGE_BUFFER_HASH_BITS)  /* 1M buckets */

/*
 * Fine-grained locking: 8K locks, each covering 128 buckets.
 * Allows 10 threads to operate with minimal contention.
 */
#define NUM_HASH_LOCKS 8192
#define BUCKETS_PER_LOCK 128  /* 1M / 8K = 128 buckets per lock */

/*
 * Unrolled linked list node - holds up to 32 entries per node.
 * Gives ~32x better cache locality during traversal vs single-entry nodes.
 */
#define PAGE_NODE_ENTRIES 32

struct page_buffer_node {
	struct {
		unsigned long vaddr;
		void *data;
	} entries[PAGE_NODE_ENTRIES];
	int count;			/* Number of valid entries in this node */
	struct hlist_node hash;
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

/* Fine-grained locks: 8K locks for 1M buckets */
static pthread_spinlock_t hash_locks[NUM_HASH_LOCKS];

/* Global lock for counters (nr_pages, nr_applied, etc.) */
static pthread_spinlock_t counter_lock;

static inline int lock_index(unsigned int hash)
{
	return hash / BUCKETS_PER_LOCK;
}

/*
 * Multithreaded drain configuration.
 * Each thread handles a range of hash buckets for parallel draining.
 */
#define NUM_DRAIN_THREADS 10

struct drain_thread_args {
	int thread_id;
	int start_bucket;
	int end_bucket;
};

static pthread_t drain_threads[NUM_DRAIN_THREADS];
static struct drain_thread_args drain_args[NUM_DRAIN_THREADS];
static atomic_bool drain_thread_stop = false;
static atomic_int drain_threads_active = 0;
static struct list_head *drain_lpis = NULL;  /* lpis list for EAGAIN handling */
static atomic_ulong total_drained = 0;  /* Total pages drained across all threads */

static inline unsigned int page_buffer_hash(unsigned long vaddr)
{
	return (vaddr >> PAGE_SHIFT) & (PAGE_BUFFER_HASH_SIZE - 1);
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
			pr_debug("COW_TRACE %s: 0x%lx EAGAIN (retry mode)\n", caller, vaddr);
			return -EAGAIN;
		}
		__sync_fetch_and_add(&cow_buffer.nr_eagain, 1);
		pr_debug("COW_TRACE %s: 0x%lx EAGAIN, queuing for retry\n", caller, vaddr);
		if (lpis) {
			/* Drain mode - use drain EAGAIN queue */
			if (cow_queue_drain_eagain_request(lpis, vaddr, data) == 0) {
				if (data_owned)
					*data_owned = true;
			}
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

	cow_buffer.hash_table = xmalloc(PAGE_BUFFER_HASH_SIZE *
					sizeof(struct hlist_head));
	if (!cow_buffer.hash_table)
		return -1;

	for (i = 0; i < PAGE_BUFFER_HASH_SIZE; i++)
		INIT_HLIST_HEAD(&cow_buffer.hash_table[i]);

	/* Initialize 8K fine-grained locks */
	for (i = 0; i < NUM_HASH_LOCKS; i++)
		pthread_spin_init(&hash_locks[i], PTHREAD_PROCESS_PRIVATE);

	/* Initialize counter lock */
	pthread_spin_init(&counter_lock, PTHREAD_PROCESS_PRIVATE);

	cow_buffer.nr_pages = 0;
	cow_buffer.nr_applied = 0;
	cow_buffer.nr_discarded = 0;
	cow_buffer.nr_eagain = 0;
	cow_buffer.max_bucket_depth = 0;
	cow_buffer.initialized = true;

	pr_info("COW page buffer initialized (buckets=%d, locks=%d)\n",
		PAGE_BUFFER_HASH_SIZE, NUM_HASH_LOCKS);
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
				page_state_set(vaddr, PAGE_STATE_IN_BUFFER);
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
				page_state_set(vaddr, PAGE_STATE_IN_BUFFER);
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
			page_state_set(vaddr, PAGE_STATE_IN_BUFFER);
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

	pthread_spin_lock(&hash_locks[lock_idx]);
	hlist_add_head(&node->hash, &cow_buffer.hash_table[hash]);
	page_state_set(vaddr, PAGE_STATE_IN_BUFFER);
	pthread_spin_unlock(&hash_locks[lock_idx]);

	__sync_fetch_and_add(&cow_buffer.nr_pages, 1);

	pr_debug("COW_TRACE ADD: 0x%lx (total=%lu)\n", vaddr, cow_buffer.nr_pages);

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
					hlist_del(&node->hash);
					xfree(node);
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

void cow_page_buffer_discard_dirty(unsigned long *dirty_ranges,
				   unsigned int nr_dirty_ranges)
{
	unsigned int r;
	unsigned long discarded = 0;

	if (!cow_buffer.initialized || !dirty_ranges || nr_dirty_ranges == 0)
		return;

	/* Iterate dirty ranges and do O(1) hash lookups with fine-grained locks */
	for (r = 0; r < nr_dirty_ranges; r++) {
		unsigned long start = dirty_ranges[r * 2];
		unsigned long len = dirty_ranges[r * 2 + 1];
		unsigned long vaddr;

		/* Iterate each page in this dirty range */
		for (vaddr = start; vaddr < start + len; vaddr += PAGE_SIZE) {
			unsigned int hash = page_buffer_hash(vaddr);
			int lock_idx = lock_index(hash);
			struct page_buffer_node *node;
			struct hlist_node *tmp;
			int i;
			bool found = false;

			pthread_spin_lock(&hash_locks[lock_idx]);
			hlist_for_each_entry_safe(node, tmp,
						  &cow_buffer.hash_table[hash], hash) {
				for (i = 0; i < node->count; i++) {
					if (node->entries[i].vaddr == vaddr) {
						page_pool_put(node->entries[i].data);
						/* Move last entry to fill gap */
						node->count--;
						if (i < node->count) {
							node->entries[i] = node->entries[node->count];
						}
						/* Remove empty nodes */
						if (node->count == 0) {
							hlist_del(&node->hash);
							xfree(node);
						}
						found = true;
						break;
					}
				}
				if (found)
					break;
			}
			pthread_spin_unlock(&hash_locks[lock_idx]);

			if (found) {
				__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);
				__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
				discarded++;
				page_state_set(vaddr, PAGE_STATE_DIRTY);
			}
		}
	}

	pr_info("Discarded %lu dirty pages from buffer\n", discarded);
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

	/* Lock all buckets and destroy contents */
	for (i = 0; i < PAGE_BUFFER_HASH_SIZE; i++) {
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

	xfree(cow_buffer.hash_table);
	cow_buffer.hash_table = NULL;
	cow_buffer.initialized = false;

	/* Destroy all fine-grained locks */
	for (i = 0; i < NUM_HASH_LOCKS; i++)
		pthread_spin_destroy(&hash_locks[i]);
	pthread_spin_destroy(&counter_lock);

	pr_info("COW page buffer destroyed: applied=%lu discarded=%lu max_bucket=%lu\n",
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
						hlist_del(&node->hash);
						xfree(node);
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

	pthread_spin_lock(&hash_locks[lock_idx]);
	hlist_add_head(&node->hash, &cow_buffer.hash_table[hash]);
	pthread_spin_unlock(&hash_locks[lock_idx]);

	__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
	page_state_set(vaddr, PAGE_STATE_EAGAIN_QUEUED);
	pr_debug("COW_TRACE DRAIN_READD: 0x%lx re-buffered for EAGAIN retry\n", vaddr);
}

/*
 * Background drain worker thread - proactively UFFDIO_COPY pages
 * from buffer to reduce future page faults and free memory.
 *
 * Each worker handles a subset of hash buckets for parallel draining.
 * Thread safety is ensured by:
 *   - Fine-grained hash bucket locks (hash_locks[])
 *   - Atomic counters for shared statistics
 *   - Thread-safe page_state and pf_tracker APIs
 */
static void *background_drain_worker(void *arg)
{
	struct drain_thread_args *args = (struct drain_thread_args *)arg;
	struct page_buffer_node *node;
	struct hlist_node *tmp;
	int bucket;
	unsigned long drained = 0;
	int thread_id = args->thread_id;
	int start_bucket = args->start_bucket;
	int end_bucket = args->end_bucket;
	char thread_name[16];

	/* Set thread name for debugging (max 15 chars + null) */
	snprintf(thread_name, sizeof(thread_name), "cow-drain-%d", thread_id);
	pthread_setname_np(pthread_self(), thread_name);

	pr_info("Drain worker %d started: buckets [%d, %d) (%lu pages buffered)\n",
		thread_id, start_bucket, end_bucket, cow_buffer.nr_pages);

	while (!atomic_load(&drain_thread_stop) && cow_buffer.nr_pages > 0) {
		bool made_progress = false;

		for (bucket = start_bucket;
		     bucket < end_bucket && !atomic_load(&drain_thread_stop);
		     bucket++) {
			int lock_idx = lock_index(bucket);

			pthread_spin_lock(&hash_locks[lock_idx]);
			hlist_for_each_entry_safe(node, tmp,
						  &cow_buffer.hash_table[bucket], hash) {
				/* Process all entries in this node */
				while (node->count > 0 && !atomic_load(&drain_thread_stop)) {
					/* Take last entry (avoids moving data) */
					int idx = node->count - 1;
					unsigned long vaddr = node->entries[idx].vaddr;
					void *data = node->entries[idx].data;
					int uffd;
					bool free_data = true;

					node->count--;
					made_progress = true;

					/* Track: removed from buffer, about to copy */
					page_state_set(vaddr, PAGE_STATE_DRAIN_PENDING);

					pr_debug("COW_TRACE DRAIN[%d]: 0x%lx (remaining=%lu)\n",
						 thread_id, vaddr, cow_buffer.nr_pages);

					pthread_spin_unlock(&hash_locks[lock_idx]);
					__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);

					/* Find uffd for this address and copy */
					uffd = cow_get_uffd_for_vaddr(drain_lpis, vaddr);
					if (uffd >= 0) {
						bool data_owned = false;
						int ret;

						ret = cow_uffd_copy_and_track(uffd, vaddr, data, 1,
									     NULL, drain_lpis,
									     COW_TRACK_STRICT,
									     "DRAIN", &data_owned);
						if (ret > 0)
							drained++;
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
					pthread_spin_lock(&hash_locks[lock_idx]);
				}

				/* Remove empty node */
				if (node->count == 0) {
					hlist_del(&node->hash);
					xfree(node);
				}
			}
			pthread_spin_unlock(&hash_locks[lock_idx]);
		}

		/*
		 * If no progress made in this pass and buffer not empty,
		 * yield to let other threads or page receivers work.
		 * This prevents busy-spinning when pages are being added
		 * to buckets outside our range.
		 */
		if (!made_progress && cow_buffer.nr_pages > 0) {
			usleep(100);  /* 100us yield */
		}
	}

	/* Update global statistics */
	atomic_fetch_add(&total_drained, drained);

	pr_info("Drain worker %d done: %lu drained\n", thread_id, drained);

	/* Decrement active thread count */
	if (atomic_fetch_sub(&drain_threads_active, 1) == 1) {
		/* Last thread to exit - log final stats */
		pr_info("All drain workers done: total=%lu applied=%lu discarded=%lu eagain=%lu\n",
			atomic_load(&total_drained), cow_buffer.nr_applied,
			cow_buffer.nr_discarded, cow_buffer.nr_eagain);
	}

	return NULL;
}

int cow_start_drain_thread(struct list_head *lpis)
{
	int i;
	int buckets_per_thread;
	int created = 0;

	if (atomic_load(&drain_threads_active) > 0)
		return 0;

	if (cow_buffer.nr_pages == 0)
		return 0;

	drain_lpis = lpis;  /* Store for EAGAIN handling */
	atomic_store(&drain_thread_stop, false);
	atomic_store(&total_drained, 0);

	/* Divide buckets evenly among threads */
	buckets_per_thread = PAGE_BUFFER_HASH_SIZE / NUM_DRAIN_THREADS;

	for (i = 0; i < NUM_DRAIN_THREADS; i++) {
		drain_args[i].thread_id = i;
		drain_args[i].start_bucket = i * buckets_per_thread;
		drain_args[i].end_bucket = (i == NUM_DRAIN_THREADS - 1)
			? PAGE_BUFFER_HASH_SIZE
			: (i + 1) * buckets_per_thread;

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

	pr_info("Started %d/%d drain threads (%lu pages buffered)\n",
		created, NUM_DRAIN_THREADS, cow_buffer.nr_pages);

	return 0;
}

void cow_stop_drain_thread(void)
{
	int i;

	if (atomic_load(&drain_threads_active) == 0)
		return;

	atomic_store(&drain_thread_stop, true);

	/* Join all threads */
	for (i = 0; i < NUM_DRAIN_THREADS; i++) {
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
static bool cow_dirty_bitmap_received = false;
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

/* Check if dirty bitmap has been received from primary */
bool cow_is_dirty_bitmap_received(void)
{
	return cow_dirty_bitmap_received;
}

/* Set dirty bitmap received flag */
void cow_set_dirty_bitmap_received(bool received)
{
	cow_dirty_bitmap_received = received;
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
	static atomic_ulong call_count = 0;
	struct lazy_pages_info *lpi;
	pthread_t self = pthread_self();
	unsigned long count = atomic_fetch_add(&call_count, 1);
	bool should_log = (count % 1000 == 0);  /* Log every 1000 calls */

	if (should_log)
		pr_warn("RACE_DEBUG: [%lu] cow_get_uffd_for_vaddr ENTER #%lu vaddr=0x%lx\n",
			(unsigned long)self, count, vaddr);

	list_for_each_entry(lpi, lpis, l) {
		if (should_log)
			pr_warn("RACE_DEBUG: [%lu] checking lpi=%p exited=%d fd=%d\n",
				(unsigned long)self, lpi, lpi->exited, lpi->lpfd.fd);
		if (lpi->exited || lpi->lpfd.fd < 0)
			continue;
		if (cow_find_iov(lpi, vaddr)) {
			if (should_log)
				pr_warn("RACE_DEBUG: [%lu] cow_get_uffd_for_vaddr EXIT fd=%d\n",
					(unsigned long)self, lpi->lpfd.fd);
			return lpi->lpfd.fd;
		}
	}
	if (should_log)
		pr_warn("RACE_DEBUG: [%lu] cow_get_uffd_for_vaddr EXIT fd=-1\n",
			(unsigned long)self);
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

/* Pending dirty bitmap — stored if it arrives before restore connects */
static unsigned long *pending_dirty_ranges = NULL;
static unsigned int pending_nr_dirty_ranges = 0;

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

void cow_cleanup_prebuffer(void)
{
	if (prebuffer_buf) {
		xfree(prebuffer_buf);
		prebuffer_buf = NULL;
	}

	if (pending_dirty_ranges) {
		xfree(pending_dirty_ranges);
		pending_dirty_ranges = NULL;
	}
	pending_nr_dirty_ranges = 0;
}

void cow_store_pending_dirty_ranges(unsigned long *ranges, unsigned int nr)
{
	pending_dirty_ranges = ranges;
	pending_nr_dirty_ranges = nr;
}

unsigned long *cow_get_pending_dirty_ranges(unsigned int *nr)
{
	unsigned long *ranges = pending_dirty_ranges;
	*nr = pending_nr_dirty_ranges;
	pending_dirty_ranges = NULL;
	pending_nr_dirty_ranges = 0;
	return ranges;
}

/*
 * Switch async reader to convergence mode.
 * Called when BOTH restore is connected AND dirty bitmap is received.
 */
void cow_switch_to_convergence_callback(void)
{
	if (!prebuffer_buf) {
		pr_warn("Cannot switch to convergence: no prebuffer_buf\n");
		return;
	}

	/* The actual callback will be set by uffd.c using convergence_io_complete wrapper */
	pr_info("Switching to convergence callback mode\n");
}

/*
 * Create IOVs for dirty ranges that don't have existing IOVs.
 * This handles new VMAs created between Phase 1 and Phase 3.
 */
int cow_create_iovs_for_new_ranges(struct list_head *lpis,
				   unsigned long *dirty_ranges,
				   unsigned int nr_dirty_ranges)
{
	struct lazy_pages_info *lpi;
	unsigned int i;
	int created = 0;

	if (!dirty_ranges || nr_dirty_ranges == 0)
		return 0;

	/* Process each dirty range */
	for (i = 0; i < nr_dirty_ranges; i++) {
		unsigned long start = dirty_ranges[i * 2];
		unsigned long len = dirty_ranges[i * 2 + 1];
		unsigned long end = start + len;
		bool fully_covered = false;

		/*
		 * Check if any lpi has IOVs fully covering this range.
		 * New VMAs shouldn't overlap with existing IOVs since they
		 * represent memory that didn't exist in Phase 1.
		 */
		list_for_each_entry(lpi, lpis, l) {
			struct lazy_iov *iov_start, *iov_end;

			if (lpi->exited)
				continue;

			iov_start = cow_find_iov(lpi, start);
			iov_end = cow_find_iov(lpi, end - 1);

			if (iov_start && iov_end) {
				fully_covered = true;
				break;
			}
		}

		if (!fully_covered) {
			/*
			 * Range not fully covered - likely a new VMA.
			 * Add IOV to the first active lpi.
			 */
			list_for_each_entry(lpi, lpis, l) {
				struct lazy_iov *iov;

				if (lpi->exited)
					continue;

				iov = xzalloc(sizeof(*iov));
				if (!iov) {
					pr_err("Failed to allocate IOV for new range\n");
					return -1;
				}

				iov->start = start;
				iov->end = end;
				iov->img_start = start;
				iov->is_new_vma = true;
				list_add_tail(&iov->l, &lpi->iovs);

				pr_info("Created IOV for new VMA: 0x%lx-0x%lx (%lu pages)\n",
					start, end, len / PAGE_SIZE);
				created++;
				break;
			}
		}
	}

	if (created > 0)
		pr_info("Created %d IOVs for new VMA ranges\n", created);

	return 0;
}

/*
 * Process dirty bitmap - create IOVs and enter convergence mode.
 * Called when dirty bitmap is received from primary.
 */
void cow_process_dirty_bitmap(struct list_head *lpis,
			      unsigned long *dirty_ranges,
			      unsigned int nr_dirty_ranges)
{
	pr_info("Processing dirty bitmap (%u ranges)\n", nr_dirty_ranges);

	cow_set_dirty_bitmap_received(true);

	if (cow_is_restore_connected()) {
		/* Restore already connected - process now */
		if (cow_create_iovs_for_new_ranges(lpis, dirty_ranges, nr_dirty_ranges) < 0)
			pr_warn("Failed to create IOVs for some new ranges\n");

		xfree(dirty_ranges);

		pr_info("Entering convergence mode\n");
		cow_switch_to_convergence_callback();
		cow_start_drain_thread(lpis);
	} else {
		/* Store for later processing when restore connects */
		pr_info("Storing dirty ranges for later (%u ranges)\n", nr_dirty_ranges);
		cow_store_pending_dirty_ranges(dirty_ranges, nr_dirty_ranges);
	}
}

/*
 * COW mode initialization for cr_lazy_pages.
 * Initializes page buffer, trackers, etc.
 */
int cow_lazy_pages_init(void)
{
	if (cow_page_buffer_init() < 0) {
		pr_err("Failed to initialize page buffer\n");
		return -1;
	}

	if (page_state_init())
		pr_warn("Failed to initialize page state tracker (non-fatal)\n");

	if (unmapped_tracker_init())
		pr_warn("Failed to initialize unmapped tracker (non-fatal)\n");

	if (pf_tracker_init())
		pr_warn("Failed to init hung page tracker (non-fatal)\n");

	return 0;
}

/*
 * COW mode cleanup for cr_lazy_pages.
 */
void cow_lazy_pages_cleanup(void)
{
	cow_page_buffer_destroy();
	pf_tracker_destroy();
	page_state_verify_all_terminal();
	page_state_destroy();
	unmapped_tracker_destroy();
	cow_cleanup_prebuffer();
}

/*
 * Full COW page fault handler - consolidates all COW-specific logic.
 * Returns: 0 on success, <0 on error
 *
 * This function handles:
 *   1. Try to serve from buffer
 *   2. Check if all pages sent -> zero fill
 *   3. Call COW mode handler for server requests
 *   4. Handle return codes with appropriate uffd operations
 */
int cow_handle_page_fault_full(struct lazy_pages_info *lpi,
			       unsigned long long address,
			       int (*do_zero)(struct lazy_pages_info *, __u64, unsigned long),
			       int (*do_handle_pages)(struct lazy_pages_info *, __u64, unsigned long, unsigned))
{
	unsigned long long img_addr = 0;
	int ret;

	/* Try to serve from buffer first */
	ret = cow_handle_page_fault_buffer(lpi, address);
	if (ret < 0)
		return ret;
	if (ret > 0)
		return 0;  /* Page served from buffer */

	/* Page not in buffer - check if all pages sent */
	if (cow_is_all_pages_sent_received()) {
		lp_debug(lpi, "Page 0x%llx not in buffer, all pages sent - zero-filling\n", address);
		page_state_set(address, PAGE_STATE_PF_PENDING);
		return do_zero(lpi, address, 1);
	}

	/* Handle COW-specific page fault logic */
	ret = cow_handle_page_fault_cow_mode(lpi, address, &img_addr);
	if (ret < 0)
		return ret;
	if (ret == 0)
		return 0;  /* Handled or waiting */
	if (ret == COW_PF_ZERO_FILL)
		return do_zero(lpi, address, 1);
	if (ret == COW_PF_HANDLE_PAGES) {
		/* PR_ASYNC | PR_ASAP = 0x3 */
		ret = do_handle_pages(lpi, img_addr, 1, 0x3);
		if (ret < 0) {
			lp_err(lpi, "Error during COW page fault request\n");
			return -1;
		}
		return 0;
	}

	return 0;
}

/*
 * Try to serve a page fault from the COW buffer.
 * Returns:
 *   1 - page found and copied (or handled)
 *   0 - page not in buffer
 *  <0 - error
 *
 * This extracts the buffer lookup and UFFD_COPY logic from handle_page_fault.
 */
int cow_handle_page_fault_buffer(struct lazy_pages_info *lpi,
				 unsigned long long address)
{
	void *data;
	int ret;

	data = cow_page_buffer_lookup_and_remove(address);

	pr_debug("COW_TRACE PF_LOOKUP: 0x%llx found=%s\n", address, data ? "YES" : "NO");

	if (!data) {
		/*
		 * Page not in buffer. If all pages have been sent,
		 * caller should zero-fill. Otherwise request from server.
		 */
		return 0;
	}

	page_state_set(address, PAGE_STATE_PF_PENDING);

	ret = cow_uffd_copy_and_track(lpi->lpfd.fd, address, data, 1,
				      lpi, NULL, 0, "PF_BUFFER", NULL);

	page_pool_put(data);

	/* Return 1 for success or soft-handled, -1 for error */
	return ret >= 0 ? 1 : -1;
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
 * Handle page fault in COW mode (called when opts.cow_dump is true).
 * This handles the COW-specific logic:
 *   - Check if all pages sent -> zero-fill
 *   - Check server availability
 *   - Handle new VMAs
 *   - Request from page server (Phase 3 vs non-Phase 3)
 *
 * Returns:
 *   0 - success (page requested or waiting for drain)
 *  -1 - error
 *   COW_PF_NOT_HANDLED - caller should continue with normal path
 */
#define COW_PF_NOT_HANDLED 2

int cow_handle_page_fault(struct lazy_pages_info *lpi,
			  unsigned long long address)
{
	struct lazy_iov *iov;

	/* Check if all pages have been sent */
	if (cow_is_all_pages_sent_received()) {
		lp_debug(lpi, "Page 0x%llx not in buffer, all pages sent - zero-filling\n", address);
		page_state_set(address, PAGE_STATE_PF_PENDING);
		return COW_PF_NOT_HANDLED;  /* Caller will call uffd_zero */
	}

	/* Check if server is available */
	if (get_page_server_sk() < 0) {
		lp_debug(lpi, "Page 0x%llx server unavailable - waiting for drain\n", address);
		return 0;
	}

	iov = cow_find_iov(lpi, address);

	if (!iov) {
		if (cow_is_all_pages_sent_received()) {
			lp_debug(lpi, "Page 0x%llx IOV not found - zero fill\n", address);
			return COW_PF_NOT_HANDLED;  /* Caller will call uffd_zero */
		}
		lp_debug(lpi, "Page 0x%llx IOV not found - waiting for drain\n", address);
		return 0;
	}

	/* New VMAs from Phase 3 - request directly from server */
	if (iov->is_new_vma) {
		lp_debug(lpi, "Page 0x%llx in new VMA - requesting from server\n", address);
		cow_uffd_stats_inc_pf(1);
		pf_tracker_add(address, 1, lpi->pid, true);
		if (request_remote_pages(lpi->pr.img_id, address, 1) < 0) {
			lp_err(lpi, "Error requesting new VMA page 0x%llx\n", address);
			return -1;
		}
		return 0;
	}

	cow_uffd_stats_inc_pf(1);
	pf_tracker_add(address, 1, lpi->pid, true);

	if (cow_is_phase3_active()) {
		/* In Phase 3, pages arrive via convergence stream */
		if (request_remote_pages(lpi->pr.img_id, address, 1) < 0) {
			lp_err(lpi, "Error requesting page 0x%llx in Phase 3\n", address);
			return -1;
		}
	} else {
		/* Pre-Phase 3: request via page reader */
		return COW_PF_NOT_HANDLED;  /* Caller will use uffd_handle_pages */
	}

	return 0;
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
 * COW-specific page fault handling (full flow).
 * Returns:
 *   0 - success (page handled or waiting)
 *  -1 - error
 *   COW_PF_ZERO_FILL (2) - caller should zero-fill the page
 *   COW_PF_HANDLE_PAGES (3) - caller should call uffd_handle_pages with img_addr_out
 */
int cow_handle_page_fault_cow_mode(struct lazy_pages_info *lpi,
				   unsigned long long address,
				   unsigned long long *img_addr_out)
{
	struct lazy_iov *iov;

	/* Check if server is available for convergence requests */
	if (get_page_server_sk() < 0) {
		/*
		 * In COW mode, don't zero-fill - the correct data should
		 * arrive via the drain thread. Return 0 to let the process
		 * retry the fault when drain thread fills the page.
		 */
		lp_debug(lpi, "Page 0x%llx server unavailable - waiting for drain\n", address);
		return 0;
	}

	iov = cow_find_iov(lpi, address);

	if (!iov) {
		/*
		 * IOV not found. If all_pages_sent received, all pages should
		 * have been transferred - zero-fill this page. Otherwise
		 * wait for drain thread to fill it.
		 */
		if (cow_is_all_pages_sent_received()) {
			lp_debug(lpi, "Page 0x%llx IOV not found - zero fill\n", address);
			return 2;  /* COW_PF_ZERO_FILL */
		}
		lp_debug(lpi, "Page 0x%llx IOV not found - waiting for drain\n", address);
		return 0;
	}

	/*
	 * New VMAs don't have pagemap entries - they didn't exist in Phase 1.
	 * Request page directly from page server.
	 */
	if (iov->is_new_vma) {
		lp_debug(lpi, "Page 0x%llx in new VMA - requesting from server\n", address);
		cow_uffd_stats_inc_pf(1);
		pf_tracker_add(address, 1, lpi->pid, true);
		if (request_remote_pages(lpi->pr.img_id, address, 1) < 0) {
			lp_err(lpi, "Error requesting new VMA page 0x%llx\n", address);
			return -1;
		}
		return 0;
	}

	*img_addr_out = iov->img_start + (address - iov->start);

	cow_uffd_stats_inc_pf(1);
	pf_tracker_add(address, 1, lpi->pid, true);

	if (cow_is_phase3_active()) {
		/* In Phase 3, pages arrive via convergence stream */
		if (request_remote_pages(lpi->pr.img_id, address, 1) < 0) {
			lp_err(lpi, "Error requesting page 0x%llx in Phase 3\n", address);
			return -1;
		}
		return 0;
	}

	/* Caller should call uffd_handle_pages with img_addr_out */
	return 3;  /* COW_PF_HANDLE_PAGES */
}

/*
 * COW bulk IO complete callback.
 * Called when a bulk page read completes in COW mode (without page server Phase 2/3).
 *
 * NOTE: In COW Phase 2/3 mode (opts.cow_dump && opts.use_page_server),
 * pages flow through prebuffer_io_complete() or convergence_io_complete() instead.
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
 * Set dirty bitmap received and process.
 * Called from uffd.c when dirty bitmap is received.
 */
void cow_set_dirty_bitmap_received_and_process(struct list_head *lpis,
					       unsigned long *dirty_ranges,
					       unsigned int nr_dirty_ranges,
					       void (*switch_to_convergence)(void))
{
	pr_info("Dirty bitmap received from primary (%u ranges)\n", nr_dirty_ranges);

	cow_set_dirty_bitmap_received(true);

	/*
	 * If restore is already connected, create IOVs for new VMAs now.
	 * Otherwise store dirty_ranges for later processing.
	 */
	if (cow_is_restore_connected()) {
		if (cow_create_iovs_for_new_ranges(lpis, dirty_ranges, nr_dirty_ranges) < 0)
			pr_warn("Failed to create IOVs for some new ranges\n");

		xfree(dirty_ranges);

		pr_info("Restore already connected, entering convergence\n");
		if (switch_to_convergence)
			switch_to_convergence();
		cow_start_drain_thread(lpis);
	} else {
		pr_info("Storing dirty ranges for later (%u ranges)\n", nr_dirty_ranges);
		cow_store_pending_dirty_ranges(dirty_ranges, nr_dirty_ranges);
	}
}

/*
 * COW post-connect initialization in handle_lazy_accept.
 * Called after restore connects to handle dirty bitmap processing
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

/*
 * COW Phase 3 page request after restore connects.
 * Requests all pages for all alive tasks.
 *
 * Returns: 0 on success, -1 on error
 */
int cow_phase3_request_all_pages(void)
{
	struct pstree_item *pi;

	if (!cow_is_phase3_active())
		return 0;

	/*
	 * Skip page requests if all pages have been sent - no page server
	 * connection exists and all pages are already in the buffer.
	 */
	if (cow_is_all_pages_sent_received()) {
		pr_info("All pages already buffered, skipping page requests\n");
		return 0;
	}

	for_each_pstree_item(pi) {
		if (task_alive(pi)) {
			pr_info("Requesting all remote pages for pid=%d\n", vpid(pi));
			if (request_all_remote_pages(vpid(pi)) < 0) {
				pr_err("Failed to request pages for pid=%d\n", vpid(pi));
				return -1;
			}
		}
	}

	return 0;
}

