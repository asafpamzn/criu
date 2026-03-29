#include <stdbool.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>
#include <linux/userfaultfd.h>

#include "int.h"
#include "page.h"
#include "cow-uffd.h"
#include "uffd.h"
#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"
#include "common/bug.h"
#include "pf-tracker.h"
#include "page-pool.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-uffd: "

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

static pthread_t drain_thread;
static volatile bool drain_thread_stop = false;
static volatile bool drain_thread_active = false;

static inline unsigned int page_buffer_hash(unsigned long vaddr)
{
	return (vaddr >> PAGE_SHIFT) & (PAGE_BUFFER_HASH_SIZE - 1);
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
	/* PAGE_STATE_DIRTY is OK - page being re-sent with newer data */

	hash = page_buffer_hash(vaddr);
	lock_idx = lock_index(hash);

	if (nocopy) {
		/* Take ownership of data pointer directly (from page_pool_get_chunk) */
		page_data = data;
	} else {
		/*
		 * Allocate page data outside lock using per-thread pool.
		 * All callers must have a valid thread_id with initialized pool.
		 */
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

	pthread_spin_lock(&hash_locks[lock_idx]);
	pr_err("DEBUG: cow_page_buffer_add existing vaddr=0x%lx nocopy=%d thread_id=%d\n", vaddr, nocopy, thread_id);
	/* Check for duplicate and find space in existing nodes (cache-friendly) */
	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		/* Check all entries in this node - contiguous in memory */
		for (i = 0; i < node->count; i++) {
			if (node->entries[i].vaddr == vaddr) {
				/* Update existing entry with newer data */
				pr_err("DEBUG: cow_page_buffer_add REPLACING existing entry vaddr=0x%lx nocopy=%d\n", vaddr, nocopy);
				page_pool_put(node->entries[i].data);
				node->entries[i].data = page_data;
				pthread_spin_unlock(&hash_locks[lock_idx]);
				return 0;
			}
		}
		/* If this node has space, add here */
		if (node->count < PAGE_NODE_ENTRIES) {
			node->entries[node->count].vaddr = vaddr;
			node->entries[node->count].data = page_data;
			node->count++;
			pthread_spin_unlock(&hash_locks[lock_idx]);
			__sync_fetch_and_add(&cow_buffer.nr_pages, 1);
			page_state_set(vaddr, PAGE_STATE_IN_BUFFER);
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
	pthread_spin_unlock(&hash_locks[lock_idx]);

	__sync_fetch_and_add(&cow_buffer.nr_pages, 1);

	/* Track page state: now in buffer */
	page_state_set(vaddr, PAGE_STATE_IN_BUFFER);

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
 * Re-add a page to the buffer for EAGAIN retry.
 * Called when UFFDIO_COPY fails with EAGAIN.
 */
static void cow_page_buffer_readd(unsigned long vaddr, void *data)
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
 * Background drain thread - proactively UFFDIO_COPY pages
 * from buffer to reduce future page faults and free memory.
 */
static void *background_drain_thread(void *arg)
{
	struct page_buffer_node *node;
	struct hlist_node *tmp;
	int bucket;
	unsigned long drained = 0;

	pr_info("Background drain thread started (%lu pages buffered)\n",
		cow_buffer.nr_pages);

	while (!drain_thread_stop && cow_buffer.nr_pages > 0) {
		for (bucket = 0; bucket < PAGE_BUFFER_HASH_SIZE && !drain_thread_stop; bucket++) {
			int lock_idx = lock_index(bucket);

			pthread_spin_lock(&hash_locks[lock_idx]);
			hlist_for_each_entry_safe(node, tmp,
						  &cow_buffer.hash_table[bucket], hash) {
				/* Process all entries in this node */
				while (node->count > 0 && !drain_thread_stop) {
					/* Take last entry (avoids moving data) */
					int idx = node->count - 1;
					unsigned long vaddr = node->entries[idx].vaddr;
					void *data = node->entries[idx].data;
					int uffd;
					bool free_data = true;

					node->count--;

					/* Track: removed from buffer, about to copy */
					page_state_set(vaddr, PAGE_STATE_DRAIN_PENDING);

					pr_debug("COW_TRACE DRAIN_REMOVE: 0x%lx (remaining=%lu)\n", vaddr, cow_buffer.nr_pages);

					pthread_spin_unlock(&hash_locks[lock_idx]);
					__sync_fetch_and_sub(&cow_buffer.nr_pages, 1);

					/* Find uffd for this address and UFFDIO_COPY */
					uffd = get_uffd_for_vaddr(vaddr);
					if (uffd >= 0) {
						struct uffdio_copy uffd_copy = {
							.dst = vaddr,
							.src = (unsigned long)data,
							.len = PAGE_SIZE,
							.mode = 0,
							.copy = 0,
						};

						if (ioctl(uffd, UFFDIO_COPY, &uffd_copy) < 0) {
							if (errno == EEXIST) {
								__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
								pr_err("BUG: DRAIN_COPY EEXIST at 0x%lx - duplicate copy!\n", vaddr);
								page_state_print_history(vaddr);
								if (page_state_get(vaddr) != PAGE_STATE_DIRTY)
									page_state_set(vaddr, PAGE_STATE_DISCARDED);
							} else if (errno == ENOENT) {
								__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
								pr_err("COW_TRACE DRAIN_COPY: 0x%lx FAILED errno=ENOENT (VMA unmapped)\n", vaddr);
								page_state_print_history(vaddr);
								if (page_state_get(vaddr) != PAGE_STATE_DIRTY)
									page_state_set(vaddr, PAGE_STATE_DISCARDED);
							} else if (errno == EAGAIN) {
								__sync_fetch_and_add(&cow_buffer.nr_eagain, 1);
								cow_page_buffer_readd(vaddr, data);
								free_data = false;
								pr_debug("COW_TRACE DRAIN_COPY: 0x%lx EAGAIN, re-buffered\n", vaddr);
							} else {
								pr_err("COW_TRACE DRAIN_COPY: 0x%lx FAILED errno=%d\n", vaddr, errno);
								page_state_print_history(vaddr);
								if (page_state_get(vaddr) != PAGE_STATE_DIRTY)
									page_state_set(vaddr, PAGE_STATE_DISCARDED);
							}
						} else {
							__sync_fetch_and_add(&cow_buffer.nr_applied, 1);
							page_state_set(vaddr, PAGE_STATE_COPIED);
							drained++;
						}
					} else {
						__sync_fetch_and_add(&cow_buffer.nr_discarded, 1);
						pr_err("COW_TRACE DRAIN_COPY: 0x%lx no uffd found\n", vaddr);
						page_state_print_history(vaddr);
						if (page_state_get(vaddr) != PAGE_STATE_DIRTY)
							page_state_set(vaddr, PAGE_STATE_DISCARDED);
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
	}

	pr_info("Drain thread done: %lu drained, %lu applied, %lu discarded, %lu eagain\n",
		drained, cow_buffer.nr_applied, cow_buffer.nr_discarded,
		cow_buffer.nr_eagain);

	drain_thread_active = false;
	return NULL;
}

int cow_start_drain_thread(void)
{
	if (drain_thread_active)
		return 0;

	if (cow_buffer.nr_pages == 0)
		return 0;

	drain_thread_stop = false;
	drain_thread_active = true;

	if (pthread_create(&drain_thread, NULL, background_drain_thread, NULL)) {
		pr_perror("Failed to create drain thread");
		drain_thread_active = false;
		return -1;
	}

	return 0;
}

void cow_stop_drain_thread(void)
{
	if (!drain_thread_active)
		return;

	drain_thread_stop = true;
	pthread_join(drain_thread, NULL);
	drain_thread_active = false;
}

bool cow_drain_thread_running(void)
{
	return drain_thread_active;
}
