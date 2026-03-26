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

#undef LOG_PREFIX
#define LOG_PREFIX "cow-uffd: "

#define PAGE_BUFFER_HASH_BITS 20
#define PAGE_BUFFER_HASH_SIZE (1 << PAGE_BUFFER_HASH_BITS)  /* 1M buckets */

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
	pthread_spinlock_t lock;
	bool initialized;
} cow_buffer = { .initialized = false };

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

	pthread_spin_init(&cow_buffer.lock, PTHREAD_PROCESS_PRIVATE);
	cow_buffer.nr_pages = 0;
	cow_buffer.nr_applied = 0;
	cow_buffer.nr_discarded = 0;
	cow_buffer.nr_eagain = 0;
	cow_buffer.max_bucket_depth = 0;
	cow_buffer.initialized = true;

	pr_info("COW page buffer initialized (buckets=%d)\n", PAGE_BUFFER_HASH_SIZE);
	return 0;
}

int cow_page_buffer_add(unsigned long vaddr, void *data)
{
	struct page_buffer_node *node;
	unsigned int hash;
	enum page_state state;
	void *page_data;
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

	/* Allocate page data outside lock */
	page_data = xmalloc(PAGE_SIZE);
	if (!page_data)
		return -1;
	memcpy(page_data, data, PAGE_SIZE);

	pthread_spin_lock(&cow_buffer.lock);

	/* Check for duplicate and find space in existing nodes (cache-friendly) */
	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		/* Check all entries in this node - contiguous in memory */
		for (i = 0; i < node->count; i++) {
			if (node->entries[i].vaddr == vaddr) {
				/* Update existing entry with newer data */
				xfree(node->entries[i].data);
				node->entries[i].data = page_data;
				pthread_spin_unlock(&cow_buffer.lock);
				return 0;
			}
		}
		/* If this node has space, add here */
		if (node->count < PAGE_NODE_ENTRIES) {
			node->entries[node->count].vaddr = vaddr;
			node->entries[node->count].data = page_data;
			node->count++;
			cow_buffer.nr_pages++;
			pthread_spin_unlock(&cow_buffer.lock);
			page_state_set(vaddr, PAGE_STATE_IN_BUFFER);
			return 0;
		}
	}

	pthread_spin_unlock(&cow_buffer.lock);

	/* Need new node - allocate outside lock */
	node = xmalloc(sizeof(*node));
	if (!node) {
		xfree(page_data);
		return -1;
	}

	node->entries[0].vaddr = vaddr;
	node->entries[0].data = page_data;
	node->count = 1;
	INIT_HLIST_NODE(&node->hash);

	pthread_spin_lock(&cow_buffer.lock);
	hlist_add_head(&node->hash, &cow_buffer.hash_table[hash]);
	cow_buffer.nr_pages++;
	pthread_spin_unlock(&cow_buffer.lock);

	/* Track page state: now in buffer */
	page_state_set(vaddr, PAGE_STATE_IN_BUFFER);

	pr_debug("COW_TRACE ADD: 0x%lx (total=%lu)\n", vaddr, cow_buffer.nr_pages);

	return 0;
}

void *cow_page_buffer_lookup_and_remove(unsigned long vaddr)
{
	struct page_buffer_node *node;
	unsigned int hash;
	void *data = NULL;
	int i;

	if (!cow_buffer.initialized)
		return NULL;

	hash = page_buffer_hash(vaddr);

	pthread_spin_lock(&cow_buffer.lock);
	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		for (i = 0; i < node->count; i++) {
			if (node->entries[i].vaddr == vaddr) {
				data = node->entries[i].data;
				/* Move last entry to fill gap */
				node->count--;
				if (i < node->count) {
					node->entries[i] = node->entries[node->count];
				}
				cow_buffer.nr_pages--;
				/* Remove empty nodes */
				if (node->count == 0) {
					hlist_del(&node->hash);
					xfree(node);
				}
				pthread_spin_unlock(&cow_buffer.lock);
				return data;
			}
		}
	}
	pthread_spin_unlock(&cow_buffer.lock);

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

	pthread_spin_lock(&cow_buffer.lock);

	/* Iterate dirty ranges and do O(1) hash lookups */
	for (r = 0; r < nr_dirty_ranges; r++) {
		unsigned long start = dirty_ranges[r * 2];
		unsigned long len = dirty_ranges[r * 2 + 1];
		unsigned long vaddr;

		/* Iterate each page in this dirty range */
		for (vaddr = start; vaddr < start + len; vaddr += PAGE_SIZE) {
			unsigned int hash = page_buffer_hash(vaddr);
			struct page_buffer_node *node;
			struct hlist_node *tmp;
			int i;
			bool found = false;

			hlist_for_each_entry_safe(node, tmp,
						  &cow_buffer.hash_table[hash], hash) {
				for (i = 0; i < node->count; i++) {
					if (node->entries[i].vaddr == vaddr) {
						xfree(node->entries[i].data);
						/* Move last entry to fill gap */
						node->count--;
						if (i < node->count) {
							node->entries[i] = node->entries[node->count];
						}
						cow_buffer.nr_pages--;
						cow_buffer.nr_discarded++;
						discarded++;
						page_state_set(vaddr, PAGE_STATE_DIRTY);
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
		}
	}

	pthread_spin_unlock(&cow_buffer.lock);

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

	pthread_spin_lock(&cow_buffer.lock);

	for (i = 0; i < PAGE_BUFFER_HASH_SIZE; i++) {
		hlist_for_each_entry_safe(node, tmp,
					  &cow_buffer.hash_table[i], hash) {
			for (j = 0; j < node->count; j++)
				xfree(node->entries[j].data);
			hlist_del(&node->hash);
			xfree(node);
		}
	}

	pthread_spin_unlock(&cow_buffer.lock);

	xfree(cow_buffer.hash_table);
	cow_buffer.hash_table = NULL;
	cow_buffer.initialized = false;

	pthread_spin_destroy(&cow_buffer.lock);

	pr_info("COW page buffer destroyed: applied=%lu discarded=%lu max_bucket=%lu\n",
		cow_buffer.nr_applied, cow_buffer.nr_discarded,
		cow_buffer.max_bucket_depth);
}

/*
 * Re-add a page to the buffer for EAGAIN retry.
 * Called when UFFDIO_COPY fails with EAGAIN.
 */
static void cow_page_buffer_readd(unsigned long vaddr, void *data)
{
	struct page_buffer_node *node;
	unsigned int hash;

	hash = page_buffer_hash(vaddr);

	pthread_spin_lock(&cow_buffer.lock);

	/* Try to find space in existing node */
	hlist_for_each_entry(node, &cow_buffer.hash_table[hash], hash) {
		if (node->count < PAGE_NODE_ENTRIES) {
			node->entries[node->count].vaddr = vaddr;
			node->entries[node->count].data = data;
			node->count++;
			cow_buffer.nr_pages++;
			pthread_spin_unlock(&cow_buffer.lock);
			page_state_set(vaddr, PAGE_STATE_EAGAIN_QUEUED);
			return;
		}
	}

	pthread_spin_unlock(&cow_buffer.lock);

	/* Need new node */
	node = xmalloc(sizeof(*node));
	if (!node) {
		pr_err("Failed to re-add page 0x%lx on EAGAIN\n", vaddr);
		xfree(data);
		return;
	}

	node->entries[0].vaddr = vaddr;
	node->entries[0].data = data;
	node->count = 1;
	INIT_HLIST_NODE(&node->hash);

	pthread_spin_lock(&cow_buffer.lock);
	hlist_add_head(&node->hash, &cow_buffer.hash_table[hash]);
	cow_buffer.nr_pages++;
	pthread_spin_unlock(&cow_buffer.lock);

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
		pthread_spin_lock(&cow_buffer.lock);

		for (bucket = 0; bucket < PAGE_BUFFER_HASH_SIZE && !drain_thread_stop; bucket++) {
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
					cow_buffer.nr_pages--;

					/* Track: removed from buffer, about to copy */
					page_state_set(vaddr, PAGE_STATE_DRAIN_PENDING);

					pr_debug("COW_TRACE DRAIN_REMOVE: 0x%lx (remaining=%lu)\n", vaddr, cow_buffer.nr_pages);

					pthread_spin_unlock(&cow_buffer.lock);

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
								cow_buffer.nr_discarded++;
								pr_err("BUG: DRAIN_COPY EEXIST at 0x%lx - duplicate copy!\n", vaddr);
								page_state_print_history(vaddr);
								if (page_state_get(vaddr) != PAGE_STATE_DIRTY)
									page_state_set(vaddr, PAGE_STATE_DISCARDED);
							} else if (errno == ENOENT) {
								cow_buffer.nr_discarded++;
								pr_err("COW_TRACE DRAIN_COPY: 0x%lx FAILED errno=ENOENT (VMA unmapped)\n", vaddr);
								page_state_print_history(vaddr);
								if (page_state_get(vaddr) != PAGE_STATE_DIRTY)
									page_state_set(vaddr, PAGE_STATE_DISCARDED);
							} else if (errno == EAGAIN) {
								cow_buffer.nr_eagain++;
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
							cow_buffer.nr_applied++;
							page_state_set(vaddr, PAGE_STATE_COPIED);
							drained++;
						}
					} else {
						cow_buffer.nr_discarded++;
						pr_err("COW_TRACE DRAIN_COPY: 0x%lx no uffd found\n", vaddr);
						page_state_print_history(vaddr);
						if (page_state_get(vaddr) != PAGE_STATE_DIRTY)
							page_state_set(vaddr, PAGE_STATE_DISCARDED);
					}

					if (free_data)
						xfree(data);
					pthread_spin_lock(&cow_buffer.lock);
				}

				/* Remove empty node */
				if (node->count == 0) {
					hlist_del(&node->hash);
					xfree(node);
				}
			}
		}

		pthread_spin_unlock(&cow_buffer.lock);
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
