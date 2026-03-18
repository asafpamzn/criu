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

#undef LOG_PREFIX
#define LOG_PREFIX "cow-uffd: "

#define PAGE_BUFFER_HASH_BITS 16
#define PAGE_BUFFER_HASH_SIZE (1 << PAGE_BUFFER_HASH_BITS)

struct page_buffer_entry {
	unsigned long vaddr;
	void *data;
	struct hlist_node hash;
};

static struct {
	struct hlist_head *hash_table;
	unsigned long nr_pages;
	unsigned long nr_applied;
	unsigned long nr_discarded;
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
	cow_buffer.initialized = true;

	pr_info("COW page buffer initialized\n");
	return 0;
}

int cow_page_buffer_add(unsigned long vaddr, void *data)
{
	struct page_buffer_entry *entry;
	unsigned int hash;

	if (!cow_buffer.initialized)
		return -1;

	hash = page_buffer_hash(vaddr);

	pthread_spin_lock(&cow_buffer.lock);

	/* Check for duplicate while holding lock */
	hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
		if (entry->vaddr == vaddr) {
			/* Update existing entry with newer data */
			memcpy(entry->data, data, PAGE_SIZE);
			pthread_spin_unlock(&cow_buffer.lock);
			return 0;
		}
	}

	pthread_spin_unlock(&cow_buffer.lock);

	/* Allocate new entry outside lock */
	entry = xmalloc(sizeof(*entry));
	if (!entry)
		return -1;

	entry->data = xmalloc(PAGE_SIZE);
	if (!entry->data) {
		xfree(entry);
		return -1;
	}

	memcpy(entry->data, data, PAGE_SIZE);
	entry->vaddr = vaddr;
	INIT_HLIST_NODE(&entry->hash);

	pthread_spin_lock(&cow_buffer.lock);
	hlist_add_head(&entry->hash, &cow_buffer.hash_table[hash]);
	cow_buffer.nr_pages++;
	pthread_spin_unlock(&cow_buffer.lock);

	return 0;
}

void *cow_page_buffer_lookup_and_remove(unsigned long vaddr)
{
	struct page_buffer_entry *entry;
	unsigned int hash;
	void *data = NULL;

	if (!cow_buffer.initialized)
		return NULL;

	hash = page_buffer_hash(vaddr);

	pthread_spin_lock(&cow_buffer.lock);
	hlist_for_each_entry(entry, &cow_buffer.hash_table[hash], hash) {
		if (entry->vaddr == vaddr) {
			data = entry->data;
			hlist_del(&entry->hash);
			cow_buffer.nr_pages--;
			xfree(entry);
			break;
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
	unsigned int i;
	unsigned long discarded = 0;

	if (!cow_buffer.initialized || !dirty_ranges || nr_dirty_ranges == 0)
		return;

	pthread_spin_lock(&cow_buffer.lock);

	/* Iterate dirty ranges and do O(1) hash lookups */
	for (i = 0; i < nr_dirty_ranges; i++) {
		unsigned long start = dirty_ranges[i * 2];
		unsigned long len = dirty_ranges[i * 2 + 1];
		unsigned long vaddr;

		/* Iterate each page in this dirty range */
		for (vaddr = start; vaddr < start + len; vaddr += PAGE_SIZE) {
			unsigned int hash = page_buffer_hash(vaddr);
			struct page_buffer_entry *entry;
			struct hlist_node *tmp;

			hlist_for_each_entry_safe(entry, tmp,
						  &cow_buffer.hash_table[hash], hash) {
				if (entry->vaddr == vaddr) {
					hlist_del(&entry->hash);
					xfree(entry->data);
					xfree(entry);
					cow_buffer.nr_pages--;
					cow_buffer.nr_discarded++;
					discarded++;
					break;  /* Found and removed, move to next page */
				}
			}
		}
	}

	pthread_spin_unlock(&cow_buffer.lock);

	pr_info("Discarded %lu dirty pages from buffer\n", discarded);
}

void cow_page_buffer_destroy(void)
{
	struct page_buffer_entry *entry;
	struct hlist_node *tmp;
	int i;

	if (!cow_buffer.initialized)
		return;

	/* Stop drain thread first */
	cow_stop_drain_thread();

	pthread_spin_lock(&cow_buffer.lock);

	for (i = 0; i < PAGE_BUFFER_HASH_SIZE; i++) {
		hlist_for_each_entry_safe(entry, tmp,
					  &cow_buffer.hash_table[i], hash) {
			hlist_del(&entry->hash);
			xfree(entry->data);
			xfree(entry);
		}
	}

	pthread_spin_unlock(&cow_buffer.lock);

	xfree(cow_buffer.hash_table);
	cow_buffer.hash_table = NULL;
	cow_buffer.initialized = false;

	pthread_spin_destroy(&cow_buffer.lock);

	pr_info("COW page buffer destroyed: applied=%lu discarded=%lu\n",
		cow_buffer.nr_applied, cow_buffer.nr_discarded);
}

/*
 * Background drain thread - proactively UFFDIO_COPY pages
 * from buffer to reduce future page faults and free memory.
 */
static void *background_drain_thread(void *arg)
{
	struct page_buffer_entry *entry;
	struct hlist_node *tmp;
	int i;
	unsigned long drained = 0;

	pr_info("Background drain thread started (%lu pages buffered)\n",
		cow_buffer.nr_pages);

	while (!drain_thread_stop && cow_buffer.nr_pages > 0) {
		pthread_spin_lock(&cow_buffer.lock);

		for (i = 0; i < PAGE_BUFFER_HASH_SIZE && !drain_thread_stop; i++) {
			hlist_for_each_entry_safe(entry, tmp,
						  &cow_buffer.hash_table[i], hash) {
				unsigned long vaddr = entry->vaddr;
				void *data = entry->data;
				int uffd;

				/* Remove from hash while holding lock */
				hlist_del(&entry->hash);
				cow_buffer.nr_pages--;
				xfree(entry);

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
							/* EEXIST: page already filled - BUT WITH WHAT? */
							cow_buffer.nr_discarded++;
							pr_err("DEBUG DRAIN EEXIST: Page 0x%lx already exists - DISCARDING CORRECT DATA!\n", vaddr);
						} else if (errno == ENOENT) {
							/* ENOENT: VMA was unmapped (app freed memory) */
							cow_buffer.nr_discarded++;
							pr_warn("Drain UFFDIO_COPY ENOENT 0x%lx - VMA unmapped\n", vaddr);
						} else {
							pr_perror("Drain UFFDIO_COPY failed 0x%lx", vaddr);
						}
					} else {
						cow_buffer.nr_applied++;
						drained++;
					}
				} else {
					cow_buffer.nr_discarded++;
				}

				xfree(data);
				pthread_spin_lock(&cow_buffer.lock);
			}
		}

		pthread_spin_unlock(&cow_buffer.lock);

		/* Yield to let page faults be handled with priority */
		usleep(100);
	}

	pr_info("Drain thread done: %lu drained, %lu applied, %lu discarded\n",
		drained, cow_buffer.nr_applied, cow_buffer.nr_discarded);

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
