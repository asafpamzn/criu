#include <string.h>
#include <time.h>
#include <pthread.h>

#include "int.h"
#include "page.h"
#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"
#include "pf-tracker.h"

#undef LOG_PREFIX
#define LOG_PREFIX "pf-tracker: "

/*
 * Comprehensive page state tracking implementation.
 * Uses hash table for O(1) lookup with millions of pages.
 */

#define PAGE_STATE_HASH_BITS 18
#define PAGE_STATE_HASH_SIZE (1 << PAGE_STATE_HASH_BITS)
#define PAGE_STATE_MAX       8

struct page_state_entry {
	unsigned long vaddr;
	enum page_state state;
	struct timespec last_change;
	struct hlist_node hash;
};

static struct {
	struct hlist_head *hash_table;
	pthread_spinlock_t lock;
	unsigned long total_pages;
	unsigned long transitions[PAGE_STATE_MAX][PAGE_STATE_MAX];
	unsigned long illegal_transitions;
	bool initialized;
} g_page_state = { .initialized = false };

static const char *state_names[] = {
	[PAGE_STATE_UNKNOWN]        = "UNKNOWN",
	[PAGE_STATE_IN_BUFFER]      = "IN_BUFFER",
	[PAGE_STATE_PF_PENDING]     = "PF_PENDING",
	[PAGE_STATE_DRAIN_PENDING]  = "DRAIN_PENDING",
	[PAGE_STATE_URGENT_PENDING] = "URGENT_PENDING",
	[PAGE_STATE_EAGAIN_QUEUED]  = "EAGAIN_QUEUED",
	[PAGE_STATE_COPIED]         = "COPIED",
	[PAGE_STATE_DISCARDED]      = "DISCARDED",
};

const char *page_state_name(enum page_state state)
{
	if (state >= PAGE_STATE_MAX)
		return "INVALID";
	return state_names[state];
}

static inline unsigned int page_state_hash(unsigned long vaddr)
{
	return (vaddr >> PAGE_SHIFT) & (PAGE_STATE_HASH_SIZE - 1);
}

/*
 * Valid state transitions:
 * UNKNOWN -> IN_BUFFER, URGENT_PENDING, PF_PENDING, DRAIN_PENDING, COPIED, DISCARDED
 * IN_BUFFER -> PF_PENDING, DRAIN_PENDING, DISCARDED
 * PF_PENDING -> COPIED, DISCARDED, EAGAIN_QUEUED
 * DRAIN_PENDING -> COPIED, DISCARDED, EAGAIN_QUEUED
 * URGENT_PENDING -> COPIED, EAGAIN_QUEUED, DISCARDED
 * EAGAIN_QUEUED -> COPIED, DISCARDED
 * COPIED -> (terminal, no further transitions)
 * DISCARDED -> (terminal, no further transitions)
 */
static bool is_valid_transition(enum page_state from, enum page_state to)
{
	switch (from) {
	case PAGE_STATE_UNKNOWN:
		/* Can transition to any initial state */
		return true;
	case PAGE_STATE_IN_BUFFER:
		return to == PAGE_STATE_PF_PENDING ||
		       to == PAGE_STATE_DRAIN_PENDING ||
		       to == PAGE_STATE_DISCARDED;
	case PAGE_STATE_PF_PENDING:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_DISCARDED ||
		       to == PAGE_STATE_EAGAIN_QUEUED;
	case PAGE_STATE_DRAIN_PENDING:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_DISCARDED ||
		       to == PAGE_STATE_EAGAIN_QUEUED;
	case PAGE_STATE_URGENT_PENDING:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_EAGAIN_QUEUED ||
		       to == PAGE_STATE_DISCARDED;
	case PAGE_STATE_EAGAIN_QUEUED:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_DISCARDED;
	case PAGE_STATE_COPIED:
	case PAGE_STATE_DISCARDED:
		/* Terminal states - no further transitions allowed */
		return false;
	default:
		return false;
	}
}

int page_state_init(void)
{
	int i;

	if (g_page_state.initialized)
		return 0;

	g_page_state.hash_table = xmalloc(PAGE_STATE_HASH_SIZE *
					  sizeof(struct hlist_head));
	if (!g_page_state.hash_table)
		return -1;

	for (i = 0; i < PAGE_STATE_HASH_SIZE; i++)
		INIT_HLIST_HEAD(&g_page_state.hash_table[i]);

	pthread_spin_init(&g_page_state.lock, PTHREAD_PROCESS_PRIVATE);
	g_page_state.total_pages = 0;
	g_page_state.illegal_transitions = 0;
	memset(g_page_state.transitions, 0, sizeof(g_page_state.transitions));
	g_page_state.initialized = true;

	pr_info("Page state tracker initialized (hash size=%d)\n",
		PAGE_STATE_HASH_SIZE);
	return 0;
}

static struct page_state_entry *page_state_find_locked(unsigned long vaddr)
{
	struct page_state_entry *entry;
	unsigned int hash = page_state_hash(vaddr);

	hlist_for_each_entry(entry, &g_page_state.hash_table[hash], hash) {
		if (entry->vaddr == vaddr)
			return entry;
	}
	return NULL;
}

int page_state_set(unsigned long vaddr, enum page_state new_state)
{
	struct page_state_entry *entry;
	enum page_state old_state;
	unsigned int hash;

	if (!g_page_state.initialized)
		return -1;

	pthread_spin_lock(&g_page_state.lock);

	entry = page_state_find_locked(vaddr);
	if (entry) {
		old_state = entry->state;

		/* Validate transition */
		if (!is_valid_transition(old_state, new_state)) {
			g_page_state.illegal_transitions++;
			pr_err("PAGE_STATE ILLEGAL_TRANSITION: 0x%lx %s -> %s\n",
			       vaddr, page_state_name(old_state),
			       page_state_name(new_state));
		}

		g_page_state.transitions[old_state][new_state]++;
		entry->state = new_state;
		clock_gettime(CLOCK_MONOTONIC, &entry->last_change);
	} else {
		/* New entry */
		entry = xmalloc(sizeof(*entry));
		if (!entry) {
			pthread_spin_unlock(&g_page_state.lock);
			return -1;
		}

		entry->vaddr = vaddr;
		entry->state = new_state;
		clock_gettime(CLOCK_MONOTONIC, &entry->last_change);
		INIT_HLIST_NODE(&entry->hash);

		hash = page_state_hash(vaddr);
		hlist_add_head(&entry->hash, &g_page_state.hash_table[hash]);
		g_page_state.total_pages++;

		g_page_state.transitions[PAGE_STATE_UNKNOWN][new_state]++;
	}

	pthread_spin_unlock(&g_page_state.lock);

	pr_debug("PAGE_STATE: 0x%lx -> %s\n", vaddr, page_state_name(new_state));
	return 0;
}

enum page_state page_state_get(unsigned long vaddr)
{
	struct page_state_entry *entry;
	enum page_state state = PAGE_STATE_UNKNOWN;

	if (!g_page_state.initialized)
		return PAGE_STATE_UNKNOWN;

	pthread_spin_lock(&g_page_state.lock);
	entry = page_state_find_locked(vaddr);
	if (entry)
		state = entry->state;
	pthread_spin_unlock(&g_page_state.lock);

	return state;
}

void page_state_print_stats(void)
{
	int i, j;
	unsigned long state_counts[PAGE_STATE_MAX] = {0};
	struct page_state_entry *entry;

	if (!g_page_state.initialized) {
		pr_info("Page state tracker not initialized\n");
		return;
	}

	pthread_spin_lock(&g_page_state.lock);

	/* Count pages in each state */
	for (i = 0; i < PAGE_STATE_HASH_SIZE; i++) {
		hlist_for_each_entry(entry, &g_page_state.hash_table[i], hash) {
			if (entry->state < PAGE_STATE_MAX)
				state_counts[entry->state]++;
		}
	}

	pr_info("=== PAGE STATE TRACKER STATS ===\n");
	pr_info("Total pages tracked: %lu\n", g_page_state.total_pages);
	pr_info("Illegal transitions: %lu\n", g_page_state.illegal_transitions);

	pr_info("Current state counts:\n");
	for (i = 0; i < PAGE_STATE_MAX; i++) {
		if (state_counts[i] > 0)
			pr_info("  %s: %lu\n", state_names[i], state_counts[i]);
	}

	pr_info("State transitions:\n");
	for (i = 0; i < PAGE_STATE_MAX; i++) {
		for (j = 0; j < PAGE_STATE_MAX; j++) {
			if (g_page_state.transitions[i][j] > 0) {
				pr_info("  %s -> %s: %lu\n",
					state_names[i], state_names[j],
					g_page_state.transitions[i][j]);
			}
		}
	}
	pr_info("=== END PAGE STATE STATS ===\n");

	pthread_spin_unlock(&g_page_state.lock);
}

void page_state_destroy(void)
{
	struct page_state_entry *entry;
	struct hlist_node *tmp;
	int i;

	if (!g_page_state.initialized)
		return;

	/* Print final stats before destroying */
	page_state_print_stats();

	pthread_spin_lock(&g_page_state.lock);

	for (i = 0; i < PAGE_STATE_HASH_SIZE; i++) {
		hlist_for_each_entry_safe(entry, tmp,
					  &g_page_state.hash_table[i], hash) {
			hlist_del(&entry->hash);
			xfree(entry);
		}
	}

	pthread_spin_unlock(&g_page_state.lock);

	xfree(g_page_state.hash_table);
	g_page_state.hash_table = NULL;
	g_page_state.initialized = false;

	pthread_spin_destroy(&g_page_state.lock);

	pr_info("Page state tracker destroyed\n");
}

struct pf_tracker_entry {
	struct list_head l;
	unsigned long long address;
	unsigned long nr_pages;
	int pid;
	enum pf_state state;
	struct timespec created;
	bool is_pf; /* true = page fault, false = background xfer */
};

static LIST_HEAD(pf_tracker);

static struct pf_tracker_entry *pf_tracker_find(unsigned long long address)
{
	struct pf_tracker_entry *entry;

	list_for_each_entry(entry, &pf_tracker, l) {
		if (entry->address == address && entry->state != PF_STATE_COMPLETED)
			return entry;
	}

	return NULL;
}

void pf_tracker_add(unsigned long long address, unsigned long nr_pages, int pid, bool is_pf)
{
	struct pf_tracker_entry *entry;

	entry = xmalloc(sizeof(*entry));
	if (!entry) {
		pr_err("Failed to allocate pf_tracker_entry\n");
		return;
	}

	entry->address = address;
	entry->nr_pages = nr_pages;
	entry->pid = pid;
	entry->state = PF_STATE_PENDING_SERVER;
	entry->is_pf = is_pf;
	clock_gettime(CLOCK_MONOTONIC, &entry->created);
	INIT_LIST_HEAD(&entry->l);

	list_add_tail(&entry->l, &pf_tracker);
}

void pf_tracker_set_state(unsigned long long address, enum pf_state state)
{
	struct pf_tracker_entry *entry;

	entry = pf_tracker_find(address);
	if (!entry) {
		if (state == PF_STATE_COMPLETED)
			pr_warn("PF_TRACKER: UFFDIO_COPY succeeded for untracked address 0x%llx\n",
				(unsigned long long)address);
		return;
	}

	entry->state = state;
}

void pf_tracker_print_stats(void)
{
	struct pf_tracker_entry *pft, *pft_next;
	unsigned long pending_server = 0, pending_eagain = 0;
	unsigned long completed = 0;
	unsigned long oldest_server_ms = 0, oldest_eagain_ms = 0;
	struct timespec ts_now;

	clock_gettime(CLOCK_MONOTONIC, &ts_now);

	list_for_each_entry(pft, &pf_tracker, l) {
		unsigned long age_ms = (ts_now.tv_sec - pft->created.tv_sec) * 1000 +
			(ts_now.tv_nsec - pft->created.tv_nsec) / 1000000;

		switch (pft->state) {
		case PF_STATE_PENDING_SERVER:
			pending_server++;
			if (age_ms > oldest_server_ms)
				oldest_server_ms = age_ms;
			break;
		case PF_STATE_PENDING_EAGAIN:
			pending_eagain++;
			if (age_ms > oldest_eagain_ms)
				oldest_eagain_ms = age_ms;
			break;
		case PF_STATE_COMPLETED:
			completed++;
			break;
		}
	}

	if (pending_server > 0 || pending_eagain > 0) {
		pr_err("  PF_TRACKER: pending_server=%lu (oldest=%lu ms) pending_eagain=%lu (oldest=%lu ms) completed=%lu\n",
			pending_server, oldest_server_ms,
			pending_eagain, oldest_eagain_ms,
			completed);

		/* Print details of long-hung entries (>2 seconds) */
		list_for_each_entry(pft, &pf_tracker, l) {
			unsigned long age_ms = (ts_now.tv_sec - pft->created.tv_sec) * 1000 +
				(ts_now.tv_nsec - pft->created.tv_nsec) / 1000000;

			if (age_ms > 2000 && pft->state != PF_STATE_COMPLETED) {
				pr_err("    HUNG: pid=%d addr=0x%llx pages=%lu state=%s age=%lu ms %s\n",
					pft->pid, pft->address, pft->nr_pages,
					pft->state == PF_STATE_PENDING_SERVER ? "PENDING_SERVER" : "PENDING_EAGAIN",
					age_ms,
					pft->is_pf ? "PF" : "BG");
			}
		}
	}

	/* Clean up completed entries */
	list_for_each_entry_safe(pft, pft_next, &pf_tracker, l) {
		if (pft->state == PF_STATE_COMPLETED) {
			list_del(&pft->l);
			xfree(pft);
		}
	}
}