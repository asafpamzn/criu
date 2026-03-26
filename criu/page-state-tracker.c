#include "page-state-tracker.h"

#ifdef CONFIG_PAGE_STATE_TRACKER

#include <string.h>
#include <time.h>
#include <pthread.h>
#include <stdio.h>

#include "int.h"
#include "page.h"
#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"

#undef LOG_PREFIX
#define LOG_PREFIX "page-state: "

/*
 * Comprehensive page state tracking implementation.
 * Uses hash table for O(1) lookup with millions of pages.
 * Stores full history of state changes with timestamps for debugging.
 */

#define PAGE_STATE_HASH_BITS 18
#define PAGE_STATE_HASH_SIZE (1 << PAGE_STATE_HASH_BITS)
#define PAGE_STATE_MAX       10
#define PAGE_STATE_HISTORY_SIZE 16  /* Max history entries per page */

struct page_state_history {
	enum page_state state;
	struct timespec timestamp;
};

struct page_state_entry {
	unsigned long vaddr;
	enum page_state state;
	struct timespec last_change;
	struct hlist_node hash;
	/* History of state changes */
	struct page_state_history history[PAGE_STATE_HISTORY_SIZE];
	int history_count;
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
	[PAGE_STATE_DIRTY]          = "DIRTY",
	[PAGE_STATE_DISCARDED]      = "DISCARDED",
	[PAGE_STATE_UNMAPPED]       = "UNMAPPED",
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
		       to == PAGE_STATE_DIRTY ||
		       to == PAGE_STATE_DISCARDED ||
		       to == PAGE_STATE_UNMAPPED;
	case PAGE_STATE_PF_PENDING:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_DISCARDED ||
		       to == PAGE_STATE_EAGAIN_QUEUED ||
		       to == PAGE_STATE_UNMAPPED;
	case PAGE_STATE_DRAIN_PENDING:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_DISCARDED ||
		       to == PAGE_STATE_EAGAIN_QUEUED ||
		       to == PAGE_STATE_UNMAPPED;
	case PAGE_STATE_URGENT_PENDING:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_EAGAIN_QUEUED ||
		       to == PAGE_STATE_DISCARDED ||
		       to == PAGE_STATE_UNMAPPED;
	case PAGE_STATE_EAGAIN_QUEUED:
		return to == PAGE_STATE_COPIED ||
		       to == PAGE_STATE_DISCARDED ||
		       to == PAGE_STATE_UNMAPPED;
	case PAGE_STATE_COPIED:
		/* COPIED pages can become DIRTY if source re-sends with newer data */
		return to == PAGE_STATE_DIRTY;
	case PAGE_STATE_DISCARDED:
		/* DISCARDED pages can become DIRTY if source re-sends with newer data */
		return to == PAGE_STATE_DIRTY;
	case PAGE_STATE_UNMAPPED:
		/* Truly terminal - region no longer exists */
		return false;
	case PAGE_STATE_DIRTY:
		/* Dirty pages can be:
		 * - Re-buffered (IN_BUFFER) if arriving pre-convergence
		 * - Directly copied (COPIED) during convergence phase
		 */
		return to == PAGE_STATE_IN_BUFFER || to == PAGE_STATE_COPIED;
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

/* Add a state change to the entry's history */
static void page_state_add_history(struct page_state_entry *entry,
				   enum page_state state,
				   struct timespec *ts)
{
	int idx;

	if (entry->history_count < PAGE_STATE_HISTORY_SIZE) {
		idx = entry->history_count++;
	} else {
		/* History full - shift left and add at end */
		memmove(&entry->history[0], &entry->history[1],
			(PAGE_STATE_HISTORY_SIZE - 1) * sizeof(entry->history[0]));
		idx = PAGE_STATE_HISTORY_SIZE - 1;
	}

	entry->history[idx].state = state;
	entry->history[idx].timestamp = *ts;
}

/* Format timestamp as HH:MM:SS.mmm relative to first entry */
static void format_timestamp(struct timespec *ts, struct timespec *base, char *buf, size_t len)
{
	long delta_sec = ts->tv_sec - base->tv_sec;
	long delta_nsec = ts->tv_nsec - base->tv_nsec;
	long ms;

	if (delta_nsec < 0) {
		delta_sec--;
		delta_nsec += 1000000000;
	}

	ms = delta_nsec / 1000000;

	snprintf(buf, len, "+%ld.%03ld", delta_sec, ms);
}

/*
 * Print the full history of state changes for a page.
 * Call this when an error occurs to understand what happened.
 */
void page_state_print_history(unsigned long vaddr)
{
	struct page_state_entry *entry;
	int i;
	char ts_buf[32];

	if (!g_page_state.initialized) {
		pr_err("PAGE_HISTORY 0x%lx: tracker not initialized\n", vaddr);
		return;
	}

	pthread_spin_lock(&g_page_state.lock);

	entry = page_state_find_locked(vaddr);
	if (!entry) {
		pthread_spin_unlock(&g_page_state.lock);
		pr_err("PAGE_HISTORY 0x%lx: no history (page not tracked)\n", vaddr);
		return;
	}

	pr_err("PAGE_HISTORY 0x%lx: %d transitions, current=%s\n",
	       vaddr, entry->history_count, page_state_name(entry->state));

	if (entry->history_count > 0) {
		struct timespec *base = &entry->history[0].timestamp;

		for (i = 0; i < entry->history_count; i++) {
			format_timestamp(&entry->history[i].timestamp, base,
					 ts_buf, sizeof(ts_buf));
			pr_err("  [%d] %s sec: %s\n", i, ts_buf,
			       page_state_name(entry->history[i].state));
		}
	}

	pthread_spin_unlock(&g_page_state.lock);
}

int page_state_set(unsigned long vaddr, enum page_state new_state)
{
	struct page_state_entry *entry;
	enum page_state old_state;
	unsigned int hash;
	struct timespec now;

	if (!g_page_state.initialized)
		return -1;

	clock_gettime(CLOCK_MONOTONIC, &now);

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
			/* Print history on illegal transition */
			pthread_spin_unlock(&g_page_state.lock);
			page_state_print_history(vaddr);
			pthread_spin_lock(&g_page_state.lock);
			/* Re-find entry after releasing lock */
			entry = page_state_find_locked(vaddr);
			if (!entry) {
				pthread_spin_unlock(&g_page_state.lock);
				return -1;
			}
		}

		g_page_state.transitions[old_state][new_state]++;
		entry->state = new_state;
		entry->last_change = now;

		/* Record in history */
		page_state_add_history(entry, new_state, &now);
	} else {
		/* New entry */
		entry = xmalloc(sizeof(*entry));
		if (!entry) {
			pthread_spin_unlock(&g_page_state.lock);
			return -1;
		}

		entry->vaddr = vaddr;
		entry->state = new_state;
		entry->last_change = now;
		entry->history_count = 0;
		INIT_HLIST_NODE(&entry->hash);

		/* Add initial state to history */
		page_state_add_history(entry, new_state, &now);

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

void page_state_mark_range_unmapped(unsigned long start, unsigned long len)
{
	unsigned long vaddr;
	enum page_state state;

	if (!g_page_state.initialized)
		return;

	for (vaddr = start; vaddr < start + len; vaddr += PAGE_SIZE) {
		state = page_state_get(vaddr);
		/* Only transition non-terminal states to UNMAPPED */
		if (state != PAGE_STATE_UNKNOWN &&
		    state != PAGE_STATE_COPIED &&
		    state != PAGE_STATE_DISCARDED &&
		    state != PAGE_STATE_UNMAPPED) {
			page_state_set(vaddr, PAGE_STATE_UNMAPPED);
		}
	}
}

void page_state_mark_dirty_ranges(unsigned long *ranges, unsigned int nr_ranges)
{
	unsigned int i;
	unsigned long marked = 0;

	if (!g_page_state.initialized || !ranges || nr_ranges == 0)
		return;

	for (i = 0; i < nr_ranges; i++) {
		unsigned long start = ranges[i * 2];
		unsigned long len = ranges[i * 2 + 1];
		unsigned long vaddr;

		for (vaddr = start; vaddr < start + len; vaddr += PAGE_SIZE) {
			enum page_state state = page_state_get(vaddr);
			/*
			 * Mark COPIED/DISCARDED pages as expecting re-send.
			 * These pages were already delivered to the application,
			 * but the source has newer data that will arrive.
			 */
			if (state == PAGE_STATE_COPIED ||
			    state == PAGE_STATE_DISCARDED) {
				page_state_set(vaddr, PAGE_STATE_DIRTY);
				marked++;
			}
		}
	}
	pr_info("Marked %lu COPIED/DISCARDED pages as DIRTY for re-receive\n", marked);
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

#endif /* CONFIG_PAGE_STATE_TRACKER */
