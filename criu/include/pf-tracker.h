#ifndef __CR_PF_TRACKER_H__
#define __CR_PF_TRACKER_H__

#include <stdbool.h>
#include "int.h"

enum pf_state {
	PF_STATE_PENDING_SERVER,  /* Waiting for page data from server */
	PF_STATE_PENDING_EAGAIN,  /* UFFDIO_COPY got EAGAIN, queued for retry */
	PF_STATE_COMPLETED,       /* UFFDIO_COPY succeeded */
};

extern void pf_tracker_add(unsigned long long address, unsigned long nr_pages, int pid, bool is_pf);
extern void pf_tracker_set_state(unsigned long long address, enum pf_state state);
extern void pf_tracker_print_stats(void);

/*
 * Comprehensive page state tracking for COW lazy restore debugging.
 * Tracks all state transitions and validates them to detect bugs.
 */
enum page_state {
	PAGE_STATE_UNKNOWN = 0,       /* Not yet tracked */
	PAGE_STATE_IN_BUFFER,         /* In COW buffer (after cow_page_buffer_add) */
	PAGE_STATE_PF_PENDING,        /* PF handler found in buffer, about to copy */
	PAGE_STATE_DRAIN_PENDING,     /* Drain thread removed from buffer, about to copy */
	PAGE_STATE_URGENT_PENDING,    /* Urgent request received, about to copy */
	PAGE_STATE_EAGAIN_QUEUED,     /* UFFDIO_COPY got EAGAIN, queued for retry */
	PAGE_STATE_COPIED,            /* UFFDIO_COPY succeeded */
	PAGE_STATE_DISCARDED,         /* Discarded (EEXIST, ENOENT, dirty, etc.) */
};

extern int page_state_init(void);
extern void page_state_destroy(void);
extern int page_state_set(unsigned long vaddr, enum page_state new_state);
extern enum page_state page_state_get(unsigned long vaddr);
extern void page_state_print_stats(void);
extern const char *page_state_name(enum page_state state);

#endif /* __CR_PF_TRACKER_H__ */