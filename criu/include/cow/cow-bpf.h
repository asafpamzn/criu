#ifndef __CR_COW_BPF_H__
#define __CR_COW_BPF_H__

#include <stdbool.h>
#include "types.h"

/*
 * Region returned by cow_bpf_drain — same layout as converge_region
 * in page-xfer.c so it can be used directly with dispatch functions.
 */
struct cow_bpf_region {
	u64 start;
	u64 end;
	u64 categories;
};

/*
 * Start BPF dirty page tracking for the given PID.
 * Hooks kfunc:do_wp_page, filters by PID, collects
 * faulting addresses in a ring buffer.
 *
 * Must be called AFTER userfaultfd WP is set up (after dump)
 * and BEFORE the T3 freeze.
 *
 * Returns 0 on success, -1 on failure (falls back to PAGEMAP_SCAN).
 */
extern int cow_bpf_start(pid_t target_pid);

/*
 * Drain the BPF ring buffer into a sorted, coalesced region list.
 *
 * @out_regions: output array (same layout as converge_region)
 * @max_regions: capacity of out_regions
 * @out_count: output: number of unique dirty pages
 *
 * Returns number of coalesced regions, -1 on error, or -2 if
 * ring buffer drops were detected (caller must use PAGEMAP_SCAN).
 */
extern int cow_bpf_drain(struct cow_bpf_region *out_regions,
			  int max_regions, unsigned long *out_count);

/*
 * Get total event count from BPF (including duplicates).
 */
extern u64 cow_bpf_event_count(void);

/*
 * Get count of dropped events (ring buffer full).
 * Non-zero means dirty page list is incomplete.
 */
extern u64 cow_bpf_drop_count(void);

/*
 * Stop BPF tracking — detach + free.
 */
extern void cow_bpf_stop(void);

/*
 * Check if BPF tracking is active.
 */
extern bool cow_bpf_active(void);

/*
 * Drain BPF ring buffer and return raw sorted/deduped addresses.
 * Caller must xfree() the returned array.
 *
 * @out_addrs: output pointer to allocated address array
 * @out_count: number of unique addresses
 *
 * Returns 0 on success, -1 on error.
 */
extern int cow_bpf_drain_addrs(unsigned long **out_addrs, unsigned long *out_count);

#ifdef SCAN_COMPARE
/*
 * Get the initial dirty pages captured at BPF start time.
 * Returns pointer to static array (do not free).
 */
extern unsigned long *cow_bpf_get_initial_dirty(unsigned long *count);
#endif

#endif /* __CR_COW_BPF_H__ */
