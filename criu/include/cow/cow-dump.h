#ifndef __CR_COW_DUMP_H_
#define __CR_COW_DUMP_H_

#include "types.h"

struct pstree_item;
struct vm_area_list;
struct parasite_ctl;

/* Forward declaration */
struct page_pipe_buf;

/* Queue entry for COW pages waiting to be sent */
struct cow_page_queue_entry {
	unsigned long vaddr;
	void *data;                      /* Original page content (4KB) */
	struct page_pipe_buf *ppb;      /* Buffer containing this page */
	unsigned int seg_idx;            /* Segment index within buffer */
	unsigned long page_idx_in_seg;   /* Page index within segment */
	struct cow_page_queue_entry *next;   /* Used by consumer-side putback list */
};

/* COW dump phases for phased migration */
enum cow_dump_phase {
	COW_PHASE_IDLE = 0,
	COW_PHASE_ASYNC_BULK,      /* WP_ASYNC active, bulk transfer in progress */
	COW_PHASE_SCAN,            /* Process frozen, scanning dirty pages */
	COW_PHASE_DONE,
};



/**
 * cow_dump_fini - Clean up COW dump resources
 *
 * Releases all resources allocated for COW tracking.
 */
extern void cow_dump_fini(void);

/**
 * cow_set_dst_id - Update the dst_id for page transfer
 * @dst_id: Process identifier (vpid)
 *
 * In COW phased dump, the initial dst_id is set before collect_pstree_ids()
 * populates vpid. Call this after collect_pstree_ids() to fix it.
 */
extern void cow_set_dst_id(u64 dst_id);



/**
 * cow_dump_is_vma_tracked - Check whether a VMA is COW-tracked
 * @source_pid: Source process pid from dump-time tree
 * @start: VMA start address
 * @end: VMA end address
 *
 * Returns: true if this exact VMA was successfully registered for COW.
 */
extern bool cow_dump_is_vma_tracked(pid_t source_pid,
				    unsigned long start,
				    unsigned long end);

struct cow_page_queue_entry;


/**
 * cow_dump_init_async - Initialize COW dump with WP_ASYNC mode
 * @item: Process tree item to set up COW tracking for
 * @vma_area_list: List of VMAs to track
 * @ctl: Parasite control structure (unused, kept for API consistency)
 *
 * Sets up userfaultfd with WP_ASYNC for non-blocking write tracking.
 * Does NOT start the monitor thread since WP_ASYNC doesn't generate faults.
 * Dirty pages are later discovered via PAGEMAP_SCAN.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_dump_init_async(struct pstree_item *item,
			       struct vm_area_list *vma_area_list,
			       struct parasite_ctl *ctl);


/**
 * cow_get_phase - Get the current COW dump phase
 *
 * Returns: Current cow_dump_phase value
 */
extern enum cow_dump_phase cow_get_phase(void);

/**
 * cow_set_phase - Set the current COW dump phase
 * @phase: New phase to set
 */
extern void cow_set_phase(enum cow_dump_phase phase);

/**
 * cow_is_phased_skeleton_dump - Check if we're in Phase 3 skeleton dump mode
 *
 * In COW phased migration, Phase 3 dumps everything EXCEPT memory pages
 * (which were already transferred in Phase 2). This function returns true
 * when dump_one_task() should skip page dumping and related COW init.
 *
 * Returns: true if in skeleton dump mode, false otherwise
 */
extern bool cow_is_phased_skeleton_dump(void);

/**
 * cow_detect_new_vmas - Detect VMAs that appeared after Phase 1
 * @vmas: Current VMA list (from collect_mappings in Phase 3)
 * @new_ranges: Output array of [start, len, ...] pairs
 * @nr_new_ranges: Output count of new ranges
 *
 * Compares current VMAs with Phase 1 tracked VMAs to find new or
 * extended regions. These need to be marked dirty for WP_SYNC.
 * Caller must xfree() the new_ranges array.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_detect_new_vmas(struct vm_area_list *vmas,
			       unsigned long **new_ranges,
			       unsigned int *nr_new_ranges);

/**
 * cow_cleanup_async_uffd - Close async uffd without unregistering VMAs
 *
 * Closes the async uffd file descriptor directly without issuing
 * UFFDIO_UNREGISTER for each VMA. The kernel automatically cleans up
 * registrations when the fd is closed. This avoids expensive page table
 * walks that can take minutes on large memory systems (300GB+).
 */
extern void cow_cleanup_async_uffd(void);



#endif /* __CR_COW_DUMP_H_ */
