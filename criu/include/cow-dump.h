#ifndef __CR_COW_DUMP_H_
#define __CR_COW_DUMP_H_

#include "types.h"
#include "common/list.h"

struct pstree_item;
struct vm_area_list;
struct parasite_ctl;
struct page_region;

/**
 * cow_dump_init - Initialize COW dump for a process
 * @item: Process tree item to set up COW tracking for
 * @vma_area_list: List of VMAs to track
 * @ctl: Parasite control structure for RPC
 *
 * Sets up userfaultfd with WP_ASYNC write-protection for all writable
 * memory regions of the target process.  Registration is performed via
 * parasite RPC; WRITEPROTECT is deferred to post-resume.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_dump_init(struct pstree_item *item,
			 struct vm_area_list *vma_area_list,
			 struct parasite_ctl *ctl);

/**
 * cow_dump_fini - Clean up COW dump resources
 */
extern void cow_dump_fini(void);

/**
 * cow_check_kernel_support - Check if kernel supports COW dump
 *
 * Verifies WP_ASYNC and PAGEFAULT_FLAG_WP (requires Linux 5.7+).
 *
 * Returns: true if supported, false otherwise
 */
extern bool cow_check_kernel_support(void);

/**
 * cow_dump_apply_writeprotect - Apply UFFDIO_WRITEPROTECT from CRIU side
 *
 * Called after the target process resumes.  Iterates all registered VMAs
 * and applies write-protection via ioctl on the uffd fd.  With WP_ASYNC
 * the kernel auto-resolves write faults (~1-2us), so the process
 * experiences no meaningful write stall.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_dump_apply_writeprotect(void);

/**
 * cow_dump_scan_dirty - Scan for dirty pages and re-apply write-protection
 * @source_pid: PID of the tracked process
 * @start: Start address of the range to scan
 * @end: End address of the range to scan
 * @regs: Output buffer for page_region entries
 * @max_regs: Size of the output buffer
 * @nr_dirty_pages: Output count of dirty pages found
 *
 * Uses PAGEMAP_SCAN with PM_SCAN_WP_MATCHING to atomically find pages
 * whose WP bit was cleared (written pages) and re-apply write-protection.
 *
 * Returns: number of page_region entries on success, -1 on error
 */
extern int cow_dump_scan_dirty(pid_t source_pid,
			       unsigned long start, unsigned long end,
			       struct page_region *regs,
			       unsigned long max_regs,
			       unsigned long *nr_dirty_pages);

/**
 * cow_get_uffd_for_pid - Get the userfaultfd for a tracked source pid
 * @source_pid: Source process pid
 *
 * Returns: userfaultfd fd on success, -1 if not found
 */
extern int cow_get_uffd_for_pid(pid_t source_pid);

/**
 * cow_dump_is_vma_tracked - Check whether a VMA is COW-tracked
 * @source_pid: Source process pid
 * @start: VMA start address
 * @end: VMA end address
 *
 * Returns: true if this exact VMA was successfully registered for COW.
 */
extern bool cow_dump_is_vma_tracked(pid_t source_pid,
				    unsigned long start,
				    unsigned long end);

#endif /* __CR_COW_DUMP_H_ */
