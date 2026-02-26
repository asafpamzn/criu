#ifndef __CR_COW_DUMP_H_
#define __CR_COW_DUMP_H_

#include "types.h"
#include "common/list.h"

struct pstree_item;
struct vm_area_list;
struct parasite_ctl;

#define COW_HASH_BITS 16
#define COW_HASH_SIZE (1 << COW_HASH_BITS)

struct cow_page {
	unsigned long vaddr;
	void *data;
	struct hlist_node hash;
};

/* Forward declaration */
struct page_pipe_buf;

/* Queue entry for COW pages waiting to be sent */
struct cow_page_queue_entry {
	unsigned long vaddr;
	struct page_pipe_buf *ppb;      /* Buffer containing this page */
	unsigned int seg_idx;            /* Segment index within buffer */
	unsigned long page_idx_in_seg;   /* Page index within segment */
	struct list_head list;
};

/**
 * cow_dump_pre_init - Pre-create uffd and pagemap_fd before freeze
 * @pid: Target process PID
 *
 * Creates the COW session, userfaultfd, and pagemap_fd before the
 * process is ptrace-seized.  These operations don't require the
 * process to be frozen.  The pre-created resources are consumed by
 * cow_dump_init() during the frozen window.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_dump_pre_init(pid_t pid);

/**
 * cow_dump_init - Initialize COW dump for a process
 * @item: Process tree item to set up COW tracking for
 * @vma_area_list: List of VMAs to track
 * @ctl: Parasite control structure for RPC (NULL to use /proc/<pid>/userfaultfd)
 *
 * Sets up userfaultfd with write-protection for all writable memory
 * regions of the target process.  When kdat.has_uffd_proc is true the
 * userfaultfd is created via /proc/<pid>/userfaultfd and VMAs are
 * registered directly from CRIU (ctl may be NULL).  Otherwise falls
 * back to parasite RPC.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_dump_init(struct pstree_item *item, struct vm_area_list *vma_area_list, struct parasite_ctl *ctl);

/**
 * cow_dump_start_wp - Launch async write-protect threads
 * cow_dump_finish_wp - Join write-protect threads and check errors
 *
 * Split from cow_dump_init() to allow overlapping WP with
 * other dump work.  Call start_wp after cow_dump_init(),
 * do other dump work, then call finish_wp before proceeding.
 */
extern int cow_dump_start_wp(void);
extern int cow_dump_finish_wp(void);

/**
 * cow_dump_fini - Clean up COW dump resources
 *
 * Releases all resources allocated for COW tracking.
 */
extern void cow_dump_fini(void);

/**
 * cow_check_kernel_support - Check if kernel supports COW dump
 *
 * Verifies that the kernel has necessary userfaultfd write-protect
 * features (requires Linux 5.7+).
 *
 * Returns: true if supported, false otherwise
 */
extern bool cow_check_kernel_support(void);

/**
 * cow_start_monitor_thread - Start background thread to monitor page faults
 *
 * Creates a pthread that continuously monitors the userfaultfd for
 * write faults and handles them immediately, preventing the target
 * process from blocking during the dump phase.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_start_monitor_thread(void);

/**
 * cow_stop_monitor_thread - Stop the monitoring thread
 *
 * Signals the monitor thread to stop and waits for it to complete.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_stop_monitor_thread(void);

/**
 * cow_get_uffd - Get the userfaultfd file descriptor
 *
 * Returns the userfaultfd associated with the current COW dump session.
 *
 * Returns: userfaultfd on success, -1 if COW dump not initialized
 */
extern int cow_get_uffd(void);

/**
 * cow_get_uffd_for_pid - Get the userfaultfd for a tracked source pid
 * @source_pid: Source process pid from dump-time tree
 *
 * Returns: userfaultfd on success, -1 if not found
 */
extern int cow_get_uffd_for_pid(pid_t source_pid);

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

/**
 * cow_lookup_page - Look up a COW page without removing it
 * @vaddr: Virtual address of the page
 *
 * Look up a page in the COW hash table without removing it.
 * IMPORTANT: Caller must hold the hash bucket lock for this page.
 *
 * Returns: cow_page structure on success, NULL if not found
 */
extern struct cow_page *cow_lookup_page(unsigned long vaddr);

/**
 * cow_remove_page - Remove and free a COW page
 * @vaddr: Virtual address of the page
 *
 * Remove a page from the COW hash table and free its memory.
 * IMPORTANT: Caller must hold the hash bucket lock for this page.
 */
extern void cow_remove_page(unsigned long vaddr);

/**
 * cow_lookup_and_remove_page - Look up and remove a COW page
 * @vaddr: Virtual address of the page
 *
 * Thread-safe lookup and removal of a copied page from the hash table.
 * The caller is responsible for freeing the returned cow_page structure
 * and its data.
 *
 * Returns: cow_page structure on success, NULL if not found
 */
extern struct cow_page *cow_lookup_and_remove_page(unsigned long vaddr);

/**
 * cow_get_hash_lock - Get pointer to the spinlock for a page's hash bucket
 * @vaddr: Virtual address of the page
 *
 * Returns the spinlock that protects the hash bucket for the given address.
 * Used for manual locking around cow_lookup_page/cow_remove_page.
 *
 * Returns: Pointer to the spinlock
 */
extern pthread_spinlock_t *cow_get_hash_lock(unsigned long vaddr);

struct cow_page_queue_entry;

/**
 * cow_get_next_page - Get next COW page from the queue
 *
 * Thread-safe dequeue of the next COW page that needs to be sent.
 * The caller is responsible for freeing the returned entry.
 *
 * Returns: cow_page_queue_entry on success, NULL if queue is empty
 */
extern struct cow_page_queue_entry *cow_get_next_page(void);

/**
 * cow_has_pending_pages - Check if there are pending COW pages
 *
 * Thread-safe check for whether the COW page queue has any entries.
 *
 * Returns: true if there are pending pages, false otherwise
 */
extern bool cow_has_pending_pages(void);

/**
 * cow_put_back_page - Put a COW page back in the queue
 * @entry: Queue entry to re-queue
 *
 * Thread-safe re-insertion of a COW page at the head of the queue.
 * Used when a page doesn't belong to the current image being processed.
 */
extern void cow_put_back_page(struct cow_page_queue_entry *entry);

/**
 * cow_get_queue_size - Get the number of pending COW pages in the queue
 *
 * Thread-safe count of COW pages waiting to be sent.
 *
 * Returns: Number of entries in the COW page queue
 */
extern unsigned long cow_get_queue_size(void);

/**
 * cow_is_wp_async - Check if WP_ASYNC mode is active
 *
 * When true, userfaultfd write-protect faults are resolved
 * automatically by the kernel (no thread parking, no events).
 * Dirty page tracking uses PAGEMAP_SCAN instead of uffd events.
 *
 * Returns: true if WP_ASYNC mode is active
 */
extern bool cow_is_wp_async(void);

/**
 * cow_get_pagemap_fd_for_pid - Get pagemap fd for PAGEMAP_SCAN
 * @source_pid: Source process pid
 *
 * Returns: pagemap fd on success, -1 if not found or not WP_ASYNC
 */
extern int cow_get_pagemap_fd_for_pid(pid_t source_pid);

/**
 * cow_scan_dirty_pages - Scan for dirty pages via PAGEMAP_SCAN
 * @source_pid: Source process pid
 * @start: Start address of range to scan
 * @end: End address of range to scan
 * @regions: Output array of page_region structs
 * @max_regions: Capacity of regions array
 * @walk_end: Output: address where scan stopped
 *
 * Scans for pages written since WP was applied/re-armed.
 * Atomically re-arms WP on dirty pages via PM_SCAN_WP_MATCHING.
 * Only valid in WP_ASYNC mode.
 *
 * Returns: number of regions found, or -1 on error
 */
extern int cow_scan_dirty_pages(pid_t source_pid,
				unsigned long start, unsigned long end,
				void *regions, unsigned long max_regions,
				unsigned long *walk_end);

#endif /* __CR_COW_DUMP_H_ */
