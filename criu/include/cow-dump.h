#ifndef __CR_COW_DUMP_H_
#define __CR_COW_DUMP_H_

#include "types.h"
#include "common/list.h"

struct pstree_item;
struct vm_area_list;
struct parasite_ctl;

/* Forward declaration */
struct page_pipe_buf;

/* Queue entry for COW pages waiting to be sent */
struct cow_page_queue_entry {
	unsigned long vaddr;
	void *data;                      /* M2: Original page content (4KB) */
	struct page_pipe_buf *ppb;      /* Buffer containing this page */
	unsigned int seg_idx;            /* Segment index within buffer */
	unsigned long page_idx_in_seg;   /* Page index within segment */
	struct cow_page_queue_entry *next;   /* Used by consumer-side putback list */
};

/**
 * cow_dump_init - Initialize COW dump for a process
 * @item: Process tree item to set up COW tracking for
 * @vma_area_list: List of VMAs to track
 * @ctl: Parasite control structure for RPC
 *
 * Sets up userfaultfd with write-protection for all writable memory
 * regions of the target process. The registration is performed via
 * parasite RPC to ensure it runs in the target process's context.
 *
 * Returns: 0 on success, -1 on error
 */
extern int cow_dump_init(struct pstree_item *item, struct vm_area_list *vma_area_list, struct parasite_ctl *ctl);

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

struct cow_page_queue_entry;

/**
 * cow_get_next_page - Get next COW page from the queue
 *
 * Lock-free dequeue of the next COW page. Checks the consumer-side
 * putback list first, then the SPSC queue.
 * The caller is responsible for freeing the returned entry and its data.
 *
 * Returns: cow_page_queue_entry on success, NULL if queue is empty
 */
extern struct cow_page_queue_entry *cow_get_next_page(void);

/**
 * cow_has_pending_pages - Check if there are pending COW pages
 *
 * Lock-free check for whether the putback list or SPSC queue
 * has any entries.
 *
 * Returns: true if there are pending pages, false otherwise
 */
extern bool cow_has_pending_pages(void);

/**
 * cow_put_back_page - Put a COW page back for later consumption
 * @entry: Queue entry to re-queue
 *
 * Adds the entry to a consumer-side local list that is drained
 * before the SPSC queue on the next cow_get_next_page() call.
 * Must only be called from the consumer thread (Thread 3).
 */
extern void cow_put_back_page(struct cow_page_queue_entry *entry);

/**
 * cow_get_queue_size - Get the approximate number of pending COW pages
 *
 * Returns the atomic queue size counter. May be slightly stale due to
 * concurrent producer/consumer operations, but accurate for statistics.
 *
 * Returns: Number of entries in the COW page queue (approximate)
 */
extern unsigned long cow_get_queue_size(void);

/**
 * cow_bitmap_fini - Free the COW tracking bitmap
 */
extern void cow_bitmap_fini(void);

/**
 * cow_set_bitmap - Mark a page as write-faulted in the bitmap
 * @vaddr: Virtual address of the faulted page
 *
 * Atomically sets the bit for this page. Called by Thread 1 (write fault
 * monitor). Thread 3 reads these bits via cow_test_bitmap().
 */
extern void cow_set_bitmap(unsigned long vaddr);

/**
 * cow_test_bitmap - Check if a page was write-faulted
 * @vaddr: Virtual address to check
 *
 * Atomically reads the bit for this page. Safe to call from any thread.
 *
 * Returns: true if the page was write-faulted, false otherwise
 */
extern bool cow_test_bitmap(unsigned long vaddr);

#endif /* __CR_COW_DUMP_H_ */