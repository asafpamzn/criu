#ifndef __CR_COW_UFFD_H__
#define __CR_COW_UFFD_H__

#include <stdbool.h>
#include "int.h"

/* Forward declarations */
struct list_head;
struct epoll_event;

/* Initialize COW page buffer (hash table, locks) */
int cow_page_buffer_init(void);

/* Initialize page pool for a receiver thread (call once per thread) */
int cow_page_buffer_thread_init(int thread_id);

/* Lookup page in buffer (thread-safe) - returns data pointer or NULL */
void *cow_page_buffer_lookup_and_remove(unsigned long vaddr);

/*
 * Add page to buffer (thread-safe).
 * thread_id: receiver thread id for lock-free pool allocation (0-15)
 *            must have called cow_page_buffer_thread_init(thread_id) first
 * nocopy: if true, takes ownership of data pointer (from page_pool_get_chunk)
 *         if false, copies data to new page pool allocation
 */
int cow_page_buffer_add(unsigned long vaddr, void *data, int thread_id, bool nocopy);

/* Get current page count */
unsigned long cow_page_buffer_count(void);

/* Destroy COW page buffer and free all resources */
void cow_page_buffer_destroy(void);

/* Discard dirty pages from buffer (called when dirty bitmap received) */
void cow_page_buffer_discard_dirty(unsigned long *dirty_ranges, unsigned int nr_dirty_ranges);

/* Start background drain thread (lpis needed for EAGAIN handling) */
int cow_start_drain_thread(struct list_head *lpis);

/* Stop background drain thread */
void cow_stop_drain_thread(void);

/* Check if drain thread is running */
bool cow_drain_thread_running(void);

/* Remove all pages in range from buffer (for UNMAP/REMOVE events) */
void cow_page_buffer_remove_range(unsigned long start, unsigned long len);

/* Re-add page to buffer for EAGAIN retry (takes ownership of data) */
void cow_page_buffer_readd(unsigned long vaddr, void *data);

/*
 * UFFD Statistics (COW mode)
 */
extern void check_and_print_uffd_stats(void);
extern int cow_get_histogram_bucket(unsigned long nr_pages);
extern void cow_uffd_stats_inc_pf(unsigned long nr_pages);
extern void cow_uffd_stats_inc_bg(unsigned long nr_pages);
extern void cow_uffd_stats_add_io_bulk(unsigned long ns);
extern void cow_uffd_stats_inc_io_bulk_start(void);
extern void cow_uffd_stats_add_copy(unsigned long ns);
extern void cow_uffd_stats_add_drop(unsigned long ns);

/*
 * EAGAIN Request Handling (COW mode)
 */
struct lazy_pages_info;
extern int cow_queue_eagain_request(struct lazy_pages_info *lpi, __u64 address,
				    unsigned long nr_pages, void *buf, const char *op_name);
extern int cow_queue_drain_eagain_request(struct list_head *lpis, unsigned long vaddr, void *data);
extern bool cow_is_eagain_queue_empty(void);
extern int cow_process_eagain_requests(void);

/*
 * IOV Debugging (COW mode)
 */
extern void cow_dump_lazy_iov_list(struct lazy_pages_info *lpi, const char *name,
				   struct list_head *iovs, unsigned int max_dump);

/* Find IOV for address (wrapper for uffd.c find_iov) */
struct lazy_iov;
extern struct lazy_iov *cow_find_iov(struct lazy_pages_info *lpi, unsigned long addr);

/*
 * COW Restore State Management
 */

/* Check/set if restore has connected (uffd available) */
extern bool cow_is_restore_connected(void);
extern void cow_set_restore_connected(bool connected);


/* Check/set if inventory.img is ready on disk */
extern bool cow_is_inventory_ready_received(void);
extern void cow_set_inventory_ready_received(void);

/* Check/set if all pages have been sent by primary */
extern bool cow_is_all_pages_sent_received(void);
extern void cow_set_all_pages_sent_received(void);

/* Return uffd for a given vaddr (for background drain thread) */
extern int cow_get_uffd_for_vaddr(struct list_head *lpis, unsigned long vaddr);

/*
 * COW Phase 2/3 Infrastructure
 * These functions handle the pre-buffering and convergence phases.
 */

/* Initialize prebuffer reader for COW mode */
extern int cow_setup_prebuffer_reader(void);

/* Cleanup prebuffer resources */
extern void cow_cleanup_prebuffer(void);

/* Get prebuffer buf pointer (for convergence callback) */
extern void *cow_get_prebuffer_buf(void);

/* Switch to convergence callback mode */
extern void cow_switch_to_convergence_callback(void);

/* Convergence IO completion callback */
extern int cow_convergence_io_complete(struct list_head *lpis,
				       unsigned long vaddr,
				       unsigned long nr_pages, void *buf);

/* Handle lazy accept in COW mode */
extern int cow_handle_lazy_accept(struct list_head *lpis, int epollfd,
				  int client, bool phase3_active);

/* Create IOVs for dirty ranges (new VMAs from Phase 1 to Phase 3) */
extern int cow_create_iovs_for_new_ranges(struct list_head *lpis,
					  unsigned long *dirty_ranges,
					  unsigned int nr_dirty_ranges);



/* Store pending dirty ranges (before restore connects) */
extern void cow_store_pending_dirty_ranges(unsigned long *ranges, unsigned int nr);


/* Set/get phase3_active flag */
extern void cow_set_phase3_active(bool active);
extern bool cow_is_phase3_active(void);

/* Handle page fault from buffer - returns 1 if served, 0 if not found, <0 on error */
extern int cow_handle_page_fault_buffer(struct lazy_pages_info *lpi,
					unsigned long long address);

/*
 * Full COW page fault handler - consolidates all COW-specific logic.
 * Takes function pointers for uffd operations to avoid making them non-static.
 */
extern int cow_handle_page_fault_full(struct lazy_pages_info *lpi,
				      unsigned long long address,
				      int (*do_zero)(struct lazy_pages_info *, __u64, unsigned long),
				      int (*do_handle_pages)(struct lazy_pages_info *, __u64, unsigned long, unsigned));

/*
 * COW-specific uffd_copy/uffd_zero error handling
 * Returns: 0 = continue/success, -1 = fatal error, 1 = handled (queued EAGAIN)
 */
extern int cow_uffd_handle_copy_error(struct lazy_pages_info *lpi,
				      __u64 address, unsigned long nr_pages,
				      void *buf, int saved_errno, long copy_result);
extern int cow_uffd_handle_zero_error(struct lazy_pages_info *lpi,
				      __u64 address, unsigned long nr_pages,
				      int saved_errno);
extern void cow_uffd_copy_success(unsigned long address);

/*
 * COW mode wrappers for uffd error handling (combines check + return).
 * Returns: -1 = fatal, 0 = handled (caller returns 0), 1 = not handled
 */
extern int cow_uffd_check_copy_error(struct lazy_pages_info *lpi,
				     __u64 address, unsigned long nr_pages,
				     void *buf, int saved_errno, long copy_result);
extern int cow_uffd_check_zero_error(struct lazy_pages_info *lpi,
				     __u64 address, unsigned long nr_pages,
				     int saved_errno);

/*
 * COW page fault handling (called from handle_page_fault when opts.cow_dump)
 * Returns: 0 = success, -1 = error
 */
extern int cow_handle_page_fault(struct lazy_pages_info *lpi,
				 unsigned long long address);

/*
 * COW convergence IO complete - called when page arrives from convergence stream
 * Finds lpi for vaddr and copies page data.
 * Returns: 0 = success, -1 = error
 */
extern int cow_convergence_copy_page(struct list_head *lpis,
				     unsigned long vaddr,
				     unsigned long nr_pages, void *buf);

/*
 * COW bulk IO complete callback
 * This is the io_complete callback for COW mode page reads.
 */
extern int cow_uffd_io_complete_bulk(struct lazy_pages_info *lpi,
				     unsigned long vaddr, unsigned long nr_pages);

/*
 * Remove buffered pages before urgent copy (uffd_io_complete).
 * Prevents EEXIST when drain thread copies later.
 */
extern void cow_uffd_remove_buffered_pages(unsigned long addr, unsigned long nr);

/*
 * Handle UNMAP/REMOVE event in COW mode.
 * Marks pages as unmapped in trackers and removes from buffer.
 */
extern void cow_handle_remove_event(unsigned long start, unsigned long len);

/*
 * COW-specific page fault handling (full flow).
 * Called from handle_page_fault when opts.cow_dump is true.
 * Returns:
 *   0 - success (page handled or waiting)
 *  -1 - error
 *   COW_PF_ZERO_FILL - caller should zero-fill the page
 *   COW_PF_HANDLE_PAGES - caller should call uffd_handle_pages
 */
#define COW_PF_ZERO_FILL     2
#define COW_PF_HANDLE_PAGES  3

extern int cow_handle_page_fault_cow_mode(struct lazy_pages_info *lpi,
					  unsigned long long address,
					  unsigned long long *img_addr_out);

/*
 * COW post-connect initialization in handle_lazy_accept.
 * Handles dirty bitmap processing and starts drain thread if ready.
 */
extern int cow_handle_lazy_accept_post_connect(struct list_head *lpis,
					       void (*switch_to_convergence)(void));



#endif /* __CR_COW_UFFD_H__ */
