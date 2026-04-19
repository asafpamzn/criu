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

/* Find IOV for address (wrapper for uffd.c find_iov) */
struct lazy_iov;
extern struct lazy_iov *cow_find_iov(struct lazy_pages_info *lpi, unsigned long addr);

/*
 * COW Restore State Management
 */

/* Check/set if restore has connected (uffd available) */
extern bool cow_is_restore_connected(void);
extern void cow_set_restore_connected(bool connected);


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


/* Get prebuffer buf pointer (for convergence callback) */
extern void *cow_get_prebuffer_buf(void);


/* Handle lazy accept in COW mode */
extern int cow_handle_lazy_accept(struct list_head *lpis, int epollfd,
				  int client, bool phase3_active);


/* Set/get phase3_active flag */
extern void cow_set_phase3_active(bool active);
extern bool cow_is_phase3_active(void);


/*
 * COW_TRACK_* flags for cow_uffd_copy()
 */
#define COW_TRACK_STRICT    (1 << 0)  /* BUG() on EEXIST/ERROR (drain mode) */
#define COW_TRACK_RETRY     (1 << 1)  /* Retry mode: no buffer stats, return -EAGAIN */

/*
 * Unified UFFDIO_COPY with full tracking for COW mode.
 * Handles buffer stats, page state, unmapped tracker, and EAGAIN queue.
 *
 * Returns:
 *   1 - success (page copied)
 *   0 - soft handled (ENOENT unmapped, EAGAIN queued, EEXIST already done)
 *  -1 - error
 *  -EAGAIN - kernel busy (only with COW_TRACK_RETRY flag)
 */
extern int cow_uffd_copy(int uffd, unsigned long vaddr, void *data,
			 unsigned long nr_pages, struct lazy_pages_info *lpi,
			 struct list_head *lpis, unsigned int flags,
			 const char *caller);

/*
 * Queue EAGAIN for zero operation (simpler than full cow_uffd_copy path).
 * Called directly from uffd_zero() for EAGAIN handling.
 */


/*
 * COW bulk IO complete callback
 * This is the io_complete callback for COW mode page reads.
 */
extern int cow_uffd_io_complete_bulk(struct lazy_pages_info *lpi,
				     unsigned long vaddr, unsigned long nr_pages);

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

/*
 * COW post-connect initialization in handle_lazy_accept.
 */
extern int cow_handle_lazy_accept_post_connect(struct list_head *lpis);



#endif /* __CR_COW_UFFD_H__ */
