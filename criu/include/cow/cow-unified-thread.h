#ifndef __CR_COW_UNIFIED_THREAD_H__
#define __CR_COW_UNIFIED_THREAD_H__

#include <stdbool.h>
#include "types.h"

/*
 * COW unified page server thread.
 *
 * This module handles the PRIMARY side COW page transfer:
 * - Page request queue (SPSC) for PS_IOV_GET requests
 * - Active images queue for tracking open transfers
 * - Unified background thread for sending pages
 */

/* Page request queue functions */
extern void cow_init_page_request_queue(void);
extern void cow_add_page_request(unsigned long vaddr, unsigned long nr_pages, int sk, u64 dst_id);
extern bool cow_has_page_requests(void);
extern unsigned long cow_get_page_request_queue_size(void);

/* Active images queue functions */
extern void cow_init_active_images_queue(void);
extern int cow_add_active_image(u64 dst_id, int sk);

/* Unified thread functions */
extern void cow_wait_for_page_server_thread(void);
extern int cow_page_server_get_all_pages(int sk, u64 dst_id);

/* Called by page_server_get_pages in page-xfer.c */
extern void cow_enqueue_page_requests(unsigned long vaddr, unsigned long nr_pages, int sk, u64 dst_id);

#endif /* __CR_COW_UNIFIED_THREAD_H__ */
