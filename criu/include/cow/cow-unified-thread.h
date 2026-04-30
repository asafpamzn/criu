#ifndef __CR_COW_UNIFIED_THREAD_H__
#define __CR_COW_UNIFIED_THREAD_H__

#include <stdbool.h>
#include "types.h"

/*
 * COW unified page server thread.
 *
 * This module handles the PRIMARY side COW page transfer by starting
 * P3 bulk sender threads to transfer pages to the replica.
 */


/* Page server thread functions */
extern void cow_wait_for_page_server_thread(void);
extern int cow_page_server_get_all_pages(int sk, u64 dst_id);

#endif /* __CR_COW_UNIFIED_THREAD_H__ */
