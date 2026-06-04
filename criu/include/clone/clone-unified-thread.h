#ifndef __CR_CLONE_UNIFIED_THREAD_H__
#define __CR_CLONE_UNIFIED_THREAD_H__

#include <stdbool.h>
#include "types.h"

/*
 * CLONE unified page server thread.
 *
 * This module handles the PRIMARY side CLONE page transfer by starting
 * P3 bulk sender threads to transfer pages to the replica.
 */


/* Page server thread functions */
extern void clone_wait_for_page_server_thread(void);
extern int clone_page_server_get_all_pages(int sk, u64 dst_id);

#endif /* __CR_CLONE_UNIFIED_THREAD_H__ */
