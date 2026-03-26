#ifndef __CR_COW_BULK_SEND_H__
#define __CR_COW_BULK_SEND_H__

#include "int.h"

#define COW_BATCH_PAGES 64
#define COW_BATCH_SIZE  (COW_BATCH_PAGES * PAGE_SIZE)  /* 256KB */

/*
 * Start the P3 bulk sender thread.
 * This thread sends regular (non-COW) pages in batches of 64 pages (256KB).
 * Returns 0 on success, -1 on error.
 */
int cow_start_p3_thread(int sk, u64 dst_id, pid_t source_pid);

/*
 * Wait for the P3 bulk sender thread to complete.
 */
void cow_wait_p3_thread(void);

/*
 * Check if P3 thread is still running.
 */
bool cow_p3_thread_running(void);

/*
 * Get number of pages sent by P3 thread.
 */
unsigned long cow_p3_pages_sent(void);

#endif /* __CR_COW_BULK_SEND_H__ */
