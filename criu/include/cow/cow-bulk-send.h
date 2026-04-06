#ifndef __CR_COW_BULK_SEND_H__
#define __CR_COW_BULK_SEND_H__

#include "int.h"

#define COW_BATCH_PAGES 64
#define COW_BATCH_SIZE  (COW_BATCH_PAGES * PAGE_SIZE)  /* 256KB */

/*
 * Send a batch of pages with LZ4 compression.
 * Used by bulk sender and dirty page dump.
 */
int send_pages_batch_compressed(int sk, const void *data,
				int nr_pages, u64 dst_id,
				unsigned long base_vaddr);

/* Number of parallel P3 threads for bulk transfer */
#define NUM_P3_THREADS 20

/*
 * Start multiple P3 bulk sender threads (up to 20 threads for parallel transfer).
 * Each thread handles 1/N of each VMA's address range and has its own socket.
 * sockets: array of socket file descriptors (one per thread)
 * num_sockets: number of sockets/threads to start (capped at 20)
 * Returns 0 on success, -1 on error.
 */
int cow_start_p3_threads(int *sockets, int num_sockets, u64 dst_id, pid_t source_pid);

/*
 * Wait for all P3 bulk sender threads to complete.
 */
void cow_wait_p3_threads(void);

/*
 * Legacy single-thread interface (backward compatible).
 */
int cow_start_p3_thread(int sk, u64 dst_id, pid_t source_pid);
void cow_wait_p3_thread(void);

/*
 * Check if any P3 thread is still running.
 */
bool cow_p3_thread_running(void);

/*
 * Get total number of pages sent by all P3 threads.
 */
unsigned long cow_p3_pages_sent(void);

/*
 * Get the number of P3 threads (for creating sockets).
 */
int cow_get_num_p3_threads(void);

#endif /* __CR_COW_BULK_SEND_H__ */
