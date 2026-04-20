#ifndef __CR_COW_BULK_SEND_H__
#define __CR_COW_BULK_SEND_H__

#include "int.h"
#include "cow/spmc-queue.h"
#include "cow/cow-conf.h"

/*
 * Dirty region entry - passed from scanner thread to sender threads via SPMC queue.
 * Each entry represents a contiguous range of dirty pages to be transferred.
 */
struct dirty_region_entry {
	unsigned long start;      /* Start address of dirty region */
	unsigned long end;        /* End address of dirty region */
	u64 dst_id;               /* Destination image ID */
	pid_t source_pid;         /* Source process PID */
};

/* SPMC queue node for dirty regions (single producer, multi consumer for work stealing) */
DECLARE_SPMC_NODE(dirty_region, struct dirty_region_entry);

/*
 * Per-sender-thread queue for receiving dirty regions from scanner.
 * SPMC allows work stealing from other threads' queues.
 * Cache-line padded to avoid false sharing.
 */
struct sender_queue {
	struct dirty_region_spmc_node *head;
	char _pad1[COW_CACHE_LINE_SIZE - sizeof(struct dirty_region_spmc_node *)];
	struct dirty_region_spmc_node *tail;
	char _pad2[COW_CACHE_LINE_SIZE - sizeof(struct dirty_region_spmc_node *)];
	unsigned long size;
	char _pad3[COW_CACHE_LINE_SIZE - sizeof(unsigned long)];
};

/*
 * Send a batch of pages with LZ4 compression.
 * Used by bulk sender and dirty page dump.
 */
int send_pages_batch_compressed(int sk, const void *data,
				int nr_pages, u64 dst_id,
				unsigned long base_vaddr);


/*
 * Initialize sender queues (one per thread).
 * Returns 0 on success, -1 on error.
 */
int cow_init_sender_queues(void);

/*
 * Start the dirty scanner thread.
 * Scanner scans all VMAs and distributes dirty regions to sender queues.
 * Returns 0 on success, -1 on error.
 */
int cow_start_scanner_thread(pid_t source_pid);

/*
 * Signal scanner to do final scan and exit.
 */
void cow_signal_scanner_freeze(void);

/*
 * Wait for scanner thread to complete.
 */
void cow_wait_scanner_thread(void);

/*
 * Check if scanner has completed (for senders to know when to exit).
 */
bool cow_is_scan_complete(void);

/*
 * Get sender queue for a thread.
 */
struct sender_queue *cow_get_sender_queue(int thread_id);

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

/*
 * Check if all P3 threads are below dirty page convergence threshold.
 * Returns true only when ALL active threads report < COW_DIRTY_CONVERGENCE_THRESHOLD.
 */
bool cow_all_threads_below_threshold(void);

/*
 * Signal P3 threads to do final scan and exit.
 * Called by main thread after freezing the process.
 */
void cow_signal_last_scan(void);

/*
 * Check if last scan has been signaled.
 */
bool cow_is_last_scan_signaled(void);

/*
 * Set new VMA ranges detected in Phase 3 for P3 threads to send.
 * ranges: array of [start, len, start, len, ...] pairs
 * nr_ranges: number of ranges
 */
void cow_set_new_vma_ranges(unsigned long *ranges, unsigned int nr_ranges);

/*
 * Free new VMA ranges after P3 threads complete.
 */
void cow_free_new_vma_ranges(void);

#ifdef CONFIG_HAS_LIBBPF
/*
 * Drain BPF ring buffer and distribute dirty regions to sender queues.
 * Called at freeze time when using BPF mode (no scanner threads).
 * Returns number of dirty pages, or -1 on error.
 * BUG() if ring buffer overflow detected.
 */
int cow_bpf_drain_to_queues(void);

/*
 * Check if using BPF mode (no scanner threads).
 */
bool cow_using_bpf_mode(void);
#endif /* CONFIG_HAS_LIBBPF */

#ifdef SCAN_COMPARE
/*
 * DEBUG: Compare BPF vs PAGEMAP_SCAN at freeze time.
 * Call after freeze. Exits after comparison - no page transfer.
 */
void cow_debug_scan_compare(void);
#endif

#endif /* __CR_COW_BULK_SEND_H__ */
