/*
 * COW Bulk Page Sender - Optimized P3 (regular page) transfer
 *
 * Sends pages in batches of 64 (256KB) for better throughput:
 * - Single process_vm_readv for 64 pages
 * - Single LZ4 compression for 256KB
 * - Single socket send
 */

#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <pthread.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <string.h>
#include <lz4.h>

#include "int.h"
#include "page.h"
#include "types.h"
#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"
#include "mem.h"
#include "cow/cow-bulk-send.h"
#include "page-xfer.h"
#include "cow/cow-page-xfer.h"
#include "atomic-bitmap.h"
#include "cr_options.h"
#include "tls.h"
#include "pagemap.h"
#include "pagemap_scan.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bulk: "

#define COW_BATCH_PAGES 64
#define COW_BATCH_SIZE  (COW_BATCH_PAGES * PAGE_SIZE)

/*
 * Protocol structs, constants, and helpers are now in page-xfer.h:
 * - struct page_server_iov
 * - PS_CMD_BITS, encode_ps_cmd()
 * - page_server_send() (replaces __send)
 *
 * COW-specific protocol defines (PS_IOV_ADD_F_COMPRESS, etc.) are in cow-page-xfer.h
 */

/* NUM_P3_THREADS is defined in cow-bulk-send.h */
#define NUM_P3_SPLITTER_THREADS (NUM_P3_THREADS - 1)  /* Threads 1-(N-1) split large VMAs */
#define MIN_VMA_SIZE_FOR_SPLIT (256 * 1024)  /* 256KB threshold */

/* Per-thread state */
struct p3_thread_ctx {
	pthread_t thread;
	int thread_id;
	int socket;           /* Per-thread socket for parallel transfer */
	u64 dst_id;
	pid_t source_pid;
	unsigned long pages_sent;
	volatile bool active;
	volatile bool error;  /* Set if thread encountered an error */

	/* Dirty scan state (for iterative convergence) */
	int pagemap_fd;                  /* Per-thread /proc/<pid>/pagemap fd */
	unsigned long last_dirty_count;  /* Dirty pages found in last scan */
	volatile bool below_threshold;   /* True when last_dirty_count < threshold */
	unsigned int iteration;          /* 0=bulk, 1+=dirty scan */
};

static struct p3_thread_ctx p3_threads[NUM_P3_THREADS];
static volatile int p3_threads_active = 0;
static unsigned long p3_total_pages_sent = 0;

/* Global flag for signaling last scan (set by main thread after freeze) */
static volatile bool g_last_scan_flag = false;

/* New VMA ranges detected in Phase 3 - set by main thread before last scan */
static unsigned long *g_new_vma_ranges = NULL;  /* [start, len, start, len, ...] */
static unsigned int g_nr_new_vma_ranges = 0;

/*
 * Single Scanner + Multiple Senders Architecture
 * ===============================================
 * Scanner thread does all PAGEMAP_SCAN (single TLB flush per iteration)
 * and distributes dirty regions to sender threads via SPSC queues.
 */
static struct sender_queue sender_queues[NUM_P3_THREADS];
static volatile bool g_scan_complete = false;
static volatile bool g_scanner_freeze_signal = false;
static pthread_t scanner_thread_handle;
static pid_t g_scanner_source_pid;
static int g_scanner_pagemap_fd = -1;

/* Synchronization: scanner waits for bulk transfer to complete */
static volatile int g_bulk_transfer_done_count = 0;
static volatile int g_num_sender_threads = 0;

int cow_init_sender_queues(void)
{
	int i;

	for (i = 0; i < NUM_P3_THREADS; i++) {
		if (spsc_init(sender_queues[i].head, sender_queues[i].tail,
			      sender_queues[i].size,
			      struct dirty_region_spsc_node)) {
			pr_err("Failed to init sender queue %d\n", i);
			return -1;
		}
	}
	g_scan_complete = false;
	g_scanner_freeze_signal = false;
	pr_info("Initialized %d sender queues\n", NUM_P3_THREADS);
	return 0;
}

struct sender_queue *cow_get_sender_queue(int thread_id)
{
	if (thread_id < 0 || thread_id >= NUM_P3_THREADS)
		return NULL;
	return &sender_queues[thread_id];
}

bool cow_is_scan_complete(void)
{
	return __atomic_load_n(&g_scan_complete, __ATOMIC_ACQUIRE);
}

void cow_signal_scanner_freeze(void)
{
	pr_err("=== SCANNER: Signaling freeze ===\n");
	__atomic_store_n(&g_scanner_freeze_signal, true, __ATOMIC_RELEASE);
	__sync_synchronize();
}

/*
 * Scanner thread - single thread does all PAGEMAP_SCAN to minimize TLB flushes.
 * Distributes dirty regions to sender threads via SPSC queues (round-robin).
 */
static void *dirty_scanner_thread(void *arg)
{
	struct list_head *lazy_vmas;
	struct lazy_vma_entry *lve;
	struct page_region *regs;
	const int max_regs = 1000;
	unsigned int iteration = 0;
	char pagemap_path[64];
	struct timespec t_start, t_end;

	pr_err("Scanner thread started, source_pid=%d\n", g_scanner_source_pid);
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* Wait for all sender threads to complete bulk transfer first */
	pr_err("Scanner: waiting for %d sender threads to complete bulk transfer...\n",
	       g_num_sender_threads);
	while (__atomic_load_n(&g_bulk_transfer_done_count, __ATOMIC_ACQUIRE) <
	       __atomic_load_n(&g_num_sender_threads, __ATOMIC_ACQUIRE)) {
		/* Check if we should abort early */
		if (__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE))
			goto out;
		usleep(10000);  /* 10ms poll */
	}
	pr_err("Scanner: all sender threads completed bulk transfer, starting dirty scan\n");

	/* Open pagemap fd */
	snprintf(pagemap_path, sizeof(pagemap_path), "/proc/%d/pagemap",
		 g_scanner_source_pid);
	g_scanner_pagemap_fd = open(pagemap_path, O_RDWR);
	if (g_scanner_pagemap_fd < 0) {
		pr_perror("Scanner: cannot open %s", pagemap_path);
		goto out;
	}

	regs = xmalloc(max_regs * sizeof(struct page_region));
	if (!regs) {
		pr_err("Scanner: failed to allocate regs buffer\n");
		goto out_close;
	}

	lazy_vmas = get_global_lazy_vmas();

	/* Iterative dirty scanning until freeze signal */
	while (!__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE)) {
		unsigned long total_dirty_pages = 0;
		unsigned int queue_idx = 0;
		struct timespec iter_start, iter_end;

		iteration++;
		clock_gettime(CLOCK_MONOTONIC, &iter_start);

		/* Scan ALL VMAs in single pass */
		list_for_each_entry(lve, lazy_vmas, list) {
			struct pm_scan_arg args;
			long regs_len;

			memset(&args, 0, sizeof(args));
			args.size = sizeof(args);
			args.flags = PM_SCAN_WP_MATCHING;  /* Clear dirty bit after scan */
			args.start = lve->start;
			args.end = lve->end;
			args.walk_end = lve->start;
			args.vec = (u64)(unsigned long)regs;
			args.vec_len = max_regs;
			args.max_pages = 0;
			args.category_anyof_mask = PAGE_IS_WRITTEN;
			args.return_mask = PAGE_IS_WRITTEN;

			do {
				int i;
				args.start = args.walk_end;

				regs_len = ioctl(g_scanner_pagemap_fd, PAGEMAP_SCAN, &args);
				if (regs_len < 0) {
					pr_perror("Scanner: PAGEMAP_SCAN failed");
					break;
				}

				if (regs_len == 0)
					break;

				/* Distribute dirty regions to sender queues (round-robin) */
				for (i = 0; i < regs_len; i++) {
					struct dirty_region_entry *entry;
					unsigned long pages;

					pages = (regs[i].end - regs[i].start) / PAGE_SIZE;
					total_dirty_pages += pages;

					entry = xmalloc(sizeof(*entry));
					if (!entry) {
						pr_err("Scanner: failed to alloc dirty_region_entry\n");
						continue;
					}
					entry->start = regs[i].start;
					entry->end = regs[i].end;
					entry->dst_id = lve->dst_id;
					entry->source_pid = g_scanner_source_pid;

					/* Round-robin distribution to sender queues */
					spsc_enqueue(sender_queues[queue_idx].tail,
						     sender_queues[queue_idx].size,
						     entry, struct dirty_region_spsc_node);
					queue_idx = (queue_idx + 1) % NUM_P3_THREADS;
				}
			} while (args.walk_end < lve->end);
		}

		clock_gettime(CLOCK_MONOTONIC, &iter_end);
		{
			long iter_ms = (iter_end.tv_sec - iter_start.tv_sec) * 1000 +
				       (iter_end.tv_nsec - iter_start.tv_nsec) / 1000000;
			pr_err("Scanner: iter=%u, %lu dirty pages distributed, %ld ms\n",
			       iteration, total_dirty_pages, iter_ms);
		}

		/*
		 * Check convergence - signal freeze if < 1M dirty pages.
		 * Once below threshold, stop scanning and wait for freeze signal.
		 */
		if (total_dirty_pages < DIRTY_SCAN_FREEZE_THRESHOLD) {
			pr_err("Scanner: %lu pages < %d threshold, requesting freeze\n",
			       total_dirty_pages, DIRTY_SCAN_FREEZE_THRESHOLD);
			/* Signal main thread to freeze */
			g_last_scan_flag = true;
			/* Stop scanning - wait for freeze signal, then do final scan */
			break;
		}

		/* Brief sleep to let senders catch up */
		usleep(1000);
	}

	/* Wait for freeze signal from main thread */
	pr_err("Scanner: waiting for freeze signal...\n");
	while (!__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE)) {
		usleep(1000);
	}

	/* Final scan after freeze - single scan captures all remaining dirty pages */
	{
		unsigned long final_dirty = 0;
		unsigned int queue_idx = 0;
		struct timespec fs_start, fs_end;

		clock_gettime(CLOCK_MONOTONIC, &fs_start);
		pr_err("Scanner: final scan (frozen)\n");

		list_for_each_entry(lve, lazy_vmas, list) {
			struct pm_scan_arg args;
			long regs_len;

			memset(&args, 0, sizeof(args));
			args.size = sizeof(args);
			args.flags = PM_SCAN_WP_MATCHING;
			args.start = lve->start;
			args.end = lve->end;
			args.walk_end = lve->start;
			args.vec = (u64)(unsigned long)regs;
			args.vec_len = max_regs;
			args.max_pages = 0;
			args.category_anyof_mask = PAGE_IS_WRITTEN;
			args.return_mask = PAGE_IS_WRITTEN;

			do {
				int i;
				args.start = args.walk_end;

				regs_len = ioctl(g_scanner_pagemap_fd, PAGEMAP_SCAN, &args);
				if (regs_len < 0)
					break;
				if (regs_len == 0)
					break;

				for (i = 0; i < regs_len; i++) {
					struct dirty_region_entry *entry;

					final_dirty += (regs[i].end - regs[i].start) / PAGE_SIZE;

					entry = xmalloc(sizeof(*entry));
					if (!entry)
						continue;
					entry->start = regs[i].start;
					entry->end = regs[i].end;
					entry->dst_id = lve->dst_id;
					entry->source_pid = g_scanner_source_pid;

					spsc_enqueue(sender_queues[queue_idx].tail,
						     sender_queues[queue_idx].size,
						     entry, struct dirty_region_spsc_node);
					queue_idx = (queue_idx + 1) % NUM_P3_THREADS;
				}
			} while (args.walk_end < lve->end);
		}

		clock_gettime(CLOCK_MONOTONIC, &fs_end);
		{
			long fs_ms = (fs_end.tv_sec - fs_start.tv_sec) * 1000 +
				     (fs_end.tv_nsec - fs_start.tv_nsec) / 1000000;
			pr_err("Scanner: final scan done, %lu dirty pages, %ld ms\n",
			       final_dirty, fs_ms);
		}
	}

	xfree(regs);

out_close:
	if (g_scanner_pagemap_fd >= 0) {
		close(g_scanner_pagemap_fd);
		g_scanner_pagemap_fd = -1;
	}

out:
	clock_gettime(CLOCK_MONOTONIC, &t_end);
	{
		long elapsed_ms = (t_end.tv_sec - t_start.tv_sec) * 1000 +
				  (t_end.tv_nsec - t_start.tv_nsec) / 1000000;
		pr_err("Scanner thread done: %u iterations, %ld ms\n", iteration, elapsed_ms);
	}

	/* Signal senders that scanning is complete */
	pr_err("Scanner: setting g_scan_complete=true\n");
	__atomic_store_n(&g_scan_complete, true, __ATOMIC_RELEASE);
	pr_err("Scanner: g_scan_complete set, exiting\n");
	return NULL;
}

int cow_start_scanner_thread(pid_t source_pid)
{
	g_scanner_source_pid = source_pid;
	g_scan_complete = false;
	g_scanner_freeze_signal = false;

	if (pthread_create(&scanner_thread_handle, NULL, dirty_scanner_thread, NULL)) {
		pr_perror("Failed to create scanner thread");
		return -1;
	}

	pr_info("Started dirty scanner thread for pid %d\n", source_pid);
	return 0;
}

void cow_wait_scanner_thread(void)
{
	if (scanner_thread_handle) {
		pthread_join(scanner_thread_handle, NULL);
		scanner_thread_handle = 0;
		pr_info("Scanner thread joined\n");
	}
	/* Senders drain their own queues during normal exit */
}

void cow_set_new_vma_ranges(unsigned long *ranges, unsigned int nr_ranges)
{
	g_new_vma_ranges = ranges;
	g_nr_new_vma_ranges = nr_ranges;
	__sync_synchronize();  /* Memory barrier for ARM */
	pr_info("Set %u new VMA ranges for P3 threads to send\n", nr_ranges);
}

void cow_free_new_vma_ranges(void)
{
	if (g_new_vma_ranges) {
		xfree(g_new_vma_ranges);
		g_new_vma_ranges = NULL;
		g_nr_new_vma_ranges = 0;
	}
}

/*
 * Send a batch of pages with LZ4 compression.
 * Protocol: header (PS_IOV_ADD_F_COMPRESS) + compressed_size + compressed_data
 * Header contains nr_pages and base_vaddr.
 */
int send_pages_batch_compressed(int sk, const void *data,
				int nr_pages, u64 dst_id,
				unsigned long base_vaddr)
{
	/* Allocate buffer for: header + compressed_size + compressed_data */
	int max_compressed = LZ4_compressBound(nr_pages * PAGE_SIZE);
	int total_uncompressed = nr_pages * PAGE_SIZE;
	char *send_buf;
	struct page_server_iov *pi;
	int *compressed_size;
	char *compressed_data;
	int total_len, ret;

	send_buf = xmalloc(sizeof(struct page_server_iov) + sizeof(int) + max_compressed);
	if (!send_buf)
		return -1;

	pi = (struct page_server_iov *)send_buf;
	compressed_size = (int *)(send_buf + sizeof(*pi));
	compressed_data = send_buf + sizeof(*pi) + sizeof(int);

	/* Compress entire batch */
	*compressed_size = LZ4_compress_default(data, compressed_data,
						total_uncompressed, max_compressed);
	if (*compressed_size <= 0) {
		pr_err("LZ4 compression failed for batch at %lx (%d pages)\n",
		       base_vaddr, nr_pages);
		xfree(send_buf);
		return -1;
	}

	/* Track compression statistics (atomic for multi-threaded access) */
	__sync_fetch_and_add(&g_compress_uncompressed_bytes, total_uncompressed);
	__sync_fetch_and_add(&g_compress_compressed_bytes, *compressed_size);

	pr_debug("Compressed batch at %lx: %d pages, %d -> %d bytes (%.1f%%)\n",
		 base_vaddr, nr_pages, total_uncompressed, *compressed_size,
		 (float)(*compressed_size) * 100 / total_uncompressed);

	/* Fill header - use nr_pages to indicate batch size */
	pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS, PE_PRESENT);
	pi->nr_pages = nr_pages;
	pi->vaddr = base_vaddr;
	pi->dst_id = dst_id;

	/* Single send: header + size + compressed data */
	total_len = sizeof(*pi) + sizeof(int) + *compressed_size;
	ret = page_server_send(sk, send_buf, total_len, 0);

	xfree(send_buf);

	if (ret != total_len) {
		pr_perror("Failed to send compressed batch (sent %d/%d)", ret, total_len);
		return -1;
	}

	return 0;
}

/*
 * Read and send a batch of contiguous pages from source process.
 * Returns number of pages actually sent, or -1 on error.
 */
static int send_lazy_vma_pages_batch(int sk, struct lazy_vma_entry *lve,
				     unsigned long base_vaddr, int max_pages,
				     u64 dst_id, pid_t source_pid)
{
	void *buffer;
	struct iovec local_iov, remote_iov;
	int nr_pages = 0;
	unsigned long vaddr;
	int ret, i;

	/* Find contiguous run of pages from base_vaddr */
	for (i = 0; i < max_pages; i++) {
		vaddr = base_vaddr + i * PAGE_SIZE;
		if (vaddr >= lve->end)
			break;
		nr_pages++;
	}

	if (nr_pages == 0)
		return 0;

	/* Allocate buffer for batch */
	buffer = xmalloc(nr_pages * PAGE_SIZE);
	if (!buffer)
		return -1;

	/* Single process_vm_readv for all pages */
	local_iov.iov_base = buffer;
	local_iov.iov_len = nr_pages * PAGE_SIZE;
	remote_iov.iov_base = (void *)base_vaddr;
	remote_iov.iov_len = nr_pages * PAGE_SIZE;

	ret = process_vm_readv(source_pid, &local_iov, 1, &remote_iov, 1, 0);
	if (ret != (ssize_t)(nr_pages * PAGE_SIZE)) {
		pr_perror("Failed to read %d pages at %lx from pid %d (got %d)",
			  nr_pages, base_vaddr, source_pid, ret);
		xfree(buffer);
		return -1;
	}

	/* Compress and send batch */
	ret = send_pages_batch_compressed(sk, buffer, nr_pages, dst_id, base_vaddr);
	xfree(buffer);

	if (ret < 0)
		return -1;

	return nr_pages;
}

/*
 * Send pages from a dirty region entry (from scanner queue).
 * Returns number of pages sent, or -1 on error.
 */
static int send_dirty_region(struct p3_thread_ctx *ctx,
			     struct dirty_region_entry *region)
{
	void *buffer;
	struct iovec local_iov, remote_iov;
	unsigned long vaddr;
	int total_sent = 0;

	buffer = xmalloc(COW_BATCH_PAGES * PAGE_SIZE);
	if (!buffer)
		return -1;

	for (vaddr = region->start; vaddr < region->end;
	     vaddr += COW_BATCH_PAGES * PAGE_SIZE) {
		int batch_pages = (region->end - vaddr) / PAGE_SIZE;
		ssize_t ret;

		if (batch_pages > COW_BATCH_PAGES)
			batch_pages = COW_BATCH_PAGES;

		/* Read pages from source process */
		local_iov.iov_base = buffer;
		local_iov.iov_len = batch_pages * PAGE_SIZE;
		remote_iov.iov_base = (void *)vaddr;
		remote_iov.iov_len = batch_pages * PAGE_SIZE;

		ret = process_vm_readv(region->source_pid, &local_iov, 1,
				       &remote_iov, 1, 0);
		if (ret != (ssize_t)(batch_pages * PAGE_SIZE)) {
			pr_debug("P3[%d] failed to read dirty region at %lx: %s\n",
				 ctx->thread_id, vaddr, strerror(errno));
			/* Skip this batch, continue with next */
			continue;
		}

		/* Compress and send */
		ret = send_pages_batch_compressed(ctx->socket, buffer,
						  batch_pages, region->dst_id, vaddr);
		if (ret < 0) {
			pr_err("P3[%d] failed to send dirty region at %lx\n",
			       ctx->thread_id, vaddr);
			xfree(buffer);
			return -1;
		}

		total_sent += batch_pages;
		ctx->pages_sent += batch_pages;
	}

	xfree(buffer);
	return total_sent;
}

/*
 * Calculate this thread's chunk of a VMA for parallel processing.
 * Returns true if this thread should process the VMA, false to skip.
 */
static bool get_thread_vma_range(struct p3_thread_ctx *ctx,
				 struct lazy_vma_entry *lve,
				 unsigned long *out_start,
				 unsigned long *out_end)
{
	unsigned long vma_size = lve->end - lve->start;
	unsigned long chunk_size;
	int thread_id = ctx->thread_id;

	if (lve->dst_id != ctx->dst_id)
		return false;

	if (vma_size < MIN_VMA_SIZE_FOR_SPLIT) {
		/* Small VMAs (< 256KB) - only thread 0 handles them */
		if (thread_id != 0)
			return false;
		*out_start = lve->start;
		*out_end = lve->end;
	} else {
		/* Large VMAs (>= 256KB) - threads 1-N split them */
		if (thread_id == 0)
			return false;

		chunk_size = vma_size / NUM_P3_SPLITTER_THREADS;
		chunk_size = (chunk_size / PAGE_SIZE) * PAGE_SIZE;

		*out_start = lve->start + (thread_id - 1) * chunk_size;

		if (thread_id == NUM_P3_THREADS - 1)
			*out_end = lve->end;
		else
			*out_end = *out_start + chunk_size;
	}

	return true;
}

/*
 * Scan for dirty pages in this thread's VMA ranges and send them.
 * Uses PAGEMAP_SCAN with PM_SCAN_WP_MATCHING to atomically detect and clear dirty bits.
 * Returns total number of dirty pages found and sent.
 */
static unsigned long do_dirty_scan_and_send(struct p3_thread_ctx *ctx)
{
	struct lazy_vma_entry *lve;
	struct list_head *lazy_vmas;
	struct page_region *regs = NULL;
	unsigned long total_dirty = 0;
	int thread_id = ctx->thread_id;
	const int max_regs = 1000;

	regs = xmalloc(max_regs * sizeof(struct page_region));
	if (!regs) {
		pr_err("P3[%d] dirty scan: failed to allocate regs buffer\n", thread_id);
		return 0;
	}

	lazy_vmas = get_global_lazy_vmas();

	list_for_each_entry(lve, lazy_vmas, list) {
		unsigned long my_start, my_end;
		struct pm_scan_arg args;
		long regs_len;

		if (!get_thread_vma_range(ctx, lve, &my_start, &my_end))
			continue;

		memset(&args, 0, sizeof(args));
		args.size = sizeof(args);
		args.flags = PM_SCAN_WP_MATCHING;  /* Clear dirty bit after scan */
		args.start = my_start;
		args.end = my_end;
		args.walk_end = my_start;
		args.vec = (u64)(unsigned long)regs;
		args.vec_len = max_regs;
		args.max_pages = 0;
		args.category_anyof_mask = PAGE_IS_WRITTEN;
		args.return_mask = PAGE_IS_WRITTEN;

		do {
			int i;
			args.start = args.walk_end;

			regs_len = ioctl(ctx->pagemap_fd, PAGEMAP_SCAN, &args);
			if (regs_len < 0) {
				pr_perror("P3[%d] PAGEMAP_SCAN failed", thread_id);
				break;
			}

			if (regs_len == 0)
				break;

			/* Process each dirty region */
			for (i = 0; i < regs_len; i++) {
				unsigned long start = regs[i].start;
				unsigned long end = regs[i].end;
				unsigned long pages = (end - start) / PAGE_SIZE;
				unsigned long vaddr;

				pr_debug("P3[%d] dirty region: 0x%lx-0x%lx (%lu pages)\n",
					 thread_id, start, end, pages);

				/* Send dirty pages in batches */
				for (vaddr = start; vaddr < end;
				     vaddr += COW_BATCH_PAGES * PAGE_SIZE) {
					int batch_pages = (end - vaddr) / PAGE_SIZE;
					int sent;

					if (batch_pages > COW_BATCH_PAGES)
						batch_pages = COW_BATCH_PAGES;

					sent = send_lazy_vma_pages_batch(
						ctx->socket, lve, vaddr, batch_pages,
						ctx->dst_id, ctx->source_pid);

					if (sent < 0) {
						pr_err("P3[%d] dirty scan: send failed at 0x%lx\n",
						       thread_id, vaddr);
						goto out;
					}

					total_dirty += sent;
					ctx->pages_sent += sent;
				}
			}
		} while (args.walk_end < my_end);
	}

out:
	xfree(regs);
	return total_dirty;
}

/*
 * Send all pages from new VMAs detected in Phase 3.
 * New VMAs need ALL their pages sent (not just dirty), split among threads.
 */
static unsigned long send_new_vma_pages(struct p3_thread_ctx *ctx)
{
	unsigned int i;
	unsigned long total_sent = 0;
	unsigned int ranges_per_thread, my_start_idx, my_end_idx;
	void *buffer;
	struct iovec local_iov, remote_iov;
	int thread_id = ctx->thread_id;

	if (!g_new_vma_ranges || g_nr_new_vma_ranges == 0)
		return 0;

	/* Split ranges among threads */
	ranges_per_thread = (g_nr_new_vma_ranges + NUM_P3_THREADS - 1) / NUM_P3_THREADS;
	my_start_idx = thread_id * ranges_per_thread;
	my_end_idx = my_start_idx + ranges_per_thread;
	if (my_end_idx > g_nr_new_vma_ranges)
		my_end_idx = g_nr_new_vma_ranges;

	if (my_start_idx >= g_nr_new_vma_ranges)
		return 0;  /* No ranges for this thread */

	pr_info("P3[%d] sending new VMA pages: ranges %u-%u of %u\n",
		thread_id, my_start_idx, my_end_idx, g_nr_new_vma_ranges);

	buffer = xmalloc(COW_BATCH_PAGES * PAGE_SIZE);
	if (!buffer)
		return 0;

	for (i = my_start_idx; i < my_end_idx; i++) {
		unsigned long start = g_new_vma_ranges[i * 2];
		unsigned long len = g_new_vma_ranges[i * 2 + 1];
		unsigned long vaddr;

		pr_debug("P3[%d] new VMA %lx-%lx (%lu pages)\n",
			 thread_id, start, start + len, len / PAGE_SIZE);

		for (vaddr = start; vaddr < start + len; ) {
			int batch_pages = (start + len - vaddr) / PAGE_SIZE;
			ssize_t ret;

			if (batch_pages > COW_BATCH_PAGES)
				batch_pages = COW_BATCH_PAGES;

			/* Read pages from source process */
			local_iov.iov_base = buffer;
			local_iov.iov_len = batch_pages * PAGE_SIZE;
			remote_iov.iov_base = (void *)vaddr;
			remote_iov.iov_len = batch_pages * PAGE_SIZE;

			ret = process_vm_readv(ctx->source_pid, &local_iov, 1,
					       &remote_iov, 1, 0);
			if (ret != (ssize_t)(batch_pages * PAGE_SIZE)) {
				pr_warn("P3[%d] failed to read new VMA pages at %lx: %s\n",
					thread_id, vaddr, strerror(errno));
				vaddr += batch_pages * PAGE_SIZE;
				continue;
			}

			/* Send compressed batch */
			ret = send_pages_batch_compressed(ctx->socket, buffer,
							  batch_pages, ctx->dst_id, vaddr);
			if (ret < 0) {
				pr_err("P3[%d] failed to send new VMA pages at %lx, aborting\n",
				       thread_id, vaddr);
				xfree(buffer);
				return total_sent;  /* Abort - socket is likely broken */
			}

			total_sent += batch_pages;
			ctx->pages_sent += batch_pages;
			vaddr += batch_pages * PAGE_SIZE;
		}
	}

	xfree(buffer);
	pr_info("P3[%d] sent %lu pages from new VMAs\n", thread_id, total_sent);
	return total_sent;
}

/*
 * P3 bulk sender thread - sends regular pages in batches.
 * Each thread handles 1/NUM_P3_THREADS of each VMA's address range.
 * After bulk transfer, transitions to iterative dirty scanning until convergence.
 */
static void *p3_bulk_sender_thread(void *arg)
{
	struct p3_thread_ctx *ctx = (struct p3_thread_ctx *)arg;
	struct lazy_vma_entry *lve;
	struct list_head *lazy_vmas;
	unsigned long total_sent = 0;
	struct timespec t_start, t_end;
	int thread_id = ctx->thread_id;

	pr_info("P3[%d] bulk sender thread started (batch=%d pages)\n",
		thread_id, COW_BATCH_PAGES);
	pr_debug("DEBUG_THREAD: P3 sender[%d] STARTED socket=%d dst_id=%lu\n",
	       thread_id, ctx->socket, (unsigned long)ctx->dst_id);
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* No longer need pagemap fd - scanner thread handles PAGEMAP_SCAN */
	ctx->pagemap_fd = -1;

	/* Initialize state */
	ctx->iteration = 0;
	ctx->last_dirty_count = 0;
	ctx->below_threshold = false;

	lazy_vmas = get_global_lazy_vmas();

	/* === Iteration 0: Bulk transfer === */
	{
		struct timespec bulk_start, bulk_end;
		long bulk_elapsed_ms;
		int vma_count = 0;

		clock_gettime(CLOCK_MONOTONIC, &bulk_start);
		pr_err("P3[%d]: Starting bulk transfer, scanning lazy_vmas\n", thread_id);
		list_for_each_entry(lve, lazy_vmas, list) {
			vma_count++;
		}
		pr_err("P3[%d]: Found %d VMAs in lazy_vmas list\n", thread_id, vma_count);

		list_for_each_entry(lve, lazy_vmas, list) {
			unsigned long my_start, my_end;
			unsigned long vaddr;
			unsigned long vma_size = lve->end - lve->start;

			pr_info("P3[%d]: Checking VMA %lx-%lx (%lu KB) dst_id=%lu (my dst_id=%lu)\n",
				thread_id, lve->start, lve->end, vma_size / 1024,
				lve->dst_id, ctx->dst_id);

			if (!get_thread_vma_range(ctx, lve, &my_start, &my_end)) {
				pr_info("P3[%d]: -> Skipped by get_thread_vma_range\n", thread_id);
				continue;
			}

			pr_info("P3[%d]: Bulk VMA %lx-%lx chunk %lx-%lx\n",
				thread_id,
				(unsigned long)lve->start, (unsigned long)lve->end,
				my_start, my_end);

			for (vaddr = my_start; vaddr < my_end;
			     vaddr += COW_BATCH_PAGES * PAGE_SIZE) {
				int batch_pages;
				int sent;

				batch_pages = (my_end - vaddr) / PAGE_SIZE;
				if (batch_pages > COW_BATCH_PAGES)
					batch_pages = COW_BATCH_PAGES;

				sent = send_lazy_vma_pages_batch(
					ctx->socket, lve, vaddr, batch_pages,
					ctx->dst_id, ctx->source_pid);

				if (sent < 0) {
					pr_err("P3[%d]: Failed to send batch at %lx\n",
					       thread_id, vaddr);
					ctx->error = true;
					goto out;
				}

				total_sent += sent;
			}
		}

		clock_gettime(CLOCK_MONOTONIC, &bulk_end);
		bulk_elapsed_ms = (bulk_end.tv_sec - bulk_start.tv_sec) * 1000 +
				  (bulk_end.tv_nsec - bulk_start.tv_nsec) / 1000000;
		pr_err("P3[%d] TIMING: Bulk transfer done: %lu pages in %ld ms\n",
		       thread_id, total_sent, bulk_elapsed_ms);

		/* Signal scanner that this thread's bulk transfer is complete */
		__atomic_fetch_add(&g_bulk_transfer_done_count, 1, __ATOMIC_RELEASE);
	}

	/* === Phase 2: Consume dirty regions from scanner queue === */
	{
		struct timespec loop_start, loop_end;
		long loop_elapsed_ms;
		unsigned long loop_total_pages = 0;
		unsigned long regions_processed = 0;
		unsigned long wait_count = 0;
		struct sender_queue *my_queue = cow_get_sender_queue(thread_id);

		clock_gettime(CLOCK_MONOTONIC, &loop_start);
		pr_err("P3[%d] starting queue consumption, queue=%p\n", thread_id, (void *)my_queue);

		/* Consume dirty regions from queue until scanner completes */
		while (!cow_is_scan_complete() || spsc_peek(my_queue->head)) {
			struct dirty_region_entry *region;
			int sent;

			/* Periodic status logging */
			if (regions_processed > 0 && regions_processed % 10000 == 0) {
				pr_err("P3[%d] queue progress: processed=%lu, queue_size=%lu, scan_complete=%d\n",
				       thread_id, regions_processed, spsc_size(my_queue->size),
				       cow_is_scan_complete());
			}

			region = spsc_dequeue(my_queue->head, my_queue->size);
			if (!region) {
				/* Queue empty, brief wait */
				wait_count++;
				if (wait_count % 10000 == 0) {
					pr_err("P3[%d] waiting: queue empty, scan_complete=%d, wait_count=%lu, peek=%d\n",
					       thread_id, cow_is_scan_complete(), wait_count,
					       spsc_peek(my_queue->head));
				}
				usleep(100);
				continue;
			}
			wait_count = 0;

			/* Send the dirty region */
			sent = send_dirty_region(ctx, region);
			if (sent > 0) {
				loop_total_pages += sent;
				regions_processed++;
			}
			xfree(region);
		}
		pr_err("P3[%d] exiting queue loop: scan_complete=%d, peek=%d\n",
		       thread_id, cow_is_scan_complete(), spsc_peek(my_queue->head));

		clock_gettime(CLOCK_MONOTONIC, &loop_end);
		loop_elapsed_ms = (loop_end.tv_sec - loop_start.tv_sec) * 1000 +
				  (loop_end.tv_nsec - loop_start.tv_nsec) / 1000000;
		pr_err("P3[%d] TIMING: Queue consumption done: %lu regions, %lu pages in %ld ms\n",
		       thread_id, regions_processed, loop_total_pages, loop_elapsed_ms);

		/* Mark as below threshold for compatibility */
		ctx->below_threshold = true;

		/* Skip legacy dirty scan code */
		goto skip_legacy_dirty_scan;

		/* Legacy code - kept for reference but skipped */
		while (!g_last_scan_flag) {
			ctx->iteration++;
			ctx->last_dirty_count = do_dirty_scan_and_send(ctx);

			ctx->below_threshold =
				(ctx->last_dirty_count < DIRTY_CONVERGENCE_THRESHOLD) || (ctx->iteration > 3);

			pr_info("P3[%d] iter=%u dirty=%lu threshold=%s\n",
				thread_id, ctx->iteration, ctx->last_dirty_count,
				ctx->below_threshold ? "YES" : "NO");

			/*
			 * Thread 0 handles small VMAs - once below threshold, stop scanning
			 * and just wait for final scan signal to avoid busy-looping.
			 */
			if (thread_id == 0 && ctx->below_threshold) {
				pr_info("P3[0] below threshold, waiting for final scan signal\n");
				while (!g_last_scan_flag)
					usleep(10000);  /* 10ms poll */
				break;
			}

			/*
			 * Sleep to reduce CPU burn and kernel lock contention when
			 * there's little dirty page activity or after initial iterations.
			 */
			if (ctx->last_dirty_count < 1000 || ctx->iteration > 3)
				usleep(30000);  /* 30ms */
		}
	}

skip_legacy_dirty_scan:
	/* === Final: Send pages from new VMAs detected in Phase 3 === */
	ctx->iteration++;
	{
		struct timespec fs_start, fs_end;
		long fs_elapsed_ms;
		unsigned long new_vma_pages = 0;

		clock_gettime(CLOCK_MONOTONIC, &fs_start);
		pr_err("P3[%d] sending new VMA pages (if any)\n", thread_id);

		/* Send pages from new VMAs detected in Phase 3 */
		new_vma_pages = send_new_vma_pages(ctx);

		clock_gettime(CLOCK_MONOTONIC, &fs_end);

		fs_elapsed_ms = (fs_end.tv_sec - fs_start.tv_sec) * 1000 +
				(fs_end.tv_nsec - fs_start.tv_nsec) / 1000000;
		pr_err("P3[%d] new VMA pages done: %lu pages, TIMING: %ld ms\n",
		       thread_id, new_vma_pages, fs_elapsed_ms);
	}

out:
	clock_gettime(CLOCK_MONOTONIC, &t_end);
	{
		long elapsed_ms = (t_end.tv_sec - t_start.tv_sec) * 1000 +
				  (t_end.tv_nsec - t_start.tv_nsec) / 1000000;

		pr_err("P3[%d] done: %lu pages, %u iterations, %ld ms\n",
		       thread_id, ctx->pages_sent, ctx->iteration, elapsed_ms);
	}

	ctx->active = false;
	__sync_fetch_and_sub(&p3_threads_active, 1);
	return NULL;
}

int cow_start_p3_threads(int *sockets, int num_sockets, u64 dst_id, pid_t source_pid)
{
	int i;
	int threads_to_start;

	if (p3_threads_active > 0) {
		pr_warn("P3 threads already running\n");
		return 0;
	}

	/* Reset global flags */
	g_last_scan_flag = false;
	g_scan_complete = false;
	g_scanner_freeze_signal = false;
	g_bulk_transfer_done_count = 0;

	/* Initialize sender queues */
	if (cow_init_sender_queues()) {
		pr_err("Failed to initialize sender queues\n");
		return -1;
	}

	/* Calculate number of threads to start (before starting scanner) */
	threads_to_start = num_sockets < NUM_P3_THREADS ? num_sockets : NUM_P3_THREADS;
	g_num_sender_threads = threads_to_start;

	/* Start scanner thread */
	if (cow_start_scanner_thread(source_pid)) {
		pr_err("Failed to start scanner thread\n");
		return -1;
	}

	/* Start one sender thread per socket */
	p3_total_pages_sent = 0;

	for (i = 0; i < threads_to_start; i++) {
		p3_threads[i].thread_id = i;
		p3_threads[i].socket = sockets[i];
		p3_threads[i].dst_id = dst_id;
		p3_threads[i].source_pid = source_pid;
		p3_threads[i].pages_sent = 0;
		p3_threads[i].active = true;
		p3_threads[i].error = false;
		p3_threads[i].thread = 0;
		/* Initialize dirty scan state */
		p3_threads[i].pagemap_fd = -1;
		p3_threads[i].last_dirty_count = 0;
		p3_threads[i].below_threshold = false;
		p3_threads[i].iteration = 0;
		__sync_fetch_and_add(&p3_threads_active, 1);

		if (pthread_create(&p3_threads[i].thread, NULL,
				   p3_bulk_sender_thread, &p3_threads[i])) {
			pr_perror("Failed to create P3 thread %d", i);
			p3_threads[i].active = false;
			__sync_fetch_and_sub(&p3_threads_active, 1);
			/* Continue with remaining threads */
		}
	}

	pr_info("Started %d P3 bulk sender threads (%d sockets)\n",
		p3_threads_active, threads_to_start);
	return p3_threads_active > 0 ? 0 : -1;
}

/* Legacy single-thread interface for backward compatibility */
int cow_start_p3_thread(int sk, u64 dst_id, pid_t source_pid)
{
	int sockets[1] = { sk };
	return cow_start_p3_threads(sockets, 1, dst_id, source_pid);
}

void cow_wait_p3_threads(void)
{
	int i;
	unsigned long total = 0;
	int errors = 0;

	/* Wait for scanner thread first */
	cow_wait_scanner_thread();

	/* Then wait for sender threads */
	for (i = 0; i < NUM_P3_THREADS; i++) {
		if (p3_threads[i].thread) {
			pr_debug("DEBUG_THREAD: Waiting for P3 sender[%d] to join\n", i);
			pthread_join(p3_threads[i].thread, NULL);
			pr_debug("DEBUG_THREAD: P3 sender[%d] JOINED pages=%lu error=%d\n",
			       i, p3_threads[i].pages_sent, p3_threads[i].error);
			total += p3_threads[i].pages_sent;
			if (p3_threads[i].error)
				errors++;
			p3_threads[i].thread = 0;
		}
	}

	p3_total_pages_sent = total;
	p3_threads_active = 0;

	if (errors > 0)
		pr_warn("P3 threads completed with %d errors, %lu pages sent\n",
			errors, total);
	else
		pr_info("All P3 threads joined: %lu total pages\n", total);
}

/* Legacy single-thread interface */
void cow_wait_p3_thread(void)
{
	cow_wait_p3_threads();
}

bool cow_p3_thread_running(void)
{
	return p3_threads_active > 0;
}

unsigned long cow_p3_pages_sent(void)
{
	return p3_total_pages_sent;
}

int cow_get_num_p3_threads(void)
{
	return NUM_P3_THREADS;
}

/*
 * Check if ready to freeze.
 * In scanner architecture: returns true when scanner signals freeze (g_last_scan_flag).
 * This replaces the old per-thread threshold check.
 */
bool cow_all_threads_below_threshold(void)
{
	/* Scanner decides when to freeze based on total dirty pages < 1M */
	return g_last_scan_flag && p3_threads_active > 0;
}

/*
 * Signal P3 threads to do final scan and exit.
 * Called by main thread after freezing the process.
 */
void cow_signal_last_scan(void)
{
	pr_err("=== CONVERGENCE: Signaling last scan ===\n");
	g_last_scan_flag = true;
	cow_signal_scanner_freeze();  /* Signal scanner to do final scan */
	__sync_synchronize();  /* Memory barrier */
}

/*
 * Check if last scan has been signaled.
 */
bool cow_is_last_scan_signaled(void)
{
	return g_last_scan_flag;
}
