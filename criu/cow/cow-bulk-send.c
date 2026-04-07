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
	unsigned long page_idx_base;
	int ret, i;

	/* Find contiguous run of unsent, non-COW pages from base_vaddr */
	page_idx_base = (base_vaddr - lve->start) / PAGE_SIZE;

	for (i = 0; i < max_pages; i++) {
		unsigned long page_idx = 0;
		vaddr = base_vaddr + i * PAGE_SIZE;
		if (vaddr >= lve->end)
			break;

		 page_idx = page_idx_base + i;

		if (atomic_bitmap_test(lve->sent_bitmap, page_idx))
			break;  /* Stop at first already-sent */

		if (lve->cow_bitmap && atomic_bitmap_test(lve->cow_bitmap, page_idx))
			break;  /* Stop at first COW page */

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

	/* Mark all pages as sent (atomic for multi-threaded access) */
	for (i = 0; i < nr_pages; i++) {
		unsigned long page_idx = page_idx_base + i;
		/* atomic_bitmap_test_and_set returns true if already set */
		if (!atomic_bitmap_test_and_set(lve->sent_bitmap, page_idx))
			__sync_fetch_and_add(&lve->sent_pages, 1);
	}

	return nr_pages;
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
	char pagemap_path[64];

	pr_info("P3[%d] bulk sender thread started (batch=%d pages)\n",
		thread_id, COW_BATCH_PAGES);
	pr_debug("DEBUG_THREAD: P3 sender[%d] STARTED socket=%d dst_id=%lu\n",
	       thread_id, ctx->socket, (unsigned long)ctx->dst_id);
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* Open pagemap fd for dirty scanning */
	snprintf(pagemap_path, sizeof(pagemap_path), "/proc/%d/pagemap",
		 ctx->source_pid);
	ctx->pagemap_fd = open(pagemap_path, O_RDWR);
	if (ctx->pagemap_fd < 0) {
		pr_perror("P3[%d] cannot open %s", thread_id, pagemap_path);
		ctx->error = true;
		goto out_no_pagemap;
	}

	/* Initialize dirty scan state */
	ctx->iteration = 0;
	ctx->last_dirty_count = 0;
	ctx->below_threshold = false;

	lazy_vmas = get_global_lazy_vmas();

	/* === Iteration 0: Bulk transfer === */
	{
		int vma_count = 0;
		pr_err("P3[%d]: Starting bulk transfer, scanning lazy_vmas\n", thread_id);
		list_for_each_entry(lve, lazy_vmas, list) {
			vma_count++;
		}
		pr_err("P3[%d]: Found %d VMAs in lazy_vmas list\n", thread_id, vma_count);
	}
	list_for_each_entry(lve, lazy_vmas, list) {
		unsigned long my_start, my_end;
		unsigned long vaddr;
		unsigned long vma_size = lve->end - lve->start;

		pr_err("P3[%d]: Checking VMA %lx-%lx (%lu KB) dst_id=%lu (my dst_id=%lu)\n",
		       thread_id, lve->start, lve->end, vma_size / 1024,
		       lve->dst_id, ctx->dst_id);

		if (!get_thread_vma_range(ctx, lve, &my_start, &my_end)) {
			pr_err("P3[%d]: -> Skipped by get_thread_vma_range\n", thread_id);
			continue;
		}

		pr_err("P3[%d]: Bulk VMA %lx-%lx chunk %lx-%lx\n",
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

	pr_err("P3[%d] bulk transfer done: %lu pages, starting dirty scan loop\n",
		thread_id, total_sent);

	/* === Iterations 1+: Dirty scan loop until convergence === */
	while (!g_last_scan_flag) {
		ctx->iteration++;
		ctx->last_dirty_count = do_dirty_scan_and_send(ctx);

		ctx->below_threshold =
			(ctx->last_dirty_count < DIRTY_CONVERGENCE_THRESHOLD);

		pr_err("P3[%d] iter=%u dirty=%lu threshold=%s\n",
		       thread_id, ctx->iteration, ctx->last_dirty_count,
		       ctx->below_threshold ? "YES" : "NO");

		/*
		 * Small sleep to avoid busy-looping when there are few dirty pages.
		 * This gives the application time to dirty more pages.
		 */
		if (ctx->last_dirty_count < 100)
			usleep(1000);  /* 1ms */
	}

	/* === Final scan after freeze === */
	ctx->iteration++;
	pr_err("P3[%d] final scan (frozen) iter=%u\n", thread_id, ctx->iteration);
	ctx->last_dirty_count = do_dirty_scan_and_send(ctx);
	pr_err("P3[%d] final scan done: %lu dirty pages\n",
	       thread_id, ctx->last_dirty_count);

out:
	if (ctx->pagemap_fd >= 0) {
		close(ctx->pagemap_fd);
		ctx->pagemap_fd = -1;
	}

out_no_pagemap:
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

	/* Start one thread per socket */
	threads_to_start = num_sockets < NUM_P3_THREADS ? num_sockets : NUM_P3_THREADS;
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
 * Check if all P3 threads are below dirty page convergence threshold.
 * Returns true only when ALL active threads report below_threshold.
 */
bool cow_all_threads_below_threshold(void)
{
	int i;

	for (i = 0; i < NUM_P3_THREADS; i++) {
		if (p3_threads[i].active && !p3_threads[i].below_threshold)
			return false;
	}
	return p3_threads_active > 0;  /* Must have at least one active thread */
}

/*
 * Signal P3 threads to do final scan and exit.
 * Called by main thread after freezing the process.
 */
void cow_signal_last_scan(void)
{
	pr_err("=== CONVERGENCE: Signaling last scan ===\n");
	g_last_scan_flag = true;
	__sync_synchronize();  /* Memory barrier */
}

/*
 * Check if last scan has been signaled.
 */
bool cow_is_last_scan_signaled(void)
{
	return g_last_scan_flag;
}
