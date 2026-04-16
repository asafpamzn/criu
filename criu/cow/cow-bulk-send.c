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
#include "common/bug.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bulk: "

/*
 * Protocol structs, constants, and helpers are now in page-xfer.h:
 * - struct page_server_iov
 * - PS_CMD_BITS, encode_ps_cmd()
 * - page_server_send() (replaces __send)
 *
 * COW-specific protocol defines (PS_IOV_ADD_F_COMPRESS, etc.) are in cow-page-xfer.h
 * COW configuration constants (COW_BATCH_PAGES, etc.) are in cow-conf.h
 */

#define NUM_P3_SPLITTER_THREADS (COW_NUM_P3_THREADS - 1)  /* Threads 1-(N-1) split large VMAs */

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
};

static struct p3_thread_ctx p3_threads[COW_NUM_P3_THREADS];
static volatile int p3_threads_active = 0;
static unsigned long p3_total_pages_sent = 0;

/* Global flag for signaling last scan (set by main thread after freeze) */
static volatile bool g_last_scan_flag = false;

/* New VMA ranges detected in Phase 3 - set by main thread before last scan */
static unsigned long *g_new_vma_ranges = NULL;  /* [start, len, start, len, ...] */
static unsigned int g_nr_new_vma_ranges = 0;

/*
 * Dual Scanner + Multiple Senders Architecture
 * =============================================
 * Two scanner threads split VMA address ranges for parallel PAGEMAP_SCAN.
 * Each scanner handles half of each VMA and distributes to half the queues.
 *   Scanner 0: first half of each VMA  → queues 0-9
 *   Scanner 1: second half of each VMA → queues 10-19
 */
#define QUEUES_PER_SCANNER (COW_NUM_P3_THREADS / COW_NUM_SCANNERS)

static struct sender_queue sender_queues[COW_NUM_P3_THREADS];
static volatile bool g_scan_complete = false;
static volatile bool g_scanner_freeze_signal = false;
static pid_t g_scanner_source_pid;

/* Dual scanner state */
struct scanner_ctx {
	int id;                    /* Scanner ID: 0 or 1 */
	pthread_t thread;
	int pagemap_fd;
	unsigned long dirty_count; /* Dirty pages found in current iteration */
	volatile bool finished;    /* Set when scanner thread exits */
};
static struct scanner_ctx scanners[COW_NUM_SCANNERS];

/* Synchronization: scanners coordinate on iteration and freeze */
static volatile int g_scanners_iter_done = 0;  /* Count of scanners done with iteration */
static volatile unsigned long g_total_dirty_pages = 0;  /* Sum of dirty pages */
static pthread_mutex_t g_scanner_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_scanner_cond = PTHREAD_COND_INITIALIZER;

/* Synchronization: scanner waits for bulk transfer to complete */
static volatile int g_bulk_transfer_done_count = 0;
static volatile int g_num_sender_threads = 0;

int cow_init_sender_queues(void)
{
	int i;

	for (i = 0; i < COW_NUM_P3_THREADS; i++) {
		if (spsc_init(sender_queues[i].head, sender_queues[i].tail,
			      sender_queues[i].size,
			      struct dirty_region_spsc_node)) {
			pr_err("Failed to init sender queue %d\n", i);
			return -1;
		}
	}
	g_scan_complete = false;
	g_scanner_freeze_signal = false;
	pr_info("Initialized %d sender queues\n", COW_NUM_P3_THREADS);
	return 0;
}

struct sender_queue *cow_get_sender_queue(int thread_id)
{
	BUG_ON(thread_id < 0 || thread_id >= COW_NUM_P3_THREADS);
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
 * Dual scanner thread - each scanner handles half of each VMA's address range.
 * Scanner 0: first half (start → midpoint) → distributes to queues 0-9
 * Scanner 1: second half (midpoint → end) → distributes to queues 10-19
 */
static void *dirty_scanner_thread(void *arg)
{
	struct scanner_ctx *ctx = (struct scanner_ctx *)arg;
	int scanner_id = ctx->id;
	int queue_base = scanner_id * QUEUES_PER_SCANNER;  /* 0 or 10 */
	struct list_head *lazy_vmas;
	struct lazy_vma_entry *lve;
	struct page_region *regs;
	const int max_regs = COW_PAGEMAP_SCAN_VEC_LEN;
	unsigned int iteration = 0;
	char pagemap_path[64];
	struct timespec t_start, t_end;

	pr_err("Scanner[%d] started, queues %d-%d, source_pid=%d\n",
	       scanner_id, queue_base, queue_base + QUEUES_PER_SCANNER - 1,
	       g_scanner_source_pid);
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* Wait for all sender threads to complete bulk transfer first */
	if (scanner_id == 0) {
		pr_err("Scanner[0]: waiting for %d sender threads to complete bulk transfer...\n",
		       g_num_sender_threads);
	}
	while (__atomic_load_n(&g_bulk_transfer_done_count, __ATOMIC_ACQUIRE) <
	       __atomic_load_n(&g_num_sender_threads, __ATOMIC_ACQUIRE)) {
		if (__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE))
			goto out;
		usleep(COW_USLEEP_10MS);
	}
	if (scanner_id == 0) {
		pr_err("Scanner: all sender threads completed bulk transfer, starting dirty scan\n");
	}

	/* Open pagemap fd - each scanner needs its own fd */
	snprintf(pagemap_path, sizeof(pagemap_path), "/proc/%d/pagemap",
		 g_scanner_source_pid);
	ctx->pagemap_fd = open(pagemap_path, O_RDWR);
	if (ctx->pagemap_fd < 0) {
		pr_perror("Scanner[%d]: cannot open %s", scanner_id, pagemap_path);
		goto out;
	}

	regs = xmalloc(max_regs * sizeof(struct page_region));
	BUG_ON(!regs);

	lazy_vmas = get_global_lazy_vmas();

	/* Iterative dirty scanning until freeze signal */
	while (!__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE)) {
		unsigned long my_dirty_pages = 0;
		unsigned int queue_idx = queue_base;
		struct timespec iter_start, iter_end;
		unsigned long scan_time_ns = 0;
		unsigned long dist_time_ns = 0;
		unsigned long num_regions = 0;

		iteration++;
		clock_gettime(CLOCK_MONOTONIC, &iter_start);

		/* Scan this scanner's portion of each VMA */
		list_for_each_entry(lve, lazy_vmas, list) {
			struct pm_scan_arg args;
			long regs_len;
			unsigned long vma_size = lve->end - lve->start;
			unsigned long total_pages = vma_size / PAGE_SIZE;
			unsigned long pages_per_scanner = total_pages / COW_NUM_SCANNERS;
			unsigned long my_start, my_end;

			/* Calculate this scanner's range (page-aligned) */
			my_start = lve->start + (scanner_id * pages_per_scanner * PAGE_SIZE);
			if (scanner_id == COW_NUM_SCANNERS - 1)
				my_end = lve->end;  /* Last scanner gets remainder */
			else
				my_end = my_start + (pages_per_scanner * PAGE_SIZE);

			/* Skip if range is too small */
			if (my_end <= my_start)
				continue;

			memset(&args, 0, sizeof(args));
			args.size = sizeof(args);
			args.flags = PM_SCAN_WP_MATCHING;
			args.start = my_start;
			args.end = my_end;
			args.walk_end = my_start;
			args.vec = (u64)(unsigned long)regs;
			args.vec_len = max_regs;
			args.max_pages = 0;
			args.category_anyof_mask = PAGE_IS_WRITTEN;
			args.return_mask = PAGE_IS_WRITTEN;

			do {
				struct timespec t1, t2, t3;
				int i;
				args.start = args.walk_end;

				clock_gettime(CLOCK_MONOTONIC, &t1);
				regs_len = ioctl(ctx->pagemap_fd, PAGEMAP_SCAN, &args);
				clock_gettime(CLOCK_MONOTONIC, &t2);
				scan_time_ns += (t2.tv_sec - t1.tv_sec) * 1000000000UL +
						(t2.tv_nsec - t1.tv_nsec);

				if (regs_len < 0) {
					pr_perror("Scanner[%d]: PAGEMAP_SCAN failed", scanner_id);
					break;
				}

				if (regs_len == 0)
					break;

				num_regions += regs_len;

				/* Distribute to this scanner's queues (round-robin within queue_base to queue_base+9) */
				for (i = 0; i < regs_len; i++) {
					struct dirty_region_entry *entry;
					unsigned long pages;

					pages = (regs[i].end - regs[i].start) / PAGE_SIZE;
					my_dirty_pages += pages;

					entry = xmalloc(sizeof(*entry));
					BUG_ON(!entry);
					entry->start = regs[i].start;
					entry->end = regs[i].end;
					entry->dst_id = lve->dst_id;
					entry->source_pid = g_scanner_source_pid;

					spsc_enqueue(sender_queues[queue_idx].tail,
						     sender_queues[queue_idx].size,
						     entry, struct dirty_region_spsc_node);
					queue_idx = queue_base + ((queue_idx - queue_base + 1) % QUEUES_PER_SCANNER);
				}
				clock_gettime(CLOCK_MONOTONIC, &t3);
				dist_time_ns += (t3.tv_sec - t2.tv_sec) * 1000000000UL +
						(t3.tv_nsec - t2.tv_nsec);
			} while (args.walk_end < my_end);
		}

		clock_gettime(CLOCK_MONOTONIC, &iter_end);

		/* Store this scanner's dirty count */
		ctx->dirty_count = my_dirty_pages;

		/* Synchronize with other scanner - wait for both to complete iteration */
		pthread_mutex_lock(&g_scanner_mutex);
		g_scanners_iter_done++;
		if (g_scanners_iter_done == COW_NUM_SCANNERS) {
			/* Last scanner to finish - calculate total and reset */
			int s;

			g_total_dirty_pages = 0;
			for (s = 0; s < COW_NUM_SCANNERS; s++)
				g_total_dirty_pages += scanners[s].dirty_count;
			g_scanners_iter_done = 0;
			pthread_cond_broadcast(&g_scanner_cond);
		} else {
			/* Wait for other scanner */
			pthread_cond_wait(&g_scanner_cond, &g_scanner_mutex);
		}
		pthread_mutex_unlock(&g_scanner_mutex);

		/* Log timing (only scanner 0 logs combined stats) */
		if (scanner_id == 0) {
			long iter_ms = (iter_end.tv_sec - iter_start.tv_sec) * 1000 +
				       (iter_end.tv_nsec - iter_start.tv_nsec) / 1000000;
			pr_err("Scanner: iter=%u, %lu total pages, scan=%lu ms, dist=%lu ms, total=%ld ms\n",
			       iteration, g_total_dirty_pages,
			       scan_time_ns / 1000000, dist_time_ns / 1000000, iter_ms);
		}

		/* Check convergence - both scanners check the combined total */
		if (g_total_dirty_pages < COW_DIRTY_SCAN_FREEZE_THRESHOLD) {
			if (scanner_id == 0) {
				pr_err("Scanner: %lu pages < %d threshold, requesting freeze\n",
				       g_total_dirty_pages, COW_DIRTY_SCAN_FREEZE_THRESHOLD);
				g_last_scan_flag = true;
			}
			break;
		}

		usleep(COW_USLEEP_1MS);
	}

	/* Wait for freeze signal from main thread */
	if (scanner_id == 0) {
		pr_err("Scanner: waiting for freeze signal...\n");
	}
	while (!__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE)) {
		usleep(COW_USLEEP_1MS);
	}

	/* Final scan after freeze - each scanner handles its half */
	{
		unsigned long final_dirty = 0;
		unsigned int queue_idx = queue_base;
		struct timespec fs_start, fs_end;

		clock_gettime(CLOCK_MONOTONIC, &fs_start);
		if (scanner_id == 0) {
			pr_err("Scanner: final scan (frozen)\n");
		}

		list_for_each_entry(lve, lazy_vmas, list) {
			struct pm_scan_arg args;
			long regs_len;
			unsigned long vma_size = lve->end - lve->start;
			unsigned long total_pages = vma_size / PAGE_SIZE;
			unsigned long pages_per_scanner = total_pages / COW_NUM_SCANNERS;
			unsigned long my_start, my_end;

			my_start = lve->start + (scanner_id * pages_per_scanner * PAGE_SIZE);
			if (scanner_id == COW_NUM_SCANNERS - 1)
				my_end = lve->end;
			else
				my_end = my_start + (pages_per_scanner * PAGE_SIZE);

			if (my_end <= my_start)
				continue;

			memset(&args, 0, sizeof(args));
			args.size = sizeof(args);
			args.flags = PM_SCAN_WP_MATCHING;
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
				if (regs_len < 0)
					break;
				if (regs_len == 0)
					break;

				for (i = 0; i < regs_len; i++) {
					struct dirty_region_entry *entry;

					final_dirty += (regs[i].end - regs[i].start) / PAGE_SIZE;

					entry = xmalloc(sizeof(*entry));
					BUG_ON(!entry);
					entry->start = regs[i].start;
					entry->end = regs[i].end;
					entry->dst_id = lve->dst_id;
					entry->source_pid = g_scanner_source_pid;

					spsc_enqueue(sender_queues[queue_idx].tail,
						     sender_queues[queue_idx].size,
						     entry, struct dirty_region_spsc_node);
					queue_idx = queue_base + ((queue_idx - queue_base + 1) % QUEUES_PER_SCANNER);
				}
			} while (args.walk_end < my_end);
		}

		clock_gettime(CLOCK_MONOTONIC, &fs_end);

		/* Store final dirty count for this scanner */
		ctx->dirty_count = final_dirty;

		/* Synchronize final scan completion */
		pthread_mutex_lock(&g_scanner_mutex);
		g_scanners_iter_done++;
		if (g_scanners_iter_done == COW_NUM_SCANNERS) {
			unsigned long total_final = 0;
			long fs_ms;
			int s;

			for (s = 0; s < COW_NUM_SCANNERS; s++)
				total_final += scanners[s].dirty_count;
			fs_ms = (fs_end.tv_sec - fs_start.tv_sec) * 1000 +
				(fs_end.tv_nsec - fs_start.tv_nsec) / 1000000;
			pr_err("Scanner: final scan done, %lu dirty pages, %ld ms\n",
			       total_final, fs_ms);
			g_scanners_iter_done = 0;
			pthread_cond_broadcast(&g_scanner_cond);
		} else {
			pthread_cond_wait(&g_scanner_cond, &g_scanner_mutex);
		}
		pthread_mutex_unlock(&g_scanner_mutex);
	}

	xfree(regs);

	if (ctx->pagemap_fd >= 0) {
		close(ctx->pagemap_fd);
		ctx->pagemap_fd = -1;
	}

out:
	clock_gettime(CLOCK_MONOTONIC, &t_end);
	{
		long elapsed_ms = (t_end.tv_sec - t_start.tv_sec) * 1000 +
				  (t_end.tv_nsec - t_start.tv_nsec) / 1000000;
		pr_err("Scanner[%d] done: %u iterations, %ld ms\n",
		       scanner_id, iteration, elapsed_ms);
	}

	/* Mark this scanner as finished */
	ctx->finished = true;

	/* Last scanner to finish signals completion to senders */
	pthread_mutex_lock(&g_scanner_mutex);
	{
		bool all_done = true;
		int s;

		for (s = 0; s < COW_NUM_SCANNERS; s++) {
			if (!scanners[s].finished) {
				all_done = false;
				break;
			}
		}
		if (all_done) {
			pr_err("Scanner: all scanners done, setting g_scan_complete=true\n");
			__atomic_store_n(&g_scan_complete, true, __ATOMIC_RELEASE);
		}
	}
	pthread_mutex_unlock(&g_scanner_mutex);

	return NULL;
}

int cow_start_scanner_thread(pid_t source_pid)
{
	int i;

	g_scanner_source_pid = source_pid;
	g_scan_complete = false;
	g_scanner_freeze_signal = false;
	g_scanners_iter_done = 0;
	g_total_dirty_pages = 0;

	/* Initialize and start dual scanners */
	for (i = 0; i < COW_NUM_SCANNERS; i++) {
		scanners[i].id = i;
		scanners[i].pagemap_fd = -1;
		scanners[i].dirty_count = 0;
		scanners[i].finished = false;

		if (pthread_create(&scanners[i].thread, NULL,
				   dirty_scanner_thread, &scanners[i])) {
			pr_perror("Failed to create scanner thread %d", i);
			return -1;
		}
	}

	pr_info("Started %d scanner threads for pid %d\n", COW_NUM_SCANNERS, source_pid);
	return 0;
}

void cow_wait_scanner_thread(void)
{
	int i;

	for (i = 0; i < COW_NUM_SCANNERS; i++) {
		if (scanners[i].thread) {
			pthread_join(scanners[i].thread, NULL);
			scanners[i].thread = 0;
			pr_info("Scanner thread %d joined\n", i);
		}
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
	BUG_ON(!send_buf);

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
	BUG_ON(!buffer);

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
	BUG_ON(!buffer);

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

	if (vma_size < COW_MIN_VMA_SIZE_FOR_SPLIT) {
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

		if (thread_id == COW_NUM_P3_THREADS - 1)
			*out_end = lve->end;
		else
			*out_end = *out_start + chunk_size;
	}

	return true;
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
	ranges_per_thread = (g_nr_new_vma_ranges + COW_NUM_P3_THREADS - 1) / COW_NUM_P3_THREADS;
	my_start_idx = thread_id * ranges_per_thread;
	my_end_idx = my_start_idx + ranges_per_thread;
	if (my_end_idx > g_nr_new_vma_ranges)
		my_end_idx = g_nr_new_vma_ranges;

	if (my_start_idx >= g_nr_new_vma_ranges)
		return 0;  /* No ranges for this thread */

	pr_info("P3[%d] sending new VMA pages: ranges %u-%u of %u\n",
		thread_id, my_start_idx, my_end_idx, g_nr_new_vma_ranges);

	buffer = xmalloc(COW_BATCH_PAGES * PAGE_SIZE);
	BUG_ON(!buffer);

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
 * Each thread handles 1/COW_NUM_P3_THREADS of each VMA's address range.
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
		struct timespec loop_start, loop_end, drain_start;
		long loop_elapsed_ms;
		unsigned long loop_total_pages = 0;
		unsigned long regions_processed = 0;
		unsigned long wait_count = 0;
		unsigned long drain_regions = 0;
		unsigned long drain_pages = 0;
		bool drain_started = false;
		struct sender_queue *my_queue = cow_get_sender_queue(thread_id);

		clock_gettime(CLOCK_MONOTONIC, &loop_start);
		pr_err("P3[%d] starting queue consumption, queue=%p\n", thread_id, (void *)my_queue);

		/* Consume dirty regions from queue until scanner completes */
		while (!cow_is_scan_complete() || spsc_peek(my_queue->head)) {
			struct dirty_region_entry *region;
			int sent;

			/* Track when we start draining after scan_complete */
			if (!drain_started && cow_is_scan_complete()) {
				drain_started = true;
				clock_gettime(CLOCK_MONOTONIC, &drain_start);
				pr_err("P3[%d] scan complete, draining queue (size=%lu)\n",
				       thread_id, spsc_size(my_queue->size));
			}

			/* Periodic status logging */
			if (regions_processed > 0 && regions_processed % COW_LOG_SAMPLE_10K == 0) {
				pr_info("P3[%d] queue progress: processed=%lu, queue_size=%lu, scan_complete=%d\n",
				       thread_id, regions_processed, spsc_size(my_queue->size),
				       cow_is_scan_complete());
			}

			region = spsc_dequeue(my_queue->head, my_queue->size);
			if (!region) {
				/* Queue empty, brief wait */
				wait_count++;
				if (wait_count % COW_LOG_SAMPLE_10K == 0) {
					pr_err("P3[%d] waiting: queue empty, scan_complete=%d, wait_count=%lu, peek=%d\n",
					       thread_id, cow_is_scan_complete(), wait_count,
					       spsc_peek(my_queue->head));
				}
				usleep(COW_USLEEP_100US);
				continue;
			}
			wait_count = 0;

			/* Send the dirty region */
			sent = send_dirty_region(ctx, region);
			if (sent > 0) {
				loop_total_pages += sent;
				regions_processed++;
				if (drain_started) {
					drain_regions++;
					drain_pages += sent;
				}
			}
			xfree(region);
		}
		pr_err("P3[%d] exiting queue loop: scan_complete=%d, peek=%d\n",
		       thread_id, cow_is_scan_complete(), spsc_peek(my_queue->head));

		clock_gettime(CLOCK_MONOTONIC, &loop_end);
		loop_elapsed_ms = (loop_end.tv_sec - loop_start.tv_sec) * 1000 +
				  (loop_end.tv_nsec - loop_start.tv_nsec) / 1000000;

		if (drain_started) {
			long drain_ms = (loop_end.tv_sec - drain_start.tv_sec) * 1000 +
					(loop_end.tv_nsec - drain_start.tv_nsec) / 1000000;
			pr_err("P3[%d] TIMING: Drain after scan_complete: %lu regions, %lu pages in %ld ms\n",
			       thread_id, drain_regions, drain_pages, drain_ms);
		}
		pr_err("P3[%d] TIMING: Queue consumption done: %lu regions, %lu pages in %ld ms\n",
		       thread_id, regions_processed, loop_total_pages, loop_elapsed_ms);
	}

	/* === Final: Send pages from new VMAs detected in Phase 3 === */
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

		pr_err("P3[%d] done: %lu pages, %ld ms\n",
		       thread_id, ctx->pages_sent, elapsed_ms);
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
	threads_to_start = num_sockets < COW_NUM_P3_THREADS ? num_sockets : COW_NUM_P3_THREADS;
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

void cow_wait_p3_threads(void)
{
	int i;
	unsigned long total = 0;
	int errors = 0;

	/* Wait for scanner thread first */
	cow_wait_scanner_thread();

	/* Then wait for sender threads */
	for (i = 0; i < COW_NUM_P3_THREADS; i++) {
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

	/* Close P3 sockets so replica receivers get EOF */
	for (i = 0; i < COW_NUM_P3_THREADS; i++) {
		if (p3_threads[i].socket >= 0) {
			pr_debug("DEBUG_THREAD: Closing P3 sender[%d] socket=%d\n",
				i, p3_threads[i].socket);
			close(p3_threads[i].socket);
			p3_threads[i].socket = -1;
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
	return COW_NUM_P3_THREADS;
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
