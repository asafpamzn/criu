/*
 * COW Bulk Page Sender - Optimized P3 (regular page) transfer
 *
 * Sends pages in batches of 64 (256KB) for better throughput:
 * - Single process_vm_readv for 64 pages
 * - Single LZ4 compression for 256KB
 * - Single socket send
 *
 * Work-stealing architecture:
 * - Bulk transfer: shared work queue of VMA chunks, threads pull work dynamically
 * - Queue consumption: threads can steal from other threads' queues when idle
 */

#include <sched.h>
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
#include <inttypes.h>
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
#ifdef CONFIG_HAS_LIBBPF
#include "cow/cow-bpf.h"
#endif

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bulk: "

/*
 * CPU affinity helpers for bulk transfer phase.
 * During bulk transfer, sender threads are pinned to one CPU to avoid
 * using multiple cores (receiver is the bottleneck, not sender).
 * After bulk transfer, threads are unpinned to use all available CPUs.
 */
#ifdef COW_P3_SENDER_CPU
static void pin_to_cpu(int cpu)
{
	cpu_set_t cpuset;

	CPU_ZERO(&cpuset);
	CPU_SET(cpu, &cpuset);
	pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
}

static void unpin_cpu(void)
{
	cpu_set_t cpuset;
	int i, ncpus;

	ncpus = sysconf(_SC_NPROCESSORS_ONLN);
	CPU_ZERO(&cpuset);
	for (i = 0; i < ncpus; i++)
		CPU_SET(i, &cpuset);
	pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
}
#endif

/*
 * Protocol structs, constants, and helpers are now in page-xfer.h:
 * - struct page_server_iov
 * - PS_CMD_BITS, encode_ps_cmd()
 * - page_server_send() (replaces __send)
 *
 * COW-specific protocol defines (PS_IOV_ADD_F_COMPRESS, etc.) are in cow-page-xfer.h
 * COW configuration constants (COW_BATCH_PAGES, etc.) are in cow-conf.h
 */

/*
 * Work-stealing chunk size: COW_WORK_CHUNK_SIZE (default 32MB).
 * Smaller chunks = better balancing but more overhead.
 * Larger chunks = less overhead but worse balancing.
 * Configuration in cow-conf.h.
 */

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

/* Timestamp when freeze signal was sent - for P3 thread timing */
static struct timespec g_freeze_signal_time;

/* New VMA ranges detected in Phase 3 - set by main thread before last scan */
static unsigned long *g_new_vma_ranges = NULL;  /* [start, len, start, len, ...] */
static unsigned int g_nr_new_vma_ranges = 0;

/*
 * MPMC convergence queue: scanners push dirty_region_entry pointers,
 * consumers CAS-claim one entry at a time. Flat array + two atomic
 * counters. One CAS per real work item, no empty-queue waste.
 */
#define CONV_QUEUE_CAP	(8 * 1024 * 1024)
static struct dirty_region_entry **g_conv_slots;
static volatile unsigned long g_conv_head;
static volatile unsigned long g_conv_tail;

static volatile bool g_scan_complete = false;
static volatile bool g_scanner_freeze_signal = false;
static pid_t g_scanner_source_pid;

/* Atomic counter for total scanned pages (for verification) */
static volatile unsigned long g_total_scanned_pages = 0;
/* Atomic counter for total sent pages (for verification) */
static volatile unsigned long g_total_sent_pages = 0;

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

/*
 * Work-stealing infrastructure for bulk transfer phase.
 * Instead of statically assigning VMA chunks to threads, we create a shared
 * work queue of chunks that threads pull from dynamically.
 */
struct bulk_work_item {
	struct lazy_vma_entry *lve;
	unsigned long start;
	unsigned long end;
};

static struct bulk_work_item g_work_queue[COW_MAX_WORK_ITEMS];
static volatile int g_work_queue_size = 0;
static volatile int g_work_queue_next = 0;  /* Next item to dequeue (atomic) */

/*
 * Build the work queue by splitting all VMAs into COW_WORK_CHUNK_SIZE pieces.
 * Must be called before starting sender threads.
 */
static void build_bulk_work_queue(u64 dst_id)
{
	struct list_head *lazy_vmas = get_global_lazy_vmas();
	struct lazy_vma_entry *lve;
	int count = 0;

	list_for_each_entry(lve, lazy_vmas, list) {
		unsigned long vaddr;

		if (lve->dst_id != dst_id)
			continue;

		/* Split VMA into COW_WORK_CHUNK_SIZE pieces */
		for (vaddr = lve->start; vaddr < lve->end; vaddr += COW_WORK_CHUNK_SIZE) {
			unsigned long chunk_end = vaddr + COW_WORK_CHUNK_SIZE;

			if (chunk_end > lve->end)
				chunk_end = lve->end;

			if (count >= COW_MAX_WORK_ITEMS) {
				pr_err("Work queue overflow! Increase COW_MAX_WORK_ITEMS\n");
				BUG();
			}

			g_work_queue[count].lve = lve;
			g_work_queue[count].start = vaddr;
			g_work_queue[count].end = chunk_end;
			count++;
		}
	}

	g_work_queue_size = count;
	g_work_queue_next = 0;
	pr_info("Built bulk work queue: %d chunks of %luMB max\n",
		count, COW_WORK_CHUNK_SIZE / (1024 * 1024));
}

/*
 * Get next work item from the shared queue (thread-safe).
 * Returns NULL when queue is exhausted.
 */
static struct bulk_work_item *get_next_work_item(void)
{
	int idx = __atomic_fetch_add(&g_work_queue_next, 1, __ATOMIC_RELAXED);

	if (idx >= g_work_queue_size)
		return NULL;

	return &g_work_queue[idx];
}

int cow_init_sender_queues(void)
{
	g_conv_slots = xzalloc(CONV_QUEUE_CAP * sizeof(g_conv_slots[0]));
	BUG_ON(!g_conv_slots);
	g_conv_head = 0;
	g_conv_tail = 0;
	g_scan_complete = false;
	g_scanner_freeze_signal = false;
	pr_info("Initialized MPMC convergence queue (%d slots)\n",
		CONV_QUEUE_CAP);
	return 0;
}

static struct dirty_region_entry *conv_queue_pop(void)
{
	unsigned long t, h;

	t = __atomic_load_n(&g_conv_tail, __ATOMIC_RELAXED);
	h = __atomic_load_n(&g_conv_head, __ATOMIC_ACQUIRE);
	if (t >= h)
		return NULL;
	if (!__atomic_compare_exchange_n(&g_conv_tail, &t, t + 1,
					 false, __ATOMIC_ACQUIRE,
					 __ATOMIC_RELAXED))
		return NULL;

	{
		struct dirty_region_entry *entry;
		while (!(entry = g_conv_slots[t]))
			;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		return entry;
	}
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
 * Scanner thread. Each scanner:
 *   - Scans its own disjoint slice of every lazy VMA's address range
 *     (slice = vma_size / COW_NUM_SCANNERS, last scanner gets the remainder).
 *   - Enqueues dirty regions into the shared MPMC convergence queue,
 *     namely [scanner_id * COW_QUEUES_PER_THREAD, scanner_id * COW_QUEUES_PER_THREAD
 *     + COW_QUEUES_PER_THREAD). This preserves the SPSC single-producer invariant:
 *     no two scanners ever enqueue to the same queue.
 */
static void *dirty_scanner_thread(void *arg)
{
	struct scanner_ctx *ctx = (struct scanner_ctx *)arg;
	int scanner_id = ctx->id;
	struct list_head *lazy_vmas;
	struct lazy_vma_entry *lve;
	struct page_region *regs;
	const int max_regs = COW_PAGEMAP_SCAN_VEC_LEN;
	unsigned int iteration = 0;
	char pagemap_path[64];
	struct timespec t_start, t_end;

	pr_err("Scanner[%d] started, source_pid=%d\n",
	       scanner_id, g_scanner_source_pid);
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

#ifdef COW_PRE_SCAN
	/* Only first COW_NUM_PRE_SCANNERS participate in pre-scan */
	if (scanner_id >= COW_NUM_PRE_SCANNERS)
		goto wait_for_freeze;

	/* Iterative dirty scanning until freeze signal */
	while (!__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE)) {
		unsigned long my_dirty_pages = 0;
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
			unsigned long pages_per_scanner = total_pages / COW_NUM_PRE_SCANNERS;
			unsigned long my_start, my_end;

			/* Calculate this scanner's range (page-aligned) */
			my_start = lve->start + (scanner_id * pages_per_scanner * PAGE_SIZE);
			if (scanner_id == COW_NUM_PRE_SCANNERS - 1)
				my_end = lve->end;  /* Last pre-scanner gets remainder */
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
			args.max_pages = 16384;
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

					{
						unsigned long slot = __atomic_fetch_add(&g_conv_head, 1, __ATOMIC_RELAXED);
						BUG_ON(slot >= CONV_QUEUE_CAP);
						g_conv_slots[slot] = entry;
						__atomic_thread_fence(__ATOMIC_RELEASE);
					}
					__sync_fetch_and_add(&g_total_scanned_pages, pages);
				}
				clock_gettime(CLOCK_MONOTONIC, &t3);
				dist_time_ns += (t3.tv_sec - t2.tv_sec) * 1000000000UL +
						(t3.tv_nsec - t2.tv_nsec);
			} while (args.walk_end < my_end);
		}

		clock_gettime(CLOCK_MONOTONIC, &iter_end);

		/* Store this scanner's dirty count */
		ctx->dirty_count = my_dirty_pages;

		/* Synchronize with other pre-scanners */
		pthread_mutex_lock(&g_scanner_mutex);
		g_scanners_iter_done++;
		if (g_scanners_iter_done == COW_NUM_PRE_SCANNERS) {
			/* Last pre-scanner to finish - calculate total and reset */
			int s;

			g_total_dirty_pages = 0;
			for (s = 0; s < COW_NUM_PRE_SCANNERS; s++)
				g_total_dirty_pages += scanners[s].dirty_count;
			g_scanners_iter_done = 0;
			pthread_cond_broadcast(&g_scanner_cond);
		} else {
			/* Wait for other scanner */
			pthread_cond_wait(&g_scanner_cond, &g_scanner_mutex);
		}
		pthread_mutex_unlock(&g_scanner_mutex);

		/* Log timing - each scanner logs its own stats */
		{
			long iter_ms = (iter_end.tv_sec - iter_start.tv_sec) * 1000 +
				       (iter_end.tv_nsec - iter_start.tv_nsec) / 1000000;
			pr_warn("DEBUG_PERF: Scanner[%d] iter=%u: pages=%lu regions=%lu scan=%lu ms dist=%lu ms total=%ld ms\n",
			       scanner_id, iteration, my_dirty_pages, num_regions,
			       scan_time_ns / 1000000, dist_time_ns / 1000000, iter_ms);
		}
		/* Scanner 0 also logs combined stats */
		if (scanner_id == 0) {
			pr_err("Scanner: iter=%u, %lu total pages\n",
			       iteration, g_total_dirty_pages);
		}

		/* Check convergence - pre-scanners check the combined total */
		if (g_total_dirty_pages < COW_DIRTY_SCAN_FREEZE_THRESHOLD) {
			if (scanner_id == 0) {
				pr_err("Scanner: %lu pages < %d threshold, will request freeze after queue drain\n",
				       g_total_dirty_pages, COW_DIRTY_SCAN_FREEZE_THRESHOLD);
			}
			break;
		}

		/* Check max iterations limit */
		if (COW_PRE_SCAN_MAX_ITERATIONS > 0 && iteration >= COW_PRE_SCAN_MAX_ITERATIONS) {
			if (scanner_id == 0) {
				pr_err("Scanner: max iterations (%u) reached, will request freeze after queue drain\n", iteration);
			}
			break;
		}

		usleep(COW_USLEEP_1MS);
	}

	/* Wait for P3 senders to drain the queue before requesting freeze */
	if (scanner_id == 0) {
		unsigned long head, tail;
		struct timespec drain_start, drain_end;
		long drain_ms;

		clock_gettime(CLOCK_MONOTONIC, &drain_start);
		pr_err("Scanner: waiting for queue drain before freeze...\n");

		while (1) {
			head = __atomic_load_n(&g_conv_head, __ATOMIC_ACQUIRE);
			tail = __atomic_load_n(&g_conv_tail, __ATOMIC_RELAXED);
			if (tail >= head)
				break;
			usleep(COW_USLEEP_1MS);
		}

		clock_gettime(CLOCK_MONOTONIC, &drain_end);
		drain_ms = (drain_end.tv_sec - drain_start.tv_sec) * 1000 +
			   (drain_end.tv_nsec - drain_start.tv_nsec) / 1000000;
		pr_err("Scanner: queue drained in %ld ms, requesting freeze\n", drain_ms);
		g_last_scan_flag = true;
	}

wait_for_freeze:
#endif /* COW_PRE_SCAN */

	/* Wait for freeze signal from main thread */
	if (scanner_id == 0) {
		pr_err("Scanner: waiting for freeze signal...\n");
	}
	while (!__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE)) {
		usleep(COW_USLEEP_1MS);
	}

	/* Final scan after freeze - each scanner handles its portion */
	{
		unsigned long final_dirty = 0;
		/* Continue round-robin from where we left off */
		struct timespec fs_start, fs_end;
#ifdef CONFIG_HAS_LIBBPF
		bool use_bpf = false;
#endif

		clock_gettime(CLOCK_MONOTONIC, &fs_start);
		if (scanner_id == 0) {
			pr_err("Scanner: final scan (frozen)\n");
		}

#ifdef CONFIG_HAS_LIBBPF
		/*
		 * If BPF dirty tracking is active, scanner 0 drains the BPF ring
		 * and other scanners skip. This is O(dirty) vs O(total_pages).
		 */
		if (scanner_id == 0 && cow_bpf_active()) {
			struct cow_bpf_region *bpf_regions;
			unsigned long bpf_pages = 0;
			int bpf_nr, i;

			use_bpf = true;
			bpf_regions = xmalloc(COW_PAGEMAP_SCAN_VEC_LEN * sizeof(*bpf_regions));
			if (bpf_regions) {
				bpf_nr = cow_bpf_drain(bpf_regions, COW_PAGEMAP_SCAN_VEC_LEN, &bpf_pages);
				if (bpf_nr >= 0) {
					pr_err("BPF drain: %d regions, %lu pages\n", bpf_nr, bpf_pages);
					for (i = 0; i < bpf_nr; i++) {
						struct dirty_region_entry *entry;
						unsigned long pages;

						pages = (bpf_regions[i].end - bpf_regions[i].start) / PAGE_SIZE;
						final_dirty += pages;

						entry = xmalloc(sizeof(*entry));
						BUG_ON(!entry);
						entry->start = bpf_regions[i].start;
						entry->end = bpf_regions[i].end;
						entry->dst_id = 0;  /* Will be set per-VMA */
						entry->source_pid = g_scanner_source_pid;

						{
							unsigned long slot = __atomic_fetch_add(&g_conv_head, 1, __ATOMIC_RELAXED);
							BUG_ON(slot >= CONV_QUEUE_CAP);
							g_conv_slots[slot] = entry;
							__atomic_thread_fence(__ATOMIC_RELEASE);
						}
					}
				} else if (bpf_nr == -2) {
					/* BPF ring drops - fall back to PAGEMAP_SCAN */
					pr_err("BPF ring drops detected, falling back to PAGEMAP_SCAN\n");
					use_bpf = false;
				}
				xfree(bpf_regions);
			}
			cow_bpf_stop();
		}

		if (use_bpf)
			goto skip_pagemap_scan;
#endif

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
			args.flags = 0;  /* No WP_MATCHING - just read dirty state */
			args.start = my_start;
			args.end = my_end;
			args.walk_end = my_start;
			args.vec = (u64)(unsigned long)regs;
			args.vec_len = max_regs;
			args.max_pages = 16384;
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
					unsigned long pages;

					pages = (regs[i].end - regs[i].start) / PAGE_SIZE;
					final_dirty += pages;

					entry = xmalloc(sizeof(*entry));
					BUG_ON(!entry);
					entry->start = regs[i].start;
					entry->end = regs[i].end;
					entry->dst_id = lve->dst_id;
					entry->source_pid = g_scanner_source_pid;

					{
						unsigned long slot = __atomic_fetch_add(&g_conv_head, 1, __ATOMIC_RELAXED);
						BUG_ON(slot >= CONV_QUEUE_CAP);
						g_conv_slots[slot] = entry;
						__atomic_thread_fence(__ATOMIC_RELEASE);
					}
					__sync_fetch_and_add(&g_total_scanned_pages, pages);
				}
			} while (args.walk_end < my_end);
		}

#ifdef CONFIG_HAS_LIBBPF
skip_pagemap_scan:
#endif
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
			pr_err("Scanner: PAGEMAP_SCAN done: %lu dirty pages found in %ld ms\n",
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
			unsigned long total_pages;
			struct timespec now;
			long from_freeze_ms;

			clock_gettime(CLOCK_MONOTONIC, &now);
			from_freeze_ms = (now.tv_sec - g_freeze_signal_time.tv_sec) * 1000 +
					 (now.tv_nsec - g_freeze_signal_time.tv_nsec) / 1000000;
			pr_err("Scanner: all scanners done, %ld ms from freeze signal\n", from_freeze_ms);

			total_pages = __atomic_load_n(&g_total_scanned_pages, __ATOMIC_RELAXED);
			pr_warn("DEBUG_PERF: MPMC queue: %lu total scanned pages, %lu entries\n",
			       total_pages, __atomic_load_n(&g_conv_head, __ATOMIC_RELAXED));

			__atomic_store_n(&g_scan_complete, true, __ATOMIC_RELEASE);
		}
	}
	pthread_mutex_unlock(&g_scanner_mutex);

	return NULL;
}

/* Track whether we're using BPF mode (no scanner threads) */
static bool g_using_bpf_mode = false;

int cow_start_scanner_thread(pid_t source_pid)
{
	int i;
	struct list_head *lazy_vmas = get_global_lazy_vmas();
	struct lazy_vma_entry *lve;
	unsigned int lve_count = 0;

	list_for_each_entry(lve, lazy_vmas, list) {
		pr_debug("VMA_TRACE: phase=SCANNER_START_LIST idx=%u vma=0x%" PRIx64 "-0x%" PRIx64
		       " pages=%lu dst_id=%" PRIu64 "\n",
		       lve_count, lve->start, lve->end, lve->total_pages,
		       (uint64_t)lve->dst_id);
		lve_count++;
	}
	pr_debug("VMA_TRACE: phase=SCANNER_START total_lazy_vmas=%u pid=%d\n",
	       lve_count, source_pid);

	g_scanner_source_pid = source_pid;
	g_scan_complete = false;
	g_scanner_freeze_signal = false;
	g_scanners_iter_done = 0;
	g_total_dirty_pages = 0;

	/* Reset verification counters */
	g_total_scanned_pages = 0;
	g_total_sent_pages = 0;

#ifdef CONFIG_HAS_LIBBPF
	/*
	 * When BPF is active, skip scanner threads entirely.
	 * BPF collects dirty pages in ring buffer during Phase 2.
	 * At freeze time, we drain the ring and distribute to sender queues.
	 */
	if (cow_bpf_active()) {
		g_using_bpf_mode = true;
		pr_err("BPF mode: skipping scanner threads for pid %d\n", source_pid);
		return 0;
	}
#endif

	g_using_bpf_mode = false;

	/* Initialize and start dual scanners (non-BPF path) */
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

#ifdef CONFIG_HAS_LIBBPF
	/* In BPF mode, no scanner threads to wait for */
	if (g_using_bpf_mode) {
		pr_err("BPF mode: no scanner threads to wait for\n");
		return;
	}
#endif

	for (i = 0; i < COW_NUM_SCANNERS; i++) {
		if (scanners[i].thread) {
			pthread_join(scanners[i].thread, NULL);
			scanners[i].thread = 0;
			pr_info("Scanner thread %d joined\n", i);
		}
	}
	/* Senders drain their own queues during normal exit */
}

#ifdef CONFIG_HAS_LIBBPF
/* Address comparison for qsort - used by BPF drain and SCAN_COMPARE */
static int addr_cmp_ul(const void *a, const void *b)
{
	unsigned long va = *(const unsigned long *)a;
	unsigned long vb = *(const unsigned long *)b;
	return (va > vb) - (va < vb);
}

/*
 * Drain BPF ring buffer and distribute dirty regions to sender queues.
 * Called at freeze time when using BPF mode.
 * Also does PAGEMAP_SCAN to catch any pages BPF missed.
 * Returns number of dirty pages, or -1 on error.
 */
int cow_bpf_drain_to_queues(void)
{
	struct cow_bpf_region *bpf_regions;
	unsigned long total_pages = 0;
	unsigned long total_regions = 0;
	int bpf_nr, i;
	u64 drops, event_count;
	int max_regions;
	/* For PAGEMAP_SCAN merge */
	struct list_head *lazy_vmas = get_global_lazy_vmas();
	struct lazy_vma_entry *lve;
	unsigned long *bpf_addrs = NULL;
	unsigned long bpf_addr_count = 0;
	unsigned long *scan_addrs = NULL;
	unsigned long scan_addr_count = 0;
	unsigned long scan_addr_cap = 0;
	unsigned long *merged_addrs = NULL;
	unsigned long merged_count = 0;
	int pagemap_fd = -1;
	char path[64];
	struct page_region *regs = NULL;
	unsigned long pgsz = PAGE_SIZE;

	if (!g_using_bpf_mode || !cow_bpf_active()) {
		pr_err("cow_bpf_drain_to_queues called but BPF not active\n");
		return -1;
	}

	/* Check for ring buffer overflow - BUG() if detected */
	drops = cow_bpf_drop_count();
	if (drops > 0) {
		pr_err("BPF ring buffer overflow: %llu events dropped!\n",
		       (unsigned long long)drops);
		BUG();
	}

	/*
	 * Step 1: Drain BPF ring buffer to get addresses
	 */
	if (cow_bpf_drain_addrs(&bpf_addrs, &bpf_addr_count) < 0) {
		pr_err("BPF drain_addrs failed\n");
		return -1;
	}
	pr_info("BPF drain: %lu unique addresses from ring buffer\n", bpf_addr_count);

	/*
	 * Step 2: Do PAGEMAP_SCAN on all lazy VMAs to catch pages BPF missed
	 */
	snprintf(path, sizeof(path), "/proc/%d/pagemap", g_scanner_source_pid);
	pagemap_fd = open(path, O_RDONLY);
	if (pagemap_fd < 0) {
		pr_perror("Failed to open pagemap for SCAN merge");
		/* Continue with BPF-only results */
		goto skip_scan_merge;
	}

	regs = xmalloc(COW_PAGEMAP_SCAN_VEC_LEN * sizeof(*regs));
	if (!regs) {
		close(pagemap_fd);
		goto skip_scan_merge;
	}

	scan_addr_cap = bpf_addr_count > 0 ? bpf_addr_count : 65536;
	scan_addrs = xmalloc(scan_addr_cap * sizeof(*scan_addrs));
	if (!scan_addrs) {
		xfree(regs);
		close(pagemap_fd);
		goto skip_scan_merge;
	}

	/* Scan each lazy VMA */
	list_for_each_entry(lve, lazy_vmas, list) {
		struct pm_scan_arg args;
		long regs_len;
		int r;
		unsigned long before_vma = scan_addr_count;

		memset(&args, 0, sizeof(args));
		args.size = sizeof(args);
		args.flags = 0;
		args.start = lve->start;
		args.end = lve->end;
		args.walk_end = lve->start;
		args.vec = (u64)(unsigned long)regs;
		args.vec_len = COW_PAGEMAP_SCAN_VEC_LEN;
		args.max_pages = 16384;
		args.category_anyof_mask = PAGE_IS_WRITTEN;
		args.return_mask = PAGE_IS_WRITTEN;

		do {
			unsigned long addr;

			args.start = args.walk_end;
			regs_len = ioctl(pagemap_fd, PAGEMAP_SCAN, &args);
			if (regs_len <= 0)
				break;

			for (r = 0; r < regs_len; r++) {
				for (addr = regs[r].start; addr < regs[r].end; addr += pgsz) {
					if (scan_addr_count >= scan_addr_cap) {
						unsigned long new_cap = scan_addr_cap * 2;
						unsigned long *tmp = xrealloc(scan_addrs, new_cap * sizeof(*tmp));
						if (!tmp)
							goto done_scan;
						scan_addrs = tmp;
						scan_addr_cap = new_cap;
					}
					scan_addrs[scan_addr_count++] = addr;
				}
			}
		} while (args.walk_end < lve->end);

		pr_debug("VMA_TRACE: phase=SCAN_MERGE vma=0x%" PRIx64 "-0x%" PRIx64 " dirty_found=%lu\n",
		       lve->start, lve->end, scan_addr_count - before_vma);
	}

done_scan:
	xfree(regs);
	close(pagemap_fd);

	pr_info("PAGEMAP_SCAN found: %lu dirty pages\n", scan_addr_count);

	/*
	 * Step 3: Merge BPF and SCAN results
	 * Both arrays should be sorted. Merge and deduplicate.
	 */
	if (scan_addr_count > 0) {
		unsigned long bpf_idx = 0, scan_idx = 0, out_idx = 0;
		unsigned long total_cap = bpf_addr_count + scan_addr_count;

		/* Sort SCAN results */
		qsort(scan_addrs, scan_addr_count, sizeof(*scan_addrs), addr_cmp_ul);

		/* Remove duplicates from SCAN */
		{
			unsigned long unique = 1;
			for (i = 1; i < (int)scan_addr_count; i++) {
				if (scan_addrs[i] != scan_addrs[unique - 1])
					scan_addrs[unique++] = scan_addrs[i];
			}
			scan_addr_count = unique;
		}

		merged_addrs = xmalloc(total_cap * sizeof(*merged_addrs));
		if (!merged_addrs) {
			xfree(scan_addrs);
			goto skip_scan_merge;
		}

		/* Merge two sorted arrays */
		while (bpf_idx < bpf_addr_count && scan_idx < scan_addr_count) {
			unsigned long bpf_val = bpf_addrs[bpf_idx];
			unsigned long scan_val = scan_addrs[scan_idx];

			if (bpf_val < scan_val) {
				if (out_idx == 0 || merged_addrs[out_idx - 1] != bpf_val)
					merged_addrs[out_idx++] = bpf_val;
				bpf_idx++;
			} else if (bpf_val > scan_val) {
				if (out_idx == 0 || merged_addrs[out_idx - 1] != scan_val)
					merged_addrs[out_idx++] = scan_val;
				scan_idx++;
			} else {
				if (out_idx == 0 || merged_addrs[out_idx - 1] != bpf_val)
					merged_addrs[out_idx++] = bpf_val;
				bpf_idx++;
				scan_idx++;
			}
		}
		while (bpf_idx < bpf_addr_count) {
			if (out_idx == 0 || merged_addrs[out_idx - 1] != bpf_addrs[bpf_idx])
				merged_addrs[out_idx++] = bpf_addrs[bpf_idx];
			bpf_idx++;
		}
		while (scan_idx < scan_addr_count) {
			if (out_idx == 0 || merged_addrs[out_idx - 1] != scan_addrs[scan_idx])
				merged_addrs[out_idx++] = scan_addrs[scan_idx];
			scan_idx++;
		}

		merged_count = out_idx;
		pr_info("Merged BPF+SCAN: %lu unique pages (BPF=%lu, SCAN=%lu, new from SCAN=%lu)\n",
			merged_count, bpf_addr_count, scan_addr_count,
			merged_count > bpf_addr_count ? merged_count - bpf_addr_count : 0);

		xfree(scan_addrs);
		xfree(bpf_addrs);
		bpf_addrs = merged_addrs;
		bpf_addr_count = merged_count;
		merged_addrs = NULL;
	} else {
		if (scan_addrs)
			xfree(scan_addrs);
	}

skip_scan_merge:
	/*
	 * Step 4: Convert addresses to regions and distribute to queues
	 */
	event_count = bpf_addr_count;
	max_regions = (event_count > 0) ? (int)event_count : COW_PAGEMAP_SCAN_VEC_LEN;
	if (max_regions > 10 * 1024 * 1024)
		max_regions = 10 * 1024 * 1024;

	bpf_regions = xmalloc(max_regions * sizeof(*bpf_regions));
	if (!bpf_regions) {
		pr_err("Failed to allocate regions buffer\n");
		if (bpf_addrs)
			xfree(bpf_addrs);
		return -1;
	}

	/* Convert sorted addresses to coalesced regions */
	bpf_nr = 0;
	if (bpf_addr_count > 0) {
		unsigned long start = bpf_addrs[0];
		unsigned long end = start + pgsz;

		for (i = 1; i < (int)bpf_addr_count && bpf_nr < max_regions; i++) {
			if (bpf_addrs[i] == end) {
				/* Contiguous - extend region */
				end += pgsz;
			} else {
				/* Not contiguous - save current region and start new */
				bpf_regions[bpf_nr].start = start;
				bpf_regions[bpf_nr].end = end;
				bpf_regions[bpf_nr].categories = 0;
				bpf_nr++;
				start = bpf_addrs[i];
				end = start + pgsz;
			}
		}
		/* Save last region */
		if (bpf_nr < max_regions) {
			bpf_regions[bpf_nr].start = start;
			bpf_regions[bpf_nr].end = end;
			bpf_regions[bpf_nr].categories = 0;
			bpf_nr++;
		}
	}

	total_pages = bpf_addr_count;
	if (bpf_addrs)
		xfree(bpf_addrs);

	pr_info("Final: %d regions, %lu dirty pages\n", bpf_nr, total_pages);

	/* Distribute regions to sender queues (round-robin) */
	for (i = 0; i < bpf_nr; i++) {
		struct dirty_region_entry *entry;
		unsigned long pages;

		pages = (bpf_regions[i].end - bpf_regions[i].start) / PAGE_SIZE;

		entry = xmalloc(sizeof(*entry));
		BUG_ON(!entry);
		entry->start = bpf_regions[i].start;
		entry->end = bpf_regions[i].end;
		entry->dst_id = 0;  /* Will use lve->dst_id when processing */
		entry->source_pid = g_scanner_source_pid;

		{
			unsigned long slot = __atomic_fetch_add(&g_conv_head, 1, __ATOMIC_RELAXED);
			BUG_ON(slot >= CONV_QUEUE_CAP);
			g_conv_slots[slot] = entry;
			__atomic_thread_fence(__ATOMIC_RELEASE);
		}
		total_regions++;
	}

	xfree(bpf_regions);

	/* Signal scan complete so sender threads process their queues */
	__atomic_store_n(&g_scan_complete, true, __ATOMIC_RELEASE);

	pr_info("BPF drain complete: %lu regions (%lu pages) to MPMC queue\n",
		total_regions, total_pages);

	return (int)total_pages;
}

#ifdef SCAN_COMPARE
static int scan_compare_addr_cmp(const void *a, const void *b)
{
	unsigned long va = *(const unsigned long *)a;
	unsigned long vb = *(const unsigned long *)b;
	return (va > vb) - (va < vb);
}

/*
 * DEBUG: Compare BPF vs PAGEMAP_SCAN at freeze time.
 * Call this after freeze, before any page transfer.
 * Exits after comparison - no page transfer happens.
 */
void cow_debug_scan_compare(void)
{
	struct list_head *lazy_vmas = get_global_lazy_vmas();
	struct lazy_vma_entry *lve;
	struct lazy_vma_entry *vma_match;
	struct page_region *regs;
	unsigned long *bpf_addrs = NULL;
	unsigned long bpf_addr_count = 0;
	int pagemap_fd;
	char path[64];
	unsigned long scan_count = 0;
	unsigned long scan_only = 0;
	unsigned long scan_in_bpf = 0;
	unsigned long pgsz = sysconf(_SC_PAGESIZE);
	u64 drops;
	/* Track SCAN addresses for duplicate detection */
	unsigned long *scan_addrs = NULL;
	unsigned long scan_addrs_cap = 0;
	unsigned long scan_addrs_count = 0;
	/* Initial dirty tracking */
	unsigned long *initial_dirty = NULL;
	unsigned long initial_dirty_count = 0;
	unsigned long missed_in_initial = 0;
	unsigned long missed_not_initial = 0;
	/* Loop variables */
	unsigned long bpf_idx, scan_idx;
	bool was_initial;
	unsigned long offset, vma_size;
	unsigned long lo, hi, mid;

	pr_err("=== SCAN_COMPARE DEBUG MODE ===\n");

	/* Check BPF status */
	if (!cow_bpf_active()) {
		pr_err("BPF not active, cannot compare\n");
		exit(1);
	}

	drops = cow_bpf_drop_count();
	if (drops > 0) {
		pr_err("BPF ring overflow: %llu drops\n", (unsigned long long)drops);
	}

	/* Drain BPF addresses */
	if (cow_bpf_drain_addrs(&bpf_addrs, &bpf_addr_count) < 0) {
		pr_err("BPF drain failed\n");
		exit(1);
	}
	pr_err("BPF found: %lu unique pages\n", bpf_addr_count);

	/* Open pagemap for SCAN */
	snprintf(path, sizeof(path), "/proc/%d/pagemap", g_scanner_source_pid);
	pagemap_fd = open(path, O_RDONLY);
	if (pagemap_fd < 0) {
		pr_perror("Failed to open pagemap");
		exit(1);
	}

	regs = xmalloc(COW_PAGEMAP_SCAN_VEC_LEN * sizeof(*regs));
	if (!regs) {
		pr_err("Failed to allocate regs\n");
		exit(1);
	}

	/* Allocate array to collect all SCAN addresses for duplicate detection */
	scan_addrs_cap = bpf_addr_count > 0 ? bpf_addr_count * 2 : 1000000;
	scan_addrs = xmalloc(scan_addrs_cap * sizeof(*scan_addrs));
	if (!scan_addrs) {
		pr_err("Failed to allocate scan_addrs\n");
		exit(1);
	}

	/* Run PAGEMAP_SCAN and compare with BPF */
	{
		unsigned long vma_count = 0;
		list_for_each_entry(lve, lazy_vmas, list) {
			pr_err("VMA[%lu]: 0x%lx-0x%lx\n", vma_count, lve->start, lve->end);
			vma_count++;
		}
		pr_err("Scanning %lu VMAs\n", vma_count);
	}

	list_for_each_entry(lve, lazy_vmas, list) {
		struct pm_scan_arg args;
		long regs_len;

		memset(&args, 0, sizeof(args));
		args.size = sizeof(args);
		args.flags = 0;  /* Just read, don't modify WP */
		args.start = lve->start;
		args.end = lve->end;
		args.walk_end = lve->start;
		args.vec = (u64)(unsigned long)regs;
		args.vec_len = COW_PAGEMAP_SCAN_VEC_LEN;
		args.max_pages = 0;
		args.category_anyof_mask = PAGE_IS_WRITTEN;
		args.return_mask = PAGE_IS_WRITTEN;

		do {
			int r;
			args.start = args.walk_end;

			regs_len = ioctl(pagemap_fd, PAGEMAP_SCAN, &args);
			if (regs_len <= 0)
				break;

			for (r = 0; r < regs_len; r++) {
				unsigned long addr;

				for (addr = regs[r].start; addr < regs[r].end; addr += pgsz) {
					/* Collect address for duplicate detection */
					if (scan_addrs_count >= scan_addrs_cap) {
						unsigned long new_cap = scan_addrs_cap * 2;
						unsigned long *tmp = xrealloc(scan_addrs, new_cap * sizeof(*tmp));
						if (!tmp) {
							pr_err("Failed to grow scan_addrs\n");
							exit(1);
						}
						scan_addrs = tmp;
						scan_addrs_cap = new_cap;
					}
					scan_addrs[scan_addrs_count++] = addr;
				}
			}
		} while (args.walk_end < lve->end);
	}

	xfree(regs);
	close(pagemap_fd);

	pr_err("Raw SCAN returned: %lu addresses\n", scan_addrs_count);

	/* Sort SCAN addresses for dedup and binary search */
	qsort(scan_addrs, scan_addrs_count, sizeof(*scan_addrs), scan_compare_addr_cmp);

	/* Count duplicates and unique SCAN addresses */
	{
		unsigned long unique = 0;
		unsigned long duplicates = 0;
		unsigned long i;

		if (scan_addrs_count > 0) {
			unique = 1;
			for (i = 1; i < scan_addrs_count; i++) {
				if (scan_addrs[i] == scan_addrs[i - 1]) {
					pr_debug("SCAN DUPLICATE: 0x%lx\n", scan_addrs[i]);
					duplicates++;
				} else {
					/* Move unique to front */
					scan_addrs[unique++] = scan_addrs[i];
				}
			}
		}
		pr_err("SCAN duplicates: %lu, unique: %lu\n", duplicates, unique);
		scan_count = unique;  /* Now scan_count = unique SCAN pages */
	}

	/* Get initial dirty pages from BPF start time */
	initial_dirty = cow_bpf_get_initial_dirty(&initial_dirty_count);
	pr_err("Initial dirty pages at BPF start: %lu\n", initial_dirty_count);

	/* Now compare: both arrays are sorted and unique */
	bpf_idx = 0;
	scan_idx = 0;

	while (bpf_idx < bpf_addr_count && scan_idx < scan_count) {
		if (bpf_addrs[bpf_idx] == scan_addrs[scan_idx]) {
			scan_in_bpf++;
			bpf_idx++;
			scan_idx++;
		} else if (bpf_addrs[bpf_idx] < scan_addrs[scan_idx]) {
			/* BPF has page that SCAN doesn't */
			bpf_idx++;
		} else {
			/* SCAN has page that BPF doesn't - BPF MISSED */
			/* Check if this was in initial dirty set */
			was_initial = false;
			if (initial_dirty && initial_dirty_count > 0) {
				/* Binary search in initial_dirty (it's sorted) */
				lo = 0;
				hi = initial_dirty_count;
				while (lo < hi) {
					mid = (lo + hi) / 2;
					if (initial_dirty[mid] == scan_addrs[scan_idx]) {
						was_initial = true;
						break;
					} else if (initial_dirty[mid] < scan_addrs[scan_idx]) {
						lo = mid + 1;
					} else {
						hi = mid;
					}
				}
			}

			/* Find which VMA this belongs to */
			vma_match = NULL;
			list_for_each_entry(lve, lazy_vmas, list) {
				if (scan_addrs[scan_idx] >= lve->start &&
				    scan_addrs[scan_idx] < lve->end) {
					vma_match = lve;
					break;
				}
			}
			if (vma_match) {
				offset = scan_addrs[scan_idx] - vma_match->start;
				vma_size = vma_match->end - vma_match->start;
				pr_err("BPF MISSED: 0x%lx (VMA 0x%lx-0x%lx, offset=0x%lx, vma_size=%luMB) %s\n",
				       scan_addrs[scan_idx], vma_match->start, vma_match->end,
				       offset, vma_size / (1024 * 1024),
				       was_initial ? "WAS_INITIAL_DIRTY" : "NEW_DIRTY");
			} else {
				pr_err("BPF MISSED: 0x%lx (NO VMA MATCH!) %s\n",
				       scan_addrs[scan_idx],
				       was_initial ? "WAS_INITIAL_DIRTY" : "NEW_DIRTY");
			}
			if (was_initial)
				missed_in_initial++;
			else
				missed_not_initial++;
			scan_only++;
			scan_idx++;
		}
	}
	/* Remaining SCAN addresses are all missed by BPF */
	while (scan_idx < scan_count) {
		/* Check if this was in initial dirty set */
		was_initial = false;
		if (initial_dirty && initial_dirty_count > 0) {
			lo = 0;
			hi = initial_dirty_count;
			while (lo < hi) {
				mid = (lo + hi) / 2;
				if (initial_dirty[mid] == scan_addrs[scan_idx]) {
					was_initial = true;
					break;
				} else if (initial_dirty[mid] < scan_addrs[scan_idx]) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
		}

		vma_match = NULL;
		list_for_each_entry(lve, lazy_vmas, list) {
			if (scan_addrs[scan_idx] >= lve->start &&
			    scan_addrs[scan_idx] < lve->end) {
				vma_match = lve;
				break;
			}
		}
		if (vma_match) {
			offset = scan_addrs[scan_idx] - vma_match->start;
			vma_size = vma_match->end - vma_match->start;
			pr_err("BPF MISSED: 0x%lx (VMA 0x%lx-0x%lx, offset=0x%lx, vma_size=%luMB) %s\n",
			       scan_addrs[scan_idx], vma_match->start, vma_match->end,
			       offset, vma_size / (1024 * 1024),
			       was_initial ? "WAS_INITIAL_DIRTY" : "NEW_DIRTY");
		} else {
			pr_err("BPF MISSED: 0x%lx (NO VMA MATCH!) %s\n",
			       scan_addrs[scan_idx],
			       was_initial ? "WAS_INITIAL_DIRTY" : "NEW_DIRTY");
		}
		if (was_initial)
			missed_in_initial++;
		else
			missed_not_initial++;
		scan_only++;
		scan_idx++;
	}

	pr_err("=== SCAN_COMPARE RESULTS ===\n");
	pr_err("BPF found:  %lu unique pages\n", bpf_addr_count);
	pr_err("SCAN found: %lu unique pages\n", scan_count);
	pr_err("In both:    %lu pages\n", scan_in_bpf);
	pr_err("SCAN only:  %lu pages (BPF MISSED)\n", scan_only);
	pr_err("  - Was initial dirty: %lu (dirty before BPF started)\n", missed_in_initial);
	pr_err("  - New dirty:         %lu (written after BPF, but BPF missed)\n", missed_not_initial);
	pr_err("BPF only:   %lu pages (SCAN missed)\n",
	       bpf_addr_count > scan_in_bpf ? bpf_addr_count - scan_in_bpf : 0);

	if (bpf_addrs)
		xfree(bpf_addrs);
	if (scan_addrs)
		xfree(scan_addrs);

	pr_err("=== EXITING DEBUG MODE ===\n");

}
#endif /* SCAN_COMPARE */

/*
 * Check if using BPF mode (no scanner threads).
 */
bool cow_using_bpf_mode(void)
{
	return g_using_bpf_mode;
}
#endif /* CONFIG_HAS_LIBBPF */

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
 * Thread-local send buffer (header + compressed-size + compressed-data),
 * sized for the max batch so it's allocated once per thread and reused.
 */
#define COW_SEND_BUF_SIZE	(sizeof(struct page_server_iov) + sizeof(int) + \
				 LZ4_COMPRESSBOUND(COW_BATCH_SIZE))
static __thread char *tls_send_buf;

/*
 * Thread-local compression counters. Snapshot at phase boundaries to report
 * per-phase compression ratio without atomics on the hot path.
 */
static __thread unsigned long tls_compress_in_bytes;
static __thread unsigned long tls_compress_out_bytes;
static __thread unsigned long tls_total_sent_pages;

/*
 * Send a batch of pages with LZ4 compression.
 * Protocol: header (PS_IOV_ADD_F_COMPRESS) + compressed_size + compressed_data
 * Header contains nr_pages and base_vaddr.
 *
 * acceleration: LZ4_compress_fast acceleration. 1 matches LZ4_compress_default
 * (best ratio). Larger values (e.g. 99) trade ratio for CPU - used during
 * Phase 2/3 convergence where CPU is the bottleneck and the payload is
 * already mostly modified (poorly-compressible) pages.
 */
int send_pages_batch_compressed(int sk, const void *data,
				int nr_pages, u64 dst_id,
				unsigned long base_vaddr,
				int acceleration)
{
	int max_compressed = LZ4_compressBound(nr_pages * PAGE_SIZE);
	int total_uncompressed = nr_pages * PAGE_SIZE;
	char *send_buf;
	struct page_server_iov *pi;
	int *compressed_size;
	char *compressed_data;
	int total_len, ret;

	if (!tls_send_buf) {
		tls_send_buf = xmalloc(COW_SEND_BUF_SIZE);
		BUG_ON(!tls_send_buf);
	}
	send_buf = tls_send_buf;

	pi = (struct page_server_iov *)send_buf;
	compressed_size = (int *)(send_buf + sizeof(*pi));
	compressed_data = send_buf + sizeof(*pi) + sizeof(int);

	/* Compress entire batch */
	*compressed_size = LZ4_compress_fast(data, compressed_data,
					     total_uncompressed, max_compressed,
					     acceleration);
	if (*compressed_size <= 0) {
		pr_err("LZ4 compression failed for batch at %lx (%d pages)\n",
		       base_vaddr, nr_pages);
		return -1;
	}

	/* Thread-local counters — flushed to globals once at thread exit */
	tls_compress_in_bytes += total_uncompressed;
	tls_compress_out_bytes += *compressed_size;

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

	if (ret != total_len) {
		pr_perror("Failed to send compressed batch (sent %d/%d)", ret, total_len);
		return -1;
	}

	return 0;
}

/*
 * Thread-local page read buffer, sized for the max batch.
 * Allocated once per thread and reused across all process_vm_readv calls.
 */
static __thread char *tls_read_buf;

static inline char *cow_get_read_buf(void)
{
	if (!tls_read_buf) {
		tls_read_buf = xmalloc(COW_BATCH_SIZE);
		BUG_ON(!tls_read_buf);
	}
	return tls_read_buf;
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

	buffer = cow_get_read_buf();

	/* Single process_vm_readv for all pages */
	local_iov.iov_base = buffer;
	local_iov.iov_len = nr_pages * PAGE_SIZE;
	remote_iov.iov_base = (void *)base_vaddr;
	remote_iov.iov_len = nr_pages * PAGE_SIZE;

	ret = process_vm_readv(source_pid, &local_iov, 1, &remote_iov, 1, 0);
	if (ret != (ssize_t)(nr_pages * PAGE_SIZE)) {
		pr_perror("Failed to read %d pages at %lx from pid %d (got %d)",
			  nr_pages, base_vaddr, source_pid, ret);
		return -1;
	}

	/*
	 * Pre-freeze bulk transfer: we have CPU time to spare because the
	 * process is still running; use best-ratio LZ4 to save network.
	 */
	ret = send_pages_batch_compressed(sk, buffer, nr_pages, dst_id,
					  base_vaddr, 1);
	if (ret < 0)
		return -1;

	return nr_pages;
}

/*
 * A slice of a dirty region: a contiguous [start, start + nr_pages*PAGE)
 * sub-range of one dirty_region_entry. A batch may mix many whole small
 * regions with a sliced portion of a larger one.
 */
struct dirty_slice {
	unsigned long start;
	int nr_pages;
	u64 dst_id;
};

/*
 * Read nr_slices ranges from the source process in one process_vm_readv,
 * then send each slice via the existing compressed path on the
 * corresponding buffer offset.
 *
 * Caller guarantees:
 *   - all slices share the same source_pid (one queue == one source today)
 *   - sum of nr_pages across slices <= COW_BATCH_PAGES (tls_read_buf is
 *     sized for COW_BATCH_SIZE)
 *   - nr_slices <= COW_BATCH_PAGES (and thus well below IOV_MAX)
 *
 * On short/failed readv we recurse per slice so one bad range (e.g. an
 * unmap-during-scan race on a single page) doesn't drop the whole batch.
 *
 * Convergence / post-freeze path. Keep acceleration=1. Experiment:
 * acceleration=99 was tried here to cut LZ4 CPU (was 51% of P3 thread
 * time). It regressed the total P3 wall-clock 2.3s -> 4.8s: ratio dropped
 * from ~25% to ~53-84% on this workload (dirty pages still compress),
 * and the extra wire bytes shifted the bottleneck into tcp_sendmsg +
 * skb_page_frag_refill, with atomic CAS contention roughly doubling.
 *
 * Returns total pages sent, or -1 on a fatal send error.
 */
static int send_dirty_slices(struct p3_thread_ctx *ctx,
			     pid_t source_pid,
			     const struct dirty_slice *slices,
			     int nr_slices)
{
	void *buffer = cow_get_read_buf();
	struct iovec local_iov;
	struct iovec remote_iov[COW_BATCH_PAGES];
	unsigned long total_bytes = 0;
	int total_sent = 0;
	ssize_t ret;
	int i;

	if (nr_slices <= 0)
		return 0;

	for (i = 0; i < nr_slices; i++) {
		unsigned long len = (unsigned long)slices[i].nr_pages * PAGE_SIZE;

		remote_iov[i].iov_base = (void *)slices[i].start;
		remote_iov[i].iov_len = len;
		total_bytes += len;
	}

	local_iov.iov_base = buffer;
	local_iov.iov_len = total_bytes;

	ret = process_vm_readv(source_pid, &local_iov, 1,
			       remote_iov, nr_slices, 0);
	BUG_ON(ret != (ssize_t)total_bytes);

	{
		unsigned long offset = 0;
		for (i = 0; i < nr_slices; i++) {
			int nr_pages = slices[i].nr_pages;
			int srv;

			srv = send_pages_batch_compressed(ctx->socket,
							  (char *)buffer + offset,
							  nr_pages, slices[i].dst_id,
							  slices[i].start, 1);
			BUG_ON(srv < 0);
			total_sent += nr_pages;
			ctx->pages_sent += nr_pages;
			tls_total_sent_pages += nr_pages;
			offset += (unsigned long)nr_pages * PAGE_SIZE;
		}
	}

	return total_sent;
}

/*
 * NOTE: Work-stealing from queue consumption was removed because the sender
 * queues use SPSC (Single Producer Single Consumer) design. Stealing would
 * introduce multiple consumers and cause race conditions.
 *
 * The bulk transfer phase uses work-stealing via the shared work queue
 * (g_work_queue), which is safe because it uses atomic fetch-and-add.
 *
 * For better queue balancing, consider:
 * 1. Having scanners distribute more evenly (round-robin by page count, not region count)
 * 2. Using MPMC queues if work-stealing is needed
 */

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

	buffer = cow_get_read_buf();

	for (i = my_start_idx; i < my_end_idx; i++) {
		unsigned long start = g_new_vma_ranges[i * 2];
		unsigned long len = g_new_vma_ranges[i * 2 + 1];
		unsigned long vaddr;

		pr_debug("P3[%d] new VMA %lx-%lx (%lu pages)\n",
			 thread_id, start, start + len, len / PAGE_SIZE);
		pr_debug("VMA_TRACE: phase=PHASE3_SEND_NEW thread_id=%d range=0x%lx-0x%lx pages=%lu\n",
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

			/*
			 * New VMAs only surface during/after freeze. Keep
			 * acceleration=1 for the same reason as
			 * send_dirty_slices: acceleration=99 regressed
			 * P3 wall-clock 2.3s -> 4.8s by shifting the
			 * bottleneck to tcp_sendmsg.
			 */
			ret = send_pages_batch_compressed(ctx->socket, buffer,
							  batch_pages, ctx->dst_id,
							  vaddr, 1);
			if (ret < 0) {
				pr_err("P3[%d] failed to send new VMA pages at %lx, aborting\n",
				       thread_id, vaddr);
				return total_sent;  /* Abort - socket is likely broken */
			}

			total_sent += batch_pages;
			ctx->pages_sent += batch_pages;
			vaddr += batch_pages * PAGE_SIZE;
		}
	}

	pr_info("P3[%d] sent %lu pages from new VMAs\n", thread_id, total_sent);
	return total_sent;
}

/*
 * P3 bulk sender thread - sends regular pages in batches.
 * Uses work-stealing: threads pull chunks from a shared work queue.
 * After bulk transfer, transitions to iterative dirty scanning until convergence.
 */
static void *p3_bulk_sender_thread(void *arg)
{
	struct p3_thread_ctx *ctx = (struct p3_thread_ctx *)arg;
	unsigned long total_sent = 0;
	struct timespec t_start, t_end;
	int thread_id = ctx->thread_id;
	int chunks_processed = 0;

	pr_info("P3[%d] bulk sender thread started (batch=%d pages)\n",
		thread_id, COW_BATCH_PAGES);
	pr_debug("DEBUG_THREAD: P3 sender[%d] STARTED socket=%d dst_id=%lu\n",
	       thread_id, ctx->socket, (unsigned long)ctx->dst_id);
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* === Iteration 0: Bulk transfer with work-stealing === */
	{
		struct timespec bulk_start, bulk_end;
		long bulk_elapsed_ms;
		struct bulk_work_item *work;
		unsigned long bulk_in_start = tls_compress_in_bytes;
		unsigned long bulk_out_start = tls_compress_out_bytes;
		unsigned long bulk_in_bytes, bulk_out_bytes;
		float bulk_ratio_pct = 0.0f;

		clock_gettime(CLOCK_MONOTONIC, &bulk_start);

		/*
		 * Only COW_NUM_P3_THREADS_BULK threads participate in bulk transfer.
		 * Other threads skip to phase 2 (dirty scanning) where all threads
		 * are needed to keep up with parallel scanners.
		 */
		if (thread_id < COW_NUM_P3_THREADS_BULK) {
#ifdef COW_P3_SENDER_CPU
			pin_to_cpu(COW_P3_SENDER_CPU);
#endif
			pr_err("P3[%d]: Starting bulk transfer (work-stealing)\n", thread_id);

			/* Pull work items from shared queue until exhausted */
			while ((work = get_next_work_item()) != NULL) {
				unsigned long vaddr;

				for (vaddr = work->start; vaddr < work->end; ) {
					unsigned long next_bound = (vaddr + COW_BATCH_SIZE) & COW_BATCH_ALIGN_MASK;
					unsigned long batch_end = (next_bound < work->end) ? next_bound : work->end;
					int batch_pages = (batch_end - vaddr) / PAGE_SIZE;
					int sent;

					sent = send_lazy_vma_pages_batch(
						ctx->socket, work->lve, vaddr, batch_pages,
						ctx->dst_id, ctx->source_pid);

					if (sent < 0) {
						pr_err("P3[%d]: Failed to send batch at %lx\n",
						       thread_id, vaddr);
						BUG();
					}

					total_sent += sent;
					vaddr += batch_pages * PAGE_SIZE;
				}
				chunks_processed++;
			}
		} else {
			pr_err("P3[%d]: Skipping bulk (idle until phase 2)\n", thread_id);
		}

		clock_gettime(CLOCK_MONOTONIC, &bulk_end);
		bulk_elapsed_ms = (bulk_end.tv_sec - bulk_start.tv_sec) * 1000 +
				  (bulk_end.tv_nsec - bulk_start.tv_nsec) / 1000000;

		bulk_in_bytes = tls_compress_in_bytes - bulk_in_start;
		bulk_out_bytes = tls_compress_out_bytes - bulk_out_start;
		if (bulk_in_bytes > 0)
			bulk_ratio_pct = (float)bulk_out_bytes * 100.0f / bulk_in_bytes;
		pr_err("P3[%d] TIMING: Bulk transfer done: %lu pages, %d chunks in %ld ms "
		       "(compress: %lu -> %lu bytes, ratio=%.1f%%)\n",
		       thread_id, total_sent, chunks_processed, bulk_elapsed_ms,
		       bulk_in_bytes, bulk_out_bytes, bulk_ratio_pct);

		/* Signal scanner that this thread's bulk transfer is complete */
		__atomic_fetch_add(&g_bulk_transfer_done_count, 1, __ATOMIC_RELEASE);

#ifdef COW_P3_SENDER_CPU
		/* Unpin CPU for phase 2 - all threads need full CPU access */
		if (thread_id < COW_NUM_P3_THREADS_BULK)
			unpin_cpu();
#endif
	}

	/* === Phase 2: Consume dirty regions from scanner queue === */
	{
		struct timespec loop_start, loop_end, p3_start, scan_done_time;
		long loop_elapsed_ms;
		unsigned long loop_total_pages = 0;
		unsigned long regions_processed = 0;
		unsigned long slices_sent = 0;
		unsigned long packed_batches = 0;
		unsigned long p3_pages = 0;
		unsigned long pages_before_scan_done = 0;
		bool p3_started = false;
		bool scan_done_logged = false;
		unsigned long cursor_vaddr = 0;
		unsigned long queue_in_start;
		unsigned long queue_out_start;
		unsigned long queue_in_bytes, queue_out_bytes;
		float queue_ratio_pct = 0.0f;

#ifndef COW_PRE_SCAN
		/* Without pre-scan, wait for freeze signal before consuming queue */
		while (!__atomic_load_n(&g_scanner_freeze_signal, __ATOMIC_ACQUIRE)) {
			usleep(COW_USLEEP_1MS);
		}
#endif
		/* With pre-scan, start consuming immediately - don't wait for freeze */

		clock_gettime(CLOCK_MONOTONIC, &loop_start);
		queue_in_start = tls_compress_in_bytes;
		queue_out_start = tls_compress_out_bytes;

		/*
		 * MPMC consumer: CAS-claim one entry at a time from the
		 * shared convergence queue. Pack entries into 64-page
		 * batches for readv + compress + send.
		 */
		{
			struct dirty_region_entry *region = NULL;

			while (1) {
				struct dirty_slice slices[COW_BATCH_PAGES];
				int nr_slices = 0;
				int pages_left = COW_BATCH_PAGES;
				int sent;

				if (!p3_started && g_last_scan_flag) {
					p3_started = true;
					clock_gettime(CLOCK_MONOTONIC, &p3_start);
				}
				if (!scan_done_logged && cow_is_scan_complete()) {
					scan_done_logged = true;
					pages_before_scan_done = p3_pages;
					clock_gettime(CLOCK_MONOTONIC, &scan_done_time);
				}

				if (!region) {
					region = conv_queue_pop();
					if (!region) {
						if (cow_is_scan_complete() &&
						    __atomic_load_n(&g_conv_tail, __ATOMIC_RELAXED) >=
						    __atomic_load_n(&g_conv_head, __ATOMIC_ACQUIRE))
							break;
						usleep(COW_USLEEP_100US);
						continue;
					}
					cursor_vaddr = region->start;
				}

				while (pages_left > 0 && region) {
					int have = (int)((region->end - cursor_vaddr) / PAGE_SIZE);
					int take = have < pages_left ? have : pages_left;

					slices[nr_slices].start = cursor_vaddr;
					slices[nr_slices].nr_pages = take;
					slices[nr_slices].dst_id = region->dst_id;
					nr_slices++;
					cursor_vaddr += (unsigned long)take * PAGE_SIZE;
					pages_left -= take;

					if (cursor_vaddr == region->end) {
						regions_processed++;
						xfree(region);
						region = conv_queue_pop();
						if (region)
							cursor_vaddr = region->start;
					}
				}

				if (nr_slices == 0)
					continue;

				sent = send_dirty_slices(ctx, g_scanner_source_pid,
							 slices, nr_slices);
				BUG_ON(sent <= 0);
				loop_total_pages += sent;
				slices_sent += nr_slices;
				packed_batches++;
				if (p3_started)
					p3_pages += sent;
			}

			if (region)
				xfree(region);
		}

		clock_gettime(CLOCK_MONOTONIC, &loop_end);

		/* Capture scan_done_time if loop exited with scan complete but flag not yet set */
		if (!scan_done_logged && cow_is_scan_complete()) {
			scan_done_logged = true;
			pages_before_scan_done = p3_pages;
			scan_done_time = loop_end;
		}

		loop_elapsed_ms = (loop_end.tv_sec - loop_start.tv_sec) * 1000 +
				  (loop_end.tv_nsec - loop_start.tv_nsec) / 1000000;

		/* Print Phase 3 specific timing */
		if (p3_started) {
			long p3_total_ms = (loop_end.tv_sec - p3_start.tv_sec) * 1000 +
					   (loop_end.tv_nsec - p3_start.tv_nsec) / 1000000;
			long send_during_scan_ms = 0;
			long send_after_scan_ms = 0;

			if (scan_done_logged) {
				send_during_scan_ms = (scan_done_time.tv_sec - p3_start.tv_sec) * 1000 +
						      (scan_done_time.tv_nsec - p3_start.tv_nsec) / 1000000;
				send_after_scan_ms = (loop_end.tv_sec - scan_done_time.tv_sec) * 1000 +
						     (loop_end.tv_nsec - scan_done_time.tv_nsec) / 1000000;
			}
			pr_err("P3[%d] TIMING P3: total=%ld ms (during_scan=%ld ms [%lu pages] + after_scan=%ld ms [%lu pages])\n",
			       thread_id, p3_total_ms, send_during_scan_ms, pages_before_scan_done,
			       send_after_scan_ms, p3_pages - pages_before_scan_done);
		}
		queue_in_bytes = tls_compress_in_bytes - queue_in_start;
		queue_out_bytes = tls_compress_out_bytes - queue_out_start;
		if (queue_in_bytes > 0)
			queue_ratio_pct = (float)queue_out_bytes * 100.0f / queue_in_bytes;
		{
			float avg_slices = 0.0f;
			float avg_pages = 0.0f;
			if (packed_batches > 0) {
				avg_slices = (float)slices_sent / packed_batches;
				avg_pages = (float)loop_total_pages / packed_batches;
			}
			pr_err("P3[%d] TIMING: Queue consumption done: "
			       "%lu regions, %lu pages in %ld ms "
			       "(packed_batches=%lu slices=%lu "
			       "avg_slices/batch=%.2f avg_pages/batch=%.2f) "
			       "(compress: %lu -> %lu bytes, ratio=%.1f%%)\n",
			       thread_id, regions_processed,
			       loop_total_pages, loop_elapsed_ms,
			       packed_batches, slices_sent,
			       avg_slices, avg_pages,
			       queue_in_bytes, queue_out_bytes, queue_ratio_pct);
		}
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


	clock_gettime(CLOCK_MONOTONIC, &t_end);
	{
		long elapsed_ms = (t_end.tv_sec - t_start.tv_sec) * 1000 +
				  (t_end.tv_nsec - t_start.tv_nsec) / 1000000;

		pr_err("P3[%d] done: %lu pages, %ld ms\n",
		       thread_id, ctx->pages_sent, elapsed_ms);
	}

	/* Flush thread-local counters to globals (one atomic per counter) */
	__sync_fetch_and_add(&g_total_sent_pages, tls_total_sent_pages);
	__sync_fetch_and_add(&g_compress_uncompressed_bytes, tls_compress_in_bytes);
	__sync_fetch_and_add(&g_compress_compressed_bytes, tls_compress_out_bytes);
	tls_total_sent_pages = 0;
	tls_compress_in_bytes = 0;
	tls_compress_out_bytes = 0;

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

	/* Build shared work queue for bulk transfer (work-stealing) */
	build_bulk_work_queue(dst_id);

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

	pr_err("Started %d P3 bulk sender threads (%d sockets)\n",
		p3_threads_active, threads_to_start);
	return p3_threads_active > 0 ? 0 : -1;
}

void cow_wait_p3_threads(void)
{
	int i;
	unsigned long total = 0;
	int errors = 0;
	struct timespec t_scanner_done, t_senders_done;
	long scanner_ms, senders_ms, total_ms;

	/* Wait for scanner thread first */
	cow_wait_scanner_thread();
	clock_gettime(CLOCK_MONOTONIC, &t_scanner_done);

	/* Then wait for sender threads */
	for (i = 0; i < COW_NUM_P3_THREADS; i++) {
		if (p3_threads[i].thread) {
			pthread_join(p3_threads[i].thread, NULL);
			total += p3_threads[i].pages_sent;
			if (p3_threads[i].error)
				errors++;
			p3_threads[i].thread = 0;
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &t_senders_done);

	/* Close P3 sockets so replica receivers get EOF */
	for (i = 0; i < COW_NUM_P3_THREADS; i++) {
		if (p3_threads[i].socket >= 0) {
			close(p3_threads[i].socket);
			p3_threads[i].socket = -1;
		}
	}

	p3_total_pages_sent = total;
	p3_threads_active = 0;

	/* Print timing breakdown from freeze signal */
	scanner_ms = (t_scanner_done.tv_sec - g_freeze_signal_time.tv_sec) * 1000 +
		     (t_scanner_done.tv_nsec - g_freeze_signal_time.tv_nsec) / 1000000;
	senders_ms = (t_senders_done.tv_sec - t_scanner_done.tv_sec) * 1000 +
		     (t_senders_done.tv_nsec - t_scanner_done.tv_nsec) / 1000000;
	total_ms = (t_senders_done.tv_sec - g_freeze_signal_time.tv_sec) * 1000 +
		   (t_senders_done.tv_nsec - g_freeze_signal_time.tv_nsec) / 1000000;

	pr_err("P3 TIMING from freeze: scanner=%ld ms, senders=%ld ms, total=%ld ms, %lu pages\n",
	       scanner_ms, senders_ms, total_ms, total);

	/* Verify all scanned pages were sent */
	{
		unsigned long scanned = __atomic_load_n(&g_total_scanned_pages, __ATOMIC_ACQUIRE);
		unsigned long sent = __atomic_load_n(&g_total_sent_pages, __ATOMIC_ACQUIRE);
		pr_err("P3 verification: scanned=%lu sent=%lu\n", scanned, sent);
		if (sent != scanned) {
			pr_err("BUG: scanned pages (%lu) != sent pages (%lu), missing %lu pages!\n",
			       scanned, sent, scanned > sent ? scanned - sent : sent - scanned);
			BUG();
		}
	}

	if (errors > 0)
		pr_warn("P3 threads completed with %d errors\n", errors);
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
 * In scanner architecture with COW_PRE_SCAN: returns true when scanner signals freeze.
 * In BPF mode or without COW_PRE_SCAN: returns true immediately after bulk transfer completes.
 */
bool cow_all_threads_below_threshold(void)
{
#ifdef CONFIG_HAS_LIBBPF
	/*
	 * In BPF mode: no iterative scanning, freeze immediately after bulk transfer.
	 * Check that all sender threads have completed bulk transfer.
	 */
	if (g_using_bpf_mode) {
		int done = __atomic_load_n(&g_bulk_transfer_done_count, __ATOMIC_ACQUIRE);
		int total = __atomic_load_n(&g_num_sender_threads, __ATOMIC_ACQUIRE);
		return done >= total && p3_threads_active > 0;
	}
#endif

#ifdef COW_PRE_SCAN
	/* Scanner decides when to freeze based on total dirty pages < threshold */
	return g_last_scan_flag && p3_threads_active > 0;
#else
	/* No pre-scan: freeze immediately after bulk transfer completes */
	int done = __atomic_load_n(&g_bulk_transfer_done_count, __ATOMIC_ACQUIRE);
	int total = __atomic_load_n(&g_num_sender_threads, __ATOMIC_ACQUIRE);
	return done >= total && p3_threads_active > 0;
#endif
}

/*
 * Signal P3 threads to do final scan and exit.
 * Called by main thread after freezing the process.
 */
void cow_signal_last_scan(void)
{
	pr_err("=== CONVERGENCE: Signaling last scan ===\n");
	clock_gettime(CLOCK_MONOTONIC, &g_freeze_signal_time);
	g_last_scan_flag = true;

#ifdef CONFIG_HAS_LIBBPF
	/*
	 * In BPF mode: drain ring buffer and distribute to sender queues.
	 * No scanner threads to signal.
	 */
	if (g_using_bpf_mode) {
		int dirty_pages = cow_bpf_drain_to_queues();
		if (dirty_pages < 0) {
			pr_err("BPF drain failed!\n");
			BUG();
		}
		pr_err("BPF mode: drained %d dirty pages to sender queues\n", dirty_pages);
		cow_bpf_stop();
		__sync_synchronize();
		return;
	}
#endif

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
