/*
 * COW (Copy-on-Write) memory tracking for lazy page migration.
 *
 * This file contains the global lazy VMA list management and
 * convergence mode handling for COW dump operations.
 */

#include <pthread.h>

#include "types.h"
#include "cr_options.h"
#include "cow-mem.h"
#include "log.h"
#include "xmalloc.h"
#include "atomic-bitmap.h"
#include "page-xfer.h"

/* Global lazy VMA list for COW dump */
static LIST_HEAD(global_lazy_vmas);
static pthread_spinlock_t lazy_vmas_lock;
static pthread_once_t lazy_vmas_lock_once = PTHREAD_ONCE_INIT;

static void init_lazy_vmas_lock_once(void)
{
	pthread_spin_init(&lazy_vmas_lock, PTHREAD_PROCESS_PRIVATE);
}

void cow_mem_init_lazy_vmas(void)
{
	pthread_once(&lazy_vmas_lock_once, init_lazy_vmas_lock_once);
}

struct list_head *get_global_lazy_vmas(void)
{
	return &global_lazy_vmas;
}

/* Find lazy VMA entry for given address and dst_id (exported for page-xfer.c) */
struct lazy_vma_entry *find_lazy_vma_for_addr(unsigned long vaddr, u64 dst_id)
{
	struct lazy_vma_entry *lve;

	/* Ensure lock is initialized (pthread_once guarantees single init) */
	cow_mem_init_lazy_vmas();

	pthread_spin_lock(&lazy_vmas_lock);

	list_for_each_entry(lve, &global_lazy_vmas, list) {
		if (vaddr >= lve->start &&
		    vaddr < lve->end &&
		    lve->dst_id == dst_id) {
			pthread_spin_unlock(&lazy_vmas_lock);
			return lve;
		}
	}
	pthread_spin_unlock(&lazy_vmas_lock);

	pr_err("Lazy VMA not found for vaddr=0x%lx dst_id=%lu\n", vaddr, dst_id);
	return NULL;
}

/* Find lazy VMA entry by address only (no dst_id filter) */
struct lazy_vma_entry *find_lazy_vma_by_addr(unsigned long vaddr)
{
	struct lazy_vma_entry *lve;

	/* Ensure lock is initialized (pthread_once guarantees single init) */
	cow_mem_init_lazy_vmas();

	pthread_spin_lock(&lazy_vmas_lock);

	list_for_each_entry(lve, &global_lazy_vmas, list) {
		if (vaddr >= lve->start && vaddr < lve->end) {
			pthread_spin_unlock(&lazy_vmas_lock);
			return lve;
		}
	}
	pthread_spin_unlock(&lazy_vmas_lock);

	return NULL;
}

/*
 * add_lazy_vma_for_new_region - Add a new VMA to global_lazy_vmas
 *
 * Called from Phase 3 when new VMAs are detected that weren't present
 * in Phase 1. These need to be added to global_lazy_vmas so the page
 * server can iterate through them during page transfer.
 *
 * @start: VMA start address
 * @len: VMA length in bytes
 * @dst_id: Process identifier for page transfer
 * @source_pid: PID for process_vm_readv
 *
 * Returns: 0 on success, -1 on error
 */
int add_lazy_vma_for_new_region(unsigned long start, unsigned long len,
				u64 dst_id, pid_t source_pid)
{
	struct lazy_vma_entry *lve;
	unsigned long nr_pages, bitmap_size;

	lve = xmalloc(sizeof(*lve));
	if (!lve)
		return -1;

	cow_mem_init_lazy_vmas();

	nr_pages = len / PAGE_SIZE;
	lve->start = start;
	lve->end = start + len;
	lve->total_pages = nr_pages;
	lve->dst_id = dst_id;
	lve->source_pid = source_pid;
	lve->vma = NULL;  /* No vma_area for Phase 3 discovered regions */

	/* Allocate bitmaps (all zeros - no pages sent yet) */
	bitmap_size = BITMAP_ALLOC_SIZE(nr_pages);
	lve->sent_bitmap = xzalloc(bitmap_size);
	if (!lve->sent_bitmap) {
		xfree(lve);
		return -1;
	}
	lve->cow_bitmap = xzalloc(bitmap_size);
	if (!lve->cow_bitmap) {
		xfree(lve->sent_bitmap);
		xfree(lve);
		return -1;
	}
	lve->sent_pages = 0;

	pthread_spin_lock(&lazy_vmas_lock);
	list_add_tail(&lve->list, &global_lazy_vmas);
	pthread_spin_unlock(&lazy_vmas_lock);

	pr_info("Added lazy VMA for new region 0x%lx-0x%lx "
		"(%lu pages, dst_id=%lu, pid=%d)\n",
		start, start + len, nr_pages,
		(unsigned long)dst_id, source_pid);

	return 0;
}

/*
 * cow_mem_add_lazy_vma - Add a lazy VMA entry during dump
 *
 * Called from generate_iovs() in mem.c when COW dump is enabled and
 * the VMA is lazy-capable. This adds the VMA to the global list for
 * later page transfer.
 *
 * @vma: VMA area being processed
 * @nr_pages: Number of pages in the VMA
 * @dst_id: Process identifier (vpid)
 * @source_pid: Real PID for process_vm_readv
 *
 * Returns: 0 on success, -1 on error
 */
int cow_mem_add_lazy_vma(struct vma_area *vma, unsigned long nr_pages,
			 u64 dst_id, pid_t source_pid)
{
	struct lazy_vma_entry *lve;
	unsigned long bitmap_size;

	lve = xmalloc(sizeof(*lve));
	if (!lve)
		return -1;

	/* Initialize global list on first use */
	cow_mem_init_lazy_vmas();

	lve->vma = vma;
	lve->total_pages = nr_pages;
	lve->dst_id = dst_id;
	lve->source_pid = source_pid;

	/* Allocate sent bitmap and cow bitmap for this VMA */
	bitmap_size = BITMAP_ALLOC_SIZE(nr_pages);
	lve->sent_bitmap = xzalloc(bitmap_size);
	if (!lve->sent_bitmap) {
		xfree(lve);
		return -1;
	}
	lve->cow_bitmap = xzalloc(bitmap_size);
	if (!lve->cow_bitmap) {
		xfree(lve->sent_bitmap);
		xfree(lve);
		return -1;
	}

	lve->start = vma->e->start;
	lve->end = vma->e->end;
	lve->sent_pages = 0;

	/* Add to global list (thread-safe) */
	pthread_spin_lock(&lazy_vmas_lock);
	list_add_tail(&lve->list, &global_lazy_vmas);
	pthread_spin_unlock(&lazy_vmas_lock);

	pr_debug("Added lazy VMA 0x%llx-0x%llx to global list "
		"(%lu pages, dst_id=%lu, pid=%d)\n",
		(unsigned long long)vma->e->start,
		(unsigned long long)vma->e->end, nr_pages,
		(unsigned long)dst_id, source_pid);

	return 0;
}

/* Count total pages in lazy VMAs for a given dst_id (exported for page-xfer.c) */
unsigned long count_lazy_vma_pages(u64 dst_id)
{
	struct lazy_vma_entry *lve;
	unsigned long total_pages = 0;

	/* Ensure lock is initialized (pthread_once guarantees single init) */
	cow_mem_init_lazy_vmas();

	pthread_spin_lock(&lazy_vmas_lock);
	list_for_each_entry(lve, &global_lazy_vmas, list) {
		if (lve->dst_id == dst_id)
			total_pages += lve->total_pages;
	}
	pthread_spin_unlock(&lazy_vmas_lock);

	return total_pages;
}

/* Convergence mode state */
static bool g_convergence_mode = false;
static unsigned long g_convergence_dirty_pages = 0;

bool is_convergence_mode(void)
{
	return g_convergence_mode;
}

unsigned long get_convergence_dirty_pages(void)
{
	return g_convergence_dirty_pages;
}

/*
 * Prepare lazy VMAs for Phase 3 convergence.
 * Clears sent_bitmap for pages in dirty_ranges so they can be re-sent.
 * Returns total number of dirty pages.
 *
 * dirty_ranges format: [start0, len0, start1, len1, ...]
 * Each range is (start_addr, length_in_bytes).
 */
unsigned long prepare_lazy_vmas_for_convergence(unsigned long *dirty_ranges,
						unsigned int nr_dirty_ranges)
{
	struct lazy_vma_entry *lve;
	unsigned long total_dirty_pages = 0;
	unsigned int i;

	if (!dirty_ranges || nr_dirty_ranges == 0) {
		pr_info("No dirty ranges for convergence\n");
		g_convergence_mode = true;
		g_convergence_dirty_pages = 0;
		return 0;
	}

	cow_mem_init_lazy_vmas();

	pthread_spin_lock(&lazy_vmas_lock);

	list_for_each_entry(lve, &global_lazy_vmas, list) {
		unsigned long vma_start = lve->start;
		unsigned long vma_end = lve->end;

		/* Check each dirty range against this VMA */
		for (i = 0; i < nr_dirty_ranges; i++) {
			unsigned long range_start = dirty_ranges[i * 2];
			unsigned long range_len = dirty_ranges[i * 2 + 1];
			unsigned long range_end = range_start + range_len;
			unsigned long overlap_start, overlap_end;
			unsigned long page_idx, page_idx_end;

			/* Calculate overlap between dirty range and this VMA */
			if (range_end <= vma_start || range_start >= vma_end)
				continue;  /* No overlap */

			overlap_start = (range_start > vma_start) ? range_start : vma_start;
			overlap_end = (range_end < vma_end) ? range_end : vma_end;

			/* Clear sent_bitmap for pages in overlap region */
			page_idx = (overlap_start - vma_start) / PAGE_SIZE;
			page_idx_end = (overlap_end - vma_start + PAGE_SIZE - 1) / PAGE_SIZE;

			if (page_idx_end > lve->total_pages)
				page_idx_end = lve->total_pages;

			for (; page_idx < page_idx_end; page_idx++) {
				if (lve->sent_bitmap &&
				    bitmap_test_nonatomic(lve->sent_bitmap, page_idx)) {
					bitmap_clear_nonatomic(lve->sent_bitmap, page_idx, &lve->sent_pages);
					total_dirty_pages++;
				}
			}
		}
	}

	pthread_spin_unlock(&lazy_vmas_lock);

	g_convergence_mode = true;
	g_convergence_dirty_pages = total_dirty_pages;
#if 0
	/* Enable debug logging for convergence phase debugging */
	opts.log_level = LOG_DEBUG;
	log_set_loglevel(opts.log_level);
	pr_info("Debug logging enabled for convergence phase\n");
#endif

	pr_info("Prepared %lu dirty pages for convergence from %u ranges\n",
		total_dirty_pages, nr_dirty_ranges);

	return total_dirty_pages;
}

/*
 * Verify all lazy VMA pages have been sent.
 * Returns 0 if all sent, or count of unsent pages.
 */
long verify_all_lazy_vmas_sent(void)
{
	struct lazy_vma_entry *lve;
	unsigned long total_sent = 0;
	unsigned long total_pages = 0;

	cow_mem_init_lazy_vmas();

	pthread_spin_lock(&lazy_vmas_lock);
	list_for_each_entry(lve, &global_lazy_vmas, list) {
		if (lve->sent_pages != lve->total_pages) {
			pr_warn("VMA %lx-%lx: sent=%lu total=%lu (unsent=%ld)\n",
				(unsigned long)lve->start, (unsigned long)lve->end,
				(unsigned long)lve->sent_pages, lve->total_pages,
				(long)(lve->total_pages - lve->sent_pages));
		}
		total_sent += lve->sent_pages;
		total_pages += lve->total_pages;
	}
	pthread_spin_unlock(&lazy_vmas_lock);

	if (total_sent != total_pages) {
		pr_warn("Total pages mismatch: sent=%lu total=%lu\n",
			total_sent, total_pages);
		return (long)(total_pages - total_sent);
	}

	pr_info("All %lu pages sent across all VMAs\n", total_pages);
	return 0;
}

/* Cleanup function for global lazy VMA list */
void free_global_lazy_vmas(void)
{
	struct lazy_vma_entry *lve, *tmp;

	/* If list is empty, nothing to free and lock may not be initialized */
	if (list_empty(&global_lazy_vmas))
		return;

	/* Init ensures lock is ready (pthread_once guarantees single init) */
	cow_mem_init_lazy_vmas();

	pthread_spin_lock(&lazy_vmas_lock);
	list_for_each_entry_safe(lve, tmp, &global_lazy_vmas, list) {
		list_del(&lve->list);
		if (lve->sent_bitmap)
			xfree(lve->sent_bitmap);
		if (lve->cow_bitmap)
			xfree(lve->cow_bitmap);
		xfree(lve);
	}
	pthread_spin_unlock(&lazy_vmas_lock);
}
