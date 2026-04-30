#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <stdbool.h>
#include <inttypes.h>
#include <linux/userfaultfd.h>
#include <pthread.h>
#include <time.h>
#include <string.h>

#include "types.h"
#include "cr_options.h"
#include "pstree.h"
#include "cow/cow-dump.h"
#include "mman.h"
#include "uffd.h"
#include "pagemap_scan.h"
#include "page-xfer.h"
#include "parasite-syscall.h"
#include "mem.h"
#include "vma.h"
#include "util.h"
#include "kerndat.h"
#include "criu-log.h"
#include "parasite.h"
#include "cow/cow-conf.h"
#include "cow/cow-bulk-send.h"
#include "common/bug.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-dump: "

struct cow_tracked_vma {
	unsigned long start;
	unsigned long end;
};

/* COW dump state for one dump session — single tracked process */
struct cow_dump_info {
	pid_t source_pid;
	u64 dst_id;            /* Process identifier for page transfer */
	int uffd;              /* WP_ASYNC uffd fd */
	unsigned int nr_tracked_vmas;
	struct cow_tracked_vma *tracked_vmas;
	enum cow_dump_phase phase;  /* Current phase */
};

/*
 * Applying UFFD write-protect over a large address space can dominate the
 * initial stall.  We apply it in parallel from the CRIU process.
 * COW_WP_CHUNK_SIZE is now defined in cow-conf.h
 */

struct cow_wp_range {
	unsigned long start;
	unsigned long len;
};

struct cow_wp_job {
	int uffd;
	struct cow_wp_range *ranges;
	unsigned int start_idx;
	unsigned int end_idx;
	int err;
};

static void *cow_wp_worker(void *arg)
{
	struct cow_wp_job *job = arg;
	struct uffdio_writeprotect wp;
	unsigned int i;

	for (i = job->start_idx; i < job->end_idx; i++) {
		wp.range.start = job->ranges[i].start;
		wp.range.len = job->ranges[i].len;
		wp.mode = UFFDIO_WRITEPROTECT_MODE_WP;

		if (ioctl(job->uffd, UFFDIO_WRITEPROTECT, &wp)) {
			job->err = -errno;
			return NULL;
		}
	}

	return NULL;
}

static unsigned int cow_wp_nr_threads(unsigned int nr_ranges)
{
	long nproc;
	unsigned int nr_threads;

	nproc = sysconf(_SC_NPROCESSORS_ONLN);
	if (nproc < 1)
		return 1;

	nr_threads = (unsigned int)nproc;
	if (nr_threads > nr_ranges)
		nr_threads = nr_ranges;
	if (nr_threads < 1)
		nr_threads = 1;

	return nr_threads;
}

static struct cow_wp_range *cow_wp_build_ranges(struct cow_dump_info *cdi,
						unsigned int *nr_ranges)
{
	struct cow_wp_range *ranges;
	unsigned long start, end, pos, len;
	unsigned int nr = 0, i, idx = 0;

	for (i = 0; i < cdi->nr_tracked_vmas; i++) {
		start = cdi->tracked_vmas[i].start;
		end = cdi->tracked_vmas[i].end;
		if (end <= start)
			continue;
		len = end - start;
		nr += (len + COW_WP_CHUNK_SIZE - 1) / COW_WP_CHUNK_SIZE;
	}

	if (nr == 0) {
		*nr_ranges = 0;
		return NULL;
	}

	*nr_ranges = nr;
	ranges = xmalloc(nr * sizeof(*ranges));
	BUG_ON(!ranges);

	for (i = 0; i < cdi->nr_tracked_vmas; i++) {
		start = cdi->tracked_vmas[i].start;
		end = cdi->tracked_vmas[i].end;
		if (end <= start)
			continue;
		pos = start;
		while (pos < end) {
			len = end - pos;
			if (len > COW_WP_CHUNK_SIZE)
				len = COW_WP_CHUNK_SIZE;
			ranges[idx].start = pos;
			ranges[idx].len = len;
			idx++;
			pos += len;
		}
	}

	*nr_ranges = idx;
	return ranges;
}

static int cow_apply_writeprotect(struct cow_dump_info *cdi)
{
	struct cow_wp_range *ranges;
	struct cow_wp_job *jobs;
	pthread_t *threads;
	struct timespec t_start, t_end;
	unsigned int nr_ranges, nr_threads, i, created = 0, per;
	unsigned long sec, nsec;
	int ret = 0;

	if (!cdi->nr_tracked_vmas)
		return 0;

	ranges = cow_wp_build_ranges(cdi, &nr_ranges);
	if (!ranges) {
		/* nr_ranges == 0 means no VMAs to protect, which is fine */
		BUG_ON(nr_ranges != 0);
		return 0;
	}

	nr_threads = cow_wp_nr_threads(nr_ranges);
	threads = xmalloc(nr_threads * sizeof(*threads));
	jobs = xzalloc(nr_threads * sizeof(*jobs));
	BUG_ON(!threads || !jobs);

	per = (nr_ranges + nr_threads - 1) / nr_threads;
	for (i = 0; i < nr_threads; i++) {
		jobs[i].uffd = cdi->uffd;
		jobs[i].ranges = ranges;
		jobs[i].start_idx = i * per;
		jobs[i].end_idx = jobs[i].start_idx + per;
		if (jobs[i].end_idx > nr_ranges)
			jobs[i].end_idx = nr_ranges;
	}

	clock_gettime(CLOCK_MONOTONIC, &t_start);

	for (i = 0; i < nr_threads; i++) {
		if (jobs[i].start_idx >= jobs[i].end_idx)
			break;
		if (pthread_create(&threads[i], NULL, cow_wp_worker, &jobs[i])) {
			pr_err("Failed to create write-protect worker thread\n");
			ret = -1;
			break;
		}
		created++;
	}

	for (i = 0; i < created; i++)
		pthread_join(threads[i], NULL);

	clock_gettime(CLOCK_MONOTONIC, &t_end);

	for (i = 0; i < created; i++) {
		if (jobs[i].err) {
			pr_err("Failed to apply UFFD write-protect: %s (%d)\n",
			       strerror(-jobs[i].err), -jobs[i].err);
			ret = -1;
			break;
		}
	}

	sec = t_end.tv_sec - t_start.tv_sec;
	if (t_end.tv_nsec < t_start.tv_nsec) {
		sec--;
		nsec = 1000000000UL + t_end.tv_nsec - t_start.tv_nsec;
	} else {
		nsec = t_end.tv_nsec - t_start.tv_nsec;
	}
	pr_info("TIMING: cow_dump_writeprotect took %lu.%06lu seconds (%u ranges, %u threads)\n",
		sec, nsec / 1000, nr_ranges, created ? created : 1);

	xfree(threads);
	xfree(jobs);
	xfree(ranges);
	return ret;
}

/* ------------------------------------------------------------------ */
/*  Global state                                                       */
/* ------------------------------------------------------------------ */

static struct cow_dump_info *g_cow_info = NULL;

/*
 * cow_set_dst_id - Update the dst_id after collect_pstree_ids() populates vpid
 *
 * In COW phased dump, g_cow_info is initialized in Phase 1 before
 * collect_pstree_ids() runs, so vpid(item) returns -1 at that time.
 * This function allows cr-dump.c to update dst_id in Phase 3 after
 * the IDs are properly collected.
 */
void cow_set_dst_id(u64 dst_id)
{
	if (g_cow_info)
		g_cow_info->dst_id = dst_id;
}

/* ------------------------------------------------------------------ */
/*  VMA registration                                                   */
/* ------------------------------------------------------------------ */

/*
 * cow_register_vmas - Build list of VMAs to track for COW
 *
 * In WP_ASYNC mode, we don't actually register with uffd for WP faults.
 * Instead, we just build the list of trackable VMAs and use PAGEMAP_SCAN
 * to detect dirty pages later.
 */
static int cow_register_vmas(struct cow_dump_info *cdi,
			     struct vm_area_list *vma_area_list,
			     unsigned long *out_total_pages)
{
	struct vma_area *vma;
	struct uffdio_register reg;
	unsigned int nr_eligible = 0, nr_tracked = 0;
	unsigned long total_pages = 0;
	struct cow_tracked_vma *tvmas;
	unsigned int i;

	list_for_each_entry(vma, &vma_area_list->h, list) {
		if (!vma_entry_can_be_lazy(vma->e))
			continue;
		if (vma_area_is(vma, VMA_AREA_GUARD))
			continue;
		if (!(vma->e->prot & PROT_WRITE))
			continue;
		if (!vma_area_is_private(vma, kdat.task_size) &&
		    !vma_area_is(vma, VMA_ANON_SHARED))
			continue;
		if (vma_entry_is(vma->e, VMA_AREA_VVAR))
			continue;
		if (vma->e->flags & MAP_DROPPABLE)
			continue;
		nr_eligible++;
	}

	if (!nr_eligible) {
		*out_total_pages = 0;
		return 0;
	}

	tvmas = xzalloc(sizeof(*tvmas) * nr_eligible);
	BUG_ON(!tvmas);

	i = 0;
	list_for_each_entry(vma, &vma_area_list->h, list) {
		unsigned long start = vma->e->start;
		unsigned long len = vma->e->end - start;
		const char *skip_reason = NULL;
		int ioctl_ret;

		if (!vma_entry_can_be_lazy(vma->e))
			skip_reason = "can_be_lazy";
		else if (vma_area_is(vma, VMA_AREA_GUARD))
			skip_reason = "guard";
		else if (!(vma->e->prot & PROT_WRITE))
			skip_reason = "not_writable";
		else if (!vma_area_is_private(vma, kdat.task_size) &&
			 !vma_area_is(vma, VMA_ANON_SHARED))
			skip_reason = "not_private";
		else if (vma_entry_is(vma->e, VMA_AREA_VVAR))
			skip_reason = "vvar";
		else if (vma->e->flags & MAP_DROPPABLE)
			skip_reason = "droppable";

		if (skip_reason) {
			pr_err("VMA_TRACE: phase=WP_REGISTER vma=0x%lx-0x%lx flags=0x%x prot=0x%x status=0x%x shmid=%" PRIu64
			       " skipped_by=%s\n",
			       start, start + len,
			       vma->e->flags, vma->e->prot, vma->e->status,
			       (uint64_t)vma->e->shmid, skip_reason);
			continue;
		}

		/*
		 * In WP_ASYNC mode, UFFDIO_REGISTER may fail - that's OK.
		 * We track via PAGEMAP_SCAN, not fault handling.
		 */
		reg.range.start = start;
		reg.range.len = len;
		reg.mode = UFFDIO_REGISTER_MODE_WP;
		ioctl_ret = ioctl(cdi->uffd, UFFDIO_REGISTER, &reg);

		pr_err("VMA_TRACE: phase=WP_REGISTER vma=0x%lx-0x%lx flags=0x%x prot=0x%x status=0x%x shmid=%" PRIu64
		       " registered ioctl_ret=%d errno=%d pages=%lu\n",
		       start, start + len,
		       vma->e->flags, vma->e->prot, vma->e->status,
		       (uint64_t)vma->e->shmid,
		       ioctl_ret, ioctl_ret < 0 ? errno : 0, len / PAGE_SIZE);

		tvmas[i].start = start;
		tvmas[i].end = start + len;
		total_pages += len / PAGE_SIZE;
		i++;
	}

	nr_tracked = i;
	if (nr_tracked > 0) {
		void *tmp = xrealloc(tvmas, sizeof(*tvmas) * nr_tracked);
		if (tmp)
			tvmas = tmp;
		cdi->tracked_vmas = tvmas;
	} else {
		xfree(tvmas);
		cdi->tracked_vmas = NULL;
	}
	cdi->nr_tracked_vmas = nr_tracked;
	*out_total_pages = total_pages;

	pr_info("Tracking %u/%u VMAs for COW: %lu pages\n",
		nr_tracked, nr_eligible, total_pages);
	return 0;
}

/* ------------------------------------------------------------------ */
/*  Init / Fini                                                        */
/* ------------------------------------------------------------------ */

void cow_dump_fini(void)
{
	if (!g_cow_info)
		return;

	wait_for_page_server_thread();
	pr_info("Cleaning up COW dump\n");

	if (g_cow_info->uffd >= 0)
		close(g_cow_info->uffd);
	xfree(g_cow_info->tracked_vmas);
	xfree(g_cow_info);
	g_cow_info = NULL;
}

/* ------------------------------------------------------------------ */
/*  Public query API                                                   */
/* ------------------------------------------------------------------ */

bool cow_dump_is_vma_tracked(pid_t source_pid, unsigned long start,
			     unsigned long end)
{
	unsigned int i;

	if (!g_cow_info || g_cow_info->source_pid != source_pid)
		return false;

	for (i = 0; i < g_cow_info->nr_tracked_vmas; i++) {
		if (g_cow_info->tracked_vmas[i].start == start &&
		    g_cow_info->tracked_vmas[i].end == end)
			return true;
	}

	return false;
}


/* ------------------------------------------------------------------ */
/*  Phase management                                                   */
/* ------------------------------------------------------------------ */

enum cow_dump_phase cow_get_phase(void)
{
	if (!g_cow_info)
		return COW_PHASE_IDLE;
	return g_cow_info->phase;
}

void cow_set_phase(enum cow_dump_phase phase)
{
	if (g_cow_info)
		g_cow_info->phase = phase;
}

bool cow_is_phased_skeleton_dump(void)
{
	/*
	 * In COW phased migration, Phase 3 (SCAN) dumps everything except
	 * memory pages. This is detected by checking:
	 * 1. COW dump mode is enabled
	 * 2. Lazy pages is enabled (required for phased migration)
	 * 3. We're at or past the SCAN phase (pages already sent in Phase 2)
	 */
	if (!opts.cow_dump || !opts.lazy_pages)
		return false;
	if (!g_cow_info)
		return false;
	return g_cow_info->phase >= COW_PHASE_SCAN;
}

/* ------------------------------------------------------------------ */
/*  WP_ASYNC phased migration support                                  */
/* ------------------------------------------------------------------ */

int cow_dump_init_async(struct pstree_item *item,
			struct vm_area_list *vma_area_list,
			struct parasite_ctl *ctl)
{
	struct cow_dump_info *cdi;
	struct parasite_cow_dump_args *args = NULL;
	unsigned long args_size;
	unsigned long total_pages;
	int ret;

	pr_info("Initializing COW dump ASYNC for pid %d\n", item->pid->real);

	if (g_cow_info) {
		pr_warn("COW tracking already initialized\n");
		return 0;
	}

	cdi = xzalloc(sizeof(*cdi));
	BUG_ON(!cdi);

	cdi->source_pid = item->pid->real;
	cdi->dst_id = vpid(item);
	cdi->uffd = -1;
	cdi->phase = COW_PHASE_ASYNC_BULK;

	g_cow_info = cdi;

	/*
	 * Create UFFD with WP_ASYNC via the parasite running inside
	 * the target process.  This avoids /proc/<pid>/userfaultfd
	 * which is deprecated / missing on some kernels.
	 */
	if (!ctl) {
		pr_err("Parasite control required for WP_ASYNC uffd creation\n");
		goto err;
	}

	args_size = sizeof(*args);
	args = compel_parasite_args_s(ctl, args_size);
	if (!args) {
		pr_err("Failed to allocate parasite args for WP_ASYNC\n");
		goto err;
	}

	args->nr_vmas = 0;
	args->total_pages = 0;
	args->nr_failed_vmas = 0;
	args->uffd_features = UFFD_FEATURE_WP_ASYNC;
	args->ret = -1;

	ret = compel_rpc_call(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0) {
		pr_err("Failed to initiate COW dump ASYNC RPC\n");
		goto err;
	}

	compel_util_recv_fd(ctl, &cdi->uffd);
	if (cdi->uffd < 0) {
		pr_err("Failed to receive WP_ASYNC uffd from parasite: %d\n",
		       cdi->uffd);
		goto err;
	}

	ret = compel_rpc_sync(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0 || args->ret != 0) {
		pr_err("Parasite COW dump ASYNC init failed: %d (ret=%d)\n",
		       ret, args->ret);
		goto err;
	}

	/* Build list of VMAs to track */
	ret = cow_register_vmas(cdi, vma_area_list, &total_pages);
	if (ret)
		goto err;

	/* Apply write-protect */
	if (cow_apply_writeprotect(cdi))
		goto err;

	pr_info("COW ASYNC initialized for pid %d: tracked=%u pages=%lu uffd=%d\n",
		item->pid->real, cdi->nr_tracked_vmas, total_pages, cdi->uffd);
	return 0;

err:
	if (cdi->uffd >= 0)
		close(cdi->uffd);
	xfree(cdi->tracked_vmas);
	xfree(cdi);
	g_cow_info = NULL;
	return -1;
}

/*
 * cow_is_vma_trackable - Check if a VMA should be COW-tracked
 *
 * Uses the same criteria as cow_register_vmas() to determine if a VMA
 * is eligible for COW tracking.
 */
static bool cow_is_vma_trackable(struct vma_area *vma)
{
	if (!vma_entry_can_be_lazy(vma->e))
		return false;
	if (vma_area_is(vma, VMA_AREA_GUARD))
		return false;
	if (!(vma->e->prot & PROT_WRITE))
		return false;
	if (!vma_area_is_private(vma, kdat.task_size) &&
	    !vma_area_is(vma, VMA_ANON_SHARED))
		return false;
	if (vma_entry_is(vma->e, VMA_AREA_VVAR))
		return false;
	if (vma->e->flags & MAP_DROPPABLE)
		return false;
	return true;
}

/*
 * cow_region_subtract - Subtract tracked region from [start, end)
 *
 * Returns the portion(s) of [start, end) not covered by the tracked VMAs.
 * Appends results to ranges array at *nr_ranges position.
 */
static int cow_region_subtract(unsigned long start, unsigned long end,
			       unsigned long **ranges, unsigned int *nr_ranges,
			       unsigned int *capacity)
{
	struct cow_dump_info *cdi = g_cow_info;
	unsigned int i;
	unsigned long cur_start = start;

	if (!cdi || !cdi->nr_tracked_vmas) {
		/* No tracked VMAs, entire region is new */
		goto add_region;
	}

	/*
	 * Walk through tracked VMAs and find gaps.
	 * tracked_vmas are sorted by start address from cow_register_vmas.
	 */
	for (i = 0; i < cdi->nr_tracked_vmas && cur_start < end; i++) {
		unsigned long t_start = cdi->tracked_vmas[i].start;
		unsigned long t_end = cdi->tracked_vmas[i].end;

		/* Skip tracked VMAs that end before our current position */
		if (t_end <= cur_start)
			continue;

		/* Skip tracked VMAs that start after our region ends */
		if (t_start >= end)
			break;

		/* Found overlap - add the gap before this tracked VMA */
		if (t_start > cur_start) {
			unsigned long gap_end = (t_start < end) ? t_start : end;
			unsigned long gap_len = gap_end - cur_start;

			if (*nr_ranges >= *capacity) {
				unsigned int new_cap = *capacity ? *capacity * 2 : 64;
				unsigned long *new_ranges;

				new_ranges = xrealloc(*ranges,
						      new_cap * 2 * sizeof(unsigned long));
				BUG_ON(!new_ranges);
				*ranges = new_ranges;
				*capacity = new_cap;
			}

			(*ranges)[(*nr_ranges) * 2] = cur_start;
			(*ranges)[(*nr_ranges) * 2 + 1] = gap_len;
			(*nr_ranges)++;

			pr_info("  new region: 0x%lx-0x%lx (%lu pages)\n",
				cur_start, gap_end, gap_len / PAGE_SIZE);
		}

		/* Move past this tracked VMA */
		cur_start = t_end;
	}

add_region:
	/* Add any remaining portion after all tracked VMAs */
	if (cur_start < end) {
		unsigned long len = end - cur_start;

		if (*nr_ranges >= *capacity) {
			unsigned int new_cap = *capacity ? *capacity * 2 : 64;
			unsigned long *new_ranges;

			new_ranges = xrealloc(*ranges,
					      new_cap * 2 * sizeof(unsigned long));
			BUG_ON(!new_ranges);
			*ranges = new_ranges;
			*capacity = new_cap;
		}

		(*ranges)[(*nr_ranges) * 2] = cur_start;
		(*ranges)[(*nr_ranges) * 2 + 1] = len;
		(*nr_ranges)++;

		pr_info("  new region: 0x%lx-0x%lx (%lu pages)\n",
			cur_start, end, len / PAGE_SIZE);
	}

	return 0;
}

/*
 * cow_extend_tracked_vmas - Add new regions to tracked_vmas array
 *
 * Extends g_cow_info->tracked_vmas to include new VMA regions discovered
 * in Phase 3. This ensures the fault handler can find these regions during
 * WP_SYNC convergence.
 *
 * Also adds these regions to global_lazy_vmas so the page server can
 * iterate through them during page transfer.
 *
 * @ranges: Array of [start, len, ...] pairs
 * @nr_ranges: Number of ranges
 *
 * Returns: 0 on success, -1 on error
 */
static int cow_extend_tracked_vmas(unsigned long *ranges, unsigned int nr_ranges)
{
	struct cow_dump_info *cdi = g_cow_info;
	struct cow_tracked_vma *new_tracked;
	unsigned int new_total;
	unsigned int i;

	if (!cdi || nr_ranges == 0)
		return 0;

	new_total = cdi->nr_tracked_vmas + nr_ranges;
	new_tracked = xrealloc(cdi->tracked_vmas,
			       new_total * sizeof(*new_tracked));
	BUG_ON(!new_tracked);

	/* Update pointer immediately - xrealloc may have moved the buffer */
	cdi->tracked_vmas = new_tracked;

	/* Append new regions (ranges are [start, len] pairs) */
	for (i = 0; i < nr_ranges; i++) {
		unsigned long start = ranges[i * 2];
		unsigned long len = ranges[i * 2 + 1];

		new_tracked[cdi->nr_tracked_vmas + i].start = start;
		new_tracked[cdi->nr_tracked_vmas + i].end = start + len;
		pr_info("Added new tracked VMA: 0x%lx-0x%lx\n", start, start + len);

		/* Also add to global_lazy_vmas for page transfer */
		if (add_lazy_vma_for_new_region(start, len,
						cdi->dst_id, cdi->source_pid)) {
			pr_err("Failed to add lazy VMA for 0x%lx-0x%lx\n",
			       start, start + len);
			BUG();
		}
	}

	cdi->nr_tracked_vmas = new_total;
	pr_info("Extended tracked_vmas: now %u total\n", new_total);

	return 0;
}

/*
 * cow_detect_new_vmas - Detect VMAs that appeared after Phase 1
 *
 * Compares the current VMA list with the tracked VMAs from Phase 1.
 * Returns ranges for any new or extended VMA regions that weren't
 * tracked. These regions need to be marked dirty for WP_SYNC since
 * we have no record of their pages from Phase 2.
 *
 * Also updates g_cow_info->tracked_vmas to include the new regions
 * so the fault handler can find them during convergence.
 *
 * @vmas: Current VMA list (from collect_mappings in Phase 3)
 * @new_ranges: Output array of [start, len, ...] pairs
 * @nr_new_ranges: Output count of new ranges
 *
 * Caller must xfree() the new_ranges array.
 * Returns: 0 on success, -1 on error
 */
int cow_detect_new_vmas(struct vm_area_list *vmas,
			unsigned long **new_ranges,
			unsigned int *nr_new_ranges)
{
	struct vma_area *vma;
	unsigned long *ranges = NULL;
	unsigned int nr_ranges = 0;
	unsigned int capacity = 0;
	struct cow_dump_info *cdi = g_cow_info;

	if (!cdi) {
		pr_err("COW dump not initialized\n");
		return -1;
	}

	*new_ranges = NULL;
	*nr_new_ranges = 0;

	pr_info("Detecting new VMAs (comparing against %u tracked VMAs)\n",
		cdi->nr_tracked_vmas);

	list_for_each_entry(vma, &vmas->h, list) {
		unsigned long start = vma->e->start;
		unsigned long end = vma->e->end;
		unsigned int before;

		/* Use same filtering as cow_register_vmas */
		if (!cow_is_vma_trackable(vma)) {
			pr_err("VMA_TRACE: phase=PHASE3_NEW_VMA_DETECT vma=0x%lx-0x%lx flags=0x%x prot=0x%x status=0x%x shmid=%" PRIu64
			       " trackable=0 skipped\n",
			       start, end,
			       vma->e->flags, vma->e->prot, vma->e->status,
			       (uint64_t)vma->e->shmid);
			continue;
		}

		pr_info("Checking VMA 0x%lx-0x%lx\n", start, end);

		before = nr_ranges;
		if (cow_region_subtract(start, end, &ranges, &nr_ranges, &capacity)) {
			xfree(ranges);
			return -1;
		}
		pr_err("VMA_TRACE: phase=PHASE3_NEW_VMA_DETECT vma=0x%lx-0x%lx flags=0x%x prot=0x%x status=0x%x shmid=%" PRIu64
		       " trackable=1 new_ranges_emitted=%u\n",
		       start, end,
		       vma->e->flags, vma->e->prot, vma->e->status,
		       (uint64_t)vma->e->shmid,
		       nr_ranges - before);
	}

	*new_ranges = ranges;
	*nr_new_ranges = nr_ranges;

	pr_err("COW NEW VMAs: Detected %u new VMA regions since Phase 1\n", nr_ranges);

	/* Log details of each new VMA range */
	if (nr_ranges > 0) {
		unsigned int i;
		pr_err("COW NEW VMAs: These VMAs exist on PRIMARY but were created AFTER Phase 1 dump:\n");
		for (i = 0; i < nr_ranges; i++) {
			unsigned long start = ranges[i * 2];
			unsigned long len = ranges[i * 2 + 1];
			pr_err("  NEW VMA [%u]: 0x%lx-0x%lx (size=%luKB)\n",
			       i, start, start + len, len / 1024);
		}
		pr_err("COW NEW VMAs: WARNING - These VMAs will NOT exist on REPLICA!\n");
		pr_err("COW NEW VMAs: The VMA metadata was not re-dumped after Phase 1.\n");
	}

	/* Extend tracked_vmas so fault handler can find new regions */
	if (cow_extend_tracked_vmas(ranges, nr_ranges))
		return -1;

	return 0;
}

/**
 * cow_cleanup_async_uffd - Close async uffd without unregistering VMAs
 *
 * The kernel automatically cleans up uffd registrations when the fd is closed.
 * This avoids the expensive UFFDIO_UNREGISTER page walks that can take minutes
 * on large memory systems.
 */
void cow_cleanup_async_uffd(void)
{
	struct cow_dump_info *cdi = g_cow_info;
	struct uffdio_range range;
	unsigned int i;
	int ret;

	if (!cdi)
		return;

	/*
	 * Unregister VMAs in chunks with yields between each.
	 * This spreads the kernel page-table walk time and allows
	 * the target process to make progress between chunks.
	 */
	pr_err("Unregistering VMAs from uffd sleeping 15 seconds\n");
	sleep(15);
	pr_err("Unregistering VMAs from uffd sleeping 15 seconds done\n");
	if (cdi->uffd >= 0 && cdi->tracked_vmas && cdi->nr_tracked_vmas > 0) {
		pr_info("Unregistering %u VMAs from uffd fd=%d (chunked)\n",
			cdi->nr_tracked_vmas, cdi->uffd);

		for (i = 0; i < cdi->nr_tracked_vmas; i++) {
			range.start = cdi->tracked_vmas[i].start;
			range.len = cdi->tracked_vmas[i].end - cdi->tracked_vmas[i].start;

			ret = ioctl(cdi->uffd, UFFDIO_UNREGISTER, &range);
			if (ret < 0 && errno != EINVAL) {
				/* EINVAL = already unregistered, ignore */
				pr_debug("UFFDIO_UNREGISTER %lx-%lx failed: %s\n",
					 (unsigned long)range.start,
					 (unsigned long)(range.start + range.len),
					 strerror(errno));
			}

			/* Yield to let target process run between chunks */
			if ((i + 1) % COW_UFFD_UNREGISTER_YIELD == 0)
				usleep(COW_USLEEP_10MS);
		}
	}

	if (cdi->uffd >= 0) {
		pr_err("Closing uffd fd=%d\n", cdi->uffd);
		close(cdi->uffd);
		cdi->uffd = -1;
	}

}


