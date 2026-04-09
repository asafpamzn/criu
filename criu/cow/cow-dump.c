#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <stdbool.h>
#include <linux/userfaultfd.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include <poll.h>
#include <sys/eventfd.h>

#include "types.h"
#include "cr_options.h"
#include "pstree.h"
#include "cow/cow-dump.h"
#include "mman.h"
#include "uffd.h"
#include "pagemap_scan.h"
#include "proc_parse.h"
#include "page-xfer.h"
#include "page-pipe.h"
#include "parasite-syscall.h"
#include "mem.h"
#include "vma.h"
#include "util.h"
#include "kerndat.h"
#include "criu-log.h"
#include "parasite.h"
#include "atomic-bitmap.h"
#include "cow/mpsc-queue.h"
#include "cow/cow-bulk-send.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-dump: "

struct cow_tracked_vma {
	unsigned long start;
	unsigned long end;
	bool is_new;  /* True if VMA was detected in Phase 3 (needs uffd registration) */
};

/* MPSC queue node type for COW page entries (multi-producer safe) */
DECLARE_MPSC_NODE(cow_page, struct cow_page_queue_entry);
struct cow_page_queue {
	struct cow_page_mpsc_node *head;
	char _pad[64 - sizeof(struct cow_page_mpsc_node *)];
	struct cow_page_mpsc_node *tail;
	unsigned long size;
};

/* COW dump state for one dump session — single tracked process */
struct cow_dump_info {
	pid_t source_pid;
	u64 dst_id;            /* Process identifier for page transfer */
	int uffd;
	int uffd_async;        /* WP_ASYNC uffd fd (kept for cleanup) */
	int uffd_sync;         /* Pre-created WP_SYNC uffd (via parasite) */
	unsigned long total_pages;
	unsigned long iteration;
	unsigned int nr_tracked_vmas;
	struct cow_tracked_vma *tracked_vmas;
	enum cow_dump_phase phase;  /* Current phase */

	struct cow_page_queue page_queue;
};

/*
 * Applying UFFD write-protect over a large address space can dominate the
 * initial stall.  We apply it in parallel from the CRIU process.
 */
#define COW_WP_CHUNK_SIZE	(64UL * 1024 * 1024)

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
	if (!ranges)
		return NULL;

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
		if (nr_ranges)
			return -1;
		return 0;
	}

	nr_threads = cow_wp_nr_threads(nr_ranges);
	threads = xmalloc(nr_threads * sizeof(*threads));
	jobs = xzalloc(nr_threads * sizeof(*jobs));
	if (!threads || !jobs) {
		ret = -1;
		goto out;
	}

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
	pr_err("TIMING: cow_dump_writeprotect took %lu.%06lu seconds (%u ranges, %u threads)\n",
	       sec, nsec / 1000, nr_ranges, created ? created : 1);

out:
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

static int g_monitor_eventfd = -1;
static struct cow_page_queue_entry *g_putback_list = NULL;

/* Per-worker page buffer pool — avoids malloc(PAGE_SIZE) on the hot path */
#define COW_PAGE_POOL_SIZE	256	/* 256 x 4 KB = 1 MB per worker */
#define COW_FAULT_WORKERS	4

/* Pre-read window: 8 pages before + faulting page + 7 pages after = 16 pages = 64KB */
#define COW_PREREAD_BEFORE	8
#define COW_PREREAD_AFTER	7
#define COW_PREREAD_TOTAL	(COW_PREREAD_BEFORE + 1 + COW_PREREAD_AFTER)

struct cow_fault_worker {
	pthread_t thread;
	int id;
	struct cow_dump_info *cdi;
	void *page_pool[COW_PAGE_POOL_SIZE];
	unsigned int pool_count;
};



/* ------------------------------------------------------------------ */
/*  Kernel support check                                               */
/* ------------------------------------------------------------------ */

bool cow_check_kernel_support(void)
{
	unsigned long features = UFFD_FEATURE_PAGEFAULT_FLAG_WP;
	int uffd, err = 0;

	uffd = uffd_open(0, &features, &err);
	if (uffd < 0) {
		if (err == ENOSYS)
			pr_info("userfaultfd not supported by kernel\n");
		else if (err == EPERM)
			pr_info("userfaultfd requires CAP_SYS_PTRACE or sysctl vm.unprivileged_userfaultfd=1\n");
		return false;
	}
	if (!(features & UFFD_FEATURE_PAGEFAULT_FLAG_WP)) {
		pr_info("userfaultfd WP pagefault flag not supported (need kernel 5.7+)\n");
		close(uffd);
		return false;
	}
	close(uffd);
	pr_info("COW dump kernel support detected\n");
	return true;
}

/* ------------------------------------------------------------------ */
/*  VMA registration                                                   */
/* ------------------------------------------------------------------ */

static int cow_register_vmas(struct cow_dump_info *cdi,
			     struct vm_area_list *vma_area_list,
			     unsigned long *out_total_pages)
{
	struct vma_area *vma;
	struct uffdio_register reg;
	unsigned int nr_eligible = 0, nr_tracked = 0, nr_failed = 0;
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
	if (!tvmas)
		return -1;

	i = 0;
	list_for_each_entry(vma, &vma_area_list->h, list) {
		unsigned long start = vma->e->start;
		unsigned long len = vma->e->end - start;
		int ret;

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

		reg.range.start = start;
		reg.range.len = len;
		reg.mode = UFFDIO_REGISTER_MODE_WP;

		ret = ioctl(cdi->uffd, UFFDIO_REGISTER, &reg);
		if (ret && cdi->phase != COW_PHASE_ASYNC_BULK) {
			pr_warn("UFFDIO_REGISTER WP %lx-%lx failed: %s\n",
				start, start + len, strerror(errno));
			nr_failed++;
			continue;
		}
		if (ret && cdi->phase == COW_PHASE_ASYNC_BULK) {
			pr_info("UFFDIO_REGISTER WP %lx-%lx skipped in WP_ASYNC "
				"(tracking via PAGEMAP_SCAN)\n",
				start, start + len);
		}

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

	pr_info("Registered %u/%u VMAs (%u failed) via /proc: %lu pages\n",
		nr_tracked, nr_eligible, nr_failed, total_pages);
	return 0;
}

/* ------------------------------------------------------------------ */
/*  Init / Fini                                                        */
/* ------------------------------------------------------------------ */
static void free_cow_page_entry(struct cow_page_queue_entry *entry)
{
	if (entry->data)
		xfree(entry->data);
	xfree(entry);
}



void cow_dump_fini(void)
{
	struct cow_page_queue_entry *qe;
	int queue_remaining = 0;

	if (!g_cow_info)
		return;


	wait_for_page_server_thread();
	pr_err("Cleaning up COW dump\n");

	if (g_monitor_eventfd >= 0) {
		close(g_monitor_eventfd);
		g_monitor_eventfd = -1;
	}

	while (g_putback_list) {
		qe = g_putback_list;
		g_putback_list = qe->next;
		if (qe->data)
			xfree(qe->data);
		xfree(qe);
		queue_remaining++;
	}

	if (g_cow_info->page_queue.head) {
		mpsc_drain(g_cow_info->page_queue.head, free_cow_page_entry);
		g_cow_info->page_queue.tail = NULL;
	}

	if (queue_remaining > 0)
		pr_warn("Freed %d remaining queue entries\n", queue_remaining);

	if (g_cow_info->uffd >= 0)
		close(g_cow_info->uffd);
	if (g_cow_info->uffd_async >= 0)
		close(g_cow_info->uffd_async);
	if (g_cow_info->uffd_sync >= 0)
		close(g_cow_info->uffd_sync);
	xfree(g_cow_info->tracked_vmas);
	xfree(g_cow_info);
	g_cow_info = NULL;
}

/* ------------------------------------------------------------------ */
/*  Public query API                                                   */
/* ------------------------------------------------------------------ */

int cow_get_uffd_for_pid(pid_t source_pid)
{
	if (!g_cow_info || g_cow_info->source_pid != source_pid)
		return -1;
	return g_cow_info->uffd;
}

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

struct cow_page_queue_entry *cow_get_next_page(void)
{
	struct cow_page_queue_entry *entry;

	if (!g_cow_info)
		return NULL;

	if (g_putback_list) {
		entry = g_putback_list;
		g_putback_list = entry->next;
		entry->next = NULL;
		return entry;
	}

	return mpsc_dequeue(g_cow_info->page_queue.head, g_cow_info->page_queue.size);
}

bool cow_has_pending_pages(void)
{
	if (!g_cow_info)
		return false;
	if (g_putback_list)
		return true;
	return mpsc_peek(g_cow_info->page_queue.head);
}

void cow_put_back_page(struct cow_page_queue_entry *entry)
{
	if (!g_cow_info || !entry)
		return;
	entry->next = g_putback_list;
	g_putback_list = entry;
	pr_debug("Re-queued COW page 0x%lx to putback list\n", entry->vaddr);
}

unsigned long cow_get_pages_queue_size(void)
{
	if (!g_cow_info)
		return 0;
	return mpsc_size(g_cow_info->page_queue.size);
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
	int ret;

	pr_info("Initializing COW dump ASYNC for pid %d\n", item->pid->real);

	if (g_cow_info) {
		pr_warn("COW tracking already initialized\n");
		return 0;
	}

	cdi = xzalloc(sizeof(*cdi));
	if (!cdi)
		return -1;

	cdi->source_pid = item->pid->real;
	cdi->dst_id = vpid(item);
	cdi->uffd = -1;
	cdi->uffd_async = -1;
	cdi->uffd_sync = -1;
	cdi->phase = COW_PHASE_ASYNC_BULK;

	if (mpsc_init(cdi->page_queue.head, cdi->page_queue.tail,
		      cdi->page_queue.size, struct cow_page_mpsc_node)) {
		xfree(cdi);
		return -1;
	}

	g_cow_info = cdi;

	if (g_monitor_eventfd >= 0) {
		close(g_monitor_eventfd);
		g_monitor_eventfd = -1;
	}
	g_monitor_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (g_monitor_eventfd < 0) {
		pr_perror("Failed to create cow monitor eventfd");
		goto err;
	}

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

	cdi->uffd_async = cdi->uffd;

	/* Register VMAs — reuse cow_register_vmas() */
	ret = cow_register_vmas(cdi, vma_area_list, &cdi->total_pages);
	if (ret)
		goto err;

	/* Apply write-protect — reuse cow_apply_writeprotect() */
	if (cow_apply_writeprotect(cdi))
		goto err;

	/* DO NOT start monitor thread — WP_ASYNC doesn't generate faults */
	pr_info("COW ASYNC initialized for pid %d: tracked=%u pages=%lu uffd=%d\n",
		item->pid->real, cdi->nr_tracked_vmas,
		cdi->total_pages, cdi->uffd);
	return 0;

err:
	if (cdi->uffd >= 0)
		close(cdi->uffd);
	xfree(cdi->tracked_vmas);
	if (cdi->page_queue.head) {
		mpsc_drain(cdi->page_queue.head, free_cow_page_entry);
		cdi->page_queue.tail = NULL;
	}
	xfree(cdi);
	g_cow_info = NULL;
	if (g_monitor_eventfd >= 0) {
		close(g_monitor_eventfd);
		g_monitor_eventfd = -1;
	}
	return -1;
}

int cow_scan_dirty_pages(unsigned long **dirty_ranges,
			 unsigned int *nr_dirty_ranges,
			 unsigned long *total_dirty_pages)
{
	struct cow_dump_info *cdi = g_cow_info;
	int pagemap_fd = -1;
	struct page_region *regs = NULL;
	unsigned long *ranges = NULL;
	unsigned int nr_ranges = 0;
	unsigned int ranges_capacity = 0;
	unsigned long total_pages = 0;
	unsigned int i, j;
	int ret = -1;
	char path[64];

	struct pm_scan_arg args = {
		.size = sizeof(struct pm_scan_arg),
		.flags = PM_SCAN_WP_MATCHING,
		.start = 0,
		.end = 0,
		.walk_end = 0,
		.vec_len = 1000,
		.max_pages = 0,
		.category_anyof_mask = PAGE_IS_WRITTEN,
		.return_mask = PAGE_IS_WRITTEN | PAGE_IS_WPALLOWED,
	};

	if (!cdi) {
		pr_err("COW dump not initialized\n");
		return -1;
	}

	*dirty_ranges = NULL;
	*nr_dirty_ranges = 0;
	*total_dirty_pages = 0;

	snprintf(path, sizeof(path), "/proc/%d/pagemap", cdi->source_pid);
	pagemap_fd = open(path, O_RDWR);
	if (pagemap_fd < 0) {
		pr_perror("Cannot open %s", path);
		return -1;
	}

	regs = xmalloc(args.vec_len * sizeof(struct page_region));
	if (!regs)
		goto out;
	args.vec = (u64)(unsigned long)regs;

	/* Scan each tracked VMA for dirty pages */
	for (i = 0; i < cdi->nr_tracked_vmas; i++) {
		unsigned long vma_start = cdi->tracked_vmas[i].start;
		unsigned long vma_end = cdi->tracked_vmas[i].end;
		long regs_len;

		args.start = vma_start;
		args.end = vma_end;
		args.walk_end = vma_start;

		do {
			args.start = args.walk_end;
			pr_debug("PAGEMAP_SCAN: scanning VMA 0x%lx-0x%lx "
				"(start=0x%lx walk_end=0x%lx)\n",
				vma_start, vma_end,
				(unsigned long)args.start,
				(unsigned long)args.walk_end);
			regs_len = ioctl(pagemap_fd, PAGEMAP_SCAN, &args);
			if (regs_len == -1) {
				pr_perror("PAGEMAP_SCAN for VMA 0x%lx-0x%lx",
					  vma_start, vma_end);
				goto out;
			}

			pr_debug("PAGEMAP_SCAN: returned %ld regions, "
				"walk_end=0x%lx (vma_end=0x%lx)\n",
				regs_len,
				(unsigned long)args.walk_end, vma_end);

			/* Safety: if no regions returned, avoid infinite loop */
			if (regs_len == 0)
				break;

			for (j = 0; j < (unsigned int)regs_len; j++) {
				unsigned long start = regs[j].start;
				unsigned long len = regs[j].end - regs[j].start;
				unsigned long pages = len / PAGE_SIZE;

				pr_debug("  dirty region[%u]: 0x%lx-0x%lx "
					"(%lu pages, categories=0x%llx)\n",
					j, start, start + len, pages,
					(unsigned long long)regs[j].categories);

				/* Grow ranges array if needed */
				if (nr_ranges >= ranges_capacity) {
					unsigned int new_cap = ranges_capacity ?
							       ranges_capacity * 2 : 64;
					unsigned long *new_ranges;

					new_ranges = xrealloc(ranges,
							      new_cap * 2 * sizeof(unsigned long));
					if (!new_ranges)
						goto out;
					ranges = new_ranges;
					ranges_capacity = new_cap;
				}

				ranges[nr_ranges * 2] = start;
				ranges[nr_ranges * 2 + 1] = len;
				nr_ranges++;
				total_pages += pages;
			}
		} while (args.walk_end != vma_end);
	}

	*dirty_ranges = ranges;
	*nr_dirty_ranges = nr_ranges;
	*total_dirty_pages = total_pages;
	ranges = NULL; /* Caller owns it now */

	cdi->phase = COW_PHASE_SCAN;
	pr_info("Scanned %u dirty ranges, %lu pages total\n",
		nr_ranges, total_pages);
	ret = 0;

out:
	xfree(regs);
	xfree(ranges);
	if (pagemap_fd >= 0)
		close(pagemap_fd);
	return ret;
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
				if (!new_ranges)
					return -1;
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
			if (!new_ranges)
				return -1;
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
	if (!new_tracked) {
		pr_err("Failed to extend tracked_vmas for %u new regions\n",
		       nr_ranges);
		return -1;
	}

	/* Append new regions (ranges are [start, len] pairs) */
	for (i = 0; i < nr_ranges; i++) {
		unsigned long start = ranges[i * 2];
		unsigned long len = ranges[i * 2 + 1];

		new_tracked[cdi->nr_tracked_vmas + i].start = start;
		new_tracked[cdi->nr_tracked_vmas + i].end = start + len;
		new_tracked[cdi->nr_tracked_vmas + i].is_new = true;
		pr_info("Added new tracked VMA: 0x%lx-0x%lx (needs uffd registration)\n",
			start, start + len);

		/* Also add to global_lazy_vmas for page transfer */
		if (add_lazy_vma_for_new_region(start, len,
						cdi->dst_id, cdi->source_pid)) {
			pr_err("Failed to add lazy VMA for 0x%lx-0x%lx\n",
			       start, start + len);
			xfree(new_tracked);
			return -1;
		}
	}

	cdi->tracked_vmas = new_tracked;
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

		/* Use same filtering as cow_register_vmas */
		if (!cow_is_vma_trackable(vma))
			continue;

		pr_info("Checking VMA 0x%lx-0x%lx\n", start, end);

		if (cow_region_subtract(start, end, &ranges, &nr_ranges, &capacity)) {
			xfree(ranges);
			return -1;
		}
	}

	*new_ranges = ranges;
	*nr_new_ranges = nr_ranges;

	pr_info("Detected %u new VMA regions\n", nr_ranges);

	/* Extend tracked_vmas so fault handler can find new regions */
	if (cow_extend_tracked_vmas(ranges, nr_ranges))
		return -1;

	return 0;
}

/*
 * cow_merge_dirty_ranges - Merge two range arrays into one
 *
 * @dirty_ranges: First array (dirty pages from PAGEMAP_SCAN)
 * @nr_dirty: Count of dirty ranges
 * @new_ranges: Second array (new VMAs)
 * @nr_new: Count of new ranges
 * @merged_ranges: Output merged array
 * @nr_merged: Output merged count
 *
 * Caller must xfree() the merged_ranges array.
 * The input arrays are NOT freed by this function.
 * Returns: 0 on success, -1 on error
 */
int cow_merge_dirty_ranges(unsigned long *dirty_ranges, unsigned int nr_dirty,
			   unsigned long *new_ranges, unsigned int nr_new,
			   unsigned long **merged_ranges, unsigned int *nr_merged)
{
	unsigned long *merged;
	unsigned int total = nr_dirty + nr_new;
	unsigned int i;

	*merged_ranges = NULL;
	*nr_merged = 0;

	if (total == 0)
		return 0;

	merged = xmalloc(total * 2 * sizeof(unsigned long));
	if (!merged)
		return -1;

	/* Copy dirty ranges */
	for (i = 0; i < nr_dirty; i++) {
		merged[i * 2] = dirty_ranges[i * 2];
		merged[i * 2 + 1] = dirty_ranges[i * 2 + 1];
	}

	/* Append new VMA ranges */
	for (i = 0; i < nr_new; i++) {
		merged[(nr_dirty + i) * 2] = new_ranges[i * 2];
		merged[(nr_dirty + i) * 2 + 1] = new_ranges[i * 2 + 1];
	}

	*merged_ranges = merged;
	*nr_merged = total;

	pr_info("Merged %u dirty + %u new = %u total ranges\n",
		nr_dirty, nr_new, total);
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
					 range.start, range.start + range.len,
					 strerror(errno));
			}

			/* Yield to let target process run between chunks */
			if ((i + 1) % 10 == 0)
				usleep(1000);  /* 1ms every 10 VMAs */
		}
	}

	if (cdi->uffd >= 0) {
		pr_info("Closing async uffd fd=%d\n", cdi->uffd);
		close(cdi->uffd);
		cdi->uffd = -1;
	}

	/* Also close pre-created sync uffd if not used */
	if (cdi->uffd_sync >= 0) {
		pr_info("Closing unused sync uffd fd=%d\n", cdi->uffd_sync);
		close(cdi->uffd_sync);
		cdi->uffd_sync = -1;
	}

	cdi->phase = COW_PHASE_DONE;
}

/**
 * cow_dump_dirty_pages - Dump dirty pages directly while process is frozen
 * @dirty_ranges: Array of [start, len, start, len, ...] pairs
 * @nr_dirty_ranges: Number of ranges
 * @source_pid: PID of source process for process_vm_readv
 *
 * Reads dirty pages using process_vm_readv() and sends them using the
 * existing batch compression protocol. Called during Phase 3 freeze,
 * eliminating the need for WP_SYNC setup and convergence.
 *
 * Returns: 0 on success, -1 on error
 */
int cow_dump_dirty_pages(unsigned long *dirty_ranges, unsigned int nr_dirty_ranges,
			 pid_t source_pid)
{
	struct cow_dump_info *cdi = g_cow_info;
	unsigned int i;
	int sk;
	u64 dst_id;
	unsigned long total_pages_sent = 0;
	struct timeval t_start, t_end, t_delta;

	if (!cdi || nr_dirty_ranges == 0) {
		pr_info("No dirty pages to dump\n");
		return 0;
	}

	sk = get_page_server_sk();
	if (sk < 0) {
		pr_err("No page server socket for dirty page dump\n");
		return -1;
	}

	dst_id = cdi->dst_id;

	gettimeofday(&t_start, NULL);
	pr_info("Dumping %u dirty ranges while frozen (pid=%d dst_id=%lu)\n",
		nr_dirty_ranges, source_pid, (unsigned long)dst_id);

	for (i = 0; i < nr_dirty_ranges; i++) {
		unsigned long start = dirty_ranges[i * 2];
		unsigned long len = dirty_ranges[i * 2 + 1];
		unsigned long offset = 0;

		while (offset < len) {
			void *buffer;
			struct iovec local_iov, remote_iov;
			unsigned long remaining = len - offset;
			int batch_pages = remaining / PAGE_SIZE;
			unsigned long batch_addr = start + offset;
			ssize_t ret;

			if (batch_pages > COW_BATCH_PAGES)
				batch_pages = COW_BATCH_PAGES;
			if (batch_pages == 0)
				break;

			buffer = xmalloc(batch_pages * PAGE_SIZE);
			if (!buffer)
				return -1;

			/* Read pages from source process */
			local_iov.iov_base = buffer;
			local_iov.iov_len = batch_pages * PAGE_SIZE;
			remote_iov.iov_base = (void *)batch_addr;
			remote_iov.iov_len = batch_pages * PAGE_SIZE;

			ret = process_vm_readv(source_pid, &local_iov, 1, &remote_iov, 1, 0);
			if (ret != (ssize_t)(batch_pages * PAGE_SIZE)) {
				pr_perror("Failed to read dirty pages at 0x%lx (%d pages)",
					  batch_addr, batch_pages);
				xfree(buffer);
				/* Continue with next batch - page may be unmapped */
				offset += batch_pages * PAGE_SIZE;
				continue;
			}

			/* Send compressed batch */
			ret = send_pages_batch_compressed(sk, buffer, batch_pages,
							  dst_id, batch_addr);
			xfree(buffer);

			if (ret < 0) {
				pr_err("Failed to send dirty pages at 0x%lx\n", batch_addr);
				return -1;
			}

			total_pages_sent += batch_pages;
			offset += batch_pages * PAGE_SIZE;
		}
	}

	gettimeofday(&t_end, NULL);
	timersub(&t_end, &t_start, &t_delta);
	pr_err("TIMING: cow_dump_dirty_pages sent %lu pages in %ld.%06ld seconds\n",
	       total_pages_sent, t_delta.tv_sec, t_delta.tv_usec);

	return 0;
}
