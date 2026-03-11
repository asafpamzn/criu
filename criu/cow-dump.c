#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <linux/userfaultfd.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include <poll.h>
#include <sys/eventfd.h>

#include "types.h"
#include "cr_options.h"
#include "pstree.h"
#include "cow-dump.h"
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
#include "cow-bitmap.h"
#include "mpsc-queue.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-dump: "

struct cow_tracked_vma {
	unsigned long start;
	unsigned long end;
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
static _Atomic bool g_stop_monitoring = false;
static pthread_mutex_t g_monitor_state_lock = PTHREAD_MUTEX_INITIALIZER;
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

static struct cow_fault_worker *g_workers = NULL;
static unsigned int g_nr_workers = 0;
static _Atomic bool g_workers_running = false;

/* Forward declarations */
static void cow_worker_pool_init(struct cow_fault_worker *w);
static void cow_worker_pool_fini(struct cow_fault_worker *w);
static void *cow_worker_pool_get(struct cow_fault_worker *w);

/* ------------------------------------------------------------------ */
/*  Statistics (atomically updated, safe from any thread)              */
/* ------------------------------------------------------------------ */

static struct {
	unsigned long write_faults;
	unsigned long fork_events;
	unsigned long remap_events;
	unsigned long unknown_events;
	unsigned long pages_copied;
	unsigned long pages_unprotected;
	unsigned long pages_woken;
	unsigned long alloc_failures;
	unsigned long read_failures;
	unsigned long unprotect_failures;
	unsigned long wake_failures;
	unsigned long eagain_errors;
	unsigned long read_errors;
	time_t last_print_time;
} cow_stats;

#define COW_STAT_INC(field) \
	__atomic_fetch_add(&cow_stats.field, 1, __ATOMIC_RELAXED)

static void check_and_print_cow_stats(void)
{
	time_t now = time(NULL);
	time_t last = __atomic_load_n(&cow_stats.last_print_time, __ATOMIC_RELAXED);
	unsigned long wr, fk, rm, un, cp, up, wk, af, rf, uf, wf, re, ea;

	if (now - last < 1)
		return;
	if (!__atomic_compare_exchange_n(&cow_stats.last_print_time, &last, now,
					 false, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
		return;

	wr = __atomic_exchange_n(&cow_stats.write_faults, 0, __ATOMIC_RELAXED);
	fk = __atomic_exchange_n(&cow_stats.fork_events, 0, __ATOMIC_RELAXED);
	rm = __atomic_exchange_n(&cow_stats.remap_events, 0, __ATOMIC_RELAXED);
	un = __atomic_exchange_n(&cow_stats.unknown_events, 0, __ATOMIC_RELAXED);
	cp = __atomic_exchange_n(&cow_stats.pages_copied, 0, __ATOMIC_RELAXED);
	up = __atomic_exchange_n(&cow_stats.pages_unprotected, 0, __ATOMIC_RELAXED);
	wk = __atomic_exchange_n(&cow_stats.pages_woken, 0, __ATOMIC_RELAXED);
	af = __atomic_exchange_n(&cow_stats.alloc_failures, 0, __ATOMIC_RELAXED);
	rf = __atomic_exchange_n(&cow_stats.read_failures, 0, __ATOMIC_RELAXED);
	uf = __atomic_exchange_n(&cow_stats.unprotect_failures, 0, __ATOMIC_RELAXED);
	wf = __atomic_exchange_n(&cow_stats.wake_failures, 0, __ATOMIC_RELAXED);
	re = __atomic_exchange_n(&cow_stats.read_errors, 0, __ATOMIC_RELAXED);
	ea = __atomic_exchange_n(&cow_stats.eagain_errors, 0, __ATOMIC_RELAXED);

	pr_err("[COW_STATS] events: wr=%lu fork=%lu remap=%lu unk=%lu | "
	       "ops: copied=%lu unprot=%lu woken=%lu | "
	       "errs: alloc=%lu read=%lu unprot_err=%lu wake_err=%lu "
	       "read_err=%lu eagain_err=%lu\n",
	       wr, fk, rm, un, cp, up, wk, af, rf, uf, wf, re, ea);
}


static void cow_monitor_wakeup(void)
{
	uint64_t one = 1;
	ssize_t ret;

	if (g_monitor_eventfd < 0)
		return;
	ret = write(g_monitor_eventfd, &one, sizeof(one));
	(void)ret;
}

static void cow_monitor_drain_eventfd(void)
{
	uint64_t v;

	if (g_monitor_eventfd < 0)
		return;
	while (read(g_monitor_eventfd, &v, sizeof(v)) == sizeof(v))
		;
}

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
static int cow_clear_written_bits(struct cow_dump_info *cdi);
static void free_cow_page_entry(struct cow_page_queue_entry *entry)
{
	if (entry->data)
		xfree(entry->data);
	xfree(entry);
}

int cow_dump_init(struct pstree_item *item, struct vm_area_list *vma_area_list,
		  struct parasite_ctl *ctl)
{
	struct cow_dump_info *cdi = g_cow_info;
	struct parasite_cow_dump_args *args = NULL;
	bool created_session = false;
	int ret;
	unsigned long args_size;

	pr_info("Initializing COW dump for pid %d\n", item->pid->real);

	if (cdi) {
		pr_warn("COW tracking already initialized for pid %d, skipping\n",
			cdi->source_pid);
		return 0;
	}

	if (!cow_check_kernel_support()) {
		pr_err("Kernel doesn't support COW dump\n");
		return -1;
	}

	cdi = xzalloc(sizeof(*cdi));
	if (!cdi)
		return -1;

	cdi->source_pid = item->pid->real;
	cdi->uffd = -1;
	cdi->uffd_async = -1;
	cdi->uffd_sync = -1;
	cdi->phase = COW_PHASE_IDLE;

	if (mpsc_init(cdi->page_queue.head, cdi->page_queue.tail,
		      cdi->page_queue.size, struct cow_page_mpsc_node)) {
		xfree(cdi);
		return -1;
	}

	g_cow_info = cdi;
	created_session = true;

	if (g_monitor_eventfd >= 0) {
		close(g_monitor_eventfd);
		g_monitor_eventfd = -1;
	}
	g_monitor_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (g_monitor_eventfd < 0) {
		pr_perror("Failed to create cow monitor eventfd");
		goto err;
	}

	if (!ctl) {
		pr_err("Parasite control required for WP_ASYNC uffd creation\n");
		goto err;
	}

	args_size = sizeof(*args);
	args = compel_parasite_args_s(ctl, args_size);
	if (!args) {
		pr_err("Failed to allocate parasite args\n");
		goto err;
	}

	args->nr_vmas = 0;
	args->total_pages = 0;
	args->nr_failed_vmas = 0;
	args->uffd_features = UFFD_FEATURE_WP_ASYNC;
	args->ret = -1;

	ret = compel_rpc_call(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0) {
		pr_err("Failed to initiate COW dump RPC\n");
		goto err;
	}

	compel_util_recv_fd(ctl, &cdi->uffd);
	if (cdi->uffd < 0) {
		pr_err("Failed to receive uffd from parasite: %d\n",
		       cdi->uffd);
		goto err;
	}

	ret = compel_rpc_sync(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0 || args->ret != 0) {
		pr_err("Parasite COW dump init failed: %d (ret=%d)\n",
		       ret, args->ret);
		goto err;
	}

	cdi->uffd_async = cdi->uffd;
	cdi->phase = COW_PHASE_ASYNC_BULK;

	ret = cow_register_vmas(cdi, vma_area_list, &cdi->total_pages);
	if (ret)
		goto err;

	if (cow_apply_writeprotect(cdi))
		goto err;

	if (cow_clear_written_bits(cdi))
		goto err;

	pr_info("COW dump initialized for pid %d: tracked=%u pages=%lu uffd=%d\n",
		item->pid->real, cdi->nr_tracked_vmas,
		cdi->total_pages, cdi->uffd);
	return 0;

err:
	if (created_session) {
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
	}
	return -1;
}

void cow_dump_fini(void)
{
	struct cow_page_queue_entry *qe;
	int queue_remaining = 0;

	if (!g_cow_info)
		return;

	if (cow_stop_monitor_thread()) {
		pr_err("Failed to stop COW workers, skipping cleanup to avoid races\n");
		return;
	}

	wait_for_page_server_thread();
	pr_info("Cleaning up COW dump\n");

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
	if (g_cow_info->uffd_async >= 0 && g_cow_info->uffd_async != g_cow_info->uffd)
		close(g_cow_info->uffd_async);
	if (g_cow_info->uffd_sync >= 0)
		close(g_cow_info->uffd_sync);
	xfree(g_cow_info->tracked_vmas);
	xfree(g_cow_info);
	g_cow_info = NULL;
}

/* ------------------------------------------------------------------ */
/*  Fault handling (hot path)                                          */
/* ------------------------------------------------------------------ */

/*
 * cow_handle_write_fault — pre-read 16 pages (64KB) around the faulting address.
 *
 * For each fault we read 8 pages before + the faulting page + 7 pages after,
 * clamped to VMA boundaries.  This amortizes the per-fault overhead:
 *   - 1 process_vm_readv  (was 1 per page)
 *   - 1 UFFDIO_WRITEPROTECT  (was 1 per page)
 *   - 1 UFFDIO_WAKE  (faulting page only)
 *
 * Neighboring pages that were already captured by another worker are
 * detected via atomic test-and-set on the COW bitmap and skipped.
 */
static int cow_handle_write_fault(struct cow_dump_info *cdi,
				  unsigned long addr,
				  struct cow_fault_worker *worker)
{
	unsigned long page_addr = addr & ~(PAGE_SIZE - 1);
	unsigned long vma_start = 0, vma_end = 0;
	unsigned long range_start, range_end;
	unsigned int nr_pages, i, enqueued = 0;
	struct iovec local_iov[COW_PREREAD_TOTAL];
	struct iovec remote_iov;
	void *page_bufs[COW_PREREAD_TOTAL];
	struct uffdio_writeprotect wp;
	struct uffdio_range wake_range;
	ssize_t ret;
	bool found_vma = false;

	pr_debug("Write fault at 0x%lx\n", page_addr);
	COW_STAT_INC(write_faults);

	/* Find the VMA containing this fault */
	for (i = 0; i < cdi->nr_tracked_vmas; i++) {
		if (page_addr >= cdi->tracked_vmas[i].start &&
		    page_addr < cdi->tracked_vmas[i].end) {
			vma_start = cdi->tracked_vmas[i].start;
			vma_end = cdi->tracked_vmas[i].end;
			found_vma = true;
			break;
		}
	}

	if (!found_vma) {
		pr_err("Write fault at 0x%lx not in any tracked VMA\n",
		       page_addr);
		return -1;
	}

	/* Calculate pre-read range, clamped to VMA boundaries */
	range_start = page_addr - (unsigned long)COW_PREREAD_BEFORE * PAGE_SIZE;
	if (range_start < vma_start || range_start > page_addr) /* underflow */
		range_start = vma_start;

	range_end = page_addr + (unsigned long)(COW_PREREAD_AFTER + 1) * PAGE_SIZE;
	if (range_end > vma_end)
		range_end = vma_end;

	nr_pages = (unsigned int)((range_end - range_start) / PAGE_SIZE);
	if (nr_pages > COW_PREREAD_TOTAL)
		nr_pages = COW_PREREAD_TOTAL;

	/* Allocate page buffers from worker pool (or malloc fallback) */
	for (i = 0; i < nr_pages; i++) {
		page_bufs[i] = worker ? cow_worker_pool_get(worker)
				      : xmalloc(PAGE_SIZE);
		if (!page_bufs[i]) {
			unsigned int j;

			for (j = 0; j < i; j++)
				xfree(page_bufs[j]);
			COW_STAT_INC(alloc_failures);
			return -1;
		}
		local_iov[i].iov_base = page_bufs[i];
		local_iov[i].iov_len = PAGE_SIZE;
	}

	/* Single process_vm_readv for the entire range (scatter local iovecs) */
	remote_iov.iov_base = (void *)range_start;
	remote_iov.iov_len = (size_t)nr_pages * PAGE_SIZE;

	ret = process_vm_readv(cdi->source_pid, local_iov, nr_pages,
			       &remote_iov, 1, 0);
	if (ret != (ssize_t)((size_t)nr_pages * PAGE_SIZE)) {
		pr_perror("Pre-read %u pages at 0x%lx from pid %d failed "
			  "(got %zd bytes)", nr_pages, range_start,
			  cdi->source_pid, ret);
		for (i = 0; i < nr_pages; i++)
			xfree(page_bufs[i]);
		COW_STAT_INC(read_failures);
		return -1;
	}
	__atomic_fetch_add(&cow_stats.pages_copied, nr_pages, __ATOMIC_RELAXED);

	/* Unprotect the entire range in one ioctl */
	wp.range.start = range_start;
	wp.range.len = (unsigned long)nr_pages * PAGE_SIZE;
	wp.mode = 0;

	if (ioctl(cdi->uffd, UFFDIO_WRITEPROTECT, &wp)) {
		pr_perror("Failed to unprotect range 0x%lx-%lx",
			  range_start, range_end);
		for (i = 0; i < nr_pages; i++)
			xfree(page_bufs[i]);
		COW_STAT_INC(unprotect_failures);
		return -1;
	}
	__atomic_fetch_add(&cow_stats.pages_unprotected, nr_pages,
			   __ATOMIC_RELAXED);

	/* Wake only the faulting page (neighbors weren't blocked) */
	wake_range.start = page_addr;
	wake_range.len = PAGE_SIZE;

	if (ioctl(cdi->uffd, UFFDIO_WAKE, &wake_range)) {
		pr_perror("Failed to wake faulting thread at 0x%lx", page_addr);
		for (i = 0; i < nr_pages; i++)
			xfree(page_bufs[i]);
		COW_STAT_INC(wake_failures);
		return -1;
	}
	COW_STAT_INC(pages_woken);
	__atomic_fetch_sub(&cdi->total_pages, nr_pages, __ATOMIC_RELAXED);

	/* Enqueue each page that isn't already captured */
	for (i = 0; i < nr_pages; i++) {
		unsigned long pa = range_start + (unsigned long)i * PAGE_SIZE;
		struct cow_page_queue_entry *entry;

		/*
		 * Atomic test-and-set: if another worker already captured
		 * this page (e.g. from an overlapping pre-read window),
		 * skip the enqueue to avoid duplicates.
		 */
		if (cow_test_and_set_bitmap(pa)) {
			xfree(page_bufs[i]);
			continue;
		}

		entry = xmalloc(sizeof(*entry));
		if (!entry) {
			pr_err("Failed to allocate queue entry for page 0x%lx, "
			       "clearing bitmap for P3 fallback\n", pa);
			xfree(page_bufs[i]);
			cow_clear_bitmap(pa);
			continue;
		}

		entry->vaddr = pa;
		entry->data = page_bufs[i];
		entry->ppb = NULL;
		entry->seg_idx = 0;
		entry->page_idx_in_seg = 0;
		entry->next = NULL;

		if (mpsc_enqueue(cdi->page_queue.tail, cdi->page_queue.size,
				 entry, struct cow_page_mpsc_node)) {
			pr_err("Failed to enqueue COW page 0x%lx\n", pa);
			xfree(entry->data);
			xfree(entry);
			cow_clear_bitmap(pa);
			continue;
		}
		enqueued++;
	}

	pr_debug("Pre-read fault 0x%lx: range 0x%lx-%lx (%u pages, %u new)\n",
		 page_addr, range_start, range_end, nr_pages, enqueued);

	return 0;
}

/* ------------------------------------------------------------------ */
/*  Event processing + poll loop                                       */
/* ------------------------------------------------------------------ */

static int cow_process_events(struct cow_dump_info *cdi, bool blocking,
			      struct cow_fault_worker *worker)
{
	struct uffd_msg msg;
	struct pollfd pfd;
	int ret, poll_ret;

	while (1) {
		check_and_print_cow_stats();

		ret = read(cdi->uffd, &msg, sizeof(msg));

		if (ret < 0 && errno == EAGAIN && blocking) {
			pfd.fd = cdi->uffd;
			pfd.events = POLLIN;
			pfd.revents = 0;

			poll_ret = poll(&pfd, 1, 500);
			if (poll_ret < 0) {
				pr_perror("poll() failed on uffd");
				COW_STAT_INC(read_errors);
				return -1;
			}
			if (poll_ret == 0)
				return 0;

			ret = read(cdi->uffd, &msg, sizeof(msg));
		}

		if (ret < 0) {
			if (errno == EAGAIN && !blocking) {
				COW_STAT_INC(eagain_errors);
				return 0;
			}
			pr_perror("Failed to read uffd event");
			COW_STAT_INC(read_errors);
			return -1;
		}

		if (ret != sizeof(msg)) {
			pr_err("Short read from uffd: %d\n", ret);
			COW_STAT_INC(read_errors);
			return -1;
		}

		switch (msg.event) {
		case UFFD_EVENT_PAGEFAULT:
			if (msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WP) {
				if (cow_handle_write_fault(cdi, msg.arg.pagefault.address, worker))
					return -1;
			}
			break;
		case UFFD_EVENT_FORK:
			COW_STAT_INC(fork_events);
			pr_warn("Process forked during COW dump (not fully supported)\n");
			break;
		case UFFD_EVENT_REMAP:
			COW_STAT_INC(remap_events);
			pr_info("Memory remap event\n");
			break;
		default:
			COW_STAT_INC(unknown_events);
			pr_err("Unexpected uffd event: %u\n", msg.event);
			return -1;
		}
	}

	return 0;
}

/*
 * cow_wait_for_events — poll the single uffd + eventfd, then dispatch.
 * Stack-allocated 2-element pollfd — zero malloc, zero lock.
 */
static int cow_wait_for_events(struct cow_dump_info *cdi, int timeout_ms,
			       struct cow_fault_worker *w)
{
	struct pollfd pfds[2];
	int ret;

	pfds[0].fd = g_monitor_eventfd;
	pfds[0].events = POLLIN;
	pfds[0].revents = 0;
	pfds[1].fd = cdi->uffd;
	pfds[1].events = POLLIN;
	pfds[1].revents = 0;

	ret = poll(pfds, 2, timeout_ms);
	if (ret < 0) {
		pr_perror("poll() failed on uffd");
		return -1;
	}
	if (ret == 0)
		return 0;

	if (pfds[0].revents & POLLIN) {
		cow_monitor_drain_eventfd();
		return 0;
	}

	if (pfds[1].revents & (POLLIN | POLLERR | POLLHUP | POLLNVAL)) {
		if (cow_process_events(cdi, false, w) < 0) {
			pr_err("Error processing COW events for pid %d\n",
			       cdi->source_pid);
			return -1;
		}
	}

	return ret;
}

/* ------------------------------------------------------------------ */
/*  Per-worker page buffer pool                                        */
/* ------------------------------------------------------------------ */

static void cow_worker_pool_init(struct cow_fault_worker *w)
{
	unsigned int i;

	for (i = 0; i < COW_PAGE_POOL_SIZE; i++) {
		w->page_pool[i] = xmalloc(PAGE_SIZE);
		if (!w->page_pool[i])
			break;
	}
	w->pool_count = i;
}

static void cow_worker_pool_fini(struct cow_fault_worker *w)
{
	unsigned int i;

	for (i = 0; i < w->pool_count; i++)
		xfree(w->page_pool[i]);
	w->pool_count = 0;
}

static void *cow_worker_pool_get(struct cow_fault_worker *w)
{
	if (w->pool_count > 0)
		return w->page_pool[--w->pool_count];
	return xmalloc(PAGE_SIZE);
}

/* ------------------------------------------------------------------ */
/*  Worker thread pool                                                 */
/* ------------------------------------------------------------------ */

static void *cow_fault_worker_fn(void *arg)
{
	struct cow_fault_worker *w = (struct cow_fault_worker *)arg;
	struct cow_dump_info *cdi = w->cdi;
	char name[16];

	snprintf(name, sizeof(name), "cow-w%d", w->id);
	pthread_setname_np(pthread_self(), name);

	cow_worker_pool_init(w);
	pr_info("COW fault worker %d started (pool=%u bufs)\n",
		w->id, w->pool_count);

	while (!__atomic_load_n(&g_stop_monitoring, __ATOMIC_ACQUIRE)) {
		int ret = cow_wait_for_events(cdi, 500, w);
		if (ret < 0) {
			pr_err("COW fault worker %d: event error, exiting\n",
			       w->id);
			break;
		}
	}

	cow_worker_pool_fini(w);
	pr_info("COW fault worker %d stopped\n", w->id);
	return NULL;
}

int cow_start_monitor_thread(void)
{
	unsigned int nr, i, created = 0;
	int ret;

	pthread_mutex_lock(&g_monitor_state_lock);

	if (!g_cow_info) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("COW dump not initialized\n");
		return -1;
	}

	if (g_monitor_eventfd < 0) {
		g_monitor_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
		if (g_monitor_eventfd < 0) {
			pthread_mutex_unlock(&g_monitor_state_lock);
			pr_perror("Failed to create cow monitor eventfd");
			return -1;
		}
	}

	if (g_cow_info->uffd < 0) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("COW tracking has no userfaultfd\n");
		return -1;
	}

	if (__atomic_load_n(&g_workers_running, __ATOMIC_ACQUIRE)) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		return 0;
	}

	nr = COW_FAULT_WORKERS;
	g_workers = xzalloc(nr * sizeof(*g_workers));
	if (!g_workers) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		return -1;
	}

	__atomic_store_n(&g_stop_monitoring, false, __ATOMIC_RELEASE);
	__atomic_store_n(&g_workers_running, true, __ATOMIC_RELEASE);

	for (i = 0; i < nr; i++) {
		g_workers[i].id = i;
		g_workers[i].cdi = g_cow_info;

		ret = pthread_create(&g_workers[i].thread, NULL,
				     cow_fault_worker_fn, &g_workers[i]);
		if (ret) {
			pr_err("Failed to create COW fault worker %u: %s\n",
			       i, strerror(ret));
			break;
		}
		created++;
	}

	g_nr_workers = created;
	pthread_mutex_unlock(&g_monitor_state_lock);

	if (!created) {
		__atomic_store_n(&g_workers_running, false, __ATOMIC_RELEASE);
		xfree(g_workers);
		g_workers = NULL;
		return -1;
	}

	pr_info("COW fault worker pool started: %u/%u workers\n", created, nr);
	return 0;
}

int cow_stop_monitor_thread(void)
{
	unsigned int i;
	void *retval;
	int ret;

	pthread_mutex_lock(&g_monitor_state_lock);
	if (!__atomic_load_n(&g_workers_running, __ATOMIC_ACQUIRE)) {
		__atomic_store_n(&g_stop_monitoring, false, __ATOMIC_RELEASE);
		pthread_mutex_unlock(&g_monitor_state_lock);
		return 0;
	}

	pr_info("Stopping COW fault worker pool (%u workers)\n", g_nr_workers);
	__atomic_store_n(&g_stop_monitoring, true, __ATOMIC_RELEASE);
	pthread_mutex_unlock(&g_monitor_state_lock);

	cow_monitor_wakeup();

	for (i = 0; i < g_nr_workers; i++) {
		ret = pthread_join(g_workers[i].thread, &retval);
		if (ret && ret != ESRCH && ret != EINVAL)
			pr_err("Failed to join COW fault worker %u: %s\n",
			       i, strerror(ret));
	}

	pthread_mutex_lock(&g_monitor_state_lock);
	__atomic_store_n(&g_workers_running, false, __ATOMIC_RELEASE);
	__atomic_store_n(&g_stop_monitoring, false, __ATOMIC_RELEASE);
	xfree(g_workers);
	g_workers = NULL;
	g_nr_workers = 0;
	pthread_mutex_unlock(&g_monitor_state_lock);

	pr_info("COW fault worker pool stopped\n");
	return 0;
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

/* ------------------------------------------------------------------ */
/*  Baseline written-bit clearing                                      */
/* ------------------------------------------------------------------ */

/*
 * cow_clear_written_bits - Clear PAGE_IS_WRITTEN on all tracked VMAs
 *
 * After setting up WP_ASYNC write-protect, pages that were already
 * dirty still have their "written" bit set.  We use PAGEMAP_SCAN with
 * PM_SCAN_WP_MATCHING to re-apply write-protect on those pages and
 * clear their written bits.  After this, only pages actually written
 * during Phase 2 (while the process runs) will show PAGE_IS_WRITTEN
 * in the Phase 3 dirty scan.
 */
static int cow_clear_written_bits(struct cow_dump_info *cdi)
{
	char path[64];
	int pagemap_fd;
	unsigned int i;
	int ret = 0;
	unsigned long total_cleared = 0;
	struct page_region debug_regs[256];

	struct pm_scan_arg args = {
		.size = sizeof(struct pm_scan_arg),
		.flags = PM_SCAN_WP_MATCHING,
		.start = 0,
		.end = 0,
		.walk_end = 0,
		.vec = (u64)(unsigned long)debug_regs,
		.vec_len = 256,
		.max_pages = 0,
		.category_anyof_mask = PAGE_IS_WRITTEN,
		.return_mask = PAGE_IS_WRITTEN,
	};

	snprintf(path, sizeof(path), "/proc/%d/pagemap", cdi->source_pid);
	pagemap_fd = open(path, O_RDWR);
	if (pagemap_fd < 0) {
		pr_perror("Cannot open %s for clearing written bits", path);
		return -1;
	}

	for (i = 0; i < cdi->nr_tracked_vmas; i++) {
		unsigned long vma_start = cdi->tracked_vmas[i].start;
		unsigned long vma_end = cdi->tracked_vmas[i].end;
		unsigned long vma_cleared = 0;

		args.start = vma_start;
		args.end = vma_end;
		args.walk_end = vma_start;

		pr_info("clear_written_bits: VMA 0x%lx-0x%lx (%lu pages)\n",
			vma_start, vma_end,
			(vma_end - vma_start) / PAGE_SIZE);

		do {
			long nregs;
			unsigned int j;

			args.start = args.walk_end;
			nregs = ioctl(pagemap_fd, PAGEMAP_SCAN, &args);
			if (nregs < 0) {
				pr_perror("PAGEMAP_SCAN WP_MATCHING clear for "
					  "VMA 0x%lx-0x%lx failed (errno=%d)",
					  vma_start, vma_end, errno);
				ret = -1;
				break;
			}

			for (j = 0; j < (unsigned int)nregs; j++) {
				unsigned long pages = (debug_regs[j].end -
						       debug_regs[j].start) /
						      PAGE_SIZE;
				vma_cleared += pages;
				pr_info("  cleared region[%u]: 0x%lx-0x%lx "
					"(%lu pages, cat=0x%llx)\n",
					j, (unsigned long)debug_regs[j].start,
					(unsigned long)debug_regs[j].end,
					pages,
					(unsigned long long)debug_regs[j].categories);
			}

			if (nregs == 0) {
				pr_info("  no regions found (walk_end=0x%lx vma_end=0x%lx)\n",
					(unsigned long)args.walk_end, vma_end);
				break;
			}
		} while (args.walk_end != vma_end);

		pr_info("clear_written_bits: VMA 0x%lx cleared %lu pages\n",
			vma_start, vma_cleared);
		total_cleared += vma_cleared;

		if (ret)
			break;
	}

	close(pagemap_fd);

	pr_info("Cleared written bits on %u VMAs: %lu total pages cleared\n",
		cdi->nr_tracked_vmas, total_cleared);
	return ret;
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

	/*
	 * Clear pre-existing PAGE_IS_WRITTEN bits so that the Phase 3
	 * dirty scan only finds pages written during Phase 2 (while
	 * the process runs with WP_ASYNC active).
	 */
	if (cow_clear_written_bits(cdi))
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
		.return_mask = PAGE_IS_WRITTEN,
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
			pr_info("PAGEMAP_SCAN: scanning VMA 0x%lx-0x%lx "
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

			pr_info("PAGEMAP_SCAN: returned %ld regions, "
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

				pr_info("  dirty region[%u]: 0x%lx-0x%lx "
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
 * cow_precreate_sync_uffd - Pre-create a WP_SYNC uffd via the parasite
 *
 * Must be called while the parasite is still alive (before compel_cure).
 * The parasite creates a userfaultfd(2) inside the target and passes it
 * back via SCM_RIGHTS so Phase 4 can use it for WP_SYNC convergence.
 */
int cow_precreate_sync_uffd(struct parasite_ctl *ctl)
{
	struct cow_dump_info *cdi = g_cow_info;
	struct parasite_cow_dump_args *args;
	unsigned long args_size;
	int ret;

	if (!cdi) {
		pr_err("COW dump not initialized\n");
		return -1;
	}

	if (!ctl) {
		pr_err("Parasite required for WP_SYNC uffd pre-creation\n");
		return -1;
	}

	pr_info("Pre-creating WP_SYNC uffd via parasite for pid %d\n",
		cdi->source_pid);

	args_size = sizeof(*args);
	args = compel_parasite_args_s(ctl, args_size);
	if (!args) {
		pr_err("Failed to allocate parasite args for WP_SYNC\n");
		return -1;
	}

	args->nr_vmas = 0;
	args->total_pages = 0;
	args->nr_failed_vmas = 0;
	args->uffd_features = 0; /* WP_SYNC: UFFD_FEATURE_PAGEFAULT_FLAG_WP */
	args->ret = -1;

	ret = compel_rpc_call(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0) {
		pr_err("Failed to initiate WP_SYNC uffd RPC\n");
		return -1;
	}

	compel_util_recv_fd(ctl, &cdi->uffd_sync);
	if (cdi->uffd_sync < 0) {
		pr_err("Failed to receive WP_SYNC uffd from parasite: %d\n",
		       cdi->uffd_sync);
		return -1;
	}

	ret = compel_rpc_sync(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0 || args->ret != 0) {
		pr_err("Parasite WP_SYNC uffd creation failed: %d (ret=%d)\n",
		       ret, args->ret);
		close(cdi->uffd_sync);
		cdi->uffd_sync = -1;
		return -1;
	}

	pr_info("Pre-created WP_SYNC uffd: fd=%d\n", cdi->uffd_sync);
	return 0;
}

int cow_setup_sync_for_dirty(unsigned long *dirty_ranges,
			     unsigned int nr_dirty_ranges)
{
	struct cow_dump_info *cdi = g_cow_info;
	struct uffdio_register reg;
	struct uffdio_writeprotect wp;
	int new_uffd;
	unsigned int i;

	if (!cdi) {
		pr_err("COW dump not initialized\n");
		return -1;
	}

	/* Fast path: no dirty pages means nothing to converge */
	if (nr_dirty_ranges == 0) {
		pr_info("No dirty pages, skipping WP_SYNC convergence\n");
		cdi->phase = COW_PHASE_DONE;
		return 0;
	}

	pr_info("Setting up WP_SYNC for %u dirty ranges\n", nr_dirty_ranges);

	if (cdi->uffd_sync >= 0) {
		/* Use pre-created WP_SYNC uffd from parasite */
		new_uffd = cdi->uffd_sync;
		cdi->uffd_sync = -1; /* Ownership transferred */
		pr_info("Using pre-created WP_SYNC uffd: fd=%d\n", new_uffd);
	} else {
		pr_err("No WP_SYNC uffd available (no pre-created fd)\n");
		return -1;
	}

	/* Close old async uffd */
	if (cdi->uffd >= 0)
		close(cdi->uffd);
	cdi->uffd = new_uffd;
	cdi->uffd_async = -1;

	/* Register and write-protect each dirty range */
	for (i = 0; i < nr_dirty_ranges; i++) {
		unsigned long start = dirty_ranges[i * 2];
		unsigned long len = dirty_ranges[i * 2 + 1];

		reg.range.start = start;
		reg.range.len = len;
		reg.mode = UFFDIO_REGISTER_MODE_WP;
		pr_info("UFFDIO_REGISTER WP_SYNC 0x%lx-%lx len=%ld\n",start, start +len, len);
		if (ioctl(cdi->uffd, UFFDIO_REGISTER, &reg)) {
			pr_perror("UFFDIO_REGISTER WP_SYNC 0x%lx-%lx failed",
				  start, start + len);
			continue; /* Best effort */
		}

		wp.range.start = start;
		wp.range.len = len;
		wp.mode = UFFDIO_WRITEPROTECT_MODE_WP;

		if (ioctl(cdi->uffd, UFFDIO_WRITEPROTECT, &wp)) {
			pr_perror("UFFDIO_WRITEPROTECT 0x%lx-%lx failed",
				  start, start + len);
			continue; /* Best effort */
		}
	}

	cdi->phase = COW_PHASE_SYNC_CONVERGE;

	/* Start monitor thread for convergence */
	if (cow_start_monitor_thread()) {
		pr_err("Failed to start monitor thread for convergence\n");
		return -1;
	}

	pr_info("WP_SYNC convergence mode active\n");
	return 0;
}
