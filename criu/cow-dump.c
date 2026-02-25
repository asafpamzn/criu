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
#include "page-xfer.h"
#include "page-pipe.h"
#include "parasite-syscall.h"
#include "mem.h"
#include "vma.h"
#include "util.h"
#include "kerndat.h"
#include "pagemap_scan.h"
#include "criu-log.h"
#include "parasite.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-dump: "

struct cow_tracked_vma {
	unsigned long start;
	unsigned long end;
};

struct cow_tracked_task {
	pid_t source_pid;
	int uffd;
	int pagemap_fd;		/* /proc/<pid>/pagemap for PAGEMAP_SCAN */
	unsigned long total_pages;
	unsigned int nr_tracked_vmas;
	struct cow_tracked_vma *tracked_vmas;
	struct list_head list;
};

/* COW dump state for one dump session */
struct cow_dump_info {
	struct list_head tracked_tasks;
	unsigned long total_pages;
	unsigned long iteration;
	struct hlist_head cow_hash[COW_HASH_SIZE];	/* Hash table for copied pages */
	pthread_spinlock_t cow_hash_locks[COW_HASH_SIZE];	/* Per-bucket spinlocks */
	struct list_head cow_page_queue;	/* FIFO queue of COW pages */
	pthread_spinlock_t queue_lock;		/* Protects the queue */
};

/* Forward declarations for statics used by WP functions */
static struct cow_dump_info *g_cow_info;
static bool g_wp_async_mode;

/*
 * Applying UFFD write-protect over a large address space can dominate the
 * initial stall. We can apply it in parallel from the CRIU process after
 * receiving the userfaultfd from the parasite.
 */
#define COW_WP_CHUNK_SIZE	(512UL * 1024 * 1024)
/* Use all available CPUs — more threads reduce WP ioctl serialization */
#define COW_WP_MAX_THREADS	0	/* 0 = use nproc (set in cow_wp_nr_threads) */

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

	/* Lower priority so main dump thread gets CPU first */
	if (nice(19) == -1 && errno != 0)
		pr_debug("nice(19) failed: %s\n", strerror(errno));
	{
		struct sched_param sp = { .sched_priority = 0 };
		sched_setscheduler(0, SCHED_BATCH, &sp);
	}

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

static struct cow_wp_range *cow_wp_build_ranges(struct cow_tracked_task *task,
						unsigned int *nr_ranges)
{
	struct cow_wp_range *ranges;
	unsigned long start, end, pos;
	unsigned long len;
	unsigned int nr = 0;
	unsigned int i, idx = 0;

	for (i = 0; i < task->nr_tracked_vmas; i++) {
		start = task->tracked_vmas[i].start;
		end = task->tracked_vmas[i].end;
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

	for (i = 0; i < task->nr_tracked_vmas; i++) {
		start = task->tracked_vmas[i].start;
		end = task->tracked_vmas[i].end;
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

/*
 * Async WP: split write-protect into start (launch threads)
 * and finish (join + check).  This allows overlapping WP with
 * other dump work to reduce total freeze time.
 */
static pthread_t *g_wp_threads;
static struct cow_wp_job *g_wp_jobs;
static struct cow_wp_range *g_wp_ranges;
static unsigned int g_wp_created;
static unsigned int g_wp_nr_ranges;
static struct timespec g_wp_t_start;


int cow_dump_start_wp(void)
{
	struct cow_tracked_task *task;
	unsigned int nr_threads, per, i;

	g_wp_threads = NULL;
	g_wp_jobs = NULL;
	g_wp_ranges = NULL;
	g_wp_created = 0;

	/* Find the first tracked task (single-process COW dump) */
	if (!g_cow_info || list_empty(&g_cow_info->tracked_tasks))
		return 0;
	task = list_first_entry(&g_cow_info->tracked_tasks,
				struct cow_tracked_task, list);
	if (!task->nr_tracked_vmas)
		return 0;

	clock_gettime(CLOCK_MONOTONIC, &g_wp_t_start);

	g_wp_ranges = cow_wp_build_ranges(task, &g_wp_nr_ranges);
	if (!g_wp_ranges) {
		if (g_wp_nr_ranges)
			return -1;
		return 0;
	}

	nr_threads = cow_wp_nr_threads(g_wp_nr_ranges);
	g_wp_threads = xmalloc(nr_threads * sizeof(*g_wp_threads));
	g_wp_jobs = xzalloc(nr_threads * sizeof(*g_wp_jobs));
	if (!g_wp_threads || !g_wp_jobs)
		goto err;

	per = (g_wp_nr_ranges + nr_threads - 1) / nr_threads;
	for (i = 0; i < nr_threads; i++) {
		g_wp_jobs[i].uffd = task->uffd;
		g_wp_jobs[i].ranges = g_wp_ranges;
		g_wp_jobs[i].start_idx = i * per;
		g_wp_jobs[i].end_idx = g_wp_jobs[i].start_idx + per;
		if (g_wp_jobs[i].end_idx > g_wp_nr_ranges)
			g_wp_jobs[i].end_idx = g_wp_nr_ranges;
	}

	for (i = 0; i < nr_threads; i++) {
		if (g_wp_jobs[i].start_idx >= g_wp_jobs[i].end_idx)
			break;
		if (pthread_create(&g_wp_threads[i], NULL,
				   cow_wp_worker, &g_wp_jobs[i])) {
			pr_err("Failed to create WP worker thread\n");
			goto err;
		}
		g_wp_created++;
	}

	pr_info("WP async: launched %u threads for %u ranges\n",
		g_wp_created, g_wp_nr_ranges);
	return 0;

err:
	for (i = 0; i < g_wp_created; i++)
		pthread_join(g_wp_threads[i], NULL);
	xfree(g_wp_threads);
	xfree(g_wp_jobs);
	xfree(g_wp_ranges);
	g_wp_threads = NULL;
	g_wp_jobs = NULL;
	g_wp_ranges = NULL;
	g_wp_created = 0;
	return -1;
}

int cow_dump_finish_wp(void)
{
	struct timespec t_end;
	unsigned long sec, nsec;
	unsigned int i;
	int ret = 0;

	if (g_wp_created) {
		for (i = 0; i < g_wp_created; i++)
			pthread_join(g_wp_threads[i], NULL);

		for (i = 0; i < g_wp_created; i++) {
			if (g_wp_jobs[i].err) {
				pr_err("UFFD write-protect failed: %s (%d)\n",
				       strerror(-g_wp_jobs[i].err),
				       -g_wp_jobs[i].err);
				ret = -1;
				break;
			}
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &t_end);

	sec = t_end.tv_sec - g_wp_t_start.tv_sec;
	if (t_end.tv_nsec < g_wp_t_start.tv_nsec) {
		sec--;
		nsec = 1000000000UL + t_end.tv_nsec - g_wp_t_start.tv_nsec;
	} else {
		nsec = t_end.tv_nsec - g_wp_t_start.tv_nsec;
	}
	pr_err("TIMING: cow_dump_writeprotect took %lu.%06lu seconds "
	       "(%u ranges, %u threads)\n",
	       sec, nsec / 1000, g_wp_nr_ranges, g_wp_created);

	xfree(g_wp_threads);
	xfree(g_wp_jobs);
	xfree(g_wp_ranges);
	g_wp_threads = NULL;
	g_wp_jobs = NULL;
	g_wp_ranges = NULL;
	g_wp_created = 0;
	return ret;
}


/* g_cow_info forward-declared above WP functions */
static pthread_t g_monitor_thread;
static volatile bool g_monitor_thread_running = false;
static volatile bool g_stop_monitoring = false;
static pthread_mutex_t g_monitor_state_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_tracked_tasks_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_monitor_eventfd = -1;

/* WP_ASYNC mode: kernel auto-resolves WP faults, no thread parking */
/* g_wp_async_mode declared above with other forward declarations */
static unsigned long g_tracked_tasks_generation;
static unsigned long g_monitor_snapshot_generation;

#define COW_CONVERGENCE_THRESHOLD 100  /* Stop if < 100 pages dirty per iteration */
#define COW_FLUSH_THRESHOLD 1000       /* Flush to disk every 1000 pages */

/* Statistics tracking structure */
static struct {
	/* Event counters */
	unsigned long write_faults;
	unsigned long fork_events;
	unsigned long remap_events;
	unsigned long unknown_events;
	
	/* Operation counters */
	unsigned long pages_copied;
	unsigned long pages_unprotected;
	unsigned long pages_woken;
	
	/* Error counters */
	unsigned long alloc_failures;
	unsigned long read_failures;
	unsigned long unprotect_failures;
	unsigned long wake_failures;
	unsigned long eagain_errors;
	unsigned long read_errors;
	
	time_t last_print_time;
} cow_stats;

static void check_and_print_cow_stats(void)
{
	time_t now = time(NULL);
	
	if (now - cow_stats.last_print_time >= 1) {
		pr_err("[COW_STATS] events: wr=%lu fork=%lu remap=%lu unk=%lu | ops: copied=%lu unprot=%lu woken=%lu | errs: alloc=%lu read=%lu unprot_err=%lu wake_err=%lu read_err=%lu eagain_err=%lu\n",
			cow_stats.write_faults,
			cow_stats.fork_events,
			cow_stats.remap_events,
			cow_stats.unknown_events,
			cow_stats.pages_copied,
			cow_stats.pages_unprotected,
			cow_stats.pages_woken,
			cow_stats.alloc_failures,
			cow_stats.read_failures,
			cow_stats.unprotect_failures,
			cow_stats.wake_failures,
			cow_stats.read_errors,
			cow_stats.eagain_errors);
		
		/* Reset all counters */
		memset(&cow_stats, 0, sizeof(cow_stats));
		cow_stats.last_print_time = now;
	}
}

static struct cow_tracked_task *cow_find_task_by_pid(pid_t source_pid)
{
	struct cow_tracked_task *task;

	if (!g_cow_info)
		return NULL;

	pthread_mutex_lock(&g_tracked_tasks_lock);
	list_for_each_entry(task, &g_cow_info->tracked_tasks, list) {
		if (task->source_pid == source_pid) {
			pthread_mutex_unlock(&g_tracked_tasks_lock);
			return task;
		}
	}
	pthread_mutex_unlock(&g_tracked_tasks_lock);

	return NULL;
}

static bool cow_monitor_is_running(void)
{
	bool running;

	pthread_mutex_lock(&g_monitor_state_lock);
	running = g_monitor_thread_running;
	pthread_mutex_unlock(&g_monitor_state_lock);

	return running;
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

static int cow_wait_monitor_snapshot(unsigned long want_generation)
{
	unsigned long seen;
	int i;

	for (i = 0; i < 1000; i++) {
		pthread_mutex_lock(&g_tracked_tasks_lock);
		seen = g_monitor_snapshot_generation;
		pthread_mutex_unlock(&g_tracked_tasks_lock);

		if (seen >= want_generation)
			return 0;

		usleep(1000);
	}

	return -1;
}

bool cow_check_kernel_support(void)
{
	unsigned long features = UFFD_FEATURE_PAGEFAULT_FLAG_WP;
	int uffd, err = 0;

	uffd = uffd_open(0, &features, &err);
	if (uffd < 0) {
		if (err == ENOSYS) {
			pr_info("userfaultfd not supported by kernel\n");
		} else if (err == EPERM) {
			pr_info("userfaultfd requires CAP_SYS_PTRACE or sysctl vm.unprivileged_userfaultfd=1\n");
		}
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

/*
 * Open a userfaultfd bound to another process's mm_struct via
 * /proc/<pid>/userfaultfd (kernel 6.11+).  Returns the fd with
 * UFFDIO_API already negotiated, or -1 on failure.
 */
static int uffd_open_proc(pid_t pid)
{
	char path[64];
	struct uffdio_api api;
	int fd;

	snprintf(path, sizeof(path), "/proc/%d/userfaultfd", pid);

	fd = open(path, O_RDWR | O_CLOEXEC | O_NONBLOCK);
	if (fd < 0) {
		pr_perror("Cannot open %s", path);
		return -1;
	}

	memset(&api, 0, sizeof(api));
	api.api = UFFD_API;
	api.features = UFFD_FEATURE_PAGEFAULT_FLAG_WP;
	if (kdat.has_wp_async && kdat.has_pagemap_scan)
		api.features |= UFFD_FEATURE_WP_ASYNC;

	if (ioctl(fd, UFFDIO_API, &api)) {
		pr_perror("UFFDIO_API on %s failed", path);
		close(fd);
		return -1;
	}

	if (!(api.features & UFFD_FEATURE_PAGEFAULT_FLAG_WP)) {
		pr_err("userfaultfd from %s lacks WP pagefault flag\n", path);
		close(fd);
		return -1;
	}

	pr_info("Opened %s: fd=%d features=0x%llx\n", path, fd,
		(unsigned long long)api.features);
	return fd;
}

/*
 * Register eligible VMAs for UFFDIO_REGISTER_MODE_WP directly from
 * CRIU's process context (requires uffd from /proc/<pid>/userfaultfd).
 * Populates task->tracked_vmas with the successfully registered VMAs.
 * Returns 0 on success, -1 on fatal error.
 */
static int cow_register_vmas(int uffd, struct cow_tracked_task *task,
			     struct vm_area_list *vma_area_list,
			     unsigned long *out_total_pages)
{
	struct vma_area *vma;
	struct uffdio_register reg;
	unsigned int nr_eligible = 0, nr_tracked = 0, nr_failed = 0;
	unsigned long total_pages = 0;
	struct cow_tracked_vma *tvmas;
	unsigned int i;

	/* First pass: count eligible VMAs */
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

	/* Second pass: register and track */
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

		ret = ioctl(uffd, UFFDIO_REGISTER, &reg);
		if (ret && !g_wp_async_mode) {
			pr_warn("UFFDIO_REGISTER WP %lx-%lx failed: %s\n",
				start, start + len, strerror(errno));
			nr_failed++;
			continue;
		}
		if (ret && g_wp_async_mode) {
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
		task->tracked_vmas = tvmas;
	} else {
		xfree(tvmas);
		task->tracked_vmas = NULL;
	}
	task->nr_tracked_vmas = nr_tracked;
	*out_total_pages = total_pages;

	pr_info("Registered %u/%u VMAs (%u failed) via /proc: %lu pages\n",
		nr_tracked, nr_eligible, nr_failed, total_pages);
	return 0;
}

int cow_dump_init(struct pstree_item *item, struct vm_area_list *vma_area_list, struct parasite_ctl *ctl)
{
	struct cow_dump_info *cdi = g_cow_info;
	struct cow_tracked_task *task = NULL;
	struct parasite_cow_dump_args *args = NULL;
	bool created_session = false;
	int ret;
	unsigned long args_size;
	unsigned int i;
	unsigned long want_generation = 0;

	pr_info("Initializing COW dump for pid %d\n", item->pid->real);

	if (!cdi) {
		if (!cow_check_kernel_support()) {
			pr_err("Kernel doesn't support COW dump\n");
			return -1;
		}

		cdi = xzalloc(sizeof(*cdi));
		if (!cdi)
			return -1;

		INIT_LIST_HEAD(&cdi->tracked_tasks);

		g_wp_async_mode = kdat.has_wp_async && kdat.has_pagemap_scan;
		pr_err("COW mode: wp_async=%d (has_wp_async=%d has_pagemap_scan=%d)\n",
		       g_wp_async_mode, kdat.has_wp_async, kdat.has_pagemap_scan);

		if (!g_wp_async_mode) {
			for (i = 0; i < COW_HASH_SIZE; i++) {
				INIT_HLIST_HEAD(&cdi->cow_hash[i]);
				pthread_spin_init(&cdi->cow_hash_locks[i],
						  PTHREAD_PROCESS_PRIVATE);
			}
			INIT_LIST_HEAD(&cdi->cow_page_queue);
			pthread_spin_init(&cdi->queue_lock,
					  PTHREAD_PROCESS_PRIVATE);
		}

		g_cow_info = cdi;
		created_session = true;

		if (!g_wp_async_mode) {
			pthread_mutex_lock(&g_tracked_tasks_lock);
			g_tracked_tasks_generation = 0;
			g_monitor_snapshot_generation = 0;
			pthread_mutex_unlock(&g_tracked_tasks_lock);

			if (g_monitor_eventfd >= 0) {
				close(g_monitor_eventfd);
				g_monitor_eventfd = -1;
			}
			g_monitor_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
			if (g_monitor_eventfd < 0) {
				pr_perror("Failed to create cow monitor eventfd");
				goto err;
			}
		}
	}

	if (cow_find_task_by_pid(item->pid->real)) {
		pr_warn("COW tracking already initialized for pid %d, skipping\n", item->pid->real);
		return 0;
	}

	task = xzalloc(sizeof(*task));
	if (!task)
		goto err;

	INIT_LIST_HEAD(&task->list);
	task->source_pid = item->pid->real;
	task->uffd = -1;
	task->pagemap_fd = -1;

	if (kdat.has_uffd_proc) {
		/*
		 * Fast path: open userfaultfd via /proc/<pid>/userfaultfd
		 * and register VMAs directly from CRIU, no parasite RPC.
		 */
		pr_info("Using /proc/%d/userfaultfd (direct path)\n",
			item->pid->real);

		task->uffd = uffd_open_proc(item->pid->real);
		if (task->uffd < 0)
			goto err;
	} else {
		/*
		 * Use parasite only to create the userfaultfd and
		 * negotiate UFFDIO_API inside the target process.
		 * Pass nr_vmas=0 so the parasite skips VMA registration;
		 * we do that from CRIU below since UFFDIO_REGISTER
		 * operates on the uffd's mm_struct, not current->mm.
		 */
		args_size = sizeof(*args);
		args = compel_parasite_args_s(ctl, args_size);
		if (!args) {
			pr_err("Failed to allocate parasite args\n");
			goto err;
		}

		args->nr_vmas = 0;
		args->total_pages = 0;
		args->nr_failed_vmas = 0;
		args->ret = -1;

		ret = compel_rpc_call(PARASITE_CMD_COW_DUMP_INIT, ctl);
		if (ret < 0) {
			pr_err("Failed to initiate COW dump RPC\n");
			goto err;
		}

		compel_util_recv_fd(ctl, &task->uffd);
		if (task->uffd < 0) {
			pr_err("Failed to receive uffd from parasite: %d\n",
			       task->uffd);
			goto err;
		}

		ret = compel_rpc_sync(PARASITE_CMD_COW_DUMP_INIT, ctl);
		if (ret < 0 || args->ret != 0) {
			pr_err("Parasite COW dump init failed: %d (ret=%d)\n",
			       ret, args->ret);
			goto err;
		}

		pr_err("Parasite uffd features: 0x%llx (WP_ASYNC=%s)\n",
		       args->uffd_features,
		       (args->uffd_features & UFFD_FEATURE_WP_ASYNC) ?
		       "YES" : "NO");

		/* Override WP_ASYNC mode based on actual features */
		if (g_wp_async_mode &&
		    !(args->uffd_features & UFFD_FEATURE_WP_ASYNC)) {
			pr_err("WP_ASYNC not granted by kernel, falling back to sync\n");
			g_wp_async_mode = false;
		}
	}

	/*
	 * Register VMAs for write-protect tracking directly from CRIU.
	 * UFFDIO_REGISTER operates on the uffd's associated mm_struct
	 * (set during userfaultfd() syscall in the target), so this
	 * works from any process holding the fd.
	 */
	ret = cow_register_vmas(task->uffd, task, vma_area_list,
				&task->total_pages);
	if (ret)
		goto err;

	/*
	 * Write-protect is now async: cow_dump_start_wp() launches
	 * the WP threads, cow_dump_finish_wp() joins them.  This
	 * allows overlapping WP with other dump work.
	 */

	if (g_wp_async_mode) {
		task->pagemap_fd = open_proc(item->pid->real, "pagemap");
		if (task->pagemap_fd < 0) {
			pr_warn("Cannot open /proc/%d/pagemap, falling back to sync WP\n",
				item->pid->real);
			g_wp_async_mode = false;
		}
	}

	pthread_mutex_lock(&g_tracked_tasks_lock);
	list_add_tail(&task->list, &cdi->tracked_tasks);
	if (!g_wp_async_mode) {
		g_tracked_tasks_generation++;
		want_generation = g_tracked_tasks_generation;
	}
	pthread_mutex_unlock(&g_tracked_tasks_lock);
	cdi->total_pages += task->total_pages;

	if (!g_wp_async_mode && cow_monitor_is_running()) {
		cow_monitor_wakeup();
		if (cow_wait_monitor_snapshot(want_generation))
			pr_warn("Timed out waiting for monitor to pick up pid %d\n",
				item->pid->real);
	}

	pr_info("COW dump initialized for pid %d: tracked=%u pages=%lu uffd=%d wp_async=%d\n",
		item->pid->real, task->nr_tracked_vmas,
		task->total_pages, task->uffd, g_wp_async_mode);

	return 0;

err:
	if (task) {
		if (task->pagemap_fd >= 0)
			close(task->pagemap_fd);
		if (task->uffd >= 0)
			close(task->uffd);
		xfree(task->tracked_vmas);
		xfree(task);
	}

	if (created_session) {
		for (i = 0; i < COW_HASH_SIZE; i++)
			pthread_spin_destroy(&cdi->cow_hash_locks[i]);
		pthread_spin_destroy(&cdi->queue_lock);
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
	struct cow_page *cp;
	struct cow_page_queue_entry *qe, *qe_tmp;
	struct cow_tracked_task *task, *task_tmp;
	struct hlist_node *n;
	int i, remaining = 0, queue_remaining = 0;

	if (!g_cow_info)
		return;

	if (!g_wp_async_mode) {
		if (cow_stop_monitor_thread()) {
			pr_err("Failed to stop COW monitor thread, skipping COW cleanup to avoid races\n");
			return;
		}
	}

	pr_info("Cleaning up COW dump (wp_async=%d)\n", g_wp_async_mode);

	if (g_monitor_eventfd >= 0) {
		close(g_monitor_eventfd);
		g_monitor_eventfd = -1;
	}

	if (!g_wp_async_mode) {
		pthread_mutex_lock(&g_tracked_tasks_lock);
		g_tracked_tasks_generation = 0;
		g_monitor_snapshot_generation = 0;
		pthread_mutex_unlock(&g_tracked_tasks_lock);

		/* Clean up any remaining queue entries */
		pthread_spin_lock(&g_cow_info->queue_lock);
		list_for_each_entry_safe(qe, qe_tmp,
					 &g_cow_info->cow_page_queue, list) {
			list_del(&qe->list);
			xfree(qe);
			queue_remaining++;
		}
		pthread_spin_unlock(&g_cow_info->queue_lock);
		pthread_spin_destroy(&g_cow_info->queue_lock);

		if (queue_remaining > 0)
			pr_warn("Freed %d remaining queue entries\n",
				queue_remaining);

		/* Clean up any remaining COW pages */
		for (i = 0; i < COW_HASH_SIZE; i++) {
			pthread_spin_lock(&g_cow_info->cow_hash_locks[i]);
			hlist_for_each_entry_safe(cp, n,
						  &g_cow_info->cow_hash[i],
						  hash) {
				hlist_del(&cp->hash);
				xfree(cp->data);
				xfree(cp);
				remaining++;
			}
			pthread_spin_unlock(&g_cow_info->cow_hash_locks[i]);
			pthread_spin_destroy(&g_cow_info->cow_hash_locks[i]);
		}

		if (remaining > 0)
			pr_warn("Freed %d remaining COW pages\n", remaining);
	}

	list_for_each_entry_safe(task, task_tmp, &g_cow_info->tracked_tasks, list) {
		list_del(&task->list);
		if (task->pagemap_fd >= 0)
			close(task->pagemap_fd);
		if (task->uffd >= 0)
			close(task->uffd);
		xfree(task->tracked_vmas);
		xfree(task);
	}

	xfree(g_cow_info);
	g_cow_info = NULL;
	g_wp_async_mode = false;
}

/*
 * Snapshot a single page into the COW hash without clearing WP or
 * waking the faulting thread.  Returns 0 on success, -1 on error.
 */
static int cow_snapshot_page(struct cow_dump_info *cdi,
			     struct cow_tracked_task *task,
			     unsigned long page_addr)
{
	struct cow_page *cp;
	unsigned int hash;
	struct iovec local_iov, remote_iov;
	ssize_t ret;
	struct cow_page_queue_entry *entry;

	cp = xmalloc(sizeof(*cp));
	if (!cp) {
		cow_stats.alloc_failures++;
		return -1;
	}
	cp->data = xmalloc(PAGE_SIZE);
	if (!cp->data) {
		xfree(cp);
		cow_stats.alloc_failures++;
		return -1;
	}

	cp->vaddr = page_addr;
	INIT_HLIST_NODE(&cp->hash);

	local_iov.iov_base = cp->data;
	local_iov.iov_len = PAGE_SIZE;
	remote_iov.iov_base = (void *)page_addr;
	remote_iov.iov_len = PAGE_SIZE;

	ret = process_vm_readv(task->source_pid, &local_iov, 1,
			       &remote_iov, 1, 0);
	if (ret != PAGE_SIZE) {
		xfree(cp->data);
		xfree(cp);
		cow_stats.read_failures++;
		return -1;
	}

	hash = (page_addr >> PAGE_SHIFT) & (COW_HASH_SIZE - 1);
	pthread_spin_lock(&cdi->cow_hash_locks[hash]);
	hlist_add_head(&cp->hash, &cdi->cow_hash[hash]);
	pthread_spin_unlock(&cdi->cow_hash_locks[hash]);

	cow_stats.pages_copied++;

	entry = xmalloc(sizeof(*entry));
	if (entry) {
		entry->vaddr = page_addr;
		entry->ppb = NULL;  /* Indicates lazy VMA - no location info needed */
		entry->seg_idx = 0;
		entry->page_idx_in_seg = 0;
		INIT_LIST_HEAD(&entry->list);
		pthread_spin_lock(&cdi->queue_lock);
		list_add_tail(&entry->list, &cdi->cow_page_queue);
		pthread_spin_unlock(&cdi->queue_lock);
		pr_debug("Added lazy VMA COW page 0x%lx to queue\n", page_addr);
	} else {
		pr_warn("Failed to allocate queue entry for page 0x%lx\n", page_addr);
	}
	cow_stats.pages_woken++;
	cdi->total_pages--;
	return 0;
}

static int addr_cmp(const void *a, const void *b)
{
	unsigned long va = *(const unsigned long *)a;
	unsigned long vb = *(const unsigned long *)b;

	return (va > vb) - (va < vb);
}

/*
 * Maximum number of faults to batch before flushing.  Larger batches
 * amortize TLB shootdown cost better but increase the time each
 * faulting thread waits.
 */
#define COW_FAULT_BATCH_MAX 256

/*
 * Parallel snapshot workers.  The monitor reads events from the uffd
 * (single-threaded, fast) and dispatches page snapshots to a pool of
 * workers that call process_vm_readv in parallel.  This parallelizes
 * the expensive per-fault snapshot (~5us each) across multiple cores.
 */
#define COW_SNAPSHOT_WORKERS 8

struct cow_snapshot_work {
	struct cow_dump_info *cdi;
	struct cow_tracked_task *task;
	unsigned long addr;
	int result;  /* 0 = success, -1 = error */
};

struct cow_snapshot_pool {
	pthread_t threads[COW_SNAPSHOT_WORKERS];
	int nr_workers;

	/* Work queue: producer (monitor) → consumers (workers) */
	struct cow_snapshot_work *queue;
	int queue_cap;
	int queue_head;   /* next slot to consume */
	int queue_tail;   /* next slot to produce */
	int queue_count;  /* items in queue */
	pthread_mutex_t lock;
	pthread_cond_t work_avail;  /* signal workers */
	pthread_cond_t work_done;   /* signal monitor */

	int pending;      /* snapshots dispatched but not completed */
	volatile bool stop;
};

static struct cow_snapshot_pool *g_snapshot_pool;

static void *cow_snapshot_worker(void *arg)
{
	struct cow_snapshot_pool *pool = arg;
	char name[16];
	int id;

	pthread_mutex_lock(&pool->lock);
	id = pool->nr_workers;  /* approximate */
	pthread_mutex_unlock(&pool->lock);

	snprintf(name, sizeof(name), "cow-snap-%d", id);
	pthread_setname_np(pthread_self(), name);

	while (1) {
		struct cow_snapshot_work work;

		pthread_mutex_lock(&pool->lock);
		while (pool->queue_count == 0 && !pool->stop)
			pthread_cond_wait(&pool->work_avail, &pool->lock);

		if (pool->stop && pool->queue_count == 0) {
			pthread_mutex_unlock(&pool->lock);
			break;
		}

		work = pool->queue[pool->queue_head];
		pool->queue_head = (pool->queue_head + 1) % pool->queue_cap;
		pool->queue_count--;
		pthread_mutex_unlock(&pool->lock);

		/* Do the actual snapshot (the expensive part) */
		work.result = cow_snapshot_page(work.cdi, work.task,
						work.addr);

		pthread_mutex_lock(&pool->lock);
		pool->pending--;
		pthread_cond_signal(&pool->work_done);
		pthread_mutex_unlock(&pool->lock);
	}

	return NULL;
}

static struct cow_snapshot_pool *cow_snapshot_pool_create(void)
{
	struct cow_snapshot_pool *pool;
	int i;

	pool = xzalloc(sizeof(*pool));
	if (!pool)
		return NULL;

	pool->queue_cap = COW_FAULT_BATCH_MAX * 4;
	pool->queue = xmalloc(pool->queue_cap * sizeof(*pool->queue));
	if (!pool->queue) {
		xfree(pool);
		return NULL;
	}

	pthread_mutex_init(&pool->lock, NULL);
	pthread_cond_init(&pool->work_avail, NULL);
	pthread_cond_init(&pool->work_done, NULL);

	for (i = 0; i < COW_SNAPSHOT_WORKERS; i++) {
		if (pthread_create(&pool->threads[i], NULL,
				   cow_snapshot_worker, pool)) {
			pr_warn("Created %d/%d snapshot workers\n",
				i, COW_SNAPSHOT_WORKERS);
			break;
		}
		pool->nr_workers++;
	}

	pr_info("Created snapshot pool with %d workers\n",
		pool->nr_workers);
	return pool;
}

static void cow_snapshot_pool_destroy(struct cow_snapshot_pool *pool)
{
	int i;

	if (!pool)
		return;

	pthread_mutex_lock(&pool->lock);
	pool->stop = true;
	pthread_cond_broadcast(&pool->work_avail);
	pthread_mutex_unlock(&pool->lock);

	for (i = 0; i < pool->nr_workers; i++)
		pthread_join(pool->threads[i], NULL);

	pthread_mutex_destroy(&pool->lock);
	pthread_cond_destroy(&pool->work_avail);
	pthread_cond_destroy(&pool->work_done);
	xfree(pool->queue);
	xfree(pool);
}

/*
 * Submit a page snapshot to the worker pool.  Returns immediately.
 * The monitor must call cow_snapshot_pool_drain() to wait for
 * all pending snapshots before flushing the WP batch.
 */
static void cow_snapshot_pool_submit(struct cow_snapshot_pool *pool,
				     struct cow_dump_info *cdi,
				     struct cow_tracked_task *task,
				     unsigned long addr)
{
	struct cow_snapshot_work work = {
		.cdi = cdi,
		.task = task,
		.addr = addr,
		.result = 0,
	};

	pthread_mutex_lock(&pool->lock);

	/* Wait if queue is full */
	while (pool->queue_count >= pool->queue_cap)
		pthread_cond_wait(&pool->work_done, &pool->lock);

	pool->queue[pool->queue_tail] = work;
	pool->queue_tail = (pool->queue_tail + 1) % pool->queue_cap;
	pool->queue_count++;
	pool->pending++;

	pthread_cond_signal(&pool->work_avail);
	pthread_mutex_unlock(&pool->lock);
}

/* Wait for all pending snapshots to complete */
static void cow_snapshot_pool_drain(struct cow_snapshot_pool *pool)
{
	pthread_mutex_lock(&pool->lock);
	while (pool->pending > 0)
		pthread_cond_wait(&pool->work_done, &pool->lock);
	pthread_mutex_unlock(&pool->lock);
}

/*
 * Flush a batch of snapshotted fault addresses: merge contiguous
 * ranges, issue one UFFDIO_WRITEPROTECT per range, then wake all
 * faulting threads.
 */
static int cow_flush_fault_batch(struct cow_tracked_task *task,
				 unsigned long *addrs, int count)
{
	struct uffdio_writeprotect wp;
	struct uffdio_range wake;
	int i, start;

	if (count == 0)
		return 0;

	qsort(addrs, count, sizeof(addrs[0]), addr_cmp);

	/* Merge contiguous pages into ranges and clear WP per range */
	start = 0;
	for (i = 1; i <= count; i++) {
		if (i < count &&
		    addrs[i] == addrs[i - 1] + PAGE_SIZE)
			continue;

		/* Range [start..i) is contiguous */
		wp.range.start = addrs[start];
		wp.range.len = (addrs[i - 1] - addrs[start]) + PAGE_SIZE;
		wp.mode = 0;

		if (ioctl(task->uffd, UFFDIO_WRITEPROTECT, &wp))
			pr_pwarn("Batch unprotect [%lx +%lu] failed",
				 (unsigned long)wp.range.start,
				 (unsigned long)(wp.range.len / PAGE_SIZE));
		else
			cow_stats.pages_unprotected +=
				wp.range.len / PAGE_SIZE;

		start = i;
	}

	/* Wake each faulting thread individually */
	for (i = 0; i < count; i++) {
		wake.start = addrs[i];
		wake.len = PAGE_SIZE;
		if (ioctl(task->uffd, UFFDIO_WAKE, &wake) == 0)
			cow_stats.pages_woken++;
	}

	return 0;
}

static int cow_process_events(struct cow_dump_info *cdi,
			      struct cow_tracked_task *task,
			      bool blocking)
{
	struct uffd_msg msg;
	struct pollfd pfd;
	int ret, poll_ret;
	unsigned long batch[COW_FAULT_BATCH_MAX];
	int batch_count = 0;

	while (1) {
		check_and_print_cow_stats();

		ret = read(task->uffd, &msg, sizeof(msg));

		if (ret < 0 && errno == EAGAIN) {
			/*
			 * No more events available.  Flush any
			 * accumulated batch before blocking or
			 * returning.
			 */
			if (batch_count > 0) {
				if (g_snapshot_pool)
					cow_snapshot_pool_drain(
						g_snapshot_pool);
				cow_flush_fault_batch(task, batch,
						      batch_count);
				batch_count = 0;
			}

			if (!blocking)
				return 0;

			pfd.fd = task->uffd;
			pfd.events = POLLIN;
			pfd.revents = 0;

			poll_ret = poll(&pfd, 1, 500);
			if (poll_ret < 0) {
				pr_perror("poll() failed on uffd");
				cow_stats.read_errors++;
				return -1;
			}
			if (poll_ret == 0)
				return 0;

			ret = read(task->uffd, &msg, sizeof(msg));
		}

		if (ret < 0) {
			pr_perror("Failed to read uffd event");
			cow_stats.read_errors++;
			return -1;
		}
		if (ret != sizeof(msg)) {
			pr_err("Short read from uffd: %d\n", ret);
			cow_stats.read_errors++;
			return -1;
		}

		switch (msg.event) {
		case UFFD_EVENT_PAGEFAULT:
			if (msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WP) {
				unsigned long pa = msg.arg.pagefault.address &
						  ~(PAGE_SIZE - 1);

				cow_stats.write_faults++;

				if (g_snapshot_pool &&
				    g_snapshot_pool->nr_workers > 0) {
					cow_snapshot_pool_submit(
						g_snapshot_pool,
						cdi, task, pa);
				} else {
					if (cow_snapshot_page(cdi, task, pa))
						return -1;
				}

				batch[batch_count++] = pa;

				if (batch_count >= COW_FAULT_BATCH_MAX) {
					if (g_snapshot_pool)
						cow_snapshot_pool_drain(
							g_snapshot_pool);
					cow_flush_fault_batch(task, batch,
							      batch_count);
					batch_count = 0;
				}
			}
			break;

		case UFFD_EVENT_FORK:
			cow_stats.fork_events++;
			pr_warn("Process forked during COW dump (not fully supported)\n");
			break;

		case UFFD_EVENT_REMAP:
			cow_stats.remap_events++;
			pr_info("Memory remap event\n");
			break;

		default:
			cow_stats.unknown_events++;
			pr_err("Unexpected uffd event: %u\n", msg.event);
			return -1;
		}
	}

	return 0;
}

static int cow_wait_for_events(struct cow_dump_info *cdi, int timeout_ms)
{
	struct cow_tracked_task *task;
	struct pollfd *pfds;
	struct cow_tracked_task **tasks;
	int nr_tasks = 0;
	int idx = 0;
	int ret;
	int i;

	pthread_mutex_lock(&g_tracked_tasks_lock);
	list_for_each_entry(task, &cdi->tracked_tasks, list)
		nr_tasks++;

	g_monitor_snapshot_generation = g_tracked_tasks_generation;

	if (!nr_tasks) {
		pthread_mutex_unlock(&g_tracked_tasks_lock);
		return 0;
	}

	pfds = xmalloc(sizeof(*pfds) * (nr_tasks + 1));
	tasks = xmalloc(sizeof(*tasks) * nr_tasks);
	if (!pfds || !tasks) {
		pthread_mutex_unlock(&g_tracked_tasks_lock);
		xfree(pfds);
		xfree(tasks);
		return -1;
	}

	pfds[0].fd = g_monitor_eventfd;
	pfds[0].events = POLLIN;
	pfds[0].revents = 0;
	idx = 1;
	i = 0;
	list_for_each_entry(task, &cdi->tracked_tasks, list) {
		tasks[i++] = task;
		pfds[idx].fd = task->uffd;
		pfds[idx].events = POLLIN;
		pfds[idx].revents = 0;
		idx++;
	}
	pthread_mutex_unlock(&g_tracked_tasks_lock);

	ret = poll(pfds, nr_tasks + 1, timeout_ms);
	if (ret < 0)
		pr_perror("poll() failed on uffd set");
	if (ret <= 0)
		goto out;

	if (pfds[0].revents & POLLIN) {
		cow_monitor_drain_eventfd();
		goto out;
	}

	for (i = 0; i < nr_tasks; i++) {
		if (!(pfds[i + 1].revents & (POLLIN | POLLERR | POLLHUP | POLLNVAL)))
			continue;
		if (cow_process_events(cdi, tasks[i], false) < 0) {
			pr_err("Error processing COW events for pid %d\n",
			       tasks[i]->source_pid);
			ret = -1;
			break;
		}
	}

out:
	xfree(tasks);
	xfree(pfds);
	return ret;
}

/* Background thread that monitors for write faults */
static void *cow_monitor_thread(void *arg)
{
	struct cow_dump_info *cdi = (struct cow_dump_info *)arg;
	bool monitor_error = false;

	pthread_setname_np(pthread_self(), "criu-cow-mon");
	pr_info("COW monitor thread started\n");

	while (!g_stop_monitoring) {
		int ret;

		ret = cow_wait_for_events(cdi, 500);
		if (ret < 0) {
			monitor_error = true;
			break;
		}
	}
	
	if (monitor_error)
		pr_err("COW monitor thread exiting on event-processing error\n");

	pr_info("COW monitor thread stopped\n");
	return NULL;
}

int cow_start_monitor_thread(void)
{
	int ret;
	bool no_tasks;
	
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

	pthread_mutex_lock(&g_tracked_tasks_lock);
	no_tasks = list_empty(&g_cow_info->tracked_tasks);
	pthread_mutex_unlock(&g_tracked_tasks_lock);
	if (no_tasks) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("COW tracking has no registered tasks\n");
		return -1;
	}

	if (g_monitor_thread_running) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		return 0;
	}
	
	g_stop_monitoring = false;

	/* Create parallel snapshot worker pool */
	if (!g_snapshot_pool) {
		g_snapshot_pool = cow_snapshot_pool_create();
		if (!g_snapshot_pool)
			pr_warn("Failed to create snapshot pool, falling back to single-threaded\n");
	}

	ret = pthread_create(&g_monitor_thread, NULL, cow_monitor_thread, g_cow_info);
	if (ret) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("Failed to create COW monitor thread: %s\n", strerror(ret));
		return -1;
	}

	g_monitor_thread_running = true;
	pthread_mutex_unlock(&g_monitor_state_lock);
	
	pr_info("COW monitor thread created successfully\n");
	return 0;
}

int cow_stop_monitor_thread(void)
{
	void *retval;
	pthread_t monitor_thread;
	int ret;
	
	pthread_mutex_lock(&g_monitor_state_lock);
	if (!g_monitor_thread_running) {
		g_stop_monitoring = false;
		pthread_mutex_unlock(&g_monitor_state_lock);
		return 0;
	}
	
	pr_info("Stopping COW monitor thread\n");
	g_stop_monitoring = true;
	monitor_thread = g_monitor_thread;
	pthread_mutex_unlock(&g_monitor_state_lock);

	cow_monitor_wakeup();
	
	/* Wait for thread to finish */
	ret = pthread_join(monitor_thread, &retval);
	if (ret && ret != ESRCH && ret != EINVAL) {
		pr_err("Failed to join COW monitor thread: %s\n", strerror(ret));
		return -1;
	}

	if (g_snapshot_pool) {
		cow_snapshot_pool_destroy(g_snapshot_pool);
		g_snapshot_pool = NULL;
	}

	pthread_mutex_lock(&g_monitor_state_lock);
	g_monitor_thread_running = false;
	g_stop_monitoring = false;
	pthread_mutex_unlock(&g_monitor_state_lock);

	pr_info("COW monitor thread stopped successfully\n");
	return 0;
}

int cow_get_uffd(void)
{
	struct cow_tracked_task *task;
	int uffd;

	if (!g_cow_info)
		return -1;

	pthread_mutex_lock(&g_tracked_tasks_lock);
	if (list_empty(&g_cow_info->tracked_tasks)) {
		pthread_mutex_unlock(&g_tracked_tasks_lock);
		return -1;
	}
	task = list_first_entry(&g_cow_info->tracked_tasks, struct cow_tracked_task, list);
	uffd = task->uffd;
	pthread_mutex_unlock(&g_tracked_tasks_lock);

	return uffd;
}

int cow_get_uffd_for_pid(pid_t source_pid)
{
	struct cow_tracked_task *task;

	task = cow_find_task_by_pid(source_pid);
	if (!task)
		return -1;

	return task->uffd;
}

bool cow_is_wp_async(void)
{
	return g_wp_async_mode;
}

int cow_get_pagemap_fd_for_pid(pid_t source_pid)
{
	struct cow_tracked_task *task;

	task = cow_find_task_by_pid(source_pid);
	if (!task)
		return -1;

	return task->pagemap_fd;
}

int cow_scan_dirty_pages(pid_t source_pid,
			 unsigned long start, unsigned long end,
			 void *regions, unsigned long max_regions,
			 unsigned long *walk_end)
{
	struct cow_tracked_task *task;
	struct pm_scan_arg arg;
	int ret;

	if (!g_wp_async_mode)
		return 0;

	task = cow_find_task_by_pid(source_pid);
	if (!task || task->pagemap_fd < 0)
		return -1;

	memset(&arg, 0, sizeof(arg));
	arg.size = sizeof(arg);
	arg.flags = PM_SCAN_WP_MATCHING;
	arg.start = start;
	arg.end = end;
	arg.vec = (u64)(unsigned long)regions;
	arg.vec_len = max_regions;
	arg.max_pages = 0;
	arg.category_anyof_mask = PAGE_IS_WRITTEN;
	arg.return_mask = PAGE_IS_WRITTEN;

	ret = ioctl(task->pagemap_fd, PAGEMAP_SCAN, &arg);
	if (ret < 0) {
		pr_perror("PAGEMAP_SCAN [%lx-%lx) failed", start, end);
		return -1;
	}

	if (walk_end)
		*walk_end = arg.walk_end;

	return ret;
}

bool cow_dump_is_vma_tracked(pid_t source_pid, unsigned long start, unsigned long end)
{
	struct cow_tracked_task *task;
	unsigned int i;

	task = cow_find_task_by_pid(source_pid);
	if (!task)
		return false;

	for (i = 0; i < task->nr_tracked_vmas; i++) {
		if (task->tracked_vmas[i].start == start &&
		    task->tracked_vmas[i].end == end)
			return true;
	}

	return false;
}

pthread_spinlock_t *cow_get_hash_lock(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	unsigned int hash;

	if (!g_cow_info || g_wp_async_mode)
		return NULL;

	hash = (page_addr >> PAGE_SHIFT) & (COW_HASH_SIZE - 1);
	return &g_cow_info->cow_hash_locks[hash];
}

struct cow_page *cow_lookup_page(unsigned long vaddr)
{
	struct cow_page *cp;
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	unsigned int hash;

	if (!g_cow_info)
		return NULL;

	hash = (page_addr >> PAGE_SHIFT) & (COW_HASH_SIZE - 1);

	/* NOTE: Caller must hold the lock for this hash bucket */
	hlist_for_each_entry(cp, &g_cow_info->cow_hash[hash], hash) {
		if (cp->vaddr == page_addr)
			return cp;
	}

	return NULL;
}

void cow_remove_page(unsigned long vaddr)
{
	struct cow_page *cp;
	struct hlist_node *n;
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	unsigned int hash;

	if (!g_cow_info)
		return;

	hash = (page_addr >> PAGE_SHIFT) & (COW_HASH_SIZE - 1);

	/* NOTE: Caller must hold the lock for this hash bucket */
	hlist_for_each_entry_safe(cp, n, &g_cow_info->cow_hash[hash], hash) {
		if (cp->vaddr == page_addr) {
			hlist_del(&cp->hash);
			xfree(cp->data);
			xfree(cp);
			pr_debug("Removed COW page at 0x%lx from hash bucket %u\n",
				 page_addr, hash);
			return;
		}
	}
}

struct cow_page *cow_lookup_and_remove_page(unsigned long vaddr)
{
	struct cow_page *cp;
	struct hlist_node *n;
	unsigned int hash;
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);

	if (!g_cow_info)
		return NULL;

	hash = (page_addr >> PAGE_SHIFT) & (COW_HASH_SIZE - 1);

	pthread_spin_lock(&g_cow_info->cow_hash_locks[hash]);
	
	hlist_for_each_entry_safe(cp, n, &g_cow_info->cow_hash[hash], hash) {
		if (cp->vaddr == page_addr) {
			hlist_del(&cp->hash);
			pthread_spin_unlock(&g_cow_info->cow_hash_locks[hash]);
			pr_debug("Found and removed COW page at 0x%lx from hash bucket %u\n", 
				 page_addr, hash);
			return cp;
		}
	}
	
	pthread_spin_unlock(&g_cow_info->cow_hash_locks[hash]);
	return NULL;
}

struct cow_page_queue_entry *cow_get_next_page(void)
{
	struct cow_page_queue_entry *entry = NULL;

	if (!g_cow_info || g_wp_async_mode)
		return NULL;

	pthread_spin_lock(&g_cow_info->queue_lock);
	if (!list_empty(&g_cow_info->cow_page_queue)) {
		entry = list_first_entry(&g_cow_info->cow_page_queue,
					 struct cow_page_queue_entry, list);
		list_del(&entry->list);
	}
	pthread_spin_unlock(&g_cow_info->queue_lock);

	return entry;
}

bool cow_has_pending_pages(void)
{
	bool has_pages;

	if (!g_cow_info || g_wp_async_mode)
		return false;

	pthread_spin_lock(&g_cow_info->queue_lock);
	has_pages = !list_empty(&g_cow_info->cow_page_queue);
	pthread_spin_unlock(&g_cow_info->queue_lock);

	return has_pages;
}

void cow_put_back_page(struct cow_page_queue_entry *entry)
{
	if (!g_cow_info || !entry || g_wp_async_mode)
		return;

	pthread_spin_lock(&g_cow_info->queue_lock);
	list_add(&entry->list, &g_cow_info->cow_page_queue);
	pthread_spin_unlock(&g_cow_info->queue_lock);

	pr_debug("Re-queued COW page 0x%lx\n", entry->vaddr);
}

unsigned long cow_get_queue_size(void)
{
	unsigned long count = 0;
	struct cow_page_queue_entry *entry;

	if (!g_cow_info || g_wp_async_mode)
		return 0;

	pthread_spin_lock(&g_cow_info->queue_lock);
	list_for_each_entry(entry, &g_cow_info->cow_page_queue, list) {
		count++;
	}
	pthread_spin_unlock(&g_cow_info->queue_lock);

	return count;
}
