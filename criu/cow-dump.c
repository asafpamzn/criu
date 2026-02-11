/*
 * COW dump with WP_ASYNC — near-zero write latency on the source.
 *
 * The kernel auto-resolves write faults on WP'd pages (~1-2us per write).
 * No monitor thread, no hash table, no per-page userspace round-trip.
 * Dirty tracking is done via PAGEMAP_SCAN with PM_SCAN_WP_MATCHING.
 */
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
#include <string.h>

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
#include "criu-log.h"
#include "parasite.h"
#include "pagemap_scan.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-dump: "

struct cow_tracked_vma {
	unsigned long start;
	unsigned long end;
};

struct cow_tracked_task {
	pid_t source_pid;
	int uffd;
	int pagemap_fd;		/* cached /proc/pid/pagemap fd for PAGEMAP_SCAN */
	unsigned long total_pages;
	unsigned int nr_tracked_vmas;
	struct cow_tracked_vma *tracked_vmas;
	struct list_head list;
};

struct cow_dump_info {
	struct list_head tracked_tasks;
	unsigned long total_pages;
};

static struct cow_dump_info *g_cow_info;

static struct cow_tracked_task *cow_find_task_by_pid(pid_t source_pid)
{
	struct cow_tracked_task *task;

	if (!g_cow_info)
		return NULL;

	list_for_each_entry(task, &g_cow_info->tracked_tasks, list) {
		if (task->source_pid == source_pid)
			return task;
	}

	return NULL;
}

bool cow_check_kernel_support(void)
{
	unsigned long features = UFFD_FEATURE_WP_ASYNC |
				 UFFD_FEATURE_PAGEFAULT_FLAG_WP;
	int uffd, err = 0;

	uffd = uffd_open(0, &features, &err);
	if (uffd < 0) {
		if (err == ENOSYS)
			pr_info("userfaultfd not supported by kernel\n");
		else if (err == EPERM)
			pr_info("userfaultfd requires CAP_SYS_PTRACE or sysctl vm.unprivileged_userfaultfd=1\n");
		return false;
	}

	if (!(features & UFFD_FEATURE_WP_ASYNC)) {
		pr_info("userfaultfd WP_ASYNC not supported (need kernel 6.1+)\n");
		close(uffd);
		return false;
	}

	close(uffd);
	pr_info("COW dump kernel support detected (WP_ASYNC)\n");
	return true;
}

int cow_dump_init(struct pstree_item *item,
		  struct vm_area_list *vma_area_list,
		  struct parasite_ctl *ctl)
{
	struct cow_dump_info *cdi = g_cow_info;
	struct cow_tracked_task *task = NULL;
	struct vma_area *vma;
	struct parasite_cow_dump_args *args;
	struct parasite_vma_entry *p_vma;
	unsigned int *failed_indices;
	bool created_session = false;
	int ret;
	unsigned long args_size;
	unsigned int nr_vmas = 0;
	unsigned int tracked_vmas = 0;
	unsigned int fallback_vmas = 0;
	bool *failed_map = NULL;
	unsigned int i;

	pr_info("Initializing COW dump for pid %d (WP_ASYNC mode)\n",
		item->pid->real);

	if (!cdi) {
		if (!cow_check_kernel_support()) {
			pr_err("Kernel doesn't support COW dump\n");
			return -1;
		}

		cdi = xzalloc(sizeof(*cdi));
		if (!cdi)
			return -1;

		INIT_LIST_HEAD(&cdi->tracked_tasks);
		g_cow_info = cdi;
		created_session = true;
	}

	if (cow_find_task_by_pid(item->pid->real)) {
		pr_warn("COW tracking already initialized for pid %d\n",
			item->pid->real);
		return 0;
	}

	task = xzalloc(sizeof(*task));
	if (!task)
		goto err;

	INIT_LIST_HEAD(&task->list);
	task->source_pid = item->pid->real;
	task->uffd = -1;
	task->pagemap_fd = -1;

	/* Count writable VMAs matching generate_vma_iovs() filters */
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
		nr_vmas++;
	}

	args_size = sizeof(*args) +
		    nr_vmas * sizeof(struct parasite_vma_entry) +
		    nr_vmas * sizeof(unsigned int);
	args = compel_parasite_args_s(ctl, args_size);
	if (!args) {
		pr_err("Failed to allocate parasite args\n");
		goto err;
	}

	args->nr_vmas = nr_vmas;
	args->total_pages = 0;
	args->nr_failed_vmas = 0;
	args->ret = -1;

	/* Fill VMA entries */
	p_vma = cow_dump_vmas(args);
	nr_vmas = 0;
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

		p_vma[nr_vmas].start = vma->e->start;
		p_vma[nr_vmas].len = vma->e->end - vma->e->start;
		p_vma[nr_vmas].prot = vma->e->prot;
		nr_vmas++;
	}

	pr_info("Calling parasite to register %u VMAs\n", args->nr_vmas);

	ret = compel_rpc_call(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0) {
		pr_err("Failed to initiate COW dump RPC\n");
		goto err;
	}

	compel_util_recv_fd(ctl, &task->uffd);
	if (task->uffd < 0) {
		pr_err("Failed to receive userfaultfd from parasite: %d\n",
		       task->uffd);
		goto err;
	}

	ret = compel_rpc_sync(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0 || args->ret != 0) {
		pr_err("Parasite COW dump init failed: %d (ret=%d)\n",
		       ret, args->ret);
		goto err;
	}

	task->total_pages = args->total_pages;

	/* Cache pagemap fd for PAGEMAP_SCAN — avoids open/close per scan */
	{
		char path[64];

		snprintf(path, sizeof(path), "/proc/%d/pagemap",
			 task->source_pid);
		task->pagemap_fd = open(path, O_RDONLY);
		if (task->pagemap_fd < 0)
			pr_pwarn("Can't open %s (dirty scan will open per call)",
				 path);
	}

	/* Build tracked VMA list excluding failures */
	if (args->nr_vmas > 0) {
		failed_map = xzalloc(args->nr_vmas * sizeof(*failed_map));
		if (!failed_map)
			goto err;
	}

	failed_indices = cow_dump_failed_indices(args);
	for (i = 0; i < args->nr_failed_vmas; i++) {
		unsigned int idx = failed_indices[i];

		if (idx >= args->nr_vmas) {
			pr_warn("Ignoring invalid failed VMA index %u\n", idx);
			continue;
		}
		if (!failed_map[idx]) {
			failed_map[idx] = true;
			fallback_vmas++;
		}
	}

	tracked_vmas = args->nr_vmas - fallback_vmas;
	if (tracked_vmas > 0) {
		unsigned int tracked_idx = 0;

		task->tracked_vmas = xzalloc(sizeof(*task->tracked_vmas) *
					     tracked_vmas);
		if (!task->tracked_vmas)
			goto err;

		p_vma = cow_dump_vmas(args);
		for (i = 0; i < args->nr_vmas; i++) {
			if (failed_map[i])
				continue;
			task->tracked_vmas[tracked_idx].start = p_vma[i].start;
			task->tracked_vmas[tracked_idx].end =
				p_vma[i].start + p_vma[i].len;
			tracked_idx++;
		}
		task->nr_tracked_vmas = tracked_vmas;
	}

	list_add_tail(&task->list, &cdi->tracked_tasks);
	cdi->total_pages += task->total_pages;

	pr_info("COW dump initialized for pid %d: vmas=%u tracked=%u fallback=%u pages=%lu uffd=%d (WP_ASYNC)\n",
		item->pid->real, args->nr_vmas, tracked_vmas, fallback_vmas,
		task->total_pages, task->uffd);

	xfree(failed_map);
	return 0;

err:
	if (task) {
		if (task->uffd >= 0)
			close(task->uffd);
		xfree(task->tracked_vmas);
		xfree(task);
	}
	xfree(failed_map);

	if (created_session) {
		xfree(cdi);
		g_cow_info = NULL;
	}
	return -1;
}

void cow_dump_fini(void)
{
	struct cow_tracked_task *task, *tmp;

	if (!g_cow_info)
		return;

	pr_info("Cleaning up COW dump\n");

	list_for_each_entry_safe(task, tmp, &g_cow_info->tracked_tasks, list) {
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
}

/*
 * Apply UFFDIO_WRITEPROTECT from CRIU side after the target process
 * has resumed.  With WP_ASYNC the kernel auto-resolves write faults,
 * so the process experiences near-zero write latency (~1-2us per write).
 *
 * The WP ioctl uses mmap_read_lock (shared) on Linux 6.14, so it does
 * not block concurrent page faults or process_vm_readv.
 */
#define WP_CHUNK_SIZE (1UL << 30)	/* 1 GB chunks */

int cow_dump_apply_writeprotect(void)
{
	struct cow_tracked_task *task;
	struct uffdio_writeprotect wp;
	unsigned int i;
	int ret;

	if (!g_cow_info)
		return -1;

	list_for_each_entry(task, &g_cow_info->tracked_tasks, list) {
		for (i = 0; i < task->nr_tracked_vmas; i++) {
			unsigned long start = task->tracked_vmas[i].start;
			unsigned long end = task->tracked_vmas[i].end;
			unsigned long addr;

			for (addr = start; addr < end; addr += WP_CHUNK_SIZE) {
				unsigned long chunk = end - addr;

				if (chunk > WP_CHUNK_SIZE)
					chunk = WP_CHUNK_SIZE;

				wp.range.start = addr;
				wp.range.len = chunk;
				wp.mode = UFFDIO_WRITEPROTECT_MODE_WP;
				ret = ioctl(task->uffd,
					    UFFDIO_WRITEPROTECT, &wp);
				if (ret) {
					pr_warn("WP failed for %lx-%lx (errno %d), skipping\n",
						addr, addr + chunk, errno);
				}
			}
		}
	}

	return 0;
}

/*
 * Scan for dirty pages using PAGEMAP_SCAN with PM_SCAN_WP_MATCHING.
 * This atomically:
 *   1. Finds pages whose uffd-WP bit was cleared (i.e. written pages)
 *   2. Re-applies write-protection to those pages
 *
 * The caller gets an array of page_region entries describing the dirty
 * ranges, and the pages are already re-protected for the next epoch.
 */
int cow_dump_scan_dirty(pid_t source_pid,
			unsigned long start, unsigned long end,
			struct page_region *regs, unsigned long max_regs,
			unsigned long *nr_dirty_pages)
{
	struct cow_tracked_task *task;
	int fd, ret, i;
	struct pm_scan_arg args = {
		.size = sizeof(args),
		.flags = PM_SCAN_WP_MATCHING,
		.start = start,
		.end = end,
		.vec = (u64)(unsigned long)regs,
		.vec_len = max_regs,
		.max_pages = 0,
		.category_inverted = 0,
		.category_mask = 0,
		.category_anyof_mask = PAGE_IS_WRITTEN,
		.return_mask = PAGE_IS_WRITTEN,
	};

	/* Use cached pagemap fd if available */
	task = cow_find_task_by_pid(source_pid);
	if (task && task->pagemap_fd >= 0) {
		fd = task->pagemap_fd;
	} else {
		char path[64];

		snprintf(path, sizeof(path), "/proc/%d/pagemap",
			 source_pid);
		fd = open(path, O_RDONLY);
		if (fd < 0) {
			pr_perror("Can't open pagemap for pid %d", source_pid);
			return -1;
		}
	}

	ret = ioctl(fd, PAGEMAP_SCAN, &args);

	/* Only close if we opened it (not the cached fd) */
	if (!task || task->pagemap_fd < 0)
		close(fd);

	if (ret < 0) {
		pr_perror("PAGEMAP_SCAN failed for %lx-%lx", start, end);
		return -1;
	}

	*nr_dirty_pages = 0;
	for (i = 0; i < ret; i++)
		*nr_dirty_pages += (regs[i].end - regs[i].start) / PAGE_SIZE;

	return ret;
}

int cow_get_uffd_for_pid(pid_t source_pid)
{
	struct cow_tracked_task *task;

	task = cow_find_task_by_pid(source_pid);
	if (!task)
		return -1;

	return task->uffd;
}

bool cow_dump_is_vma_tracked(pid_t source_pid,
			     unsigned long start, unsigned long end)
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
