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
#include "atomic-bitmap.h"
#include "spsc-queue.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-dump: "

/* M5: SPSC queue node type for COW page entries */
DECLARE_SPSC_NODE(cow_page, struct cow_page_queue_entry);

/* From criu-cow: per-VMA tracking for multi-task support */
struct cow_tracked_vma {
	unsigned long start;
	unsigned long end;
};

/* From criu-cow: per-task tracking (replaces single pstree_item *item) */
struct cow_tracked_task {
	pid_t source_pid;
	int uffd;
	unsigned long total_pages;
	unsigned int nr_tracked_vmas;
	struct cow_tracked_vma *tracked_vmas;
	struct list_head list;
};

/* M1: Per-VMA bitmap for tracking write-faulted pages */
struct cow_vma_bitmap {
	unsigned long start;		/* VMA start address (page-aligned) */
	unsigned long end;		/* VMA end address (page-aligned) */
	uint8_t *bitmap;		/* 1 bit per page in this VMA */
	unsigned long bitmap_bytes;	/* Size of bitmap in bytes */
};

/* COW dump state for one dump session */
struct cow_dump_info {
	struct list_head tracked_tasks;		/* criu-cow: multi-task support */
	unsigned long total_pages;		/* Total pages being tracked */
	unsigned long iteration;		/* Current iteration number */

	/*
	 * M5: Lock-free SPSC queue (Thread 1 produces, Thread 3 consumes).
	 * head and tail are on separate cache lines to prevent false sharing.
	 */
	struct cow_page_spsc_node *spsc_head;	/* Consumer side */
	char _pad[64 - sizeof(struct cow_page_spsc_node *)];
	struct cow_page_spsc_node *spsc_tail;	/* Producer side */
	unsigned long spsc_size;		/* Atomic: approximate queue size */

	/* M1: Per-VMA bitmaps tracking write-faulted pages */
	struct cow_vma_bitmap *vma_bitmaps;	/* Array of per-VMA bitmaps */
	unsigned int nr_vma_bitmaps;		/* Number of entries in array */
};


static struct cow_dump_info *g_cow_info = NULL;
static pthread_t g_monitor_thread;
static volatile bool g_monitor_thread_running = false;
static volatile bool g_stop_monitoring = false;

/* criu-cow: protects monitor thread state across concurrent cow_dump_init calls */
static pthread_mutex_t g_monitor_state_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * M5: Consumer-side putback list. Only Thread 3 accesses this,
 * so no synchronization is needed. cow_get_next_page() drains
 * this list before checking the SPSC queue.
 */
static struct cow_page_queue_entry *g_putback_list = NULL;

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
		pr_debug("[COW_STATS] events: wr=%lu fork=%lu remap=%lu unk=%lu | ops: copied=%lu unprot=%lu woken=%lu | errs: alloc=%lu read=%lu unprot_err=%lu wake_err=%lu read_err=%lu eagain_err=%lu\n",
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

	list_for_each_entry(task, &g_cow_info->tracked_tasks, list) {
		if (task->source_pid == source_pid)
			return task;
	}

	return NULL;
}

bool cow_check_kernel_support(void)
{
	unsigned long features = UFFD_FEATURE_WP_ASYNC |
				 UFFD_FEATURE_PAGEFAULT_FLAG_WP | 
				 UFFD_FEATURE_EVENT_FORK |
				 UFFD_FEATURE_EVENT_REMAP;
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

	if (!(features & UFFD_FEATURE_WP_ASYNC)) {
		pr_info("userfaultfd write-protect feature not supported (need kernel 5.7+)\n");
		close(uffd);
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
static int cow_bitmap_init_vmas(struct vm_area_list *vma_area_list);

static void free_cow_page_entry(struct cow_page_queue_entry *entry)
{
	if (entry->data)
		xfree(entry->data);
	xfree(entry);
}

int cow_dump_init(struct pstree_item *item, struct vm_area_list *vma_area_list, struct parasite_ctl *ctl)
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

	pr_info("Initializing COW dump for pid %d (via parasite)\n", item->pid->real);

	/* criu-cow: refuse late registration after monitor thread started */
	pthread_mutex_lock(&g_monitor_state_lock);
	if (g_monitor_thread_running) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("COW monitor thread is already running; refusing late task registration\n");
		return -1;
	}
	pthread_mutex_unlock(&g_monitor_state_lock);

	if (!cdi) {
		if (!cow_check_kernel_support()) {
			pr_err("Kernel doesn't support COW dump\n");
			return -1;
		}

		cdi = xzalloc(sizeof(*cdi));
		if (!cdi)
			return -1;

		/* criu-cow: multi-task list */
		INIT_LIST_HEAD(&cdi->tracked_tasks);

		/* M5: Initialize lock-free SPSC queue with dummy node */
		if (spsc_init(cdi->spsc_head, cdi->spsc_tail,
			      cdi->spsc_size,
			      struct cow_page_spsc_node)) {
			xfree(cdi);
			return -1;
		}

		g_cow_info = cdi;
		created_session = true;
	}

	/* criu-cow: check for duplicate task registration */
	if (cow_find_task_by_pid(item->pid->real)) {
		pr_warn("COW tracking already initialized for pid %d, skipping\n", item->pid->real);
		return 0;
	}

	/* criu-cow: allocate per-task tracking struct */
	task = xzalloc(sizeof(*task));
	if (!task)
		goto err;

	INIT_LIST_HEAD(&task->list);
	task->source_pid = item->pid->real;
	task->uffd = -1;

	/* Prepare parasite arguments - count writable VMAs */
	/* IMPORTANT: Apply same filters as generate_vma_iovs() to avoid mismatches */
	nr_vmas = 0;
	list_for_each_entry(vma, &vma_area_list->h, list) {
		if (!vma_entry_can_be_lazy(vma->e))
		{		
			continue;
		}
		if (vma_area_is(vma, VMA_AREA_GUARD))
			continue;
		
		/* Must be writable */
		if (!(vma->e->prot & PROT_WRITE))
			continue;
		
		/* Match generate_vma_iovs() filters */
		if (!vma_area_is_private(vma, kdat.task_size) && !vma_area_is(vma, VMA_ANON_SHARED))
			continue;
		
		if (vma_entry_is(vma->e, VMA_AREA_VVAR))
			continue;
		
		if (vma->e->flags & MAP_DROPPABLE)
			continue;

		nr_vmas++;
	}

	/* Allocate parasite args - includes space for VMAs and failed indices */
	args_size = sizeof(*args) + 
		    nr_vmas * sizeof(struct parasite_vma_entry) +
		    nr_vmas * sizeof(unsigned int);  /* Space for failed indices */
	args = compel_parasite_args_s(ctl, args_size);
	if (!args) {
		pr_err("Failed to allocate parasite args\n");
		goto err;
	}

	args->nr_vmas = nr_vmas;
	args->total_pages = 0;
	args->nr_failed_vmas = 0;
	args->ret = -1;

	/* Fill VMA entries - must match the filters used above */
	p_vma = cow_dump_vmas(args);
	nr_vmas = 0;
	list_for_each_entry(vma, &vma_area_list->h, list) {
		if (!vma_entry_can_be_lazy(vma->e))
		{
			continue;
		}
		if (vma_area_is(vma, VMA_AREA_GUARD))
			continue;
		
		if (!(vma->e->prot & PROT_WRITE))
			continue;
		
		/* Match generate_vma_iovs() filters */
		if (!vma_area_is_private(vma, kdat.task_size) && !vma_area_is(vma, VMA_ANON_SHARED))
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

	/* Call parasite to create uffd and perform registration (async) */
	ret = compel_rpc_call(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0) {
		pr_err("Failed to initiate COW dump RPC\n");
		goto err;
	}

	/* Receive userfaultfd from parasite */
	compel_util_recv_fd(ctl, &task->uffd);
	if (task->uffd < 0) {
		pr_err("Failed to receive userfaultfd from parasite: %d\n", task->uffd);
		goto err;
	}
	pr_info("Got fd %d VMAs\n", task->uffd);
	/* Wait for parasite to complete */
	ret = compel_rpc_sync(PARASITE_CMD_COW_DUMP_INIT, ctl);
	if (ret < 0 || args->ret != 0) {
		pr_err("Parasite COW dump init failed: %d (ret=%d)\n", ret, args->ret);
		goto err;
	}

	/* criu-cow: per-task page count and failed VMA handling */
	task->total_pages = args->total_pages;

	if (args->nr_vmas > 0) {
		failed_map = xzalloc(args->nr_vmas * sizeof(*failed_map));
		if (!failed_map)
			goto err;
	}

	failed_indices = cow_dump_failed_indices(args);
	for (i = 0; i < args->nr_failed_vmas; i++) {
		unsigned int idx = failed_indices[i];

		if (idx >= args->nr_vmas) {
			pr_warn("Ignoring invalid failed VMA index %u (nr_vmas=%u)\n", idx, args->nr_vmas);
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

		task->tracked_vmas = xzalloc(sizeof(*task->tracked_vmas) * tracked_vmas);
		if (!task->tracked_vmas)
			goto err;

		p_vma = cow_dump_vmas(args);
		for (i = 0; i < args->nr_vmas; i++) {
			if (failed_map[i])
				continue;

			task->tracked_vmas[tracked_idx].start = p_vma[i].start;
			task->tracked_vmas[tracked_idx].end = p_vma[i].start + p_vma[i].len;
			tracked_idx++;
		}
		task->nr_tracked_vmas = tracked_vmas;
	}

	list_add_tail(&task->list, &cdi->tracked_tasks);
	cdi->total_pages += task->total_pages;

	pr_info("COW dump initialized for pid %d: vm_as=%u tracked=%u fallback=%u pages=%lu uffd=%d\n",
		item->pid->real, args->nr_vmas, tracked_vmas, fallback_vmas,
		task->total_pages, task->uffd);
	pr_info("COW dump tracking armed\n");

	/* M1: Allocate per-VMA bitmaps */
	if (cow_bitmap_init_vmas(vma_area_list)) {
		pr_err("Failed to initialize COW bitmaps (non-fatal)\n");
	}

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
		/* M5: Free SPSC dummy node */
		if (cdi->spsc_head) {
			spsc_drain(cdi->spsc_head, free_cow_page_entry);
			cdi->spsc_tail = NULL;
		}
		xfree(cdi);
		g_cow_info = NULL;
	}

	return -1;
}

/*
 * cow_bitmap_init_vmas - Allocate per-VMA bitmaps from the VMA list
 * that was already iterated during cow_dump_init.
 *
 * Must be called after cow_dump_init sets g_cow_info.
 */
static int cow_bitmap_init_vmas(struct vm_area_list *vma_area_list)
{
	struct vma_area *vma;
	unsigned int count = 0;
	unsigned int idx = 0;
	unsigned long total_bitmap_bytes = 0;

	if (!g_cow_info)
		return -1;

	/* First pass: count writable lazy VMAs (same filters as cow_dump_init) */
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
		count++;
	}

	if (count == 0) {
		pr_warn("cow_bitmap_init_vmas: no writable lazy VMAs found\n");
		g_cow_info->vma_bitmaps = NULL;
		g_cow_info->nr_vma_bitmaps = 0;
		return 0;
	}

	g_cow_info->vma_bitmaps = xzalloc(count * sizeof(struct cow_vma_bitmap));
	if (!g_cow_info->vma_bitmaps)
		return -1;
	g_cow_info->nr_vma_bitmaps = count;

	/* Second pass: allocate bitmap per VMA */
	list_for_each_entry(vma, &vma_area_list->h, list) {
		unsigned long nr_pages, bitmap_bytes;

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

		nr_pages = (vma->e->end - vma->e->start) / PAGE_SIZE;
		bitmap_bytes = BITMAP_ALLOC_SIZE(nr_pages);

		g_cow_info->vma_bitmaps[idx].start = vma->e->start;
		g_cow_info->vma_bitmaps[idx].end = vma->e->end;
		g_cow_info->vma_bitmaps[idx].bitmap = xzalloc(bitmap_bytes);
		if (!g_cow_info->vma_bitmaps[idx].bitmap) {
			pr_err("Failed to allocate bitmap for VMA 0x%lx-0x%lx\n",
			       (unsigned long)vma->e->start,
			       (unsigned long)vma->e->end);
			goto err;
		}
		g_cow_info->vma_bitmaps[idx].bitmap_bytes = bitmap_bytes;
		total_bitmap_bytes += bitmap_bytes;
		idx++;
	}

	pr_warn("COW bitmap initialized: %u VMAs, total bitmap %lu bytes\n",
		count, total_bitmap_bytes);
	return 0;

err:
	/* Free already allocated bitmaps */
	for (unsigned int i = 0; i < idx; i++)
		xfree(g_cow_info->vma_bitmaps[i].bitmap);
	xfree(g_cow_info->vma_bitmaps);
	g_cow_info->vma_bitmaps = NULL;
	g_cow_info->nr_vma_bitmaps = 0;
	return -1;
}

void cow_bitmap_fini(void)
{
	unsigned int i;

	if (!g_cow_info)
		return;

	if (g_cow_info->vma_bitmaps) {
		for (i = 0; i < g_cow_info->nr_vma_bitmaps; i++)
			xfree(g_cow_info->vma_bitmaps[i].bitmap);
		xfree(g_cow_info->vma_bitmaps);
		g_cow_info->vma_bitmaps = NULL;
	}
	g_cow_info->nr_vma_bitmaps = 0;
}

/*
 * Find the per-VMA bitmap that contains vaddr.
 * Returns NULL if vaddr is not in any tracked VMA.
 */
static struct cow_vma_bitmap *cow_find_vma_bitmap(unsigned long vaddr)
{
	unsigned int i;

	if (!g_cow_info || !g_cow_info->vma_bitmaps)
		return NULL;

	for (i = 0; i < g_cow_info->nr_vma_bitmaps; i++) {
		struct cow_vma_bitmap *vb = &g_cow_info->vma_bitmaps[i];
		if (vaddr >= vb->start && vaddr < vb->end)
			return vb;
	}

	return NULL;
}

void cow_set_bitmap(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	struct cow_vma_bitmap *vb;
	unsigned long page_idx;

	vb = cow_find_vma_bitmap(page_addr);
	if (!vb) {
		pr_warn("cow_set_bitmap: addr 0x%lx not in any tracked VMA\n",
			page_addr);
		return;
	}

	page_idx = (page_addr - vb->start) / PAGE_SIZE;

	atomic_bitmap_set(vb->bitmap, page_idx);
}

void cow_clear_bitmap(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	struct cow_vma_bitmap *vb;
	unsigned long page_idx;

	vb = cow_find_vma_bitmap(page_addr);
	if (!vb)
		return;

	page_idx = (page_addr - vb->start) / PAGE_SIZE;

	atomic_bitmap_clear(vb->bitmap, page_idx);
}

bool cow_test_bitmap(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	struct cow_vma_bitmap *vb;
	unsigned long page_idx;

	vb = cow_find_vma_bitmap(page_addr);
	if (!vb)
		return false;

	page_idx = (page_addr - vb->start) / PAGE_SIZE;

	return atomic_bitmap_test(vb->bitmap, page_idx);
}

void cow_dump_fini(void)
{
	struct cow_page_queue_entry *qe;
	struct cow_tracked_task *task, *task_tmp;
	int queue_remaining = 0;

	if (!g_cow_info)
		return;

	if (cow_stop_monitor_thread()) {
		pr_err("Failed to stop COW monitor thread, skipping COW cleanup to avoid races\n");
		return;
	}

	pr_info("Cleaning up COW dump\n");

	/* M1: Free bitmap */
	cow_bitmap_fini();

	/* M5: Drain consumer-side putback list */
	while (g_putback_list) {
		qe = g_putback_list;
		g_putback_list = qe->next;
		if (qe->data)
			xfree(qe->data);
		xfree(qe);
		queue_remaining++;
	}

	/* M5: Drain SPSC queue */
	if (g_cow_info->spsc_head) {
		spsc_drain(g_cow_info->spsc_head, free_cow_page_entry);
		g_cow_info->spsc_tail = NULL;
	}

	if (queue_remaining > 0)
		pr_warn("Freed %d remaining queue entries\n", queue_remaining);

	/* criu-cow: clean up all tracked tasks */
	list_for_each_entry_safe(task, task_tmp, &g_cow_info->tracked_tasks, list) {
		list_del(&task->list);
		if (task->uffd >= 0)
			close(task->uffd);
		xfree(task->tracked_vmas);
		xfree(task);
	}

	xfree(g_cow_info);
	g_cow_info = NULL;
}

static int cow_handle_write_fault(struct cow_dump_info *cdi,
				  struct cow_tracked_task *task,
				  unsigned long addr)
{
	unsigned long page_addr = addr & ~(PAGE_SIZE - 1);
	struct uffdio_writeprotect wp;
	struct uffdio_range range;
	ssize_t ret;
	struct cow_page_queue_entry *entry;
	struct iovec local_iov, remote_iov;
	void *page_data;

	pr_debug("Write fault at 0x%lx\n", page_addr);

	cow_stats.write_faults++;

	/*
	 * M4: Allocate a single page buffer. This will be transferred
	 * directly to the queue entry (zero-copy within the fault handler).
	 */
	page_data = xmalloc(PAGE_SIZE);
	if (!page_data) {
		pr_err("Failed to allocate page data buffer\n");
		cow_stats.alloc_failures++;
		return -1;
	}

	/* Read original page content using process_vm_readv */
	local_iov.iov_base = page_data;
	local_iov.iov_len = PAGE_SIZE;
	remote_iov.iov_base = (void *)page_addr;
	remote_iov.iov_len = PAGE_SIZE;

	/* criu-cow: use task->source_pid instead of cdi->item->pid->real */
	ret = process_vm_readv(task->source_pid, &local_iov, 1, &remote_iov, 1, 0);
	if (ret != PAGE_SIZE) {
		pr_perror("Failed to read page at 0x%lx from pid %d (read %zd bytes)",
			  page_addr, task->source_pid, ret);
		xfree(page_data);
		cow_stats.read_failures++;
		return -1;
	}

	cow_stats.pages_copied++;

	/* M1: Set bitmap bit (Thread 3 reads via cow_test_bitmap) */
	cow_set_bitmap(page_addr);

	/* Unprotect the page so the process can continue */
	wp.range.start = page_addr;
	wp.range.len = PAGE_SIZE;
	wp.mode = 0; /* Clear write-protect */

	/* criu-cow: use task->uffd instead of cdi->uffd */
	if (ioctl(task->uffd, UFFDIO_WRITEPROTECT, &wp)) {
		pr_perror("Failed to unprotect page at 0x%lx", page_addr);
		xfree(page_data);
		cow_stats.unprotect_failures++;
		return -1;
	}

	cow_stats.pages_unprotected++;

	/* Wake up the faulting thread */
	range.start = page_addr;
	range.len = PAGE_SIZE;

	/* criu-cow: use task->uffd instead of cdi->uffd */
	if (ioctl(task->uffd, UFFDIO_WAKE, &range)) {
		pr_perror("Failed to wake thread after unprotect");
		xfree(page_data);
		cow_stats.wake_failures++;
		return -1;
	}

	cow_stats.pages_woken++;
	__atomic_fetch_sub(&cdi->total_pages, 1, __ATOMIC_RELAXED);

	/*
	 * M4: Enqueue with zero-copy — transfer page_data ownership
	 * to the queue entry. No memcpy, no intermediate cow_page struct.
	 * P1 (send_cow_page_lazy) will send from entry->data and free it.
	 */
	/*
	 * M5: Lock-free SPSC enqueue. Allocate a wrapper node and link
	 * it at the tail. The release store on tail->next publishes both
	 * the node and the entry data to the consumer (Thread 3).
	 */
	/*
	 * M5: Lock-free SPSC enqueue.  The entry and node are tiny
	 * (~50 bytes each) — if allocation fails, retry a few times
	 * before giving up.  We must not lose page_data: it holds the
	 * pre-write snapshot that P1 will send to the destination.
	 */
	{
		int attempts;

		for (attempts = 0; attempts < 3; attempts++) {
			entry = xmalloc(sizeof(*entry));
			if (entry)
				break;
			pr_warn("Retry %d: alloc queue entry for 0x%lx\n",
				attempts + 1, page_addr);
		}
		if (!entry) {
			pr_err("Failed to allocate queue entry for page 0x%lx "
			       "after retries, clearing bitmap for P3 fallback\n",
			       page_addr);
			xfree(page_data);
			cow_clear_bitmap(page_addr);
			return 0;
		}
	}

	{
		int attempts;

		entry->vaddr = page_addr;
		entry->data = page_data;	/* Transfer ownership */
		page_data = NULL;		/* Prevent double-free */
		entry->ppb = NULL;
		entry->seg_idx = 0;
		entry->page_idx_in_seg = 0;
		entry->next = NULL;

		for (attempts = 0; attempts < 3; attempts++) {
			if (!spsc_enqueue(cdi->spsc_tail, cdi->spsc_size,
					  entry, struct cow_page_spsc_node))
				break;
			pr_warn("Retry %d: alloc SPSC node for 0x%lx\n",
				attempts + 1, page_addr);
		}
		if (attempts == 3) {
			pr_err("Failed to allocate SPSC node for page 0x%lx "
			       "after retries, clearing bitmap for P3 fallback\n",
			       page_addr);
			xfree(entry->data);
			xfree(entry);
			cow_clear_bitmap(page_addr);
			return 0;
		}
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

	while (1) {
		/* Check and print stats */
		check_and_print_cow_stats();
		
		/* Try reading directly first - avoids poll() overhead when data is ready */
		ret = read(task->uffd, &msg, sizeof(msg));
		
		if (ret < 0 && errno == EAGAIN && blocking) {
			/* No data available and we want to block - use poll() with timeout */
			pfd.fd = task->uffd;
			pfd.events = POLLIN;
			pfd.revents = 0;
			
			poll_ret = poll(&pfd, 1, 500);  /* 500ms timeout */
			if (poll_ret < 0) {
				pr_perror("poll() failed on uffd");
				cow_stats.read_errors++;
				return -1;
			}
			
			if (poll_ret == 0) {
				/* Timeout - no events within 500ms */
				return 0;
			}
			
			/* Data ready after poll - retry read */
			ret = read(task->uffd, &msg, sizeof(msg));
		}
		
		if (ret < 0) {
			if (errno == EAGAIN && !blocking) {
				/* Non-blocking mode and no data */
				cow_stats.eagain_errors++;
				return 0;
			}
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
				/* Write fault - track it */
				if (cow_handle_write_fault(cdi, task, msg.arg.pagefault.address))
					return -1;
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

/* criu-cow: poll all tracked task uffds simultaneously */
static int cow_wait_for_events(struct cow_dump_info *cdi, int timeout_ms)
{
	struct cow_tracked_task *task;
	struct pollfd *pfds;
	int nr_tasks = 0;
	int idx = 0;
	int ret;

	list_for_each_entry(task, &cdi->tracked_tasks, list)
		nr_tasks++;

	if (!nr_tasks)
		return 0;

	pfds = xmalloc(sizeof(*pfds) * nr_tasks);
	if (!pfds)
		return -1;

	list_for_each_entry(task, &cdi->tracked_tasks, list) {
		pfds[idx].fd = task->uffd;
		pfds[idx].events = POLLIN;
		pfds[idx].revents = 0;
		idx++;
	}

	ret = poll(pfds, nr_tasks, timeout_ms);
	if (ret < 0)
		pr_perror("poll() failed on uffd set");

	xfree(pfds);
	return ret;
}

/* Background thread that monitors for write faults */
static void *cow_monitor_thread(void *arg)
{
	struct cow_dump_info *cdi = (struct cow_dump_info *)arg;
	struct cow_tracked_task *task;
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
		if (ret == 0)
			continue;

		list_for_each_entry(task, &cdi->tracked_tasks, list) {
			if (cow_process_events(cdi, task, false) < 0) {
				pr_err("Error processing COW events for pid %d\n",
				       task->source_pid);
				monitor_error = true;
				break;
			}
		}

		if (monitor_error)
			break;
	}

	if (monitor_error)
		pr_err("COW monitor thread exiting on event-processing error\n");

	pr_info("COW monitor thread stopped\n");
	return NULL;
}

int cow_start_monitor_thread(void)
{
	int ret;
	
	pthread_mutex_lock(&g_monitor_state_lock);

	if (!g_cow_info) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("COW dump not initialized\n");
		return -1;
	}

	if (list_empty(&g_cow_info->tracked_tasks)) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("COW tracking has no registered tasks\n");
		return -1;
	}

	if (g_monitor_thread_running) {
		pthread_mutex_unlock(&g_monitor_state_lock);
		return 0;
	}

	g_stop_monitoring = false;
	g_monitor_thread_running = true;  /* Set BEFORE create to prevent race */

	ret = pthread_create(&g_monitor_thread, NULL, cow_monitor_thread, g_cow_info);
	if (ret) {
		g_monitor_thread_running = false;  /* Rollback on failure */
		pthread_mutex_unlock(&g_monitor_state_lock);
		pr_err("Failed to create COW monitor thread: %s\n", strerror(ret));
		return -1;
	}

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
	
	/* Wait for thread to finish */
	ret = pthread_join(monitor_thread, &retval);
	if (ret && ret != ESRCH && ret != EINVAL) {
		pr_err("Failed to join COW monitor thread: %s\n", strerror(ret));
		return -1;
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

	if (!g_cow_info || list_empty(&g_cow_info->tracked_tasks))
		return -1;

	task = list_first_entry(&g_cow_info->tracked_tasks, struct cow_tracked_task, list);
	return task->uffd;
}

int cow_get_uffd_for_pid(pid_t source_pid)
{
	struct cow_tracked_task *task;

	task = cow_find_task_by_pid(source_pid);
	if (!task)
		return -1;

	return task->uffd;
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

struct cow_page_queue_entry *cow_get_next_page(void)
{
	struct cow_page_queue_entry *entry;

	if (!g_cow_info)
		return NULL;

	/* M5: Check consumer-side putback list first (no atomics needed) */
	if (g_putback_list) {
		entry = g_putback_list;
		g_putback_list = entry->next;
		entry->next = NULL;
		__atomic_fetch_sub(&g_cow_info->spsc_size, 1, __ATOMIC_RELAXED);
		return entry;
	}

	/* M5: Lock-free SPSC dequeue */
	return spsc_dequeue(g_cow_info->spsc_head, g_cow_info->spsc_size);
}

bool cow_has_pending_pages(void)
{
	if (!g_cow_info)
		return false;

	/* Check putback list first (consumer-local, no atomic needed) */
	if (g_putback_list)
		return true;

	/* Check SPSC queue */
	return spsc_peek(g_cow_info->spsc_head);
}

void cow_put_back_page(struct cow_page_queue_entry *entry)
{
	if (!g_cow_info || !entry)
		return;

	/*
	 * M5: Push onto consumer-side putback list. Only Thread 3 calls
	 * this, so no synchronization needed. cow_get_next_page() drains
	 * this list before the SPSC queue, preserving FIFO-ish ordering.
	 */
	entry->next = g_putback_list;
	g_putback_list = entry;

	/* Don't increment spsc_size — it was already counted when enqueued */
	pr_debug("Re-queued COW page 0x%lx to putback list\n", entry->vaddr);
}

unsigned long cow_get_queue_size(void)
{
	if (!g_cow_info)
		return 0;

	return spsc_size(g_cow_info->spsc_size);
}