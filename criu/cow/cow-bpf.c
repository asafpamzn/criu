/*
 * eBPF-based dirty page tracker for COW migration.
 *
 * Replaces PAGEMAP_SCAN at T3 freeze time. Instead of walking
 * all 50M+ PTEs (O(total_pages), ~150ms), we drain a ring buffer
 * of addresses collected by a BPF program hooked to do_wp_page
 * (O(dirty_pages), microseconds).
 *
 * Lifecycle:
 *   1. cow_bpf_start(pid)  — load BPF, attach kfunc, start collecting
 *   2. cow_bpf_drain(...)  — drain ring → sorted, dedup'd region list
 *   3. cow_bpf_stop()      — detach + free
 */
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <bpf/libbpf.h>

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bpf: "

#include "types.h"
#include "log.h"
#include "common/xmalloc.h"
#include "cow/cow-bpf.h"
#include "cow/cow-conf.h"
#include "bpf/dirty_track.skel.h"
#include "pagemap_scan.h"

static struct dirty_track_bpf *g_skel;
static int g_ring_fd = -1;

/* Accumulator for ring buffer callback */
struct drain_ctx {
	unsigned long *addrs;
	int count;
	int cap;
};

static int ring_event_cb(void *ctx, void *data, size_t size)
{
	struct drain_ctx *dc = ctx;
	unsigned long addr;

	if (size < sizeof(addr))
		return 0;

	addr = *(unsigned long *)data;

	if (dc->count >= dc->cap) {
		int new_cap = dc->cap * 2;
		unsigned long *tmp;

		tmp = xrealloc(dc->addrs, new_cap * sizeof(*tmp));
		if (!tmp)
			return -1;
		dc->addrs = tmp;
		dc->cap = new_cap;
	}
	dc->addrs[dc->count++] = addr;
	return 0;
}

static int addr_cmp(const void *a, const void *b)
{
	unsigned long va = *(const unsigned long *)a;
	unsigned long vb = *(const unsigned long *)b;

	return (va > vb) - (va < vb);
}

#ifdef SCAN_COMPARE
/*
 * DEBUG: Store initial dirty pages found at BPF start time.
 * These are pages that were dirty BEFORE we started tracking.
 */
static unsigned long *g_initial_dirty_addrs = NULL;
static unsigned long g_initial_dirty_count = 0;
static pid_t g_debug_pid = 0;

/*
 * DEBUG: Scan all VMAs and collect dirty page addresses.
 * Returns count of dirty pages found.
 */
static unsigned long debug_scan_dirty_pages(pid_t pid, unsigned long **out_addrs)
{
	char path[64];
	int fd;
	struct pm_scan_arg args;
	struct page_region regs[1024];
	unsigned long count = 0;
	unsigned long cap = 65536;
	unsigned long *addrs;
	unsigned long page_size = sysconf(_SC_PAGESIZE);
	struct timeval start, end, delta;

	gettimeofday(&start, NULL);

	addrs = xmalloc(cap * sizeof(*addrs));
	if (!addrs) {
		*out_addrs = NULL;
		return 0;
	}

	snprintf(path, sizeof(path), "/proc/%d/pagemap", pid);
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		xfree(addrs);
		*out_addrs = NULL;
		return 0;
	}

	/* Scan entire user address space */
	memset(&args, 0, sizeof(args));
	args.size = sizeof(args);
	args.flags = 0;
	args.start = 0;
	args.end = 0x7fffffffffff;  /* Max user address */
	args.walk_end = 0;
	args.vec = (u64)(unsigned long)regs;
	args.vec_len = 1024;
	args.max_pages = 0;
	args.category_anyof_mask = PAGE_IS_WRITTEN;
	args.return_mask = PAGE_IS_WRITTEN;

	do {
		long ret;
		int i;

		args.start = args.walk_end;
		ret = ioctl(fd, PAGEMAP_SCAN, &args);
		if (ret <= 0)
			break;

		for (i = 0; i < ret; i++) {
			unsigned long addr;
			for (addr = regs[i].start; addr < regs[i].end; addr += page_size) {
				if (count >= cap) {
					unsigned long new_cap = cap * 2;
					unsigned long *tmp = xrealloc(addrs, new_cap * sizeof(*tmp));
					if (!tmp) {
						close(fd);
						*out_addrs = addrs;
						return count;
					}
					addrs = tmp;
					cap = new_cap;
				}
				addrs[count++] = addr;
			}
		}
	} while (args.walk_end < args.end);

	close(fd);

	gettimeofday(&end, NULL);
	timersub(&end, &start, &delta);
	pr_err("SCAN_COMPARE: debug_scan_dirty_pages took %ld.%06ld sec, found %lu pages\n",
	       delta.tv_sec, delta.tv_usec, count);

	*out_addrs = addrs;
	return count;
}

/*
 * Get the initial dirty pages captured at BPF start.
 */
unsigned long *cow_bpf_get_initial_dirty(unsigned long *count)
{
	*count = g_initial_dirty_count;
	return g_initial_dirty_addrs;
}
#endif

int cow_bpf_start(pid_t target_pid)
{
	struct dirty_track_bpf *skel;
	int err;
#ifdef SCAN_COMPARE
	unsigned long *dirty_before_addrs = NULL;
	unsigned long dirty_before_count;

	g_debug_pid = target_pid;
	dirty_before_count = debug_scan_dirty_pages(target_pid, &dirty_before_addrs);
	pr_err("SCAN_COMPARE: Dirty pages BEFORE BPF attach: %lu\n", dirty_before_count);

	/* Store for later comparison */
	g_initial_dirty_addrs = dirty_before_addrs;
	g_initial_dirty_count = dirty_before_count;

	/* Print first 20 addresses if any */
	if (dirty_before_count > 0) {
		unsigned long i;
		unsigned long to_print = dirty_before_count < 20 ? dirty_before_count : 20;
		pr_err("SCAN_COMPARE: First %lu initial dirty addresses:\n", to_print);
		for (i = 0; i < to_print; i++) {
			pr_err("  INITIAL_DIRTY[%lu]: 0x%lx\n", i, dirty_before_addrs[i]);
		}
	}
#endif

	skel = dirty_track_bpf__open();
	if (!skel) {
		pr_perror("BPF dirty tracker: failed to open skeleton");
		return -1;
	}

	skel->rodata->target_pid = target_pid;
	skel->rodata->page_shift = __builtin_ctz(sysconf(_SC_PAGESIZE));

	err = dirty_track_bpf__load(skel);
	if (err) {
		pr_err("BPF dirty tracker: failed to load: %d\n", err);
		dirty_track_bpf__destroy(skel);
		return -1;
	}

	err = dirty_track_bpf__attach(skel);
	if (err) {
		pr_err("BPF dirty tracker: failed to attach: %d\n", err);
		dirty_track_bpf__destroy(skel);
		return -1;
	}

	g_ring_fd = bpf_map__fd(skel->maps.dirty_ring);
	g_skel = skel;

#ifdef SCAN_COMPARE
	{
		unsigned long *dirty_after_addrs = NULL;
		unsigned long dirty_after_count;

		dirty_after_count = debug_scan_dirty_pages(target_pid, &dirty_after_addrs);
		pr_err("SCAN_COMPARE: Dirty pages AFTER BPF attach: %lu\n", dirty_after_count);

		if (dirty_after_addrs)
			xfree(dirty_after_addrs);
	}
#endif

	pr_warn("BPF dirty tracker: attached to do_wp_page "
	       "for pid %d (ring_fd=%d)\n", target_pid, g_ring_fd);
	return 0;
}

/*
 * Drain the ring buffer into a sorted, coalesced region list.
 *
 * Returns number of regions written to out_regions.
 * Each region has .start/.end (page-aligned) and .categories=0.
 *
 * out_count: number of dirty pages (before coalescing).
 */
int cow_bpf_drain(struct cow_bpf_region *out_regions, int max_regions,
		  unsigned long *out_count)
{
	struct ring_buffer *rb;
	struct drain_ctx dc;
	int err, nr_regions;
	int i;

	*out_count = 0;

	if (!g_skel || g_ring_fd < 0)
		return 0;

	dc.cap = COW_BPF_DRAIN_INITIAL_CAP;
	dc.count = 0;
	dc.addrs = xmalloc(dc.cap * sizeof(*dc.addrs));
	if (!dc.addrs)
		return -1;

	rb = ring_buffer__new(g_ring_fd, ring_event_cb, &dc, NULL);
	if (!rb) {
		pr_perror("BPF drain: ring_buffer__new failed");
		xfree(dc.addrs);
		return -1;
	}

	/* Consume all pending events (non-blocking) */
	err = ring_buffer__consume(rb);
	ring_buffer__free(rb);

	if (err < 0 && err != -EAGAIN) {
		pr_err("BPF drain: consume error %d\n", err);
		xfree(dc.addrs);
		return -1;
	}

	/* Check for ring buffer drops — if any events were lost,
	 * the dirty page list is incomplete. Caller must fall
	 * back to PAGEMAP_SCAN for correctness. */
	{
		u64 drops = cow_bpf_drop_count();

		if (drops > 0) {
			pr_err("BPF drain: %llu events dropped (ring full) "
			       "— falling back to PAGEMAP_SCAN\n",
			       (unsigned long long)drops);
			xfree(dc.addrs);
			return -2; /* special: drops detected */
		}
	}

	if (dc.count == 0) {
		xfree(dc.addrs);
		return 0;
	}

	/* Sort + dedup */
	qsort(dc.addrs, dc.count, sizeof(*dc.addrs), addr_cmp);

	/* Remove duplicates in-place */
	{
		int unique = 1;

		for (i = 1; i < dc.count; i++) {
			if (dc.addrs[i] != dc.addrs[unique - 1])
				dc.addrs[unique++] = dc.addrs[i];
		}
		*out_count = unique;
		dc.count = unique;
	}

	/* Coalesce contiguous pages into regions */
	nr_regions = 0;
	i = 0;
	{
	unsigned long page_size = sysconf(_SC_PAGESIZE);

	while (i < dc.count && nr_regions < max_regions) {
		unsigned long start = dc.addrs[i];
		unsigned long end = start + page_size;

		while (i + 1 < dc.count && dc.addrs[i + 1] == end) {
			end += page_size;
			i++;
		}
		out_regions[nr_regions].start = start;
		out_regions[nr_regions].end = end;
		out_regions[nr_regions].categories = 0;
		nr_regions++;
		i++;
	}
	}

	xfree(dc.addrs);
	return nr_regions;
}

u64 cow_bpf_event_count(void)
{
	u32 zero = 0;
	u64 count = 0;

	if (!g_skel)
		return 0;

	bpf_map__lookup_elem(g_skel->maps.event_count,
			     &zero, sizeof(zero),
			     &count, sizeof(count), 0);
	return count;
}

u64 cow_bpf_drop_count(void)
{
	u32 zero = 0;
	u64 count = 0;

	if (!g_skel)
		return 0;

	bpf_map__lookup_elem(g_skel->maps.drop_count,
			     &zero, sizeof(zero),
			     &count, sizeof(count), 0);
	return count;
}

void cow_bpf_stop(void)
{
	if (g_skel) {
		dirty_track_bpf__destroy(g_skel);
		g_skel = NULL;
		g_ring_fd = -1;
		pr_info("BPF dirty tracker: detached\n");
	}
}

bool cow_bpf_active(void)
{
	return g_skel != NULL;
}

/*
 * Drain BPF ring buffer and return raw sorted/deduped addresses.
 * Used for debug comparison with PAGEMAP_SCAN.
 */
int cow_bpf_drain_addrs(unsigned long **out_addrs, unsigned long *out_count)
{
	struct ring_buffer *rb;
	struct drain_ctx dc;
	int err, i, unique;

	*out_addrs = NULL;
	*out_count = 0;

	if (!g_skel || g_ring_fd < 0)
		return 0;

	dc.cap = COW_BPF_DRAIN_INITIAL_CAP;
	dc.count = 0;
	dc.addrs = xmalloc(dc.cap * sizeof(*dc.addrs));
	if (!dc.addrs)
		return -1;

	rb = ring_buffer__new(g_ring_fd, ring_event_cb, &dc, NULL);
	if (!rb) {
		pr_perror("BPF drain_addrs: ring_buffer__new failed");
		xfree(dc.addrs);
		return -1;
	}

	err = ring_buffer__consume(rb);
	ring_buffer__free(rb);

	if (err < 0 && err != -EAGAIN) {
		pr_err("BPF drain_addrs: consume error %d\n", err);
		xfree(dc.addrs);
		return -1;
	}

	if (dc.count == 0) {
		xfree(dc.addrs);
		return 0;
	}

	/* Sort + dedup */
	qsort(dc.addrs, dc.count, sizeof(*dc.addrs), addr_cmp);

	unique = 1;
	for (i = 1; i < dc.count; i++) {
		if (dc.addrs[i] != dc.addrs[unique - 1])
			dc.addrs[unique++] = dc.addrs[i];
	}

	*out_addrs = dc.addrs;
	*out_count = unique;
	return 0;
}
