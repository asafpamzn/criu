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
#include <bpf/libbpf.h>

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bpf: "

#include "types.h"
#include "log.h"
#include "common/xmalloc.h"
#include "cow-bpf.h"
#include "bpf/dirty_track.skel.h"

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

int cow_bpf_start(pid_t target_pid)
{
	struct dirty_track_bpf *skel;
	int err;

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

	pr_info("BPF dirty tracker: attached to do_wp_page "
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

	dc.cap = 65536;
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
