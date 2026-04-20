// SPDX-License-Identifier: GPL-2.0
/*
 * eBPF dirty page tracker for CRIU COW migration.
 *
 * Hooks do_wp_page() — the kernel function that handles
 * write-protect faults. When WP_ASYNC is enabled, every
 * write to a WP-protected page goes through do_wp_page.
 *
 * We filter by target PID and push the faulting page address
 * to a ring buffer. The page server drains the ring at T3
 * instead of walking all page tables with PAGEMAP_SCAN.
 *
 * Cost: O(dirty) instead of O(total_pages).
 */
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

/* Target PID — set by userspace before attaching */
const volatile pid_t target_pid = 0;
/* Page shift — set by userspace from sysconf(_SC_PAGESIZE) */
const volatile unsigned int page_shift = 12;

/* Ring buffer for dirty page addresses */
struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 512 * 1024 * 1024); /* 512MB ring - ~64M addresses */
} dirty_ring SEC(".maps");

/* Stats — readable from userspace */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u64);
} event_count SEC(".maps");

/* Counts ring buffer drops (ring full) */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u64);
} drop_count SEC(".maps");

SEC("fentry/do_wp_page")
int BPF_PROG(track_wp_fault, struct vm_fault *vmf)
{
	__u64 addr;
	__u64 *valp;
	__u32 zero = 0;
	pid_t pid;
	__u64 count;

	pid = bpf_get_current_pid_tgid() >> 32;
	if (pid != target_pid)
		return 0;

	addr = BPF_CORE_READ(vmf, address);
	/* Page-align using configurable page shift */
	addr &= ~(((__u64)1 << page_shift) - 1);

	/* Push to ring buffer — track drops for correctness */
	if (bpf_ringbuf_output(&dirty_ring, &addr, sizeof(addr), 0) != 0) {
		__u64 *dropp;
		__u32 dz = 0;

		dropp = bpf_map_lookup_elem(&drop_count, &dz);
		if (dropp)
			__sync_fetch_and_add(dropp, 1);
	}

	/* Bump counter and log every 100K faults */
	valp = bpf_map_lookup_elem(&event_count, &zero);
	if (valp) {
		count = __sync_fetch_and_add(valp, 1) + 1;
		if (count % 100000 == 0) {
			__u64 ns = bpf_ktime_get_ns();
			bpf_printk("COW-BPF: %llu page faults, time=%llu ns\n",
				   count, ns);
		}
	}

	return 0;
}

char LICENSE[] SEC("license") = "GPL";
