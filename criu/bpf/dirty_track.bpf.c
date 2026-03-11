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

/* Ring buffer for dirty page addresses */
struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 64 * 1024 * 1024); /* 64MB ring */
} dirty_ring SEC(".maps");

/* Stats — readable from userspace */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u64);
} event_count SEC(".maps");

SEC("fentry/do_wp_page")
int BPF_PROG(track_wp_fault, struct vm_fault *vmf)
{
	__u64 addr;
	__u64 *valp;
	__u32 zero = 0;
	pid_t pid;

	pid = bpf_get_current_pid_tgid() >> 32;
	if (pid != target_pid)
		return 0;

	addr = BPF_CORE_READ(vmf, address);
	/* Page-align */
	addr &= ~((__u64)4095);

	/* Push to ring buffer — drop if full (non-blocking) */
	bpf_ringbuf_output(&dirty_ring, &addr, sizeof(addr), 0);

	/* Bump counter */
	valp = bpf_map_lookup_elem(&event_count, &zero);
	if (valp)
		__sync_fetch_and_add(valp, 1);

	return 0;
}

char LICENSE[] SEC("license") = "GPL";
