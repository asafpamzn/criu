# CRIU COW Dump — Live Migration for Valkey

Near-zero-downtime live migration using copy-on-write page tracking
and eBPF dirty page detection. 44ms freeze, 200GB at 3.3 GB/s.

## Quick Start

```bash
# Build on both machines (requires clang, bpftool, libbpf-dev)
make -j$(nproc)
cd tools && gcc -O2 -o page-recv page-recv.c -llz4 -lpthread && cd ..
sudo cp criu/criu /usr/local/sbin/criu
echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd

# Configure scripts/.env with IPs and SSH key

# Fill 100GB + migrate with live traffic + verify 7 tests:
sudo ./scripts/verify-migration.sh 100

# Or run migrate.sh directly (data already loaded):
sudo env SKIP_FILL=1 KEEP_SOURCE_RUNNING=1 \
  RUN_WORKLOAD_DURING_MIGRATION=1 \
  bash scripts/migrate.sh 200
```

## How It Works

```
  SOURCE MACHINE                           REPLICA MACHINE

  Valkey running, serving clients
        │
  ┌─────▼──────────────────────┐
  │  1. FREEZE (23-48ms)       │    ┌──────────────────────────┐
  │  - Seize process (ptrace)  │───▶│  Start restore.sh        │
  │  - Capture: VMAs, pagemap  │    │  CRIU restore + page-recv│
  │  - Setup WP tracking       │    └──────────────────────────┘
  │  - Resume (--leave-running)│
  │  - eBPF attach (no gap)    │
  │  ◄ Valkey running again ►  │
  └─────┬──────────────────────┘
        │
  ┌─────▼──────────────────────┐    ┌──────────────────────────┐
  │  2. BULK TRANSFER (~60s)   │    │  RECEIVE                 │
  │  - Read from live source   │───▶│  - 8 TCP streams         │
  │  - 8 TCP streams, LZ4     │    │  - process_vm_writev     │
  │  - 3.3 GB/s throughput     │    │    into restored process │
  │  - Valkey still serving ►  │    │  - Restored Valkey is    │
  │  (writes tracked by eBPF)  │    │    stopped (ptrace-trap) │
  └─────┬──────────────────────┘    └──────────┬───────────────┘
        │                                      │
  ┌─────▼──────────────────────┐               │
  │  3. T3 FREEZE (44ms)       │───────────────┘
  │  - SIGSTOP source          │    (sends dirty + non-lazy pages)
  │  - eBPF ring drain (~0ms)  │
  │  - T3 state capture (10ms) │───▶  Saves t3_regs.dat, etc.
  │  - Dirty pages dispatch    │───▶  Overwrites stale pages
  │  - Non-lazy re-send (stacks│───▶  127 pages (stacks + .data)
  │    + file-backed rw)       │
  │  - VMA diff (new mmaps)    │───▶  Injects mmap
  │  - SIGCONT source          │
  └─────┬──────────────────────┘
        │
  ┌─────▼──────────────────────┐    ┌──────────────────────────┐
  │  4. CUTOVER (0ms)          │    │  CUTOVER                 │
  │  - Send TCP "GO" ──────────│───▶│  - Apply T3 registers    │
  │  - No SIGSTOP needed       │    │  - PTRACE_DETACH all     │
  │                            │    │  - Valkey is live!       │
  └────────────────────────────┘    └──────────────────────────┘
```

### eBPF Dirty Page Tracking

The key optimization. A BPF program hooks `fentry/do_wp_page` in the
kernel, filtering by the target PID. Every write-protect fault pushes
the faulting page address to a 64MB ring buffer.

At T3, the page server drains the ring in microseconds — O(dirty_pages)
instead of walking all 50M+ page table entries with PAGEMAP_SCAN
(O(total_pages), ~150ms). This reduced T3 freeze from 170ms to 29ms.

**Safety**: Ring buffer drops are detected via a BPF counter. If the
64MB ring fills up, the page server falls back to PAGEMAP_SCAN
automatically — no silent data loss.

**Critical**: BPF attaches immediately after WP in `cr-dump.c` (zero
gap). Any gap causes missed dirty pages → futex deadlock on replica.

### Non-lazy Page Re-send

Stacks and file-backed rw segments (.data/.bss of libc, ld.so, etc.)
are not WP-tracked. They're re-sent from the frozen source at T3 for
memory consistency. 127 pages (508KB) total.

### What Gets Transferred

| Resource | How |
|----------|-----|
| Memory (heap, mmap) | 8-stream bulk + eBPF convergence via page-recv |
| Stacks + lib .data | Non-lazy re-send from frozen source at T3 |
| CPU registers | T3 capture + apply via PTRACE_SETREGSET |
| File descriptors | CRIU image files |
| TCP sockets | Closed (`--tcp-close`), clients reconnect |
| Signal handlers | T3 capture via parasite RPC |
| New VMAs (jemalloc) | VMA diff protocol + ptrace mmap injection |

## Performance

Tested on m7g.16xlarge (494GB RAM, 64 CPUs), same-AZ VPC.

| Test | Size | Transfer | Throughput | Freeze | Result |
|------|------|----------|------------|--------|--------|
| Live traffic | 100GB | 29s | 3,381 MB/s | 55ms | **7/7** |
| Live traffic | 200GB | 59s | 3,313 MB/s | ~55ms | **7/7** |

**Source unavailability: 55ms total** (26ms dump + 29ms T3, two separate windows).

### Stage Timing (100GB + live traffic)

| Stage | Duration | Notes |
|-------|----------|-------|
| Dump freeze | **26 ms** | Seize + maps + parasite + pagemap |
| WP setup (post-resume) | 35 ms | WP_ASYNC, 63 threads, not frozen |
| eBPF attach | 0 ms | Immediately after WP |
| Sigacts capture | 2 ms | Direct ptrace injection, pre-freeze |
| Bulk transfer (8 streams) | 29 s | LZ4, process_vm_readv |
| T3 freeze | **29 ms** | eBPF drain + regs + FDs + dirty + non-lazy + VMA diff |
| **Total source frozen** | **55 ms** | 26ms dump + 29ms T3 |

### vs REPLICAOF

| Metric | COW Migration | REPLICAOF |
|--------|--------------|-----------|
| 200GB transfer | **59s** | >20min |
| Throughput | **3.4 GB/s** | ~500 MB/s |
| Source freeze | **55ms** | ~1.7s (BGSAVE fork) |
| Source memory spike | None | 2× RSS (fork COW) |

## Verification Tests (7/7)

1. **Migration completed** — full flow without errors
2. **Replica PONG** — responds to commands
3. **Key count match** — same keys as source
4. **Memory within 10%** — 0.0% divergence in practice
5. **500-key spot check** — random keys compared byte-by-byte
6. **BGSAVE success** — heap consistent, no corruption
7. **RANDOMKEY type check** — can read and identify key types

## Key Files

| File | Role |
|------|------|
| `criu/bpf/dirty_track.bpf.c` | eBPF program (fentry/do_wp_page) |
| `criu/cow-bpf.c` | eBPF userspace: load, drain, stop |
| `criu/page-xfer.c` | Page server, 8-stream bulk, T3 capture |
| `criu/cow-dump.c` | WP_ASYNC tracking, userfaultfd injection |
| `criu/cr-dump.c` | Dump orchestration, early resume, BPF start |
| `criu/cr-restore.c` | Restore, T3 state loading |
| `tools/page-recv.c` | Standalone receiver, process_vm_writev |
| `scripts/migrate.sh` | Source orchestration |
| `scripts/restore.sh` | Replica orchestration |
| `scripts/verify-migration.sh` | 7-test verification suite |

## Requirements

- Linux kernel 6.7+ (userfaultfd WP_ASYNC + PAGEMAP_SCAN)
- `sysctl vm.unprivileged_userfaultfd=1`
- `/sys/kernel/btf/vmlinux` (BTF for eBPF)
- clang, bpftool, libbpf-dev (eBPF build toolchain)
- aarch64 or x86_64
- Both machines reachable over TCP

## Troubleshooting

### Permission denied for userfaultfd
```bash
echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd
```

### Disk full on replica during BGSAVE
BGSAVE forks the process — needs RSS + RDB file space. For large
datasets, ensure replica disk > 2× dataset size.

### Replica io-threads config
The replica's `/etc/valkey/valkey.conf` must match the source's
`io-threads` setting. Mismatch can cause restore issues.

### BPF fails to attach
Requires `CAP_BPF` (or root) and BTF kernel support. Falls back to
PAGEMAP_SCAN if BPF attach fails, but this may cause futex deadlock
on the replica due to missed dirty pages.

## Limitations

1. **Transfer speed**: ~3.3 GB/s, limited by `process_vm_readv`.
2. **Replica RAM**: must fit the full dataset.
3. **Client connections**: closed on dump (`--tcp-close`).
4. **x86_64**: tested on aarch64. x86_64 expected to work but not yet
   validated at scale.
5. **aarch64 only**: T3 register application uses a glibc-specific
   fix (x0→x19 for interrupted syscalls). x86_64 will need an
   equivalent for `orig_rax`.
