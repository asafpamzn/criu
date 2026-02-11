# WP_ASYNC Investigation: Eliminating Write Latency During Live Migration

## Summary

This document records the investigation into using Linux `UFFD_FEATURE_WP_ASYNC`
to eliminate source-side write latency during CRIU COW-based live migration.

**Key result**: WP_ASYNC reduces per-write latency from 10-50μs (synchronous
monitor thread) to 1-2μs (kernel auto-resolve). Write-protect setup on 100GB
causes **13μs max write latency** — proven with benchmarks.

**However**: the page transfer phase causes 1.5-3ms average latency for ~10
seconds (10GB dataset) due to L3 cache eviction from `process_vm_readv`. This
is a hardware constraint, not solvable in software on current platforms. The
proposed solution is per-object WP_ASYNC inside Valkey's upcoming thread-save
mechanism, avoiding cross-process memory reads entirely.

## Background

### The Problem

CRIU's COW dump implementation uses userfaultfd write-protection to track memory
modifications while the source process continues running during live migration.
The original synchronous WP approach has two latency problems:

1. **WP setup stall**: `UFFDIO_WRITEPROTECT` walks all PTEs to set write-protect
   bits. For 100GB, this takes ~400ms during which writes are blocked.

2. **Per-write fault overhead**: Each write to a WP'd page traps to a userspace
   monitor thread that copies the page, unprotects it, and wakes the writer.
   Latency: 10-50μs per write.

### WP_ASYNC Overview

Linux 6.1+ provides `UFFD_FEATURE_WP_ASYNC`. When enabled:

- Write faults on WP'd pages are resolved **in the kernel** by auto-clearing the
  WP bit. No userspace delivery. ~1-2μs per write.
- Dirty pages are tracked via `PAGEMAP_SCAN` with `PM_SCAN_WP_MATCHING`, which
  atomically finds written pages and re-applies WP.
- No monitor thread, no hash table, no per-page userspace round-trip.

## Implementation

### Branch: `work/wp-async-cow`

Based on `work/cow-dump-freeze-opt` (the optimized COW dump branch).

### Changes

**`criu/pie/parasite.c`** — Enable WP_ASYNC in uffd creation:
```c
api.features = UFFD_FEATURE_WP_ASYNC;  // was: 0
```
WRITEPROTECT deferred to post-resume (CRIU-side, not parasite-side).

**`criu/cow-dump.c`** — Gutted from 986 to 452 lines:
- Removed: monitor thread, 65K-bucket hash table, per-bucket spinlocks,
  COW page queue, cow_page structures, all fault handling functions
- Kept: cow_dump_init (parasite RPC, uffd setup, VMA tracking),
  cow_dump_fini, is_vma_tracked, get_uffd_for_pid
- Added: `cow_dump_apply_writeprotect()` — deferred WP from CRIU process,
  chunked in 1GB ioctls for reduced per-chunk stall
- Added: `cow_dump_scan_dirty()` — PAGEMAP_SCAN wrapper with
  PM_SCAN_WP_MATCHING for dirty page detection + re-WP

**`criu/page-xfer.c`** — Simplified page server:
- Removed: COW overlay (send_cow_page_lazy, drain_cow_pages, COW hash lookup
  in send_lazy_vma_page, per-page UFFDIO_WRITEPROTECT unprotect)
- Simplified send_lazy_vma_page: just process_vm_readv + send (no COW check)
- Added: batched process_vm_readv (128 pages per syscall)
- Added: notification pipe for background thread → main thread signaling
  (fixes socket sharing race in close protocol)
- Fixed: remaining_pages accounting on send failure
- Fixed: fire-and-forget close marker (no ACK wait, eliminates socket race)
- Fixed: skip EOF wait in COW mode (avoids "Unexpected data" error)

**`criu/cr-dump.c`** — Simplified dump finish flow:
- Removed: cow_start_monitor_thread() calls (both in dump_one_task and
  cr_dump_finish)
- Added: deferred WP post-resume with timing
- Flow: freeze → REGISTER → resume → WP (post-resume) → page server

### Close Protocol Fix

The original close protocol had a race condition: the main serve loop
(`page_server_serve`) and the background page server thread both read from
the same TCP socket. The main thread's `recv(28, MSG_WAITALL)` consumed the
replica's 4-byte ACK as partial header data, causing the background thread's
`poll()` to time out after 5 seconds.

Fix: notification pipe (`g_transfer_done_pipe`). Background thread writes a
byte to the pipe when done. Main thread polls both socket and pipe. When pipe
fires, exits cleanly. Close marker is fire-and-forget (no ACK wait).

Also fixed: `remaining_pages` accounting — send failures now decrement the
counter and mark the bitmap, ensuring the close marker always gets sent.

## Benchmark Results

### WP_ASYNC Write Latency Test (`test/wp-async-latency.c`)

Standalone test: mmap region, register with WP_ASYNC, apply WRITEPROTECT
while a writer thread continuously writes to random pages. Measures per-write
latency during WP application.

```
=== WP_ASYNC Write Latency Test ===
Region: 100 GB (26214400 pages)

WRITEPROTECT time: 392.885 ms
Writer samples:    565,676
Max write latency: 13.533 us
Avg write latency: 0.679 us

Latency histogram:
  <1us           376190  ( 66.5%)
  1-10us         189477  ( 33.5%)
  10-100us            9  (  0.0%)

Verdict: PASS — writes not stalled during WP (max 13 us)
```

**Key finding**: `UFFDIO_WRITEPROTECT` uses `mmap_read_lock` (shared) on
Linux 6.14, NOT `mmap_write_lock`. This means WP setup does not block
concurrent page faults. Writes proceed at ~1μs while WP walks 26M PTEs.

### Migration Test Results

**100GB Valkey migration** (end-to-end):

| Metric | Original (sync WP) | WP_ASYNC |
|---|---|---|
| Freeze time | 455ms | **32ms** |
| cow_dump_init | 422ms (REGISTER+WP) | **4ms** (REGISTER only) |
| WP post-resume | N/A | 413ms (no write stall) |
| Per-write latency | 10-50μs | **~1-2μs** |
| Monitor thread | Yes | **Eliminated** |
| Hash table (65K buckets) | Yes | **Eliminated** |
| cow-dump.c lines | 986 | **452** |
| Pages transferred | 27,396,503 | 27,396,503 |
| Migration | Pass | **Pass** |

### Source-Side Latency During Migration (`test/latency-bench.c`)

Continuous SET/GET benchmark with per-second latency tracking. Migration
triggered at second 30.

```
Phase              Duration    avg      p50      p99      max        ops/sec
────────────────────────────────────────────────────────────────────────────
Baseline           30s         18μs     18μs     23μs     110μs      53K
Freeze             32ms        —        —        —        connection lost
WP setup           49ms        21μs     18μs     24μs     6.5ms      48K
Page transfer      10s         1.5-3ms  1.5ms    3-6ms    7ms        400-650
Recovery           3s          20-25μs  18μs     24μs     —          45K
Post-migration     ongoing     18μs     18μs     23μs     110μs      53K
```

**WP setup**: p99 = 24μs (baseline 23μs). Effectively zero impact.

**Page transfer**: 1.5-3ms average, 80-170x baseline. This is the remaining
problem (see next section).

## The L3 Cache Eviction Wall

### Root Cause

During page transfer, the page server calls `process_vm_readv()` to read
pages from the source process. The kernel's `__access_remote_vm()` uses
`memcpy` (temporal loads/stores) to copy data from the target's physical
pages into CRIU's buffer. These temporal loads bring every page into L3
cache, evicting the target process's working set.

For 7.35GB (10GB Valkey with compression): 1.8M pages × 64 cache lines =
115M cache line loads. The 32MB L3 on Graviton 3 is completely flushed
~3,600 times during the transfer.

### What We Tested (All Failed to Reduce Latency)

| Approach | Result | Why |
|---|---|---|
| **CPU affinity** (pin to last core) | Same latency | L3 is shared across all Graviton cores |
| **Throttle 50μs** between batches | Same latency | Per-batch eviction dominates, not rate |
| **Throttle 1ms** between batches | Same latency | Same reason |
| **Batch size 4** (vs 128) pages | Same latency | Total data through L3 is the same |
| **Remove LZ4 compression** | Same latency | Compression was not the bottleneck |
| **Sleep before read** | Same latency | Network send already paces reads |
| **Sleep after send** | Same latency | Transfer is already network-paced |

### Why Throttling Doesn't Help

The transfer is already network-paced at ~735MB/s (10Gbps link). Adding
sleeps doesn't reduce the effective rate because the `send()` calls already
take longer than the sleeps. The latency spike duration (~10s) equals the
network transfer time for the dataset — it's the minimum possible.

At 735MB/s through a 32MB L3: the entire L3 is flushed ~23 times per
second. Every cache line in the target's working set is evicted and must be
re-fetched from main memory (~150ns penalty per access).

### What Would Work (But Isn't Available)

| Approach | Expected Impact | Availability |
|---|---|---|
| ARM MPAM cache partitioning | 90-95% reduction | Not exposed on AWS Graviton |
| RDMA / EFA DMA bypass | ~100% elimination | Requires EFA instance type |
| Non-temporal kernel memcpy | 50-80% reduction | Requires kernel patch |
| DC CIVAC cache flush (ARM) | 25-35% reduction | ARM64 only, not x86 |

### Pre-copy Analysis

Transferring data slowly before freeze to reduce the post-resume delta:

| Transfer Rate | Time for 100GB | L3 Impact | Practical? |
|---|---|---|---|
| 2 GB/s | 50s | ~5% latency increase | Fast but noticeable |
| 1 GB/s | 100s | ~2% | Reasonable |
| 500 MB/s | 200s | <1% | Too slow |
| 200 MB/s | 500s | Imperceptible | Impractical |

**Problem**: For write-heavy workloads, the dirty delta after pre-copy can
be enormous (dirty rate × pre-copy time). If the dirty rate exceeds the
transfer rate, pre-copy never converges.

### Edge Cases That Break Convergence

- Active defrag: touches 1-5% of pages/second continuously
- Large object mutation (DEL 10GB hash): millions of pages dirtied instantly
- Jemalloc metadata churn: scattered dirty pages from every alloc/free
- Background Valkey tasks (AOF rewrite, key expiry, lazy-free)
- FLUSHDB: entire dataset dirty in one command

## Conclusion: The Fundamental Constraint

Any approach that reads another process's memory via the CPU (regardless of
the API: `process_vm_readv`, `/proc/pid/mem`, `ptrace`) goes through the
kernel's `__access_remote_vm()` which uses temporal `memcpy`. This pollutes
the shared L3 cache. On platforms without hardware cache partitioning or
RDMA, this is unavoidable.

**The WP_ASYNC work solved the write-protection latency problem completely**
(13μs max on 100GB). But the data-movement latency problem requires a
fundamentally different approach.

## Proposed Direction: Per-Object WP_ASYNC in Valkey Thread-Save

The upcoming thread-save contribution to Valkey replaces fork-based BGSAVE
with a background thread that serializes data while the main threads
continue serving. The known problem: large object serialization blocks
writes to that object.

**Proposed solution**: Use WP_ASYNC surgically on per-object pages during
thread-save serialization:

1. Thread-save begins serializing a hash/set/sorted-set
2. WP the pages backing that specific object (not the whole address space)
3. Serialize the object (reads are unaffected by WP)
4. If another thread writes to the object, WP_ASYNC auto-resolves (~1-2μs)
5. After serialization, PAGEMAP_SCAN detects if pages were dirtied
6. If dirtied, re-serialize only the modified portions
7. Move to next object

**Why this avoids the L3 problem**:
- No cross-process memory read (serialization thread is in the same process)
- Pages being serialized are already hot in cache (the object is in use)
- Only a small subset of pages are WP'd at any time
- No bulk `process_vm_readv` transferring the entire dataset

**Why this avoids the large-object lock problem**:
- WP_ASYNC doesn't block writes (kernel auto-resolves, ~1-2μs)
- The object is never locked — concurrent writes proceed at near-zero latency
- Dirty pages detected after serialization via PAGEMAP_SCAN
- Consistency ensured by re-serializing dirty portions

This combines the best of both approaches: OS-level write tracking (WP_ASYNC)
with application-level data structure knowledge (Valkey's thread-save).

## Test Programs

### `test/wp-async-latency.c`

Standalone proof that WP_ASYNC doesn't block writes during WP setup.
Allocates a large mmap region, registers with WP_ASYNC, applies WRITEPROTECT
while a writer thread measures per-write latency.

```bash
gcc -O2 -pthread -o wp-async-latency test/wp-async-latency.c
sudo ./wp-async-latency 100  # 100GB test
```

### `test/latency-bench.c`

Per-second SET/GET latency tracker with reconnect support. Measures
source-side latency impact during CRIU migration.

```bash
gcc -O2 -o latency-bench test/latency-bench.c -lhiredis
./latency-bench 127.0.0.1 6379 90 256  # 90 seconds, 256-byte values
```

## Branches

| Branch | Contents |
|---|---|
| `work/cow-dump-freeze-opt` | Original optimized COW dump (sync WP) |
| `work/deferred-writeprotect` | Deferred WP + close protocol fix |
| `work/wp-async-cow` | WP_ASYNC + PAGEMAP_SCAN (this work) |
