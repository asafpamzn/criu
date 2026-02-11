# Live Migration Latency Investigation — Complete Handover

## TL;DR

We set out to eliminate source-side write latency during CRIU-based live
migration of Valkey. We succeeded at the write-protection layer (WP_ASYNC:
13μs max write latency on 100GB, down from 10-50μs per write). But we hit
a hardware wall: any cross-process memory read (`process_vm_readv`) pollutes
the shared L3 cache, causing 1.5-3ms avg latency for the entire transfer
duration (84 seconds for 100GB). This is not solvable in software on
current hardware. The path forward is application-level transfer inside
Valkey's upcoming thread-save, using per-object WP_ASYNC.

## What We Built

### Branch: `work/wp-async-cow`
PR: https://github.com/asafpamzn/criu/pull/14

Replaced the synchronous userfaultfd write-protect monitor thread with
`UFFD_FEATURE_WP_ASYNC` + `PAGEMAP_SCAN`.

### Changes (5 commits)

```
30df4fc84 perf: add SO_SNDBUF tuning, TCP cork batching, dirty-delta resend
47e177163 perf: eliminate per-page malloc, cache pagemap fd, fix spinlock-during-I/O
6ca2f8777 fix: review feedback — EINVAL sign, dead code, errno handling
a73c13885 docs: add WP_ASYNC investigation results and proposed direction
8f519f677 cow: replace synchronous WP monitor with WP_ASYNC + PAGEMAP_SCAN
```

### Architecture Change

**Before (synchronous WP):**
```
Write to WP'd page
  → page fault → userfaultfd delivers to monitor thread
  → monitor: xmalloc 4KB buffer
  → monitor: process_vm_readv (copy original page content)
  → monitor: insert into 65K-bucket hash table with spinlock
  → monitor: UFFDIO_WRITEPROTECT(mode=0) to unprotect
  → monitor: UFFDIO_WAKE to resume writer
  → writer resumes
Per-write latency: 10-50μs
```

**After (WP_ASYNC):**
```
Write to WP'd page
  → page fault → kernel clears WP bit → write proceeds
Per-write latency: 1-2μs
```

### Files Changed

| File | Lines Before → After | What Changed |
|---|---|---|
| `criu/cow-dump.c` | 986 → 475 | Removed: monitor thread, 65K hash table, page queue, spinlocks. Added: `cow_dump_apply_writeprotect()` (chunked 1GB ioctls), `cow_dump_scan_dirty()` (PAGEMAP_SCAN + PM_SCAN_WP_MATCHING), cached pagemap fd |
| `criu/page-xfer.c` | ~3200 → ~3100 | Removed: COW overlay (send_cow_page_lazy, drain_cow_pages), per-page unprotect. Added: batched process_vm_readv (128 pages/call), persistent per-thread mmap buffer, notification pipe for close protocol, TCP cork batching, SO_SNDBUF 4MB, dirty-delta resend loop, remaining_pages accounting fix |
| `criu/cr-dump.c` | | Removed: cow_start_monitor_thread() calls. Added: deferred WP post-resume with timing |
| `criu/pie/parasite.c` | | Enable UFFD_FEATURE_WP_ASYNC, defer WRITEPROTECT to post-resume |
| `criu/include/cow-dump.h` | 194 → 93 | Removed: hash/queue/monitor declarations. Added: apply_writeprotect, scan_dirty |

### Test Programs

| File | Purpose |
|---|---|
| `test/wp-async-latency.c` | Proves WP_ASYNC doesn't block writes during WP setup. Allocates N GB region, registers with WP_ASYNC, applies WRITEPROTECT while writer thread measures per-write latency. |
| `test/latency-bench.c` | Per-second SET/GET latency tracker with reconnect support. Reports ops/sec, avg, p50, p99, max per second. Used to measure source-side impact during migration. |

## Benchmark Results

### Test Environment

- AWS Graviton 3 (Neoverse-V1), 32 cores, 1 NUMA node
- L3: 32MB shared across all cores
- Kernel: Linux 6.14.0-1018-aws (aarch64)
- Network: 10Gbps between source and replica instances
- Valkey: single-threaded, jemalloc allocator

### WP_ASYNC Write Latency (test/wp-async-latency)

```
Region: 100 GB (26,214,400 pages)
WRITEPROTECT time: 392.885 ms
Writer samples:    565,676
Max write latency: 13.533 μs
Avg write latency: 0.679 μs

Histogram:
  <1μs           376,190  (66.5%)
  1-10μs         189,477  (33.5%)
  10-100μs             9  ( 0.0%)
```

**Key finding**: `UFFDIO_WRITEPROTECT` uses `mmap_read_lock` (shared) on
Linux 6.14. Multiple threads can hold it concurrently. WP_ASYNC page fault
resolution also uses shared locks. Therefore WP setup does NOT block
process writes.

### Source-Side Latency During Migration

#### 7.35 GB Valkey (10GB fill, ~1.8M pages)

```
Phase              Duration    ops/sec     avg       p50       p99       max
Baseline           30s         52K         18μs      18μs      23μs      110μs
Freeze             35ms        —           —         —         —         reconnect
WP setup           49ms        45K         20μs      18μs      23μs      6.5ms
Page transfer      10s         340-610     1.6-2.9ms 1.4-2.2ms 2.8-6.5ms 3-7ms
Recovery           3s          3-52K       20-28μs   18μs      23μs      varies
Post-migration     ongoing     52K         18μs      18μs      23μs      110μs
```

#### 122 GB Valkey (100GB fill, ~27M pages)

```
Phase              Duration    ops/sec     avg       p50       p99       max
Baseline           30s         52K         18μs      18μs      23μs      125μs
Freeze             35ms        38K         25μs      19μs      25μs      233ms
WP setup           438ms       44K         21μs      19μs      25μs      6.6ms
Xfer early (s32-50) 18s        350-490     2-3ms     1.7-2.3ms 3.5-6.5ms 5-7ms
Xfer mid (s50-80)  30s         470-520     1.9-2.1ms 1.6-1.7ms 3.2-3.5ms 3.5-4.7ms
Xfer late (s80-116) 36s        550-630     1.6-1.8ms 1.4-1.5ms 2.8-3ms   2.9-3.1ms
Recovery           3s          13-50K      19-97μs   19μs      24μs      varies
Post-migration     ongoing     50-52K      18μs      18-19μs   23-24μs   125μs
```

### CRIU Timing Breakdown (100GB)

```
dump_one_task TOTAL:              35ms    (freeze window)
  parse_maps_cow:                 1.5ms
  cow_dump_init (REGISTER only):  4ms
  parasite_dump_pages_seized:     4.5ms
  compel_stop_daemon:             13ms
  dump_task_threads:              8ms
cow_dump_apply_writeprotect:      438ms   (post-resume, WP_ASYNC, no write stall)
Page transfer:                    84s     (L3 cache eviction, hardware-limited)
```

## What We Tested and Ruled Out

### Approaches to Reduce L3 Cache Eviction During Transfer

Every test showed identical latency profiles (~1.5-3ms during transfer).
The L3 eviction is fundamental to `process_vm_readv`.

| Approach | Expected | Actual | Why It Failed |
|---|---|---|---|
| CPU affinity (pin to last core) | Isolate L3 | No change | L3 is shared across all Graviton cores |
| Throttle 50μs between batches | Reduce burst rate | No change | Transfer is already network-paced (~735MB/s) |
| Throttle 1ms between batches | More aggressive pacing | No change | Same — usleep dwarfed by send() time |
| Batch size 4 pages (was 128) | Smaller cache burst | No change | Total bytes through L3 is the same |
| Remove LZ4 compression | Fewer cache passes | No change | Compression not the bottleneck |
| Sleep before process_vm_readv | Pace the reads | No change | Reads already paced by network sends |
| Sleep after batch send | Pace full pipeline | No change | Pipeline already network-limited |
| Rate limiter (300MB/s target) | Below L3 eviction threshold | No change | Rate limiter didn't bite — already below target |
| SO_SNDBUF 4MB | Avoid send stalls | Marginal | Bottleneck is process_vm_readv, not TCP |
| TCP cork batching | Coalesce packets | Marginal | Same bottleneck |

### Root Cause Analysis

`process_vm_readv` calls `__access_remote_vm()` in the kernel, which uses
`memcpy` with temporal loads (standard `LDP`/`STP` on ARM64, `MOV` on x86).
Each 4KB page brings 64 cache lines into L3. For 100GB:

- 27M pages × 64 cache lines = 1.7 billion cache line loads
- 32MB L3 / 64B per line = 524K lines total
- L3 is completely flushed ~3,300 times during the 84-second transfer
- Every cache line in Valkey's working set is evicted and must be re-fetched
  from main memory (~150ns penalty per access)

### Approaches That Would Work But Are Unavailable

| Approach | Expected Impact | Why Unavailable |
|---|---|---|
| ARM MPAM cache partitioning | 90-95% reduction | Not exposed on AWS Graviton |
| RDMA / EFA DMA bypass | ~100% elimination | Requires EFA instance type |
| Non-temporal kernel memcpy (LDNP/STNP) | 50-80% reduction | Requires kernel patch, ARM hint behavior uncertain |
| DC CIVAC cache flush | 25-35% reduction | ARM64 only, not cross-platform |

### Pre-copy Analysis

Slow transfer before freeze, then dirty delta after:

| Transfer Rate | Time for 100GB | L3 Impact | Practical? |
|---|---|---|---|
| 2 GB/s | 50s | ~5% latency increase | Fast but noticeable |
| 1 GB/s | 100s | ~2% | Network-limited anyway |
| 500 MB/s | 200s | <1% | Too slow |

**Problem**: For write-heavy workloads, dirty delta after pre-copy can be
enormous (dirty rate × pre-copy time). Active defrag, large object deletion,
FLUSHDB — all break convergence. Final freeze time becomes unpredictable.

### Edge Cases That Break Dirty Convergence

- Active defrag: touches 1-5% of pages/second continuously
- Large object mutation (DEL 10GB hash): millions of pages dirtied instantly
- Jemalloc metadata churn: scattered dirty pages from every alloc/free
- Background tasks (AOF rewrite, key expiry, lazy-free)
- FLUSHDB: entire dataset dirty in one command

## The Fundamental Constraint

Any approach that reads another process's memory via the CPU goes through
the kernel's `__access_remote_vm()` which uses temporal `memcpy`. This
pollutes the shared L3 cache. On platforms without hardware cache
partitioning (MPAM) or DMA bypass (RDMA), this is unavoidable.

**WP_ASYNC solved the write-protection latency** (13μs max on 100GB).
**But the data-movement latency requires a different architecture.**

## Proposed Direction: Per-Object WP_ASYNC in Valkey Thread-Save

### The Insight

The L3 cache eviction happens because CRIU reads memory from OUTSIDE the
target process (`process_vm_readv` from a separate process). If the
serialization happens INSIDE the target process, the data being read is
already hot in cache — no cross-process reads, no L3 eviction.

Valkey's upcoming thread-save contribution replaces fork-based BGSAVE with
a background thread that serializes data. The known problem: large object
serialization blocks writes to that object (application-level lock).

### Per-Object WP_ASYNC

Use `UFFD_FEATURE_WP_ASYNC` surgically on per-object pages during
thread-save serialization:

1. Thread-save begins serializing a hash/set/sorted-set
2. Identify the memory pages backing that specific object
3. WP just those pages (not the whole address space) — tiny WP scope
4. Serialize the object (reads are unaffected by WP)
5. If another thread writes to the object, WP_ASYNC auto-resolves (~1-2μs)
6. After serialization, PAGEMAP_SCAN detects if pages were dirtied
7. If dirtied, re-serialize only the modified portions
8. Move to next object

### Why This Avoids Both Problems

**No L3 eviction:**
- Serialization thread is in the same process — same address space, same cache
- Pages being serialized are already hot (the object is in use by Valkey)
- No `process_vm_readv`, no cross-process memcpy

**No large-object lock:**
- WP_ASYNC doesn't block writes (kernel auto-resolves, ~1-2μs per write)
- The object is never locked — concurrent writes proceed at near-zero latency
- Consistency ensured by PAGEMAP_SCAN detecting dirty pages after serialization
- Only modified portions re-serialized

**Surgical WP scope:**
- Only a few pages protected at a time (the current object being serialized)
- Not the entire 100GB address space
- Minimal PAGEMAP_SCAN overhead (scanning a few pages, not millions)

### Implementation Sketch

```c
/* In Valkey's thread-save background thread */
void serialize_object_with_wp_async(robj *obj) {
    void *ptr = obj->ptr;
    size_t size = zmalloc_usable_size(ptr);
    unsigned long start = (unsigned long)ptr & ~(PAGE_SIZE - 1);
    unsigned long end = ((unsigned long)ptr + size + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);

    /* WP just this object's pages */
    struct uffdio_writeprotect wp = {
        .range = { .start = start, .len = end - start },
        .mode = UFFDIO_WRITEPROTECT_MODE_WP,
    };
    ioctl(uffd, UFFDIO_WRITEPROTECT, &wp);

    /* Serialize (reads only, WP doesn't affect reads) */
    rdbSaveObject(rdb, obj);

    /* Check if any page was dirtied during serialization */
    struct pm_scan_arg scan = {
        .size = sizeof(scan),
        .flags = PM_SCAN_WP_MATCHING,
        .start = start, .end = end,
        .category_anyof_mask = PAGE_IS_WRITTEN,
        .return_mask = PAGE_IS_WRITTEN,
        .vec = (u64)regions, .vec_len = max_regions,
    };
    int dirty = ioctl(pagemap_fd, PAGEMAP_SCAN, &scan);

    if (dirty > 0) {
        /* Object was modified — re-serialize the dirty parts */
        rdbResaveObject(rdb, obj, regions, dirty);
    }
}
```

### Requirements

- Linux 6.1+ (`UFFD_FEATURE_WP_ASYNC`)
- Linux 6.7+ (`PAGEMAP_SCAN` with `PM_SCAN_WP_MATCHING`)
- Valkey thread-save implementation (upcoming contribution)
- Mapping from Valkey objects to their backing memory pages
- Integration with replication protocol for remote transfer

### Open Questions

1. **Object-to-page mapping**: Valkey objects are allocated via jemalloc.
   A single object may span multiple pages, and a single page may contain
   multiple small objects. How to WP-protect one object without affecting
   neighbors on the same page?

2. **Nested objects**: A hash contains many field-value pairs, each
   separately allocated. WP-protecting the parent hash doesn't protect
   the children. Need to walk the object's allocation tree.

3. **jemalloc slab pages**: Small objects share slab pages. WP on a slab
   page affects all objects in that slab — could cause false positives
   in dirty detection.

4. **Replication protocol integration**: Thread-save produces an RDB stream.
   For live migration, this stream needs to go to a remote replica. The
   existing PSYNC protocol handles this, but the consistency model changes
   with per-object WP_ASYNC.

5. **Fallback for objects that can't be WP'd**: Some VMAs may not support
   userfaultfd WP registration. Need a fallback path (copy the object
   before serializing, like the current thread-save approach).

## Repository Layout

```
work/cow-dump-freeze-opt    — Original optimized COW dump (sync WP, production)
work/deferred-writeprotect  — Deferred WP + close protocol fix
work/wp-async-cow           — WP_ASYNC + PAGEMAP_SCAN (this work)
```

## Key Files Reference

### CRIU (branch: work/wp-async-cow)

| File | Key Functions | Purpose |
|---|---|---|
| `criu/cow-dump.c` | `cow_dump_init`, `cow_dump_apply_writeprotect`, `cow_dump_scan_dirty`, `cow_dump_fini` | WP_ASYNC lifecycle: uffd setup via parasite RPC, deferred chunked WP, PAGEMAP_SCAN dirty tracking |
| `criu/page-xfer.c` | `send_lazy_pages_batch`, `process_vma_pages`, `unified_page_server_thread`, `send_image_complete` | Batched page reading (128 pages/call), priority-sorted VMA iteration, dirty-delta resend, fire-and-forget close with notification pipe |
| `criu/cr-dump.c` | `cr_dump_finish` (COW path) | Orchestration: freeze → REGISTER → resume → WP (post-resume) → page server |
| `criu/pie/parasite.c` | `parasite_cow_dump_init` | Runs in target process: creates uffd with WP_ASYNC, registers VMAs (WP deferred) |
| `criu/include/cow-dump.h` | | API: init, fini, apply_writeprotect, scan_dirty, is_vma_tracked |

### Valkey (for next phase)

| File | Key Functions | Purpose |
|---|---|---|
| `src/rdb.c` | `rdbSaveBackground`, `rdbSaveRio`, `rdbSaveDb`, `rdbSaveObject` | RDB serialization — entry point for thread-save integration |
| `src/replication.c` | `syncCommand`, `startBgsaveForReplication`, `primaryTryPartialResynchronization` | Replication protocol — FULLRESYNC and PSYNC flows |
| `src/server.h` | `struct serverObject` | Object layout — ptr field points to data, need to map to pages |
| `src/rio.h` | `struct _rio` | I/O abstraction — supports buffer, file, socket, connset targets |

## What Next

### Phase 1: Prototype WP_ASYNC Inside Valkey (Proof of Concept)

**Goal**: Prove that per-object WP_ASYNC eliminates the latency spike during
serialization, using a standalone test inside Valkey.

1. **Add userfaultfd setup to Valkey startup**
   - Create uffd with `UFFD_FEATURE_WP_ASYNC` in `server.c:initServer()`
   - Register the heap region(s) with `UFFDIO_REGISTER_MODE_WP`
   - Open `/proc/self/pagemap` for `PAGEMAP_SCAN`
   - Store uffd and pagemap_fd in `server` struct

2. **Build a `DEBUG WP-SERIALIZE` test command**
   - Pick a large key (e.g., a hash with 1M fields)
   - WP the pages backing the object
   - Serialize to a buffer using existing `rdbSaveObject()`
   - `PAGEMAP_SCAN` to check if any pages were dirtied
   - Log: serialization time, dirty pages found, re-serialization count
   - Unprotect (or let WP_ASYNC handle it)
   - Run `latency-bench` during this to measure write latency impact

3. **Validate zero L3 eviction**
   - The serialization thread reads from its OWN address space
   - Data is already in cache — no cross-process reads
   - Benchmark should show baseline latency throughout serialization

**Files to modify**: `src/server.c`, `src/server.h`, `src/debug.c`
**Dependencies**: Linux 6.1+ (WP_ASYNC), Linux 6.7+ (PAGEMAP_SCAN)
**Expected outcome**: Serialization of a 1GB hash with <50μs max write
latency impact (vs current lock-based approach which blocks for the
entire serialization duration)

### Phase 2: Integrate with Thread-Save

**Goal**: Replace thread-save's per-object locking with per-object WP_ASYNC.

4. **Hook into thread-save's object iteration loop**
   - Before serializing each object: WP its pages
   - After serializing: PAGEMAP_SCAN for dirty detection
   - If dirty: re-serialize modified portions
   - This replaces the application-level copy-on-write / object lock

5. **Handle jemalloc slab sharing**
   - Small objects share slab pages — WP on a slab page affects neighbors
   - Options: (a) accept false positives (re-serialize more than needed),
     (b) WP only for large objects (>PAGE_SIZE), use existing locking for small ones,
     (c) batch serialize all objects on the same slab page together

6. **Handle nested allocations**
   - A hash's field-value pairs are separately allocated
   - Need to walk the object's allocation tree to find all pages
   - For each object type: STRING is contiguous, HASH/SET/ZSET have
     hashtable + entries, LIST has quicklist nodes
   - `rdbSaveObject()` already knows how to iterate each type — WP
     before the iteration begins, scan after it completes

### Phase 3: Integrate with Replication for Live Migration

**Goal**: Use thread-save with WP_ASYNC as the data source for live migration.

7. **Stream RDB to remote replica via replication protocol**
   - Thread-save produces an RDB byte stream (via `rio`)
   - Route this stream to the replication channel (existing `rdbSaveToReplicasSockets` pattern)
   - Replica loads the stream as a standard full-sync

8. **Use PSYNC for the delta**
   - During thread-save, all write commands go to the replication backlog
   - After RDB stream completes, replica catches up via PSYNC
   - This is the standard replication flow — no CRIU needed

9. **Cutover**
   - When replica is caught up (backlog offset matches), promote replica
   - Redirect clients to the new instance
   - Client reconnection handled by standard Redis/Valkey client libraries

### Phase 4: Benchmark and Validate

10. **Compare with current approaches**
    - Baseline: fork-based BGSAVE + replication (current)
    - Thread-save without WP_ASYNC (application-level locking)
    - Thread-save with per-object WP_ASYNC (our approach)
    - CRIU COW dump (our current implementation, for reference)
    - Metrics: source p99 latency, total migration time, memory overhead

11. **Test edge cases**
    - Large objects (1GB+ hashes, sorted sets)
    - Active defrag during migration
    - High write rate during migration
    - Object resize / rehash during serialization
    - Memory pressure (jemalloc arena contention)

### Key Decision Points

- **Phase 1 result determines viability**: If the `DEBUG WP-SERIALIZE`
  test shows >100μs write latency, the jemalloc slab sharing problem
  is worse than expected and we need a different approach.

- **Phase 2 depends on thread-save availability**: The upstream thread-save
  contribution must land (or we fork it) before integration work begins.

- **Phase 3 may not need CRIU at all**: If thread-save + replication
  handles migration correctly, CRIU becomes unnecessary for Valkey
  migration. CRIU's value would be limited to non-Valkey workloads.

## Proven Facts (with test data)

1. **WP_ASYNC max write latency: 13μs** (100GB, 565K samples, 393ms WP duration)
2. **UFFDIO_WRITEPROTECT uses mmap_read_lock** (shared, not exclusive) on Linux 6.14
3. **process_vm_readv L3 pollution is per-byte, not per-syscall** — throttling/batching/affinity don't help
4. **LZ4 compression is not the L3 bottleneck** — removing it shows identical latency
5. **Transfer is network-paced at ~735MB/s** — usleep between batches has no effect
6. **100GB transfer takes 84 seconds** with continuous 1.5-3ms avg latency on source
7. **Dirty convergence breaks for write-heavy workloads** — active defrag, large object mutations, jemalloc churn
8. **Per-object WP_ASYNC inside the process avoids L3 eviction entirely** — data already hot in cache
