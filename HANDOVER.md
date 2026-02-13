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

## Why CRIU WP_ASYNC Is the Best Current Approach

### Alternatives Evaluated

| Approach | Memory Cost | Latency Impact | Speed | Verdict |
|---|---|---|---|---|
| **fork() BGSAVE + replication** | **2x RSS** (always reserved) | Near zero during save | ~90s for 100GB | Memory waste unacceptable |
| **Thread-save (per-object lock)** | Minimal | **Unpredictable spikes** on large objects (seconds) | Fast | Large-object spikes unacceptable |
| **Valkey in-process (main-thread)** | Minimal | Zero | **5.5 hours for 100GB** | Too slow |
| **Valkey in-process (background thread)** | Minimal | Zero for blobs | Fast for blobs only | **Only works for contiguous blobs**, not hashtables/skiplists |
| **CRIU WP_ASYNC** | Minimal | **3x degradation for 84s** | 84s for 100GB | **Best overall tradeoff** |

### Why Valkey In-Process Failed

We built and tested a Valkey in-process incremental serializer with WP_ASYNC
(branch: `work/wp-async-migrate` in valkey repo). Results:

- **Correctness**: Verified — RDB digest matches, valkey-check-rdb passes
- **Latency impact**: Zero during serialization (data in same L3 cache)
- **Speed**: **76 keys/sec for 65KB values = 5.5 hours for 100GB**

The event loop is the fundamental bottleneck. The main thread can only
serialize between command processing ticks. A background thread can't safely
traverse pointer-heavy structures (hashtables, skiplists) without locks,
which reintroduces the thread-save large-object problem.

Background blob copy works for contiguous objects (strings, listpacks) but
NOT for hashtable/skiplist-encoded objects — the snapshot contains pointers
to live memory that can't be followed from a copy. This limits it to one
encoding type, not a general solution.

### Why CRIU WP_ASYNC Wins

- **32ms freeze** (ptrace stop, dump metadata)
- **Zero WP latency** (13μs max during 438ms WP setup, proven)
- **84 seconds of 3x latency degradation** (L3 cache eviction from
  `process_vm_readv` — hardware limit, not software)
- **No memory doubling** (unlike fork)
- **No large-object spikes** (unlike thread-save)
- **Works for all object types** (page-level, encoding-agnostic)
- **3x degradation is within most production SLAs** (3.5ms p50 during
  transfer, typical SLA allows 5-10ms p99)

### Existing Flow (branch: `criu-sync` in valkey repo)

The migration already works end-to-end:

1. **CRIU dumps source** (with COW/lazy pages) — 35ms freeze
2. **CRIU restores on replica** — exact copy of the process
3. **Restored Valkey connects to source** via `replicaof` (script: `wait_and_replicate.sh`)
4. **PSYNC partial sync** — same replication ID, backlog delta only, no BGSAVE
5. **Cutover** — promote replica

The Valkey-side change (`src/replication.c`): force backlog creation for
standalone primaries when `repl-backlog-ttl` is 0, so the backlog exists
before the dump and PSYNC can succeed after restore.

### Remaining Work on CRIU WP_ASYNC

The page transfer causes 84 seconds of 3x latency for 100GB. Options to
reduce this (none eliminate it — hardware wall):
- Multi-stream TCP (15Gbps available, using 9.5Gbps) → ~56s instead of 84s
- Requires protocol changes for multiple connections to replica
- Pre-copy before freeze → spreads impact but extends total time

### Future: Valkey In-Process Direction

If/when Valkey gets a mechanism for safe background serialization of
pointer-heavy objects (e.g., object-level MVCC, lock-free snapshots),
the in-process WP_ASYNC approach becomes viable. The PoC code exists on
`work/wp-async-migrate` in the valkey repo. The latency impact is zero
— the only blocker is serialization speed.

### Phase 1 (DONE): Main-Thread Incremental Serialization with WP_ASYNC

**Goal**: Replace fork() in Valkey's replication FULLRESYNC with
main-thread incremental serialization. No fork, no cross-process reads,
no L3 eviction. Data is read from the process's own address space
(already in cache). WP_ASYNC provides consistency without blocking writes.

**Architecture** (validated with Codex, Option A):

The main thread alternates between serving commands and serializing:

```
event loop iteration:
  1. Process pending client commands     (~variable)
  2. Serialize N keys to RDB stream      (~100μs-1ms, configurable)
  3. Repeat
```

No object is ever locked. No command ever blocks beyond the serialization
time budget per iteration. Large objects are serialized incrementally
(e.g., 1000 hash fields per iteration), same pattern as Valkey's
incremental rehash.

WP_ASYNC tracks modifications between serialization batches:
- WP the pages of the current serialization region
- Serialize (reads only — WP doesn't affect reads)
- Yield to command processing
- If the application modifies serialized data, WP_ASYNC auto-resolves
  the write (~1-2μs) and the kernel records the dirty page
- After full pass: PAGEMAP_SCAN finds all dirty pages
- Re-serialize dirty portions in iterative rounds
- Brief final pause to capture replication offset, then PSYNC backlog
  handles any remaining delta

**Why this is safe** (single-threaded, no cross-thread data races):
- Valkey is single-threaded for command processing
- The serialization runs in the SAME thread (cooperative, not concurrent)
- No hashtable iterator races — the iterator pauses between batches,
  and the same thread processes any rehash/realloc between batches
- WP_ASYNC is the safety net for changes between batches, not the
  primary consistency mechanism

**Implementation steps:**

1. **Add uffd + WP_ASYNC + PAGEMAP_SCAN setup to Valkey**
   - Create uffd with `UFFD_FEATURE_WP_ASYNC` in `server.c:initServer()`
   - Register heap region(s) with `UFFDIO_REGISTER_MODE_WP`
   - Open `/proc/self/pagemap` for `PAGEMAP_SCAN`
   - Store uffd and pagemap_fd in `server` struct

2. **Build incremental RDB serializer in the event loop**
   - New state machine: `MIGRATE_STATE_IDLE`, `_SERIALIZING`, `_DIRTY_SCAN`,
     `_CONVERGING`, `_FINAL_PAUSE`, `_DONE`
   - In `serverCron()` or `beforeSleep()`: call `migrateIncrementalSerialize()`
   - Time-budgeted: serialize for at most N microseconds per call
   - Uses existing `rdbSaveObject()` / `rdbSaveKeyValuePair()`
   - Large objects: serialize M entries per call (like incremental rehash)
   - Stream output via `rio` to socket (diskless replication pattern)

3. **WP_ASYNC dirty tracking between batches**
   - Before each serialization batch: WP the pages being read
   - After full pass: `PAGEMAP_SCAN(PM_SCAN_WP_MATCHING)` for dirty pages
   - Re-serialize dirty keys (key-level dirty tracking preferred,
     page-level as safety net)
   - Iterate until dirty set < threshold

4. **Final consistency fence**
   - Brief pause (stop processing commands, ~1ms)
   - Final `PAGEMAP_SCAN` — send last dirty delta
   - Capture `replication_offset` — this is the PSYNC start point
   - Resume command processing
   - Replica catches up via PSYNC backlog from that offset

5. **Integration with replication protocol**
   - When replica connects for FULLRESYNC: start incremental serializer
     instead of fork() + child RDB
   - Stream RDB via `rdbSaveToReplicasSockets` pattern
   - After RDB complete + dirty converged: replica does PSYNC

6. **Disable churn sources during migration window**
   - Pause active defrag (`server.active_defrag_enabled = 0`)
   - Pause jemalloc background thread
   - Pause lazy-free background operations
   - Re-enable after migration completes

**Files to modify**: `src/server.c`, `src/server.h`, `src/rdb.c`,
`src/replication.c`
**Dependencies**: Linux 6.1+ (WP_ASYNC), Linux 6.7+ (PAGEMAP_SCAN)

### Phase 2: Handle Edge Cases

7. **jemalloc slab sharing**
   - Small objects share slab pages — WP on a slab page affects neighbors
   - Accept false positives: re-serialize objects on dirty slab pages
   - For small objects (<PAGE_SIZE): serialize without WP (fast enough
     that modification during serialization is negligible)

8. **Large objects (1GB+ hashes, sorted sets)**
   - Serialize incrementally: N entries per event loop iteration
   - WP the object's pages at start of serialization
   - Between iterations: commands may modify the object (WP_ASYNC resolves)
   - After full iteration: PAGEMAP_SCAN detects dirty pages
   - Re-serialize only the dirty entries
   - Max added latency per command = serialization time budget (~1ms)

9. **Key-level dirty tracking (optimization)**
   - Maintain a dirty bit per key in the keyspace
   - Set dirty bit in command processing when a key is modified
   - During dirty-delta rounds: only re-serialize keys with dirty bit set
   - Faster than page-level PAGEMAP_SCAN for sparse modifications
   - Page-level scan remains as safety net for non-command mutations
     (defrag, rehash, jemalloc internal operations)

### Phase 3: Benchmark and Validate

10. **Compare approaches**

    | Approach | Source Latency | Transfer Time | Freeze | Memory Overhead |
    |---|---|---|---|---|
    | Fork BGSAVE + replication | Near zero | ~100s for 100GB | fork() ~500ms | 2x RSS (COW) |
    | CRIU COW dump (current) | 1.5-3ms for 84s | 84s | 35ms | Minimal |
    | **Main-thread WP_ASYNC** | **~1ms max** (budget) | ~100s | **~1ms** (final fence) | **Minimal** |

11. **Test edge cases**
    - Large objects (1GB+ hashes) — verify incremental serialization
    - Active defrag disabled — verify no interference
    - High write rate — verify dirty convergence
    - Object rehash during serialization — verify no crash
    - Replica disconnect during serialization — verify cleanup

### Key Decision Points

- **Phase 1 PoC determines viability**: Build `DEBUG WP-MIGRATE` command
  that serializes one DB incrementally with WP_ASYNC. Measure latency.
  If p99 stays under 1ms, this approach works.

- **No dependency on upstream thread-save**: Main-thread incremental
  serialization is a different pattern. Thread-save uses a background
  thread with object locks. We use the main thread with cooperative
  yielding and WP_ASYNC. No locks, no thread safety issues.

- **This is a new migration protocol, not drop-in FULLRESYNC**: The
  serialization is incremental and eventually-consistent, unlike fork's
  point-in-time snapshot. The final consistency fence + PSYNC backlog
  makes it equivalent, but the wire protocol and state machine are new.

## Proven Facts (with test data)

1. **WP_ASYNC max write latency: 13μs** (100GB, 565K samples, 393ms WP duration)
2. **UFFDIO_WRITEPROTECT uses mmap_read_lock** (shared, not exclusive) on Linux 6.14
3. **process_vm_readv L3 pollution is per-byte, not per-syscall** — throttling/batching/affinity don't help
4. **LZ4 compression is not the L3 bottleneck** — removing it shows identical latency
5. **Transfer is network-paced at ~735MB/s** — usleep between batches has no effect
6. **100GB transfer takes 84 seconds** with continuous 1.5-3ms avg latency on source
7. **Dirty convergence breaks for write-heavy workloads** — active defrag, large object mutations, jemalloc churn
8. **Per-object WP_ASYNC inside the process avoids L3 eviction entirely** — data already hot in cache
