# COW Dump Design Document

## Overview

The COW (Copy-on-Write) dump implementation is an experimental feature in this
CRIU fork designed to minimize source process downtime during live migration.
Instead of freezing the process for the entire dump duration, COW dump uses
Linux's userfaultfd write-protect (`UFFD_FEATURE_WP_ASYNC`) to track memory
writes while the process continues running, enabling incremental page transfer.

## Goals

1. **Minimize downtime**: Keep the source process running during the bulk of
   the memory transfer; only freeze for a short skeleton dump + final dirty
   flush.
2. **Parallel, compressed transfer**: Multiple sender/receiver sockets, LZ4
   compression, batched I/O.
3. **Convergence**: Iteratively re-send dirty pages until a threshold is
   reached (overwriting older copies in the replica batch buffer), then do
   a single final frozen scan.
4. **Completeness**: Capture dynamically created VMAs as well as the
   originally-tracked set.

## Top-Level Architecture

```
┌──────────────────────────────────────┐   ┌──────────────────────────────────────┐
│            PRIMARY SIDE              │   │            REPLICA SIDE              │
├──────────────────────────────────────┤   ├──────────────────────────────────────┤
│                                      │   │                                      │
│ Phase 1: Seize + Dirty-tracking init │   │   (waiting for P3 connections)       │
│   - collect_pstree, pre_dump_one_task│   │                                      │
│   - parasite opens UFFD WP_ASYNC     │   │                                      │
│   - UFFDIO_WRITEPROTECT // 64MB      │   │                                      │
│   - Unfreeze: process runs live      │   │                                      │
│                                      │   │                                      │
├──────────────────────────────────────┤   ├──────────────────────────────────────┤
│                                      │   │                                      │
│ Phase 2a: Bulk transfer              │   │ P3 Receivers (N threads, 1/socket)   │
│   ┌─────────────┐                    │   │   - LZ4 decompress into page pool    │
│   │ P3 Senders  │  work-steal from   │   │   - cow_page_buffer_add_batch()      │
│   │ (bulk set)  │  g_work_queue      │   │     → new 1MB entry in batch buffer  │
│   └──────┬──────┘  32MB chunks       │   │                                      │
│          │ LZ4 → per-thread socket ──┼───┼──►  Batch Buffer                     │
│          ▼  (scanners idle, waiting) │   │      (hash, 256K buckets)            │
│                                      │   │                                      │
├──────────────────────────────────────┤   │   NO DRAIN YET — pages only buffer   │
│                                      │   │                                      │
│ Phase 2b: Pre-scan + send dirty      │   │                                      │
│   (after all bulk senders finish)    │   │                                      │
│   ┌─────────────┐                    │   │                                      │
│   │ Scanners    │  PAGEMAP_SCAN      │   │                                      │
│   │ (N threads) │  (WP_MATCHING)     │   │                                      │
│   └──────┬──────┘  iterate < thresh  │   │                                      │
│          ▼                           │   │                                      │
│   ┌─────────────────────────────┐    │   │   Dirty re-sends OVERWRITE existing  │
│   │ MPMC convergence queue      │    │   │   batch-buffer entries in-place:     │
│   │ (CAS-claim region per pop)  │    │   │     get_data_ptr() + decompress      │
│   └──────┬──────────────────────┘    │   │     directly into entry->data        │
│          ▼ consumers = P3 senders    │   │     (zero alloc, zero memcpy)        │
│   Pack → process_vm_readv → LZ4 ─────┼───┼──►                                   │
│          → per-thread socket         │   │                                      │
│                                      │   │                                      │
├──────────────────────────────────────┤   ├──────────────────────────────────────┤
│                                      │   │                                      │
│ Phase 3: Freeze + skeleton dump      │   │ (still buffering only)               │
│   - reseize_pstree, collect_ids      │   │                                      │
│   - cow_detect_new_vmas() vs Phase 1 │   │                                      │
│   - cow_set_new_vma_ranges()         │   │                                      │
│   - cow_signal_last_scan()           │   │                                      │
│   - scanners: final frozen scan ─────┼───┼──► final dirty batches arrive,       │
│   - dump_one_task() (skeleton)       │   │    overwrite existing entries        │
│   - write_img_inventory()            │   │                                      │
│                                      │   │                                      │
├──────────────────────────────────────┤   ├──────────────────────────────────────┤
│                                      │   │                                      │
│ Phase 4: cr_dump_finish              │   │ 1. Restore connects to lazy socket   │
│   - unfreeze tasks                   │   │ 2. Catches tasks, sends TASKS_FROZEN │
│   - send PS_IOV_ALL_PAGES_SENT ──────┼───┼──► 3. Drain threads start            │
│                                      │   │      - walk chunk_index (work-steal) │
│                                      │   │      - UFFDIO_COPY whole 1MB batches │
│                                      │   │      - free pool chunks as empty     │
│                                      │   │      - EAGAIN → retry queue          │
│                                      │   │    (restore blocks, waiting)         │
│                                      │   │ 4. Drain complete → buffer empty     │
│                                      │   │ 5. Restore unfreezes tasks           │
│   - wait for ACK ◄───────────────────┼───┤ 6. Send PS_IOV_ALL_PAGES_SENT_ACK    │
│   - cow_cleanup_async_uffd()         │   │                                      │
│                                      │   │                                      │
└──────────────────────────────────────┘   └──────────────────────────────────────┘
        ▲                                                ▲
        └────────── per-thread TCP sockets ──────────────┘
                      (N connections)
```

## Phases

### Phase 1 — Seize + WP_ASYNC Initialization

**Entry**: `cr_dump_tasks_cow_phased()` in `criu/cr-dump.c` (redirected from
`cr_dump_tasks()` when `opts.cow_dump && opts.lazy_pages`).

**Steps**:

1. Seize process tree (`collect_pstree`, `pre_dump_one_task` for each item).
2. Parasite RPC `PARASITE_CMD_COW_DUMP_INIT` opens a userfaultfd with
   `UFFD_FEATURE_WP_ASYNC` inside the target and returns the fd via
   `compel_util_recv_fd()` — `cow_dump_init_async()` in `criu/cow/cow-dump.c`.
3. `cow_register_vmas()` walks the VMA list and records eligible ones
   (writable, private or anon-shared, not guard/VVAR/droppable). For each it
   issues `UFFDIO_REGISTER` in `UFFDIO_REGISTER_MODE_WP` mode (register
   failures are tolerated — `PAGEMAP_SCAN` is the source of truth).
4. `cow_apply_writeprotect()` splits all tracked VMAs into
   `COW_WP_CHUNK_SIZE` (64MB) ranges and issues `UFFDIO_WRITEPROTECT` in
   parallel from `sysconf(_SC_NPROCESSORS_ONLN)` threads.
5. `arch_set_thread_regs()` + `pstree_switch_state(TASK_ALIVE)` — process
   resumes with write-protection in effect.

### Phase 2a — Bulk Transfer

`cr_page_server()` establishes the PRIMARY side page-server and spawns the
unified thread (`cow-unified-thread.c`) which accepts `N` P3 connections
and calls `cow_start_p3_threads()`.

Bulk transfer (`cow-bulk-send.c`):

- A shared work queue `g_work_queue` is built by splitting each lazy VMA
  into `COW_WORK_CHUNK_SIZE` (32MB) chunks (bounded by `COW_MAX_WORK_ITEMS`
  = 16384).
- Only `COW_NUM_P3_THREADS_BULK` threads participate in bulk; the rest sit
  idle until Phase 2b pre-scan. (Rationale: during bulk the receiver is
  usually the bottleneck, so extra senders contend for CPU.)
- Each thread pulls items via `__atomic_fetch_add(&g_work_queue_next)` and,
  for each item, reads `COW_BATCH_PAGES` (256) pages with
  `process_vm_readv`, LZ4-compresses them, and sends a
  `PS_IOV_ADD_F_COMPRESS` batch on its socket.
- Scanner threads are started but block on `g_bulk_transfer_done_count`
  until every bulk sender has exhausted the work queue
  (`cow-bulk-send.c:158-159, 304-317`). There is no interleaving — Phase 2b
  begins only after all bulk work is done.

Replica side (Phase 2a): P3 receivers LZ4-decompress each batch into a
page-pool buffer and insert it as a new 1MB-aligned entry in the batch
buffer hash table (`cow_page_buffer_add_batch`). **No draining happens
yet** — pages only accumulate in the batch buffer.

### Phase 2b — Pre-Scan + Send Dirty (Overwrite Older Pages)

Gated by `COW_PRE_SCAN` (default on). Triggered when all bulk senders have
completed.

- `cow_start_scanner_thread()` started `COW_NUM_SCANNERS` scanner threads
  at the start of Phase 2; only the first `COW_NUM_PRE_SCANNERS` actively
  scan in Phase 2b, and all of them are woken once bulk is done. The rest
  wait directly for the freeze signal.
- Each scanner walks `global_lazy_vmas`, divides each VMA by pre-scanner
  count, and runs `PAGEMAP_SCAN` in `COW_PAGEMAP_SCAN_RANGE_SIZE` (4MB)
  sub-chunks with `PM_SCAN_WP_MATCHING`. The 4MB cap bounds `mmap_lock`
  hold time in the kernel.
- Dirty regions become `struct dirty_region_entry` and are pushed into the
  MPMC convergence queue (a flat array `g_conv_slots[CONV_QUEUE_CAP]` with
  atomic `g_conv_head`/`g_conv_tail`; pop = CAS on tail, one region per
  claim).
- Same P3 sender threads (now all `COW_NUM_P3_THREADS` of them) consume
  the queue, pack regions into `COW_BATCH_PAGES`-sized batches of
  `dirty_slice`s, and issue a single multi-iov `process_vm_readv` per
  batch followed by LZ4 + send.
- Scanners iterate until total dirty pages drop below
  `COW_DIRTY_SCAN_FREEZE_THRESHOLD` (2,000,000) or
  `COW_PRE_SCAN_MAX_ITERATIONS` is reached, then scanner 0 waits for the
  queue to drain and sets `g_last_scan_flag`. `cow_all_threads_below_threshold()`
  returns true → main thread moves to Phase 3.

Replica side (Phase 2b): on re-send of a page whose 1MB region is already
present in the batch buffer, the receiver calls
`cow_page_buffer_get_data_ptr()` to find the existing entry and
LZ4-decompresses **directly into `entry->data`** at the page offset,
**overwriting the older bulk copy in place** (`cow-p3-receiver.c:126-152`).
Zero allocation, zero memcpy. Still no drain.

### Phase 3 — Freeze + Skeleton Dump

Measured wall-clock is the primary metric here; this is the downtime.

1. `reseize_pstree()` + `collect_pstree_ids()`.
2. `cow_set_dst_id(vpid(root_item))` — dst_id wasn't valid in Phase 1.
3. `collect_mappings()` → `cow_detect_new_vmas()` compares against Phase 1
   tracked set and emits ranges for VMAs that appeared while the process
   was running. New ranges are also appended to `tracked_vmas` and
   `global_lazy_vmas` (`add_lazy_vma_for_new_region`).
4. `cow_set_new_vma_ranges()` hands the new-range array to senders.
5. `cow_signal_last_scan()` sets `g_scanner_freeze_signal`. Scanners do a
   frozen `PAGEMAP_SCAN` (no range chunking, no WP_MATCHING — just read
   `PAGE_IS_WRITTEN`) over their slice of each VMA and push to the MPMC
   queue.
6. Skeleton dump loop: `dump_one_task()` for every item. Phase flag
   (`COW_PHASE_SCAN`) makes `cow_is_phased_skeleton_dump()` return true and
   page-dump paths short-circuit (`generate_iovs`, etc.).
7. `cr_dump_post_task_operations()`.
8. `cow_wait_p3_threads()` — senders drain the MPMC queue and then send any
   pages from the new-VMA ranges (`send_new_vma_pages`, split round-robin
   by thread id).
9. `cow_free_new_vma_ranges()`; `cow_set_phase(COW_PHASE_DONE)`.
10. `write_img_inventory()` (still frozen).

Replica side (Phase 3): receivers keep buffering final-scan batches into
the batch buffer, overwriting existing entries on re-sends. Drain has
still not started.

### Phase 4 — Unfreeze + Replica Restore

Primary:

- `cr_dump_finish` unfreezes tasks, sends `PS_IOV_ALL_PAGES_SENT`, waits
  for `PS_IOV_ALL_PAGES_SENT_ACK`, then calls `cow_cleanup_async_uffd()`
  (currently guarded by a hardcoded 15s sleep in the code; the cleanup
  path unregisters VMAs in chunks with 10ms yields every 10 VMAs, then
  closes the uffd).

Replica (serial — **restore does NOT run concurrently with drain**):

1. `criu restore` connects to the lazy socket (`handle_lazy_accept` in
   `criu/uffd.c:1616`). Drain is **not** started here — tasks are still
   running (`criu/uffd.c:1667-1680`).
2. Restore catches all tasks via `PTRACE_INTERRUPT`, then sends
   `LAZY_PAGES_TASKS_FROZEN` over the lazy socket.
3. `lazy_sk_read_event` receives `TASKS_FROZEN` and calls
   `cow_handle_lazy_accept_post_connect()` → `cow_start_drain_thread()`
   (`criu/uffd.c:1545-1551`, `cow-uffd.c:1842-1853`). Drain threads walk
   `chunk_index` (work-stealing via `next_drain_chunk`) and issue
   `UFFDIO_COPY` for whole 1MB batches, freeing page-pool chunks as each
   empties. `EAGAIN` → retry queue; `EEXIST`/`ENOENT` → soft-handle.
4. `cow_phase3_restore_loop` blocks on
   `while (cow_drain_thread_running() || cow_page_buffer_count() > 0)`
   (`criu/uffd.c:1796`). Restore only proceeds past this point after the
   batch buffer is empty.
5. Restore unfreezes the tasks and sends `PS_IOV_ALL_PAGES_SENT_ACK` back
   to the primary; primary closes the sockets.

## Key Data Structures

### Dump-side state (`cow-dump.c`)

```c
struct cow_dump_info {
    pid_t                     source_pid;
    u64                       dst_id;                /* updated after collect_pstree_ids */
    int                       uffd;                  /* WP_ASYNC fd from parasite */
    unsigned int              nr_tracked_vmas;
    struct cow_tracked_vma   *tracked_vmas;          /* appended on new-VMA detect */
    enum cow_dump_phase       phase;                 /* IDLE / ASYNC_BULK / SCAN / DONE */
};
```

### Lazy VMA (`cow-mem.c`, `cow-mem.h`)

```c
struct lazy_vma_entry {
    uint64_t              start, end;
    struct list_head      list;
    struct vma_area      *vma;         /* NULL for regions discovered in Phase 3 */
    unsigned long         total_pages;
    u64                   dst_id;
    pid_t                 source_pid;
};
```

### Dirty region (`cow-bulk-send.h`)

```c
struct dirty_region_entry {
    unsigned long start, end;
    u64           dst_id;
    pid_t         source_pid;
};
```

### MPMC convergence queue (`cow-bulk-send.c`)

```c
#define CONV_QUEUE_CAP (8 * 1024 * 1024)
static struct dirty_region_entry **g_conv_slots;
static volatile unsigned long      g_conv_head;   /* producers: fetch-add */
static volatile unsigned long      g_conv_tail;   /* consumers: CAS claim */
```

Producers (scanners) fetch-add `g_conv_head` and write the slot. Consumers
(P3 senders) CAS `g_conv_tail` then busy-read the slot until the producer
completes the store. One region per CAS — no empty-queue polling waste.

### Replica batch buffer (`cow-uffd.c`)

```c
struct batch_buffer_entry {
    unsigned int           magic;
    unsigned long          base_vaddr;           /* COW_BATCH_SIZE (1MB) aligned */
    void                  *data;                 /* 256 contiguous 4KB pages */
    cow_batch_bitmap_t     page_bitmap;          /* which of the 256 are present */
    cow_batch_bitmap_t     initial_bitmap;       /* for drain accounting */
    int                    nr_pages;
    struct hlist_node      hash;
    struct list_head       chunk_list;           /* for chunk-ordered drain */
    int                    chunk_id;
};
```

- Hash table: `COW_BATCH_BUFFER_HASH_SIZE` = 256K buckets, one spinlock per
  bucket (`COW_BATCH_NUM_HASH_LOCKS == COW_BATCH_BUFFER_HASH_SIZE`).
- A secondary index `chunk_index[COW_MAX_POOL_CHUNKS]` maps each page-pool
  chunk to the list of batches that came out of it — drain can free chunks
  progressively.

### Page pool (`page-pool.c`)

- Chunk = `COW_CHUNK_SIZE` (64MB), chunk-aligned, max
  `COW_MAX_POOL_CHUNKS` (8192) ⇒ 512GB ceiling.
- Per-thread bump allocator (`COW_MAX_THREADS` slots); ownership transfer
  via `page_pool_get_pages()` / `page_pool_put()`.
- Atomic refcount per chunk header (page 0 of each chunk). `munmap()` once
  refcount drops to 0.

## Configuration

All tunables live in `criu/include/cow/cow-conf.h`. Key constants:

| Constant | Value | Purpose |
|---|---|---|
| `COW_BATCH_PAGES` | 256 | Pages per transfer batch (1MB) |
| `COW_BATCH_SIZE` | 1MB | Size of a batch (aligned on replica) |
| `COW_BATCH_SHIFT` | 20 | log2(batch size) |
| `COW_WP_CHUNK_SIZE` | 64MB | Parallel UFFDIO_WRITEPROTECT chunk |
| `COW_WORK_CHUNK_SIZE` | 32MB | Bulk work-stealing chunk |
| `COW_MAX_WORK_ITEMS` | 16384 | Bulk work queue cap |
| `COW_PAGEMAP_SCAN_RANGE_SIZE` | 4MB | Per-ioctl scan range (bounds `mmap_lock`) |
| `COW_PAGEMAP_SCAN_VEC_LEN` | 1000 | Max regions per `PAGEMAP_SCAN` ioctl |
| `COW_DIRTY_SCAN_FREEZE_THRESHOLD` | 2,000,000 | Pre-scan convergence target |
| `COW_PRE_SCAN_MAX_ITERATIONS` | 2 | Cap pre-scan iterations |
| `COW_CHUNK_SIZE` | 64MB | Page-pool chunk size |
| `COW_MAX_POOL_CHUNKS` | 8192 | 64MB × 8192 = 512GB pool cap |
| `COW_BATCH_BUFFER_HASH_SIZE` | 256K | Replica batch-buffer buckets |
| `CONV_QUEUE_CAP` (in `cow-bulk-send.c`) | 8M | MPMC slot count |

Thread counts are profile-gated:

| Constant | `COW_PROFILE_SMALL` (default) | `COW_PROFILE_LARGE` |
|---|---|---|
| `COW_NUM_P3_THREADS` | 4 | 15 |
| `COW_NUM_P3_THREADS_BULK` | 1 | 15 |
| `COW_NUM_SCANNERS` | 4 | 20 |
| `COW_NUM_PRE_SCANNERS` | 1 | 1 |
| `COW_NUM_DRAIN_THREADS` | 4 | 10 |
| `COW_MAX_THREADS` | 16 | 33 |

Notable compile-time feature flags (also in `cow-conf.h`):

- `COW_PRE_SCAN` — iterative pre-freeze scan (on by default).
- `CONFIG_PAGE_STATE_TRACKER`, `CONFIG_HUNG_PAGE_TRACKER`,
  `CONFIG_COW_COMPARE`, `CONFIG_COW_COMPARE_PAGES` — diagnostics, off by
  default.

## Protocol Extensions (`cow-page-xfer.h`)

```c
#define PS_IOV_GET_ALL              8
#define PS_IOV_ADD_F_PF             9
#define PS_IOV_ADD_F_COMPRESS      10
#define PS_IOV_START_RESTORE       12   /* unused in current flow */
#define PS_IOV_BULK_COMPLETE_ACK   13
#define PS_IOV_ALL_PAGES_SENT      16   /* primary → replica */
#define PS_IOV_ALL_PAGES_SENT_ACK  17   /* replica → primary */
```

`PS_IOV_ADD_F_COMPRESS` wire format:

```
struct page_server_iov {
    u32 cmd;            /* encode_ps_cmd(PS_IOV_ADD_F_COMPRESS, PE_PRESENT) */
    u32 nr_pages;       /* ≤ COW_BATCH_PAGES */
    u64 vaddr;          /* base address (may not be batch-aligned) */
    u64 dst_id;
};
int compressed_size;    /* 4 bytes */
char data[compressed_size]; /* LZ4-compressed page payload */
```

LZ4 acceleration: **1** on both the pre-freeze bulk path and the frozen
convergence/new-VMA path. An experiment with acceleration=99 during
convergence regressed total P3 wall-clock from 2.3s → 4.8s (larger wire
size shifted the bottleneck to `tcp_sendmsg`/`skb_page_frag_refill`).

## Soft Error Handling (UFFDIO_COPY on replica)

Wrapper: `cow_uffd_copy_pages()` / `cow_uffd_copy()` in `cow-uffd.c`.

| Error | Meaning | Action |
|---|---|---|
| `EAGAIN` | Kernel busy | Queue for retry (drain EAGAIN queue or lpi queue) |
| `EEXIST` | Page already mapped | `PAGE_STATE_DISCARDED`; strict mode = BUG |
| `ENOENT` | Page unmapped between phases | Mark in `unmapped_tracker`, discard |
| other | Hard error | `BUG()` |

Flags: `COW_TRACK_STRICT` (BUG on EEXIST/ERROR, used during drain),
`COW_TRACK_RETRY` (retry mode, returns `-EAGAIN` instead of queueing).

## File Organization

### `criu/cow/` (C implementations)

| File | ~Lines | Purpose |
|---|---|---|
| `cow-dump.c` | 1,043 | COW dump init/fini, WP apply, new-VMA detect, cleanup |
| `cow-mem.c` | 196 | `global_lazy_vmas` list management |
| `cow-page-xfer.c` | 258 | COW-specific PS_IOV commands, ACK handling |
| `cow-unified-thread.c` | 244 | Primary page-server thread; spawns P3 senders |
| `cow-bulk-send.c` | 2,300 | P3 senders, scanner threads, MPMC queue, work-stealing |
| `cow-bulk-recv.c` | 185 | Replica control-message receive (`PS_IOV_ALL_PAGES_SENT`) |
| `cow-p3-receiver.c` | 476 | Replica P3 receiver threads (LZ4 decompress → batch buffer) |
| `cow-uffd.c` | 1,856 | Replica batch buffer, drain threads, UFFDIO_COPY, stats |
| `cow-lazy-pages.c` | 308 | Lazy-page integration glue |
| `cow-compare.c` | 725 | PRIMARY/REPLICA state-compare debug mode |
| `page-pool.c` | 481 | Per-thread 64MB-chunk bump allocator |
| `page-state-tracker.c` | 851 | Optional per-page state machine (debug) |
| `hung-page-tracker.c` | 250 | Optional hung-page detector (debug) |
| `unmapped-tracker.c` | 225 | Replica-side unmapped-range set |
| `cow-bitmap.c` | 69 | Misc bitmap helpers |

### `criu/include/cow/` (headers)

`cow-conf.h` (central tunables), `cow-dump.h`, `cow-mem.h`, `cow-bulk-send.h`,
`cow-bulk-recv.h`, `cow-page-xfer.h`, `cow-unified-thread.h`, `cow-uffd.h`,
`cow-lazy-pages.h`, `cow-compare.h`, `cow-batch-bitmap.h`, `cow-bitmap.h`,
`page-pool.h`, `page-state-tracker.h`, `hung-page-tracker.h`,
`unmapped-tracker.h`, `pf-tracker.h`, `spsc-queue.h`, `spmc-queue.h`,
`mpsc-queue.h`.

Note: `mpsc-queue.h` and `spsc-queue.h` are kept for the page-request
queue (`cow-unified-thread.c`) and legacy interfaces. The scanner→sender
hot path now uses the flat-array MPMC convergence queue defined
directly in `cow-bulk-send.c`.

## Integration Points

### `criu/cr-dump.c`

- `cr_dump_tasks()` redirects to `cr_dump_tasks_cow_phased()` when
  `opts.cow_dump && opts.lazy_pages`.
- Inside `cr_dump_tasks_cow_phased()`:
  - Phase 1: `pre_dump_one_task` loop, `pstree_switch_state(TASK_ALIVE)`.
  - Phase 2a + 2b: `cr_page_server(false, true, -1)` (which internally
    invokes `cow_dump_init_async` + parasite RPC earlier, then starts P3
    threads via the unified thread). Bulk senders exhaust `g_work_queue`
    first (Phase 2a); scanners then wake and iterate
    `PAGEMAP_SCAN`/convergence-queue sends (Phase 2b). The loop
    `while (!cow_all_threads_below_threshold()) usleep(10ms)` holds until
    both complete.
  - Phase 3: `reseize_pstree`, `collect_pstree_ids`, `cow_set_dst_id`,
    `collect_mappings` + `cow_detect_new_vmas` + `cow_set_new_vma_ranges`,
    `cow_signal_last_scan`, skeleton `dump_one_task` loop,
    `cow_wait_p3_threads`, `cow_free_new_vma_ranges`,
    `cow_set_phase(COW_PHASE_DONE)`, `write_img_inventory`.
- `cr_dump_finish()`: sends `PS_IOV_ALL_PAGES_SENT`, calls
  `cow_cleanup_async_uffd()`, `cow_dump_fini()`.

### `criu/config.c`

Option `--cow-dump` → `opts.cow_dump` (case 1105).

### `criu/mem.c`

When `opts.cow_dump`, `generate_iovs()` calls `cow_mem_add_lazy_vma()` to
append lazy-capable VMAs to `global_lazy_vmas`. In Phase 3 skeleton dump
mode, it short-circuits page iteration (`mdc.cow_skeleton_non_lazy = true`).

### `criu/pie/parasite.c`

`PARASITE_CMD_COW_DUMP_INIT` handler (`parasite_cow_dump_init`) creates the
uffd with the requested features (`UFFD_FEATURE_WP_ASYNC`) and returns the
fd over the compel socket.

## Debugging

- `TIMING:` prefix on wall-clock logs at every phase boundary and major
  step. `TIMING @X.XXXXXX:` tags deltas relative to freeze start.
- `VMA_TRACE:` prefix on VMA-lifecycle logs (registration, scanning, new
  detection).
- `DEBUG_PERF:` per-scanner/sender iteration timing.
- `check_and_print_uffd_stats()` — UFFD fault histogram by batch size
  (`cow_get_histogram_bucket`).
- `g_compress_uncompressed_bytes` / `g_compress_compressed_bytes` —
  aggregate compression ratio. Thread-local counters are flushed at thread
  exit (one atomic per counter, not per-send).
- `page-state-tracker.c` (off by default) keeps a 16-entry history per
  page for diagnosing migration divergence.

## Usage

```bash
# PRIMARY (source)
sudo criu dump -t <pid> -D /images --cow-dump --lazy-pages \
    --page-server --address <replica-ip> --port 27

# REPLICA (destination)
sudo criu page-server --images-dir /images --port 27 --lazy-pages
sudo criu restore   --images-dir /images --lazy-pages
```

## Known Limitations

1. **Single process tree**: tracking metadata is shared-global
   (`g_cow_info`, `global_lazy_vmas`).
2. **Kernel requirements**: Linux 5.7+ for `UFFD_FEATURE_WP_ASYNC`.
   `PAGEMAP_SCAN` (kernel 6.6+) or fallback path needed.
3. **Memory overhead**: replica uses up to 512GB worth of 64MB chunks
   (`COW_MAX_POOL_CHUNKS`), refcount-freed as drain progresses.
4. **UFFD cleanup**: page-table walks during `UFFDIO_UNREGISTER` can take
   seconds on large memory; `cow_cleanup_async_uffd()` chunks the work and
   relies on close-on-exit for the rest. A hardcoded 15s sleep currently
   precedes cleanup (investigation outstanding).
5. **Network lock skipped**: COW path does not call `network_lock()`.
6. **New-VMA metadata gap**: VMAs created between Phase 1 and Phase 3 have
   their pages transferred but their VMA records are not re-dumped — they
   won't exist on the replica. Logged as `pr_err` at Phase 3.

## Future Improvements

1. Multi-process tree support (drop global singletons).
2. Adaptive convergence threshold driven by measured write rate.
3. Better handling of rapidly-dirtying workloads (e.g. pin-to-CPU heuristics
   during bulk already in `COW_P3_SENDER_CPU`).
4. Integration with container runtimes (containerd, CRI-O).
