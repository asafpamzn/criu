# COW Dump Design Document

## Overview

The COW (Copy-on-Write) dump implementation is an experimental feature in this CRIU fork designed to minimize source process downtime during live migration. Instead of freezing the process for the entire dump duration, COW dump uses Linux's userfaultfd write-protect mechanism to track memory writes while the process continues running, enabling incremental page transfer.

## Goals

1. **Minimize downtime**: Reduce the time the source process is frozen during migration
2. **Efficient transfer**: Use parallel bulk transfer with compression
3. **Convergence**: Iteratively reduce dirty pages until a threshold is reached
4. **Correctness**: Ensure all memory pages are captured, including dynamically created VMAs

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              PRIMARY SIDE                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐    Phase 1: Seize + WP_ASYNC Init                     │
│  │   cr-dump   │    ────────────────────────────                       │
│  │   (main)    │    - Seize process, collect VMAs                       │
│  └──────┬──────┘    - Create UFFD with WP_ASYNC via parasite            │
│         │           - Apply write-protect (parallel, 64MB chunks)        │
│         │           - Unfreeze process (runs with WP_ASYNC)              │
│         │                                                                │
│         ▼           Phase 2: Bulk Transfer                               │
│  ┌─────────────┐    ──────────────────────                              │
│  │ Page Server │                                                         │
│  │   Thread    │    ┌──────────────────────────────────────────────┐    │
│  └──────┬──────┘    │          Scanner Threads (4x)                 │    │
│         │           │  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐     │    │
│         │           │  │Scan   │ │Scan   │ │Scan   │ │Scan   │     │    │
│         │           │  │VMA 0-N│ │VMA N-M│ │VMA M-P│ │VMA P-Z│     │    │
│         │           │  └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘     │    │
│         │           │      │         │         │         │          │    │
│         │           │      ▼         ▼         ▼         ▼          │    │
│         │           │  ┌─────────────────────────────────────┐      │    │
│         │           │  │     SPSC Queues (dirty regions)     │      │    │
│         │           │  └─────────────────────────────────────┘      │    │
│         │           │      │         │         │         │          │    │
│         │           │      ▼         ▼         ▼         ▼          │    │
│         │           │  ┌──────┐  ┌──────┐  ┌──────┐  ... (20x)      │    │
│         │           │  │P3 #1 │  │P3 #2 │  │P3 #3 │                 │    │
│         │           │  │Sender│  │Sender│  │Sender│                 │    │
│         │           │  └──┬───┘  └──┬───┘  └──┬───┘                 │    │
│         │           └─────┼─────────┼─────────┼─────────────────────┘    │
│         │                 │         │         │                          │
│         │                 ▼         ▼         ▼                          │
│         │           ┌─────────────────────────────────┐                  │
│         │           │  LZ4 Compressed Page Batches    │                  │
│         │           │  (64 pages = 256KB per batch)   │                  │
│         │           └──────────────┬──────────────────┘                  │
│         │                          │                                     │
└─────────┼──────────────────────────┼─────────────────────────────────────┘
          │                          │
          │  PS_IOV_* Protocol       │  20 parallel sockets
          ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              REPLICA SIDE                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────┐     ┌─────────────────────────┐   │
│  │     P3 Receiver Threads (20x)    │     │   Page Buffer           │   │
│  │  ┌──────┐ ┌──────┐ ┌──────┐     │     │  ┌─────────────────┐    │   │
│  │  │Recv  │ │Recv  │ │Recv  │ ... │ ──► │  │ 1M bucket hash  │    │   │
│  │  │ #1   │ │ #2   │ │ #3   │     │     │  │ 8K fine locks   │    │   │
│  │  └──────┘ └──────┘ └──────┘     │     │  │ 256MB pools     │    │   │
│  └─────────────────────────────────┘     │  └────────┬────────┘    │   │
│                                          └───────────┼─────────────┘   │
│                                                      │                  │
│                                                      ▼                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Drain Threads (10x)                           │   │
│  │  ┌─────────────────────────────────────────────────────────┐    │   │
│  │  │  UFFDIO_COPY pages to target process memory             │    │   │
│  │  │  Handle soft errors: EAGAIN (retry), EEXIST (skip)      │    │   │
│  │  └─────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────┐                                                       │
│  │   CRIU      │  Wait for PS_IOV_INVENTORY_READY → start restore      │
│  │  Restore    │  Wait for PS_IOV_ALL_PAGES_SENT → zero-fill rest      │
│  └─────────────┘                                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Phases

### Phase 1: Seize + WP_ASYNC Initialization

**Entry point**: `cr_dump_tasks_cow_phased()` in `criu/cr-dump.c`

**Steps**:
1. Seize the target process tree (`collect_pstree()`)
2. Inject parasite code into the target process
3. Create userfaultfd with `UFFD_FEATURE_WP_ASYNC` via parasite RPC
4. Register eligible VMAs (writable, private/shared anonymous)
5. Apply write-protect using parallel threads (64MB chunks)
6. Add VMAs to global lazy VMA list for page server reference
7. Unfreeze the process - it runs with WP_ASYNC enabled

**Key function**: `cow_dump_init_async()` in `criu/cow/cow-dump.c`

**Configuration**:
- `COW_WP_CHUNK_SIZE`: 64MB chunks for parallel write-protect
- Number of threads: `sysconf(_SC_NPROCESSORS_ONLN)` (up to number of ranges)

**Output**: Process continues running with WP_ASYNC tracking all writes.

---

### Phase 2: Bulk Page Transfer + Dirty Scan Convergence

**Entry point**: `cr_page_server()` starts page server thread which spawns P3 threads

**Architecture**:

```
Scanner Threads (4x)           SPSC Queues            P3 Sender Threads (20x)
┌─────────────────┐         ┌─────────────┐         ┌─────────────────────┐
│ PAGEMAP_SCAN    │         │             │         │ process_vm_readv()  │
│ PM_SCAN_WP_     │ ──────► │ dirty_region│ ──────► │ LZ4 compress        │
│ MATCHING        │         │ entries     │         │ socket send         │
└─────────────────┘         └─────────────┘         └─────────────────────┘
```

**Scanner Thread Logic** (`cow-bulk-send.c`):
1. Iterate over VMAs in `global_lazy_vmas`
2. Use `PAGEMAP_SCAN` with `PM_SCAN_WP_MATCHING` to find dirty pages
3. Distribute dirty regions to per-sender SPSC queues (round-robin by VMA position)
4. Repeat until convergence threshold reached

**P3 Sender Thread Logic** (`cow-bulk-send.c`):
1. Dequeue dirty region from assigned SPSC queue
2. Read pages using `process_vm_readv()` (64 pages = 256KB batch)
3. Compress with LZ4
4. Send via dedicated socket using `PS_IOV_ADD_F_COMPRESS` protocol

**Convergence**:
- `DIRTY_SCAN_FREEZE_THRESHOLD`: 300,000 pages (~1.2GB)
- When all scanners find < threshold dirty pages, signal `cow_all_threads_below_threshold()`
- Main thread proceeds to Phase 3

---

### Phase 3: Freeze + Skeleton Dump

**Steps**:
1. Re-seize all tasks (`reseize_pstree()`)
2. Collect pstree IDs (`collect_pstree_ids()`)
3. Update COW dst_id (`cow_set_dst_id()`)
4. **Detect new VMAs** (`cow_detect_new_vmas()`):
   - Re-scan current VMAs (`collect_mappings()`)
   - Compare against Phase 1 tracked VMAs
   - Identify regions created between Phase 1 and Phase 3
   - Pass new ranges to P3 threads (`cow_set_new_vma_ranges()`)
5. Signal P3 threads for final scan (`cow_signal_last_scan()`)
6. Dump skeleton (everything except pages)
7. Write inventory (`write_img_inventory()`)
8. Send `PS_IOV_INVENTORY_READY` to replica
9. Wait for P3 threads to complete final scan
10. Unfreeze process
11. Send `PS_IOV_ALL_PAGES_SENT` to replica
12. Clean up async UFFD (`cow_cleanup_async_uffd()`)

---

### Phase 4: Replica Restore

**Steps**:
1. Wait for `PS_IOV_INVENTORY_READY` signal
2. Start CRIU restore (process starts with UFFD-protected pages)
3. Drain threads copy buffered pages to process memory via `UFFDIO_COPY`
4. Wait for `PS_IOV_ALL_PAGES_SENT` signal
5. Zero-fill any remaining page faults (pages from Phase 3 new VMAs)

---

## Key Data Structures

### COW Dump State (`cow-dump.c`)

```c
struct cow_dump_info {
    pid_t source_pid;                    // Source process PID
    u64 dst_id;                         // Destination process ID
    int uffd;                           // Current userfaultfd
    int uffd_async;                     // WP_ASYNC uffd for Phase 2
    int uffd_sync;                      // Pre-created WP_SYNC uffd
    unsigned long total_pages;
    unsigned int nr_tracked_vmas;
    struct cow_tracked_vma *tracked_vmas;
    enum cow_dump_phase phase;
    struct cow_page_queue page_queue;   // MPSC queue for COW pages
};

struct cow_tracked_vma {
    unsigned long start;
    unsigned long end;
    bool is_new;  // True if detected in Phase 3
};

enum cow_dump_phase {
    COW_PHASE_IDLE = 0,
    COW_PHASE_ASYNC_BULK,      // WP_ASYNC active, bulk transfer
    COW_PHASE_SCAN,            // Process frozen, scanning dirty pages
    COW_PHASE_SYNC_CONVERGE,   // WP_SYNC on dirty pages, convergence
    COW_PHASE_DONE,
};
```

### Lazy VMA Entry (`cow-mem.c`)

```c
struct lazy_vma_entry {
    uint64_t start;
    uint64_t end;
    struct list_head list;
    struct vma_area *vma;           // Original VMA (NULL for Phase 3 regions)
    unsigned long total_pages;
    u64 dst_id;
    pid_t source_pid;
};
```

### Dirty Region Entry (`cow-bulk-send.h`)

```c
struct dirty_region_entry {
    unsigned long start;
    unsigned long end;
    u64 dst_id;
    pid_t source_pid;
};
```

### Page Buffer (Replica Side, `cow-uffd.c`)

```c
// Hash table: 1M buckets, 8K fine-grained locks
#define PAGE_BUFFER_HASH_SIZE (1 << 20)  // 1M buckets
#define NUM_HASH_LOCKS 8192
#define BUCKETS_PER_LOCK 128

struct page_buffer_node {
    struct {
        unsigned long vaddr;
        void *data;
    } entries[PAGE_NODE_ENTRIES];     // 32 entries per node
    int count;
    struct hlist_node hash;
};
```

---

## Lock-Free Data Structures

### MPSC Queue (Multi-Producer Single-Consumer)

Used for COW page queue where fault handlers (multiple threads) enqueue pages and page server (single thread) dequeues.

**Implementation**: `criu/include/cow/mpsc-queue.h`

```c
// Wait-free enqueue via atomic exchange on tail
// Lock-free dequeue with memory ordering
DECLARE_MPSC_NODE(cow_page, struct cow_page_queue_entry);
```

### SPSC Queue (Single-Producer Single-Consumer)

Used for dirty region queues between scanner (single producer) and sender threads (single consumer per queue).

**Implementation**: `criu/include/cow/spsc-queue.h`

```c
// Lock-free enqueue/dequeue with cache-line padding
// 64-byte padding between head and tail to avoid false sharing
struct sender_queue {
    struct dirty_region_spsc_node *head;
    char _pad1[64 - sizeof(...)];
    struct dirty_region_spsc_node *tail;
    char _pad2[64 - sizeof(...)];
    unsigned long size;
    char _pad3[64 - sizeof(...)];
};
```

---

## Memory Efficiency Optimizations

### 256MB Page Pools

**Problem**: 20 receiver threads x millions of pages = massive malloc contention

**Solution**: Per-thread bump allocators with 256MB aligned chunks

```c
#define CHUNK_SIZE (256UL * 1024 * 1024)
#define MAX_THREADS 32
#define MAX_CHUNKS 512  // 128GB max

// Zero-copy allocation path
// Atomic refcounting per chunk (munmap when refcount=0)
// Eliminates malloc/mprotect serialization
```

### Unrolled Page Buffer Nodes

```c
// 32 entries per hash node vs 1 entry per node
// ~32x better cache locality during traversal
struct page_buffer_node {
    struct {
        unsigned long vaddr;
        void *data;
    } entries[32];
    int count;
    struct hlist_node hash;
};
```

### LZ4 Compression

- Reduces bandwidth 60-70% (typical ~40-50% compression ratio)
- Single 256KB compression per batch (not per-page)
- Reduces socket round-trips

---

## Protocol Extensions

### COW-Specific Commands (`cow-page-xfer.h`)

```c
#define PS_IOV_GET_ALL              8   // Get all pages for dst_id
#define PS_IOV_ADD_F_PF             9   // Add page (fault response)
#define PS_IOV_ADD_F_COMPRESS      10   // Add compressed page batch
#define PS_IOV_START_RESTORE       12   // Replica → restart process
#define PS_IOV_BULK_COMPLETE_ACK   13   // Replica → bulk pages received
#define PS_IOV_INVENTORY_READY     14   // Primary → inventory.img written
#define PS_IOV_ALL_PAGES_SENT      16   // Primary → all pages sent
#define PS_IOV_ALL_PAGES_SENT_ACK  17   // Replica → ACK, safe to close
```

### Compressed Batch Format

```
┌──────────────────────────────────────────────────────────┐
│  PS_IOV_ADD_F_COMPRESS header                            │
│  - dst_id                                                │
│  - base_vaddr                                            │
│  - nr_pages (64)                                         │
├──────────────────────────────────────────────────────────┤
│  compressed_size (4 bytes)                               │
├──────────────────────────────────────────────────────────┤
│  LZ4 compressed data (variable)                          │
│  - Original: 64 * 4KB = 256KB                            │
│  - Compressed: typically 100-150KB                       │
└──────────────────────────────────────────────────────────┘
```

---

## Soft Error Handling (UFFD Restore)

During UFFDIO_COPY on the replica side:

| Error | Meaning | Action |
|-------|---------|--------|
| `EAGAIN` | Page temporarily unavailable | Queue for retry |
| `EEXIST` | Page already mapped | Skip (concurrent fault) |
| `ENOENT` | Page unmapped between phases | Track as unmapped |

Background drain threads retry EAGAIN pages iteratively.

---

## Configuration Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `COW_WP_CHUNK_SIZE` | 64MB | Parallel write-protect chunk size |
| `COW_BATCH_PAGES` | 64 | Pages per transfer batch |
| `COW_BATCH_SIZE` | 256KB | Bytes per transfer batch |
| `DIRTY_SCAN_FREEZE_THRESHOLD` | 300,000 | Dirty pages threshold for freeze |
| `NUM_P3_THREADS` | 20 | Parallel sender threads |
| `PAGE_BUFFER_HASH_SIZE` | 1M | Hash table buckets |
| `NUM_HASH_LOCKS` | 8192 | Fine-grained lock count |
| `CHUNK_SIZE` | 256MB | Per-thread page pool size |

---

## File Organization

### Core COW Files (`criu/cow/`)

| File | Lines | Purpose |
|------|-------|---------|
| `cow-dump.c` | ~1,300 | COW dump initialization and lifecycle |
| `cow-mem.c` | ~235 | Global lazy VMA list management |
| `cow-page-xfer.c` | ~13,900 | Page transfer protocol and compression |
| `cow-unified-thread.c` | ~20,900 | PRIMARY side page server thread |
| `cow-uffd.c` | ~53,100 | REPLICA side UFFD handling |
| `cow-bulk-send.c` | ~39,700 | P3 parallel sender threads + scanner |
| `cow-p3-receiver.c` | ~13,400 | P3 parallel receiver threads |
| `cow-bulk-recv.c` | ~15,200 | Bulk page reception protocol |
| `cow-lazy-pages.c` | ~8,400 | Lazy page handling |
| `page-pool.c` | ~7,400 | Per-thread lock-free page buffers |
| `page-state-tracker.c` | ~22,500 | Page state tracking with history |
| `unmapped-tracker.c` | ~5,200 | Track unmapped pages |
| `hung-page-tracker.c` | ~6,500 | Track hung page faults |
| `cow-bitmap.c` | ~70 | Bitmap operations |

**Total**: ~207,000 lines

### Headers (`criu/include/cow/`)

| File | Purpose |
|------|---------|
| `cow-dump.h` | Public COW dump API |
| `cow-mem.h` | Lazy VMA management |
| `cow-page-xfer.h` | Page transfer protocol extensions |
| `cow-unified-thread.h` | Page request/response queues |
| `cow-uffd.h` | UFFD error handling and state |
| `cow-bulk-send.h` | Bulk sender configuration |
| `spsc-queue.h` | Single-Producer Single-Consumer queue |
| `mpsc-queue.h` | Multi-Producer Single-Consumer queue |

---

## Integration Points

### cr-dump.c

- Line ~1549: `cow_dump_init_async()` after parasite infect
- Line ~1583-84: `mdc.pre_dump=false, mdc.lazy=true` for COW mode
- Line ~2380: Redirect to `cr_dump_tasks_cow_phased()`
- Line ~2729: `cow_set_dst_id()` after collect_pstree_ids()
- Line ~2757: `cow_detect_new_vmas()` for Phase 3 VMA detection
- Line ~2785: `cow_signal_last_scan()` before freeze completion
- Line ~2999: `cow_set_phase(COW_PHASE_DONE)`
- Line ~3005: `cow_cleanup_async_uffd()` after unfreeze

### config.c

- Option `--cow-dump` (case 1105) enables COW mode via `opts.cow_dump`

### mem.c

- When `opts.cow_dump` enabled, calls `cow_mem_add_lazy_vma()` to mark VMAs for lazy transfer

### pie/parasite.c

- `PARASITE_CMD_COW_DUMP_INIT` triggers `parasite_cow_dump_init()`
- Creates UFFD with `UFFD_FEATURE_WP_ASYNC` inside target process

---

## Debugging Infrastructure

### Timing Instrumentation

All major phases logged with `gettimeofday()` deltas:
```
TIMING: <operation> took X.XXXXXX seconds
TIMING @X.XXXXXX: <operation> starting
```

### Statistics

- `check_and_print_uffd_stats()`: UFFD fault histogram
- `cow_check_and_print_stats()`: Page server statistics
- `g_compress_uncompressed_bytes`, `g_compress_compressed_bytes`: Compression ratio

### Page State Tracking

`page-state-tracker.c` maintains full page state machine with 16-entry history per page for debugging migration issues.

---

## Usage

Enable COW dump with the `--cow-dump` flag:

```bash
# On PRIMARY (source machine)
sudo criu dump -t <pid> -D /images --cow-dump --lazy-pages --page-server --address <replica-ip> --port 27

# On REPLICA (destination machine)
sudo criu page-server --images-dir /images --port 27 --lazy-pages
sudo criu restore -D /images --lazy-pages
```

---

## Known Limitations

1. **Single process tree**: Current implementation tracks one source process
2. **Kernel requirements**: Requires Linux 5.7+ for `UFFD_FEATURE_WP_ASYNC`
3. **Memory overhead**: 256MB page pools per receiver thread
4. **UFFD cleanup**: Can take minutes on 300GB+ systems (mitigated by chunked cleanup)

---

## Future Improvements

1. Multi-process tree support
2. Adaptive convergence threshold based on write rate
3. Better handling of rapidly-dirtying workloads
4. Integration with container runtimes (containerd, CRI-O)
