# COW-Based Live Migration Design Document


## Introduction
This feature implements COW (Copy-On-Write) based live migration for CRIU, enabling process duplication to remote instances to achieve the goal of: 
1. Minimized downtime at the source. 
2. Making the destination alive ASAP like in the current design of lazy dump.
3. Transfer the data at high speed to complete the process soon and reduce the amount of COW operations.
   
   
The approach uses userfaultfd write-protection to track memory modifications while the process continues running at the source and the destination is loaded same as in the lazy dump implementation. It overcomes the main issue with the lazy dump where the source is frozen during the dump.

## Architecture Overview

### Implementation Contracts (Current Branch)

1. **Bulk close contract**
   - Sender emits `PS_IOV_CLOSE` with `nr_pages == 0`.
   - Receiver replies with a 32-bit status ACK.
   - Sender accepts ACK as success and also accepts clean EOF/close as
     backward-compatible completion for older peers.

2. **Session ownership**
   - COW tracking is dump-session scoped.
   - Task registration is per process in the tree.
   - Monitor thread starts once (just before resume) and stops once.

3. **VMA eligibility**
   - VMAs that failed UFFD WP registration are excluded from COW lazy tracking.
   - Those VMAs are forced through the standard dump path.
   - Lazy VMA lookup during transfer is keyed by `dst_id` + address.

### Data Flow Source


**Phase 1: Setup via Parasite RPC**
  - Create userfaultfd in target process
  - Register VMAs with UFFDIO_REGISTER_MODE_WP
  - Apply write-protection (UFFDIO_WRITEPROTECT)
  - Send userfaultfd back to CRIU
  - Record failed VMA indices (fallback VMAs)

**Phase 2: Base dump and fallback split**
  - VMAs successfully registered for COW are sent via lazy/COW flow
  - VMAs that failed registration are dumped via standard non-COW path

**Phase 3: Session Monitor Thread (Background)**
  - Single monitor thread starts once for the dump session
  - Polls all tracked task UFFDs
  - On write fault:
    1. Read page from /proc/pid/mem (before modification)
    2. Copy the page and store it in hash table
    3. Unprotect page
    4. Wake faulting thread at the source process

**Phase 4: Page Transfer (page_server_get_pages)**
  - Lookup COW pages in hash table
  - Fast path: No COW → splice (zero-copy)
  - Slow path: COW present → buffer + overlay
  - Bulk unprotect after transfer
  - End stream with close marker (`nr_pages==0`) and receiver ACK

#### Detailed design source

##### 1. cow-dump.c (CRIU-side Coordinator)

Main coordinator for COW tracking on the CRIU side. Manages the lifecycle of COW dump operations.

*Key Data Structures*

```c
/* Per-process COW dump state */
struct cow_dump_info {
    struct pstree_item *item;
    int uffd;                      /* userfaultfd from target */
    int proc_mem_fd;               /* /proc/pid/mem handle */
    unsigned long total_pages;     /* Total pages tracked */
    unsigned long dirty_pages;     /* Modified pages count */
    
    /* Hash table: 65K buckets for O(1) lookup */
    struct hlist_head cow_hash[COW_HASH_SIZE];  /* 2^16 buckets */
    pthread_spinlock_t cow_hash_locks[COW_HASH_SIZE]; //Lock for each hash entry to have fine grain locking.
};

/* Hash table entry for copied pages */
struct cow_page {
    unsigned long vaddr;           /* Virtual address */
    void *data;                    /* 4KB page content */
    struct hlist_node hash;        /* Hash linkage */
};

#define COW_HASH_SIZE (1 << 16)    /* 65536 buckets */
```

*Key Functions*

**Init- Initialize COW tracking**
- Opens `/proc/pid/mem` for reading page contents
- Calls parasite RPC to setup userfaultfd
- Receives userfaultfd from parasite
- Initializes hash table and spinlocks
- Init COW monitoring thread


**cow_monitor_thread()** - Background monitoring
- Continuously reads from userfaultfd
- Processes write fault events

**cow_handle_write_fault()** - Handle write fault event
```
Input: fault address
1. Allocate cow_page structure
2. Read page from /proc/pid/mem (BEFORE modification)
3. Add to hash table (thread-safe)
4. Unprotect page (UFFDIO_WRITEPROTECT mode=0)
5. Wake faulting thread (UFFDIO_WAKE)
```


**cow_lookup_and_remove_page()** - Thread-safe page lookup
- Hash-based O(1) lookup
- Removes from hash table atomically

##### 2. pie/parasite.c (In-Process Setup)

Runs inside the target process to setup userfaultfd with write-protection.

**Purpose:** The parasite code is injected into the target process and executes in its context to create and configure the userfaultfd.

*Key Function: parasite_cow_dump_init()*


**Why Parasite-Based?**
1. **Context Requirement:** userfaultfd must be created in target process context
2. **Inheritance:** Automatically inherited by all threads
3. **Permissions:** Avoids ptrace permission issues
4. **Atomic Setup:** All VMAs protected before process resumes


##### 3. page-xfer.c (Page Server Integration)

Integrates COW tracking with page transfer, overlaying modified pages during transfer.

Key Function: page_server_get_pages()

Step 1: Read pages from page_pipe
  page_pipe_read(pp, &pipe_read_dest, vaddr, &nr_pages)

Step 2: Check for COW pages at the hash table, recall each modified page is stored in the hash table (single pass)              
 for each page:                                         
    cow_pages[i] = cow_lookup_and_remove_page(addr)     
    cow_count = number of non-NULL entries 

Fast path: (cow_count is zero, same as in the current lazy implementation)
Zero-copy splice: splice(pipe -> sock) 
No memory copies!


Slow path:  (cow_count is above zero)
1. read(pipe -> buffer)
2. overlay COW pages 

Step 3: Bulk unprotect         
wp.range.start = vaddr       
wp.range.len = len            
wp.mode = 0                   
ioctl(uffd, UFFDIO_WRITEPROTECT)


### Data Flow Destination

Destination behavior is still lazy-pages based, but this fork also includes COW
bulk-stream handling changes:

- bulk end-marker handling sends explicit ACK to unblock sender close path,
- sender side keeps compatibility fallback for peers that close without ACK,
- bulk completion drops copied ranges from IOV tracking to avoid duplicate faults.

```
┌─────────────────────────────────────────────────────────┐
│ Traditional: Sequential (1 request at a time)           │
│                                                         │
│  Request → Wait → Response → Request → Wait → Response  │
│                                                         │
│  Throughput: Limited by RTT                             │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ Aggressive: Pipeline (256 requests in-flight)           │
│                                                         │
│  Request ─┐                                             │
│  Request ─┤                                             │
│  Request ─┤                                             │
│    ...    ├─► In Flight (256 concurrent)                │
│  Request ─┤                                             │
│  Request ─┤                                             │
│  Request ─┘                                             │
│                                                         │
│  Response → IMMEDIATELY refill pipeline                 │
│                                                         │
│  Throughput: Near maximum network bandwidth             │
└─────────────────────────────────────────────────────────┘
```


## Kernel Requirements

### Minimum Kernel Version
**Linux 5.7+** (released May 2020)

### Required Features

| Feature | Flag | Purpose | Since |
|---------|------|---------|-------|
| WP Flag | `UFFD_FEATURE_PAGEFAULT_FLAG_WP` | Identify write faults | 5.7 |


### System Configuration

**Unprivileged Access:**
```bash
# Allow unprivileged userfaultfd
echo 1 > /proc/sys/vm/unprivileged_userfaultfd

# Or require CAP_SYS_PTRACE
```



---

## Recent Architectural Improvements

### dst_id Fix and Page Server Lifecycle

**Problem:** Lazy VMAs stored an encoded `xfer->dst_id` (with type bits shifted
in) while the replica sent raw PIDs in `PS_IOV_GET_ALL` requests. The page
server background thread found zero matching pages, exited, closed the socket.

**Fix:** Lazy VMAs now store `vpid(item)` (the virtual PID) as `dst_id`,
matching what `request_all_remote_pages()` sends. The `write_lazy_vmas_before`
function uses `lve->start`/`lve->end` instead of dereferencing `lve->vma->e`
(which may be freed after dump completes).

**Lifecycle:** `wait_for_page_server_thread()` in `cr-dump.c` ensures the
background thread finishes before `cow_dump_fini()` destroys the hash table.
`send_lazy_vma_page()` handles a missing COW hash lock gracefully by falling
back to `process_vm_readv`.

### VMA Priority Sort

The background page server thread now sorts VMAs by transfer priority before
iterating:

| Priority | VMA Type | Example | Typical Size |
|---|---|---|---|
| 0 | Stack (`VMA_AREA_STACK`) | `[stack]` | 132KB |
| 1 | Heap (`VMA_AREA_HEAP`) | `[heap]` | 132KB |
| 2 | Other (by size ascending) | anonymous, thread stacks | 4KB - 1.95GB |

`VMA_AREA_STACK` is now set for `[stack]` VMAs in `proc_parse.c` (was defined
but never assigned). The ~600 critical pages for process startup arrive in
~20ms, before cutover triggers.

### Page Fault Prefetch

`handle_page_fault()` in `uffd.c` now requests 64 pages centered on the fault
address (clamped to IOV boundaries) instead of a single page. Each batch is one
network round-trip.

### Pagemap Cache Skip

`generate_vma_iovs()` in `mem.c` calls `is_cow_lazy_eligible()` before
`pmc_get_map()`. COW-lazy VMAs bypass the PAGEMAP_SCAN ioctl entirely
(`generate_vma_iovs`: 187ms -> 0.15ms for 40GB).

### Benchmark Results (Valkey)

| Metric | Original | After all opts (40GB) | After all opts (100GB) |
|---|---|---|---|
| Background pages found | 0 (broken) | 521,111 | 27,396,503 |
| Dump freeze | 925ms | 218ms (-72%) | 455ms |
| Cutover window | N/A (hung) | 511ms | ~510ms |
| PING response | N/A | 297ms | ~300ms |
| `parse_maps/smaps` | 468ms | 1.5ms | 1.5ms |
| `generate_vma_iovs` | 187ms | 0.15ms | 0.15ms |
| `lazy iovec memcpy` | 216ms (100GB) | N/A | 0.02ms |

Cutover is data-size-independent: 1GB, 40GB, and 100GB show the same
~510ms window.

### Optimization: parse_maps_cow

`parse_smaps` dominated the dump freeze at ~468ms (40GB). The kernel walks
page tables for `/proc/pid/smaps` to generate RSS/PSS/Referenced counters
that CRIU discards. `parse_maps_cow` reads `/proc/pid/maps` instead (~1.5ms,
constant regardless of data size).

**Implementation:** `criu/proc_parse.c:parse_maps_cow()` — same structure as
`parse_smaps()` but opens `maps`, skips VmFlags handling, applies
`MAP_GROWSDOWN` for `[stack]` VMAs. Gated behind `opts.cow_dump` in
`collect_mappings()`.

**Trade-off:** VmFlags are defaulted to zero. `MAP_LOCKED`, `MAP_DROPPABLE`,
`MADV_*`, shadow stack status are lost in the image. Acceptable for live
migration where the process continues running. See the trade-offs analysis
in `COW_DUMP_README.md`.

### Optimization: Lazy iovec memcpy fix

After `generate_vma_iovs`, the code copies iovec arrays into parasite args
for lazy mode. The copy used `pp->nr_iovs` (total allocated = `nr_priv_pages`,
27.4M for 100GB) instead of `pp->free_iov` (actually populated, ~19 for COW
mode). This copied 438MB of mostly zeros during freeze.

**Fix:** `memcpy(pargs_iovs(args), pp->iovs, sizeof(struct iovec) * pp->free_iov)`

### Remaining Bottleneck

`cow_dump_init` dominates at 413ms (100GB) / 176ms (40GB). This is the
kernel walking page tables to set write-protect bits via
`UFFDIO_WRITEPROTECT`. The cost is O(pages) — the kernel's
`change_protection()` must flip PTE WP bits on every mapped page.

For 100GB Valkey: 18 VMAs, dominated by one 104GB mapping. Batching or
merging REGISTER calls doesn't help (only ~18 ioctls, microseconds of
overhead). The 413ms is pure kernel PTE walk time.

---

## Future Work — Directions and Trade-offs

### 1. PROCMAP_QUERY ioctl for VmFlags recovery (Linux 6.7+)

**Problem:** `parse_maps_cow` defaults VmFlags to zero, losing `MAP_LOCKED`,
`MADV_*`, and shadow stack status in the image.

**Approach:** After parsing `/proc/pid/maps`, use the `PROCMAP_QUERY` ioctl
on `/proc/pid/maps` to retrieve per-VMA `vm_flags` without page table walks.
Each query is O(log n) in the VMA tree — no RSS/PSS computation.

**Trade-offs:**
- Recovers full VmFlags fidelity in the image
- Adds ~18 ioctl calls (one per VMA), microseconds each
- Requires kernel 6.7+; needs fallback for older kernels
- Raw `vm_flags` need translation to CRIU's `MAP_*` / `MADV_*` format
- Moderate implementation complexity

**Expected impact on freeze:** Negligible (microseconds). This is about
image correctness, not performance.

### 2. UFFD_FEATURE_WP_ASYNC — eliminate monitor thread

**Problem:** The COW monitor thread handles synchronous write faults:
read page, copy to hash, unprotect, wake thread. This adds jitter to
the source process and requires a dedicated thread.

**Approach:** With `WP_ASYNC` (Linux 6.1+), the kernel auto-resolves
write faults by clearing the WP bit without delivering to userspace.
Dirty pages are later identified via `PAGEMAP_SCAN` with
`PM_SCAN_WP_MATCHING`.

**Trade-offs:**
- Eliminates monitor thread and source-side write jitter
- Simpler architecture: no hash table, no per-fault page copies
- Does NOT reduce initial UFFDIO_WRITEPROTECT cost (still O(pages))
- Requires a second-pass scan before transfer to identify dirty pages
- Changes the transfer model: instead of "snapshot on first write", it
  becomes "mark dirty, scan later, read current content"
- Risk: page content may change between scan and read (need careful
  ordering or a final freeze-and-scan pass before cutover)
- The current codebase already checks for WP_ASYNC support
  (`cow-dump.c:136`) but uses synchronous WP faults

**Expected impact on freeze:** None (doesn't change initial WP setup).
Impact is on background transfer latency and source-side jitter.

### 3. Deferred WRITEPROTECT — reduce freeze to ~42ms (IN PROGRESS)

**Problem:** `cow_dump_init` costs 413ms (100GB) / 176ms (40GB). This
is the kernel walking PTEs to set write-protect bits via
`UFFDIO_WRITEPROTECT`. The cost is O(pages).

**Key insight (verified):** `UFFDIO_REGISTER` and `UFFDIO_WRITEPROTECT`
both operate on `ctx->mm` (target's mm set at uffd creation), not
`current->mm`. CRIU can call them from its own process after the target
resumes. `UFFDIO_REGISTER` is cheap (VMA metadata, no PTE walk).
`UFFDIO_WRITEPROTECT` is the expensive O(pages) part.

**Approach:** Keep REGISTER in the parasite during freeze (cheap,
enables fallback on failure). Defer WRITEPROTECT to after resume —
CRIU applies it from its own process on the uffd fd.

**Implementation status:** Prototype built and tested. Results:
- Freeze confirmed at **41ms** for 100GB (down from 455ms)
- WP applied post-resume in 406ms (process running, not frozen)
- 1GB migration: **works end-to-end**
- 40GB/100GB migration: page transfer completes but **bulk stream
  close protocol fails** — source sends all pages (11.5M/27M), replica
  never ACKs the close marker. Replica Valkey doesn't respond.

**Root cause of failure:** The close-protocol handshake between
`page-xfer.c` (source) and `uffd.c` (replica) times out at scale.
Source logs: "Timed out waiting for close acknowledgment". Replica
logs: stuck in `page_server_start_read` loop. The page transfer itself
completes correctly (all pages sent, 526 COW overlays, 4871 demand
requests served). The issue is in the close/teardown sequence, not
data transfer.

**Stashed changes:** `git stash` contains the working prototype:
- `criu/pie/parasite.c`: WRITEPROTECT removed from per-VMA loop
- `criu/cow-dump.c`: new `cow_dump_apply_writeprotect()` function
- `criu/cr-dump.c`: reordered post-resume flow (resume → monitor →
  WP → page server)
- `criu/include/cow-dump.h`: declaration

**To complete:** Debug the bulk stream close protocol interaction.
The close handshake (`nr_pages==0` marker, 32-bit ACK) may have a
timing dependency on WP being active during the image creation
phase. Investigate `page-xfer.c:2031-2052` (close timeout) and
`uffd.c` (ACK handling).

**Safety considerations for deferred WP:**
- Writes between resume and WP completion are untracked — page server
  reads current content via `process_vm_readv` (correct for live
  migration, not for exact-snapshot checkpointing)
- VMA mutations (mmap/munmap) between resume and WP: handle
  `UFFDIO_WRITEPROTECT` returning `-ENOENT` gracefully
- `UFFDIO_WRITEPROTECT` holds `mmap_write_lock`, blocking
  `process_vm_readv` — page server must not run concurrently with WP
  on the same mm

**Other options explored (not viable):**
- Batch UFFDIO_REGISTER calls: only 18 VMAs, no benefit
- Single UFFDIO_WRITEPROTECT for entire VA: can't span gaps
- Soft-dirty tracking: `clear_refs` also walks page tables (similar cost)
- Kernel-side async WP setup: would require upstream kernel patches

### 4. Reduce communication overhead between source and destination

Currently the communication is driven by the destination which sends
requests. Improvement: make the source push pages proactively and have
the destination only request pages on read faults. Reduces round-trips.

### 5. Multithreaded source transfer

Parallelize page reading and network transfer on the source side.
Multiple threads could read different VMA ranges simultaneously via
`process_vm_readv` while a sender thread handles the network.

### 6. Non-registerable VMAs

Some VMAs cannot be write-protected via UFFD (e.g., certain shared
mappings). These fall back to the standard dump path via the
`failed_indices` mechanism in the parasite. The fallback is functional
but means those VMAs' pages must be fully captured during freeze.


### Usage
```bash
criu dump --cow-dump --lazy-pages ...
```

## Appendix - Statistics and Monitoring

### COW Tracking Statistics

**Per-Second Logging:**
```
[COW_STATS] events: wr=1234 fork=0 remap=0 unk=0 | 
            ops: copied=1234 unprot=1234 woken=1234 | 
            errs: alloc=0 read=0 unprot_err=0 wake_err=0 
                  read_err=0 eagain_err=0
```

**Metrics:**

| Metric | Description | Good Value | Alert If |
|--------|-------------|------------|----------|
| `wr` | Write faults | Varies | - |
| `copied` | Pages copied | = wr | < wr |
| `unprot` | Pages unprotected | = wr | < wr |
| `woken` | Threads woken | = wr | < wr |
| `alloc_failures` | Allocation failures | 0 | > 0 |
| `read_failures` | Read failures | 0 | > 0 |
| `eagain_errors` | EAGAIN on read | Low | High |

### Page Server Statistics

**Per-Second Logging:**
```
[PAGE_SERVER_STATS] get_pages: reqs=500 with_cow=50 no_cow=450 
                               pages=8000 cow=400 errs=0 | 
                    serve: open2=1 parent=0 add_f=7950 get=500 
                          close=1
```

**Metrics:**

| Metric | Description | Indicates |
|--------|-------------|-----------|
| `reqs` | Total requests | Transfer activity |
| `with_cow` | Slow path taken | COW overlay needed |
| `no_cow` | Fast path taken | Zero-copy efficiency |
| `pages` | Total pages transferred | Bandwidth |
| `cow` | COW pages overlaid | Write activity |

### UFFD Daemon Statistics

**Per-Second Logging:**
```
[UFFD_STATS] reqs=1000(pf:50,bg:950) pages=8000 pipe_avg=180
  PF:  4K=30 64K=15 128K=5
  BG:  4K=100 64K=500 128K=200 256K=100 512K=50
```

**Histograms:**
- **PF (Page Fault):** Destination-initiated requests
- **BG (Background):** Proactive prefetch

**Pipeline Depth:**
- `pipe_avg`: Average in-flight requests
- Target: Close to `max_pipeline_depth` (256)
