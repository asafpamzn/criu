# COW Live Migration — T3 Register Re-capture: Engineering Guide

## Overview

This document walks through every step of the COW (copy-on-write) live
migration with T3 register re-capture. It's aimed at engineers who need
to understand, debug, or extend the code.

## Architecture

```
Source Machine                              Replica Machine
─────────────                              ───────────────
Valkey (running, serving clients)

Phase 1: CRIU Dump (23ms freeze)
  ├─ Freeze process (ptrace seize)
  ├─ Capture: VMAs, pagemap, FDs, regs     CRIU Restore
  ├─ Inject userfaultfd (WP tracking)        ├─ Fork process tree
  ├─ Resume process                          ├─ Map VMAs
  └─ Start page-server                      └─ Start page-recv
       │                                          │
       │         8 TCP streams (LZ4)               │
       ├──────────────────────────────────────────►│
       │         Bulk pages (~60s)                 │
       │                                           │
Phase 2: Convergence                               │
  ├─ Fork T1 snapshot                        Receives pages via
  ├─ Send T1 dirty pages                    process_vm_writev
  ├─ Send post-fork T2 dirty pages           │
  ├─ SIGSTOP (second freeze)                 │
  ├─ Send T3 dirty pages                     │
  │                                           │
  │  NEW: T3 Register Re-capture             │
  ├─ Capture T3 regs (all threads)     ────►  Saves t3_regs.dat
  ├─ Send libc rw- from frozen source  ────►  Installs arena pages
  ├─ Send VMA diff (new VMAs)          ────►  Injects mmap
  ├─ SIGCONT source                          │
  │                                           │
Phase 3: Cutover (1ms)                  Finalize Restore
  ├─ Source frozen for cutover           ├─ Load t3_regs.dat
  └─ Replica takes over                 ├─ Apply T3 regs (PTRACE_SETREGSET)
                                         ├─ Detach workers (10ms settle)
                                         └─ Detach main thread → LIVE
```

## Code Flow: Step by Step

### 1. Migration Start (`scripts/migrate.sh`)

**File**: `scripts/migrate.sh`

The orchestrator script. Key steps:

```
Line 70: CONFIG SET save "" (disable background saves)
Line 72: No CLIENT PAUSE (T3 regs make this unnecessary)
Line 82: PID=$(pgrep -x valkey-server)
Line 108: CRIU_ARGS=(sudo ... criu dump --tree $PID --cow-dump --lazy-pages ...)
Line 110: COW_PRE_FREEZE_CMD="" (no pre-freeze quiesce needed)
```

The script launches CRIU dump, waits for the page server to be ready,
then monitors progress. No CLIENT PAUSE is needed because T3 register
re-capture makes thread state at dump time irrelevant.

### 2. CRIU Dump — Phase 1 (`criu/cr-dump.c`)

**File**: `criu/cr-dump.c`, function `cr_dump_tasks()` (~line 2387)

**What happens during the 23ms freeze:**

```
cr_dump_tasks()
  ├─ cow_inject_userfaultfd(pid)        [cow-dump.c]
  │    Creates userfaultfd via ptrace syscall injection (~5ms)
  │    PTRACE_SEIZE → inject SVC(userfaultfd) → pidfd_getfd → inject close
  │
  ├─ collect_pstree()                   [seize.c]
  │    PTRACE_SEIZE all 21 threads
  │
  ├─ dump_one_task()                    [cr-dump.c:1612]
  │    ├─ collect_mappings()            Parse /proc/pid/maps → VMA list
  │    ├─ collect_fds()                 Read /proc/pid/fd → FD table
  │    ├─ parasite_infect_seized()      Inject parasite blob
  │    ├─ cow_dump_init()              [cow-dump.c:734]
  │    │    Register VMAs with userfaultfd for WP tracking
  │    ├─ parasite_dump_pages_seized()  Scan pagemap (~8ms)
  │    ├─ dump_task_threads()           Capture registers via ptrace
  │    ├─ compel_cure()                 Remove parasite
  │    └─ dump_task_mm()                Write MM image (VMA metadata)
  │
  ├─ COW early resume                   [cr-dump.c:2562]
  │    Process unfrozen — clients resume immediately
  │
  └─ cow_dump_start_wp() / finish_wp()  [cow-dump.c]
       Apply write-protect AFTER unfreeze (WP_ASYNC mode)
       UFFDIO_WRITEPROTECT on all registered VMAs
```

**Total freeze time: ~23ms** (dominated by parasite infect + pagemap scan)

### 3. Page Server — Bulk Transfer (`criu/page-xfer.c`)

**File**: `criu/page-xfer.c`, function `page_server_serve()` → multi-stream

**Bulk transfer (~60s for 200GB):**

```
page_server_serve()
  └─ Multi-TCP section (~line 4320)
       ├─ fork_source_snapshot(source_pid)  [line 2960]
       │    PTRACE_SEIZE → inject clone() → fork child → PTRACE_DETACH
       │    Creates COW fork for consistent bulk read (T0 snapshot)
       │
       ├─ 8 stream worker threads           [stream_worker_func]
       │    Each reads assigned VMA ranges from fork via process_vm_readv
       │    Compresses with LZ4, sends over TCP
       │    ~1650 MB/s throughput (3200 MB/s on 16xl)
       │
       └─ Signal bulk_send_done
            Orchestrator starts live workload
```

**Key**: All bulk pages come from a single COW fork (T0). This gives
temporal consistency for the initial transfer. Convergence handles
pages that change after T0.

### 4. Convergence (`criu/page-xfer.c`)

**File**: `criu/page-xfer.c`, function `cow_converge_dirty_pages_parallel()`

**After bulk transfer, while source runs live traffic:**

```
cow_converge_dirty_pages_parallel()
  │
  ├─ Fork T1 convergence snapshot           [line 3576]
  │    Source briefly SIGSTOP'd, fork, SIGCONT
  │    Reads dirty pages from fork (T1 consistency)
  │
  ├─ Send T1 dirty pages                    [line 3647]
  │    PAGEMAP_SCAN finds pages written since T0
  │    ~39 dirty pages typical for quiesced, ~30K for live
  │
  ├─ Post-fork T2 dirty pages
  │    Pages dirtied while reading T1 fork
  │    Read from live source via process_vm_readv
  │
  ├─ Second freeze (T3)
  │    kill(source_pid, SIGSTOP)
  │    Scan + send final dirty pages (T3)
  │
  ├─ *** T3 REGISTER RE-CAPTURE ***
  │    capture_and_send_t3_regs()  (see §5 below)
  │    Re-send libc rw- from frozen source
  │
  ├─ VMA diff detection
  │    Scan /proc/pid/maps for new VMAs created after dump
  │    Send PS_IOV_VMA_DIFF to replica
  │
  └─ SIGCONT source
```

### 5. T3 Register Re-capture (`criu/page-xfer.c`)

**File**: `criu/page-xfer.c`, function `capture_and_send_t3_regs()` (~line 3125)

**The key innovation. Called while source is SIGSTOP'd at T3:**

```
capture_and_send_t3_regs(source_pid, socket, dst_id)
  │
  ├─ opendir("/proc/{pid}/task/")
  │    Enumerate all thread TIDs
  │    Sort ascending (matches dump-time thread index order)
  │
  ├─ For each thread (21 threads, ~5ms total):
  │    ├─ ptrace(PTRACE_SEIZE, tid)
  │    ├─ ptrace(PTRACE_INTERRUPT, tid)
  │    │    Transitions from group-stop to ptrace-stop
  │    ├─ waitpid(tid)
  │    ├─ ptrace(PTRACE_GETREGSET, tid, NT_PRSTATUS, &gp_regs)
  │    │    Captures: x0-x30, sp, pc, pstate (272 bytes)
  │    ├─ ptrace(PTRACE_GETREGSET, tid, 0x401, &tls)
  │    │    Captures: tpidr_el0 (8 bytes)
  │    └─ ptrace(PTRACE_DETACH, tid)
  │         Thread returns to group-stop (SIGSTOP still active)
  │
  ├─ Send PS_IOV_T3_REGS header
  │    { cmd=12, nr_pages=21, vaddr=0, dst_id }
  │
  └─ Send payload: 21 × struct t3_thread_regs (5880 bytes)
       struct t3_thread_regs {
           u64 regs[31];   // x0-x30
           u64 sp, pc, pstate, tls;
       };
```

**After T3 regs sent, also re-send libc rw- from frozen source:**

```
  └─ converge_dispatch_parallel(img, source_pid, ..., &libc_rw_region, 1)
       Sends 2 pages of libc rw- data segment from SIGSTOP'd source
       libc rw- is file-backed (not MAP_ANONYMOUS) so WP never tracks it
       Explicit re-send from frozen source is the ONLY delivery path
```

### 6. Page Receiver (`tools/page-recv.c`)

**File**: `tools/page-recv.c`, function `stream_worker()`

**Standalone receiver on the replica. Handles the wire protocol:**

```
stream_worker()
  │
  ├─ Receives page_server_iov headers
  ├─ For PS_IOV_ADD_F / PS_IOV_ADD_F_COMPRESS:
  │    Decompress (LZ4) → process_vm_writev into restored process
  │
  ├─ For PS_IOV_VMA_DIFF:
  │    Write new_vmas.dat → signal vma_diff_ready
  │    Wait for CRIU to create VMAs → resume
  │
  ├─ For PS_IOV_T3_REGS:                    [NEW]
  │    recv nr_threads × sizeof(t3_thread_regs)
  │    Write to {images_dir}/t3_regs.dat:
  │      4 bytes: thread count
  │      N × 280 bytes: register data per thread
  │
  └─ On connection close: report stats, signal STAGED
```

### 7. CRIU Restore — Apply T3 Registers (`criu/cr-restore.c`)

**File**: `criu/cr-restore.c`

**Two key locations:**

**A. Load T3 regs + skip arena reset (~line 3461):**

```
restore_root_task()
  │
  ├─ load_t3_regs()                         [line 2930]
  │    Opens {imgs_dir}/t3_regs.dat
  │    Reads: count + array of t3_thread_regs
  │    Sets g_t3_regs + g_t3_regs_count
  │
  ├─ if (g_t3_regs):
  │    "T3 regs: skipping arena reset + alloc cleanup"
  │    All workarounds skipped — registers match T3 memory
  │
  └─ else (fallback — T3 regs not available):
       Arena mutex zero, tcache null, io_threads blob zero,
       signal_handler_lock unlock, FUTEX_WAKE injection
```

**B. Apply T3 regs before detach (~line 2981):**

```
finalize_restore_detach()
  │
  ├─ For each thread i:
  │    ├─ arch_set_thread_regs_nosigrt()     (no-op on aarch64)
  │    │
  │    ├─ if (g_t3_regs && i < g_t3_regs_count):
  │    │    ├─ Build user_regs_struct from g_t3_regs[i]
  │    │    │    regs.regs[0..30] = t3.regs[0..30]
  │    │    │    regs.sp = t3.sp
  │    │    │    regs.pc = t3.pc
  │    │    │    regs.pstate = t3.pstate
  │    │    │
  │    │    ├─ ptrace(PTRACE_SETREGSET, pid, NT_PRSTATUS, &regs)
  │    │    │    Overrides dump-time registers with T3 state
  │    │    │
  │    │    └─ ptrace(PTRACE_SETREGSET, pid, 0x401, &tls)
  │    │         Sets tpidr_el0 to T3 TLS value
  │    │
  │    └─ PTRACE_DETACH pid
  │         Thread resumes with T3 registers + T3 memory
  │
  ├─ Workers detached first (resume into wait syscalls)
  ├─ 10ms settle
  └─ Main thread detached last (enters event loop)
```

## Data Structures

### Wire Protocol: PS_IOV_T3_REGS (command 12)

```c
// Header (same as all page-server commands)
struct page_server_iov {
    u32 cmd;        // encode_ps_cmd(PS_IOV_T3_REGS, 0)
    u32 nr_pages;   // number of threads (reused field)
    u64 vaddr;      // 0
    u64 dst_id;     // process identifier
};

// Payload: nr_pages × sizeof(t3_thread_regs)
struct t3_thread_regs {
    u64 regs[31];   // x0-x30 general purpose registers
    u64 sp;         // stack pointer
    u64 pc;         // program counter
    u64 pstate;     // processor state (flags)
    u64 tls;        // tpidr_el0 (thread-local storage base)
};
// 280 bytes per thread × 21 threads = 5880 bytes total
```

### Thread Index Mapping

The thread index `i` is invariant across all phases:

```
Source (dump time):   readdir(/proc/pid/task/) sorted ascending → threads[i]
Source (T3 capture):  readdir(/proc/pid/task/) sorted ascending → t3_regs[i]
Replica (restore):    pstree_item->threads[i] → same order

threads[0] = main thread (lowest TID)
threads[1] = first worker
threads[2..16] = IO threads
threads[17..20] = bio threads
```

## Why It Works: The Temporal Consistency Argument

**Before T3 re-capture:**
- Registers: T_dump (time 0)
- Memory: T0 (bulk) + T1/T2/T3 (dirty) = T3 for changed pages
- Arena: T_dump (libc rw- not WP-tracked)
- Result: 60s mismatch → deadlocks, data corruption

**After T3 re-capture:**
- Registers: T3 (re-captured from frozen source)
- Memory: T0 (bulk) + T1/T2/T3 (dirty) = T3 for changed pages
- Arena: T3 (re-sent from frozen source)
- Result: everything at T3 → consistent → deterministic 7/7

**Critical insight**: `vma_entry_can_be_lazy()` requires `MAP_ANONYMOUS`.
The libc rw- segment (mapped from libc.so) is file-backed, so WP is
never applied. Dirty tracking never captures arena changes. The explicit
2-page re-send from the frozen source is the only mechanism that delivers
T3 arena state to the replica.

## Files Modified

| File | Lines Changed | Purpose |
|------|--------------|---------|
| `criu/page-xfer.c` | +149 | T3 capture, protocol, skip re-send |
| `criu/cr-restore.c` | +71 | Load T3 regs, apply, skip cleanup |
| `tools/page-recv.c` | +54 | Receive T3 regs, write t3_regs.dat |
| `scripts/migrate.sh` | -30 | Remove CLIENT PAUSE |
