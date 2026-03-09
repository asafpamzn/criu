# CRIU COW Dump: Design + Code Map

Design and code walkthrough for the COW live migration with T3 register
re-capture. For setup see `COW_DEVELOPER.md`, for overview see
`COW_DUMP_README.md`.

## Architecture

```
PRIMARY (source)                                    REPLICA (destination)
──────────────────────────────────────              ─────────────────────────────────────
Valkey (running)                                     page-recv (standalone, 8 streams)
  ↑  writes                                             ↑   process_vm_writev
  │                                                      │
CRIU dump process                                        CRIU restore
  ├─ parasite RPC: create UFFD + register VMAs (WP)         ├─ fork process tree, map VMAs
  ├─ apply UFFDIO_WRITEPROTECT (WP_ASYNC, post-resume)      ├─ apply T3 registers
  ├─ page server: 8-stream bulk + convergence + T3 regs     └─ detach threads
  └─ fork snapshot for consistent bulk read
```

## Dump-Side Flow (Source)

### 1. Freeze + Dump (23ms)

**File**: `criu/cr-dump.c`, function `cr_dump_tasks()`

```
cr_dump_tasks()
  ├─ cow_inject_userfaultfd(pid)        [cow-dump.c]
  │    Creates userfaultfd via ptrace syscall injection
  │    PTRACE_SEIZE → inject SVC(userfaultfd) → pidfd_getfd
  │
  ├─ collect_pstree()                   [seize.c]
  │    PTRACE_SEIZE all threads
  │
  ├─ dump_one_task()                    [cr-dump.c]
  │    ├─ collect_mappings()            Parse /proc/pid/maps
  │    ├─ parasite_infect_seized()      Inject parasite blob
  │    ├─ cow_dump_init()              [cow-dump.c]
  │    │    Register VMAs with UFFDIO_REGISTER_MODE_WP
  │    ├─ parasite_dump_pages_seized()  Scan pagemap
  │    ├─ dump_task_threads()           Capture registers
  │    ├─ compel_cure()                 Remove parasite
  │    └─ dump_task_mm()                Write MM image
  │
  ├─ COW early resume                   [cr-dump.c]
  │    Process unfrozen — clients resume immediately
  │
  └─ cow_dump_start_wp() / finish_wp()  [cow-dump.c]
       Apply write-protect AFTER unfreeze (WP_ASYNC mode)
       UFFDIO_WRITEPROTECT in 256MB chunks, worker threads
```

### 2. Bulk Transfer (~60s for 200GB)

**File**: `criu/page-xfer.c`, function `unified_page_server_thread()`

```
page_server_serve()
  └─ Multi-TCP section
       ├─ fork_source_snapshot(source_pid)
       │    PTRACE_SEIZE → inject clone() → fork child
       │    Creates COW fork for consistent bulk read (T0)
       │
       ├─ 8 stream worker threads  [stream_worker_func]
       │    Each reads assigned VMA ranges via process_vm_readv
       │    Compresses with LZ4, sends over TCP
       │    VMAs > pages_per_worker are pre-split across workers
       │
       └─ Signal bulk_send_done
```

### 3. Convergence

**File**: `criu/page-xfer.c`, function `cow_converge_dirty_pages_parallel()`

```
cow_converge_dirty_pages_parallel()
  │
  ├─ Fork T1 convergence snapshot
  │    Source briefly SIGSTOP'd, fork, SIGCONT
  │    Reads dirty pages from fork (T1 consistency)
  │
  ├─ Pre-freeze dirty scan (source running)
  │    PAGEMAP_SCAN finds dirty pages (~20 pages at T3)
  │
  ├─ T3 freeze (52ms total, no fork)
  │    kill(source_pid, SIGSTOP)
  │    ├─ Signal handlers via parasite re-inject (7ms)
  │    ├─ Registers via PTRACE_GETREGSET (<1ms)
  │    ├─ FD table from /proc/pid/fd (<1ms)
  │    ├─ Dirty pages from frozen source (<1ms)
  │    ├─ libc rw- re-send (<1ms)
  │    └─ SIGCONT
  │
  ├─ VMA diff detection (source running)
  │    Scan /proc/pid/maps for new VMAs
  │    Send PS_IOV_VMA_DIFF to replica
  │
  └─ SIGCONT source
```

### 4. T3 Register Re-capture

**File**: `criu/page-xfer.c`, function `capture_and_send_t3_regs()`

Called while source is SIGSTOP'd at T3:

```
capture_and_send_t3_regs(source_pid, socket, dst_id)
  │
  ├─ opendir("/proc/{pid}/task/")
  │    Enumerate all thread TIDs
  │    qsort ascending (matches dump-time thread index)
  │
  ├─ For each thread (~21 threads, ~5ms total):
  │    ├─ ptrace(PTRACE_SEIZE, tid)
  │    ├─ ptrace(PTRACE_INTERRUPT, tid)
  │    ├─ waitpid(tid)
  │    ├─ ptrace(PTRACE_GETREGSET, tid, NT_PRSTATUS, &gp_regs)
  │    │    Captures: x0-x30, sp, pc, pstate (272 bytes)
  │    ├─ ptrace(PTRACE_GETREGSET, tid, 0x401, &tls)
  │    │    Captures: tpidr_el0 (8 bytes)
  │    └─ ptrace(PTRACE_DETACH, tid)
  │
  ├─ Send PS_IOV_T3_REGS header
  │    { cmd=12, nr_pages=nr_threads, vaddr=0, dst_id }
  │
  └─ Send payload: nr_threads × struct t3_thread_regs
```

After T3 regs, re-send libc rw- from frozen source:

```
  └─ converge_dispatch_parallel(img, source_pid, ..., &libc_rw_region, 1)
       Sends 2 pages of libc rw- data segment from SIGSTOP'd source
       libc rw- is file-backed (not MAP_ANONYMOUS) so WP never tracks it
       Explicit re-send is the ONLY delivery path
```

## Restore-Side Flow (Replica)

### 5. CRIU Restore + page-recv

**Files**: `criu/cr-restore.c`, `tools/page-recv.c`

```
restore_root_task()
  ├─ Fork process tree, map VMAs, start restorer
  │    All threads stopped in ptrace-trap
  │
  ├─ run_page_recv()
  │    Fork page-recv in the ptrace-trap window
  │    page-recv connects 8 TCP streams to source
  │    Receives pages via process_vm_writev
  │    Writes t3_regs.dat and new_vmas.dat
  │
  ├─ inject_new_vmas()
  │    Read new_vmas.dat
  │    For each new VMA: ptrace inject mmap(MAP_FIXED)
  │
  ├─ load_t3_regs()
  │    Read t3_regs.dat → g_t3_regs + g_t3_regs_count
  │    If missing: abort restore
  │
  └─ restore_rseq_cs()
```

### 6. Apply T3 Registers + Detach

**File**: `criu/cr-restore.c`, function `finalize_restore_detach()`

```
finalize_restore_detach()
  │
  ├─ For each thread i:
  │    ├─ arch_set_thread_regs_nosigrt()
  │    │
  │    ├─ if (g_t3_regs && i < g_t3_regs_count):
  │    │    ├─ Build user_regs_struct from g_t3_regs[i]
  │    │    ├─ ptrace(PTRACE_SETREGSET, pid, NT_PRSTATUS, &regs)
  │    │    └─ ptrace(PTRACE_SETREGSET, pid, 0x401, &tls)
  │    │
  │    └─ Track main_idx
  │
  ├─ Detach workers first (resume into wait syscalls)
  └─ Detach main thread last (enters event loop)
```

## Wire Protocol

### PS_IOV_T3_REGS (command 12)

```c
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
    u64 pstate;     // processor state
    u64 tls;        // tpidr_el0
};
// 280 bytes per thread × 21 threads = 5880 bytes total
```

### PS_IOV_VMA_DIFF (command 11)

```c
// Header: page_server_iov with cmd=PS_IOV_VMA_DIFF
// Payload: nr_pages × sizeof(vma_diff_entry)
struct vma_diff_entry {
    u64 start;
    u64 end;
    u32 prot;
    u32 pad;
};
```

### Thread Index Invariance

```
Source (dump time):   readdir(/proc/pid/task/) sorted ascending → threads[i]
Source (T3 capture):  readdir(/proc/pid/task/) sorted ascending → t3_regs[i]
Replica (restore):    pstree_item->threads[i] → same order

threads[0] = main thread (lowest TID)
threads[1..N] = worker threads
```

## Why T3 Regs Work

**Before T3 re-capture:**
- Registers: T_dump (time 0)
- Memory: T3 (60 seconds later)
- Result: 60s mismatch → deadlocks, data corruption

**After T3 re-capture:**
- Registers: T3 (re-captured from frozen source)
- Memory: T3 (bulk + convergence + dirty)
- Arena: T3 (libc rw- re-sent from frozen source)
- Result: everything at T3 → consistent → deterministic 7/7

## Implementation Contracts

1. **COW session**: scoped to `g_cow_info`, per dump session.
2. **VMA eligibility**: same filters as lazy-pages. Non-eligible VMAs
   dumped via normal path.
3. **Stream termination**: `PS_IOV_CLOSE` with `nr_pages == 0`. No ACK.
   Receiver close after marker treated as clean (`EPIPE` ok).
4. **Teardown**: `wait_for_page_server_thread()` before `cow_dump_fini()`.
5. **T3 regs required**: restore aborts if `t3_regs.dat` missing.

## Threading Model

**Source (dump process):**
- main thread: orchestrates dump + resumes process
- `criu-page-srv` thread: 8-stream bulk transfer + convergence
- `cow-wp` worker threads: parallel UFFDIO_WRITEPROTECT

**Replica:**
- `page-recv` process: 8 TCP streams, process_vm_writev
- `restore` process: applies T3 registers, injects VMAs, detaches

## Kernel Requirements

- Linux 6.1+ (userfaultfd WP_ASYNC requires 6.7+)
- `sysctl vm.unprivileged_userfaultfd=1`
