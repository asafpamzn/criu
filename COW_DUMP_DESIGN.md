# CRIU COW Dump: Design + Code Map

Design and code walkthrough for the COW live migration with converge register
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
  ├─ apply UFFDIO_WRITEPROTECT (WP_ASYNC, post-resume)      ├─ apply converge registers (PTRACE_SETREGSET)
  ├─ eBPF attach (fentry/do_wp_page, zero gap after WP)     └─ detach threads
  ├─ page server: 8-stream bulk (from live source)
  ├─ converge: SIGSTOP → eBPF drain → state capture → SIGCONT
  └─ VMA diff detection + send
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

### 2. eBPF Dirty Tracker (attached right after WP)

**Files**: `criu/cr-dump.c` (attach), `criu/cow-bpf.c` (userspace),
`criu/bpf/dirty_track.bpf.c` (kernel)

```
cr_dump_tasks()
  └─ after WP applied + process resumed:
       cow_bpf_start(pid)
         ├─ dirty_track_bpf__open()
         ├─ set rodata->target_pid
         ├─ dirty_track_bpf__load() + __attach()
         └─ fentry/do_wp_page hook active
              Filters by PID, pushes page address to 64MB ring buffer
              O(1) per fault, no userspace involvement until drain
```

### 3. Bulk Transfer (~60s for 200GB)

**File**: `criu/page-xfer.c`, function `page_server_serve()`

```
page_server_serve()
  └─ Multi-TCP section
       ├─ 8 stream worker threads  [stream_worker_func]
       │    Each reads assigned VMA ranges via process_vm_readv
       │    Reads directly from live source (no fork)
       │    Compresses with LZ4, sends over TCP
       │
       └─ Signal bulk_send_done
```

No fork snapshot — WP_ASYNC ensures pages read before any write
have dump-time content. Dirty pages are re-sent at converge.

### 3b. Pre-Converge Sigacts Capture (~2ms, source live)

**File**: `criu/page-xfer.c`, function `capture_and_send_converge_sigacts()`

```
capture_and_send_converge_sigacts(source_pid, ..., already_stopped=false)
  │
  ├─ PTRACE_SEIZE main thread only
  ├─ PTRACE_INTERRUPT + waitpid
  │
  ├─ For each signal 1-64 (skip SIGKILL, SIGSTOP):
  │    Write SVC+BRK at PC
  │    Set regs: rt_sigaction(sig, NULL, &oldact, 8)
  │    PTRACE_CONT → wait SIGTRAP
  │    Read oldact from stack (handler, flags, mask)
  │
  ├─ Restore original code + stack + regs
  ├─ PTRACE_DETACH — thread resumes immediately
  │
  └─ Send PS_IOV_CONVERGE_SIGACTS (2KB)

No parasite, no collect_mappings, no compel_cure.
Single thread paused ~2ms. Other threads unaffected.
```

### 4. Converge Phase (~29ms)

**File**: `criu/page-xfer.c`, function `cow_converge_dirty_pages_parallel()`

```
cow_converge_dirty_pages_parallel()
  │
  ├─ kill(source_pid, SIGSTOP)
  │    Poll /proc/pid/status for State:T (verified stop)
  │
  ├─ eBPF ring drain (cow_bpf_drain)         ~0ms
  │    Sort + dedup + coalesce → region list
  │    If drops detected → fall back to PAGEMAP_SCAN
  │    94-230 dirty pages typical
  │
  ├─ converge state capture
  │    ├─ Registers via PTRACE_GETREGSET            ~5ms
  │    └─ FD table from /proc/pid/fd                ~1ms
  │    (sigacts already captured pre-freeze)
  │
  ├─ Dirty page dispatch (from frozen source)       ~5ms
  │    process_vm_readv → LZ4 → TCP (8 streams)
  │
  ├─ Non-lazy re-send (stacks + lib .data/.bss)     ~5ms
  │    127 pages (508KB) — file-backed rw + [stack]
  │
  ├─ VMA diff detection (source still frozen)        ~5ms
  │    Scan /proc/pid/maps for new VMAs
  │    Send PS_IOV_VMA_DIFF + page data to replica
  │
  └─ kill(source_pid, SIGCONT)
```

### 4. Converge Register Re-capture

**File**: `criu/page-xfer.c`, function `capture_and_send_converge_regs()`

Called while source is SIGSTOP'd at converge:

```
capture_and_send_converge_regs(source_pid, socket, dst_id)
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
  ├─ Send PS_IOV_CONVERGE_REGS header
  │    { cmd=12, nr_pages=nr_threads, vaddr=0, dst_id }
  │
  └─ Send payload: nr_threads × struct converge_thread_regs
```

After converge regs, re-send libc rw- from frozen source:

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
  │    Writes converge_regs.dat and new_vmas.dat
  │
  ├─ inject_new_vmas()
  │    Read new_vmas.dat
  │    For each new VMA: ptrace inject mmap(MAP_FIXED)
  │
  ├─ load_converge_regs()
  │    Read converge_regs.dat → g_converge_regs + g_converge_regs_count
  │    If missing: abort restore
  │
  └─ restore_rseq_cs()
```

### 6. Apply Converge Registers + Detach

**File**: `criu/cr-restore.c`, function `finalize_restore_detach()`

```
finalize_restore_detach()
  │
  ├─ For each thread i:
  │    ├─ arch_set_thread_regs_nosigrt()
  │    │
  │    ├─ if (g_converge_regs && i < g_converge_regs_count):
  │    │    ├─ Build user_regs_struct from g_converge_regs[i]
  │    │    ├─ ptrace(PTRACE_SETREGSET, pid, NT_PRSTATUS, &regs)
  │    │    └─ ptrace(PTRACE_SETREGSET, pid, 0x401, &tls)
  │    │
  │    └─ Track main_idx
  │
  ├─ Detach workers first (resume into wait syscalls)
  └─ Detach main thread last (enters event loop)
```

## Wire Protocol

### PS_IOV_CONVERGE_REGS (command 12)

```c
struct page_server_iov {
    u32 cmd;        // encode_ps_cmd(PS_IOV_CONVERGE_REGS, 0)
    u32 nr_pages;   // number of threads (reused field)
    u64 vaddr;      // 0
    u64 dst_id;     // process identifier
};

// Payload: nr_pages × sizeof(converge_thread_regs)
struct converge_thread_regs {
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
Source (converge capture):  readdir(/proc/pid/task/) sorted ascending → converge_regs[i]
Replica (restore):    pstree_item->threads[i] → same order

threads[0] = main thread (lowest TID)
threads[1..N] = worker threads
```

## Why Converge Regs Work

**Before converge re-capture:**
- Registers: T_dump (time 0)
- Memory: converge (60 seconds later)
- Result: 60s mismatch → deadlocks, data corruption

**After converge re-capture:**
- Registers: converge (re-captured from frozen source)
- Memory: converge (bulk + convergence + dirty)
- Arena: converge (libc rw- re-sent from frozen source)
- Result: everything at converge → consistent → deterministic 7/7

## Implementation Contracts

1. **COW session**: scoped to `g_cow_info`, per dump session.
2. **VMA eligibility**: same filters as lazy-pages. Non-eligible VMAs
   dumped via normal path.
3. **Stream termination**: `PS_IOV_CLOSE` with `nr_pages == 0`. No ACK.
   Receiver close after marker treated as clean (`EPIPE` ok).
4. **Teardown**: `wait_for_page_server_thread()` before `cow_dump_fini()`.
5. **converge regs required**: restore aborts if `converge_regs.dat` missing.

## Threading Model

**Source (dump process):**
- main thread: orchestrates dump + resumes process
- `criu-page-srv` thread: 8-stream bulk transfer + convergence
- `cow-wp` worker threads: parallel UFFDIO_WRITEPROTECT

**Replica:**
- `page-recv` process: 8 TCP streams, process_vm_writev
- `restore` process: applies converge registers, injects VMAs, detaches

## Kernel Requirements

- Linux 6.1+ (userfaultfd WP_ASYNC requires 6.7+)
- `sysctl vm.unprivileged_userfaultfd=1`
