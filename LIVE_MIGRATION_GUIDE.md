# Valkey Live Migration — How It Works

A guide for the team explaining how we move a running Valkey instance
from one machine to another with near-zero downtime.

---

## The Problem

You have a Valkey server handling 200GB of data with active clients.
You need to move it to a different machine — maybe for hardware
maintenance, scaling, or rebalancing. The catch: clients should not
notice. No multi-second freezes, no data loss, no connection errors.

## The Solution in One Sentence

We use **CRIU** (Checkpoint/Restore In Userspace) with a **COW
(Copy-On-Write) dump** to snapshot the Valkey process memory while it
continues running, stream the snapshot to a replica machine, then do a
sub-millisecond cutover.

---

## Glossary

| Term | What it means |
|------|---------------|
| **CRIU** | A Linux tool that can freeze a running process, save its entire state (memory, file descriptors, sockets, etc.) to files, and later restore it on another machine as if nothing happened. Think of it as "save game" for Linux processes. |
| **Checkpoint / Dump** | The act of saving the process state. CRIU calls this a "dump". |
| **Restore** | The act of recreating the process from the saved state on another machine. |
| **COW dump** | Our custom mode where CRIU takes the snapshot while the process keeps running. "COW" means Copy-On-Write — the kernel tracks which memory pages the process modifies after the snapshot starts. |
| **Freeze / Frozen time** | The brief period (50-76ms in our case) when the Valkey process is completely paused. During this window, CRIU collects process metadata and sets up page tracking. Clients see a brief latency spike but no errors. |
| **Page** | A 4KB block of memory. The kernel manages memory in pages. A 200GB Valkey instance has ~50 million pages. |
| **Write-protect (WP)** | A kernel feature where we mark memory pages as read-only. When Valkey writes to a protected page, the kernel generates a fault notification, and CRIU captures the old content before the write goes through. This is how we track changes after the snapshot. |
| **Page server** | The CRIU component on the source machine that reads pages from Valkey's memory and streams them over TCP to the replica. |
| **page-recv** | Our custom tool on the replica that receives pages over TCP and writes them into the restored process's memory. |
| **Convergence** | After the bulk transfer, some pages may have been modified by Valkey. The convergence phase captures these final changes. We use a fork-snapshot to get a consistent view without freezing Valkey again. |
| **Cutover** | The moment we switch traffic from source to replica. The source is frozen (SIGSTOP), a TCP signal tells the replica to wake up (SIGCONT), and the source is released. Total: ~1ms. |
| **VMA** | Virtual Memory Area — a contiguous range of virtual addresses in a process. Valkey's 200GB heap is typically one large VMA. |
| **FSx** | A shared filesystem (AWS FSx for Lustre) mounted on both machines. Used for CRIU image files and coordination. Not on the critical path for page transfer. |

---

## The Migration Flow

Below is the end-to-end sequence. Each phase is labeled with its
typical duration at 200GB.

```
  SOURCE MACHINE                           REPLICA MACHINE
  ══════════════                           ═══════════════

  Valkey running, serving clients
        │
  ┌─────▼──────────────────────┐
  │  1. PREPARE (7s)           │    ┌──────────────────────────┐
  │  - SSH to replica          │───▶│  Start restore.sh        │
  │  - Clean old state         │    │  Wait for page-server    │
  │  - Pre-collect sockets     │    └──────────────────────────┘
  └─────┬──────────────────────┘
        │
  ┌─────▼──────────────────────┐
  │  2. FREEZE (76ms)          │
  │  - Pause Valkey (SIGSTOP)  │
  │  - Collect process state   │
  │    (registers, file        │
  │     descriptors, VMAs)     │
  │  - Write-protect all       │
  │    memory pages            │
  │  - Resume Valkey (SIGCONT) │
  │  ◄ Valkey running again ►  │
  └─────┬──────────────────────┘
        │
  ┌─────▼──────────────────────┐    ┌──────────────────────────┐
  │  3. BULK TRANSFER (114s)   │    │  3. RESTORE + RECEIVE    │
  │  - Page server reads       │───▶│  - CRIU forks process    │
  │    Valkey memory            │    │    tree (50ms)           │
  │  - 8 TCP streams           │    │  - page-recv connects    │
  │  - 1,707 MB/s              │    │    8 streams             │
  │  - 50 million pages        │    │  - Writes pages into     │
  │  - LZ4 compression when    │    │    restored process      │
  │    data is compressible    │    │    via process_vm_writev  │
  │                            │    │                          │
  │  Valkey still running ►    │    │  Restored Valkey is      │
  │  (writes tracked by WP)    │    │  stopped (SIGSTOP)       │
  └─────┬──────────────────────┘    └──────────┬───────────────┘
        │                                      │
  ┌─────▼──────────────────────┐               │
  │  4. CONVERGENCE (<1s)      │               │
  │  - Fork snapshot of        │───────────────┘
  │    source (catches final   │    (sends last dirty pages)
  │    dirty pages)            │
  │  - No freeze needed        │
  └─────┬──────────────────────┘
        │
  ┌─────▼──────────────────────┐    ┌──────────────────────────┐
  │  5. CUTOVER (1ms)          │    │  5. CUTOVER              │
  │  - SIGSTOP source          │    │  - Receive TCP "GO"      │
  │  - Send TCP "GO" ──────────│───▶│  - SIGCONT replica       │
  │  - SIGCONT source          │    │  - Valkey is live!       │
  └────────────────────────────┘    └──────────────────────────┘
```

---

## Phase Details

### Phase 1: Prepare (scripting overhead, not part of migration)

The `migrate.sh` script SSHs to the replica, kills any leftover
processes from previous runs, cleans the shared filesystem, and starts
`restore.sh` on the replica. The replica creates a "ready" signal file
on the shared filesystem and waits.

This takes ~7 seconds and happens **before** migration timing starts.

### Phase 2: Freeze (50-76ms)

This is the only moment Valkey is unresponsive. CRIU does three things:

1. **Collects metadata** — registers, file descriptors, open sockets,
   signal handlers, namespaces. This goes into image files on the
   shared filesystem.

2. **Write-protects memory** — uses the kernel's userfaultfd mechanism
   to mark all of Valkey's memory pages as write-protected. After this,
   any write by Valkey triggers a notification that CRIU uses to track
   changes.

3. **Resumes Valkey** — Valkey continues serving clients. From this
   point, CRIU works entirely in the background.

The freeze time scales linearly with memory:
- 95GB: ~50ms
- 200GB: ~76ms
- Projected 400GB: ~150ms

### Phase 3: Bulk Transfer (95% of migration time)

The source's page server reads pages from Valkey's memory using
`process_vm_readv` (a Linux system call that reads another process's
memory) and streams them over 8 parallel TCP connections to the
replica's `page-recv` tool.

```
Source                                    Replica
┌──────────┐    8 TCP streams    ┌──────────┐
│ page     │ ═══════════════════▶│ page     │
│ server   │ ═══════════════════▶│ recv     │
│          │ ═══════════════════▶│          │
│ reads    │ ═══════════════════▶│ writes   │
│ process  │ ═══════════════════▶│ process  │
│ memory   │ ═══════════════════▶│ memory   │
│          │ ═══════════════════▶│          │
│          │ ═══════════════════▶│          │
└──────────┘  @ 1,707 MB/s      └──────────┘
```

Key details:
- **Throughput**: 1,707 MB/s (13.7 Gbps) on a 25 Gbps network
- **Bottleneck**: `process_vm_readv` in the kernel, not network or CPU
- **Compression**: LZ4 is used when it helps (structured data compresses
  59:1). For random data it's skipped automatically.
- **Batch size**: 512 pages (2MB) per read system call

During this phase, Valkey is running normally. If it writes to a page
that hasn't been transferred yet, the write-protect mechanism captures
the pre-write content so the replica gets the correct snapshot.

### Phase 4: Convergence (<1 second)

After the bulk transfer, some pages may have been dirtied by Valkey.
Instead of freezing Valkey again, we use a **fork-snapshot**:

1. Fork the source Valkey (Linux COW fork — instant, no memory copy)
2. Scan for dirty pages since the bulk transfer
3. Read dirty pages from the forked copy (guaranteed consistent)
4. Send them to the replica
5. Kill the fork

In a quiesced migration (no active writes), this finds ~20-80 dirty
pages (mostly allocator metadata). With active traffic, more pages are
dirty and multiple convergence rounds may be needed.

### Phase 5: Cutover (1 millisecond)

The atomic switchover:

1. Source sends `SIGSTOP` to Valkey (instant kernel signal)
2. Source sends TCP "GO" to replica port 9003 (bash `/dev/tcp` builtin,
   no fork)
3. Replica receives "GO", sends `SIGCONT` to the restored Valkey
4. Source sends `SIGCONT` to its own Valkey (so it can serve as
   fallback)

The source is frozen for ~1ms. The replica Valkey resumes and starts
serving immediately.

---

## What Gets Transferred

CRIU transfers the **complete process state**:

| Resource | How it's handled |
|----------|-----------------|
| Memory (heap, stack, mmap regions) | Bulk page transfer via page-recv |
| CPU registers | Saved in CRIU image files, restored via sigreturn |
| File descriptors | Recorded in image files, recreated on restore |
| TCP sockets | Closed on source (`--tcp-close`), clients reconnect |
| Signal handlers | Saved and restored |
| Thread state | All threads checkpointed and restored |
| mmap regions | VMA layout recorded, pages transferred |
| Process tree | Fork relationships preserved |

The restored Valkey on the replica is **byte-for-byte identical** to
the source at the moment of the snapshot. It continues executing from
the exact instruction where it was frozen.

---

## Performance Numbers

Tested on AWS Graviton3 (aarch64), 32 cores, 247GB RAM, 25 Gbps
network.

### At 95GB (1.6 million keys, 64KB values)

```
Migration time:    57.7 seconds
  Transfer:        57.2s  @ 1,708 MB/s
  Overhead:        0.4s
Freeze time:       50ms
Cutover:           1ms
Memory match:      0.0% divergence
Verification:      ALL 7 TESTS PASS
```

### At 200GB (3.2 million keys, 64KB values)

```
Migration time:    114.9 seconds
  Transfer:        114.4s @ 1,707 MB/s
  Overhead:        0.5s
Freeze time:       76ms
Cutover:           0ms
Memory match:      0.0% divergence
Verification:      ALL 7 TESTS PASS
```

### Scaling characteristics

| Metric | Per GB |
|--------|--------|
| Transfer time | 0.60 s/GB |
| Freeze time | 0.38 ms/GB |
| Overhead | ~0.5s (constant) |

---

## Verification

Every migration is verified with 7 tests:

1. **Migration completed** — the full flow ran without errors
2. **Replica PONG** — replica Valkey responds to PING
3. **Key count match** — exact same number of keys on source and replica
4. **Memory within 10%** — RSS memory matches (0.0% divergence in practice)
5. **500-key spot check** — random sample of keys compared byte-by-byte
6. **BGSAVE success** — replica can do a full RDB save (proves heap is consistent and not corrupted)
7. **RANDOMKEY type check** — can read and identify key types

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     SOURCE MACHINE                          │
│                                                             │
│  ┌─────────────┐       ┌──────────────────────────────┐    │
│  │   Valkey     │       │         CRIU                 │    │
│  │   Server     │◀─────▶│                              │    │
│  │             │  ptrace│  ┌────────────┐              │    │
│  │  200GB heap  │       │  │ COW dump   │              │    │
│  │  3.2M keys   │       │  │ (parasite) │              │    │
│  │             │  WP    │  └────────────┘              │    │
│  │             │◀──────▶│  ┌────────────┐  8 TCP       │    │
│  │             │  fault │  │ Page       │──streams──┐  │    │
│  └─────────────┘  notify│  │ server     │           │  │    │
│                         │  └────────────┘           │  │    │
│                         └──────────────────────────────┘    │
│                                                        │    │
└────────────────────────────────────────────────────────│────┘
                                                         │
                          25 Gbps network                │
                          1,707 MB/s actual              │
                                                         │
┌────────────────────────────────────────────────────────│────┐
│                     REPLICA MACHINE                    │    │
│                                                        │    │
│  ┌─────────────┐       ┌──────────────────────────────┐    │
│  │   Valkey     │       │         CRIU                 │    │
│  │   (restored) │◀──────│                              │    │
│  │             │ process│  ┌────────────┐              │    │
│  │  200GB heap  │ _vm_  │  │ page-recv  │◀─────────┘  │    │
│  │  3.2M keys   │ writev│  │ (8 threads)│              │    │
│  │             │       │  └────────────┘              │    │
│  │  SIGSTOP    │       │                              │    │
│  │  until all   │       │  Restore forks process,     │    │
│  │  pages       │       │  sets up VMAs, then          │    │
│  │  installed   │       │  page-recv fills memory      │    │
│  └─────────────┘       └──────────────────────────────┘    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Files

| File | What it does |
|------|-------------|
| `scripts/migrate.sh` | Main orchestration script, runs on the source |
| `scripts/restore.sh` | Replica-side script, starts CRIU restore + page-recv |
| `criu/cr-dump.c` | CRIU dump entry point, initiates COW tracking |
| `criu/cow-dump.c` | COW page tracking: write-protect, fault handling |
| `criu/page-xfer.c` | Page server: reads memory, 8-stream TCP transfer |
| `tools/page-recv.c` | Replica-side page receiver, multi-threaded |
| `criu/cr-restore.c` | CRIU restore: forks process, sets up VMAs, runs page-recv |
| `scripts/verify-migration.sh` | Automated 7-test verification suite |

---

## Limitations and Known Issues

1. **Live traffic (writes during migration)**: Quiesced migration is
   production-ready. Live traffic migration has a known issue where
   jemalloc allocations during the transfer can create VMA layout
   mismatches on the replica. See `FUTEX_DEADLOCK_RESEARCH.md` for
   details.

2. **Transfer speed**: Bottlenecked at 1.7 GB/s by `process_vm_readv`
   kernel overhead (page table walks). Not network or CPU limited.
   Transparent Huge Pages could help but aren't supported by the current
   allocator.

3. **Memory requirement**: The replica must have enough RAM to hold the
   full dataset. Both machines need sufficient memory for the process +
   OS overhead.

4. **TCP connections**: Existing client TCP connections are closed
   (`--tcp-close`). Clients must reconnect to the replica. This is
   standard for Valkey failover.
