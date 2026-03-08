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
| **Freeze / Frozen time** | The brief period (~42-76ms in our case) when the Valkey process is paused via cgroup freezer. During this window, CRIU collects process metadata and sets up page tracking. Clients see a brief latency spike but no errors. Measured as `dump_one_task` in our logs. |
| **Page** | A 4KB block of memory. The kernel manages memory in pages. A 200GB Valkey instance has ~50 million pages. |
| **Write-protect (WP)** | A kernel feature where we mark memory pages as read-only. We use WP_ASYNC mode: when Valkey writes to a protected page, the kernel allows the write immediately (no stall) but marks the page as dirty. CRIU later discovers which pages were dirtied by scanning the kernel's pagemap. This is how we track changes after the snapshot without slowing Valkey down. |
| **Page server** | The CRIU component on the source machine that reads pages from Valkey's memory and streams them over TCP to the replica. |
| **page-recv** | Our custom tool on the replica that receives pages over TCP and writes them into the restored process's memory. |
| **Convergence** | After the bulk transfer, some pages may have been modified by Valkey. The convergence phase captures these final changes. We use a fork-snapshot to get a consistent view without freezing Valkey again. |
| **Cutover** | The moment we switch traffic from source to replica. The source is frozen (SIGSTOP), a TCP signal tells the replica to wake up (SIGCONT), and the source is released. Total: ~1ms. |
| **VMA** | Virtual Memory Area — a contiguous range of virtual addresses in a process. Valkey's 200GB heap is typically one large VMA. |
| **Images** | CRIU checkpoint files (process state metadata). Transferred via TCP from source to replica at dump time. Stored in `/tmp/criu-images`. |

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
  │  2. FREEZE (42-76ms)       │
  │  - Freeze via cgroup       │
  │  - Collect process state   │
  │    (registers, file        │
  │     descriptors, VMAs)     │
  │  - Write-protect all       │
  │    memory pages            │
  │  - Unfreeze (--leave-      │
  │    running)                │
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

This takes 5-10 seconds (SSH latency, process cleanup) and happens
**before** migration timing starts.

### Phase 2: Freeze (42-76ms)

This is the only moment Valkey is unresponsive. CRIU freezes all
threads via the cgroup freezer (`--freeze-cgroup`), then does three
things:

1. **Collects metadata** — registers, file descriptors, open sockets,
   signal handlers, namespaces. This goes into protobuf image files on
   the shared filesystem.

2. **Write-protects memory** — uses the kernel's userfaultfd
   `UFFDIO_WRITEPROTECT` ioctl to mark all of Valkey's memory pages as
   write-protected. We use WP_ASYNC mode: after this, any write by
   Valkey goes through immediately (no stall) but the kernel marks the
   page as dirty. CRIU discovers dirty pages later via `PAGEMAP_SCAN`.

3. **Unfreezes Valkey** (`--leave-running`) — the cgroup freeze is
   released and Valkey continues serving clients. From this point,
   CRIU works entirely in the background.

The freeze time (`dump_one_task`) scales roughly linearly with memory:
- 95GB: ~42-51ms
- 200GB: ~76ms

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

During this phase, Valkey is running normally. If it writes to a
write-protected page, the kernel allows the write immediately but
marks the page dirty. The convergence phase (next) discovers these
dirty pages and re-sends them from a consistent fork snapshot.

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
dirty but the fork-snapshot guarantees consistency.

**VMA mirroring** (for live traffic): during the transfer, Valkey's
allocator may `mmap` new memory regions that didn't exist at dump time.
The convergence phase detects these new VMAs by diffing the source's
current `/proc/<pid>/maps` against the dump-time layout. For each new
VMA, it sends a `VMA_DIFF` message to the replica. The replica's CRIU
uses ptrace to inject `mmap(MAP_FIXED)` into the restored process,
creating matching memory regions before the page data arrives. This
ensures the replica's memory layout matches the source at cutover.

**T3 register re-capture**: at the second freeze (T3), CRIU re-captures
all thread registers via `PTRACE_SEIZE/INTERRUPT/GETREGSET` and sends them
to the replica. The replica applies them via `PTRACE_SETREGSET` before
detach. This ensures registers match T3 memory — no allocator reset,
tcache null, or mutex unlock needed. T3 regs are required; restore aborts
if they are unavailable.

### Phase 5: Cutover (1 millisecond)

The atomic switchover:

1. Source sends `kill -STOP` to Valkey (instant kernel signal)
2. Source sends TCP "GO" to replica port 9003 (bash `/dev/tcp` builtin,
   no fork)
3. Replica receives "GO", sends `kill -CONT` to the restored Valkey
4. If `KEEP_SOURCE_RUNNING=1`: source also sends `kill -CONT` to its
   own Valkey (so it can serve as fallback)

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

### At 95GB with live traffic (1.6M keys, 64KB SET workload during migration)

```
Migration time:    58.6 seconds
  Transfer:        58.2s  @ 1,680 MB/s
  Overhead:        0.4s
Freeze:            50ms (dump_one_task)
Cutover:           1ms
Memory match:      0.0% divergence
Spot-check:        500/500 keys match
Verification:      ALL 7 TESTS PASS
```

### At 95GB with live traffic + io-threads 16

```
Migration time:    57.6 seconds
  Transfer:        57.1s  @ 1,714 MB/s
  Overhead:        0.5s
Freeze:            ~50ms (dump_one_task)
Cutover:           1ms
Memory match:      0.0% divergence
Spot-check:        500/500 keys match
Verification:      ALL 7 TESTS PASS
```

### At 200GB with live traffic (3.2 million keys, 64KB values)

```
Migration time:    114.9 seconds
  Transfer:        114.4s @ 1,707 MB/s
  Overhead:        0.5s
Freeze:            76ms (dump_one_task)
Cutover:           0ms
Memory match:      0.0% divergence
Verification:      ALL 7 TESTS PASS
```

### Scaling

| Metric | Per GB | Notes |
|--------|--------|-------|
| Transfer time | ~0.60 s/GB | Limited by process_vm_readv kernel page-table walks |
| Freeze (dump_one_task) | ~0.38 ms/GB | Dominated by UFFDIO_WRITEPROTECT ioctl |
| Overhead | ~0.5s | Constant: restore fork + page-recv connect + convergence + cutover |

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

## Running a Migration

### Prerequisites

On both machines:
```bash
make -j$(nproc)                                  # build CRIU
cd tools && gcc -O2 -o page-recv page-recv.c -llz4 -lpthread  # build page-recv
sudo cp criu/criu /usr/local/sbin/criu           # install
echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd  # allow uffd
```

Configure `scripts/.env` with the IPs and SSH key of both machines.

### Run a migration with full verification

```bash
# Fill 100GB + migrate with live traffic + verify all 7 tests:
sudo ./scripts/verify-migration.sh 100

# Skip fill if data is already loaded:
sudo ./scripts/verify-migration.sh 100 --skip-fill

# Benchmark only (skip BGSAVE + spot-check, saves ~4 min):
sudo ./scripts/verify-migration.sh 100 --skip-fill --benchmark
```

### Run migrate.sh directly (no test harness)

```bash
# On source — fill data first, then:
sudo env SKIP_FILL=1 KEEP_SOURCE_RUNNING=1 \
  RUN_WORKLOAD_DURING_MIGRATION=1 \
  bash scripts/migrate.sh 100
```

### What the CRIU commands look like

**Source** (runs inside migrate.sh):
```bash
criu dump \
  --tree $PID                    # Valkey's process ID
  --images-dir /tmp/criu-images  # local temp dir (no shared filesystem)
  --cow-dump                     # COW mode (our fork's feature)
  --lazy-pages                   # stream pages, don't write to disk
  --address $SOURCE_IP           # page-server listens here
  --port 9002                    # page-server port (8 TCP streams)
  --serve-images 9005            # serve .img files over TCP
  --tcp-close                    # close client sockets on dump
  --skip-in-flight               # ignore half-open sockets
  --ext-unix-sk                  # handle external unix sockets
  --leave-running                # resume process after dump
  --freeze-cgroup $CGROUP        # freeze via cgroup (not SIGSTOP)
  --display-stats                # write timing to stats-dump
```

**Replica** (runs inside restore.sh):
```bash
criu restore \
  --images-dir /tmp/criu-images  # local dir (images downloaded here)
  --fetch-images $SOURCE_IP:9005 # download .img files from source
  --lazy-pages --tcp-close --cow-dump \
  --restore-detached --leave-stopped \
  --skip-file-rwx-check --file-validation filesize
```

### Live traffic workload

During migration, the test runs a write workload against the source:

```bash
valkey-benchmark \
  -t set                 # SET commands only
  -r 1000000             # random keys from 1M keyspace
  -c 13                  # 13 concurrent clients (~8-9K writes/s)
  -P 16                  # pipeline depth 16
  -d 64000               # 64KB values (matches dataset)
  -n 1000000000          # runs until migration completes
```

### Ports used

| Port | Direction | Purpose |
|------|-----------|---------|
| 9002 | source → replica | Page server (8-stream bulk page transfer) |
| 9003 | source → replica | Cutover "GO" signal |
| 9004 | replica → source | Staged "STAGED" signal (sent by page-recv) |
| 9005 | source → replica | CRIU image file transfer (~500KB) |

---

## Architecture

```
Source                                        Replica
┌─────────────┐                              ┌─────────────┐
│   Valkey     │                              │   Valkey     │
│   (running)  │                              │  (restored,  │
│              │                              │   stopped)   │
└──────┬───────┘                              └──────┬───────┘
       │ cgroup freeze                               │ process_vm_writev
       │ + uffd WP_ASYNC                             │
┌──────▼───────┐    8 TCP streams @ 1.7 GB/s  ┌──────▼───────┐
│  CRIU dump   │ ════════════════════════════▶ │  page-recv   │
│  + page      │ ════════════════════════════▶ │  (8 threads) │
│    server    │ ════════════════════════════▶ │              │
│              │ ════════════════════════════▶ │  CRIU restore│
│ process_vm_  │ ════════════════════════════▶ │  forks the   │
│ readv        │ ════════════════════════════▶ │  process and │
│ (512-page    │ ════════════════════════════▶ │  sets up VMAs│
│  batches)    │ ════════════════════════════▶ │              │
└──────────────┘                              └──────────────┘
       │                                             │
       │  Convergence: fork-snapshot                 │
       │  → scan dirty pages                         │
       │  → send final diffs ───────────────────────▶│
       │                                             │
       │  Cutover: kill -STOP ──── TCP "GO" ────────▶│ kill -CONT
       │          (1ms)                              │ Valkey live!
```

---

## Key Files

| File | Role |
|------|------|
| `scripts/migrate.sh` | Source-side orchestration (prepare, dump, cutover) |
| `scripts/restore.sh` | Replica-side orchestration (CRIU restore, page-recv, SIGCONT) |
| `scripts/verify-migration.sh` | End-to-end test: fill, migrate, verify 7 checks |
| `criu/cow-dump.c` | COW engine: write-protect setup, dirty page tracking |
| `criu/page-xfer.c` | Page server: 8-stream bulk transfer, fork-snapshot convergence, VMA diff |
| `tools/page-recv.c` | Replica page receiver: 8-thread TCP, LZ4, process_vm_writev |
| `criu/cr-restore.c` | CRIU restore: fork process tree, VMA injection, run page-recv |
| `criu/cr-dump.c` | CRIU dump: freeze, collect state, launch COW |

---

## Status

Production-ready at 200GB with live traffic, including multi-threaded
Valkey (io-threads 16). Tested on aarch64 (Graviton3).

## Limitations

1. **Transfer speed**: 1.7 GB/s, limited by kernel `process_vm_readv`
   page-table walks. Migration time is ~0.6s per GB of data.

2. **Memory**: replica needs enough RAM for the full dataset.

3. **Client connections**: TCP sockets are closed (`--tcp-close`).
   Clients reconnect to the replica after cutover.

4. **x86_64**: tested on aarch64 only. x86_64 is expected to work
   (CRIU supports it) but not yet validated.
