# CRIU COW Dump — Live Migration for Valkey

Near-zero-downtime live migration using copy-on-write page tracking.
23ms freeze, 1ms cutover, 200GB at 3.3 GB/s.

## Quick Start

```bash
# Build on both machines
make -j$(nproc)
cd tools && gcc -O2 -o page-recv page-recv.c -llz4 -lpthread && cd ..
sudo cp criu/criu /usr/local/sbin/criu
echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd

# Configure scripts/.env with IPs and SSH key

# Fill 100GB + migrate with live traffic + verify 7 tests:
sudo ./scripts/verify-migration.sh 100

# Or run migrate.sh directly (data already loaded):
sudo env SKIP_FILL=1 KEEP_SOURCE_RUNNING=1 \
  RUN_WORKLOAD_DURING_MIGRATION=1 \
  bash scripts/migrate.sh 200
```

## How It Works

```
  SOURCE MACHINE                           REPLICA MACHINE

  Valkey running, serving clients
        │
  ┌─────▼──────────────────────┐
  │  1. FREEZE (23ms)          │    ┌──────────────────────────┐
  │  - Seize process (ptrace)  │───▶│  Start restore.sh        │
  │  - Capture: VMAs, pagemap  │    │  CRIU restore + page-recv│
  │  - Setup WP tracking       │    └──────────────────────────┘
  │  - Resume (--leave-running)│
  │  ◄ Valkey running again ►  │
  └─────┬──────────────────────┘
        │
  ┌─────▼──────────────────────┐    ┌──────────────────────────┐
  │  2. BULK TRANSFER (~60s)   │    │  RECEIVE                 │
  │  - Fork COW snapshot       │───▶│  - 8 TCP streams         │
  │  - 8 TCP streams, LZ4     │    │  - process_vm_writev     │
  │  - 3.3 GB/s throughput     │    │    into restored process │
  │  - Valkey still serving ►  │    │  - Restored Valkey is    │
  │  (writes tracked by WP)    │    │    stopped (ptrace-trap) │
  └─────┬──────────────────────┘    └──────────┬───────────────┘
        │                                      │
  ┌─────▼──────────────────────┐               │
  │  3. CONVERGENCE (<2s)      │───────────────┘
  │  - Fork snapshot (T1)      │    (sends dirty + T3 pages)
  │  - SIGSTOP (T3)            │
  │  - T3 register capture     │───▶  Saves t3_regs.dat
  │  - libc rw- re-send        │───▶  Installs arena pages
  │  - VMA diff (new mmaps)    │───▶  Injects mmap
  │  - SIGCONT source          │
  └─────┬──────────────────────┘
        │
  ┌─────▼──────────────────────┐    ┌──────────────────────────┐
  │  4. CUTOVER (1ms)          │    │  CUTOVER                 │
  │  - SIGSTOP source          │    │  - Apply T3 regs         │
  │  - Send TCP "GO" ──────────│───▶│  - PTRACE_DETACH all     │
  │  - SIGCONT source          │    │  - Valkey is live!       │
  └────────────────────────────┘    └──────────────────────────┘
```

### T3 Register Re-capture

The key innovation. Without it, registers are from dump time (T0) but
memory is from T3 (60 seconds later) — every thread resumes at the wrong
instruction. T3 re-capture fixes this:

At the second freeze, `capture_and_send_t3_regs()` does PTRACE_SEIZE +
PTRACE_GETREGSET on every thread, sends the registers to the replica.
The replica applies them via PTRACE_SETREGSET before detach. Registers
match T3 memory — deterministic 7/7.

**Critical detail**: libc's rw- data segment is file-backed (not
MAP_ANONYMOUS), so WP never tracks it. The explicit 2-page re-send from
the frozen source is the only mechanism that delivers arena state.

### What Gets Transferred

| Resource | How |
|----------|-----|
| Memory (heap, stack, mmap) | 8-stream bulk + convergence via page-recv |
| CPU registers | T3 re-capture via PTRACE_SETREGSET |
| File descriptors | CRIU image files |
| TCP sockets | Closed (`--tcp-close`), clients reconnect |
| Signal handlers, thread state | CRIU image files, restored via sigreturn |
| New VMAs (jemalloc extents) | VMA diff protocol + ptrace mmap injection |

## Performance

Tested on m7g.16xlarge (494GB RAM, 64 CPUs), same-AZ VPC.

| Test | Size | Transfer | Throughput | Freeze | Cutover | Result |
|------|------|----------|------------|--------|---------|--------|
| Quiesced | 200GB | 62s | 3159 MB/s | 23ms | 1ms | **7/7** |
| Live traffic | 200GB | 63s | 3279 MB/s | 23ms | 1ms | **7/7** |
| Live + heavy bench | 100GB | 32s | 3054 MB/s | 23ms | 1ms | **7/7** |

**Source unavailability: 23ms freeze + 1ms cutover = 24ms total.**

### Stage Timing (200GB + live traffic)

| Stage | Duration | Notes |
|-------|----------|-------|
| Freeze (dump_one_task) | 23 ms | Seize + pagemap + parasite |
| WP setup (post-resume) | 56 ms | 3120 ranges, WP_ASYNC |
| Bulk transfer (8 streams) | ~62 s | 51M pages, LZ4, process_vm_readv |
| Convergence + T3 | ~2 s | Fork snapshot + dirty scan + T3 regs |
| Cutover | 1 ms | SIGSTOP → TCP "GO" |

### vs REPLICAOF

| Metric | COW Migration | REPLICAOF |
|--------|--------------|-----------|
| 200GB transfer | **63s** | >20min (62GB incomplete) |
| Throughput | **3.3 GB/s** | ~500 MB/s |
| Source freeze | **23ms** | ~200ms (BGSAVE fork) |
| Replica downtime | **24ms** | Entire sync duration |
| Memory spike | None | 2× RSS (fork COW) |

## Verification Tests (7/7)

1. **Migration completed** — full flow without errors
2. **Replica PONG** — responds to commands
3. **Key count match** — same keys as source
4. **Memory within 10%** — 0.0% divergence in practice
5. **500-key spot check** — random keys compared byte-by-byte
6. **BGSAVE success** — heap consistent, no corruption
7. **RANDOMKEY type check** — can read and identify key types

## Running

### CRIU Commands (inside migrate.sh / restore.sh)

**Source:**
```bash
criu dump \
  --tree $PID --images-dir /tmp/criu-images \
  --cow-dump --lazy-pages \
  --address $SOURCE_IP --port 9002 \
  --serve-images 9005 \
  --tcp-close --skip-in-flight --ext-unix-sk \
  --leave-running --freeze-cgroup $CGROUP \
  --display-stats
```

**Replica:**
```bash
criu restore \
  --images-dir /tmp/criu-images \
  --fetch-images $SOURCE_IP:9005 \
  --lazy-pages --tcp-close --cow-dump \
  --restore-detached --leave-stopped \
  --skip-file-rwx-check --file-validation filesize
```

### Ports

| Port | Purpose |
|------|---------|
| 9002 | Page server (8-stream bulk transfer) |
| 9003 | Cutover "GO" signal |
| 9004 | Staged signal (page-recv → source) |
| 9005 | CRIU image file transfer |

## Key Files

| File | Role |
|------|------|
| `scripts/migrate.sh` | Source orchestration |
| `scripts/restore.sh` | Replica orchestration |
| `scripts/verify-migration.sh` | 7-test verification suite |
| `criu/page-xfer.c` | Page server, 8-stream bulk, convergence, T3 capture |
| `criu/cow-dump.c` | WP_ASYNC tracking, userfaultfd injection |
| `criu/cr-dump.c` | Dump orchestration, early resume |
| `criu/cr-restore.c` | Restore, T3 reg application, VMA injection |
| `tools/page-recv.c` | Standalone receiver, process_vm_writev |

## Requirements

- Linux kernel 6.1+ (userfaultfd WP_ASYNC requires 6.7+)
- `sysctl vm.unprivileged_userfaultfd=1`
- aarch64 or x86_64
- Both machines reachable over TCP

## Troubleshooting

### Permission denied for userfaultfd
```bash
echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd
```

### Disk full on replica during BGSAVE
The REPLICAOF command triggers a full sync RDB write. Disable saves:
```bash
valkey-cli config set save ""
```

### Replica accepts writes
The `wait_and_replicate.sh` script sets `REPLICAOF` which makes the
replica read-only. If this didn't run, manually:
```bash
valkey-cli replicaof $SOURCE_IP 6379
```

## Limitations

1. **Transfer speed**: ~3.3 GB/s, limited by `process_vm_readv` bandwidth.
2. **Replica RAM**: must fit the full dataset.
3. **Client connections**: closed on dump (`--tcp-close`), clients reconnect.
4. **x86_64**: tested on aarch64. x86_64 expected to work (T3 regs have
   x86 paths) but not yet validated at scale.
