# COW Dump - Copy-on-Write Live Migration for CRIU

## Overview

COW (Copy-on-Write) dump enables live migration of processes with minimal
downtime by using userfaultfd write-protection to track memory modifications
while the process continues running.

**Current results (40GB Valkey):**

| Metric | Value |
|---|---|
| Dump freeze (process paused) | 771ms |
| Cutover window (no server available) | 511ms |
| PING response on replica | 297ms |
| Cutover is data-size-independent | 1GB and 40GB show same ~510ms |

## How It Works

Traditional CRIU lazy-pages mode keeps the source process halted during
the entire memory transfer. COW dump changes this:

1. **Dump freeze (~0.8s for 40GB)**
   - CRIU seizes the process, collects VMA metadata, injects parasite.
   - Parasite registers writable VMAs with userfaultfd write-protect (UFFD WP).
   - VMAs that fail registration are dumped through the normal (non-lazy) path.
   - Process resumes with write-protection active (`--leave-running`).

2. **Background pre-transfer (source running, ~2s)**
   - A background page server thread sends pages to the replica over TCP.
   - **VMA priority sort**: stack and heap are sent first (~20ms for ~600 pages),
     followed by small control VMAs, thread stacks, then the bulk data store last.
   - COW monitor thread handles write faults: snapshots the page, queues it for
     transfer, unprotects, and wakes the source thread.
   - Source sees microsecond-level jitter on first writes to protected pages.

3. **Cutover (~0.5s)**
   - `CLIENT PAUSE WRITE` on source (50ms), then `SIGSTOP` source process.
   - `SIGCONT` replica process (restored in stopped state via `--leave-stopped`).
   - Replica demand-faults any pages not yet transferred. Each fault prefetches
     64 surrounding pages to reduce network round-trips.
   - Replica responds to PING in ~297ms.

4. **Post-cutover**
   - Replica configured as `REPLICAOF` source.
   - Write protection verified (READONLY response).
   - Network gate removed — external clients can connect.

## Architecture

### Process Tree Notes
- COW tracking state is session-level, but VMA registration is per task.
- Lazy VMA lookup is keyed by `vpid` (virtual PID) and address.
- Page counts and transfers are scoped per destination image.

### Bulk Stream Close Contract
- Sender ends stream with `nr_pages == 0` close marker.
- Receiver sends a 32-bit status ACK.
- Sender treats ACK as completion; for older peers that close without ACK,
  sender accepts clean EOF/close as a backward-compatible fallback.

### Page Server Lifecycle
- The dump process waits for the background page server thread to finish
  (`wait_for_page_server_thread`) before destroying the COW session.
- If the COW session is destroyed while the thread is still running, the
  thread falls back to `process_vm_readv` instead of crashing.

## Key Optimizations

1. **VMA priority sort**: Stack (prio 0) -> heap (prio 1) -> other by size
   ascending. Critical startup pages arrive before cutover.
2. **Page fault prefetch**: Each fault fetches 64 pages, not 1. Reduces
   network round-trips during cutover by up to 64x.
3. **Pagemap cache skip**: COW-lazy VMAs bypass the expensive PAGEMAP_SCAN
   ioctl during dump (187ms -> 0.15ms for `generate_vma_iovs`).
4. **Socket-ready poll**: Restore script polls for lazy-pages socket instead
   of sleeping 1 second.

## Requirements

- **Kernel**: Linux 5.7+ (for `UFFD_FEATURE_PAGEFAULT_FLAG_WP`)
- **Privileges**: `sudo` or `CAP_SYS_PTRACE`
- **Shared storage**: Between source and replica (FSx, NFS)
- **Two machines**: Source (primary) and replica (destination)

## Usage

### Quick Test (1GB)
```bash
# On PRIMARY (source must have valkey running via systemd)
sudo FAST_CUTOVER=1 ./scripts/migrate.sh 1
```

### Production Test (40GB with traffic)
```bash
./scripts/run_migration_scenario.sh 40
```

### Scripts

| Script | Role |
|---|---|
| `scripts/migrate.sh` | Source-side orchestrator (run on PRIMARY) |
| `scripts/restore.sh` | Replica-side orchestrator (invoked by migrate.sh via SSH) |
| `scripts/wait_and_replicate.sh` | Configures REPLICAOF on restored replica |
| `scripts/run_migration_scenario.sh` | End-to-end scenario with traffic harness |
| `scripts/valkey_traffic_harness.py` | Traffic generator with integrity checks |

### Key CRIU Flags

| Flag | Purpose |
|---|---|
| `--cow-dump` | Enable COW write-protect tracking |
| `--lazy-pages` | Enable lazy page transfer |
| `--leave-running` | Keep source process running after dump |
| `--leave-stopped` | Restore process in SIGSTOP state (fast cutover) |
| `--file-validation filesize` | Skip build-ID check (cross-host compat) |
| `--skip-file-rwx-check` | Skip file permission validation |
| `--tcp-close` | Handle TCP socket migration |

## Scenario Harness

The traffic harness simulates production workload during migration:
- Continuous writes/reads to source
- Continuous reads to replica
- Intentional write attempts to replica (must get READONLY)
- Client-side latency, error, and outage-window tracking
- Post-run replication catch-up and sampled value integrity checks

```bash
./scripts/run_migration_scenario.sh 40
```

Artifacts:
- `/tmp/valkey_traffic_harness_report.json`
- `/tmp/valkey_traffic_harness.log`

Pass conditions:
- `replica_write_accepted == 0`
- `replication_caught_up == true`
- `sample_value_mismatches == 0`

## Troubleshooting

### Kernel Doesn't Support Write-Protect
```
Error: userfaultfd write-protect not supported
```
**Solution**: Upgrade to Linux 5.7+

### Build-ID Mismatch on Restore
```
Error: File libsystemd.so has bad build-ID
```
**Solution**: Add `--file-validation filesize` to restore args (already in
`scripts/restore.sh`). This happens when source and replica have different
library versions.

### Restore Hangs — "Remote side closed connection"
The page server exited before the replica could request pages. Check
`/fsx/lazy/lazy-primary.log` for "has no lazy VMA pages" — this indicates
a `dst_id` mismatch (fixed in the dst_id lifecycle PR).

### Replica Valkey Not Responding After Cutover
Check `/fsx/lazy/lazy-restore.log` and `/fsx/lazy/lazy-server.log`.
Common causes:
- Lazy-pages daemon disconnected (page server lifecycle issue)
- Empty-image race (retry logic in `restore.sh` handles this)
- Network gate not removed (check iptables)
