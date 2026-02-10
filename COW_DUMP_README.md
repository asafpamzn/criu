# COW Dump - Copy-on-Write Live Migration for CRIU

## Overview

COW (Copy-on-Write) dump enables live migration of processes with minimal
downtime by using userfaultfd write-protection to track memory modifications
while the process continues running.

**Current results (Valkey):**

| Metric | 40GB | 100GB |
|---|---|---|
| Dump freeze (process paused) | 218ms | 455ms |
| Cutover window (no server available) | 511ms | ~510ms |
| PING response on replica | 297ms | ~300ms |
| Cutover is data-size-independent | Yes | Yes |

## How It Works

Traditional CRIU lazy-pages mode keeps the source process halted during
the entire memory transfer. COW dump changes this:

1. **Dump freeze (~0.2s for 40GB, ~0.45s for 100GB)**
   - CRIU seizes the process, collects VMA metadata via `/proc/pid/maps`
     (fast path, avoids expensive `/proc/pid/smaps` page table walks).
   - Injects parasite, registers writable VMAs with userfaultfd write-protect.
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

1. **Fast VMA parsing (`parse_maps_cow`)**: In COW mode, reads
   `/proc/pid/maps` instead of `/proc/pid/smaps`. The kernel walks page
   tables to generate RSS/PSS stats in smaps that CRIU never uses. Maps
   provides identical VMA metadata in ~1.5ms vs ~468ms for smaps (40GB).
   VmFlags are defaulted (MAP_GROWSDOWN inferred for stack; see trade-offs
   section below).
2. **Lazy iovec memcpy fix**: The page pipe iovec copy uses `pp->free_iov`
   (populated count) instead of `pp->nr_iovs` (allocated capacity). In COW
   mode only ~19 iovecs are populated out of 27M allocated for 100GB. The
   old code copied 438MB of zeros during freeze (216ms -> 0.02ms).
3. **VMA priority sort**: Stack (prio 0) -> heap (prio 1) -> other by size
   ascending. Critical startup pages arrive before cutover.
4. **Page fault prefetch**: Each fault fetches 64 pages, not 1. Reduces
   network round-trips during cutover by up to 64x.
5. **Pagemap cache skip**: COW-lazy VMAs bypass the expensive PAGEMAP_SCAN
   ioctl during dump (187ms -> 0.15ms for `generate_vma_iovs`).
6. **Socket-ready poll**: Restore script polls for lazy-pages socket instead
   of sleeping 1 second.

### Dump Freeze Breakdown (100GB Valkey)

| Component | Time | % |
|---|---|---|
| `cow_dump_init` (UFFD WP registration) | 413ms | 91% |
| `compel_stop_daemon` (unfreeze) | 22ms | 5% |
| `dump_task_threads` | 8ms | 2% |
| `parasite_dump_pages_seized` | 5ms | 1% |
| `parse_maps_cow` | 1.5ms | <1% |
| Everything else | ~5ms | ~1% |
| **Total** | **455ms** | |

### Trade-offs: maps vs smaps

The `parse_maps_cow` fast path defaults VmFlags to zero for all VMAs
because `/proc/pid/maps` does not include VmFlags lines. This means:

- `MAP_LOCKED`, `MAP_DROPPABLE`, `MADV_*` hints, and shadow stack flags
  are not captured in the CRIU image for COW-mode dumps.
- `MAP_GROWSDOWN` is explicitly set for `[stack]` VMAs (detected from
  the file path, same in maps and smaps).
- Anonymous private VMAs cannot have `VM_IO`/`VM_PFNMAP`, so the
  `VMA_UNSUPP` safety check is not a risk for COW candidates.
- The UFFD registration fallback catches VMAs that can't be
  write-protected regardless of VmFlags.
- For live migration this is acceptable: the process continues running
  on the replica, and lost madvise hints affect only future kernel
  behavior (e.g., THP), not correctness.

To recover full VmFlags without smaps cost, a future option is the
`PROCMAP_QUERY` ioctl (Linux 6.7+) which returns per-VMA `vm_flags`
without page table walks. See Future Work below.

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
