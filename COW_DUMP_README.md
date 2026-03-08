# CRIU COW Dump (Copy-on-Write live migration)

## Summary

`--cow-dump` is an experimental CRIU mode that keeps the source process running
while memory is transferred, by tracking writes with `userfaultfd` write-protect
(WP) and shipping the **pre-write** contents of dirtied pages.

This fork is tested with **Valkey**: the restored instance is configured as a
Valkey replica of the source, so it catches up after the point-in-time
snapshot.

## Quick start (Valkey)

1. Follow `COW_DEVELOPER.md` to set up PRIMARY+REPLICA, shared `IMAGES_DIR`
   (e.g. `/fsx/lazy`), and `scripts/.env`.
2. On PRIMARY:

```bash
# Basic migration (fills dataset, then migrates)
sudo ./scripts/migrate.sh 40

# Real scenario with traffic + integrity checks (recommended)
./scripts/run_migration_scenario.sh 40
```

Artifacts are written under `artifacts/<run_id>/` on PRIMARY.

## Architecture (human view)

### Actors

- **Valkey (PRIMARY)**: the live source process.
- **CRIU dump (PRIMARY)**: creates the base checkpoint and runs the page server.
- **COW monitor (PRIMARY)**: background thread that snapshots pages on first
  write fault.
- **page-recv (REPLICA)**: standalone receiver, 8 TCP streams, installs pages
  via `process_vm_writev`.
- **CRIU restore (REPLICA)**: restores the process tree, applies T3 registers.
- **Valkey replication**: `REPLICAOF` makes the replica catch up.

### Timeline (what happens)

1. **Stop-the-world (short):** CRIU seizes the process tree to build a consistent
   base snapshot.
2. **Arm COW tracking:**
   - parasite creates a `userfaultfd` inside the target process and sends it
     back to CRIU (or CRIU opens `/proc/<pid>/userfaultfd` on kernel 6.11+),
   - CRIU registers eligible VMAs with `UFFDIO_REGISTER_MODE_WP` and applies
     `UFFDIO_WRITEPROTECT` to those ranges (parallelized),
   - the COW monitor thread starts (or is kept running) to handle write faults.
3. **Base dump completes:** CRIU writes images and prints `PAGE SERVER READY TO SERVE`.
4. **Source resumes:** the source keeps running under WP tracking.
5. **Page transfer:** the page server streams lazy pages to the replica; if a
   page was modified after the dump, the streamed content is the pre-write
   snapshot captured by the monitor.
6. **Replica becomes usable:**
   - restore starts Valkey from images,
   - scripts configure it as a replica and verify it rejects writes (`READONLY`),
   - only then external clients are allowed in (iptables gate removed).

### Bulk stream termination (no hangs)

The page stream ends with an end marker: a `PS_IOV_CLOSE` header with
`nr_pages == 0`. The receiver does **not** send an ACK back on the same socket
(mixing control bytes with the bulk stream desynchronizes the protocol).

## Measuring downtime (what numbers mean)

There are two different measurements:

- **CRIU frozen time**: from `stats-dump` (`freezing_time` + `frozen_time`).
- **Client-observed latency/outage**: from ping/traffic monitors.

Useful commands:

```bash
# Per-phase latency using artifacts/<run_id>/source_markers.log + source-ping.log
python3 scripts/analyze_phase_latency.py artifacts/<run_id>

# CRIU internal timings (archived by migrate.sh)
cat artifacts/<run_id>/stats-dump.json
cat artifacts/<run_id>/stats-restore.json
```

For app-like KPIs (p99 read/write latency, max outage windows, data-integrity
checks), use:

```bash
./scripts/run_migration_scenario.sh 40
```

## Performance results (m7g.16xlarge, aarch64, VPC)

Tested on AWS EC2 m7g.16xlarge (494GB RAM, 64 CPUs), two instances in the
same VPC.  Source runs Valkey filled with 64KB random values.  All tests
pass 7/7 verification (PONG, key count, memory, spot-check, BGSAVE,
RANDOMKEY).

### Source unavailability (the number that matters)

| Dataset | Benchmark traffic | Freeze + cutover |
|---------|-------------------|------------------|
| 100 GB  | heavy (90K ops/s) | **24 ms**        |
| 200 GB  | no                | **24 ms**        |
| 200 GB  | yes (80K ops/s)   | **24 ms**        |

Source unavailability = dump freeze (23ms) + cutover (1ms).  Does not
scale with dataset size because the bulk transfer runs while the
source is live.

### Stage-by-stage timing (200 GB + live traffic)

| Stage | Duration | Notes |
|-------|----------|-------|
| Parasite infect + dump | 23 ms | Seize, snapshot metadata |
| WP setup (userfaultfd) | 56 ms | 3120 ranges, post-resume |
| Source resumed | immediate | `--leave-running` |
| Bulk transfer (8 streams) | ~62 s | 51M pages, 3279 MB/s |
| Convergence (fork + dirty) | ~2 s | Fork snapshot + T3 dirty |
| T3 register capture | ~5 ms | 21 threads, 5880 bytes |
| Cutover | **1 ms** | SIGSTOP source → GO signal |

### Transfer configuration

- **Streams**: 8 parallel TCP connections (`COW_TRANSFER_STREAMS`)
- **Batch send**: 512 pages compressed (LZ4) into one `send()` call
- **Page delivery**: `process_vm_writev` (not UFFDIO_COPY)
- **Cutover**: TCP listener (sub-ms latency)
- **T3 register re-capture**: all thread registers re-sent at T3

### Known limitations

- **Disk space on replica**: REPLICAOF triggers full sync which writes
  a temp RDB.  For 200GB datasets, replica needs >250GB free disk or
  RDB saves must be disabled (`CONFIG SET save ""`).

## Requirements

- **Kernel**: Linux 5.7+ (for `UFFD_FEATURE_PAGEFAULT_FLAG_WP`)
- **Privileges**: root, or `vm.unprivileged_userfaultfd=1`

## Troubleshooting

### Permission denied for userfaultfd

```
userfaultfd requires CAP_SYS_PTRACE or sysctl vm.unprivileged_userfaultfd=1
```

Run as root, or:

```bash
sudo sysctl -w vm.unprivileged_userfaultfd=1
```

### Replica accepts writes

The replica must be configured via `REPLICAOF` before opening it to clients.
Check:

- `scripts/wait_and_replicate.sh`
- `/fsx/lazy/lazy-restore.log`
- `/fsx/lazy/lazy-server.log`

If Valkey can't persist replication state, ensure permissions:

```bash
sudo chown -R ubuntu:ubuntu /var/lib/valkey
sudo chmod 750 /var/lib/valkey
```
