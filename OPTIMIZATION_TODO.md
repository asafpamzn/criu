# Optimization Backlog

Current state: **74ms total unresponsive** (66ms frozen + 8ms cutover) at 117GB.

## Remaining frozen window breakdown (66ms)

```
frozen_time: 66ms
├── pre-dump overhead:       14ms
│   ├── collect_namespaces:  ~8ms  (mount parsing + image opens)
│   ├── collect_lsm:         ~4ms  (AppArmor profiles)
│   └── other:               ~2ms
├── dump_one_task:           52ms
│   ├── WP (28 threads):    51ms  (196 ioctls, SCHED_BATCH)
│   │   overlaps with:
│   │   ├── dump_pages:      ~5ms real (48ms wall from WP contention)
│   │   ├── compel_stop:    16ms
│   │   └── dump_threads:    9ms
│   └── other:               1ms
└── post-dump (early resume): ~0ms (moved after resume)
```

## Priority 1: Reduce frozen_time (target: <50ms)

### P1-A: CPU affinity for WP threads (~10-15ms)
Pin WP threads to cores 2-31, reserve core 0-1 for the main dump
thread.  Currently SCHED_BATCH + nice(19) helps but kernel-mode page
table walks still compete.  `pthread_setaffinity_np` with a CPU mask
that excludes the main thread's core would eliminate contention.

**Effort**: Low.  Add `cpu_set_t` setup in `cow_dump_start_wp()`.
**Risk**: Low.  Worst case WP takes slightly longer on fewer cores.

### P1-B: Skip parasite fini in COW mode (~10ms)
`compel_stop_daemon` takes 16ms for parasite teardown (PTRACE_INTERRUPT
→ wait → PARASITE_CMD_FINI → single-step to sigreturn).  In COW mode
the process is about to be resumed with WP active — the parasite code
region will be overwritten by COW tracking anyway.  Could skip the fini
and just detach.

**Effort**: Medium.  Need `compel_cure_remote` without `compel_stop_daemon`.
**Risk**: Medium.  Parasite code remains mapped.  Need to verify no
conflict with COW WP on the parasite pages.

### P1-C: Skip AppArmor/LSM collection (~4ms)
`collect_and_suspend_lsm` reads AppArmor profiles.  For Valkey in
unconfined mode, this is pure overhead.  Add `--skip-lsm` flag or
auto-detect unconfined and skip.

**Effort**: Low.  Gate on `opts.cow_dump` or check confined status.
**Risk**: Low.  Only affects COW mode.  Standard dump unaffected.

### P1-D: Faster mount collection (~5ms)
`collect_mnt_namespaces` parses `/proc/PID/mountinfo`.  36 mounts on
this system but the parsing + image creation takes ~8ms (tmpfs helped
but still significant).  Could pre-parse mountinfo before freeze and
just write the image during the frozen window.

**Effort**: Medium.  Split collect_mnt_namespaces into parse (pre-freeze)
and write (frozen).
**Risk**: Low.  Mount info doesn't change while process is frozen.

## Priority 2: Reduce cutover (target: <5ms)

### P2-A: Pre-connected cutover socket (~3ms)
Currently: bash `/dev/tcp` does TCP connect + send during cutover (8ms
total includes `sudo kill` + TCP handshake).  Pre-open the TCP connection
before SIGSTOP and just send "GO" — saves the 3-way handshake.

**Effort**: Low.  Open fd before the critical section, write to it after
SIGSTOP.
**Risk**: Low.  Fall back to current path if pre-connect fails.

### P2-B: Drop sudo for kill (~1ms)
`sudo kill -STOP $PID` forks sudo.  CRIU already runs as root.  If the
migration script runs as root, `kill` directly avoids the fork.

**Effort**: Trivial.  Check `$EUID` and skip sudo.
**Risk**: None.

## Priority 3: Reduce total migration time (currently ~120s for 117GB)

### P3-A: More TCP streams
Currently 4 streams at ~1 GB/s total.  The network is 25 Gbps capable.
8-16 streams could reach 2-3 GB/s, cutting transfer from 120s to 40-60s.

**Effort**: Low.  Change stream count constant.
**Risk**: Low.  More memory usage for buffers.

### P3-B: io_uring for page sends
Replace write()/send() with io_uring for zero-copy async I/O on the
page transfer path.  Could improve throughput 20-40%.

**Effort**: High.  New I/O path in page-xfer.c.
**Risk**: Medium.  io_uring API complexity.

### P3-C: Adaptive LZ4 compression
Skip compression for incompressible pages (random data).  Currently
every page goes through LZ4 even when it doesn't compress.  A quick
entropy check could skip ~50% of pages.

**Effort**: Medium.
**Risk**: Low.

## Priority 4: Eliminate cutover freeze entirely

### P4-A: Rolling cutover via Valkey replication
Instead of SIGSTOP source → signal replica → SIGCONT replica, use
Valkey's built-in replication:
1. After bulk transfer, configure replica as `REPLICAOF source`
2. Let replication catch up (seconds)
3. Promote replica with `REPLICAOF NO ONE`
4. Redirect clients (DNS/proxy)
5. No SIGSTOP needed

**Effort**: High.  Application-level coordination.
**Risk**: Medium.  Requires Valkey replication to work post-restore.

## Quick wins summary

| ID | Change | Savings | Effort |
|----|--------|---------|--------|
| P1-A | CPU affinity for WP | ~10-15ms | Low |
| P1-C | Skip LSM | ~4ms | Low |
| P2-B | Drop sudo for kill | ~1ms | Trivial |
| P2-A | Pre-connected socket | ~3ms | Low |
| P1-B | Skip parasite fini | ~10ms | Medium |
| **Total quick wins** | | **~30-33ms** | |

With quick wins: 74ms - 30ms = **~44ms total unresponsive**.
