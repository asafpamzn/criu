# Optimization Backlog

Current state: **~80ms total unresponsive** (79ms frozen + 1ms cutover) at 100GB.

## Completed optimizations

| Optimization | Before | After | Savings |
|-------------|--------|-------|---------|
| WP_ASYNC VMA tracking fix | 3,010ms | 96ms | 2,914ms |
| 512MB WP chunks + CPU affinity | 96ms | 48ms | 48ms |
| Skip rt_sigreturn (compel_stop_daemon_fast) | 16ms | 0.06ms | 16ms |
| Defer thread core writes | 8.5ms | 0.4ms | 8ms |
| Pre-collect sockets before freeze | 13ms | 0.007ms | 13ms |
| Pre-create uffd before freeze | N/A | N/A | 0ms (needs kernel 6.11+) |
| Skip network_lock in COW mode | ~2ms | 0ms | 2ms |
| compel_cure_local | ~1ms | 0ms | 1ms |
| Cutover: bash builtins | 41ms | 1ms | 40ms |

## Remaining frozen window breakdown (79ms)

```
frozen_time: 79ms
├── pre-dump overhead:       7.6ms
│   ├── collect_lsm:        ~5.7ms  (AppArmor profiles)
│   ├── seccomp_filters:    ~0.9ms
│   ├── get_parent_inv:     ~0.5ms
│   ├── collect_namespaces: ~0.3ms  (was 20ms, pre-collected)
│   └── other:              ~0.2ms
├── dump_one_task:          67ms
│   ├── WP (31 threads):   51ms  (214 ioctls, SCHED_BATCH+affinity)
│   │   overlaps with:
│   │   ├── dump_pages:     ~5ms real (wall from WP contention)
│   │   ├── compel_stop:   0.06ms (fast path)
│   │   └── dump_threads:  0.4ms (deferred writes)
│   ├── cow_dump_init:      3.7ms  (VMA registration)
│   ├── collect_mappings:   7.4ms  (parse_maps)
│   └── other:              ~5ms
└── cutover:                 1ms
```

## Priority 1: Reduce frozen_time (target: <60ms)

### P1-A: Skip AppArmor/LSM collection (~5.7ms) — BLOCKED
`collect_and_suspend_lsm` reads AppArmor profiles.  Previously tried
skipping in COW mode — broke parasite communication ("Trimmed message
received").  LSM suspension is required for parasite RPC.

**Status**: Blocked.  Need to understand why parasite needs LSM.

### P1-B: Reduce WP contention (~10ms)
WP threads (51ms) dominate.  Already using SCHED_BATCH + nice(19) +
sched_setaffinity(cores 2+).  Further options:
- `SCHED_IDLE` instead of `SCHED_BATCH` (more aggressive)
- Yield main thread CPU during WP (risky)
- Reduce number of WP threads (fewer kernel locks)

**Effort**: Low.  Experiment with scheduling policies.
**Risk**: Low.  WP may take longer but main thread gets more CPU.

### P1-C: Skip parse_maps for known layout (~7ms)
`collect_mappings()` → `parse_maps()` reads `/proc/<pid>/maps`.  For
repeated migrations of the same Valkey instance, the VMA layout is
predictable.  Could cache and verify rather than re-parse.

**Effort**: High.  Need VMA change detection.
**Risk**: Medium.  Stale cache = wrong VMAs.

## Priority 2: Reduce total migration time (currently ~106s for 100GB)

### P2-A: More TCP streams
Currently 4 streams at ~1 GB/s total.  The network is 25 Gbps capable.
8-16 streams could reach 2-3 GB/s, cutting transfer from 106s to 40-60s.

**Effort**: Low.  Change stream count constant.
**Risk**: Low.  More memory usage for buffers.

### P2-B: Adaptive LZ4 compression
Skip compression for incompressible pages (random data).  Currently
every page goes through LZ4 even when it doesn't compress.  A quick
entropy check could skip ~50% of pages.

**Effort**: Medium.
**Risk**: Low.

## Priority 3: Eliminate cutover freeze entirely

### P3-A: Rolling cutover via Valkey replication
Instead of SIGSTOP source → signal replica → SIGCONT replica, use
Valkey's built-in replication after restore.

**Effort**: High.  Application-level coordination.
**Risk**: Medium.  Requires Valkey replication to work post-restore.
