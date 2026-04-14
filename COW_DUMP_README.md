# COW Dump - Minimized Downtime Live Migration

## What is COW Dump?

COW (Copy-on-Write) dump is an experimental CRIU feature that minimizes source process downtime during live migration. Traditional CRIU dump freezes the process for the entire duration while saving memory to disk. COW dump uses Linux's userfaultfd write-protect mechanism to track memory writes while the process continues running.

## How It Works

### Traditional CRIU Dump
```
Time: ─────────────────────────────────────────────────────────►

Process: [RUNNING] ──► [FROZEN ████████████████████████] ──► [KILLED/ALIVE]
                           │
                           └─ Dump all memory + state
                              (can take minutes for large processes)
```

### COW Dump
```
Time: ─────────────────────────────────────────────────────────►

Process: [RUNNING] ─► [FROZEN] ─► [RUNNING █████████████] ─► [FROZEN] ─► [KILLED]
                         │               │                      │
                         │               │                      └─ Final dirty pages
                         │               │                         + skeleton dump
                         │               │                         (~seconds)
                         │               │
                         │               └─ Bulk transfer + iterative dirty scan
                         │                  (process runs with write tracking)
                         │
                         └─ Init WP_ASYNC tracking
                            (~seconds)
```

## Requirements

- **Linux kernel 5.7+** with `UFFD_FEATURE_WP_ASYNC` support
- CRIU built with COW support (this fork)
- Network connectivity between primary and replica

## Quick Start

### 1. Start Page Server on Replica

```bash
# On REPLICA machine
sudo criu page-server \
    --images-dir /path/to/images \
    --port 27 \
    --lazy-pages
```

### 2. Run COW Dump on Primary

```bash
# On PRIMARY machine
sudo criu dump \
    -t <PID> \
    -D /path/to/images \
    --cow-dump \
    --lazy-pages \
    --page-server \
    --address <REPLICA_IP> \
    --port 27 \
    -v4
```

### 3. Restore on Replica

```bash
# On REPLICA machine (after page-server signals ready)
sudo criu restore \
    -D /path/to/images \
    --lazy-pages \
    -v4
```

## Command-Line Options

| Option | Description |
|--------|-------------|
| `--cow-dump` | Enable COW dump mode |
| `--lazy-pages` | Required for COW dump (pages transferred on-demand) |
| `--page-server` | Enable page server for remote transfer |
| `--address <IP>` | Replica IP address |
| `--port <PORT>` | Page server port (default: 27) |

## Architecture Overview

```
PRIMARY                                    REPLICA
┌─────────────────┐                       ┌─────────────────┐
│                 │                       │                 │
│   CRIU Dump     │   20 parallel         │   Page Server   │
│   + P3 Threads  │ ◄─────────────────────► + Receivers     │
│                 │   LZ4 compressed      │                 │
│   ┌───────────┐ │   page batches        │   ┌───────────┐ │
│   │ WP_ASYNC  │ │                       │   │   Page    │ │
│   │ Tracking  │ │                       │   │   Buffer  │ │
│   └───────────┘ │                       │   └───────────┘ │
│                 │                       │                 │
│   ┌───────────┐ │                       │   ┌───────────┐ │
│   │  Source   │ │                       │   │  Target   │ │
│   │  Process  │ │                       │   │  Process  │ │
│   └───────────┘ │                       │   └───────────┘ │
│                 │                       │                 │
└─────────────────┘                       └─────────────────┘
```

## Phases

### Phase 1: Initialize (~1-5 seconds freeze)
- Seize process and collect VMA information
- Create userfaultfd with WP_ASYNC
- Apply write-protect to all tracked VMAs
- **Unfreeze process** - it continues running

### Phase 2: Bulk Transfer (process running)
- 20 parallel sender threads transfer pages
- 4 scanner threads find dirty pages via PAGEMAP_SCAN
- Iterative dirty scanning until convergence threshold
- LZ4 compression reduces bandwidth by 60-70%

### Phase 3: Final Freeze (~1-10 seconds)
- Freeze process for final dirty page scan
- Dump process metadata ("skeleton dump")
- Send remaining dirty pages
- **Unfreeze process** (or kill, depending on options)

## Performance Characteristics

| Metric | Typical Value |
|--------|---------------|
| Phase 1 freeze | 1-5 seconds |
| Phase 2 duration | Depends on write rate |
| Phase 3 freeze | 1-10 seconds |
| Convergence threshold | ~1.2GB dirty pages |
| Parallel senders | 20 threads |
| Batch size | 256KB (64 pages) |
| Compression ratio | 40-50% |

## Monitoring Progress

COW dump outputs timing information to stderr:

```
=== PHASE 1: Seize + Pre-dump + WP_ASYNC ===
TIMING: Phase 1 freeze started
TIMING: cow_dump_init_async took 2.345678 seconds
TIMING: Phase 1 freeze ended - process frozen for 3.456789 seconds

=== PHASE 2: Bulk page transfer + dirty scan convergence ===
TIMING: P3 bulk transfer started
=== CONVERGENCE: All threads below threshold ===

=== PHASE 3: Freeze + skeleton dump ===
TIMING: Phase 3 freeze started
TIMING: skeleton dump loop took 0.234567 seconds
P3 threads completed: 1234567 total pages sent
TIMING: Phase 3 freeze ended - process frozen for 5.678901 seconds
```

## Troubleshooting

### Kernel Support Check

```bash
# Check if kernel supports WP_ASYNC
grep -i uffd /proc/kallsyms | grep -i async
```

### Common Issues

**"Kernel does not support COW dump (requires UFFD_FEATURE_WP_ASYNC)"**
- Upgrade to Linux 5.7+
- Ensure userfaultfd is enabled in kernel config

**Slow convergence**
- Process has high write rate
- Consider increasing `DIRTY_SCAN_FREEZE_THRESHOLD`

**Long UFFD cleanup**
- Normal for large memory systems (300GB+)
- Cleanup is chunked to avoid kernel lockups

## Limitations

1. **Single process tree**: Currently tracks one process tree
2. **Kernel version**: Requires Linux 5.7+ 
3. **Write-intensive workloads**: May not converge quickly
4. **Network dependency**: Requires stable network to replica

## Technical Details

For implementation details, see [COW_DUMP_DESIGN.md](COW_DUMP_DESIGN.md).

## Source Files

Core implementation in `criu/cow/`:
- `cow-dump.c` - Main COW dump logic
- `cow-bulk-send.c` - Parallel sender threads
- `cow-unified-thread.c` - Page server thread
- `cow-uffd.c` - Restore-side UFFD handling
