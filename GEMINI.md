# CRIU - COW Dump Development

## Hard Constraints (non-negotiable)

- **ALWAYS include tests** that cover new functionality and pass
  *WHY: Untested code causes regressions; CRIU bugs can corrupt process state.*

- **ALWAYS run multi-agent review** before creating PRs
  *WHY: Parallel specialist agents (code quality, security, performance, test coverage) catch issues humans miss. Spawn parallel review agents for each pass.*

- **ALWAYS address all review findings** before creating PRs
  *WHY: Multi-perspective review catches issues early; unresolved findings indicate incomplete work.*

- **ALWAYS address all PR comments** after PR creation
  *WHY: Reviewer feedback is critical; unaddressed comments block merge and erode trust.*

- **ALWAYS verify with tests and benchmarks** - never assume behavior
  *WHY: Assumptions about memory/kernel behavior cause subtle bugs.*

- **ALWAYS open issues for bugs** discovered, even if out of scope
  *WHY: Tracking prevents forgotten issues; helps prioritization.*

## Key Rules

- Direct and concise, no compliments or apologies
  *WHY: Saves tokens and keeps focus on technical content.*

- Ask if unsure, stop and reassess if looping
  *WHY: Prevents wasted effort on wrong approaches.*

- Commit frequently with meaningful messages
  *WHY: Git history is documentation; enables bisect debugging.*

- Keep PRs small and focused
  *WHY: Easier review, faster merge, cleaner history.*

## Priority Order

When rules conflict: Hard Constraints > Key Rules > Convenience.
When code style conflicts with functionality: Functionality wins.

---

# Current Focus: COW Dump Stabilization & Optimization

Our main task is stabilizing and optimizing the **COW (Copy-on-Write) dump**
feature for live migration of large-memory processes (e.g., Valkey/Redis).

## Idea

COW dump allows a process to **keep running** during migration by using
userfaultfd write-protection (Linux 5.7+):

1. Register writable pages with write-protection
2. Process continues running; writes trigger faults
3. On fault: copy original page, unprotect, queue for transfer
4. Page server sends original copies to replica on-demand
5. Replica restores lazily, becomes a replica of the still-running primary

## Key Files

| File | Purpose |
|------|---------|
| `criu/cow-dump.c` | Core COW logic: init, monitor thread, fault handling, hash table |
| `criu/include/cow-dump.h` | COW API and data structures |
| `criu/page-xfer.c` | Page transfer - integrates COW pages with page server |
| `criu/uffd.c` | Userfaultfd utilities |
| `criu/pie/parasite.c` | Parasite code for in-process operations (including COW uffd registration) |

## Migration Scripts

| Script | Purpose |
|--------|---------|
| `scripts/migrate.sh` | Master script (PRIMARY): starts valkey, fills data, runs dump |
| `scripts/restore.sh` | Replica script: waits for page server, runs lazy-pages + restore |
| `scripts/wait_and_replicate.sh` | Configures restored valkey as replica of primary |
| `scripts/.env` | Configuration: IPs, ports, paths |

## Migration Flow

```
PRIMARY                              REPLICA
  |                                    |
  | 1. valkey-server + fill data       |
  |                                    |
  | 2. criu dump --cow-dump            |
  |    --lazy-pages --leave-running    |
  |    [process keeps running]   -->   | 3. criu lazy-pages (connect to primary)
  |                                    | 4. criu restore --lazy-pages
  |                                    |    [pages fetched on-demand]
  | 5. COW monitor catches writes,     |
  |    sends original page copies      | 6. valkey starts, becomes replica
```

---

# Project Overview

CRIU (Checkpoint/Restore In Userspace) saves running application state to files
and restores it later. Primary commands:

- `criu dump` - Checkpoint process tree to image files
- `criu restore` - Restore process from images

## Quick Start

```bash
# 1. Start a process
sleep 1000 &
PID=$!

# 2. Dump it
sudo criu dump -t $PID -D /tmp/images -v4 --shell-job

# 3. Restore it
sudo criu restore -D /tmp/images -v4 --shell-job
```

---

# Developer Reference

## Dump Process

- Uses kernel interfaces to collect process info
- Injects "parasite" blob via Compel for in-process data
- Parasite runs in target's address space to access thread-local state

## Restore Process

- Reads image files to reconstruct processes
- Stages defined in `criu/include/restorer.h` (`CR_STATE_*`)
- Coordinator forks process tree, restores resources
- Restorer blob unmaps CRIU memory, calls `sigreturn` to resume

## Compel

- Subproject for generating parasite/restorer blobs
- Handles Position-Independent Executable (PIE) code injection
- Located in `./compel`

## Code Layout

| Directory | Purpose |
|-----------|---------|
| `./criu` | Main criu tool source |
| `./compel` | Compel sub-project |
| `./images` | Protobuf image definitions |
| `./test/zdtm` | ZDTM test suite |
| `./scripts` | Helper scripts |
| `./crit` | Image inspection tool |
| `./soccr` | TCP socket C/R library |

## Coding Style

Linux Kernel style:
- Tabs (8 chars), 80-120 char lines
- Function braces on new line, block braces on same line
- C-style comments (`/* ... */`)

## Tests

```bash
# Run single ZDTM test
sudo ./test/zdtm.py run -t zdtm/static/env00
```

Test stages: preparation → C/R → validation.
Process calls `test_daemon()` when ready, `test_waitsig()` to wait for restore.
