# CRIU (Checkpoint/Restore In User-space)

CRIU is a tool for saving the state of a running application to a set of files
(checkpointing) and restoring it back to a live state. It is primarily used for
live migration of containers, in-place updates, and fast application startup.

It is implemented as a command-line tool called `criu`. The two primary commands
are `dump` and `restore`.

- `dump`: Saves a process tree and all its related resources (file
  descriptors, IPC, sockets, namespaces, etc.) into a collection of image
  files.
- `restore`: Restores processes from image files to the same state they were
  in before the dump.

## Quick Start

To get a feel for `criu`, you can try checkpointing and restoring a simple
process.

1.  **Run a simple process:**
    Open a terminal and run a command that will run for a while. Find its PID.
    ```bash
    sleep 1000 &
    [1] 12345
    ```

2.  **Dump the process:**
    As root, use `criu dump` with the process ID (`-t`) and a directory for the
    image files (`-D`).
    ```bash
    sudo criu dump -t 12345 -D /tmp/sleep_images -v4 --shell-job
    ```
    The `sleep` process will no longer be running.

3.  **Restore the process:**
    Use `criu restore` to bring the process back to life from the images.
    ```bash
    sudo criu restore -D /tmp/sleep_images -v4 --shell-job
    ```
    The `sleep` process will be running again as if nothing happened.

## For Developers and Contributors

This section contains more technical details about CRIU's internals and
development process.

### Dump Process

On dump, CRIU uses available kernel interfaces to collect information about
processes. For properties that can only be retrieved from within the process
itself, CRIU injects a binary blob (called a "parasite") into the process's
address space and executes it in the context of one of the process's threads.
This injection is handled by a subproject called **Compel**.

### Restore Process

On restore, CRIU reads the image files to reconstruct the processes. The goal is
to restore them to the exact state they were in before the dump. The restore
process is divided into several stages (defined as `CR_STATE_*` in
`./criu/include/restorer.h`).

The main `criu` process acts as a coordinator. It first restores resources with
inter-process dependencies (file descriptors, sockets, shared memory,
namespaces, etc.). It then forks the process tree and sets up namespaces.
Finally, it restores process-specific resources like file descriptors and memory
mappings.

A key step involves a small, self-contained binary called the "restorer". All
restored processes switch to executing this code, which unmaps the CRIU-specific
memory and restores the application's original memory mappings. On the final
step, the restorer calls `sigreturn` on a prepared signal frame to resume the
process with the state it had at the moment of the dump.

### Compel

Compel is a subproject responsible for generating the binary blobs used for the
parasite code (for dumping) and the restorer code (for restoring). It provides a
library for injecting and executing this code within the target process's
address space. It is a separate project because the logic for generating and
injecting Position-Independent Executable (PIE) code is complex and
self-contained.

### Project Goal

**Near-zero-interruption live migration for Valkey.**

This is not a research project. The goal is a production-grade solution that
Valkey users can adopt. The constraints are non-negotiable:

- **Source unresponsive time: 2-digit milliseconds maximum.** Any approach that
  causes the source Valkey to be unresponsive for more than ~99ms is rejected.
  This includes dump freeze time, cutover freeze, and any other window where
  clients see errors or timeouts.
- **No special infrastructure requirements.** The solution must work with
  standard Linux (kernel 6.x+), standard networking (TCP), and standard
  hardware. Requiring shared filesystems (FSx, NFS), specific cloud providers,
  or exotic kernel modules disqualifies an approach. It must work for any
  Valkey deployment on any two Linux machines connected by a network.
- **Architecture: both aarch64 and x86_64.** The solution must work on both
  architectures. No architecture-specific hacks or workarounds that only
  apply to one.
- **Target scale: 100GB+ datasets with live traffic.** The solution is required to handle
  large Valkey instances under active read/write workloads during migration.
- **Source stays up, replica can be down.** The source Valkey is required to remain
  responsive to clients throughout the entire migration. The replica is not
  serving traffic — it can be down/stopped/rebuilding for as long as needed.
  Only the source's availability matters.

Any direction that violates these constraints is a dead end. Do not propose
solutions that require shared storage, that accept multi-second source freezes,
or that only work at small scale.

### COW Dump (this fork)

This repository contains an experimental COW (copy-on-write) dump implementation
aimed at minimizing source downtime during migration by tracking writes while
the process continues to run.

- User-facing docs: `COW_DUMP_README.md`
- Design doc + code pointers: `COW_DUMP_DESIGN.md`
- Entry points and key files:
  - `criu/config.c` parses `--cow-dump`
  - `criu/cr-dump.c` initializes COW tracking on dump and resumes the process
    early when combined with lazy pages
  - `criu/pie/parasite.c` registers VMAs for userfaultfd write-protect inside
    the target process via parasite RPC
  - `criu/cow-dump.c` monitors userfaultfd events and snapshots pages on first
    write (WP fault)
  - `criu/mem.c` changes lazy-capable VMA handling for COW mode
  - `criu/page-xfer.c` integrates COW pages into page-server transfer
  - `criu/uffd.c` has restore-side changes for bulk transfer in COW mode

### Current State (Feb 2026)

**200GB quiesced migration: SOLVED.** Production-ready. 76ms freeze, 1ms
cutover, 114.9s total migration at 1,707 MB/s. ALL 7 verification tests pass.

**100GB live traffic migration: SOLVED.** Production-ready. 51ms freeze, 1ms
cutover, 58.2s total migration at 1,704 MB/s. ALL 7 verification tests pass
(12/14 historical runs passed; 2 failures were test-harness fill issues, not
migration bugs).

The live traffic SIGSEGV (jemalloc creating new mmap extents during transfer)
was fixed with three defenses:
  1. **VMA mirroring**: source detects new VMAs during convergence, sends
     `PS_IOV_VMA_DIFF` to replica, CRIU injects `mmap(MAP_FIXED)` via ptrace
  2. **Fork-snapshot convergence**: reads dirty pages from a COW fork instead
     of the live process, guaranteeing temporal consistency
  3. **Arena reset**: zeros glibc fastbins and empties bins at restore time
See `FUTEX_DEADLOCK_RESEARCH.md` Acts XVII-XIX for the full analysis.

Architecture (proven at 200GB quiesced, 100GB live):
```
Source: COW dump (51ms freeze) → page-server (8 TCP streams) → bulk + converge
Replica: criu restore → ptrace-trap → page-recv (process_vm_writev) → SIGCONT
```

Key files:
  - `criu/cr-restore.c`: `run_page_recv()` forks page-recv in the ptrace-trap
    window; `inject_new_vmas()` handles VMA mirroring via ptrace
  - `tools/page-recv.c`: standalone page receiver, handles bulk + convergence
    + VMA diff protocol, multi-stream, LZ4 decompression
  - `criu/page-xfer.c`: source-side page server, 8-stream bulk transfer,
    fork-snapshot convergence, VMA diff detection and transmission
  - `FUTEX_DEADLOCK_RESEARCH.md`: full debugging journal
  - `LIVE_MIGRATION_GUIDE.md`: team-facing end-to-end guide

### Deployment

**CRITICAL**: The replica's `scripts/restore.sh` prefers `/usr/local/sbin/criu`
over the local build (line 23-24). When deploying a new CRIU binary, ALWAYS
copy it to BOTH paths on the replica:
```bash
scp criu/criu ubuntu@REPLICA:/tmp/criu-new
ssh ubuntu@REPLICA 'sudo cp /tmp/criu-new /usr/local/sbin/criu && chmod +x /usr/local/sbin/criu'
ssh ubuntu@REPLICA 'cp /tmp/criu-new ~/work/criu/criu/criu'
```
Similarly, deploy `tools/page-recv` to the replica's `~/work/criu/tools/page-recv`.
Always verify with `md5sum` on both sides after deployment.

### Coding Style

The C code in the CRIU project follows the
[Linux Kernel Coding Style](https://www.kernel.org/doc/html/latest/process/coding-style.html).
Here are some of the main points:

-   **Indentation**: Use tabs, which are set to 8 characters.
-   **Line Length**: The preferred line limit is 80 characters, but it can be
    extended to 120 if it improves code readability.
-   **Braces**:
    -   The opening brace for a function goes on a new line.
    -   The opening brace for a block (like `if`, `for`, `while`, `switch`) goes
        on the same line.
-   **Spaces**: Use spaces around operators (`+`, `-`, `*`, `/`, `%`, `<`, `>`,
    `=`, etc.).
-   **Naming**: Use descriptive names for functions and variables.
-   **Comments**: Use C-style comments (`/* ... */`). For multi-line comments,
    the preferred format is:
    ```c
    /*
     * This is a multi-line
     * comment.
     */
    ```

### Code Layout

The code is organized into the following directories:

-   `./compel`: The Compel sub-project.
-   `./criu`: The main `criu` tool source code.
-   `./images`: Protobuf descriptions for the image files.
-   `./test`: All tests.
-   `./test/zdtm`: The Zero-Downtime Migration (ZDTM) test suite.
-   `./test/zdtm.py`: The executor script for ZDTM tests.
-   `./scripts`: Helper scripts.
-   `./scripts/build`: Docker image files used for CI and cross-compilation
    checks.
-   `./crit`: A tool to inspect and manipulate CRIU image files.
-   `./soccr`: A library for TCP socket checkpoint/restore.

### Building

Build CRIU with:

```bash
make -j"$(nproc)"
```

### Tests

The main test suite is ZDTM. Here is an example of how to run a single test:

```bash
sudo ./test/zdtm.py run -t zdtm/static/env00
```

Each ZDTM test has three stages: preparation, C/R, and results checks. During
the test, a process calls `test_daemon()` to signal it is ready for C/R, then
calls `test_waitsig()` to wait for the C/R stage to complete. After being
restored, the test checks that all its resources are still in a valid state.

