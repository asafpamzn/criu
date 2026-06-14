# CLONE Dump

CLONE dump is an experimental CRIU mode for live-migrating a process with
minimal downtime. Instead of freezing the source for the whole dump, CRIU
freezes briefly to set up userfaultfd write-protect, lets the process keep
running while pages stream to the target, then freezes once more for a
final dirty-page pass.

## Requirements

- Linux 6.7+ on both source and target (`UFFD_FEATURE_WP_ASYNC` and
  `PAGEMAP_SCAN`).
- Unprivileged userfaultfd enabled on both hosts:
  ```sh
  sudo sysctl -w vm.unprivileged_userfaultfd=1
  ```
- Network reachability from source to target.

## How to clone a process

Two hosts — start the receiver on the target, then run the dump on the
source.

### 1. On the target — receive pages

```sh
sudo criu clone-receive \
    --images-dir /path/to/images \
    --address <SOURCE_IP> --port 27 \
    -v4
```

### 2. On the source — dump

```sh
sudo criu dump \
    -t <PID> \
    --images-dir /path/to/images \
    --clone-dump \
    --page-server --address <TARGET_IP> --port 27 \
    -v4
```

`--clone-dump` enables the phased flow described above. `--page-server`
streams pages directly to the target — no intermediate disk image is
needed for memory.

### 3. On the target — restore

`clone-receive` triggers the restore itself once all pages have been
received; no separate `criu restore` step is required.

## Tunables

Runtime knobs (defaults are fine for most workloads):

- `--clone-p3-threads N` — parallel page-sender threads
- `--clone-scanners N` — dirty-page scanner threads
- `--clone-drain-threads N` — UFFDIO_COPY drain threads on the target
- `--clone-pre-scan` — run iterative dirty scans before the freeze

## Source layout

Implementation lives under `criu/clone/`:

- `clone-dump.c` — phase orchestration on the source
- `clone-bulk-send.c` — scanner and parallel sender threads
- `clone-uffd.c` — userfaultfd handling on the target
- `clone-phase2.c` — `clone-receive` entry point
- `clone-conf.h` — compile-time defaults and tunable maxima
