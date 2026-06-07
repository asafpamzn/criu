# Irmap (Inode Reverse Mapping)

Irmap is CRIU's engine for resolving an `(inode, device)` pair back into a filesystem path. This is primarily required for restoring **fsnotify** (inotify and fanotify) instances, which internally reference inodes but do not preserve the paths used to create them.

## The Problem

When an application creates an inotify watch, the kernel resolves the path to an inode and attaches the watch to it. The original path string is then discarded by the kernel. During a checkpoint, CRIU can see which inode is being watched but needs a valid path to recreate that watch during restoration.

## How Irmap Works

Irmap uses a combination of predefined hints and brute-force scanning to build a reverse mapping cache.

### 1. Heuristic Hints
CRIU starts by scanning "known" locations where applications typically place watches, such as:
- `/etc` (configuration files)
- `/var/log` (log monitoring)
- `/var/spool`
- D-Bus and Polkit service paths (`/usr/share/dbus-1/services`, etc.)
- `/lib/udev`
- The root directory (`/`)

### 2. User-Defined Scan Paths
Users can provide additional directories to scan via the command line to help CRIU find application-specific files more quickly:
```bash
criu dump --irmap-scan-path /path/to/my/app ...
```
These paths are prioritized and scanned before the default hints.

### 3. Caching and Pre-dump
Scanning large filesystems can be slow and resource-intensive. To mitigate this:
- **irmap-cache.img**: Scan results are stored in this image file within the images directory.
- **Pre-dump Optimization**: CRIU can perform the irmap scan during a `pre-dump` while the application is still running. This populates the cache early, significantly reducing the time the application must remain frozen during the final dump.
- **Validation**: On subsequent runs, CRIU loads the cache and re-validates entries individually (checking if the inode/device still matches the path) rather than performing a full re-scan.

## Support for Filesystems

*   **Standard Filesystems**: Works well on most local filesystems (ext4, xfs, etc.).
*   **Tmpfs**: Paths are generally available via `/proc` and don't strictly require Irmap, though it can still be used.
*   **OverlayFS**: Irmap has historically had difficulties with OverlayFS due to how inodes are reported across different layers. In modern kernels, **open_by_handle_at** (leveraging file handles exposed in `/proc/$pid/fdinfo`) is the preferred and more reliable alternative to Irmap.

## See also
* [FSNotify](fsnotify.md)
* [Re-opening nameless files](how-to-open-a-file-without-open-system-call.md)
