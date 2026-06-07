# Invisible and Nameless Files

In Linux, a file can remain accessible to a process even if it no longer has a visible path in the filesystem. This occurs when a file is unlinked (deleted) while still open or when its path becomes inaccessible due to mount shadowing. This document explains how CRIU detects and reconstructs these "invisible" files.

## How Files Lose Their Paths

### 1. Unlinked while Open
The most common case is when an application opens a file and then immediately deletes it:
```c
int fd = open("/tmp/secret", O_RDWR);
unlink("/tmp/secret");
```
The file data persists in the kernel as long as the file descriptor remains open, but it no longer exists in the filesystem directory structure.

### 2. Virtual Filesystem Deletion
On virtual filesystems like `/proc`, if a process dies, its entries (e.g., `/proc/$PID/cmdline`) disappear. However, if another process still has an open file descriptor to one of these entries, the file remains alive but "nameless."

### 3. Mount Shadowing (Overmounts)
If a process opens a file in `/mnt/data` and then a new filesystem is mounted over `/mnt`, the original file becomes inaccessible via its path.

## CRIU's Detection and Reconstruction Strategies

CRIU uses the `/proc/$pid/fd/` and `/proc/$pid/fdinfo/` interfaces to identify open files and their expected paths.

### Ghost Files (Link Count = 0)
If a file has a link count of zero (`st_nlink == 0`), it is truly deleted.
*   **Dumping**: CRIU reads the entire content of the file and stores it within the image directory as a "ghost file."
*   **Restoring**: During restoration, CRIU recreates the file in a temporary location, opens it, and then immediately unlinks it to restore the original unlinked state.
*   **Optimization**: For large sparse files, CRIU can use the `--ghost-fiemap` option to only capture the data blocks, significantly reducing image size.

### Link-Remap (Link Count > 0)
If a file has a positive link count but its expected path is missing or points to a different file, it means the specific name used to open the file was deleted, but other hard links still exist.
*   **Strategy**: CRIU uses `linkat()` with the `AT_EMPTY_PATH` flag to create a temporary name for the file on the same filesystem. This allows it to be re-opened via a path during restoration.
*   **Option**: This behavior is enabled via the `--link-remap` flag.

### Virtual File Remap (The PID Helper)
For deleted `/proc` entries, CRIU cannot use ghost files or `linkat()`. Instead:
1.  It records the PID of the original process that the `/proc` entry referred to.
2.  During restoration, it creates a temporary **TASK_HELPER** process with that specific PID.
3.  The restored application opens the `/proc/$PID/...` entry of this helper.
4.  The helper is terminated once all restoration tasks are complete.

### Filesystem-Specific Handling

*   **NFS**: CRIU detects "Silly Rename" files (`.nfsXXX`) and handles them via the link-remap mechanism.
*   **OverlayFS**: Since `linkat()` may fail on OverlayFS if the file resides on a read-only lower layer, CRIU automatically falls back to the ghost file strategy in these cases.
*   **Devpts**: Files on `devpts` (like PTYs) are managed by the kernel and are restored using specific PTY master/slave allocation logic rather than file-based reconstruction.

## Technical Details

*   **--ghost-limit**: By default, CRIU limits ghost files to **1 MB** to prevent excessive disk usage. This can be increased via the `--ghost-limit` option.
*   **--evasive-devices**: Allows CRIU to proceed even if a character or block device path has changed, provided the device numbers (`st_rdev`) match.

## See also
* [Dumping File Descriptors](dumping-files.md)
* [Filesystem Peculiarities](filesystems-pecularities.md)
* [Mount Points](mount-points.md)
