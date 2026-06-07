# Re-opening Files without Paths (open_by_handle_at)

Occasionally, CRIU encounters an open file descriptor for which the kernel no longer maintains a path. This document explains how CRIU uses file handles and Inode Reverse Mapping (Irmap) to reconstruct these "nameless" files.

## When Paths Are Lost

The most common scenario for path loss occurs with **fsnotify** (inotify and fanotify) instances. 
When an application calls `inotify_add_watch(path)`, the kernel:
1.  Resolves the path to an **inode**.
2.  Attaches a watch generator to that inode.
3.  Immediately forgets the path used to create the watch.

The resulting file descriptor points to the fsnotify instance, which knows *which* inode it is watching but not *where* that inode lives in the filesystem hierarchy. Because the dentry (directory entry) cache can be shrunk by the kernel at any time, the path information is often permanently lost to userspace.

## Strategy 1: open_by_handle_at

Linux provides a specialized system call, `open_by_handle_at()`, designed for userspace NFS servers. It allows opening a file using a **File Handle**—a filesystem-specific blob of bytes that uniquely identifies an inode.

### The Handle mechanism
1.  **Dumping**: CRIU reads the file handle for a watch from `/proc/$pid/fdinfo/$fd`. (CRIU developers upstreamed patches to the Linux kernel to ensure this information is exposed).
2.  **Restoring**: During restoration, CRIU takes this handle and calls `open_by_handle_at()`. This returns an `O_PATH` file descriptor pointing to the original inode, even if its original path is unknown.
3.  **Re-attaching**: CRIU then uses this `O_PATH` descriptor to re-establish the inotify or fanotify watch, effectively "tricking" the kernel into watching the correct inode.

## Strategy 2: Irmap (Inode Reverse Mapping)

Not all filesystems support file handles (e.g., some older or specialized filesystems). In these cases, CRIU must resort to a brute-force approach called **Irmap**.

The Irmap engine maintains a cache that maps `(device, inode)` pairs back to their filesystem paths.
1.  **Scanning**: Irmap recursively scans "known" directories (like configuration paths or application homes) and records every name-to-inode mapping it finds.
2.  **Lookup**: When CRIU needs a path for a specific inode, it queries the Irmap cache.
3.  **Pre-dump Integration**: To minimize the performance impact of filesystem scanning, CRIU can perform this scan during a **pre-dump** while the application is still running. The results are saved to an `irmap-cache.img` file and reused during the final dump.

## Filesystem Specifics

*   **Tmpfs**: This filesystem pins its dentries in memory. For tmpfs, paths are almost always available via `/proc` and do not require handles or Irmap.
*   **OverlayFS**: Due to its layered nature, OverlayFS can have complex handle behaviors. CRIU includes specific logic to navigate these layers during handle resolution.

## See also
* [Dumping File Descriptors](dumping-files.md)
* [FSNotify (Inotify and Fanotify)](fsnotify.md)
* [Irmap](irmap.md)
