# Shared Memory

CRIU provides comprehensive support for capturing and reconstructing the various ways processes share memory in Linux. This includes anonymous shared regions, file-backed shared mappings, and System V IPC segments.

## Types of Shared Memory

CRIU categorizes shared memory into three primary types, each with a dedicated restoration strategy:

### 1. Shared Anonymous Mappings
Created via `mmap(..., MAP_SHARED | MAP_ANONYMOUS, ...)`, these regions have no persistent backing file on disk but are shared between a parent and its children after a `fork()`, or between unrelated processes that inherit the mapping.

*   **Identification**: CRIU identifies these regions by parsing `/proc/$pid/maps`. For shared anonymous regions, the kernel assigns a unique **internal inode number** (often appearing in the `inode` column of maps). CRIU uses this inode number as a `shmid` to group and identify identical mappings across the entire process tree.
*   **Restoration via memfd**: During restoration, CRIU uses the `memfd_create()` system call to create an anonymous, RAM-backed file.
    *   One process (the designated "master" for that specific `shmid`) creates the `memfd`, populates it with the memory contents captured in `pages-shmem.img`, and maps it.
    *   All other processes that shared the original region map the same `memfd` file descriptor, ensuring that any subsequent writes are visible to all participants, just as they were before the checkpoint.

### 2. Shared File Mappings
Created via `mmap(..., MAP_SHARED, fd, ...)`, these regions are backed by a regular file on the filesystem.

*   **Mechanism**: CRIU records the file's unique identity (device and inode), the offset within the file, and the mapping length.
*   **Restoration**: Each process re-opens the original file (or a restored version of it) and calls `mmap()` with the `MAP_SHARED` flag. The Linux kernel's standard page cache mechanism automatically handles the sharing and synchronization between processes.

### 3. System V IPC Shared Memory
Managed via the legacy `shmget()` and `shmat()` APIs, these segments are part of the kernel's IPC subsystem.

*   **Mechanism**: CRIU captures the segment's metadata (key, ID, permissions, size) and its full data contents during the dump.
*   **Restoration**: CRIU recreates the IPC segments using `shmget()` with the original parameters and repopulates the data. The restored processes then attach to these segments using `shmat()`, ensuring that IPC-based communication continues seamlessly.

## Advanced Coordination Features

CRIU leverages modern kernel features to handle complex sharing accurately:
*   **kcmp**: Used to definitively verify if two memory mappings in different processes refer to the same underlying kernel object (via `KCMP_VM`), ensuring that shared resources are only dumped once.
*   **Futex Synchronization**: During restoration, CRIU uses futexes to coordinate between the "master" process (which populates shared memory) and "slave" processes, ensuring that no process starts execution until the shared memory state is fully consistent.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
* [Kcmp Trees](kcmp-trees.md)
