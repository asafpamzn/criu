# Dumping File Descriptors

This document explains the internal mechanisms CRIU uses to capture the state of opened file descriptors (FDs).

## Linux File Objects: Inodes, Dentries, and Files

In the Linux kernel, an opened file is represented by a chain of three distinct objects:

1.  **Inode**: Contains metadata (owner, type, size) and pointers to the actual data on disk.
2.  **Dentry (Directory Entry)**: A helper object used to resolve file paths. An inode can have multiple dentries if hard links exist.
3.  **File (or "File Description")**: Represents an active handle to a dentry/inode pair. It maintains state such as the current file position (`pos`) and access flags.

Crucially, **file descriptors** are per-task integers that point to these shared "File" objects. When a task calls `fork()`, the child's FDs point to the same "File" objects as the parent's.

## How CRIU Collects FD Information

Dumping FDs requires CRIU to collect state from both the kernel's `/proc` filesystem and the file objects themselves.

### 1. Identifying Open FDs
CRIU reads `/proc/$pid/fd/` and `/proc/$pid/fdinfo/` to determine which FD numbers are currently open and to retrieve their basic properties (position and flags).

### 2. Retrieving File Objects (SCM_RIGHTS)
To perform deeper inspection (like `fstat` or `ioctl`), CRIU needs a local copy of the file descriptor. It achieves this by:
*   Injecting **parasite code** into the target task.
*   Commanding the parasite to send the FDs to the CRIU coordinator via a Unix domain socket using the `SCM_RIGHTS` mechanism.

### 3. Detecting Shared Files (gen_id and kcmp)
To minimize image size and avoid redundant dumps, CRIU must identify if FDs in different tasks (or even the same task) point to the same underlying "File" object. It uses a two-stage optimization:
1.  **gen_id**: CRIU calculates a "generation ID" based on the file's device ID, inode number, and current position. If two FDs have different `gen_id`s, they are guaranteed to be different.
2.  **kcmp**: If `gen_id`s match, CRIU uses the `kcmp()` system call (with the `KCMP_FILE` flag) to definitively determine if the two descriptors refer to the same kernel "File" object.

## Image Storage

CRIU stores FD information in a two-tier structure:

### The `fdinfo-$id.img` Image
This per-task image maps task-specific FD numbers to global **File IDs**. Each entry contains:
*   `fd`: The numeric descriptor in the task.
*   `id`: A unique identifier for the underlying file object.

### Specialized File Images
The actual state of the file objects is stored in specialized images based on their type:
*   `reg-files.img`: Regular files (includes the path).
*   `pipes.img`: Pipes and FIFOs.
*   `unixsk.img` / `inetsk.img`: Sockets.
*   `signalfd.img`, `eventfd.img`, `epoll.img`, etc.

This separation allows CRIU to efficiently handle shared files: multiple `fdinfo` entries can point to a single entry in a specialized file image.

## See also

* [Kcmp Trees](kcmp-trees.md)
* [Parasite Code](parasite-code.md)
* [Invisible Files](invisible-files.md)
