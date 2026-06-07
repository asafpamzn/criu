# Shared Object Detection (Kcmp Trees)

CRIU must frequently determine if system resources (such as file descriptions, memory mappings, or namespaces) are shared between different processes. While some objects have unique kernel-provided IDs (like inode numbers for files on disk), many do not. This document explains how CRIU uses the `kcmp()` system call and red-black trees to efficiently detect these shared objects.

## The Challenge

Comparing every resource in every process against every other process would result in $O(N^2)$ complexity, where $N$ is the total number of resources (e.g., 100 tasks with 100 files each = 10,000 files, or 50 million pairs). This is prohibitively slow.

## The Solution: `kcmp()` and Pointer Comparison

The `kcmp()` system call identifies whether two kernel objects are the same. Crucially, its return value is not a simple boolean; it returns the result of an internal kernel pointer comparison:
*   **0**: The objects are identical.
*   **1**: The first object's pointer is "less than" the second.
*   **2**: The first object's pointer is "greater than" the second.
*   **-1**: Error.

This ordering information allows CRIU to use **red-black trees** to sort and search for objects with $O(N \log N)$ complexity.

## Two-Level Red-Black Trees

To further optimize performance and minimize the number of expensive `kcmp()` system calls, CRIU uses a two-level tree structure:

### Level 1: Fast ID (genid)
CRIU first calculates a "generation ID" (`genid`) using cheap, locally available metadata. For regular files, this is derived from the device ID, inode number, and current file position.
*   Objects are inserted into a primary red-black tree ordered by `genid`.
*   If two objects have different `genid`s, they are guaranteed to be different, and no system call is needed.

### Level 2: Sub-tree (kcmp)
If two objects have identical `genid`s, they *might* be the same.
*   CRIU then descends into a sub-tree associated with that `genid`.
*   In this sub-tree, objects are ordered using the `kcmp()` system call.
*   If `kcmp()` returns 0, the objects are confirmed as shared.

## Supported Object Types

CRIU uses `kcmp()` for various object types, including:
*   **KCMP_FILE**: Individual file descriptions.
*   **KCMP_VM**: Virtual memory address spaces.
*   **KCMP_FILES**: The entire file descriptor table.
*   **KCMP_FS**: Filesystem information (umask, root, cwd).
*   **KCMP_SIGHAND**: Signal handler tables.
*   **KCMP_IO**: I/O context.
*   **KCMP_SYSV_SEM**: System V semaphore undo lists.
*   **KCMP_EPOLL_TFD**: Specific descriptors within an epoll interest list.

## See also
* [Dumping File Descriptors](dumping-files.md)
* [Copy-on-write memory](copy-on-write-memory.md)
