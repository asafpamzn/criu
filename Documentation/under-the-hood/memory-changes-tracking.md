# Memory Changes Tracking

Memory changes tracking (also known as "dirty memory tracking") is a critical feature in CRIU that enables efficient **live migration** with minimal downtime. By identifying and capturing only the memory pages that have been modified since a previous snapshot, CRIU can perform iterative dumps while the application continues to run.

## The Problem: Memory Dump Latency

During a standard checkpoint, CRIU freezes the process tree and dumps its entire memory state to disk. For memory-intensive applications (like large databases), this process can take several seconds, during which the application is completely unresponsive. This "freeze time" is directly proportional to the amount of memory used by the application.

## The Solution: Iterative Dumps

To minimize freeze time, CRIU supports an iterative migration scheme:
1.  **Initial Pre-dump**: Capture a full snapshot of the application's memory while it is still running.
2.  **Subsequent Pre-dumps**: Periodically capture only those pages that have been modified (made "dirty") since the last pre-dump.
3.  **Final Dump**: Freeze the processes and capture the final set of dirty pages. Since most memory was already transferred in previous steps, the final freeze time is significantly reduced.

## Kernel Mechanisms for Tracking

CRIU relies on two primary kernel mechanisms to track dirty pages:

### 1. The Soft-Dirty Bit
Linux maintains a "soft-dirty" bit for each Page Table Entry (PTE). 
*   **Resetting**: CRIU enables tracking by writing "4" to `/proc/$pid/clear_refs`, which clears the soft-dirty bit for all pages in the task's address space.
*   **Tracking**: Any subsequent write to a page causes the kernel to set its soft-dirty bit.
*   **Reading**: CRIU identifies dirty pages by reading the bit from the process's `/proc/$pid/pagemap` interface.

### 2. ioctl(PAGEMAP_SCAN)
Reading the entire `/proc/$pid/pagemap` file can be slow for very large address spaces. Modern kernels (v6.7+) support the `PAGEMAP_SCAN` ioctl, which allows CRIU to:
*   **Efficient Scanning**: Identify dirty pages across a large address space in a single kernel call.
*   **Filtering**: Directly filter for specific page categories (e.g., only dirty and present pages).
*   **Atomic Reset**: Optionally clear the soft-dirty bit while scanning, ensuring no writes are missed between scanning and resetting.

CRIU automatically detects and uses `PAGEMAP_SCAN` if available, falling back to manual `/proc` parsing on older kernels.

## Implementation in CRIU

Iterative migration is managed through the `pre-dump` command:

1.  **Chained Images**: Each pre-dump creates a set of image files in a new directory. These directories are linked together using the `--prev-images-dir` option.
2.  **Consolidated Restore**: During restoration, CRIU traverses the chain of images from newest to oldest. For any given memory address, it restores the most recent version of the page found in the image stack.
3.  **Page Server**: To avoid writing iterative dumps to disk, they can be sent over the network to a **page server** on the destination host.

## See also
* [Iterative Migration](iterative-migration.md)
* [Memory Images Deduplication](memory-images-deduplication.md)
* [Page Server](page-server.md)
