# Pagemap Cache

When dumping the memory of a process, CRIU must frequently query the kernel to determine which virtual memory pages are currently present in RAM, swapped out, or modified (dirty). This information is typically retrieved from the `/proc/$pid/pagemap` file. However, reading and parsing this file repeatedly for every Virtual Memory Area (VMA) is inefficient. To solve this, CRIU implements a high-performance **Pagemap Cache**.

## The Performance Problem

The `/proc/$pid/pagemap` file is a 64-bit-per-page binary stream. For a process with a large address space, this file can be several megabytes in size. While a single sequential read is fast, CRIU needs this data across multiple stages of the dump (e.g., initial size estimation, private memory dumping, shared memory dumping, and iterative pre-dumps). Performing multiple full reads and frequent `lseek()` calls into this file introduces significant overhead, especially for applications with thousands of small VMAs.

## Implementation Details

The pagemap cache (`struct pmc`) optimizes access through several advanced techniques:

### 1. Sliding Window Caching
Instead of reading the entire pagemap at once, CRIU uses a sliding window (typically **2MB** in size, defined as `PMC_SIZE`).
*   When a VMA is accessed that is not currently in the cache (a cache miss), the cache "refills" itself by reading the pagemap for the required range.
*   **Greedy Prefetching**: If the current VMA is small, the cache tries to fill the remainder of the 2MB window by pre-reading information for subsequent, adjacent VMAs. This significantly reduces the total number of `read()` system calls and minimizes the overhead of kernel-side page table walks.

### 2. ioctl(PAGEMAP_SCAN) Integration
On modern kernels (v6.7+), the pagemap cache leverages the `PAGEMAP_SCAN` ioctl. This interface is far more efficient than the legacy `/proc` file:
*   **Bulk Retrieval**: It allows CRIU to fetch information for multiple, non-contiguous page ranges in a single kernel call.
*   **Kernel-Side Filtering**: CRIU can instruct the kernel to only return information for specific categories of pages (e.g., pages that are both present in RAM and marked as "soft-dirty"), further reducing the amount of data transferred and processed in userspace.

### 3. Cache Invalidation
To ensure consistency, the pagemap cache is per-process and is strictly managed:
*   The cache is typically populated while the target process is **frozen** to ensure a stable view of memory.
*   The cache is invalidated whenever the process state might have changed or when the dumper transitions between different memory processing phases.

## Debugging and Control

The pagemap cache can be disabled for troubleshooting or performance comparison by setting the `CRIU_PMC_OFF` environment variable.

## See also
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
* [Memory Changes Tracking](memory-changes-tracking.md)
* [Optimizing Pre-dump Algorithm](optimizing-pre-dump-algorithm.md)
