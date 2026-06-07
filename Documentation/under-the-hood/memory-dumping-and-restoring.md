# Memory Dumping and Restoring

Dumping and restoring the memory of a process tree is one of the most critical and complex tasks performed by CRIU. This document details the mechanisms, optimizations, and kernel interfaces involved in this process.

## The Virtual Memory Layout (VMAs)

A process's address space is composed of several Virtual Memory Areas (VMAs). CRIU identifies these areas by parsing `/proc/$pid/smaps` and `/proc/$pid/map_files/`.
*   **Metadata**: Each VMA's start address, end address, protection flags (read, write, execute), and sharing status (private or shared) are recorded in the `mm-$id.img` file.
*   **Backing Store**: CRIU also records whether a VMA is anonymous (backed by RAM/swap) or file-backed.

## The Dumping Process

Capturing memory contents while maintaining consistency and performance requires a multi-stage approach.

### 1. Parasite Injection
CRIU cannot efficiently read a process's private memory from the outside. Instead, it injects **parasite code** into the target task. This code runs within the task's own address space and context, allowing it direct access to all memory regions.

### 2. Zero-Copy Dumping (vmsplice)
To transfer memory from the parasite to the CRIU dumper with minimal overhead, CRIU uses a zero-copy mechanism:
1.  **Pipe Setup**: CRIU creates a pipe and sends one end to the parasite via a Unix domain socket.
2.  **vmsplice**: The parasite uses the `vmsplice()` system call with the `SPLICE_F_GIFT` flag. This effectively "gifts" the memory pages to the kernel's pipe buffer without copying the data in userspace.
3.  **Splice to Image**: The CRIU dumper then uses `splice()` to move the data from the pipe directly into the image file (`pages-$id.img`) or to a network socket (for the page server).

### 3. Page Deduplication and Skipping
CRIU avoids dumping unnecessary data to save time and space:
*   **Unchanged File Pages**: Read-only, file-backed pages (like library code) that have not been modified are not dumped. CRIU simply records the file and offset to re-map them during restoration.
*   **Dirty Tracking**: Using the **soft-dirty bit** (or `PAGEMAP_SCAN`), CRIU can identify and dump only those pages that have changed since a previous pre-dump.

---

## The Restoration Process

Restoring memory involves reconstructing the exact address space layout the application had at the moment of the checkpoint.

### 1. Re-mapping VMAs
During the early stages of restoration, each process calls `mmap()` to recreate its VMAs based on the data in `mm-$id.img`. 
*   **Anonymous Memory**: Mapped as private and anonymous.
*   **File Mappings**: Re-mapped from their original files on disk.

### 2. Filling Memory Contents
CRIU then repopulates the mappings with the data stored in the `pages-$id.img` files. For efficiency, CRIU uses its own optimized I/O routines to read the images and fill the memory regions.

### 3. COW Preservation
CRIU uses a specialized strategy to ensure that memory shared via `fork()` (Copy-on-Write) remains shared after restoration. This minimizes the total physical memory footprint of the restored process tree. See [COW Memory](copy-on-write-memory.md) for details.

## Advanced Migration Techniques

*   **Page Server**: During live migration, memory pages are sent over the network to a page server on the destination host, avoiding expensive disk I/O.
*   **Lazy Migration (Userfaultfd)**: CRIU can restore a process immediately without its memory and then load pages on demand as the application accesses them. This is powered by the `userfaultfd` kernel feature and is essential for reducing initial downtime.

## See also
* [Memory Changes Tracking](memory-changes-tracking.md)
* [Copy-on-write Memory](copy-on-write-memory.md)
* [Userfaultfd](userfaultfd.md)
* [Page Server](page-server.md)
