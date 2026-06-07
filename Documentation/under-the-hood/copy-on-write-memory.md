# Copy-on-Write (COW) Memory Restoration

CRIU employs a specialized multi-stage process to preserve Copy-on-Write (COW) sharing of private anonymous memory mappings during restoration. This prevents the memory duplication that would occur if each process's memory were restored independently, thereby significantly reducing the memory footprint of the restored process tree.

## The Problem

When a process calls `fork()`, the Linux kernel optimizes memory usage by sharing private anonymous mappings between the parent and child. Physical pages are only duplicated (COW) when one of the processes modifies them. 

Traditional checkpointing captures each process's memory separately. If restored naively (by mapping and filling each VMA individually), the kernel would allocate separate physical pages for the parent and child, even for pages that were originally shared. This leads to a massive increase in physical memory usage upon restoration.

## CRIU's COW Restoration Strategy

To keep COW mappings intact, CRIU performs restoration in a way that mimics the original `fork()` behavior.

### 1. Identifying COW Candidates
Before forking the process tree, CRIU analyzes the memory maps of all tasks:
*   It compares each task's VMAs with those of its parent.
*   Two VMAs are identified as COW candidates if they have identical start/end addresses, the same protection flags (e.g., `PROT_READ`, `PROT_WRITE`), and belong to the same executable.
*   This mapping is stored internally, marking which VMAs are "inherited" from a parent.

### 2. Pre-mapping and Filling
During restoration, processes are created in a specific order:
1.  **Root VMA Population**: If a VMA is the "root" of a COW set (it is not inherited), the restoring task maps it and fills it with data from the image files.
2.  **Inheritance via Fork**: When a task forks a child, the child automatically inherits the parent's memory mappings via the standard kernel COW mechanism.
3.  **Content Verification**: The child then iterates through its own memory images:
    *   It compares the page contents in the image with the data already present in its inherited memory (which it got from the parent).
    *   If the contents match exactly, the physical page remains shared with the parent.
    *   If they differ (meaning the page was modified in either process after the original fork), the child overwrites the page with the data from its image, triggering a kernel COW event for that specific page.

### 3. Cleaning Up (madvise)
A parent may contain pages that were unmapped or modified in the child process. To ensure the child's memory layout is perfectly accurate:
*   CRIU maintains a bitmap of pages touched during the content verification stage.
*   After all pages are processed, CRIU uses `madvise(MADV_DONTNEED)` on any pages that exist in the inherited VMA but were not present in the child's dump images. This effectively "punches holes" in the child's VMA to match its original state while preserving the sharing of other pages.

## Current Limitations

*   **Reparenting to Init**: If a process was reparented to the system `init` (PID 1) and that `init` process is not part of the checkpointed process tree, CRIU cannot identify the parent's VMAs, and COW sharing will not be restored for that process.
*   **VMA Movement**: If a VMA was moved (e.g., via `mremap`) after the original `fork()`, CRIU's current address-based matching algorithm will fail to identify it as a COW candidate.

## See Also
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
* [Restorer Context](restorer-context.md)
