# Restorer Context

The **Restorer Context** refers to the final stage of the restoration process, where a CRIU process "morphs" itself into the target application. This critical transition is performed by a specialized [PIE](code-blobs.md) blob known as the **Restorer PIE**.

## Why a Dedicated Context is Necessary

During the final stage of restoration, CRIU must accomplish two conflicting goals:
1.  **Memory Swapping**: It must unmap all of CRIU's own code, stack, and data to completely clear the address space for the application.
2.  **Memory Re-mapping**: It must map the application's original memory regions (VMAs) back into their exact original addresses.

While these operations are occurring, some code must remain in the address space to execute the necessary `munmap()` and `mmap()` system calls. The Restorer PIE is designed to reside in a temporary "safe hole" in the address space—a range that does not conflict with either CRIU's temporary mappings or the application's final layout.

## The Restoration Workflow

1.  **Preparation**: The root CRIU process identifies the restorer code and prepares it for distribution.
2.  **Forking**: The process tree is recreated. Since the restorer code is mapped in the root task before forking, all child processes share the same physical memory for the restorer via standard Copy-on-Write (COW).
3.  **Safe Hole Detection**: Each restored process scans its target memory layout (from the `mm.img` file) to find a contiguous area large enough to hold the restorer code and its stack.
4.  **Remapping**: Each process uses `mremap()` to move the shared restorer blob to its specific safe hole.
5.  **Execution Jump**: The process jumps from the main CRIU code into the restorer PIE.
6.  **Cleanup and Reconstruction**: The restorer PIE unmaps CRIU, recreates the application's original mappings, and populates them with data from the image files.
7.  **Final Transition (Sigreturn)**: The very last step is calling `sigreturn()`. The restorer prepares a special signal frame on the stack containing the application's original register state (including the instruction pointer). The kernel then loads this state, effectively resuming the application from the exact point of the checkpoint.

## Technical Characteristics

### Freestanding PIE
Because the restorer runs in an environment where standard libraries have been unmapped, it is a **freestanding** Position-Independent Executable. It contains its own minimal assembly-level system call wrappers and does not depend on `glibc` or any external runtime.

### Conflict Avoidance
The algorithm for finding the "safe hole" is architecture-specific. It must account for various kernel-mapped regions like the vDSO, the stack, and potential guard pages to ensure that the restorer code never overlaps with memory that the application needs.

## See also
* [Code Blobs](code-blobs.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
