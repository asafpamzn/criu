# Checkpoint and Restore Architecture

This page describes the high-level design and internal mechanics of the Checkpoint and Restore processes in CRIU.

## Checkpoint

The checkpoint procedure captures the full state of a process tree. It combines information from the Linux kernel's `/proc` filesystem with data extracted directly from the processes' address space.

### 1. Freezing the Process Tree
CRIU begins by identifying the process group leader (via the `--tree` option) and recursively collecting all threads and children. To ensure a consistent snapshot, the entire tree must be "frozen."

*   **ptrace**: CRIU uses `PTRACE_SEIZE` followed by `PTRACE_INTERRUPT` to stop tasks without delivering signals that could be visible to the application.
*   **Freezer CGroup**: Alternatively, the [Freezer CGroup](freezing-the-tree.md) can be used to freeze all tasks in a single operation.

### 2. Resource Collection (External State)
CRIU gathers state that the kernel exposes via `/proc`:
*   **File Descriptors**: Parsed from `/proc/$pid/fdinfo` (which includes positions and flags).
*   **Memory Maps**: Captured from `/proc/$pid/smaps` and `/proc/$pid/map_files`.
*   **Core State**: Task statistics and basic identifiers from `/proc/$pid/stat`.

### 3. Parasite Injection (Internal State)
Some state (like memory contents and specific credentials) can only be captured from within the process. CRIU uses a technique called **parasite injection**:
1.  **Infection**: CRIU uses `ptrace` to inject a small bit of code into the task's instruction stream (at the current `CS:IP`).
2.  **Bootstrap**: This code executes an `mmap` syscall to allocate space for the full **parasite blob**.
3.  **Execution**: The parasite code runs as a daemon inside the task, communicating with the CRIU coordinator via a Unix socket to dump memory pages and other internal metadata.

### 4. Cleanup
Once the state is captured, CRIU uses `ptrace` to remove the parasite code and restore the original instructions. The processes are then either resumed or killed, depending on the command-line options.

---

## Restore

The restore procedure is essentially the reverse of a checkpoint. CRIU "morphs" itself into the process tree it is restoring through a multi-stage process.

### 1. Resolve Shared Resources
CRIU analyzes the image files to identify resources shared between processes (e.g., shared memory segments, pipes, or inherited file descriptors). It determines which process will "create" the resource and how others will "inherit" it.

### 2. Fork the Process Tree
CRIU calls `fork()` repeatedly to recreate the original process hierarchy. To restore specific PIDs, it uses the `ns_last_pid` interface or the `clone3` system call. At this stage, only process leaders are created; threads are restored later.

### 3. Restore Basic Resources
Each process in the new tree begins restoring its environment:
*   **Namespaces**: Joins or creates Network, Mount, UTS, and IPC namespaces.
*   **Files and Sockets**: Reopens file descriptors and recreates network sockets.
*   **Memory Prep**: Maps anonymous memory regions and fills them with data from the images.

### 4. The Restorer Context
To restore the final memory layout, CRIU must unmap its own code and data. This requires a **restorer blob**:
*   **Self-Contained**: The blob is a Position-Independent Executable (PIE) that contains all necessary logic to perform the final `mmap` and `munmap` calls.
*   **Non-Conflicting**: It is mapped into a "hole" in the task's address space that does not conflict with either CRIU's current mappings or the task's original mappings.
*   **Final Transition**: The process jumps into the restorer blob, which unmaps CRIU, maps the final memory regions, restores timers and credentials, and recreates any additional threads.

### 5. Sigreturn
The very last step of the restorer is to call `sigreturn`. CRIU prepares a special signal frame on the stack that contains the original register state (including the instruction pointer) of the process at the time of the checkpoint. The `sigreturn` syscall tells the kernel to load this state and resume execution of the application code.

*See also: [Restorer Context](restorer-context.md), [Tree After Restore](tree-after-restore.md)*
