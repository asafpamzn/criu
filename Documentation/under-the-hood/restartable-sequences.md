# Restartable Sequences (rseq)

Restartable Sequences (rseq) is a Linux kernel feature (introduced in v4.18) that enables high-performance userspace operations on per-CPU data without requiring atomic instructions or traditional locking. Each thread registers a `struct rseq`, and the kernel ensures that if a thread is preempted or interrupted while inside a critical section, it is "restarted" by jumping to a predefined abort handler.

## The Challenge of C/R with rseq

Checkpointing and restoring rseq is exceptionally delicate because the kernel's rseq state is tightly coupled with process execution.

1.  **Dumping Sensitivity**: If an infected thread is allowed to run even briefly (e.g., to execute parasite code), the kernel's `rseq_handle_notify_resume` hook may be triggered. This would cause the kernel to "fix up" the rseq state, clearing critical section pointers in userspace memory and losing the very state CRIU needs to capture.
2.  **Restoration Morphing**: During restoration, CRIU "morphs" into the target process. If the CRIU binary itself was compiled with rseq support (common in modern distributions), it may have an active rseq registration that must be carefully managed before the memory layout is swapped.

## How CRIU Handles rseq

CRIU provides robust support for rseq, ensuring that threads interrupted within a critical section correctly restart after restoration.

### 1. Checkpointing (Dumping)
CRIU captures the rseq configuration without disturbing the thread's execution state:

*   **PTRACE_GET_RSEQ_CONF**: CRIU uses this ptrace command (Kernel v5.13+) to retrieve the address, size, and signature of the `struct rseq` registered for each thread.
*   **External Peeking**: To avoid triggering kernel fixups, CRIU **does not** use its standard parasite code to read rseq-related memory. Instead, it uses `PTRACE_PEEKDATA` to read the `struct rseq` and `struct rseq_cs` (critical section descriptor) directly from the outside while the task is frozen.
*   **Critical Section Detection**: By reading the `rseq_cs` pointer within the `struct rseq`, CRIU identifies if a thread was in the middle of a sequence at the time of the snapshot.

### 2. Restoration
The restoration process involves two critical rseq-related steps:

*   **Unregistering Restorer rseq**: Before CRIU performs the final "morphing" (unmapping its own memory and mapping the application's memory), it must explicitly **unregister** any rseq area used by the CRIU process itself. Failing to do so would cause the kernel to attempt to update a `cpu_id` field in memory that has been unmapped, resulting in an immediate segmentation fault.
*   **Re-establishing Application rseq**: Once the application's memory layout and thread registers are restored, CRIU calls the `rseq()` system call for each thread. It re-registers the original `struct rseq` at its original address.
*   **Automatic Restart**: Because the `rseq_cs` pointer is restored as part of the thread's memory, the kernel will detect the active critical section upon the first resumption and automatically trigger the application's restart/abort logic, ensuring data integrity.

## Kernel Requirements

*   **rseq support**: Linux Kernel v4.18+
*   **PTRACE_GET_RSEQ_CONF**: Linux Kernel v5.13+ (Required for reliable automated detection).

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Parasite Code](parasite-code.md)
* [Restorer Context](restorer-context.md)
