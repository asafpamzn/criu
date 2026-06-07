# Pending Signals

In Linux, a signal is marked as **pending** if it has been delivered to a task but has not yet been handled (e.g., because the signal is blocked or the task is currently stopped). CRIU provides full support for capturing and restoring these pending signal queues, ensuring that the application's signal state remains perfectly consistent across a checkpoint.

## Checkpoint and Restore of Pending Signals

CRIU manages pending signals using specialized `ptrace` interfaces and signal injection system calls.

### 1. Checkpointing (Dumping)
During a dump, CRIU must extract both the list of pending signals and the detailed metadata associated with each one (the `siginfo_t` structure).

*   **PTRACE_PEEKSIGINFO**: CRIU uses this system call (introduced in Linux kernel v3.10 specifically to support CRIU) to read the signal queues of the target task without actually delivering them.
    *   **Private Signals**: Signals delivered to a specific thread are read using standard peeking.
    *   **Shared Signals**: Signals delivered to the entire process (which can be handled by any thread) are read by adding the `PTRACE_PEEKSIGINFO_SHARED` flag.
*   **Batch Processing**: CRIU reads signals in batches (typically 32 at a time) to efficiently capture entire queues, which is common in high-throughput applications.
*   **Signal Mask**: In addition to the pending signals, CRIU uses `PTRACE_GETSIGMASK` to capture the set of signals currently blocked by each thread. This mask is essential because it determines why the signals were pending in the first place.

### 2. Restoration
To recreate the pending state, CRIU re-injects the captured signals into the newly created process tree before it begins normal execution.

*   **rt_sigqueueinfo()**: For process-wide (shared) signals, CRIU uses this system call to send a signal to a process with the original `siginfo_t` data.
*   **rt_tgsigqueueinfo()**: For thread-specific (private) signals, CRIU uses this variant to target a specific thread ID (TID) within a process.
*   **Preserving siginfo**: These system calls allow CRIU to pass the exact original `siginfo_t` structure (including the sender's PID, UID, and any signal-specific data), ensuring the restored task sees the identical signal context.

## Shared vs. Private Pending Signals

*   **Multi-threaded Handling**: In multi-threaded applications, signals are carefully tracked:
    *   **Shared signals** are stored in the process leader's `core.img`.
    *   **Private signals** are stored in the `core.img` corresponding to each individual thread.
*   **Restore Order**: Signals are restored while the task is still under CRIU's control, ensuring that they remain pending until the task is finally resumed and its original signal mask is applied.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Parasite Code](parasite-code.md)
