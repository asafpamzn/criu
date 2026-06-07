# Stages of Restoration

Restoring a complex process tree is a multi-step operation coordinated by a central CRIU process and executed across the newly created process tree. Each stage is synchronized to ensure that dependencies (such as shared files and parent-child relationships) are met and security invariants are maintained.

## The Synchronization Mechanism

CRIU uses a global state machine (defined as `CR_STATE_*` constants) to coordinate between the main CRIU process and the tasks being restored. Tasks use **futexes** in shared memory to signal the completion of their work in each stage and wait for the coordinator to signal the transition to the next stage.

## Stage 1: Root Task Initiation (`CR_STATE_ROOT_TASK`)
The main CRIU process performs initial image analysis, resolves shared resources, and prepares the restorer code blobs. It then forks the **root task** of the tree being restored. The root task performs initial pre-checks and begins its environmental setup.

## Stage 2: Namespace Preparation (`CR_STATE_PREPARE_NAMESPACES`)
The root task (and specialized helpers) initializes the required namespaces (Mount, Network, IPC, UTS, Time). This ensures that all subsequent processes in the tree are created within the correct containerized environment from the moment of their birth.

## Stage 3: Process Tree Forking (`CR_STATE_FORKING`)
The process tree is recursively forked until all processes are recreated. 
*   **PID Restoration**: Processes are created with their original PIDs using the `clone3()` system call or the `ns_last_pid` interface.
*   **Transport Setup**: Each task creates an abstract Unix domain socket to "receive" shared file descriptors from its designated "master" peers.

## Stage 4: Main Resource Restoration (`CR_STATE_RESTORE`)
This is the primary stage where the bulk of the application state is reconstructed:
*   **Files and Sockets**: File descriptors are opened locally or received via `SCM_RIGHTS`.
*   **Memory Mapping**: VMAs are recreated via `mmap()`.
*   **Restorer Jump**: Each task "morphs" by jumping from the main CRIU code into the freestanding **Restorer PIE** blob.
*   **Threads**: Individual application threads are recreated within each process.

## Stage 5: Signal Synchronization (`CR_STATE_RESTORE_SIGCHLD`)
Tasks restore their original `SIGCHLD` handlers. This stage serves as a critical synchronization point to transition from CRIU's internal error-tracking (which relies on `SIGCHLD` to detect failed restoration steps in children) to the application's original signal handling logic.

## Stage 6: Security and Credentials (`CR_STATE_RESTORE_CREDS`)
For security reasons, this is the final stage before the application resumes execution. CRIU ensures that sensitive attributes are restored in a specific order:
1.  **Credentials**: UIDs, GIDs, and Capability sets are applied.
2.  **Seccomp**: Security filters are enabled only after the final credentials are in place.
3.  **Process Attributes**: The "dumpable" status and parent-death signals (`pdeath_sig`) are re-established.

By delaying these steps until the very end, CRIU prevents potential security vulnerabilities where a partially-restored process could be intercepted or manipulated while in a transitional state.

## Stage 7: Resumption (`CR_STATE_COMPLETE`)
The tasks execute their final `sigreturn()` call from within the Restorer PIE. This restores the original register state (including the instruction pointer) and jumps the CPU back into the application's code. The process tree is now fully restored and running.

## See also
* [Restorer Context](restorer-context.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [PID Restoration](pid-restore.md)
