# Pidfd Support

A **pidfd** is a file descriptor that refers to a specific process. Unlike traditional numeric PIDs, which can be reused by the kernel once a process terminates, a pidfd is a stable and race-free handle. It remains valid as long as the descriptor is open, even after the process it refers to has died. CRIU provides full support for checkpointing and restoring pidfds owned by applications.

## How CRIU Handles Pidfds

CRIU treats pidfds as a specialized type of file descriptor. During a dump, it captures both the target of the pidfd and its configuration.

### 1. Checkpointing (Dumping)
When CRIU encounters a pidfd in a process's file descriptor table:
*   **Target Identification**: It parses `/proc/$pid/fdinfo/$fd` to determine the numeric PID that the pidfd currently refers to.
*   **Tree Validation**: CRIU verifies that this target PID is part of the process tree being checkpointed. This ensures that the process will be available for re-binding during restoration.
*   **Metadata Capture**: CRIU records the target process's namespace-local PID and any flags associated with the pidfd (such as `O_NONBLOCK` or `O_CLOEXEC`).

### 2. Restoration
Restoring a pidfd involves recreating a handle that points to the equivalent process in the newly restored tree.

*   **Alive Processes**: If the target process is alive, CRIU simply calls the `pidfd_open()` system call on the restored PID of that task.
*   **Dead Processes**: A unique feature of pidfds is that they can be held open even after the target process has exited. To restore this state, CRIU:
    1.  Creates a temporary "helper" process.
    2.  Opens a pidfd to this helper.
    3.  Terminates the helper process.
    This leaves the restored application with a valid pidfd that refers to a dead process, perfectly mimicking the original state.

## Kernel Evolution: From Anonymous Inodes to `pidfs`

The underlying implementation of pidfds in the Linux kernel has changed over time:
*   **Pre-v6.9**: Pidfds were implemented using anonymous inodes. In `/proc/$pid/fd`, they appeared as `anon_inode:[pidfd]`.
*   **v6.9 and later**: Pidfds are now part of a dedicated **pidfs** filesystem. They appear in `/proc` as `pidfd:[N]`.

CRIU automatically detects these kernel differences and handles both formats transparently, ensuring that pidfds are correctly identified and restored regardless of the host kernel version.

## Current Limitations
*   **PIDFD_THREAD**: Support for pidfds that target specific threads (created with the `PIDFD_THREAD` flag) is currently not implemented.

## See also
* [Pidfd Store (Iterative Migration)](pidfd-store.md)
* [PID Restoration](pid-restore.md)
* [Kerndat Feature Detection](kerndat.md)
