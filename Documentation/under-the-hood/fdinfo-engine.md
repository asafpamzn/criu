# File Restoration Engine (fdinfo)

CRIU uses a sophisticated state machine to restore file descriptors (FDs) across a process tree, handling shared files, complex dependencies, and inter-process synchronization.

## Master and Slave Descriptors

In the Linux kernel, multiple FDs can refer to the same underlying "File Description." CRIU mirrors this by categorizing FDs into **Masters** and **Slaves**:

1.  **Master**: For each unique file object, one FD is designated as the master. This process is responsible for the actual `open()`, `socket()`, or `pipe()` system call that recreates the object.
2.  **Slaves**: All other FDs referring to the same object are slaves. They do not perform the creation call themselves; instead, they receive the file descriptor from the master.
3.  **Transport (SCM_RIGHTS)**: CRIU uses Unix domain sockets and the `SCM_RIGHTS` mechanism to "send" file descriptors from the master process to slave processes.

## Per-Process Restore Loop

Each task in the process tree executes a loop (`open_fdinfos`) to restore its descriptors. The core of this loop is the `file_desc_ops->open()` method.

### The `open()` State Machine
The `open()` method for a master file can return one of three values:
*   **0 (Success)**: The file is fully restored.
*   **1 (In Progress)**: The file has been opened (or the process has started opening it), but it cannot be completed yet due to a dependency on another file. The loop will call this method again in the next iteration.
*   **-1 (Failure)**: An error occurred, and restoration must abort.

### Early FD Distribution
To maximize parallelism, a master can return a valid FD in the `new_fd` argument even if it returns `1` (In Progress). This allows CRIU to immediately distribute the FD to all slave processes via `SCM_RIGHTS`, even before the master has finished its own restoration steps (e.g., a connected Unix socket waiting for its peer).

## Inter-Process Synchronization

CRIU uses **futexes** and a specialized event mechanism to coordinate between processes:
*   **set_fds_event(pid)**: Signals a task that a file it was waiting for (as a slave) is now available or that a dependency has changed.
*   **wait_fds_event()**: Causes a task to sleep until it receives a notification.
*   **FLE Stages**: Each descriptor entry (`struct fdinfo_list_entry` or `fle`) transitions through stages: `INITIALIZED` -> `OPEN` -> `RESTORED`.

## Key Dependencies

The engine must resolve complex dependencies between different file types:
1.  **TTYs**: A slave TTY can only be fully restored after its master peer is active.
2.  **Unix Sockets**: A connected socket must wait for its peer to `bind()` to its address before it can `connect()`.
3.  **Epoll**: An epoll FD can be created immediately, but adding FDs to its interest list must wait until those FDs are themselves restored.
4.  **Pipes and Socketpairs**: These calls create two FDs at once. One is treated as the primary master, and the second is distributed to the appropriate task (which might be the same task or a different one).

## Technical Notes

*   **Service FDs**: CRIU maintains its own internal FDs (for images, logs, etc.) in a "protected" range to avoid conflicts with the application's FDs during restoration.
*   **Ordering**: Descriptors are generally restored in ascending order of their FD number to improve efficiency, though dependencies can override this order.
