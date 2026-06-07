# Service Descriptors (Service FDs)

During dump and restore operations, CRIU requires numerous internal file descriptors (FDs) to manage logs, images, RPC communication, and transport sockets. Because the application being checkpointed or restored may use any arbitrary FD number, CRIU must ensure its internal descriptors never conflict with those of the application. To achieve this, CRIU uses the **Service FD Engine**.

## The Protected Range

CRIU avoids FD collisions by placing its internal descriptors in a "protected" range at the very top of the process's file descriptor table.

*   **Lifting Limits**: Upon startup, CRIU attempts to lift its `RLIMIT_NOFILE` resource limit (using `rlimit_unlimit_nofile()`) to a very high value (typically 1,048,576 or higher).
*   **Top-Down Allocation**: Service FDs are allocated starting from the maximum allowed FD number and working downwards. This strategy places them as far as possible from the range typically used by applications (which usually start from 0 and work upwards).

## Service FD Engine Mechanisms

The engine (`criu/servicefd.c`) provides a robust abstraction for managing these descriptors through several key techniques:

### 1. Per-Process Isolation in Shared Tables
In scenarios where multiple processes share the same file descriptor table (e.g., threads or processes created with `CLONE_FILES`), CRIU assigns a unique `service_fd_id` to each task. The engine uses this ID to offset the service FD range, ensuring that even tasks sharing an FD table have distinct, non-overlapping slots for their internal CRIU descriptors.

### 2. Descriptor Relocation
When CRIU opens a file for its own use (such as an image file or the log), the kernel initially assigns it the lowest available FD number (e.g., FD 3). CRIU then uses `fcntl(F_DUPFD_CLOEXEC)` or `dup3()` to "move" that descriptor to its designated high-range slot and immediately closes the original low-numbered descriptor.

### 3. Protection Flags and Safety
During critical phases of restoration—specifically when the application's FDs are being "planted" into their final numeric slots—CRIU sets a global `sfds_protected` flag. While this flag is set, the service FD engine is "locked." Any attempt by the code to modify or close a service descriptor will trigger an immediate safety crash (BUG), preventing accidental corruption of the restoration state.

## Common Service FD Types

The engine manages various types of descriptors, each with a specific role:
*   **LOG_FD**: The descriptor for the main CRIU log file.
*   **IMG_FD**: The descriptor used for accessing image files.
*   **RPC_SK**: The socket used for RPC communication with external management tools.
*   **TRANSPORT_FD**: Sockets used to "send" and "receive" FDs between processes via `SCM_RIGHTS`.
*   **PROC_FD**: A stable handle to the `/proc` filesystem.
*   **CGROUP_YARD**: A descriptor for the temporary directory used during cgroup restoration.

## See also
* [Dumping File Descriptors](dumping-files.md)
* [Descriptor Assignment](how-to-assign-needed-file-descriptor-to-a-file.md)
