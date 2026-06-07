# TTY (Teletype) Support

CRIU provides support for checkpointing and restoring various types of Linux terminals (TTYs), with a primary focus on **Unix98 Pseudoterminals (PTYs)**.

## Key Information Captured

For each TTY instance, CRIU captures a comprehensive set of kernel metadata:
1.  **Identity**: The TTY type (PTY, Console, Serial, or Virtual Terminal), subtype (Master or Slave), and its unique kernel index.
2.  **Configuration**: Detailed `termios` settings (baud rate, parity, control characters) and window size parameters (`winsize`).
3.  **Ownership and Permissions**: The original UID/GID and mode of the TTY device node.
4.  **Process Context**: Controlling terminal status, Session ID (SID), and Foreground Process Group (PGRP).
5.  **Extended State**: Lock status (`TIOCGLCKTRMIOS`), exclusive mode settings, and packet mode (`TIOCPKT`) flags.

## The PTY Index Challenge

A major challenge in restoring PTYs is that the Linux kernel assigns indices (e.g., the `N` in `/dev/pts/N`) sequentially when `/dev/ptmx` is opened. Standard userspace APIs do not allow requesting a specific index.

### The "Sequential Opening" Strategy
To ensure each PTY is restored with its original index, CRIU employs a specialized "brute-force" technique:
1.  **Looping Open**: CRIU enters a loop, repeatedly calling `open("/dev/ptmx")`.
2.  **Index Verification**: After each open, it queries the assigned index using the `TIOCGPTN` ioctl.
3.  **Consuming Indices**: If the assigned index is lower than the target index, CRIU **keeps the file descriptor open**. This prevents the kernel from reassigning that index.
4.  **Target Match**: Once the kernel assigns the correct original index, CRIU uses that descriptor as the restored master PTY.
5.  **Cleanup**: All "placeholder" descriptors opened during the loop are then closed, freeing those indices for the rest of the system.

## Restoration Workflow

1.  **Master Peer Reconstruction**: A designated process recreates the master PTY using the sequential opening strategy.
2.  **Slave Peer Attachment**: Slave processes open the corresponding `/dev/pts/N` devices. Because the master was created with the correct index, these slaves automatically link to the correct peer.
3.  **State Application**: Termios, window sizes, and device ownership are applied to the newly opened descriptors.
4.  **Controlling Terminal Re-binding**: CRIU re-establishes the relationship between each process and its controlling terminal using the `TIOCSCTTY` ioctl.

## Current Limitations

*   **Buffered Data**: Captured TTY input and output queues (data that was sent but not yet read) are currently not fully restored. CRIU ensures the *interface* is restored, but the application may see a reset of buffered streams.
*   **Legacy BSD PTYs**: Support for older BSD-style PTYs is not implemented, as the modern Linux kernel does not provide the necessary introspection to reliably pair these devices.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Descriptor Assignment](how-to-assign-needed-file-descriptor-to-a-file.md)
* [Kerndat Feature Detection](kerndat.md)
