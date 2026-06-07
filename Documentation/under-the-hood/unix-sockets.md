# Unix Domain Sockets

CRIU supports checkpointing and restoring Unix domain sockets (AF_UNIX), including Stream, Datagram, and Sequential Packet types. Unix sockets are unique because they can be bound to paths in the filesystem and are frequently used to transfer file descriptors between processes.

## Key Challenges

1.  **Path and Inode Decoupling**: A Unix socket's address (its bind path) and the actual socket file on disk are not intrinsically linked in the kernel. If a socket file is moved or unlinked after `bind()`, the socket still reports its original address.
2.  **In-flight Descriptors**: Unix sockets can contain "in-flight" file descriptors sent via `SCM_RIGHTS` that have been sent by one process but not yet received by the peer.
3.  **Cross-Namespace Bindings**: Sockets in one mount namespace may be bound to paths that are only visible or reachable from another mount namespace.

## CRIU's Solution: `SIOCUNIXFILE`

To reliably restore Unix sockets, CRIU developers upstreamed the `SIOCUNIXFILE` ioctl to the Linux kernel. This ioctl allows CRIU to:
*   Retrieve an `O_PATH` file descriptor to the actual socket file on disk, regardless of its current name or overmounting status in the filesystem.
*   By obtaining this `O_PATH` descriptor, CRIU can definitively identify the exact mount point and inode of the socket file, ensuring it can be recreated in the correct location during restoration.

## Dumping Workflow

1.  **Identity and State**: CRIU uses the `sock_diag` netlink interface to retrieve the socket's type, state, and peer ID.
2.  **Peer Linking**: For connected or related sockets (like those created via `socketpair()`), CRIU uses the peer information to link them together in the process tree model.
3.  **File Handle Retrieval**: For sockets bound to the filesystem, CRIU uses `SIOCUNIXFILE` to get a handle to the socket file and records its location.
4.  **Queue and FD Capture**: Send and receive queues are peeked to capture pending data. Crucially, any file descriptors currently residing in the socket's queues are also captured and dumped.

## Restoration Workflow

1.  **Socket Creation**: CRIU recreate the socket using the original family, type, and protocol.
2.  **Address Binding**:
    *   CRIU creates a temporary "yard" (a `tmpfs` mount) to safely recreate socket files without interfering with the host filesystem.
    *   It creates the required directory structure and uses symlinks to ensure the `bind()` call targets the correct path.
3.  **Peer Connection**: For connected stream sockets, one peer performs a `bind()` and `listen()`, while the other calls `connect()`. CRIU's file restoration engine coordinates this to ensure the server end is ready before the client attempts to connect.
4.  **State and Data Injection**: Socket options and pending data are restored.
5.  **Descriptor Redelivery**: In-flight file descriptors are re-injected into the socket's queue using the `SCM_RIGHTS` mechanism, ensuring the application receives them upon resumption.

## External Unix Sockets

If a socket is connected to a process *outside* the tree being checkpointed, CRIU cannot capture the peer's state. These are **External Sockets**.
*   Restoration will fail by default for these sockets to prevent inconsistent states.
*   Users can explicitly allow these connections using the `--external unix[ID]` option, which tells CRIU to treat the socket as a persistent external dependency.

## See also
* [Network Sockets](sockets.md)
* [External UNIX socket](external-unix-socket.md)
* [Descriptor Assignment](how-to-assign-needed-file-descriptor-to-a-file.md)
* [Kerndat Feature Detection](kerndat.md)
