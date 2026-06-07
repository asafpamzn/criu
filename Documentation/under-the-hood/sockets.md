# Network Sockets

CRIU provides extensive support for checkpointing and restoring a wide variety of Linux network sockets, including Unix domain sockets, IPv4/IPv6 (TCP, UDP, RAW), Netlink, and Packet sockets.

## Key Information Captured

To faithfully restore a socket, CRIU must capture its full kernel state:
1.  **Identity**: Family (AF_INET, AF_UNIX, etc.), type (SOCK_STREAM, SOCK_DGRAM), and protocol (TCP, UDP, etc.).
2.  **Addresses**: Local binding addresses and, for connected sockets, the remote peer address and port.
3.  **Socket Options**: A wide range of options (e.g., `SO_KEEPALIVE`, `SO_REUSEADDR`, `TCP_NODELAY`, buffer sizes) are captured and reapplied.
4.  **Queues**: Data currently residing in the send and receive buffers is extracted and re-injected upon restoration.
5.  **State**: Whether the socket is listening, connected, or in a transitional state (like `FIN_WAIT` or `CLOSE_WAIT` for TCP).

## The Dumping Process

CRIU combines information from multiple sources to build a complete picture of each socket.

### 1. sock_diag
The primary source of truth is the **sock_diag** kernel module. CRIU sends Netlink requests to `sock_diag` to retrieve detailed internal state for most socket families. This provides protocol-level information that is not available via standard userspace APIs.

### 2. SCM_RIGHTS and Parasite
For deeper inspection—such as peeking at socket queues or enabling TCP repair mode—CRIU uses its **parasite code** to send the actual socket file descriptor to the CRIU process via a Unix domain socket using the `SCM_RIGHTS` mechanism. This allows the CRIU coordinator to perform `ioctl`, `getsockopt`, and `recv(MSG_PEEK)` calls directly on a local copy of the socket.

## Restoration Strategies

### TCP Repair Mode
Restoring a TCP connection without disrupting the peer (and without sending any packets) is a major challenge. CRIU uses a specialized kernel feature called **TCP Repair Mode**:
1.  CRIU creates a new socket and immediately puts it into repair mode.
2.  While in this mode, CRIU can manually set the sequence numbers, window sizes, and other protocol-level state to match the captured dump.
3.  It populates the send and receive queues with the dumped data.
4.  Finally, it takes the socket out of repair mode, allowing the connection to resume as if it were never interrupted.

### Unix Sockets and SCM_RIGHTS
Unix sockets are unique because they can be used to transfer other file descriptors. CRIU captures these "in-flight" descriptors (files that have been sent but not yet received) and ensures they are correctly re-queued for the restored process.

## Supported Socket Families

*   **AF_UNIX**: Full support for Stream, Datagram, and Sequential Packet types, including abstract and file-backed names.
*   **AF_INET / AF_INET6**: 
    *   **TCP**: Full connection state restoration via Repair Mode.
    *   **UDP / UDPLITE**: Captures addresses, options, and queues.
    *   **RAW**: Captures protocol settings and binding state.
*   **AF_NETLINK**: Captures the state of Netlink sockets used for kernel communication (e.g., for routing or audit).
*   **AF_PACKET**: Supports capturing packet filters (BPF) and specific interface bindings.

## See also
* [TCP Connection Details](tcp-connection.md)
* [Unix Sockets and SCM_RIGHTS](unix-sockets.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Kerndat Feature Detection](kerndat.md)
