# TCP Connection Checkpoint and Restore

Checkpointing and restoring established TCP connections is one of CRIU's most advanced features. It allows migrating live applications without dropping active network sessions, provided that the network infrastructure (such as IP routing, virtual IPs, or NAT) supports the transition.

## The Challenge

Standard TCP is managed entirely by the kernel's network stack. Under normal circumstances, userspace cannot:
1.  Read or set internal sequence numbers.
2.  Directly populate the kernel's send and receive buffers.
3.  Transition a socket between states (e.g., from `SYN_SENT` to `ESTABLISHED`) without performing an actual network handshake.

Attempting to restore a connection without specific kernel support would lead to immediate sequence number mismatches and connection resets (RST) from the remote peer.

## The Solution: TCP Repair Mode

To address these limitations, CRIU developers implemented **TCP Repair Mode** in the Linux kernel. When a socket is placed into repair mode, the TCP state machine is suspended, and the kernel allows userspace to manipulate its internal parameters directly.

### Checkpointing (Dumping)
1.  **Network Locking**: Before capturing the socket state, CRIU "locks" the connection using **iptables** or **nftables**. This ensures the kernel drops any incoming packets from the peer, preventing the connection state from changing while CRIU is performing the dump.
2.  **Enable Repair**: CRIU puts the socket into repair mode (`TCP_REPAIR`).
3.  **State Capture**: Using the `libsoccr` library, CRIU extracts:
    *   **Sequence Numbers**: The current positions in the data stream (`TCP_QUEUE_SEQ`).
    *   **TCP Options**: Window scaling factors, timestamps, and SACK settings (`TCP_REPAIR_OPTIONS`).
    *   **Window Parameters**: Send and receive window sizes and offsets (`TCP_REPAIR_WINDOW`).
    *   **Queue Data**: The actual bytes currently residing in the kernel's send and receive buffers.
4.  **Silent Close**: Once the state is captured, the socket is closed while still in repair mode. This is crucial as it prevents the kernel from sending `FIN` or `RST` packets to the peer, keeping the connection "alive" from the peer's perspective.

### Restoration
1.  **Socket Creation**: CRIU creates a new socket and immediately enables repair mode.
2.  **Binding**: The socket is bound to the original local IP address and port.
3.  **State Injection**: captured parameters (sequences, windows, options) are applied to the new socket using `setsockopt`.
4.  **Queue Re-population**: The send and receive buffers are re-filled with the original data.
5.  **Activation**: CRIU takes the socket out of repair mode. The kernel now considers the connection to be in the exact state it was at the moment of the checkpoint.
6.  **Network Unlocking**: Finally, the network locks are removed. The application resumes, and the next packet sent or received will have perfectly consistent sequence numbers.

## Network Locking Methods

CRIU supports multiple strategies to manage the network during migration:
*   **nftables** (Preferred): Uses the modern `nft` API to create efficient, temporary rules.
*   **iptables**: Uses traditional `iptables` commands to drop packets for the specific 4-tuple.
*   **Skip**: Allows external orchestration (e.g., by an SDN controller) to handle packet buffering and redirection.

## See also
* [Network Sockets](sockets.md)
* [Changing IP Addresses](change-ip-address.md)
* [Kerndat Feature Detection](kerndat.md)
