# TUN/TAP Interface Support

CRIU supports checkpointing and restoring Linux TUN/TAP virtual network interfaces. These devices are frequently used in VPN clients, virtualization platforms (like QEMU), and container networking.

## How CRIU Handles TUN/TAP

CRIU manages TUN/TAP as a combination of a **Network Interface** (the link visible to the kernel) and a **File Descriptor** (the handle used by the application to send and receive packets).

### 1. Checkpointing (Dumping)
During a dump, CRIU identifies TUN/TAP descriptors and collects their full kernel state:
*   **Device Attributes**: The interface name, type (TUN for L3 or TAP for L2), and operational flags (e.g., `IFF_NO_PI`, `IFF_VNET_HDR`).
*   **Persistency**: Whether the device is persistent (`IFF_PERSIST`), meaning it survives even when no process has it open.
*   **Buffer Sizes**: Captures the send buffer size (`TUNGETSNDBUF`) and the virtual network header size (`TUNGETVNETHDRSZ`).
*   **Multi-Queue State**: CRIU identifies if multiple file descriptors are attached to different queues of the same TUN/TAP device, allowing for parallel I/O.
*   **Ownership**: Captures the UID and GID associated with the TUN/TAP device.

### 2. Restoration
To recreate the TUN/TAP environment exactly as it was, CRIU performs the following:
*   **Interface Creation**: Recreates the virtual link or attaches to an existing persistent one using `TUNSETIFF`.
*   **Index Preservation**: Uses the `TUNSETIFINDEX` ioctl to ensure the restored interface has the exact same numeric index as the original. This is critical for applications that have cached the interface index.
*   **Queue Re-attachment**: For multi-queue devices, CRIU uses `TUNSETIFF` in combination with `TUNSETQUEUE` to correctly re-link each restored file descriptor to its original queue.
*   **State Application**: Restores the original buffer sizes, ownership, and persistent status. If a device was not originally persistent, CRIU explicitly drops the persistency after the application has attached to it.

## Current Limitations

*   **Packet Filters (BPF)**: Capturing a TAP interface with a complex BPF filter attached is currently **not supported**. The kernel does not provide a robust way to extract the filter program and re-attach it during restoration without the original application context.
*   **In-flight Packets**: Data currently residing in the kernel's internal TUN/TAP queues (packets sent by the application but not yet processed by the virtual device, or vice versa) is not preserved across a checkpoint.

## See also
* [Network Sockets](sockets.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Kerndat Feature Detection](kerndat.md)
