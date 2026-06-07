# Foundational Technologies

CRIU relies on a wide array of advanced Linux kernel features and userspace libraries to perform transparent checkpoint and restore.

## Kernel Technologies

### Core C/R Capabilities
*   **kcmp()**: A system call used to identify shared resources (files, memory mappings, namespaces) between processes by performing internal kernel pointer comparisons.
*   **clone3()**: A modern process creation interface that allows CRIU to atomically request specific PIDs and TIDs, even across nested PID namespaces.
*   **prctl() Extensions**:
    *   `PR_SET_MM`: Allows the restorer to reconstruct a process's original memory layout (code, data, heap, etc.).
    *   `PR_GET_TID_ADDRESS`: Captures the address used for `set_tid_address`.
    *   `PR_SET_THP_DISABLE`: Preserves the status of Transparent Huge Pages.
*   **ptrace() Extensions**:
    *   `PTRACE_SEIZE` & `PTRACE_INTERRUPT`: Enables non-disruptive task stopping.
    *   `PTRACE_GETSIGMASK` & `PTRACE_SETSIGMASK`: Captures and restores thread signal masks.
    *   `PTRACE_PEEKSIGINFO`: Reads pending signal queues without delivering them.
    *   `PTRACE_GET_RSEQ_CONF`: Retrieves Restartable Sequences (rseq) registration details.

### Resource Introspection
*   **/proc Filesystem**:
    *   `/proc/$pid/map_files`: Provides stable handles to files mapped into a process's memory.
    *   `/proc/$pid/fdinfo`: Exposes internal state for file descriptors, including positions, flags, and socket handles.
    *   `ioctl(PAGEMAP_SCAN)`: Efficiently identifies dirty and present pages in large address spaces.
*   **sock_diag**: A netlink-based interface used to retrieve detailed protocol-level state for sockets (TCP, UDP, Unix, etc.).

### Advanced Subsystems
*   **TCP Repair Mode**: A specialized socket state that allows CRIU to capture and restore the full internal state of TCP connections without sending network packets.
*   **Userfaultfd**: Enables **Lazy Migration** by allowing CRIU to handle page faults in userspace and load memory pages on-demand.
*   **Mount V2 APIs**: Uses `fsopen()`, `fsmount()`, `open_tree()`, and `move_mount()` to robustly reconstruct complex filesystem hierarchies and propagation groups.
*   **Netfilter (nftables/iptables)**: Used to "lock" network connections during migration to prevent state changes.

## Userspace Technologies

### Compel
[Compel](../compel.md) is a dedicated sub-project that provides the infrastructure for **Parasite Injection**. It allows CRIU to execute self-contained code (PIE) within the context of a target process to capture internal state.

### Google Protocol Buffers (protobuf)
CRIU uses [Protocol Buffers](https://developers.google.com/protocol-buffers/) as the standard serialization format for all image files. This ensures a structured, extensible, and cross-version compatible way to store process state.

### ZDTM (Zero-Downtime Migration)
[ZDTM](zdtm-test-suite.md) is CRIU's comprehensive test suite. It includes hundreds of tests that verify the functional correctness of C/R across various architectures and kernel versions.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Memory Changes Tracking](memory-changes-tracking.md)
* [Mount V2](mount-v2.md)
* [PID Restoration](pid-restore.md)
