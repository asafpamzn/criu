# Comparison to Other Checkpoint/Restore Projects

This page explains the primary differences between CRIU and other checkpoint/restore (C/R) solutions available for Linux.

## DMTCP (Distributed MultiThreaded Checkpointing)

DMTCP implements checkpoint/restore at the library level. To use it, an application must be launched with the DMTCP library dynamically linked from the start. This library intercepts library calls, builds an internal shadow database of the process state, and forwards requests to `glibc` or the kernel.

**Key Characteristics of DMTCP:**
*   **No Kernel Patches**: Works on standard kernels without requiring specific features.
*   **Library Level**: Intercepts calls at the userspace level, which can introduce performance overhead.
*   **PID Virtualization**: Since the kernel does not traditionally allow setting a specific PID during fork, DMTCP "fools" the application by intercepting `getpid()` and returning a fake value. This can be problematic if the application accesses `/proc` using its real PID.
*   **Limited API Coverage**: May not support all kernel APIs (e.g., `inotify` support is limited).

In contrast, **CRIU** does not require pre-loading libraries. It uses standard kernel interfaces (extended where necessary for C/R) to transparently capture and restore arbitrary applications.

## BLCR (Berkeley Lab Checkpoint/Restart)

BLCR is a system-level checkpointer designed primarily for High Performance Computing (HPC) and MPI jobs. It is implemented as a loadable kernel module.

**Key Characteristics of BLCR:**
*   **Kernel Module**: Requires a specific GPL-licensed kernel module.
*   **HPC Focused**: Optimized for CPU and memory-intensive batch jobs.
*   **Limited Scope**: Traditionally lacks support for complex modern features like namespaces, containers, or diverse socket types.

## PinPlay

PinPlay is a checkpointing tool built on top of Intel's PIN binary instrumentation tool. It is primarily used for deterministic replay and architectural simulation. It records architectural register state and memory pages, often focusing on reducing runtime for simulators.

## OpenVZ (In-Kernel)

Legacy OpenVZ (RHEL6 and earlier) featured an in-kernel C/R implementation. While highly efficient and robust for its time, it required a heavily patched kernel. CRIU was developed as the "user-space" successor to this technology, moving the logic out of the kernel to improve maintainability and facilitate upstream adoption.

---

## Comparison Table

| Feature | CRIU | DMTCP | BLCR | OpenVZ (Legacy) |
| :--- | :--- | :--- | :--- | :--- |
| **Architectures** | x86_64, ARM, AArch64, PPC64, s390, MIPS, RISC-V, LoongArch | x86, x86_64, ARM | x86, x86_64, PPC, ARM | x86, x86_64 |
| **OS** | Linux | Linux | Linux | Linux |
| **Standard Kernel?** | Yes (v3.11+) | Yes | Yes (needs module) | No (Custom kernel) |
| **No Preloading?** | Yes | No | No | Yes |
| **Non-Root Support?** | Yes (limited) | Yes | Yes | No |
| **Unmodified Apps?** | Yes | Yes | No (Static/Threaded issues) | Yes |
| **Unprepared Tasks?** | Yes | No | No | Yes |
| **Retains Behavior?** | Yes | No (Wrappers used) | No (Wrappers used) | Yes |
| **Live Migration** | Yes (Optimized) | Yes | Yes (Identical env only) | Yes |
| **Containers** | Yes (LXC, Docker, Podman) | No | No | Yes |
| **GDB Support** | No (same interface) | Yes | No | Yes |
| **Unix Sockets** | Yes | Yes | No | Yes |
| **TCP Sockets** | Yes | Yes | No | Yes |
| **Established TCP** | Yes | No (needs plugin) | No | Yes |
| **Namespaces** | Yes | No | No | Yes |
| **System V IPC** | Yes | Yes | No | Yes |
| **Non-POSIX Files** | Yes (Inotify, Epoll) | Yes | No | Yes |
| **Timers** | Yes | No | Yes | Yes |

## Sources and External Links

*   **DMTCP**: [dmtcp.sourceforge.net](http://dmtcp.sourceforge.net/)
*   **BLCR**: [ftg.lbl.gov/projects/CheckpointRestart](https://ftg.lbl.gov/projects/CheckpointRestart/)
*   **CRIU FAQ**: [How does DMTCP differ?](http://dmtcp.sourceforge.net/FAQ.html#Internals)
