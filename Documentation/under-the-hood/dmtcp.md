# DMTCP vs. CRIU

This article explains the fundamental differences between CRIU and DMTCP (Distributed MultiThreaded Checkpointing), focusing on their architectural approaches to process capture and restoration.

## Architectural Approach

### DMTCP: Library-Level Interception
DMTCP implements checkpoint/restore at the **userspace library level**. To use it, an application must be launched with the DMTCP library dynamically linked (`LD_PRELOAD`).
*   **Mechanism**: The library intercepts library calls (e.g., `glibc` wrappers for syscalls), builds an internal shadow database of the process state, and then forwards requests to the kernel.
*   **Implications**: This approach can introduce performance overhead due to proxying. Only applications compatible with the DMTCP library can be reliably dumped. Furthermore, DMTCP may not support all kernel APIs; for instance, complex features like `inotify` or specific socket types may lack sufficient proxies.

### CRIU: Kernel-Level Integration
CRIU, by contrast, operates primarily from **outside the process** using standard kernel interfaces (extended where necessary for C/R).
*   **Mechanism**: CRIU uses tools like `ptrace`, `/proc`, and specialized system calls (e.g., `kcmp`, `map_files`) to transparently capture the process state without requiring pre-loaded libraries.
*   **Implications**: It can checkpoint and restore virtually any application, provided the kernel supports the required features. It requires a relatively modern kernel version but offers much deeper integration with system resources like namespaces and cgroups.

## PID Handling and Virtualization

Restoring a process tree often requires restoring specific Process IDs (PIDs).

*   **DMTCP "Fake" PIDs**: Because the kernel does not traditionally allow userspace to request a specific PID during `fork()`, DMTCP "fools" the application. It intercepts the `getpid()` call and returns a fake value that matches the original PID. This is highly dangerous, as the application may see inconsistent information in the `/proc` filesystem (where directories are named by the *real* PID).
*   **CRIU Real PIDs**: CRIU restores the **actual PID** of the process. It achieves this by using the `ns_last_pid` interface or the modern `clone3` system call with a specified PID. This ensures that the restored process has the exact same identity as the original, with no inconsistencies in `/proc` or other kernel interfaces.

## Summary

DMTCP is often easier to deploy on older kernels since it doesn't require specific kernel support, but it suffers from the inherent limitations and risks of userspace interception. CRIU is the more robust and transparent solution for modern Linux systems, offering faithful restoration of the entire process environment.
