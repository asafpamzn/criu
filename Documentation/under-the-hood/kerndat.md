# Kerndat (Kernel Data)

**Kerndat** is a CRIU module responsible for detecting the capabilities and features of the currently running Linux kernel. Since CRIU's functionality depends heavily on specific kernel system calls and behaviors, runtime detection is essential for ensuring compatibility and selecting the most efficient algorithms.

## Feature Detection

CRIU performs a wide array of checks during initialization. These include:
*   **System Call Availability**: Checking for `kcmp()`, `userfaultfd()`, `memfd_create()`, `clone3()`, `openat2()`, `membarrier()`, and more.
*   **Filesystem Features**: Verifying `pagemap` functionality, `PAGEMAP_SCAN` support, and anonymous shared mapping behaviors.
*   **Namespace Support**: Detecting Time namespaces, CGroup namespaces, and namespace-specific identifiers.
*   **Architecture-Specific Quirks**: Identifying known CPU bugs or features, such as the x86 FPU/XSAVE ptrace bug.

The results of these checks are stored in a global `kdat` structure, which other CRIU modules query to determine how to proceed during dump and restore operations.

## Persistent Caching

Executing hundreds of kernel feature checks can be time-consuming. To speed up subsequent CRIU invocations, the results are cached on disk.

*   **Cache Location**:
    *   **Root**: `/run/criu.kdat` (typically stored on `tmpfs` to ensure it is cleared on reboot).
    *   **Non-root**: `$XDG_RUNTIME_DIR/criu.kdat`.
*   **Lifecycle**: CRIU attempts to load this cache during `kerndat_init()`. If the cache is missing or stale (e.g., if the CRIU binary has been updated with new checks), CRIU performs a full detection and saves the new results back to the cache file.

## Kerndat vs. Inventory

It is important to distinguish between **kerndat** and the **inventory image** (`inventory.img`):
*   **Kerndat**: Captures the capabilities of the **host kernel**. It is system-wide and typically survives across different CRIU operations on the same host.
*   **Inventory**: Captures critical metadata about a **specific checkpoint**. It is stored within the images directory and includes the CRIU version used for the dump, the host's LSM type (SELinux/AppArmor), and the root task's original IDs.

## Inspection

To see the features detected by CRIU on your current system, use the check command:
```bash
criu check --extra
```
This command triggers a kerndat initialization and prints the status of various required and optional kernel features, allowing you to verify that your environment is ready for CRIU.
