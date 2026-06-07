# vDSO and VVAR Handling

The **vDSO** (virtual Dynamic Shared Object) and **VVAR** (virtual VARiable) areas are specialized memory regions mapped by the Linux kernel into every process. They enable high-performance userspace execution of specific system calls (such as `gettimeofday()` or `clock_gettime()`) by providing direct access to kernel-maintained code and data without the overhead of a full context switch.

## The Challenge of C/R

The vDSO is uniquely challenging for checkpoint/restore because its contents and memory layout are determined by the **host kernel**.
1.  **Address Dependencies**: Applications frequently cache the addresses of vDSO functions. These must remain identical after restoration.
2.  **ABI and Kernel Compatibility**: If a process is migrated to a different kernel version, the vDSO code from the original host might be incompatible with the new host's internal kernel-to-userspace data interfaces.

## CRIU's Restoration Strategy

CRIU uses two primary strategies to handle vDSO migration, automatically selecting the best one based on kernel capabilities detected during the [Kerndat](kerndat.md) phase.

### 1. The Proxy (Patching) Method
This is the fallback approach used when the kernel does not support mapping the vDSO at an arbitrary address:
*   **Checkpoint**: CRIU captures the original vDSO contents and parses its ELF symbol table to identify the offsets of essential functions (e.g., `__vdso_gettimeofday`, `__vdso_time`).
*   **Restoration**:
    1.  CRIU maps the original vDSO binary at its original virtual address.
    2.  It identifies the **new vDSO** provided by the current host kernel.
    3.  For each essential symbol, CRIU locates the corresponding function in the new vDSO.
    4.  CRIU **patches** the code in the original vDSO with a "trampoline" (a small jump instruction) that redirects execution to the equivalent function in the new host's vDSO.
*   **Result**: The application continues to call the memory addresses it originally linked against, but it transparently executes the code optimized for the current host kernel.

### 2. The `arch_prctl` Method (Modern)
On modern kernels (v4.18+ for x86_64), CRIU uses a significantly more efficient mechanism:
*   CRIU uses the `arch_prctl()` system call with the `ARCH_MAP_VDSO_64` (or `ARCH_MAP_VDSO_32`) flag to instruct the kernel to map its **current, native** vDSO directly at the application's original virtual address.
*   **Advantage**: This eliminates the complexity of ELF patching and ensures the application always uses the most optimal, native code path for the host kernel.

## VVAR Handling

The **VVAR** area contains the raw data (such as the current clock value) that the vDSO code reads. 
*   VVAR is a data-only region and is not executable.
*   CRIU identifies the VVAR mapping during the dump and ensures it is correctly re-established on the destination host, usually adjacent to the restored vDSO.
*   When using the `arch_prctl` method, the kernel automatically manages the associated VVAR mapping when the vDSO is moved.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Kerndat Feature Detection](kerndat.md)
* [Restorer Context](restorer-context.md)
