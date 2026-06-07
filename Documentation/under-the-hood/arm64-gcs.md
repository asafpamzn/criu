# ARM64 Guarded Control Stack (GCS)

CRIU supports checkpointing and restoring the **Guarded Control Stack (GCS)** feature on ARM64 (AArch64) architectures. GCS is a hardware-assisted shadow stack mechanism designed to prevent return-oriented programming (ROP) attacks by maintaining a protected stack of return addresses.

## How CRIU Handles GCS

GCS support is integrated into CRIU's architecture-specific code for AArch64 (`arch/aarch64/gcs.c`).

### 1. Checkpointing (Dumping)
During the dump phase, CRIU detects if a task has GCS enabled by checking its CPU features and hardware capabilities (`HWCAP_GCS`).
*   **State Capture**: CRIU uses `ptrace(PTRACE_GETREGSET, ..., NT_ARM_GCS, ...)` to retrieve the current GCS state.
*   **Key Parameters**:
    *   `gcspr_el0`: The current Guarded Control Stack Pointer.
    *   `features_enabled`: The GCS configuration flags (e.g., `PR_SHADOW_STACK_ENABLE`).
*   **VMA Identification**: CRIU identifies the memory region (VMA) used for the shadow stack, which is marked with special kernel attributes.

### 2. Restoration
Restoring GCS requires carefully re-establishing the shadow stack before the process resumes normal execution.
*   **Shadow Stack Mapping**: CRIU uses the `map_shadow_stack` system call to recreate the shadow stack at its original virtual address.
*   **Context Setup**: The captured GCS state (`gcspr_el0` and flags) is integrated into the task's **restorer context**.
*   **Sigframe Integration**: To ensure a seamless transition, CRIU places a `gcs_context` entry into the signal frame used for the final `sigreturn`. This informs the kernel to switch to the restored shadow stack as the process resumes.

## Kernel Requirements

GCS support in CRIU requires an ARM64 host and a kernel that supports the Guarded Control Stack ABI, typically including:
*   `PR_SHADOW_STACK_ENABLE` prctl support.
*   The `map_shadow_stack` system call.
*   `NT_ARM_GCS` ptrace regset.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Restorer Context](restorer-context.md)
