# Freezing the Process Tree

Before CRIU can begin checkpointing, it must ensure that the entire process tree is completely "immobilized." This prevents tasks from changing their state (e.g., opening files, creating children, or receiving network packets) while the snapshot is being taken. This freezing process must be transparent to the application, meaning it should not observe any disruption or unexpected signals.

CRIU employs two primary methods to achieve this:

## Capturing with ptrace

The most common method for freezing a tree is using the Linux `ptrace` interface. Unlike traditional debuggers that might send disruptive signals like `SIGSTOP`, CRIU uses a more modern, non-invasive approach:

1.  **SEIZE**: CRIU calls `ptrace(PTRACE_SEIZE, pid, ...)` for every task in the tree. This "attaches" to the process without stopping it or delivering any signals.
2.  **INTERRUPT**: Once seized, CRIU sends a `ptrace(PTRACE_INTERRUPT, pid, ...)` command. This causes the kernel to stop the task at the next possible opportunity (typically upon entering or exiting a syscall or being preempted).
3.  **WAIT**: CRIU then waits for the task to enter the `TRAP_STOP` state. This state is invisible to the task's own signal handling logic, ensuring transparency.

By seizing every task in the tree, CRIU ensures that no task can resume execution or fork new children during the dump.

## Using Freezer CGroups

For large process trees or environments where `ptrace` might be restricted or inefficient, CRIU can use the Linux **Freezer CGroup**. This allows the kernel to freeze an entire group of processes in a single, atomic operation.

CRIU supports both versions of the freezer:

### CGroup v1 Freezer
*   **Mechanism**: CRIU identifies the freezer cgroup containing the process tree and writes `FROZEN` to the `freezer.state` file.
*   **Handling Inconsistency**: Historically, the v1 freezer could be unreliable, sometimes getting stuck in a `FREEZING` state. CRIU includes "kludges" to handle this, such as periodically retrying the freeze command or briefly thawing and re-freezing the group to kick the kernel's internal state machine.

### CGroup v2 Freezer
*   **Mechanism**: In the unified cgroup v2 hierarchy, CRIU writes `1` to the `cgroup.freeze` file.
*   **Verification**: It then monitors the `cgroup.events` file, waiting for the `frozen 1` event to signal that all processes in the sub-hierarchy have successfully stopped.

**Note**: Even when using a freezer cgroup, CRIU still attaches to the tasks via `ptrace` after they are frozen. This is necessary to perform internal inspections, such as extracting register states and injecting parasite code.

## See also

* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Process Tree Final States](final-states.md)
* [Parasite Code](parasite-code.md)
