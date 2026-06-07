# Process Tree Final States

This document describes the possible states a process tree can end up in after a successful CRIU **dump** or **restore** operation.

## Supported Final States

CRIU supports three primary final states for the process tree:

1.  **Running (`TASK_ALIVE`)**: The processes continue execution as normal.
2.  **Stopped (`TASK_STOPPED`)**: The processes are left in a stopped state (equivalent to receiving `SIGSTOP`).
3.  **Dead (`TASK_DEAD`)**: The processes are terminated (equivalent to receiving `SIGKILL`).

## Controlling the Final State

You can specify the desired final state using the following command-line options:

*   `--leave-running`: Forces the process tree to continue running after the operation.
*   `--leave-stopped`: Forces the process tree to remain stopped after the operation.

### Default Behavior for `criu dump`

By default, `criu dump` terminates the process tree (**Dead**).

**Rationale**: Leaving a process tree running after a full dump is risky. If the processes continue to run, they will likely modify the filesystem, network state, or shared memory. These changes can make the captured image inconsistent or impossible to restore later, as the system state will no longer match the process's internal state at the moment of the dump.

*   **Exceptions**: The `pre-dump` command always enforces the **Running** state, as its purpose is to capture memory changes while the application continues to operate.

### Default Behavior for `criu restore`

By default, `criu restore` resumes the process tree (**Running**).

**Rationale**: The primary goal of restoration is typically to resume the application's work immediately.

*   **Debugging**: Using `--leave-stopped` during restoration can be extremely useful for debugging. It allows you to inspect the restored process tree (e.g., via `/proc` or a debugger) before it begins executing any code.

## Resuming a Stopped Tree

If a process tree was left in the **Stopped** state (either by dump or restore), you can resume its execution by sending a `SIGCONT` signal to all processes in the tree.

For complex process trees, the [pstree_cont.py](https://github.com/checkpoint-restore/criu-scripts/blob/master/pstree_cont.py) script (available in the `criu-scripts` repository) can be used to safely resume the entire hierarchy by targeting the root PID.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Freezing the Tree](freezing-the-tree.md)
