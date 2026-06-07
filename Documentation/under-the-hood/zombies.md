# Zombie Processes

CRIU supports checkpointing and restoring **zombie processes** (tasks that have terminated but have not yet been reaped by their parent). These processes are a vital part of a process tree's state, as they maintain exit codes that the parent may eventually need to read.

## How CRIU Handles Zombies

Zombie processes are unique because they have no active memory, no file descriptors, and no CPU state. However, they still occupy an entry in the kernel's process table and maintain an identity via their PID.

### 1. Checkpointing (Dumping)
During the dump phase, CRIU identifies zombie tasks by checking their state in `/proc/$pid/stat`.
*   **State Capture**: CRIU records the zombie's PID and its original **exit code**.
*   **Minimal Footprint**: Because zombies have no address space, CRIU does not attempt to inject parasite code or dump memory for them.

### 2. Restoration
Restoring a zombie process involves recreating a task and immediately forcing it into a terminated state without allowing its parent to reap it.

*   **The Helper Technique**: CRIU forks a new process using the original PID (via `clone3` or `ns_last_pid`).
*   **Immediate Termination**: This process immediately calls the `exit()` system call with the captured exit code.
*   **Parent Coordination**: The parent process of the zombie (which is also being restored) is managed to ensure it does not accidentally reap the new zombie before the restoration is complete.
*   **Result**: This leaves the new process in the zombie state, perfectly matching the original environment's PID table.

## See also
* [Process Tree Final States](final-states.md)
* [PID Restoration](pid-restore.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
