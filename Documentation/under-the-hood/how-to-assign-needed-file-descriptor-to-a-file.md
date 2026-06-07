# Assigning Descriptors and Sharing Files

Once a file is [opened during restoration](how-hard-is-it-to-open-a-file.md), it often needs to be moved to a specific numeric file descriptor (FD) and potentially shared with other tasks in the process tree. This document explains how CRIU coordinates this process.

## The Basic Mechanism: `dup2`

In Linux, the `dup2(oldfd, newfd)` system call is the standard way to assign a file to a specific descriptor number. CRIU uses this to move a newly opened file from its temporary descriptor (assigned by the kernel) to the target descriptor expected by the application.

```c
int fd = open_a_file(f->file);
dup2(fd, f->target_fd);
close(fd);
```

## Handling Multiple Descriptors for One File

A single task may have multiple FDs referring to the same kernel "File Description" (e.g., a shell where FD 0, 1, and 2 all point to the same TTY). CRIU handles this by identifying the unique file object, opening it once, and then calling `dup2()` for every target FD slot the application expects.

## Sharing Files Across the Process Tree

Files are frequently shared between processes. While these files were originally inherited via `fork()`, CRIU must often distribute them between processes that do not have a direct parent-child relationship during the restore phase.

### Master and Slave Descriptors
For every unique file object in a checkpoint:
1.  **The Master**: One task is designated as the "master" for that file. It is responsible for the actual system call that recreates the object (e.g., `open()`, `socket()`, or `pipe()`).
2.  **The Slaves**: All other tasks that share the same file are "slaves." They do not create the file themselves.

### Transport via SCM_RIGHTS
CRIU uses Unix domain sockets to "send" descriptors from the master process to slave processes using the `SCM_RIGHTS` mechanism.

**The Workflow:**
1.  **Master Opens**: The master task creates the file object.
2.  **Master Sends**: The master sends the resulting file descriptor to each slave task over a dedicated transport socket.
3.  **Slave Receives**: The slave task waits on its transport socket, receives the FD, and uses `dup2()` to plant it into the correct numeric slot.

## Solving the Coordination Problem

Distributing thousands of descriptors across a complex process tree requires careful management to avoid deadlocks and descriptor collisions.

### 1. Transport Sockets
CRIU creates abstract Unix sockets for each process to receive descriptors. The names are uniquely generated using the PID and a `criu_run_id` (e.g., `\0x/crtools-fd-123-abcdef`) to ensure that multiple simultaneous CRIU runs on the same host do not interfere with each other.

### 2. Deterministic "Master" Selection
To prevent circular dependencies (e.g., Task A waiting for Task B while B waits for A), CRIU uses a deterministic priority system to select the master. Typically, the task with the highest priority—usually the one closest to the root of the tree or with the lowest PID—is chosen to open and distribute the file.

### 3. Descriptor Collisions
A task's target FDs may conflict with the internal "service" FDs CRIU uses for images, logs, or transport sockets. CRIU resolves this by:
*   **Service FD Range**: Restricting CRIU's own FDs to a specific range.
*   **Dynamic Relocation**: If a target FD slot is occupied by an active service FD, CRIU moves the service FD to a new, free slot using `dup()` before planting the application's FD.

## Complex Dependencies

Some file types have inherent dependencies. For instance, an `epoll` descriptor cannot be fully restored until the files it monitors are already opened and their numeric descriptors are known. CRIU's file restoration engine handles this via a multi-pass state machine, where some files are opened but their full restoration is deferred until their dependencies are satisfied.

*See also: [File Restoration Engine (fdinfo)](fdinfo-engine.md)*
