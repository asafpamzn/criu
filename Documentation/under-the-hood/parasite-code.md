# Parasite Code Injection and Execution

The **parasite code** is a specialized binary blob that CRIU injects into the address space of a target process during a checkpoint. Its primary purpose is to extract internal task state—such as private memory contents, credentials, and signal handlers—that is not available via standard kernel interfaces like `/proc`.

## The Infection Process

Infection is a multi-stage operation managed by the **Compel** sub-project, leveraging the `ptrace` system call to take control of the target process.

### 1. Seizing the Task
CRIU stops the target task using `PTRACE_SEIZE` followed by `PTRACE_INTERRUPT`. This ensures a non-disruptive stop without delivering signals to the application, maintaining transparency.

### 2. Bootstrap Payload
CRIU identifies the task's current instruction pointer (`RIP`/`PC`) and uses `PTRACE_POKEDATA` to temporarily inject a small bootstrap payload. This payload is typically designed to execute a system call (such as `mmap` or `memfd_create`) to allocate a dedicated memory region for the full parasite blob.

### 3. Memory Exchange Optimization
To maximize efficiency and avoid thousands of slow `ptrace` calls, CRIU uses a **memory exchange** technique:
*   The parasite's memory region is often backed by a file descriptor (e.g., `memfd`).
*   CRIU maps this same file descriptor into its own address space.
*   This allows the CRIU coordinator to write the parasite code, Global Offset Table (GOT), and arguments directly into the target's memory at local memory speeds.

### 4. Relocation and GOT Patching
Since the parasite is a Position-Independent Executable (PIE), CRIU must patch its GOT table with the actual addresses where the blob was mapped in the target process's address space.

### 5. Starting the Daemon
CRIU sets the task's instruction pointer to the entry point of the parasite and resumes execution using `PTRACE_CONT`. The parasite initializes its own stack, sets up signal handling for its own internal use, and enters **daemon mode**.

## Execution and Communication

The parasite runs as a daemon within the target task's context, communicating with the main CRIU process via a Unix domain socket.

### Control Loop
The parasite enters a loop where it waits for commands from the CRIU coordinator. Each command follows a **Request-Response** pattern:
1.  **Request**: CRIU sends a command ID (e.g., `PARASITE_CMD_DUMP_PAGES`) and any necessary arguments through the socket.
2.  **Execution**: The parasite executes the requested action within the task's context (e.g., calling `vmsplice` on its own memory).
3.  **Response**: The parasite sends an acknowledgment (ACK) and optional data back to CRIU.

### Supported Actions
*   **Memory Dumping**: Efficiently transfers memory pages to CRIU using the `vmsplice()` system call.
*   **Credential Extraction**: Captures UIDs, GIDs, and capability sets.
*   **Timer and Signal State**: Reads interval timers and signal action tables that are not visible through `/proc`.
*   **Thread Coordination**: In multi-threaded processes, the parasite coordinates state collection across all threads.

## Cleanup and Cure

Once the state capture is complete, CRIU performs a "cure" operation to return the process to its original state:
1.  CRIU sends the `PARASITE_CMD_FINI` command to the daemon.
2.  The parasite unmaps its allocated memory and prepares to exit.
3.  CRIU restores the original register state (including the instruction pointer) and the original code bytes that were overwritten during the bootstrap phase.
4.  CRIU detaches from the task, allowing it to resume normal operation or terminating it as requested.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Code Blobs](code-blobs.md)
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
