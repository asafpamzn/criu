# Optimized Pre-dump Algorithm

Pre-dumping is the process of capturing dirty memory pages while an application continues to run, aiming to minimize the final "freeze time" during live migration. CRIU provides two primary modes for pre-dumping: `read` and `splice`.

## Traditional vs. Optimized Pre-dump

### The `read` Mode (Traditional)
In this mode, CRIU uses the `process_vm_readv()` system call to read memory from the target process. 
*   **Workflow**: Tasks are briefly frozen to identify dirty pages and reset the soft-dirty bit, then resumed. CRIU then reads the pages from the running process's address space.
*   **Challenge**: Reading memory while a process is running can lead to minor inconsistencies if the process modifies a page *while* it is being read. Furthermore, `process_vm_readv()` requires the target process to be alive and its memory mappings to remain stable during the read.

### The `splice` Mode (Optimized & Default)
The `splice` mode (enabled via `--pre-dump-mode=splice`) uses a zero-copy "gift" mechanism to further reduce freeze time and improve reliability.

#### How `splice` Mode Works:
1.  **Brief Freeze**: CRIU seizes the tasks and injects the parasite code.
2.  **vmsplice "Gifting"**: The parasite identifies dirty pages and calls `vmsplice()` with the `SPLICE_F_GIFT` flag. This flag tells the kernel that the process is "giving" these pages to a pipe.
3.  **Immediate Resume**: Once the `vmsplice()` calls are complete (which is extremely fast as no data is actually copied), the parasite is removed, and the tasks are resumed immediately.
4.  **Parallel Draining**: While the tasks are running, the main CRIU process "drains" the data from the pipes and writes it to the image files or sends it to the page server.

#### Why `splice` is Better:
*   **Consistency via COW**: The `SPLICE_F_GIFT` flag ensures that if the process modifies a "gifted" page after resuming, the kernel performs a **Copy-on-Write (COW)**. The pipe buffer continues to hold the *original* version of the page as it existed at the moment of the `vmsplice()` call, ensuring a perfectly consistent snapshot of that page.
*   **Minimized Downtime**: The "freeze" duration is reduced to just the time needed for the parasite to execute the `vmsplice()` system calls, rather than the time needed to transfer gigabytes of memory data over the network or to disk.

## Usage

The optimized `splice` mode is the default in modern CRIU. It can be explicitly requested using the `--pre-dump-mode` option:
```bash
criu pre-dump --pre-dump-mode splice ...
```

## See also
* [Memory Changes Tracking](memory-changes-tracking.md)
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
* [Iterative Migration](iterative-migration.md)
