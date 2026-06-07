# Userfaultfd and Lazy Migration

**Userfaultfd** is a powerful Linux kernel feature that allows a userspace process (a "monitor") to handle page faults for other processes. CRIU leverages this feature to implement **Lazy Migration**, which significantly reduces the initial downtime when migrating memory-intensive applications.

## Lazy Migration Overview

In a traditional migration, the destination host must receive the entire memory dump (potentially many gigabytes) before the application can resume. This "freeze time" can be several seconds or even minutes for large applications.

With **Lazy Migration**:
1.  CRIU captures only the minimal process state (registers, file descriptors, etc.) and essential memory pages.
2.  The process tree is resumed immediately on the destination host with most of its memory regions mapped but empty.
3.  Memory pages are transferred from the source host only when the application actually tries to access them ("on-demand").

## How it Works: The Lazy Pages Daemon

CRIU implements lazy migration through a dedicated background process called the **Lazy Pages Daemon**.

### 1. The Handover
During the restoration process, each process in the tree:
*   Opens a `userfaultfd` file descriptor.
*   Registers its memory regions with the kernel for tracking.
*   Sends the descriptor to the Lazy Pages Daemon via a Unix domain socket using the `SCM_RIGHTS` mechanism.
*   Resumes execution of the application code via `sigreturn`.

### 2. Handling Page Faults
When the application accesses a page that hasn't been loaded yet, the kernel pauses the faulting thread and sends a message to the Lazy Pages Daemon.
1.  **Notification**: The daemon receives a `UFFD_EVENT_PAGEFAULT` message containing the faulting address.
2.  **Retrieval**: The daemon fetches the required page contents, either from the local `pages.img` images or from a remote **Page Server** on the source host.
3.  **Injection**: The daemon uses the `UFFDIO_COPY` (to fill data) or `UFFDIO_ZEROPAGE` (to fill zeros) ioctl to inject the page into the application's address space.
4.  **Resumption**: Once the kernel confirms the page is filled, it automatically resumes the paused thread.

## Advanced Features: Non-Cooperative UFFD

CRIU utilizes "non-cooperative" kernel features to maintain consistency if the application modifies its memory layout while being lazily restored:
*   **UFFD_FEATURE_EVENT_FORK**: If the process calls `fork()`, the kernel notifies the daemon, which then begins monitoring the new child process.
*   **UFFD_FEATURE_EVENT_REMAP**: If the process moves memory using `mremap()`, the daemon updates its internal mapping table to ensure it continues to fetch the correct data for the new addresses.
*   **UFFD_FEATURE_EVENT_UNMAP / REMOVE**: Handles scenarios where the application releases memory.

## Benefits and Trade-offs

*   **Reduced Downtime**: Applications resume in milliseconds, regardless of their total memory size.
*   **Network Jitter**: The application may experience minor stalls (latency spikes) during the initial phase as pages are fetched over the network.
*   **Source Dependency**: The source host and the Page Server must remain alive and connected until the entire memory state has been successfully transferred to the destination.

## See also
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
* [Page Server](page-server.md)
* [Kerndat Feature Detection](kerndat.md)
