# Asynchronous I/O (AIO)

CRIU supports checkpointing and restoring kernel-level Asynchronous I/O (AIO) contexts, which are managed via the `io_setup`, `io_submit`, `io_getevents`, and `io_destroy` system calls.

## How CRIU Handles AIO

To successfully checkpoint and restore an AIO context, CRIU manages three primary components:

1.  **The AIO Ring Buffer**: This is a memory-mapped area where the kernel and userspace communicate. CRIU identifies these areas by their `[aio]` label in `/proc/pid/maps` or by detecting the specific VMA attributes.
2.  **Completed Events**: Events that have finished and are already residing in the ring buffer are dumped as part of the process's memory.
3.  **AIO Context State**: This includes the kernel's internal tracking of the ring's head and tail.

### The Restoration Process

The restoration of an AIO ring is complex because the kernel's AIO context ID (the `aio_context_t` value) is an internal pointer that cannot be arbitrarily assigned by userspace. CRIU uses the following strategy to restore it:

1.  **New Ring Creation**: The restorer calls `io_setup` to create a fresh AIO ring with the original number of requested events.
2.  **Tail Synchronization**: To move the kernel's internal `tail` pointer to the original position, CRIU submits dummy I/O requests (typically writes to `/dev/null`). Since these operations are synchronous for the device, the kernel advances the tail as each request completes.
3.  **Head Synchronization**: CRIU manually adjusts the `head` pointer in the ring header to match the state at the time of the dump.
4.  **Event Data Restoration**: The original `io_events` data (the completed but unread events) is copied from the dump image into the new ring buffer.
5.  **Memory Remapping**: Finally, CRIU uses `mremap` to move the new ring buffer to its original virtual address, ensuring the application can continue using its existing AIO context ID.

## Limitations: In-Flight Events

Currently, **in-flight events** (I/O requests that have been submitted but not yet completed at the time of the dump) are **not supported**.

*   **Dumping**: CRIU's parasite code checks for AIO rings but does not currently wait for pending requests to complete. If a request completes during or after the dump, it may lead to data inconsistency or a failed restore.
*   **Restoring**: There is no mechanism to re-submit pending I/O requests upon restoration. Applications using AIO should ideally be in a quiescent state (all submitted I/O completed) before being checkpointed.

## See also

* [Memory dumping and restoring](memory-dumping-and-restoring.md)
