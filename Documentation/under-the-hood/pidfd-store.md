# Pidfd Store: Reliable Process Identification

The **Pidfd Store** is an internal CRIU mechanism used during iterative migration to reliably identify processes across multiple pre-dump iterations. It leverages the Linux kernel's `pidfd` interface to eliminate the risks associated with PID reuse.

## The Problem: PID Reuse

In an iterative migration workflow, CRIU performs multiple `pre-dump` operations. Each iteration captures memory pages that have changed since the previous snapshot. To do this safely, CRIU must ensure that it is still talking to the *exact same process* it was in the previous iteration.

If a process dies between iterations and the kernel assigns its old PID to a new, unrelated process, a naive check based only on the PID would fail to detect this change. Performing an incremental dump on a new process using the state of an old one would lead to corrupted images and a failed restoration.

## How the Pidfd Store Works

The Pidfd Store allows CRIU to maintain a persistent, race-free handle for every process in the tree.

### 1. Capturing Pidfds
During the first pre-dump, CRIU calls `pidfd_open()` for every task it captures. Unlike a numeric PID, a **pidfd** is a file descriptor that refers to a specific process *instance*. If that process terminates, its pidfd becomes invalid and will never refer to a subsequent process, even if the numeric PID is reused.

### 2. Persistent Storage via "The Socket Trick"
CRIU often operates as a service, receiving commands via RPC. To keep pidfds alive between independent RPC calls, CRIU uses a clever "socket trick":
*   CRIU creates a Unix domain socket and connects it to itself.
*   It "sends" the captured pidfds into this socket using the `SCM_RIGHTS` mechanism.
*   The Linux kernel stores these file descriptors in the socket's internal buffer. Because the socket is connected to itself, the descriptors remain queued in the kernel until CRIU explicitly reads them back.

### 3. Identity Verification
In each subsequent `pre-dump` or the final `dump` command:
1.  CRIU "drains" the pidfds from its internal storage socket.
2.  It builds a hash table mapping PIDs to these stable pidfd handles.
3.  Before capturing state for a PID, CRIU verifies the task against the stored pidfd.
4.  If the pidfd is still valid, CRIU knows it is the same process and can safely perform an incremental memory dump.
5.  If the pidfd is invalid or missing, CRIU detects a **PID reuse** event and treats the process as entirely new, performing a full dump to maintain consistency.

## Kernel Requirements

The Pidfd Store requires modern kernel features (automatically detected via [Kerndat](kerndat.md)):
*   `pidfd_open()` (Kernel v5.3+)
*   `pidfd_getfd()` (Kernel v5.6+, used to transfer the storage socket between service components).

## See also
* [Memory Changes Tracking](memory-changes-tracking.md)
* [Iterative Migration](iterative-migration.md)
* [Kerndat](kerndat.md)
