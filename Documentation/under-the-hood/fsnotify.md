# FSNotify (Inotify and Fanotify)

CRIU supports checkpointing and restoring `inotify` and `fanotify` instances. These mechanisms allow applications to monitor filesystem events (like file creation, modification, or deletion).

## The Challenges of FSNotify C/R

Restoring an fsnotify instance is inherently difficult because the kernel does not provide a direct way to retrieve the original path of a watched object (the "watchee"). Furthermore, the event queues themselves pose consistency risks.

### 1. Identifying the Watchee
When an application adds a watch (via `inotify_add_watch`), the kernel associates the watch with an **inode**, but it does not store the **path** used to create it. To restore the watch, CRIU must find a valid path to that specific inode.
*   **Open by Handle**: CRIU first attempts to use `open_by_handle_at()`. If the filesystem supports file handles, CRIU captures the handle during the dump and uses it to re-open the inode during restoration without needing the original path.
*   **Irmap (Inode Reverse Mapping)**: If file handles are unavailable, CRIU uses the [Irmap](irmap.md) engine to scan the filesystem and find a path that leads to the target inode.

### 2. Event Queue Consistency
If there are pending events in the fsnotify queue at the time of the dump, CRIU cannot currently "peek" at them or safely migrate them.
*   **Dropped Events**: During a dump, CRIU checks if the fsnotify file descriptor has data. If it does, CRIU emits a warning: `The ... inotify events will be dropped`. These events are lost, and the application must be prepared to handle this gap in its event stream.
*   **Spurious Events**: The process of checkpointing and restoring itself can trigger new filesystem events. For example, creating or deleting **ghost files** (temporary files used to restore unlinked but open files) can generate `IN_CREATE` or `IN_DELETE` events that the application will see upon resumption.

### 3. Ghost Files and Circular Dependencies
A "ghost file" is a file that was deleted by the application but is still held open. During restoration, CRIU must recreate these files. This action itself generates notify events, potentially confusing applications that monitor the directories where these ghost files are temporarily placed.

## Support for Fanotify

CRIU also supports `fanotify`, including:
*   **Inode Marks**: Similar to inotify, these target specific files or directories.
*   **Mount Marks**: Fanotify can monitor entire mount points. CRIU identifies the mount ID and restores the mark on the corresponding mount in the restored namespace.

## Current Strategy: "Chopping the Knot"

Due to the complexity of perfectly migrating event queues, CRIU's current strategy is:
1.  **Warn and Drop**: Acknowledge that pending events are lost.
2.  **Restore the Watches**: Ensure the application continues to receive *new* events after restoration.
3.  **Namespace Integration**: Correctly map mount-level fanotify marks within their respective mount namespaces.

## See also
* [Irmap](irmap.md)
* [Invisible Files](invisible-files.md)
* [Mount Points](mount-points.md)
