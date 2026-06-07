# Mount V2: Advanced Mount Restoration

Introduced in CRIU v3.16, **Mount V2** is a sophisticated restoration engine that leverages modern Linux kernel APIs to handle complex mount hierarchies, propagation groups, and overmounts with high reliability.

## Why Mount V2 was Necessary

The original mount restoration mechanism (Mount V1) relied on sequential, path-based `mount()` calls. This approach had several critical flaws:
1.  **Overmount Sensitivity**: If a directory was already covered by another mount, performing a path-based mount on it could fail or target the wrong filesystem.
2.  **Circular Dependencies**: Resolving mounts that depend on each other in non-linear ways was difficult and often resulted in ordering failures.
3.  **Propagation Complexity**: Establishing `shared` and `slave` relationships required creating dummy mount points and performing specific sequences of `mount --make-shared/slave` calls, which was fragile in complex scenarios.

## How Mount V2 Works

Mount V2 moves away from path-based mounting, instead using **File Descriptor-based** mounting provided by newer kernel system calls.

### 1. Detached Mounts
CRIU creates each required mount as a **detached mount**. These mounts exist in the kernel but are not yet attached to any visible path in the filesystem.
*   **New Filesystems**: Created using `fsopen()` and `fsmount()`.
*   **Bind Mounts**: Created using `open_tree()` with the `OPEN_TREE_CLONE` flag to create an unattached clone of an existing path.

### 2. Precise Propagation Grouping
Using the `move_mount()` syscall with the `MOVE_MOUNT_SET_GROUP` flag (introduced in kernel v5.15), CRIU can explicitly assign a detached mount to a specific **shared or slave propagation group**. This eliminates the need for dummy mounts and ensures that the propagation state is perfectly restored as recorded in the images.

### 3. Tree Construction via File Descriptors
CRIU constructs the entire mount hierarchy by attaching child mounts to their parents using their respective file descriptors. Since this happens "off-line" (outside of any mount namespace), it is immune to path shadowing, path resolution errors, or overmounting issues.

### 4. Atomic Final Attachment
Once the complete hierarchy is assembled as a tree of detached mounts, CRIU performs a final `move_mount()` to attach the root of this reconstructed tree into the target mount namespace at the desired destination path.

## Kernel Requirements

Mount V2 requires a modern kernel that supports:
*   `fsopen()`, `fsmount()`, `move_mount()` (Kernel v5.2+)
*   `open_tree()` (Kernel v5.3+)
*   `MOVE_MOUNT_SET_GROUP` (Kernel v5.15+)

CRIU automatically detects these features during the [Kerndat](kerndat.md) phase. It will fall back to the older Mount V1 engine if these calls are unavailable, though many modern container layouts now effectively require Mount V2 for a successful restore.

## See also
* [Mount Points](mount-points.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Kerndat](kerndat.md)
