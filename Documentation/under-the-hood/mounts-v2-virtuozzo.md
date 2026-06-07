# Mount V2: Detailed Algorithm

The Mount V2 engine (originally developed by Virtuozzo and later merged into upstream CRIU) is designed to resolve complex issues with restoring sharing groups, over-mounted files, and cross-namespace bind mounts. This document provides a technical breakdown of its operation.

## 1. Mount Image Processing Stage

During initialization, CRIU processes the mount images for all namespaces to build an internal model of the filesystem state:
- **Hierarchy Construction**: Build a per-namespace mount tree based on parent IDs.
- **Bind Grouping**: Group mounts by superblock equality into "bind" lists to identify shared underlying filesystems.
- **Sharing Groups**: Organize shared and slave groups into a tree structure (e.g., where a parent's `shared_id` matches a child's `master_id`).
- **The Root Yard**: Create a helper mount (`root_yard_mp`) at a temporary location (e.g., `/tmp/.criu.mntns.XXXXXX/`). All mount trees from all namespaces are initially merged as subdirectories of this "root yard."

## 2. Pre-Fork Mounting Stage

This stage is executed from the init task in a dedicated "service" mount namespace before the target process tree is forked:
1.  **Plain Mounting**: CRIU walks the merged mount tree and creates all mounts in a "plain" (unattached) and "private" state.
2.  **Source Resolution**: For each mount, CRIU identifies its source (a real filesystem, a bind mount from another already-mounted superblock, or an external source).
3.  **Cross-Namespace Handling**: By maintaining all mounts within a single service namespace during this stage, CRIU can easily handle bind mounts that cross namespace boundaries.

## 3. Propagation and Shared Group Restoration

CRIU restores complex propagation relationships using modern kernel APIs:
- **Slavery and Sharing**: For each sharing group, CRIU identifies the "master" mount. It uses the `move_mount()` system call with the `MOVE_MOUNT_SET_GROUP` flag (or the legacy `MS_SET_GROUP` mechanism) to establish slave/shared relationships precisely as they existed during the dump.
- **Settings Replication**: Once the sharing state is established for the primary mount in a group, all other members of the group inherit these settings.

## 4. Namespace Transition and Final Positioning

For each target mount namespace being restored:
1.  **Unshare**: CRIU calls `unshare(CLONE_NEWNS)` to create a fresh, empty mount namespace.
2.  **Tree Positioning**: Move the "plain" mounts from the root yard into their final hierarchical positions within the new namespace using `move_mount()`.
3.  **Pivot Root**: Execute `pivot_root()` to switch to the new namespace root, effectively hiding the temporary "yard" and finalizing the mount hierarchy.

## 5. Post-Fork Fixups

Certain mounts cannot be fully restored until the process tree is established:
- **Delayed Procfs**: `proc` mounts for nested PID namespaces must wait until the target PID namespace is created. CRIU enters these namespaces after forking to perform the final mounts.
- **Internal Yards**: In some cases, temporary `tmpfs` mounts ("internal yards") are used within a namespace to hold mounts that must be moved or adjusted after the process tree is fully alive.

## See also
* [Mount V2 Overview](mount-v2.md)
* [Mount Points](mount-points.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
