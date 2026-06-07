# CGroups

CRIU provides comprehensive support for checkpointing and restoring Control Groups (CGroups) for both cgroup v1 and cgroup v2.

## Overview

When managing CGroups, CRIU handles three main aspects:
1.  **Process Placement**: The specific cgroup sets (a list of controller/path pairs) that each task in the process tree belongs to.
2.  **Hierarchy and Properties**: The existing cgroup directory tree, its permissions, and various control properties (e.g., CPU shares, memory limits).
3.  **Namespace Boundaries**: Support for CGroup namespaces (`CLONE_NEWCGROUP`), ensuring that the restored tasks have the same view of the cgroup hierarchy.

## Default Behavior

By default, CRIU manages cgroups in **soft mode** (`--manage-cgroups=soft`). In this mode:
*   CRIU automatically dumps process cgroup memberships.
*   Upon restoration, it attempts to recreate the cgroup hierarchy and restore properties for cgroups that it created.
*   If a cgroup already exists, CRIU avoids overwriting its properties to prevent interference with other tasks on the system.

## CGroup V2 Support

CRIU fully supports the unified cgroup v2 hierarchy. Key features include:
*   **Global Properties**: Restoration of global v2 attributes such as `cgroup.subtree_control`, `cgroup.max.descendants`, and `cgroup.max.depth`.
*   **Process Migration**: Moving tasks between v2 cgroups using `cgroup.procs` (or `cgroup.threads` for threaded controllers).
*   **Freezer**: Integrated support for the cgroup v2 freezer mechanism (`cgroup.freeze`).

## CGroup Namespaces

CRIU leverages cgroup namespaces to accurately restore a container's view of the cgroup tree. During restoration:
1.  It identifies the cgroup namespace boundary (the path prefix) for each controller.
2.  It moves the root task into the appropriate cgroup relative to the host.
3.  It calls `unshare(CLONE_NEWCGROUP)` to pin the root of the cgroup namespace to that location, matching the original environment.

## Mountpoints of the "cgroup" Filesystem

CRIU supports dumping and restoring cgroup filesystem mountpoints. However, a significant limitation exists regarding bind-mounted subgroups:

**Root Mount Requirement**: By default, CRIU expects to find the "root" mount of a cgroup controller (where the mount root is `/`) within the dumped mount namespace.
*   If a container has only bind-mounted **subgroups** (e.g., `/sys/fs/cgroup/memory/my-container` is bind-mounted to `/sys/fs/cgroup/memory`) without a corresponding root mount of that controller being visible, CRIU may fail the dump.
*   This is because CRIU needs to identify the full path of the cgroup relative to the hierarchy root to accurately reconstruct it.

To overcome this, such mounts must often be treated as **external mounts** (`--external mnt[...]`) or the full hierarchy must be made visible to CRIU during the dump.

## CGroups Restoration Strategy

The `--manage-cgroups=MODE` option allows for fine-grained control:

*   `none`: Requires cgroups to pre-exist; does not restore properties.
*   `props`: Requires cgroups to pre-exist; restores properties from the image.
*   `soft` (Default): Restores properties only for cgroups created by CRIU.
*   `full`: Always recreates all cgroups and restores all properties.
*   `strict`: Recreates all cgroups from scratch; fails if any already exist.
*   `ignore`: Completely ignores cgroup information.

## External CGroup Yard

The `--cgroup-yard PATH` option allows CRIU to use a pre-mounted cgroup hierarchy located at `PATH`. This is particularly useful in unprivileged environments where CRIU may not have the `CAP_SYS_ADMIN` capability required to mount cgroup filesystems itself. For every cgroup mount, there should be exactly one directory named after the controller(s) co-mounted there (or "unified" for cgroup v2).
