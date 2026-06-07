# Checkpoint and Restore of Mount Points

CRIU provides deep support for capturing and reconstructing Linux mount namespaces and the complex hierarchies of mount points within them. This includes support for bind mounts, shared propagation, and external dependencies.

## Key Information Captured

For every mount namespace, CRIU parses `/proc/$pid/mountinfo` to extract:
1.  **Mount Hierarchy**: The parent-child relationships between mount points.
2.  **Filesystem Details**: Device IDs, filesystem types, and the mount source.
3.  **Root and Target**: The specific directory within the filesystem being mounted and its destination in the process's view.
4.  **Propagation State**: Whether a mount is `shared`, `slave`, `private`, or `unbindable`.
5.  **Mount Options**: Flags such as `ro`, `nodev`, `noexec`, and `nosuid`.

## The Restoration Challenge

Restoring mounts is one of CRIU's most difficult tasks because it must recreate the exact same state that the kernel built up over time. This requires:
*   **Dependency Sorting**: Mounts must be recreated in the correct order (e.g., a parent must exist before its child can be mounted).
*   **Source Resolution**: CRIU must be able to access the original filesystem source.
*   **Propagation Reconstruction**: Shared and slave relationships must be established in the correct sequence to ensure future mount events propagate as expected.

## Mount V2: The Modern Engine

CRIU includes an advanced restoration engine called **Mount V2** (`--mount-v2`). This engine uses a more robust algorithm to handle:
*   **Complex Overmounts**: Scenarios where multiple mounts are stacked on the same directory.
*   **Circular Dependencies**: Resolving cases where mounts depend on each other in non-trivial ways.
*   **Namespace Sharing**: Efficiently handling processes that share the same mount namespace.

## External and Auto-detected Mounts

Sometimes, the source of a mount point is located outside the container or process tree being checkpointed (e.g., a host directory bind-mounted into a container).

### 1. External Mounts (`--external`)
Users can manually specify how to handle these external dependencies by mapping the mount's identifier to a path on the destination host:
```bash
criu restore --external mnt[ID]:/new/host/path ...
```

### 2. Auto-detection
CRIU can often automatically identify external bind mounts by comparing the mount points in the target process with those in its own mount namespace. This simplifies migration by reducing the need for manual mapping.

## Common Issues

*   **Unsupported Filesystems**: Some specialized or virtual filesystems may not support standard checkpointing. These often require plugins or must be marked as external.
*   **Hidden Sources**: If a bind mount's source is overmounted and no longer visible, CRIU may fail to identify how to recreate it without the Mount V2 engine or manual hints.

## See also
* [Mount V2 Details](mount-v2.md)
* [Filesystem Peculiarities](filesystems-pecularities.md)
* [Invisible Files (Overmounts)](invisible-files.md)
