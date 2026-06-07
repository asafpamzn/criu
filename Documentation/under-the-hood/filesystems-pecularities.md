# Filesystem Peculiarities in CRIU

While Linux aims for a uniform filesystem interface, several filesystems have unique behaviors ("peculiarities") that require specialized handling in CRIU to ensure accurate checkpointing and restoration.

## BTRFS: Virtual Device Numbers

When you `stat()` a file on BTRFS, the kernel often reports a **virtual device ID** (`st_dev`) that is unique to that specific subvolume or snapshot. However, other kernel interfaces, such as `/proc/$pid/mountinfo` or the `sock_diag` subsystem, may report the **physical device ID**.

**Problem**: CRIU cannot rely on simple `st_dev` comparisons to identify which mount a file belongs to, as the virtual and physical IDs will mismatch.

**Solution**: CRIU performs userspace path-to-device resolution. It analyzes `/proc/$pid/mountinfo` to build a mapping between virtual and physical IDs, allowing it to correctly resolve file locations. See `mount.c:phys_stat_resolve_dev()`.

**Workaround**: In some environments (like Podman), disabling Copy-on-Write for the container storage (`chattr +C`) can mitigate some BTRFS-related complexities.

## NFS: "Silly Rename" and Unlinked Files

NFS handles unlinked but open files differently than local filesystems. When a file is unlinked while still open, the NFS client performs a **"Silly Rename"**, renaming the file to something like `.nfsXXX` instead of truly removing it.

**Problem**: CRIU's standard logic for detecting unlinked files (checking if `st_nlink == 0`) fails on NFS because the "silly renamed" file still has a link count of 1.

**Solution**: CRIU explicitly checks if a file resides on an NFS mount. If it does, it examines the filename for the `.nfs` prefix. If both conditions match, CRIU treats the file as "opened and unlinked," capturing its contents into the image as a **ghost file**. See `files-reg.c:nfs_silly_rename()`.

## OverlayFS: Path Inconsistencies and Link-Remap

OverlayFS, the standard for modern container engines, has several known issues:

1.  **Path Mismatches (Pre-v4.2)**: On older kernels, `/proc/$pid/fd/` and `/proc/$pid/fdinfo/` could report paths that did not include the OverlayFS mountpoint. CRIU detects OverlayFS mounts and manually corrects these paths using information from the mount table.
2.  **linkat() Failures**: In OverlayFS, the `linkat()` system call fails with `ENOENT` if the file being linked resides on a **lower layer** (read-only layer) and has been unlinked from the upper layer.
    *   **CRIU Response**: When a "link-remap" (linking a deleted file back to the filesystem) fails on OverlayFS, CRIU automatically falls back to dumping the file as a **ghost file** (copying its contents into the image).

## AUFS: Branch Path Leakage (Legacy)

AUFS (mostly superseded by OverlayFS) has a bug where `/proc/$pid/maps` reveals the path of a file within its internal **branch** directory instead of its visible path within the AUFS mount.

**Solution**: CRIU identifies AUFS mounts, reads the branch configuration from `sysfs`, and "fixes" the paths in the memory map to ensure the file can be correctly located during restoration. See `sysfs_parse.c:fixup_aufs_vma_fd`.

## See also
* [How Hard is it to Open a File?](how-hard-is-it-to-open-a-file.md)
* [Invisible Files](invisible-files.md)
* [Mount Points](mount-points.md)
