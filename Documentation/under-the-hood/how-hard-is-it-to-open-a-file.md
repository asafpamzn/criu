# The Complexity of Re-opening Files during Restore

Re-creating an open file descriptor during restoration is far more complex than simply calling `open(path, flags)`. This article explores the numerous edge cases CRIU must handle to faithfully reconstruct the file state.

## 1. Basic Opening

At its simplest, a file is defined by its path and access mode:
```c
int fd = open(f->path, f->mode);
```
However, this is only the beginning of the process.

## 2. FIFOs and Blocking
A standard `open()` call on a FIFO (named pipe) can hang indefinitely if there is no corresponding reader or writer on the other end. CRIU avoids this by first opening the FIFO with `O_RDWR` (to ensure at least one of each is present) and then using `dup2` to establish the final descriptor with the correct original flags.

## 3. Unlinked but Open Files (Ghost Files)
Linux allows files to be deleted while they are still open. These "invisible" files no longer have a path in the filesystem.
*   **link-remap**: If the file still has other hard links elsewhere, CRIU may create a temporary link to it to allow it to be re-opened via a path.
*   **Ghost Files**: If the link count is zero, CRIU captures the entire content of the file during the dump. During restore, it recreates this file in a temporary location, opens it, and then unlinks it to match the original state.

## 4. Directories and Hard Links
Directories cannot be hard-linked. If a directory was unlinked, CRIU must recreate it, open it, and then remove it. For files with multiple hard links that were all deleted, CRIU must ensure they all point back to the same physical inode upon restoration, requiring careful tracking of "temporary" paths and user-space reference counts.

## 5. Mount Namespaces and Chroot
The same path (e.g., `/etc/passwd`) might refer to entirely different files depending on the mount namespace or `chroot` environment of the process.
*   **mnt_id**: CRIU records the mount ID for every file during the dump.
*   **open_ns_root**: During restoration, CRIU uses file descriptors referring to the root of the specific mount namespace to ensure that `openat()` targets the correct physical file, regardless of the restorer's current root.

## 6. File Ownership and Signals (fown)
Files can have an associated "owner" (a PID or PGID) that receives signals (like `SIGIO` or `SIGPOLL`) when I/O events occur.
*   **F_SETOWN_EX**: CRIU restores this ownership using the extended owner structure.
*   **UID Switching**: Setting the owner of a file may require specific privileges. CRIU may temporarily switch its effective UIDs during the `fcntl` call to satisfy kernel permission checks if the file owner differs from the restorer.
*   **F_SETSIG**: The specific signal number to be delivered is also faithfully restored.

## 7. Position and Flags
*   **Lseek**: The current byte offset (`pos`) is restored using `lseek`.
*   **Flag Sanitization**: Certain flags (like `O_CREAT`, `O_EXCL`, `O_TRUNC`) only make sense during the initial creation of a file. CRIU strips these before the restore-time `open()` to avoid accidentally creating or truncating existing files.
*   **O_PATH**: Files opened with `O_PATH` are handled as pure path references; they do not have positions, ownership, or data access.

## 8. The Final Step: Descriptor Planting
Once a file is successfully opened (at a temporary descriptor number assigned by the kernel), it must be moved to the exact numeric descriptor the application expects (e.g., FD 42). This is achieved via `dup2()`, but requires coordination when descriptors are shared across a process tree.

*See also: [How to assign needed file descriptor to a file](how-to-assign-needed-file-descriptor-to-a-file.md)*
