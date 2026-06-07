# File Validation on Restore

CRIU can verify that regular files and shared libraries being restored on the destination host are identical to the ones captured during the checkpoint. This is a critical security and stability feature, as mismatching libraries (e.g., a different version of `libc.so`) can lead to immediate application crashes or subtle data corruption due to changed offsets and symbols.

## How File Validation Works

File validation is managed via the `--file-validation` option. CRIU automatically captures metadata for all regular, file-backed mappings during the dump and stores it in the image files.

### Supported Validation Methods

CRIU supports two primary methods for validating files:

#### 1. Build-ID (Default)
Most modern Linux executables and shared libraries include a **GNU Build-ID**—a unique, compiler-generated hash stored in a dedicated ELF note section (`NT_GNU_BUILD_ID`).
*   **Dumping**: CRIU identifies ELF files by checking their magic numbers. For each ELF file, it maps at most the first **1 MB** of the file (defined as `BUILD_ID_MAP_SIZE`) and extracts the Build-ID hash.
*   **Restoring**: During restoration, CRIU performs the same extraction on the file residing on the target host. If the resulting hash does not match the one stored in the image, CRIU aborts the restoration to prevent corruption.
*   **Fallback**: If a file is not an ELF or lacks a Build-ID, CRIU automatically falls back to validating the file by its size.

#### 2. File Size (`filesize`)
A simpler and faster validation method that only compares the total size of the file in bytes.
*   **Advantage**: Minimal overhead as it only requires a `stat()` call.
*   **Disadvantage**: Less reliable than Build-ID, as different versions of a file can occasionally have identical sizes.

## Usage and Configuration

File validation is enabled by default using the `buildid` method. You can explicitly configure the behavior using the `--file-validation` flag:

```bash
# Explicitly use Build-ID validation
criu restore --file-validation buildid ...

# Use only file size validation
criu restore --file-validation filesize ...
```

## Security and Integrity

File validation ensures that the restored process tree runs against the same binary environment it was captured in. This prevents "library injection" scenarios where an attacker might try to force a restored process to run against malicious versions of its original dependencies. It also ensures that internal pointers (such as function addresses) remain valid, as they are often tied to specific library versions.

## See also
* [Dumping File Descriptors](dumping-files.md)
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Filesystem Peculiarities](filesystems-pecularities.md)
