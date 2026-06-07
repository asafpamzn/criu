# Memory Images Deduplication

During iterative migration, CRIU produces multiple snapshots of a process's memory. Since most memory pages remain unchanged between iterations, saving every page in every snapshot would result in significant disk space waste and increased migration time. CRIU uses several deduplication techniques to address this.

## How Deduplication Works

Deduplication relies on identifying pages that are identical to those in a previous snapshot (the "parent" image).

### 1. The `in_parent` Flag
The `pagemap-$id.img` file describes the virtual memory layout. Each entry (`pagemap_entry`) can include an `in_parent` flag:
*   **If `false`**: The page's contents are stored in the current `pages-$id.img` file.
*   **If `true`**: The page's contents are identical to the one in the parent image. CRIU does not write the data to the current `pages-$id.img`, saving both disk space and I/O time.

### 2. Detection via Soft-Dirty
During a `pre-dump`, CRIU uses the kernel's **soft-dirty bit** to identify which pages have been modified.
*   If a page was present in the previous iteration and its soft-dirty bit is **not set**, CRIU knows the content remains unchanged.
*   It marks the page as `in_parent` in the current pagemap image and skips dumping its data.

## Auto-Deduplication (`--auto-dedup`)

CRIU provides an advanced `--auto-dedup` mode that optimizes both the dumping and restoration processes.

### During Dump
When `--auto-dedup` is enabled during a dump, CRIU actively manages the relationship between the current and parent image sets to ensure maximum deduplication efficiency. It traverses the previous images to verify which regions can be safely referenced rather than re-dumped.

### During Restore (Disk Space Optimization)
A unique and powerful feature of `--auto-dedup` during restoration is **online disk space reclamation**:
*   As CRIU reads pages from the `pages-$id.img` files to restore the process's memory, it uses the `fallocate(FALLOC_FL_PUNCH_HOLE)` system call on the image files.
*   This "punches holes" in the images, effectively freeing the underlying physical disk blocks as soon as the data has been loaded into RAM.
*   This is critical for systems with limited disk space when restoring from a large number of iterative pre-dumps, as it prevents the total image size from exceeding the available storage.

## Implementation Details

*   **Image Chaining**: Deduplication requires a chain of images established via the `--prev-images-dir` option, allowing CRIU to look back through multiple layers of snapshots.
*   **Sparse File Support**: The hole-punching mechanism leverages the host filesystem's support for sparse files, ensuring that the restored environment remains efficient.

## See also
* [Memory Changes Tracking](memory-changes-tracking.md)
* [Iterative Migration](iterative-migration.md)
* [Memory Dumping and Restoring](memory-dumping-and-restoring.md)
