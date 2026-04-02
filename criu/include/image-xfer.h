#ifndef __CR_IMAGE_XFER_H__
#define __CR_IMAGE_XFER_H__

/*
 * Transfer CRIU image files (.img, stats-*) over TCP.
 * Used by COW dump to eliminate shared filesystem requirement.
 *
 * Protocol (big-endian):
 *   [4 bytes] file_count
 *   Per file:
 *     [2 bytes] name_length
 *     [N bytes] filename
 *     [8 bytes] file_size
 *     [file_size bytes] data
 */

/* Source side: serve all image files from imgs_dir on given port.
 * Accepts one connection, sends all files, then returns.
 * Returns 0 on success, -1 on error. */
extern int serve_image_files(int port, const char *imgs_dir);

/* Replica side: download image files from host:port into imgs_dir.
 * Retries connection for up to timeout_s seconds.
 * Returns 0 on success, -1 on error. */
extern int fetch_image_files(const char *host, int port,
			     const char *imgs_dir, int timeout_s);

/* Thread wrapper for serve_image_files — takes struct { int port; const char *dir; } */
extern void *serve_image_files_thread(void *arg);

#endif /* __CR_IMAGE_XFER_H__ */
