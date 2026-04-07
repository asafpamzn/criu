#ifndef __CR_COW_BULK_RECV_H__
#define __CR_COW_BULK_RECV_H__

#include <stdbool.h>
#include "page-xfer.h"

struct epoll_rfd;

/*
 * COW bulk stream receiver (REPLICA side).
 *
 * This module handles continuous bulk page reception from the primary
 * during COW phased migration. It processes headers and compressed/
 * uncompressed pages as they arrive without correlation to requests.
 */

/* Bulk stream reader entry points */
extern int page_server_async_read_bulk(struct epoll_rfd *f);
extern int page_server_start_async_read_bulk(void *buf, unsigned long nr_pages,
					     ps_async_read_complete complete, void *priv);

/* Update callback for COW convergence phase */
extern int page_server_update_async_callback(ps_async_read_complete complete, void *priv);

/* Cleanup async bulk reader state (call before closing socket) */
extern void page_server_cleanup_async_bulk(void);

/* TCP helper (implemented in page-xfer.c) */
extern void page_server_tcp_nodelay(int sk, bool on);

#endif /* __CR_COW_BULK_RECV_H__ */
