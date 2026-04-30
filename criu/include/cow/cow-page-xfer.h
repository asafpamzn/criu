#ifndef __CR_COW_PAGE_XFER_H__
#define __CR_COW_PAGE_XFER_H__

#include <stdbool.h>
#include "int.h"

/*
 * COW-specific page server protocol commands.
 * These extend the base PS_IOV_* protocol for COW migration.
 */
#define PS_IOV_GET_ALL            8
#define PS_IOV_ADD_F_PF           9
#define PS_IOV_ADD_F_COMPRESS     10
#define PS_IOV_START_RESTORE      12  /* Signal replica to start process */
#define PS_IOV_ALL_PAGES_SENT     16  /* Primary -> Replica: all pages sent, zero-fill rest */
#define PS_IOV_ALL_PAGES_SENT_ACK 17  /* Replica -> Primary: ACK, safe to close connection */

/* Global compression statistics (used by cow-bulk-send.c) */
extern unsigned long g_compress_uncompressed_bytes;
extern unsigned long g_compress_compressed_bytes;



/* COW all-pages-sent ACK state */
extern void set_all_pages_sent_ack_received(void);
extern bool is_all_pages_sent_ack_received(void);

/* Wait for all_pages_sent ACK (called from page-xfer.c) */
extern int wait_for_all_pages_sent_ack(int sk);

/*
 * COW signaling functions are declared in page-xfer.h:
 * - send_all_pages_sent_signal()
 * - send_all_pages_sent_ack()
 * - start_p3_receiver_connections()
 * - stop_p3_receiver_connections()
 */


/* P3 parallel receiver functions (cow-p3-receiver.c) */
extern int accept_p3_connections(int *sockets, int max_connections, int timeout_ms);
extern void close_p3_sockets(int *sockets, int num_sockets);

/* COW request all pages (batch mode) */
extern int cow_request_all_remote_pages(unsigned long img_id);

/* COW server-side socket close */
extern void cow_close_page_server_socket(void);

/* COW lazy VMA pagemap writing */
struct page_xfer;
struct lazy_vma_entry;
struct page_server_iov;
extern int cow_write_lazy_vmas_before(struct page_xfer *xfer, unsigned long before_vaddr,
				      struct lazy_vma_entry **cur_lve);

/* COW protocol command handler (returns 0=handled, 1=not COW cmd, -1=error) */
extern int cow_handle_protocol_cmd(u32 cmd, struct page_server_iov *pi, int sk,
				   int *ret_val, bool *flushed, bool *bulk_ack);


#endif /* __CR_COW_PAGE_XFER_H__ */
