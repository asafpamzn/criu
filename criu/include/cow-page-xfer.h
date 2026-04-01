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
#define PS_IOV_DIRTY_BITMAP       11  /* Primary sends dirty bitmap to replica */
#define PS_IOV_START_RESTORE      12  /* Signal replica to start process */
#define PS_IOV_BULK_COMPLETE_ACK  13  /* Replica -> Primary: all bulk pages received */
#define PS_IOV_INVENTORY_READY    14  /* Primary -> Replica: inventory.img written */
#define PS_IOV_DIRTY_BITMAP_ACK   15  /* Replica -> Primary: dirty bitmap received */
#define PS_IOV_ALL_PAGES_SENT     16  /* Primary -> Replica: all pages sent, zero-fill rest */
#define PS_IOV_ALL_PAGES_SENT_ACK 17  /* Replica -> Primary: ACK, safe to close connection */

/* Compression state machine states for bulk stream reader */
enum compress_read_state {
	COMPRESS_STATE_READING_HEADER = 0,    /* Reading page_server_iov header */
	COMPRESS_STATE_READING_SIZE,          /* Reading compressed_size (4 bytes) */
	COMPRESS_STATE_READING_COMPRESSED,    /* Reading compressed data */
	COMPRESS_STATE_READING_UNCOMPRESSED,  /* Reading uncompressed page data */
	COMPRESS_STATE_READING_DIRTY_BITMAP,  /* Reading dirty bitmap ranges */
};

/* Global compression statistics (used by cow-bulk-send.c) */
extern unsigned long g_compress_uncompressed_bytes;
extern unsigned long g_compress_compressed_bytes;

/* COW bulk stream state */
extern bool page_server_bulk_stream_done(void);
extern void set_bulk_stream_done(void);
extern void reset_bulk_stream_done(void);

/* COW all-pages-sent ACK state */
extern void set_all_pages_sent_ack_received(void);
extern bool is_all_pages_sent_ack_received(void);

/* COW signaling functions */
extern int send_dirty_bitmap_to_replica(int sk, u64 dst_id,
					unsigned long *ranges,
					unsigned int nr_ranges);
extern int send_cow_dirty_bitmap(unsigned long *ranges, unsigned int nr_ranges);
extern int send_all_pages_sent_signal(int sk);
extern int send_all_pages_sent_ack(void);
extern int send_inventory_ready_signal(void);

/* P3 parallel receiver connections (REPLICA side) */
extern int start_p3_receiver_connections(int num_connections);
extern void stop_p3_receiver_connections(void);

#endif /* __CR_COW_PAGE_XFER_H__ */
