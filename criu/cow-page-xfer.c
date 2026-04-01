/*
 * COW page transfer support.
 *
 * This file contains COW-specific page transfer functionality including
 * compression statistics, state management, and protocol extensions for
 * COW migration.
 */

#include <stdbool.h>
#include "cow-page-xfer.h"

/* Global compression statistics for stats printing (used by cow-bulk-send.c too) */
unsigned long g_compress_uncompressed_bytes = 0;
unsigned long g_compress_compressed_bytes = 0;

/* COW state flags for phased migration */
static bool bulk_stream_done = false;
static bool all_pages_sent_ack_received = false;

bool page_server_bulk_stream_done(void)
{
	return bulk_stream_done;
}

void set_bulk_stream_done(void)
{
	bulk_stream_done = true;
}

void reset_bulk_stream_done(void)
{
	bulk_stream_done = false;
}

void set_all_pages_sent_ack_received(void)
{
	all_pages_sent_ack_received = true;
}

bool is_all_pages_sent_ack_received(void)
{
	return all_pages_sent_ack_received;
}
