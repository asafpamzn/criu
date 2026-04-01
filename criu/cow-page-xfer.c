/*
 * COW page transfer support.
 *
 * This file contains COW-specific page transfer functionality including
 * compression statistics, state management, signaling functions, and
 * protocol extensions for COW migration.
 */

#include <stdbool.h>
#include <unistd.h>
#include <sys/socket.h>

#include "cow-page-xfer.h"
#include "page-xfer.h"
#include "pstree.h"
#include "cr_options.h"
#include "criu-log.h"
#include "image.h"

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

/*
 * Send dirty bitmap to replica (COW phased migration).
 * Called by primary after Phase 3 dirty scan completes.
 * Format: header with cmd=PS_IOV_DIRTY_BITMAP, nr_pages=nr_ranges,
 * followed by ranges array: [start0, len0, start1, len1, ...]
 */
int send_dirty_bitmap_to_replica(int sk, u64 dst_id,
				 unsigned long *ranges,
				 unsigned int nr_ranges)
{
	struct page_server_iov pi = {
		.cmd = encode_ps_cmd(PS_IOV_DIRTY_BITMAP, 0),
		.nr_pages = nr_ranges,
		.vaddr = 0,
		.dst_id = dst_id,
	};
	size_t ranges_size = nr_ranges * 2 * sizeof(unsigned long);

	pr_info("Sending dirty bitmap: %u ranges (%zu bytes)\n",
		nr_ranges, ranges_size);

	if (send_psi(sk, &pi))
		return -1;

	if (nr_ranges > 0 && page_server_send(sk, ranges, ranges_size, 0) != ranges_size) {
		pr_perror("Failed to send dirty ranges");
		return -1;
	}

	return 0;
}

/*
 * Wait for all_pages_sent ACK from replica.
 * Called by primary after sending PS_IOV_ALL_PAGES_SENT.
 *
 * Note: The ACK is received by page_server_serve() which sets a flag.
 * We poll the flag here to avoid race conditions with socket reads.
 */
int wait_for_all_pages_sent_ack(int sk)
{
	(void)sk;  /* unused - ACK comes via page_server_serve() */

	pr_info("Waiting for all_pages_sent ACK from replica...\n");
	while (!is_all_pages_sent_ack_received()) {
		usleep(1000);  /* 1ms poll */
	}
	pr_info("Received all_pages_sent ACK from replica\n");
	return 0;
}

/*
 * Wait for dirty bitmap ACK from replica.
 * Called by primary after sending all dirty bitmaps.
 */
static int wait_for_dirty_bitmap_ack(void)
{
	struct page_server_iov pi;
	int sk = get_page_server_sk();

	while (true) {
		pr_info("Waiting for dirty bitmap ACK from replica...\n");
		if (page_server_recv(sk, &pi, sizeof(pi), MSG_WAITALL) != sizeof(pi)) {
			pr_perror("Failed to receive dirty bitmap ACK");
			return -1;
		}

		if (decode_ps_cmd(pi.cmd) != PS_IOV_DIRTY_BITMAP_ACK) {
			pr_err("Expected dirty bitmap ACK, got cmd=%u\n", decode_ps_cmd(pi.cmd));
			continue;
		}
		break;
	}

	pr_info("Received dirty bitmap ACK from replica\n");
	return 0;
}

/*
 * Send dirty bitmap to replica using the current page server connection.
 * Called from cr-dump.c after skeleton dump completes.
 */
int send_cow_dirty_bitmap(unsigned long *ranges, unsigned int nr_ranges)
{
	struct pstree_item *item;
	int sk = get_page_server_sk();

	pr_info("send_cow_dirty_bitmap: page_server_sk=%d, nr_ranges=%u\n",
		sk, nr_ranges);

	if (sk < 0) {
		pr_err("Page server not connected (page_server_sk=%d), cannot send dirty bitmap\n",
		       sk);
		return -1;
	}

	/*
	 * Send dirty bitmap for each task. The replica needs to know
	 * which pages are dirty so it can apply WP_SYNC for convergence.
	 */
	for_each_pstree_item(item) {
		u64 dst_id;

		if (!task_alive(item))
			continue;

		dst_id = encode_pm_id(CR_FD_PAGEMAP, vpid(item));

		pr_info("Sending dirty bitmap for pid=%d (dst_id=%lu)\n",
			vpid(item), (unsigned long)dst_id);

		if (send_dirty_bitmap_to_replica(sk, dst_id, ranges, nr_ranges))
			return -1;
	}

	/* Wait for ACK from replica to ensure it processed the dirty bitmap */
	if (wait_for_dirty_bitmap_ack())
		return -1;

	return 0;
}

/*
 * Send "all pages sent" signal to replica (COW phased migration).
 * Called by primary after dirty bitmap transfer completes, so replica
 * knows it can zero-fill any remaining page faults for new VMAs.
 * If sk >= 0, use that socket; otherwise use global page_server_sk.
 */
int send_all_pages_sent_signal(int sk)
{
	struct page_server_iov pi = {
		.cmd = PS_IOV_ALL_PAGES_SENT,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = 0,
	};
	int use_sk = (sk >= 0) ? sk : get_page_server_sk();

	if (use_sk < 0) {
		pr_err("No page server socket for all_pages_sent signal\n");
		return -1;
	}

	pr_info("Sending all_pages_sent signal to replica (sk=%d)\n", use_sk);
	return send_psi(use_sk, &pi);
}

/*
 * Send ACK for all_pages_sent signal (COW phased migration).
 * Called by replica after drain thread finishes, so primary knows
 * it's safe to close the connection.
 */
int send_all_pages_sent_ack(void)
{
	struct page_server_iov pi = {
		.cmd = PS_IOV_ALL_PAGES_SENT_ACK,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = 0,
	};
	int sk = get_page_server_sk();

	if (sk < 0) {
		pr_err("No page server socket for all_pages_sent ACK\n");
		return -1;
	}

	pr_info("Sending all_pages_sent ACK to primary (sk=%d)\n", sk);
	return send_psi(sk, &pi);
}

/*
 * Send inventory ready signal to replica (COW phased migration).
 * Called by primary after writing inventory.img, so replica knows
 * it's safe to load the pstree.
 */
int send_inventory_ready_signal(void)
{
	struct page_server_iov pi = {
		.cmd = PS_IOV_INVENTORY_READY,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = 0,
	};
	int sk = get_page_server_sk();

	pr_info("Sending inventory ready signal to replica\n");

	if (sk < 0) {
		pr_err("Page server not connected, cannot send inventory ready signal\n");
		return -1;
	}

	if (send_psi(sk, &pi)) {
		pr_err("Failed to send inventory ready signal\n");
		return -1;
	}

	return 0;
}
