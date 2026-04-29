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
#include <string.h>

#include "cow/cow-page-xfer.h"
#include "page-xfer.h"
#include "page.h"
#include "pstree.h"
#include "cr_options.h"
#include "criu-log.h"
#include "image.h"
#include "pagemap.h"
#include "mem.h"
#include "cow/cow-unified-thread.h"
#include "common/list.h"
#include "common/bug.h"
#include "util.h"

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
 * Wait for all_pages_sent ACK from replica.
 * Called by primary after sending PS_IOV_ALL_PAGES_SENT.
 * Actually reads from socket to receive the ACK.
 */
int wait_for_all_pages_sent_ack(int sk)
{
	struct page_server_iov pi;

	pr_info("Waiting for all_pages_sent ACK from replica...\n");
	BUG_ON(page_server_recv(sk, &pi, sizeof(pi), MSG_WAITALL) != sizeof(pi));
	BUG_ON(decode_ps_cmd(pi.cmd) != PS_IOV_ALL_PAGES_SENT_ACK);

	pr_info("Received all_pages_sent ACK from replica\n");
	set_all_pages_sent_ack_received();
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

	BUG_ON(use_sk < 0);

	pr_info("Sending all_pages_sent signal to replica\n");
	return send_psi(use_sk, &pi);
}

/*
 * Send end-of-transfer marker (PS_IOV_CLOSE) to replica.
 * Called by primary after all P3 threads complete bulk + dirty transfer.
 */
int send_image_complete(int sk, u64 dst_id)
{
	struct page_server_iov close_cmd = {
		.cmd = PS_IOV_CLOSE,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = dst_id,
	};

	pr_info("Image dst_id=%lu complete\n", dst_id);

	if (send_psi(sk, &close_cmd)) {
		if (errno == EPIPE || errno == ECONNRESET) {
			pr_info("Receiver closed after close marker, treating as completion\n");
			return 0;
		}
		pr_err("Failed to send close command\n");
		return -1;
	}
	return 0;
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

	BUG_ON(sk < 0);

	pr_info("Sending all_pages_sent ACK to primary\n");
	return send_psi(sk, &pi);
}

/*
 * Request all pages from primary in batch mode.
 * COW-specific: used for bulk page transfer.
 */
int cow_request_all_remote_pages(unsigned long img_id)
{
	struct page_server_iov pi = {
		.cmd = PS_IOV_GET_ALL,
		.nr_pages = 0,  /* Not used in batch mode */
		.vaddr = 0,     /* Not used in batch mode */
		.dst_id = img_id,
	};

	pr_info("Requesting all pages for img_id=%lu in batch mode\n", img_id);
	BUG_ON(send_psi(get_page_server_sk(), &pi));

	page_server_tcp_nodelay(get_page_server_sk(), true);
	return 0;
}

/*
 * Close the page server socket (server-side).
 * Used after sending dirty bitmap in COW phased migration.
 */
void cow_close_page_server_socket(void)
{
	int sk = get_page_server_sk();

	if (sk >= 0)
		pr_info("Closing page server socket (server-side)\n");
	/* Also close the listen socket to release the port */
	close_listen_socket();
}

/*
 * Helper to write lazy VMA pagemap entries that come before a given vaddr.
 * COW-specific: used to interleave lazy VMA entries with pipe entries.
 */
int cow_write_lazy_vmas_before(struct page_xfer *xfer, unsigned long before_vaddr,
			       struct lazy_vma_entry **cur_lve)
{
	struct list_head *global_list = get_global_lazy_vmas();
	struct lazy_vma_entry *lve = *cur_lve;

	/* Start from beginning if not set */
	if (!lve && !list_empty(global_list))
		lve = list_first_entry(global_list, struct lazy_vma_entry, list);

	/* Write all lazy VMAs that start before before_vaddr.
	 * Use lve->start/end (not lve->vma->e) since vma structs
	 * may be freed after the dump completes.
	 */
	while (lve && &lve->list != global_list) {
		struct iovec iov;
		u32 flags = PE_LAZY;
		unsigned long vma_start = lve->start;

		/*
		 * In server mode, filter VMAs by dst_id (for multi-process dumps).
		 * In local mode, xfer->dst_id is unreliable (union with pmi/pi),
		 * so skip the check. COW local mode dumps single process anyway.
		 */
		if (opts.use_page_server && lve->dst_id != xfer->dst_id) {
			lve = list_entry(lve->list.next, struct lazy_vma_entry, list);
			continue;
		}

		/* Stop if this VMA starts at or after our limit */
		if (vma_start >= before_vaddr)
			break;

		iov.iov_base = (void *)(unsigned long)lve->start;
		iov.iov_len = lve->end - lve->start;

		BUG_ON(iov.iov_base < (void *)xfer->offset);
		iov.iov_base -= xfer->offset;

		pr_debug("Writing lazy VMA pagemap: 0x%lx-0x%lx (%lu pages)\n",
			(unsigned long)lve->start, (unsigned long)lve->end,
			(unsigned long)(iov.iov_len / PAGE_SIZE));

		BUG_ON(xfer->write_pagemap(xfer, &iov, flags));

		lve = list_entry(lve->list.next, struct lazy_vma_entry, list);
	}

	*cur_lve = lve;
	return 0;
}

/*
 * Handle COW-specific protocol commands in page_server_serve().
 * Returns: 0 = handled, 1 = not a COW command, -1 = error
 * Sets *ret_val, *flushed, *bulk_ack on success.
 */
int cow_handle_protocol_cmd(u32 cmd, struct page_server_iov *pi, int sk,
			    int *ret_val, bool *flushed, bool *bulk_ack)
{
	switch (cmd) {
	case PS_IOV_GET_ALL:		
		*ret_val = cow_page_server_get_all_pages(sk, pi->dst_id);
		return 0;

	case PS_IOV_START_RESTORE:
		pr_info("Received start restore signal\n");
		*ret_val = 0;
		return 0;

	case PS_IOV_BULK_COMPLETE_ACK:
		/*
		 * Replica acknowledges all bulk pages received.
		 * Break out of the serve loop so the primary can
		 * proceed to Phase 3 (skeleton dump + dirty scan).
		 */
		pr_info("Received bulk complete ACK from replica\n");
		*ret_val = 0;
		*flushed = true;
		*bulk_ack = true;
		return 0;

	case PS_IOV_ALL_PAGES_SENT_ACK:
		/*
		 * Replica acknowledges all_pages_sent signal received.
		 * Set flag so unified_page_server_thread can continue.
		 */
		pr_info("Received all_pages_sent ACK from replica\n");
		set_all_pages_sent_ack_received();
		*ret_val = 0;
		*flushed = true;
		return 0;

	default:
		return 1;  /* Not a COW command */
	}
}

