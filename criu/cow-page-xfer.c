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
#include <time.h>
#include <lz4.h>

#include "cow-page-xfer.h"
#include "page-xfer.h"
#include "page.h"
#include "pstree.h"
#include "cr_options.h"
#include "criu-log.h"
#include "image.h"
#include "pagemap.h"
#include "mem.h"
#include "cow-unified-thread.h"
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

/*
 * Send dirty bitmap ACK to primary.
 * Called by replica after fully receiving the dirty bitmap.
 */
int send_dirty_bitmap_ack(void)
{
	struct page_server_iov pi = {
		.cmd = encode_ps_cmd(PS_IOV_DIRTY_BITMAP_ACK, 0),
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = 0,
	};
	int sk = get_page_server_sk();

	pr_info("Sending dirty bitmap ACK to primary\n");
	return send_psi(sk, &pi);
}

/*
 * Send a page with LZ4 compression.
 * Protocol: header (PS_IOV_ADD_F_COMPRESS) + compressed_size (4 bytes) + compressed_data
 * Optimized: single buffer, single send() syscall
 */
int send_page_compressed(int sk, const void *data, u64 dst_id, unsigned long vaddr)
{
	/* Buffer layout: [header][compressed_size][compressed_data] */
	char send_buf[sizeof(struct page_server_iov) + sizeof(int) + LZ4_compressBound(PAGE_SIZE)];
	struct page_server_iov *pi = (struct page_server_iov *)send_buf;
	int *compressed_size = (int *)(send_buf + sizeof(*pi));
	char *compressed_data = send_buf + sizeof(*pi) + sizeof(int);
	int total_len;
	int ret;

	/* 1. Compress directly into send buffer (no memcpy!) */
	*compressed_size = LZ4_compress_default(data, compressed_data, PAGE_SIZE,
						LZ4_compressBound(PAGE_SIZE));
	if (*compressed_size <= 0) {
		pr_err("LZ4 compression failed for page at %lx\n", vaddr);
		return -1;
	}

	/* Track compression statistics */
	g_compress_uncompressed_bytes += PAGE_SIZE;
	g_compress_compressed_bytes += *compressed_size;

	pr_debug("Compressed page at %lx: %lu -> %d bytes (%.1f%%)\n",
		 vaddr, PAGE_SIZE, *compressed_size,
		 (float)(*compressed_size) * 100 / PAGE_SIZE);

	/* 2. Fill in header (after compression so we know it succeeded) */
	pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS, PE_PRESENT);
	pi->nr_pages = 1;
	pi->vaddr = vaddr;
	pi->dst_id = dst_id;

	/* 3. Single send: header + size + compressed data */
	total_len = sizeof(*pi) + sizeof(int) + *compressed_size;
	ret = page_server_send(sk, send_buf, total_len, 0);
	if (ret != total_len) {
		pr_perror("Failed to send compressed page (sent %d/%d)", ret, total_len);
		return -1;
	}

	return 0;
}

int send_page_uncompressed(int sk, const void *data, u64 dst_id, unsigned long vaddr)
{
	char send_buf[sizeof(struct page_server_iov) + PAGE_SIZE];
	struct page_server_iov *pi = (struct page_server_iov *)send_buf;
	void *payload = send_buf + sizeof(*pi);
	int total_len;
	int ret;

	memcpy(payload, data, PAGE_SIZE);

	pi->cmd = encode_ps_cmd(PS_IOV_ADD_F, PE_PRESENT);
	pi->nr_pages = 1;
	pi->vaddr = vaddr;
	pi->dst_id = dst_id;

	total_len = sizeof(*pi) + PAGE_SIZE;
	ret = page_server_send(sk, send_buf, total_len, 0);
	if (ret != total_len) {
		pr_perror("Failed to send page (sent %d/%d)", ret, total_len);
		return -1;
	}

	return 0;
}

/*
 * Statistics tracking for COW page server (debug/monitoring).
 */
static struct {
	unsigned long serve_open;
	unsigned long serve_open2;
	unsigned long serve_parent;
	unsigned long serve_add_f;
	unsigned long serve_add;
	unsigned long serve_hole;
	unsigned long serve_close;
	unsigned long serve_force_close;
	unsigned long serve_get;
	unsigned long serve_unknown;
	time_t last_print_time;
} cow_ps_stats = {0};

void cow_ps_stats_inc_open(void) { cow_ps_stats.serve_open++; }
void cow_ps_stats_inc_open2(void) { cow_ps_stats.serve_open2++; }
void cow_ps_stats_inc_parent(void) { cow_ps_stats.serve_parent++; }
void cow_ps_stats_inc_add_f(void) { cow_ps_stats.serve_add_f++; }
void cow_ps_stats_inc_add(void) { cow_ps_stats.serve_add++; }
void cow_ps_stats_inc_hole(void) { cow_ps_stats.serve_hole++; }
void cow_ps_stats_inc_close(void) { cow_ps_stats.serve_close++; }
void cow_ps_stats_inc_force_close(void) { cow_ps_stats.serve_force_close++; }
void cow_ps_stats_inc_get(void) { cow_ps_stats.serve_get++; }
void cow_ps_stats_inc_unknown(void) { cow_ps_stats.serve_unknown++; }

void cow_check_and_print_stats(void)
{
	time_t now = time(NULL);

	if (now - cow_ps_stats.last_print_time >= 60) {
		pr_err("[PAGE_SERVER_STATS] serve: open=%lu open2=%lu parent=%lu add_f=%lu add=%lu hole=%lu get=%lu close=%lu unknown=%lu\n",
			cow_ps_stats.serve_open,
			cow_ps_stats.serve_open2,
			cow_ps_stats.serve_parent,
			cow_ps_stats.serve_add_f,
			cow_ps_stats.serve_add,
			cow_ps_stats.serve_hole,
			cow_ps_stats.serve_get,
			cow_ps_stats.serve_close + cow_ps_stats.serve_force_close,
			cow_ps_stats.serve_unknown);

		/* Reset all counters */
		memset(&cow_ps_stats, 0, sizeof(cow_ps_stats));
		cow_ps_stats.last_print_time = now;
	}
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

	if (send_psi(get_page_server_sk(), &pi))
		return -1;

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

	pr_debug("DEBUG_SOCKET: cow_close_page_server_socket called fd=%d\n", sk);
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

		if (xfer->write_pagemap(xfer, &iov, flags)) {
			pr_err("Failed to write pagemap for lazy VMA\n");
			return -1;
		}

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
		cow_ps_stats_inc_get();
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

/*
 * Receive and decompress compressed pages from dump client.
 * Called by page_server_add() when PS_IOV_ADD_F_COMPRESS is received.
 *
 * Protocol: For each page, receive [compressed_size (4 bytes)][compressed_data]
 * Decompress and write to pipe, then call write_pages to store in image.
 */
int cow_receive_compressed_pages(int sk, struct page_server_iov *pi,
				 int write_fd, int read_fd,
				 struct page_xfer *lxfer)
{
	unsigned long pages_left = pi->nr_pages;

	while (pages_left > 0) {
		int compressed_size;
		char compressed_buf[LZ4_compressBound(PAGE_SIZE)];
		char decompressed[PAGE_SIZE];
		int decomp_ret;

		/* Receive compressed size */
		if (page_server_recv(sk, &compressed_size, sizeof(compressed_size),
				     MSG_WAITALL) != sizeof(compressed_size)) {
			pr_perror("Failed to receive compressed size");
			return -1;
		}

		if (compressed_size <= 0 || compressed_size > LZ4_compressBound(PAGE_SIZE)) {
			pr_err("Invalid compressed size: %d\n", compressed_size);
			return -1;
		}

		/* Receive compressed data */
		if (page_server_recv(sk, compressed_buf, compressed_size,
				     MSG_WAITALL) != compressed_size) {
			pr_perror("Failed to receive compressed data");
			return -1;
		}

		/* Decompress */
		decomp_ret = LZ4_decompress_safe(compressed_buf, decompressed,
						 compressed_size, PAGE_SIZE);
		if (decomp_ret != PAGE_SIZE) {
			pr_err("LZ4 decompression failed: expected %lu, got %d\n",
			       PAGE_SIZE, decomp_ret);
			return -1;
		}

		pr_debug("Decompressed page: %d -> %lu bytes\n",
			 compressed_size, PAGE_SIZE);

		/* Write decompressed page data to pipe and then to image */
		if (write(write_fd, decompressed, PAGE_SIZE) != PAGE_SIZE) {
			pr_perror("Failed to write decompressed page to pipe");
			return -1;
		}

		if (lxfer->write_pages(lxfer, read_fd, PAGE_SIZE))
			return -1;

		pages_left--;
	}

	return 0;
}
