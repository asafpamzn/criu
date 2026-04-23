/*
 * COW bulk stream receiver (REPLICA side).
 *
 * This module handles continuous bulk page reception from the primary
 * during COW phased migration. It processes headers and compressed/
 * uncompressed pages as they arrive without correlation to requests.
 */

#include <errno.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <lz4.h>

#include "types.h"
#include "page.h"
#include "criu-log.h"
#include "cr_options.h"
#include "xmalloc.h"
#include "page-xfer.h"
#include "cow/cow-page-xfer.h"
#include "cow/cow-bulk-send.h"
#include "cow/cow-bulk-recv.h"
#include "cow/cow-uffd.h"
#include "uffd.h"
#include "common/bug.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bulk-recv: "

/* Bulk stream return codes (local defines) */
#define BULK_STREAM_WOULD_BLOCK 0
#define BULK_STREAM_PROGRESS    1
#define BULK_STREAM_COMPLETE    2

/*
 * Async read state structure for bulk stream processing.
 * Extended with compression and dirty bitmap support.
 */
struct ps_async_read_bulk {
	unsigned long rb;      /* Bytes read */
	unsigned long goal;    /* Total bytes expected */

	struct page_server_iov pi;
	void *pages;

	ps_async_read_complete complete;
	void *priv;

	struct list_head l;

	/* Compression support */
	int compressed_size;     /* Size of compressed data (0 = uncompressed) */
	int compressed_rb;       /* Bytes read of compressed data */
	char *compressed_buf;    /* Buffer for compressed data */
	int compress_state;      /* enum compress_read_state */
};

static LIST_HEAD(bulk_async_reads);

/* Bulk stream statistics */
static struct {
	unsigned long recv_calls;
	unsigned long recv_would_block;
	unsigned long recv_bytes;
	unsigned long recv_wait_time_ns;
	unsigned long pages_completed;
	unsigned long decompress_calls;
	unsigned long decompress_time_ns;
	unsigned long callback_calls;
	time_t last_print_time;
} bulk_stats;

/*
 * Helper function for common recv pattern with stats tracking.
 * Returns bytes received on success, -EAGAIN on would-block, -1 on error.
 */
static int bulk_recv(void *buf, int need, int flags)
{
	struct timespec t_recv_start, t_recv_end;
	int ret;
	int sk = get_page_server_sk();

	clock_gettime(CLOCK_MONOTONIC, &t_recv_start);
	bulk_stats.recv_calls++;
	ret = page_server_recv(sk, buf, need, flags);
	clock_gettime(CLOCK_MONOTONIC, &t_recv_end);
	bulk_stats.recv_wait_time_ns += (t_recv_end.tv_sec - t_recv_start.tv_sec) * 1000000000 +
					(t_recv_end.tv_nsec - t_recv_start.tv_nsec);

	if (ret < 0) {
		if (flags == MSG_DONTWAIT && (errno == EAGAIN || errno == EINTR)) {
			bulk_stats.recv_would_block++;
			return -EAGAIN;
		}
		return -1;
	}

	bulk_stats.recv_bytes += ret;
	return ret;
}

/*
 * Handle end-of-transfer marker: send ACK and handle COW mode continuation.
 */
static int handle_end_of_transfer(struct ps_async_read_bulk *ar, u32 cmd)
{
	struct page_server_iov ack = {
		.cmd = PS_IOV_BULK_COMPLETE_ACK,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = 0,
	};
	int sk = get_page_server_sk();

	pr_err("=== REPLICA PHASE 2: Bulk transfer complete ===\n");
	pr_info("Received end-of-transfer marker (cmd=%u dst_id=%lu)\n", cmd,
		(unsigned long)ar->pi.dst_id);
	set_bulk_stream_done();

	/*
	 * Send ACK back to primary so it can break out of
	 * page_server_serve() and proceed to Phase 3 (skeleton dump).
	 */
	page_server_tcp_nodelay(sk, true);
	if (page_server_send(sk, &ack, sizeof(ack), 0) != sizeof(ack))
		pr_perror("Failed to send bulk complete ACK");
	else
		pr_info("Sent bulk complete ACK to primary\n");

	/*
	 * COW mode: don't return BULK_STREAM_COMPLETE yet.
	 * Dirty pages will arrive as compressed batches (Phase 4 sends them
	 * directly during freeze), followed by PS_IOV_ALL_PAGES_SENT signal.
	 * Reset to read next header and continue.
	 */
	if (opts.cow_dump && !cow_is_all_pages_sent_received()) {
		pr_err("=== REPLICA: Waiting for PHASE 3-4 (skeleton + dirty pages) ===\n");
		ar->rb = 0;
		ar->compress_state = COMPRESS_STATE_READING_HEADER;
		return BULK_STREAM_PROGRESS;
	}

	return BULK_STREAM_COMPLETE;
}



/*
 * Read bulk header and dispatch to next state.
 */
static int read_bulk_header(struct ps_async_read_bulk *ar, int flags)
{
	int ret;
	u32 cmd;

	if (ar->rb < sizeof(ar->pi)) {
		void *buf = ((void *)&ar->pi) + ar->rb;
		int need = sizeof(ar->pi) - ar->rb;

		ret = bulk_recv(buf, need, flags);
		if (ret == -EAGAIN)
			return BULK_STREAM_WOULD_BLOCK;
		if (ret < 0) {
			pr_perror("Error reading header from page server");
			return -1;
		}
		ar->rb += ret;
	}

	if (ar->rb < sizeof(ar->pi))
		return BULK_STREAM_PROGRESS;

	/* Header complete, dispatch based on command */
	cmd = decode_ps_cmd(ar->pi.cmd);

	if (ar->pi.nr_pages == 0 && cmd != PS_IOV_ALL_PAGES_SENT)
		return handle_end_of_transfer(ar, cmd);

	switch (cmd) {
	case PS_IOV_ALL_PAGES_SENT:
		/* Primary signals all pages sent - replica can zero-fill rest */
		pr_err("=== REPLICA PHASE 4: All pages sent signal received ===\n");
		cow_set_all_pages_sent_received();
		ar->rb = 0;
		ar->compress_state = COMPRESS_STATE_READING_HEADER;
		/*
		 * Return COMPLETE to stop reading - primary is waiting for ACK.
		 * The main loop will check cow_handle_exit() and send the ACK.
		 */
		return BULK_STREAM_COMPLETE;

	case PS_IOV_ADD_F_COMPRESS:
		ar->compress_state = COMPRESS_STATE_READING_SIZE;
		ar->compressed_size = 0;
		ar->compressed_rb = 0;
		break;

	default:
		/* Uncompressed page data */
		ar->compress_state = COMPRESS_STATE_READING_UNCOMPRESSED;
		ar->goal = sizeof(ar->pi) + ar->pi.nr_pages * PAGE_SIZE;
		break;
	}

	return BULK_STREAM_PROGRESS;
}

/*
 * Read 4-byte compressed size and allocate buffer.
 */
static int read_compressed_size(struct ps_async_read_bulk *ar, int flags)
{
	int ret;
	int need = sizeof(ar->compressed_size) - ar->compressed_rb;
	void *buf = ((char *)&ar->compressed_size) + ar->compressed_rb;

	ret = bulk_recv(buf, need, flags);
	if (ret == -EAGAIN)
		return BULK_STREAM_WOULD_BLOCK;
	if (ret < 0) {
		pr_perror("Error reading compressed size");
		return -1;
	}
	ar->compressed_rb += ret;

	if (ar->compressed_rb < sizeof(ar->compressed_size))
		return BULK_STREAM_PROGRESS;

	/* Validate compressed size - allow up to COW_BATCH_PAGES (64 pages) */
	if (ar->compressed_size <= 0 ||
	    ar->compressed_size > LZ4_compressBound(COW_BATCH_SIZE)) {
		pr_err("Invalid compressed size: %d (max=%d)\n",
		       ar->compressed_size, LZ4_compressBound(COW_BATCH_SIZE));
		return -1;
	}

	ar->compressed_buf = xmalloc(ar->compressed_size);
	BUG_ON(!ar->compressed_buf);

	ar->compressed_rb = 0;
	ar->compress_state = COMPRESS_STATE_READING_COMPRESSED;
	return BULK_STREAM_PROGRESS;
}

/*
 * Read compressed data, decompress, and invoke callback.
 */
static int read_compressed_data(struct ps_async_read_bulk *ar, int flags)
{
	int ret;
	int need = ar->compressed_size - ar->compressed_rb;
	void *buf = ar->compressed_buf + ar->compressed_rb;

	ret = bulk_recv(buf, need, flags);
	if (ret == -EAGAIN)
		return BULK_STREAM_WOULD_BLOCK;
	if (ret < 0) {
		pr_perror("Error reading compressed data");
		xfree(ar->compressed_buf);
		ar->compressed_buf = NULL;
		return -1;
	}
	ar->compressed_rb += ret;

	if (ar->compressed_rb < ar->compressed_size)
		return BULK_STREAM_PROGRESS;

	/* Decompress and invoke callback for each page in batch */
	{
		int decomp_ret;
		struct timespec t1, t2;
		int expected_size = ar->pi.nr_pages * PAGE_SIZE;
		void *decomp_buf;
		unsigned long i;

		/* Allocate buffer for batch decompression */
		if (ar->pi.nr_pages > 1) {
			decomp_buf = xmalloc(expected_size);
			BUG_ON(!decomp_buf);
		} else {
			decomp_buf = ar->pages;  /* Single page - use existing buffer */
		}

		clock_gettime(CLOCK_MONOTONIC, &t1);
		decomp_ret = LZ4_decompress_safe(ar->compressed_buf, decomp_buf,
						 ar->compressed_size, expected_size);
		clock_gettime(CLOCK_MONOTONIC, &t2);
		bulk_stats.decompress_calls++;
		bulk_stats.decompress_time_ns +=
			(t2.tv_sec - t1.tv_sec) * 1000000000 + (t2.tv_nsec - t1.tv_nsec);
		xfree(ar->compressed_buf);
		ar->compressed_buf = NULL;

		if (decomp_ret != expected_size) {
			pr_err("LZ4 decompression failed: expected %d, got %d\n",
			       expected_size, decomp_ret);
			if (ar->pi.nr_pages > 1)
				xfree(decomp_buf);
			return -1;
		}

		pr_err("BULK_RECV_DEBUG: decompressed batch vaddr=0x%lx nr_pages=%u cmd=%u\n",
		       (unsigned long)ar->pi.vaddr, ar->pi.nr_pages,
		       decode_ps_cmd(ar->pi.cmd));

		/* Invoke callback for each page in batch */
		for (i = 0; i < ar->pi.nr_pages; i++) {
			void *page_data = decomp_buf + i * PAGE_SIZE;
			unsigned long page_vaddr = ar->pi.vaddr + i * PAGE_SIZE;

			/* Copy to ar->pages for single-page compat, or pass directly */
			if (ar->pi.nr_pages > 1)
				memcpy(ar->pages, page_data, PAGE_SIZE);

			bulk_stats.callback_calls++;
			bulk_stats.pages_completed++;

			ret = ar->complete((int)ar->pi.dst_id, page_vaddr, 1, ar->priv);
			if (ret < 0) {
				if (ar->pi.nr_pages > 1)
					xfree(decomp_buf);
				return ret;
			}
		}

		if (ar->pi.nr_pages > 1)
			xfree(decomp_buf);
	}

	ar->rb = 0;
	ar->goal = 0;
	ar->compress_state = COMPRESS_STATE_READING_HEADER;
	ar->compressed_size = 0;
	ar->compressed_rb = 0;
	return BULK_STREAM_PROGRESS;
}

/*
 * Read uncompressed page data and invoke callback.
 */
static int read_uncompressed_data(struct ps_async_read_bulk *ar, int flags)
{
	int ret;
	void *buf = ar->pages + (ar->rb - sizeof(ar->pi));
	int need = ar->goal - ar->rb;

	ret = bulk_recv(buf, need, flags);
	if (ret == -EAGAIN)
		return BULK_STREAM_WOULD_BLOCK;
	if (ret < 0) {
		pr_perror("Error reading uncompressed page data");
		return -1;
	}
	ar->rb += ret;

	if (ar->rb < ar->goal)
		return BULK_STREAM_PROGRESS;

	bulk_stats.callback_calls++;
	bulk_stats.pages_completed++;

	ret = ar->complete((int)ar->pi.dst_id, (unsigned long)ar->pi.vaddr,
			   (int)ar->pi.nr_pages, ar->priv);
	if (ret < 0)
		return ret;

	ar->rb = 0;
	ar->goal = 0;
	ar->compress_state = COMPRESS_STATE_READING_HEADER;
	return BULK_STREAM_PROGRESS;
}

/*
 * Bulk mode continuous stream reader.
 * Processes headers and pages as they arrive without correlation to requests.
 * The server's background thread sends pages continuously.
 * Supports compressed pages (PS_IOV_ADD_F_COMPRESS).
 */
static int page_server_read_bulk_stream(struct ps_async_read_bulk *ar, int flags)
{
	pr_debug("bulk_stream: state=%d rb=%lu flags=%d\n",
		ar->compress_state, ar->rb, flags);

	switch (ar->compress_state) {
	case COMPRESS_STATE_READING_HEADER:
		return read_bulk_header(ar, flags);

	case COMPRESS_STATE_READING_SIZE:
		return read_compressed_size(ar, flags);

	case COMPRESS_STATE_READING_COMPRESSED:
		return read_compressed_data(ar, flags);

	case COMPRESS_STATE_READING_UNCOMPRESSED:
		return read_uncompressed_data(ar, flags);

	default:
		pr_err("Invalid bulk stream state: %d\n", ar->compress_state);
		BUG();
	}
}

static void check_and_print_bulk_stats(void)
{
	time_t now = time(NULL);

	if (now - bulk_stats.last_print_time >= 1) {
		struct timespec ts;
		struct tm *tm;
		clock_gettime(CLOCK_REALTIME, &ts);
		tm = localtime(&ts.tv_sec);
		pr_debug("[BULK_RECV_STATS] [%02d:%02d:%02d.%03ld] recv=%lu block=%lu bytes=%lu recv_wait_ns=%lu pages=%lu decomp=%lu decomp_ns=%lu cb=%lu\n",
			tm->tm_hour, tm->tm_min, tm->tm_sec, ts.tv_nsec / 1000000,
			bulk_stats.recv_calls,
			bulk_stats.recv_would_block,
			bulk_stats.recv_bytes,
			bulk_stats.recv_wait_time_ns,
			bulk_stats.pages_completed,
			bulk_stats.decompress_calls,
			bulk_stats.decompress_time_ns,
			bulk_stats.callback_calls);

		memset(&bulk_stats, 0, sizeof(bulk_stats));
		bulk_stats.last_print_time = now;
	}
}

int page_server_async_read_bulk(struct epoll_rfd *f)
{
	struct ps_async_read_bulk *ar;
	int ret;

	check_and_print_bulk_stats();

	if (list_empty(&bulk_async_reads)) {
		if (opts.cow_dump && page_server_bulk_stream_done())
			return 0;
		pr_err("Bulk async read with empty queue\n");
		BUG();
	}

	ar = list_first_entry(&bulk_async_reads, struct ps_async_read_bulk, l);
	ret = page_server_read_bulk_stream(ar, MSG_DONTWAIT);

	if (ret == BULK_STREAM_COMPLETE) {
		/* End marker - cleanup stream reader */
		list_del(&ar->l);
		xfree(ar);
		/* Only break epoll loop for all_pages_sent - other COMPLETE cases continue */
		if (cow_is_all_pages_sent_received()) {
			pr_info("page_server_async_read_bulk: BULK_STREAM_COMPLETE + all_pages_sent, returning 1 to break epoll\n");
			return 1;
		}
		pr_info("page_server_async_read_bulk: BULK_STREAM_COMPLETE, returning 0\n");
		return 0;
	}
	if (ret < 0) {
		pr_err("page_server_async_read_bulk: bulk_stream returned %d\n", ret);
		return -1;
	}

	/* ret == BULK_STREAM_WOULD_BLOCK or BULK_STREAM_PROGRESS - keep going */
	return 0;
}

int page_server_start_async_read_bulk(void *buf, unsigned long nr_pages,
				      ps_async_read_complete complete, void *priv)
{
	struct ps_async_read_bulk *ar;

	/* In bulk mode, only create reader once - it processes continuous stream */
	if (!list_empty(&bulk_async_reads))
		return 0;

	ar = xmalloc(sizeof(*ar));
	BUG_ON(!ar);

	ar->pages = buf;
	ar->rb = 0;
	ar->goal = 0; /* Will be set when header arrives */
	ar->complete = complete;
	ar->priv = priv;

	/* Initialize compression state */
	ar->compress_state = COMPRESS_STATE_READING_HEADER;
	ar->compressed_size = 0;
	ar->compressed_rb = 0;
	ar->compressed_buf = NULL;

	list_add_tail(&ar->l, &bulk_async_reads);
	return 0;
}

/*
 * Cleanup async bulk reader state.
 * Called when closing the page server socket to prevent stale fd reads.
 */
void page_server_cleanup_async_bulk(void)
{
	struct ps_async_read_bulk *ar, *tmp;

	list_for_each_entry_safe(ar, tmp, &bulk_async_reads, l) {
		list_del(&ar->l);
		if (ar->compressed_buf)
			xfree(ar->compressed_buf);
		xfree(ar);
	}
	pr_debug("Cleaned up async bulk reader state\n");
}
