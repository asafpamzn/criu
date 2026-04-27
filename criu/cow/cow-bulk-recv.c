/*
 * COW control message receiver (REPLICA side).
 *
 * All page data is transferred via P3 receiver threads. This module
 * only handles control messages on the main page server socket:
 * - End-of-transfer marker (nr_pages == 0)
 * - PS_IOV_ALL_PAGES_SENT signal
 */

#include <errno.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#include "types.h"
#include "criu-log.h"
#include "cr_options.h"
#include "xmalloc.h"
#include "page-xfer.h"
#include "cow/cow-page-xfer.h"
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
 * Async read state for control message processing on the main socket.
 */
struct ps_async_read_bulk {
	unsigned long rb;      /* Bytes read of current header */
	struct page_server_iov pi;
	struct list_head l;
};

static LIST_HEAD(bulk_async_reads);

/*
 * Helper: recv with EAGAIN/EINTR handling for non-blocking reads.
 * Returns bytes received on success, -EAGAIN on would-block, -1 on error.
 */
static int bulk_recv(void *buf, int need, int flags)
{
	int sk = get_page_server_sk();
	int ret;

	ret = page_server_recv(sk, buf, need, flags);
	if (ret < 0) {
		if (flags == MSG_DONTWAIT && (errno == EAGAIN || errno == EINTR))
			return -EAGAIN;
		return -1;
	}

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
	 * Wait for PS_IOV_ALL_PAGES_SENT signal. Reset to read next header.
	 */
	if (opts.cow_dump && !cow_is_all_pages_sent_received()) {
		pr_err("=== REPLICA: Waiting for PHASE 3-4 (skeleton + dirty pages) ===\n");
		ar->rb = 0;
		return BULK_STREAM_PROGRESS;
	}

	return BULK_STREAM_COMPLETE;
}

/*
 * Read control message header from main socket.
 * Only end-of-transfer markers and PS_IOV_ALL_PAGES_SENT are expected.
 * Page data on this socket is a bug — all data goes via P3 threads.
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

	/* Header complete — reset for next header */
	ar->rb = 0;
	cmd = decode_ps_cmd(ar->pi.cmd);

	/* End-of-transfer marker: nr_pages == 0, not ALL_PAGES_SENT */
	if (ar->pi.nr_pages == 0 && cmd != PS_IOV_ALL_PAGES_SENT)
		return handle_end_of_transfer(ar, cmd);

	if (cmd == PS_IOV_ALL_PAGES_SENT) {
		/* Primary signals all pages sent - replica can zero-fill rest */
		pr_err("=== REPLICA PHASE 4: All pages sent signal received ===\n");
		cow_set_all_pages_sent_received();
		/*
		 * Return COMPLETE to stop reading - primary is waiting for ACK.
		 * The main loop will check cow_handle_exit() and send the ACK.
		 */
		return BULK_STREAM_COMPLETE;
	}

	/*
	 * Unexpected page data on main socket. All page data should go
	 * through P3 receiver threads. Log details for debugging.
	 */
	pr_err("BUG: page data arrived on main socket!\n");
	pr_err("  cmd=%u nr_pages=%lu vaddr=0x%lx dst_id=%lu\n",
	       cmd, (unsigned long)ar->pi.nr_pages,
	       (unsigned long)ar->pi.vaddr,
	       (unsigned long)ar->pi.dst_id);
	pr_err("  bulk_stream_done=%d all_pages_sent=%d\n",
	       page_server_bulk_stream_done(),
	       cow_is_all_pages_sent_received());
	BUG();
	return -1; /* unreachable */
}

int page_server_async_read_bulk(struct epoll_rfd *f)
{
	struct ps_async_read_bulk *ar;
	int ret;

	if (list_empty(&bulk_async_reads)) {
		if (opts.cow_dump && page_server_bulk_stream_done())
			return 0;
		pr_err("Bulk async read with empty queue\n");
		BUG();
	}

	ar = list_first_entry(&bulk_async_reads, struct ps_async_read_bulk, l);
	ret = read_bulk_header(ar, MSG_DONTWAIT);

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

int page_server_start_async_read_bulk(void)
{
	struct ps_async_read_bulk *ar;

	/* Only create reader once - it processes the continuous control stream */
	if (!list_empty(&bulk_async_reads))
		return 0;

	ar = xmalloc(sizeof(*ar));
	BUG_ON(!ar);

	ar->rb = 0;
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
		xfree(ar);
	}
	pr_debug("Cleaned up async bulk reader state\n");
}
