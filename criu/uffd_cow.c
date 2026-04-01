/*
 * COW (Copy-on-Write) specific handling for lazy pages (uffd).
 *
 * This file contains COW-specific functions extracted from uffd.c
 * for handling bulk page transfer completion and buffer draining
 * during COW phased migration.
 */

#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"
#include "uffd-internal.h"
#include "uffd.h"
#include "cow-uffd.h"
#include "page-xfer.h"

#undef LOG_PREFIX
#define LOG_PREFIX "uffd-cow: "

/*
 * Handle COW mode exit conditions.
 *
 * Exit sequence:
 * 1. Wait for all_pages_sent signal (guarantees all pages received from socket)
 * 2. Wait for drain thread to finish (buffer empty)
 * 3. Send ACK to primary
 * 4. Cleanup and exit
 *
 * Returns:
 *   1  - should break the main loop (all done)
 *   0  - should continue the main loop
 */
int cow_handle_exit(struct list_head *lpis)
{
	struct lazy_pages_info *lpi, *n;

	/* Only log when state changes to avoid log spam */
	static int last_signal = -1, last_drain = -1;
	static unsigned long call_count = 0;
	int cur_signal = is_all_pages_sent_received();
	int cur_drain = cow_drain_thread_running();

	call_count++;
	if (cur_signal != last_signal || cur_drain != last_drain || call_count % 100 == 0) {
		pr_err("cow_handle_exit[%lu]: signal=%d drain=%d buffer=%lu\n",
		       call_count, cur_signal, cur_drain, cow_page_buffer_count());
		last_signal = cur_signal;
		last_drain = cur_drain;
	}

	/* Condition 1: Wait for all_pages_sent signal from primary */
	if (!cur_signal) {
		return 0;
	}

	/* Condition 2: Wait for drain thread to finish */
	if (cur_drain) {
		return 0;
	}

	/* Condition 3: Wait for buffer to be empty */
	if (cow_page_buffer_count() > 0) {
		pr_err("cow_handle_exit: waiting for buffer to drain (%lu pages remaining)\n",
		       cow_page_buffer_count());
		return 0;
	}

	/* Condition 4: Wait for EAGAIN requests to be processed */
	if (!is_eagain_queue_empty()) {
		pr_err("cow_handle_exit: waiting for EAGAIN requests to be processed\n");
		return 0;
	}

	/* All conditions met - send ACK to primary */
	pr_err("All pages received and drained, sending ACK to primary\n");
	if (send_all_pages_sent_ack() < 0)
		pr_warn("Failed to send all_pages_sent ACK\n");

	/* Cleanup all lpis */
	list_for_each_entry_safe(lpi, n, lpis, l) {
		lazy_pages_summary(lpi);
		list_del(&lpi->l);
		lpi_put(lpi);
	}

	return 1;  /* Exit main loop */
}
