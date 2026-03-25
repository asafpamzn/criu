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

	/* Condition 1: Wait for all_pages_sent signal from primary */
	if (!is_all_pages_sent_received()) {
		pr_debug("Waiting for all_pages_sent signal\n");
		return 0;
	}

	/* Condition 2: Wait for drain thread to finish */
	if (cow_drain_thread_running()) {
		pr_debug("Waiting for drain thread to finish\n");
		return 0;
	}

	/* Condition 3: Wait for buffer to be empty */
	if (cow_page_buffer_count() > 0) {
		pr_debug("Waiting for buffer to drain (%lu pages remaining)\n",
			 cow_page_buffer_count());
		return 0;
	}

	/* All conditions met - send ACK to primary */
	pr_info("All pages received and drained, sending ACK to primary\n");
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
