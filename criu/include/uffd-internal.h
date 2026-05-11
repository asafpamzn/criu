#ifndef __CR_UFFD_INTERNAL_H_
#define __CR_UFFD_INTERNAL_H_

#include <stdbool.h>
#include <sys/uio.h>
#include "int.h"
#include "common/list.h"
#include "util.h"
#include "pagemap.h"
#include "common/lock.h"

/*
 * Internal uffd structures shared between uffd.c and uffd_cow.c
 */

struct lazy_iov {
	struct list_head l;
	unsigned long start;	 /* run-time start address, tracks remaps */
	unsigned long end;	 /* run-time end address, tracks remaps */
	unsigned long img_start; /* start address at the dump time */
	bool is_new_vma;	 /* true if this IOV is for a Phase 3 new VMA */
};

struct lazy_pages_info {
	int pid;
	bool exited;

	struct list_head iovs;
	struct list_head reqs;

	struct lazy_pages_info *parent;
	unsigned ref_cnt;

	struct page_read pr;

	unsigned long xfer_len; /* in pages */
	unsigned long total_pages;
	unsigned long copied_pages;

	struct epoll_rfd lpfd;

	struct list_head l;

	unsigned long buf_size;
	void *buf;
};

/* Pending EAGAIN requests (for bulk mode) */
struct uffd_eagain_request {
	struct list_head l;
	struct lazy_pages_info *lpi;
	__u64 address;
	unsigned long nr_pages;
	void *buf;  /* Copy of data that couldn't be written */
};


/* Logging macros for lazy_pages_info */
#define lp_debug(lpi, fmt, arg...)  pr_debug("%d-%d: " fmt, lpi->pid, lpi->lpfd.fd, ##arg)
#define lp_info(lpi, fmt, arg...)   pr_info("%d-%d: " fmt, lpi->pid, lpi->lpfd.fd, ##arg)
#define lp_warn(lpi, fmt, arg...)   pr_warn("%d-%d: " fmt, lpi->pid, lpi->lpfd.fd, ##arg)
#define lp_err(lpi, fmt, arg...)    pr_err("%d-%d: " fmt, lpi->pid, lpi->lpfd.fd, ##arg)
#define lp_perror(lpi, fmt, arg...) pr_perror("%d-%d: " fmt, lpi->pid, lpi->lpfd.fd, ##arg)

/* Helper functions from uffd.c needed by cow-uffd.c */
extern void lpi_put(struct lazy_pages_info *lpi);
extern void lazy_pages_summary(struct lazy_pages_info *lpi);

/*
 * COW-specific functions from uffd_cow.c
 */

/*
 * Handle COW mode exit conditions.
 * Waits for: all_pages_sent signal, drain thread done, buffer empty.
 * Sends ACK to primary, cleans up lpis.
 * Returns: 1 = exit main loop, 0 = continue
 */
extern int cow_handle_exit(struct list_head *lpis);

#endif /* __CR_UFFD_INTERNAL_H_ */
