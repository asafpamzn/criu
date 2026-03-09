#ifndef __CR_COW_LAZY_PAGES_H__
#define __CR_COW_LAZY_PAGES_H__

#include <stdbool.h>
#include <sys/epoll.h>

/*
 * COW Phase 2 lazy-pages entry point.
 * Called when --cow-dump --page-server are specified.
 */
extern int cr_lazy_pages_cow_phase2(bool daemon);

/*
 * Event loop for Phase 2 page buffering.
 */
extern int cow_phase2_handle_pages(int epollfd, struct epoll_event *events, int nr_fds);

/*
 * Add a page to the COW page buffer.
 */
extern int cow_page_buffer_add(int pid, unsigned long vaddr, void *data, size_t len);

/*
 * Lookup a page in the COW page buffer.
 * Returns pointer to page data or NULL if not found.
 */
extern void *cow_page_buffer_lookup(int pid, unsigned long vaddr);

/*
 * Statistics.
 */
extern unsigned long cow_get_buffered_pages_count(void);
extern unsigned long cow_get_buffered_pages_bytes(void);

#endif /* __CR_COW_LAZY_PAGES_H__ */
