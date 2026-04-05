#ifndef __CR_COW_LAZY_PAGES_H__
#define __CR_COW_LAZY_PAGES_H__

#include <stdbool.h>
#include <sys/epoll.h>

/*
 * COW Phase 2 lazy-pages entry point.
 * Called when --cow-dump --page-server are specified.
 * Buffers incoming pages without requiring inventory.img/pstree.img.
 */
extern int cr_lazy_pages_cow_phase2(bool daemon);

/*
 * Event loop for Phase 2 page buffering.
 */
extern int cow_phase2_handle_pages(int epollfd, struct epoll_event *events, int nr_fds);

#endif /* __CR_COW_LAZY_PAGES_H__ */
