#ifndef __CR_CLONE_LAZY_PAGES_H__
#define __CR_CLONE_LAZY_PAGES_H__

#include <stdbool.h>
#include <sys/epoll.h>

/*
 * CLONE Phase 2 lazy-pages entry point.
 * Called when --clone-dump --page-server are specified.
 * Buffers incoming pages without requiring inventory.img/pstree.img.
 */
extern int cr_lazy_pages_clone_phase2(bool daemon);

/*
 * Event loop for Phase 2 page buffering.
 */
extern int clone_phase2_handle_pages(int epollfd, struct epoll_event *events, int nr_fds);

#endif /* __CR_CLONE_LAZY_PAGES_H__ */
