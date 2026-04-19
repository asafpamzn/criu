#ifndef __CR_UFFD_H_
#define __CR_UFFD_H_

struct task_restore_args;
struct epoll_event;

extern int uffd_open(int flags, unsigned long *features, int *err);
extern bool uffd_noncooperative(void);
extern int setup_uffd(int pid, struct task_restore_args *task_args);
extern int lazy_pages_setup_zombie(int pid);
extern int prepare_lazy_pages_socket(void);
extern int lazy_pages_finish_restore(void);

/* COW phased migration: apply buffered pages after receiving dirty bitmap */
extern int apply_buffered_pages(int uffd, unsigned long *dirty_ranges,
				unsigned int nr_dirty_ranges);

/* Return uffd of first active lazy_pages_info. Used by page-xfer.c. */
extern int get_first_lpi_uffd(void);

/*
 * COW state accessors are in cow-uffd.c (include cow/cow-uffd.h):
 * - cow_is_restore_connected(), cow_set_restore_connected()
 * - cow_is_all_pages_sent_received(), cow_set_all_pages_sent_received()
 * - cow_get_uffd_for_vaddr()
 * - cow_queue_drain_eagain_request(), cow_is_eagain_queue_empty()
 */



/* COW Phase 2: Initialize page buffer for pre-buffering */
extern int page_buffer_init(void);

/* COW Phase 2: Set up async bulk reader for pre-buffering pages */
extern int cow_setup_prebuffer_reader(void);

/* COW Phase 3: Enter restore loop after pages buffered and pstree loaded */
extern int cow_phase3_restore_loop(int epollfd, struct epoll_event **events, int nr_fds);

#endif /* __CR_UFFD_H_ */
