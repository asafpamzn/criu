#ifndef __CR_UFFD_H_
#define __CR_UFFD_H_

struct task_restore_args;

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

/* Store dirty bitmap if it arrives before restore connects */
extern void store_pending_dirty_bitmap(unsigned long *ranges, unsigned int nr_ranges);

/* Check if restore has connected (uffd available) */
extern bool is_restore_connected(void);

#endif /* __CR_UFFD_H_ */
