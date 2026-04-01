#ifndef __CR_COW_MEM_H__
#define __CR_COW_MEM_H__

#include <stdbool.h>
#include "int.h"
#include "list.h"
#include "vma.h"

struct lazy_vma_entry {
	uint64_t start;
	uint64_t end;
	struct list_head list;
	struct vma_area *vma;
	unsigned char *sent_bitmap;   /* Track which pages have been sent */
	uint8_t *cow_bitmap;          /* Track which pages were write-faulted */
	unsigned long total_pages;    /* Total pages in this VMA */
	_Atomic unsigned long sent_pages;  /* Count of set bits in sent_bitmap */
	u64 dst_id;                   /* Process identifier for this VMA */
	pid_t source_pid;             /* PID for process_vm_readv */
};

/* Global lazy VMA list management */
extern struct list_head *get_global_lazy_vmas(void);
extern void cow_mem_init_lazy_vmas(void);
extern void free_global_lazy_vmas(void);

/* Lazy VMA lookup functions */
extern struct lazy_vma_entry *find_lazy_vma_for_addr(unsigned long vaddr, u64 dst_id);
extern struct lazy_vma_entry *find_lazy_vma_by_addr(unsigned long vaddr);
extern unsigned long count_lazy_vma_pages(u64 dst_id);
extern int add_lazy_vma_for_new_region(unsigned long start, unsigned long len,
				       u64 dst_id, pid_t source_pid);

/* Add a lazy VMA entry during dump (called from generate_iovs in mem.c) */
extern int cow_mem_add_lazy_vma(struct vma_area *vma, unsigned long nr_pages,
				u64 dst_id, pid_t source_pid);

/* COW convergence mode - Phase 3 dirty page handling */
extern bool is_convergence_mode(void);
extern unsigned long get_convergence_dirty_pages(void);
extern unsigned long prepare_lazy_vmas_for_convergence(unsigned long *dirty_ranges,
						       unsigned int nr_dirty_ranges);

/* Verify all lazy VMA pages have been sent */
extern long verify_all_lazy_vmas_sent(void);

#endif /* __CR_COW_MEM_H__ */
