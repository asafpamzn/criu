#ifndef __CR_CLONE_MEM_H__
#define __CR_CLONE_MEM_H__

#include <stdbool.h>
#include "int.h"
#include "common/list.h"
#include "vma.h"

struct lazy_vma_entry {
	uint64_t start;
	uint64_t end;
	struct list_head list;
	struct vma_area *vma;
	unsigned long total_pages;    /* Total pages in this VMA */	
	u64 dst_id;                   /* Process identifier for this VMA */
	pid_t source_pid;             /* PID for process_vm_readv */
};

/* Global lazy VMA list management */
extern struct list_head *get_global_lazy_vmas(void);
extern void clone_mem_init_lazy_vmas(void);
extern void free_global_lazy_vmas(void);

/* Lazy VMA lookup functions */
extern unsigned long count_lazy_vma_pages(u64 dst_id);
extern int add_lazy_vma_for_new_region(unsigned long start, unsigned long len,
				       u64 dst_id, pid_t source_pid);

/* Add a lazy VMA entry during dump (called from generate_iovs in mem.c) */
extern int clone_mem_add_lazy_vma(struct vma_area *vma, unsigned long nr_pages,
				u64 dst_id, pid_t source_pid);




#endif /* __CR_CLONE_MEM_H__ */
