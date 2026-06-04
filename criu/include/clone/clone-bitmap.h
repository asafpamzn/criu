#ifndef __CR_CLONE_BITMAP_H__
#define __CR_CLONE_BITMAP_H__

#include <stdbool.h>

/* CLONE bitmap operations for tracking write-faulted pages */
extern void clone_set_bitmap(unsigned long vaddr);
extern void clone_clear_bitmap(unsigned long vaddr);
extern bool clone_test_bitmap(unsigned long vaddr);
extern bool clone_test_and_set_bitmap(unsigned long vaddr);

#endif /* __CR_CLONE_BITMAP_H__ */
